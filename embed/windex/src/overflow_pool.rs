use std::{
  ptr::{null_mut, slice_from_raw_parts_mut},
  sync::atomic::{AtomicPtr, AtomicU64, Ordering},
};

use crate::{Result, bucket::HashBucket, error::Error};

/// 溢出桶内存池
///
/// 对照 C# Tsavorite `MallocFixedPageSize<HashBucket>`（core/Allocator/MallocFixedPageSize.cs）：
/// - 同：两级分块连续内存、1-based 索引（0 恒为无效分配索引）、原子计数递增分配、
///   free-list 机会主义复用、并发竞争败者安全回收冗余块；
/// - 异：free-list 用 Treiber 无锁栈 + 32 位 ABA 代数标签（C# 为 ConcurrentQueue 队列）；
///   chunk 严格按需延迟分配（C# 分配当前页时预分配下一页）；溢出指针 48 位可寻址
///   空间下池容量上限 MAX_CHUNKS×CHUNK_SIZE = 2^22 桶。
///
/// 采用两级分块连续内存分配机制：
/// - 每个内存块（Chunk）包含 1024 个连续的 64 字节对齐 `HashBucket`
/// - 通过原子序号按需分配，返回从 1 开始的 1-based 索引（0 表示无溢出桶）
/// - 支持高并发无锁读取与线程安全延迟块分配
pub struct OverflowPool {
  chunks: Box<[AtomicPtr<HashBucket>]>,
  allocated: AtomicU64,
  free_head: AtomicU64,
  /// 空闲栈长度：`free` 压栈成功 +1，`allocate` 出栈成功 -1（线性化点同步；
  /// 瞬态快照可偏 ±转移中节点，守恒校验须静止态执行）
  free_count: AtomicU64,
}

impl OverflowPool {
  /// 每个内存块的桶数量（2^10 = 1024）
  pub const CHUNK_BITS: usize = 10;
  /// 每个内存块的桶数量（1024 个）
  pub const CHUNK_SIZE: usize = 1 << Self::CHUNK_BITS;
  /// 块内桶索引掩码（0x3FF）
  pub const CHUNK_MASK: usize = Self::CHUNK_SIZE - 1;
  /// 最大支持的内存块数量（4096 个块，支持超 400 万个溢出桶）
  pub const MAX_CHUNKS: usize = 4096;
  /// 空闲栈低 32 位桶索引掩码（高 32 位为 ABA 代数标签）
  pub const FREE_LIST_ID_MASK: u64 = 0xFFFF_FFFF;

  /// 构造一个空的溢出桶内存池
  pub fn new() -> Self {
    let chunks = (0..Self::MAX_CHUNKS)
      .map(|_| AtomicPtr::new(null_mut()))
      .collect::<Box<[_]>>();
    Self {
      chunks,
      allocated: AtomicU64::new(0),
      free_head: AtomicU64::new(0),
      free_count: AtomicU64::new(0),
    }
  }

  /// 原子分配一个新的溢出桶，返回从 1 开始的 1-based 索引
  ///
  /// 优先复用 free-list 中回收的桶，完全对齐 libs/storage/Tsavorite/cs/src/core/Allocator/MallocFixedPageSize.cs:MallocFixedPageSize。
  pub fn allocate(&self) -> Result<u64> {
    // 1. 优先尝试从无锁空闲单向栈（Treiber Stack with 32-bit ABA Tag）中复用已回收的溢出桶
    let mut curr = self.free_head.load(Ordering::Acquire);
    while (curr & Self::FREE_LIST_ID_MASK) != 0 {
      let head_id = curr & Self::FREE_LIST_ID_MASK;
      let tag = (curr >> 32) as u32;
      if let Some(bucket) = self.get(head_id) {
        let next_id = bucket.entries[0].load(Ordering::Acquire) & Self::FREE_LIST_ID_MASK;
        let next_val = (((tag.wrapping_add(1)) as u64) << 32) | next_id;
        match self.free_head.compare_exchange_weak(
          curr,
          next_val,
          Ordering::AcqRel,
          Ordering::Acquire,
        ) {
          Ok(_) => {
            bucket.entries[0].store(0, Ordering::Release);
            self.free_count.fetch_sub(1, Ordering::AcqRel);
            return Ok(head_id);
          }
          Err(actual) => curr = actual,
        }
      } else {
        break;
      }
    }

    // 2. 空闲栈为空，执行原子序号递增分配
    let prev = self.allocated.fetch_add(1, Ordering::AcqRel);
    let id = prev + 1;
    let zero_based = prev as usize;
    let chunk_idx = zero_based >> Self::CHUNK_BITS;

    if chunk_idx >= Self::MAX_CHUNKS {
      self.allocated.fetch_sub(1, Ordering::AcqRel);
      return Err(Error::OverflowPoolExhausted);
    }

    if self.chunks[chunk_idx].load(Ordering::Acquire).is_null()
      && let Err(e) = self.ensure_chunk(chunk_idx)
    {
      // 块分配失败：回滚计数，保证 allocated_count 与实际可用桶严格一致
      self.allocated.fetch_sub(1, Ordering::AcqRel);
      return Err(e);
    }

    Ok(id)
  }

  /// 归还/回收一个未被成功挂载的溢出桶（对标 libs/storage/Tsavorite/cs/src/core/Allocator/MallocFixedPageSize.cs:Free）
  ///
  /// 使用基于 AtomicU64 的无锁单向栈与 32 位代数计数器（Tag），彻底杜绝 ABA 问题。
  pub fn free(&self, id: u64) {
    if id == 0 || id > self.allocated.load(Ordering::Acquire) {
      return;
    }
    if let Some(bucket) = self.get(id) {
      // 清空该桶内容，确保复用时状态纯净
      for entry in &bucket.entries {
        entry.store(0, Ordering::Relaxed);
      }
      let mut curr = self.free_head.load(Ordering::Acquire);
      loop {
        let tag = (curr >> 32) as u32;
        let head_id = curr & Self::FREE_LIST_ID_MASK;
        bucket.entries[0].store(head_id, Ordering::Release);
        let next_val = (((tag.wrapping_add(1)) as u64) << 32) | (id & Self::FREE_LIST_ID_MASK);
        match self.free_head.compare_exchange_weak(
          curr,
          next_val,
          Ordering::AcqRel,
          Ordering::Acquire,
        ) {
          Ok(_) => {
            self.free_count.fetch_add(1, Ordering::AcqRel);
            break;
          }
          Err(actual) => curr = actual,
        }
      }
    }
  }

  /// 判定当前是否存在可复用的已回收溢出桶
  ///
  /// 委托单一真相源 [`Self::free_count`] 计数器（读取空闲栈头指针会引入
  /// 第二个观测点，两读之间可能已变化；计数读为单次原子快照）。
  #[inline]
  pub fn has_free(&self) -> bool {
    self.free_count.load(Ordering::Acquire) != 0
  }

  /// 当前空闲栈中的桶数量（O(1)）
  ///
  /// 线性化点同步：`free` 压栈成功才 +1，`allocate` 出栈成功才 -1；
  /// 并发瞬态快照可偏 ±转移中节点，守恒校验须静止态执行
  /// （`allocated_count` == 已挂载数 + 本值，抓泄漏与重复回收）。
  /// 其余用于池水位观测。注意 C# 同类池（MallocFixedPageSize.Free / OverflowPool）
  /// 语义为机会主义复用，静止态残留空闲桶属正常行为。
  #[inline]
  pub fn free_count(&self) -> u64 {
    self.free_count.load(Ordering::Acquire)
  }

  /// 根据 1-based 索引获取对应的溢出桶引用（纯只读，零副作用）
  ///
  /// 合法索引（`allocate()` 返回值及已挂载链上的索引）对应内存块必然已由 `allocate()`
  /// 同步初始化，因此本方法不触发任何分配；未初始化/越界索引一律返回 `None`。
  #[inline]
  pub fn get(&self, id: u64) -> Option<&HashBucket> {
    if id == 0 || id > self.allocated.load(Ordering::Acquire) {
      return None;
    }
    let zero_based = (id - 1) as usize;
    let chunk_idx = zero_based >> Self::CHUNK_BITS;
    let slot_idx = zero_based & Self::CHUNK_MASK;

    let ptr = self.chunks.get(chunk_idx)?.load(Ordering::Acquire);
    if ptr.is_null() {
      return None;
    }

    // SAFETY: 块内 slot_idx 由 id 掩码推导，严格小于 CHUNK_SIZE；
    // 指针由 Box 稳定持有且永不被释放（Drop 时机在池析构，届时引用不可再存在）。
    unsafe { Some(&*ptr.add(slot_idx)) }
  }

  /// 内部快速路径：无校验取回 1-based 索引对应的溢出桶引用（跳过上界检查与 null 兜底）
  ///
  /// 仅供 crate 内部从合法 overflow_index 取值的热路径使用（ChainWalker::advance 等），
  /// 公开 [`Self::get`] 保持完整的校验与兜底语义不变。
  ///
  /// # Safety
  /// 调用方须保证 `id` 来自桶上已合法挂载的溢出指针：恒由 [`Self::allocate`]
  /// 成功产出（`1..=allocated`，且 allocate 内部已 ensure 对应 chunk 非空）。
  /// 不变量：`allocated` 单调不减（free 仅回收进空闲栈不回退计数）、chunk
  /// 指针一旦非空永不变回空（Box 稳定持有，直至池 Drop），故 id 恒可解析。
  /// 可见性：挂载方 set_overflow_index 以 AcqRel CAS 发布，读者经
  /// overflow_index() 的 Acquire 读取建立 happens-before，Relaxed 读 chunk
  /// 指针必见已发布非空值。
  #[inline]
  pub unsafe fn get_unchecked(&self, id: u64) -> &HashBucket {
    let zero_based = (id - 1) as usize;
    // SAFETY: 调用方保证 id 为合法挂载值，chunk_idx < chunks.len()（MAX_CHUNKS）
    let chunk = unsafe { self.chunks.get_unchecked(zero_based >> Self::CHUNK_BITS) };
    debug_assert!(
      !chunk.load(Ordering::Relaxed).is_null(),
      "合法溢出索引对应的 chunk 必须已初始化"
    );
    // SAFETY: chunk 指针一旦非空永不变，Relaxed 读取即可（happens-before 由
    // 调用方的 overflow_index() Acquire 建立）
    unsafe {
      &*chunk
        .load(Ordering::Relaxed)
        .add(zero_based & Self::CHUNK_MASK)
    }
  }

  /// 获取当前已分配的溢出桶总数量
  #[inline]
  pub fn allocated_count(&self) -> u64 {
    self.allocated.load(Ordering::Acquire)
  }

  /// 确保指定下标的内存块已被分配并初始化
  fn ensure_chunk(&self, chunk_idx: usize) -> Result<()> {
    if chunk_idx >= Self::MAX_CHUNKS {
      return Err(Error::OverflowPoolExhausted);
    }
    if !self.chunks[chunk_idx].load(Ordering::Acquire).is_null() {
      return Ok(());
    }

    let chunk = (0..Self::CHUNK_SIZE)
      .map(|_| HashBucket::new())
      .collect::<Box<[HashBucket]>>();
    let raw = Box::into_raw(chunk) as *mut HashBucket;

    match self.chunks[chunk_idx].compare_exchange(
      null_mut(),
      raw,
      Ordering::AcqRel,
      Ordering::Acquire,
    ) {
      Ok(_) => Ok(()),
      Err(_) => {
        // 并发竞态：其他线程已抢先完成该块初始化，安全回收当前冗余块
        // SAFETY: raw 由上方 Box::into_raw 产出，布局恰为 [HashBucket; CHUNK_SIZE]；
        // CAS 失败意味着本指针从未被发布共享，此处独占回收无别名风险。
        unsafe {
          let slice_ptr = slice_from_raw_parts_mut(raw, Self::CHUNK_SIZE);
          let _ = Box::from_raw(slice_ptr);
        }
        Ok(())
      }
    }
  }
}

impl Default for OverflowPool {
  fn default() -> Self {
    Self::new()
  }
}

impl Drop for OverflowPool {
  fn drop(&mut self) {
    for chunk in self.chunks.iter() {
      let ptr = chunk.load(Ordering::Relaxed);
      if !ptr.is_null() {
        // SAFETY: 指针均由 ensure_chunk 的 Box::into_raw 发布且析构期无并发访问
        //（&mut self 独占），布局为 [HashBucket; CHUNK_SIZE]，还原 Box 归还分配器。
        unsafe {
          let slice_ptr = slice_from_raw_parts_mut(ptr, Self::CHUNK_SIZE);
          let _ = Box::from_raw(slice_ptr);
        }
      }
    }
  }
}
