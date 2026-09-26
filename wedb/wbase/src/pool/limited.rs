//! 网络分级定长缓冲区池化管理
//!
//! 1:1 对标微软 Garnet LimitedFixedBufferPool（libs/common/Memory/LimitedFixedBufferPool.cs）
//!
//! 内部管理为分级队列数组：第 i 层承载容量 `min_allocation_size << i` 的定长缓冲
//! （对标 C# `queue[i] contains memory segments each of size (2^i * sectorSize)`，
//! LimitedFixedBufferPool.cs:16-21 类注释）。借出时按请求尺寸向上对齐定位层级弹出复用，
//! 归还时按缓冲实际容量精确匹配层级回池，仅越界（超最高层级或非 2 的幂次）缓冲走系统释放，
//! 使 64KB~512KB 区间内扩容与收敛的收发缓冲均能无损池化复用。
//!
//! 队列选型：C# 侧每层池为 `ConcurrentQueue<PoolEntry>`（libs/common/Memory/PoolLevel.cs:16），
//! 借出/归还是每 I/O 两次的纯非阻塞 TryDequeue/Enqueue（LimitedFixedBufferPool.cs:158/:120），
//! 竞争激烈且无需异步等待 → crossfire::flavor::Array（有界 MPMC 环，`Queue` trait 的
//! push/pop 即 TryDequeue/Enqueue 的零 CAS 重试等价物），环满裁断对应 C#
//! `Interlocked.Increment(size) <= maxEntriesPerLevel` 的入池上限（LimitedFixedBufferPool.cs:117）。

use std::{
  fmt,
  ops::{Deref, DerefMut},
  sync::{
    Arc,
    atomic::{AtomicUsize, Ordering::Relaxed},
  },
};

use crossfire::flavor::{Array, Queue};

use crate::align::CachePadded;

/// 默认基础（最低层级）网络缓冲区规格：64KB
pub const DEFAULT_BUFFER_SIZE: usize = 1 << 16;
/// 每层级最大常驻闲置缓冲数（对标 C# 构造函数 maxEntriesPerLevel 默认值 16）
pub const DEFAULT_MAX_ENTRIES_PER_LEVEL: usize = 16;
/// 默认层级数（对标 C# 构造函数 numLevels 默认值 4，即 min << 0..3 共 4 档）
pub const DEFAULT_NUM_LEVELS: usize = 4;

/// 池化缓冲区句柄（零 Arc 开销，RAII 自动归还；对标 C# `PoolEntry` 的
/// Get/Return 单一借还口）
pub struct PooledRefBuffer<'a> {
  buffer: Option<Vec<u8>>,
  /// TLS 读空闲段初始化代际（见 [`Self::prime_key`]）
  prime_key: Option<(usize, usize)>,
  pool: &'a LimitedFixedBufferPool,
}

impl<'a> PooledRefBuffer<'a> {
  /// 获取底层 Vec 引用
  #[inline]
  pub fn vec_ref(&self) -> &Vec<u8> {
    self.buffer.as_ref().expect("pooled buffer active")
  }

  /// 获取底层可变 Vec 引用
  #[inline]
  pub fn vec_mut(&mut self) -> &mut Vec<u8> {
    self.buffer.as_mut().expect("pooled buffer active")
  }

  /// 提取底层 Vec 并放弃自动归还池
  pub fn take(mut self) -> Vec<u8> {
    self.buffer.take().unwrap_or_default()
  }

  /// 临时提取底层 Vec 用于所有权转移（如异步 I/O），后续须通过 [`Self::set_buffer`] 归还
  #[inline]
  pub fn take_buffer(&mut self) -> Option<Vec<u8>> {
    self.buffer.take()
  }

  /// 归还或更新底层 Vec
  #[inline]
  pub fn set_buffer(&mut self, buf: Vec<u8>) {
    self.buffer = Some(buf);
  }

  /// TLS 读空闲段初始化代际（键随同块缓冲的借还转存：槽为本连接唯一
  /// 借还锚点，键由借出方在读毕回写、下次借出取用，同一块缓冲跨读免重清零）
  #[inline]
  pub fn prime_key(&self) -> Option<(usize, usize)> {
    self.prime_key
  }

  /// 回写初始化代际（键须描述本次借出的同一块缓冲；换块必清，键-缓冲同生命周期）
  #[inline]
  pub fn set_prime_key(&mut self, key: Option<(usize, usize)>) {
    self.prime_key = key;
  }
}

impl<'a> Deref for PooledRefBuffer<'a> {
  type Target = Vec<u8>;
  #[inline]
  fn deref(&self) -> &Self::Target {
    self.vec_ref()
  }
}

impl<'a> DerefMut for PooledRefBuffer<'a> {
  #[inline]
  fn deref_mut(&mut self) -> &mut Self::Target {
    self.vec_mut()
  }
}

impl<'a> Drop for PooledRefBuffer<'a> {
  fn drop(&mut self) {
    // 归还逻辑对标 C# `Return`（LimitedFixedBufferPool.cs:107-128）：
    // 按容量精确匹配层级，命中则清零回池，越界/非 2 的幂次就地系统释放，
    // 环满（超出 maxEntriesPerLevel）push 失败同样就地释放，全程零 realloc；
    // 提取后未归还（take_buffer 在途丢弃）仅递减借出计数
    if let Some(mut buf) = self.buffer.take() {
      self.pool.borrowed_count.fetch_sub(1, Relaxed);
      if let Some(level) = self.pool.position(buf.capacity()) {
        buf.clear();
        let _ = self.pool.levels[level].push(buf);
      }
    } else {
      self.pool.borrowed_count.fetch_sub(1, Relaxed);
    }
  }
}

impl<'a> fmt::Debug for PooledRefBuffer<'a> {
  fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    f.debug_struct("PooledRefBuffer")
      .field("len", &self.buffer.as_ref().map_or(0, Vec::len))
      .field(
        "capacity",
        &self.buffer.as_ref().map(|v| v.capacity()).unwrap_or(0),
      )
      .finish()
  }
}

/// 固定规格分级网络缓冲池（基于 crossfire::flavor::Array 纯原子无锁架构）
pub struct LimitedFixedBufferPool {
  /// 第 i 层承载容量 `min_allocation_size << i` 的闲置缓冲队列
  /// （对标 C# `PoolLevel[] pool`，层级预建，池级对象仅数字节环成本）
  levels: Box<[Array<Vec<u8>>]>,
  min_allocation_size: usize,
  max_entries_per_level: usize,
  num_levels: usize,
  /// 池可承载的最大规格：`min_allocation_size << (num_levels - 1)`
  /// （对标 C# `maxAllocationSize`，LimitedFixedBufferPool.cs:29）
  max_allocation_size: usize,
  allocated_count: CachePadded<AtomicUsize>,
  /// 越界（超出最高层级）分配累计数（对标 C# `totalOutOfBoundAllocations`）
  out_of_bound_allocations: CachePadded<AtomicUsize>,
  pub(crate) borrowed_count: CachePadded<AtomicUsize>,
}

impl fmt::Debug for LimitedFixedBufferPool {
  fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    f.debug_struct("LimitedFixedBufferPool")
      .field("min_allocation_size", &self.min_allocation_size)
      .field("max_allocation_size", &self.max_allocation_size)
      .field("num_levels", &self.num_levels)
      .field("max_entries_per_level", &self.max_entries_per_level)
      .field("borrowed_count", &self.borrowed_count())
      .field("free_count", &self.free_count())
      .finish()
  }
}

impl LimitedFixedBufferPool {
  /// 创建分级网络缓冲池（对标 C# 构造函数
  /// `LimitedFixedBufferPool(minAllocationSize, maxEntriesPerLevel = 16, numLevels = 4)`，
  /// LimitedFixedBufferPool.cs:67；本签名对应 C# 省略默认层级参数的调用形，恒用
  /// [`DEFAULT_NUM_LEVELS`]，0 入参回退默认值）
  pub fn new(min_allocation_size: usize, max_entries_per_level: usize) -> Arc<Self> {
    Self::with_levels(
      min_allocation_size,
      max_entries_per_level,
      DEFAULT_NUM_LEVELS,
    )
  }

  /// 全参构造（对标 C# 构造函数显式传 numLevels 的形态，
  /// NetworkBufferSettings.CreateBufferPool 依网络规格推导层级数的场景）
  ///
  /// 在 garnet 中的相对路径:libs/common/NetworkBufferSettings.cs:NetworkBufferSettings.CreateBufferPool
  fn with_levels(
    min_allocation_size: usize,
    max_entries_per_level: usize,
    num_levels: usize,
  ) -> Arc<Self> {
    // 分级体系刚性要求 2 的幂次基础规格（C# GetLevel 以 Debug.Assert(IsPow2) 前置，
    // 此处向上圆整为运行期自愈），保证层规格恒为 2 的幂、归还端等值命中
    let min = min_allocation_size.max(1).next_power_of_two();
    let entries = if max_entries_per_level == 0 {
      DEFAULT_MAX_ENTRIES_PER_LEVEL
    } else {
      max_entries_per_level
    };
    // 至少 1 级，避免 `min << (n - 1)` 在 0 级下移位饱和出无意义规格域
    let num_levels = num_levels.max(1);
    let max_allocation_size = min << (num_levels - 1);
    Arc::new(Self {
      levels: (0..num_levels)
        .map(|_| Array::new(entries))
        .collect::<Vec<_>>()
        .into_boxed_slice(),
      min_allocation_size: min,
      max_entries_per_level: entries,
      num_levels,
      max_allocation_size,
      allocated_count: CachePadded::new(AtomicUsize::new(0)),
      out_of_bound_allocations: CachePadded::new(AtomicUsize::new(0)),
      borrowed_count: CachePadded::new(AtomicUsize::new(0)),
    })
  }

  /// 层级规格：`min_allocation_size << level`（对标 C# `minAllocationSize << i`）
  #[inline]
  fn level_size(&self, level: usize) -> usize {
    self.min_allocation_size << level
  }

  /// 规格 → 层级索引映射（对标 C# `Position` + `GetLevel`，
  /// LimitedFixedBufferPool.cs:271-292）：仅接受不低于基础规格、不超最高层级
  /// 且为 2 的幂次的规格；层级 = 比值 2 的幂指数差，常数阶位运算
  #[inline]
  fn position(&self, size: usize) -> Option<usize> {
    if size < self.min_allocation_size || !size.is_power_of_two() {
      return None;
    }
    let level = Self::get_level(self.min_allocation_size, size);
    (level < self.num_levels).then_some(level)
  }

  /// 由基础规格与目标规格求层级（对标 C# 静态 `GetLevel`：
  /// ratio == 1 → 0，否则 Log2(ratio - 1) + 1；调用方保证双 2 的幂且 ratio >= 1）
  #[inline]
  fn get_level(min_allocation_size: usize, requested_size: usize) -> usize {
    let ratio = requested_size / min_allocation_size;
    if ratio <= 1 {
      0
    } else {
      (ratio - 1).ilog2() as usize + 1
    }
  }

  #[inline]
  fn alloc_or_pop(&self, min_size: usize) -> Vec<u8> {
    self.borrowed_count.fetch_add(1, Relaxed);
    // 对标 C# `Get`：请求规格向上对齐至所属层级（低于基础规格并入 0 级），
    // 保证池中块容量恒为层级规格、归还端可无损复配
    let aligned = min_size.max(self.min_allocation_size).next_power_of_two();
    if aligned > self.max_allocation_size {
      // 越界分配（totalOutOfBoundAllocations 计数臂）：精确按需走系统堆，不入分级域
      self.out_of_bound_allocations.fetch_add(1, Relaxed);
      self.allocated_count.fetch_add(1, Relaxed);
      return Vec::with_capacity(min_size);
    }
    let level = Self::get_level(self.min_allocation_size, aligned);
    match self.levels[level].pop() {
      Some(v) => v,
      None => {
        self.allocated_count.fetch_add(1, Relaxed);
        Vec::with_capacity(self.level_size(level))
      }
    }
  }

  /// 借出一个覆盖指定最小容量需求的缓冲区句柄（对标 C# `Get`，唯一生产借出口）
  #[inline]
  pub fn get_ref(&self, min_size: usize) -> PooledRefBuffer<'_> {
    let buffer = self.alloc_or_pop(min_size);
    PooledRefBuffer {
      buffer: Some(buffer),
      prime_key: None,
      pool: self,
    }
  }

  /// 清空全部分级队列中闲置缓冲区（对标 C# `Purge`，LimitedFixedBufferPool.cs:184-193）
  pub fn purge(&self) {
    for level in self.levels.iter() {
      while level.pop().is_some() {}
    }
  }

  /// 当前池中全层级闲置可用缓冲区总数
  #[inline]
  pub fn free_count(&self) -> usize {
    self.levels.iter().map(Array::len).sum()
  }

  /// 当前借出中的缓冲区数量
  #[inline]
  pub fn borrowed_count(&self) -> usize {
    self.borrowed_count.load(Relaxed)
  }

  /// 累计分配的新缓冲区总数
  #[inline]
  pub fn allocated_count(&self) -> usize {
    self.allocated_count.load(Relaxed)
  }

  /// 累计越界（超最高层级）分配数（对标 C# `totalOutOfBoundAllocations`）
  #[inline]
  pub fn out_of_bound_allocations(&self) -> usize {
    self.out_of_bound_allocations.load(Relaxed)
  }

  /// 池基准定长块容量（借还契约唯一锚点：网络泵从本池借出并归还的块容量
  /// 须恒等于此值，归还端据此精准配平层级）
  ///
  /// 在 garnet 中的相对路径:libs/common/Memory/LimitedFixedBufferPool.cs:MinAllocationSize
  /// （C# NetworkHandler.cs:105 即以该访问器单点读取池基准规格构造接收缓冲）
  #[inline]
  pub fn buffer_size(&self) -> usize {
    self.min_allocation_size
  }

  /// 池统计串（统计输出单点，格式串全仓仅此一处；
  /// rust format! 模板仅接受字面量，无法引用常量）
  ///
  /// 在 garnet 中的相对路径:libs/common/Memory/LimitedFixedBufferPool.cs:GetStats
  pub fn get_stats(&self) -> String {
    let mut stats = format!(
      "borrowed_buffers={} num_levels={} max_entries_per_level={} min_allocation_size={} max_allocation_size={} total_free_buffers={} allocated_buffers={} out_of_bound_allocations={}",
      self.borrowed_count(),
      self.num_levels,
      self.max_entries_per_level,
      self.min_allocation_size,
      self.max_allocation_size,
      self.free_count(),
      self.allocated_count(),
      self.out_of_bound_allocations(),
    );
    // 各层级闲置分布（对标 C# GetStats 的 `,{items}={Format.MemoryBytes(size)}` 尾段）
    for (level, queue) in self.levels.iter().enumerate() {
      stats.push_str(&format!(
        " free_at_{}KB={}",
        self.level_size(level) >> 10,
        queue.len()
      ));
    }
    stats
  }
}

#[cfg(test)]
mod tests {
  use std::{mem::align_of, thread};

  use super::*;

  #[test]
  fn test_buffer_pool_cache_line_alignment() {
    let pool = LimitedFixedBufferPool::new(1024, 16);
    assert_eq!(align_of::<CachePadded<AtomicUsize>>(), 128);
    let alloc_addr = &*pool.allocated_count as *const AtomicUsize as usize;
    let borrow_addr = &*pool.borrowed_count as *const AtomicUsize as usize;
    assert!(alloc_addr.abs_diff(borrow_addr) >= 128);
  }

  #[test]
  fn test_buffer_pool_base_size_accessor() {
    // buffer_size 借还契约锚点：恒等于基准规格（构造期 2 的幂次向上圆整值）
    assert_eq!(LimitedFixedBufferPool::new(4096, 0).buffer_size(), 4096);
    assert_eq!(LimitedFixedBufferPool::new(3000, 0).buffer_size(), 4096);
    assert_eq!(
      LimitedFixedBufferPool::new(DEFAULT_BUFFER_SIZE, 0).buffer_size(),
      DEFAULT_BUFFER_SIZE
    );
  }

  #[test]
  fn test_buffer_pool_reuse_and_purge() {
    let pool = LimitedFixedBufferPool::new(1024, 16);
    assert_eq!(pool.allocated_count(), 0);
    assert_eq!(pool.free_count(), 0);

    {
      let mut b1 = pool.get_ref(512);
      assert_eq!(pool.allocated_count(), 1);
      assert_eq!(pool.borrowed_count(), 1);
      b1.extend_from_slice(b"hello world");
      assert_eq!(&b1[..], b"hello world");
    }

    // Drop 后自动归还
    assert_eq!(pool.borrowed_count(), 0);
    assert_eq!(pool.free_count(), 1);

    // 再次借出应复用
    {
      let b2 = pool.get_ref(1024);
      assert_eq!(pool.allocated_count(), 1);
      assert_eq!(pool.free_count(), 0);
      assert!(b2.is_empty());
    }

    assert_eq!(pool.free_count(), 1);
    pool.purge();
    assert_eq!(pool.free_count(), 0);
  }

  #[test]
  fn test_buffer_pool_take_and_drop() {
    let pool = LimitedFixedBufferPool::new(1024, 16);
    let b = pool.get_ref(1024);
    assert_eq!(pool.borrowed_count(), 1);

    // take 后主动放弃归还
    let raw = b.take();
    assert_eq!(raw.capacity(), 1024);
    assert_eq!(pool.borrowed_count(), 0);
    assert_eq!(pool.free_count(), 0);
  }

  #[test]
  fn test_buffer_pool_take_buffer_and_set_buffer() {
    let pool = LimitedFixedBufferPool::new(1024, 16);
    let mut b = pool.get_ref(1024);
    assert_eq!(pool.borrowed_count(), 1);

    // 模拟所有权临时移交给底层异步 I/O (如 Read)
    let raw = b.take_buffer().expect("buffer should exist");
    assert!(b.buffer.is_none());

    // 异步完成归还
    b.set_buffer(raw);
    assert!(b.buffer.is_some());

    drop(b);
    assert_eq!(pool.borrowed_count(), 0);
    assert_eq!(pool.free_count(), 1);
  }

  #[test]
  fn test_buffer_pool_dropped_without_set_buffer() {
    let pool = LimitedFixedBufferPool::new(1024, 16);
    let mut b = pool.get_ref(1024);
    assert_eq!(pool.borrowed_count(), 1);

    // 提取后若发生异常丢弃，未 set_buffer 归还
    let _raw = b.take_buffer();
    drop(b);

    // 依然正确扣减借出计数，避免计数永久泄漏
    assert_eq!(pool.borrowed_count(), 0);
    assert_eq!(pool.free_count(), 0);
  }

  #[test]
  fn test_buffer_pool_oversized_allocation() {
    // 基础 1KB × 4 级：阶梯顶 8KB，1MB 请求越界走系统堆
    let pool = LimitedFixedBufferPool::new(1024, 16);
    {
      let b = pool.get_ref(1 << 20);
      assert!(b.capacity() >= 1 << 20);
      assert_eq!(pool.borrowed_count(), 1);
      assert_eq!(pool.allocated_count(), 1);
      assert_eq!(pool.out_of_bound_allocations(), 1);
    }

    // 越界缓冲归还不入分级池，就地系统释放
    assert_eq!(pool.borrowed_count(), 0);
    assert_eq!(pool.free_count(), 0);
  }

  #[test]
  fn test_buffer_pool_capacity_limit() {
    let pool = LimitedFixedBufferPool::new(1024, 2);
    let b1 = pool.get_ref(1024);
    let b2 = pool.get_ref(1024);
    let b3 = pool.get_ref(1024);

    drop(b1);
    drop(b2);
    assert_eq!(pool.free_count(), 2);

    // 验证每层级池容量有界，超出 max_entries_per_level 的归还被丢弃
    drop(b3);
    assert_eq!(pool.free_count(), 2);
  }

  #[test]
  fn test_buffer_pool_multithread_contention() {
    let pool = LimitedFixedBufferPool::new(1024, 32);
    let mut handles = Vec::new();

    for _ in 0..8 {
      let p = Arc::clone(&pool);
      handles.push(thread::spawn(move || {
        for _ in 0..1000 {
          let mut b = p.get_ref(1024);
          b.extend_from_slice(b"concurrent test");
          assert_eq!(&b[..], b"concurrent test");
        }
      }));
    }

    for h in handles {
      h.join().unwrap();
    }

    assert_eq!(pool.borrowed_count(), 0);
    assert!(pool.free_count() <= 32);
  }
}
