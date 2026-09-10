#[cfg(target_arch = "aarch64")]
use core::arch::asm;
#[cfg(target_arch = "x86_64")]
use core::arch::x86_64::{_MM_HINT_T0, _mm_prefetch};
use std::{
  fmt,
  hint::spin_loop,
  iter::Chain,
  mem::size_of,
  ops::{Deref, DerefMut, Index},
  slice::{Iter, IterMut, from_raw_parts, from_raw_parts_mut},
  sync::atomic::{Ordering, fence},
  thread::{sleep, yield_now},
  time::Duration,
};

use whasher::fast_hash;
use wram::{DirectVirtualMemory, DirectVmBlock};

use crate::{
  Result,
  bucket::{BucketExclusiveGuard, BucketSharedGuard, DATA_ENTRIES, HashBucket},
  entry::HashBucketEntry,
  error::Error,
  overflow_pool::OverflowPool,
};

/// 溢出链单步推进结果
enum ChainStep {
  /// 成功前进到链上下一个桶
  Next,
  /// 链在此终止（溢出指针为 0）
  End,
  /// 步数超上限判定为链环/数据损坏（正常数据永不出现，纯防御）
  Cycle,
}

/// 溢出链遍历步数上限：溢出桶挂链后永不释放（free 仅回收挂载竞争败者的未挂载桶），
/// 故链长严格受限于池容量上限 MAX_CHUNKS×CHUNK_SIZE = 2^22；超限即判定数据损坏/链环，
/// 立即终止遍历而非死循环
const MAX_CHAIN_STEPS: usize = 1 << 22;

/// 溢出链遍历器（步数上限防御）
///
/// `curr` 沿溢出链逐步前进，`step` 计数达到 `MAX_CHAIN_STEPS` 即终止遍历；
/// 相比 Floyd 龟兔双指针，免去每两步一次的 tortoise 追踪与二次池查找，
/// 常数开销减半且状态更简单，保证损坏数据下所有链遍历安全退出而非死循环。
struct ChainWalker<'a> {
  curr: &'a HashBucket,
  step: usize,
}

impl<'a> ChainWalker<'a> {
  /// 从主桶出发开始遍历
  #[inline]
  fn new(start: &'a HashBucket) -> Self {
    Self {
      curr: start,
      step: 0,
    }
  }

  /// 沿溢出链前进一步
  #[inline]
  fn advance(&mut self, pool: &'a OverflowPool) -> ChainStep {
    let overflow_idx = self.curr.overflow_index();
    if overflow_idx == 0 {
      return ChainStep::End;
    }
    // SAFETY: 溢出链上挂载的索引恒由 OverflowPool::allocate 合法产出
    // （1..=allocated 且对应 chunk 已初始化），跳过 get 的上界检查与 null 兜底
    self.curr = unsafe { pool.get_unchecked(overflow_idx) };
    self.step += 1;
    if self.step >= MAX_CHAIN_STEPS {
      ChainStep::Cycle
    } else {
      ChainStep::Next
    }
  }
}

/// 哈希槽位精确定位句柄（严格对标 C# Garnet HashEntryInfo）
///
/// 封装单趟遍历定位出的哈希桶指针、槽位索引、旧条目与 Tag，
/// 支持无需二次哈希、无需重新扫描桶链的定点原子 CAS（对标 libs/storage/Tsavorite/cs/src/core/Index/Tsavorite/Implementation/HashEntryInfo.cs:TryCAS）。
#[derive(Debug)]
pub struct HashEntryInfo<'a> {
  pub(crate) bucket: &'a HashBucket,
  pub(crate) slot: usize,
  pub(crate) raw: u64,
  pub(crate) tag: u16,
}

impl<'a> HashEntryInfo<'a> {
  /// 是否在哈希表中命中已存在的匹配 Tag 槽位
  #[inline]
  pub fn is_found(&self) -> bool {
    self.raw != 0
  }

  /// 获取匹配槽位当前存储的逻辑地址
  #[inline]
  pub fn address(&self) -> u64 {
    HashBucketEntry::from_raw(self.raw).address()
  }

  /// 直接在已知槽位上尝试原子 CAS 写入新地址（严格对标 libs/storage/Tsavorite/cs/src/core/Index/Tsavorite/Implementation/HashEntryInfo.cs:TryCAS）
  ///
  /// - 若为新 Key（`!is_found()`，即 `raw == 0`）：执行 CAS 0 -> new_raw
  /// - 若为已有 Key 更新：执行 CAS old_raw -> new_raw
  #[inline]
  pub fn try_cas(&mut self, new_address: u64) -> bool {
    if new_address == HashBucketEntry::INVALID_ADDRESS
      || new_address > HashBucketEntry::ADDRESS_MASK
    {
      return false;
    }
    debug_assert!(self.slot < DATA_ENTRIES, "try_cas 槽位越界");
    let new_entry = HashBucketEntry::new(new_address, self.tag, false);
    let new_raw = new_entry.as_raw();
    if unsafe { self.bucket.entries.get_unchecked(self.slot) }
      .compare_exchange(self.raw, new_raw, Ordering::AcqRel, Ordering::Acquire)
      .is_ok()
    {
      self.raw = new_raw;
      true
    } else {
      false
    }
  }

  /// 尝试原子置零当前槽位以实现物理脱钩删除（Record Elision）
  ///
  /// 若当前槽位包含有效记录且未被并发覆写，单指令 CAS 0 脱钩回收物理槽位。
  /// raw 为 0 时必须直接失败：CAS(0 -> 0) 恒成功会误报删除成功。
  #[inline]
  pub fn try_elide(&mut self) -> bool {
    debug_assert!(self.slot < DATA_ENTRIES, "try_elide 槽位越界");
    if !self.is_found() {
      return false;
    }
    if unsafe { self.bucket.entries.get_unchecked(self.slot) }
      .compare_exchange(self.raw, 0, Ordering::AcqRel, Ordering::Acquire)
      .is_ok()
    {
      self.raw = 0;
      true
    } else {
      false
    }
  }
}

/// 栈上候选地址小列表，避免热点查询时的高频堆分配（内嵌 8 槽位 64B 数组）
#[derive(Debug, Clone)]
pub struct CandidateAddresses {
  buf: [u64; 8],
  len: u8,
  extra: Vec<u64>,
}

impl Default for CandidateAddresses {
  #[inline]
  fn default() -> Self {
    Self::new()
  }
}

impl CandidateAddresses {
  /// 构造一个全空的候选地址小列表
  #[inline]
  pub const fn new() -> Self {
    Self {
      buf: [0; 8],
      len: 0,
      extra: Vec::new(),
    }
  }

  /// 追加一个候选逻辑地址
  #[inline]
  pub fn push(&mut self, addr: u64) {
    if (self.len as usize) < self.buf.len() {
      self.buf[self.len as usize] = addr;
      self.len += 1;
    } else {
      self.extra.push(addr);
    }
  }

  /// 检查列表是否为空
  #[inline]
  pub fn is_empty(&self) -> bool {
    self.len == 0
  }

  /// 获取候选地址总数
  #[inline]
  pub fn len(&self) -> usize {
    (self.len as usize) + self.extra.len()
  }

  /// 获取首个候选地址（若存在；不变量：extra 非空时 buf 必满，len==0 则整体为空）
  #[inline]
  pub fn first(&self) -> Option<u64> {
    (self.len != 0).then(|| self.buf[0])
  }

  /// 获取候选地址双端迭代器（栈数组优先，链上溢出堆切片）
  #[inline]
  pub fn iter(&self) -> impl DoubleEndedIterator<Item = &u64> {
    self.buf[..self.len as usize]
      .iter()
      .chain(self.extra.iter())
  }

  /// 判定是否包含指定逻辑地址
  #[inline]
  pub fn contains(&self, addr: u64) -> bool {
    self.buf[..self.len as usize].contains(&addr) || self.extra.contains(&addr)
  }

  /// 当候选地址未发生堆溢出时返回栈上连续切片，发生堆溢出时返回 None
  #[inline]
  pub fn as_slice(&self) -> Option<&[u64]> {
    if self.extra.is_empty() {
      Some(&self.buf[..self.len as usize])
    } else {
      None
    }
  }

  /// 保留满足谓词的候选地址
  #[inline]
  pub fn retain<F: FnMut(u64) -> bool>(&mut self, mut f: F) {
    let len = self.len as usize;
    let mut new_len = 0;
    for i in 0..len {
      let val = self.buf[i];
      if f(val) {
        self.buf[new_len] = val;
        new_len += 1;
      }
    }
    self.len = new_len as u8;
    self.extra.retain(|&x| f(x));

    // 若 buf 产生空余空间且 extra 存有溢出项，搬回栈数组，恢复栈快速路径
    let available = self.buf.len() - (self.len as usize);
    if available > 0 && !self.extra.is_empty() {
      let take = available.min(self.extra.len());
      let start = self.len as usize;
      self.buf[start..start + take].copy_from_slice(&self.extra[..take]);
      self.extra.drain(..take);
      self.len += take as u8;
    }
  }

  /// 按地址降序快速排列（保证优先匹配最新写入的逻辑地址）
  #[inline]
  pub fn sort_descending(&mut self) {
    if self.extra.is_empty() {
      let len = self.len as usize;
      match len {
        0 | 1 => {}
        2 => {
          if self.buf[0] < self.buf[1] {
            self.buf.swap(0, 1);
          }
        }
        _ => {
          self.buf[..len].sort_unstable_by(|a, b| b.cmp(a));
        }
      }
    } else {
      let buf_len = self.len as usize;
      self.extra.reserve(buf_len);
      self.extra.extend_from_slice(&self.buf[..buf_len]);
      self.extra.sort_unstable_by(|a, b| b.cmp(a));
      let main_len = self.buf.len().min(self.extra.len());
      self.buf[..main_len].copy_from_slice(&self.extra[..main_len]);
      self.extra.drain(..main_len);
      self.len = main_len as u8;
    }
  }

  /// 转换为标准 Vec
  pub fn to_vec(&self) -> Vec<u64> {
    let mut v = Vec::with_capacity(self.len());
    v.extend_from_slice(&self.buf[..self.len as usize]);
    v.extend_from_slice(&self.extra);
    v
  }
}

impl Index<usize> for CandidateAddresses {
  type Output = u64;

  #[inline]
  fn index(&self, idx: usize) -> &Self::Output {
    let len = self.len as usize;
    if idx < len {
      &self.buf[idx]
    } else {
      &self.extra[idx - len]
    }
  }
}

impl PartialEq for CandidateAddresses {
  fn eq(&self, other: &Self) -> bool {
    self.len() == other.len() && self.iter().zip(other.iter()).all(|(a, b)| a == b)
  }
}

impl Eq for CandidateAddresses {}

/// 候选地址拥有权双端迭代器
///
/// 由 [`CandidateAddresses::into_iter`] 产出；crate 根不导出（opaque 迭代器，
/// 调用方经 `IntoIterator` 使用，无需命名本类型）。
pub struct CandidateAddressesIntoIter {
  candidates: CandidateAddresses,
  start: usize,
  end: usize,
}

impl Iterator for CandidateAddressesIntoIter {
  type Item = u64;

  #[inline]
  fn next(&mut self) -> Option<Self::Item> {
    if self.start >= self.end {
      return None;
    }
    let val = self.candidates[self.start];
    self.start += 1;
    Some(val)
  }

  #[inline]
  fn size_hint(&self) -> (usize, Option<usize>) {
    let rem = self.end - self.start;
    (rem, Some(rem))
  }
}

impl DoubleEndedIterator for CandidateAddressesIntoIter {
  #[inline]
  fn next_back(&mut self) -> Option<Self::Item> {
    if self.start >= self.end {
      return None;
    }
    self.end -= 1;
    Some(self.candidates[self.end])
  }
}

impl ExactSizeIterator for CandidateAddressesIntoIter {}

impl IntoIterator for CandidateAddresses {
  type Item = u64;
  type IntoIter = CandidateAddressesIntoIter;

  #[inline]
  fn into_iter(self) -> Self::IntoIter {
    let end = self.len();
    CandidateAddressesIntoIter {
      candidates: self,
      start: 0,
      end,
    }
  }
}

impl<'a> IntoIterator for &'a CandidateAddresses {
  type Item = &'a u64;
  type IntoIter = Chain<Iter<'a, u64>, Iter<'a, u64>>;

  #[inline]
  fn into_iter(self) -> Self::IntoIter {
    self.buf[..self.len as usize]
      .iter()
      .chain(self.extra.iter())
  }
}

/// 基于 DirectVirtualMemory 的 Demand-Zero 瞬时映射哈希桶数组 (严格对标 C# Tsavorite `DirectVirtualMemory.Allocate`)
///
/// 具备以下核心性能特性：
/// 1. Demand-Zero 首次访问由内核置零，创建哈希表时耗时亚微秒级，彻底清除用户态堆循环清零；
/// 2. 64 字节 Cacheline 严格对齐；在 Linux 上若 >= 2MB 自动 2MB 边界对齐并开启透明大页 (MADV_HUGEPAGE)，削减 80%~90% 的 dTLB Miss；
/// 3. RAII 自动通过 munmap / VirtualFree 安全退役释放，实现 Send + Sync。
pub struct HashBuckets {
  block: DirectVmBlock,
  len: usize,
}

impl HashBuckets {
  /// 分配指定数量的哈希桶
  pub fn new(len: usize) -> Result<Self> {
    if len == 0 {
      return Err(Error::InvalidBucketCount(0));
    }
    let size_bytes = len
      .checked_mul(size_of::<HashBucket>())
      .ok_or(Error::InvalidBucketCount(len))?;
    let block = DirectVirtualMemory::allocate(size_bytes, 64)?;
    Ok(Self { block, len })
  }

  /// 获取桶总数
  #[inline(always)]
  pub const fn len(&self) -> usize {
    self.len
  }

  /// 是否为空
  #[inline(always)]
  pub const fn is_empty(&self) -> bool {
    self.len == 0
  }

  /// 获取只读切片
  #[inline(always)]
  pub fn as_slice(&self) -> &[HashBucket] {
    if self.len == 0 {
      &[]
    } else {
      unsafe { from_raw_parts(self.block.aligned_ptr as *const HashBucket, self.len) }
    }
  }

  /// 获取可变切片
  #[inline(always)]
  pub fn as_mut_slice(&mut self) -> &mut [HashBucket] {
    if self.len == 0 {
      &mut []
    } else {
      unsafe { from_raw_parts_mut(self.block.aligned_ptr as *mut HashBucket, self.len) }
    }
  }

  /// 获取底层对齐指针
  #[inline(always)]
  pub fn as_ptr(&self) -> *const HashBucket {
    self.block.aligned_ptr as *const HashBucket
  }

  /// 获取底层对齐可变指针
  #[inline(always)]
  pub fn as_mut_ptr(&mut self) -> *mut HashBucket {
    self.block.aligned_ptr as *mut HashBucket
  }

  /// 无越界检查直接获取哈希桶只读引用（单指令寻址，零切片构造与分支开销）
  ///
  /// # Safety
  /// 调用方须保证 `index < self.len`。
  #[inline(always)]
  pub unsafe fn get_unchecked(&self, index: usize) -> &HashBucket {
    unsafe { &*(self.block.aligned_ptr as *const HashBucket).add(index) }
  }
}

impl Deref for HashBuckets {
  type Target = [HashBucket];

  #[inline(always)]
  fn deref(&self) -> &Self::Target {
    self.as_slice()
  }
}

impl DerefMut for HashBuckets {
  #[inline(always)]
  fn deref_mut(&mut self) -> &mut Self::Target {
    self.as_mut_slice()
  }
}

impl AsRef<[HashBucket]> for HashBuckets {
  #[inline(always)]
  fn as_ref(&self) -> &[HashBucket] {
    self.as_slice()
  }
}

impl AsMut<[HashBucket]> for HashBuckets {
  #[inline(always)]
  fn as_mut(&mut self) -> &mut [HashBucket] {
    self.as_mut_slice()
  }
}

impl<'a> IntoIterator for &'a HashBuckets {
  type Item = &'a HashBucket;
  type IntoIter = Iter<'a, HashBucket>;

  #[inline(always)]
  fn into_iter(self) -> Self::IntoIter {
    self.as_slice().iter()
  }
}

impl<'a> IntoIterator for &'a mut HashBuckets {
  type Item = &'a mut HashBucket;
  type IntoIter = IterMut<'a, HashBucket>;

  #[inline(always)]
  fn into_iter(self) -> Self::IntoIter {
    self.as_mut_slice().iter_mut()
  }
}

impl fmt::Debug for HashBuckets {
  fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    f.debug_struct("HashBuckets")
      .field("len", &self.len)
      .field("aligned_ptr", &self.block.aligned_ptr)
      .finish()
  }
}

/// 64 字节 Cacheline 对齐无锁哈希索引表
///
/// 仿写 Microsoft Garnet Tsavorite 的核心哈希索引：
/// - 每个主哈希桶大小严格为 64 字节，与 CPU 缓存行对齐
/// - 内部包含 7 个数据槽位与 1 个溢出桶/自旋锁管理槽位
/// - 使用 15 位 Tag 进行常数级哈希碰撞前置过滤
/// - 单次 CAS 无锁并发插入（0 -> 完整条目，无半成品窗口；C# 两阶段 Tentative
///   协议的查重职责由调用方候选地址择新承担，见 `insert_by_hash` 注释）
/// - 支持 RCU 无锁 CAS 更新与原子置零删除
///
/// # 寻址不变量
///
/// `mask == buckets.len() - 1` 且 `buckets.len()` 恒为 2 的幂（构造时强制校验），
/// 三者一经构造终身绑定不可变：索引定容，构造后不支持在线扩容。
/// 全部 `get_unchecked` 裸寻址的安全前提均依赖该不变量（`x & mask < len` 恒成立）；
/// 公开字段仅供只读检查，外部改写将破坏该安全前提。
pub struct HashIndex {
  pub buckets: HashBuckets,
  pub overflow_pool: OverflowPool,
  pub size: usize,
  pub mask: usize,
}

impl HashIndex {
  /// 硬件预取滑动窗口大小（1:1 对标 Garnet Tsavorite PrefetchSize = 12）
  pub const PREFETCH_WINDOW: usize = 12;
  /// 栈上内联加锁条目数上限
  pub const INLINE_LOCK_ENTRIES: usize = 16;
  /// 自旋让步阈值（超过后让出 CPU 时间片）
  pub const SPIN_RETRY_THRESHOLD: usize = 32;
  /// 指数退避自旋幂次上限
  pub const SPIN_LIMIT_MAX_EXP: usize = 5;
  /// 自旋抖动掩码
  pub const SPIN_LIMIT_JITTER_MASK: usize = 0x7;
  /// yield 让核重试预算（越过此值进入睡眠退避段）
  pub const YIELD_RETRY_BUDGET: usize = 1024;
  /// 睡眠退避重试预算（100µs→1ms 封顶，合计约 8-16s 耐心窗口，抵御高负载下 CPU 调度毛刺）
  pub const SLEEP_RETRY_BUDGET: usize = 16384;
  /// 睡眠退避起始等待时间（微秒）
  pub const SLEEP_BASE_MICROS: u64 = 100;
  /// 睡眠退避递增上限（微秒）
  pub const SLEEP_MAX_ADDITIONAL_MICROS: u64 = 900;

  /// 创建指定容量的哈希索引表
  ///
  /// 要求 `num_buckets` 必须是 2 的幂且大于 0。
  pub fn new(num_buckets: usize) -> Result<Self> {
    if num_buckets == 0 || !num_buckets.is_power_of_two() {
      return Err(Error::InvalidBucketCount(num_buckets));
    }

    let buckets = HashBuckets::new(num_buckets)?;

    Ok(Self {
      buckets,
      overflow_pool: OverflowPool::new(),
      size: num_buckets,
      mask: num_buckets - 1,
    })
  }

  /// 获取指定下标的主哈希桶引用（内部自动按 mask 截断，100% 内存安全且消除分支预测越界检查）
  #[inline(always)]
  pub fn get_bucket(&self, bucket_idx: usize) -> &HashBucket {
    let idx = bucket_idx & self.mask;
    unsafe { self.buckets.get_unchecked(idx) }
  }

  /// 使用 gxhash 高性能哈希函数计算键的 64 位哈希值
  #[inline]
  pub fn hash_key(key: &[u8]) -> u64 {
    fast_hash(key)
  }

  /// 快速单槽位探针查找（严格对照 libs/storage/Tsavorite/cs/src/core/Index/Tsavorite/TsavoriteBase.cs:FindTag）
  ///
  /// 一旦遇到第 1 个匹配 tag 且非 tentative、address != 0 的 entry，立即返回其逻辑地址。
  /// 绝大多数情况下（99.9%）哈希桶第 0 或第 1 槽位即命中，完全规避全桶 7 槽位扫描原子加载与候选数组分配开销。
  #[inline]
  pub fn find_tag(&self, key: &[u8]) -> Option<u64> {
    let hash = Self::hash_key(key);
    self.find_tag_by_hash(hash)
  }

  /// 基于预先计算的哈希值进行快速单槽位探针查找
  #[inline]
  pub fn find_tag_by_hash(&self, hash: u64) -> Option<u64> {
    let tag = HashBucketEntry::tag_from_hash(hash);
    let bucket_idx = (hash as usize) & self.mask;

    let mut walker = ChainWalker::new(self.get_bucket(bucket_idx));
    loop {
      if let Some(addr) = walker.curr.find_tag_address(tag) {
        return Some(addr);
      }
      match walker.advance(&self.overflow_pool) {
        ChainStep::Next => {}
        ChainStep::End | ChainStep::Cycle => return None,
      }
    }
  }

  /// 查询匹配指定 Key 对应 Tag 的所有候选逻辑地址（零堆分配栈小数组，带链步数上限保护）
  #[inline]
  pub fn lookup_candidates(&self, key: &[u8]) -> CandidateAddresses {
    self.lookup_candidates_by_hash(Self::hash_key(key))
  }

  /// 基于预先计算好的哈希值查询候选逻辑地址
  pub fn lookup_candidates_by_hash(&self, hash: u64) -> CandidateAddresses {
    let tag = HashBucketEntry::tag_from_hash(hash);
    let bucket_idx = (hash as usize) & self.mask;

    let mut results = CandidateAddresses::new();
    let mut walker = ChainWalker::new(self.get_bucket(bucket_idx));
    let expected_hi = (tag as u64) & HashBucketEntry::TAG_MASK;

    loop {
      for item in &walker.curr.entries[..DATA_ENTRIES] {
        // 扫描仅过滤 tag/addr，Relaxed 足矣（内存序论证参见 HashBucket::find_tag_address）
        let raw = item.load(Ordering::Relaxed);
        if (raw >> HashBucketEntry::TAG_SHIFT) == expected_hi {
          let addr = raw & HashBucketEntry::ADDRESS_MASK;
          if addr != 0 {
            results.push(addr);
          }
        }
      }

      match walker.advance(&self.overflow_pool) {
        ChainStep::Next => {}
        ChainStep::End | ChainStep::Cycle => break,
      }
    }

    // 命中屏障：调用方将按候选地址解引用记录内存，fence(Acquire) 与发布方
    // CAS(AcqRel) 构成 release/acquire 同步，整链仅此一次
    if !results.is_empty() {
      fence(Ordering::Acquire);
    }

    results
  }

  /// 查询匹配指定 Key 对应 Tag 的所有候选逻辑地址列表（`Vec<u64>` 便捷封装）
  #[inline]
  pub fn lookup(&self, key: &[u8]) -> Vec<u64> {
    self.lookup_candidates(key).to_vec()
  }

  /// 向哈希索引中插入键与对应的逻辑地址
  ///
  /// 寻找空位或沿溢出链插入，单次 CAS 原子发布完整条目（无半成品窗口；语义详见
  /// [`Self::insert_by_hash`] 与其 C# 两阶段协议对照注释），链遍历以步数上限防死循环。
  /// 注意：本方法不做同 Tag 查重——键已存在时会产生多候选（同键多版本场景），需要查重语义的
  /// 调用方请使用 `find_tag_or_insert` / `find_or_create_tag`（对标 libs/storage/Tsavorite/cs/src/core/Index/Tsavorite/TsavoriteBase.cs:FindOrCreateTag）。
  #[inline]
  pub fn insert(&self, key: &[u8], address: u64) -> Result<()> {
    self.insert_by_hash(Self::hash_key(key), address)
  }

  /// 基于哈希值插入逻辑地址（并发冲突时自动归还冗余溢出桶，杜绝泄漏）
  ///
  /// 试探性 CAS 被并发竞争者抢占时，从链头重走寻找下一个空槽位（严格对标 libs/storage/Tsavorite/cs/src/core/Index/Tsavorite/TsavoriteBase.cs:FindOrCreateTag
  /// 的整链重试协议），避免链头附近留下永久空洞、推高溢出链深度恶化探测复杂度。
  pub fn insert_by_hash(&self, hash: u64, address: u64) -> Result<()> {
    if address == HashBucketEntry::INVALID_ADDRESS {
      return Err(Error::InvalidAddress(address));
    }
    if address > HashBucketEntry::ADDRESS_MASK {
      return Err(Error::AddressOverflow(address));
    }

    let tag = HashBucketEntry::tag_from_hash(hash);
    let bucket_idx = (hash as usize) & self.mask;

    'retry: loop {
      let mut walker = ChainWalker::new(self.get_bucket(bucket_idx));

      loop {
        // 1. 单次 CAS 空槽位插入最终完整条目（CAS(AcqRel) 自带发布屏障，
        //    内存序论证参见 HashBucket::find_tag_address）
        //
        //    C# 对照（TsavoriteBase.FindOrCreateTag）：C# 为两阶段协议——先 CAS 装
        //    Tentative 占位，两阶段之间夹 FindOtherSlotForThisTagMaybeTentativeInternal
        //    全链同 Tag 查重去并存。本实现把同 Tag 查重刻意上移为调用方按候选地址择新
        //    解决（见 find_or_create_tag_by_hash_with_min_addr 异同注释），两阶段之间已无
        //    任何逻辑：相邻的「CAS 装 Tentative + 平写提交」在可观测性上严格等价于单次
        //    CAS 0 -> 完整条目（读者经 matches_tag 只会看到空槽或完整条目），故合并为
        //    单次原子操作，插入热路径少一次原子写且无半成品条目窗口。
        if let Some(slot) = walker.curr.find_empty_slot() {
          if walker.curr.try_insert(slot, tag, address) {
            return Ok(());
          }
          // 空槽位被并发竞争者抢占：从链头重走，寻找下一个空槽位
          continue 'retry;
        }

        // 2. 当前桶数据槽位已满，沿溢出链推进
        match walker.advance(&self.overflow_pool) {
          ChainStep::Next => {}
          ChainStep::End => {
            // 链尾无溢出桶：分配新桶并 CAS 挂载；并发败者归还冗余桶（对标
            // libs/storage/Tsavorite/cs/src/core/Allocator/MallocFixedPageSize.cs:Free），此后无论谁挂载成功，下一桶均已就位
            if walker.curr.overflow_index() == 0 {
              let new_idx = self.overflow_pool.allocate()?;
              if !walker.curr.set_overflow_index(new_idx) {
                self.overflow_pool.free(new_idx);
              }
            }
            // 推进进入新桶继续寻找空槽；End 分支理论不可达（上一行已保证溢出指针
            // 非零），纯防御兜底
            match walker.advance(&self.overflow_pool) {
              ChainStep::Next => {}
              ChainStep::Cycle => return Err(Error::OverflowCycleDetected),
              ChainStep::End => return Err(Error::OverflowPoolExhausted),
            }
          }
          ChainStep::Cycle => return Err(Error::OverflowCycleDetected),
        }
      }
    }
  }

  /// 单次遍历执行查找或试探性插入（严格对标 libs/storage/Tsavorite/cs/src/core/Index/Tsavorite/TsavoriteBase.cs:FindOrCreateTag）
  ///
  /// 若已存在匹配 tag 且有效非试探的非零地址，直接返回 `Ok((Some(existing_addr), false))`；
  /// 若未找到，则在首个空槽位原子 CAS 插入 `(address, tag)` 并返回 `Ok((None, true))`。
  pub fn find_tag_or_insert_by_hash(&self, hash: u64, address: u64) -> Result<(Option<u64>, bool)> {
    if address == HashBucketEntry::INVALID_ADDRESS {
      return Err(Error::InvalidAddress(address));
    }
    if address > HashBucketEntry::ADDRESS_MASK {
      return Err(Error::AddressOverflow(address));
    }

    let mut spins = 0usize;
    loop {
      let mut hei = self.find_or_create_tag_by_hash(hash)?;
      if hei.is_found() {
        return Ok((Some(hei.address()), false));
      }
      if hei.try_cas(address) {
        return Ok((None, true));
      }
      spins += 1;
      if spins < Self::SPIN_RETRY_THRESHOLD {
        spin_loop();
      } else {
        yield_now();
      }
    }
  }

  /// 单次遍历查找或插入键（对标 libs/storage/Tsavorite/cs/src/core/Index/Tsavorite/TsavoriteBase.cs:FindOrCreateTag）
  #[inline]
  pub fn find_tag_or_insert(&self, key: &[u8], address: u64) -> Result<(Option<u64>, bool)> {
    self.find_tag_or_insert_by_hash(Self::hash_key(key), address)
  }

  /// 单趟遍历定位匹配 Tag 槽位或首个可用空闲槽位（严格对标 libs/storage/Tsavorite/cs/src/core/Index/Tsavorite/TsavoriteBase.cs:FindOrCreateTag / HashEntryInfo）
  #[inline]
  pub fn find_or_create_tag(&self, key: &[u8]) -> Result<HashEntryInfo<'_>> {
    self.find_or_create_tag_with_min_addr(key, 0)
  }

  /// 单趟遍历定位匹配 Tag 槽位或首个可用空闲槽位，并基于 min_valid_addr 实时清理清退已截断的死槽位
  /// （严格对标 C# Garnet TsavoriteBase.cs:338-352 FindOrCreateTag & kInvalidAddress CAS 原位清退复用）
  #[inline]
  pub fn find_or_create_tag_with_min_addr(
    &self,
    key: &[u8],
    min_valid_addr: u64,
  ) -> Result<HashEntryInfo<'_>> {
    self.find_or_create_tag_by_hash_with_min_addr(Self::hash_key(key), min_valid_addr)
  }

  /// 基于哈希值单趟遍历定位匹配 Tag 槽位或首个可用空闲槽位
  #[inline]
  pub fn find_or_create_tag_by_hash(&self, hash: u64) -> Result<HashEntryInfo<'_>> {
    self.find_or_create_tag_by_hash_with_min_addr(hash, 0)
  }

  /// 基于哈希值单趟遍历定位匹配 Tag 槽位或首个可用空闲槽位，带已截断死槽位无锁实时清退与就地复用
  ///
  /// 与 C# `TsavoriteBase.FindOrCreateTag` 的异同：
  /// - 同：单趟链遍历、记录首个空槽位、截断死槽位（address < min_valid_addr）原位 CAS 置零清退复用、
  ///   链尾无空槽时分配并 CAS 挂载新溢出桶（失败方归还冗余桶后沿赢家桶深入）；
  /// - 异：本实现不做 C# 的"先装 Tentative 占位再全链查重"两阶段协议，而是把最终值的原子 CAS
  ///   留给调用方 `HashEntryInfo::try_cas` 一次完成——读者永远只会看到 0 或完整条目，天然免去
  ///   半成品条目窗口；同 Tag 并发插入可能各占一槽形成多候选，由上层按候选地址择新解决。
  ///
  /// 内存序：扫描阶段仅过滤 tag/addr，Relaxed 加载足矣；两处命中返回前补
  /// fence(Acquire)，与发布方 CAS(AcqRel) 建立 release/acquire 同步，保证调用方
  /// 解引用命中地址的记录数据时可见其全部前置写（论证参见 HashBucket::find_tag_address）。
  pub fn find_or_create_tag_by_hash_with_min_addr(
    &self,
    hash: u64,
    min_valid_addr: u64,
  ) -> Result<HashEntryInfo<'_>> {
    let tag = HashBucketEntry::tag_from_hash(hash);
    let bucket_idx = (hash as usize) & self.mask;

    let mut walker = ChainWalker::new(self.get_bucket(bucket_idx));
    let mut first_free: Option<(&HashBucket, usize)> = None;

    'search: loop {
      for (slot, item) in walker.curr.entries[..DATA_ENTRIES].iter().enumerate() {
        let raw = item.load(Ordering::Relaxed);
        if raw == 0 {
          first_free.get_or_insert((walker.curr, slot));
          continue;
        }

        let entry = HashBucketEntry::from_raw(raw);

        // 严格对标 C# TsavoriteBase.cs FindTagOrFreeInternal：
        // 已提交条目指向被日志截断回收的地址（address < min_valid_addr）时，
        // 单指令 CAS 置零原位清退为生槽，彻底阻断溢出桶伪分配；
        // ReadCache 条目地址含指示位，数值上不会落入截断区，显式排除（与 C# 数值比较天然等效）。
        if min_valid_addr > 0
          && entry.is_valid()
          && !entry.is_read_cache()
          && entry.address() < min_valid_addr
        {
          match item.compare_exchange(raw, 0, Ordering::AcqRel, Ordering::Acquire) {
            Ok(_) => {
              // CAS 置零成功：该槽位已成为空闲生槽
              first_free.get_or_insert((walker.curr, slot));
            }
            Err(actual_raw) => {
              if actual_raw == 0 {
                // 另一线程已抢先将其清退置零，当前槽位已成为空闲槽位
                first_free.get_or_insert((walker.curr, slot));
                continue;
              }
              let actual = HashBucketEntry::from_raw(actual_raw);
              if actual.matches_tag(tag)
                && (actual.is_read_cache() || actual.address() >= min_valid_addr)
              {
                // 另一线程已将该槽位并发覆写为目标 Tag 的最新有效条目，直接返回命中
                fence(Ordering::Acquire);
                return Ok(HashEntryInfo {
                  bucket: walker.curr,
                  slot,
                  raw: actual_raw,
                  tag,
                });
              }
              // 该槽已被其他有效条目占用且非目标 Tag，继续沿桶推进检查后续槽位
            }
          }
          continue;
        }

        if entry.matches_tag(tag) {
          // 命中屏障：调用方将按命中地址解引用记录内存（论证见函数头内存序注释）
          fence(Ordering::Acquire);
          return Ok(HashEntryInfo {
            bucket: walker.curr,
            slot,
            raw,
            tag,
          });
        }
      }

      match walker.advance(&self.overflow_pool) {
        ChainStep::Next => {}
        ChainStep::End => {
          // 链尾重读溢出指针为 0：排除「另一线程刚抢先挂载溢出桶」的竞争窗口
          if walker.curr.overflow_index() == 0 {
            if let Some((free_bucket, slot)) = first_free {
              return Ok(HashEntryInfo {
                bucket: free_bucket,
                slot,
                raw: 0,
                tag,
              });
            }

            // 整条链均无空槽位：分配新溢出桶并 CAS 挂载；
            // 并发败者归还冗余桶后沿赢家桶继续深入遍历
            let new_overflow_idx = self.overflow_pool.allocate()?;
            if walker.curr.set_overflow_index(new_overflow_idx) {
              // 挂载成功：新桶由本线程独占产出（全零），slot 0 即首个空槽
              // SAFETY: new_overflow_idx 由本线程 allocate() 刚产出，恒合法且对应 chunk 已初始化
              let new_bucket = unsafe { self.overflow_pool.get_unchecked(new_overflow_idx) };
              return Ok(HashEntryInfo {
                bucket: new_bucket,
                slot: 0,
                raw: 0,
                tag,
              });
            }
            self.overflow_pool.free(new_overflow_idx);
          }
          // 沿赢家桶深入遍历；End 分支理论不可达（上方已保证溢出指针非零），纯防御兜底
          match walker.advance(&self.overflow_pool) {
            ChainStep::Next => continue 'search,
            ChainStep::Cycle => return Err(Error::OverflowCycleDetected),
            ChainStep::End => return Err(Error::OverflowPoolExhausted),
          }
        }
        ChainStep::Cycle => return Err(Error::OverflowCycleDetected),
      }
    }
  }
  /// 原子 CAS 更新逻辑地址（RCU 路径，带链步数上限保护）
  ///
  /// 如果在索引中找到匹配的 `(tag, old_address)` 条目，则原子将其地址替换为 `new_address`。
  #[inline]
  pub fn update_address(&self, key: &[u8], old_address: u64, new_address: u64) -> bool {
    self.update_address_by_hash(Self::hash_key(key), old_address, new_address)
  }

  /// 基于哈希值原子 CAS 更新逻辑地址（严格对标 libs/storage/Tsavorite/cs/src/core/Index/Tsavorite/TsavoriteBase.cs:FindTag 定位 + HashEntryInfo.TryCAS）
  pub fn update_address_by_hash(&self, hash: u64, old_address: u64, new_address: u64) -> bool {
    if new_address == HashBucketEntry::INVALID_ADDRESS
      || new_address > HashBucketEntry::ADDRESS_MASK
      || old_address == HashBucketEntry::INVALID_ADDRESS
    {
      return false;
    }
    let Some(mut hei) = self.find_exact_entry_by_hash(hash, old_address) else {
      return false;
    };
    hei.try_cas(new_address)
  }

  /// 原子置零删除指定条目（带链遍历保护）
  #[inline]
  pub fn delete(&self, key: &[u8], address: u64) -> bool {
    self.delete_by_hash(Self::hash_key(key), address)
  }

  /// 基于哈希值原子置零删除指定条目（严格对标 libs/storage/Tsavorite/cs/src/core/Index/Tsavorite/TsavoriteBase.cs:FindTag 定位 + HashEntryInfo.TryElide 记录脱钩）
  pub fn delete_by_hash(&self, hash: u64, address: u64) -> bool {
    if address == HashBucketEntry::INVALID_ADDRESS {
      return false;
    }
    let Some(mut hei) = self.find_exact_entry_by_hash(hash, address) else {
      return false;
    };
    hei.try_elide()
  }

  /// 沿溢出链定位精确匹配 `(tag, address)` 的已提交条目（严格对标 libs/storage/Tsavorite/cs/src/core/Index/Tsavorite/TsavoriteBase.cs:FindTag + HashEntryInfo 装载）
  ///
  /// 复用 [`HashBucket::find_entry_by_address`] 单桶定位与步数上限链遍历，产出可定点
  /// [`HashEntryInfo::try_cas`] / [`HashEntryInfo::try_elide`] 的哈希槽位句柄。
  fn find_exact_entry_by_hash(&self, hash: u64, address: u64) -> Option<HashEntryInfo<'_>> {
    let tag = HashBucketEntry::tag_from_hash(hash);
    let mut walker = ChainWalker::new(self.get_bucket((hash as usize) & self.mask));
    loop {
      if let Some((slot, entry)) = walker.curr.find_entry_by_address(tag, address) {
        return Some(HashEntryInfo {
          bucket: walker.curr,
          slot,
          raw: entry.as_raw(),
          tag,
        });
      }
      match walker.advance(&self.overflow_pool) {
        ChainStep::Next => {}
        ChainStep::End | ChainStep::Cycle => return None,
      }
    }
  }

  /// 获取已分配的溢出桶总数
  #[inline]
  pub fn overflow_bucket_count(&self) -> u64 {
    self.overflow_pool.allocated_count()
  }

  /// 获取指定下标的主桶引用
  #[inline]
  pub fn bucket(&self, bucket_idx: usize) -> &HashBucket {
    self.get_bucket(bucket_idx)
  }

  /// 获取指定键对应的主桶引用
  #[inline]
  pub fn bucket_for_key(&self, key: &[u8]) -> &HashBucket {
    let hash = Self::hash_key(key);
    self.get_bucket((hash as usize) & self.mask)
  }

  /// 计算哈希值对应的主桶索引下标
  #[inline]
  pub fn bucket_index_for_hash(&self, hash: u64) -> usize {
    (hash as usize) & self.mask
  }

  /// 计算键对应的主桶索引下标
  #[inline]
  pub fn bucket_index_for_key(&self, key: &[u8]) -> usize {
    let hash = Self::hash_key(key);
    (hash as usize) & self.mask
  }

  /// 尝试对键对应的主桶获取共享锁（S-Latch）
  #[inline]
  pub fn try_lock_shared(&self, key: &[u8]) -> bool {
    self.bucket_for_key(key).try_lock_shared()
  }

  /// 释放键对应主桶的共享锁
  #[inline]
  pub fn unlock_shared(&self, key: &[u8]) {
    self.bucket_for_key(key).unlock_shared();
  }

  /// 尝试对键对应的主桶获取独占锁（X-Latch）
  #[inline]
  pub fn try_lock_exclusive(&self, key: &[u8]) -> bool {
    self.bucket_for_key(key).try_lock_exclusive()
  }

  /// 释放键对应主桶的独占锁
  #[inline]
  pub fn unlock_exclusive(&self, key: &[u8]) {
    self.bucket_for_key(key).unlock_exclusive();
  }

  /// 将键对应主桶的独占锁原子降级为共享锁
  #[inline]
  pub fn downgrade(&self, key: &[u8]) {
    self.bucket_for_key(key).downgrade_latch();
  }

  /// 判定键对应的主桶是否存在任意锁占用（对标 Garnet IsLocked）
  #[inline]
  pub fn is_locked(&self, key: &[u8]) -> bool {
    self.bucket_for_key(key).is_latched()
  }

  /// 获取键对应主桶的共享锁 RAII 守卫
  #[inline]
  pub fn lock_shared_guard(&self, key: &[u8]) -> Option<BucketSharedGuard<'_>> {
    self.bucket_for_key(key).lock_shared_guard()
  }

  /// 获取键对应主桶的独占锁 RAII 守卫
  #[inline]
  pub fn lock_exclusive_guard(&self, key: &[u8]) -> Option<BucketExclusiveGuard<'_>> {
    self.bucket_for_key(key).lock_exclusive_guard()
  }

  /// 哈希批量统一流水线驱动：预热窗口 + 滑动窗口预取逐项回调
  ///
  /// 12 项滑动预取窗口 1:1 对标 Garnet Tsavorite ContextReadWithPrefetch
  /// （PrefetchSize = 12）：先预热前 12 个主桶，随后每处理第 i 项前预取
  /// 第 i+12 项主桶，使预取延迟与遍历耗时重叠。
  fn batch_pipeline(&self, hashes: &[u64], mut query: impl FnMut(&Self, u64)) {
    if hashes.is_empty() {
      return;
    }
    for &hash in &hashes[..Self::PREFETCH_WINDOW.min(hashes.len())] {
      prefetch_read_l1(self.get_bucket((hash as usize) & self.mask));
    }
    for (i, &hash) in hashes.iter().enumerate() {
      if let Some(&next_hash) = hashes.get(i + Self::PREFETCH_WINDOW) {
        prefetch_read_l1(self.get_bucket((next_hash as usize) & self.mask));
      }
      query(self, hash);
    }
  }

  /// 基于预先计算好的哈希值进行流水线批量预取与候选地址查询
  pub fn lookup_candidates_batch_by_hash(
    &self,
    hashes: &[u64],
    results: &mut [CandidateAddresses],
  ) {
    let count = hashes.len().min(results.len());
    let mut idx = 0;
    self.batch_pipeline(&hashes[..count], |index, hash| {
      results[idx] = index.lookup_candidates_by_hash(hash);
      idx += 1;
    });
  }

  /// 基于预先计算好的哈希值进行流水线批量预取与快速单槽位探针查找
  pub fn find_tag_batch_by_hash(&self, hashes: &[u64], results: &mut [Option<u64>]) {
    let count = hashes.len().min(results.len());
    let mut idx = 0;
    self.batch_pipeline(&hashes[..count], |index, hash| {
      results[idx] = index.find_tag_by_hash(hash);
      idx += 1;
    });
  }

  /// 原地切片去重，单次单向遍历，将唯一元素排在前部并返回有效长度（零堆分配，稳定版标准 Rust）
  #[inline]
  fn in_place_dedup_by<T: Copy, F>(slice: &mut [T], mut same_bucket: F) -> usize
  where
    F: FnMut(&T, &T) -> bool,
  {
    if slice.len() <= 1 {
      return slice.len();
    }
    let mut write_idx = 1;
    for read_idx in 1..slice.len() {
      if !same_bucket(&slice[write_idx - 1], &slice[read_idx]) {
        if write_idx != read_idx {
          slice[write_idx] = slice[read_idx];
        }
        write_idx += 1;
      }
    }
    write_idx
  }

  /// 统一多键加锁驱动：桶寻址 -> 全序排序去重 -> 两阶段加锁
  ///
  /// 1. 桶下标恒由 `hash & mask` 截断产出（构造不变量 `mask == buckets.len() - 1`），
  ///    为 [`Self::acquire_unique_locked_entries`] 的 get_unchecked 提供安全前提；
  /// 2. 按桶下标升序排序形成全局加锁全序（杜绝死锁），同桶排他锁优先并去重
  ///    （读写混合时保留最高锁级；纯排他路径该 tie-break 为恒等，语义不变）；
  /// 3. 条目数 <= 16 走栈上内联零分配，超出走堆缓冲。
  fn acquire_bucket_locks<I>(&self, items: I) -> Result<MultiBucketGuard<'_>>
  where
    I: ExactSizeIterator<Item = (usize, bool)>,
  {
    let count = items.len();
    if count == 0 {
      return Ok(MultiBucketGuard::new(self));
    }

    let mut stack_entries = [(0usize, false); Self::INLINE_LOCK_ENTRIES];
    let mut heap_entries;
    let entries: &mut [(usize, bool)] = if count <= Self::INLINE_LOCK_ENTRIES {
      for (slot, e) in stack_entries[..count].iter_mut().zip(items) {
        *slot = e;
      }
      &mut stack_entries[..count]
    } else {
      heap_entries = items.collect::<Vec<_>>();
      &mut heap_entries
    };

    // 桶下标升序全序（防死锁）；同桶排他优先（true 排前），相邻去重保留首个
    // 即保留最高锁级（slice 无 dedup_by——该方法为 Vec 专属，栈/堆统一切片
    // 借用故自行实现）
    entries.sort_unstable_by(|a, b| a.0.cmp(&b.0).then_with(|| b.1.cmp(&a.1)));
    let deduped_len = Self::in_place_dedup_by(entries, |a, b| a.0 == b.0);

    self.acquire_unique_locked_entries(&entries[..deduped_len])
  }

  /// 基于两阶段锁（2PL）获取多个键的独占锁（严格对标 Garnet OverflowBucketLockTable）
  #[inline]
  pub fn acquire_keys_lock_exclusive(&self, keys: &[&[u8]]) -> Result<MultiBucketGuard<'_>> {
    self.acquire_bucket_locks(keys.iter().map(|k| (self.bucket_index_for_key(k), true)))
  }

  /// 获取多个哈希值对应的桶锁（支持读写混合锁，排他锁优先，对标 libs/server/Transaction/TxnKeyEntry.cs:LockAllKeys）
  #[inline]
  pub fn acquire_hash_locks(&self, items: &[(u64, bool)]) -> Result<MultiBucketGuard<'_>> {
    self.acquire_bucket_locks(
      items
        .iter()
        .map(|&(h, ex)| (self.bucket_index_for_hash(h), ex)),
    )
  }

  /// 统一核心加锁驱动引擎（零堆分配回滚、指数退避防活锁）
  ///
  /// 重试预算分三段：短自旋（32 次）→ yield 让核（[`Self::YIELD_RETRY_BUDGET`]）→
  /// 睡眠退避（[`Self::SLEEP_RETRY_BUDGET`]，100µs→1ms 封顶）后报 [`Error::LockTimeout`]
  fn acquire_unique_locked_entries(
    &self,
    unique_entries: &[(usize, bool)],
  ) -> Result<MultiBucketGuard<'_>> {
    let mut retry_count = 0usize;

    loop {
      let mut locked_count = 0usize;

      // 阶段一：顺次尝试加锁
      for &(b_idx, is_exclusive) in unique_entries {
        // SAFETY: 本函数私有，unique_entries 恒由 acquire_bucket_locks 产出，
        // b_idx 源自 bucket_index_for_key/hash（hash & mask 截断，恒小于
        // buckets.len()），无越界风险，免去检查开销
        let bucket = unsafe { self.buckets.get_unchecked(b_idx) };
        let ok = if is_exclusive {
          bucket.try_lock_exclusive()
        } else {
          bucket.try_lock_shared()
        };

        if ok {
          locked_count += 1;
        } else {
          break;
        }
      }

      // 阶段二：校验是否全量加锁成功
      if locked_count == unique_entries.len() {
        return Ok(MultiBucketGuard::from_slice(self, unique_entries));
      }

      // 阶段三：部分加锁失败，在栈上就地逆序回滚解锁（零堆分配开销！）
      for &(b_idx, is_exclusive) in unique_entries[..locked_count].iter().rev() {
        // SAFETY: 同阶段一，b_idx 恒为 hash & mask 截断后的合法桶下标
        let bucket = unsafe { self.buckets.get_unchecked(b_idx) };
        if is_exclusive {
          bucket.unlock_exclusive();
        } else {
          bucket.unlock_shared();
        }
      }

      retry_count += 1;
      if retry_count >= Self::YIELD_RETRY_BUDGET + Self::SLEEP_RETRY_BUDGET {
        return Err(Error::LockTimeout);
      }

      // 指数退避与自旋抖动（Jitter）：彻底消除对称竞争活锁
      if retry_count < Self::SPIN_RETRY_THRESHOLD {
        let spin_limit = (1usize << retry_count.min(Self::SPIN_LIMIT_MAX_EXP))
          | (retry_count & Self::SPIN_LIMIT_JITTER_MASK);
        for _ in 0..spin_limit {
          spin_loop();
        }
      } else if retry_count < Self::YIELD_RETRY_BUDGET {
        yield_now();
      } else {
        // 耐心等待阶段：临界区可合法跨越慢速 I/O await（如 ZADD 持锁写盘），持有者
        // 被 OS 冻结或 I/O 抖动时纯 yield 预算会瞬间烧穿并产生伪 LockTimeout（并发
        // 回归测试在全量套件高负载下曾偶发）。改为 100µs→1ms 封顶的指数睡眠退避，
        // 给出秒级等待窗口后再判超时
        let elapsed = retry_count - Self::YIELD_RETRY_BUDGET;
        let backoff_us = Self::SLEEP_BASE_MICROS
          .saturating_add(((elapsed >> 3) as u64).min(Self::SLEEP_MAX_ADDITIONAL_MICROS));
        sleep(Duration::from_micros(backoff_us));
      }
    }
  }
}

/// 批处理多哈希桶 RAII 锁守卫（严格对标 C# Garnet OverflowBucketLockTable / TransactionalContext）
///
/// 封装一组已成功获取自旋锁的哈希桶下标及锁类型。
/// 离开作用域时自动逆序全部解锁，保证异常安全与完全释放。
pub struct MultiBucketGuard<'a> {
  index: &'a HashIndex,
  stack: [(usize, bool); HashIndex::INLINE_LOCK_ENTRIES],
  len: usize,
  extra: Vec<(usize, bool)>,
}

impl<'a> MultiBucketGuard<'a> {
  /// 栈上内联锁条目容量
  pub const INLINE_CAPACITY: usize = HashIndex::INLINE_LOCK_ENTRIES;

  /// 创建新的空锁守卫
  #[inline]
  pub fn new(index: &'a HashIndex) -> Self {
    Self {
      index,
      stack: [(0, false); Self::INLINE_CAPACITY],
      len: 0,
      extra: Vec::new(),
    }
  }

  /// 从已成功锁定的切片高效批量构造锁守卫（小集合单次切片拷贝，零循环开销）
  #[inline]
  pub(crate) fn from_slice(index: &'a HashIndex, entries: &[(usize, bool)]) -> Self {
    let count = entries.len();
    if count <= Self::INLINE_CAPACITY {
      let mut stack = [(0, false); Self::INLINE_CAPACITY];
      stack[..count].copy_from_slice(entries);
      Self {
        index,
        stack,
        len: count,
        extra: Vec::new(),
      }
    } else {
      let mut stack = [(0, false); Self::INLINE_CAPACITY];
      stack.copy_from_slice(&entries[..Self::INLINE_CAPACITY]);
      Self {
        index,
        stack,
        len: Self::INLINE_CAPACITY,
        extra: entries[Self::INLINE_CAPACITY..].to_vec(),
      }
    }
  }

  /// 已加锁的桶数量
  #[inline]
  pub fn len(&self) -> usize {
    self.len + self.extra.len()
  }

  /// 是否为空
  #[inline]
  pub fn is_empty(&self) -> bool {
    self.len() == 0
  }

  /// 迭代遍历所有已加锁的条目
  pub fn iter(&self) -> impl DoubleEndedIterator<Item = &(usize, bool)> {
    self.stack[..self.len].iter().chain(self.extra.iter())
  }
}

impl Drop for MultiBucketGuard<'_> {
  fn drop(&mut self) {
    // 逆序解锁，满足两阶段锁（2PL）释放规范
    for &(bucket_idx, is_exclusive) in self.iter().rev() {
      // SAFETY: push 为 crate 私有，登记仅发生在 acquire_unique_locked_entries
      // 加锁成功路径，bucket_idx 恒由 bucket_index_for_key/hash 产出（hash & mask
      // 截断，恒小于 buckets.len()），无越界风险
      let bucket = unsafe { self.index.buckets.get_unchecked(bucket_idx) };
      if is_exclusive {
        bucket.unlock_exclusive();
      } else {
        bucket.unlock_shared();
      }
    }
  }
}

/// 预取目标指针到 CPU L1 数据缓存行（对标 Garnet Tsavorite Sse.Prefetch0）
#[inline(always)]
pub fn prefetch_read_l1<T>(p: *const T) {
  #[cfg(target_arch = "x86_64")]
  unsafe {
    _mm_prefetch(p.cast(), _MM_HINT_T0);
  }
  #[cfg(target_arch = "aarch64")]
  unsafe {
    asm!(
      "prfm pldl1keep, [{p}]",
      p = in(reg) p,
      options(nostack, readonly, preserves_flags)
    );
  }
  #[cfg(not(any(target_arch = "x86_64", target_arch = "aarch64")))]
  {
    let _ = p;
  }
}
