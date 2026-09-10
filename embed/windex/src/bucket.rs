use std::{
  fmt,
  hint::spin_loop,
  mem::forget,
  result::Result,
  sync::atomic::{AtomicU64, Ordering, fence},
  thread::yield_now,
};

use crate::entry::HashBucketEntry;

/// 每个哈希桶严格占满一个 64 字节 CPU 缓存行（Cacheline 对齐）
///
/// 包含 8 个 AtomicU64 槽位：
/// - 槽位 0..7：存放数据项条目（HashBucketEntry）
/// - 槽位 7（OVERFLOW_INDEX）：
///   - 低 48 位：溢出桶索引（0 表示无溢出桶，1-based 索引）
///   - 次高 15 位：共享锁读者计数器（Shared Latch，最大 32767 并发读者）
///   - 最高 1 位：独占写者锁标记（Exclusive Latch）
#[repr(C, align(64))]
pub struct HashBucket {
  pub entries: [AtomicU64; 8],
}

/// 用于存放真实数据条目的槽位数量（槽位 0..7 共 7 个数据槽位）
pub const DATA_ENTRIES: usize = 7;
/// 溢出桶指针与自旋锁所在槽位索引（最后一个槽位，紧随数据槽位之后）
pub const OVERFLOW_INDEX: usize = DATA_ENTRIES;
/// 每个哈希桶中的条目总数（7 个数据槽位 + 1 个溢出指针槽位）
pub const ENTRIES_PER_BUCKET: usize = DATA_ENTRIES + 1;

impl HashBucket {
  /// 数据槽位数量关联常量（再导出自由常量，下游 wcpr 等包以此命名空间引用）
  pub const DATA_ENTRIES: usize = DATA_ENTRIES;
  /// 溢出槽位索引关联常量（再导出自由常量，下游 wcpr 等包以此命名空间引用）
  pub const OVERFLOW_INDEX: usize = OVERFLOW_INDEX;
  /// 自旋锁获取的最大自旋次数（C# Constants.kMaxLockSpins = 10；放大至 128 以
  /// 配合先 spin_loop 后 yield 的两级退避，降低高争用下的误失败率）
  pub const MAX_LOCK_SPINS: usize = 128;
  /// 独占锁等待活跃读者完全退出的最大自旋次数
  pub const MAX_READER_DRAIN_SPINS: usize = 1024;
  /// 自旋让步阈值（超过后让出 CPU 时间片）
  pub const SPIN_THRESHOLD: usize = 32;
  /// 排空重试让步阈值
  pub const DRAIN_RETRIES_THRESHOLD: usize = 16;

  /// 共享锁占用的比特位数（15 位）
  pub const SHARED_LATCH_BITS: u32 = 15;
  /// 共享锁在 u64 中的起始偏移（第 48 位）
  pub const SHARED_LATCH_SHIFT: u32 = HashBucketEntry::ADDRESS_BITS;
  /// 共享锁掩码（0x7FFF_0000_0000_0000）
  pub const SHARED_LATCH_MASK: u64 =
    ((1u64 << Self::SHARED_LATCH_BITS) - 1) << Self::SHARED_LATCH_SHIFT;
  /// 共享锁每次递增的步长（1 << 48）
  pub const SHARED_LATCH_INC: u64 = 1u64 << Self::SHARED_LATCH_SHIFT;

  /// 独占写锁偏移量（第 63 位）
  pub const EXCLUSIVE_LATCH_SHIFT: u32 = 63;
  /// 独占写锁掩码（0x8000_0000_0000_0000）
  pub const EXCLUSIVE_LATCH_MASK: u64 = 1u64 << Self::EXCLUSIVE_LATCH_SHIFT;

  /// 复合锁状态掩码（涵盖共享读者计数与独占写标记）
  pub const LATCH_MASK: u64 = Self::SHARED_LATCH_MASK | Self::EXCLUSIVE_LATCH_MASK;

  /// 构造一个全空的 64 字节对齐哈希桶
  pub const fn new() -> Self {
    Self {
      entries: [const { AtomicU64::new(0) }; ENTRIES_PER_BUCKET],
    }
  }

  /// 尝试获取共享锁（Shared Latch）
  pub fn try_lock_shared(&self) -> bool {
    for i in 0..Self::MAX_LOCK_SPINS {
      let curr = self.entries[OVERFLOW_INDEX].load(Ordering::Acquire);
      if (curr & Self::EXCLUSIVE_LATCH_MASK) == 0
        && (curr & Self::SHARED_LATCH_MASK) != Self::SHARED_LATCH_MASK
      {
        let new_val = curr + Self::SHARED_LATCH_INC;
        if self.entries[OVERFLOW_INDEX]
          .compare_exchange_weak(curr, new_val, Ordering::AcqRel, Ordering::Acquire)
          .is_ok()
        {
          return true;
        }
      }
      if i < Self::SPIN_THRESHOLD {
        spin_loop();
      } else {
        yield_now();
      }
    }
    false
  }

  /// 释放共享锁
  pub fn unlock_shared(&self) {
    let prev = self.entries[OVERFLOW_INDEX].fetch_sub(Self::SHARED_LATCH_INC, Ordering::Release);
    debug_assert!(
      (prev & Self::SHARED_LATCH_MASK) != 0,
      "试图释放未持有的共享锁"
    );
    debug_assert!(
      (prev & Self::LATCH_MASK) != Self::EXCLUSIVE_LATCH_MASK,
      "试图对仅持独占锁的桶释放共享锁"
    );
  }

  /// 尝试获取独占锁（Exclusive Latch）
  pub fn try_lock_exclusive(&self) -> bool {
    let mut acquired_bit = false;
    for i in 0..Self::MAX_LOCK_SPINS {
      let curr = self.entries[OVERFLOW_INDEX].load(Ordering::Acquire);
      if (curr & Self::EXCLUSIVE_LATCH_MASK) == 0 {
        let new_val = curr | Self::EXCLUSIVE_LATCH_MASK;
        if self.entries[OVERFLOW_INDEX]
          .compare_exchange_weak(curr, new_val, Ordering::AcqRel, Ordering::Acquire)
          .is_ok()
        {
          acquired_bit = true;
          break;
        }
      }
      if i < Self::SPIN_THRESHOLD {
        spin_loop();
      } else {
        yield_now();
      }
    }

    if !acquired_bit {
      return false;
    }

    // 等待活跃读者完全排空
    for i in 0..Self::MAX_READER_DRAIN_SPINS {
      let curr = self.entries[OVERFLOW_INDEX].load(Ordering::Acquire);
      if (curr & Self::SHARED_LATCH_MASK) == 0 {
        return true;
      }
      if i < Self::SPIN_THRESHOLD {
        spin_loop();
      } else {
        yield_now();
      }
    }

    // 排空超时，回退独占标记
    self.entries[OVERFLOW_INDEX].fetch_and(!Self::EXCLUSIVE_LATCH_MASK, Ordering::Release);
    false
  }

  /// 尝试将当前持有的共享锁（S-Latch）原子升级为独占锁（X-Latch）
  ///
  /// 对照 C# Tsavorite `HashBucket.TryPromoteLatch` 实现：
  /// 1. 原子将独占标记位置 1 并扣减自身持有的一个共享读者计数
  /// 2. 自旋等待其余活跃读者排空（最多 MAX_READER_DRAIN_SPINS 次）
  /// 3. 若排空超时，原子回退独占标记并补回共享读者计数，返回 false
  pub fn try_promote_latch(&self) -> bool {
    let mut acquired_bit = false;
    for i in 0..Self::MAX_LOCK_SPINS {
      let curr = self.entries[OVERFLOW_INDEX].load(Ordering::Acquire);
      if (curr & Self::SHARED_LATCH_MASK) == 0 {
        return false;
      }
      if (curr & Self::EXCLUSIVE_LATCH_MASK) == 0 {
        let new_val = (curr | Self::EXCLUSIVE_LATCH_MASK) - Self::SHARED_LATCH_INC;
        if self.entries[OVERFLOW_INDEX]
          .compare_exchange_weak(curr, new_val, Ordering::AcqRel, Ordering::Acquire)
          .is_ok()
        {
          acquired_bit = true;
          break;
        }
      }
      if i < Self::SPIN_THRESHOLD {
        spin_loop();
      } else {
        yield_now();
      }
    }

    if !acquired_bit {
      return false;
    }

    // 等待其余活跃读者排空
    for i in 0..Self::MAX_READER_DRAIN_SPINS {
      let curr = self.entries[OVERFLOW_INDEX].load(Ordering::Acquire);
      if (curr & Self::SHARED_LATCH_MASK) == 0 {
        return true;
      }
      if i < Self::SPIN_THRESHOLD {
        spin_loop();
      } else {
        yield_now();
      }
    }

    // 排空超时，回退：清除独占标记位，并补回共享读者计数
    let mut curr = self.entries[OVERFLOW_INDEX].load(Ordering::Acquire);
    let mut retries = 0usize;
    loop {
      let new_val = (curr & !Self::EXCLUSIVE_LATCH_MASK) + Self::SHARED_LATCH_INC;
      match self.entries[OVERFLOW_INDEX].compare_exchange_weak(
        curr,
        new_val,
        Ordering::AcqRel,
        Ordering::Acquire,
      ) {
        Ok(_) => break,
        Err(actual) => {
          curr = actual;
          retries += 1;
          if retries < Self::DRAIN_RETRIES_THRESHOLD {
            spin_loop();
          } else {
            yield_now();
          }
        }
      }
    }
    false
  }

  /// 将独占锁（X-Latch）原子降级为共享锁（S-Latch）
  ///
  /// 原子清除独占标记并增加一个共享读者计数，保证没有任何并发写者能在这期间插入
  pub fn downgrade_latch(&self) {
    let mut curr = self.entries[OVERFLOW_INDEX].load(Ordering::Acquire);
    loop {
      debug_assert!(
        (curr & Self::EXCLUSIVE_LATCH_MASK) != 0,
        "尝试降级未持有独占锁的桶"
      );
      let new_val = (curr & !Self::EXCLUSIVE_LATCH_MASK) + Self::SHARED_LATCH_INC;
      match self.entries[OVERFLOW_INDEX].compare_exchange_weak(
        curr,
        new_val,
        Ordering::AcqRel,
        Ordering::Acquire,
      ) {
        Ok(_) => break,
        Err(actual) => curr = actual,
      }
      spin_loop();
    }
  }

  /// 释放独占锁
  pub fn unlock_exclusive(&self) {
    let prev =
      self.entries[OVERFLOW_INDEX].fetch_and(!Self::EXCLUSIVE_LATCH_MASK, Ordering::Release);
    debug_assert!(
      (prev & Self::EXCLUSIVE_LATCH_MASK) != 0,
      "试图释放未持有的独占锁"
    );
  }

  /// 判定当前是否处于独占加锁状态
  #[inline]
  pub fn is_latched_exclusive(&self) -> bool {
    (self.entries[OVERFLOW_INDEX].load(Ordering::Acquire) & Self::EXCLUSIVE_LATCH_MASK) != 0
  }

  /// 判定当前是否处于共享加锁状态
  #[inline]
  pub fn is_latched_shared(&self) -> bool {
    (self.entries[OVERFLOW_INDEX].load(Ordering::Acquire) & Self::SHARED_LATCH_MASK) != 0
  }

  /// 获取当前并发共享读者数量
  #[inline]
  pub fn num_latched_shared(&self) -> u16 {
    ((self.entries[OVERFLOW_INDEX].load(Ordering::Acquire) & Self::SHARED_LATCH_MASK)
      >> Self::SHARED_LATCH_SHIFT) as u16
  }

  /// 判定当前是否存在任意类型的锁占用
  #[inline]
  pub fn is_latched(&self) -> bool {
    (self.entries[OVERFLOW_INDEX].load(Ordering::Acquire) & Self::LATCH_MASK) != 0
  }

  /// 读取溢出桶索引（0 表示当前桶链在此终止）
  #[inline]
  pub fn overflow_index(&self) -> u64 {
    self.entries[OVERFLOW_INDEX].load(Ordering::Acquire) & HashBucketEntry::ADDRESS_MASK
  }

  /// 原子 CAS 安装新的溢出桶索引（保留原有的锁位状态不变）
  ///
  /// 返回 `false` 表示已有溢出桶（`overflow_idx == 0` 视为非法输入，同样拒绝）。
  pub fn set_overflow_index(&self, overflow_idx: u64) -> bool {
    if overflow_idx == 0 {
      return false;
    }
    let target_addr = overflow_idx & HashBucketEntry::ADDRESS_MASK;
    let mut curr = self.entries[OVERFLOW_INDEX].load(Ordering::Acquire);
    loop {
      if (curr & HashBucketEntry::ADDRESS_MASK) != 0 {
        return false;
      }
      let new_val = curr | target_addr;
      match self.entries[OVERFLOW_INDEX].compare_exchange_weak(
        curr,
        new_val,
        Ordering::AcqRel,
        Ordering::Acquire,
      ) {
        Ok(_) => return true,
        Err(actual) => {
          curr = actual;
          spin_loop();
        }
      }
    }
  }

  /// 查找当前桶内第一个匹配指定 Tag 的有效地址（严格对标 TsavoriteBase FindTag 首项快速探针）
  ///
  /// 内存序论证（本 crate 桶扫描通用基线）：扫描阶段仅做 tag/address 过滤，
  /// u64 对齐原子加载无撕裂，Relaxed 足矣；真正需要 happens-before 的是
  /// 「依据命中地址解引用记录内存」的时刻——发布方先写记录数据、再以
  /// CAS(AcqRel) 发布槽位条目，读者以 Relaxed 读到该条目后、解引用记录前
  /// 执行 fence(Acquire)，按内存模型「Release 写 X → Relaxed 读 X 读到 →
  /// sequenced-before Acquire fence」即与发布方建立 release/acquire 同步，
  /// 等价于原先逐槽 Acquire 加载，而屏障开销从每桶 7 次降为命中时 1 次。
  #[inline]
  pub fn find_tag_address(&self, tag: u16) -> Option<u64> {
    let expected_hi = (tag as u64) & HashBucketEntry::TAG_MASK;
    for item in &self.entries[..DATA_ENTRIES] {
      let raw = item.load(Ordering::Relaxed);
      if (raw >> HashBucketEntry::TAG_SHIFT) == expected_hi {
        let addr = raw & HashBucketEntry::ADDRESS_MASK;
        if addr != 0 {
          // 命中屏障：保证调用方对命中地址记录数据的读取可见发布方全部前置写
          fence(Ordering::Acquire);
          return Some(addr);
        }
      }
    }
    None
  }

  /// 在当前桶的数据槽位中查找匹配指定 Tag 和逻辑地址的有效条目
  ///
  /// 内存序论证参见 [`HashBucket::find_tag_address`]：Relaxed 扫描 + 命中后 fence(Acquire)
  #[inline]
  pub fn find_entry_by_address(&self, tag: u16, address: u64) -> Option<(usize, HashBucketEntry)> {
    let target_raw = HashBucketEntry::new(address, tag, false).as_raw();
    for (slot, item) in self.entries[..DATA_ENTRIES].iter().enumerate() {
      let raw = item.load(Ordering::Relaxed);
      if raw == target_raw {
        fence(Ordering::Acquire);
        return Some((slot, HashBucketEntry::from_raw(raw)));
      }
    }
    None
  }

  /// 查找当前桶内的第一个空槽位
  ///
  /// 仅定位空槽供后续 CAS 插入（CAS 自带 AcqRel 发布屏障），无需 Acquire
  #[inline]
  pub fn find_empty_slot(&self) -> Option<usize> {
    for (slot, item) in self.entries[..DATA_ENTRIES].iter().enumerate() {
      if item.load(Ordering::Relaxed) == 0 {
        return Some(slot);
      }
    }
    None
  }

  /// 尝试向指定空槽位原子 CAS 插入完整有效条目（0 -> 条目，AcqRel 自带发布屏障）
  ///
  /// 内存序论证（本 crate 桶扫描通用基线，参见 [`Self::find_tag_address`]）：
  /// 发布方以 CAS(AcqRel) 发布条目，读者以 Relaxed 扫描命中后 fence(Acquire)
  /// 建立与条目指向记录数据的 release/acquire 同步，此处无需额外屏障。
  ///
  /// 返回 `false` 表示空槽位已被并发竞争者抢占，调用方应重新定位空槽。
  /// 对照 C# `HashBucketEntry.Set`：tentative 恒为 false——本 crate 无锁插入协议
  /// 把最终值的原子发布收敛为单次 CAS（见 `HashIndex::insert_by_hash` 的 C# 两阶段
  /// 协议对照注释），读者经 `matches_tag` 只会看到空槽或完整条目，无半成品窗口。
  #[inline]
  pub fn try_insert(&self, slot: usize, tag: u16, address: u64) -> bool {
    if slot >= DATA_ENTRIES {
      return false;
    }
    let entry = HashBucketEntry::new(address, tag, false);
    self.entries[slot]
      .compare_exchange(0, entry.as_raw(), Ordering::AcqRel, Ordering::Acquire)
      .is_ok()
  }

  /// 获取共享锁 RAII 守卫
  pub fn lock_shared_guard(&self) -> Option<BucketSharedGuard<'_>> {
    BucketSharedGuard::new(self)
  }

  /// 获取独占锁 RAII 守卫
  pub fn lock_exclusive_guard(&self) -> Option<BucketExclusiveGuard<'_>> {
    BucketExclusiveGuard::new(self)
  }
}

impl Default for HashBucket {
  fn default() -> Self {
    Self::new()
  }
}

/// 桶共享锁 RAII 守卫
pub struct BucketSharedGuard<'a> {
  bucket: &'a HashBucket,
}

impl<'a> BucketSharedGuard<'a> {
  /// 尝试获取共享锁守卫
  pub fn new(bucket: &'a HashBucket) -> Option<Self> {
    if bucket.try_lock_shared() {
      Some(Self { bucket })
    } else {
      None
    }
  }

  /// 尝试将共享锁升级为独占锁守卫，失败时保留原共享锁守卫
  pub fn try_promote(self) -> Result<BucketExclusiveGuard<'a>, Self> {
    if self.bucket.try_promote_latch() {
      let bucket = self.bucket;
      forget(self);
      Ok(BucketExclusiveGuard { bucket })
    } else {
      Err(self)
    }
  }
}

impl Drop for BucketSharedGuard<'_> {
  fn drop(&mut self) {
    self.bucket.unlock_shared();
  }
}

/// 桶独占锁 RAII 守卫
pub struct BucketExclusiveGuard<'a> {
  bucket: &'a HashBucket,
}

impl<'a> BucketExclusiveGuard<'a> {
  /// 尝试获取独占锁守卫
  pub fn new(bucket: &'a HashBucket) -> Option<Self> {
    if bucket.try_lock_exclusive() {
      Some(Self { bucket })
    } else {
      None
    }
  }

  /// 原子将独占锁降级为共享锁守卫
  pub fn downgrade(self) -> BucketSharedGuard<'a> {
    self.bucket.downgrade_latch();
    let bucket = self.bucket;
    forget(self);
    BucketSharedGuard { bucket }
  }
}

impl Drop for BucketExclusiveGuard<'_> {
  fn drop(&mut self) {
    self.bucket.unlock_exclusive();
  }
}

impl fmt::Debug for HashBucket {
  fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    f.debug_struct("HashBucket")
      .field("exclusive", &self.is_latched_exclusive())
      .field("shared_readers", &self.num_latched_shared())
      .field("overflow_index", &self.overflow_index())
      .finish()
  }
}

impl fmt::Debug for BucketSharedGuard<'_> {
  fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    f.debug_struct("BucketSharedGuard")
      .field("readers", &self.bucket.num_latched_shared())
      .finish()
  }
}

impl fmt::Debug for BucketExclusiveGuard<'_> {
  fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    f.debug_struct("BucketExclusiveGuard")
      .field("exclusive", &self.bucket.is_latched_exclusive())
      .finish()
  }
}
