use std::{
  fmt,
  hint::spin_loop,
  sync::atomic::{AtomicU64, Ordering},
};

use wbase::backoff::Backoff;

use crate::{entry::HashBucketEntry, table::HashIndex};

/// 每个哈希桶严格占满一个 64 字节 CPU 缓存行（Cacheline 对齐）
///
/// 包含 8 个 AtomicU64 槽位：
/// - 槽位 0..7：存放数据项条目（HashBucketEntry）
/// - 槽位 7（OVERFLOW_INDEX）：
///   - 低 48 位：溢出桶索引（0 表示无溢出桶，1-based 索引）
///   - 次高 13 位：共享锁读者计数器（Shared Latch，最大 8191 并发读者）
///   - 第 61 位：等闩让渡登记位（Handoff，等闩预算环登记在册，新独占取闩者见位让位）
///   - 第 62 位：放闩通知位（Release Notify，独占放闩侧置位、等闩者消费）
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

/// 自旋等待原语（对标 C# HashBucket.cs 各取闩循环自旋间的 `Thread.Yield`）
///
/// 位转译口径：C# `kMaxLockSpins` 名为自旋、实以 `Thread.Yield` 充当自旋间让渡
///（.NET 的 Yield 同核无就绪者时近乎空操作）；rust 侧 `sched_yield` 每次 miss
/// 都是一次真实上下文切换，套件级 CPU 超订阅下（nextest 多测试并行）8 竞争者
/// 互相踩踏成活锁风暴——8 线程压力测试 16s 热点全部落在 `swtch_pri` 陷阱。
/// 故 miss 位改真自旋提示 [`spin_loop`]：界限（10/100）与回退语义逐位保留，
/// 单次取闩全程纯内存、无系统调用、无睡眠（compio 反应器线程安全），自旋总量
/// 至多 110 拍 ≈ 微秒级。
#[inline(always)]
fn latch_spin() {
  spin_loop();
}

impl HashBucket {
  /// 数据槽位数量关联常量（再导出自由常量，下游 wcpr 等包以此命名空间引用）
  pub const DATA_ENTRIES: usize = DATA_ENTRIES;
  /// 溢出槽位索引关联常量（再导出自由常量，下游 wcpr 等包以此命名空间引用）
  pub const OVERFLOW_INDEX: usize = OVERFLOW_INDEX;
  /// 自旋锁获取的最大自旋次数（对标 C# Constants.kMaxLockSpins = 10；仅桶闩自旋内核内部消费，不导出）
  pub(crate) const MAX_LOCK_SPINS: usize = 10;
  /// 独占锁等待活跃读者完全退出的最大自旋次数（对标 C# Constants.kMaxReaderLockDrainSpins = kMaxLockSpins * 10 = 100）
  const MAX_READER_DRAIN_SPINS: usize = 100;

  /// 共享锁占用的比特位数（13 位，第 61 位划归等闩让渡登记位、第 62 位为放闩通知位）
  const SHARED_LATCH_BITS: u32 = 13;
  /// 共享锁在 u64 中的起始偏移（第 48 位）
  const SHARED_LATCH_SHIFT: u32 = HashBucketEntry::ADDRESS_BITS;
  /// 共享锁掩码（0x1FFF_0000_0000_0000）
  const SHARED_LATCH_MASK: u64 =
    ((1u64 << Self::SHARED_LATCH_BITS) - 1) << Self::SHARED_LATCH_SHIFT;
  /// 共享锁每次递增的步长（1 << 48）
  pub const SHARED_LATCH_INC: u64 = 1u64 << Self::SHARED_LATCH_SHIFT;

  /// 等闩让渡登记位偏移（第 61 位）
  const HANDOFF_SHIFT: u32 = Self::SHARED_LATCH_SHIFT + Self::SHARED_LATCH_BITS;
  /// 等闩让渡登记位掩码（0x2000_0000_0000_0000）
  const HANDOFF_MASK: u64 = 1u64 << Self::HANDOFF_SHIFT;

  /// 放闩通知位偏移（第 62 位，紧邻让渡登记位）
  const RELEASE_NOTIFY_SHIFT: u32 = Self::HANDOFF_SHIFT + 1;
  /// 放闩通知位掩码（0x4000_0000_0000_0000）
  const RELEASE_NOTIFY_MASK: u64 = 1u64 << Self::RELEASE_NOTIFY_SHIFT;

  /// 独占写锁偏移量（第 63 位）
  const EXCLUSIVE_LATCH_SHIFT: u32 = 63;
  /// 独占写锁掩码（0x8000_0000_0000_0000）
  pub const EXCLUSIVE_LATCH_MASK: u64 = 1u64 << Self::EXCLUSIVE_LATCH_SHIFT;

  /// 复合锁状态掩码（涵盖共享读者计数与独占写标记）
  const LATCH_MASK: u64 = Self::SHARED_LATCH_MASK | Self::EXCLUSIVE_LATCH_MASK;

  /// 构造一个全空的 64 字节对齐哈希桶
  pub const fn new() -> Self {
    Self {
      entries: [const { AtomicU64::new(0) }; ENTRIES_PER_BUCKET],
    }
  }

  /// 数据槽位统一扫描迭代器：产出 `(槽位下标, 槽位原子引用)`，覆盖前 `DATA_ENTRIES`
  /// 个数据槽（末槽为溢出指针/锁位，不入扫描面，对标 C# `h < EntriesPerBucket` 常量循环）。
  ///
  /// 全仓 7 槽位「查找建槽 / 候选收集 / 免查重插入 / 分裂迁移」四处扫描一律经此单点收敛，
  /// 取代此前「手写 7 连展开 / `for slot in 0..DATA_ENTRIES` 下标循环 / `.iter().take()`」
  /// 并存的三种写法。对定长 `[AtomicU64; 8]` 前缀切片、常量边界 `DATA_ENTRIES` 的迭代，
  /// LLVM 必然完全展开并消除越界检查，与手写展开逐指令等价（无性能回退）；桶内极速探针
  /// `find_tag_address` / `find_entry_by_address` / `find_empty_slot` 依其「刻意展开消除
  /// 循环边界检查与分支预测失效」注释保留原形态，不纳入本迭代器。
  #[inline(always)]
  pub(crate) fn data_slots(&self) -> impl Iterator<Item = (usize, &AtomicU64)> {
    self.entries.iter().enumerate().take(DATA_ENTRIES)
  }

  /// 尝试获取共享锁（Shared Latch）
  ///
  /// 在 garnet 中的相对路径:libs/storage/Tsavorite/cs/src/core/Index/Tsavorite/HashBucket.cs:TryAcquireSharedLatch
  /// 在 garnet 中的相对路径:libs/storage/Tsavorite/cs/src/core/Index/Tsavorite/Implementation/Locking/OverflowBucketLockTable.cs:TryLockShared
  pub fn try_lock_shared(&self) -> bool {
    for _ in 0..Self::MAX_LOCK_SPINS {
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
      latch_spin();
    }
    false
  }

  /// 释放共享锁
  ///
  /// 在 garnet 中的相对路径:libs/storage/Tsavorite/cs/src/core/Index/Tsavorite/HashBucket.cs:ReleaseSharedLatch
  /// 在 garnet 中的相对路径:libs/storage/Tsavorite/cs/src/core/Index/Tsavorite/Implementation/Locking/OverflowBucketLockTable.cs:UnlockShared
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

  /// 尝试获取独占锁（Exclusive Latch）——公平门控臂
  ///
  /// 在 garnet 中的相对路径:libs/storage/Tsavorite/cs/src/core/Index/Tsavorite/HashBucket.cs:TryAcquireExclusiveLatch
  ///（含读者排空与超时 CAS 回退两段，语义逐段对齐）
  ///
  /// 与 [`Self::try_lock_exclusive_now`] 的唯一差在前置公平门：桶上有等闩者在册
  /// （[`Self::handoff_pending`]）即**不进入自旋**、单次判定直接回 `false`——对位
  /// C# LockTable pending 队列的 FIFO 让闩序（新取闩者排在已登记等闩者之后）。
  /// 登记面唯一装配点在 wkv 等闩预算环（`RMW_LATCH_YIELD_BUDGET` 环，其自身重试
  /// 走绕开门控的 [`Self::try_lock_exclusive_now`]，无「自见自让」面）；门控为
  /// 咨询性排位信号，互斥安全性始终由独占位 CAS 本体承接，位滞留至多退化为
  /// 现行竞速形态、不破坏正确性。
  #[inline]
  pub fn try_lock_exclusive(&self) -> bool {
    if self.handoff_pending() {
      return false;
    }
    self.try_lock_exclusive_now()
  }

  /// 独占取闩成功后的收尾：自旋排空活跃读者；超时则清独占标记并回 `false`。
  ///
  /// 对标 C# HashBucket.cs:100-114 读者排空与超时 CAS 回退两段。
  #[inline]
  fn drain_or_rollback(&self) -> bool {
    // 等待活跃读者完全排空（对标 C# HashBucket.cs:100-105）
    for _ in 0..Self::MAX_READER_DRAIN_SPINS {
      let curr = self.entries[OVERFLOW_INDEX].load(Ordering::Acquire);
      if (curr & Self::SHARED_LATCH_MASK) == 0 {
        return true;
      }
      latch_spin();
    }

    // 排空超时，回退独占标记（对标 C# HashBucket.cs:108-114）
    loop {
      let curr = self.entries[OVERFLOW_INDEX].load(Ordering::Acquire);
      let new_val = curr & !Self::EXCLUSIVE_LATCH_MASK;
      if self.entries[OVERFLOW_INDEX]
        .compare_exchange_weak(curr, new_val, Ordering::AcqRel, Ordering::Acquire)
        .is_ok()
      {
        break;
      }
      latch_spin();
    }
    false
  }

  /// 尝试获取独占锁——绕开公平门控的裸取闩臂（等闩者专用）
  ///
  /// 已登记等闩者（持 [`Self::set_handoff`] 凭据）的预算环重试臂经本入口取闩：
  /// 在册者自身不受自家让渡位阻挡（方向 A 的「登记位自礼让自拖」失败形的修正点），
  /// 其余取闩者一律走 [`Self::try_lock_exclusive`] 让位。取闩体（含读者排空与
  /// 超时回退）与门控臂同一实现。
  pub fn try_lock_exclusive_now(&self) -> bool {
    let mut acquired_bit = false;
    for _ in 0..Self::MAX_LOCK_SPINS {
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
      latch_spin();
    }

    if !acquired_bit {
      return false;
    }

    // 公共收尾：排空活跃读者，超时则清独占标记回退
    self.drain_or_rollback()
  }

  /// 释放独占锁
  ///
  /// 在 garnet 中的相对路径:libs/storage/Tsavorite/cs/src/core/Index/Tsavorite/HashBucket.cs:ReleaseExclusiveLatch
  /// 在 garnet 中的相对路径:libs/storage/Tsavorite/cs/src/core/Index/Tsavorite/Implementation/Locking/OverflowBucketLockTable.cs:UnlockExclusive
  /// 放闩侧单点置放闩通知位（garnet 无此位——C# 等闩者由 LockTable pending 唤醒承接，
  /// rust 侧等价物即本位 + [`HashBucket::consume_release_notify`] 消费，经本函数
  /// 一处放闩覆盖全部独占闩消费面：rmw 窗口两臂、wtxn 事务键锁、TTL 键闩同桶同放闩点）
  pub fn unlock_exclusive(&self) {
    let prev =
      self.entries[OVERFLOW_INDEX].fetch_and(!Self::EXCLUSIVE_LATCH_MASK, Ordering::Release);
    debug_assert!(
      (prev & Self::EXCLUSIVE_LATCH_MASK) != 0,
      "试图释放未持有的独占锁"
    );
    self.entries[OVERFLOW_INDEX].fetch_or(Self::RELEASE_NOTIFY_MASK, Ordering::Release);
  }

  /// 消费放闩通知位：见位即原子清位并返回 `true`，未见返回 `false`
  ///
  /// 契约（等闩者预算环的确定性重试判据）：[`Self::unlock_exclusive`] 每次放闩
  /// 必置位一次；等闩者得 `true` 后必须**不让核立即重试取闩**——重试相位由此锚定
  /// 放闩时刻，取代盲采样：持闩者高频重入令闩空闲窗仅纳秒级占比时（同键 collect
  /// 循环 vs 点查写），让核盲采的成败取决于采样相位，即写者概率性饿死的根因。
  /// 位为单粒度信号非等待队列：多等闩者竞争消费恰一者得 `true`，未得者照旧让核，
  /// 公平性由调用侧 fail-closed 预算环兜底；无人等闩时位残留至下次消费，无害。
  /// 位与共享计数、独占标记同字并存：共享计数满员由 [`Self::try_lock_shared`]
  /// 先行判守拒绝，增减永不进位吞位。
  pub fn consume_release_notify(&self) -> bool {
    let mut curr = self.entries[OVERFLOW_INDEX].load(Ordering::Acquire);
    if curr & Self::RELEASE_NOTIFY_MASK == 0 {
      return false;
    }
    loop {
      match self.entries[OVERFLOW_INDEX].compare_exchange_weak(
        curr,
        curr & !Self::RELEASE_NOTIFY_MASK,
        Ordering::AcqRel,
        Ordering::Acquire,
      ) {
        Ok(_) => return true,
        Err(actual) => {
          if actual & Self::RELEASE_NOTIFY_MASK == 0 {
            return false;
          }
          curr = actual;
        }
      }
    }
  }

  /// 登记等闩让渡位（入册）：登记后的等闩者在独占闩队列中持位
  ///
  /// 契约：与 [`Self::clear_handoff`] 严格配对（RAII 承载，任何退出路径必清），
  /// 位存续期内 [`Self::try_lock_exclusive`]（门控臂）见位即让位——即「重取闩排在
  /// 已登记等闩者之后」的序凭据；登记者自身重试走 [`Self::try_lock_exclusive_now`]
  /// 绕开门控，杜绝自见自让。登记为单粒度咨询位非等待队列：多位并存时共位者
  /// 退化为同闩竞速（现行形态），滞留未清由配对纪律收口（超时/成功/panic 皆经
  /// Drop 清位），绝不跨请求存活。与共享计数、通知位、独占标记同字并存：
  /// 计数满员由 [`Self::try_lock_shared`] 判守拒绝，进位永不吞高位。
  pub fn set_handoff(&self) {
    self.entries[OVERFLOW_INDEX].fetch_or(Self::HANDOFF_MASK, Ordering::Release);
  }

  /// 撤销等闩让渡登记（出册）：等闩者成功持闩或预算耗尽上抛时由守卫 Drop 调用
  pub fn clear_handoff(&self) {
    self.entries[OVERFLOW_INDEX].fetch_and(!Self::HANDOFF_MASK, Ordering::Release);
  }

  /// 是否有等闩者在册（[`Self::try_lock_exclusive`] 门控臂的让位判据）
  #[inline]
  pub fn handoff_pending(&self) -> bool {
    (self.entries[OVERFLOW_INDEX].load(Ordering::Acquire) & Self::HANDOFF_MASK) != 0
  }

  /// 判定当前是否处于独占加锁状态
  ///
  /// 在 garnet 中的相对路径:libs/storage/Tsavorite/cs/src/core/Index/Tsavorite/HashBucket.cs:IsLatchedExclusive
  #[inline]
  pub fn is_latched_exclusive(&self) -> bool {
    (self.entries[OVERFLOW_INDEX].load(Ordering::Acquire) & Self::EXCLUSIVE_LATCH_MASK) != 0
  }

  /// 判定当前是否处于共享加锁状态（供并发测试与锁状态断言）
  ///
  /// 对照 C# HashBucket 的 IsLatched 语义（主映射归属 HashBucket::is_latched；本函数为共享计数非零判定拆分）
  ///（共享位非零判定，C# 侧由 IsLatched + NumLatchedShared 组合覆盖）
  ///
  /// 测试握手：生产读面仅内部 `try_unlock_shared` 消费，pub 可见性仅供并发测试
  /// 断言锁态（同 wtxn `TxnKeysBuffer::len` 先例）。
  #[doc(hidden)]
  #[inline]
  pub fn is_latched_shared(&self) -> bool {
    (self.entries[OVERFLOW_INDEX].load(Ordering::Acquire) & Self::SHARED_LATCH_MASK) != 0
  }

  /// 获取当前并发共享读者数量
  ///
  /// 在 garnet 中的相对路径:libs/storage/Tsavorite/cs/src/core/Index/Tsavorite/HashBucket.cs:NumLatchedShared
  #[inline]
  pub fn num_latched_shared(&self) -> u16 {
    ((self.entries[OVERFLOW_INDEX].load(Ordering::Acquire) & Self::SHARED_LATCH_MASK)
      >> Self::SHARED_LATCH_SHIFT) as u16
  }

  /// 判定当前是否存在任意类型的锁占用
  ///
  /// 在 garnet 中的相对路径:libs/storage/Tsavorite/cs/src/core/Index/Tsavorite/HashBucket.cs:IsLatched
  ///
  /// 测试握手:生产路径零消费,仅并发测试断言锁态（同 [`Self::is_latched_shared`] 先例）
  #[doc(hidden)]
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
  /// CAS 竞争回退走 wbase Backoff 三阶退避
  pub fn set_overflow_index(&self, overflow_idx: u64) -> bool {
    if overflow_idx == 0 {
      return false;
    }
    let target_addr = overflow_idx & HashBucketEntry::ADDRESS_MASK;
    let mut backoff = Backoff::new();
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
          backoff.snooze();
        }
      }
    }
  }

  /// 查找当前桶内第一个匹配指定 Tag 的有效地址（严格对标 TsavoriteBase FindTag 首项快速探针）
  ///
  /// 内存序论证（本 crate 桶扫描通用基线）：扫描阶段仅做 tag/address 过滤，
  /// u64 对齐原子加载无撕裂，Relaxed 足矣；真正需要 happens-before 的是
  /// 「依据命中地址解引用记录内存」的时刻——发布方先写记录数据、再以
  /// CAS(AcqRel) 发布槽位条目，读者以 Relaxed 读到该条目后，以 Acquire 复读
  /// 命中槽建立 release/acquire 同步（对标 C# TsavoriteBase.cs:226-265 FindTag
  /// 的 volatile 读语义：arm64 上单条 LDAR 替代 DMB 全局屏障，且不阻断后续
  /// load 重排）。复读窗口内槽位被并发 CAS 更新时，读到的是更新条目自身的
  /// 同步链（其 Release 发布已含全部前置写），返回复读值恒消费「已同步」地址；
  /// 复读不再匹配则继续扫描后续槽位。
  /// 7 槽位极速展开，消除循环边界检查与分支预测失效（严格对标 C# FindTag）
  #[inline(always)]
  pub fn find_tag_address(&self, tag: u16) -> Option<u64> {
    let expected_hi = (tag as u64) & HashBucketEntry::TAG_MASK;

    #[inline(always)]
    fn check_slot(item: &AtomicU64, expected_hi: u64) -> Option<u64> {
      let raw = item.load(Ordering::Relaxed);
      if raw == 0 {
        return None;
      }
      if (raw >> HashBucketEntry::TAG_SHIFT) == expected_hi {
        let synced = item.load(Ordering::Acquire);
        if (synced >> HashBucketEntry::TAG_SHIFT) == expected_hi {
          let addr = synced & HashBucketEntry::ADDRESS_MASK;
          if addr != 0 {
            return Some(addr);
          }
        }
      }
      None
    }

    if let Some(addr) = check_slot(&self.entries[0], expected_hi) {
      return Some(addr);
    }
    if let Some(addr) = check_slot(&self.entries[1], expected_hi) {
      return Some(addr);
    }
    if let Some(addr) = check_slot(&self.entries[2], expected_hi) {
      return Some(addr);
    }
    if let Some(addr) = check_slot(&self.entries[3], expected_hi) {
      return Some(addr);
    }
    if let Some(addr) = check_slot(&self.entries[4], expected_hi) {
      return Some(addr);
    }
    if let Some(addr) = check_slot(&self.entries[5], expected_hi) {
      return Some(addr);
    }
    if let Some(addr) = check_slot(&self.entries[6], expected_hi) {
      return Some(addr);
    }
    None
  }

  /// 在当前桶的数据槽位中查找匹配指定 Tag 和逻辑地址的有效条目
  ///
  /// 内存序论证参见 [`HashBucket::find_tag_address`]：Relaxed 扫描 + 命中槽 Acquire 复读
  #[inline(always)]
  pub fn find_entry_by_address(&self, tag: u16, address: u64) -> Option<(usize, HashBucketEntry)> {
    let target_raw = HashBucketEntry::new(address, tag, false).as_raw();

    #[inline(always)]
    fn check_slot(
      item: &AtomicU64,
      target_raw: u64,
      slot: usize,
    ) -> Option<(usize, HashBucketEntry)> {
      let raw = item.load(Ordering::Relaxed);
      if raw == 0 {
        return None;
      }
      if raw == target_raw {
        // 命中定序：Acquire 复读命中槽（论证见 find_tag_address）
        let synced = item.load(Ordering::Acquire);
        if synced == target_raw {
          return Some((slot, HashBucketEntry::from_raw(synced)));
        }
      }
      None
    }

    if let Some(res) = check_slot(&self.entries[0], target_raw, 0) {
      return Some(res);
    }
    if let Some(res) = check_slot(&self.entries[1], target_raw, 1) {
      return Some(res);
    }
    if let Some(res) = check_slot(&self.entries[2], target_raw, 2) {
      return Some(res);
    }
    if let Some(res) = check_slot(&self.entries[3], target_raw, 3) {
      return Some(res);
    }
    if let Some(res) = check_slot(&self.entries[4], target_raw, 4) {
      return Some(res);
    }
    if let Some(res) = check_slot(&self.entries[5], target_raw, 5) {
      return Some(res);
    }
    if let Some(res) = check_slot(&self.entries[6], target_raw, 6) {
      return Some(res);
    }
    None
  }

  /// 查找当前桶内的第一个空槽位（展开 7 槽位极速定位）
  ///
  /// 仅定位空槽供后续 CAS 插入（CAS 自带 AcqRel 发布屏障），无需 Acquire
  #[inline(always)]
  pub fn find_empty_slot(&self) -> Option<usize> {
    if self.entries[0].load(Ordering::Relaxed) == 0 {
      return Some(0);
    }
    if self.entries[1].load(Ordering::Relaxed) == 0 {
      return Some(1);
    }
    if self.entries[2].load(Ordering::Relaxed) == 0 {
      return Some(2);
    }
    if self.entries[3].load(Ordering::Relaxed) == 0 {
      return Some(3);
    }
    if self.entries[4].load(Ordering::Relaxed) == 0 {
      return Some(4);
    }
    if self.entries[5].load(Ordering::Relaxed) == 0 {
      return Some(5);
    }
    if self.entries[6].load(Ordering::Relaxed) == 0 {
      return Some(6);
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
  /// 把最终值的原子发布收敛为单次 CAS（见 `HashIndex::insert_to_bucket` 的 C# 两阶段
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
}

impl Drop for BucketExclusiveGuard<'_> {
  fn drop(&mut self) {
    self.bucket.unlock_exclusive();
  }
}

/// 单键独占闩守卫：按哈希寻址视角下的独占桶闩（类型即 `BucketExclusiveGuard` 别名）
///
/// `HashIndex::try_lock_key_hash_exclusive` 的返回形态，对标 C# Tsavorite 的单键
/// ephemeral 独占闩（`Implementation/Locking/TransientLocking.cs` 里的
/// `TryEphemeralXLock`）：
/// 一次尝试取闩、失败即返回 `None`、离开作用域放闩，无自旋驱动无回滚。
/// 本别名不引入第二份锁实现——底层与桶本体守卫同字、同 Drop，仅命名点区分
/// 「按哈希定位的读写窗口」与「按桶/槽位定位的临时探针」两种调用侧语义。
pub type KeyLatch<'a> = BucketExclusiveGuard<'a>;

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

/// HashIndex 按哈希独占闩转发域：桶闩 CAS 体唯一位点在本件 HashBucket 原语上，
/// HashIndex 侧只余按哈希寻址与一行转调，无第二份锁实现
impl HashIndex {
  /// 按算定键哈希取主桶独占闩的 RAII 守卫（scoped 会合域口径入口）
  ///
  /// 调用方已经 `whasher::scoped_hash(prefix, key)` 全仓单点算定归属域哈希
  ///（会话物理前缀种子，与事务键锁 `wtxn::TxnKeyEntryComparison::scoped_key_hash`
  /// 同一构造口径）后经本入口寻桶取闩——桶下标截断为 `(hash as usize) & mask`。
  /// 消费面为 wkv TTL 键闩（`ttl.rs`），与 wkv rmw 窗口、wtxn 事务键锁三面
  /// 同键同桶互斥（票 wtxn-wkv-keybucket-hash-scope-desync）。
  #[inline]
  pub fn try_lock_key_hash_exclusive(&self, hash: u64) -> Option<KeyLatch<'_>> {
    self
      .get_bucket((hash as usize) & self.mask)
      .lock_exclusive_guard()
  }
}
