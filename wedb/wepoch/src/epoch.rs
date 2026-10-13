//! LightEpoch 纪元保护管理器
//!
//! 对照 C# Tsavorite Epochs/LightEpoch.cs：采用无锁 (Latch-free) 惰性同步机制，
//! 管理并发读写事务的纪元生命周期与安全回收判定。

use std::{
  array::from_fn,
  cell::UnsafeCell,
  fmt,
  iter::repeat_with,
  mem::{align_of, offset_of, size_of},
  ptr,
  ptr::eq,
  sync::{
    Arc,
    atomic::{AtomicU32, AtomicU64, AtomicUsize, Ordering, fence},
  },
  thread::{available_parallelism, yield_now},
};

use log::{debug, trace};
use wbase::{backoff::Backoff, thread::current_thread_id};
use whasher::mix_thread_id;

use crate::{
  EpochEntry, Error, Participant, ProtectedScope, Result,
  tls::{
    FAST_ENTRY, FAST_PARTICIPANT, FastEntry, cached_slot, clear_thread_entry, get_thread_entry,
    note_participant_slot, set_thread_entry,
  },
};

/// 延迟清理动作队列容量（对照 C# Tsavorite kDrainListSize = 16）
///
/// 生产面仅条目表构造内部消费；dev/test 构建按 LightEpoch.TestHooks 同例经
/// lib.rs cfg 门再导出供 tests 压力常量取值，发布构建收敛为 crate 内部
#[cfg(debug_assertions)]
pub const DRAIN_LIST_SIZE: usize = 16;
#[cfg(not(debug_assertions))]
pub(crate) const DRAIN_LIST_SIZE: usize = 16;

/// 延迟清理槽位空闲标记值（u64::MAX）
const DRAIN_ENTRY_FREE: u64 = u64::MAX;
/// 延迟清理槽位独占抢占/执行标记值（u64::MAX - 1，对照 C# LightEpoch 内部 CAS 状态机）
const DRAIN_ENTRY_CLAIMING: u64 = u64::MAX - 1;

/// 延迟清理动作包装（类型擦除函数指针形态）
struct EpochAction {
  ptr: *mut (),
  call: unsafe fn(*mut ()),
  drop: unsafe fn(*mut ()),
}

// SAFETY: EpochAction 内部拥有的 ptr 指向堆分配的 FnOnce 闭包（由 Box::into_raw 创建），
// 拥有独占所有权且闭包类型要求 Send + 'static，可在线程间安全转移所有权。
unsafe impl Send for EpochAction {}

impl EpochAction {
  fn new<F: FnOnce() + Send + 'static>(f: F) -> Self {
    unsafe fn call_fn<F: FnOnce()>(ptr: *mut ()) {
      // SAFETY: caller guarantees ptr was created by Box::into_raw
      let b = unsafe { Box::from_raw(ptr as *mut F) };
      b();
    }
    unsafe fn drop_fn<F>(ptr: *mut ()) {
      // SAFETY: caller guarantees ptr was created by Box::into_raw
      unsafe {
        drop(Box::from_raw(ptr as *mut F));
      }
    }
    Self {
      ptr: Box::into_raw(Box::new(f)) as *mut (),
      call: call_fn::<F>,
      drop: drop_fn::<F>,
    }
  }

  fn call(mut self) {
    let call = self.call;
    let ptr = self.ptr;
    self.ptr = ptr::null_mut();
    // SAFETY: self.ptr 转移到本地变量后已置空，保证 call 仅被消费一次；
    // ptr 由 Box::into_raw 创建，call 函数指针由 new 中的类型安全推导转换为对应闭包类型。
    unsafe {
      call(ptr);
    }
  }
}

impl Drop for EpochAction {
  fn drop(&mut self) {
    if !self.ptr.is_null() {
      // SAFETY: ptr 非空表示尚未被 call 消费，由 drop_fn 安全恢复 Box 并销毁。
      unsafe {
        (self.drop)(self.ptr);
      }
    }
  }
}

/// 待执行的纪元延迟清理动作项（1:1 对标 C# EpochActionPair，由 epoch CAS 状态机保证独占互斥）
///
/// 64 字节 Cacheline 对齐：多线程并发 CAS 各槽位 `epoch` 时互不串扰缓存行，
/// 消除 C# 原版（16 字节/槽，4 槽共享一行）存在的伪共享。
#[repr(align(64))]
struct DrainEntry {
  epoch: AtomicU64,
  action: UnsafeCell<Option<EpochAction>>,
}

// SAFETY: DrainEntry 的内部可变性 UnsafeCell 由 epoch CAS 状态机严格互斥保护，
// 仅当原子 CAS 将 epoch 从空闲翻转为当前活跃纪元成功的线程拥有写入权；
// 仅当安全纪元推进后触发 drain 时的独占清理线程拥有读出与消费权。无并发读写竞态。
unsafe impl Send for DrainEntry {}
unsafe impl Sync for DrainEntry {}

// 编译期钉死布局：未来字段变动若破坏「单槽独占缓存行」性质，直接编译失败
const _: () = assert!(size_of::<DrainEntry>() == 64);
const _: () = assert!(align_of::<DrainEntry>() == 64);

impl DrainEntry {
  const fn new() -> Self {
    Self {
      epoch: AtomicU64::new(DRAIN_ENTRY_FREE),
      action: UnsafeCell::new(None),
    }
  }
}

static NEXT_INSTANCE_ID: AtomicU64 = AtomicU64::new(1);
static ACTIVE_INSTANCES: AtomicUsize = AtomicUsize::new(0);

/// Microsoft Garnet Tsavorite 架构风格的 LightEpoch 纪元保护管理器
///
/// 采用无锁 (Latch-free) 惰性同步机制，管理并发读写事务的纪元生命周期与安全回收判定。
/// `repr(C, align(64))` + 显式缓存行填充：current_epoch（写热点）/ safe_to_reclaim_epoch
/// （读热点）/ drain_count 各占独立缓存行，杜绝控制面伪共享。
/// 结构体级 64 字节对齐保证任意分配基址下填充隔离均成立，而非仅相对偏移成立。
#[repr(C, align(64))]
pub struct LightEpoch {
  /// 唯一实例 ID
  pub id: u64,
  /// 全局推进的当前纪元（初始为 1，0 专用于表示未受保护）
  /// 独占缓存行：bump 路径 fetch_add 写热点
  pub current_epoch: AtomicU64,
  _pad0: [u8; 48],
  /// 本地缓存的全局最低安全回收纪元
  /// 独占缓存行：is_safe_to_reclaim 高频 Acquire 读热点，免受 bump 写串扰
  pub safe_to_reclaim_epoch: AtomicU64,
  _pad1: [u8; 56],
  /// 待触发 drain 动作计数
  pub drain_count: AtomicU32,
  _pad2: [u8; 60],
  /// 参与者条目表（每个元素独占 64 字节 Cacheline）
  pub entries: Box<[EpochEntry]>,
  /// 延迟回收动作列表（固定 16 个槽位，槽位间 64 字节隔离）
  drain_list: Box<[DrainEntry; DRAIN_LIST_SIZE]>,
  /// 最大会话数容量，也是 register 时扫描槽位的上限
  pub max_sessions: usize,
}

impl LightEpoch {
  /// 默认最大线程/参与者容量
  const DEFAULT_MAX_THREADS: usize = 128;

  /// 默认条目表容量（严格对标 libs/storage/Tsavorite/cs/src/core/Epochs/LightEpoch.cs:kTableSize = Math.Max(128, Environment.ProcessorCount * 2)）
  #[inline]
  fn default_table_size() -> usize {
    available_parallelism()
      .map(|n| (n.get() * 2).max(128))
      .unwrap_or(Self::DEFAULT_MAX_THREADS)
  }

  /// 创建指定最大容量的 LightEpoch 实例
  ///
  /// 对照 libs/storage/Tsavorite/cs/src/core/Epochs/LightEpoch.cs:LightEpoch（构造器）
  /// 对照 libs/client/LightEpoch.cs:LightEpoch（构造器）
  /// 刻意差异：C# 表容量固定取 max(128, CPU 数 × 2)，此处由调用方显式指定；
  /// 实例 ID 以全局单调计数器分配、永不复用（C# SelectInstance 回收槽位，理论存在
  /// 实例 ID 复用窗口，Rust 从根上规避）
  pub fn new(max_sessions: usize) -> Self {
    let max_sessions = max_sessions.max(1);
    // 容量与预留解耦：表容量 = max_sessions + TLS 余量
    let capacity = max_sessions.saturating_add(Self::default_table_size());
    let entries: Box<[EpochEntry]> = repeat_with(EpochEntry::new).take(capacity).collect();

    ACTIVE_INSTANCES.fetch_add(1, Ordering::Relaxed);
    Self {
      id: NEXT_INSTANCE_ID.fetch_add(1, Ordering::Relaxed),
      current_epoch: AtomicU64::new(1),
      _pad0: [0; 48],
      safe_to_reclaim_epoch: AtomicU64::new(0),
      _pad1: [0; 56],
      drain_count: AtomicU32::new(0),
      _pad2: [0; 60],
      entries,
      drain_list: Box::new(from_fn(|_| DrainEntry::new())),
      max_sessions,
    }
  }

  /// 注册当前线程或会话为参与者
  ///
  /// 扫描条目表并尝试 CAS 抢占空闲槽位。成功后返回独占该槽位的 `Participant`。
  /// C# 单一保护机制下无显式注册（Resume/Acquire 即隐式占位）；Rust 双轨拆分出
  /// 显式会话句柄，拆分理由见 participant.rs 模块文档
  pub fn register(self: &Arc<Self>) -> Result<Participant> {
    for (idx, entry) in self.entries.iter().take(self.max_sessions).enumerate() {
      if entry.try_reserve() {
        trace!("成功注册参与者，分配条目索引: {idx}");
        note_participant_slot(self.id, idx, entry);
        return Ok(Participant::new(Arc::clone(self), idx));
      }
    }
    Err(Error::ExceededMaxThreads(self.max_sessions))
  }

  /// 当前线程在本实例已登记的活动槽位下标（0-based）；未登记返回 None
  #[inline]
  fn active_idx(&self) -> Option<usize> {
    let entry = get_thread_entry(self.id);
    (entry != 0 && entry <= self.entries.len()).then(|| entry - 1)
  }

  /// 条目由指定线程占用且处于保护态的基础判定（多路扫描路径共享）
  #[inline]
  fn protected_by(entry: &EpochEntry, tid: u64) -> bool {
    entry.is_protected() && entry.thread_id() == tid
  }

  /// 单槽快速缓存（FAST_ENTRY / FAST_PARTICIPANT）归属实例 + 线程属主 + 保护态联合命中判定
  ///
  /// # Safety 缓存归属本实例时 `ptr` 指向存活条目表（实例 ID 全局单调不复用担保）
  #[inline]
  unsafe fn cached_hit<'a>(fe: FastEntry, id: u64, tid: u64) -> Option<&'a EpochEntry> {
    if fe.instance_id != id || fe.slot == 0 {
      return None;
    }
    // SAFETY: 前置条件保证指针不悬垂；生命周期由调用方以 self 存活期绑定
    let entry = unsafe { &*fe.ptr };
    Self::protected_by(entry, tid).then_some(entry)
  }

  /// 定位本线程经 TLS 机制（`resume`/`protected_scope`）持有的受保护条目
  ///
  /// 单槽快速缓存优先（O(1)，免去 RefCell 借用与线性 find），未命中回退本实例
  /// TLS 登记槽；均校验线程 ID 属主与保护态
  #[inline]
  fn tls_protected_entry(&self, tid: u64) -> Option<&EpochEntry> {
    if let Some(entry) = unsafe { Self::cached_hit(FAST_ENTRY.get(), self.id, tid) } {
      return Some(entry);
    }
    let idx = self.active_idx()?;
    // SAFETY: active_idx 已保证 idx < entries.len()
    let entry = unsafe { self.entries.get_unchecked(idx) };
    Self::protected_by(entry, tid).then_some(entry)
  }

  /// 有 pending 延迟动作时协助收割（不刷新本线程公布纪元，重入路径专用）
  #[inline]
  pub(crate) fn drain_if_pending(&self) {
    if self.has_pending_drain() {
      self.drain();
    }
  }

  /// 尝试为本线程 CAS 抢占下标 `idx` 槽位；成功则完成 TLS 登记并按需协助收割
  ///
  /// # Safety 前置条件
  /// 调用方须保证 `idx < self.entries.len()`
  #[inline]
  fn claim_entry(self: &Arc<Self>, idx: usize, tid: u64) -> bool {
    let entry = unsafe {
      // SAFETY: 调用方保证 idx < len
      self.entries.get_unchecked(idx)
    };
    if !entry.try_claim(tid, &self.current_epoch) {
      return false;
    }
    set_thread_entry(self.id, idx + 1, || Arc::downgrade(self));
    // 对照 C# Acquire 尾部：所有获取路径统一检查 pending 延迟动作并协助收割；
    // 只可 drain 不可 refresh（同 resume 快路径判据）——同 tid 双轨并存时可能存在
    // 钉住旧纪元的 Participant 守卫（如跨 await 屏障临界区），全量刷新会提前解除
    // 其对旧纪元的保护，属于内存安全问题；新槽位经 try_claim 现场读取最新纪元发布，
    // 本就无需刷新
    self.drain_if_pending();
    true
  }

  /// 当前线程进入受保护的纪元区（对照 libs/client/LightEpoch.cs:Resume）
  ///
  /// 对照 libs/storage/Tsavorite/cs/src/core/Epochs/LightEpoch.cs:Resume
  /// 对照 libs/storage/Tsavorite/cs/src/core/Epochs/LightEpoch.cs:Acquire
  ///
  /// 单一扫描状态机：
  /// 1. 重入快路径：本线程已持有本实例保护槽位时仅递增重入计数（单槽缓存 O(1) 优先）；
  /// 2. 乐观 O(1) 优先尝试重用上次缓存的槽位（单次 CAS 即返回）；
  /// 3. 慢路径以 gxhash 散列起点环形切片扫描全表（杜绝模除开销），表满时按三级退避重试。
  ///
  /// 刻意差异（对照 C# 论证）：C# `Acquire` 首句 `DebugAssertEntryNotReserved` 禁止重入
  /// （重入即断言失败，防回调重入 Tsavorite 引发死锁）；Rust 以重入计数放宽为安全的
  /// 嵌套语义，供 `ProtectedScope`/`EpochGuard` RAII 嵌套与上游同步 API 复用，
  /// 由 `tests/epoch/protection.rs` 嵌套重入用例锁定
  ///
  /// 恢复保护入口与条目占用断言的对应：
  /// libs/storage/Tsavorite/cs/src/core/Epochs/LightEpoch.cs:ResumeIfNotProtected
  /// libs/storage/Tsavorite/cs/src/core/Epochs/LightEpoch.EntryTable.cs:DebugAssertEntryNotReserved
  pub fn resume(self: &Arc<Self>) {
    let tid = current_thread_id();

    if let Some(entry) = self.tls_protected_entry(tid) {
      entry.inc_reentrant();
      // 对照 C# Acquire 尾部：所有获取路径统一检查 pending 延迟动作并协助收割；
      // 此处只可 drain() 而不可 refresh——外层作用域可能仍在读取旧纪元数据，
      // 刷新公布纪元会提前解除对旧纪元的保护，属于内存安全问题
      self.drain_if_pending();
      return;
    }

    let len = self.entries.len();

    // 快路径：乐观 O(1) 重用上次缓存的槽位，单次 CAS 命中即直接返回
    if let Some(idx) = cached_slot(self.id).checked_sub(1).filter(|&idx| idx < len)
      && self.claim_entry(idx, tid)
    {
      return;
    }

    // 慢路径：以 gxhash 散列起点环形扫描全表，分支换算环形下标（探查路径零模除），退避重试
    let start = mix_thread_id(tid) % len;
    let mut backoff = Backoff::new();
    loop {
      for offset in 0..len {
        let sum = start + offset;
        if self.claim_entry(if sum < len { sum } else { sum - len }, tid) {
          return;
        }
      }
      backoff.snooze();
    }
  }

  /// 当前线程退出受保护的纪元区（对照 libs/client/LightEpoch.cs:Suspend）
  ///
  /// 对照 libs/storage/Tsavorite/cs/src/core/Epochs/LightEpoch.cs:Suspend
  /// 对照 libs/storage/Tsavorite/cs/src/core/Epochs/LightEpoch.cs:Release
  pub fn suspend(&self) {
    let Some(entry) = self.tls_protected_entry(current_thread_id()) else {
      return;
    };
    // 重入安全：当存在嵌套保护时仅递减重入计数；只有计数降为 0 时才真正释放槽位
    if !entry.exit() {
      return;
    }

    clear_thread_entry(self.id);
    self.after_release();
  }

  /// 退出保护区之后的统一收尾：有待处理的延迟动作时协助排空
  ///
  /// 对照 C# Release→Suspend 尾部的 `if (drainCount > 0) SuspendDrain()`。
  /// SeqCst 屏障由 [`Self::suspend_drain`] 循环首句自带（对应 C# 的
  /// Thread.MemoryBarrier），drain_count == 0 的热路径上零屏障开销。
  #[inline]
  pub(crate) fn after_release(&self) {
    if self.has_pending_drain() {
      self.suspend_drain();
    }
  }

  /// 检查当前线程在此 LightEpoch 实例中是否正处于保护区（对照 libs/client/LightEpoch.cs:ThisInstanceProtected）
  ///
  /// 对照 libs/storage/Tsavorite/cs/src/core/Epochs/LightEpoch.cs:ThisInstanceProtected
  /// 仅覆盖 TLS `resume`/`suspend` 配对路径；显式 `Participant::enter` 的保护请用 [`Self::thread_protected`]。
  pub fn this_instance_protected(&self) -> bool {
    self.tls_protected_entry(current_thread_id()).is_some()
  }

  /// 当前线程是否以任一机制（TLS 作用域或 `Participant` 会话）在本实例受保护
  ///
  /// C# 只有单一保护机制，`ThisInstanceProtected` 即完整判定；Rust 拆分为两条机制后，
  /// 排空屏障类调用方（推进纪元并等待 `is_safe_to_reclaim`）必须用本方法识别自身保护，
  /// 否则自身钉住目标纪元将导致屏障活锁。线程 ID 全局唯一不复用，扫描判定无歧义。
  pub fn thread_protected(&self) -> bool {
    self.thread_protected_entry().is_some()
  }

  /// 扫描条目表定位本线程以任一机制（TLS 作用域或 `Participant` 会话）持有的受保护条目
  ///
  /// TLS 与 Participant 两个单槽缓存优先（均 O(1)，对照 C# ProtectAndDrain 经 TLS
  /// 索引 O(1) 定位）；均未命中再全表兜底扫描（同线程多 Participant 等罕见布局）。
  /// 线程 ID 全局唯一不复用，扫描判定无歧义；未受保护返回 None
  fn thread_protected_entry(&self) -> Option<&EpochEntry> {
    let tid = current_thread_id();
    if let Some(entry) = self.tls_protected_entry(tid) {
      return Some(entry);
    }
    if let Some(entry) = unsafe { Self::cached_hit(FAST_PARTICIPANT.get(), self.id, tid) } {
      return Some(entry);
    }
    self.entries.iter().find(|e| Self::protected_by(e, tid))
  }

  /// 获取基于 RAII 作用域自动管理生命周期的保护守卫（对照 libs/storage/Tsavorite/cs/test/test.epoch/helpers/EpochProtection.cs:ProtectedScope）
  pub fn protected_scope<'a>(self: &'a Arc<Self>) -> ProtectedScope<'a> {
    ProtectedScope::new(self)
  }

  /// 递增全局当前纪元并尝试触发安全回收（对照 libs/client/LightEpoch.cs:BumpCurrentEpoch）
  ///
  /// 对照 libs/storage/Tsavorite/cs/src/core/Epochs/LightEpoch.cs:BumpCurrentEpoch
  ///
  /// 刻意差异：C# 版 Debug.Assert 要求调用线程必须处于保护区，此处放宽为任意线程
  /// 可推进（无 panic 约束），保护态仅作为上游使用约定而非本层强制。
  pub fn bump_current_epoch(&self) -> u64 {
    let new_epoch = self.current_epoch.fetch_add(1, Ordering::AcqRel) + 1;
    trace!("递增全局纪元至: {new_epoch}");
    if self.has_pending_drain() {
      self.drain();
    } else {
      self.compute_safe_to_reclaim_epoch_with(new_epoch);
    }
    new_epoch
  }

  /// 刷新指定线程持有的所有受保护条目公布的纪元至目标纪元
  ///
  /// 用于 `wepoch::wait_condition_async` 等显式刷新路径（调用方自证不在跨 await
  /// 守卫临界区内）解除本线程对旧纪元的自钉；bump 注册路径不走本全量口径
  /// （定向判据见 [`Self::help_drain`]）
  #[inline]
  pub fn refresh_thread_protected_entries(&self, tid: u64, target_epoch: u64) {
    for entry in self.entries.iter().filter(|e| Self::protected_by(e, tid)) {
      entry.refresh_epoch(target_epoch);
    }
  }

  /// 以本线程所能尽力推进延迟清理：定向刷新 + 收割（`bump_current_epoch_action`
  /// 注册路径专用，列表满自旋臂与注册收尾同罩同一判据）
  ///
  /// 对照 C# 分界（garnet LightEpoch.cs）：
  /// - `Acquire`（:513-529）尾部仅 Drain——获取路径不刷新（本仓 claim_entry 票已
  ///   统一口径为 `drain_if_pending`）；
  /// - `ProtectAndDrain`（:296-316）才是显式刷新且文档明示 Drops protection for
  ///   the old epoch；`BumpCurrentEpoch(Action)`（:383-430）自旋臂/收尾均调它，
  ///   但 C# 单轨保护且 `Debug.Assert(ThisInstanceProtected)` 前置——被刷新的
  ///   只是调用线程自己的同步临界区条目。
  ///
  /// Rust 双轨定向刷新面（守护纪元与待排水纪元比对，单点判据）：
  /// - TLS 轨条目：C# 单轨同构（同步临界区，无跨 await 借用），无条件刷新——
  ///   陈旧 TLS 钉若不刷新，满列表自旋臂将自钉活锁
  ///   （concurrent_register_drain_interleave_conserves_drain_count 锁定）；
  /// - 其余条目（`Participant` 轨）仅当公布纪元恰为 `prior_epoch`（新鲜钉：注册
  ///   线程自己在本次 bump 前沿取得或经就地注册保新）才刷新——participant_refresh /
  ///   participant_long_guard_drain_list_exhaustion 锁定的就地注册排水活性；
  ///   契约安全点的旧钉自抬走定向单条 [`Self::refresh_entry_relaxed`]，不在本
  ///   判据内（防同线程他任务守卫被误抬，c01d 防线同源）；
  /// - 钉旧纪元（< `prior_epoch`）的 `Participant` 守卫绝不在刷新面内：横跨 await
  ///   的长期守卫被抬即旧纪元保护静默失效（c01d 受害机理，claim_entry 票同源）。
  ///
  /// 活性取舍：持陈旧 `Participant` 守卫触达且 drain_list 被更高纪元动作占满时，
  /// 注册自旋等待守卫自行解除而不代为抬守卫——静默抬守卫是内存安全事故，可见
  /// 自旋是更优失败面。调用面契约：跨 await 的 `Participant` 守卫不得触达本
  /// 注册路径——守卫存续期内 bump 前须先 drop 守卫，或走契约安全点的
  /// [`Self::bump_current_epoch_action_relaxed`] 定向自抬；持守卫调用方若违约
  /// 触达，满列表自旋即等待守卫自行解除，旧纪元保护绝不静默失效。
  /// 仅慢路径进入（drain_count > 0 / 列表满），O(N) 表扫描不伤热路径。
  fn help_drain(&self, prior_epoch: u64) {
    let tid = current_thread_id();
    let tls_idx = self.active_idx();
    let current = self.current_epoch.load(Ordering::Acquire);
    for (idx, entry) in self.entries.iter().enumerate() {
      if Self::protected_by(entry, tid)
        && (Some(idx) == tls_idx || entry.protected_epoch() == prior_epoch)
      {
        entry.refresh_epoch(current);
      }
    }
    self.drain();
  }

  /// 递增全局纪元并将关联动作注册到前置纪元，等待前置纪元安全回收时执行（对照 LightEpoch BumpCurrentEpoch(Action) 重载）
  ///
  /// 刻意差异（对照 C# :383-430 自旋臂/收尾的无条件 ProtectAndDrain）：本仓双轨
  /// 保护下收尾与列表满自旋臂同罩 [`Self::help_drain`] 的定向刷新判据——TLS 轨
  /// 与公布纪元恰为 prior_epoch 的新鲜钉照刷（C# 单轨合约面），钉旧纪元的跨
  /// await `Participant` 守卫不刷（对齐 C# Acquire 就地 Drain 的获取侧口径）；
  /// C# 无条件刷新形在双轨下即 c01d 同款守卫抬升事故
  ///
  /// # 所有权契约（强环孤岛防线）
  ///
  /// `on_drain` 闭包**不得强持有经任何路径强引用本纪元实例的对象**（如宿主
  /// 结构体经 `store_epoch` 类强字段回指本纪元：epoch→动作→宿主→epoch 强环
  /// 在引用计数回收下使三方永不析构——C# 原型同形环经追迹 GC 免疫，rust 无
  /// 等价物）。跨纪元宿主一律捕 [`Arc::downgrade`] 弱引用，升级失败即宿主已
  /// 终，动作按各自退化语义收尾。宿主析构（[`Self::drop`]）为动作的**最后
  /// 法定收割点**：析构时对全部已发布槽位无条件执行（不判 safe_to_reclaim，
  /// `&mut` 独占上下文结构上无并发）
  pub fn bump_current_epoch_action<F>(&self, on_drain: F)
  where
    F: FnOnce() + Send + 'static,
  {
    self.bump_current_epoch_action_impl(on_drain, None)
  }

  /// 契约安全点专用注册（[`Self::bump_current_epoch_action`] 的自抬放宽形；
  /// 生产唯一消费面 `wkv::read_cache::ReadCache::pump_close_barrier`）
  ///
  /// `relaxed` 定向语义：注册自旋环内对 `relaxed` 参与者**单条**保护条目（含
  /// 钉旧纪元的跨 await 长守卫）周期性抬升至最新纪元——即 C#
  /// `BumpCurrentEpoch(Action)` 对调用线程 ProtectAndDrain（"Drops protection
  /// for the old epoch"）的循环版。仅限调用方处于「出借期已结束的契约安全点」：
  /// 该参与者此后至守卫 drop 不再依赖任何旧纪元保护做裸指针直读（pump 契约见
  /// append.rs；c01d 防线不受影响——**定向单条**抬升，同线程他任务/他线程的
  /// 守卫绝不触碰，relax 系调用方显式选择而非默认）。
  pub fn bump_current_epoch_action_relaxed<F>(&self, relaxed: &Participant, on_drain: F)
  where
    F: FnOnce() + Send + 'static,
  {
    // 实例一致性（审查 P2）：错实例的 entry_idx 会落本表任意槽位，thread_id
    // 校验虽缩小爆炸半径但不消除——debug 下直接拦在调用面
    debug_assert!(
      eq(relaxed.epoch_ptr(), self),
      "relaxed 参与者注册于另一纪元实例，entry_idx 跨表不可用"
    );
    self.bump_current_epoch_action_impl(on_drain, Some(relaxed.entry_idx()))
  }

  fn bump_current_epoch_action_impl<F>(&self, on_drain: F, relaxed_entry: Option<usize>)
  where
    F: FnOnce() + Send + 'static,
  {
    let prior_epoch = self.bump_current_epoch() - 1;
    let mut action_opt = Some(EpochAction::new(on_drain));

    'outer: loop {
      for entry in self.drain_list.iter() {
        let curr_epoch = entry.epoch.load(Ordering::Acquire);
        // 单一 CAS 闭环：FREE 槽位直接抢占；已发布槽位须达安全纪元方可回收替换。
        // 哨兵 FREE/CLAIMING 大于任何真实安全纪元，被同一比较自然排除，杜绝 ABA
        if (curr_epoch == DRAIN_ENTRY_FREE
          || curr_epoch <= self.safe_to_reclaim_epoch.load(Ordering::Acquire))
          && Self::cas_claim_slot(entry, curr_epoch)
        {
          // 安全性保证：CAS 成功即取得槽位独占权；FREE 槽无前驱动作
          let new_action = action_opt.take();
          let prev_action = unsafe {
            let ptr = entry.action.get();
            let prev = (*ptr).take();
            *ptr = new_action;
            prev
          };
          if curr_epoch == DRAIN_ENTRY_FREE {
            // 先递增计数再公布纪元（对照 C# BumpCurrentEpoch(Action) 的「先发布后
            // Increment」非镜像序；加/减计数为可交换 RMW 总量恒守恒，此处镜像次序
            // 使 drain 侧 Acquire 读到公布纪元必见计数已加，观测面无瞬时偏差）
            self.drain_count.fetch_add(1, Ordering::AcqRel);
          }
          entry.epoch.store(prior_epoch, Ordering::Release);
          if let Some(act) = prev_action {
            act.call();
          }
          break 'outer;
        }
      }

      // 列表满且无可回收槽位：以本线程所能尽力推进收割，再让出调度权
      // （定向刷新判据同收尾，见 help_drain；relaxed 定向条目逐轮自抬——
      // 单次 refresh 不够：多注册方交错 bump 下，彼此的「refresh 时刻快照」
      // 互为对方槽位的回收障碍，逐轮自抬方破多注册方锁死，rc_grow 案实证）
      if let Some(ridx) = relaxed_entry {
        self.refresh_entry_relaxed(ridx);
      }
      self.help_drain(prior_epoch);
      yield_now();
    }

    self.help_drain(prior_epoch);
  }

  /// 定向单条契约安全点刷新（[`Self::bump_current_epoch_action_relaxed`] 自旋
  /// 环专用，O(1) 不走全表扫描）：仅当条目受保护且属主为调用线程时抬至最新
  /// 纪元——属主校验与 [`Participant::refresh`] 同一单属主契约，抬升面精确到
  /// 调用方显式声明的参与者，同线程他任务/他线程守卫零暴露
  fn refresh_entry_relaxed(&self, idx: usize) {
    if let Some(entry) = self.entries.get(idx)
      && entry.is_protected()
      && entry.thread_id() == current_thread_id()
    {
      entry.refresh_epoch(self.current_epoch.load(Ordering::Acquire));
    }
  }

  /// 获取当前全局纪元号
  ///
  /// 对照 C# 内部字段 CurrentEpoch（libs/storage/Tsavorite/cs/src/core/Epochs/LightEpoch.cs）
  #[inline]
  pub fn current_epoch(&self) -> u64 {
    self.current_epoch.load(Ordering::Acquire)
  }

  /// 扫描所有活跃 entries 找出全局最小保护纪元，并单调更新缓存
  ///
  /// 对照 libs/storage/Tsavorite/cs/src/core/Epochs/LightEpoch.cs:ComputeNewSafeToReclaimEpoch
  /// 对照 libs/client/LightEpoch.cs:ComputeNewSafeToReclaimEpoch
  /// 刻意差异：C# 以普通 store 无条件覆写缓存；此处 fetch_max 单调推进，
  /// 杜绝并发交错下的瞬时回退观测
  pub fn compute_safe_to_reclaim_epoch(&self) -> u64 {
    self.compute_safe_to_reclaim_epoch_with(self.current_epoch.load(Ordering::Acquire))
  }

  /// 内部带入指定上限纪元的安全回收纪元计算实现
  #[inline]
  pub(crate) fn compute_safe_to_reclaim_epoch_with(&self, current_epoch: u64) -> u64 {
    let mut oldest = current_epoch;

    for entry in self.entries.iter() {
      let epoch = entry.protected_epoch();
      if epoch != 0 && epoch < oldest {
        oldest = epoch;
        if oldest == 1 {
          break;
        }
      }
    }

    let safe = oldest.saturating_sub(1);
    // 先 Relaxed 读旧值，仅在新下界更高时才以 fetch_max 原子写入，
    // 保证并发推进时单调不回退，且低负载下无多余 RMW 开销
    let prev = self.safe_to_reclaim_epoch.load(Ordering::Relaxed);
    if safe > prev {
      let actual_prev = self.safe_to_reclaim_epoch.fetch_max(safe, Ordering::AcqRel);
      actual_prev.max(safe)
    } else {
      prev
    }
  }

  /// 延迟槽位 CAS 抢占统一样板：从 expect 态翻转为独占 CLAIMING 标记，成功即获槽位独占权
  #[inline]
  fn cas_claim_slot(entry: &DrainEntry, expect: u64) -> bool {
    entry
      .epoch
      .compare_exchange(
        expect,
        DRAIN_ENTRY_CLAIMING,
        Ordering::AcqRel,
        Ordering::Acquire,
      )
      .is_ok()
  }

  /// 原子抢占一个已就绪（trigger_epoch ≤ safe_epoch）的延迟动作槽位
  ///
  /// 哨兵 FREE/CLAIMING 大于任何真实安全纪元，被同一比较自然排除，杜绝 ABA；
  /// CAS 成功即取得槽位独占消费权
  #[inline]
  fn try_claim_ready_slot(entry: &DrainEntry, safe_epoch: u64) -> bool {
    let trigger_epoch = entry.epoch.load(Ordering::Acquire);
    trigger_epoch <= safe_epoch && Self::cas_claim_slot(entry, trigger_epoch)
  }

  /// 扫描延迟清理列表并触发所有达到安全回收纪元的动作（对照 libs/client/LightEpoch.cs:Drain）
  ///
  /// 对照 libs/storage/Tsavorite/cs/src/core/Epochs/LightEpoch.cs:Drain
  /// 与 C# 的顺序差异（行为等价，协议美化）：C# 先 `epoch = long.MaxValue` 再
  /// `Decrement`，与注册侧「先发布纪元后 Increment」非镜像——加/减计数虽为可交换
  /// RMW、总量恒守恒（C# 无正确性问题），但交错瞬间 `drain_count` 会短暂偏离
  /// 「在册动作数」真值（先 0 后 1 或先 2 后 1）。本实现镜像
  /// [`Self::bump_current_epoch_action`] 的注册协议：先减计数、后发布 FREE（Release），
  /// 使注册方经 Acquire 看到 FREE 时减计数必已落定，任意交错下 `drain_count` 与
  /// 槽位状态严格同步变化，杜绝观测窗口
  pub fn drain(&self) {
    let safe_epoch = self.compute_safe_to_reclaim_epoch();

    for entry in self.drain_list.iter() {
      if Self::try_claim_ready_slot(entry, safe_epoch) {
        // 安全性保证：CAS 成功即取得槽位独占消费权
        let action = unsafe { (*entry.action.get()).take() };
        // 先减计数再发布 FREE：与注册侧「先加计数再公布纪元」镜像配对，见上注释
        self.drain_count.fetch_sub(1, Ordering::AcqRel);
        entry.epoch.store(DRAIN_ENTRY_FREE, Ordering::Release);
        if let Some(act) = action {
          act.call();
        }
        if self.drain_count.load(Ordering::Acquire) == 0 {
          break;
        }
      }
    }
  }

  /// 当最后一个受保护的线程挂起时，代为执行所有未完成的延迟动作（对照 libs/client/LightEpoch.cs:SuspendDrain）
  ///
  /// 对照 libs/storage/Tsavorite/cs/src/core/Epochs/LightEpoch.cs:SuspendDrain
  /// 此时已无人受保护，安全纪元必为 current-1，全部就绪动作均可直接收割，
  /// 等价于 C# 的 Resume/Release 循环但免除额外的槽位占用与原子开销。
  fn suspend_drain(&self) {
    while self.drain_count.load(Ordering::Acquire) > 0 {
      // SeqCst 屏障确保看到最新的条目表状态，保证最后挂起的线程收割全部就绪动作
      //（对照 C# SuspendDrain 的 Thread.MemoryBarrier）
      fence(Ordering::SeqCst);
      // 仍有任何线程受保护则移交，绝不提前触发动作
      if self.entries.iter().any(EpochEntry::is_protected) {
        return;
      }
      self.drain();
      yield_now();
    }
  }

  /// 安全回收判断
  ///
  /// 先检查本地缓存的 `safe_to_reclaim_epoch`；若不满足则重新扫描活跃条目并更新缓存。
  /// 对照 microsoft/FASTER cc/src/core/light_epoch.h:IsSafeToReclaim（C# 两版无此方法，调用方直接
  /// 比较 SafeToReclaimEpoch 字段；本方法额外在缓存未命中时主动重扫推进缓存）
  #[inline]
  pub fn is_safe_to_reclaim(&self, target_epoch: u64) -> bool {
    if target_epoch <= self.safe_to_reclaim_epoch.load(Ordering::Acquire) {
      return true;
    }
    target_epoch <= self.compute_safe_to_reclaim_epoch()
  }

  /// 推进纪元并自旋/yield 等待所有早于或等于该纪元的读事务完全退出 (drain)
  ///
  /// 对照 C# 外部调用模式（BumpCurrentEpoch 后自旋等待 SafeToReclaimEpoch 追平，
  /// Tsavorite 各调用方的收尾套路）；microsoft/FASTER cc/src/core/light_epoch.h:SpinWaitForSafeToReclaim
  /// 为同构等待（其不含 bump 侧，由调用方先行推进）
  ///
  /// 契约：调用线程不得以 ≤ `target_epoch` 的纪元处于保护区（自身钉住旧纪元将导致活锁），
  /// 与 C# `BumpCurrentEpoch` 要求调用线程受保护的约定同源。
  pub fn bump_and_wait(&self, target_epoch: u64) {
    debug!("开始 bump_and_wait 等待纪元 {target_epoch} 完全 drain");
    let tid = current_thread_id();
    // 活锁防护谓词：本线程以 ≤ target_epoch 的纪元受保护（TLS 或 Participant 任一
    // 机制）将永久钉住目标纪元。线程 ID 全局唯一，按 tid 扫描判定无歧义，对应 C#
    // "BumpCurrentEpoch 必须在受保护线程上调用" 的契约面。
    let pinned = |e: &EpochEntry| Self::protected_by(e, tid) && e.protected_epoch() <= target_epoch;
    // 活锁防护断言（debug 专用）
    debug_assert!(
      !self.entries.iter().any(pinned),
      "bump_and_wait 活锁：调用线程以 ≤ {target_epoch} 的纪元受保护，须先 suspend/refresh"
    );
    while self.current_epoch.load(Ordering::Acquire) <= target_epoch {
      self.bump_current_epoch();
    }
    // 活锁防御：若本线程名下仍有 ≤ target_epoch 的保护纪元（如误在保护区内触发
    // bump_and_wait），自动刷新至最新 current_epoch，彻底消除 release 构建下
    // 自身钉住目标纪元导致的永久自旋活锁
    let cur = self.current_epoch.load(Ordering::Acquire);
    for entry in self.entries.iter().filter(|e| pinned(e)) {
      entry.refresh_epoch(cur);
    }
    crate::wait_condition_sync(
      None,
      false, // Caller should be unprotected, already checked above logically
      || self.is_safe_to_reclaim(target_epoch),
      |_backoff| {
        self.drain_if_pending();
        // Since we are running on_step each iteration, wait_condition_sync calls backoff.snooze(),
        // so we don't need to manually advance or snooze here.
      },
      None,
    );
    self.drain_if_pending();
    debug!("纪元 {target_epoch} drain 完成");
  }

  /// 是否存在等待排空的纪元操作（对照 Tsavorite Epoch drain 检查）
  ///
  /// 对照 C# drainCount > 0 检查模式（Acquire/Suspend/ProtectAndDrain 尾部统一判定）
  #[inline]
  pub fn has_pending_drain(&self) -> bool {
    self.drain_count.load(Ordering::Acquire) > 0
  }
}

/// 测试钩子读侧（对位 C# 独立分部文件
/// libs/storage/Tsavorite/cs/src/core/Epochs/LightEpoch.TestHooks.cs：TestHook 五口
/// 全 `internal`，文件头明示「Read-only views of LightEpoch's internal state, used
/// only by the unit tests in Tsavorite.test.epoch」，即「独立文件门 + internal
/// 可见性门」双门）。
///
/// rust 无 internal 与分部文件两级机制，本块以单一 `debug_assertions` 门复现该可见性面：
/// 用例（tests/epoch/{protection,support,concurrency,drain}.rs）在 dev profile 下编译可见，
/// 发布构建整体不导出，生产代码零引用（C# 同侧亦零生产引用）。块内另收口 C#
/// 对位面的生产零消费口（ActiveInstanceCount / ResetAllInstances / TrySuspend /
/// ProtectAndDrain / SafeToReclaimEpoch 读口 / EntryCount），同享此门。
#[cfg(debug_assertions)]
impl LightEpoch {
  /// 获取当前线程分配到的条目槽位（1-based，0 表示未分配，对照 libs/storage/Tsavorite/cs/src/core/Epochs/LightEpoch.TestHooks.cs:TestHookThisThreadEntry）
  #[inline]
  #[doc(hidden)]
  pub fn test_hook_this_thread_entry(&self) -> usize {
    get_thread_entry(self.id)
  }

  /// 获取当前线程公布的纪元号（0 表示未保护，对照 libs/storage/Tsavorite/cs/src/core/Epochs/LightEpoch.TestHooks.cs:TestHookThisThreadAnnouncedEpoch）
  #[inline]
  #[doc(hidden)]
  pub fn test_hook_this_thread_announced_epoch(&self) -> u64 {
    self.slot_field(get_thread_entry(self.id), EpochEntry::protected_epoch)
  }

  /// 按 1-based 槽位号越界安全地读取条目字段，越界返回 0（test hook 共用）
  #[inline]
  fn slot_field(&self, entry: usize, read: fn(&EpochEntry) -> u64) -> u64 {
    if entry != 0 && entry <= self.entries.len() {
      unsafe { read(self.entries.get_unchecked(entry - 1)) }
    } else {
      0
    }
  }

  /// 获取指定槽位公布的纪元号（1-based，对照 libs/storage/Tsavorite/cs/src/core/Epochs/LightEpoch.TestHooks.cs:TestHookAnnouncedEpochAt）
  #[inline]
  #[doc(hidden)]
  pub fn test_hook_announced_epoch_at(&self, entry: usize) -> u64 {
    self.slot_field(entry, EpochEntry::protected_epoch)
  }

  /// 获取指定槽位绑定的线程 ID（1-based，对照 libs/storage/Tsavorite/cs/src/core/Epochs/LightEpoch.TestHooks.cs:TestHookThreadIdAt）
  #[inline]
  #[doc(hidden)]
  pub fn test_hook_thread_id_at(&self, entry: usize) -> u64 {
    self.slot_field(entry, EpochEntry::thread_id)
  }

  /// 延迟清理列表总容量（C# 为静态属性，本口同为关联函数，对照
  /// libs/storage/Tsavorite/cs/src/core/Epochs/LightEpoch.TestHooks.cs:TestHookDrainListCapacity）
  #[inline]
  #[doc(hidden)]
  pub fn test_hook_drain_list_capacity() -> usize {
    DRAIN_LIST_SIZE
  }

  /// 活动 LightEpoch 实例数
  ///
  /// 对照 libs/storage/Tsavorite/cs/src/core/Epochs/LightEpoch.cs:ActiveInstanceCount
  /// 对照 libs/client/LightEpoch.cs:ActiveInstanceCount
  #[inline]
  #[doc(hidden)]
  pub fn active_instance_count() -> usize {
    ACTIVE_INSTANCES.load(Ordering::Relaxed)
  }

  /// 重置所有实例计数状态，用于测试环境
  ///
  /// 对照 libs/storage/Tsavorite/cs/src/core/Epochs/LightEpoch.cs:ResetAllInstances
  /// 对照 libs/client/LightEpoch.cs:ResetAllInstances
  #[inline]
  #[doc(hidden)]
  pub fn reset_all_instances() {
    ACTIVE_INSTANCES.store(0, Ordering::Relaxed);
  }

  /// 若当前线程处于保护区则退出并返回 true，否则返回 false（对照 libs/client/LightEpoch.cs:TrySuspend）
  ///
  /// 对照 libs/storage/Tsavorite/cs/src/core/Epochs/LightEpoch.cs:TrySuspend
  /// 对照 libs/storage/Tsavorite/cs/src/core/Epochs/IEpochAccessor.cs:TrySuspend
  #[doc(hidden)]
  pub fn try_suspend(&self) -> bool {
    if self.this_instance_protected() {
      self.suspend();
      true
    } else {
      false
    }
  }

  /// 刷新当前线程在条目表中公布的纪元至全局最新值，并触发就绪的延迟动作（对照 libs/client/LightEpoch.cs:ProtectAndDrain）
  /// libs/storage/Tsavorite/cs/src/core/Epochs/LightEpoch.cs:ProtectAndDrain
  #[doc(hidden)]
  pub fn protect_and_drain(&self) {
    let tid = current_thread_id();
    let Some(entry) = self.tls_protected_entry(tid) else {
      debug_assert!(false, "试图刷新未受保护的纪元");
      return;
    };

    // 刷新公布纪元至 CurrentEpoch：长期不刷新的持有者会拖延全局回收进度
    let current = self.current_epoch();
    entry.refresh_epoch(current);
    self.drain_if_pending();
  }

  /// 获取本地缓存的最低安全回收纪元
  ///
  /// 对照 C# 内部字段 SafeToReclaimEpoch（libs/storage/Tsavorite/cs/src/core/Epochs/LightEpoch.cs）
  #[inline]
  #[doc(hidden)]
  pub fn safe_to_reclaim_epoch(&self) -> u64 {
    self.safe_to_reclaim_epoch.load(Ordering::Acquire)
  }

  /// 获取条目表容量（对照 libs/storage/Tsavorite/cs/src/core/Epochs/LightEpoch.cs:EntryCount）
  #[inline]
  #[doc(hidden)]
  pub fn entry_count(&self) -> usize {
    self.entries.len()
  }
}

impl Drop for LightEpoch {
  fn drop(&mut self) {
    // 宿主消亡终态收割（强环孤岛自愈的收口半，与注册面所有权契约配对，见
    // `Self::bump_current_epoch_action` 文档）：遍历延迟清理列表，凡已发布槽位
    // （epoch != FREE）一律 take 出动作并无条件执行——不判 safe_to_reclaim。
    // Drop 为 &mut 独占上下文：他线程处于本实例任何方法都须持本 Arc 的可活
    // 借用，引用计数归零时结构上不可能有并发访问者；CLAIMING 瞬态仅存在于
    // 注册/收割线程的 CAS 临界区内，同因不可达。环断（弱引用捕获）后此析构
    // 收割才可达，使「从库换库等实例消亡形态下滞留的 unlink/close 动作」在
    // 宿主终态全部落地，unlink 永不到场的孤岛自愈闭环
    for entry in self.drain_list.iter_mut() {
      if entry.epoch.load(Ordering::Acquire) == DRAIN_ENTRY_FREE {
        continue;
      }
      // 镜像 drain 协议（先减计数、后发布 FREE、再执行），维持槽位状态与
      // drain_count 的严格同步变化语义
      self.drain_count.fetch_sub(1, Ordering::AcqRel);
      entry.epoch.store(DRAIN_ENTRY_FREE, Ordering::Release);
      // SAFETY: &mut 独占上下文，UnsafeCell 经 get_mut 安全取出
      if let Some(act) = entry.action.get_mut().take() {
        act.call();
      }
    }
    // 饱和递减：兼容测试先 reset_all_instances 清零再 Drop 实例的序列，
    // 杜绝 usize 无符号回绕（对照 C# Dispose 释放 InstanceTracker 槽位）
    // libs/storage/Tsavorite/cs/src/core/Epochs/LightEpoch.cs:Dispose
    let _ = ACTIVE_INSTANCES.try_update(Ordering::Relaxed, Ordering::Relaxed, |v| v.checked_sub(1));
  }
}

// 编译期钉死控制面缓存行隔离布局：三个热点字段组各自独占 64 字节缓存行
const _: () = {
  assert!(size_of::<LightEpoch>().is_multiple_of(64));
  assert!(align_of::<LightEpoch>() == 64);
  assert!(offset_of!(LightEpoch, current_epoch) == 8);
  assert!(offset_of!(LightEpoch, safe_to_reclaim_epoch) == 64);
  assert!(offset_of!(LightEpoch, drain_count) == 128);
};

impl fmt::Debug for LightEpoch {
  fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    f.debug_struct("LightEpoch")
      .field("id", &self.id)
      .field("current_epoch", &self.current_epoch.load(Ordering::Relaxed))
      .field(
        "safe_to_reclaim_epoch",
        &self.safe_to_reclaim_epoch.load(Ordering::Relaxed),
      )
      .field("drain_count", &self.drain_count.load(Ordering::Relaxed))
      .field("max_threads", &self.entries.len())
      .finish()
  }
}
