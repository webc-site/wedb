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
  sync::{
    Arc,
    atomic::{AtomicI64, AtomicU32, AtomicU64, Ordering, fence},
  },
  thread::yield_now,
};

use log::{debug, trace};
use wbase::{backoff::Backoff, thread::current_thread_id};
use whasher::mix_thread_id;

use crate::{
  EpochEntry, Error, MAX_USER_WORDS, Participant, ProtectedScope, Result,
  tls::{
    FAST_ENTRY, FAST_PARTICIPANT, cached_slot, clear_thread_entry, get_thread_entry,
    note_participant_slot, set_thread_entry,
  },
};

/// 延迟清理动作队列容量（对照 C# Tsavorite kDrainListSize = 16）
pub const DRAIN_LIST_SIZE: usize = 16;

/// 延迟清理槽位空闲标记值（u64::MAX）
const DRAIN_ENTRY_FREE: u64 = u64::MAX;
/// 延迟清理槽位独占抢占/执行标记值（u64::MAX - 1，对照 C# LightEpoch 内部 CAS 状态机）
const DRAIN_ENTRY_CLAIMING: u64 = u64::MAX - 1;

/// 待执行的纪元延迟清理动作项（1:1 对标 C# EpochActionPair，由 epoch CAS 状态机保证独占互斥）
///
/// 64 字节 Cacheline 对齐：多线程并发 CAS 各槽位 `epoch` 时互不串扰缓存行，
/// 消除 C# 原版（16 字节/槽，4 槽共享一行）存在的伪共享。
#[repr(align(64))]
struct DrainEntry {
  epoch: AtomicU64,
  action: UnsafeCell<Option<Box<dyn FnOnce() + Send + 'static>>>,
}

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
static ACTIVE_INSTANCES: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);

/// Microsoft Garnet Tsavorite 架构风格的 LightEpoch 纪元保护管理器
///
/// 采用无锁 (Latch-free) 惰性同步机制，管理并发读写事务的纪元生命周期与安全回收判定。
/// `repr(C, align(64))` + 显式缓存行填充：current_epoch（写热点）/ safe_to_reclaim_epoch
/// （读热点）/ drain_count + user_word_mask 各占独立缓存行，杜绝控制面伪共享。
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
  /// 用户字已占用槽位掩码（CAS 原子分配与回收，对照 libs/client/LightEpoch.cs:userWordMask）
  pub user_word_mask: AtomicU32,
  _pad2: [u8; 56],
  /// 参与者条目表（每个元素独占 64 字节 Cacheline）
  pub entries: Arc<[EpochEntry]>,
  /// 延迟回收动作列表（固定 16 个槽位，槽位间 64 字节隔离）
  drain_list: Box<[DrainEntry; DRAIN_LIST_SIZE]>,
}

impl LightEpoch {
  /// 默认最大线程/参与者容量
  pub const DEFAULT_MAX_THREADS: usize = 128;

  /// 创建指定最大容量的 LightEpoch 实例
  pub fn new(max_threads: usize) -> Self {
    let max_threads = max_threads.max(1);
    let entries: Arc<[EpochEntry]> = repeat_with(EpochEntry::new).take(max_threads).collect();

    ACTIVE_INSTANCES.fetch_add(1, Ordering::Relaxed);
    Self {
      id: NEXT_INSTANCE_ID.fetch_add(1, Ordering::Relaxed),
      current_epoch: AtomicU64::new(1),
      _pad0: [0; 48],
      safe_to_reclaim_epoch: AtomicU64::new(0),
      _pad1: [0; 56],
      drain_count: AtomicU32::new(0),
      user_word_mask: AtomicU32::new(0),
      _pad2: [0; 56],
      entries,
      drain_list: Box::new(from_fn(|_| DrainEntry::new())),
    }
  }

  /// 活动 LightEpoch 实例数
  ///
  /// 对照 libs/storage/Tsavorite/cs/src/core/Epochs/LightEpoch.cs:ActiveInstanceCount
  /// 对照 libs/client/LightEpoch.cs:ActiveInstanceCount
  #[inline]
  pub fn active_instance_count() -> usize {
    ACTIVE_INSTANCES.load(Ordering::Relaxed)
  }

  /// 重置所有实例计数状态，用于测试环境
  ///
  /// 对照 libs/storage/Tsavorite/cs/src/core/Epochs/LightEpoch.cs:ResetAllInstances
  /// 对照 libs/client/LightEpoch.cs:ResetAllInstances
  #[inline]
  pub fn reset_all_instances() {
    ACTIVE_INSTANCES.store(0, Ordering::Relaxed);
  }

  /// 注册当前线程或会话为参与者
  ///
  /// 扫描条目表并尝试 CAS 抢占空闲槽位。成功后返回独占该槽位的 `Participant`。
  pub fn register(self: &Arc<Self>) -> Result<Participant> {
    for (idx, entry) in self.entries.iter().enumerate() {
      if entry.try_reserve() {
        trace!("成功注册参与者，分配条目索引: {idx}");
        note_participant_slot(self.id, idx, entry);
        return Ok(Participant::new(Arc::clone(self), idx));
      }
    }
    Err(Error::ExceededMaxThreads(self.entries.len()))
  }

  /// 当前线程在本实例已登记的活动槽位下标（0-based）；未登记返回 None
  #[inline]
  fn active_idx(&self) -> Option<usize> {
    let entry = get_thread_entry(self.id);
    (entry != 0 && entry <= self.entries.len()).then(|| entry - 1)
  }

  /// 定位本线程经 TLS 机制（`resume`/`protected_scope`）持有的受保护条目
  ///
  /// 单槽快速缓存优先（O(1)，免去 RefCell 借用与线性 find），未命中回退本实例
  /// TLS 登记槽；均校验线程 ID 属主与保护态
  #[inline]
  fn tls_protected_entry(&self, tid: u64) -> Option<&EpochEntry> {
    let fe = FAST_ENTRY.get();
    if fe.instance_id == self.id && fe.slot != 0 {
      // SAFETY: 实例 ID 全局单调不复用，与存活 self 匹配即保证 entries Arc 存活，指针不悬垂
      let entry = unsafe { &*fe.ptr };
      if entry.thread_id() == tid && entry.is_protected() {
        return Some(entry);
      }
    }
    let idx = self.active_idx()?;
    let entry = unsafe {
      // SAFETY: active_idx 已保证 idx < entries.len()
      self.entries.get_unchecked(idx)
    };
    (entry.thread_id() == tid && entry.is_protected()).then_some(entry)
  }

  /// 有 pending 延迟动作时协助收割（不刷新本线程公布纪元，重入路径专用）
  #[inline]
  pub(crate) fn drain_if_pending(&self) {
    if self.drain_count.load(Ordering::Acquire) > 0 {
      self.drain();
    }
  }

  /// 有 pending 延迟动作时刷新本线程公布纪元并协助收割（对照 C# ProtectAndDrain）
  #[inline]
  fn help_drain_if_pending(&self) {
    if self.drain_count.load(Ordering::Acquire) > 0 {
      self.help_drain();
    }
  }

  /// 尝试为本线程 CAS 抢占下标 `idx` 槽位；成功则完成 TLS 登记并按需协助收割
  ///
  /// # Safety 前置条件
  /// 调用方须保证 `idx < self.entries.len()`
  #[inline]
  fn claim_entry(&self, idx: usize, tid: u64) -> bool {
    let entry = unsafe {
      // SAFETY: 调用方保证 idx < len
      self.entries.get_unchecked(idx)
    };
    if !entry.try_claim(tid, &self.current_epoch) {
      return false;
    }
    set_thread_entry(self.id, idx + 1, || Arc::downgrade(&self.entries));
    // 对照 C# Acquire 尾部：仅在有 pending 延迟动作时才收割，
    // 避免热路径上无谓的刷新 store 与保护态检查；
    // 此处刚以现场读取的最新纪元发布槽位，无旧纪元读取在途，refresh 安全
    self.help_drain_if_pending();
    true
  }

  /// 当前线程进入受保护的纪元区（对照 libs/client/LightEpoch.cs:Resume）
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
  pub fn resume(&self) {
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
    if self.drain_count.load(Ordering::Acquire) > 0 {
      self.suspend_drain();
    }
  }

  /// 若当前线程处于保护区则退出并返回 true，否则返回 false（对照 libs/client/LightEpoch.cs:TrySuspend）
  pub fn try_suspend(&self) -> bool {
    if self.this_instance_protected() {
      self.suspend();
      true
    } else {
      false
    }
  }

  /// 若当前线程尚未受保护则进入并返回 true，否则返回 false（对照 libs/client/LightEpoch.cs:ResumeIfNotProtected）
  pub fn resume_if_not_protected(&self) -> bool {
    if self.this_instance_protected() {
      false
    } else {
      self.resume();
      true
    }
  }

  /// 检查当前线程在此 LightEpoch 实例中是否正处于保护区（对照 libs/client/LightEpoch.cs:ThisInstanceProtected）
  ///
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
    let fp = FAST_PARTICIPANT.get();
    if fp.instance_id == self.id && fp.slot != 0 {
      // SAFETY: 实例 ID 全局单调不复用，与存活 self 匹配即保证 entries Arc 存活，指针不悬垂
      let entry = unsafe { &*fp.ptr };
      if entry.thread_id() == tid && entry.is_protected() {
        return Some(entry);
      }
    }
    self
      .entries
      .iter()
      .find(|entry| entry.is_protected() && entry.thread_id() == tid)
  }

  /// 当前线程先挂起再重新恢复保护，赋予其他等待线程调度机会（对照 libs/client/LightEpoch.cs:SuspendResume）
  pub fn suspend_resume(&self) {
    self.suspend();
    self.resume();
  }

  /// 刷新当前线程在条目表中公布的纪元至全局最新值，并触发就绪的延迟动作（对照 libs/client/LightEpoch.cs:ProtectAndDrain）
  pub fn protect_and_drain(&self) {
    let Some(idx) = self.active_idx() else {
      debug_assert!(false, "试图刷新未受保护的纪元");
      return;
    };
    let entry = unsafe { self.entries.get_unchecked(idx) };
    debug_assert!(
      entry.thread_id() == current_thread_id() && entry.is_protected(),
      "试图刷新未受保护的纪元"
    );

    // 刷新公布纪元至 CurrentEpoch：长期不刷新的持有者会拖延全局回收进度
    let current = self.current_epoch();
    entry.refresh_epoch(current);
    self.drain_if_pending();
  }

  /// 获取基于 RAII 作用域自动管理生命周期的保护守卫（对照 libs/client/LightEpoch.cs:ProtectedScope）
  pub fn protected_scope(&self) -> ProtectedScope<'_> {
    ProtectedScope::new(self)
  }

  /// 递增全局当前纪元并尝试触发安全回收（对照 libs/client/LightEpoch.cs:BumpCurrentEpoch）
  ///
  /// 刻意差异：C# 版 Debug.Assert 要求调用线程必须处于保护区，此处放宽为任意线程
  /// 可推进（无 panic 约束），保护态仅作为上游使用约定而非本层强制。
  pub fn bump_current_epoch(&self) -> u64 {
    let new_epoch = self.current_epoch.fetch_add(1, Ordering::AcqRel) + 1;
    trace!("递增全局纪元至: {new_epoch}");
    if self.drain_count.load(Ordering::Acquire) > 0 {
      self.drain();
    } else {
      self.compute_safe_to_reclaim_epoch();
    }
    new_epoch
  }

  /// 递增全局纪元的简洁别名（wedb_hlog / wedb_store 下游在用）
  #[inline]
  pub fn bump_epoch(&self) -> u64 {
    self.bump_current_epoch()
  }

  /// 以本线程所能尽力推进延迟清理：全量刷新本线程以任一机制持有的保护条目至
  /// 全局最新纪元，再收割就绪延迟动作
  ///
  /// 对照 libs/client/LightEpoch.cs:ProtectAndDrain：刷新本线程公布纪元以解除对旧纪元的自钉，
  /// 再收割就绪延迟动作。C# 单一保护机制下刷新 entry 即完整覆盖；Rust 存在 TLS
  /// 作用域与 `Participant` 显式句柄双轨保护，同一线程可能同时以两条机制持有多条
  /// 保护条目（如 TLS 短临界区嵌套长期 Participant 会话守卫），必须全量刷新——
  /// 若漏看任一旧纪元条目，本线程将自钉 safe_to_reclaim 推进，16 槽 drain_list
  /// 耗尽后 `bump_current_epoch_action` 的注册路径永久自旋（活锁）。仅慢路径进入
  /// （drain_count > 0），O(N) 表扫描不伤热路径。
  ///
  /// 安全性：刷新语义与 C# ProtectAndDrain 一致——调用方约定不在跨刷新窗口持有
  /// 旧纪元裸指针（wedb 同步批处理 API 的闭包均在单次调用内闭环消费，满足约定）
  fn help_drain(&self) {
    let current = self.current_epoch.load(Ordering::Acquire);
    let tid = current_thread_id();
    for entry in self.entries.iter() {
      if entry.is_protected() && entry.thread_id() == tid {
        entry.refresh_epoch(current);
      }
    }
    self.drain();
  }

  /// 递增全局纪元并将关联动作注册到前置纪元，等待前置纪元安全回收时执行（对照 libs/client/LightEpoch.cs:BumpCurrentEpoch(Action)）
  pub fn bump_current_epoch_action<F>(&self, on_drain: F)
  where
    F: FnOnce() + Send + 'static,
  {
    let prior_epoch = self.bump_current_epoch() - 1;
    let mut action_opt = Some(Box::new(on_drain) as Box<dyn FnOnce() + Send + 'static>);

    'outer: loop {
      for entry in self.drain_list.iter() {
        let curr_epoch = entry.epoch.load(Ordering::Acquire);
        // 单一 CAS 闭环：FREE 槽位直接抢占；已发布槽位须达安全纪元方可回收替换。
        // 哨兵 FREE/CLAIMING 大于任何真实安全纪元，被同一比较自然排除，杜绝 ABA
        if (curr_epoch == DRAIN_ENTRY_FREE
          || curr_epoch <= self.safe_to_reclaim_epoch.load(Ordering::Acquire))
          && entry
            .epoch
            .compare_exchange(
              curr_epoch,
              DRAIN_ENTRY_CLAIMING,
              Ordering::AcqRel,
              Ordering::Acquire,
            )
            .is_ok()
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
            act();
          }
          break 'outer;
        }
      }

      // 列表满且无可回收槽位：以本线程所能尽力推进收割，再让出调度权
      self.help_drain();
      yield_now();
    }

    self.help_drain();
  }

  /// 获取当前全局纪元号
  #[inline]
  pub fn current_epoch(&self) -> u64 {
    self.current_epoch.load(Ordering::Acquire)
  }

  /// 获取本地缓存的最低安全回收纪元
  #[inline]
  pub fn safe_to_reclaim_epoch(&self) -> u64 {
    self.safe_to_reclaim_epoch.load(Ordering::Acquire)
  }

  /// 扫描所有活跃 entries 找出全局最小保护纪元，并单调更新缓存
  pub fn compute_safe_to_reclaim_epoch(&self) -> u64 {
    let curr = self.current_epoch.load(Ordering::Acquire);
    let mut oldest = curr;

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
      self.safe_to_reclaim_epoch.fetch_max(safe, Ordering::AcqRel);
    }
    prev.max(safe)
  }

  /// 原子抢占一个已就绪（trigger_epoch ≤ safe_epoch）的延迟动作槽位
  ///
  /// 哨兵 FREE/CLAIMING 大于任何真实安全纪元，被同一比较自然排除，杜绝 ABA；
  /// CAS 成功即取得槽位独占消费权
  #[inline]
  fn try_claim_ready_slot(entry: &DrainEntry, safe_epoch: u64) -> bool {
    let trigger_epoch = entry.epoch.load(Ordering::Acquire);
    trigger_epoch <= safe_epoch
      && entry
        .epoch
        .compare_exchange(
          trigger_epoch,
          DRAIN_ENTRY_CLAIMING,
          Ordering::AcqRel,
          Ordering::Acquire,
        )
        .is_ok()
  }

  /// 扫描延迟清理列表并触发所有达到安全回收纪元的动作（对照 libs/client/LightEpoch.cs:Drain）
  ///
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
          act();
        }
        if self.drain_count.load(Ordering::Acquire) == 0 {
          break;
        }
      }
    }
  }

  /// 当最后一个受保护的线程挂起时，代为执行所有未完成的延迟动作（对照 libs/client/LightEpoch.cs:SuspendDrain）
  ///
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
  #[inline]
  pub fn is_safe_to_reclaim(&self, target_epoch: u64) -> bool {
    if target_epoch <= self.safe_to_reclaim_epoch.load(Ordering::Acquire) {
      return true;
    }
    target_epoch <= self.compute_safe_to_reclaim_epoch()
  }

  /// 推进纪元并自旋/yield 等待所有早于或等于该纪元的读事务完全退出 (drain)
  ///
  /// 契约：调用线程不得以 ≤ `target_epoch` 的纪元处于保护区（自身钉住旧纪元将导致活锁），
  /// 与 C# `BumpCurrentEpoch` 要求调用线程受保护的约定同源。
  pub fn bump_and_wait(&self, target_epoch: u64) {
    debug!("开始 bump_and_wait 等待纪元 {target_epoch} 完全 drain");
    // 活锁防护断言（debug 专用）：本线程若以 ≤ target_epoch 的纪元受保护（TLS 或
    // Participant 任一机制），自身将永久钉住目标纪元。线程 ID 全局唯一，按 tid
    // 扫描判定无歧义，对应 C# "BumpCurrentEpoch 必须在受保护线程上调用" 的契约面。
    debug_assert!(
      !self.entries.iter().any(|e| e.is_protected()
        && e.thread_id() == current_thread_id()
        && e.protected_epoch() <= target_epoch),
      "bump_and_wait 活锁：调用线程以 ≤ {target_epoch} 的纪元受保护，须先 suspend/refresh"
    );
    while self.current_epoch.load(Ordering::Acquire) <= target_epoch {
      self.bump_epoch();
    }
    let mut backoff = Backoff::new();
    while !self.is_safe_to_reclaim(target_epoch) {
      self.drain_if_pending();
      backoff.snooze();
    }
    self.drain_if_pending();
    debug!("纪元 {target_epoch} drain 完成");
  }

  /// 是否存在等待排空的纪元操作（对照 Tsavorite Epoch drain 检查）
  #[inline]
  pub fn has_pending_drain(&self) -> bool {
    self.drain_count.load(Ordering::Acquire) > 0
  }

  /// 获取当前线程分配到的条目槽位（1-based，0 表示未分配，对照 libs/client/LightEpoch.cs:TestHookThisThreadEntry）
  #[inline]
  pub fn test_hook_this_thread_entry(&self) -> usize {
    get_thread_entry(self.id)
  }

  /// 获取当前线程公布的纪元号（0 表示未保护，对照 libs/client/LightEpoch.cs:TestHookThisThreadAnnouncedEpoch）
  #[inline]
  pub fn test_hook_this_thread_announced_epoch(&self) -> u64 {
    self
      .active_idx()
      .map(|idx| unsafe { self.entries.get_unchecked(idx).protected_epoch() })
      .unwrap_or(0)
  }

  /// 获取指定槽位公布的纪元号（1-based，对照 libs/client/LightEpoch.cs:TestHookAnnouncedEpochAt）
  #[inline]
  pub fn test_hook_announced_epoch_at(&self, entry: usize) -> u64 {
    if entry == 0 || entry > self.entries.len() {
      0
    } else {
      unsafe { self.entries.get_unchecked(entry - 1).protected_epoch() }
    }
  }

  /// 获取指定槽位绑定的线程 ID（1-based，对照 libs/client/LightEpoch.cs:TestHookThreadIdAt）
  #[inline]
  pub fn test_hook_thread_id_at(&self, entry: usize) -> u64 {
    if entry == 0 || entry > self.entries.len() {
      0
    } else {
      unsafe { self.entries.get_unchecked(entry - 1).thread_id() }
    }
  }

  /// 获取延迟清理列表总容量（对照 libs/client/LightEpoch.cs:TestHookDrainListCapacity）
  #[inline]
  pub fn test_hook_drain_list_capacity(&self) -> usize {
    DRAIN_LIST_SIZE
  }

  /// 获取条目表容量（对照 libs/client/LightEpoch.cs:EntryCount）
  #[inline]
  pub fn entry_count(&self) -> usize {
    self.entries.len()
  }

  /// 获取支持的最大用户字槽位数量（对照 libs/client/LightEpoch.cs:MaxUserWords）
  #[inline]
  pub fn test_hook_max_user_words(&self) -> usize {
    MAX_USER_WORDS
  }

  /// 分配一个全局用户字槽位并将其初始化为 initial_value（对照 libs/client/LightEpoch.cs:AllocateUserWord）
  pub fn allocate_user_word(&self, initial_value: i64) -> Result<usize> {
    loop {
      let mask = self.user_word_mask.load(Ordering::Acquire);
      let idx = (!mask).trailing_zeros() as usize;
      if idx >= MAX_USER_WORDS {
        return Err(Error::ExceededMaxUserWords(MAX_USER_WORDS));
      }
      let new_mask = mask | (1 << idx);
      if self
        .user_word_mask
        .compare_exchange_weak(mask, new_mask, Ordering::AcqRel, Ordering::Acquire)
        .is_err()
      {
        continue;
      }
      // 成功获得该槽位独占权，初始化所有条目的用户字（快速无越界检查路径）
      for entry in self.entries.iter() {
        unsafe { entry.set_user_word_unchecked(idx, initial_value) };
      }
      return Ok(idx);
    }
  }

  /// 释放先前分配的用户字槽位（对照 libs/client/LightEpoch.cs:ReleaseUserWord）
  pub fn release_user_word(&self, word_index: usize) -> Result<()> {
    if word_index >= MAX_USER_WORDS {
      return Err(Error::InvalidUserWordIndex(word_index));
    }
    loop {
      let mask = self.user_word_mask.load(Ordering::Acquire);
      let new_mask = mask & !(1 << word_index);
      if self
        .user_word_mask
        .compare_exchange_weak(mask, new_mask, Ordering::AcqRel, Ordering::Acquire)
        .is_ok()
      {
        return Ok(());
      }
    }
  }

  /// 获取当前线程对应用户字的原子引用（对照 libs/client/LightEpoch.cs:ThisThreadUserWord）
  ///
  /// 须在本线程经 `resume`/`protected_scope` 进入保护区后调用。
  #[inline]
  pub fn this_thread_user_word_atomic(&self, word_index: usize) -> Result<&AtomicI64> {
    if word_index >= MAX_USER_WORDS {
      return Err(Error::InvalidUserWordIndex(word_index));
    }
    let idx = self.active_idx().ok_or(Error::NotProtected)?;
    // 安全性：active_idx() 保证 idx < self.entries.len()，word_index < MAX_USER_WORDS 已校验
    unsafe {
      Ok(
        self
          .entries
          .get_unchecked(idx)
          .user_word_atomic_unchecked(word_index),
      )
    }
  }

  /// 获取当前线程对应的用户字数值（通过 [`Self::this_thread_user_word_atomic`] 读取）
  #[inline]
  pub fn this_thread_user_word(&self, word_index: usize) -> Result<i64> {
    Ok(
      self
        .this_thread_user_word_atomic(word_index)?
        .load(Ordering::Acquire),
    )
  }

  /// 设置当前线程对应的用户字
  #[inline]
  pub fn set_this_thread_user_word(&self, word_index: usize, val: i64) -> Result<()> {
    self
      .this_thread_user_word_atomic(word_index)?
      .store(val, Ordering::Release);
    Ok(())
  }

  /// 扫描所有活跃条目并返回指定用户字的最小值（对照 libs/client/LightEpoch.cs:GetMinUserWord）
  pub fn get_min_user_word(&self, word_index: usize) -> Result<i64> {
    if word_index >= MAX_USER_WORDS {
      return Err(Error::InvalidUserWordIndex(word_index));
    }
    // 安全性：已校验 word_index < MAX_USER_WORDS
    Ok(self.entries.iter().fold(i64::MAX, |min, e| {
      unsafe { e.user_word_unchecked(word_index) }.min(min)
    }))
  }
}

impl Default for LightEpoch {
  fn default() -> Self {
    Self::new(Self::DEFAULT_MAX_THREADS)
  }
}

impl Drop for LightEpoch {
  fn drop(&mut self) {
    ACTIVE_INSTANCES.fetch_sub(1, Ordering::Relaxed);
  }
}

// 编译期钉死控制面缓存行隔离布局：三个热点字段组各自独占 64 字节缓存行
const _: () = {
  assert!(size_of::<LightEpoch>().is_multiple_of(64));
  assert!(align_of::<LightEpoch>() == 64);
  assert!(offset_of!(LightEpoch, current_epoch) == 8);
  assert!(offset_of!(LightEpoch, safe_to_reclaim_epoch) == 64);
  assert!(offset_of!(LightEpoch, drain_count) == 128);
  assert!(offset_of!(LightEpoch, user_word_mask) == 132);
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
