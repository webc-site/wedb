//! 虚拟子日志回放状态（对标 libs/server/AOF/ReadConsistency/
//! VirtualSublogReplayState.cs:VirtualSublogReplayState）
//!
//! 每虚拟子日志一份：key → 序列号草图（2^15 槽，槽 = hash >> 32 掩码）、
//! 子日志最大前沿、按目标序列号升序的等待队列。写侧（回放线程）单调推进，
//! 读侧（一致读）等待前沿越过会话序列号。

use std::{
  hint::spin_loop,
  sync::{
    Arc,
    atomic::{AtomicI64, Ordering},
  },
  time::{Duration, Instant},
};

use parking_lot::{Condvar, Mutex};

/// 草图槽数（2 的幂；C# SketchSlotSize = 1 << 15）。
const SKETCH_SLOT_SIZE: usize = 1 << 15;

/// 草图槽掩码。
const SKETCH_SLOT_MASK: usize = SKETCH_SLOT_SIZE - 1;

/// 自旋上限：超过后落入等待队列（C# MaxSpinCount = 64）。
const MAX_SPIN_COUNT: u32 = 64;

/// 可复用等待节点（C# ReadSessionWaiter：ManualResetEventSlim 形态）。
#[derive(Default)]
pub struct ReadSessionWaiter {
  /// 目标序列号（wait 装载）。
  target: AtomicI64,
  /// 唤醒对：signaled 标志 + 条件变量。
  pair: Mutex<bool>,
  /// 唤醒信号。
  signal: Condvar,
}

impl ReadSessionWaiter {
  /// 新建空节点。
  pub fn new() -> Self {
    Self::default()
  }

  /// 装载目标并清除陈旧信号（C# Reset；仅属主会话线程触碰）。
  fn reset(&self, target: i64) {
    self.target.store(target, Ordering::Release);
    *self.pair.lock() = false;
  }

  /// 阻塞至 signaled 或超时；返回 false 表示超时。
  fn wait(&self, timeout: Duration) -> bool {
    let mut signaled = self.pair.lock();
    let deadline = Instant::now() + timeout;
    while !*signaled {
      let Some(remain) = deadline.checked_duration_since(Instant::now()) else {
        return false;
      };
      self.signal.wait_for(&mut signaled, remain);
    }
    true
  }
}

/// 虚拟子日志回放状态。
pub struct VirtualSublogReplayState {
  /// key → 序列号草图（C# sketch）。
  sketch: Box<[AtomicI64]>,
  /// 子日志前沿（C# sublogReplayMetadata.Frontier）。
  frontier: AtomicI64,
  /// 队首等待者的最小目标（C# MinSequenceNumberTarget）。
  min_waiter_target: AtomicI64,
  /// 下一漂移检查窗口下界（属主回放线程私有；C# NextDriftCheckWindow…）。
  next_drift_check_window_lower_bound: AtomicI64,
  /// 等待队列（按目标升序；C# waiterHead 链表的有序 Vec 形态）。
  waiters: Mutex<Vec<Arc<ReadSessionWaiter>>>,
}

impl VirtualSublogReplayState {
  /// 构造：`next_drift_check_seed` 为首次漂移扫描窗口下界（i64::MAX = 关闭）。
  pub fn new(next_drift_check_seed: i64) -> Self {
    Self {
      sketch: (0..SKETCH_SLOT_SIZE).map(|_| AtomicI64::new(0)).collect(),
      frontier: AtomicI64::new(0),
      min_waiter_target: AtomicI64::new(i64::MAX),
      next_drift_check_window_lower_bound: AtomicI64::new(next_drift_check_seed),
      waiters: Mutex::new(Vec::new()),
    }
  }

  /// 子日志最大前沿（C# Max / MaxRef）。
  #[inline]
  pub fn max(&self) -> i64 {
    self.frontier.load(Ordering::Acquire)
  }

  /// 下一漂移检查窗口下界（属主线程专用）。
  #[inline]
  pub fn next_drift_check_window_lower_bound(&self) -> i64 {
    self
      .next_drift_check_window_lower_bound
      .load(Ordering::Relaxed)
  }

  /// 推进下一漂移检查窗口下界（属主线程专用）。
  #[inline]
  pub fn set_next_drift_check_window_lower_bound(&self, value: i64) {
    self
      .next_drift_check_window_lower_bound
      .store(value, Ordering::Relaxed);
  }

  /// 草图槽下标（C# GetSketchSlot）。
  #[inline]
  pub const fn get_sketch_slot(hash: i64) -> usize {
    ((hash as u64 >> 32) as usize) & SKETCH_SLOT_MASK
  }

  /// 前沿序列号：max(草图谱值, 子日志前沿)（C# GetFrontierSequenceNumber）。
  #[inline]
  pub fn get_frontier_sequence_number(&self, hash: i64) -> i64 {
    let slot = self.sketch[Self::get_sketch_slot(hash)].load(Ordering::Acquire);
    slot.max(self.frontier.load(Ordering::Acquire))
  }

  /// key 序列号（C# GetKeySequenceNumber）。
  #[inline]
  pub fn get_key_sequence_number(&self, hash: i64) -> i64 {
    self.sketch[Self::get_sketch_slot(hash)].load(Ordering::Acquire)
  }

  /// 草图谱预取（C# PrefetchKeySequenceNumber 的 Sse.Prefetch0；rust 侧为
  /// 无操作提示位：语义仅为热身缓存，无正确性影响）。
  #[inline]
  pub fn prefetch_key_sequence_number(&self, hash: i64) {
    let _ = hash;
  }

  /// 推进子日志最大序列号（单调；C# UpdateMaxSequenceNumber）。
  pub fn update_max_sequence_number(&self, sequence_number: i64) {
    self.frontier.fetch_max(sequence_number, Ordering::AcqRel);
    self.signal_waiters();
  }

  /// 推进 key 序列号草图（单调；C# UpdateKeySequenceNumber）。
  pub fn update_key_sequence_number(&self, hash: i64, sequence_number: i64) {
    let slot = &self.sketch[Self::get_sketch_slot(hash)];
    slot.fetch_max(sequence_number, Ordering::AcqRel);
    self.signal_waiters();
  }

  /// 唤醒目标已达成的等待者（C# SignalWaiters；自队首起摘除）。
  fn signal_waiters(&self) {
    if self.frontier.load(Ordering::Acquire) <= self.min_waiter_target.load(Ordering::Acquire) {
      return;
    }
    let mut waiters = self.waiters.lock();
    let current_max = self.frontier.load(Ordering::Acquire);
    let mut satisfied = 0;
    while satisfied < waiters.len()
      && waiters[satisfied].target.load(Ordering::Acquire) < current_max
    {
      let node = &waiters[satisfied];
      *node.pair.lock() = true;
      node.signal.notify_all();
      satisfied += 1;
    }
    if satisfied > 0 {
      waiters.drain(..satisfied);
      self.update_min_waiter_target_locked(&waiters);
    }
  }

  /// 等待子日志前沿越过会话最大序列号（C# WaitForSequenceNumber）。
  ///
  /// 自旋快路径 → 装载可复用节点入队 → 条件等待；超时返回 false
  ///（C# 抛 TimeoutException，rust 由调用方按一致读超时处置）。
  pub fn wait_for_sequence_number(
    &self,
    maximum_session_sequence_number: i64,
    waiter: &Arc<ReadSessionWaiter>,
    timeout: Duration,
  ) -> bool {
    // 阶段一：自旋快路径（回放跟得上时零阻塞通过）
    for _ in 0..MAX_SPIN_COUNT {
      if maximum_session_sequence_number < self.frontier.load(Ordering::Acquire) {
        return true;
      }
      spin_loop();
    }

    // 阶段二：装载节点并阻塞
    waiter.reset(maximum_session_sequence_number);
    {
      let mut waiters = self.waiters.lock();
      if maximum_session_sequence_number < self.frontier.load(Ordering::Acquire) {
        return true;
      }
      // 先插入再复查：若更新方在本插入前刚扫过空队列，此处兜底摘除
      self.insert_waiter(&mut waiters, Arc::clone(waiter));
      self.update_min_waiter_target_locked(&waiters);
      if maximum_session_sequence_number < self.frontier.load(Ordering::Acquire) {
        self.remove_waiter_locked(&mut waiters, waiter);
        self.update_min_waiter_target_locked(&waiters);
        return true;
      }
    }
    waiter.wait(timeout)
  }

  /// 升序插入等待节点（C# InsertWaiter；须持锁）。
  fn insert_waiter(&self, waiters: &mut Vec<Arc<ReadSessionWaiter>>, node: Arc<ReadSessionWaiter>) {
    let target = node.target.load(Ordering::Acquire);
    let idx = waiters.partition_point(|w| w.target.load(Ordering::Acquire) <= target);
    waiters.insert(idx, node);
  }

  /// 摘除节点（C# RemoveWaiter 内核；须持锁）。
  fn remove_waiter_locked(
    &self,
    waiters: &mut Vec<Arc<ReadSessionWaiter>>,
    node: &Arc<ReadSessionWaiter>,
  ) {
    if let Some(idx) = waiters.iter().position(|w| Arc::ptr_eq(w, node)) {
      waiters.remove(idx);
    }
  }

  /// 摘除节点（公开形态，供超时/取消方调用；C# RemoveWaiter）。
  pub fn remove_waiter(&self, node: &Arc<ReadSessionWaiter>) {
    let mut waiters = self.waiters.lock();
    self.remove_waiter_locked(&mut waiters, node);
    self.update_min_waiter_target_locked(&waiters);
  }

  /// 刷新最小等待目标（C# UpdateMinWaiterTarget；须持锁）。
  fn update_min_waiter_target_locked(&self, waiters: &[Arc<ReadSessionWaiter>]) {
    let min = waiters
      .first()
      .map_or(i64::MAX, |w| w.target.load(Ordering::Acquire));
    self.min_waiter_target.store(min, Ordering::Release);
  }

  /// 刷新最小等待目标（公开形态）。
  pub fn update_min_waiter_target(&self) {
    let waiters = self.waiters.lock();
    self.update_min_waiter_target_locked(&waiters);
  }
}

#[cfg(test)]
mod tests {
  use std::thread;

  use super::*;

  #[test]
  fn sketch_slot_is_high_word_masked() {
    let hash = 0x1234_5678_9abc_def0_i64;
    assert_eq!(
      VirtualSublogReplayState::get_sketch_slot(hash),
      (hash as u64 >> 32) as usize & SKETCH_SLOT_MASK
    );
    assert!(VirtualSublogReplayState::get_sketch_slot(hash) < SKETCH_SLOT_SIZE);
  }

  #[test]
  fn monotonic_updates_and_frontier() {
    let state = VirtualSublogReplayState::new(i64::MAX);
    state.update_max_sequence_number(10);
    state.update_max_sequence_number(5);
    assert_eq!(state.max(), 10);

    state.update_key_sequence_number(0x1234, 7);
    assert_eq!(state.get_key_sequence_number(0x1234), 7);
    // 前沿 = max(草图谱值, 子日志前沿)
    assert_eq!(state.get_frontier_sequence_number(0x1234), 10);
    assert_eq!(state.get_frontier_sequence_number(0x9999), 10);
  }

  #[test]
  fn waiter_signaled_when_frontier_passes_target() {
    let state = VirtualSublogReplayState::new(i64::MAX);
    let waiter = Arc::new(ReadSessionWaiter::new());
    state.update_max_sequence_number(5);

    // 前沿已过目标：自旋快路径立即通过
    assert!(state.wait_for_sequence_number(4, &waiter, Duration::from_millis(1)));

    // 目标高于前沿：入队等待，由推进方唤醒
    let state2 = Arc::new(VirtualSublogReplayState::new(i64::MAX));
    let waiter2 = Arc::new(ReadSessionWaiter::new());
    let state3 = Arc::clone(&state2);
    let waiter3 = Arc::clone(&waiter2);
    let handle =
      thread::spawn(move || state3.wait_for_sequence_number(20, &waiter3, Duration::from_secs(5)));
    thread::sleep(Duration::from_millis(10));
    state2.update_max_sequence_number(21);
    assert!(handle.join().unwrap());
  }

  #[test]
  fn wait_times_out_when_replay_lags() {
    let state = VirtualSublogReplayState::new(i64::MAX);
    let waiter = Arc::new(ReadSessionWaiter::new());
    assert!(!state.wait_for_sequence_number(100, &waiter, Duration::from_millis(20)));
    // 超时节点应可摘除
    state.remove_waiter(&waiter);
    assert!(state.waiters.lock().is_empty());
  }

  #[test]
  fn min_waiter_target_tracks_head() {
    let state = VirtualSublogReplayState::new(i64::MAX);
    assert_eq!(state.min_waiter_target.load(Ordering::Acquire), i64::MAX);
    let w = Arc::new(ReadSessionWaiter::new());
    w.reset(42);
    {
      let mut waiters = state.waiters.lock();
      state.insert_waiter(&mut waiters, Arc::clone(&w));
      state.update_min_waiter_target_locked(&waiters);
    }
    assert_eq!(state.min_waiter_target.load(Ordering::Acquire), 42);
    state.update_max_sequence_number(43);
    // 达成后队列清空，目标回到 MAX
    assert!(state.waiters.lock().is_empty());
    assert_eq!(state.min_waiter_target.load(Ordering::Acquire), i64::MAX);
  }
}
