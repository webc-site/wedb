//! 回放对齐栅栏（对标 libs/server/AOF/ReadConsistency/
//! ReplayAlignBarrier.cs:ReplayAlignBarrier）
//!
//! 约束副本侧跨虚拟子日志的回放漂移：观察到大幅漂移的一方（即将阻塞的读
//! 会话，或越过进度门的回放线程）按领先者前沿值开一轮（round）；各回放线程
//! 推进至目标即到达（arrival）并等待，全员到齐后由最后到达者集体放行。
//! 栅栏仅是性能辅助——前缀一致性由读侧等待保证，轮次早/晚/超时放弃均不
//! 影响正确性。到达按虚拟子日志去重。Fast path：无轮次时单次原子读 + 比较。

use std::{
  hint::spin_loop,
  sync::{
    Arc,
    atomic::{AtomicBool, AtomicI32, AtomicI64, Ordering},
  },
  time::{Duration, Instant},
};

use parking_lot::{Condvar, Mutex};

/// 轮次：目标前沿 + 未到达参与者数 + 放行标志。
struct Round {
  /// 目标前沿序列号。
  target: AtomicI64,
  /// 未到达参与者数。
  remaining: AtomicI32,
  /// 权威放行信号（自旋者轮询；C# Round.released）。
  released: AtomicBool,
  /// 放行广播（阻塞等待者）。
  release_pair: Mutex<bool>,
  /// 放行条件变量。
  release_signal: Condvar,
}

impl Round {
  fn new(target: i64, participant_count: i32) -> Self {
    Self {
      target: AtomicI64::new(target),
      remaining: AtomicI32::new(participant_count),
      released: AtomicBool::new(false),
      release_pair: Mutex::new(false),
      release_signal: Condvar::new(),
    }
  }

  /// 放行（C# Round.Release）。
  fn release(&self) {
    self.released.store(true, Ordering::Release);
    *self.release_pair.lock() = true;
    self.release_signal.notify_all();
  }
}

/// 回放对齐栅栏。
pub struct ReplayAlignBarrier {
  /// 参与者数（每虚拟子日志一个）。
  participant_count: i32,
  /// 各参与者最近到达的轮次（去重；C# lastArrivedRound）。
  last_arrived_round: Vec<Mutex<Option<Arc<Round>>>>,
  /// 当前轮次（None = 无；C# currentRound）。
  current_round: Mutex<Option<Arc<Round>>>,
  /// 阻塞等待上限（None = 永等；C# replicaSyncTimeout）。
  replica_sync_timeout: Option<Duration>,
}

impl ReplayAlignBarrier {
  /// 构造（C# ReplayAlignBarrier(participantCount, spinUs, replicaSyncTimeout)；
  /// rust 侧自旋预算并入等待实现，仅保留阻塞上限）。
  pub fn new(participant_count: usize, replica_sync_timeout: Option<Duration>) -> Self {
    Self {
      participant_count: participant_count as i32,
      last_arrived_round: (0..participant_count).map(|_| Mutex::new(None)).collect(),
      current_round: Mutex::new(None),
      replica_sync_timeout,
    }
  }

  /// 是否有轮次进行中（C# InProgress；禁用态的“永不完成轮”也算进行中）。
  pub fn in_progress(&self) -> bool {
    self.current_round.lock().is_some()
  }

  /// libs/server/AOF/ReadConsistency/ReplayAlignBarrier.cs:TryOpenRound
  ///
  /// 观察到大幅漂移时按目标开轮；已有轮次时空操作。
  pub fn try_open_round(&self, target: i64) {
    let mut current = self.current_round.lock();
    if current.is_some() {
      return;
    }
    *current = Some(Arc::new(Round::new(target, self.participant_count)));
  }

  /// libs/server/AOF/ReadConsistency/ReplayAlignBarrier.cs:SignalArrivalAndWait
  ///
  /// 回放线程推进前沿后调用：达标的首次到达者阻塞等待全员到齐。
  pub fn signal_arrival_and_wait(&self, virtual_sublog_idx: usize, frontier: i64) {
    let Some(round) = self.arrive(virtual_sublog_idx, frontier) else {
      return;
    };
    self.wait_for_all_arrivals(&round, virtual_sublog_idx);
  }

  /// libs/server/AOF/ReadConsistency/ReplayAlignBarrier.cs:SignalArrival
  ///
  /// 空闲虚拟子日志的到场计数（不阻塞调用方）。
  pub fn signal_arrival(&self, virtual_sublog_idx: usize, frontier: i64) {
    if let Some(round) = self.arrive(virtual_sublog_idx, frontier) {
      if round.remaining.load(Ordering::Acquire) > 0 {
        return;
      }
      self.release_round(&round);
    }
  }

  /// 到场登记：返回 Some(轮次) 表示本次为该子日志在此轮的首次有效到达。
  fn arrive(&self, virtual_sublog_idx: usize, frontier: i64) -> Option<Arc<Round>> {
    let round = Arc::clone(self.current_round.lock().as_ref()?);
    if frontier < round.target.load(Ordering::Acquire) {
      return None;
    }
    let mut last = self.last_arrived_round[virtual_sublog_idx].lock();
    if last.as_ref().is_some_and(|r| Arc::ptr_eq(r, &round)) {
      return None;
    }
    *last = Some(Arc::clone(&round));
    round.remaining.fetch_sub(1, Ordering::AcqRel);
    Some(round)
  }

  /// libs/server/AOF/ReadConsistency/ReplayAlignBarrier.cs:WaitForAllArrivals
  ///
  /// 最后到达者放行全员；否则阻塞（自旋后转入条件等待，超时放弃对齐）。
  fn wait_for_all_arrivals(&self, round: &Arc<Round>, _virtual_sublog_idx: usize) {
    if round.remaining.load(Ordering::Acquire) <= 0 {
      self.release_round(round);
      return;
    }

    // 自旋阶段（C# spin 预算段；标量轮询放行标志）
    let spin_deadline = Instant::now() + Duration::from_micros(64);
    while Instant::now() < spin_deadline {
      if round.released.load(Ordering::Acquire) {
        return;
      }
      spin_loop();
    }

    // 阻塞阶段：等待权威放行标志（有界超时；超时即放弃本轮对齐，不影响正确性）
    let deadline = self.replica_sync_timeout.map(|t| Instant::now() + t);
    let mut released = round.release_pair.lock();
    while !round.released.load(Ordering::Acquire) {
      let remain = match deadline.and_then(|d| d.checked_duration_since(Instant::now())) {
        Some(remain) => remain.min(Duration::from_millis(100)),
        // 无超时配置：分段睡眠轮询，保持放行响应性
        None => Duration::from_millis(100),
      };
      round.release_signal.wait_for(&mut released, remain);
      if deadline.is_some_and(|d| Instant::now() >= d) {
        return;
      }
    }
  }

  /// libs/server/AOF/ReadConsistency/ReplayAlignBarrier.cs:ReleaseRound
  ///
  /// 放行自旋者与阻塞者；轮次仍为当前轮时摘除。
  fn release_round(&self, round: &Arc<Round>) {
    round.release();
    let mut current = self.current_round.lock();
    if current.as_ref().is_some_and(|r| Arc::ptr_eq(r, round)) {
      *current = None;
    }
  }

  /// 广播放行（C# SignalAll：逐参与者事件置位；rust 单 release 对已覆盖
  /// 全员唤醒，此处为保持调用形态的显式广播点）。
  fn signal_all(&self, round: &Arc<Round>) {
    round.release();
  }

  /// libs/server/AOF/ReadConsistency/ReplayAlignBarrier.cs:Disable
  ///
  /// 以“永不完成轮”占位（目标 i64::MAX、已放行）：拒绝新轮并放行既有等待者，
  /// 防止即将退出的参与者令同侪无限等待。
  pub fn disable(&self) {
    let mut current = self.current_round.lock();
    if let Some(round) = current.take() {
      self.signal_all(&round);
    }
    // 占位轮：永不达成且立即视为已放行
    let inert = Arc::new(Round::new(i64::MAX, i32::MAX));
    inert.release();
    *current = Some(inert);
  }

  /// libs/server/AOF/ReadConsistency/ReplayAlignBarrier.cs:Enable
  ///
  /// 清空占位轮，恢复开轮能力。
  pub fn enable(&self) {
    let mut current = self.current_round.lock();
    if let Some(round) = current.take() {
      self.signal_all(&round);
    }
  }
}

#[cfg(test)]
mod tests {
  use std::thread;

  use super::*;

  #[test]
  fn open_round_and_release_on_all_arrivals() {
    let barrier = ReplayAlignBarrier::new(2, Some(Duration::from_secs(1)));
    assert!(!barrier.in_progress());
    barrier.try_open_round(100);
    assert!(barrier.in_progress());
    // 已有轮次：开轮空操作
    barrier.try_open_round(200);
    assert!(barrier.in_progress());

    // 未达标不计数
    barrier.signal_arrival(0, 50);
    // 达标但重复到达去重：只计一次
    barrier.signal_arrival(0, 150);
    assert!(barrier.in_progress());
    // 第二参与者达标到场 → 全员到齐 → 轮次摘除
    barrier.signal_arrival(1, 150);
    assert!(!barrier.in_progress());
  }

  #[test]
  fn blocking_arrival_released_by_peer() {
    let barrier = Arc::new(ReplayAlignBarrier::new(2, Some(Duration::from_secs(5))));
    barrier.try_open_round(10);

    // 参与者 1 阻塞等待
    let b2 = Arc::clone(&barrier);
    let handle = thread::spawn(move || b2.signal_arrival_and_wait(1, 12));
    thread::sleep(Duration::from_millis(20));
    // 参与者 0 到场触发全员放行
    barrier.signal_arrival_and_wait(0, 12);
    handle.join().unwrap();
    assert!(!barrier.in_progress());
  }

  #[test]
  fn timeout_proceeds_unaligned() {
    let barrier = ReplayAlignBarrier::new(2, Some(Duration::from_millis(30)));
    barrier.try_open_round(10);
    // 仅一方到场：等待超时后直接返回（不卡死）
    let started = Instant::now();
    barrier.signal_arrival_and_wait(0, 12);
    assert!(started.elapsed() >= Duration::from_millis(30));
    assert!(barrier.in_progress(), "轮次未被放行者摘除（仅超时退出）");
  }

  #[test]
  fn disable_rejects_and_enable_restores() {
    let barrier = ReplayAlignBarrier::new(2, None);
    barrier.try_open_round(10);
    barrier.disable();
    assert!(barrier.in_progress());
    // 禁用态开轮被占位轮拒绝
    barrier.try_open_round(20);
    barrier.enable();
    assert!(!barrier.in_progress());
    // 恢复后可正常开轮与放行
    barrier.try_open_round(30);
    barrier.signal_arrival(0, 40);
    barrier.signal_arrival(1, 40);
    assert!(!barrier.in_progress());
  }
}
