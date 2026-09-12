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
    atomic::{AtomicBool, AtomicI32, AtomicI64, AtomicU64, Ordering},
  },
  time::{Duration, Instant},
};

use parking_lot::{Condvar, Mutex};

/// 轮次：唯一ID + 目标前沿 + 未到达参与者数 + 权威放行标志。
struct Round {
  /// 轮次唯一自增 ID（去重判定）。
  id: u64,
  /// 目标前沿序列号。
  target: i64,
  /// 未到达参与者数。
  remaining: AtomicI32,
  /// 权威放行信号（自旋者轮询；C# Round.released）。
  released: AtomicBool,
}

impl Round {
  fn new(id: u64, target: i64, participant_count: i32) -> Self {
    Self {
      id,
      target,
      remaining: AtomicI32::new(participant_count),
      released: AtomicBool::new(false),
    }
  }

  /// 放行（C# Round.Release）。
  #[inline]
  fn release(&self) {
    self.released.store(true, Ordering::Release);
  }
}

/// 每参与者复用唤醒事件（对标 C# participantEvents[virtualSublogIdx]）。
/// 仅属主虚拟子日志回放线程等待与复位，放行者仅置位不复位，零信号遗失风险。
struct ParticipantEvent {
  pair: Mutex<bool>,
  signal: Condvar,
}

impl ParticipantEvent {
  fn new() -> Self {
    Self {
      pair: Mutex::new(false),
      signal: Condvar::new(),
    }
  }

  #[inline]
  fn reset(&self) {
    *self.pair.lock() = false;
  }

  #[inline]
  fn set(&self) {
    *self.pair.lock() = true;
    self.signal.notify_all();
  }

  fn wait(&self, timeout: Option<Duration>) -> bool {
    let mut signaled = self.pair.lock();
    if *signaled {
      return true;
    }
    match timeout {
      None => {
        while !*signaled {
          self.signal.wait(&mut signaled);
        }
        true
      }
      Some(to) => {
        let deadline = Instant::now() + to;
        while !*signaled {
          let Some(remain) = deadline.checked_duration_since(Instant::now()) else {
            return false;
          };
          let res = self.signal.wait_for(&mut signaled, remain);
          if res.timed_out() && !*signaled {
            return false;
          }
        }
        true
      }
    }
  }
}

/// 回放对齐栅栏（对标 libs/server/AOF/ReadConsistency/ReplayAlignBarrier.cs:ReplayAlignBarrier）。
pub struct ReplayAlignBarrier {
  /// 参与者数（每虚拟子日志一个）。
  participant_count: usize,
  /// 极速快路径原子位点：无活动轮或禁用时为 i64::MAX；活动轮时为当前目标值。
  /// （单次原子 load(Relaxed) 拦截 99.999% 非对齐位点）。
  target: AtomicI64,
  /// 轮次是否进行中（C# InProgress：活动轮或禁用态均为 true）。
  in_progress: AtomicBool,
  /// 禁用态标记（对标 C# Disable 永不完成轮）。
  disabled: AtomicBool,
  /// 轮次自增 ID 生成器。
  next_round_id: AtomicU64,
  /// 当前活动轮次 ID（0 表示无轮次）。
  active_round_id: AtomicU64,
  /// 当前轮次实体（仅在开轮、到场计数、放行时保护访问）。
  current_round: Mutex<Option<Arc<Round>>>,
  /// 每参与者最近到达的轮次 ID（对标 C# lastArrivedRound，无锁原子比对去重）。
  last_arrived_round: Box<[AtomicU64]>,
  /// 每参与者复用唤醒事件（对标 C# participantEvents）。
  participant_events: Box<[ParticipantEvent]>,
  /// 阻塞等待上限（None = 永等；C# replicaSyncTimeout）。
  replica_sync_timeout: Option<Duration>,
}

impl ReplayAlignBarrier {
  /// 构造（C# ReplayAlignBarrier(participantCount, spinUs, replicaSyncTimeout)）。
  pub fn new(participant_count: usize, replica_sync_timeout: Option<Duration>) -> Self {
    Self {
      participant_count,
      target: AtomicI64::new(i64::MAX),
      in_progress: AtomicBool::new(false),
      disabled: AtomicBool::new(false),
      next_round_id: AtomicU64::new(1),
      active_round_id: AtomicU64::new(0),
      current_round: Mutex::new(None),
      last_arrived_round: (0..participant_count).map(|_| AtomicU64::new(0)).collect(),
      participant_events: (0..participant_count)
        .map(|_| ParticipantEvent::new())
        .collect(),
      replica_sync_timeout,
    }
  }

  /// 是否有轮次进行中（C# InProgress；禁用态的“永不完成轮”也算进行中）。
  #[inline]
  pub fn in_progress(&self) -> bool {
    self.in_progress.load(Ordering::Acquire)
  }

  /// libs/server/AOF/ReadConsistency/ReplayAlignBarrier.cs:TryOpenRound
  ///
  /// 观察到大幅漂移时按目标开轮；已有轮次时空操作。
  pub fn try_open_round(&self, target: i64) {
    if self.in_progress.load(Ordering::Acquire) {
      return;
    }
    let mut current = self.current_round.lock();
    if self.in_progress.load(Ordering::Acquire) {
      return;
    }
    let round_id = self.next_round_id.fetch_add(1, Ordering::Relaxed);
    let round = Arc::new(Round::new(round_id, target, self.participant_count as i32));
    *current = Some(round);
    self.active_round_id.store(round_id, Ordering::Release);
    self.target.store(target, Ordering::Release);
    self.in_progress.store(true, Ordering::Release);
  }

  /// libs/server/AOF/ReadConsistency/ReplayAlignBarrier.cs:SignalArrivalAndWait
  ///
  /// 回放线程推进前沿后调用：达标的首次到达者阻塞等待全员到齐。
  #[inline(always)]
  pub fn signal_arrival_and_wait(&self, virtual_sublog_idx: usize, frontier: i64) {
    // 极速快路径 1：无轮次（target=i64::MAX）或未跨入目标，单次 Relaxed 读 + 比较直接放行
    if frontier < self.target.load(Ordering::Relaxed) {
      return;
    }
    // 极速快路径 2：已达标，但本参与者在当前轮已到场，单次原子比较直接放行（去重）
    let round_id = self.active_round_id.load(Ordering::Acquire);
    if round_id != 0
      && self.last_arrived_round[virtual_sublog_idx].load(Ordering::Relaxed) == round_id
    {
      return;
    }
    self.signal_arrival_and_wait_slow(virtual_sublog_idx, frontier);
  }

  #[cold]
  fn signal_arrival_and_wait_slow(&self, virtual_sublog_idx: usize, frontier: i64) {
    let round = {
      let current = self.current_round.lock();
      let Some(round) = current.as_ref() else {
        return;
      };
      if frontier < round.target {
        return;
      }
      let last = self.last_arrived_round[virtual_sublog_idx].load(Ordering::Relaxed);
      if last == round.id {
        return;
      }
      self.last_arrived_round[virtual_sublog_idx].store(round.id, Ordering::Relaxed);
      Arc::clone(round)
    };

    self.wait_for_all_arrivals(&round, virtual_sublog_idx);
  }

  /// libs/server/AOF/ReadConsistency/ReplayAlignBarrier.cs:SignalArrival
  ///
  /// 空闲虚拟子日志的到场计数（不阻塞调用方）。
  #[inline(always)]
  pub fn signal_arrival(&self, virtual_sublog_idx: usize, frontier: i64) {
    if frontier < self.target.load(Ordering::Relaxed) {
      return;
    }
    let round_id = self.active_round_id.load(Ordering::Acquire);
    if round_id != 0
      && self.last_arrived_round[virtual_sublog_idx].load(Ordering::Relaxed) == round_id
    {
      return;
    }
    self.signal_arrival_slow(virtual_sublog_idx, frontier);
  }

  #[cold]
  fn signal_arrival_slow(&self, virtual_sublog_idx: usize, frontier: i64) {
    let round = {
      let current = self.current_round.lock();
      let Some(round) = current.as_ref() else {
        return;
      };
      if frontier < round.target {
        return;
      }
      let last = self.last_arrived_round[virtual_sublog_idx].load(Ordering::Relaxed);
      if last == round.id {
        return;
      }
      self.last_arrived_round[virtual_sublog_idx].store(round.id, Ordering::Relaxed);
      if round.remaining.fetch_sub(1, Ordering::AcqRel) > 1 {
        return;
      }
      Arc::clone(round)
    };

    self.release_round(&round);
  }

  /// libs/server/AOF/ReadConsistency/ReplayAlignBarrier.cs:WaitForAllArrivals
  ///
  /// 最后到达者放行全员；否则阻塞（自旋后转入每参与者专用事件等待，超时放弃对齐）。
  fn wait_for_all_arrivals(&self, round: &Arc<Round>, virtual_sublog_idx: usize) {
    if round.remaining.fetch_sub(1, Ordering::AcqRel) <= 1 {
      self.release_round(round);
      return;
    }

    // 自旋阶段（对标 C# SpinWait）
    for _ in 0..256 {
      if round.released.load(Ordering::Acquire) {
        return;
      }
      spin_loop();
    }

    // 阻塞阶段：等待本参与者专属唤醒句柄
    let ev = &self.participant_events[virtual_sublog_idx];
    ev.reset();
    if round.released.load(Ordering::Acquire) {
      return;
    }
    ev.wait(self.replica_sync_timeout);
  }

  /// libs/server/AOF/ReadConsistency/ReplayAlignBarrier.cs:ReleaseRound
  ///
  /// 放行自旋者与阻塞者；轮次仍为当前轮时摘除。
  fn release_round(&self, round: &Arc<Round>) {
    round.release();
    self.signal_all();
    let mut current = self.current_round.lock();
    if let Some(cur) = current.as_ref()
      && Arc::ptr_eq(cur, round)
    {
      *current = None;
      self.target.store(i64::MAX, Ordering::Release);
      self.active_round_id.store(0, Ordering::Release);
      if !self.disabled.load(Ordering::Acquire) {
        self.in_progress.store(false, Ordering::Release);
      }
    }
  }

  /// libs/server/AOF/ReadConsistency/ReplayAlignBarrier.cs:SignalAll
  ///
  /// 唤醒全部已挂起的参与者（对标 C# participantEvents[i].Set()）。
  fn signal_all(&self) {
    for ev in self.participant_events.iter() {
      ev.set();
    }
  }

  /// libs/server/AOF/ReadConsistency/ReplayAlignBarrier.cs:Disable
  ///
  /// 以“永不完成轮”占位（目标 i64::MAX、已放行）：拒绝新轮并放行既有等待者，
  /// 防止即将退出的参与者令同侪无限等待。
  pub fn disable(&self) {
    self.disabled.store(true, Ordering::Release);
    self.in_progress.store(true, Ordering::Release);
    self.target.store(i64::MAX, Ordering::Release);
    self.active_round_id.store(0, Ordering::Release);
    let old = self.current_round.lock().take();
    if let Some(round) = old {
      round.release();
    }
    self.signal_all();
  }

  /// libs/server/AOF/ReadConsistency/ReplayAlignBarrier.cs:Enable
  ///
  /// 清空占位轮，恢复开轮能力。
  pub fn enable(&self) {
    self.disabled.store(false, Ordering::Release);
    let old = self.current_round.lock().take();
    if let Some(round) = old {
      round.release();
    }
    self.target.store(i64::MAX, Ordering::Release);
    self.active_round_id.store(0, Ordering::Release);
    self.in_progress.store(false, Ordering::Release);
    self.signal_all();
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

  #[test]
  fn fast_path_throughput_no_round() {
    let barrier = ReplayAlignBarrier::new(4, None);
    // 无轮次或未达标时单次原子读直接跳过，零锁争用
    for _ in 0..100_000 {
      barrier.signal_arrival_and_wait(0, 500);
      barrier.signal_arrival(1, 500);
    }
  }
}
