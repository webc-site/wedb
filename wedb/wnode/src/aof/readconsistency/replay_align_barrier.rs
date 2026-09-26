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
  time::Duration,
};

use coarsetime::Instant;
use event_listener::{Event, Listener};
use parking_lot::Mutex;
use wbase::align::CachePadded;

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

  /// libs/server/AOF/ReadConsistency/ReplayAlignBarrier.cs:Release
  ///
  /// 放行（C# Round.Release：置位权威放行信号，自旋者轮询直读）。
  #[inline]
  fn release(&self) {
    self.released.store(true, Ordering::Release);
  }
}

/// 每参与者复用唤醒事件（对标 C# participantEvents[virtualSublogIdx]）。
/// 仅属主虚拟子日志回放线程等待与复位，放行者仅置位不复位，零信号遗失风险。
#[derive(Default)]
struct ParticipantEvent {
  signaled: AtomicBool,
  event: Event,
}

impl ParticipantEvent {
  #[inline]
  fn new() -> Self {
    Self::default()
  }

  #[inline]
  fn reset(&self) {
    self.signaled.store(false, Ordering::Release);
  }

  #[inline]
  fn set(&self) {
    self.signaled.store(true, Ordering::Release);
    self.event.notify(usize::MAX);
  }

  fn wait(&self, timeout: Option<Duration>) -> bool {
    // 快路径：单次原子读，0 锁，<1ns
    if self.signaled.load(Ordering::Acquire) {
      return true;
    }

    match timeout {
      None => loop {
        let listener = self.event.listen();
        if self.signaled.load(Ordering::Acquire) {
          return true;
        }
        listener.wait();
        if self.signaled.load(Ordering::Acquire) {
          return true;
        }
      },
      Some(to) => {
        let deadline = Instant::now() + to.into();
        loop {
          let listener = self.event.listen();
          if self.signaled.load(Ordering::Acquire) {
            return true;
          }
          // coarsetime 的 Instant - Instant 为饱和减法（越界归零），
          // 归零时 wait_timeout 立即返回并落入下方 deadline 判定，
          // 与原 checked_duration_since 的 None 分支等价
          let remain = deadline - Instant::now();
          let _ = listener.wait_timeout(remain.into());
          if self.signaled.load(Ordering::Acquire) {
            return true;
          }
          if Instant::now() >= deadline {
            return false;
          }
        }
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
  /// 128 字节缓存行对齐，彻底消除多核并发伪共享。
  last_arrived_round: Box<[CachePadded<AtomicU64>]>,
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
      last_arrived_round: (0..participant_count)
        .map(|_| CachePadded::new(AtomicU64::new(0)))
        .collect(),
      participant_events: (0..participant_count)
        .map(|_| ParticipantEvent::new())
        .collect(),
      replica_sync_timeout,
    }
  }

  /// libs/server/AOF/ReadConsistency/ReplayAlignBarrier.cs:InProgress
  ///
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
  // 计时断言刻意保留 std 高精度单调时钟：coarse 时钟约 1-10ms 粒度，
  // 会令 elapsed >= 30ms 这类下界断言抖动，不适用 coarsetime 统一出口
  use std::{sync::mpsc, thread, time::Instant};

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
  fn three_participants_mixed_arrival_release() {
    let barrier = Arc::new(ReplayAlignBarrier::new(3, Some(Duration::from_secs(5))));
    barrier.try_open_round(100);

    // 参与者 0、1 阻塞到场，挂起等待全员
    let handles: Vec<_> = (0..2)
      .map(|i| {
        let b = Arc::clone(&barrier);
        thread::spawn(move || b.signal_arrival_and_wait(i, 120))
      })
      .collect();
    thread::sleep(Duration::from_millis(50));

    // 参与者 2（空闲子日志）非阻塞到场 → 全员到齐 → 集体放行
    barrier.signal_arrival(2, 120);
    for h in handles {
      h.join().unwrap();
    }
    assert!(!barrier.in_progress());
  }

  #[test]
  fn three_participants_partial_timeout_then_release() {
    let timeout = Duration::from_millis(60);
    let barrier = Arc::new(ReplayAlignBarrier::new(3, Some(timeout)));
    barrier.try_open_round(10);

    // 参与者 0 到场阻塞，期间无人放行，超时弃权继续执行
    let b0 = Arc::clone(&barrier);
    let h0 = thread::spawn(move || {
      let started = Instant::now();
      b0.signal_arrival_and_wait(0, 12);
      started.elapsed()
    });
    thread::sleep(timeout + Duration::from_millis(40));

    // 弃权后参与者 1 非阻塞到场计数
    barrier.signal_arrival(1, 12);

    // 参与者 2 最后到场 → 减至 0 → 放行并摘除轮次
    barrier.signal_arrival_and_wait(2, 12);
    let elapsed0 = h0.join().unwrap();
    assert!(elapsed0 >= timeout, "参与者 0 应在超时后才弃权返回");
    assert!(!barrier.in_progress());
  }

  #[test]
  fn five_participants_concurrent_rounds_rollover() {
    let barrier = Arc::new(ReplayAlignBarrier::new(5, Some(Duration::from_millis(500))));
    // 连续多轮并发：上一轮到达记录（旧轮 ID）不得令新轮去重误判，
    // 每轮 5 名参与者交错到场，全员到齐后摘除，紧接开下一轮
    for round in 1..=4 {
      let target = 100 * i64::from(round);
      barrier.try_open_round(target);
      assert!(barrier.in_progress());
      let handles: Vec<_> = (0..5)
        .map(|i| {
          let b = Arc::clone(&barrier);
          thread::spawn(move || b.signal_arrival_and_wait(i, target + i as i64))
        })
        .collect();
      for h in handles {
        h.join().unwrap();
      }
      assert!(!barrier.in_progress(), "第 {round} 轮未摘除");
    }
  }

  #[test]
  fn three_participants_repeat_arrival_dedupe_per_round() {
    let barrier = ReplayAlignBarrier::new(3, Some(Duration::from_secs(1)));
    // 连续多轮：同轮重复到场只计一次，跨轮（rollover）同参与者可再次到场
    for round in 1..=3 {
      let target = 10 * i64::from(round);
      barrier.try_open_round(target);
      for i in 0..3 {
        barrier.signal_arrival(i, target);
        barrier.signal_arrival(i, target);
      }
      assert!(!barrier.in_progress(), "第 {round} 轮未摘除");
    }
  }

  #[test]
  fn three_participants_disable_releases_blocked() {
    let barrier = Arc::new(ReplayAlignBarrier::new(3, None));
    barrier.try_open_round(10);

    // 参与者 0、1 永等阻塞
    let (tx, rx) = mpsc::channel();
    for i in 0..2 {
      let b = Arc::clone(&barrier);
      let tx = tx.clone();
      thread::spawn(move || {
        b.signal_arrival_and_wait(i, 12);
        let _ = tx.send(i);
      });
    }
    thread::sleep(Duration::from_millis(50));

    // disable 放行既有阻塞等待者
    barrier.disable();
    for _ in 0..2 {
      rx.recv_timeout(Duration::from_secs(2))
        .expect("disable 应放行阻塞参与者");
    }
    // 禁用态占位轮仍表现为进行中
    assert!(barrier.in_progress());

    // enable 恢复后可正常开轮与放行
    barrier.enable();
    assert!(!barrier.in_progress());
    barrier.try_open_round(20);
    for i in 0..3 {
      barrier.signal_arrival(i, 30);
    }
    assert!(!barrier.in_progress());
  }
}
