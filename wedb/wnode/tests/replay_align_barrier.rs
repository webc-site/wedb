//! 回放对齐栅栏与参与者事件并发测试
//! （对应 libs/server/AOF/ReadConsistency/ReplayAlignBarrier.cs:ReplayAlignBarrier）

use std::{sync::Arc, thread, time::Duration};

use wnode::aof::readconsistency::replay_align_barrier::{ParticipantEvent, ReplayAlignBarrier};

#[test]
fn participant_event_wait_none_and_timeout() {
  let ev = Arc::new(ParticipantEvent::new());

  // 1. None 臂：走 listener.wait() 永等 loop
  let ev_clone = Arc::clone(&ev);
  let handle = thread::spawn(move || ev_clone.wait(None));
  thread::sleep(Duration::from_millis(20));
  ev.set();
  assert!(
    handle.join().unwrap(),
    "None 臂应在 set 后放行成功返回 true"
  );

  // 2. Some 臂：带时限等待，超时返回 false
  ev.reset();
  let timed_out = !ev.wait(Some(Duration::from_millis(20)));
  assert!(timed_out, "超时应返回 false");

  // 3. 立即满足：提前 set 后 wait 立即返回 true
  ev.set();
  assert!(ev.wait(Some(Duration::from_millis(10))));
}

#[test]
fn barrier_round_open_and_all_arrive_releases() {
  let barrier = Arc::new(ReplayAlignBarrier::new(2, Some(Duration::from_millis(200))));
  assert!(!barrier.in_progress());

  // 打开轮次，目标 100
  barrier.try_open_round(100);
  assert!(barrier.in_progress());

  let b1 = Arc::clone(&barrier);
  let h1 = thread::spawn(move || {
    // 参与者 0 到达 100
    b1.signal_arrival_and_wait(0, 100);
  });

  // 参与者 1 到达 100，触发全员放行
  barrier.signal_arrival_and_wait(1, 100);
  h1.join().unwrap();
  assert!(!barrier.in_progress());
}
