//! 虚拟子日志回放状态集成测试与草图槽/等待队列验证
//! （对应 libs/server/AOF/ReadConsistency/VirtualSublogReplayState.cs:VirtualSublogReplayState）

use std::{
  sync::{Arc, atomic::Ordering},
  time::Duration,
};

use wnode::aof::readconsistency::virtual_sublog_replay_state::{
  ReadSessionWaiter, SKETCH_SLOT_MASK, SKETCH_SLOT_SIZE, VirtualSublogReplayState,
};

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
fn wait_times_out_when_replay_lags() {
  let state = VirtualSublogReplayState::new(i64::MAX);
  let waiter = Arc::new(ReadSessionWaiter::new());
  assert!(!state.wait_for_sequence_number(100, &waiter, Some(Duration::from_millis(20))));
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
