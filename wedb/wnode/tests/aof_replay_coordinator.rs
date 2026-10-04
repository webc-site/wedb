//! AOF 回放协调器与同步栅栏集成测试
//! （对应 libs/server/AOF/ReplayCoordinator/AofReplayCoordinator.cs）

use std::{sync::Arc, thread::spawn, time::Duration};

use wnode::aof::replaycoordinator::aof_replay_coordinator::{
  AofReplayCoordinator, BarrierKey, LeaderBarrier,
};

/// 测试栅栏会合时限
const TEST_SYNC_TIMEOUT: Duration = Duration::from_secs(60);

#[test]
fn test_barrier_lifecycle() {
  let coord = AofReplayCoordinator::new(1, false, 1, Some(TEST_SYNC_TIMEOUT));
  let key = BarrierKey::new(1, 100);
  let barrier = coord.get_barrier(key, 1);
  assert_eq!(barrier.participant_count, 1);
  assert!(coord.try_remove_barrier(key));
  assert!(!coord.try_remove_barrier(key));
}

#[test]
fn test_leader_barrier_none_explicit_condvar_wait() {
  let barrier = Arc::new(LeaderBarrier::new(2));
  let b1 = Arc::clone(&barrier);
  // 两侧到场即「Leader 身份返回 + Leader 释放」，到场次序不定（spawn 与主线程
  // 抢首签是竞态），任一侧为 Leader 都必须由该侧 release 放行，否则另一侧
  // None 臂 Condvar::wait 显式永等即死锁（None = 无超时收敛，契约依赖释放）
  let handle = spawn(move || {
    let is_leader = b1.try_signal_or_wait(None);
    if is_leader {
      b1.release();
    }
    is_leader
  });
  let main_is_leader = barrier.try_signal_or_wait(None);
  if main_is_leader {
    barrier.release();
  }
  let spawned_is_leader = handle.join().unwrap();
  assert_ne!(
    main_is_leader, spawned_is_leader,
    "两侧必互为 Leader/Follower（None 臂显式永等直至会合放行）"
  );
}

#[test]
fn test_leader_barrier_timed_out_break() {
  let barrier = LeaderBarrier::new(2);
  // 只有 1 人到场，带时限等待超时收敛返回 true (Leader)
  let is_leader = barrier.try_signal_or_wait(Some(Duration::from_millis(20)));
  assert!(is_leader);
}
