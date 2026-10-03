//! 读取一致性管理器集成测试与状态机测试
//! （对应 libs/server/AOF/ReadConsistency/ReadConsistencyManager.cs:ReadConsistencyManager）

use std::time::Duration;

use wnode::aof::readconsistency::read_consistency_manager::ReadConsistencyManager;

#[test]
fn drift_bounding_opens_barrier_round() {
  // 阈值 5：漂移 > 5 即开轮
  let m = ReadConsistencyManager::new(1, 2, 1, 5, 0, Some(Duration::from_millis(10)));
  assert!(!m.replay_barrier.in_progress());

  // 漂移未超阈值（5 <= 5）：不开轮
  m.update_virtual_sublog_max_sequence_number(0, 5);
  m.bound_replay_drift();
  assert!(!m.replay_barrier.in_progress());

  // 漂移超阈值：开轮
  m.update_virtual_sublog_max_sequence_number(0, 100);
  m.bound_replay_drift();
  assert!(m.replay_barrier.in_progress(), "漂移 100 应开轮");

  // 轮次进行中不重复开
  m.bound_replay_drift();
  assert!(m.replay_barrier.in_progress());
}

#[test]
fn drift_sequence_vector_formatting() {
  let m = ReadConsistencyManager::new(1, 2, 1, 10, 0, Some(Duration::from_millis(10)));
  m.update_physical_sublog_max_sequence_number(0, 100);
  m.update_physical_sublog_max_sequence_number(1, 80);

  let vector = m.get_physical_sublog_max_drift_sequence_vector();
  assert_eq!(vector, "0,20");
}
