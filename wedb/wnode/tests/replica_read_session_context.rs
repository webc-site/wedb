//! 副本读会话状态机集成测试
//! （对应 libs/server/AOF/ReadConsistency/ReplicaReadSessionContext.cs:ReplicaReadSessionContext）

use std::{sync::Arc, time::Duration};

use wnode::{
  PrimaryTasks,
  aof::readconsistency::{
    read_consistency_manager::ReadConsistencyManager,
    replica_read_session_context::ReadSessionState,
  },
};

#[test]
fn power_of_two_sizes() {
  assert_eq!(ReadSessionState::get_power_of_two_size(0), 1);
  assert_eq!(ReadSessionState::get_power_of_two_size(1), 1);
  assert_eq!(ReadSessionState::get_power_of_two_size(5), 8);
  assert_eq!(ReadSessionState::get_power_of_two_size(16), 16);
  assert_eq!(ReadSessionState::get_power_of_two_size(17), 32);
}

#[test]
fn expand_shrink_and_cache_roundtrip() {
  let manager = Arc::new(ReadConsistencyManager::new(
    1,
    2,
    2,
    -1,
    0,
    Some(Duration::from_millis(100)),
  ));
  let state = ReadSessionState::new(manager, 4, Some(Duration::from_millis(100)));

  // expand/shrink 是回调两臂分派的唯一伸缩入口，此用例直接压其容量语义
  state.expand_key_hash_cache(5);
  assert_eq!(state.key_hash_cache.read().len(), 8);

  state.shrink_key_hash_cache(5);
  assert_eq!(state.key_hash_cache.read().len(), 8);

  state.shrink_key_hash_cache(3);
  assert_eq!(state.key_hash_cache.read().len(), 4);
}

#[test]
fn read_session_state_lifecycle() {
  let manager = Arc::new(ReadConsistencyManager::new(
    1,
    2,
    2,
    -1,
    0,
    Some(Duration::from_millis(100)),
  ));
  // 回放事件源先行推进两物理子日志前沿，使跨子日志新鲜度校验真实成立
  manager.update_physical_sublog_max_sequence_number(0, 1);
  manager.update_physical_sublog_max_sequence_number(1, 1);
  let state = Arc::new(ReadSessionState::new(
    manager,
    4,
    Some(Duration::from_millis(100)),
  ));

  // 单 key 前置与后置
  state.pre_single_key_consistent_read(0x1234).unwrap();
  state.post_single_key_consistent_read_callback();

  // 批量读流程
  let keys: &[&[u8]] = &[b"key1", b"key2", b"key3"];
  state.pre_batch_key_consistent_read_callback(keys).unwrap();
  assert!(state.post_batch_key_consistent_read_callback(keys.len()));
  // 伸缩经回调两臂接线：3 键触发扩容臂至 2 的幂；空批触发缩容臂
  assert_eq!(state.key_hash_cache.read().len(), 4);
  state.pre_batch_key_consistent_read_callback(&[]).unwrap();
  assert_eq!(state.key_hash_cache.read().len(), 1);
  assert!(state.post_batch_key_consistent_read_callback(0));
}

#[test]
fn attach_derives_topology_and_gate_from_manager() {
  // 2 物理 × 3 回放 = 6 虚拟子日志；attach 自 manager 派生拓扑与超时
  let manager = Arc::new(ReadConsistencyManager::new(
    1,
    2,
    3,
    -1,
    0,
    Some(Duration::from_millis(77)),
  ));
  let state = ReadSessionState::attach(manager, None);
  assert_eq!(state.batch_read_context.cached_sublog_max.len(), 6);
  assert_eq!(state.read_timeout, Some(Duration::from_millis(77)));
  assert!(state.role_gate.is_none());

  // 角色门直通：主库角色（replica = false）时 pre 零协议直通
  let gate = Arc::new(PrimaryTasks::default());
  let manager = Arc::new(ReadConsistencyManager::new(
    1,
    2,
    2,
    -1,
    0,
    Some(Duration::from_millis(50)),
  ));
  let gated = ReadSessionState::attach(manager, Some(gate));
  assert!(!gated.role_gate.as_ref().unwrap().is_replica());
  // 非副本：pre 直通，会话上下文不进入协议（last_virtual_sublog_idx 保持 -1）
  gated.pre_single_key_consistent_read(0x1234).unwrap();
  assert_eq!(gated.replica_read_context.last_virtual_sublog_idx(), -1);

  // 翻转副本角色后协议生效
  gated.role_gate.as_ref().unwrap().set_replica(true);
  gated.pre_single_key_consistent_read(0x1234).unwrap();
  assert_ne!(gated.replica_read_context.last_virtual_sublog_idx(), -1);
}
