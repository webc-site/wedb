//! 事务管理器与分片 AOF 日志集成测试

use std::{sync::Arc, time::Duration};

use wnode::{
  aof::{
    garnet_log::{GarnetLog, InMemorySublog},
    sequence_number_generator::SequenceNumberGenerator,
    sublog::Sublog,
  },
  config::runtime_server_options::RuntimeServerOptions,
};
use wtxn::{LockType, TransactionManager, TxnAofLog, TxnState, WatchVersionMap};

fn test_log(sublogs: usize, replay_tasks: i32) -> Arc<GarnetLog> {
  let options = RuntimeServerOptions {
    aof_physical_sublog_count: sublogs as i32,
    aof_replay_task_count: replay_tasks,
    ..RuntimeServerOptions::default()
  };
  let backends: Vec<Arc<Sublog>> = (0..sublogs.max(1))
    .map(|_| Arc::new(Sublog::Mem(InMemorySublog::new())))
    .collect();
  let seq_num_gen = (sublogs > 1).then(|| Arc::new(SequenceNumberGenerator::new(0)));
  Arc::new(GarnetLog::new(&options, backends, seq_num_gen))
}

/// test/standalone/Garnet.test/AofShardedTxnRecoveryTests.cs
#[test]
fn test_compute_sublog_access_vector_sharded() {
  let log = test_log(2, 2);
  let mut mgr = TransactionManager::new(Arc::new(WatchVersionMap::new(64)), Some(Arc::clone(&log)));

  // 尚未加锁时向量为空
  let (pvec, vvec, cnt) = mgr.compute_sublog_access_vector();
  assert_eq!(pvec, 0);
  assert_eq!(cnt, 0);
  assert_eq!(vvec.len(), 2);

  // 登记加锁键 k1 与 k2
  mgr.save_key_entry_to_lock(b"k1", LockType::Exclusive);
  mgr.save_key_entry_to_lock(b"k2", LockType::Exclusive);

  let (pvec, vvec, cnt) = mgr.compute_sublog_access_vector();
  assert!(pvec > 0);
  assert!(cnt > 0);
  assert_eq!(vvec.len(), 2);

  // 校验物理向量与回放任务位图覆盖了登记的所有键哈希
  for hash in mgr.key_entries.key_hashes() {
    let physical_idx = log.get_physical_sublog_idx(hash);
    let replay_idx = log.get_replay_task_idx(hash);
    assert_ne!(pvec & (1 << physical_idx), 0);
    assert_ne!(
      vvec[physical_idx][replay_idx / 8] & (1 << (replay_idx % 8)),
      0
    );
  }
}

#[test]
fn test_compute_sublog_access_vector_single_log_noop() {
  let log = test_log(1, 1);
  let mut mgr = TransactionManager::new(Arc::new(WatchVersionMap::new(64)), Some(Arc::clone(&log)));
  mgr.save_key_entry_to_lock(b"k1", LockType::Exclusive);

  // 单日志单回放任务下无需计算分片向量
  let (pvec, vvec, cnt) = mgr.compute_sublog_access_vector();
  assert_eq!(pvec, 0);
  assert!(vvec.is_empty());
  assert_eq!(cnt, 0);
}

#[test]
fn test_compute_sublog_access_vector_no_aof_log() {
  let mut mgr = TransactionManager::new(Arc::new(WatchVersionMap::new(64)), None::<()>);
  mgr.save_key_entry_to_lock(b"k1", LockType::Exclusive);

  let (pvec, vvec, cnt) = mgr.compute_sublog_access_vector();
  assert_eq!(pvec, 0);
  assert!(vvec.is_empty());
  assert_eq!(cnt, 0);
}

#[test]
fn test_compute_sublog_access_vector_dedup_participant_count() {
  let log = test_log(2, 2);
  let mut mgr = TransactionManager::new(Arc::new(WatchVersionMap::new(64)), Some(Arc::clone(&log)));
  // 登记多组键，验证 participant_count 与实际置位的 bit 数量严格一致（去重正确）
  for i in 0..20 {
    mgr.save_key_entry_to_lock(format!("k{i}").as_bytes(), LockType::Exclusive);
  }

  let (pvec, vvec, cnt) = mgr.compute_sublog_access_vector();
  assert!(pvec > 0);
  let total_bits: u32 = vvec
    .iter()
    .map(|sublog| sublog.iter().map(|b| b.count_ones()).sum::<u32>())
    .sum();
  assert_eq!(cnt, total_bits);
}

#[test]
fn test_compute_sublog_access_vector_spill_over_inline_capacity() {
  // 8 个物理子日志，超出 SmallVec 内联容量 4，验证堆溢出回退路径正确性
  let log = test_log(8, 4);
  let mut mgr = TransactionManager::new(Arc::new(WatchVersionMap::new(64)), Some(Arc::clone(&log)));
  for i in 0..20 {
    mgr.save_key_entry_to_lock(format!("key_{i}").as_bytes(), LockType::Exclusive);
  }
  let (pvec, vvec, cnt) = mgr.compute_sublog_access_vector();
  assert_eq!(vvec.len(), 8);
  assert!(pvec > 0);
  assert!(cnt > 0);
  let total_bits: u32 = vvec
    .iter()
    .map(|sublog| sublog.iter().map(|b| b.count_ones()).sum::<u32>())
    .sum();
  assert_eq!(cnt, total_bits);
}

#[test]
fn test_txn_start_commit_sharded_enqueue() {
  let log = test_log(2, 2);
  let mut mgr = TransactionManager::new(Arc::new(WatchVersionMap::new(64)), Some(Arc::clone(&log)));
  mgr.save_key_entry_to_lock(b"k1", LockType::Exclusive);
  mgr.save_key_entry_to_lock(b"k2", LockType::Exclusive);
  assert!(mgr.perform_writes);

  assert!(mgr.run(false, false, Duration::ZERO));
  assert_eq!(mgr.state, TxnState::Running);

  mgr.commit(false);
  assert_eq!(mgr.state, TxnState::None);
}
