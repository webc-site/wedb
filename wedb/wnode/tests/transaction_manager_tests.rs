//! 事务管理器与分片 AOF 日志集成测试

use std::{sync::Arc, time::Duration};

use waof::SequenceNumberGenerator;
use wbase::map::HashSet;
use wconf::RuntimeServerOptions;
use wnode::aof::garnet_log::GarnetLog;
use wtxn::{
  LockType, TransactionManager, TxnKeyEntryComparison, TxnLockTable, TxnState, WatchVersionMap,
};
use wval::SessionPrefixBuf;

/// 根域会话前缀（测试缺省域单点，与生产恒有前缀形态一致）
fn root() -> SessionPrefixBuf {
  SessionPrefixBuf::ROOT
}

fn test_log(sublogs: usize, replay_tasks: i32) -> Arc<GarnetLog> {
  let options = RuntimeServerOptions {
    aof_physical_sublog_count: sublogs as i32,
    aof_replay_task_count: replay_tasks,
    ..RuntimeServerOptions::default()
  };
  let (_dirs, backends) = wnode_test::test_sublogs("txn_mgr", sublogs.max(1));
  let seq_num_gen = (sublogs > 1).then(|| Arc::new(SequenceNumberGenerator::new(0)));
  Arc::new(GarnetLog::new(&options, backends, seq_num_gen).expect("构造 GarnetLog"))
}

/// test/standalone/Garnet.test/AofShardedTxnRecoveryTests.cs
#[test]
fn test_compute_sublog_access_vector_sharded() {
  let log = test_log(2, 2);
  let mut mgr = TransactionManager::new(
    TxnLockTable::new(),
    Arc::new(WatchVersionMap::new(64)),
    Some(Arc::clone(&log) as Arc<dyn wtxn::TxnAofLog>),
  );

  // 尚未加锁时向量为空
  let (pvec, vvec, cnt) = mgr.compute_sublog_access_vector();
  assert_eq!(pvec, 0);
  assert_eq!(cnt, 0);
  assert_eq!(vvec.len(), 2);

  // 登记加锁键 k1 与 k2
  mgr.save_key_entry_to_lock(b"k1", LockType::Exclusive);
  mgr.save_key_entry_to_lock(b"k2", LockType::Exclusive);
  mgr.register_run_preamble(root().as_slice(), true);

  let (pvec, vvec, cnt) = mgr.compute_sublog_access_vector();
  assert!(pvec > 0);
  assert!(cnt > 0);
  assert_eq!(vvec.len(), 2);

  // 校验物理向量与回放任务位图覆盖了登记的所有键裸路由哈希（对齐 GarnetLog::hash 裸键路由）
  for hash in mgr.key_entries.routing_hashes() {
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
  let mut mgr = TransactionManager::new(
    TxnLockTable::new(),
    Arc::new(WatchVersionMap::new(64)),
    Some(Arc::clone(&log) as Arc<dyn wtxn::TxnAofLog>),
  );
  mgr.save_key_entry_to_lock(b"k1", LockType::Exclusive);
  mgr.register_run_preamble(root().as_slice(), true);

  // 单日志单回放任务下无需计算分片向量
  let (pvec, vvec, cnt) = mgr.compute_sublog_access_vector();
  assert_eq!(pvec, 0);
  assert!(vvec.is_empty());
  assert_eq!(cnt, 0);
}

#[test]
fn test_compute_sublog_access_vector_no_aof_log() {
  let mut mgr = TransactionManager::new(
    TxnLockTable::new(),
    Arc::new(WatchVersionMap::new(64)),
    None,
  );
  mgr.save_key_entry_to_lock(b"k1", LockType::Exclusive);
  mgr.register_run_preamble(root().as_slice(), true);

  let (pvec, vvec, cnt) = mgr.compute_sublog_access_vector();
  assert_eq!(pvec, 0);
  assert!(vvec.is_empty());
  assert_eq!(cnt, 0);
}

#[test]
fn test_compute_sublog_access_vector_dedup_participant_count() {
  let log = test_log(2, 2);
  let mut mgr = TransactionManager::new(
    TxnLockTable::new(),
    Arc::new(WatchVersionMap::new(64)),
    Some(Arc::clone(&log) as Arc<dyn wtxn::TxnAofLog>),
  );
  // 登记多组键，验证 participant_count 与实际置位的 bit 数量严格一致（去重正确）
  for i in 0..20 {
    mgr.save_key_entry_to_lock(format!("k{i}").as_bytes(), LockType::Exclusive);
  }
  mgr.register_run_preamble(root().as_slice(), true);

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
  let mut mgr = TransactionManager::new(
    TxnLockTable::new(),
    Arc::new(WatchVersionMap::new(64)),
    Some(Arc::clone(&log) as Arc<dyn wtxn::TxnAofLog>),
  );
  for i in 0..20 {
    mgr.save_key_entry_to_lock(format!("key_{i}").as_bytes(), LockType::Exclusive);
  }
  mgr.register_run_preamble(root().as_slice(), true);
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
  let mut mgr = TransactionManager::new(
    TxnLockTable::new(),
    Arc::new(WatchVersionMap::new(64)),
    Some(Arc::clone(&log) as Arc<dyn wtxn::TxnAofLog>),
  );
  mgr.save_key_entry_to_lock(b"k1", LockType::Exclusive);
  mgr.save_key_entry_to_lock(b"k2", LockType::Exclusive);
  assert!(!mgr.is_read_only());

  assert!(mgr.run(root().as_slice(), false, false, Duration::ZERO));
  assert!(mgr.perform_writes);
  assert_eq!(mgr.state, TxnState::Running);

  mgr.commit(false).unwrap();
  assert_eq!(mgr.state, TxnState::None);
}

/// 回归测试：多子日志拓扑下 TxnStart/TxnCommit 标记位图与数据条目裸键路由恒对齐
///
/// 验证点：
/// 1. 会话物理前缀（锁轨域）使 scoped_key_hash 与裸键 routing_hash 分叉；
/// 2. compute_sublog_access_vector 展开的 physical_vector 与 virtual_vectors
///    严格覆盖各数据键与 WATCH 键经 GarnetLog::hash 路由的实际子日志与任务槽；
/// 3. participant_count 精确等于数据条目实际落位子日志虚拟槽位的去重计数。
#[test]
fn test_sublog_access_vector_raw_routing_hash_alignment_regression() {
  let log = test_log(4, 4);
  let mut mgr = TransactionManager::new(
    TxnLockTable::new(),
    Arc::new(WatchVersionMap::new(64)),
    Some(Arc::clone(&log) as Arc<dyn wtxn::TxnAofLog>),
  );

  let prefix = SessionPrefixBuf::ROOT;
  let data_keys: &[&[u8]] = &[
    b"user:profile:1001",
    b"order:status:2002",
    b"item:inventory:3003",
    b"cache:meta:4004",
  ];
  let watch_key: &[u8] = b"balance:5005";

  // 1. 断言前缀锁轨哈希与裸键路由哈希确实分叉（存在种子域偏移）
  let mut has_diverged_routing = false;
  for &k in data_keys.iter().chain([&watch_key]) {
    let raw_hash = GarnetLog::hash(k);
    let scoped_hash = TxnKeyEntryComparison::scoped_key_hash(prefix.as_slice(), k);
    assert_ne!(
      scoped_hash, raw_hash,
      "scoped 前缀哈希必须与裸键哈希正交分叉"
    );
    let raw_sublog = (
      log.get_physical_sublog_idx(raw_hash),
      log.get_replay_task_idx(raw_hash),
    );
    let scoped_sublog = (
      log.get_physical_sublog_idx(scoped_hash),
      log.get_replay_task_idx(scoped_hash),
    );
    if raw_sublog != scoped_sublog {
      has_diverged_routing = true;
    }
  }
  assert!(
    has_diverged_routing,
    "测试键集中至少有一键在 scoped 与 raw 间发生路由漂移，确认触发分叉条件"
  );

  // 2. 登记普通排队写键与监视键
  for &k in data_keys {
    mgr.save_key_entry_to_lock(k, LockType::Exclusive);
  }
  mgr.watch(prefix.as_slice(), watch_key);

  // 展开运行前导
  mgr.register_run_preamble(prefix.as_slice(), false);

  // 3. 计算多子日志访问向量
  let (pvec, vvec, participant_count) = mgr.compute_sublog_access_vector();
  assert!(pvec > 0);
  assert!(participant_count > 0);
  assert_eq!(vvec.len(), 4);

  // 4. 断言所有键的数据条目实际落位槽位（GarnetLog::hash）均被标记位图严格覆盖
  let mut expected_slots = HashSet::default();
  for &k in data_keys.iter().chain([&watch_key]) {
    let raw_hash = GarnetLog::hash(k);
    let physical_idx = log.get_physical_sublog_idx(raw_hash);
    let replay_idx = log.get_replay_task_idx(raw_hash);

    expected_slots.insert((physical_idx, replay_idx));

    // 物理子日志位必须置位
    assert_ne!(
      pvec & (1u64 << physical_idx),
      0,
      "数据条目实际路由物理子日志 {physical_idx} 必须在 physical_vector 中置位"
    );

    // 虚拟回放任务位必须置位
    let byte_idx = replay_idx / 8;
    let bit_mask = 1u8 << (replay_idx % 8);
    assert_ne!(
      vvec[physical_idx][byte_idx] & bit_mask,
      0,
      "数据条目实际路由任务槽 ({physical_idx}, {replay_idx}) 必须在 virtual_vectors 中置位"
    );
  }

  // 5. participant_count 必须与数据条目实际槽位去重数完全一致
  assert_eq!(
    participant_count,
    expected_slots.len() as u32,
    "参与者计数必须与数据条目实际落位的去重 (physical, replay) 槽位数一致"
  );
}
