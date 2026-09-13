use std::{sync::Arc, time::Duration};

use compio::runtime::Runtime;
use waof::AofAddress;
use wedb::server::{
  replication::{
    aof_sync_driver::AofSyncDriver,
    aof_sync_task::AofSyncTask,
    checkpoint_entry::{CheckpointEntry, CheckpointMetadata},
    checkpoint_store::CheckpointStore,
    network_buffer::{MAX_CHUNK_SIZE, ReplicationSendBufferPool},
    recovery_status::RecoveryStatus,
    replication_manager::{ReplicationManager, ResyncStrategy},
    sync_metadata::SyncMetadata,
  },
  worker::NodeRole,
};

#[test]
fn test_full_replication_sync_pipeline() {
  Runtime::new().unwrap().block_on(async {
    // 1. 初始化主节点与从节点复制管理器
    let primary_mgr = ReplicationManager::with_options(2, None);
    let replica_mgr = ReplicationManager::with_options(2, None);

    // 对标 C#：初始位点为 kFirstValidAofAddress(64)
    assert_eq!(primary_mgr.get_replication_offset(0), 64);
    assert_eq!(replica_mgr.get_replication_offset(0), 64);

    // 2. 主节点生成 AOF 写入并推进自身位点到 1000
    primary_mgr.set_sublog_replication_offset(0, 1000);
    primary_mgr.set_sublog_replication_offset(1, 800);
    assert_eq!(primary_mgr.get_replication_offset(0), 1000);
    assert_eq!(primary_mgr.get_replication_offset(1), 800);

    // 3. 构造主从握手 SyncMetadata 协商
    let mut ckpt_meta = CheckpointMetadata::new(2);
    ckpt_meta.store_version = 1;
    ckpt_meta.store_hlog_token = 0x1122_3344_5566_7788;
    ckpt_meta.store_index_token = 0x99aa_bbcc_ddee_ff00;
    ckpt_meta.store_primary_repl_id = Some(primary_mgr.primary_repl_id());
    ckpt_meta.store_checkpoint_covered_aof_address = AofAddress::create(2, 500);
    let ckpt_entry = CheckpointEntry::new(ckpt_meta);

    let sync_meta = SyncMetadata {
      full_sync: false,
      origin_node_role: NodeRole::Replica,
      origin_node_id: "replica-node-1".to_string(),
      current_primary_repl_id: replica_mgr.primary_repl_id(),
      current_store_version: 1,
      current_aof_begin_address: AofAddress::create(2, 0),
      current_aof_tail_address: primary_mgr.get_current_replication_offset(),
      current_replication_offset: replica_mgr.get_current_replication_offset(),
      checkpoint_entry: Some(ckpt_entry),
    };

    let sync_meta_bytes = sync_meta.to_byte_array();
    let decoded_sync_meta =
      SyncMetadata::from_byte_array(&sync_meta_bytes).expect("decode sync meta");
    assert_eq!(decoded_sync_meta.origin_node_id, "replica-node-1");
    assert_eq!(decoded_sync_meta.current_store_version, 1);

    // 4. 主节点登记从节点的 AofSyncDriver
    let start_addr = AofAddress::create(2, 0);
    let sync_driver = Arc::new(AofSyncDriver::new(
      "primary-node-1".to_string(),
      "replica-node-1".to_string(),
      &start_addr,
    ));

    let added = primary_mgr
      .aof_sync_driver_store
      .try_add_replication_driver(sync_driver.clone(), false);
    assert!(added);
    assert_eq!(primary_mgr.aof_sync_driver_store.count(), 1);

    // 5. 模拟主节点 AOF 日志流水线向 AofSyncTask 推送增量
    let task0 = sync_driver.get_task(0).expect("task 0 exists");
    let task1 = sync_driver.get_task(1).expect("task 1 exists");

    let aof_payload0 = b"*3\r\n$3\r\nSET\r\n$4\r\nuser\r\n$5\r\nalice\r\n";
    let aof_payload1 = b"*3\r\n$3\r\nSET\r\n$4\r\norder\r\n$3\r\n101\r\n";

    // sublog 0 推送 [64 -> 600]（起始位点规整为 kFirstValidAofAddress）
    task0
      .consume(aof_payload0, 64, 600)
      .expect("consume sublog 0");
    assert_eq!(task0.previous_address(), 600);

    // sublog 1 推送 [64 -> 500]
    task1
      .consume(aof_payload1, 64, 500)
      .expect("consume sublog 1");
    assert_eq!(task1.previous_address(), 500);

    // 节流与高水位报备
    let pub0 = task0.throttle(100);
    assert_eq!(pub0, Some(600));

    // 6. 主节点安全截断验证：主自身为 1000/800，副本为 600/500，截断 1200 时安全界限为 600
    let safe_sublog0 = primary_mgr
      .aof_sync_driver_store
      .safe_truncate_sublog(1200, 0, i64::MAX);
    assert_eq!(safe_sublog0, 600);
    let safe_all = primary_mgr
      .aof_sync_driver_store
      .safe_truncate_aof(&AofAddress::create(2, 1200));
    assert_eq!(safe_all.get(0), Some(600));
    assert_eq!(safe_all.get(1), Some(500));

    // 7. 从节点初始化 ReplicaReplayDriver 并直接消费重放
    assert!(replica_mgr.initialize_replica_replay_driver(0));
    assert!(replica_mgr.initialize_replica_replay_driver(1));

    let (replay_driver0, replay_driver1) = {
      let replay_store = &replica_mgr.replica_replay_driver_store;
      (
        replay_store.get_replay_driver(0).expect("replay driver 0"),
        replay_store.get_replay_driver(1).expect("replay driver 1"),
      )
    };

    let mut replayed_commands = Vec::new();
    let next0 = replay_driver0
      .consume_direct(aof_payload0, 0, 600, |rec, addr| {
        replayed_commands.push((0, addr, rec.to_vec()));
      })
      .expect("replay 0 ok");
    replica_mgr.set_sublog_replication_offset(0, next0);

    let next1 = replay_driver1
      .consume_direct(aof_payload1, 0, 500, |rec, addr| {
        replayed_commands.push((1, addr, rec.to_vec()));
      })
      .expect("replay 1 ok");
    replica_mgr.set_sublog_replication_offset(1, next1);

    assert_eq!(replayed_commands.len(), 2);
    assert_eq!(replica_mgr.get_replication_offset(0), 600);
    assert_eq!(replica_mgr.get_replication_offset(1), 500);

    // 8. 副本位点确认 ACK 上报与主节点确认推进
    let ack0 = replay_driver0.create_replication_ack("replica-node-1");
    let ack1 = replay_driver1.create_replication_ack("replica-node-1");
    assert!(primary_mgr.handle_replica_ack(
      &ack0.node_id,
      ack0.physical_sublog_idx,
      ack0.acked_offset
    ));
    assert!(primary_mgr.handle_replica_ack(
      &ack1.node_id,
      ack1.physical_sublog_idx,
      ack1.acked_offset
    ));
    assert_eq!(sync_driver.get_acked_address(0), 600);
    assert_eq!(sync_driver.get_acked_address(1), 500);

    // 9. 异步追赶位点测试：等待从节点追平 (600, 500)
    let target = AofAddress::from_string("600,500").expect("target address");
    let caught_up = replica_mgr
      .wait_for_replication_offset_async(&target, Duration::from_millis(500))
      .await;
    assert!(caught_up);

    // 10. 故障转移测试：旧主历史位点留存并生成新纪元 ID
    let old_primary_id = primary_mgr.primary_repl_id();
    primary_mgr.try_update_for_failover();
    assert_eq!(primary_mgr.primary_repl_id2(), old_primary_id);
    assert_ne!(primary_mgr.primary_repl_id(), old_primary_id);
    assert_eq!(primary_mgr.get_replication_offset2().get(0), Some(1000));
  });
}

#[test]
fn test_resync_strategy_decision_matrix() {
  let mgr = ReplicationManager::with_options(2, None);

  let mut m = CheckpointMetadata::new(2);
  m.store_version = 100;
  m.store_hlog_token = 0xabc;
  m.store_primary_repl_id = Some(mgr.primary_repl_id());
  m.store_checkpoint_covered_aof_address = AofAddress::create(2, 5000);
  let entry = CheckpointEntry::new(m);
  mgr
    .checkpoint_store
    .write()
    .add_checkpoint_entry(entry.clone(), true);

  let committed = AofAddress::create(2, 8000);
  let begin = AofAddress::create(2, 1000);

  // 1. 同一历史且位点无缝对接 -> PartialResync
  let sync_meta_partial = SyncMetadata {
    full_sync: false,
    origin_node_role: NodeRole::Replica,
    origin_node_id: "r1".to_string(),
    current_primary_repl_id: mgr.primary_repl_id(),
    current_store_version: 100,
    current_aof_begin_address: AofAddress::create(2, 1000),
    current_aof_tail_address: AofAddress::create(2, 7000),
    current_replication_offset: AofAddress::create(2, 7000),
    checkpoint_entry: Some(entry.clone()),
  };
  let strat1 = mgr.determine_resync_strategy(&sync_meta_partial, &committed, &begin, false);
  assert!(matches!(strat1, ResyncStrategy::PartialResync { .. }));

  // 2. 副本 AOF 起始位点超过了检查点覆盖线（中间存在空洞） -> 强制 FullResync
  let sync_meta_hole = SyncMetadata {
    current_aof_begin_address: AofAddress::create(2, 6000), // > 5000
    origin_node_id: "r2".to_string(),
    ..sync_meta_partial.clone()
  };
  let strat2 = mgr.determine_resync_strategy(&sync_meta_hole, &committed, &begin, false);
  assert!(matches!(strat2, ResyncStrategy::FullResync { .. }));
}

#[test]
fn test_network_buffer_pool_reuse_and_backpressure() {
  let pool = ReplicationSendBufferPool::new(4, MAX_CHUNK_SIZE, (MAX_CHUNK_SIZE * 2) as i64);
  assert!(!pool.is_throttled());

  let mut buf = pool.acquire();
  assert_eq!(buf.capacity(), MAX_CHUNK_SIZE);
  let written = buf.write(b"SAMPLE_AOF_PAYLOAD");
  assert_eq!(written, 18);

  // 触发背压
  assert!(pool.track_inflight_send(MAX_CHUNK_SIZE * 2));
  assert!(pool.is_throttled());

  // ACK 后解除背压
  pool.acknowledge_inflight(MAX_CHUNK_SIZE * 2);
  assert!(!pool.is_throttled());

  pool.release(buf);
}

#[test]
fn test_large_record_chunking_and_inflight_tracking() {
  let task = AofSyncTask::new(0, 0, "local".to_string(), "remote".to_string());
  let large_record = vec![0x42u8; MAX_CHUNK_SIZE * 2 + 512];
  task
    .consume(&large_record, 0, large_record.len() as i64)
    .expect("consume large record");
  assert_eq!(task.previous_address(), large_record.len() as i64);
  assert_eq!(task.shipped_watermark_address(), large_record.len() as i64);
}

#[test]
fn test_checkpoint_store_reader_suspension_and_token_pruning() {
  let mut store = CheckpointStore::new(true);

  // 检查点 1: 独占 index token
  let mut m1 = CheckpointMetadata::new(1);
  m1.store_version = 10;
  m1.store_hlog_token = 0x111;
  m1.store_index_token = 0x222;
  let e1 = CheckpointEntry::new(m1);
  store.add_checkpoint_entry(e1, true);

  // 检查点 2: 增量检查点，复用 index token 0x222，拥有新 hlog token 0x333
  let mut m2 = CheckpointMetadata::new(1);
  m2.store_version = 11;
  m2.store_hlog_token = 0x333;
  let e2 = CheckpointEntry::new(m2);
  store.add_checkpoint_entry(e2, false); // full_checkpoint = false, 继承 index token

  // 此时 index token 被共享，因此检查点 1 无法被淘汰（即使无读者）
  assert_eq!(store.entry_count(), 2);

  // 检查点 3: 全量检查点，生成新的 index token 0x444
  let mut m3 = CheckpointMetadata::new(1);
  m3.store_version = 12;
  m3.store_hlog_token = 0x555;
  m3.store_index_token = 0x444;
  let e3 = CheckpointEntry::new(m3);
  store.add_checkpoint_entry(e3, true);

  let latest = store.try_get_latest_checkpoint_entry_from_memory().unwrap();
  assert_eq!(latest.metadata.store_version, 12);
  latest.remove_reader();
}

#[test]
fn test_recovery_status_state_machine() {
  let mgr = ReplicationManager::with_options(1, None);

  // 初始状态
  assert_eq!(mgr.recovery_status(), RecoveryStatus::NoRecovery);
  assert!(!mgr.is_recovering());

  // 尝试升级锁（未持有 ReadRole 时应失败）
  assert!(!mgr.begin_recovery(RecoveryStatus::ClusterFailover, true));

  // 获取 ReadRole 锁
  assert!(mgr.begin_recovery(RecoveryStatus::ReadRole, false));
  assert!(!mgr.is_recovering()); // ReadRole 不算 recovering

  // 从 ReadRole 升级为 ClusterFailover
  assert!(mgr.begin_recovery(RecoveryStatus::ClusterFailover, true));
  assert!(mgr.is_recovering());

  // 结束恢复降级为 ReadRole
  mgr.end_recovery(RecoveryStatus::ReadRole, true);
  assert_eq!(mgr.recovery_status(), RecoveryStatus::ReadRole);

  // 彻底结束恢复
  mgr.end_recovery(RecoveryStatus::NoRecovery, false);
  assert_eq!(mgr.recovery_status(), RecoveryStatus::NoRecovery);
}
