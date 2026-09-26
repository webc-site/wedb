use std::{sync::Arc, time::Duration};

use event_listener::Event;
use waof::AofAddress;
use wedb::server::{
  replication::{
    aof_sync_driver::AofSyncDriver,
    aof_sync_task::AofSyncTask,
    checkpoint_entry::{CheckpointEntry, CheckpointMetadata},
    checkpoint_store::CheckpointStore,
    recovery_status::RecoveryStatus,
    replication_manager::{ReplicationManager, ResyncStrategy},
    sync_metadata::SyncMetadata,
  },
  worker::NodeRole,
};

/// 大记录构造基准：单次 AOF 分包上限 1MB，测试用于构造超分包上限的记录。
/// rust 复制/迁移流采全局单池（尺寸单点见 wbase::pool::DEFAULT_BUFFER_SIZE），
/// 原生产规格常量已随死代码清理移除，归本测试本地持有
const MAX_CHUNK_SIZE: usize = 1 << 20;

#[compio::test]
async fn test_full_replication_sync_pipeline() {
  // 1. 初始化主节点与从节点复制管理器
  let primary_mgr = ReplicationManager::with_options(2, None, false);
  let replica_mgr = Arc::new(ReplicationManager::with_options(2, None, false));

  // 对标 C#：初始位点为空日志起点（rust WalLog 无头区，判据 begin == tail）
  assert_eq!(primary_mgr.get_replication_offset(0), 0);
  assert_eq!(replica_mgr.get_replication_offset(0), 0);

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
    origin_node_id: 0x0000_0000_0000_0000_0000_0000_0002_E701,
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
  assert_eq!(
    decoded_sync_meta.origin_node_id,
    0x0000_0000_0000_0000_0000_0000_0002_E701
  );
  assert_eq!(decoded_sync_meta.current_store_version, 1);

  // 4. 主节点登记从节点的 AofSyncDriver
  let start_addr = AofAddress::create(2, 0);
  let sync_driver = Arc::new(AofSyncDriver::new(
    0x0000_0000_0000_0000_0000_0000_0000_DE11,
    0x0000_0000_0000_0000_0000_0000_0002_E701,
    2,
    &start_addr,
    None,
  ));

  let added = primary_mgr
    .aof_sync_driver_store
    .try_add_replication_driver(sync_driver.clone(), false);
  assert!(added);
  assert_eq!(primary_mgr.aof_sync_driver_store.count(), 1);

  // 5. 模拟主节点 AOF 日志流水线向 AofSyncTask 推送增量
  let task0 = sync_driver.task_ref(0).expect("task 0 exists");
  let task1 = sync_driver.task_ref(1).expect("task 1 exists");

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
  let safe_all = primary_mgr
    .aof_sync_driver_store
    .safe_truncate_aof(&AofAddress::create(2, 1200))
    .await;
  assert_eq!(safe_all.get(0), Some(600));
  assert_eq!(safe_all.get(1), Some(500));

  // 7. 从节点初始化 ReplicaReplayDriver（退化形态：无重放资产，不启动
  //    背景重放，位点由会话落盘面 enqueued 直推；背景重放在场时由重放
  //    链应用后经驱动权威面回推，见 replica_replay_task）
  assert!(replica_mgr.initialize_replica_replay_driver(0));
  assert!(replica_mgr.initialize_replica_replay_driver(1));

  let (replay_driver0, replay_driver1) = {
    let replay_store = replica_mgr.current_replica_replay_driver_store();
    (
      replay_store.get_replay_driver(0).expect("replay driver 0"),
      replay_store.get_replay_driver(1).expect("replay driver 1"),
    )
  };
  assert!(
    !replay_driver0.background_replay_started() && !replay_driver1.background_replay_started(),
    "退化形态不启动背景重放"
  );

  // 记录帧落盘后会话位点推进至帧尾（600/500）
  replica_mgr.set_sublog_replication_offset(0, 600);
  replica_mgr.set_sublog_replication_offset(1, 500);
  assert_eq!(replica_mgr.get_replication_offset(0), 600);
  assert_eq!(replica_mgr.get_replication_offset(1), 500);

  // 8. 异步追赶位点测试：等待从节点追平 (600, 500)。调用侧有界形态
  //    （对标 C# WaitAsync(timeout) 包裹，本地哨兵 listener 恒不触发）
  let target = AofAddress::from_string("600,500").expect("target address");
  let no_abort = Event::new();
  let mut no_abort_listener = no_abort.listen();
  let caught_up = replica_mgr
    .wait_for_replication_offset_async_with_abort(
      &target,
      Some(Duration::from_millis(500)),
      &mut no_abort_listener,
    )
    .await;
  assert!(caught_up);

  // 9. 故障转移测试：旧主历史位点留存并生成新纪元 ID
  let old_primary_id = primary_mgr.primary_repl_id();
  primary_mgr.try_update_for_failover();
  assert_eq!(primary_mgr.primary_repl_id2(), old_primary_id);
  assert_ne!(primary_mgr.primary_repl_id(), old_primary_id);
  assert_eq!(primary_mgr.get_replication_offset2().get(0), Some(1000));
}

#[test]
fn test_disk_resync_strategy_decision_matrix() {
  let mgr = ReplicationManager::with_options(2, None, false);

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
    origin_node_id: 0x0000_0000_0000_0000_0000_0000_0000_2E71,
    current_primary_repl_id: mgr.primary_repl_id(),
    current_store_version: 100,
    current_aof_begin_address: AofAddress::create(2, 1000),
    current_aof_tail_address: AofAddress::create(2, 7000),
    current_replication_offset: AofAddress::create(2, 7000),
    checkpoint_entry: Some(entry.clone()),
  };
  let strat1 = mgr.disk_resync_strategy(&sync_meta_partial, &committed, &begin, false);
  assert!(matches!(strat1, ResyncStrategy::PartialResync { .. }));

  // 2. 副本 AOF 起始位点超过了检查点覆盖线（中间存在空洞） -> 强制 FullResync
  let sync_meta_hole = SyncMetadata {
    current_aof_begin_address: AofAddress::create(2, 6000), // > 5000
    origin_node_id: 0x0000_0000_0000_0000_0000_0000_0000_2E72,
    ..sync_meta_partial.clone()
  };
  let strat2 = mgr.disk_resync_strategy(&sync_meta_hole, &committed, &begin, false);
  assert!(matches!(strat2, ResyncStrategy::FullResync { .. }));
}

#[test]
fn test_multi_sublog_negotiation_consumes_full_address_vector() {
  // 主端协商位点向量须按装配子日志数构造（replica_sync_session 动态维度承接，
  // 对标 C# ComputeAofSyncReplayAddress 按 AofPhysicalSublogCount 遍历；
  // 若硬编码单子日志，committed.get(1) 退化 i64::MAX，高位位点丢失）
  let mgr = ReplicationManager::with_options(2, None, false);

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

  // 副本与主端同检查点历史同版本（skipLocalMainStoreCheckpoint = true）；
  // 高位子日志 tail 已越过主端提交线 8000：replay_until 须被 committed 高位
  // 槽位截断至 8000，而非放行到 9000
  let mut rep_tail = AofAddress::create(2, 0);
  rep_tail.set(0, 6000);
  rep_tail.set(1, 9000);
  let sync_meta = SyncMetadata {
    full_sync: false,
    origin_node_role: NodeRole::Replica,
    origin_node_id: 0x0000_0000_0000_0000_0000_0000_0000_2E7F,
    current_primary_repl_id: mgr.primary_repl_id(),
    current_store_version: 100,
    current_aof_begin_address: AofAddress::create(2, 1000),
    current_aof_tail_address: rep_tail,
    current_replication_offset: rep_tail,
    checkpoint_entry: Some(entry),
  };

  let ResyncStrategy::PartialResync {
    sync_start_address,
    replay_aof_mask,
  } = mgr.disk_resync_strategy(&sync_meta, &committed, &begin, false)
  else {
    panic!("expected PartialResync");
  };
  // 逐子日志比对生效：低位承接 tail 6000，高位被 committed 截断至 8000
  assert_eq!(sync_start_address.get(0), Some(6000));
  assert_eq!(sync_start_address.get(1), Some(8000));
  assert_eq!(replay_aof_mask, 0b11);
}

/// §116 same_history2 钳位臂三态语义锁（回改陷阱防线：钳位臂现无其他测试覆盖，
/// 按 C# 字面回改即静默通过其余全部测试）
///
/// 语义钉：故障转移后重挂的副本，仅当其上报 current_primary_repl_id 命中本节点
/// repl_id2（旧主 ID）时，replay_until 才钳至 repl_offset2，截断已确认的分叉
/// 历史区间（对齐 C# GarnetAppendOnlyFile.cs:218 注释自陈意图与 Redis replid2
/// 截断标准语义）。C# ReplicaSyncSession.cs:162 sameHistory2 系
/// IsNullOrEmpty && Equals 互斥字面、恒假钳位死码，rust 采设计意图形谓词，
/// 属修复型偏离——见 doc/zh/deviations.md §116，严禁按 C# :162 字面回改。
///
/// 三态各一：未 failover（repl_id2 空串 + repl_offset2 i64::MAX 未转移哨）
/// 不钳；failover 且副本 id 命中钳至 repl_offset2；failover 但副本 id 不符
/// 不钳。经 pub 出口 disk_resync_strategy 断言协商出的 sync_start_address 形。
#[test]
fn test_disk_resync_same_history2_replay_until_clamp_three_states() {
  let mgr = ReplicationManager::with_options(2, None, false);

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
  // 副本尾 7000：高于检查点覆盖线 5000、低于提交线 8000，不钳时接续位点即 7000
  let rep_tail = AofAddress::create(2, 7000);

  let replay_until = |meta: &SyncMetadata| -> AofAddress {
    let ResyncStrategy::PartialResync {
      sync_start_address, ..
    } = mgr.disk_resync_strategy(meta, &committed, &begin, false)
    else {
      panic!("expected PartialResync");
    };
    sync_start_address
  };

  // 形态 1：未 failover——repl_id2 空串谓词假、repl_offset2 为 i64::MAX 未转移
  // 哨即便抵达亦不钳，接续位点保持 min(rep_tail, committed) = 7000
  let meta_no_failover = SyncMetadata {
    full_sync: false,
    origin_node_role: NodeRole::Replica,
    origin_node_id: 0x0000_0000_0000_0000_0000_0000_0000_2E81,
    current_primary_repl_id: mgr.primary_repl_id(),
    current_store_version: 100,
    current_aof_begin_address: AofAddress::create(2, 1000),
    current_aof_tail_address: rep_tail,
    current_replication_offset: rep_tail,
    checkpoint_entry: Some(entry.clone()),
  };
  let addr1 = replay_until(&meta_no_failover);
  assert_eq!(addr1.get(0), Some(7000), "未 failover 不钳");
  assert_eq!(addr1.get(1), Some(7000), "未 failover 不钳");

  // failover：旧主 ID 转存 repl_id2，停写提交尾 6500 同批冻结为 repl_offset2
  mgr.set_current_replication_offset(AofAddress::create(2, 6500));
  let old_primary_id = mgr.primary_repl_id();
  mgr.try_update_for_failover();
  assert_eq!(mgr.primary_repl_id2(), old_primary_id);
  assert_eq!(mgr.get_replication_offset2().get(0), Some(6500));

  // 形态 2：failover 且副本上报命中 repl_id2（曾挂旧主）——replay_until 钳至
  // repl_offset2 = 6500，分叉历史区间 (6500, 7000] 不回放
  let meta_hit = SyncMetadata {
    current_primary_repl_id: old_primary_id,
    ..meta_no_failover.clone()
  };
  let addr2 = replay_until(&meta_hit);
  assert_eq!(
    addr2.get(0),
    Some(6500),
    "副本 id 命中旧主须钳至 repl_offset2"
  );
  assert_eq!(
    addr2.get(1),
    Some(6500),
    "副本 id 命中旧主须钳至 repl_offset2"
  );

  // 形态 3：failover 但副本上报 id 不符（挂的是本节点新纪元）——不钳，7000
  let meta_miss = SyncMetadata {
    current_primary_repl_id: mgr.primary_repl_id(),
    origin_node_id: 0x0000_0000_0000_0000_0000_0000_0000_2E82,
    ..meta_no_failover.clone()
  };
  let addr3 = replay_until(&meta_miss);
  assert_eq!(addr3.get(0), Some(7000), "副本 id 不符不得钳");
  assert_eq!(addr3.get(1), Some(7000), "副本 id 不符不得钳");
}

#[test]
fn test_large_record_chunking() {
  let task = AofSyncTask::new(0, 0, 0x10CA1, 0x2E707E, None);
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
  let mgr = ReplicationManager::with_options(1, None, false);

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

/// attach 恢复窗口的 AOF 防线三态（C# ReplicationManager.cs:49
/// CannotStreamAOF => IsRecovering && status != CheckpointRecoveredAtReplica
/// 的时序对偶）：持锁传送/置换段拒流、恢复点回报后放行、收尾释放后常态
#[test]
fn test_cannot_stream_aof_across_attach_recovery_window() {
  let mgr = ReplicationManager::with_options(1, None, false);

  // attach 握锁（try_add_replica_async / 启动臂前置同形态）：
  // 传送与引擎置换窗口拒收 AOF 帧
  assert!(mgr.begin_recovery(RecoveryStatus::ClusterReplicate, false));
  assert!(mgr.is_recovering());
  assert!(mgr.cannot_stream_aof(), "传送/置换窗口必须拒收 AOF 帧");

  // 并发互斥：窗口内第二次 begin（二次 REPLICAOF / CLUSTER FAILOVER 同形）
  // 必须被拒（C# 由恢复锁返回 CannotAcquireRecoveryLock）
  assert!(!mgr.begin_recovery(RecoveryStatus::ClusterReplicate, false));

  // 恢复点回报（replica_diskbased/diskless_sync 收尾段）：放行 AOF 流但锁仍持
  mgr.end_recovery(RecoveryStatus::CheckpointRecoveredAtReplica, false);
  assert_eq!(
    mgr.recovery_status(),
    RecoveryStatus::CheckpointRecoveredAtReplica
  );
  assert!(mgr.is_recovering(), "恢复点后锁仍持");
  assert!(!mgr.cannot_stream_aof(), "恢复点后必须放行 AOF 流");

  // attach 收尾（finish_replica_sync）：释放到 NoRecovery
  mgr.end_recovery(RecoveryStatus::NoRecovery, false);
  assert_eq!(mgr.recovery_status(), RecoveryStatus::NoRecovery);
  assert!(!mgr.is_recovering());
  assert!(!mgr.cannot_stream_aof());
}
