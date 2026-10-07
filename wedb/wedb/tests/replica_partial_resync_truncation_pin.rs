#![recursion_limit = "256"]
//! 磁盘链 PartialResync 钳制往返截断钉线集成测试（票
//! task/ing/wedb-repl-diskbased-partial-resync-missing-truncation-pin.md）
//!
//! 对标 C# 契约：
//! - garnet/libs/cluster/Server/Replication/PrimaryOps/DiskbasedReplication/ReplicaSyncSession.cs:
//!   SendCheckpointAsync :118 AcquireCheckpointEntryAsync 先于快照/往返 TryAddReplicationDriver 预锁钉线
//! - garnet/libs/cluster/Server/Replication/PrimaryOps/AofOperations/AofSyncDriverStore.cs:
//!   SafeTruncateAof :71-117 以在册驱动 previousAddress 取小钳制
//!
//! 验证场景：
//! 1. 钳制往返窗内并发 safe_truncate_aof：断言 truncated_until 恒不越过 granted 位点、attach 必成功；
//! 2. 往返失败/超时臂对齐退钉：断言预锁驱动出册、截断线解除钳制；
//! 3. fast_aof_truncate=true 且预锁被拒场景：断言单轮降级 FullResync 收敛不风暴；
//! 4. 默认分支：断言截断越线后协商退化 FullResync 而非静默丢段。

use std::{
  fs::create_dir_all,
  sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
  },
  time::Duration,
};

use compio::{net::TcpListener, runtime::spawn, time::sleep};
use tempfile::tempdir;
use waof::{AofAddress, WalConfig, WalLog};
use wconf::RuntimeServerOptions;
use wdev::SegmentedDevice;
use wedb::server::{
  cluster_provider::{ClusterProvider, PrimaryReplicationAssets},
  replication::{
    aof_replication_pump::AofReplicationPump,
    checkpoint_entry::{CheckpointEntry, CheckpointMetadata},
    error::{ConnectStage, ReplicationError},
    replica_sync_session::ReplicaSyncSession,
    replication_manager::ResyncStrategy,
    sync_metadata::SyncMetadata,
  },
  worker::NodeRole,
};
use wedb_test::fake_frame_pump::pump_frames;
use wnode::{
  aof::{GarnetAppendOnlyFile, GarnetLog},
  database::{GarnetDatabase, SingleDatabaseManager},
};
use wtest_base::wait_for;

const PRIMARY_ID: u128 = 0x5E1D_0000_0000_0000_0000_0000_0000_0001;
const REPLICA_ID: u128 = 0x5E1D_0000_0000_0000_0000_0000_0000_0002;

struct StubReplica {
  addr: String,
  begin_recover_seen: Arc<AtomicBool>,
  recover_gate: Arc<AtomicBool>,
}

async fn spawn_stub_replica(recover_gate: Arc<AtomicBool>, granted_address: i64) -> StubReplica {
  let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
  let addr = listener.local_addr().unwrap().to_string();
  let begin_recover_seen = Arc::new(AtomicBool::new(false));
  {
    let seen = Arc::clone(&begin_recover_seen);
    let gate = Arc::clone(&recover_gate);
    spawn(async move {
      while let Ok((mut stream, _)) = listener.accept().await {
        let (seen, gate) = (Arc::clone(&seen), Arc::clone(&gate));
        spawn(async move {
          // 读循环骨架见 `wedb_test::fake_frame_pump`（RESP2 帧解析单源
          // wtest_base::parse_frame_slices；本册原 owned 载荷面由参数切片直取替代）
          pump_frames(
            &mut stream,
            4096,
            async |_: &[u8], args: &[&[u8]]| match args.get(1).copied() {
              Some(b"BEGIN_REPLICA_RECOVER") => {
                seen.store(true, Ordering::SeqCst);
                while !gate.load(Ordering::SeqCst) {
                  sleep(Duration::from_millis(5)).await;
                }
                let s = format!("{granted_address}");
                Some(format!("${}\r\n{}\r\n", s.len(), s).into_bytes())
              }
              _ => Some(b"+OK\r\n".to_vec()),
            },
          )
          .await;
        })
        .detach();
      }
    })
    .detach();
  }
  StubReplica {
    addr,
    begin_recover_seen,
    recover_gate,
  }
}

/// 钳制往返窗内并发 safe_truncate_aof：断言 truncated_until 恒不越过 granted 位点、attach 必成功
#[compio::test]
async fn partial_resync_clamp_window_pins_safe_truncate_aof_and_attach_succeeds() {
  let dir = tempdir().unwrap();
  let wal_device = Arc::new(SegmentedDevice::single_file(dir.path().join("primary.wal")).unwrap());
  let wal = Arc::new(WalLog::new(wal_device, WalConfig::default()).unwrap());

  let provider = ClusterProvider::new();
  provider.initialize_replication_manager(1, None, false);
  let rm = provider.replication_manager().unwrap();
  let assets = Arc::new(PrimaryReplicationAssets {
    wal: Arc::clone(&wal),
    pump: Arc::new(AofReplicationPump::new(Arc::clone(
      &rm.aof_sync_driver_store,
    ))),
    sync_session: Arc::new(ReplicaSyncSession::new(Arc::clone(&rm))),
  });

  // 主端登记检查点（pin_start = 100）
  let mut meta = CheckpointMetadata::new(1);
  meta.store_version = 0;
  meta.store_hlog_token = 0x123;
  meta.store_primary_repl_id = Some(rm.primary_repl_id());
  meta.store_checkpoint_covered_aof_address = AofAddress::create(1, 100);
  let entry = CheckpointEntry::new(meta);
  rm.checkpoint_store
    .write()
    .add_checkpoint_entry(entry.clone(), true);

  // 副本端点：授予位点 200，初始关闸
  let recover_gate = Arc::new(AtomicBool::new(false));
  let stub = spawn_stub_replica(recover_gate, 200).await;

  let replica_meta = SyncMetadata {
    full_sync: false,
    origin_node_role: NodeRole::Replica,
    origin_node_id: REPLICA_ID,
    current_primary_repl_id: rm.primary_repl_id(),
    current_store_version: 0,
    current_aof_begin_address: AofAddress::create(1, 100),
    current_aof_tail_address: AofAddress::create(1, 200),
    checkpoint_entry: Some(entry.clone()),
  };

  // 验证协商策略确为 PartialResync
  let committed = AofAddress::create(1, 1000);
  let primary_begin = AofAddress::create(1, 0);
  assert!(matches!(
    rm.disk_resync_strategy(&replica_meta, &committed, &primary_begin, false),
    ResyncStrategy::PartialResync { .. }
  ));

  // 发起同步
  let provider_clone = Arc::clone(&provider);
  let assets_clone = Arc::clone(&assets);
  let stub_addr = stub.addr.clone();
  let meta_clone = replica_meta.clone();
  let sync_task = spawn(async move {
    assets_clone
      .sync_session
      .initiate_replica_sync(
        &provider_clone,
        &assets_clone,
        PRIMARY_ID,
        &stub_addr,
        &meta_clone,
      )
      .await
  });

  // 等待进入钳制往返窗口
  assert!(
    wait_for(
      || stub.begin_recover_seen.load(Ordering::SeqCst),
      Duration::from_secs(5),
    )
    .await,
    "主端必须发起 BEGIN_REPLICA_RECOVER 往返"
  );

  // 1. 断言预锁钉线在册且位点为 pin_start (100)
  assert_eq!(rm.aof_sync_driver_store.count(), 1, "预锁驱动必须已入册");
  let drivers = rm.aof_sync_driver_store.drivers();
  assert_eq!(drivers[0].remote_node_id(), REPLICA_ID);
  assert_eq!(
    drivers[0].get_previous_address(0),
    100,
    "预锁位点对齐检查点覆盖位"
  );

  // 2. 钳制往返窗内并发 safe_truncate_aof：推进目标 500 > granted (200) > pin_start (100)
  let clamped = rm
    .aof_sync_driver_store
    .safe_truncate_aof(&AofAddress::create(1, 500))
    .await;
  assert_eq!(
    clamped.get(0),
    Some(100),
    "safe_truncate_aof 必须被预锁驱动钳制在 100"
  );
  let trunc_until = rm.aof_sync_driver_store.get_truncated_until();
  assert_eq!(trunc_until.get(0), Some(100));
  assert!(
    trunc_until.get(0).unwrap() <= 200,
    "truncated_until 恒不越过 granted 位点 200"
  );

  // 3. 开闸放行回传授予位点
  stub.recover_gate.store(true, Ordering::SeqCst);

  // 4. 等待同步全链完成
  let res = sync_task.await.unwrap();
  assert!(res.is_ok(), "attach 必成功: {res:?}");
  assert_eq!(res.unwrap().get(0), Some(200), "授予位点必须对齐 200");

  // 5. 断言 attach 成功后原地置换为授予位点 200
  assert_eq!(rm.aof_sync_driver_store.count(), 1);
  let drivers = rm.aof_sync_driver_store.drivers();
  assert_eq!(drivers[0].remote_node_id(), REPLICA_ID);
  assert_eq!(
    drivers[0].get_previous_address(0),
    200,
    "驱动已原地置换为授予位点 200"
  );

  // 6. 二次截断断言：现在受新位点 200 钳制
  let clamped2 = rm
    .aof_sync_driver_store
    .safe_truncate_aof(&AofAddress::create(1, 500))
    .await;
  assert_eq!(clamped2.get(0), Some(200), "置换后截断线可安全推进至 200");
}

/// 往返失败/超时臂对齐退钉：断言预锁驱动出册、截断线解除钳制
#[compio::test]
async fn partial_resync_clamp_roundtrip_failure_unpins_driver() {
  let dir = tempdir().unwrap();
  let wal_device = Arc::new(SegmentedDevice::single_file(dir.path().join("primary.wal")).unwrap());
  let wal = Arc::new(WalLog::new(wal_device, WalConfig::default()).unwrap());

  let provider = ClusterProvider::new();
  provider.initialize_replication_manager(1, None, false);
  let rm = provider.replication_manager().unwrap();
  let assets = Arc::new(PrimaryReplicationAssets {
    wal: Arc::clone(&wal),
    pump: Arc::new(AofReplicationPump::new(Arc::clone(
      &rm.aof_sync_driver_store,
    ))),
    sync_session: Arc::new(ReplicaSyncSession::new(Arc::clone(&rm))),
  });

  let mut meta = CheckpointMetadata::new(1);
  meta.store_version = 0;
  meta.store_hlog_token = 0x123;
  meta.store_primary_repl_id = Some(rm.primary_repl_id());
  meta.store_checkpoint_covered_aof_address = AofAddress::create(1, 100);
  let entry = CheckpointEntry::new(meta);
  rm.checkpoint_store
    .write()
    .add_checkpoint_entry(entry.clone(), true);

  let replica_meta = SyncMetadata {
    full_sync: false,
    origin_node_role: NodeRole::Replica,
    origin_node_id: REPLICA_ID,
    current_primary_repl_id: rm.primary_repl_id(),
    current_store_version: 0,
    current_aof_begin_address: AofAddress::create(1, 100),
    current_aof_tail_address: AofAddress::create(1, 200),
    checkpoint_entry: Some(entry),
  };

  // 对不可达端点发起同步，往返建连失败
  let res = assets
    .sync_session
    .initiate_replica_sync(&provider, &assets, PRIMARY_ID, "127.0.0.1:1", &replica_meta)
    .await;
  assert!(
    res.as_ref().is_err_and(|e| {
      matches!(
        e,
        ReplicationError::Connect {
          stage: ConnectStage::RecoverRoundtrip,
          ..
        }
      )
    }),
    "往返失败应上抛 RecoverRoundtrip 错误: {res:?}"
  );

  // 退钉断言：预锁驱动必须被 try_remove 摘除
  assert_eq!(
    rm.aof_sync_driver_store.count(),
    0,
    "往返失败后驱动必须出册"
  );

  // 截断线解除钳制：推进目标 500 可直接生效
  let unclamped = rm
    .aof_sync_driver_store
    .safe_truncate_aof(&AofAddress::create(1, 500))
    .await;
  assert_eq!(
    unclamped.get(0),
    Some(500),
    "退钉后截断线不再被历史 pin 阻滞"
  );
}

/// fast_aof_truncate=true 且预锁被拒场景：断言单轮降级 FullResync 收敛不风暴
#[compio::test]
async fn fast_aof_truncate_prelock_rejected_degrades_to_full_resync() {
  let (store_dir, store) = wtest_base::open_test_store("prelock_degrade").unwrap();
  let wal_device =
    Arc::new(SegmentedDevice::single_file(store_dir.path().join("primary.wal")).unwrap());
  let wal = Arc::new(WalLog::new(wal_device, WalConfig::default()).unwrap());
  let cp_dir = store_dir.path().join("checkpoints");
  create_dir_all(&cp_dir).unwrap();
  let options = RuntimeServerOptions::default();
  let (_aof_dirs, backends) = wnode_test::test_sublogs("prelock_degrade_aof", 1);
  let aof = Arc::new(GarnetAppendOnlyFile::new(
    Arc::new(GarnetLog::new(&options, backends, None).expect("构造 GarnetLog")),
    &options,
    None,
  ));
  let db = Arc::new(GarnetDatabase::new(
    0,
    Arc::clone(&store),
    Arc::clone(&store.device),
    cp_dir.clone(),
    Some(aof),
  ));

  let provider = ClusterProvider::new();
  provider.set_store(Arc::clone(&store));
  provider.set_checkpoint_dir(cp_dir.clone());
  provider.set_database_manager(Arc::new(SingleDatabaseManager::new(cp_dir, db)));
  provider.initialize_replication_manager(1, None, false);
  provider.set_fast_aof_truncate(true);
  provider.set_on_demand_checkpoint(true);
  assert!(!provider.allow_data_loss());

  let rm = provider.replication_manager().unwrap();
  rm.set_current_replication_offset(AofAddress::create(1, 350));
  let assets = Arc::new(PrimaryReplicationAssets {
    wal: Arc::clone(&wal),
    pump: Arc::new(AofReplicationPump::new(Arc::clone(
      &rm.aof_sync_driver_store,
    ))),
    sync_session: Arc::new(ReplicaSyncSession::new(Arc::clone(&rm))),
  });

  // 主端旧检查点覆盖位点 100
  let mut meta = CheckpointMetadata::new(1);
  meta.store_version = 0;
  meta.store_hlog_token = 0x123;
  meta.store_primary_repl_id = Some(rm.primary_repl_id());
  meta.store_checkpoint_covered_aof_address = AofAddress::create(1, 100);
  let entry = CheckpointEntry::new(meta);
  rm.checkpoint_store
    .write()
    .add_checkpoint_entry(entry.clone(), true);

  // 截断线已推进至 300（越过旧检查点覆盖位 100）
  rm.aof_sync_driver_store
    .update_truncated_until(&AofAddress::create(1, 300));

  let replica_meta = SyncMetadata {
    full_sync: false,
    origin_node_role: NodeRole::Replica,
    origin_node_id: REPLICA_ID,
    current_primary_repl_id: rm.primary_repl_id(),
    current_store_version: 0,
    current_aof_begin_address: AofAddress::create(1, 100),
    current_aof_tail_address: AofAddress::create(1, 200),
    checkpoint_entry: Some(entry),
  };

  // fast_aof_truncate=true 豁免了 rep_tail < trunc_floor 预检，策略判定 PartialResync
  let committed = AofAddress::create(1, 1000);
  let primary_begin = AofAddress::create(1, 0);
  assert!(matches!(
    rm.disk_resync_strategy(&replica_meta, &committed, &primary_begin, true),
    ResyncStrategy::PartialResync { .. }
  ));

  // 副本端点（开闸放行，授予位点 350）
  let recover_gate = Arc::new(AtomicBool::new(true));
  let stub = spawn_stub_replica(recover_gate, 350).await;

  // 调用 initiate_replica_sync：
  // 1. disk_resync_strategy 协商为 PartialResync
  // 2. 进入 begin_replica_recover_clamp：预锁以 pin_start=100 入库，
  //    因 100 < truncated_until(300) 且 allow_data_loss=false，被 start_gate_ok 拒绝
  // 3. 拒绝后单轮降级 FullResync（send_checkpoint_and_recover）
  // 4. send_checkpoint_and_recover 触发 on-demand checkpoint 重拍
  // 5. 新检查点成功发送至 stub，attach 必成功，单轮收敛不风暴
  let res = assets
    .sync_session
    .initiate_replica_sync(&provider, &assets, PRIMARY_ID, &stub.addr, &replica_meta)
    .await;
  assert!(
    res.is_ok(),
    "预锁被拒后必须平滑降级 FullResync 路径收敛: {res:?}"
  );
  assert_eq!(res.unwrap().get(0), Some(350));
}

/// 默认分支：断言截断越线后协商退化 FullResync 而非静默丢段
#[test]
fn default_branch_truncation_past_tail_negotiates_full_resync() {
  let provider = ClusterProvider::new();
  provider.initialize_replication_manager(1, None, false);
  let rm = provider.replication_manager().unwrap();

  let mut meta = CheckpointMetadata::new(1);
  meta.store_version = 0;
  meta.store_hlog_token = 0x123;
  meta.store_primary_repl_id = Some(rm.primary_repl_id());
  meta.store_checkpoint_covered_aof_address = AofAddress::create(1, 100);
  let entry = CheckpointEntry::new(meta);
  rm.checkpoint_store
    .write()
    .add_checkpoint_entry(entry.clone(), true);

  // 截断线推进至 300
  rm.aof_sync_driver_store
    .update_truncated_until(&AofAddress::create(1, 300));

  let replica_meta = SyncMetadata {
    full_sync: false,
    origin_node_role: NodeRole::Replica,
    origin_node_id: REPLICA_ID,
    current_primary_repl_id: rm.primary_repl_id(),
    current_store_version: 0,
    current_aof_begin_address: AofAddress::create(1, 100),
    current_aof_tail_address: AofAddress::create(1, 200), // < 300
    checkpoint_entry: Some(entry),
  };

  let committed = AofAddress::create(1, 1000);
  let primary_begin = AofAddress::create(1, 0);

  // 默认分支 fast_aof_truncate=false：截断越过从库尾位点时退化 FullResync
  assert!(matches!(
    rm.disk_resync_strategy(&replica_meta, &committed, &primary_begin, false),
    ResyncStrategy::FullResync { .. }
  ));

  // 对照组：fast_aof_truncate=true 豁免预检判 PartialResync
  assert!(matches!(
    rm.disk_resync_strategy(&replica_meta, &committed, &primary_begin, true),
    ResyncStrategy::PartialResync { .. }
  ));
}
