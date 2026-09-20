//! 无盘全量同步闭环收敛集成测试（恢复帧接线 + 回传位点锚定推流起点）
//!
//! 对标 C# diskless 闭环：
//! - 主端恢复握手：libs/cluster/Server/Replication/PrimaryOps/DisklessReplication/
//!   ReplicaSyncSession.cs:BeginAofSyncAsync（构造 primary 元数据经推流连接发
//!   ATTACH_SYNC，以副本回传位点 TryAddReplicationDriver 建驱动）
//! - 副本恢复承接：libs/cluster/Server/Replication/ReplicaOps/ReplicaDisklessSync.cs:
//!   TryReplicaDisklessRecovery（WAL Initialize 对齐 + 复制位点 + ReplicationId 收敛）
//!
//! 链路：主端 try_begin_diskless_sync_async（FullResync 协商 → 快照流 → 推流
//! 连接上发 ATTACH_SYNC primary 元数据 → 副本恢复回传位点 → 以回传位点建驱动
//! 补扫积压）→ 真 socket GarnetServer 副本集群会话承接 → 断言副本恢复位点与
//! 主端对齐、APPENDLOG 记录帧衔接不再 divergent 断流、复制 ID 收敛、增量续推。

use std::{sync::Arc, time::Duration};

use compio::runtime::Runtime;
use waof::{AofAddress, WalConfig, WalLog};
use wdev::SegmentedDevice;
use wedb::server::{
  cluster::IClusterProvider,
  cluster_config::ClusterConfig,
  cluster_manager::ClusterManager,
  cluster_provider::{ClusterProvider, PrimaryReplicationAssets},
  replication::{
    aof_replication_pump::AofReplicationPump,
    cluster_replication_session::ClusterReplicationSession, recovery_status::RecoveryStatus,
    replica_diskless_sync::try_begin_diskless_sync_async, replica_sync_session::ReplicaSyncSession,
    replication_manager::ReplicationManager, sync_metadata::SyncMetadata,
  },
  worker::{LocalWorkerSpec, NodeRole},
};
use wkv::WedbStore;
use wnode::{
  GarnetServer, RespSessionConsumer, SessionProviderFace, WireFormat,
  resp::{garnet_api::StoreGarnetApi, resp_server_session::RespServerSessionOptions},
};
use wtest_base::wait_for;
use wtxn::{TxnLockTable, WatchVersionMap};

/// 测试节点身份（内部 u128；协议面渲染 32 字符小写 hex）
const PRIMARY_ID: u128 = 0x0DE1_0000_0000_0000_0000_0000_0000_0001;
const REPLICA_ID: u128 = 0x0DE1_0000_0000_0000_0000_0000_0000_0002;

/// 独立存储节点（db 文件 + wal）
struct NodeStorage {
  _dir: tempfile::TempDir,
  store: Arc<WedbStore<SegmentedDevice>>,
  wal: Arc<WalLog<SegmentedDevice>>,
}

fn open_node(tag: &str) -> NodeStorage {
  let dir = tempfile::tempdir().unwrap();
  let device =
    Arc::new(SegmentedDevice::single_file(dir.path().join(format!("{tag}.db"))).unwrap());
  let config = wtest_base::test_store_config();
  let store = Arc::new(WedbStore::open(config, device).unwrap());
  let wal_device =
    Arc::new(SegmentedDevice::single_file(dir.path().join(format!("{tag}.wal"))).unwrap());
  let wal = Arc::new(WalLog::new(wal_device, WalConfig::default()).unwrap());
  NodeStorage {
    _dir: dir,
    store,
    wal,
  }
}

/// 带角色 provider（对齐宿主装配：rm/store/wal 全接线，集群配置自持；
/// 副本角色固定挂 primary_1——APPENDLOG init 的主 ID 校验依赖该字段）
fn provider_with_role(
  node: &NodeStorage,
  node_id: u128,
  port: i32,
  role: NodeRole,
) -> Arc<ClusterProvider> {
  let provider = ClusterProvider::new();
  provider.initialize_replication_manager(1, None, false);
  // 关闭 leader 攒批窗口（C# ReplicaDisklessSyncDelay 默认 5 秒；测试直驱
  // 会话入口，无需等同批副本）
  provider.set_replica_diskless_sync_delay(0);
  // 关闭 leader 攒批窗口（C# ReplicaDisklessSyncDelay 默认 5 秒；测试直驱
  // 会话入口，无需等同批副本）
  provider.set_replica_diskless_sync_delay(0);
  let cm = Arc::new(ClusterManager::new(Arc::clone(&provider)));
  let mut config = ClusterConfig::new();
  config.initialize_local_worker(LocalWorkerSpec {
    node_id,
    address: "127.0.0.1",
    port,
    config_epoch: 1,
    role,
    replica_of_node_id: (role == NodeRole::Replica).then_some(PRIMARY_ID),
    hostname: None,
  });
  *cm.current_config.write() = config;
  *provider.cluster_manager.write() = Some(cm);
  provider.set_store(Arc::clone(&node.store));
  provider.set_wal(Arc::clone(&node.wal));
  provider
}

/// 副本宿主会话装配面（每连接新集群会话——对齐宿主 get_session）
struct ReplicaSessionProvider {
  provider: Arc<ClusterProvider>,
}

impl SessionProviderFace for ReplicaSessionProvider {
  type Consumer = RespSessionConsumer;

  fn get_session(
    &self,
    _wire_format: WireFormat,
    _network_sender_id: u64,
  ) -> Option<RespSessionConsumer> {
    let cluster_session = self.provider.create_cluster_session();
    let store = self.provider.try_store()?;
    let mut consumer = RespSessionConsumer::with_cluster(
      1,
      RespServerSessionOptions::default(),
      cluster_session,
      self.provider.provider_handle(),
      Arc::new(StoreGarnetApi::new(store.new_session().ok()?)),
    );
    consumer.attach_transaction_components(Arc::new(WatchVersionMap::new(64)), TxnLockTable::new());
    Some(consumer)
  }
}

/// 主端发起无盘全量同步装配（推流泵与驱动同册， PrimaryReplicationAssets 形态）
fn primary_assets(source: &NodeStorage, rm: &Arc<ReplicationManager>) -> PrimaryReplicationAssets {
  PrimaryReplicationAssets {
    wal: Arc::clone(&source.wal),
    pump: Arc::new(AofReplicationPump::new(Arc::clone(
      &rm.aof_sync_driver_store,
    ))),
    sync_session: Arc::new(ReplicaSyncSession::new(Arc::clone(rm))),
  }
}

/// 非空日志主端 diskless 闭环：恢复帧接线后副本位点与主端对齐、复制 ID
/// 收敛、积压与增量 APPENDLOG 记录帧衔接不再 divergent 断流
#[test]
fn diskless_sync_recovers_replica_offset_and_converges_repl_id() {
  let rt = Runtime::new().unwrap();
  rt.block_on(async {
    // ===== 主端：先落非空日志（排除空日志零位点巧合对齐）
    let source = open_node("diskless_loop_source");
    let provider_p = provider_with_role(&source, PRIMARY_ID, 7000, NodeRole::Primary);
    for i in 0..3 {
      source
        .wal
        .enqueue(format!("loop-backlog-{i}").as_bytes())
        .unwrap();
    }
    let primary_tail = source.wal.tail_address() as i64;
    assert!(primary_tail > 0, "主端日志必须非空");

    // ===== 副本：带旧本地日志（旧地址空间 tail > 0）+ 全接线 + 宿主服务器
    //（恢复帧 safe_initialize 重置对齐是衔接前提；无恢复帧时旧尾位与主端
    // 首帧必然 divergent 断流，测试据此区分新旧行为）
    let replica = open_node("diskless_loop_replica");
    for i in 0..2 {
      replica
        .wal
        .enqueue(format!("loop-stale-{i}").as_bytes())
        .unwrap();
    }
    assert!(replica.wal.tail_address() > 0, "副本旧地址空间必须非空");
    let provider_r = provider_with_role(&replica, REPLICA_ID, 7001, NodeRole::Replica);
    provider_r.set_replica_replication_session(Some(Arc::new(ClusterReplicationSession::new(
      Arc::clone(&provider_r),
      Arc::clone(&replica.wal),
      None,
    ))));
    let server = GarnetServer::new(
      &["127.0.0.1:0".to_string()],
      65536,
      100,
      Arc::new(ReplicaSessionProvider {
        provider: Arc::clone(&provider_r),
      }),
    )
    .unwrap();
    server.start(None).unwrap();
    let replica_addr = server.local_addr().unwrap().to_string();

    let rm_p = provider_p.replication_manager().unwrap();
    let rm_r = provider_r.replication_manager().unwrap();
    let primary_repl_id = rm_p.primary_repl_id();
    assert_ne!(
      rm_r.primary_repl_id(),
      primary_repl_id,
      "独立节点复制 ID 初值必不同"
    );
    // 副本握手完成后的读角色门控（对标 C# TryBeginReplicaSyncAsync 尾态
    // EndRecovery(ReadRole, downgradeLock: true)；恢复帧 end_recovery 的
    // ReadRole → CheckpointRecoveredAtReplica 为合法迁移）
    assert!(
      rm_r.begin_recovery(RecoveryStatus::ReadRole, false),
      "副本恢复门控就位"
    );

    // ===== 主端发起无盘全量同步（无检查点历史 + 副本零位点 → FullResync）
    let assets = primary_assets(&source, &rm_p);
    let meta = SyncMetadata {
      full_sync: false,
      origin_node_role: NodeRole::Replica,
      origin_node_id: REPLICA_ID,
      current_primary_repl_id: rm_r.primary_repl_id(),
      current_store_version: 0,
      current_aof_begin_address: AofAddress::create(1, 0),
      current_aof_tail_address: AofAddress::create(1, 0),
      current_replication_offset: AofAddress::create(1, 0),
      checkpoint_entry: None,
    };
    let sync_from =
      try_begin_diskless_sync_async(&provider_p, &assets, PRIMARY_ID, &replica_addr, &meta)
        .await
        .unwrap();

    // 恢复帧回传位点：FullResync 副本位点收敛到主端快照覆盖锚（扫描前尾
    // = primary_tail，锚前积压记录已含快照不再重放，AOF 恰从锚续推）
    assert_eq!(
      sync_from.get(0),
      Some(primary_tail),
      "副本恢复位点必经 ATTACH_SYNC 回传并收敛到主端授予锚"
    );

    // 复制 ID 收敛（恢复帧 try_update_my_primary_repl_id 生效）
    assert_eq!(
      rm_r.primary_repl_id(),
      primary_repl_id,
      "副本主复制 ID 必须经恢复帧收敛为主端 ID"
    );

    // WAL 地址空间对齐：副本日志从锚重置后与主端尾严格衔接（恢复帧未接线
    // 时副本保留旧地址空间尾，首个记录帧即 divergent 断流）
    let converged = wait_for(
      || replica.wal.tail_address() as i64 == primary_tail,
      Duration::from_secs(5),
    )
    .await;
    assert!(converged, "副本 WAL 尾必须追平主端（积压记录帧全量落盘）");
    assert_eq!(
      rm_r.get_current_replication_offset().get(0),
      Some(primary_tail),
      "副本复制位点必须推进到主端尾"
    );
    let driver = rm_p
      .aof_sync_driver_store
      .drivers()
      .into_iter()
      .find(|d| d.remote_node_id() == REPLICA_ID)
      .expect("推流驱动在册");
    assert!(
      driver.is_connected(),
      "推流驱动必须保持连接（记录帧衔接通过，未触发 divergent 断流）"
    );

    // ===== 增量续推衔接：主端追加记录，副本位点继续推进（divergent 断流则冻结）
    for i in 0..2 {
      source
        .wal
        .enqueue(format!("loop-live-{i}").as_bytes())
        .unwrap();
    }
    let new_tail = source.wal.tail_address() as i64;
    let _ = assets.pump.sync_backlog(&source.wal).await;
    let caught_up = wait_for(
      || rm_r.get_current_replication_offset().get(0) == Some(new_tail),
      Duration::from_secs(5),
    )
    .await;
    assert!(caught_up, "增量记录帧必须衔接落位（副本位点持续推进）");
    assert_eq!(replica.wal.tail_address() as i64, new_tail);

    server.dispose();
  });
}
