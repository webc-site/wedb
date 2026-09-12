//! 集群端到端网络复制推流与位点 ACK 闭环集成测试
//!
//! 深度对标 Garnet C#:
//! - Primary: AofSyncTask + AofSyncDriver + AofReplicationPump (BulkConsume/attach_sink)
//! - Wire: encode_append_log_init_frame (-1/-1/-1) + encode_append_log_frame
//! - Replica: ClusterReplicationSession (NetworkClusterAppendLog + ProcessPrimaryStream)
//! - ACK: ReplicaReplayDriver (create_replication_ack) -> ReplicationManager (handle_replica_ack)

use std::sync::Arc;

use compio::runtime::Runtime;
use waof::{AofAddress, WalConfig, WalLog};
use wdev::SegmentedDevice;
use wedb::server::{
  cluster_config::ClusterConfig,
  cluster_manager::ClusterManager,
  cluster_provider::ClusterProvider,
  replication::{
    aof_replication_pump::AofReplicationPump,
    aof_sync_driver::AofSyncDriver,
    cluster_replication_session::ClusterReplicationSession,
    driver_registry::DriverLifecycle,
    replica_wire::{CallbackWire, encode_append_log_init_frame},
    replication_manager::ReplicationManager,
  },
  worker::{LocalWorkerSpec, NodeRole, Worker},
};
use wnode::MessageConsumerFace;

fn create_wal(dir: &tempfile::TempDir, name: &str) -> Arc<WalLog<SegmentedDevice>> {
  let path = dir.path().join(name);
  let dev = Arc::new(SegmentedDevice::single_file(path).expect("create device"));
  Arc::new(WalLog::new(dev, WalConfig::default()).expect("create wal"))
}

fn setup_replica_provider(
  local_id: &str,
  primary_id: &str,
) -> (Arc<ClusterProvider>, Arc<ReplicationManager>) {
  let provider = Arc::new(ClusterProvider::default());
  provider.initialize_replication_manager();
  let rm = provider.replication_manager().expect("rm ready");

  let cm = Arc::new(ClusterManager::new(provider.clone()));
  let mut config = ClusterConfig::new();
  config.initialize_local_worker(LocalWorkerSpec {
    node_id: local_id,
    address: "127.0.0.1",
    port: 7001,
    config_epoch: 1,
    role: NodeRole::Replica,
    replica_of_node_id: Some(primary_id),
    hostname: None,
  });
  config.workers.push(Worker {
    nodeid: Some(primary_id.to_string()),
    address: "127.0.0.1".to_string(),
    port: 7000,
    config_epoch: 1,
    role: NodeRole::Primary,
    replica_of_node_id: None,
    replication_offset: 0,
    hostname: None,
  });
  *cm.current_config.write() = config;
  *provider.cluster_manager.write() = Some(cm);

  (provider, rm)
}

#[test]
fn test_replication_full_chain_stream_and_ack() {
  Runtime::new().unwrap().block_on(async {
    let dir = tempfile::tempdir().unwrap();
    let primary_wal = create_wal(&dir, "primary.wal");
    let replica_wal = create_wal(&dir, "replica.wal");

    let primary_id = "primary-node-1";
    let replica_id = "replica-node-1";

    let primary_mgr = ReplicationManager::with_options(1, None);
    let (replica_provider, replica_mgr) = setup_replica_provider(replica_id, primary_id);

    // 1. 初始化 Replica 接收端会话
    let replica_session =
      ClusterReplicationSession::new(replica_provider.clone(), replica_wal.clone(), None);

    // 2. 发送握手帧 (-1/-1/-1)，对标 C# ExecuteClusterAppendLogInit
    let init_frame = encode_append_log_init_frame(primary_id, 0, -1, -1, -1);
    let (consumed, resp) = replica_session.try_consume_messages(&init_frame);
    assert_eq!(consumed, init_frame.len());
    assert_eq!(resp, b"+OK\r\n", "握手应答必须为 +OK");
    assert!(
      replica_mgr.has_active_replication_stream(),
      "握手成功后副本标记活跃复制流"
    );

    let replica_replay_driver = replica_mgr
      .replica_replay_driver_store
      .get_replay_driver(0)
      .expect("重放驱动必须已注册");

    // 3. 构建 Primary 端同步驱动，接线 CallbackWire 直通 Replica 会话
    let session_clone = replica_session.clone();
    let wire = Arc::new(CallbackWire::new(move |frame: &[u8]| {
      let (cons, _) = session_clone.try_consume_messages(frame);
      cons == frame.len()
    }));

    let primary_driver_store = primary_mgr.aof_sync_driver_store.clone();
    let sync_driver = Arc::new(AofSyncDriver::new(
      primary_id.to_string(),
      replica_id.to_string(),
      &AofAddress::create(1, 0),
    ));
    sync_driver.attach_wire(wire);
    assert!(primary_driver_store.try_add_replication_driver(sync_driver.clone(), false));

    // 4. 挂接推流泵到 Primary WAL
    let pump = AofReplicationPump::new(primary_driver_store.clone());
    assert!(pump.attach_sink(&primary_wal));

    // 5. 主端连续写入业务记录：推流端口同栈逐帧发送到从端
    let records = [
      b"SET user:1 alice".as_slice(),
      b"SET user:2 bob".as_slice(),
      b"DEL user:1".as_slice(),
    ];

    for r in &records {
      primary_wal.enqueue(r).expect("enqueue primary wal");
    }
    primary_wal.commit().await.expect("commit primary wal");
    let primary_tail = primary_wal.tail_address() as i64;
    assert!(primary_tail > 0);

    // 6. 验证从端 WAL 与位点闭环
    assert_eq!(
      replica_wal.tail_address() as i64,
      primary_tail,
      "副本 WAL 尾部应严格等于主端写入尾"
    );
    assert_eq!(
      replica_mgr.get_replication_offset(0),
      primary_tail,
      "副本复制位点应同步推进至日志尾"
    );
    assert_eq!(
      replica_replay_driver.replayed_offset(),
      primary_tail,
      "副本直接重放驱动位点应追平"
    );

    // 7. 从端生成 ReplicationAck 并向主端上报
    let ack = replica_replay_driver.create_replication_ack(replica_id);
    assert_eq!(ack.node_id, replica_id);
    assert_eq!(ack.physical_sublog_idx, 0);
    assert_eq!(ack.acked_offset, primary_tail);

    assert!(primary_mgr.handle_replica_ack(
      &ack.node_id,
      ack.physical_sublog_idx,
      ack.acked_offset,
    ));
    assert_eq!(
      sync_driver.get_acked_address(0),
      primary_tail,
      "主端记录的副本已确认 ACK 位点必须对齐"
    );

    // 8. 验证安全截断水位
    let safe_cut = primary_driver_store.safe_truncate_sublog(primary_tail + 1000, 0, i64::MAX);
    assert_eq!(
      safe_cut, primary_tail,
      "安全截断水位应受副本 ACK 确认位点约束"
    );

    // 9. 节流机制测试
    let task = sync_driver.get_task(0).unwrap();
    let wm = task.throttle(10);
    assert!(wm.is_some(), "增量超过门限触发节流报备");

    // 10. 资源释放与断开
    sync_driver.dispose();
    assert!(!sync_driver.is_connected());
  });
}
