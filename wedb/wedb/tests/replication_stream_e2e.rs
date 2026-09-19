//! 集群端到端网络复制推流与位点闭环集成测试
//!
//! 深度对标 Garnet C#:
//! - Primary: AofSyncTask + AofSyncDriver + AofReplicationPump (信号唤醒增量拉取)
//! - Wire: encode_append_log_init_frame (-1/-1/-1) + encode_append_log_frame
//! - Replica: ClusterReplicationSession (NetworkClusterAppendLog + ProcessPrimaryStream)

use std::{sync::Arc, time::Duration};

use compio::runtime::Runtime;
use waof::{AofAddress, WalConfig, WalLog};
use wconn::session::encode_append_log_init_frame;
use wdev::SegmentedDevice;
use wedb::server::{
  cluster_config::ClusterConfig,
  cluster_manager::ClusterManager,
  cluster_provider::ClusterProvider,
  replication::{
    aof_replication_pump::AofReplicationPump, aof_sync_driver::AofSyncDriver,
    cluster_replication_session::ClusterReplicationSession, replica_wire::CallbackWire,
    replication_manager::ReplicationManager,
  },
  worker::{LocalWorkerSpec, NodeRole, Worker},
};
use wnode::MessageConsumerFace;
use wtest_base::wait_for;

fn create_wal(dir: &tempfile::TempDir, name: &str) -> Arc<WalLog<SegmentedDevice>> {
  let path = dir.path().join(name);
  let dev = Arc::new(SegmentedDevice::single_file(path).expect("create device"));
  Arc::new(WalLog::new(dev, WalConfig::default()).expect("create wal"))
}

fn setup_replica_provider(
  local_id: u128,
  primary_id: u128,
) -> (Arc<ClusterProvider>, Arc<ReplicationManager>) {
  let provider = Arc::new(ClusterProvider::default());
  provider.initialize_replication_manager(1, None, false);
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
    nodeid: Some(primary_id),
    address: "127.0.0.1".into(),
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
fn test_replication_full_chain_stream() {
  Runtime::new().unwrap().block_on(async {
    let dir = tempfile::tempdir().unwrap();
    let primary_wal = create_wal(&dir, "primary.wal");
    let replica_wal = create_wal(&dir, "replica.wal");

    // 节点 id 内部为 u128；协议帧面为 32 字符小写 hex
    let primary_id = 0x0DE1_0000_0000_0000_0000_0000_0000_0001u128;
    let replica_id = 0x0DE1_0000_0000_0000_0000_0000_0000_0002u128;

    let primary_mgr = ReplicationManager::with_options(1, None, false);
    let (replica_provider, replica_mgr) = setup_replica_provider(replica_id, primary_id);

    // 1. 初始化 Replica 接收端会话
    let mut replica_session =
      ClusterReplicationSession::new(replica_provider.clone(), replica_wal.clone(), None);

    // 2. 发送握手帧 (-1/-1/-1)，对标 C# ExecuteClusterAppendLogInit
    let init_frame = encode_append_log_init_frame(&format!("{primary_id:032x}"), 0, -1, -1, -1);
    let mut resp = Vec::new();
    replica_session.recv_buffer.extend_from_slice(&init_frame);
    let remaining = replica_session.try_consume_messages_into(&mut resp);
    assert_eq!(remaining, Some(0));
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
    let wire = Arc::new(CallbackWire::new(replica_session.clone()));

    let primary_driver_store = primary_mgr.aof_sync_driver_store.clone();
    let sync_driver = Arc::new(AofSyncDriver::new(
      primary_id,
      replica_id,
      1,
      &AofAddress::create(1, 0),
      None,
    ));
    sync_driver.attach_wire(wire);
    assert!(primary_driver_store.try_add_replication_driver(sync_driver.clone(), false));

    // 4. 挂接推流泵到 Primary WAL（入队信号唤醒增量拉取）
    let pump = AofReplicationPump::new(primary_driver_store.clone());
    assert!(pump.attach_wake(&primary_wal));

    // 5. 主端连续写入业务记录：唤醒循环按地址序拉取并转发到从端
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
    assert!(primary_tail > 0, "主端提交后日志尾必须前移");

    // 唤醒循环为独立 compio 任务：等其把全部帧（含 commit 元数据帧）转发落
    // 从端后再断言。追平面取三面同步位点——副本日志尾、副本复制位点、
    // 主端已发水位；泵在末帧 consume 的同一非挂起段内即调 throttle_replica
    // 报备水位，故三面追平后步骤 7/8 的断言不再依赖让出时长（慢机不再 flaky）
    let synced = wait_for(
      || {
        replica_wal.tail_address() as i64 >= primary_tail
          && replica_mgr.get_replication_offset(0) >= primary_tail
          && sync_driver.get_previous_address(0) >= primary_tail
      },
      Duration::from_secs(5),
    )
    .await;
    assert!(
      synced,
      "复制链路未在超时内追平：主端尾 {primary_tail}、副本尾 {}、副本位点 {}、主端已发水位 {}",
      replica_wal.tail_address() as i64,
      replica_mgr.get_replication_offset(0),
      sync_driver.get_previous_address(0)
    );

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
    // 退化形态（无重放资产）：驱动注册但不启动背景重放，位点由会话
    // 落盘面 enqueued 直推（上方位点断言已覆盖权威面）
    assert!(
      !replica_replay_driver.background_replay_started(),
      "退化形态不启动背景重放"
    );

    // 7. 验证安全截断水位（对标 C# SafeTruncateAof：返回受在册副本
    // 已发位点收敛后的安全水位；未 attach 日志句柄时退化为纯记账）
    let safe_cut = primary_driver_store
      .safe_truncate_aof(&AofAddress::create(1, primary_tail + 1000))
      .await;
    assert_eq!(
      safe_cut.get(0),
      Some(primary_tail),
      "安全截断水位应受在册副本已发位点约束"
    );

    // 8. 节流机制测试：信号化拉取下报备由 pump_backlog 自动履行
    //（throttle_replica 以 publish_delta=1 门限即时发布），此处验证
    // 高水位报备的幂等语义——已发布水位不得重复报备
    let task = sync_driver.task_ref(0).unwrap();
    let wm = task.throttle(10);
    assert_eq!(wm, None, "已报备水位不得重复发布");

    // 9. 资源释放与断开
    sync_driver.dispose();
    assert!(!sync_driver.is_connected());
  });
}
