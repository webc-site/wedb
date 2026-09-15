//! 复制数据面生产装配集成测试
//!
//! 验证 wedb::server::replication::assembly 的生产装配体
//!（[`wire_replication_data_plane`]）与副本重连动作
//!（ensure_replication 后台直调 recover_replication）在真 TCP 双节点
//! 拓扑下点亮复制数据面：
//! 副本 REPLICAOF → ensure_replication 自动发 INITIATE_REPLICA_SYNC
//! → 主端回连副本建 APPENDLOG 推流 → 主端 SET 经推流链路落盘至副本。
//!
//! 对标 C# 装配：ReplicationManager 构造期 storeWrapper 反查装配
//!（libs/cluster/Server/Replication/ReplicationManager.cs）+ 副本发起
//! ReplicaSyncAttachTaskAsync（ReplicaDiskbasedSync.cs:181）。
//! 未装配资产时主端 arm 回 CLUSTER NOT INITIALIZED（基线缺陷对照测试
//! primary_arm_without_assets_reports_not_initialized）。

use std::{sync::Arc, time::Duration};

use aok::{Error, Result, Void};
use compio::runtime::Runtime;
use tempfile::tempdir;
use waof::{WalConfig, WalLog, WalScanIterator};
use wconn::GarnetClient;
use wdev::SegmentedDevice;
use wedb::server::{
  cluster::IClusterProvider,
  cluster_config::ClusterConfig,
  cluster_provider::ClusterProvider,
  cluster_session::ClusterSession,
  hash_slot::SlotState,
  replication::{checkpoint_entry::CheckpointEntry, wire_replication_data_plane},
  worker::{LOCAL_WORKER_ID, LocalWorkerSpec, NodeRole, Worker},
};
use wedb_test::{test_store_config, wait_for};
use wkv::WedbStore;
use wnode::{
  GarnetServer, MessageConsumerFace, RespSessionConsumer,
  resp::{garnet_api::StoreGarnetApi, resp_server_session::RespServerSessionOptions},
  service::StorageSessionProvider,
};
use wtxn::WatchVersionMap;

/// 集群形态 maxDatabases（C# EnableCluster 构造）
const CLUSTER_MAX_DATABASES: i32 = 2;

/// 起一个生产宿主形态节点（随机端口；宏承接节点各自闭包的单态化类型，
/// 泛型辅助函数无法为匿名闭包命名返回类型）
macro_rules! start_node {
  ($provider:expr) => {{
    let provider = $provider;
    let cluster = Arc::clone(&provider);
    let decorate = move |network_sender_id: u64, api: StoreGarnetApi<SegmentedDevice>| {
      let options = RespServerSessionOptions {
        max_databases: CLUSTER_MAX_DATABASES,
        ..RespServerSessionOptions::default()
      };
      Some(RespSessionConsumer::with_cluster_session(
        network_sender_id,
        options,
        cluster.create_cluster_session(),
        api,
      ))
    };
    let dir = tempdir()?;
    let session_provider = StorageSessionProvider::open_with_config_and_aof(
      test_store_config(),
      dir.path().join("node.db"),
      None,
      None,
      decorate,
    )?;
    let wal = session_provider
      .wal()
      .cloned()
      .expect("AOF 门控点亮后 wal 必在场");
    let server = GarnetServer::new(
      &["127.0.0.1:0".to_string()],
      65536,
      100,
      Arc::new(session_provider),
    );
    server.start(None)?;
    let port = server.local_addr()?.port();
    (dir, server, provider, wal, port)
  }};
}

/// RESP 客户端往返字符串应答（建连 + 单命令）
async fn client_roundtrip(endpoint: &str, command: &[&str]) -> Result<String> {
  let mut client = GarnetClient::new(endpoint.to_string(), None, None, Some("test".into()), 32);
  client.connect_async().await?;
  let resp = client.execute_for_string_result_async(command).await;
  resp.map_err(Error::from)
}

/// 副本复制链路建立等待面：重放驱动在册（主端 init 帧握手注册）即流已建立
fn replica_stream_active(provider: &ClusterProvider) -> bool {
  provider
    .replication_manager()
    .is_some_and(|rm| rm.has_active_replication_stream())
}

/// 主端全槽指派（worker_id 为本地 worker）
fn assign_all_slots(config: &mut ClusterConfig) {
  let slots: Vec<usize> = (0..16384).collect();
  config.assign_slots(&slots, LOCAL_WORKER_ID as u16, SlotState::Stable);
}

/// 远程 worker 条目（互指拓扑）
fn remote_worker(node_id: &str, port: i32) -> Worker {
  Worker {
    nodeid: Some(node_id.to_string()),
    address: "127.0.0.1".to_string(),
    port,
    config_epoch: 1,
    role: NodeRole::Primary,
    replica_of_node_id: None,
    replication_offset: 0,
    hostname: None,
  }
}

/// 真 TCP 双节点冒烟（生产装配体）：副本 REPLICAOF 主端 → ensure_replication
/// 钩子自动发起同步 → 主端 SET → 副本日志字节级追平。未装配数据面的基线
/// 上复制流永不建立（等待处超时失败——红）
/// 泵等价消费（直填会话接收缓冲 → 唯一入口 → 应答取出）
/// 返回 (消费后残余, 应答)：Some(0) = 完整消费，None = 协议违规
fn pump(consumer: &mut RespSessionConsumer, frame: &[u8]) -> (Option<usize>, Vec<u8>) {
  let mut scratch = consumer.take_recv_scratch();
  scratch.extend_from_slice(frame);
  consumer.return_recv_scratch(scratch);
  let mut resp = Vec::new();
  let remaining = consumer.try_consume_messages_into(&mut resp);
  (remaining, resp)
}

#[test]
fn production_assembly_replicates_over_real_tcp() -> Void {
  Runtime::new().unwrap().block_on(async {
    // ===== 双节点生产宿主装配（session_factory + AOF + 监听，同 main.rs）
    let (pdir, pserver, pprovider, pwal, pport) = start_node!(ClusterProvider::new());
    let (rdir, rserver, rprovider, rwal, rport) = start_node!(ClusterProvider::new());

    // ===== 集群拓扑配置（互指真实端口；主端持全槽）
    {
      let cm = pprovider.cluster_manager().expect("cm");
      let mut config = cm.current_config.write();
      config.initialize_local_worker(LocalWorkerSpec {
        node_id: "primary_node",
        address: "127.0.0.1",
        port: pport as i32,
        config_epoch: 1,
        role: NodeRole::Primary,
        replica_of_node_id: None,
        hostname: None,
      });
      config
        .workers
        .push(remote_worker("replica_node", rport as i32));
      assign_all_slots(&mut config);
    }
    {
      let cm = rprovider.cluster_manager().expect("cm");
      let mut config = cm.current_config.write();
      config.initialize_local_worker(LocalWorkerSpec {
        node_id: "replica_node",
        address: "127.0.0.1",
        port: rport as i32,
        config_epoch: 1,
        role: NodeRole::Primary,
        replica_of_node_id: None,
        hostname: None,
      });
      config
        .workers
        .push(remote_worker("primary_node", pport as i32));
    }

    // ===== 复制数据面装配（生产唯一装配体；宿主 main.rs 同款调用）
    wire_replication_data_plane(&pprovider, Arc::clone(&pwal));
    wire_replication_data_plane(&rprovider, Arc::clone(&rwal));

    // ===== 副本重连轮询频率（宿主 main.rs 同款装配；断链时
    // ensure_replication 后台直调 recover_replication）
    rprovider.set_replication_reestablishment_timeout(1);

    // ===== REPLICAOF 指派主端（配置面翻转）
    let resp = client_roundtrip(
      &format!("127.0.0.1:{}", rport),
      &["REPLICAOF", "127.0.0.1", &pport.to_string()],
    )
    .await?;
    assert_eq!(resp, "OK", "REPLICAOF 应成功");
    assert!(
      rprovider
        .cluster_manager()
        .is_some_and(|cm| cm.current_config().is_replica()),
      "REPLICAOF 后配置角色应为 REPLICA"
    );
    assert!(
      !replica_stream_active(&rprovider),
      "数据面尚未建立（基线在此永远保持本状态——缺陷本体）"
    );

    // ===== gossip 健康检查驱动等价入口：断链应触发自动重连
    rprovider.ensure_replication(Some("primary_node"));

    // ===== 复制流建立（INITIATE_REPLICA_SYNC → 主端回连 init 握手注册驱动）
    assert!(
      wait_for(
        || replica_stream_active(&rprovider),
        Duration::from_secs(10)
      )
      .await,
      "复制流应经生产装配自动建立"
    );

    // ===== 主端 SET（RESP 写入 → AOF 镜像 → 推流链路）
    let resp = client_roundtrip(
      &format!("127.0.0.1:{}", pport),
      &["SET", "assembly_smoke_key", "assembly_smoke_value"],
    )
    .await?;
    assert_eq!(resp, "OK", "主端 SET 应成功");
    assert!(
      wait_for(
        || rwal.tail_address() == pwal.tail_address(),
        Duration::from_secs(10)
      )
      .await,
      "副本日志尾应经真 socket 追平主端"
    );

    // ===== 副本落盘字节级保真 + 复制位点推进
    let collect = |wal: &WalLog<SegmentedDevice>| {
      let mut iter: WalScanIterator<SegmentedDevice> =
        wal.scan(wal.begin_address(), wal.tail_address());
      async move {
        iter
          .collect_all()
          .await
          .expect("日志扫描不可失败")
          .into_iter()
          .map(|r| (r.address, r.reconstruct_frame()))
          .collect::<Vec<_>>()
      }
    };
    let pri = collect(&pwal).await;
    let rep = collect(&rwal).await;
    assert!(!pri.is_empty(), "主端应含 SET 记录");
    assert_eq!(pri, rep, "副本记录帧必须与主端逐条字节一致");
    let tail = pwal.tail_address() as i64;
    assert!(
      wait_for(
        || {
          rprovider
            .replication_manager()
            .map(|rm| rm.get_replication_offset(0))
            == Some(tail)
        },
        Duration::from_secs(10)
      )
      .await,
      "副本复制位点应上报推进至主端日志尾"
    );

    pserver.dispose();
    rserver.dispose();
    let _ = (pdir, rdir);
    Ok(())
  })
}

/// 主端 arm 缺陷对照：未注入推流资产时 CLUSTER INITIATE_REPLICA_SYNC 回
/// CLUSTER NOT INITIALIZED（基线行为；装配后由 arm 装配测试覆盖）
#[test]
fn primary_arm_without_assets_reports_not_initialized() -> Void {
  Runtime::new().unwrap().block_on(async {
    let provider = ClusterProvider::new();
    let cluster_session = provider.create_cluster_session();
    let mut consumer = cluster_consumer(&provider, Arc::clone(&cluster_session));

    let frame = initiate_frame();
    // 资产缺位为同步拒绝路径（不登记慢路径），错误直书输出
    let (_, out) = pump(&mut consumer, &frame);
    assert!(
      out.starts_with(b"-ERR Cluster not initialized"),
      "未装配资产时 arm 必须同步回 NOT INITIALIZED: {}",
      String::from_utf8_lossy(&out)
    );
    Ok(())
  })
}

/// 装配后主端 arm：INITIATE_REPLICA_SYNC 通过资产在位检查（不再回
/// NOT INITIALIZED），进入策略协商与副本建连段（副本 endpoint 不可达 →
/// 建连错误透出，证明慢路径真实驱动了同步流程）
#[test]
fn primary_arm_after_wiring_proceeds_past_not_initialized() -> Void {
  Runtime::new().unwrap().block_on(async {
    let dir = tempdir()?;
    let provider = ClusterProvider::new();
    {
      let cm = provider.cluster_manager().expect("cm");
      let mut config = cm.current_config.write();
      config.initialize_local_worker(LocalWorkerSpec {
        node_id: "primary_node",
        address: "127.0.0.1",
        port: 7000,
        config_epoch: 1,
        role: NodeRole::Primary,
        replica_of_node_id: None,
        hostname: None,
      });
      // 副本 endpoint 指向不可达端口 1（arm 通过资产检查后建连失败回错误文案）
      config.workers.push(Worker {
        nodeid: Some("replica_node".to_string()),
        address: "127.0.0.1".to_string(),
        port: 1,
        config_epoch: 1,
        role: NodeRole::Primary,
        replica_of_node_id: None,
        replication_offset: 0,
        hostname: None,
      });
    }

    // 生产装配体注入
    let device = Arc::new(SegmentedDevice::single_file(dir.path().join("wal.log"))?);
    let wal = Arc::new(WalLog::new(device, WalConfig::default())?);
    wire_replication_data_plane(&provider, wal);
    assert!(
      provider.try_primary_replication().is_some()
        && provider.try_replica_replication_session().is_some()
        && provider.try_wal().is_some(),
      "装配后三类资产应在位"
    );

    let cluster_session = provider.create_cluster_session();
    let mut consumer = cluster_consumer(&provider, Arc::clone(&cluster_session));

    let frame = initiate_frame();
    let (_, out) = pump(&mut consumer, &frame);
    assert!(out.is_empty());
    let slow = consumer.take_slow_wait().expect("arm 应登记慢路径执行体");
    let out = slow.resolve().await;
    let text = String::from_utf8_lossy(&out).to_string();
    assert!(
      !text.contains("CLUSTER NOT INITIALIZED"),
      "装配后 arm 不得回 NOT INITIALIZED: {text}"
    );
    assert!(
      text.starts_with("-ERR Failed connecting to replica for aofSync"),
      "arm 应推进到副本建连段: {text}"
    );
    Ok(())
  })
}

/// 集群会话消费者装配（StoreGarnetApi + 集群切面；arm 单测形态）
fn cluster_consumer(
  provider: &ClusterProvider,
  cluster_session: Arc<ClusterSession>,
) -> RespSessionConsumer {
  let device =
    Arc::new(SegmentedDevice::single_file(tempdir().unwrap().path().join("arm.db")).unwrap());
  // 小预算测试配置（对标 C# 16MB 基线），GC 关闭保持历史语义
  let mut config = test_store_config();
  config.gc.enabled = false;
  let store = Arc::new(WedbStore::open(config, device).unwrap());
  provider.set_store(Arc::clone(&store));
  let mut consumer = RespSessionConsumer::with_cluster_session(
    1,
    RespServerSessionOptions {
      max_databases: 2,
      ..RespServerSessionOptions::default()
    },
    cluster_session,
    Arc::new(StoreGarnetApi::new(store.new_session().unwrap())),
  );
  consumer.attach_transaction_components(Arc::new(WatchVersionMap::new(64)));
  consumer
}

/// CLUSTER INITIATE_REPLICA_SYNC 5 参帧（副本节点 id、指派主 repl id、
/// 检查点条目、副本 AOF begin/tail span）
fn initiate_frame() -> Vec<u8> {
  let replid: &[u8] = b"0123456789abcdef0123456789abcdef01234567";
  let checkpoint = CheckpointEntry::with_sublogs(1).to_byte_array();
  let mut frame = format!(
    "*7\r\n$7\r\nCLUSTER\r\n$21\r\nINITIATE_REPLICA_SYNC\r\n$12\r\nreplica_node\r\n${}\r\n",
    replid.len()
  )
  .into_bytes();
  frame.extend_from_slice(replid);
  frame.extend_from_slice(format!("\r\n${}\r\n", checkpoint.len()).as_bytes());
  frame.extend_from_slice(&checkpoint);
  // begin/tail span：单槽 8B LE（C# AofAddress.Span）
  for addr in [0i64, 0] {
    frame.extend_from_slice(b"\r\n$8\r\n");
    frame.extend_from_slice(&addr.to_le_bytes());
  }
  frame.extend_from_slice(b"\r\n");
  frame
}
