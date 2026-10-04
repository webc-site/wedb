//! 复制链路认证握手集成测试（对标 C# test/cluster/Garnet.test.cluster/
//! ClusterAuthCommsTests.cs:ClusterReplicationAuth）
//!
//! C# 场景：全节点装 ACL（requirepass 同位物），集群互信凭据
//!（cluster-username / cluster-password）配置后，副本 attach 主端全链认证：
//! 副本发起客户端（INITIATE_REPLICA_SYNC）向主端 AUTH，主端推流客户端
//!（快照下发 / AofSync 建连）向副本 AUTH；凭据缺失即拒。
//!
//! rust 拓扑同构（行为等价说明）：
//! - 认证门在「被拨入端」：rust 复制链路主端回连副本推流
//!   （libs/cluster AofSyncTask 对位，wedb/src/server/replication/replica_wire.rs:211
//!   `TcpSessionWire::connect` 的 `auth` 形参），故两侧节点均配 requirepass、
//!   两侧 provider 均经 `ClusterProvider::update_cluster_auth` 播种互信凭据
//!   （wedb/tests/cluster_auth_startup.rs 已单测播种面，本册测传输面）；
//! - 凭据单源：副本发起臂 `recover_replication` 的 GarnetClient::with_auth 与
//!   主端回连臂 `connect_replica_stream_wire` 的 TcpSessionWire::connect 均现取
//!   provider.cluster_username / cluster_password（C# AuthContainer 同位）。
//!
//! 三册断言：
//! 1. wire 级三臂：不带凭据 init 帧被 -NOAUTH 拒（文案经客户端错误路径透传）、
//!    错凭据 AUTH 握手 WRONGPASS、
//!    正确凭据建连 + init 握手 +OK（replica_wire.rs:211 auth 形参直测）；
//! 2. 带凭据端到端：REPLICAOF → OK → 复制流建立 → 主端 SET 经推流落盘副本；
//! 3. 不带凭据端到端：REPLICAOF 回 -ERR NOAUTH，复制流永不建立。

use std::num::NonZeroUsize;

use wedb::server::cluster_config::ClusterConfig;
use wnode::service::SharedStore;
#[path = "common/replica_net.rs"]
mod replica_net;
use std::{sync::Arc, time::Duration};

use aok::{Result, Void};
use replica_net::{client_roundtrip, network_pool};
use tempfile::{TempDir, tempdir};
use waof::{WalLog, WalScanIterator};
use wbase::hash_slot::CLUSTER_SLOT_COUNT;
use wconf::RuntimeServerOptions;
use wdev::SegmentedDevice;
use wedb::server::{
  cluster_provider::ClusterProvider,
  replication::{replica_wire::TcpSessionWire, wire_replication_data_plane},
  worker::{LOCAL_WORKER_ID, LocalWorkerSpec, NodeRole},
};
use wedb_test::{cluster_decorate, cluster_seed::seed_local_worker};
use wnode::{
  GarnetServer, RespSessionConsumer, resp::garnet_api::StoreGarnetApi,
  service::StorageSessionProvider,
};
use wtest_base::{test_store_config, wait_for};

/// 测试节点身份（内部 u128；协议面渲染为 32 字符小写 hex）
const PRIMARY_ID: u128 = 0x0DE2_0000_0000_0000_0000_0000_0000_0001;
const REPLICA_ID: u128 = 0x0DE2_0000_0000_0000_0000_0000_0000_0002;

/// requirepass / 集群互信凭据共用口令与用户名（C# admin 凭据对位；rust
/// requirepass 装配为 default 用户单档）
const USER: &str = "default";
const PWD: &str = "repl-secret-pw";

/// 节点装配产物（wedb_test::NodeAssembly 五元 + 引擎句柄播种位：
/// 集群面重放资产以 try_store 在位为判据，副本存储级校验必需）
type SecureNodeAssembly<D> = (
  TempDir,
  GarnetServer<StorageSessionProvider<D>>,
  Arc<ClusterProvider>,
  Arc<WalLog<SegmentedDevice>>,
  u16,
  SharedStore<SegmentedDevice>,
);

/// 起一个生产宿主形态节点（随机端口，AOF 门控点亮，可选 requirepass）
///
/// wedb_test::start_node 同款装配 + 认证注入位：`with_requirepass` 须在
/// provider 入 Arc 前调用（wedb_test 装配体无认证形参，本册就地展开）
fn start_secure_node<D>(
  provider: Arc<ClusterProvider>,
  decorate: D,
  requirepass: Option<&str>,
) -> Result<SecureNodeAssembly<D>>
where
  D:
    Fn(u64, StoreGarnetApi<SegmentedDevice>) -> Option<RespSessionConsumer> + Send + Sync + 'static,
{
  let dir = tempdir()?;
  let session_provider = StorageSessionProvider::open_with_config_and_aof(
    test_store_config(),
    dir.path().join("node.db"),
    None,
    RuntimeServerOptions::default(),
    decorate,
  )?
  .with_requirepass(requirepass);
  let wal = session_provider
    .wal()
    .cloned()
    .expect("AOF 门控点亮后 wal 必在场");
  // 引擎句柄外提（集群面 set_store 播种用：重放资产装配前必须在位）
  let store = session_provider.store();
  let server = GarnetServer::new(
    &["127.0.0.1:0".to_string()],
    65536,
    Arc::new(session_provider),
  )?;
  // 单 worker 收敛（wedb_test::start_node 同款，防 CI 全核 worker 线程爆炸）
  server.start(NonZeroUsize::new(1))?;
  let port = server.local_addr()?.port();
  Ok((dir, server, provider, wal, port, store))
}

/// 副本复制链路建立等待面：重放驱动在册（主端 init 帧握手注册）即流已建立
fn replica_stream_active(provider: &ClusterProvider) -> bool {
  provider
    .replication_manager()
    .is_some_and(|rm| rm.has_active_replication_stream())
}

/// 主端全槽指派（worker_id 为本地 worker）
fn assign_all_slots(config: &mut ClusterConfig) {
  use wedb::server::hash_slot::SlotState;
  let slots: Vec<usize> = (0..CLUSTER_SLOT_COUNT).collect();
  config.assign_slots(&slots, LOCAL_WORKER_ID as u16, SlotState::Stable);
}

/// wire 级认证三臂（replica_wire.rs:211 `auth` 形参直测）：
/// 不带凭据 init 帧被 -NOAUTH 拒（应答非 OK → 本地「Failed to initialize
/// AofSync stream!」）、错凭据 AUTH 握手 WRONGPASS、正确凭据建连 + init
/// 握手 +OK 且通道健康
#[compio::test]
async fn replica_wire_auth_handshake_three_way() -> Void {
  let provider = ClusterProvider::new();
  let decorate = cluster_decorate(Arc::clone(&provider));
  let (dir, server, secure_provider, secure_wal, port, secure_store) =
    start_secure_node(provider, decorate, Some(PWD))?;
  let endpoint = format!("127.0.0.1:{port}");
  // 受端集群面装配（APPENDLOG init 握手角色校验要求本端为 Replica 且挂靠
  // 来方主 id；本节点以副本身份承接主端回连推流，C# 副本节点 ACL + 集群
  // 配置同位）
  {
    let cm = secure_provider.cluster_manager().expect("cm");
    let mut config = cm.current_config.write();
    config.initialize_local_worker(LocalWorkerSpec {
      node_id: REPLICA_ID,
      address: "127.0.0.1",
      port: port as i32,
      config_epoch: 1,
      role: NodeRole::Replica,
      replica_of_node_id: Some(PRIMARY_ID),
      hostname: None,
    });
  }
  secure_provider.set_store(Arc::clone(&secure_store));
  wire_replication_data_plane(&secure_provider, Arc::clone(&secure_wal));

  // 臂一：不带凭据——TCP 成连后 init 帧被 -NOAUTH 拒绝（服务端错误应答经
  // 客户端错误路径透传文案，与本地「Failed to initialize AofSync stream!」
  // 兜底臂等价拒连）
  let err = TcpSessionWire::connect(
    &endpoint,
    PRIMARY_ID,
    0,
    (None, None),
    network_pool(),
    Some(Duration::from_secs(30)),
    #[cfg(feature = "tls")]
    None,
  )
  .await
  .err()
  .expect("不带凭据建连必须被拒");
  assert!(
    err.to_string().contains("NOAUTH"),
    "不带凭据须以 NOAUTH 拒绝收场: {err}"
  );

  // 臂二：错误凭据——AUTH 握手 WRONGPASS
  let err = TcpSessionWire::connect(
    &endpoint,
    PRIMARY_ID,
    0,
    (Some(USER), Some("totally-wrong-pass")),
    network_pool(),
    Some(Duration::from_secs(30)),
    #[cfg(feature = "tls")]
    None,
  )
  .await
  .err()
  .expect("错误凭据建连必须被拒");
  assert!(
    err.to_string().contains("WRONGPASS"),
    "错误凭据须以 WRONGPASS 收场: {err}"
  );

  // 臂三：正确凭据——建连 + init 握手 +OK，通道健康
  let wire = TcpSessionWire::connect(
    &endpoint,
    PRIMARY_ID,
    0,
    (Some(USER), Some(PWD)),
    network_pool(),
    Some(Duration::from_secs(30)),
    #[cfg(feature = "tls")]
    None,
  )
  .await
  .expect("正确凭据建连须成功");
  assert!(wire.is_connected(), "正确凭据建连后通道须为已连态");
  wire.disconnect();

  server.dispose();
  let _ = dir;
  Ok(())
}

/// 带凭据端到端（C# ClusterReplicationAuth 主干）：双端 requirepass + 双端
/// 集群互信凭据，副本 REPLICAOF → 复制流建立 → 主端 SET 推流落盘副本
#[compio::test]
async fn replication_with_cluster_credentials_synchronizes() -> Void {
  // ===== 双节点装配：requirepass + 集群互信凭据（C# credManager + clusterCreds 对位）
  let primary_provider = ClusterProvider::new();
  primary_provider.update_cluster_auth(Some(USER.to_string()), Some(PWD.to_string()));
  let pdecorate = cluster_decorate(Arc::clone(&primary_provider));
  let (pdir, pserver, pprovider, pwal, pport, pstore) =
    start_secure_node(Arc::clone(&primary_provider), pdecorate, Some(PWD))?;

  let replica_provider = ClusterProvider::new();
  replica_provider.update_cluster_auth(Some(USER.to_string()), Some(PWD.to_string()));
  let rdecorate = cluster_decorate(Arc::clone(&replica_provider));
  let (rdir, rserver, rprovider, rwal, rport, rstore) =
    start_secure_node(Arc::clone(&replica_provider), rdecorate, Some(PWD))?;

  // ===== 集群拓扑配置（互指真实端口；主端持全槽）
  {
    let cm = pprovider.cluster_manager().expect("cm");
    let mut config = cm.current_config.write();
    seed_local_worker(
      &mut config,
      PRIMARY_ID,
      pport as i32,
      1,
      Some((REPLICA_ID, rport as i32, 1)),
      false,
    );
    assign_all_slots(&mut config);
  }
  {
    let cm = rprovider.cluster_manager().expect("cm");
    let mut config = cm.current_config.write();
    seed_local_worker(
      &mut config,
      REPLICA_ID,
      rport as i32,
      1,
      Some((PRIMARY_ID, pport as i32, 1)),
      false,
    );
  }

  // ===== 复制数据面生产装配（replication_assembly_e2e 同款）
  // 引擎句柄播种先于数据面装配（副本重放资产以 try_store 在位为判据）
  pprovider.set_store(pstore);
  rprovider.set_store(Arc::clone(&rstore));
  wire_replication_data_plane(&pprovider, Arc::clone(&pwal));
  wire_replication_data_plane(&rprovider, Arc::clone(&rwal));

  // ===== 副本 REPLICAOF（控制面客户端自身也要过副本的 requirepass 门）
  let resp = client_roundtrip(
    &format!("127.0.0.1:{rport}"),
    Some((USER, PWD)),
    &["REPLICAOF", "127.0.0.1", &pport.to_string()],
  )
  .await
  .expect("客户端往返应成功");
  assert_eq!(resp, "OK", "带互信凭据 REPLICAOF 应成功");

  // ===== 复制流建立（主端回连副本经 AUTH 握手注册推流驱动）
  assert!(
    wait_for(
      || replica_stream_active(&rprovider),
      Duration::from_secs(15)
    )
    .await,
    "带凭据复制流应在 REPLICAOF 后建立"
  );

  // ===== 主端 SET 经推流链路落盘副本（数据面认证链全通）
  let resp = client_roundtrip(
    &format!("127.0.0.1:{pport}"),
    Some((USER, PWD)),
    &["SET", "auth_repl_key", "auth_repl_value"],
  )
  .await
  .expect("客户端往返应成功");
  assert_eq!(resp, "OK", "主端 SET 应成功");

  assert!(
    wait_for(
      || rwal.tail_address() == pwal.tail_address(),
      Duration::from_secs(15)
    )
    .await,
    "副本日志尾应经认证推流链路追平主端"
  );

  // ===== 副本数据保真验证（C# ValidateKVCollectionAgainstReplica 轻量对位；
  // 存储级应用链由 replica_background_replay.rs 专用重放夹具承接，本册以
  // 记录帧逐条字节一致 + 载荷在位为凭）
  let collect = |wal: &WalLog<SegmentedDevice>| {
    let mut iter: WalScanIterator<SegmentedDevice> =
      wal.scan(wal.begin_address(), wal.tail_address());
    async move {
      let mut records = Vec::new();
      while let Some(r) = iter.next_frame().await.expect("日志扫描不可失败") {
        records.push((r.address, r.frame));
      }
      records
    }
  };
  let pri = collect(&pwal).await;
  let rep = collect(&rwal).await;
  assert!(!pri.is_empty(), "主端应含 SET 记录");
  assert_eq!(pri, rep, "副本记录帧必须与主端逐条字节一致");
  assert!(
    pri
      .iter()
      .any(|(_, frame)| frame.windows(15).any(|w| w == b"auth_repl_value")),
    "主端记录帧应含认证链路写入的键值载荷"
  );

  pserver.dispose();
  rserver.dispose();
  let _ = (pdir, rdir);
  Ok(())
}

/// 不带凭据端到端：双端 requirepass、双端零互信凭据——副本发起臂 INITIATE_
/// REPLICA_SYNC 被主端 -NOAUTH 拒绝，REPLICAOF 回 -ERR，复制流永不建立
#[compio::test]
async fn replication_without_credentials_rejected() -> Void {
  // ===== 双节点装配：仅 requirepass，不播种集群互信凭据
  let primary_provider = ClusterProvider::new();
  let pdecorate = cluster_decorate(Arc::clone(&primary_provider));
  let (pdir, pserver, pprovider, pwal, pport, pstore) =
    start_secure_node(Arc::clone(&primary_provider), pdecorate, Some(PWD))?;

  let replica_provider = ClusterProvider::new();
  let rdecorate = cluster_decorate(Arc::clone(&replica_provider));
  let (rdir, rserver, rprovider, rwal, rport, rstore) =
    start_secure_node(Arc::clone(&replica_provider), rdecorate, Some(PWD))?;

  // ===== 集群拓扑配置（互指；无凭据）
  {
    let cm = pprovider.cluster_manager().expect("cm");
    let mut config = cm.current_config.write();
    seed_local_worker(
      &mut config,
      PRIMARY_ID,
      pport as i32,
      1,
      Some((REPLICA_ID, rport as i32, 1)),
      false,
    );
    assign_all_slots(&mut config);
  }
  {
    let cm = rprovider.cluster_manager().expect("cm");
    let mut config = cm.current_config.write();
    seed_local_worker(
      &mut config,
      REPLICA_ID,
      rport as i32,
      1,
      Some((PRIMARY_ID, pport as i32, 1)),
      false,
    );
  }
  // 引擎句柄播种先于数据面装配（副本重放资产以 try_store 在位为判据）
  pprovider.set_store(pstore);
  rprovider.set_store(Arc::clone(&rstore));
  wire_replication_data_plane(&pprovider, Arc::clone(&pwal));
  wire_replication_data_plane(&rprovider, Arc::clone(&rwal));

  // ===== 副本 REPLICAOF：控制面客户端持凭据过本端门，发起臂无凭据被主端拒
  let err = client_roundtrip(
    &format!("127.0.0.1:{rport}"),
    Some((USER, PWD)),
    &["REPLICAOF", "127.0.0.1", &pport.to_string()],
  )
  .await
  .expect_err("无互信凭据 REPLICAOF 必须失败");
  // 拒绝文案说明：无凭据发起客户端的握手（CLIENT SETINFO）即被主端 ACL 门
  // 以 -NOAUTH 打回（服务端告警日志面），connect_async 失败后发起臂归一为
  // 「not connected」原因——wire 级 NOAUTH 文案由三臂册直断
  let err = err.to_string();
  assert!(
    err.contains("Failed to initiate replica sync")
      && (err.contains("NOAUTH") || err.contains("not connected")),
    "无凭据发起须以拒绝收场: {err}"
  );

  // ===== 复制流不得建立、主端写入不得到达副本
  assert!(
    !wait_for(|| replica_stream_active(&rprovider), Duration::from_secs(2)).await,
    "无凭据复制流不得建立"
  );
  let resp = client_roundtrip(
    &format!("127.0.0.1:{pport}"),
    Some((USER, PWD)),
    &["SET", "rejected_key", "v"],
  )
  .await
  .expect("客户端往返应成功");
  assert_eq!(resp, "OK", "主端 SET 应成功");
  assert!(
    rwal.tail_address() != pwal.tail_address() || !replica_stream_active(&rprovider),
    "无凭据链路上主端写入不得经推流到达副本"
  );

  pserver.dispose();
  rserver.dispose();
  let _ = (pdir, rdir);
  Ok(())
}
