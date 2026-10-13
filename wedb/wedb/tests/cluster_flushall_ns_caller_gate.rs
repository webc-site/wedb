#![recursion_limit = "256"]
//! CLUSTER FLUSHALL_NS 调用方身份门禁集成判据（doc/zh/db.md 3.5）
//!
//! CLUSTER FLUSHALL_NS 属 rust 自增的多租户管理面（C# 无 ns 维度无对应
//! 命令），其安全边界：仅节点间连接（gossip 建链确立的 remote_node_id
//! 在册）或 ns0 超管会话可发起。本文件用真实 TCP 集群装配验证三面：
//! 1. 裸发拒绝（原有判据）：具备 @admin+@dangerous+@garnet 位的 ns1
//!    租户会话对 ns2 发收令帧必须回权限错误且 ns2 键原样保留；ns0 超管
//!    同帧正常换号（门禁不误伤合法管理面）；
//! 2. 两帧自封旁路拒绝（票 gossip-forgery 收口锁面）：租户会话 WITHMEET
//!    空帧拿本机配置字节后转投，再发 FLUSHALL_NS 仍拒；自造含不在册
//!    幽灵节点的 WITHMEET 帧不 merge 进 CLUSTER NODES 也不置位
//!    remote_node_id（「节点间身份」合并门以信道锚 deny-by-default，
//!    见 wedb cluster_gossip_slow 的 gossip_channel_trusted）；
//! 3. 真实建链放行：NodeConnection 形态（出站握手同源的 gossip 客户端）
//!    建链置位后同连接发 FLUSHALL_NS，断言放行换号。
//!
//! 端点同源判据的控制面：无凭证部署下合并/置位双门退端点同源（合并门
//! 锚「任一在册节点注册端点 ip 与会话对端源 ip 同源」，置位门在此基础上
//! 加帧载荷节点在册），故测试以注册地址 127.0.0.2（与连接源
//! 127.0.0.1 不同源）部署攻击面、以 127.0.0.1（同源）部署放行面。
//! 装配复刻 cluster_flushall_broadcast.rs 的 boot.rs 集群注入段形态。

use std::{num::NonZeroUsize, sync::Arc, time::Duration};

use aok::{Error, Result, Void};
use tempfile::{TempDir, tempdir};
use wacl::AccessControlList;
use wbase::{hash_slot::CLUSTER_SLOT_COUNT, hex::hex_str_u128};
use wconf::RuntimeServerOptions;
use wconn::client::GarnetClient;
use wdev::SegmentedDevice;
use wedb::server::{
  cluster_provider::ClusterProvider,
  gossip::node_connection::NodeConnection,
  hash_slot::SlotState,
  worker::{LOCAL_WORKER_ID, LocalWorkerSpec, NodeRole, Worker},
};
use wedb_test::cluster_decorate;
use wnode::{GarnetServer, resp::garnet_api::StoreGarnetApi, service::StorageSessionProvider};
use wtest_base::test_store_config;

const NODE_A: u128 = 0x0FB1_0000_0000_0000_0000_0000_0000_0001;
const ORIGIN_X: u128 = 0x0FB1_0000_0000_0000_0000_0000_0000_0003;
/// 自造幽灵节点（不在册；攻击者可伪造任意配置字节宣告）
const GHOST_Z: u128 = 0x0FB1_0000_0000_0000_0000_0000_0000_00FF;
/// 攻击面部署的注册地址（与测试连接源 127.0.0.1 不同源，令端点同源
/// 判据可分辨真伪信道）
const FORGED_REG_ADDR: &str = "127.0.0.2";

struct FlushNode<
  D: Fn(u64, StoreGarnetApi<SegmentedDevice>) -> Option<wnode::RespSessionConsumer>
    + Send
    + Sync
    + 'static,
> {
  _dir: TempDir,
  _server: GarnetServer<StorageSessionProvider<D>>,
  port: u16,
}

/// 起一个带清库广播门的集群形态节点（与 broadcast 测试同型装配，随机端口）
fn start_flush_node<D>(cluster: Arc<ClusterProvider>, decorate: D) -> Result<FlushNode<D>>
where
  D: Fn(u64, StoreGarnetApi<SegmentedDevice>) -> Option<wnode::RespSessionConsumer>
    + Send
    + Sync
    + 'static,
{
  let dir = tempdir()?;
  let session_provider = Arc::new(
    StorageSessionProvider::open_with_config_and_aof(
      test_store_config(),
      dir.path().join("node.db"),
      None,
      RuntimeServerOptions::default(),
      decorate,
    )?
    .with_acl(Arc::new(AccessControlList::new("")?)),
  );
  cluster.set_store(session_provider.store());
  cluster.set_database_manager(Arc::clone(&session_provider.database_manager));
  session_provider
    .database_manager
    .attach_flush_gate(cluster.clone());
  let server = GarnetServer::new(
    &["127.0.0.1:0".to_string()],
    65536,
    Arc::clone(&session_provider),
  )?;
  server.start(NonZeroUsize::new(1))?;
  let port = server.local_addr()?.port();
  Ok(FlushNode {
    _dir: dir,
    _server: server,
    port,
  })
}

/// 本地主登记 + origin 在册他节点（epoch 5，收端 epoch 守卫按此放行）；
/// registered_addr 为两端点的注册宣告地址（端点同源判据控制面）
fn config_cluster(cluster: &Arc<ClusterProvider>, local_port: u16, registered_addr: &str) -> Void {
  let cm = cluster.cluster_manager().expect("cluster manager");
  let mut config = cm.current_config.write();
  config.initialize_local_worker(LocalWorkerSpec {
    node_id: NODE_A,
    address: registered_addr,
    port: local_port as i32,
    config_epoch: 1,
    role: NodeRole::Primary,
    replica_of_node_id: None,
    hostname: None,
  });
  config.workers.push(Worker {
    nodeid: Some(ORIGIN_X),
    address: registered_addr.into(),
    port: local_port as i32,
    config_epoch: 5,
    role: NodeRole::Primary,
    replica_of_node_id: None,
    replication_offset: 0,
    hostname: None,
  });
  let slots: Vec<usize> = (0..CLUSTER_SLOT_COUNT).collect();
  config.assign_slots(&slots, LOCAL_WORKER_ID as u16, SlotState::Stable);
  Ok(())
}

/// 免认证短连接（ns0 超管 default 会话形态）
async fn ask(port: u16, command: &[&str]) -> Result<String> {
  let mut client = GarnetClient::new(
    format!("127.0.0.1:{port}"),
    None,
    None,
    Some("test".into()),
    32,
    0,
  )
  .unwrap();
  client.connect_async().await?;
  let resp = client.execute_for_string_result_async(command).await;
  drop(client);
  resp.map_err(Error::from)
}

/// 认证为指定 <ns>#user 的长连接
async fn authed_session(port: u16, user: &str, password: &str) -> Result<GarnetClient> {
  let mut client = GarnetClient::new(
    format!("127.0.0.1:{port}"),
    None,
    None,
    Some("test".into()),
    32,
    0,
  )
  .unwrap();
  client.connect_async().await?;
  let ok = client
    .execute_for_string_result_async(&["AUTH", user, password])
    .await?;
  assert_eq!(ok, "OK", "用户 {user} 应认证成功");
  Ok(client)
}

/// 建租户用户（经 ns0 超管 ACL SETUSER，doc/zh/db.md 三、认证格式）
async fn seed_user(port: u16, user: &str, password: &str, rules: &[&str]) -> Void {
  let pwd_token = format!(">{password}");
  let mut command = vec!["ACL", "SETUSER", user, "on", pwd_token.as_str()];
  command.extend(rules);
  let out = ask(port, &command).await?;
  assert_eq!(out, "OK");
  Ok(())
}

/// ns1 租户（授 @admin+@dangerous+@garnet）对 ns2 发起跨租户清库帧：
/// 门禁生效后必须回单源 NOPERM 权限错误且 ns2 键原样保留；
/// 随后 ns0 超管同帧正常换号（合法管理面不误伤）
#[compio::test]
async fn flushall_ns_denies_foreign_tenant_session() -> Void {
  let ca = ClusterProvider::new();
  let na = start_flush_node(Arc::clone(&ca), cluster_decorate(Arc::clone(&ca)))?;
  config_cluster(&ca, na.port, FORGED_REG_ADDR)?;
  // 攻击者：显式授齐 @admin+@dangerous+@garnet 三位（复刻票面场景，
  // 走 ACL 命令权限门而非 +@all 兜底）
  seed_user(
    na.port,
    "1#mallory",
    "malpw",
    &["+@admin", "+@dangerous", "+@garnet"],
  )
  .await?;
  // 受害者：ns2 正常租户
  seed_user(na.port, "2#carol", "capw", &["+@all"]).await?;

  let carol = authed_session(na.port, "2#carol", "capw").await?;
  assert_eq!(
    carol
      .execute_for_string_result_async(&["SET", "vk", "vv"])
      .await?,
    "OK"
  );

  let origin_hex = hex_str_u128(ORIGIN_X);
  // 两帧自封攻击形态（票面旁路）：WITHMEET 空帧应答取回本机真实配置
  // 字节，转投同会话再发——置位臂「节点间身份」硬门不中（租户信道，
  // 注册端点 127.0.0.2 与连接源 127.0.0.1 不同源），不置 remote_node_id
  let mallory = authed_session(na.port, "1#mallory", "malpw").await?;
  let self_bytes = mallory
    .execute_for_bytes_result_async(&[b"CLUSTER", b"GOSSIP", b"WITHMEET", b""])
    .await?;
  assert!(!self_bytes.is_empty(), "WITHMEET 空帧应答应携带配置字节");
  let _ = mallory
    .execute_for_bytes_result_async(&[b"CLUSTER", b"GOSSIP", &self_bytes])
    .await?;
  let frame = ["CLUSTER", "FLUSHALL_NS", "2", origin_hex.as_str(), "5"];
  let err = mallory
    .execute_for_string_result_async(&frame)
    .await
    .expect_err("两帧自封后收令帧仍必须被身份门拒绝");
  assert!(
    err.to_string().contains("NOPERM"),
    "应回 cmd_strings 单源 NOPERM 权限错误，实际: {err}"
  );
  drop(mallory);

  assert_eq!(
    carol
      .execute_for_string_result_async(&["GET", "vk"])
      .await?,
    "vv",
    "门禁拒绝后 ns2 键必须原样保留"
  );
  drop(carol);

  // ns0 超管（免认证 default 会话）同帧：合法管理面，正常换号清库
  assert_eq!(
    ask(na.port, &frame).await?,
    "OK",
    "ns0 超管发起路径不得被门禁误伤"
  );
  let carol = authed_session(na.port, "2#carol", "capw").await?;
  assert_eq!(
    carol
      .execute_for_string_result_async(&["GET", "vk"])
      .await?,
    "",
    "ns0 合法帧应完成换号，旧键消失"
  );
  Ok(())
}

/// 自造含不在册幽灵节点的 WITHMEET 帧被置位臂硬门整帧拒绝：不 merge
/// （CLUSTER NODES 不出现幽灵节点）也不置 remote_node_id（同会话
/// FLUSHALL_NS 仍拒）——deny-by-default 双断言锁面
#[compio::test]
async fn gossip_forged_withmeet_denies_merge_and_remote_node_id() -> Void {
  let ca = ClusterProvider::new();
  let na = start_flush_node(Arc::clone(&ca), cluster_decorate(Arc::clone(&ca)))?;
  config_cluster(&ca, na.port, FORGED_REG_ADDR)?;
  seed_user(
    na.port,
    "1#mallory",
    "malpw",
    &["+@admin", "+@dangerous", "+@garnet"],
  )
  .await?;

  // 幽灵配置字节：本机配置克隆 + 不在册节点 Z（攻击者可任意伪造宣告，
  // 端点同源判据以本机注册表为锚，载荷宣告不参与）
  let cm = ca.cluster_manager().expect("cluster manager");
  let mut ghost = cm.current_config().clone();
  ghost.workers.push(Worker {
    nodeid: Some(GHOST_Z),
    address: "127.0.0.1".into(),
    port: 1,
    config_epoch: 5,
    role: NodeRole::Primary,
    replica_of_node_id: None,
    replication_offset: 0,
    hostname: None,
  });
  let ghost_bytes = ghost.to_byte_array();

  let ghost_hex = hex_str_u128(GHOST_Z);
  let mallory = authed_session(na.port, "1#mallory", "malpw").await?;
  let _ = mallory
    .execute_for_bytes_result_async(&[b"CLUSTER", b"GOSSIP", b"WITHMEET", &ghost_bytes])
    .await?;
  // 不 merge：CLUSTER NODES 不出现幽灵节点
  let nodes = ask(na.port, &["CLUSTER", "NODES"]).await?;
  assert!(
    !nodes.contains(&ghost_hex),
    "幽灵节点 {ghost_hex} 不得被 merge 进集群视图"
  );
  // 不置位：同会话 FLUSHALL_NS 仍拒
  let origin_hex = hex_str_u128(ORIGIN_X);
  let err = mallory
    .execute_for_string_result_async(&["CLUSTER", "FLUSHALL_NS", "2", origin_hex.as_str(), "5"])
    .await
    .expect_err("幽灵 WITHMEET 帧后收令帧仍必须被身份门拒绝");
  assert!(
    err.to_string().contains("NOPERM"),
    "应回单源 NOPERM 权限错误，实际: {err}"
  );
  Ok(())
}

/// 真实 gossip 建链（NodeConnection 形态，与出站握手同源的无凭证信道）
/// 置位放行：端点同源判据命中（注册端点 127.0.0.1 == 连接源 127.0.0.1），
/// 同连接发 FLUSHALL_NS 断言放行换号——门禁不误伤节点间连接
#[compio::test]
async fn node_connection_gossip_channel_flushall_ns_allowed() -> Void {
  let ca = ClusterProvider::new();
  let na = start_flush_node(Arc::clone(&ca), cluster_decorate(Arc::clone(&ca)))?;
  config_cluster(&ca, na.port, "127.0.0.1")?;
  seed_user(na.port, "2#carol", "capw", &["+@all"]).await?;

  let carol = authed_session(na.port, "2#carol", "capw").await?;
  assert_eq!(
    carol
      .execute_for_string_result_async(&["SET", "vk", "vv"])
      .await?,
    "OK"
  );
  drop(carol);

  // 建链：NodeConnection 复用 provider 出站客户端单点（同源 gossip 帧）
  let cm = ca.cluster_manager().expect("cluster manager");
  let config_bytes = cm.current_config().to_byte_array();
  let nc = NodeConnection::new(NODE_A, "127.0.0.1".into(), na.port as i32, &ca);
  let delay = Duration::from_secs(5);
  let resp = nc.try_gossip_async(&config_bytes, delay).await?;
  assert!(!resp.is_empty(), "同源 gossip 建链应答应携带配置字节");

  // 同连接发 FLUSHALL_NS：remote_node_id 已由建链确立，调用方门放行
  let origin_hex = hex_str_u128(ORIGIN_X);
  nc.try_flushall_ns_async(2, &origin_hex, 5, delay)
    .await
    .expect("节点间连接的收令帧必须放行");

  let carol = authed_session(na.port, "2#carol", "capw").await?;
  assert_eq!(
    carol
      .execute_for_string_result_async(&["GET", "vk"])
      .await?,
    "",
    "节点间合法帧应完成换号，旧键消失"
  );
  Ok(())
}
