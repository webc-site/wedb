//! CLUSTER FLUSHALL_NS 调用方身份门禁集成判据（doc/zh/db.md 3.5）
//!
//! CLUSTER FLUSHALL_NS 属 rust 自增的多租户管理面（C# 无 ns 维度无对应
//! 命令），其安全边界：仅节点间连接（gossip 建链确立的 remote_node_id
//! 在册）或 ns0 超管会话可发起。本文件用真实 TCP 集群装配验证：
//! 具备 @admin+@dangerous+@garnet 位的 ns1 租户会话对 ns2 发收令帧，
//! 必须回既有权限错误（cmd_strings 单源 NOPERM 文案）且 ns2 键原样保留；
//! ns0 超管会话同帧仍正常换号（门禁不误伤合法管理面）。
//! 装配复刻 cluster_flushall_broadcast.rs 的 boot.rs 集群注入段形态。

use std::sync::Arc;

use aok::{Error, Result, Void};
use compio::runtime::Runtime;
use tempfile::{TempDir, tempdir};
use wacl::AccessControlList;
use wbase::{hash_slot::CLUSTER_SLOT_COUNT, hex::hex_str_u128};
use wconn::client::GarnetClient;
use wdev::SegmentedDevice;
use wedb::server::{
  cluster_provider::ClusterProvider,
  hash_slot::SlotState,
  worker::{LOCAL_WORKER_ID, LocalWorkerSpec, NodeRole, Worker},
};
use wedb_test::cluster_decorate;
use wnode::{GarnetServer, resp::garnet_api::StoreGarnetApi, service::StorageSessionProvider};
use wtest_base::test_store_config;

const NODE_A: u128 = 0x0FB1_0000_0000_0000_0000_0000_0000_0001;
const ORIGIN_X: u128 = 0x0FB1_0000_0000_0000_0000_0000_0000_0003;

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
      None,
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
    100,
    Arc::clone(&session_provider),
  )?;
  server.start(None)?;
  let port = server.local_addr()?.port();
  Ok(FlushNode {
    _dir: dir,
    _server: server,
    port,
  })
}

/// 本地主登记 + origin 在册他节点（epoch 5，收端 epoch 守卫按此放行）
fn config_cluster(cluster: &Arc<ClusterProvider>, local_port: u16) -> Void {
  let cm = cluster.cluster_manager().expect("cluster manager");
  let mut config = cm.current_config.write();
  config.initialize_local_worker(LocalWorkerSpec {
    node_id: NODE_A,
    address: "127.0.0.1",
    port: local_port as i32,
    config_epoch: 1,
    role: NodeRole::Primary,
    replica_of_node_id: None,
    hostname: None,
  });
  config.workers.push(Worker {
    nodeid: Some(ORIGIN_X),
    address: "127.0.0.1".into(),
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
  command.extend_from_slice(rules);
  let out = ask(port, &command).await?;
  assert_eq!(out, "OK");
  Ok(())
}

/// ns1 租户（授 @admin+@dangerous+@garnet）对 ns2 发起跨租户清库帧：
/// 门禁生效后必须回单源 NOPERM 权限错误且 ns2 键原样保留；
/// 随后 ns0 超管同帧正常换号（合法管理面不误伤）
#[test]
fn flushall_ns_denies_foreign_tenant_session() -> Void {
  Runtime::new().unwrap().block_on(async {
    let ca = ClusterProvider::new();
    let na = start_flush_node(Arc::clone(&ca), cluster_decorate(Arc::clone(&ca)))?;
    config_cluster(&ca, na.port)?;
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
    let mallory = authed_session(na.port, "1#mallory", "malpw").await?;
    let frame = ["CLUSTER", "FLUSHALL_NS", "2", origin_hex.as_str(), "5"];
    let err = mallory
      .execute_for_string_result_async(&frame)
      .await
      .expect_err("非节点间且非 ns0 的租户会话必须被身份门拒绝");
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
  })
}
