//! FLUSHALL 集群总线换号广播集成测试（doc/zh/db.md 4.5）
//!
//! 真实 TCP 多主拓扑验证 CLUSTER FLUSHALL_NS 定向帧全链路：
//! 协调者本地换号后经总线收齐全部 Primary ack 才回 +OK（任一节点不可达
//! 回错误，严禁先应答）；接收端守卫（元数 / ns 0 / 自回声 / 不可知或
//! 陈旧 origin epoch / 非 Primary）。节点装配复刻 boot.rs 集群注入段
//!（set_store / set_database_manager / attach_flush_gate），换号执行
//! 复用 RESP 主路径同一 SingleDatabaseManager 漏斗。

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
use wnode::{
  GarnetServer, database::SingleDatabaseManager, resp::garnet_api::StoreGarnetApi,
  service::StorageSessionProvider,
};
use wtest_base::test_store_config;

/// 租户身份（与既有命名空间测试同型：<ns>#<user> 认证格式）
const TENANT_NS: u64 = 1;
const NODE_A: u128 = 0x0FA1_0000_0000_0000_0000_0000_0000_0001;
const NODE_B: u128 = 0x0FA1_0000_0000_0000_0000_0000_0000_0002;
const ORIGIN_X: u128 = 0x0FA1_0000_0000_0000_0000_0000_0000_0003;

/// 生产注入面节点装配（boot.rs 集群注入段最小子集 + 数据目录托管）
struct FlushNode<
  D: Fn(u64, StoreGarnetApi<SegmentedDevice>) -> Option<wnode::RespSessionConsumer>
    + Send
    + Sync
    + 'static,
> {
  _dir: TempDir,
  _server: GarnetServer<StorageSessionProvider<D>>,
  manager: Arc<SingleDatabaseManager<SegmentedDevice>>,
  port: u16,
}

/// 起一个带清库广播门的集群形态节点（复刻 boot.rs 装配注入点，随机端口）
fn start_flush_node<D>(cluster: Arc<ClusterProvider>, decorate: D) -> Result<FlushNode<D>>
where
  D: Fn(u64, StoreGarnetApi<SegmentedDevice>) -> Option<wnode::RespSessionConsumer>
    + Send
    + Sync
    + 'static,
{
  let dir = tempdir()?;
  // 装配 ACL 认证器：默认用户为 ns0 超管（空口令免认证），租户 <ns>#user
  // 由 seed_tenant_user 经超管 ACL SETUSER 建入（doc/zh/db.md 三、认证格式）
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
    manager: Arc::clone(&session_provider.database_manager),
    port,
  })
}

/// 集群本地 worker 初始化 + 远端 worker 登记（own_all_slots 时全槽归本地，
/// 双主各自可服务）
fn config_cluster(
  cluster: &Arc<ClusterProvider>,
  local_id: u128,
  local_port: u16,
  role: NodeRole,
  remotes: &[(u128, u16, i64)],
  own_all_slots: bool,
) {
  let cm = cluster.cluster_manager().expect("cluster manager");
  let mut config = cm.current_config.write();
  config.initialize_local_worker(LocalWorkerSpec {
    node_id: local_id,
    address: "127.0.0.1",
    port: local_port as i32,
    config_epoch: 1,
    role,
    replica_of_node_id: None,
    hostname: None,
  });
  for (id, port, epoch) in remotes {
    config.workers.push(Worker {
      nodeid: Some(*id),
      address: "127.0.0.1".into(),
      port: *port as i32,
      config_epoch: *epoch,
      role: NodeRole::Primary,
      replica_of_node_id: None,
      replication_offset: 0,
      hostname: None,
    });
  }
  if own_all_slots {
    let slots: Vec<usize> = (0..CLUSTER_SLOT_COUNT).collect();
    config.assign_slots(&slots, LOCAL_WORKER_ID as u16, SlotState::Stable);
  }
}

/// 一键 RESP 客户端（短连接单命令；nil 应答渲染为空串）
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

/// 租户长连接（AUTH 1#bob 后保持）
async fn tenant_session(port: u16) -> Result<GarnetClient> {
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
    .execute_for_string_result_async(&["AUTH", "1#bob", "bobpw"])
    .await?;
  assert_eq!(ok, "OK", "租户 1#bob 应认证成功");
  Ok(client)
}

/// 超管在指定节点建租户用户
async fn seed_tenant_user(port: u16) -> Result<()> {
  let out = ask(port, &["ACL", "SETUSER", "1#bob", "on", ">bobpw", "+@all"]).await?;
  assert_eq!(out, "OK");
  Ok(())
}

/// 双主真 TCP：协调者 FLUSHALL 收齐全部 Primary ack 后，两节点租户域同清，
/// 换号后新写入正常且旧数据不复活
#[test]
fn flushall_broadcast_flushes_all_primaries() -> Void {
  Runtime::new().unwrap().block_on(async {
    let ca = ClusterProvider::new();
    let na = start_flush_node(Arc::clone(&ca), cluster_decorate(Arc::clone(&ca)))?;
    let cb = ClusterProvider::new();
    let nb = start_flush_node(Arc::clone(&cb), cluster_decorate(Arc::clone(&cb)))?;
    config_cluster(
      &ca,
      NODE_A,
      na.port,
      NodeRole::Primary,
      &[(NODE_B, nb.port, 1)],
      true,
    );
    config_cluster(
      &cb,
      NODE_B,
      nb.port,
      NodeRole::Primary,
      &[(NODE_A, na.port, 1)],
      true,
    );
    seed_tenant_user(na.port).await?;
    seed_tenant_user(nb.port).await?;

    let sa = tenant_session(na.port).await?;
    let sb = tenant_session(nb.port).await?;
    assert_eq!(
      sa.execute_for_string_result_async(&["SET", "ka", "va"])
        .await?,
      "OK"
    );
    assert_eq!(
      sb.execute_for_string_result_async(&["SET", "kb", "vb"])
        .await?,
      "OK"
    );

    // 协调者 FLUSHALL：本地换号 + 收齐 B 的 ack 后才 +OK
    assert_eq!(
      sa.execute_for_string_result_async(&["FLUSHALL"]).await?,
      "OK"
    );
    assert_eq!(
      sa.execute_for_string_result_async(&["GET", "ka"]).await?,
      "",
      "协调节点旧租户键应随换号消失"
    );
    assert_eq!(
      sb.execute_for_string_result_async(&["GET", "kb"]).await?,
      "",
      "远端主节点旧租户键应随广播换号消失"
    );
    // 换号后写入落新虚拟 ns，旧数据不复活
    assert_eq!(
      sb.execute_for_string_result_async(&["SET", "kb2", "v2"])
        .await?,
      "OK"
    );
    assert_eq!(
      sb.execute_for_string_result_async(&["GET", "kb2"]).await?,
      "v2"
    );
    assert_eq!(
      sb.execute_for_string_result_async(&["GET", "kb"]).await?,
      "",
      "换号后旧键不得复活"
    );
    Ok(())
  })
}

/// +OK 时序：任一 Primary 不可达即回错误，绝不误答 +OK（本地换号先行
/// 生效，部分提交不回滚，对标 C# 清库语义）
#[test]
fn flushall_broadcast_unreachable_primary_errors() -> Void {
  Runtime::new().unwrap().block_on(async {
    let ca = ClusterProvider::new();
    let na = start_flush_node(Arc::clone(&ca), cluster_decorate(Arc::clone(&ca)))?;
    let cb = ClusterProvider::new();
    let nb = start_flush_node(Arc::clone(&cb), cluster_decorate(Arc::clone(&cb)))?;
    // 1 号端口无人监听：connect 拒绝 → ack 失败
    config_cluster(
      &ca,
      NODE_A,
      na.port,
      NodeRole::Primary,
      &[(NODE_B, nb.port, 1), (ORIGIN_X, 1, 1)],
      true,
    );
    config_cluster(
      &cb,
      NODE_B,
      nb.port,
      NodeRole::Primary,
      &[(NODE_A, na.port, 1)],
      true,
    );
    seed_tenant_user(na.port).await?;

    let sa = tenant_session(na.port).await?;
    sa.execute_for_string_result_async(&["SET", "ka", "va"])
      .await?;
    let err = sa
      .execute_for_string_result_async(&["FLUSHALL"])
      .await
      .expect_err("不可达主节点场景必须回错误而非 +OK");
    assert!(
      err.to_string().contains("FLUSHALL broadcast failed"),
      "错误应指明广播失败: {err}"
    );
    assert_eq!(
      sa.execute_for_string_result_async(&["GET", "ka"]).await?,
      "",
      "本地换号已先行生效"
    );
    Ok(())
  })
}

/// 接收端守卫：元数 / ns 0 / 自回声 / 不可知或陈旧 origin epoch / 非 Primary
#[test]
fn flushall_ns_receiver_guards() -> Void {
  Runtime::new().unwrap().block_on(async {
    let ca = ClusterProvider::new();
    let na = start_flush_node(Arc::clone(&ca), cluster_decorate(Arc::clone(&ca)))?;
    // origin 在册 epoch 5：epoch 4 帧即陈旧
    config_cluster(
      &ca,
      NODE_A,
      na.port,
      NodeRole::Primary,
      &[(ORIGIN_X, na.port, 5)],
      true,
    );
    seed_tenant_user(na.port).await?;
    let s = tenant_session(na.port).await?;
    s.execute_for_string_result_async(&["SET", "kg", "vg"])
      .await?;
    drop(s);

    // 元数不足
    assert!(
      ask(na.port, &["CLUSTER", "FLUSHALL_NS", "1"])
        .await
        .is_err()
    );
    // ns 0 越界
    let err = ask(
      na.port,
      &["CLUSTER", "FLUSHALL_NS", "0", &hex_str_u128(ORIGIN_X), "5"],
    )
    .await
    .expect_err("ns 0 不属租户换号域");
    assert!(err.to_string().contains("out of scope"), "{err}");
    // 自回声
    let err = ask(
      na.port,
      &["CLUSTER", "FLUSHALL_NS", "1", &hex_str_u128(NODE_A), "5"],
    )
    .await
    .expect_err("origin 为本机应拒");
    assert!(err.to_string().contains("self"), "{err}");
    // 不可知 origin
    let err = ask(
      na.port,
      &["CLUSTER", "FLUSHALL_NS", "1", &hex_str_u128(NODE_B), "5"],
    )
    .await
    .expect_err("origin 不在配置应拒");
    assert!(err.to_string().contains("stale or unknown"), "{err}");
    // 陈旧 epoch
    let err = ask(
      na.port,
      &["CLUSTER", "FLUSHALL_NS", "1", &hex_str_u128(ORIGIN_X), "4"],
    )
    .await
    .expect_err("epoch 低于本机 origin 配置应拒");
    assert!(err.to_string().contains("stale or unknown"), "{err}");
    // 合法帧：换号生效，租户旧键消失
    assert_eq!(
      ask(
        na.port,
        &["CLUSTER", "FLUSHALL_NS", "1", &hex_str_u128(ORIGIN_X), "5"],
      )
      .await?,
      "OK"
    );
    let s = tenant_session(na.port).await?;
    assert_eq!(
      s.execute_for_string_result_async(&["GET", "kg"]).await?,
      "",
      "合法帧接收端应完成换号"
    );
    drop(s);

    // 非 Primary 拒收
    let cr = ClusterProvider::new();
    let nr = start_flush_node(Arc::clone(&cr), cluster_decorate(Arc::clone(&cr)))?;
    config_cluster(
      &cr,
      NODE_B,
      nr.port,
      NodeRole::Replica,
      &[(ORIGIN_X, nr.port, 1)],
      false,
    );
    let err = ask(
      nr.port,
      &["CLUSTER", "FLUSHALL_NS", "1", &hex_str_u128(ORIGIN_X), "1"],
    )
    .await
    .expect_err("副本不得执行换号");
    assert!(err.to_string().contains("not a master"), "{err}");
    Ok(())
  })
}

/// 广播门单机形态：集群配置无远端 Primary 时目标集为空，本地换号即 +OK；
/// flush_gate 缺省（未装配）时 flushall_broadcast 门返回 None
#[test]
fn flushall_single_primary_ok_without_peers() -> Void {
  Runtime::new().unwrap().block_on(async {
    let ca = ClusterProvider::new();
    let na = start_flush_node(Arc::clone(&ca), cluster_decorate(Arc::clone(&ca)))?;
    config_cluster(&ca, NODE_A, na.port, NodeRole::Primary, &[], true);
    seed_tenant_user(na.port).await?;
    let s = tenant_session(na.port).await?;
    s.execute_for_string_result_async(&["SET", "kl", "vl"])
      .await?;
    assert_eq!(
      s.execute_for_string_result_async(&["FLUSHALL"]).await?,
      "OK"
    );
    assert_eq!(s.execute_for_string_result_async(&["GET", "kl"]).await?, "");
    // 装配形态门在场（广播 future 单机空目标恒 Ok，由上一条 +OK 承接验证）
    assert!(na.manager.flushall_broadcast(TENANT_NS).is_some());
    Ok(())
  })
}
