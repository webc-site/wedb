#![recursion_limit = "256"]
#![cfg(feature = "tls")]
//!
//! C# 复制域四构造锚全带 `serverOptions.TlsOptions?.TlsClientOptions` 形参：
//! - libs/cluster/Server/Replication/ReplicaOps/ReplicaDiskbasedSync.cs:ReplicaSyncAttachTaskAsync
//!   （:111-112 GetNetworkPool + tlsOptions；rust 位 assembly::recover_replication，本册锁）
//! - libs/cluster/Server/Replication/ReplicaOps/ReplicaDisklessSync.cs:TryBeginReplicaSyncAsync
//!   （:148-149 networkPool + tlsOptions；rust 位 replica_diskless_sync::replica_diskless_attach，
//!   经公开入口 try_replicate_diskless_sync_async 全骨架触达，本册锁）
//! - libs/cluster/Server/Replication/PrimaryOps/DiskbasedReplication/ReplicaSyncSession.cs
//!   （:102-103；rust 先在例 replica_sync_session.rs:159 egress_client，既有形制不动，本册不重锁）
//! - libs/cluster/Server/Replication/PrimaryOps/AofOperations/AofSyncTask.cs:AofSyncTask
//!   （:137-138 GetNetworkPool + tlsOptions；rust 无盘扇出构造位
//!   diskless_replication/replication_sync_manager.rs stream_sync 单点承接 C# 会话网络臂
//!   （ConnectAsync/IssueFlushAllAsync 委托 AofSyncDriver 逐任务 garnetClient）的构造形参，
//!   本册锁）
//!
//! 对拍形制（锁「构造位读取 provider TLS 选项并在建连生效」）：同一 TLS 服务面上
//! - provider 未配出站 TLS → 明文握手被 TLS 服务面拒绝 → 建连位确定性失败（失败面）；
//! - provider 配置出站 TLS → TLS 握手成功 → 错误只能出自命令应答面或全链收敛（成功面）；
//!
//! 另带一条明文服务面基线对拍臂，锁定建连成功面非 TLS 环境巧合。
//! 客户端 TLS 形参取 [`wnode_tls_test::test_client_tls`] 的
//! ServerCertificateRequired=false 对位臂（证书校验豁免仍为真 TLS 握手）。

use std::{num::NonZeroUsize, result, sync::Arc, time::Duration};

use aok::Result;
use waof::AofAddress;
use wedb::server::{
  cluster_provider::ClusterProvider,
  replication::{
    assembly::recover_replication,
    cluster_replication_session::ClusterReplicationSession,
    error::ReplicationError,
    recovery_status::RecoveryStatus,
    replica_diskless_sync::{try_begin_diskless_sync_async, try_replicate_diskless_sync_async},
    replicate_sync_options::ReplicateSyncOptions,
    sync_metadata::SyncMetadata,
  },
  worker::{NodeRole, Worker},
};
use wedb_test::{
  node_storage::{open_node, provider_with_role},
  primary_assets::primary_assets,
  replica_host::{ReplicaSessionProvider, replica_host},
};
use wnode::GarnetServer;
use wnode_tls_test::{start_tls_server, test_client_tls, test_server_tls};
use wtest_base::wait_for;

/// 测试节点身份（内部 u128；协议面渲染 32 字符小写 hex）
const PRIMARY_ID: u128 = 0x0DE1_0000_0000_0000_0000_0000_0000_0001;
const REPLICA_ID: u128 = 0x0DE1_0000_0000_0000_0000_0000_0000_0002;

/// 监听地址串取端口
fn port_of(addr: &str) -> i32 {
  addr
    .rsplit_once(':')
    .expect("host:port 形态")
    .1
    .parse()
    .expect("端口可析")
}

/// 给副本 provider 配置面补登主端 endpoint worker（get_local_node_primary_address 反查源）
fn attach_primary_worker(provider: &Arc<ClusterProvider>, port: i32) {
  let cm = provider.cluster_manager().expect("集群句柄就位");
  cm.current_config.write().workers.push(Worker {
    nodeid: Some(PRIMARY_ID),
    address: "127.0.0.1".into(),
    port,
    config_epoch: 1,
    role: NodeRole::Primary,
    replica_of_node_id: None,
    replication_offset: 0,
    hostname: None,
  });
}

/// 起真 TLS 服务面（wnode 集群会话供应器 + 测试自签服务端配置），回服务器句柄与 host:port
fn tls_cluster_host(
  provider: &Arc<ClusterProvider>,
) -> Result<(GarnetServer<ReplicaSessionProvider>, String)> {
  let (server, addr) = start_tls_server(
    Arc::new(ReplicaSessionProvider {
      provider: Arc::clone(provider),
    }),
    test_server_tls()?,
    NonZeroUsize::new(1),
  )?;
  Ok((server, addr.to_string()))
}

/// 链 1：INITIATE_REPLICA_SYNC 发起位（assembly::recover_replication）三臂对拍
///
/// 对标 C# libs/cluster/Server/Replication/ReplicaOps/ReplicaDiskbasedSync.cs:
/// ReplicaSyncAttachTaskAsync gcs 构造的 tlsOptions 形参位（:112）
#[compio::test]
async fn initiate_replica_sync_egress_tls_gate() -> Result<()> {
  // ===== 基线对拍臂：明文服务面 + 明文 provider，建连必成（错误只许出在应答面，
  //       锁定后续 TLS 拒连判据非环境巧合）
  let p_node = open_node("egress_init_base_primary");
  let provider_p = provider_with_role(
    &p_node,
    PRIMARY_ID,
    7100,
    NodeRole::Primary,
    PRIMARY_ID,
    false,
    Some(0),
  );
  let (_base_server, base_addr) = replica_host(&provider_p, NonZeroUsize::new(1));
  let r_node = open_node("egress_init_base_replica");
  let provider_r = provider_with_role(
    &r_node,
    REPLICA_ID,
    7101,
    NodeRole::Replica,
    PRIMARY_ID,
    false,
    Some(0),
  );
  attach_primary_worker(&provider_r, port_of(&base_addr));
  let err = recover_replication(&provider_r, PRIMARY_ID)
    .await
    .expect_err("主端宿主未接主复制资产，应答面确定性回 -ERR");
  assert!(
    !err.to_string().contains("not connected"),
    "明文对明文建连位必须通畅，错误只许落在应答面：{err}"
  );

  // ===== TLS 服务面 + 明文出站（provider 未配 TLS）：建连位拒绝（失败面）
  let p_node2 = open_node("egress_init_tls_primary");
  let provider_p2 = provider_with_role(
    &p_node2,
    PRIMARY_ID,
    7102,
    NodeRole::Primary,
    PRIMARY_ID,
    false,
    Some(0),
  );
  let (_tls_server, tls_addr) = tls_cluster_host(&provider_p2)?;

  let r_node2 = open_node("egress_init_plain_vs_tls");
  let provider_r2 = provider_with_role(
    &r_node2,
    REPLICA_ID,
    7103,
    NodeRole::Replica,
    PRIMARY_ID,
    false,
    Some(0),
  );
  attach_primary_worker(&provider_r2, port_of(&tls_addr));
  let err = recover_replication(&provider_r2, PRIMARY_ID)
    .await
    .expect_err("明文客户端打 TLS 服务面必须建连失败");
  assert!(
    err.to_string().contains("not connected"),
    "失败面判据：建连位拒绝（实际：{err}）"
  );

  // ===== TLS 服务面 + provider 配出站 TLS：握手成功，错误只许出自命令应答面
  //（构造位读取 provider TLS 选项的行为确证——与失败臂唯一差异即该选项）
  let r_node3 = open_node("egress_init_tls_vs_tls");
  let provider_r3 = provider_with_role(
    &r_node3,
    REPLICA_ID,
    7104,
    NodeRole::Replica,
    PRIMARY_ID,
    false,
    Some(0),
  );
  provider_r3.set_cluster_tls_client(Some(Arc::new(test_client_tls()?)));
  attach_primary_worker(&provider_r3, port_of(&tls_addr));
  match recover_replication(&provider_r3, PRIMARY_ID).await {
    Ok(()) => {}
    Err(e) => assert!(
      !e.to_string().contains("not connected"),
      "配了出站 TLS 后建连位必须经 TLS 握手成功，错误只许出自命令应答面：{e}"
    ),
  }
  Ok(())
}

/// 无盘副本 attach 单臂装配（链 2 对拍体）：本节点主角色 + 在册主端（TLS 服务面），
/// 经公开入口 [`try_replicate_diskless_sync_async`] 全骨架跑 ATTACH_SYNC 发起
///（TryAddReplica 登记改挂 → 纪元等待 → attach；attach 位即出站 TLS 消费点）
///
/// 对标 C# libs/cluster/Server/Replication/ReplicaOps/ReplicaDisklessSync.cs:
/// TryBeginReplicaSyncAsync gcs 构造的 tlsOptions 形参位（:149）
async fn attach_sync_arm(tls_client: bool) -> Result<result::Result<(), ReplicationError>> {
  let (p_tag, r_tag, p_port) = if tls_client {
    (
      "egress_attach_tls_primary",
      "egress_attach_tls_replica",
      7110,
    )
  } else {
    (
      "egress_attach_plain_primary",
      "egress_attach_plain_replica",
      7108,
    )
  };
  let p_node = open_node(p_tag);
  let provider_p = provider_with_role(
    &p_node,
    PRIMARY_ID,
    p_port,
    NodeRole::Primary,
    PRIMARY_ID,
    false,
    Some(0),
  );
  let (_tls_server, tls_addr) = tls_cluster_host(&provider_p)?;

  let r_node = open_node(r_tag);
  let provider_r = provider_with_role(
    &r_node,
    REPLICA_ID,
    p_port + 1,
    NodeRole::Primary,
    PRIMARY_ID,
    false,
    Some(0),
  );
  attach_primary_worker(&provider_r, port_of(&tls_addr));
  if tls_client {
    provider_r.set_cluster_tls_client(Some(Arc::new(test_client_tls()?)));
  }
  let opts = ReplicateSyncOptions::new(
    PRIMARY_ID, false, // background：前台取回 attach 错误
    false, // force：本节点主角色 + 无槽位指派，走完整校验臂
    true,  // try_add_replica：真实登记改挂（握 ClusterReplicate 锁）
    false, // allow_replica_reset_on_failure：保留错误文案原样上抛
    false, // upgrade_lock
  );
  Ok(try_replicate_diskless_sync_async(&provider_r, opts).await)
}

/// 链 2：ATTACH_SYNC attach 位（replica_diskless_attach）双臂对拍
#[compio::test]
async fn attach_sync_egress_tls_gate() -> Result<()> {
  // 失败面：TLS 服务面 + 明文出站 → 建连位确定性拒绝
  let res = attach_sync_arm(false).await?;
  let err = res.expect_err("明文客户端打 TLS 服务面必须建连失败");
  assert!(
    err
      .to_string()
      .contains("failed connecting to primary for diskless attach sync"),
    "失败面判据：建连位拒绝（实际：{err}）"
  );

  // 成功面：TLS 服务面 + 出站 TLS → 握手成功，错误只许出自 ATTACH_SYNC 应答面
  //（主端宿主未接主复制资产，应答回 -ERR；建连失败文案不得再现）
  let res = attach_sync_arm(true).await?;
  match res {
    Ok(()) => {}
    Err(e) => assert!(
      !e.to_string()
        .contains("failed connecting to primary for diskless attach sync"),
      "配了出站 TLS 后建连位必须经 TLS 握手成功，错误只许出自命令应答面：{e}"
    ),
  }
  Ok(())
}

/// 链 3：无盘扇出构造位（replication_sync_manager::stream_sync）单臂装配
///
/// 副本宿主挂 TLS 服务面；`tls_client` 控主端 provider 出站 TLS 选项：
/// - false → 扇出建连拒绝、会话判败，错误经 replication_sync_driver 判败早返上抛；
/// - true → 全链经 TLS 收敛（快照扇出 + 恢复帧 ATTACH_SYNC 往返 + APPENDLOG
///   推流衔接，形制同明文册 diskless_loop_convergence）
///
/// 对标 C# libs/cluster/Server/Replication/PrimaryOps/AofOperations/
/// AofSyncTask.cs:AofSyncTask garnetClient 构造的 tlsOptions 形参位（:138）
async fn stream_sync_fanout_arm(tls_client: bool) -> result::Result<AofAddress, ReplicationError> {
  let tag = if tls_client { "tls" } else { "plain" };
  // ===== 主端：非空日志 + provider（出站 TLS 选项按臂配置）
  let source = open_node(&format!("egress_fanout_{tag}_primary"));
  let provider_p = provider_with_role(
    &source,
    PRIMARY_ID,
    7120,
    NodeRole::Primary,
    PRIMARY_ID,
    false,
    Some(0),
  );
  if tls_client {
    provider_p.set_cluster_tls_client(Some(Arc::new(test_client_tls().expect("出站 TLS 装配"))));
  }
  for i in 0..3 {
    source
      .wal
      .enqueue(format!("fanout-{tag}-backlog-{i}").as_bytes())
      .unwrap();
  }
  let primary_tail = source.wal.tail_address() as i64;
  assert!(primary_tail > 0, "主端日志必须非空");

  // ===== 副本：带旧本地日志 + 接收会话全接线 + TLS 服务面宿主
  let replica = open_node(&format!("egress_fanout_{tag}_replica"));
  for i in 0..2 {
    replica
      .wal
      .enqueue(format!("fanout-{tag}-stale-{i}").as_bytes())
      .unwrap();
  }
  let provider_r = provider_with_role(
    &replica,
    REPLICA_ID,
    7121,
    NodeRole::Replica,
    PRIMARY_ID,
    false,
    Some(0),
  );
  provider_r.set_replica_replication_session(Some(Arc::new(ClusterReplicationSession::new(
    Arc::clone(&provider_r),
    Arc::clone(&replica.wal),
    None,
  ))));
  let (_server, replica_addr) = tls_cluster_host(&provider_r).expect("TLS 副本宿主起服");

  let rm_p = provider_p.replication_manager().expect("rm 就位");
  let rm_r = provider_r.replication_manager().expect("rm 就位");
  // 副本握手完成后的读角色门控（同明文册口径）
  assert!(
    rm_r.begin_recovery(RecoveryStatus::ReadRole, false),
    "副本恢复门控就位"
  );

  let assets = primary_assets(&source, &rm_p);
  let meta = SyncMetadata {
    full_sync: false,
    origin_node_role: NodeRole::Replica,
    origin_node_id: REPLICA_ID,
    current_primary_repl_id: rm_r.primary_repl_id(),
    current_store_version: 0,
    current_aof_begin_address: AofAddress::create(1, 0),
    current_aof_tail_address: AofAddress::create(1, 0),
    checkpoint_entry: None,
  };
  let result =
    try_begin_diskless_sync_async(&provider_p, &assets, PRIMARY_ID, &replica_addr, &meta).await;

  // 成功臂收敛面：恢复帧回传位点收敛主端快照覆盖锚、复制 ID 收敛、副本 WAL 追平
  if tls_client {
    let sync_from = result.expect("配了出站 TLS 后扇出建连位必须经 TLS 握手成功并全链收敛");
    assert_eq!(
      sync_from.get(0),
      Some(primary_tail),
      "副本恢复位点必经 ATTACH_SYNC 回传并收敛到主端授予锚（TLS 传输）"
    );
    assert_eq!(
      rm_r.primary_repl_id(),
      rm_p.primary_repl_id(),
      "副本主复制 ID 必须经 TLS 链恢复帧收敛为主端 ID"
    );
    let converged = wait_for(
      || replica.wal.tail_address() as i64 == primary_tail,
      Duration::from_secs(5),
    )
    .await;
    assert!(
      converged,
      "积压记录帧必须经 TLS 推流全量落盘（副本 WAL 尾追平主端）"
    );
    Ok(sync_from)
  } else {
    result
  }
}

/// 链 3：无盘扇出双臂对拍
#[compio::test]
async fn stream_sync_egress_tls_gate() -> Result<()> {
  // 失败面：TLS 副本服务面 + 主端明文扇出 → 扇出建连位确定性拒绝
  let err = stream_sync_fanout_arm(false)
    .await
    .expect_err("明文扇出打 TLS 服务面必须建连失败");
  assert!(
    err
      .to_string()
      .contains("failed connecting to replica for stream sync"),
    "失败面判据：扇出建连位拒绝（实际：{err}）"
  );

  // 成功面：TLS 副本服务面 + 主端出站 TLS → 全链经 TLS 收敛
  assert!(stream_sync_fanout_arm(true).await.is_ok());
  Ok(())
}
