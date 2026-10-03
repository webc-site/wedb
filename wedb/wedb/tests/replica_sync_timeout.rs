//! 副本同步超时臂端到端集成测试（对标 C# test/cluster/Garnet.test.cluster/
//! ClusterNegativeTests.cs:ClusterReplicaSyncTimeoutTest :427）
//!
//! C# 场景：ReplicaSyncTimeout 配 1 秒 + 接收检查点异常注入令副本追不平，
//! CLUSTER REPLICATE 按超时语义失败返回（"The operation has timed out."），
//! 绝不永久挂起；修复后重试可成功同步。
//!
//! rust 行为等价设计（异常注入形态不同，超时语义同源）：
//! - 配置钩子：wedb/src/server/boot.rs:307-309 自 RuntimeServerOptions 注入、
//!   wedb/src/server/cluster_provider/flags.rs:54 `replica_sync_timeout()` 折影
//!   （无限哨兵 → None 不挂计时器），本册直调注入口 `set_replica_sync_timeout_secs`
//!   调到测试级 1 秒并断言折影；
//! - 追不平形态：主端回连的副本端点挂「半活假对端」（应答 CLIENT 族握手帧、
//!   对停等命令帧永不应答，对位 C# 接收检查点停摆）——主端 egress 建连成达后，
//!   BEGIN_REPLICA_RECOVER 停等往返撞 `replica_sync_timeout`（1s）限时失败；
//! - 失败退出面：副本 REPLICAOF（CLUSTER REPLICATE 同一发起体
//!   `recover_replication`）回 -ERR 超时文案，有界墙钟内必达。
//!
//! 建连限时说明：egress_client（checkpoint send / recover roundtrip 两阶段
//! 共用）建连已包 `replica_sync_timeout` 限时（`wait_async` 包裹
//! `connect_async`，对标 C# ConnectAsync(ReplicaSyncTimeout) 同位限时，
//! replica_sync_session.rs:171）——对「全静默」对端主端 arm 有界失败。
//! 本册建连阶段限时由 tcp_wire_connect_times_out_against_silent_peer 册以
//! 全静默对端在 `TcpSessionWire::connect` 面直测；端到端臂以「应握手不应停等」
//! 假对端把限时落在停等往返（recover_roundtrip 的 wait_async）上验证超时
//! 语义本体。

use std::{
  io::ErrorKind,
  sync::Arc,
  time::{Duration, Instant},
};

use aok::Void;
use wedb::server::worker::Worker;
#[path = "common/replica_net.rs"]
mod replica_net;
use replica_net::{client_roundtrip, network_pool};
use wconf::node_options::INFINITE_SYNC_TIMEOUT_SECS;
use wedb::server::{
  cluster_provider::ClusterProvider,
  replication::{replica_wire::TcpSessionWire, wire_replication_data_plane},
  worker::NodeRole,
};
use wedb_test::{cluster_decorate, cluster_seed::seed_local_worker, start_node};
use wtest_base::{SilentNode, bind_blackhole, wait_for};

/// 测试节点身份（内部 u128；协议面渲染为 32 字符小写 hex）
const PRIMARY_ID: u128 = 0x0DE3_0000_0000_0000_0000_0000_0000_0001;
const REPLICA_ID: u128 = 0x0DE3_0000_0000_0000_0000_0000_0000_0002;

/// 测试级超时（秒；C# replicaSyncTimeout: 1 同值）
const TIMEOUT_SECS: u64 = 1;

/// 有界失败上限（超时 1s + 编排余量；超限即视为永久挂起语义回归）
const FAIL_DEADLINE: Duration = Duration::from_secs(10);

/// 半活假对端握手额度（覆盖 wconn 建连握手的 CLIENT SETINFO/SETNAME 两帧）：
/// `SilentNode::bind` 对前 2 个完整 RESP 命令帧各回 `+OK\r\n`，其后帧只收
/// 不应——停等命令（BEGIN_REPLICA_RECOVER / SNAPSHOT_DATA）永不应答，令主端
/// 限时臂必撞
const REPLY_HANDSHAKE_FRAMES: usize = 2;

/// 配置钩子折影（boot.rs:307-309 注入口 + flags.rs:54 读取口）：
/// 测试级小值按秒折 Some；无限哨兵折 None（不挂计时器，绝无
/// Duration::MAX 送 compio 定时器的溢出形态）
#[test]
fn replica_sync_timeout_hook_projects_finite_and_infinite() {
  let provider = ClusterProvider::new();
  provider.set_replica_sync_timeout_secs(TIMEOUT_SECS);
  assert_eq!(
    provider.replica_sync_timeout(),
    Some(Duration::from_secs(TIMEOUT_SECS)),
    "有限超时须按秒折 Some(Duration)"
  );
  provider.set_replica_sync_timeout_secs(INFINITE_SYNC_TIMEOUT_SECS);
  assert_eq!(
    provider.replica_sync_timeout(),
    None,
    "无限哨兵须折 None（不挂计时器）"
  );
}

/// 建连臂超时（C# RunAofSyncTaskAsync ConnectAsync(ReplicaSyncTimeout) 对位）：
/// 静默黑洞对端上建连 + init 握手必须按配置限时失败，错误即 TimedOut 语义，
/// 绝不永久挂起
#[compio::test]
async fn tcp_wire_connect_times_out_against_silent_peer() -> Void {
  let addr = bind_blackhole().await.to_string();

  let started = Instant::now();
  let err = TcpSessionWire::connect(
    &addr,
    PRIMARY_ID,
    0,
    (None, None),
    network_pool(),
    Some(Duration::from_secs(TIMEOUT_SECS)),
    #[cfg(feature = "tls")]
    None,
  )
  .await
  .err()
  .expect("静默对端建连必须超时失败");
  let elapsed = started.elapsed();

  assert_eq!(
    err.kind(),
    ErrorKind::TimedOut,
    "建连臂超时须为 TimedOut 语义: {err}"
  );
  assert!(
    err.to_string().contains("replica sync connect timed out"),
    "超时文案须可辨识: {err}"
  );
  assert!(
    elapsed >= Duration::from_secs(TIMEOUT_SECS),
    "超时不得早于配置阈值触发: {elapsed:?}"
  );
  assert!(
    elapsed < FAIL_DEADLINE,
    "建连臂须在配置阈值附近有界失败而非挂起: {elapsed:?}"
  );
  Ok(())
}

/// 端到端超时臂（C# ClusterReplicaSyncTimeoutTest 主干）：主端真实、副本
/// 端点为静默黑洞——副本 REPLICAOF 发起同步后，主端回连黑洞建连撞限时，
/// REPLICAOF 按超时语义回 -ERR，有界墙钟内必达而非永久挂起
#[compio::test]
async fn replicaof_fails_with_timeout_semantics_against_silent_replica() -> Void {
  // ===== 主端真实节点：超时钩子调到测试级 1 秒；副本端点指向半活假对端
  let blackhole = SilentNode::bind(REPLY_HANDSHAKE_FRAMES).await;
  let primary = ClusterProvider::new();
  primary.set_replica_sync_timeout_secs(TIMEOUT_SECS);
  let (pdir, pserver, pprovider, pwal, pport) =
    start_node(Arc::clone(&primary), cluster_decorate(primary))?;
  {
    let cm = pprovider.cluster_manager().expect("cm");
    let mut config = cm.current_config.write();
    seed_local_worker(&mut config, PRIMARY_ID, pport as i32, 1, None, false);
    // 副本 worker 行：endpoint 指向静默黑洞（accept 不应答）
    config.workers.push(Worker {
      nodeid: Some(REPLICA_ID),
      address: "127.0.0.1".into(),
      port: blackhole.port() as i32,
      config_epoch: 1,
      role: NodeRole::Primary,
      replica_of_node_id: None,
      replication_offset: 0,
      hostname: None,
    });
  }
  wire_replication_data_plane(&pprovider, Arc::clone(&pwal));

  // ===== 副本真实节点：互指主端真实端口
  let replica = ClusterProvider::new();
  let (rdir, rserver, rprovider, rwal, rport) =
    start_node(Arc::clone(&replica), cluster_decorate(replica))?;
  {
    let cm = rprovider.cluster_manager().expect("cm");
    let mut config = cm.current_config.write();
    seed_local_worker(&mut config, REPLICA_ID, rport as i32, 1, None, false);
    config.workers.push(Worker {
      nodeid: Some(PRIMARY_ID),
      address: "127.0.0.1".into(),
      port: pport as i32,
      config_epoch: 1,
      role: NodeRole::Primary,
      replica_of_node_id: None,
      replication_offset: 0,
      hostname: None,
    });
  }
  wire_replication_data_plane(&rprovider, Arc::clone(&rwal));

  // ===== 副本发起同步：主端回连假对端，停等往返撞 replica_sync_timeout
  //（策略协商 PartialResync → egress 建连成达 → BEGIN_REPLICA_RECOVER 永不应答
  //→ 1s 限时失败 → INITIATE_REPLICA_SYNC 回 -ERR），REPLICAOF 有界失败
  let started = Instant::now();
  let err = client_roundtrip(
    &format!("127.0.0.1:{rport}"),
    None,
    &["REPLICAOF", "127.0.0.1", &pport.to_string()],
  )
  .await
  .expect_err("副本追不平（对端停等不回）时 REPLICAOF 必须失败");
  let elapsed = started.elapsed();

  assert!(
    err.contains("timed out"),
    "REPLICAOF 须回超时语义文案: {err}"
  );
  assert!(
    elapsed < FAIL_DEADLINE,
    "同步发起须按超时语义有界失败而非永久挂起: {elapsed:?}"
  );

  // ===== 失败后复制流不得建立、主端写入不得到达副本（重试由重连轮询臂
  // 承担，本册未开启自动重连）
  assert!(
    !wait_for(
      || rprovider
        .replication_manager()
        .is_some_and(|rm| rm.has_active_replication_stream()),
      Duration::from_secs(2)
    )
    .await,
    "超时失败后复制流不得建立"
  );
  // 主端未建推流通道，副本日志零推进（INITIATE 失败无自动重试臂：重连轮询默认关闭）
  assert_eq!(rwal.tail_address(), 0, "超时失败后副本日志尾必须保持零推进");

  pserver.dispose();
  rserver.dispose();
  let _ = (pdir, rdir);
  Ok(())
}
