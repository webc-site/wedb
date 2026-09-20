//! 集群故障转移集成测试，对标 Garnet 复制与故障转移测试
use std::{
  sync::Arc,
  time::{Duration, Instant},
};

use aok::Void;
use compio::runtime::Runtime;
use wedb::server::{
  cluster_manager::ClusterManager,
  cluster_provider::ClusterProvider,
  failover::{failover_manager::FailoverManager, failover_option::FailoverOption},
  hash_slot::SlotState,
  replication::recovery_status::RecoveryStatus,
  worker::{LOCAL_WORKER_ID, LocalWorkerSpec, NodeRole, Worker},
};
use wtest_base::{FailoverNode, SilentNode, StopWritesNode, wait_for};

async fn wait_until(m: &FailoverManager, expect: &str) {
  m.wait_failover_done().await;
  assert_eq!(m.get_last_failover_status(), expect);
}

/// test/cluster/Garnet.test.cluster.replication/ReplicationTests/ClusterReplicationBaseTests.cs:ClusterReplicationSimpleFailover
#[test]
fn cluster_replication_simple_failover() -> Void {
  Runtime::new()?.block_on(async {
    let m = Arc::new(FailoverManager::new(Arc::new(ClusterProvider::default())));
    assert!(m.try_start_replica_failover(FailoverOption::Takeover, Duration::ZERO));
    wait_until(&m, "failover-completed").await;

    // Session completes and resets status to no-failover
    assert_eq!(m.get_failover_status(), "no-failover");

    // Primary failover
    assert!(m.try_start_primary_failover(
      "10.0.0.2",
      7000,
      FailoverOption::Takeover,
      Duration::ZERO
    ));
    m.wait_failover_done().await;
    assert_eq!(m.get_failover_status(), "no-failover");
    aok::OK
  })
}

/// test/cluster/Garnet.test.cluster.replication/ReplicationTests/ClusterReplicationBaseTests.cs:ClusterFailoverAttachReplicas
#[test]
fn cluster_failover_attach_replicas() -> Void {
  Runtime::new()?.block_on(async {
    // 1. Replica takes over primary slots
    let replica = ClusterManager::new(Arc::new(ClusterProvider::default()));
    {
      let mut config = replica.current_config.write();
      config.initialize_local_worker(LocalWorkerSpec {
        node_id: 0x0000_0000_0000_0000_0000_0000_0002_E701,
        address: "10.0.0.2",
        port: 7002,
        config_epoch: 0,
        role: NodeRole::Replica,
        replica_of_node_id: Some(0x0000_0000_0000_0000_0000_0000_0000_DE11),
        hostname: None,
      });
      let p_idx = {
        config.workers.push(Worker {
          nodeid: Some(0x0000_0000_0000_0000_0000_0000_0000_DE11),
          address: "10.0.0.1".into(),
          port: 7001,
          config_epoch: 3,
          role: NodeRole::Primary,
          replica_of_node_id: None,
          replication_offset: 0,
          hostname: None,
        });
        (config.workers.len() - 1) as u16
      };
      config.assign_slots(&[0, 1], p_idx, SlotState::Stable);
    }
    assert!(replica.try_take_over_for_primary());
    {
      let config = replica.current_config.read();
      assert!(config.is_primary());
      assert_eq!(config.local_node_primary_id(), None);
      assert_eq!(config.get_worker_id_from_slot(0), LOCAL_WORKER_ID);
      assert!(config.local_node_config_epoch() > 3);
    }

    // 2. Failover manager concurrent exclusion & abort recovery
    let m = Arc::new(FailoverManager::new(Arc::new(ClusterProvider::default())));
    assert!(m.try_start_replica_failover(FailoverOption::Takeover, Duration::ZERO));
    assert!(
      !m.try_start_replica_failover(FailoverOption::Takeover, Duration::ZERO),
      "concurrent failover rejected while session runs"
    );
    wait_until(&m, "failover-completed").await;

    // Abort detaches session and recovers lock
    assert!(m.try_start_replica_failover(FailoverOption::Takeover, Duration::ZERO));
    m.try_abort_replica_failover();
    assert_eq!(m.get_failover_status(), "no-failover");
    wait_until(&m, "failover-aborted").await;

    // 锁已释放，可再次启动并顺利完成
    assert!(m.try_start_replica_failover(FailoverOption::Takeover, Duration::ZERO));
    wait_until(&m, "failover-completed").await;

    aok::OK
  })
}

/// 模拟已有恢复锁被占用，接管必须被拒绝
#[test]
fn cluster_failover_lock_rejection() -> Void {
  Runtime::new()?.block_on(async {
    let cp = ClusterProvider::new();
    let rm = cp.replication_manager().unwrap();
    // 模拟已有恢复锁被占用，接管必须被拒绝
    assert!(rm.begin_recovery(RecoveryStatus::ClusterReplicate, false));

    let m = Arc::new(FailoverManager::new(cp));
    assert!(m.try_start_replica_failover(FailoverOption::Takeover, Duration::ZERO));
    wait_until(&m, "failover-aborted").await;

    aok::OK
  })
}

/// test/cluster/Garnet.test.cluster/ClusterManagementTests.cs:ClusterFailoverBadOptions
#[test]
fn cluster_failover_bad_options() -> Void {
  let m = Arc::new(FailoverManager::new(Arc::new(ClusterProvider::default())));
  assert_eq!(m.get_failover_status(), "no-failover");
  aok::OK
}

/// 装配副本节点拓扑：replica_node（本地）挂 primary_node@primary_port，持槽 0..2
fn replica_provider(primary_port: i32) -> Arc<ClusterProvider> {
  let cp = ClusterProvider::new();
  let m = cp.cluster_manager().expect("cluster manager 在场");
  {
    let mut config = m.current_config.write();
    config.initialize_local_worker(LocalWorkerSpec {
      node_id: 0x0000_0000_0000_0000_0000_0000_0002_E701,
      address: "127.0.0.1",
      port: 7002,
      config_epoch: 0,
      role: NodeRole::Replica,
      replica_of_node_id: Some(0x0000_0000_0000_0000_0000_0000_0000_DE11),
      hostname: None,
    });
    let p_idx = {
      config.workers.push(Worker {
        nodeid: Some(0x0000_0000_0000_0000_0000_0000_0000_DE11),
        address: "127.0.0.1".into(),
        port: primary_port,
        config_epoch: 3,
        role: NodeRole::Primary,
        replica_of_node_id: None,
        replication_offset: 0,
        hostname: None,
      });
      (config.workers.len() - 1) as u16
    };
    config.assign_slots(&[0, 1], p_idx, SlotState::Stable);
  }
  cp
}

/// 专用连接不毒化 gossip 连接池（对标 ReplicaFailoverSession.cs CreateConnectionAsync 每次新建语义）：failover 恒新建专用连接，gossip
/// connection_store 中的共享 client 不被复用、不被 dispose
#[test]
fn cluster_failover_dedicated_connection_keeps_gossip_store_intact() -> Void {
  Runtime::new().unwrap().block_on(async {
    let fake = SilentNode::bind(2).await; // 仅握手应答：停写命令将沉默超时
    let cp = replica_provider(fake.port() as i32);

    // 预置 gossip 连接池中指向主端的共享连接并建链成功
    let gm = cp.gossip_manager().expect("gossip manager 在场");
    let conn = gm.connection_store.get_or_add(
      0x0000_0000_0000_0000_0000_0000_0000_DE11,
      "127.0.0.1",
      fake.port() as i32,
      &cp,
    );
    conn.initialize_async().await;
    assert!(conn.client.is_connected(), "gossip 共享连接应已建立");

    let m = Arc::new(FailoverManager::new(Arc::clone(&cp)));
    assert!(m.try_start_replica_failover(FailoverOption::Default, Duration::from_millis(300)));
    m.wait_failover_done().await;
    // 停写应答沉默超时 → 本次 failover 放弃
    assert_eq!(m.get_last_failover_status(), "failover-aborted");

    // failover 全程用自建专用连接：gossip 共享连接不被动用、不被 dispose
    assert!(
      conn.client.is_connected(),
      "gossip 共享连接不应被 failover 毒化"
    );
    aok::OK
  })
}

/// 控制命令超时治理（覆盖 C# PauseWritesAndWaitForSyncAsync 的 WaitAsync 超时语义）：主端假端点握手后对 CLUSTER FAILSTOPWRITES 全程沉默，
/// failover 任务须在 failover_timeout 内以 failover-aborted 收场而非无限挂起
#[test]
fn cluster_failover_control_command_timeout_aborts() -> Void {
  Runtime::new().unwrap().block_on(async {
    let fake = SilentNode::bind(2).await; // 握手成功后对控制命令全程沉默
    let cp = replica_provider(fake.port() as i32);

    let m = Arc::new(FailoverManager::new(Arc::clone(&cp)));
    let start = Instant::now();
    assert!(m.try_start_replica_failover(FailoverOption::Default, Duration::from_millis(300)));
    m.wait_failover_done().await;
    let elapsed = start.elapsed();
    assert_eq!(m.get_last_failover_status(), "failover-aborted");
    assert!(
      elapsed < Duration::from_secs(5),
      "停写应答超时后 failover 应收敛而非挂起: {elapsed:?}"
    );
    aok::OK
  })
}

/// abort 打断在途位点等待（对标 C# TryAbortReplicaFailover 经 cts.Cancel
/// 打断 WaitAsync 的取消语义）：主端确认停写并应答领先位点、会话挂起等
/// 位点追平时下发 abort——位点等待立即收口、会话按 failover-aborted 结束、
/// 向主端回发空载荷停写复位（主端恢复写），全程不等满剩余超时
#[test]
fn cluster_failover_abort_interrupts_offset_wait() -> Void {
  Runtime::new().unwrap().block_on(async {
    // 领先本地位点的停写确认应答：副本永远追不上，位点等待只能靠 abort
    // 打断或挂满超时（超时给足 30s，若打断失效测试必红）
    let probe = ClusterProvider::new();
    let mut ahead = probe
      .replication_manager()
      .expect("replication manager 在场")
      .get_current_replication_offset();
    ahead.set(0, ahead.get(0).unwrap_or(0) + 1000);
    let primary = StopWritesNode::bind(Arc::new(ahead.to_aof_string())).await;
    let cp = replica_provider(primary.port() as i32);

    let m = Arc::new(FailoverManager::new(Arc::clone(&cp)));
    assert!(m.try_start_replica_failover(FailoverOption::Default, Duration::from_secs(30)));
    // 等会话进入位点等待阶段（主端已确认停写并应答领先位点）
    assert!(
      wait_for(
        || m.get_failover_status() == "waiting-for-sync",
        Duration::from_secs(5)
      )
      .await,
      "会话应进入 waiting-for-sync"
    );

    let start = Instant::now();
    m.try_abort_replica_failover();
    wait_until(&m, "failover-aborted").await;
    let elapsed = start.elapsed();

    assert!(
      elapsed < Duration::from_secs(5),
      "abort 应立即打断位点等待而非挂满剩余超时: {elapsed:?}"
    );
    assert!(
      primary.reset_received(),
      "abort 后应回发空载荷停写复位，主端恢复写"
    );
    aok::OK
  })
}

/// 装配主节点拓扑：primary_node（本地）挂两个候选副本探测靶端。靶端 worker
/// role 取 Primary 供 get_local_node_primary_endpoints 收集端点、
/// replica_of_node_id 登记供 get_replica_ids/try_stop_writes 走通
fn primary_provider(slow_port: u16, fast_port: u16) -> Arc<ClusterProvider> {
  let cp = ClusterProvider::new();
  let m = cp.cluster_manager().expect("cluster manager 在场");
  {
    let mut config = m.current_config.write();
    config.initialize_local_worker(LocalWorkerSpec {
      node_id: 0x0000_0000_0000_0000_0000_0000_0000_DE11,
      address: "127.0.0.1",
      port: 7001,
      config_epoch: 3,
      role: NodeRole::Primary,
      replica_of_node_id: None,
      hostname: None,
    });
    for (node_id, port) in [(0x5107, slow_port), (0xFA57, fast_port)] {
      config.workers.push(Worker {
        nodeid: Some(node_id),
        address: "127.0.0.1".into(),
        port: port as i32,
        config_epoch: 3,
        role: NodeRole::Primary,
        replica_of_node_id: Some(0x0000_0000_0000_0000_0000_0000_0000_DE11),
        replication_offset: 0,
        hostname: None,
      });
    }
  }
  cp
}

/// 本地位点应答载荷（靶端以此应答 FAILREPLICATIONOFFSET 即视为位点追平）
fn local_offset_reply() -> Arc<String> {
  let cp = ClusterProvider::new();
  Arc::new(
    cp.replication_manager()
      .expect("replication manager 在场")
      .get_current_replication_offset()
      .to_aof_string(),
  )
}

/// 首副本探测竞速取快者（对标 PrimaryFailoverSession.cs:WaitForFirstReplicaSyncAsync
/// 的 Task.WhenAny 竞速语义）：一慢一快副本并发探测、慢者排前，快副本位点
/// 追平即刻当选接管，总耗时不受慢副本应答拖累（串行探测会先等慢者应答）
#[test]
fn cluster_failover_race_picks_fast_replica() -> Void {
  Runtime::new().unwrap().block_on(async {
    let offset_reply = local_offset_reply();
    let slow = FailoverNode::bind(Arc::clone(&offset_reply), Duration::from_millis(1500)).await;
    let fast = FailoverNode::bind(offset_reply, Duration::ZERO).await;
    let cp = primary_provider(slow.port(), fast.port());

    let m = Arc::new(FailoverManager::new(cp));
    let start = Instant::now();
    assert!(m.try_start_primary_failover(
      "",
      -1,
      FailoverOption::Takeover,
      Duration::from_secs(10)
    ));
    m.wait_failover_done().await;
    let elapsed = start.elapsed();

    assert!(fast.takeover_received(), "位点先追平的快副本应当选接管");
    assert!(!slow.takeover_received(), "慢副本不应被当选");
    // C# finally：无论成败状态归位 no-failover
    assert_eq!(m.get_failover_status(), "no-failover");
    assert!(
      elapsed < Duration::from_millis(1000),
      "竞速不应等待慢副本应答: {elapsed:?}"
    );
    aok::OK
  })
}

/// 首副本探测整体超时（对标 DelayToDefaultAsync(failoverTimeout) 哨兵竞速）：
/// 双慢副本均未在 failover_timeout 内应答追平，探测须整体超时收敛收场，
/// 而非按 N×cluster_timeout 串行累计挂起
#[test]
fn cluster_failover_race_overall_timeout() -> Void {
  Runtime::new().unwrap().block_on(async {
    let offset_reply = local_offset_reply();
    let slow_a = FailoverNode::bind(Arc::clone(&offset_reply), Duration::from_secs(30)).await;
    let slow_b = FailoverNode::bind(offset_reply, Duration::from_secs(30)).await;
    let cp = primary_provider(slow_a.port(), slow_b.port());

    let m = Arc::new(FailoverManager::new(cp));
    let start = Instant::now();
    assert!(m.try_start_primary_failover(
      "",
      -1,
      FailoverOption::Takeover,
      Duration::from_millis(300)
    ));
    m.wait_failover_done().await;
    let elapsed = start.elapsed();

    assert!(
      !slow_a.takeover_received() && !slow_b.takeover_received(),
      "整体超时不应有副本被接管"
    );
    assert_eq!(m.get_failover_status(), "no-failover");
    assert!(
      elapsed < Duration::from_secs(5),
      "整体超时应由 failover_timeout 哨兵收敛而非串行累计: {elapsed:?}"
    );
    aok::OK
  })
}

/// 集群节点超时动态生效（对标 FailoverManager.cs:clusterTimeout 动态属性，
/// 每次发起 failover 即时求值传入会话）：cluster_provider 的毫秒槽即时生效，
/// 不再是硬编码 60s——限时小于副本应答延迟时候选探测按超时放弃，调大后
/// 同延迟副本可当选接管
#[test]
fn cluster_failover_cluster_timeout_from_provider() -> Void {
  Runtime::new().unwrap().block_on(async {
    let offset_reply = local_offset_reply();
    let replica = FailoverNode::bind(offset_reply, Duration::from_millis(800)).await;

    // 限时 200ms < 应答延迟 800ms：探测超时空应答，候选落选；
    // failover_timeout 给足，收敛须由探测限时驱动而非哨兵
    let cp = primary_provider(replica.port(), replica.port());
    cp.set_cluster_node_timeout_ms(200);
    let m = Arc::new(FailoverManager::new(cp));
    let start = Instant::now();
    assert!(m.try_start_primary_failover(
      "",
      -1,
      FailoverOption::Takeover,
      Duration::from_secs(10)
    ));
    m.wait_failover_done().await;
    assert!(!replica.takeover_received(), "限时小于应答延迟时候选应落选");
    let elapsed = start.elapsed();
    assert!(
      elapsed < Duration::from_secs(5),
      "探测限时收敛不应拖到哨兵: {elapsed:?}"
    );

    // 限时 3s > 应答延迟 800ms：同一延迟副本当选接管
    let cp = primary_provider(replica.port(), replica.port());
    cp.set_cluster_node_timeout_ms(3000);
    let m = Arc::new(FailoverManager::new(cp));
    assert!(m.try_start_primary_failover(
      "",
      -1,
      FailoverOption::Takeover,
      Duration::from_secs(10)
    ));
    m.wait_failover_done().await;
    assert!(replica.takeover_received(), "限时大于应答延迟时候选应当选");
    aok::OK
  })
}
