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
use wedb_test::SilentNode;

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
        node_id: "replica_node",
        address: "10.0.0.2",
        port: 7002,
        config_epoch: 0,
        role: NodeRole::Replica,
        replica_of_node_id: Some("primary_node"),
        hostname: None,
      });
      let p_idx = {
        config.workers.push(Worker {
          nodeid: Some("primary_node".to_string()),
          address: "10.0.0.1".to_string(),
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

/// test/cluster/Garnet.test.cluster.replication/ReplicationTests/ClusterReplicationBaseTests.cs:ClusterFailoverLockRejection
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
      node_id: "replica_node",
      address: "127.0.0.1",
      port: 7002,
      config_epoch: 0,
      role: NodeRole::Replica,
      replica_of_node_id: Some("primary_node"),
      hostname: None,
    });
    let p_idx = {
      config.workers.push(Worker {
        nodeid: Some("primary_node".to_string()),
        address: "127.0.0.1".to_string(),
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
    let conn = gm
      .connection_store
      .get_or_add("primary_node", "127.0.0.1", fake.port() as i32);
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
