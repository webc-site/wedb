//! 集群故障转移集成测试，对标 Garnet 复制与故障转移测试
use std::{sync::Arc, time::Duration};

use aok::Void;
use compio::{runtime::Runtime, time::sleep};
use wedb::server::{
  cluster_manager::ClusterManager,
  cluster_provider::ClusterProvider,
  failover::{
    failover_manager::FailoverManager, failover_option::FailoverOption,
    failover_session::FailoverSession, failover_status::FailoverStatus,
  },
  hash_slot::SlotState,
  replication::recovery_status::RecoveryStatus,
  worker::{LOCAL_WORKER_ID, LocalWorkerSpec, NodeRole, Worker},
};

async fn wait_until(m: &FailoverManager, expect: &str) {
  for _ in 0..200 {
    if m.get_last_failover_status() == expect {
      return;
    }
    sleep(Duration::from_millis(5)).await;
  }
  panic!("后台会话未收敛: last={}", m.get_last_failover_status());
}

/// test/cluster/Garnet.test.cluster.replication/ReplicationTests/ClusterReplicationBaseTests.cs:ClusterReplicationSimpleFailover
#[test]
fn cluster_replication_simple_failover() -> Void {
  Runtime::new()?.block_on(async {
    let m = Arc::new(FailoverManager::new(Arc::new(ClusterProvider::default())));
    assert!(m.try_start_replica_failover(FailoverOption::Default, Duration::ZERO));
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
    for _ in 0..200 {
      if m.get_failover_status() == "no-failover" {
        break;
      }
      sleep(Duration::from_millis(5)).await;
    }
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

/// test/cluster/Garnet.test.cluster/ClusterManagementTests.cs:ClusterFailoverBadOptions
#[test]
fn cluster_failover_bad_options() {
  let s = FailoverSession::new(
    Arc::new(ClusterProvider::default()),
    FailoverOption::Takeover,
    Duration::ZERO,
    Duration::from_secs(1),
    false,
    "",
    -1,
  );
  // Idempotent dispose
  s.dispose();
  s.dispose();
  assert_eq!(s.status(), FailoverStatus::BeginFailover);
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
