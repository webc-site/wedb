//! 集群复制集成测试，对标 Garnet ClusterReplicationBaseTests
use std::{sync::Arc, time::Duration};

use aok::Void;
use compio::{runtime::Runtime, time::sleep};
use wedb::server::{
  cluster::IClusterProvider,
  cluster_config::ClusterConfig,
  cluster_manager::ClusterManager,
  cluster_provider::ClusterProvider,
  hash_slot::SlotState,
  replication::{recovery_status::RecoveryStatus, replication_manager::ReplicationManager},
  worker::{LOCAL_WORKER_ID, LocalWorkerSpec, NodeRole, Worker},
};

/// test/cluster/Garnet.test.cluster.replication/ReplicationTests/ClusterReplicationBaseTests.cs:ClusterSRReplicaOfTest
#[test]
fn cluster_sr_replica_of_test() -> Void {
  let mut config = ClusterConfig::new();
  config.initialize_local_worker(LocalWorkerSpec {
    node_id: "replica_node",
    address: "127.0.0.1",
    port: 7001,
    config_epoch: 2,
    role: NodeRole::Primary,
    replica_of_node_id: None,
    hostname: None,
  });

  config.workers.push(Worker {
    nodeid: Some("primary_node".to_string()),
    address: "127.0.0.1".to_string(),
    port: 7000,
    config_epoch: 1,
    role: NodeRole::Primary,
    replica_of_node_id: None,
    replication_offset: 0,
    hostname: None,
  });

  config.make_replica_of(Some("primary_node"));
  assert!(config.is_replica());
  assert_eq!(config.local_node_primary_id(), Some("primary_node"));

  // Reset replica back to primary
  let cp = Arc::new(ClusterProvider::default());
  let m = ClusterManager::new(cp);
  *m.current_config.write() = config;
  m.try_reset_replica();
  assert!(m.current_config.read().is_primary());
  assert_eq!(m.current_config.read().local_node_primary_id(), None);

  aok::OK
}

/// test/cluster/Garnet.test.cluster.replication/ReplicationTests/ClusterReplicationBaseTests.cs:ClusterSRTest
#[test]
fn cluster_sr_test() {
  let rm = ReplicationManager::new();
  // 对标 C#：初始位点为 kFirstValidAofAddress(64)
  assert_eq!(rm.get_replication_offset(0), 64);

  rm.set_sublog_replication_offset(0, 4096);
  assert_eq!(rm.get_replication_offset(0), 4096);

  // 对标 C#：SetSublogReplicationOffset 直接赋值
  rm.set_sublog_replication_offset(0, 2048);
  assert_eq!(rm.get_replication_offset(0), 2048);
  rm.set_sublog_replication_offset(0, 8192);
  assert_eq!(rm.get_replication_offset(0), 8192);

  let init_id = rm.primary_repl_id();
  assert_eq!(init_id.len(), 40);

  // Failover rotations
  rm.try_update_for_failover();
  assert_eq!(rm.primary_repl_id2(), init_id);
  assert_ne!(rm.primary_repl_id(), init_id);
  assert_eq!(rm.get_replication_offset2().get(0), Some(8192));

  // Recovery status transitions
  assert_eq!(rm.recovery_status(), RecoveryStatus::NoRecovery);
  assert!(!rm.is_recovering());

  assert!(rm.begin_recovery(RecoveryStatus::ClusterReplicate, false));
  assert!(rm.is_recovering());
  assert!(rm.cannot_stream_aof());

  rm.end_recovery(RecoveryStatus::CheckpointRecoveredAtReplica, false);
  assert!(rm.is_recovering());
  assert!(!rm.cannot_stream_aof());

  rm.reset_recovery();
  assert_eq!(rm.recovery_status(), RecoveryStatus::NoRecovery);
  assert!(!rm.is_recovering());
}

/// test/cluster/Garnet.test.cluster.replication/ReplicationTests/ClusterReplicationBaseTests.cs:ClusterSRRedirectWrites
#[test]
fn cluster_sr_redirect_writes() {
  let mut primary = ClusterConfig::new();
  primary.initialize_local_worker(LocalWorkerSpec {
    node_id: "primary_1",
    address: "127.0.0.1",
    port: 7000,
    config_epoch: 1,
    role: NodeRole::Primary,
    replica_of_node_id: None,
    hostname: None,
  });
  primary.assign_slots(&[10], LOCAL_WORKER_ID as u16, SlotState::Stable);

  let mut replica = ClusterConfig::new();
  replica.initialize_local_worker(LocalWorkerSpec {
    node_id: "replica_1",
    address: "127.0.0.1",
    port: 7001,
    config_epoch: 1,
    role: NodeRole::Replica,
    replica_of_node_id: Some("primary_1"),
    hostname: None,
  });
  replica.workers.push(Worker {
    nodeid: Some("primary_1".to_string()),
    address: "127.0.0.1".to_string(),
    port: 7000,
    config_epoch: 1,
    role: NodeRole::Primary,
    replica_of_node_id: None,
    replication_offset: 0,
    hostname: None,
  });
  replica.update_slot_state(10, 2, SlotState::Stable);

  // Write path on replica is not local -> redirects with MOVED
  assert!(!replica.is_local(10, false));
  // Read path on replica sees primary slots
  assert!(replica.is_local(10, true));
  // Write path on primary is local
  assert!(primary.is_local(10, false));
}

/// EnsureReplication 判定链（对标 C# ReplicationManager.EnsureReplication 的
/// 节流 / 角色归属 / IsReplicating 状态面 / failover 抑制四重门）
#[test]
fn ensure_replication_gate_chain() -> Void {
  // 第 7 步重连动作经 compio::runtime::spawn 后台驱动（生产中本函数总在
  // gossip / 集群会话的 runtime 内被调用），测试同形包进 runtime
  Runtime::new().unwrap().block_on(async {
    let provider = ClusterProvider::new();

    // 1. 轮询频率 0 = 禁用：不推进心跳
    provider.ensure_replication(Some("primary_1"));
    let rm = provider.replication_manager().unwrap();
    assert_eq!(rm.last_primary_sync_seconds(), 0);

    // 2. 开启轮询 + 设为 REPLICA：到期即推进心跳（gossip 来自其 primary）
    provider.set_replication_reestablishment_timeout(60);
    let cm = provider.cluster_manager().unwrap();
    cm.try_initialize_local_worker(LocalWorkerSpec {
      node_id: "replica_node",
      address: "127.0.0.1",
      port: 7001,
      config_epoch: 1,
      role: NodeRole::Replica,
      replica_of_node_id: Some("primary_1"),
      hostname: None,
    });
    provider.ensure_replication(Some("primary_1"));
    assert!(!rm.has_active_replication_stream());
    rm.update_last_primary_sync_time();
    assert!(rm.last_primary_sync_seconds() >= 0);

    // 3. 节流：频率窗口内第二次调用不再重置尝试时间戳（经 due 判定面验证）
    assert!(!rm.ensure_replication_due(60), "窗口内应被节流");
    assert!(rm.ensure_replication_due(0), "频率 0 到期判定恒真");

    // 4. IsReplicating 状态面：注册副本重放驱动后活跃复制流为真
    assert!(rm.initialize_replica_replay_driver(0));
    assert!(rm.has_active_replication_stream());

    // 5. failover 抑制：无进行中 failover 时判定面为假
    let fm = provider.failover_manager().unwrap();
    assert!(!fm.is_failover_in_progress());

    // 6. 会话归属 + 禁用复位：非 primary 会话与频率归零后判定链不再有动作面
    provider.set_replication_reestablishment_timeout(0);
    provider.ensure_replication(Some("other_node"));
    provider.ensure_replication(Some("primary_1"));
    Ok(())
  })
}

/// EnsureReplication 第 7 步重连动作面（对标 C# PreventRoleChange +
/// 后台 RecoverReplication 任务 + finally AllowRoleChange 的完整时序；
/// 重连动作 = replication::assembly::recover_replication 后台直调）
#[test]
fn ensure_replication_reconnect_action_face() -> Void {
  Runtime::new().unwrap().block_on(async {
    let provider = ClusterProvider::new();
    provider.set_replication_reestablishment_timeout(1);

    // 副本角色（of primary_1），无活跃复制流（断链态）
    let cm = provider.cluster_manager().unwrap();
    cm.try_initialize_local_worker(LocalWorkerSpec {
      node_id: "replica_node",
      address: "127.0.0.1",
      port: 7001,
      config_epoch: 1,
      role: NodeRole::Replica,
      replica_of_node_id: Some("primary_1"),
      hostname: None,
    });
    let rm = provider.replication_manager().unwrap();
    assert!(!rm.has_active_replication_stream());

    // 断链触发：第 7 步同步段 PreventRoleChange，后台任务直调
    // recover_replication（首步清空重放驱动仓库，对标 C#
    // TryReplicateDiskbasedSyncAsync 的 ResetReplicaReplayDriverStore）
    provider.ensure_replication(Some("primary_1"));
    // 角色锁在重连动作全程被持有（C# PreventRoleChange 的 TOCTOU 防护：
    // 后台任务尚未执行，锁仍被持有）
    assert!(!provider.prevent_role_change(), "重连期间角色锁必须被持有");
    sleep(Duration::from_millis(100)).await;
    // 后台任务完成：finally 段已释放角色锁（C# AllowRoleChange）；
    // 测试环境无主端 endpoint，重连静默返回后不得残留活跃复制流
    assert!(provider.prevent_role_change(), "重连完成后角色锁必须已释放");
    provider.allow_role_change();
    assert!(
      !rm.has_active_replication_stream(),
      "重连静默后不得残留活跃复制流"
    );

    // 轮询窗口内第二次调用被节流：不再进入第 7 步（锁未被 prevent 即为反证）
    provider.ensure_replication(Some("primary_1"));
    assert!(
      provider.prevent_role_change(),
      "轮询窗口内第二次调用必须被节流"
    );
    provider.allow_role_change();
    Ok(())
  })
}
