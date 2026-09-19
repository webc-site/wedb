//! 集群复制集成测试，对标 Garnet ClusterReplicationBaseTests
use std::{
  sync::{Arc, atomic::Ordering},
  time::Duration,
};

use aok::Void;
use compio::{runtime::Runtime, time::sleep};
use waof::AofAddress;
use wbase::time::now_ms;
use wedb::server::{
  cluster::IClusterProvider,
  cluster_config::ClusterConfig,
  cluster_manager::ClusterManager,
  cluster_provider::ClusterProvider,
  failover::failover_status::FailoverStatus,
  hash_slot::SlotState,
  replication::{
    aof_sync_driver::AofSyncDriver, assembly::recover_replication, recovery_status::RecoveryStatus,
    replication_manager::ReplicationManager,
  },
  worker::{LOCAL_WORKER_ID, LocalWorkerSpec, NodeRole, Worker},
};

/// 测试节点身份（内部 u128；协议面渲染 32 字符小写 hex）
const PRIMARY_ID: u128 = 0x0DE1_0000_0000_0000_0000_0000_0000_0001;
const REPLICA_ID: u128 = 0x0DE1_0000_0000_0000_0000_0000_0000_0002;

/// test/cluster/Garnet.test.cluster.replication/ReplicationTests/ClusterReplicationBaseTests.cs:ClusterSRReplicaOfTest
#[test]
fn cluster_sr_replica_of_test() -> Void {
  let mut config = ClusterConfig::new();
  config.initialize_local_worker(LocalWorkerSpec {
    node_id: 0x0000_0000_0000_0000_0000_0000_0002_E701,
    address: "127.0.0.1",
    port: 7001,
    config_epoch: 2,
    role: NodeRole::Primary,
    replica_of_node_id: None,
    hostname: None,
  });

  config.workers.push(Worker {
    nodeid: Some(0x0000_0000_0000_0000_0000_0000_0000_DE11),
    address: "127.0.0.1".into(),
    port: 7000,
    config_epoch: 1,
    role: NodeRole::Primary,
    replica_of_node_id: None,
    replication_offset: 0,
    hostname: None,
  });

  config.make_replica_of(Some(0x0000_0000_0000_0000_0000_0000_0000_DE11));
  assert!(config.is_replica());
  assert_eq!(
    config.local_node_primary_id(),
    Some(0x0000_0000_0000_0000_0000_0000_0000_DE11)
  );

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
  // 对标 C#：初始位点为空日志起点（rust WalLog 无头区，判据 begin == tail）
  assert_eq!(rm.get_replication_offset(0), 0);

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
    node_id: PRIMARY_ID,
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
    node_id: REPLICA_ID,
    address: "127.0.0.1",
    port: 7001,
    config_epoch: 1,
    role: NodeRole::Replica,
    replica_of_node_id: Some(PRIMARY_ID),
    hostname: None,
  });
  replica.workers.push(Worker {
    nodeid: Some(PRIMARY_ID),
    address: "127.0.0.1".into(),
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
    provider.ensure_replication(Some(PRIMARY_ID));
    let rm = provider.replication_manager().unwrap();
    assert_eq!(rm.last_primary_sync_seconds(), 0);

    // 2. 开启轮询 + 设为 REPLICA：到期进入重连判定链，但不刷新心跳
    //（对标 C#：EnsureReplication 本体无 UpdateLastPrimarySyncTime，心跳
    // 仅随同步建立推进——见副本 APPENDLOG 初始化帧握手挂点）
    provider.set_replication_reestablishment_timeout(60);
    let cm = provider.cluster_manager().unwrap();
    cm.try_initialize_local_worker(LocalWorkerSpec {
      node_id: 0x0000_0000_0000_0000_0000_0000_0002_E701,
      address: "127.0.0.1",
      port: 7001,
      config_epoch: 1,
      role: NodeRole::Replica,
      replica_of_node_id: Some(PRIMARY_ID),
      hostname: None,
    });
    provider.ensure_replication(Some(PRIMARY_ID));
    assert!(!rm.has_active_replication_stream());
    assert_eq!(
      rm.last_primary_sync_seconds(),
      0,
      "重连轮询不得制造虚假心跳"
    );

    // 3. 节流：频率窗口内到期判定为 None（判定为纯读；消费时序见
    //    ensure_replication_window_consumed_at_tail）
    assert!(rm.ensure_replication_due(60).is_none(), "窗口内应被节流");
    assert!(
      rm.ensure_replication_due(0).is_some(),
      "频率 0 到期判定恒真"
    );

    // 4. IsReplicating 状态面：注册副本重放驱动后活跃复制流为真
    assert!(rm.initialize_replica_replay_driver(0));
    assert!(rm.has_active_replication_stream());

    // 5. failover 抑制：无进行中 failover 时判定面为假
    let fm = provider.failover_manager().unwrap();
    assert!(!fm.is_failover_in_progress());

    // 6. 会话归属 + 禁用复位：非 primary 会话与频率归零后判定链不再有动作面
    provider.set_replication_reestablishment_timeout(0);
    provider.ensure_replication(Some(0x999));
    provider.ensure_replication(Some(PRIMARY_ID));
    Ok(())
  })
}

/// EnsureReplication 节流时序拆分（对标 C# ReplicationManager.cs:192 判定为
/// `Volatile.Read` 纯读、:251-256 在 PreventRoleChange 与 TOCTOU 复检通过后才
/// `Interlocked.CompareExchange` 消费）：被角色归属 / IsReplicating / failover
/// 门挡回的到期帧一律不得推进 last_ensure_replication_attempt_ms，下一帧立即
/// 仍可判到期；四道门全开真正发起时窗口恰消费一次；CAS 基准不等（另有尝试在
/// 途）回 false 且不改写时间戳
#[test]
fn ensure_replication_window_consumed_at_tail() -> Void {
  Runtime::new().unwrap().block_on(async {
    let provider = ClusterProvider::new();
    provider.set_replication_reestablishment_timeout(60);
    let cm = provider.cluster_manager().unwrap();
    cm.try_initialize_local_worker(LocalWorkerSpec {
      node_id: 0x0000_0000_0000_0000_0000_0000_0002_E701,
      address: "127.0.0.1",
      port: 7001,
      config_epoch: 1,
      role: NodeRole::Replica,
      replica_of_node_id: Some(PRIMARY_ID),
      hostname: None,
    });
    let rm = provider.replication_manager().unwrap();
    let attempt_ms = || {
      rm.last_ensure_replication_attempt_ms
        .load(Ordering::Acquire)
    };

    // 预置早已到期的尝试时间戳（非 0：挡回帧若误消费即被推到当下，可断言）
    let stale_ms = now_ms() as i64 - 10 * 60 * 1000;
    rm.last_ensure_replication_attempt_ms
      .store(stale_ms, Ordering::Release);

    // 1. 第 3 步角色归属门挡回：活跃会话非本 primary
    provider.ensure_replication(Some(0x999));
    assert_eq!(attempt_ms(), stale_ms, "非本 primary 的到期帧不得消费窗口");

    // 2. 第 4 步 IsReplicating 状态面门挡回
    assert!(rm.initialize_replica_replay_driver(0));
    provider.ensure_replication(Some(PRIMARY_ID));
    assert_eq!(attempt_ms(), stale_ms, "活跃复制流在册的到期帧不得消费窗口");
    assert!(
      rm.ensure_replication_due(60).is_some(),
      "挡回后下一帧必须仍立即可判到期"
    );
    rm.replica_replay_driver_store.reset();
    assert!(!rm.has_active_replication_stream());

    // 3. 第 5 步 failover 抑制门挡回
    let fm = provider.failover_manager().unwrap();
    *fm.last_failover_status.write() = FailoverStatus::FailoverInProgress;
    provider.ensure_replication(Some(PRIMARY_ID));
    assert_eq!(
      attempt_ms(),
      stale_ms,
      "failover 抑制期的到期帧不得消费窗口"
    );
    assert!(
      rm.ensure_replication_due(60).is_some(),
      "挡回后下一帧必须仍立即可判到期"
    );
    *fm.last_failover_status.write() = FailoverStatus::NoFailover;

    // 4. 四道门全开：判定回读消费基准原值，真正发起才消费一次
    let observed_ms = rm.ensure_replication_due(60).expect("全门开后应判到期");
    assert_eq!(observed_ms, stale_ms, "到期判定须回读未消费的原值");
    provider.ensure_replication(Some(PRIMARY_ID));
    let consumed_ms = attempt_ms();
    assert!(consumed_ms > stale_ms, "窗口只应由真正发起的这一帧消费一次");
    assert!(
      rm.ensure_replication_due(60).is_none(),
      "消费后同窗口内不得再判到期"
    );

    // 5. CAS 失败臂（C# :252-256 another-attempt bail）：基准已被推进即回
    // false 且不得改写时间戳（调用方据此 AllowRoleChange 放弃本轮）
    assert!(!rm.try_consume_ensure_replication_window(observed_ms));
    assert_eq!(attempt_ms(), consumed_ms, "CAS 不等不得改写尝试时间戳");
    assert!(rm.try_consume_ensure_replication_window(consumed_ms));
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
      node_id: 0x0000_0000_0000_0000_0000_0000_0002_E701,
      address: "127.0.0.1",
      port: 7001,
      config_epoch: 1,
      role: NodeRole::Replica,
      replica_of_node_id: Some(PRIMARY_ID),
      hostname: None,
    });
    let rm = provider.replication_manager().unwrap();
    assert!(!rm.has_active_replication_stream());

    // 断链触发：第 7 步同步段 PreventRoleChange，后台任务直调
    // recover_replication（首步清空重放驱动仓库，对标 C#
    // TryReplicateDiskbasedSyncAsync 的 ResetReplicaReplayDriverStore）
    provider.ensure_replication(Some(PRIMARY_ID));
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
    provider.ensure_replication(Some(PRIMARY_ID));
    assert!(
      provider.prevent_role_change(),
      "轮询窗口内第二次调用必须被节流"
    );
    provider.allow_role_change();
    Ok(())
  })
}

/// recover_replication attach 序列同步清残留主端推流驱动（对标 C#
/// ReplicaSyncAttachTaskAsync 相邻序列 ResetReplicaReplayDriverStore +
/// aofSyncDriverStore.Reset——"Remove aofSync tasks if this node was a
/// primary"）：断链重连时本节点可能刚被 gossip 翻回主角色残留驱动，
/// 无主端 endpoint 回 Err 的形态下也必须在第 1 步同步清空
#[test]
fn recover_replication_resets_aof_sync_driver_store() -> Void {
  Runtime::new().unwrap().block_on(async {
    let provider = ClusterProvider::new();
    // 副本角色（of PRIMARY_ID；workers 表无该主端条目 = 无主端 endpoint）
    let cm = provider.cluster_manager().unwrap();
    cm.try_initialize_local_worker(LocalWorkerSpec {
      node_id: 0x0000_0000_0000_0000_0000_0000_0002_E701,
      address: "127.0.0.1",
      port: 7001,
      config_epoch: 1,
      role: NodeRole::Replica,
      replica_of_node_id: Some(PRIMARY_ID),
      hostname: None,
    });

    // 预置主端残留推流驱动
    let rm = provider.replication_manager().unwrap();
    let d = Arc::new(AofSyncDriver::new(
      0x0000_0000_0000_0000_0000_0000_0002_E701,
      0x0000_0000_0000_0000_0000_0000_0000_DE22,
      1,
      &AofAddress::create(1, 0),
      None,
    ));
    assert!(
      rm.aof_sync_driver_store
        .try_add_replication_driver(d, false)
    );
    assert_eq!(
      rm.aof_sync_driver_store.count(),
      1,
      "前置：残留推流驱动在册"
    );

    // 直调重连动作：endpoint 缺席回 Err（供命令臂回 -ERR 文案），
    // 前置清驱动段必须已执行
    let err = recover_replication(&provider, PRIMARY_ID)
      .await
      .expect_err("无主端 endpoint 必须回 Err，不得静默成功");
    assert!(
      err.contains("primary endpoint unknown"),
      "实际错误文案：{err}"
    );
    assert_eq!(
      rm.aof_sync_driver_store.count(),
      0,
      "attach 序列必须同步清空残留主端推流驱动"
    );
    Ok(())
  })
}
