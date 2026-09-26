//! 集群复制集成测试，对标 Garnet ClusterReplicationBaseTests
use std::{
  io,
  num::NonZeroUsize,
  path::Path,
  sync::{Arc, atomic::Ordering},
  time::Duration,
};

use aok::{Result, Void};
use compio::{
  runtime::{Runtime, spawn},
  time::sleep,
};
use waof::AofAddress;
use wbase::{hash_slot::CLUSTER_SLOT_COUNT, time::now_ms_i64};
use wconf::RuntimeServerOptions;
use wconn::client::GarnetClient;
use wdev::SegmentedDevice;
use wedb::server::{
  cluster::IClusterProvider,
  cluster_config::ClusterConfig,
  cluster_manager::ClusterManager,
  cluster_provider::ClusterProvider,
  failover::failover_status::FailoverStatus,
  hash_slot::SlotState,
  replication::{
    StoreCommitFn,
    aof_sync_driver::AofSyncDriver,
    assembly::{recover_replication, wire_replication_data_plane},
    recovery_status::RecoveryStatus,
    replication_manager::ReplicationManager,
  },
  worker::{LOCAL_WORKER_ID, LocalWorkerSpec, NodeRole, Worker},
};
use wedb_test::cluster_decorate;
use wkv::WedbStore;
use wnode::{
  GarnetServer, RespSessionConsumer, aof::garnet_log::GarnetLog, resp::garnet_api::StoreGarnetApi,
  service::StorageSessionProvider, storage::session::storage_session::StorageSession,
};
use wtest_base::{test_store_config, wait_for};

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
    //    ensure_replication_window_consumed_at_tail）。基线显式回填为「刚尝试
    //    过」：单调域下初值 0 与当下之差不足一个 poll 周期（与 C# 开机首秒
    //    TickCount64 − 0 同形），故不以进程寿命凑判据
    rm.last_ensure_replication_attempt_ms
      .store(now_ms_i64(), Ordering::Release);
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
#[compio::test]
async fn ensure_replication_window_consumed_at_tail() -> Void {
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
  // 单调域锚点为进程起动，10 分钟前的值在短命进程内即为负，判据不变
  let stale_ms = now_ms_i64() - 10 * 60 * 1000;
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
  rm.reset_replica_replay_driver_store();
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
}

/// EnsureReplication 第 7 步重连动作面（对标 C# PreventRoleChange +
/// 后台 RecoverReplication 任务 + finally AllowRoleChange 的完整时序；
/// 重连动作 = replication::assembly::recover_replication 后台直调）
#[compio::test]
async fn ensure_replication_reconnect_action_face() -> Void {
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

  // 预置已过期的尝试时间戳令首轮必判到期：单调域下初值 0 与当下差不足
  // 一个 poll 周期即被节流（C# 开机首秒 TickCount64 − 0 同形），首轮动作面
  // 判据不得依赖进程寿命
  rm.last_ensure_replication_attempt_ms
    .store(now_ms_i64() - 60 * 1000, Ordering::Release);

  // 断链触发：第 7 步同步段 PreventRoleChange，后台任务直调
  // recover_replication（首步清空重放驱动仓库，对标 C#
  // TryReplicateDiskbasedSyncAsync 的 ResetReplicaReplayDriverStore）
  provider.ensure_replication(Some(PRIMARY_ID));
  // 角色锁在重连动作全程被持有（C# PreventRoleChange 的 TOCTOU 防护：
  // 后台任务尚未执行，锁仍被持有）
  assert!(!provider.prevent_role_change(), "重连期间角色锁必须被持有");
  // 有界轮询至后台任务 finally 段释放角色锁（prevent 成功即锁已空，
  // 立即 allow 复位；5s 上界）
  for _ in 0..1000 {
    if provider.prevent_role_change() {
      provider.allow_role_change();
      break;
    }
    sleep(Duration::from_millis(5)).await;
  }
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
}

/// recover_replication attach 序列同步清残留主端推流驱动（对标 C#
/// ReplicaSyncAttachTaskAsync 相邻序列 ResetReplicaReplayDriverStore +
/// aofSyncDriverStore.Reset——"Remove aofSync tasks if this node was a
/// primary"）：断链重连时本节点可能刚被 gossip 翻回主角色残留驱动，
/// 无主端 endpoint 回 Err 的形态下也必须在第 1 步同步清空
#[compio::test]
async fn recover_replication_resets_aof_sync_driver_store() -> Void {
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
}

// ===== 真实节点形态端到端段（生产宿主起服 + 真协议 + 真复制数据面）=====

/// 第三节点身份（内部 u128；协议面渲染 32 字符小写 hex）
const REPLICA2_ID: u128 = 0x0DE1_0000_0000_0000_0000_0000_0000_0003;

/// 单轮写入键值规模（对标 C# keyCount 基线；ClusterDivergentReplicasTest 的
/// 初始 population 亦为 16）
const KV_COUNT: usize = 16;
/// 分叉后新主写入规模（C# `kvpairCount <<= 1`）
const DIVERGE_KV_COUNT: usize = KV_COUNT << 1;
/// 复制追平 / 引擎置换 / 重连等待上限
const SYNC_TIMEOUT: Duration = Duration::from_secs(20);
/// 断链重连轮询周期（对标 C# EnsureReplication 轮询臂节流）
const ENSURE_POLL_MS: u64 = 100;
/// 停机前尾部稳定取样的观察窗步长与上限（提交标记缓涨收敛判定）
const TAIL_SAMPLE_MS: u64 = 50;
const TAIL_SAMPLE_ROUNDS: usize = 5;

/// 节点会话供应器产物（服务器 + 供应器 + 监听端口）
type NodeBoot<D> = (
  GarnetServer<StorageSessionProvider<D>>,
  Arc<StorageSessionProvider<D>>,
  u16,
);

/// 宿主形态起服（数据路径外置支持重启恢复；`recover` 臂走 --recover 装配口。
/// 集群资产接线对标宿主 wedb/src/server/boot.rs 装配段：复制域管理器先建，
/// 存储 / 数据库管理器 / 检查点目录 / 集群句柄 / AOF 提交通道 / 置换槽
/// 逐一注入后挂复制数据面唯一装配体）
async fn boot_node<D>(
  data_path: &Path,
  provider: &Arc<ClusterProvider>,
  decorate: D,
  recover: bool,
) -> Result<NodeBoot<D>>
where
  D:
    Fn(u64, StoreGarnetApi<SegmentedDevice>) -> Option<RespSessionConsumer> + Send + Sync + 'static,
{
  let sp = if recover {
    StorageSessionProvider::open_recovered_with_config_and_aof(
      test_store_config(),
      data_path,
      None,
      RuntimeServerOptions::default(),
      false,
      decorate,
    )
    .await?
  } else {
    StorageSessionProvider::open_with_config_and_aof(
      test_store_config(),
      data_path,
      None,
      RuntimeServerOptions::default(),
      decorate,
    )?
  };
  let sp = Arc::new(sp);
  // 复制域管理器（boot.rs:110 同序：先于挂 rm 资产的注入建立；恢复臂按
  // --recover 语义回放复制历史）
  provider.initialize_replication_manager(1, Some(&sp.checkpoint_dir.join("cluster")), recover);
  provider.set_store(sp.store());
  provider.set_database_manager(Arc::clone(&sp.database_manager));
  // 集群句柄下达（flush 门控与检查点版本切换标记同源挂点，对标
  // boot.rs:126 attach_flush_gate——SAVE 经此登记复制域检查点条目）
  sp.database_manager
    .attach_flush_gate(provider.provider_handle());
  provider.set_checkpoint_dir(sp.checkpoint_dir.clone());
  if let Some(rm) = provider.replication_manager() {
    rm.set_checkpoint_dir(sp.checkpoint_dir.clone());
  }
  if let Some(aof) = sp.aof() {
    // AOF 门面注入（set_aof 同步点亮复制域日志尾动态读源与主端角色谓词）
    provider.set_aof(Some(Arc::clone(aof)));
    let log = Arc::clone(aof.log());
    let commit: StoreCommitFn = Arc::new(move |op_type, version| {
      // 检查点版本切换标记入 AOF（C# EnqueueCommit 无返回码吞错同口径）
      let _ = log.enqueue_database_commit(op_type, version);
    });
    provider.set_commit_channel(Some(commit));
  }
  provider.set_store_swap_slot(sp.store_swap_slot());
  wire_replication_data_plane(provider, sp.wal().expect("AOF 门控点亮").clone());
  if recover && let Some(rm) = provider.replication_manager() {
    if let Some(tail) = sp.recovered_aof_tail() {
      rm.set_current_replication_offset(tail);
    }
    rm.recover_async(provider.is_primary()).await;
  }

  let server = GarnetServer::new(&["127.0.0.1:0".to_string()], 65536, 100, Arc::clone(&sp))?;
  server.start(NonZeroUsize::new(1))?;
  let port = server.local_addr()?.port();
  Ok((server, sp, port))
}

/// 本地位初始化（节点 id / 端点 / 纪元 / 角色）
fn init_local(
  config: &mut ClusterConfig,
  node_id: u128,
  port: u16,
  role: NodeRole,
  replica_of: Option<u128>,
) {
  config.initialize_local_worker(LocalWorkerSpec {
    node_id,
    address: "127.0.0.1",
    port: port as i32,
    config_epoch: 1,
    role,
    replica_of_node_id: replica_of,
    hostname: None,
  });
}

/// 远程 worker 条目（互指拓扑）
fn remote_worker(node_id: u128, port: u16) -> Worker {
  Worker {
    nodeid: Some(node_id),
    address: "127.0.0.1".into(),
    port: port as i32,
    config_epoch: 1,
    role: NodeRole::Primary,
    replica_of_node_id: None,
    replication_offset: 0,
    hostname: None,
  }
}

/// 主端全槽指派（worker_id 为本地 worker）
fn assign_all_slots(config: &mut ClusterConfig) {
  let slots: Vec<usize> = (0..CLUSTER_SLOT_COUNT).collect();
  config.assign_slots(&slots, LOCAL_WORKER_ID as u16, SlotState::Stable);
}

/// RESP 客户端往返字符串应答（建连 + 单命令）
async fn client_roundtrip(endpoint: &str, command: &[&str]) -> Result<String> {
  let mut client =
    GarnetClient::new(endpoint.to_string(), None, None, Some("test".into()), 32, 0).unwrap();
  client.connect_async().await?;
  let resp = client.execute_for_string_result_async(command).await;
  resp.map_err(Into::into)
}

/// 键值集经 RESP 逐键写入并断言 +OK（对标 C# PopulatePrimary 的 SET 通道）
async fn client_set_all(endpoint: &str, kvs: &[(String, String)]) -> Result<()> {
  for (key, value) in kvs {
    let resp = client_roundtrip(endpoint, &["SET", key, value]).await?;
    assert_eq!(resp, "OK", "SET {key} 应成功");
  }
  Ok(())
}

/// 确定性键值集（C# 随机键空间换确定性序号，断言口径不受随机性影响）
fn kv_batch(prefix: &str, count: usize) -> Vec<(String, String)> {
  (0..count)
    .map(|i| (format!("{prefix}:{i:04}"), format!("v-{prefix}-{i}")))
    .collect()
}

/// 引擎直读 string 键（置换槽最新引擎取数，副本数据校验数据面）
async fn store_read(store: &Arc<WedbStore<SegmentedDevice>>, key: &[u8]) -> Option<Vec<u8>> {
  let session = store.new_session().expect("会话可得");
  let batch = session.enter_batch();
  StorageSession::new_readonly(batch)
    .read_string(key)
    .await
    .expect("读不可失败")
}

/// 逐键逐值校验（对标 C# ValidateKVCollectionAgainstReplica）
async fn assert_store_has(store: &Arc<WedbStore<SegmentedDevice>>, kvs: &[(String, String)]) {
  for (key, value) in kvs {
    let got = store_read(store, key.as_bytes()).await;
    assert_eq!(got.as_deref(), Some(value.as_bytes()), "键 {key} 值须一致");
  }
}

/// 逐键缺席校验（分叉历史不得在对齐后的副本复活）
async fn assert_store_lacks(store: &Arc<WedbStore<SegmentedDevice>>, kvs: &[(String, String)]) {
  for (key, _) in kvs {
    assert!(
      store_read(store, key.as_bytes()).await.is_none(),
      "分叉历史键 {key} 不得出现在副本"
    );
  }
}

/// 副本复制位点追平等待（对标 C# WaitForReplicaAofSync：副本上报位点须
/// 追平主端日志尾；rust 副本角色读重放链权威回推的 replication_offset）
async fn wait_replica_sync(rm: &ReplicationManager, primary_tail: u64) -> bool {
  let tail = primary_tail as i64;
  wait_for(|| rm.get_replication_offset(0) >= tail, SYNC_TIMEOUT).await
}

/// 停机前 AOF 尾地址稳定取样（提交标记在提交窗内缓涨：刷盘 → 复读直到
/// 两个样本相等，终值即停机前落盘尾）
async fn settled_tail(wal: &Arc<waof::WalLog<SegmentedDevice>>, aof_log: &Arc<GarnetLog>) -> u64 {
  let mut last = wal.tail_address();
  for _ in 0..TAIL_SAMPLE_ROUNDS {
    sleep(Duration::from_millis(TAIL_SAMPLE_MS)).await;
    aof_log.commit_async().await;
    let cur = wal.tail_address();
    if cur == last {
      break;
    }
    last = cur;
  }
  last
}

/// test/cluster/Garnet.test.cluster.replication/ReplicationTests/ClusterReplicationBaseTests.cs:ClusterSRPrimaryRestart
///
/// 全槽主节点写入 → SAVE 检查点 → 记录停机前 AOF 尾地址 → 销毁实例（保留
/// 数据目录与 nodes.conf）→ 同目录 --recover 重启 → 恢复 AOF 尾地址与停机前
/// 一致，节点身份 / 槽位归属 / 键值数据完整无损
#[compio::test]
async fn cluster_sr_primary_restart() -> Void {
  let dir = tempfile::tempdir()?;
  let data_path = dir.path().join("node.db");
  let nodes_conf = dir.path().join("nodes.conf");

  // ===== 第一代：全槽主节点（拓扑落盘对标 AddDelSlotsRange + BumpEpoch）
  let provider = ClusterProvider::new();
  let (server, sp, port) = boot_node(
    &data_path,
    &provider,
    cluster_decorate(Arc::clone(&provider)),
    false,
  )
  .await?;
  provider
    .initialize_cluster_config("127.0.0.1", port as i32, &nodes_conf, 0, false, "")
    .expect("集群拓扑装配");
  {
    let cm = provider.cluster_manager().expect("cm ready");
    let mut config = cm.current_config.write();
    init_local(&mut config, PRIMARY_ID, port, NodeRole::Primary, None);
    assign_all_slots(&mut config);
    config.bump_local_node_config_epoch();
  }
  provider.cluster_manager().expect("cm ready").flush_config();
  assert!(nodes_conf.exists(), "flush 后 nodes.conf 须落盘");

  // ===== 写入 + SAVE 检查点 + 记录停机前 AOF 尾地址
  let endpoint = format!("127.0.0.1:{port}");
  let kvs = kv_batch("restart", KV_COUNT);
  client_set_all(&endpoint, &kvs).await?;
  assert_eq!(
    client_roundtrip(&endpoint, &["SAVE"]).await?,
    "OK",
    "SAVE 检查点须成功"
  );
  let wal = sp.wal().expect("wal").clone();
  let aof_log = sp.aof().expect("aof").log().clone();
  aof_log.commit_async().await;
  let saved_tail = settled_tail(&wal, &aof_log).await;

  // ===== 停机销毁（Dispose(false)：数据目录与 nodes.conf 保留）
  server.dispose();
  drop(sp);
  drop(provider);

  // ===== 第二代：同目录 --recover 重启（cleanClusterConfig:false 语义）
  let provider2 = ClusterProvider::new();
  let (server2, sp2, port2) = boot_node(
    &data_path,
    &provider2,
    cluster_decorate(Arc::clone(&provider2)),
    true,
  )
  .await?;
  provider2
    .initialize_cluster_config("127.0.0.1", port2 as i32, &nodes_conf, 0, false, "")
    .expect("集群拓扑装配");

  // ===== 槽位归属与节点身份随 nodes.conf 恢复
  {
    let cm = provider2.cluster_manager().expect("cm ready");
    let config = cm.current_config();
    assert_eq!(
      config.local_node_id(),
      Some(PRIMARY_ID),
      "节点身份须随 nodes.conf 恢复"
    );
    assert!(
      config.has_assigned_slots(LOCAL_WORKER_ID as u16),
      "全槽归属须恢复"
    );
    assert!(
      config.is_local(0, false) && config.is_local((CLUSTER_SLOT_COUNT - 1) as u16, false),
      "首尾槽位须归属本节点"
    );
  }

  // ===== 恢复 AOF 尾地址与停机前一致 + 键值数据完整
  let recovered_tail = sp2
    .recovered_aof_tail()
    .expect("恢复装配点亮 recovered_aof_tail")
    .get(0)
    .expect("单槽地址") as u64;
  assert_eq!(recovered_tail, saved_tail, "恢复 AOF 尾地址须与停机前一致");
  let endpoint2 = format!("127.0.0.1:{port2}");
  for (key, value) in &kvs {
    assert_eq!(
      client_roundtrip(&endpoint2, &["GET", key]).await?,
      *value,
      "重启恢复后键 {key} 须完整可读"
    );
  }
  server2.dispose();
  Ok(())
}

/// test/cluster/Garnet.test.cluster.replication/ReplicationTests/ClusterReplicationBaseTests.cs:ClusterSRNoCheckpointRestartSecondary
///
/// 主从建立复制 → 初始数据同步校验 → 副本停机销毁（无检查点）→ 主端持续
/// 写入 → 副本同目录重启自动重连 → AOF 增量追平 → 逐键逐值一致
#[compio::test]
async fn cluster_sr_no_checkpoint_restart_secondary() -> Void {
  // ===== 双节点起服（主端 + 待挂副本）
  let pdir = tempfile::tempdir()?;
  let rdir = tempfile::tempdir()?;
  let primary = ClusterProvider::new();
  let replica = ClusterProvider::new();
  let (pserver, psp, pport) = boot_node(
    &pdir.path().join("node.db"),
    &primary,
    cluster_decorate(Arc::clone(&primary)),
    false,
  )
  .await?;
  let (rserver, rsp, rport) = boot_node(
    &rdir.path().join("node.db"),
    &replica,
    cluster_decorate(Arc::clone(&replica)),
    false,
  )
  .await?;

  // ===== 互指拓扑（主端持全槽）
  {
    let cm = primary.cluster_manager().expect("cm ready");
    let mut config = cm.current_config.write();
    init_local(&mut config, PRIMARY_ID, pport, NodeRole::Primary, None);
    assign_all_slots(&mut config);
    config.workers.push(remote_worker(REPLICA_ID, rport));
  }
  {
    let cm = replica.cluster_manager().expect("cm ready");
    let mut config = cm.current_config.write();
    init_local(&mut config, REPLICA_ID, rport, NodeRole::Primary, None);
    config.workers.push(remote_worker(PRIMARY_ID, pport));
  }

  // ===== REPLICAOF 建立复制流
  let p_endpoint = format!("127.0.0.1:{pport}");
  let r_endpoint = format!("127.0.0.1:{rport}");
  assert_eq!(
    client_roundtrip(&r_endpoint, &["REPLICAOF", "127.0.0.1", &pport.to_string()]).await?,
    "OK",
    "REPLICAOF 应成功"
  );
  let rm = replica.replication_manager().expect("rm ready");
  assert!(
    wait_for(|| rm.has_active_replication_stream(), SYNC_TIMEOUT).await,
    "复制流应在 REPLICAOF 后建立"
  );

  // ===== 初始数据写入 → 位点追平 → 副本数据一致
  let kvs1 = kv_batch("sr-1", KV_COUNT);
  client_set_all(&p_endpoint, &kvs1).await?;
  // 主端显式提交（提交水位 ≥ 日志尾：副本重启后续同步协商的 sync_start =
  // min(副本尾, 主端提交水位) 必须落在副本真实衔接位上，杜绝协商位点
  // 落后副本尾的拒收-重试死循环）
  psp.aof().expect("aof").log().commit_async().await;
  let tail1 = psp.wal().expect("wal").tail_address();
  assert!(wait_replica_sync(&rm, tail1).await, "初始数据位点应追平");
  assert_store_has(&replica.try_store().expect("store 在位"), &kvs1).await;

  // ===== 停机销毁副本（Dispose(false)，不执行检查点），主端持续写入
  // 对标 C# nodes[replicaIndex].Dispose(false)：
  // C# GarnetServer.Dispose -> Provider.Dispose -> AppendOnlyFile.Dispose -> TsavoriteLog.Dispose
  // 副本停机前将已接收记录显式落盘，并彻底释放旧会话句柄。
  // 副本须用纯刷盘入口（不写本地 commit 元数据帧）：副本 AOF 是主端流的
  // 严格镜像，本地帧会令重启后的增量协商位点漂出主端帧边界——主端扫描
  // 在记录负载中段读帧判 Invalid 终止，增量流死锁（C# 对偶：副本 Dispose
  // 链无 CommitAsync，TrueDispose 仅释放资源）
  rsp
    .aof()
    .expect("aof")
    .log()
    .commit_flush_only_async()
    .await;
  rserver.dispose();
  drop(rsp);
  drop(replica);
  let kvs2 = kv_batch("sr-2", KV_COUNT);
  client_set_all(&p_endpoint, &kvs2).await?;
  psp.aof().expect("aof").log().commit_async().await;
  let tail2 = psp.wal().expect("wal").tail_address();
  assert!(tail2 > tail1, "主端停机窗口内应有新写入");

  // ===== 重启副本（同目录 --recover）并恢复复制关系
  let replica2 = ClusterProvider::new();
  let (rserver2, rsp2, rport2) = boot_node(
    &rdir.path().join("node.db"),
    &replica2,
    cluster_decorate(Arc::clone(&replica2)),
    true,
  )
  .await?;
  {
    let cm = replica2.cluster_manager().expect("cm ready");
    let mut config = cm.current_config.write();
    init_local(
      &mut config,
      REPLICA_ID,
      rport2,
      NodeRole::Replica,
      Some(PRIMARY_ID),
    );
    config.workers.push(remote_worker(PRIMARY_ID, pport));
  }
  // 副本端点重宣告（对标 gossip 的端点收敛：重启副本端口漂移经 CLUSTER
  // NODES 同步回主端，无 gossip 测试拓扑下按收敛终态直改属主条目）
  {
    let cm = primary.cluster_manager().expect("cm ready");
    let mut config = cm.current_config.write();
    for w in config.workers.iter_mut() {
      if w.nodeid == Some(REPLICA_ID) {
        w.port = rport2 as i32;
      }
    }
  }
  replica2.set_replication_reestablishment_timeout(1);
  // 断链重连驱动（对标 cluster_session/basic.rs gossip 会话尾的
  // ensure_replication 挂点：无 gossip 测试拓扑下按生产调用契约周期驱动）
  let driver_provider = Arc::clone(&replica2);
  spawn(async move {
    loop {
      driver_provider.ensure_replication(Some(PRIMARY_ID));
      sleep(Duration::from_millis(ENSURE_POLL_MS)).await;
    }
  })
  .detach();

  // ===== 自动重连 + AOF 增量追平 + 逐键逐值一致
  let rm2 = replica2.replication_manager().expect("rm ready");
  assert!(
    wait_for(|| rm2.has_active_replication_stream(), SYNC_TIMEOUT).await,
    "重启后复制流应自动重建"
  );
  assert!(wait_replica_sync(&rm2, tail2).await, "增量位点应追平");
  assert!(
    rsp2.wal().expect("wal").tail_address() >= tail2,
    "副本日志尾不应落后业务写入尾"
  );
  let store2 = replica2.try_store().expect("store 在位");
  assert_store_has(&store2, &kvs1).await;
  assert_store_has(&store2, &kvs2).await;

  pserver.dispose();
  rserver2.dispose();
  Ok(())
}

/// test/cluster/Garnet.test.cluster.replication/ReplicationTests/ClusterReplicationBaseTests.cs:ClusterDivergentReplicasTest
///
/// 1 主 2 从同步初始数据 → Node 1 脱离独立升主（REPLICAOF NO ONE）→ 旧主
/// 写入新数据仅 Node 2 可达后下线 → 槽位改派 Node 1 并推进纪元 → Node 1
/// 写入分叉新历史并 SAVE → Node 2 挂靠 Node 1：分叉位点触发快照全量重同步
/// （引擎置换），最终与 Node 1 逐键逐值对齐、分叉丢失数据不复活
#[compio::test]
async fn cluster_divergent_replicas_test() -> Void {
  // ===== 三节点起服（Node 0 主，Node 1 / Node 2 待挂从）
  let dirs = (0..3)
    .map(|_| tempfile::tempdir())
    .collect::<io::Result<Vec<_>>>()?;
  let primary = ClusterProvider::new();
  let new_primary = ClusterProvider::new();
  let replica2 = ClusterProvider::new();
  let (pserver, psp, pport) = boot_node(
    &dirs[0].path().join("node.db"),
    &primary,
    cluster_decorate(Arc::clone(&primary)),
    false,
  )
  .await?;
  let (n1server, _n1sp, n1port) = boot_node(
    &dirs[1].path().join("node.db"),
    &new_primary,
    cluster_decorate(Arc::clone(&new_primary)),
    false,
  )
  .await?;
  let (n2server, _n2sp, n2port) = boot_node(
    &dirs[2].path().join("node.db"),
    &replica2,
    cluster_decorate(Arc::clone(&replica2)),
    false,
  )
  .await?;

  // ===== 全互指拓扑（Node 0 持全槽）
  {
    let cm = primary.cluster_manager().expect("cm ready");
    let mut config = cm.current_config.write();
    init_local(&mut config, PRIMARY_ID, pport, NodeRole::Primary, None);
    assign_all_slots(&mut config);
    config.workers.push(remote_worker(REPLICA_ID, n1port));
    config.workers.push(remote_worker(REPLICA2_ID, n2port));
  }
  {
    let cm = new_primary.cluster_manager().expect("cm ready");
    let mut config = cm.current_config.write();
    init_local(&mut config, REPLICA_ID, n1port, NodeRole::Primary, None);
    config.workers.push(remote_worker(PRIMARY_ID, pport));
    config.workers.push(remote_worker(REPLICA2_ID, n2port));
  }
  {
    let cm = replica2.cluster_manager().expect("cm ready");
    let mut config = cm.current_config.write();
    init_local(&mut config, REPLICA2_ID, n2port, NodeRole::Primary, None);
    config.workers.push(remote_worker(PRIMARY_ID, pport));
    config.workers.push(remote_worker(REPLICA_ID, n1port));
  }

  let p_endpoint = format!("127.0.0.1:{pport}");
  let n1_endpoint = format!("127.0.0.1:{n1port}");
  let n2_endpoint = format!("127.0.0.1:{n2port}");
  let rm1 = new_primary.replication_manager().expect("rm ready");
  let rm2 = replica2.replication_manager().expect("rm ready");

  // ===== 双从挂靠旧主 + 初始数据全量同步
  for endpoint in [&n1_endpoint, &n2_endpoint] {
    assert_eq!(
      client_roundtrip(endpoint, &["REPLICAOF", "127.0.0.1", &pport.to_string()]).await?,
      "OK",
      "REPLICAOF 应成功"
    );
  }
  assert!(
    wait_for(
      || rm1.has_active_replication_stream() && rm2.has_active_replication_stream(),
      SYNC_TIMEOUT
    )
    .await,
    "双从复制流应建立"
  );
  let kvs1 = kv_batch("dv-a", KV_COUNT);
  client_set_all(&p_endpoint, &kvs1).await?;
  let tail1 = psp.wal().expect("wal").tail_address();
  assert!(wait_replica_sync(&rm1, tail1).await, "Node 1 位点应追平");
  assert!(wait_replica_sync(&rm2, tail1).await, "Node 2 位点应追平");

  // ===== Node 1 脱离集群独立升主（REPLICAOF NO ONE）
  assert_eq!(
    client_roundtrip(&n1_endpoint, &["REPLICAOF", "NO", "ONE"]).await?,
    "OK",
    "REPLICAOF NO ONE 应成功"
  );
  assert!(
    new_primary
      .cluster_manager()
      .is_some_and(|cm| cm.current_config.read().is_primary()),
    "脱离后 Node 1 应为独立主节点"
  );

  // ===== 旧主写入分叉前数据（仅仍在场的 Node 2 可达）
  let kvs2 = kv_batch("dv-b", KV_COUNT);
  client_set_all(&p_endpoint, &kvs2).await?;
  let tail2 = psp.wal().expect("wal").tail_address();
  assert!(wait_replica_sync(&rm2, tail2).await, "Node 2 位点应追平");
  assert_store_lacks(&new_primary.try_store().expect("store 在位"), &kvs2).await;

  // ===== 下线旧主（Dispose(false)）
  pserver.dispose();
  drop(primary);

  // ===== 槽位改派 Node 1 + 推进纪元（对标 AddDelSlotsRange 先摘后授 +
  // BumpEpoch 的收敛终态；Node 2 配置视图对标 gossip 收敛后的属主改指）
  {
    let cm = new_primary.cluster_manager().expect("cm ready");
    let mut config = cm.current_config.write();
    assign_all_slots(&mut config);
    config.bump_local_node_config_epoch();
  }
  {
    let cm = replica2.cluster_manager().expect("cm ready");
    let mut config = cm.current_config.write();
    // 槽位属主改指 Node 1（对标 gossip 收敛后 Node 2 的配置视图）
    let owner_idx = config
      .workers
      .iter()
      .position(|w| w.nodeid == Some(REPLICA_ID))
      .expect("Node 1 条目在册") as u16;
    let slots: Vec<usize> = (0..CLUSTER_SLOT_COUNT).collect();
    config.assign_slots(&slots, owner_idx, SlotState::Stable);
  }

  // ===== Node 1 写入分叉新历史 + SAVE（快照下发源）
  let kvs3 = kv_batch("dv-c", DIVERGE_KV_COUNT);
  client_set_all(&n1_endpoint, &kvs3).await?;
  assert_eq!(
    client_roundtrip(&n1_endpoint, &["SAVE"]).await?,
    "OK",
    "Node 1 SAVE 检查点须成功"
  );
  let n1_tail = rm1.get_replication_offset(0) as u64;

  // ===== Node 2 挂靠 Node 1：分叉位点须触发快照全量重同步（引擎置换）
  let old_store = Arc::clone(&replica2.try_store().expect("store 在位"));
  assert_eq!(
    client_roundtrip(
      &n2_endpoint,
      &["REPLICAOF", "127.0.0.1", &n1port.to_string()]
    )
    .await?,
    "OK",
    "挂靠新主应成功"
  );
  assert!(
    wait_for(
      || {
        replica2
          .try_store()
          .is_some_and(|s| !Arc::ptr_eq(&s, &old_store))
      },
      SYNC_TIMEOUT
    )
    .await,
    "分叉位点须触发快照全量重同步并置换在线引擎"
  );
  assert!(
    wait_for(|| rm2.has_active_replication_stream(), SYNC_TIMEOUT).await,
    "全量重同步后复制流应建立"
  );
  assert!(wait_replica_sync(&rm2, n1_tail).await, "重同步位点应追平");

  // ===== 最终对齐：Node 2 == Node 1；分叉丢失数据不复活；日志尾一致
  let store1 = new_primary.try_store().expect("store 在位");
  let store2 = replica2.try_store().expect("store 在位");
  assert_store_has(&store2, &kvs1).await;
  assert_store_has(&store2, &kvs3).await;
  assert_store_lacks(&store2, &kvs2).await;
  assert_store_has(&store1, &kvs1).await;
  assert_store_lacks(&store1, &kvs2).await;
  // 日志尾判据取业务写入尾（快照覆盖点之后的记录帧逐字节续推落盘；
  // applied 位点对周期提交标记在高负载下有固有追平滞后，等值判定不稳）
  let n2_wal = replica2.try_wal().expect("wal");
  assert!(
    n2_wal.tail_address() >= n1_tail,
    "Node 2 日志尾不应落后 Node 1 业务写入尾"
  );

  n1server.dispose();
  n2server.dispose();
  Ok(())
}
