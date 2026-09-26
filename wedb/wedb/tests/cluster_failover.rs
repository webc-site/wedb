//! 集群故障转移集成测试，对标 Garnet 复制与故障转移测试
use std::{
  sync::Arc,
  time::{Duration, Instant},
};

use aok::Void;
use compio::runtime::Runtime;
use wbase::hash_slot::slot_of;
use wconf::RuntimeServerOptions;
use wdev::SegmentedDevice;
use wedb::server::{
  cluster::IClusterProvider,
  cluster_manager::ClusterManager,
  cluster_provider::ClusterProvider,
  cluster_session::ClusterSession,
  failover::{failover_manager::FailoverManager, failover_option::FailoverOption},
  hash_slot::SlotState,
  replication::recovery_status::RecoveryStatus,
  worker::{LOCAL_WORKER_ID, LocalWorkerSpec, NodeRole, Worker},
};
use wkv::WedbStore;
use wnode::{
  GarnetAppendOnlyFile, GarnetLog, MessageConsumerFace, RespSessionConsumer,
  resp::{garnet_api::StoreGarnetApi, resp_server_session::RespServerSessionOptions},
};
use wnode_test::test_sublogs;
use wresp::cmd_strings::{
  RESP_ERR_GENERIC_VALUE_IS_NOT_INTEGER,
  cluster::{
    ERR_GENERIC_CANNOT_FAILOVER_FROM_NON_MASTER, ERR_GENERIC_REPLICATION_AOF_TURNEDOFF,
    ERR_GENERIC_UNKNOWN_ENDPOINT,
  },
};
use wtest_base::{
  FailoverNode, SilentNode, StopWritesNode, resp_frame_str, test_store_config, wait_for,
};
use wtxn::{TxnLockTable, WatchVersionMap};

/// 默认会话 (0, 0) 库槽位（库级定槽，键内容不参与定槽）
const SLOT0: u16 = slot_of(0, 0);

async fn wait_until(m: &FailoverManager, expect: &str) {
  m.wait_failover_done().await;
  assert!(
    wait_for(
      || m.get_last_failover_status() == expect,
      Duration::from_secs(5)
    )
    .await,
    "预期 failover 终态为 {expect}，实际为 {}",
    m.get_last_failover_status()
  );
}

/// test/cluster/Garnet.test.cluster.replication/ReplicationTests/ClusterReplicationBaseTests.cs:ClusterReplicationSimpleFailover
#[test]
fn cluster_replication_simple_failover() -> Void {
  Runtime::new()?.block_on(async {
    // replica_provider 预置合法主从集群：本地 E701 挂主节点 DE11 为副本、
    // 槽 0..1 归旧主（Takeover 选项按 C# 跳过停写与投票，不拨旧主端点，
    // 7001 仅作拓扑地址簿承载）
    let cp = replica_provider(7001);
    let m = Arc::new(FailoverManager::new(Arc::clone(&cp)));
    assert!(m.try_start_replica_failover(FailoverOption::Takeover, Duration::ZERO));
    wait_until(&m, "failover-completed").await;

    // 真闭环核验接管落地（对标 C# TryTakeOverForPrimary 的配置改写）：
    // 副本升主、接管槽位归本地、纪元推进——门控修复前空壳 provider
    // 也能虚假报完成，此处必须见槽位与角色实际翻转
    let cm = cp.cluster_manager().expect("集群拓扑在场");
    {
      let config = cm.current_config.read();
      assert!(config.is_primary());
      assert_eq!(config.local_node_primary_id(), None);
      assert_eq!(config.get_worker_id_from_slot(0), LOCAL_WORKER_ID);
      assert!(config.local_node_config_epoch() > 3);
    }

    // Session completes and resets status to no-failover
    assert_eq!(m.get_failover_status(), "no-failover");

    // Primary failover：本地名下无登记副本（旧主 DE11 非本地副本），
    // C# first_replica 缺失分支即刻放弃，状态归位 no-failover
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
    // 接管完成须由 replica_provider 预置的合法主从拓扑真驱动（门控修复后
    // 空壳 ClusterProvider::default() 只可能 failover-aborted）
    let m_a = Arc::new(FailoverManager::new(replica_provider(7001)));
    assert!(m_a.try_start_replica_failover(FailoverOption::Takeover, Duration::ZERO));
    assert!(
      !m_a.try_start_replica_failover(FailoverOption::Takeover, Duration::ZERO),
      "concurrent failover rejected while session runs"
    );
    wait_until(&m_a, "failover-completed").await;

    // Abort detaches session and recovers lock：abort 发生在会话首个
    // is_aborted 检查（任何状态推进前），本地仍是副本拓扑未动
    let m_b = Arc::new(FailoverManager::new(replica_provider(7001)));
    assert!(m_b.try_start_replica_failover(FailoverOption::Takeover, Duration::ZERO));
    m_b.try_abort_replica_failover();
    assert_eq!(m_b.get_failover_status(), "no-failover");
    wait_until(&m_b, "failover-aborted").await;

    // 锁已释放，同一 manager 再以副本拓扑发起可顺利完成真接管
    assert!(m_b.try_start_replica_failover(FailoverOption::Takeover, Duration::ZERO));
    wait_until(&m_b, "failover-completed").await;

    aok::OK
  })
}

/// 负向回归（takeover 门控反转票）：修复前 `if let Some(cm) = ... &&
/// !cm.try_take_over_for_primary()` 组合式在 cluster_manager 缺席（单机/
/// 集群未就绪）时模式不匹配、整条件为 false，穿透 else 成功分支虚假报
/// failover-completed。对标 C# ClusterManager.cs:377-385 双闸（IsReplica
/// 且 LocalNodePrimaryId 在场），cm 缺席与主从关系未配置均须拒绝接管，
/// 终态必 failover-aborted（cm 缺席支修复前必红）
#[test]
fn cluster_failover_takeover_without_primary_replica_setup_aborts() -> Void {
  Runtime::new()?.block_on(async {
    // 1) cluster_manager 缺席（ClusterProvider::default() 单机形态）
    let m = Arc::new(FailoverManager::new(Arc::new(ClusterProvider::default())));
    assert!(m.try_start_replica_failover(FailoverOption::Takeover, Duration::ZERO));
    wait_until(&m, "failover-aborted").await;

    // 2) cluster_manager 在场但未配置主从关系：try_take_over_for_primary
    // 按 C# 双闸拒绝，接管同样不得放行
    let m = Arc::new(FailoverManager::new(ClusterProvider::new()));
    assert!(m.try_start_replica_failover(FailoverOption::Takeover, Duration::ZERO));
    wait_until(&m, "failover-aborted").await;
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
    conn.initialize_async(Duration::from_secs(1)).await;
    assert!(conn.client.is_connected(), "gossip 共享连接应已建立");

    let m = Arc::new(FailoverManager::new(Arc::clone(&cp)));
    assert!(m.try_start_replica_failover(FailoverOption::Default, Duration::from_millis(80)));
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
    assert!(m.try_start_replica_failover(FailoverOption::Default, Duration::from_millis(80)));
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

/// 公告臂 ABORT 双臂收口（对标 C# ReplicaFailoverSession.cs:205/:245
/// WaitAsync(failoverTimeout, cts.Token)）：修复前 rust 公告臂系裸 timeout
/// 单臂，公告期下发 ABORT 不经 race_abort 承接——旧会话臂体必等满每臂
/// 独立 failover_timeout（gossip→attach 串行，静默对端单节点最坏
/// 2×failover_timeout），last_failover_status 卡在途态压制
/// ensure_replication 自动重挂、per-node 连接迟释。靶端握手 2 帧后对
/// GOSSIP/REPLICAOF 全程沉默；公告期中途 try_abort_replica_failover：
/// 臂体秒级收口（挂满档必红）、靶端见对端断开（连接释放）、终态落账后
/// 抑制解除；ABORT 后新发 failover 可行（锁逃生门防回改）
#[test]
fn cluster_failover_broadcast_arm_abort_interrupts() -> Void {
  Runtime::new()?.block_on(async {
    let fake = SilentNode::bind(2).await; // 握手即应答；GOSSIP/REPLICAOF 沉默挂起
    // 本地 E701 从属旧主 DE11@7001（纯地址簿，Takeover 不拨号），另挂一名
    // 副本 3333 于沉默靶端：接管完成后 issue_attach 只对它广播改挂
    let cp = replica_provider(7001);
    {
      let cm = cp.cluster_manager().expect("cluster manager 在场");
      let mut config = cm.current_config.write();
      config.workers.push(Worker {
        nodeid: Some(0x0000_0000_0000_0000_0000_0000_0000_3333),
        address: "127.0.0.1".into(),
        port: fake.port() as i32,
        config_epoch: 3,
        role: NodeRole::Replica,
        replica_of_node_id: Some(0x0000_0000_0000_0000_0000_0000_0000_DE11),
        replication_offset: 0,
        hostname: None,
      });
    }

    let m = Arc::new(FailoverManager::new(Arc::clone(&cp)));
    // 每臂 30 秒档远大于 5 秒收口断言窗：单臂形态（缺取消半边）必红
    assert!(m.try_start_replica_failover(FailoverOption::Takeover, Duration::from_secs(30)));
    // 等旧会话进公告期：GOSSIP 帧已入靶端被静默吞下（挂起在途确证）
    assert!(
      wait_for(|| fake.silent_frame_count() >= 1, Duration::from_secs(10)).await,
      "会话应进入公告期且 GOSSIP 挂起靶端"
    );
    // 公告在途中 last_failover_status 停在途态，ensure_replication 抑制门生效
    assert!(m.is_failover_in_progress(), "公告期应判 failover 进行中");

    let start = Instant::now();
    m.try_abort_replica_failover();
    // 两臂取消半边经 race_abort 承接：臂体秒级收口而非等满剩余超时档
    wait_until(&m, "failover-completed").await;
    let elapsed = start.elapsed();
    assert!(
      elapsed < Duration::from_secs(5),
      "ABORT 应即时打断公告臂而非挂满 2×30s 串行档: {elapsed:?}"
    );
    // 公告臂 per-node 连接于臂尾自释：靶端观测到对端断开
    assert!(
      wait_for(|| fake.peer_closed(), Duration::from_secs(5)).await,
      "abort 收口后公告臂连接应释放"
    );
    // 迟到 settle 落终态后 failover 抑制解除
    assert!(
      !m.is_failover_in_progress(),
      "终态落账后应解除 failover 抑制"
    );

    // ABORT 后新发 CLUSTER FAILOVER 可行（防回改）：接管已完成、本地已升主，
    // 再发起被接管门拒归 failover-aborted，但发起本身须被放行
    assert!(
      m.try_start_replica_failover(FailoverOption::Takeover, Duration::from_secs(5)),
      "ABORT 后任务锁应即时可夺"
    );
    wait_until(&m, "failover-aborted").await;
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
    let replica = FailoverNode::bind(offset_reply, Duration::from_millis(250)).await;

    // 限时 60ms < 应答延迟 250ms：探测超时空应答，候选落选；
    // failover_timeout 给足，收敛须由探测限时驱动而非哨兵
    let cp = primary_provider(replica.port(), replica.port());
    cp.set_cluster_node_timeout_ms(60);
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

    // 限时 1.5s > 应答延迟 250ms：同一延迟副本当选接管
    let cp = primary_provider(replica.port(), replica.port());
    cp.set_cluster_node_timeout_ms(1500);
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

/// 装配持槽主节点拓扑（primary_provider 加槽版）：本地 DE11 PRIMARY 持
/// SLOT0（库 (0,0) 定槽），候选副本 worker 挂账供 get_replica_ids 选取
fn slot_primary_provider(replica_port: u16) -> Arc<ClusterProvider> {
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
    config.workers.push(Worker {
      nodeid: Some(0x5107),
      address: "127.0.0.1".into(),
      port: replica_port as i32,
      config_epoch: 3,
      role: NodeRole::Primary,
      replica_of_node_id: Some(0x0000_0000_0000_0000_0000_0000_0000_DE11),
      replication_offset: 0,
      hostname: None,
    });
    config.assign_slots(&[SLOT0 as usize], LOCAL_WORKER_ID as u16, SlotState::Stable);
  }
  cp
}

/// 回滚终态断言：主节点角色与让渡前一致——PRIMARY、无复制源、SLOT0 赎回
/// 归本地且 Stable、纪元较让渡前推进（赎回自增）
fn assert_rolled_back(cp: &Arc<ClusterProvider>, epoch_before: i64) {
  let cm = cp.cluster_manager().expect("cluster manager 在场");
  let config = cm.current_config.read();
  assert!(config.is_primary(), "回滚后角色必须恢复 PRIMARY");
  assert_eq!(config.local_node_primary_id(), None, "复制源必须清空");
  assert_eq!(config.get_worker_id_from_slot(SLOT0), LOCAL_WORKER_ID);
  assert_eq!(config.get_state(SLOT0), SlotState::Stable);
  assert!(
    config.local_node_config_epoch() > epoch_before,
    "赎回必须自增纪元: {} !> {epoch_before}",
    config.local_node_config_epoch()
  );
}

/// 主节点 failover 位点同步超时回滚（PR #1670 立论：让渡槽位但从节点未
/// 接管 = 槽位无主非法状态）：副本靶端位点应答延迟超 failover_timeout，
/// 会话放弃后必须赎回槽位、恢复 PRIMARY 角色并推进纪元，last_failover_status
/// 按 replica 路径同形收敛 failover-aborted
#[test]
fn cluster_failover_primary_sync_timeout_rolls_back_slots() -> Void {
  Runtime::new().unwrap().block_on(async {
    let offset_reply = local_offset_reply();
    let slow = FailoverNode::bind(offset_reply, Duration::from_secs(30)).await;
    let cp = slot_primary_provider(slow.port());
    let epoch_before = {
      let cm = cp.cluster_manager().unwrap();
      cm.current_config.read().local_node_config_epoch()
    };

    let m = Arc::new(FailoverManager::new(Arc::clone(&cp)));
    assert!(m.try_start_primary_failover(
      "",
      -1,
      FailoverOption::Takeover,
      Duration::from_millis(300)
    ));
    m.wait_failover_done().await;

    assert_eq!(m.get_last_failover_status(), "failover-aborted");
    assert!(!slow.takeover_received(), "同步超时不应有副本被接管");
    assert_rolled_back(&cp, epoch_before);
    aok::OK
  })
}

/// 主节点 failover 接管应答失败回滚：副本位点追平当选但 TAKEOVER 应答
/// -ERR，让渡已落地必须赎回——否则主节点永久沦为无槽 Replica 而从节点
/// 未接管，槽位彻底失主
#[test]
fn cluster_failover_primary_takeover_rejection_rolls_back_slots() -> Void {
  Runtime::new().unwrap().block_on(async {
    let offset_reply = local_offset_reply();
    let rejector = FailoverNode::bind_rejecting_takeover(offset_reply, Duration::ZERO).await;
    let cp = slot_primary_provider(rejector.port());
    let epoch_before = {
      let cm = cp.cluster_manager().unwrap();
      cm.current_config.read().local_node_config_epoch()
    };

    let m = Arc::new(FailoverManager::new(Arc::clone(&cp)));
    assert!(m.try_start_primary_failover(
      "",
      -1,
      FailoverOption::Takeover,
      Duration::from_secs(10)
    ));
    m.wait_failover_done().await;

    assert!(rejector.takeover_received(), "位点追平副本应当选下发接管");
    assert_eq!(m.get_last_failover_status(), "failover-aborted");
    assert_rolled_back(&cp, epoch_before);
    aok::OK
  })
}

/// 挂接集群切面 + 存储执行域的会话消费者（对标 failover_timeout_bounds 同款）
fn session_consumer(cp: &Arc<ClusterProvider>) -> RespSessionConsumer {
  let cluster_session: Arc<ClusterSession> = cp.create_cluster_session();
  let dir = tempfile::tempdir().unwrap().keep();
  let device = Arc::new(SegmentedDevice::single_file(dir.join("fo.db")).unwrap());
  let store = Arc::new(WedbStore::open(test_store_config(), device).unwrap());
  cp.set_store(Arc::clone(&store));
  let mut consumer = RespSessionConsumer::with_cluster(
    1,
    RespServerSessionOptions {
      max_databases: 2,
      ..RespServerSessionOptions::default()
    },
    cluster_session,
    cp.provider_handle(),
    Arc::new(StoreGarnetApi::new(store.new_session().unwrap())),
  );
  consumer.attach_transaction_components(Arc::new(WatchVersionMap::new(64)), TxnLockTable::new());
  consumer
}

/// 泵等价消费单命令往返
fn roundtrip(consumer: &mut RespSessionConsumer, frame: &[u8]) -> Vec<u8> {
  let mut scratch = consumer.take_recv_scratch();
  scratch.extend_from_slice(frame);
  consumer.return_recv_scratch(scratch);
  let mut resp = Vec::new();
  let consumed = consumer.try_consume_messages_into(&mut resp);
  assert_eq!(consumed, Some(0), "帧应被完整消费: {frame:?}");
  resp
}

/// 外部 FAILOVER ABORT 赎回闭环（network_failover ABORT 分支）：会话挂起
/// 等位点同步时下发 ABORT——命令面仅触发 try_abort_replica_failover()，
/// 让渡赎回由主端会话失败路径闭环（角色与槽位一并恢复，修复前命令面裸赎
/// 回无让渡判据、非主放行即成窃主通道）；会话收口 failover-aborted，节点
/// 回归可读写服务态
#[test]
fn cluster_failover_primary_abort_reclaims_slots_and_serves_writes() -> Void {
  Runtime::new().unwrap().block_on(async {
    let offset_reply = local_offset_reply();
    let slow = FailoverNode::bind(offset_reply, Duration::from_secs(30)).await;
    let cp = slot_primary_provider(slow.port());
    let epoch_before = {
      let cm = cp.cluster_manager().unwrap();
      cm.current_config.read().local_node_config_epoch()
    };
    let fm = cp.failover_manager().expect("failover manager 在场");
    let mut consumer = session_consumer(&cp);

    let frame = resp_frame_str(&["FAILOVER", "TIMEOUT", "30000"]);
    assert_eq!(roundtrip(&mut consumer, &frame), b"+OK\r\n");
    // 等会话让渡落地（停写降级为 Replica、SLOT0 划归副本 worker）
    assert!(
      wait_for(
        || !cp
          .cluster_manager()
          .unwrap()
          .current_config
          .read()
          .is_primary(),
        Duration::from_secs(5)
      )
      .await,
      "会话应进入停写让渡形态"
    );
    {
      let cm = cp.cluster_manager().unwrap();
      let config = cm.current_config.read();
      let replica_worker = config.get_worker_id_from_node_id(0x5107);
      assert_ne!(
        replica_worker as usize, LOCAL_WORKER_ID,
        "副本 worker 须异于本地"
      );
      assert_eq!(
        config.get_worker_id_from_slot(SLOT0),
        replica_worker as usize,
        "让渡后 SLOT0 应划归副本 worker"
      );
    }

    let abort_frame = resp_frame_str(&["FAILOVER", "ABORT"]);
    assert_eq!(roundtrip(&mut consumer, &abort_frame), b"+OK\r\n");
    // ABORT 唤醒会话中止 → 失败路径自赎（槽位收回 + PRIMARY 恢复），
    // 轮询等待赎回落地后终判
    assert!(
      wait_for(
        || {
          let cm = cp.cluster_manager().unwrap();
          let config = cm.current_config.read();
          config.is_primary() && config.get_worker_id_from_slot(SLOT0) == LOCAL_WORKER_ID
        },
        Duration::from_secs(5)
      )
      .await,
      "ABORT 后会话自赎须恢复 PRIMARY 并收回 SLOT0"
    );

    fm.wait_failover_done().await;
    assert_eq!(fm.get_last_failover_status(), "failover-aborted");
    // 会话失败路径幂等：二次赎回判据拒绝，配置不再漂移
    assert_rolled_back(&cp, epoch_before);

    // 回滚后节点回归服务态：SLOT0 库可写可读
    let set = resp_frame_str(&["SET", "bar", "val"]);
    assert_eq!(roundtrip(&mut consumer, &set), b"+OK\r\n");
    let get = resp_frame_str(&["GET", "bar"]);
    assert_eq!(roundtrip(&mut consumer, &get), b"$3\r\nval\r\n");
    aok::OK
  })
}

/// P0 门禁（票 zcode-r26-autofailover 发现一）：常态副本单命令 FAILOVER
/// ABORT 零变化——C# FailoverCommand.cs:68-73 角色门无条件前置，ABORT 臂
///（:103-107）对副本不可达；修复前角色门 `!is_primary && !abort` 放行副本
/// ABORT、裸 try_restore_stop_writes 以「复制源在场」判据把主端全部槽位
/// 划归本端自升主。修复后：命令回 CAN FAILOVER 错误帧，角色/复制源/槽属主/
/// 纪元零变化；判据面独立锁让渡标志缺失时 try_restore_stop_writes 拒绝
#[test]
fn cluster_failover_abort_on_ordinary_replica_changes_nothing() -> Void {
  Runtime::new().unwrap().block_on(async {
    // 常态副本拓扑：本地 E702 从属正常主 DE11@7001，主持 SLOT0
    let cp = replica_provider(7001);
    let cm = cp.cluster_manager().expect("cluster manager 在场");
    let mut consumer = session_consumer(&cp);
    let before = {
      let config = cm.current_config.read();
      (
        config.local_node_config_epoch(),
        config.get_worker_id_from_slot(SLOT0),
      )
    };

    let abort_frame = resp_frame_str(&["FAILOVER", "ABORT"]);
    assert_eq!(
      roundtrip(&mut consumer, &abort_frame),
      format!("-{}\r\n", ERR_GENERIC_CANNOT_FAILOVER_FROM_NON_MASTER).into_bytes(),
      "常态副本 ABORT 须被无条件角色门拒绝"
    );
    {
      let config = cm.current_config.read();
      assert!(!config.is_primary(), "角色必须仍是副本");
      assert_eq!(
        config.local_node_primary_id(),
        Some(0x0000_0000_0000_0000_0000_0000_0000_DE11),
        "复制源必须原样在场"
      );
      assert_eq!(
        config.get_worker_id_from_slot(SLOT0),
        before.1,
        "槽属主必须仍是主端 worker"
      );
      assert_eq!(config.local_node_config_epoch(), before.0, "纪元必须零变化");
    }

    // 判据面独立锁：未让渡（让渡标志缺位）时赎回拒绝——常态副本的
    // 「复制源在场」形态不再构成让渡判据
    assert!(
      !cm.try_restore_stop_writes(),
      "未让渡节点 try_restore_stop_writes 必须拒绝"
    );
    {
      let config = cm.current_config.read();
      assert_eq!(config.get_worker_id_from_slot(SLOT0), before.1);
      assert_eq!(config.local_node_config_epoch(), before.0);
    }
    aok::OK
  })
}

/// 端口 int32 值域门禁（票 zcode-r26-autofailover 发现四，与 CLUSTER MEET /
/// REPLICAOF 同根因单点）：FAILOVER TO 端口越界 int32（4294967297 截断后
/// 恰命中真实端口 7001）解析即拒 value-is-not-integer，不再静默截断驱动
/// 可用性敏感操作；合法端口路径保持语法校验次序（端点未知回 UNKNOWN_ENDPOINT）
#[test]
fn failover_to_port_out_of_i32_range_rejected() -> Void {
  Runtime::new().unwrap().block_on(async {
    let cp = primary_provider(0, 0);
    let mut consumer = session_consumer(&cp);
    let frame = resp_frame_str(&["FAILOVER", "TO", "127.0.0.1", "4294967297"]);
    assert_eq!(
      roundtrip(&mut consumer, &frame),
      format!("-{}\r\n", RESP_ERR_GENERIC_VALUE_IS_NOT_INTEGER).into_bytes(),
    );
    // 正常路径回归：合法端口但端点未知 → UNKNOWN_ENDPOINT（主节点名下
    // 无该副本）
    let frame = resp_frame_str(&["FAILOVER", "TO", "127.0.0.1", "7099"]);
    assert_eq!(
      roundtrip(&mut consumer, &frame),
      format!("-{}\r\n", ERR_GENERIC_UNKNOWN_ENDPOINT).into_bytes(),
    );
    aok::OK
  })
}

/// AOF 装配注入（从端入口 AOF 前置门放行面，同 failover_timeout_bounds 形）
fn attach_aof(cp: &Arc<ClusterProvider>) {
  let options = RuntimeServerOptions::default();
  let log = Arc::new(
    GarnetLog::new(
      &options,
      {
        let (_dirs, backends) = test_sublogs("failover_entry", 1);
        backends
      },
      None,
    )
    .expect("构造 GarnetLog"),
  );
  cp.set_aof(Some(Arc::new(GarnetAppendOnlyFile::new(
    Arc::clone(&log),
    &options,
    None,
  ))));
}

/// deviations §117a 从端选项词表白名单锁：C# 顶层词法器七枚举全转、从端
/// 入口仅拦 DEFAULT/INVALID（RespClusterFailoverCommands.cs:33-34）且
/// TryStartReplicaFailover 无选项门（FailoverManager.cs:80-88），TO/TIMEOUT
/// 在副本上静默按 DEFAULT 发起真实故障转移；rust 三词白名单（ABORT/FORCE/
/// TAKEOVER）其余一律 option-not-supported 拒——TO 与 TIMEOUT+秒数第二参
/// 逐字节钉死，对照臂白名单三词不得落白名单拒臂（无 AOF 装配落 §13 同源
/// AOF 门），杜绝按 C# 词表放宽回改
#[test]
fn cluster_failover_replica_entry_option_whitelist_rejects_to_timeout() -> Void {
  Runtime::new()?.block_on(async {
    let cp = replica_provider(7001);
    let mut consumer = session_consumer(&cp);
    let fm = cp.failover_manager().expect("failover manager 在场");

    for extra in [&[][..], &["5"][..]] {
      let mut args = vec!["CLUSTER", "FAILOVER", "TO"];
      args.extend_from_slice(extra);
      assert_eq!(
        roundtrip(&mut consumer, &resp_frame_str(&args)),
        b"-ERR Failover option (TO) not supported\r\n",
        "{args:?} 须被从端白名单拒绝"
      );
    }
    assert_eq!(
      roundtrip(
        &mut consumer,
        &resp_frame_str(&["CLUSTER", "FAILOVER", "TIMEOUT", "5"])
      ),
      b"-ERR Failover option (TIMEOUT) not supported\r\n",
      "TIMEOUT 词在从端无从消费，须白名单拒（C# 静默按 DEFAULT 发起形严禁回改）"
    );
    // 对照：ABORT/FORCE/TAKEOVER 过白名单（未装配 AOF，落 AOF 门而非白名单
    // 拒臂），三门措辞互斥即词表收窄仅在四拒词
    for word in ["ABORT", "FORCE", "TAKEOVER"] {
      assert_eq!(
        roundtrip(
          &mut consumer,
          &resp_frame_str(&["CLUSTER", "FAILOVER", word])
        ),
        format!("-{}\r\n", ERR_GENERIC_REPLICATION_AOF_TURNEDOFF).into_bytes(),
        "{word} 须穿透白名单落后续门"
      );
    }
    assert_eq!(
      fm.get_failover_status(),
      "no-failover",
      "拒收输入不得发起任何 failover 会话"
    );
    aok::OK
  })
}

/// deviations §117b TO 端口 -1 必验锁：C# 门条件 `replicaPort != -1 &&`
/// （FailoverCommand.cs:76）-1 即整体跳过三闸、TO 指定地址被丢弃且探测集
/// 错向 GetLocalNodePrimaryEndpoints（FailoverSession.cs:66-68 全集群主端点）；
/// rust 给了地址即无条件三闸（failover.rs TO 校验臂），-1 查无此端回
/// unknown endpoint——primary_failover_session.rs 的同形 -1 臂自命令面
/// 不可达（纯防御镜像），严禁复刻跳验放行门形
#[test]
fn failover_to_minus_one_port_unknown_endpoint() -> Void {
  Runtime::new()?.block_on(async {
    let cp = primary_provider(7003, 7004);
    let mut consumer = session_consumer(&cp);
    let fm = cp.failover_manager().expect("failover manager 在场");
    assert_eq!(
      roundtrip(
        &mut consumer,
        &resp_frame_str(&["FAILOVER", "TO", "127.0.0.1", "-1"])
      ),
      format!("-{}\r\n", ERR_GENERIC_UNKNOWN_ENDPOINT).into_bytes(),
      "-1 端口须过无条件三闸，拒绝 C# 跳验放行形"
    );
    assert_eq!(
      fm.get_failover_status(),
      "no-failover",
      "跳闸形态不得入 TryStartPrimaryFailover"
    );
    aok::OK
  })
}

/// deviations §117c 发起失败文案单源锁：从端在途会话占住任务锁时 FORCE 臂
/// 发起失败，应答逐字节钉 primary(addr:port) 冒号单层括号形并前缀 `primary(`
/// 锁单源——C# 以 ValueTuple 整体插值渲染 primary((addr, port)) 双层括号+
/// 逗号空格畸形形（RespClusterFailoverCommands.cs:71 + ClusterConfig.cs:268
/// GetLocalNodePrimaryAddress 返回 (string, int)），不做逐字对齐、严禁回改；
/// 收尾 abort 打断在途会话（race_abort 臂）释放任务锁
#[test]
fn cluster_failover_start_failure_primary_addrport_wording() -> Void {
  Runtime::new()?.block_on(async {
    // 仅握手应答的沉默主端：在途 DEFAULT 会话挂停写应答等待（预算 30s，
    // 收口全靠尾部 abort），任务锁确定性占用
    let fake = SilentNode::bind(2).await;
    let cp = replica_provider(fake.port() as i32);
    attach_aof(&cp);
    let fm = cp.failover_manager().expect("failover manager 在场");
    assert!(fm.try_start_replica_failover(FailoverOption::Default, Duration::from_secs(30)));

    let mut consumer = session_consumer(&cp);
    let out = roundtrip(
      &mut consumer,
      &resp_frame_str(&["CLUSTER", "FAILOVER", "FORCE"]),
    );
    assert!(
      out.starts_with(b"-ERR failed to start failover for primary("),
      "primary( 前缀文案单源锁: {out:?}"
    );
    assert_eq!(
      out,
      format!(
        "-ERR failed to start failover for primary(127.0.0.1:{})\r\n",
        fake.port()
      )
      .into_bytes(),
      "rust 渲染 addr:port 冒号单层形，非 C# 双层括号逗号空格形"
    );

    // 收尾：abort 打断在途会话（race_abort 臂），reset 与监督收尾并发，
    // 终态落账循 wait_until 轮询（同 cluster_failover_abort_interrupts 形）
    fm.try_abort_replica_failover();
    wait_until(&fm, "failover-aborted").await;
    aok::OK
  })
}

/// 分片槽界（0..HALF_SLOTS 归分片 A、HALF_SLOTS..16384 归分片 B）
const HALF_SLOTS: usize = 8192;
const PRIMARY_A: u128 = 0x0000_0000_0000_0000_0000_0000_0000_0A11;
const PRIMARY_B: u128 = 0x0000_0000_0000_0000_0000_0000_0000_0B22;
const REPLICA_A: u128 = 0x0000_0000_0000_0000_0000_0000_0000_0A01;
const REPLICA_B: u128 = 0x0000_0000_0000_0000_0000_0000_0000_0B02;

/// 双分片副本视图装配（C# ClusterParallelFailoverOnDistinctShards 四节点
/// 拓扑的本地视图对译）：PA 持槽 0..=8191、PB 持 8192..=16383，各挂一名
/// 副本；本地为 `follow` 主名下副本。远端 worker 仅地址簿挂账——TAKEOVER
/// 会话的 attach 广播只拨 `get_replica_ids(旧主)` 排除本地后的目标，本视图
/// 为空集，全程零远端拨号
fn shard_replica_view(local_id: u128, follow: u128) -> Arc<ClusterProvider> {
  let cp = ClusterProvider::new();
  let m = cp.cluster_manager().expect("cluster manager 在场");
  {
    let mut config = m.current_config.write();
    config.initialize_local_worker(LocalWorkerSpec {
      node_id: local_id,
      address: "127.0.0.1",
      port: 7002,
      config_epoch: 1,
      role: NodeRole::Replica,
      replica_of_node_id: Some(follow),
      hostname: None,
    });
    for (id, role, primary) in [
      (PRIMARY_A, NodeRole::Primary, None),
      (PRIMARY_B, NodeRole::Primary, None),
      (REPLICA_A, NodeRole::Replica, Some(PRIMARY_A)),
      (REPLICA_B, NodeRole::Replica, Some(PRIMARY_B)),
    ] {
      if id == local_id {
        continue;
      }
      config.workers.push(Worker {
        nodeid: Some(id),
        address: "127.0.0.1".into(),
        port: 7099,
        config_epoch: 2,
        role,
        replica_of_node_id: primary,
        replication_offset: 0,
        hostname: None,
      });
    }
    let pa = config.get_worker_id_from_node_id(PRIMARY_A);
    let pb = config.get_worker_id_from_node_id(PRIMARY_B);
    config.assign_slots(&(0..HALF_SLOTS).collect::<Vec<_>>(), pa, SlotState::Stable);
    config.assign_slots(
      &(HALF_SLOTS..HALF_SLOTS * 2).collect::<Vec<_>>(),
      pb,
      SlotState::Stable,
    );
  }
  cp
}

/// 单分片副本视图的接管后定格：本地升主并全持本分片槽；他分片槽属主
/// 原样不动；任何在册 Replica 角色 worker 不持槽（C# :519-528
/// 「无副本带槽」全集群扫描的本视图对译）。`local_owns_high` 指认本地
/// 副本追随的是高段分片（B）还是低段分片（A）
fn assert_shard_takeover(cp: &Arc<ClusterProvider>, other_primary: u128, local_owns_high: bool) {
  let cm = cp.cluster_manager().expect("cluster manager 在场");
  let config = cm.current_config.read();
  assert!(config.is_primary(), "本分片副本接管后应升主");
  assert_eq!(config.local_node_primary_id(), None);
  assert!(config.local_node_config_epoch() > 2, "接管须推进纪元");
  let other_idx = config.get_worker_id_from_node_id(other_primary);
  for slot in 0..HALF_SLOTS as u16 {
    let (mine, theirs) = if local_owns_high {
      (slot + HALF_SLOTS as u16, slot)
    } else {
      (slot, slot + HALF_SLOTS as u16)
    };
    assert_eq!(
      config.get_worker_id_from_slot(mine),
      LOCAL_WORKER_ID,
      "本分片槽 {mine} 应归本地"
    );
    assert_eq!(config.get_state(mine), SlotState::Stable);
    assert_eq!(
      config.get_worker_id_from_slot(theirs),
      other_idx as usize,
      "他分片槽 {theirs} 应保留原属主"
    );
  }
  for (idx, w) in config.workers.iter().enumerate() {
    if w.nodeid.is_some() && w.role == NodeRole::Replica {
      assert!(
        !config.slot_map.iter().any(|s| s.worker_id as usize == idx),
        "Replica 角色 worker {idx} 并行接管后不得持槽（C# :526 形态）"
      );
    }
  }
}

/// 多 shard 并行 failover（对标 test/cluster/Garnet.test.cluster/
/// ClusterNegativeTests.cs:477-529 ClusterParallelFailoverOnDistinctShards）：
/// 双分片副本并发下发 CLUSTER FAILOVER——两 +OK（:513-514）互不阻塞
/// （分片间不得被全局互斥串行化），各自 failover-completed 且状态归位
/// no-failover（:516-517 WaitForNoFailover），槽位归属按分片隔离。
/// 形态差异申报：C# 缺省 DEFAULT 选项走停写投票+旧主降级全网链路，本例
/// 取本文件 cluster_replication_simple_failover 既有 TAKEOVER 形态（跳过
/// 停写与投票、不拨旧主端点），锁「并发接管准入 + 分片归属隔离」面；
/// DEFAULT 旧主降级链路同形由本文件 cluster_failover_primary_* 系与
/// failover_timeout_bounds 系覆盖
#[test]
fn cluster_parallel_failover_on_distinct_shards() -> Void {
  Runtime::new()?.block_on(async {
    let cp_a = shard_replica_view(REPLICA_A, PRIMARY_A);
    let cp_b = shard_replica_view(REPLICA_B, PRIMARY_B);
    attach_aof(&cp_a);
    attach_aof(&cp_b);
    let mut consumer_a = session_consumer(&cp_a);
    let mut consumer_b = session_consumer(&cp_b);
    let fm_a = cp_a.failover_manager().expect("failover manager 在场");
    let fm_b = cp_b.failover_manager().expect("failover manager 在场");

    let frame = resp_frame_str(&["CLUSTER", "FAILOVER", "TAKEOVER"]);
    // C# :513-514：两会话并发在途，第二发不得因互斥落发起失败臂
    assert_eq!(
      roundtrip(&mut consumer_a, &frame),
      b"+OK\r\n",
      "分片 A 副本接管应被受理"
    );
    assert_eq!(
      roundtrip(&mut consumer_b, &frame),
      b"+OK\r\n",
      "分片 B 副本接管应并发被受理，不得串行拒绝"
    );

    wait_until(&fm_a, "failover-completed").await;
    wait_until(&fm_b, "failover-completed").await;
    assert_eq!(fm_a.get_failover_status(), "no-failover");
    assert_eq!(fm_b.get_failover_status(), "no-failover");

    assert_shard_takeover(&cp_a, PRIMARY_B, false);
    assert_shard_takeover(&cp_b, PRIMARY_A, true);
    aok::OK
  })
}

/// 恢复期 failover 三步收口（对标 ClusterNegativeTests.cs:190-249
/// ClusterFailoverDuringRecovery）：副本同步恢复门占用期内 TAKEOVER 判
/// failover-aborted 且拓扑零改写；恢复收敛 NoRecovery 后再发接管以
/// failover-completed 完成真接管。C# 以 async replicate + 10s 无盘延迟
/// 天然形成恢复窗，本例以 begin_recovery(ClusterReplicate) 直驱同一恢复门
/// 占用；单点拒收臂既有 cluster_failover_lock_rejection 锁闸，本例补
/// 「恢复期中止→恢复收敛→重发完成」全序列与 RECOVER_STATUS 两面观测
/// （:211-212、:229-235）
#[test]
fn cluster_failover_during_recovery_abort_then_retry_completes() -> Void {
  Runtime::new()?.block_on(async {
    let cp = replica_provider(7001);
    let rm = cp.replication_manager().expect("replication manager 在场");
    assert!(rm.begin_recovery(RecoveryStatus::ClusterReplicate, false));
    // C# :211-212 RECOVER_STATUS == ClusterReplicate 可观测面
    assert_eq!(rm.recovery_status(), RecoveryStatus::ClusterReplicate);

    let m = Arc::new(FailoverManager::new(Arc::clone(&cp)));
    // C# :215-216 恢复窗口中即时下发接管：会话获准发起后被恢复门判败
    assert!(m.try_start_replica_failover(FailoverOption::Takeover, Duration::ZERO));
    wait_until(&m, "failover-aborted").await; // C# :219-226
    // 拒收不得改写拓扑：本地仍副本、旧主仍在场
    {
      let cm = cp.cluster_manager().expect("cluster manager 在场");
      let config = cm.current_config.read();
      assert!(config.is_replica(), "恢复窗内被拒的接管不得升主");
      assert_eq!(
        config.local_node_primary_id(),
        Some(0x0000_0000_0000_0000_0000_0000_0000_DE11)
      );
    }

    // C# :229-235 replicate 完成 → RECOVER_STATUS 归 NoRecovery
    rm.end_recovery(RecoveryStatus::NoRecovery, false);
    assert_eq!(rm.recovery_status(), RecoveryStatus::NoRecovery);

    // C# :238-249 重发接管 → completed，真接管落地
    assert!(m.try_start_replica_failover(FailoverOption::Takeover, Duration::ZERO));
    wait_until(&m, "failover-completed").await;
    let cm = cp.cluster_manager().expect("cluster manager 在场");
    let config = cm.current_config.read();
    assert!(config.is_primary());
    assert_eq!(config.get_worker_id_from_slot(0), LOCAL_WORKER_ID);
    assert!(config.local_node_config_epoch() > 3);
    aok::OK
  })
}
