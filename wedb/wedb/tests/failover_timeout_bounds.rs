//! FAILOVER / CLUSTER FAILOVER 超时参数面与 deadline 溢出边界用例（票
//! wfailover-network-failover-duration-max-overflow）：主节点入口
//! timeout_ms<=0 与从节点入口同口径传零时长、由会话层单点归一为 600 秒
//! 缺省预算，杜绝 Duration::MAX 绕过归一并溢出 coarsetime deadline；
//! 显式超大 TIMEOUT 构造会话不 panic 且可限时收敛

use std::{
  sync::Arc,
  time::{Duration, Instant},
};

use aok::Void;
use compio::runtime::Runtime;
use wconf::RuntimeServerOptions;
use wdev::SegmentedDevice;
use wedb::server::{
  cluster::IClusterProvider,
  cluster_provider::ClusterProvider,
  cluster_session::ClusterSession,
  hash_slot::SlotState,
  worker::{LocalWorkerSpec, NodeRole, Worker},
};
use wkv::WedbStore;
use wnode::{
  GarnetAppendOnlyFile, GarnetLog, MessageConsumerFace, RespSessionConsumer,
  resp::{garnet_api::StoreGarnetApi, resp_server_session::RespServerSessionOptions},
};
use wresp::cmd_strings::RESP_ERR_GENERIC_SYNTAX_ERROR;
use wtest_base::{resp_frame_str, test_store_config};
use wtxn::{TxnLockTable, WatchVersionMap};

/// 泵等价消费单命令往返（对标 cluster_resp_session 的会话内闭环形态）
fn roundtrip(consumer: &mut RespSessionConsumer, frame: &[u8]) -> Vec<u8> {
  let mut scratch = consumer.take_recv_scratch();
  scratch.extend_from_slice(frame);
  consumer.return_recv_scratch(scratch);
  let mut resp = Vec::new();
  let consumed = consumer.try_consume_messages_into(&mut resp);
  assert_eq!(consumed, Some(0), "帧应被完整消费: {frame:?}");
  resp
}

/// 构造挂接集群切面 + 存储执行域的会话消费者
fn cluster_consumer(cp: &Arc<ClusterProvider>) -> RespSessionConsumer {
  let cluster_session: Arc<ClusterSession> = cp.create_cluster_session();
  let dir = tempfile::tempdir().unwrap().keep();
  let device = Arc::new(SegmentedDevice::single_file(dir.join("fo.db")).unwrap());
  let store = Arc::new(WedbStore::open(test_store_config(), device).unwrap());
  cp.set_store(Arc::clone(&store));
  if cp.try_aof().is_none() {
    let options = RuntimeServerOptions::default();
    let log = Arc::new(
      GarnetLog::new(
        &options,
        {
          let (_dirs, backends) = wnode_test::test_sublogs("failover_timeout", 1);
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

/// 本地主节点、名下无副本：主端 FAILOVER 会话在停写目标缺失分支即刻收口，
/// 会话是否panic/挂起完全由入参超时形态决定
fn primary_provider() -> Arc<ClusterProvider> {
  let cp = ClusterProvider::new();
  let m = cp.cluster_manager().expect("cluster manager 在场");
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
  cp
}

/// 本地副本挂旧主 DE11@7001（cluster_failover.rs 同形拓扑）：TAKEOVER 走
/// 本地接管即刻完成，从节点侧缺省超时面为既有正确口径的对照组
fn replica_provider() -> Arc<ClusterProvider> {
  let cp = ClusterProvider::new();
  let m = cp.cluster_manager().expect("cluster manager 在场");
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
  cp
}

/// 主节点入口缺省超时面（network_failover，C# FailoverCommand.cs:110 的
/// `timeout <= 0` 半边）：不带 TIMEOUT、显式 0、负值三种写法一律经
/// Duration::ZERO 归一至 600 秒缺省预算——会话构造即算 deadline（C#
/// FailoverSession.cs:86 同位），修复前传 Duration::MAX 在 coarsetime 裸
/// u64 ticks 加法（instant.rs:323）上 debug 溢出 panic、命令帧直接炸；
/// 修复后命令应答 +OK 且会话在有限时间内收口
#[test]
fn primary_failover_default_timeout_planes() -> Void {
  Runtime::new()?.block_on(async {
    let cp = primary_provider();
    let mut consumer = cluster_consumer(&cp);
    let fm = cp.failover_manager().expect("failover manager 装配");
    let cases: [Vec<&str>; 3] = [
      vec!["FAILOVER"],
      vec!["FAILOVER", "TIMEOUT", "0"],
      vec!["FAILOVER", "TIMEOUT", "-7"],
    ];
    for args in cases {
      let frame = resp_frame_str(&args);
      let start = Instant::now();
      assert_eq!(roundtrip(&mut consumer, &frame), b"+OK\r\n", "{args:?}");
      fm.wait_failover_done().await;
      assert_eq!(fm.get_failover_status(), "no-failover");
      assert!(
        start.elapsed() < Duration::from_secs(5),
        "缺省超时会话须限时收口而非挂起: {args:?}"
      );
    }
    aok::OK
  })
}

/// 从节点入口对照面（network_cluster_failover，
/// RespClusterFailoverCommands.cs:29/55 的 default→600s 归一）：显式 0 秒
/// 与缺省同形传 Duration::ZERO，接管会话照常完成——与上一用例共同焊住
/// 两入口 timeout_ms<=0 参数面一致
#[test]
fn replica_failover_default_timeout_plane_matches_primary() -> Void {
  Runtime::new()?.block_on(async {
    let cases: [Vec<&str>; 2] = [
      vec!["CLUSTER", "FAILOVER", "TAKEOVER"],
      vec!["CLUSTER", "FAILOVER", "TAKEOVER", "0"],
    ];
    for args in cases {
      // TAKEOVER 会话会改写本地拓扑，每个参数面单独装配
      let cp = replica_provider();
      let mut consumer = cluster_consumer(&cp);
      let fm = cp.failover_manager().expect("failover manager 装配");
      let frame = resp_frame_str(&args);
      let start = Instant::now();
      assert_eq!(roundtrip(&mut consumer, &frame), b"+OK\r\n", "{args:?}");
      fm.wait_failover_done().await;
      assert_eq!(fm.get_last_failover_status(), "failover-completed");
      assert!(
        start.elapsed() < Duration::from_secs(5),
        "缺省超时会话须限时收口而非挂起: {args:?}"
      );
    }
    aok::OK
  })
}

/// 显式超大 TIMEOUT（正数分支的极端入参，i64::MAX 毫秒）：命令面构造
/// FailoverSession 不得在 deadline 加法上溢出 panic（修复前裸 `+` 于
/// coarsetime checked 加法必炸；修复后 saturating_add 饱和为远期
/// deadline），会话照常可收口
#[test]
fn primary_failover_max_timeout_constructs_without_panic() -> Void {
  Runtime::new()?.block_on(async {
    let cp = primary_provider();
    let mut consumer = cluster_consumer(&cp);
    let fm = cp.failover_manager().expect("failover manager 装配");
    let frame = resp_frame_str(&["FAILOVER", "TIMEOUT", "9223372036854775807"]);
    assert_eq!(roundtrip(&mut consumer, &frame), b"+OK\r\n");
    fm.wait_failover_done().await;
    assert_eq!(fm.get_failover_status(), "no-failover");
    aok::OK
  })
}

/// 顶层 FAILOVER TAKEOVER 为不可用输入（票 zcode-r26-autofailover 发现二，
/// 对标 FailoverCommand.cs:35-65 switch 无 TAKEOVER 分支落 default throw）：
/// 修复前 rust 静默降格 FORCE 发起真实停写让渡，修复后回语法错误帧；FORCE
/// 合法臂回归不变（发现二验证点第二半）
#[test]
fn primary_failover_takeover_rejected_force_regression_unchanged() -> Void {
  Runtime::new()?.block_on(async {
    let cp = primary_provider();
    let mut consumer = cluster_consumer(&cp);
    let frame = resp_frame_str(&["FAILOVER", "TAKEOVER"]);
    assert_eq!(
      roundtrip(&mut consumer, &frame),
      format!("-{}\r\n", RESP_ERR_GENERIC_SYNTAX_ERROR).into_bytes(),
      "顶层 TAKEOVER 须被拒绝（C# 不可用输入）"
    );
    let fm = cp.failover_manager().expect("failover manager 装配");
    let frame = resp_frame_str(&["FAILOVER", "FORCE"]);
    assert_eq!(
      roundtrip(&mut consumer, &frame),
      b"+OK\r\n",
      "FORCE 回归不变"
    );
    fm.wait_failover_done().await;
    aok::OK
  })
}

/// 从端负 TIMEOUT 语义面（票 zcode-r26-autofailover 发现三验证点）：
/// FORCE 臂负值不受超时影响照常完成（C# FORCE 跳过同步等待，deadline 不
/// 被消费）；会话层即刻失败半边由 failover_session 单测
/// negative_failover_timeout_is_immediate_deadline 锁定（DEFAULT 臂停写等
/// 同步首站超时终判）。命令应答 +OK 且会话限时收口
#[test]
fn replica_failover_force_negative_timeout_unaffected() -> Void {
  Runtime::new()?.block_on(async {
    let cp = replica_provider();
    let mut consumer = cluster_consumer(&cp);
    let fm = cp.failover_manager().expect("failover manager 装配");
    let frame = resp_frame_str(&["CLUSTER", "FAILOVER", "FORCE", "-5"]);
    let start = Instant::now();
    assert_eq!(roundtrip(&mut consumer, &frame), b"+OK\r\n");
    fm.wait_failover_done().await;
    assert_eq!(
      fm.get_last_failover_status(),
      "failover-completed",
      "FORCE 臂不受负超时影响，照常完成接管"
    );
    assert!(
      start.elapsed() < Duration::from_secs(5),
      "会话须限时收口而非挂起"
    );
    aok::OK
  })
}

/// deviations §117d TIMEOUT 超 i32 值域收形锁：C# 顶层毫秒与从端秒均走
/// TryGetInt int32 值域（FailoverCommand.cs:42/:50、RespClusterFailover
/// Commands.cs:47），4000000000 界外回 not-integer；rust 超时档统一
/// strict_i64 收下（顶层 failover.rs TIMEOUT 臂、从端秒数臂），非零不进
/// 600 秒归一、原样入会话预算（归一臂消费断言见 failover_session 单测
/// i32_overflow_failover_timeout_flows_into_budget_unnormalized）。
/// 双臂命令面收形 +OK 且会话限时收口，严禁按 C# int32 档回缩值域
#[test]
fn failover_timeout_beyond_i32_range_accepted() -> Void {
  Runtime::new()?.block_on(async {
    // 顶层毫秒档：无副本主节点会话在停写目标缺失分支即刻收口（同
    // primary_failover_default_timeout_planes 形），应答不得落 not-integer
    let cp = primary_provider();
    let mut consumer = cluster_consumer(&cp);
    let fm = cp.failover_manager().expect("failover manager 装配");
    let frame = resp_frame_str(&["FAILOVER", "TIMEOUT", "4000000000"]);
    let start = Instant::now();
    assert_eq!(
      roundtrip(&mut consumer, &frame),
      b"+OK\r\n",
      "超 i32 值域 TIMEOUT 须被 strict_i64 收下（C# 拒收形严禁回改）"
    );
    fm.wait_failover_done().await;
    assert_eq!(fm.get_failover_status(), "no-failover");
    assert!(
      start.elapsed() < Duration::from_secs(5),
      "界外大值会话须限时收口而非挂起"
    );
    // 从端秒档：FORCE 臂收下 4000000000 秒预算照常完成接管（FORCE 跳过同步
    // 等待，同 replica_failover_force_negative_timeout_unaffected 形）
    let cp = replica_provider();
    let mut consumer = cluster_consumer(&cp);
    let fm = cp.failover_manager().expect("failover manager 装配");
    let frame = resp_frame_str(&["CLUSTER", "FAILOVER", "FORCE", "4000000000"]);
    assert_eq!(
      roundtrip(&mut consumer, &frame),
      b"+OK\r\n",
      "从端秒档超 i32 值域须被 strict_i64 收下"
    );
    fm.wait_failover_done().await;
    assert_eq!(fm.get_last_failover_status(), "failover-completed");
    aok::OK
  })
}
