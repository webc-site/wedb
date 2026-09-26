//! failover 停写位点快照链纪元排空栅栏返值承判收口回归测试（票
//! wedb-failover-stopwrites-epoch-drain-return-ignored-ack-write-loss）。
//!
//! 背景（对标 garnet C# 一手锚）：`bump_and_wait_for_epoch_transition_async`
//! 原语在 C# 结构上恒真（ClusterProvider.cs:366-389 无限自旋至全会话静止），
//! 故 C# 各调用点丢弃返值无失语义——停写应答面
//! （RespClusterFailoverCommands.cs:128-129 BlockingWait 达成后才回位点）、
//! 主发起面（PrimaryFailoverSession.cs:113-117 静止达成才探测再 TAKEOVER）、
//! 副本消费面（ReplicaFailoverSession.cs:71-112 WaitAsync 超时/异常走
//! catch → false，「无有效应答」与「位点为零」绝不混同）。rust 有界化后
//! false = 静止未达成：弃返值即令滞留批内在途写越过采样位点提交，已 ACK
//! 写随槽位让渡永久丢失、无自愈路径（deviations §95 判败同族，与 r25 迁移
//! 族同原语同害同判）；pause 臂 `_ =>` 坍缩空串经 AofAddress::from_string("")
//! = Some(零位点) 被误判「主端位点为零、副本已追平」，未经任何确认直入接管。
//!
//! 三面锁：
//! - (a) 主端 CLUSTER FAILSTOPWRITES：排空未达成 → 判败回 -ERR、零位点应答，
//!   停写让渡已经 try_restore_stop_writes 赎回（角色/槽位/主指针原状）；
//! - (b) 主发起 FAILOVER：排空未达成 → 零位点探测零 TAKEOVER 下发，
//!   赎回臂回滚，终态 failover-aborted；
//! - (c) 副本 pause 臂：主端应答缺席形（超时/断连坍缩/空串 bulk）与错误帧
//!   一律判败放弃，绝不以零位点假目标放行接管（状态不越过 IssuingPauseWrites、
//!   不复位主端、本地不翻转接管）。
//!
//! 恒不追平夹具复用 diskless_epoch_drain_failclose.rs 形态：注册会话先行批首
//! 纪元快照（acquire_current_epoch），其后原语自 bump 即恒落后，
//! set_cluster_node_timeout_ms 极小值令超时即刻达。
//!
//! 反证基线（revert-proof）：还原任一处弃返值/坍缩臂——(a) 照回位点 bulk
//! 且本端滞留副本态、(b) TAKEOVER 下发落账、(c) 零位点直入接管翻角色，
//! 对应用例转红。既有 failover 面（cluster_failover.rs /
//! failover_primary_probe.rs / failover_timeout_bounds.rs /
//! primary_live_repl_offset.rs）不回退。
use std::{net::SocketAddr, str::from_utf8, sync::Arc, time::Duration};

use aok::Void;
use compio::{
  buf::BufResult,
  io::AsyncRead,
  net::{TcpListener, TcpStream},
  runtime::Runtime,
};
use wdev::SegmentedDevice;
use wedb::server::{
  cluster::IClusterProvider,
  cluster_provider::ClusterProvider,
  cluster_session::ClusterSession,
  failover::{failover_manager::FailoverManager, failover_option::FailoverOption},
  hash_slot::SlotState,
  worker::{LOCAL_WORKER_ID, LocalWorkerSpec, NodeRole, Worker},
};
use wkv::WedbStore;
use wnode::{
  ClusterSessionFace, MessageConsumerFace, RespSessionConsumer,
  resp::{garnet_api::StoreGarnetApi, resp_server_session::RespServerSessionOptions},
};
use wtest_base::{
  FailoverNode, SilentNode, StopWritesNode, parse_frame_slices, resp_frame_str, test_store_config,
};
use wtxn::{TxnLockTable, WatchVersionMap};

const LOCAL_ID: u128 = 0x0000_0000_0000_0000_0000_0000_0000_DE11;
const REPLICA_ID: u128 = 0x0000_0000_0000_0000_0000_0000_0000_5107;
const OLD_PRIMARY_ID: u128 = LOCAL_ID;
/// 停写让渡目标（a 面：未知副本 id 亦走真实让渡置位形，同主测夹具）
const DELEGATE_ID: u128 = 0x0000_0000_0000_0000_0000_0000_0000_beef;
/// 排空未达成注入档：200ms 既令超时即刻达，又足以让 revert 臂的真应答在限时内先达
const DRAIN_TIMEOUT_MS: u64 = 200;

/// 装配主端 RESP 会话消费者（真命令入口泵 CLUSTER FAILSTOPWRITES 帧）
fn cluster_consumer(cp: &Arc<ClusterProvider>) -> RespSessionConsumer {
  let cluster_session = cp.create_cluster_session();
  let dir = tempfile::tempdir().unwrap().keep();
  let device = Arc::new(SegmentedDevice::single_file(dir.join("failov.db")).unwrap());
  let store = Arc::new(WedbStore::open(test_store_config(), device).unwrap());
  cp.set_store(Arc::clone(&store));
  let mut consumer = RespSessionConsumer::with_cluster(
    1,
    RespServerSessionOptions::default(),
    cluster_session,
    cp.provider_handle(),
    Arc::new(StoreGarnetApi::new(store.new_session().unwrap())),
  );
  consumer.attach_transaction_components(Arc::new(WatchVersionMap::new(64)), TxnLockTable::new());
  consumer
}

/// 泵一帧，回即时应答；慢路径登记在场时交调用方经 take_slow_wait 驱动
fn consume_frame(consumer: &mut RespSessionConsumer, frame: &[u8]) -> Vec<u8> {
  let mut scratch = consumer.take_recv_scratch();
  scratch.extend_from_slice(frame);
  consumer.return_recv_scratch(scratch);
  let mut resp = Vec::new();
  let remaining = consumer.try_consume_messages_into(&mut resp);
  assert_eq!(remaining, Some(0), "帧应被完整消费");
  resp
}

/// 不追平夹具（diskless_epoch_drain_failclose.rs 同款）：注册会话先行批首
/// 纪元快照，其后原语自 bump 恒落后 → 排空等待必超时
fn park_lagging_session(cp: &Arc<ClusterProvider>) -> Arc<ClusterSession> {
  cp.bump_current_epoch();
  let lag = cp.create_cluster_session();
  lag.acquire_current_epoch();
  assert_eq!(lag.local_current_epoch(), cp.current_epoch());
  cp.set_cluster_node_timeout_ms(DRAIN_TIMEOUT_MS);
  lag
}

/// (a) 主端停写应答面：恒不追平夹具令排空判败 → 应答必须为 -ERR（零位点
/// bulk）、让渡已赎回原状。revert（还原裸语句弃返值）即照回位点应答且本端
/// 滞留副本态，两断言同时转红
#[test]
fn fail_stop_writes_drain_failure_returns_err_and_redelegates() -> Void {
  let cp = ClusterProvider::new();
  let m = cp.cluster_manager().expect("cluster manager 在场");
  {
    let mut config = m.current_config.write();
    config.initialize_local_worker(LocalWorkerSpec {
      node_id: LOCAL_ID,
      address: "127.0.0.1",
      port: 7000,
      config_epoch: 1,
      role: NodeRole::Primary,
      replica_of_node_id: None,
      hostname: None,
    });
    config.assign_slots(&[0, 1], LOCAL_WORKER_ID as u16, SlotState::Stable);
  }
  let lag = park_lagging_session(&cp);
  let mut consumer = cluster_consumer(&cp);

  // CLUSTER FAILSTOPWRITES <32hex 副本节点 id>（非空载荷 = 让渡路径）
  let frame = resp_frame_str(&["CLUSTER", "FAILSTOPWRITES", &hex_id(DELEGATE_ID)]);
  assert!(
    consume_frame(&mut consumer, &frame).is_empty(),
    "停写慢路径不产即时应答"
  );
  let slow = consumer.take_slow_wait().expect("停写登记慢路径");
  let ack = Runtime::new()?.block_on(async { slow.resolve().await });
  let text = from_utf8(&ack).unwrap_or_default().to_string();
  assert!(
    text.starts_with("-ERR") && text.contains("epoch drain not settled"),
    "排空未达成必须判败回 -ERR、零位点应答，实得 {text:?}"
  );

  // 赎回原状：让渡标志解除、角色回 PRIMARY、主指针清空、槽位收回本地
  assert!(!m.is_stop_writes_delegated(), "判败后让渡标志须已赎回");
  let config = m.current_config.read();
  assert!(config.is_primary(), "判败后本端角色须回 PRIMARY");
  assert_eq!(config.local_node_primary_id(), None, "主指针须清空");
  assert_eq!(
    config.get_worker_id_from_slot(0),
    LOCAL_WORKER_ID,
    "槽位 0 须收回本地"
  );
  drop(lag);
  aok::OK
}

/// (b) 主发起 FAILOVER：停写让渡后排空判败 → 零位点探测零 TAKEOVER 下发，
/// 既有 !success && stopped_writes 赎回臂回滚原状，终态 failover-aborted。
/// revert（还原弃返值）则放行探测、FailoverNode 应答追平当选、TAKEOVER 下发
/// 落账且不再赎回，takeover_received/角色/槽位三断言转红
#[test]
fn primary_failover_drain_failure_issues_zero_takeover() -> Void {
  let offset_reply = Arc::new(
    ClusterProvider::new()
      .replication_manager()
      .expect("rm 在场")
      .get_current_replication_offset()
      .to_aof_string(),
  );
  Runtime::new()?.block_on(async {
    let fake = FailoverNode::bind(offset_reply, Duration::ZERO).await;
    let cp = ClusterProvider::new();
    let m = cp.cluster_manager().expect("cluster manager 在场");
    {
      let mut config = m.current_config.write();
      config.initialize_local_worker(LocalWorkerSpec {
        node_id: LOCAL_ID,
        address: "127.0.0.1",
        port: 7001,
        config_epoch: 3,
        role: NodeRole::Primary,
        replica_of_node_id: None,
        hostname: None,
      });
      config.workers.push(Worker {
        nodeid: Some(REPLICA_ID),
        address: "127.0.0.1".into(),
        port: fake.port() as i32,
        config_epoch: 3,
        // 靶 worker 角色取 Primary 供 -1 端点选集收集，replica_of 登记供
        // get_replica_ids/try_stop_writes 走通（failover_primary_probe.rs 同形）
        role: NodeRole::Primary,
        replica_of_node_id: Some(LOCAL_ID),
        replication_offset: 0,
        hostname: None,
      });
      config.assign_slots(&[0, 1], LOCAL_WORKER_ID as u16, SlotState::Stable);
    }
    let _lag = park_lagging_session(&cp);

    let fm = Arc::new(FailoverManager::new(Arc::clone(&cp)));
    assert!(fm.try_start_primary_failover(
      "",
      -1,
      FailoverOption::Takeover,
      Duration::from_secs(10)
    ));
    fm.wait_failover_done().await;
    assert_eq!(fm.get_last_failover_status(), "failover-aborted");
    assert!(
      !fake.takeover_received(),
      "排空未达成即判败：不得探测位点、不得下发 CLUSTER FAILOVER TAKEOVER"
    );
    assert!(!m.is_stop_writes_delegated(), "赎回臂须已解除让渡标志");
    let config = m.current_config.read();
    assert!(config.is_primary(), "判败回滚后角色须回 PRIMARY");
    assert_eq!(config.local_node_primary_id(), None);
    assert_eq!(
      config.get_worker_id_from_slot(0),
      LOCAL_WORKER_ID,
      "槽位须收回本地"
    );
    aok::OK
  })
}

/// (c) 副本 pause 臂应答缺席形全谱：主端 FAILSTOPWRITES 应答以超时（沉默）、
/// 断连（握手后掐链）、错误帧、空串 bulk 任一形态缺席，一律判败放弃本次
/// failover——绝不坍缩零位点假目标直入接管。各档断言：终态 failover-aborted、
/// 本地角色/槽位/纪元原状、未越过 IssuingPauseWrites 半边以「不复位主端」
/// （reset 帧零发出）直证。revert（还原 `_ => String::new()` 坍缩臂）：
/// 空串/断连档零位点直入接管且翻角色成功转 completed、超时档复位帧发出，
/// 对应断言转红
#[test]
fn replica_pause_arm_rejects_absent_or_empty_ack() -> Void {
  Runtime::new()?.block_on(async {
    // 档一：沉默超时（既有 cluster_failover_control_command_timeout_aborts 拓扑，
    // 判别升格为 reset 帧零发出——旧形位点等待超时兜底也回 aborted，唯复位臂可分坍缩与否）
    let silent = SilentNode::bind(2).await;
    let cp = replica_provider(silent.port() as i32);
    let before = snapshot_config(&cp);
    let m = Arc::new(FailoverManager::new(Arc::clone(&cp)));
    assert!(m.try_start_replica_failover(FailoverOption::Default, Duration::from_millis(300)));
    m.wait_failover_done().await;
    assert_eq!(m.get_last_failover_status(), "failover-aborted");
    assert_replica_untouched(&cp, &before);
    assert_eq!(
      silent.silent_frame_count(),
      1,
      "判败只发过停写请求一帧：不得进入位点等待态、不得回发复位"
    );

    // 档二：主端确认应答但位点为空串 bulk（from_string("") 零位点坍缩点直打）
    let empty = StopWritesNode::bind(Arc::new(String::new())).await;
    let cp = replica_provider(empty.port() as i32);
    let before = snapshot_config(&cp);
    let m = Arc::new(FailoverManager::new(Arc::clone(&cp)));
    assert!(m.try_start_replica_failover(FailoverOption::Default, Duration::from_secs(10)));
    m.wait_failover_done().await;
    assert_eq!(m.get_last_failover_status(), "failover-aborted");
    assert_replica_untouched(&cp, &before);
    assert!(
      !empty.reset_received(),
      "判败未越过位点确认门，不得回发复位"
    );

    // 档三：断连坍缩（client.rs execute_cluster_fail_stop_writes_async
    // unwrap_or_default 空串形）——停写请求即掐链
    let dropped = PauseFakePrimary::bind(DropMode::CloseOnStopWrites).await;
    let cp = replica_provider(dropped.port() as i32);
    let before = snapshot_config(&cp);
    let m = Arc::new(FailoverManager::new(Arc::clone(&cp)));
    assert!(m.try_start_replica_failover(FailoverOption::Default, Duration::from_secs(10)));
    m.wait_failover_done().await;
    assert_eq!(m.get_last_failover_status(), "failover-aborted");
    assert_replica_untouched(&cp, &before);

    // 档四：主端错误帧（-ERR 文本非位点文法）——既有 from_string 判败臂形状锁
    let errored = PauseFakePrimary::bind(DropMode::ErrorFrameOnStopWrites).await;
    let cp = replica_provider(errored.port() as i32);
    let before = snapshot_config(&cp);
    let m = Arc::new(FailoverManager::new(Arc::clone(&cp)));
    assert!(m.try_start_replica_failover(FailoverOption::Default, Duration::from_secs(10)));
    m.wait_failover_done().await;
    assert_eq!(m.get_last_failover_status(), "failover-aborted");
    assert_replica_untouched(&cp, &before);
    aok::OK
  })
}

/// 32 字符小写 hex 节点 id（协议线形）
fn hex_id(id: u128) -> String {
  format!("{id:032x}")
}

/// 副本节点拓扑（cluster_failover.rs::replica_provider 同形）：本地 E701 从属
/// 旧主 DE11@127.0.0.1:primary_port、槽 0..1 归旧主 worker——DEFAULT 故障转移
/// 真接管一旦放行即翻角色/槽位/纪元，pause 臂判败面须寸土不动
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
      replica_of_node_id: Some(OLD_PRIMARY_ID),
      hostname: None,
    });
    let p_idx = {
      config.workers.push(Worker {
        nodeid: Some(OLD_PRIMARY_ID),
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

/// pause 臂注入靶端行为档：对 CLUSTER FAILSTOPWRITES 掐断连接 / 回错误帧，
/// 其余帧照常应答（模拟「握手可用、停写应答缺席」的主端）
#[derive(Clone, Copy)]
enum DropMode {
  CloseOnStopWrites,
  ErrorFrameOnStopWrites,
}

/// pause 臂专用假主端（SilentNode/StopWritesNode 同族形态的本地补形：
/// 既有 wtest_base 靶端无「停写请求掐链/错误帧」档，不扩共享面）
struct PauseFakePrimary {
  addr: SocketAddr,
}

impl PauseFakePrimary {
  async fn bind(mode: DropMode) -> Self {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    compio::runtime::spawn(async move {
      while let Ok((stream, _)) = listener.accept().await {
        compio::runtime::spawn(async move { serve(stream, mode).await }).detach();
      }
    })
    .detach();
    Self { addr }
  }

  fn port(&self) -> u16 {
    self.addr.port()
  }
}

/// 单连接服务循环：对 CLUSTER FAILSTOPWRITES 按档位掐链或回 -ERR，
/// 其余帧（含握手）逐帧回 +OK（SilentNode 一请求一应答节奏）
async fn serve(mut stream: TcpStream, mode: DropMode) {
  use compio::io::AsyncWriteExt;
  let mut acc: Vec<u8> = Vec::new();
  let mut buf = vec![0u8; 4096];
  loop {
    let BufResult(res, next) = stream.read(buf).await;
    buf = next;
    let Ok(n) = res else { return };
    if n == 0 {
      return;
    }
    acc.extend_from_slice(&buf[..n]);
    let mut consumed = 0;
    while let Some((frame_len, payloads)) = parse_frame_slices(&acc[consumed..]) {
      consumed += frame_len;
      let is_stop_writes =
        payloads.len() >= 3 && payloads[0] == b"CLUSTER" && payloads[1] == b"FAILSTOPWRITES";
      if is_stop_writes {
        match mode {
          DropMode::CloseOnStopWrites => return,
          DropMode::ErrorFrameOnStopWrites => {
            if stream.write_all(b"-ERR boom\r\n").await.is_err() {
              return;
            }
          }
        }
      } else if stream.write_all(b"+OK\r\n").await.is_err() {
        return;
      }
    }
    if consumed > 0 {
      acc.drain(..consumed);
    }
  }
}

/// 接管放行判据快照（角色/主指针/槽位归属/纪元/让渡标志）
struct ConfigSnapshot {
  role: NodeRole,
  primary_id: Option<u128>,
  slot0_owner: usize,
  epoch: i64,
}

fn snapshot_config(cp: &Arc<ClusterProvider>) -> ConfigSnapshot {
  let m = cp.cluster_manager().expect("cluster manager 在场");
  let config = m.current_config.read();
  ConfigSnapshot {
    role: config.local_node_role(),
    primary_id: config.local_node_primary_id(),
    slot0_owner: config.get_worker_id_from_slot(0),
    epoch: config.local_node_config_epoch(),
  }
}

/// pause 臂判败后本地寸土不动：仍为副本、主指针在场、槽位仍归旧主、
/// 纪元未被接管推进（take_over 一旦放行即四项齐翻——revert 必红）
fn assert_replica_untouched(cp: &Arc<ClusterProvider>, before: &ConfigSnapshot) {
  let now = snapshot_config(cp);
  assert_eq!(
    now.role,
    NodeRole::Replica,
    "角色不得翻转（pause 判败被零位点放行）"
  );
  assert_eq!(now.primary_id, before.primary_id, "主指针不得清空");
  assert_eq!(now.slot0_owner, before.slot0_owner, "槽位不得收回本地");
  assert_eq!(now.epoch, before.epoch, "配置纪元不得被接管推进");
}
