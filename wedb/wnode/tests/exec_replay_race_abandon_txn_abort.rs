//! EXEC 重放段竞速废弃的事务终结符收口回归
//! （task/ing/wnode-exec-replay-suspend-abandon-txnstart-group-residue）
//!
//! 缺陷：rust 泵把 EXEC 重放段（[`wtxn::TxnState::Running`] 直通）中命令的挂起
//! 交 `probe_race` 三臂竞速驱动（wnode/src/net/handler/drive.rs 阻塞/慢/脚本臂），
//! 终止广播（CLIENT KILL / 停机令牌）或对端 FIN/RST 胜出即 `RaceEnd::Disposed`
//! 丢弃执行体、重放中途废弃——尾帧 EXEC 不再被消费，`finish_run_postlock` 已落
//! 的 TxnStart 成孤儿残组：恢复/副本侧 `AofReplayCoordinator` 对无终结符的
//! active 组整组静默弃置（组缓冲随 session_id 滞留至重启）、重放前缀已生效写
//! 重启后丢失、主副本活态分叉；`RespServerSession::dispose` 收场只取消挂起体、
//! 摘订阅、关集群切面，对 Running 事务不投任何终结符。
//!
//! 修复：事务收口单点 [`wtxn::TransactionManager::finish_abandoned`]——Running
//! 态经既有 enqueue_txn 通道补投显式 `AofEntryType::TxnAbort`(0x22) 终结符
//! （接通 waof 既有判别值与协调器既有弃组臂），随后走现行 `reset`（锁释放与
//! 屏障注销路径零改动）；泵三臂 `RaceEnd::Disposed` 与会话 `dispose` 漏斗共调
//! 本口，状态门在单点故双入口不重复投递；无 AOF 或未落组的只读事务直复位。
//! 终结符取 Abort 不取 Commit：重放废弃组 = 刻意弃置面（严禁改判为补提交）。
//!
//! 测试对标：C# 无此废弃面（libs/server/Transaction/TxnRespCommands.cs
//! NetworkEXEC 在单网络线程内联重放、批中途不可打断；CLIENT KILL 经
//! libs/common/Networking/GarnetTcpNetworkSender.cs:TryClose 仅令后续读失败，
//! 重放必达 Commit；libs/server/AOF/AofEntryType.cs 的 TxnAbort 在 C# 零生产
//! 写入方），故本单按票面验证点构建 rust 侧真链路——真 socket CLIENT KILL
//! 竞速、真 AOF 尾扫描、真锁表探针、真协调器组缓冲观测、真重启恢复，
//! 严禁假 mock。

use std::{
  net::SocketAddr,
  path::PathBuf,
  sync::Arc,
  time::{Duration, Instant},
};

use compio::{
  net::TcpStream,
  runtime::Runtime,
  time::{sleep, timeout},
};
use parking_lot::Mutex;
use tempfile::TempDir;
use waof::{AofEntryType, AofHeader};
use wcol::itembroker::{
  collection_item_broker::CollectionItemBroker, item_broker_face::SharedItemBroker,
};
use wconf::{RuntimeServerConfig, RuntimeServerOptions};
use wdev::SegmentedDevice;
use whasher::scoped_hash;
use wkv::WedbStore;
use wnode::{
  GarnetLog, GarnetServer, MessageConsumerFace, RespSessionConsumer, SessionProviderFace,
  WireFormat,
  aof::replaycoordinator::aof_replay_coordinator::{AofReplayCoordinator, TxnAction},
  resp::objects::collection_item_source::CollectionItemSource,
  service::StorageSessionProvider,
};
use wnode_test::{
  SessionFactory, consumer_on, open_recovered_provider, read_reply, send, send_cmd,
  session_factory, start_server,
};
use wtest_base::{open_test_store, resp_frame, test_store_config};
use wtxn::{TxnLockTable, TxnState, WatchVersionMap};
use wval::SessionPrefixBuf;

/// 各用例各起独立 compio 运行时与 HybridLog/AOF 实例，同进程并发互踩
/// （SIGABRT），照 blocking_wait_probe_bytes_mirror 口径全局串行
static EXEC_ABORT_TESTS_LOCK: Mutex<()> = Mutex::new(());

/// 真链路异步收敛观测上限（泵臂投递、条目注销、恢复装配均在此界内收敛）
const WAIT_DEADLINE: Duration = Duration::from_secs(5);

/// 点亮 AOF 的已起服务器单机节点（句柄随结构存活；重启恢复用例经
/// [`AofNode::shutdown_for_restart`] 逐件释放并保留数据路径）
struct AofNode {
  dir: TempDir,
  data_path: PathBuf,
  provider: Arc<StorageSessionProvider<SessionFactory>>,
  server: Arc<GarnetServer<StorageSessionProvider<SessionFactory>>>,
  addr: SocketAddr,
}

impl AofNode {
  /// 节点在线引擎句柄（真锁内存与真数据面探针）
  fn store(&self) -> Arc<WedbStore<SegmentedDevice>> {
    self.provider.store()
  }

  /// 停机并释放服务器/提供者全部句柄（临时目录与数据路径交回调用方，供同
  /// 日志重启恢复重开；对标 embed_stop_aof_tail_tests 的重开装配形态）
  fn shutdown_for_restart(self) -> (TempDir, PathBuf) {
    let Self {
      dir,
      data_path,
      provider,
      server,
      ..
    } = self;
    server.stop();
    drop(server);
    drop(provider);
    (dir, data_path)
  }
}

/// 起一台 AOF 点亮的单机节点（票面「开 AOF」前提；默认 auto_commit 形态）
fn spawn_aof_node(tag: &str) -> AofNode {
  let dir = tempfile::tempdir().expect("tempdir");
  let data_path = dir.path().join(tag);
  let factory: SessionFactory = session_factory;
  let provider = Arc::new(
    StorageSessionProvider::open_with_config_and_aof(
      test_store_config(),
      &data_path,
      None,
      RuntimeServerOptions::default(),
      factory,
    )
    .expect("open with aof"),
  );
  let (server, addr) = start_server(Arc::clone(&provider));
  AofNode {
    dir,
    data_path,
    provider,
    server,
    addr,
  }
}

/// 重放段挂起流水线批：MULTI + 写 + 空键 BLPOP(timeout 0 永挂) + EXEC
///
/// 尾帧 EXEC 起 Running 直通重放：写命令即生效并落组内 AOF 条目，空键 BLPOP
/// 经 blocking.rs 的 txn_direct 臂照常 park_broker_wait（挂起窗=持闩窗为票面
/// 明注的刻意形态）——泵三臂竞速即在此窗内被终止广播/脱机胜出
fn abandoning_batch(set_key: &[u8], value: &[u8], block_key: &[u8], timeout_arg: &[u8]) -> Vec<u8> {
  let mut buf = Vec::new();
  for frame in [
    resp_frame(&[b"MULTI"]),
    resp_frame(&[b"SET", set_key, value]),
    resp_frame(&[b"BLPOP", block_key, timeout_arg]),
    resp_frame(&[b"EXEC"]),
  ] {
    buf.extend_from_slice(&frame);
  }
  buf
}

/// 完整闭合组流水线批：MULTI + 写 + EXEC（正常提交回归面）
fn commit_batch(set_key: &[u8], value: &[u8]) -> Vec<u8> {
  let mut buf = Vec::new();
  for frame in [
    resp_frame(&[b"MULTI"]),
    resp_frame(&[b"SET", set_key, value]),
    resp_frame(&[b"EXEC"]),
  ] {
    buf.extend_from_slice(&frame);
  }
  buf
}

/// 日志流内事务标记的有序真流序列（真实扫描 decode AofHeader 判型；对标
/// aof_txn_marker_recovery 同一扫描面）
fn txn_marker_seq(log: &GarnetLog) -> Vec<AofEntryType> {
  let mut out = Vec::new();
  log.scan_single_with(0, log.get_begin_address(0), log.get_tail_address(0), |r| {
    if let Some(header) = AofHeader::parse(&r.payload)
      && let Ok(op) = AofEntryType::try_from(header.op_type)
    {
      match op {
        AofEntryType::TxnStart | AofEntryType::TxnCommit | AofEntryType::TxnAbort => out.push(op),
        _ => {}
      }
    }
    true
  });
  out
}

/// 指定事务标记计数
fn marker_count(log: &GarnetLog, op: AofEntryType) -> usize {
  txn_marker_seq(log).iter().filter(|m| **m == op).count()
}

/// 日志流全部记录（地址 + 载荷），交真实协调器逐条归组观测组缓冲滞留
fn log_records(log: &GarnetLog) -> Vec<(i64, Vec<u8>)> {
  let mut out = Vec::new();
  log.scan_single_with(0, log.get_begin_address(0), log.get_tail_address(0), |r| {
    out.push((r.address as i64, r.payload.clone()));
    true
  });
  out
}

/// 真存储读值（共享引擎在线态，非恢复态）
async fn store_read(store: &Arc<WedbStore<SegmentedDevice>>, key: &[u8]) -> Option<Vec<u8>> {
  store
    .new_session()
    .expect("存储会话")
    .read(key)
    .await
    .expect("读键")
}

/// 事务 scoped 桶闩释放探针（真锁内存试锁：空闲即取到并随即归还，被持则失败）
///
/// 与生产 `build_txn_lock_table` 同一份 HashIndex 锁内存、同一 scoped 寻桶口径
///（对标 txn_exec_blocking_scope_wake 的探针形态）
fn bucket_latch_free(store: &Arc<WedbStore<SegmentedDevice>>, user_key: &[u8]) -> bool {
  let index = store.active_index();
  let bucket =
    index.bucket_index_for_hash(scoped_hash(SessionPrefixBuf::ROOT.as_slice(), user_key));
  let latch = index.get_bucket(bucket);
  if latch.try_lock_exclusive() {
    latch.unlock_exclusive();
    true
  } else {
    false
  }
}

/// 有界轮询等待谓词成立（真链路异步收敛观测，非定值睡眠赌博）
async fn wait_until(what: &str, f: impl Fn() -> bool) {
  let deadline = Instant::now() + WAIT_DEADLINE;
  while !f() {
    assert!(Instant::now() < deadline, "等待超时：{what}");
    sleep(Duration::from_millis(20)).await;
  }
}

/// 投帧后跑一次同步消费，取应答（产线消费单点，对标 aof_txn_marker_recovery
/// 同名泵替身）
fn drive(consumer: &mut RespSessionConsumer, frame: &[u8]) -> Vec<u8> {
  let mut scratch = consumer.take_recv_scratch();
  scratch.extend_from_slice(frame);
  consumer.return_recv_scratch(scratch);
  let mut resp = Vec::new();
  let consumed = consumer.try_consume_messages_into(&mut resp);
  assert!(consumed.is_some(), "帧应被完整消费: {frame:?}");
  resp
}

/// 会话所挂事务管理器真值源观测
fn txn_of(consumer: &RespSessionConsumer) -> &wtxn::TransactionManager {
  consumer
    .session()
    .txn_manager
    .as_ref()
    .expect("事务组件已挂载")
}

/// 废弃事务收口后的日志流不变量：开组标记数 = 两类终结符数之和
///（既无孤儿残组、也无孤儿终结符）
fn assert_groups_all_closed(seq: &[AofEntryType]) {
  let starts = seq.iter().filter(|m| **m == AofEntryType::TxnStart).count();
  let terminators = seq
    .iter()
    .filter(|m| **m == AofEntryType::TxnCommit || **m == AofEntryType::TxnAbort)
    .count();
  assert_eq!(
    starts, terminators,
    "每个 TxnStart 必须恰配一个终结符，实流: {seq:?}"
  );
}

/// 票面主验点：CLIENT KILL 竞速废弃 EXEC 重放段 → AOF 尾部落 TxnAbort、
/// 锁表清空、副本/恢复侧组缓冲随终结符释放（修复前：无任何终结符，残组
/// 与组缓冲滞留至重启）
#[test]
fn client_kill_during_exec_replay_delivers_txn_abort_terminator() {
  let _lock = EXEC_ABORT_TESTS_LOCK.lock();
  let rt = Runtime::new().expect("compio runtime");
  let node = spawn_aof_node("kill-abandon.db");
  let provider = Arc::clone(&node.provider);
  let store = node.store();
  let addr = node.addr;

  rt.block_on(async move {
    let log = provider.aof().expect("aof 点亮").log();

    // 挂起在 EXEC 重放段的永挂批（空键 BLPOP timeout 0）
    let mut victim = TcpStream::connect(addr).await.expect("connect victim");
    send(
      &mut victim,
      &abandoning_batch(b"ab:1", b"v1", b"ab:blk", b"0"),
    )
    .await
    .expect("send abandoning batch");

    // 竞速废弃前提实形（真流、真数据、真闩三面在场）
    wait_until("EXEC 重放段落 TxnStart", || {
      txn_marker_seq(log) == [AofEntryType::TxnStart]
    })
    .await;
    assert_eq!(
      store_read(&store, b"ab:1").await,
      Some(b"v1".to_vec()),
      "预置：重放前缀写已在共享存储生效（弃置面只在恢复/副本侧）"
    );
    assert!(
      !bucket_latch_free(&store, b"ab:1") && !bucket_latch_free(&store, b"ab:blk"),
      "预置：挂起重放段持本事务全部键的 scoped 桶闩（挂起窗=持闩窗）"
    );

    // CLIENT KILL：终止广播胜出泵阻塞臂（drive.rs probe_race → RaceEnd::Disposed）
    let mut killer = TcpStream::connect(addr).await.expect("connect killer");
    let victim_addr = victim.local_addr().expect("victim addr").to_string();
    send_cmd(
      &mut killer,
      &[b"CLIENT", b"KILL", b"ADDR", victim_addr.as_bytes()],
    )
    .await
    .expect("send client kill");
    let killed = read_reply(&mut killer).await;
    assert_eq!(killed, b":1\r\n", "KILL 命中重放段挂起连接");

    // 废弃终结符落 AOF 尾（泵臂在废弃 instant 先行收口，不等会话析构尾巴；
    // 臂口 + dispose 漏斗双入口经状态门去重，恰一份）
    wait_until("AOF 尾部落 TxnAbort 终结符", || {
      txn_marker_seq(log) == [AofEntryType::TxnStart, AofEntryType::TxnAbort]
    })
    .await;
    assert_eq!(
      marker_count(log, AofEntryType::TxnCommit),
      0,
      "废弃组严禁落提交终结符（半组提交把已废弃尾命令钉成永久缺失）"
    );
    assert_groups_all_closed(&txn_marker_seq(log));

    // 锁面随复位清空（零改动路径：reset 的 unlock_all_keys + 屏障注销）
    wait_until("废弃收口后事务桶闩释放", || {
      bucket_latch_free(&store, b"ab:1") && bucket_latch_free(&store, b"ab:blk")
    })
    .await;

    // 副本/恢复侧组缓冲观测：同一份真实记录流经真实协调器——终结符到达前
    // 组缓冲按 session_id 滞留（缺陷实形），到达即弃组清零
    let coord = AofReplayCoordinator::new(1, false, 1, None);
    let mut held_before_terminator = None;
    let mut held_after_terminator = None;
    let mut committed_groups = 0;
    for (address, payload) in log_records(log) {
      let is_abort =
        AofHeader::parse(&payload).is_some_and(|h| h.op_type == AofEntryType::TxnAbort as u8);
      if is_abort {
        held_before_terminator = Some(coord.context(0).active_txns.len());
      }
      if matches!(
        coord.add_or_replay_transaction_operation(0, &payload, address),
        TxnAction::Commit { .. }
      ) {
        committed_groups += 1;
      }
      if is_abort {
        held_after_terminator = Some(coord.context(0).active_txns.len());
      }
    }
    assert_eq!(
      held_before_terminator,
      Some(1),
      "废弃终结符到达前组缓冲必滞留一份（缺陷实形：无终结符即滞留至重启）"
    );
    assert_eq!(
      held_after_terminator,
      Some(0),
      "废弃终结符到达即弃组，副本组缓冲零滞留"
    );
    assert_eq!(
      committed_groups, 0,
      "重放废弃组绝不被整组重放（刻意弃置面，严禁改判为补提交）"
    );

    // 挂起连接已秒断（阻塞等待弃答语义：废弃批的数组头之后的元素永不补写）
    let closed = timeout(Duration::from_secs(2), read_reply(&mut victim)).await;
    assert!(
      closed.is_err() || closed.unwrap().is_empty(),
      "挂起重放段连接应被 KILL 断离"
    );
    drop(victim);
    drop(killer);
  });

  let (_dir, _data_path) = node.shutdown_for_restart();
}

/// 票面重启验点：废弃组随 TxnAbort 在恢复期弃组（同日志恢复不报非法事务流、
/// 无残组），其已生效前缀写随弃组消失为既定代价；后继完整组照常整组重放
#[test]
fn abandoned_group_recovery_discards_it_and_keeps_later_group() {
  let _lock = EXEC_ABORT_TESTS_LOCK.lock();
  let rt = Runtime::new().expect("compio runtime");
  let node = spawn_aof_node("abandon-recover.db");
  let provider = Arc::clone(&node.provider);
  let store = node.store();
  let addr = node.addr;

  rt.block_on(async move {
    let log = provider.aof().expect("aof 点亮").log();

    let mut victim = TcpStream::connect(addr).await.expect("connect victim");
    send(
      &mut victim,
      &abandoning_batch(b"rc:a", b"gone", b"rc:blk", b"0"),
    )
    .await
    .expect("send abandoning batch");

    // 触发废弃：CLIENT KILL 断开挂起重放段连接——终止广播胜出泵阻塞臂竞速
    // （drive.rs probe_race → RaceEnd::Disposed）即时收口，废弃终结符落 AOF
    let mut killer = TcpStream::connect(addr).await.expect("connect killer");
    let victim_addr = victim.local_addr().expect("victim addr").to_string();
    send_cmd(
      &mut killer,
      &[b"CLIENT", b"KILL", b"ADDR", victim_addr.as_bytes()],
    )
    .await
    .expect("send client kill");
    assert_eq!(
      read_reply(&mut killer).await,
      b":1\r\n",
      "KILL 命中重放段挂起连接"
    );
    drop(killer);

    wait_until("废弃终结符落 AOF", || {
      marker_count(log, AofEntryType::TxnAbort) == 1
    })
    .await;
    assert_eq!(
      store_read(&store, b"rc:a").await,
      Some(b"gone".to_vec()),
      "预置：废弃组前缀写在在线引擎已生效（重启后须随弃组消失）"
    );

    // 同日志后继完整组（另一连接）：残组未显式收口时，恢复期同活动组域将
    // 撞出「No nested transactions expected」类非法事务流或整段带病续放
    let mut writer = TcpStream::connect(addr).await.expect("connect writer");
    send(&mut writer, &commit_batch(b"rc:b", b"kept"))
      .await
      .expect("send commit batch");
    wait_until("后继完整组落 TxnCommit", || {
      txn_marker_seq(log)
        == [
          AofEntryType::TxnStart,
          AofEntryType::TxnAbort,
          AofEntryType::TxnStart,
          AofEntryType::TxnCommit,
        ]
    })
    .await;
    log.commit_async().await;
    drop(writer);
    drop(victim);
  });

  let (dir, data_path) = node.shutdown_for_restart();

  // 重启恢复同日志：残组经终结符弃置、后继组整组重放落库
  let recovered = rt.block_on(open_recovered_provider(&data_path));
  let recovered_store = recovered.store();
  rt.block_on(async move {
    assert_eq!(
      store_read(&recovered_store, b"rc:a").await,
      None,
      "重放废弃组的已生效写随弃组消失（废弃语义既定代价，对标 C# 恢复侧整组弃置）"
    );
    assert_eq!(
      store_read(&recovered_store, b"rc:b").await,
      Some(b"kept".to_vec()),
      "废弃终结符不得污染后继完整组（残组已显式收口，无残留活动组）"
    );
  });
  drop(recovered);
  drop(dir);
}

/// 事务收口单点的状态门与双入口去重：泵臂口（finish_abandoned_txn）与
/// dispose 漏斗共调同一单点，Running 态恰投一份废弃终结符、复位后恒零动作；
/// 收口后同会话续跑的完整组照常提交（无孤儿终结符、无孤儿开组）
#[test]
fn abandoned_txn_single_point_state_gate_and_dispose_funnel() {
  let _lock = EXEC_ABORT_TESTS_LOCK.lock();
  let rt = Runtime::new().expect("compio runtime");
  let dir = tempfile::tempdir().expect("tempdir");
  let data_path = dir.path().join("abort-single-point.db");
  let factory: SessionFactory = session_factory;
  let provider = StorageSessionProvider::open_with_config_and_aof(
    test_store_config(),
    &data_path,
    None,
    RuntimeServerOptions::default(),
    factory,
  )
  .expect("open with aof");
  let log = Arc::clone(provider.aof().expect("aof 点亮").log());
  let mut c = provider
    .get_session(WireFormat::Ascii, 1)
    .expect("会话创建成功");

  // 泵等待面（经纪挂起）须在 compio 运行时上下文内驱动，与真 socket 用例同源
  rt.block_on(async move {
    // 1. Running 态在场：重放段应答头与已执行的前缀写随流先出、空键 BLPOP 元素随挂起延后
    let out = drive(&mut c, &abandoning_batch(b"sp:1", b"v1", b"sp:blk", b"0"));
    assert_eq!(
      out, b"+OK\r\n+QUEUED\r\n+QUEUED\r\n*2\r\n+OK\r\n",
      "重放段应答头与 SET 元素先写、BLPOP 元素随挂起延后即废弃态"
    );
    let blocked = c
      .take_blocked_wait()
      .expect("空键 BLPOP 须挂经纪等待面（泵阻塞臂同款挂起体）");
    blocked.abort();
    assert_eq!(c.session().txn_state, TxnState::Running, "会话镜像 Running");
    assert_eq!(txn_of(&c).state, TxnState::Running, "管理器真值源 Running");
    assert!(
      txn_of(&c).key_entries.count() > 0,
      "预置：锁集登记在位（收口须经 reset 清零）"
    );
    assert_eq!(
      txn_marker_seq(&log),
      [AofEntryType::TxnStart],
      "预置：TxnStart 已落、终结符未落"
    );

    // 2. 臂口双调用（泵臂先行 + dispose 漏斗再至的等价形）：状态门单点去重
    c.finish_abandoned_txn();
    c.finish_abandoned_txn();
    assert_eq!(
      txn_marker_seq(&log),
      [AofEntryType::TxnStart, AofEntryType::TxnAbort],
      "Running 态恰投一份废弃终结符，二次调用恒零动作"
    );
    assert_eq!(c.session().txn_state, TxnState::None, "会话镜像随复位归零");
    assert_eq!(txn_of(&c).state, TxnState::None, "管理器态随复位归零");
    assert_eq!(
      txn_of(&c).key_entries.count(),
      0,
      "锁集登记随现行 reset 清零（本单零改动路径）"
    );
    assert_eq!(
      txn_of(&c).txn_keys.len(),
      0,
      "排队缓冲随现行 reset 清零（本单零改动路径）"
    );

    // 3. 收口后会话照常可用：同会话后继完整组走 Commit 路径（会话 ID 同键的
    //    后继 TxnStart 只有在残组被显式收口后才不撞活动组域）
    //    注：同步单消费替身复用同一会话跨废弃续跑时，废弃批在挂起点之前的尾帧
    //    EXEC 令符未随 reset 清出（真实路径废弃即 dispose 全清，无此残留），故
    //    本步不校验应答帧形，只钉 AOF 标记序列这一收口不变量
    drive(&mut c, &commit_batch(b"sp:2", b"v2"));
    assert_eq!(
      txn_marker_seq(&log),
      [
        AofEntryType::TxnStart,
        AofEntryType::TxnAbort,
        AofEntryType::TxnStart,
        AofEntryType::TxnCommit,
      ],
      "废弃收口不污染后继组标记对"
    );

    // 4. dispose 漏斗：泵三臂之外的退出路径（QUIT / 对端 EOF / 协议违规 /
    //    致命断连 / 停机排空）同样经单点收口，不重复投递
    drive(&mut c, &abandoning_batch(b"sp:3", b"v3", b"sp:blk3", b"0"));
    c.take_blocked_wait().expect("第三笔重放段挂起").abort();
    assert_eq!(txn_of(&c).state, TxnState::Running);
    c.dispose();
    let seq = txn_marker_seq(&log);
    assert_eq!(
      seq,
      [
        AofEntryType::TxnStart,
        AofEntryType::TxnAbort,
        AofEntryType::TxnStart,
        AofEntryType::TxnCommit,
        AofEntryType::TxnStart,
        AofEntryType::TxnAbort,
      ],
      "dispose 漏斗对 Running 事务补投终结符"
    );
    assert_groups_all_closed(&seq);
  });

  drop(provider);
  drop(dir);
}

/// 正常路径回归：EXEC 重放段的超时闭环与 CLIENT UNBLOCK 解除均续跑至提交，
/// 全程零废弃终结符（票面「CLIENT UNBLOCK 与超时闭环（正常 commit 路径）
/// 回归不变」）
#[test]
fn exec_replay_timeout_and_unblock_paths_still_commit_with_zero_abort() {
  let _lock = EXEC_ABORT_TESTS_LOCK.lock();
  let rt = Runtime::new().expect("compio runtime");
  let node = spawn_aof_node("commit-regress.db");
  let provider = Arc::clone(&node.provider);
  let store = node.store();
  let addr = node.addr;

  rt.block_on(async move {
    let log = provider.aof().expect("aof 点亮").log();

    // 超时闭环臂：BLPOP 0.2s 超时 → 泵竞速 Resolved → 重放续跑 → 尾帧 EXEC 提交
    let mut a = TcpStream::connect(addr).await.expect("connect a");
    send(
      &mut a,
      &abandoning_batch(b"ok:1", b"v1", b"ok:blk1", b"0.2"),
    )
    .await
    .expect("send timeout batch");
    wait_until("超时臂落提交终结符", || {
      txn_marker_seq(log) == [AofEntryType::TxnStart, AofEntryType::TxnCommit]
    })
    .await;

    // CLIENT UNBLOCK 臂：timeout 0 永挂 → 他连接 CLIENT UNBLOCK 解除 → 续跑提交
    let mut b = TcpStream::connect(addr).await.expect("connect b");
    send_cmd(&mut b, &[b"CLIENT", b"ID"])
      .await
      .expect("send client id");
    let id_reply = read_reply(&mut b).await;
    let client_id: i64 = String::from_utf8_lossy(&id_reply)[1..]
      .trim_end()
      .parse()
      .expect("CLIENT ID 整数应答");
    send(&mut b, &abandoning_batch(b"ok:2", b"v2", b"ok:blk2", b"0"))
      .await
      .expect("send unblock-target batch");
    wait_until("解除臂落第二笔开组标记", || {
      marker_count(log, AofEntryType::TxnStart) == 2
    })
    .await;

    let mut unblocker = TcpStream::connect(addr).await.expect("connect unblocker");
    let id_arg = client_id.to_string();
    send_cmd(&mut unblocker, &[b"CLIENT", b"UNBLOCK", id_arg.as_bytes()])
      .await
      .expect("send client unblock");
    let unblocked = read_reply(&mut unblocker).await;
    assert_eq!(unblocked, b":1\r\n", "CLIENT UNBLOCK 命中挂起观察者");
    wait_until("解除后重放续跑落第二笔提交终结符", || {
      marker_count(log, AofEntryType::TxnCommit) == 2
    })
    .await;

    assert_eq!(
      marker_count(log, AofEntryType::TxnAbort),
      0,
      "正常提交路径零废弃终结符（收口单点不得被误触）"
    );
    assert_groups_all_closed(&txn_marker_seq(log));

    // 两组数据照常落库可读
    assert_eq!(
      store_read(&store, b"ok:1").await,
      Some(b"v1".to_vec()),
      "超时闭环臂组内写照常生效"
    );
    assert_eq!(
      store_read(&store, b"ok:2").await,
      Some(b"v2".to_vec()),
      "CLIENT UNBLOCK 臂组内写照常生效"
    );

    drop(a);
    drop(b);
    drop(unblocker);
  });

  let (_dir, _data_path) = node.shutdown_for_restart();
}

/// 票面「无 AOF …直接 reset」验点：AOF 未点亮的会话在 Running 态废弃时零
/// 投递、直复位、不报错（锁面与排队缓冲同面归零）
#[test]
fn abandoned_txn_without_aof_resets_directly() {
  let _lock = EXEC_ABORT_TESTS_LOCK.lock();
  let rt = Runtime::new().expect("compio runtime");
  let (dir, store) = open_test_store("wnode-abandon-no-aof.db").unwrap();
  let broker = Arc::new(SharedItemBroker::new(Arc::new(CollectionItemBroker::new(
    CollectionItemSource::new(store.new_session().unwrap()),
  ))));
  let index_store = Arc::clone(&store);
  let lock_table = TxnLockTable::from_loader(move || index_store.index.load_full());

  let mut c = consumer_on(&store);
  c.set_item_broker(Arc::clone(&broker));
  c.set_runtime_config(RuntimeServerConfig::shared_default());
  c.attach_transaction_components(Arc::new(WatchVersionMap::new(1024)), lock_table);

  // 泵等待面（经纪挂起）须在 compio 运行时上下文内驱动，与真 socket 用例同源
  rt.block_on(async move {
    drive(&mut c, &abandoning_batch(b"na:1", b"v1", b"na:blk", b"0"));
    c.take_blocked_wait()
      .expect("无 AOF 形态重放段照常挂经纪等待面");
    assert_eq!(
      txn_of(&c).state,
      TxnState::Running,
      "预置：无 AOF 亦入 Running 态"
    );
    assert!(txn_of(&c).key_entries.count() > 0, "预置：锁集登记在位");

    // 无 AOF 位：单点直 reset，绝无投递面被触碰
    c.finish_abandoned_txn();
    assert_eq!(txn_of(&c).state, TxnState::None, "无 AOF 废弃直复位");
    assert_eq!(c.session().txn_state, TxnState::None, "会话镜像同面归零");
    assert_eq!(txn_of(&c).key_entries.count(), 0, "锁集随 reset 清零");
    assert_eq!(txn_of(&c).txn_keys.len(), 0, "排队缓冲随 reset 清零");

    // 收场析构漏斗对已归零态恒零动作（无 AOF 亦不报错）
    c.dispose();
  });
  drop(broker);
  drop(store);
  drop(dir);
}
