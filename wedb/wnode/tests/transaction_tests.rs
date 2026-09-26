use std::{
  future::Future,
  str::from_utf8,
  sync::{
    Arc,
    atomic::{AtomicBool, AtomicU64, Ordering},
    mpsc::{RecvTimeoutError, channel},
  },
  thread,
  time::Duration,
};

use compio::{
  runtime::{Runtime, spawn},
  time::sleep,
};
use tempfile::tempdir;
use wacl::{AccessControlList, AclPassword, GarnetAclAuthenticator, User};
use wbase::{align::DEFAULT_SECTOR_SIZE, store_type::StoreType};
use wdev::SegmentedDevice;
use wkv::{GcConfig, GcManager, StoreConfig, WedbStore, store::grow_index_blocking};
use wnode::{
  MessageConsumerFace, SessionProviderFace, WireFormat,
  resp::{
    acl_store::AclStore,
    garnet_api::StoreGarnetApi,
    resp_server_session::{RespServerSession, RespServerSessionOptions},
    resp_session_consumer::RespSessionConsumer,
    txn_resp_commands::TxnRespCommandsExt,
  },
  service::StorageSessionProvider,
};
use wresp::{argslice::ArgSlice, catalog::try_get_resp_command_info_by_cmd, command::RespCommand};
use wtest_base::{resp_frame_str, test_store_config};
use wtxn::{
  TransactionManager, TransactionStoreTypes, TxnCommandKeys, TxnKeyEntryComparison, TxnKeySpec,
  TxnLockTable, TxnQueuedCommandInfo, TxnState, WatchVersionMap,
};
use wval::SessionPrefixBuf;

/// 生产装配的会话装饰钩子（对标 [`StorageSessionProvider::open_with_config`]
/// 单机形态：仅构造消费者，全部存储组件由宿主注入）
fn make_consumer(
  sender_id: u64,
  api: StoreGarnetApi<SegmentedDevice>,
) -> Option<RespSessionConsumer> {
  Some(RespSessionConsumer::new(
    sender_id,
    RespServerSessionOptions::default(),
    Arc::new(api),
  ))
}

fn manager() -> TransactionManager {
  TransactionManager::new(
    TxnLockTable::new(),
    Arc::new(WatchVersionMap::new(64)),
    None,
  )
}

fn session_with_args(args: &[&[u8]]) -> (RespServerSession, Vec<u8>) {
  // 槽位记宿主缓冲区间（offset 形态），宿主缓冲与生产路径一致写入接收缓冲
  let mut session = RespServerSession::default();
  let mut slices = Vec::with_capacity(args.len());
  for arg in args {
    slices.push(ArgSlice::new(session.recv_buffer.len(), arg.len()));
    session.recv_buffer.extend_from_slice(arg);
  }
  session.parse_state.initialize(slices.len());
  session.parse_state.root_buffer[..slices.len()].copy_from_slice(&slices);
  let buffer = session.recv_buffer.clone();
  (session, buffer)
}

/// test/standalone/Garnet.test.scripting/TransactionTests.cs:TxnSetTest
#[test]
fn txn_set_test() {
  let mut txn = manager();
  let mut session = RespServerSession::default();

  // MULTI
  assert!(txn.network_multi(&mut session));
  assert_eq!(session.output, b"+OK\r\n");
  assert_eq!(txn.state, TxnState::Started);
  session.output.clear();

  // QUEUED SET mykey1 val1
  let (mut sess_k1, _b1) = session_with_args(&[b"mykey1", b"abcdefg1"]);
  let set_info = TxnQueuedCommandInfo {
    name: "set",
    arity: 3,
    allowed_in_txn: true,
    is_sub_command: false,
    keys: Some(TxnCommandKeys {
      store_type: StoreType::Main,
      key_specs: vec![TxnKeySpec::new(0, 0, 1, false)],
    }),
  };
  assert!(txn.network_skip(&mut sess_k1, RespCommand::Set, Some(&set_info)));
  assert_eq!(sess_k1.output, b"+QUEUED\r\n");
  assert_eq!(txn.operation_cnt_txn, 1);

  // QUEUED SET mykey2 val2
  let (mut sess_k2, _b2) = session_with_args(&[b"mykey2", b"abcdefg2"]);
  assert!(txn.network_skip(&mut sess_k2, RespCommand::Set, Some(&set_info)));
  assert_eq!(sess_k2.output, b"+QUEUED\r\n");
  assert_eq!(txn.operation_cnt_txn, 2);

  // EXEC (1st call starts running and outputs *2)
  assert!(txn.network_exec(&mut session));
  assert_eq!(session.output, b"*2\r\n");
  assert_eq!(txn.state, TxnState::Running);

  // EXEC (2nd call at end of replay commits)
  assert!(txn.network_exec(&mut session));
  assert_eq!(txn.state, TxnState::None);
}

/// test/standalone/Garnet.test.scripting/TransactionTests.cs:TxnExecuteTest
#[test]
fn txn_execute_test() {
  let mut txn = manager();
  let mut session = RespServerSession::default();

  assert!(txn.network_multi(&mut session));
  assert_eq!(session.output, b"+OK\r\n");
  session.output.clear();

  let (mut s1, _b1) = session_with_args(&[b"mykey1", b"abcdefg1"]);
  let set_info = TxnQueuedCommandInfo {
    name: "set",
    arity: 3,
    allowed_in_txn: true,
    is_sub_command: false,
    keys: Some(TxnCommandKeys {
      store_type: StoreType::Main,
      key_specs: vec![TxnKeySpec::new(0, 0, 1, false)],
    }),
  };
  assert!(txn.network_skip(&mut s1, RespCommand::Set, Some(&set_info)));
  assert_eq!(s1.output, b"+QUEUED\r\n");

  assert!(txn.network_exec(&mut session));
  assert_eq!(txn.state, TxnState::Running);
  assert!(txn.network_exec(&mut session));
  assert_eq!(txn.state, TxnState::None);
}

/// test/standalone/Garnet.test.scripting/TransactionTests.cs:TxnGetTest
#[test]
fn txn_get_test() {
  let mut txn = manager();
  let mut session = RespServerSession::default();

  assert!(txn.network_multi(&mut session));
  session.output.clear();

  let get_info = TxnQueuedCommandInfo {
    name: "get",
    arity: 2,
    allowed_in_txn: true,
    is_sub_command: false,
    keys: Some(TxnCommandKeys {
      store_type: StoreType::Main,
      key_specs: vec![TxnKeySpec::new(0, 0, 1, true)],
    }),
  };

  let (mut s1, _b1) = session_with_args(&[b"mykey1"]);
  assert!(txn.network_skip(&mut s1, RespCommand::Get, Some(&get_info)));
  assert_eq!(s1.output, b"+QUEUED\r\n");

  let (mut s2, _b2) = session_with_args(&[b"mykey2"]);
  assert!(txn.network_skip(&mut s2, RespCommand::Get, Some(&get_info)));
  assert_eq!(s2.output, b"+QUEUED\r\n");

  assert!(txn.network_exec(&mut session));
  assert_eq!(txn.state, TxnState::Running);
  assert!(txn.network_exec(&mut session));
  assert_eq!(txn.state, TxnState::None);
}

/// test/standalone/Garnet.test.scripting/TransactionTests.cs:TxnGetSetTest
#[test]
fn txn_get_set_test() {
  let mut txn = manager();
  let mut session = RespServerSession::default();

  assert!(txn.network_multi(&mut session));
  session.output.clear();

  let get_info = TxnQueuedCommandInfo {
    name: "get",
    arity: 2,
    allowed_in_txn: true,
    is_sub_command: false,
    keys: Some(TxnCommandKeys {
      store_type: StoreType::Main,
      key_specs: vec![TxnKeySpec::new(0, 0, 1, true)],
    }),
  };
  let (mut s1, _b1) = session_with_args(&[b"mykey1"]);
  assert!(txn.network_skip(&mut s1, RespCommand::Get, Some(&get_info)));
  assert_eq!(s1.output, b"+QUEUED\r\n");

  let set_info = TxnQueuedCommandInfo {
    name: "set",
    arity: 3,
    allowed_in_txn: true,
    is_sub_command: false,
    keys: Some(TxnCommandKeys {
      store_type: StoreType::Main,
      key_specs: vec![TxnKeySpec::new(0, 0, 1, false)],
    }),
  };
  let (mut s2, _b2) = session_with_args(&[b"mykey2", b"abcdefg2"]);
  assert!(txn.network_skip(&mut s2, RespCommand::Set, Some(&set_info)));
  assert_eq!(s2.output, b"+QUEUED\r\n");

  assert!(txn.network_exec(&mut session));
  assert_eq!(txn.state, TxnState::Running);
  assert!(txn.network_exec(&mut session));
  assert_eq!(txn.state, TxnState::None);
}

/// test/standalone/Garnet.test.scripting/TransactionTests.cs:SimpleWatchTest
#[test]
fn simple_watch_test() {
  let map = Arc::new(WatchVersionMap::new(64));
  let mut txn = TransactionManager::new(TxnLockTable::new(), Arc::clone(&map), None);
  let mut session = RespServerSession::default();
  // 无存储执行域的缺省会话归属根域（与 RespServerSession::session_prefix 同源）
  let root = SessionPrefixBuf::ROOT;

  // WATCH key1
  txn.watch(root.as_slice(), b"key1");
  assert!(txn.watch_container.validate_watch_version());

  // MULTI
  assert!(txn.network_multi(&mut session));
  assert_eq!(session.output, b"+OK\r\n");
  session.output.clear();

  // Concurrent modification to key1（同根域写推进版本槽）
  map.increment_version(TxnKeyEntryComparison::scoped_key_hash(root.as_slice(), b"key1") as u64);

  // EXEC should abort
  assert!(txn.network_exec(&mut session));
  assert_eq!(session.output, b"*-1\r\n");
  assert_eq!(txn.state, TxnState::None);

  // Next transaction should commit
  session.output.clear();
  assert!(txn.network_multi(&mut session));
  assert_eq!(session.output, b"+OK\r\n");
  session.output.clear();

  assert!(txn.network_exec(&mut session));
  assert_eq!(txn.state, TxnState::Running);
  assert!(txn.network_exec(&mut session));
  assert_eq!(txn.state, TxnState::None);
}

/// test/standalone/Garnet.test.scripting/TransactionTests.cs:WatchNonExistentKey
#[test]
fn watch_non_existent_key() {
  let map = Arc::new(WatchVersionMap::new(64));
  let mut txn = TransactionManager::new(TxnLockTable::new(), Arc::clone(&map), None);
  let mut session = RespServerSession::default();
  let root = SessionPrefixBuf::ROOT;

  // WATCH key1 (non-existent, version = 0)
  txn.watch(root.as_slice(), b"key1");

  // MULTI
  assert!(txn.network_multi(&mut session));
  session.output.clear();

  // key1 is created/modified concurrently -> version becomes 1
  map.increment_version(TxnKeyEntryComparison::scoped_key_hash(root.as_slice(), b"key1") as u64);

  // EXEC should abort
  assert!(txn.network_exec(&mut session));
  assert_eq!(session.output, b"*-1\r\n");
  assert_eq!(txn.state, TxnState::None);
}

/// test/standalone/Garnet.test.scripting/TransactionTests.cs:TxnCommandCoverage
#[test]
fn txn_command_coverage() {
  let mut txn = manager();
  assert_eq!(txn.state, TxnState::None);
  txn.state = TxnState::Started;
  assert_eq!(txn.state, TxnState::Started);
  txn.state = TxnState::Aborted;
  assert_eq!(txn.state, TxnState::Aborted);
  txn.state = TxnState::Running;
  assert_eq!(txn.state, TxnState::Running);

  // Store type mapping
  let mut txn = manager();
  txn.add_transaction_store_type(StoreType::Main);
  txn.add_transaction_store_type(StoreType::Object);
  assert!(txn.store_types.contains(TransactionStoreTypes::Main));
  assert!(txn.store_types.contains(TransactionStoreTypes::Object));
  assert!(!txn.store_types.contains(TransactionStoreTypes::Unified));
  txn.add_transaction_store_type(StoreType::All);
  assert!(txn.store_types.contains(TransactionStoreTypes::Unified));
}

// ==================== 事务 × 在线扩容全事务屏障并发回归 ====================
//
// 对标 task/ing/wtxn-lock-all-keys-bypasses-prepare-grow-barrier-lock-desync.md：
// 事务加锁面（`TxnKeyEntries` 钉定的索引版本）与读写面（会话 `pin()` 现取的
// 活跃索引）在 `grow_index` 切表时必须严丝合缝——否则事务把桶闩落在旧表桶上、
// 读写打进新表同哈希桶，等于对同一键集无锁并发，丢写、跨版本侵入、撕裂分裂
// 三者并发显现。以下两用例全为真存储驱动（`SegmentedDevice` 单文件 + 生产
// `StorageSessionProvider` 会话链 + 生产 `build_txn_lock_table` 屏障装配），
// 无任何假 mock。

/// 并发事务 worker 数（同落 compio 单 worker 运行时，会话级并发即协程级交错）
const WORKERS: u64 = 6;
/// 场景内 `grow_index` 目标翻倍次数
const GROW_TARGET: usize = 4;
/// 每次翻倍前要求的累计已提交事务轮数（定序：屏障必须在事务流量中排空，
/// 杜绝「先扩完再跑事务」的伪并发）
const TRAFFIC_PER_GROW: u64 = 1200;
/// 全部翻倍收敛后仍要求的累计事务轮数（Rest 态与分裂尾段的并发正确性）
const POST_GROW_TRAFFIC: u64 = 5000;
/// 单 worker 最少提交轮数（低于此值即并发压力不成立，判 RED）
const MIN_WORKER_ROUNDS: u64 = 150;
/// 单 worker 轮数硬上限（防扩容异常时 worker 永不出循环；触顶即置 capped）
const MAX_WORKER_ROUNDS: u64 = 6000;
/// 扩容入口 CAS 让步重试上限（本用例无并发扩容者与检查点，长期失败即异常）
const GROW_CAS_SPIN_LIMIT: u32 = 2_000_000;
/// 一轮事务的排队命令数（私有键对 INCR/GET + 共享键对 INCR/GET）
const QUEUED_PER_ROUND: usize = 8;
/// 一轮事务 EXEC 数组元素数（与排队命令同序）
const EXEC_ELEMS_PER_ROUND: usize = QUEUED_PER_ROUND;

/// 一轮事务的命令帧（序固定，便于逐位断言）
///
/// 私有键对（own_a/own_b）只被本 worker 的事务触碰：四个读数必严格相等且必
/// 等于「本 worker 已提交轮数 + 1」，任何偏差即该事务加锁面与读写面脱节；
/// 共享键对（shared_a/shared_b）被全部 worker 的事务同锁，构造真实桶闩争用与
/// EXEC 慢臂重驱（`ExecRun::Contended` 的键集/钉锁原样保留路径）。
fn txn_round_frames(own_a: &str, own_b: &str, shared_a: &str, shared_b: &str) -> Vec<Vec<u8>> {
  vec![
    resp_frame_str(&["MULTI"]),
    resp_frame_str(&["INCR", own_a]),
    resp_frame_str(&["INCR", own_b]),
    resp_frame_str(&["GET", own_a]),
    resp_frame_str(&["GET", own_b]),
    resp_frame_str(&["INCR", shared_a]),
    resp_frame_str(&["INCR", shared_b]),
    resp_frame_str(&["GET", shared_a]),
    resp_frame_str(&["GET", shared_b]),
    resp_frame_str(&["EXEC"]),
  ]
}

/// 取一行（含剥除行尾 CR），越界即判框架违规
fn next_line<'a>(payload: &'a [u8], cursor: &mut usize) -> &'a [u8] {
  let rest = &payload[*cursor..];
  let end = rest
    .iter()
    .position(|b| *b == b'\n')
    .unwrap_or_else(|| panic!("应答体残缺：{:?}", String::from_utf8_lossy(payload)));
  *cursor += end + 1;
  let line = &rest[..end];
  if line.ends_with(b"\r") {
    &line[..line.len() - 1]
  } else {
    line
  }
}

/// 顺序抽取应答中的值文本：`+`/`:` 取行内值，`$bulk` 取次行值，`*` 数组头跳行
///
/// 出现 `-错误` 帧或 nil bulk 即刻断言失败——本用例应答面不含任何合法错误，
/// nil 即「键在读面缺席」（写落旧表、读打新表）的直接证据。
fn resp_value_texts(payload: &[u8]) -> Vec<Vec<u8>> {
  let mut cursor = 0usize;
  let mut values = Vec::new();
  while cursor < payload.len() {
    let line = next_line(payload, &mut cursor);
    if line.is_empty() {
      continue;
    }
    match line[0] {
      b'*' => {}
      b'+' | b':' => values.push(line[1..].to_vec()),
      b'$' => {
        let len: i64 = from_utf8(&line[1..])
          .expect("bulk 长度为 ASCII")
          .parse()
          .expect("bulk 长度为整数");
        assert!(
          len >= 0,
          "应答出现 nil bulk：{:?}",
          String::from_utf8_lossy(payload)
        );
        values.push(next_line(payload, &mut cursor).to_vec());
      }
      c => panic!(
        "应答出现非预期帧头 {:?}：{:?}",
        c as char,
        String::from_utf8_lossy(payload)
      ),
    }
  }
  values
}

/// 值文本 → u64（本用例全部值为无符号十进制整数）
fn as_u64(text: &[u8]) -> u64 {
  from_utf8(text)
    .expect("值为 ASCII")
    .parse()
    .expect("值为无符号整数")
}

/// 泵驱单会话至全部帧应答完成（消费 → EXEC 取锁/屏障争用慢臂让步 → 重驱）
async fn pump(consumer: &mut RespSessionConsumer, frames: &[Vec<u8>]) -> Vec<u8> {
  {
    let mut scratch = consumer.take_recv_scratch();
    for frame in frames {
      scratch.extend_from_slice(frame);
    }
    consumer.return_recv_scratch(scratch);
  }
  let mut out = Vec::new();
  let mut redrives = 0u32;
  loop {
    assert!(
      consumer.try_consume_messages_into(&mut out).is_some(),
      "命令帧不应触发协议违规"
    );
    assert!(
      consumer.take_blocked_wait().is_none(),
      "本用例不发起阻塞命令，不应出现挂起等待"
    );
    let Some(slow) = consumer.take_slow_wait() else {
      break;
    };
    // 空应答（取锁争用让步体）不写线；resolve 内部 yield_now 让出执行器
    out.extend_from_slice(&slow.resolve().await);
    redrives += 1;
    assert!(redrives < 10_000_000, "EXEC 屏障/取锁争用重驱不收敛");
  }
  out
}

/// 场景回传体（终态判据一律回到主线程断言，杜绝后台线程静默吞断言）
struct GrowBarrierVerdict {
  /// 每 worker 提交成功的事务轮数
  worker_rounds: Vec<u64>,
  /// 每 worker 非事务 INCR 成功数
  worker_plain: Vec<u64>,
  /// `grow_index` 成功翻倍次数
  grown: usize,
  initial_index_size: usize,
  final_index_size: usize,
  /// 每 worker 私有键对终值（读自存储引擎）
  final_own: Vec<(u64, u64)>,
  final_shared: (u64, u64),
  final_plain: Vec<u64>,
  active_txns_after: usize,
  growing_after: bool,
}

/// 单 worker 协程出参
struct WorkerOut {
  rounds: u64,
  plain_ok: u64,
  own: (String, String),
  plain: String,
}

/// 在独立 OS 线程上跑「生产会话链并发 MULTI/EXEC × 循环 grow_index」全场景
///
/// 事务侧与扩容侧真并发：事务 worker 群跑在单线程 compio 运行时（会话级交错与
/// EXEC 争用慢臂重驱），扩容驱动跑在独立 std 线程（与生产
/// [`wkv::store::grow_index_blocking`] 的 spawn_blocking 同形态，绝不上 reactor）。
fn run_grow_barrier_scenario() -> GrowBarrierVerdict {
  let dir = tempdir().expect("临时数据目录");
  let mut config = test_store_config();
  // 参与者容量须覆盖「每连接会话 + 经纪/回收/INFO 槽 + 扩容驱动」全占位
  config.max_sessions = config.max_sessions.max(64);
  let provider =
    StorageSessionProvider::open_with_config(config, dir.path().join("node.db"), make_consumer)
      .expect("生产会话链装配（StorageSessionProvider）");
  let store = provider.store();
  let initial_index_size = store.active_index().size;

  let traffic = Arc::new(AtomicU64::new(0));
  let grow_finished = Arc::new(AtomicBool::new(false));
  let capped = Arc::new(AtomicBool::new(false));

  // 扩容驱动线程：按已在途事务轮数定序触发每一次翻倍
  let grow_store = Arc::clone(&store);
  let grow_traffic = Arc::clone(&traffic);
  let grow_capped = Arc::clone(&capped);
  let grow_done_flag = Arc::clone(&grow_finished);
  let grower = thread::spawn(move || {
    let mut grown = 0usize;
    for step in 1..=GROW_TARGET {
      let want = (step as u64 - 1) * TRAFFIC_PER_GROW;
      while grow_traffic.load(Ordering::SeqCst) < want && !grow_capped.load(Ordering::SeqCst) {
        thread::yield_now();
      }
      let mut spins = 0u32;
      loop {
        match grow_store.grow_index() {
          Ok(true) => {
            grown += 1;
            break;
          }
          Ok(false) => {
            spins += 1;
            assert!(
              spins < GROW_CAS_SPIN_LIMIT,
              "扩容入口 CAS 长期失败：相位被非预期持有者占用"
            );
            thread::yield_now();
          }
          Err(e) => panic!("在线扩容失败: {e}"),
        }
      }
    }
    while grow_traffic.load(Ordering::SeqCst) < POST_GROW_TRAFFIC
      && !grow_capped.load(Ordering::SeqCst)
    {
      thread::yield_now();
    }
    grow_done_flag.store(true, Ordering::SeqCst);
    grown
  });

  let shared_a = "grow-barrier-shared-a".to_string();
  let shared_b = "grow-barrier-shared-b".to_string();
  let runtime = Runtime::new().expect("compio 单 worker 运行时");
  let outputs = runtime.block_on(async {
    let mut handles = Vec::new();
    for id in 0..WORKERS {
      let mut consumer = provider
        .get_session(WireFormat::Ascii, 100 + id)
        .expect("生产会话创建");
      let own_a = format!("grow-barrier-own-a-{id}");
      let own_b = format!("grow-barrier-own-b-{id}");
      let plain = format!("grow-barrier-plain-{id}");
      let frames = txn_round_frames(&own_a, &own_b, &shared_a, &shared_b);
      let plain_frame = vec![resp_frame_str(&["INCR", &plain])];
      let traffic = Arc::clone(&traffic);
      let grow_finished = Arc::clone(&grow_finished);
      let capped = Arc::clone(&capped);
      handles.push(spawn(async move {
        let mut rounds = 0u64;
        let mut plain_ok = 0u64;
        let mut last_shared = 0u64;
        loop {
          if rounds >= MIN_WORKER_ROUNDS && grow_finished.load(Ordering::SeqCst) {
            break;
          }
          if rounds >= MAX_WORKER_ROUNDS {
            capped.store(true, Ordering::SeqCst);
            break;
          }
          // 事务轮：加锁面与读写面必须同版本，读数逐位精确可推
          let reply = pump(&mut consumer, &frames).await;
          let values = resp_value_texts(&reply);
          assert_eq!(
            values.len(),
            1 + QUEUED_PER_ROUND + EXEC_ELEMS_PER_ROUND,
            "worker {id} 事务应答元素数不符（错误应答或应答丢失）：{:?}",
            String::from_utf8_lossy(&reply)
          );
          assert_eq!(values[0], b"OK", "MULTI 应答异常");
          for queued in &values[1..=QUEUED_PER_ROUND] {
            assert_eq!(queued, b"QUEUED", "入队应答异常");
          }
          let expected_own = rounds + 1;
          for slot in 0..4usize {
            let got = as_u64(&values[1 + QUEUED_PER_ROUND + slot]);
            assert_eq!(
              got, expected_own,
              "worker {id} 第 {rounds} 轮私有键读数偏离（加锁版本与读写版本脱节 = 跨版本丢写/侵入）"
            );
          }
          let shared: Vec<u64> = values[1 + QUEUED_PER_ROUND + 4..]
            .iter()
            .map(|v| as_u64(v))
            .collect();
          assert!(
            shared.iter().all(|v| *v == shared[0]),
            "共享键对四个读数不再互等 = 事务原子性被并发破坏：{shared:?}"
          );
          assert!(
            shared[0] >= last_shared,
            "共享键读数倒退（旧表回读）：{last_shared} → {}",
            shared[0]
          );
          last_shared = shared[0];
          rounds += 1;
          traffic.fetch_add(1, Ordering::SeqCst);

          // 非事务轮：普通 INCR 亦经全事务屏障持纪元守卫，与排空窗口正面并发
          let plain_reply = pump(&mut consumer, &plain_frame).await;
          let plain_values = resp_value_texts(&plain_reply);
          assert_eq!(
            plain_values.len(),
            1,
            "非事务 INCR 应答异常：{:?}",
            String::from_utf8_lossy(&plain_reply)
          );
          assert_eq!(
            as_u64(&plain_values[0]),
            plain_ok + 1,
            "worker {id} 非事务 INCR 丢写（扩容切表期读写面错缝）"
          );
          plain_ok += 1;
        }
        WorkerOut {
          rounds,
          plain_ok,
          own: (own_a, own_b),
          plain,
        }
      }));
    }
    let mut outs = Vec::new();
    for handle in handles {
      outs.push(handle.await.expect("事务 worker 协程完成"));
    }
    outs
  });

  // 扩容驱动收敛后再取终值（此后全系统无在途写）
  let grown = grower.join().expect("扩容驱动线程完成");

  let worker_keys: Vec<(String, String, String)> = outputs
    .iter()
    .map(|o| (o.own.0.clone(), o.own.1.clone(), o.plain.clone()))
    .collect();
  let finals = runtime.block_on(async {
    let mut consumer = provider
      .get_session(WireFormat::Ascii, 900)
      .expect("校验会话创建");
    let mut frames = Vec::new();
    for (own_a, own_b, plain) in &worker_keys {
      frames.push(resp_frame_str(&["GET", own_a]));
      frames.push(resp_frame_str(&["GET", own_b]));
      frames.push(resp_frame_str(&["GET", plain]));
    }
    frames.push(resp_frame_str(&["GET", &shared_a]));
    frames.push(resp_frame_str(&["GET", &shared_b]));
    let reply = pump(&mut consumer, &frames).await;
    let texts = resp_value_texts(&reply);
    // 显式释放校验会话，令「活跃事务计数终值必为 0」可判
    drop(consumer);
    texts.into_iter().map(|t| as_u64(&t)).collect::<Vec<u64>>()
  });

  let active_txns_after = store.resize.num_active_txns.load(Ordering::SeqCst);
  let growing_after = store.is_growing();
  let final_index_size = store.active_index().size;
  drop(provider);

  let mut final_own = Vec::new();
  let mut final_plain = Vec::new();
  for i in 0..WORKERS as usize {
    let base = i * 3;
    final_own.push((finals[base], finals[base + 1]));
    final_plain.push(finals[base + 2]);
    assert_eq!(
      finals[base],
      finals[base + 1],
      "worker {i} 私有键对终值不等（一孔写落旧表、另一孔写落新表）"
    );
  }
  let shared_base = WORKERS as usize * 3;

  GrowBarrierVerdict {
    worker_rounds: outputs.iter().map(|o| o.rounds).collect(),
    worker_plain: outputs.iter().map(|o| o.plain_ok).collect(),
    grown,
    initial_index_size,
    final_index_size,
    final_own,
    final_shared: (finals[shared_base], finals[shared_base + 1]),
    final_plain,
    active_txns_after,
    growing_after,
  }
}

#[test]
fn multi_exec_interleaved_with_grow_index_keeps_lock_and_data_consistent() {
  let (verdict_tx, verdict_rx) = channel::<GrowBarrierVerdict>();
  // 后台线程承载全场景；主线程有界等待，将屏障死锁/重驱不收敛转可判别 RED
  // （背景线程与其上运行时随测试进程 main 返回被强杀，与同目录既有并发用例同源）
  thread::spawn(move || {
    let verdict = run_grow_barrier_scenario();
    let _ = verdict_tx.send(verdict);
  });

  let verdict = match verdict_rx.recv_timeout(Duration::from_secs(180)) {
    Ok(v) => v,
    Err(RecvTimeoutError::Timeout) => {
      panic!("事务 × 扩容场景 180s 未收敛：PREPARE_GROW 排空与事务重驱必须互进，挂死即屏障死锁")
    }
    Err(RecvTimeoutError::Disconnected) => {
      panic!("场景线程断言失败（ panic 详情见上方捕获输出）")
    }
  };

  assert_eq!(
    verdict.grown, GROW_TARGET,
    "扩容驱动须完成 {GROW_TARGET} 次翻倍（少于目标即在并发窗口内被拒或提前收敛）"
  );
  assert_eq!(
    verdict.final_index_size,
    verdict.initial_index_size * (1 << verdict.grown),
    "索引容量未按翻倍收敛"
  );
  assert!(
    verdict
      .worker_rounds
      .iter()
      .all(|rounds| *rounds >= MIN_WORKER_ROUNDS),
    "存在轮数不足 MIN_WORKER_ROUNDS 的 worker，并发压力不成立：{:?}",
    verdict.worker_rounds
  );
  let total_rounds: u64 = verdict.worker_rounds.iter().sum();
  let total_plain: u64 = verdict.worker_plain.iter().sum();
  for (i, rounds) in verdict.worker_rounds.iter().enumerate() {
    assert_eq!(
      verdict.final_own[i],
      (*rounds, *rounds),
      "worker {i} 私有键对终值 ≠ 其已提交事务轮数（跨版本丢写/重复应用）"
    );
    assert_eq!(
      verdict.final_plain[i], verdict.worker_plain[i],
      "worker {i} 非事务 INCR 终值 ≠ 成功次数（扩容期普通操作丢写）"
    );
  }
  assert_eq!(
    verdict.final_shared,
    (total_rounds, total_rounds),
    "共享键对终值 ≠ 全局事务轮数：事务原子写被并发扩容破坏"
  );
  assert!(
    total_rounds >= WORKERS * MIN_WORKER_ROUNDS && total_plain >= WORKERS * MIN_WORKER_ROUNDS,
    "有效交错量不足：rounds={total_rounds} plain={total_plain}"
  );
  assert!(
    !verdict.growing_after,
    "场景收敛后扩容态未回 REST（状态机泄漏，后续扩容/检查点恒被拒）"
  );
  assert_eq!(
    verdict.active_txns_after, 0,
    "活跃事务计数未归零：递减出口缺失（泄漏）或计数下溢（usize::MAX）"
  );
}

/// 大表分裂完整性回归：跨巨大地址跨度铺开的记录在 `grow_index` 分块迁移后仍逐条可达
#[test]
fn grow_index_split_keeps_spread_records_reachable() -> aok::Result<()> {
  const RECORDS: u64 = 4000;
  const CHUNK: usize = 256;

  let dir = tempdir()?;
  // 极小索引（64 主桶 / 448 数据槽）对 RECORDS 条记录严重超订：记录沿极长碰撞
  // 链铺开，单个主桶背后的物理地址跨度达数十 KB 级
  let mut config = StoreConfig::new(64, DEFAULT_SECTOR_SIZE, 16, 0.5)?;
  config.gc.enabled = false;
  let provider =
    StorageSessionProvider::open_with_config(config, dir.path().join("split.db"), make_consumer)?;
  let store = provider.store();
  let initial_index_size = store.active_index().size;
  let pad = "7".repeat(200);
  let keys: Vec<String> = (0..RECORDS).map(|i| format!("spread-{i:05}")).collect();
  let expected: Vec<String> = keys.iter().map(|k| format!("{k}-{pad}")).collect();

  let runtime = Runtime::new()?;
  runtime.block_on(async {
    let mut writer = provider
      .get_session(WireFormat::Ascii, 1)
      .expect("写面会话创建");
    for (chunk_keys, chunk_values) in keys.chunks(CHUNK).zip(expected.chunks(CHUNK)) {
      let frames: Vec<Vec<u8>> = chunk_keys
        .iter()
        .zip(chunk_values)
        .map(|(k, v)| resp_frame_str(&["SET", k, v]))
        .collect();
      let reply = pump(&mut writer, &frames).await;
      let texts = resp_value_texts(&reply);
      assert_eq!(texts.len(), chunk_keys.len(), "SET 应答数不符");
      assert!(
        texts.iter().all(|t| t == b"OK"),
        "SET 出现非 OK 应答：{:?}",
        String::from_utf8_lossy(&reply)
      );
    }
    drop(writer);

    // 记录确已跨巨大地址跨度铺开（非单桶局部样本）：每桶平均跨度 ≥ 2 页，
    // 总跨度 ≥ 256KB——不合作分裂的实现在此规模下必丢条目
    let tail = store.hlog.addresses.tail_address.load(Ordering::Acquire);
    let begin = store.hlog.addresses.begin_address.load(Ordering::Acquire);
    let span = tail - begin;
    assert!(
      span >= 256 * 1024,
      "记录地址跨度过小（{span} B），本用例大表前提不成立"
    );
    assert!(
      span / initial_index_size as u64 >= 2 * DEFAULT_SECTOR_SIZE as u64,
      "单桶平均地址跨度 < 2 页（{span} B / {initial_index_size} 桶），跨度证据不足"
    );

    for step in 1..=2usize {
      let grown = grow_index_blocking(Arc::clone(&store))
        .await
        .expect("在线扩容成功");
      assert!(grown, "第 {step} 次 grow_index 应抢到扩容权并完成");
    }
    assert_eq!(store.active_index().size, initial_index_size * 4);
    assert!(!store.is_growing(), "分裂完成后未回 REST 态");
    assert!(
      store.resize.old_index.load_full().is_none(),
      "旧表句柄未在收敛点释放"
    );

    // 迁移后读面逐条校验（读经会话分裂协同 + whlog 磁盘读）
    let mut reader = provider
      .get_session(WireFormat::Ascii, 2)
      .expect("读面会话创建");
    for (chunk_keys, chunk_values) in keys.chunks(CHUNK).zip(expected.chunks(CHUNK)) {
      let frames: Vec<Vec<u8>> = chunk_keys
        .iter()
        .map(|k| resp_frame_str(&["GET", k]))
        .collect();
      let reply = pump(&mut reader, &frames).await;
      let texts = resp_value_texts(&reply);
      for (i, text) in texts.iter().enumerate() {
        assert_eq!(
          text.as_slice(),
          chunk_values[i].as_bytes(),
          "分块迁移后键 {} 读数被破坏或缺席（索引重建前不可丢条目）",
          chunk_keys[i]
        );
      }
    }
    drop(reader);
  });
  drop(provider);
  Ok(())
}

/// 装配带 ACL 认证器的会话（对照 resp_server_session_tests:acl_session）
fn acl_session() -> RespServerSession {
  let authenticator = Arc::new(GarnetAclAuthenticator::new(Arc::new(
    AccessControlList::default(),
  )));
  let mut session = RespServerSession::default();
  session.attach_acl(Some(authenticator));
  session
}

/// ns1 命名用户 bob（口令 secret）规则体
fn ns1_bob() -> User {
  let mut bob = User::new("bob".to_string());
  bob.set_enabled(true);
  bob.add_password_hash(AclPassword::from_string("secret"));
  bob
}

/// 鉴权命名空间切换联动清退事务与监视键（wedb 自有面回归：
/// task/todo/wnode-auth-namespace-switch-watch-container-leak）
///
/// ns0 会话 WATCH key1 并置入 MULTI 排队态后换租 ns1：会话本地
/// watch_container 必须清空（旧租户监视条目以旧前缀定格 scoped_key_hash，
/// 残留即并入新租户事务锁集与版本校验）、事务状态镜像与事务管理器状态
/// 归零；此后旧租户 key1 写推进版本，新租户事务仍正常提交，不受干扰
///
/// C# 对位（garnet 真源，f1b520ea 门禁裁决记录）：C# 排队态不设 AUTH
/// 即时执行面——ProcessMessages 的未决事务白名单仅
/// EXEC/MULTI/DISCARD/QUIT（RespServerSession.cs:668-675），其余一律
/// NetworkSKIP；且 C# AUTH 元数据无 NoMulti 旗
/// （libs/resources/RespCommandsInfo.json AUTH = "Fast, Loading, NoAuth,
/// NoScript, Stale, AllowBusy"），NetworkSKIP 判 AllowedInTxn 为真、回
/// +QUEUED 延至 EXEC 执行态回放（TxnRespCommands.cs:110、199-202）。
/// 即「排队态 AUTH 即时生效回 +OK」在 C# 不存在；本仓为多租户单向强隔离
/// 给 AUTH 元数据补 NoMulti 并在入口门拦截（auth.rs
/// RESP_ERR_AUTH_IN_MULTI），排队/执行态拒 AUTH 面已由
/// auth_is_no_multi_and_rejected_inside_pending_txn 覆盖。换租清退回归改
/// 由门不覆盖、C# 与本仓目录均无 NoMulti 的合法事务内认证面——HELLO 认证
/// 臂（process_hello_command_state → apply_authenticated_handle）驱动，
/// 清退断言语义不变
/// 同步测试壳内闭环 async ACL 存储访问链（全链 async 化的测试对位）
fn block_on<F: Future>(fut: F) -> F::Output {
  Runtime::new().unwrap().block_on(fut)
}

#[test]
fn auth_namespace_switch_resets_txn_and_watch_container() {
  let map = Arc::new(WatchVersionMap::new(64));
  let mut session = acl_session();
  let (_dir, store) = wtest_base::open_test_store("txn-auth-ns-switch.db").unwrap();
  let acl_store_session = store.new_session().unwrap();
  let acl_store = AclStore::new(&acl_store_session);
  block_on(acl_store.write(1, b"bob", &ns1_bob().to_bytes())).unwrap();
  session.txn_manager = Some(TransactionManager::new(
    TxnLockTable::new(),
    Arc::clone(&map),
    None,
  ));

  // ns0 会话 WATCH key1（监视条目定格入容器）+ MULTI 排队态
  let mut txn = session.txn_manager.take().unwrap();
  let (mut s_key, _buf) = session_with_args(&[b"key1"]);
  assert!(txn.network_watch(&mut s_key));
  assert!(txn.network_multi(&mut session));
  assert_eq!(session.txn_state, TxnState::Started);
  session.txn_manager = Some(txn);
  session.output.clear();

  // HELLO AUTH 1#bob secret 换租（挂载臂直达 apply_authenticated_handle）：
  // 清退 watch_container、事务管理器与状态镜像；认证成功即返回 true
  let mut hello_out = Vec::new();
  assert!(block_on(
    session.process_hello_command_state::<SegmentedDevice>(
      None,
      b"1#bob",
      b"secret",
      None,
      Some(&acl_store),
      &mut hello_out,
    )
  ));
  assert!(!hello_out.is_empty());
  assert_eq!(session.namespace, 1);
  assert_eq!(session.txn_state, TxnState::None);
  let txn = session.txn_manager.as_ref().unwrap();
  assert_eq!(txn.state, TxnState::None);
  assert!(txn.watch_container.save_keys_to_key_list().next().is_none());
  session.output.clear();

  // 旧租户（根域）key1 写推进版本；新租户事务正常提交不受干扰（若容器
  // 残留旧监视条目，EXEC 校验命中版本变更即假性中止回 *-1）
  let root = SessionPrefixBuf::ROOT;
  map.increment_version(TxnKeyEntryComparison::scoped_key_hash(root.as_slice(), b"key1") as u64);

  let mut txn = session.txn_manager.take().unwrap();
  assert!(txn.network_multi(&mut session));
  session.output.clear();
  assert!(txn.network_exec(&mut session));
  assert_eq!(session.output, b"*0\r\n");
  assert!(txn.network_exec(&mut session));
  assert_eq!(txn.state, TxnState::None);
}

/// AUTH 元数据 NoMulti 投影剔除 + 事务进行中入口拦截（wedb 自有面：
/// C# 原型 AUTH 无 NoMulti，单租户无此门；本仓排队态 AUTH 由
/// NetworkSKIP 报错中止，事务执行态直通臂由入口门拦回错误帧）
#[test]
fn auth_is_no_multi_and_rejected_inside_pending_txn() {
  // 事务排队投影（txn_only）剔除 AUTH；常规目录仍可见
  assert!(try_get_resp_command_info_by_cmd(RespCommand::Auth, true).is_none());
  assert!(try_get_resp_command_info_by_cmd(RespCommand::Auth, false).is_some());

  let map = Arc::new(WatchVersionMap::new(64));
  let mut session = acl_session();
  let (_dir, store) = wtest_base::open_test_store("txn-auth-ns-switch-reject.db").unwrap();
  let acl_store_session = store.new_session().unwrap();
  let acl_store = AclStore::new(&acl_store_session);
  block_on(acl_store.write(1, b"bob", &ns1_bob().to_bytes())).unwrap();
  session.txn_manager = Some(TransactionManager::new(
    TxnLockTable::new(),
    Arc::clone(&map),
    None,
  ));

  // MULTI 置排队态后 AUTH：入口拦截回错误帧，不换租、不落 +OK
  let mut txn = session.txn_manager.take().unwrap();
  assert!(txn.network_multi(&mut session));
  assert_eq!(session.txn_state, TxnState::Started);
  session.txn_manager = Some(txn);
  session.output.clear();

  let args: Vec<&[u8]> = vec![b"1#bob", b"secret"];
  assert!(block_on(session.network_auth_session::<SegmentedDevice>(&args, &acl_store)).unwrap());
  assert_eq!(session.namespace, 0);
  assert_eq!(
    String::from_utf8(session.output).unwrap(),
    "-ERR AUTH inside MULTI is not allowed\r\n"
  );
}

/// 方案 3 验证点：MULTI → SET k v → HELLO 3 <冷租户用户> <口令> → EXEC
/// 断言收尾应答为完整错误帧（-EXECABORT）而非撕裂数组、事务整体未生效（k 不存在）、
fn feed_txn_session(s: &mut RespServerSession, frame: &[u8]) -> Vec<u8> {
  s.recv_buffer.extend_from_slice(frame);
  let mut resp_buf = Vec::new();
  let consumed = s.try_consume_messages();
  assert!(consumed.is_some(), "帧应被完整消费: {frame:?}");
  s.take_output_into(&mut resp_buf);
  Runtime::new()
    .unwrap()
    .block_on(wnode_test::drive_pending_parks(s, &mut resp_buf, true));
  s.output.extend_from_slice(&resp_buf);
  wnode_test::drain_output(s)
}

/// 方案 3 验证点：MULTI → SET k v → HELLO 3 <冷租户用户> <口令> → EXEC
/// 断言收尾应答为完整错误帧（-EXECABORT）而非撕裂数组、事务整体未生效（k 不存在）、
/// 会话 namespace 与协议版本保持旧值、随后裸 SELECT/GET 行为与切库前一致。
#[test]
fn hello_cold_tenant_in_txn_queue_rejected_and_txn_aborts() {
  let (_dir, store) = wtest_base::open_test_store("txn-hello-cold-abort.db").unwrap();
  let acl_store_session = store.new_session().unwrap();
  let acl_store = AclStore::new(&acl_store_session);
  // 在命名空间 100（冷租户）写入用户 bob，口令 secret
  block_on(acl_store.write(100, b"bob", &ns1_bob().to_bytes())).unwrap();

  let acl = Arc::new(AccessControlList::new("").unwrap());
  let mut s = RespServerSession::new(
    1,
    RespServerSessionOptions {
      default_user: "default".into(),
      ..RespServerSessionOptions::default()
    },
  );
  s.attach_acl(Some(Arc::new(GarnetAclAuthenticator::new(acl))));
  s.set_garnet_api(Arc::new(StoreGarnetApi::new(store.new_session().unwrap())));
  s.attach_transaction_components(Arc::new(WatchVersionMap::new(64)), TxnLockTable::new());

  // 1. MULTI
  assert_eq!(
    feed_txn_session(&mut s, b"*1\r\n$5\r\nMULTI\r\n"),
    b"+OK\r\n"
  );
  assert_eq!(s.txn_state, TxnState::Started);

  // 2. SET k v
  assert_eq!(
    feed_txn_session(&mut s, b"*3\r\n$3\r\nSET\r\n$1\r\nk\r\n$1\r\nv\r\n"),
    b"+QUEUED\r\n"
  );
  assert_eq!(s.txn_state, TxnState::Started);

  // 3. HELLO 3 AUTH 100#bob secret -> 排队期直接拦截拒绝并置 Aborted
  assert_eq!(
    feed_txn_session(
      &mut s,
      b"*5\r\n$5\r\nHELLO\r\n$1\r\n3\r\n$4\r\nAUTH\r\n$7\r\n100#bob\r\n$6\r\nsecret\r\n",
    ),
    b"-ERR HELLO is currently unsupported inside a transaction.\r\n"
  );
  assert_eq!(s.txn_state, TxnState::Aborted);
  assert_eq!(s.namespace, 0, "会话 namespace 严格保持旧值 0");
  assert_eq!(s.resp_protocol_version, 2, "协议版本严格保持旧值 2");

  // 4. EXEC -> 事务整体中止，回完整错误帧 -EXECABORT 而非撕裂数组
  assert_eq!(
    feed_txn_session(&mut s, b"*1\r\n$4\r\nEXEC\r\n"),
    b"-EXECABORT Transaction discarded because of previous errors.\r\n"
  );
  assert_eq!(s.txn_state, TxnState::None, "事务收口复位");

  // 5. 验证事务整体未生效：k 不存在
  assert_eq!(
    feed_txn_session(&mut s, b"*2\r\n$3\r\nGET\r\n$1\r\nk\r\n"),
    b"$-1\r\n",
    "SET 未生效，键不存在"
  );

  // 6. 验证随后裸 SELECT/GET 行为与切库前一致
  assert_eq!(
    feed_txn_session(&mut s, b"*2\r\n$6\r\nSELECT\r\n$1\r\n0\r\n"),
    b"+OK\r\n"
  );
  assert_eq!(
    feed_txn_session(&mut s, b"*2\r\n$3\r\nGET\r\n$1\r\nk\r\n"),
    b"$-1\r\n"
  );
}

/// 方案 1 围栏单元验证：在 Running 事务态下直接调用 process_hello_command 遇到冷租户时，
/// 必须拒绝停泊、弃置 ColdContextPending、回写错误帧且保持旧 namespace
#[compio::test]
async fn hello_cold_tenant_parking_fence_in_process_hello_command() {
  let mut cfg = test_store_config();
  cfg.gc = GcConfig {
    route_idle_evict_secs: 0,
    ..GcConfig::default()
  };
  let dir = tempdir().unwrap();
  let store = Arc::new(
    WedbStore::open(
      cfg,
      Arc::new(SegmentedDevice::single_file(dir.path().join("cold.db")).unwrap()),
    )
    .unwrap(),
  );

  // 阶段 A：非严格会话建域并持久化用户 bob，evict + GC 使租户 100 变冷
  {
    let session = store.new_session().unwrap();
    session.set_context(100, 0);
    AclStore::new(&session)
      .write(100, b"bob", &ns1_bob().to_bytes())
      .await
      .unwrap();
  }
  store.flush_and_evict_all().await.unwrap();
  sleep(Duration::from_millis(5)).await;
  GcManager::new(&store).run_once().await.unwrap();
  assert!(store.vdb.is_cold_db(100, 0), "租户 100 必须冷");

  let acl_store_session = store.new_session().unwrap();
  let acl_store = AclStore::new(&acl_store_session);

  let acl = Arc::new(AccessControlList::new("").unwrap());
  let mut s = RespServerSession::new(
    1,
    RespServerSessionOptions {
      default_user: "default".into(),
      ..RespServerSessionOptions::default()
    },
  );
  s.attach_acl(Some(Arc::new(GarnetAclAuthenticator::new(acl))));
  let session = store.new_session().unwrap();
  session.set_strict_context(true);
  s.set_garnet_api(Arc::new(StoreGarnetApi::new(session)));

  // 模拟处于事务 Running 态
  s.txn_state = TxnState::Running;

  let mut output = Vec::new();
  let res = s
    .process_hello_command(
      Some(3),
      b"100#bob",
      b"secret",
      None,
      &acl_store,
      &mut output,
    )
    .await;
  assert!(res.is_ok());
  assert!(
    s.cold_pending_ctx().is_none(),
    "ColdContextPending 必须被弃置"
  );
  assert!(s.pending_slow.is_none(), "严禁登记 SlowWait 停泊");
  assert_eq!(s.namespace, 0, "命名空间保持旧值");
  assert_eq!(
    output,
    b"-ERR HELLO is currently unsupported inside a transaction.\r\n"
  );
}

/// §58a 排队判据精化回归锁（多形共测，票
/// wnode-multi-hello-auth-option-coarse-scan-false-abort）
///
/// C# 锚：HELLO 无 NoMulti 旗（libs/resources/RespCommandsInfo.json:2107），
/// NetworkHELLO 全函数无事务门（libs/server/Resp/BasicCommands.cs:1438）——
/// C# 对任意 HELLO 形均排队 +QUEUED；本仓仅「位序文法可解析出合法 AUTH 选项
/// 组」形于排队期中止（§58a）。C# 测试面 TxnCommandCoverage 显式排除 HELLO
/// （Garnet.test.scripting/TransactionTests.cs:265），无一手对拍先例，本锁
/// 按 §58 判据 + network_hello 文法（parse_hello_args 单源）钉死误杀面。
/// 执行窗一态收口（§58d）：携 AUTH 形与文法错形回 §58b 事务窗禁停泊围栏
/// 错误帧；文法合法且无 AUTH 形（如 SETNAME 值位 AUTH）经同步快臂直出正常
/// 应答 map，事务整体均不 -EXECABORT
const HELLO_TXN_ERR: &[u8] = b"-ERR HELLO is currently unsupported inside a transaction.\r\n";

/// 回归锁 a：排队臂 AUTH 落 SETNAME 值位（合法客户端名字符集内）不构成
/// 认证选项组，必须 +QUEUED 而非中止整笔事务；重放窗零停泊零点查，
/// §58d 同步快臂直出正常 HELLO map
#[test]
fn multi_hello_auth_token_as_setname_value_not_abort_txn() {
  let (_dir, store) = wtest_base::open_test_store("txn-hello-setname-auth-value.db").unwrap();
  let acl = Arc::new(AccessControlList::new("").unwrap());
  let mut s = RespServerSession::new(
    1,
    RespServerSessionOptions {
      default_user: "default".into(),
      ..RespServerSessionOptions::default()
    },
  );
  s.attach_acl(Some(Arc::new(GarnetAclAuthenticator::new(acl))));
  s.set_garnet_api(Arc::new(StoreGarnetApi::new(store.new_session().unwrap())));
  s.attach_transaction_components(Arc::new(WatchVersionMap::new(64)), TxnLockTable::new());

  assert_eq!(
    feed_txn_session(&mut s, b"*1\r\n$5\r\nMULTI\r\n"),
    b"+OK\r\n"
  );
  assert_eq!(
    feed_txn_session(&mut s, b"*3\r\n$3\r\nSET\r\n$1\r\nk\r\n$1\r\nv\r\n"),
    b"+QUEUED\r\n"
  );
  // HELLO 3 SETNAME AUTH——"AUTH" 在值位（合法客户端名），不可触认证，排队放行
  assert_eq!(
    feed_txn_session(
      &mut s,
      b"*4\r\n$5\r\nHELLO\r\n$1\r\n3\r\n$7\r\nSETNAME\r\n$4\r\nAUTH\r\n"
    ),
    b"+QUEUED\r\n",
    "值位 AUTH 形排队即中止系粗扫误杀（修复前此断言失败）"
  );
  assert_eq!(s.txn_state, TxnState::Started);
  // EXEC：SET 生效 +OK；HELLO 元素为正常应答 map（§58d 无 AUTH 合法形经
  // 重放窗同步快臂直出），逐字节与独立同形 HELLO 参照等形，非整体 -EXECABORT
  let exec = feed_txn_session(&mut s, b"*1\r\n$4\r\nEXEC\r\n");
  let ref_map = feed_txn_session(
    &mut s,
    b"*4\r\n$5\r\nHELLO\r\n$1\r\n3\r\n$7\r\nSETNAME\r\n$4\r\nAUTH\r\n",
  );
  assert_eq!(
    exec,
    [b"*2\r\n+OK\r\n".as_slice(), ref_map.as_slice()].concat(),
    "重放窗 HELLO 元素须与独立同形 HELLO 应答逐字节等形"
  );
  assert_eq!(s.txn_state, TxnState::None, "事务收口复位");
  assert_eq!(
    s.resp_protocol_version, 3,
    "重放窗 HELLO 执行真实落位协议版本"
  );
  assert_eq!(
    s.client_name.as_deref(),
    Some("AUTH"),
    "SETNAME 载荷真实落位"
  );
  assert_eq!(
    feed_txn_session(&mut s, b"*2\r\n$3\r\nGET\r\n$1\r\nk\r\n"),
    b"$1\r\nv\r\n",
    "事务真实提交，SET 生效"
  );
}

/// 回归锁 b：AUTH 落选项位但尾随不足两参（文法 syntax error 形）不构成
/// 合法认证组，排队不中止
#[test]
fn multi_hello_auth_option_missing_trailing_args_not_abort_txn() {
  let (_dir, store) = wtest_base::open_test_store("txn-hello-auth-trailing.db").unwrap();
  let acl = Arc::new(AccessControlList::new("").unwrap());
  let mut s = RespServerSession::new(
    1,
    RespServerSessionOptions {
      default_user: "default".into(),
      ..RespServerSessionOptions::default()
    },
  );
  s.attach_acl(Some(Arc::new(GarnetAclAuthenticator::new(acl))));
  s.set_garnet_api(Arc::new(StoreGarnetApi::new(store.new_session().unwrap())));
  s.attach_transaction_components(Arc::new(WatchVersionMap::new(64)), TxnLockTable::new());

  assert_eq!(
    feed_txn_session(&mut s, b"*1\r\n$5\r\nMULTI\r\n"),
    b"+OK\r\n"
  );
  // HELLO 3 SETNAME x AUTH——选项位 AUTH 后零参，文法应 syntax error，排队放行
  assert_eq!(
    feed_txn_session(
      &mut s,
      b"*5\r\n$5\r\nHELLO\r\n$1\r\n3\r\n$7\r\nSETNAME\r\n$1\r\nx\r\n$4\r\nAUTH\r\n"
    ),
    b"+QUEUED\r\n",
    "选项位缺尾参形排队即中止系粗扫误杀（修复前此断言失败）"
  );
  assert_eq!(
    feed_txn_session(&mut s, b"*1\r\n$4\r\nEXEC\r\n"),
    [b"*1\r\n".as_slice(), HELLO_TXN_ERR].concat(),
    "EXEC 整体不得 -EXECABORT"
  );
}

/// 回归锁 c：协议版本位错形（"AUTH" 悬于版本位）按文法永不消费为认证组，
/// 排队不中止；非事务窗执行臂文法臂版本错语义不变（C# BasicCommands.cs
/// :1455-1458 RESP_ERR_PROTOCOL_VALUE_IS_NOT_INTEGER）
#[test]
fn multi_hello_auth_token_at_version_slot_not_abort_txn() {
  let (_dir, store) = wtest_base::open_test_store("txn-hello-version-slot.db").unwrap();
  let acl = Arc::new(AccessControlList::new("").unwrap());
  let mut s = RespServerSession::new(
    1,
    RespServerSessionOptions {
      default_user: "default".into(),
      ..RespServerSessionOptions::default()
    },
  );
  s.attach_acl(Some(Arc::new(GarnetAclAuthenticator::new(acl))));
  s.set_garnet_api(Arc::new(StoreGarnetApi::new(store.new_session().unwrap())));
  s.attach_transaction_components(Arc::new(WatchVersionMap::new(64)), TxnLockTable::new());

  assert_eq!(
    feed_txn_session(&mut s, b"*1\r\n$5\r\nMULTI\r\n"),
    b"+OK\r\n"
  );
  assert_eq!(
    feed_txn_session(
      &mut s,
      b"*4\r\n$5\r\nHELLO\r\n$4\r\nAUTH\r\n$1\r\n1\r\n$1\r\np\r\n"
    ),
    b"+QUEUED\r\n",
    "版本位 AUTH 形排队即中止系粗扫误杀（修复前此断言失败）"
  );
  assert_eq!(
    feed_txn_session(&mut s, b"*1\r\n$4\r\nEXEC\r\n"),
    [b"*1\r\n".as_slice(), HELLO_TXN_ERR].concat(),
    "EXEC 整体不得 -EXECABORT"
  );
  // 非事务窗对照：同形经执行臂文法落版本错帧（文法语义零改动）
  assert_eq!(
    feed_txn_session(
      &mut s,
      b"*4\r\n$5\r\nHELLO\r\n$4\r\nAUTH\r\n$1\r\n1\r\n$1\r\np\r\n"
    ),
    b"-ERR Protocol version is not an integer or out of range.\r\n"
  );
}

/// 回归锁 e：合法 AUTH 选项组大小写混排仍排队中止（§58a 等值口径不回退）；
/// 同形小写 token 落 SETNAME 值位则排队放行（粗扫在该形必误杀），重放窗
/// 经 §58d 同步快臂直出正常 HELLO map
#[test]
fn multi_hello_auth_group_case_insensitive_still_aborts() {
  let (_dir, store) = wtest_base::open_test_store("txn-hello-auth-case-arm.db").unwrap();
  let acl = Arc::new(AccessControlList::new("").unwrap());
  let mut s = RespServerSession::new(
    1,
    RespServerSessionOptions {
      default_user: "default".into(),
      ..RespServerSessionOptions::default()
    },
  );
  s.attach_acl(Some(Arc::new(GarnetAclAuthenticator::new(acl))));
  s.set_garnet_api(Arc::new(StoreGarnetApi::new(store.new_session().unwrap())));
  s.attach_transaction_components(Arc::new(WatchVersionMap::new(64)), TxnLockTable::new());

  // 合法组 + 小写选项位：排队即中止（a 臂语义一字不变）
  assert_eq!(
    feed_txn_session(&mut s, b"*1\r\n$5\r\nMULTI\r\n"),
    b"+OK\r\n"
  );
  assert_eq!(
    feed_txn_session(
      &mut s,
      b"*5\r\n$5\r\nHELLO\r\n$1\r\n3\r\n$4\r\nauth\r\n$1\r\nu\r\n$1\r\np\r\n"
    ),
    HELLO_TXN_ERR
  );
  assert_eq!(s.txn_state, TxnState::Aborted);
  assert_eq!(
    feed_txn_session(&mut s, b"*1\r\n$4\r\nEXEC\r\n"),
    b"-EXECABORT Transaction discarded because of previous errors.\r\n"
  );

  // 对照形：小写 "auth" 落 SETNAME 值位——粗扫必误杀，位序文法下排队放行
  assert_eq!(
    feed_txn_session(&mut s, b"*1\r\n$5\r\nMULTI\r\n"),
    b"+OK\r\n"
  );
  assert_eq!(
    feed_txn_session(
      &mut s,
      b"*4\r\n$5\r\nHELLO\r\n$1\r\n3\r\n$7\r\nSETNAME\r\n$4\r\nauth\r\n"
    ),
    b"+QUEUED\r\n",
    "值位小写 auth 形排队即中止系粗扫误杀（修复前此断言失败）"
  );
  // §58d 收口：文法合法无 AUTH 形重放窗直出正常 map（逐字节与独立同形参照等形）
  let exec = feed_txn_session(&mut s, b"*1\r\n$4\r\nEXEC\r\n");
  let ref_map = feed_txn_session(
    &mut s,
    b"*4\r\n$5\r\nHELLO\r\n$1\r\n3\r\n$7\r\nSETNAME\r\n$4\r\nauth\r\n",
  );
  assert_eq!(
    exec,
    [b"*1\r\n".as_slice(), ref_map.as_slice()].concat(),
    "EXEC 整体不得 -EXECABORT，元素须为正常 HELLO map（修复前共回围栏错误帧）"
  );
}
