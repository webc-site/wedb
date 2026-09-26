//! 大 MULTI 队列 + 多键 WATCH 深臂 C# 对标补测（票 r318）
//!
//! 对标 garnet test/standalone/Garnet.test.scripting/TransactionTests.cs：
//! - `LargeTxnWatch([Values(512,2048,4096)])`（:190）：size 键全量 WATCH +
//!   2*size 交替 GET/SET 入队，条件全真 → EXEC 成功臂逐元素应答对标；
//!   追加一把 4096 规模的条件失效臂（他会话触碰任一被监键 → EXEC 夭折
//!   `*-1`、队列零副作用），钉死版本表规模化校验面。
//! - `LargeTxn([Values(512,2048,8192)])`（:153）：无 WATCH 的超长队列
//!   EXEC 臂（取最大 8192 形）＋ 尾接 DISCARD 的整队作废臂（C#
//!   NetworkDISCARD 回 +OK 且整队丢弃：garnet
//!   libs/server/Transaction/TxnRespCommands.cs:209）。
//!
//! 既有覆盖甄别：simple_watch_test / watch_non_existent_key
//! （transaction_tests.rs:229/:267）、单键并发夭折（transaction_session_test
//! .rs:206）、SELECT 切库作废、分层/范围索引栅栏等已收小形臂，本文件只补
//! 「多键批量登记 × 超长队列」深臂，不复抄。
//!
//! 装配走生产会话链（StorageSessionProvider::open_with_config，版本表写面
//! 钩子与事务组件 provider 单点注入），双会话：WATCH 方 × 外部写方。泵复用
//! wnode_test::pump + drive_pending_parks_consumer 单源，组帧复用
//! wtest_base::resp_frame；应答面为整段字节精确比对（含水位满刷重入后的
//! 拼合序），禁恒真断言。

use std::sync::Arc;

use tempfile::tempdir;
use wnode::{
  SessionProviderFace, WireFormat, resp::resp_session_consumer::RespSessionConsumer,
  service::StorageSessionProvider,
};
use wnode_test::{SessionFactory, drive_pending_parks_consumer, pump, session_factory};
use wtest_base::{resp_frame, test_store_config};

/// C# LargeTxnWatch 值面复刻：key = "mykey"+i、value = "abcdefg"+i
fn key(i: usize) -> Vec<u8> {
  format!("mykey{i}").into_bytes()
}

fn val(i: usize) -> Vec<u8> {
  format!("abcdefg{i}").into_bytes()
}

/// 双会话环境：watcher（WATCH/MULTI 方）与 writer（外部直写方），共享
/// provider 版本表与引擎写面钩子（生产装配同径）
struct Env {
  watcher: RespSessionConsumer,
  writer: RespSessionConsumer,
  _provider: Arc<StorageSessionProvider<SessionFactory>>,
  _dir: tempfile::TempDir,
}

fn env() -> Env {
  let dir = tempdir().expect("tempdir");
  let provider = Arc::new(
    StorageSessionProvider::open_with_config(
      test_store_config(),
      dir.path().join("node.db"),
      session_factory as SessionFactory,
    )
    .expect("provider 装配"),
  );
  let watcher = provider
    .get_session(WireFormat::Ascii, 1)
    .expect("watcher 会话");
  let writer = provider
    .get_session(WireFormat::Ascii, 2)
    .expect("writer 会话");
  Env {
    watcher,
    writer,
    _provider: provider,
    _dir: dir,
  }
}

/// 整批投喂 → 应答：一次 pump 后按残余水位重入排空（大 EXEC 应答达
/// OUTPUT_WATERMARK_BYTES 在命令边界停住），末了闭环慢路径挂起臂
async fn send(c: &mut RespSessionConsumer, frame: &[u8]) -> Vec<u8> {
  let (mut consumed, mut resp) = pump(c, frame);
  while consumed != Some(0) {
    assert!(consumed.is_some(), "帧应被完整消费（None=协议违规）");
    let (r, more) = pump(c, &[]);
    resp.extend_from_slice(&more);
    consumed = r;
  }
  drive_pending_parks_consumer(c, &mut resp).await;
  resp
}

/// RESP2 bulk 应答元素
fn bulk(v: &[u8]) -> Vec<u8> {
  let mut out = format!("${}\r\n", v.len()).into_bytes();
  out.extend_from_slice(v);
  out.extend_from_slice(b"\r\n");
  out
}

/// 前置写面：writer 直发 size 条 SET，应答必须逐条 +OK（对标 C# 基线
/// db.StringSet 预置段，:197-198）
async fn seed(w: &mut RespSessionConsumer, size: usize) {
  let mut inp = Vec::new();
  for i in 0..size {
    inp.extend_from_slice(&resp_frame(&[b"SET", &key(i), &val(i)]));
  }
  assert_eq!(
    send(w, &inp).await,
    b"+OK\r\n".to_vec().repeat(size),
    "预置 {size} 键必须逐条 +OK"
  );
}

/// 多键单帧 WATCH（WATCH k0 .. kN-1），应答 +OK
async fn watch_all(a: &mut RespSessionConsumer, size: usize) {
  let keys: Vec<Vec<u8>> = (0..size).map(key).collect();
  let mut parts: Vec<&[u8]> = Vec::with_capacity(size + 1);
  parts.push(b"WATCH");
  for k in &keys {
    parts.push(k);
  }
  assert_eq!(
    send(a, &resp_frame(&parts)).await,
    b"+OK\r\n",
    "批量 WATCH {size} 键应回 +OK"
  );
}

/// MULTI + 2*size 交替 GET key(i/2)/SET key((i-1)/2+size) + EXEC 的入队
/// 批与 EXEC 期望应答（C# LargeTxnWatch :202-214 逐对复刻）
fn txn_queue_frames(size: usize) -> (Vec<u8>, Vec<u8>) {
  let mut inp = resp_frame(&[b"MULTI"]);
  let mut exec = format!("*{}\r\n", size * 2).into_bytes();
  for i in 0..size * 2 {
    if i % 2 == 0 {
      let k = key(i / 2);
      inp.extend_from_slice(&resp_frame(&[b"GET", &k]));
      exec.extend_from_slice(&bulk(&val(i / 2)));
    } else {
      let c = (i - 1) / 2 + size;
      inp.extend_from_slice(&resp_frame(&[b"SET", &key(c), &val(c)]));
      exec.extend_from_slice(b"+OK\r\n");
    }
  }
  inp.extend_from_slice(&resp_frame(&[b"EXEC"]));
  (inp, exec)
}

/// LargeTxnWatch 三尺寸成功臂（:190）：条件全真 → EXEC 逐元素应答与
/// C# 语义同形（GET 回预置值 / SET 回 +OK），且新键真实落库
#[compio::test]
async fn large_txn_watch_512() {
  large_txn_watch_commit(512).await;
}

#[compio::test]
async fn large_txn_watch_2048() {
  large_txn_watch_commit(2048).await;
}

#[compio::test]
async fn large_txn_watch_4096() {
  large_txn_watch_commit(4096).await;
}

async fn large_txn_watch_commit(size: usize) {
  let mut e = env();
  seed(&mut e.writer, size).await;
  watch_all(&mut e.watcher, size).await;

  let (inp, exec) = txn_queue_frames(size);
  let queued = "+QUEUED\r\n".repeat(size * 2);
  let expect = [b"+OK\r\n".as_slice(), queued.as_bytes(), exec.as_slice()].concat();
  assert_eq!(
    send(&mut e.watcher, &inp).await,
    expect,
    "{size} 规模 WATCH 全真事务 EXEC 应答必须逐元素精确对标"
  );
  // 提交面复核：事务内 SET 的新键已落库（对标 C# :216-226 尾检）
  let mut chk = Vec::new();
  for i in [size, size + size / 2, size * 2 - 1] {
    chk.extend_from_slice(&resp_frame(&[b"GET", &key(i)]));
  }
  let expect_chk = [
    bulk(&val(size)).as_slice(),
    bulk(&val(size + size / 2)).as_slice(),
    bulk(&val(size * 2 - 1)).as_slice(),
  ]
  .concat();
  assert_eq!(
    send(&mut e.writer, &chk).await,
    expect_chk,
    "EXEC 提交后新键必须可读回事务内写入值"
  );
}

/// 4096 规模失效臂：WATCH 全量入队后他会话触碰任一被监键 → EXEC 夭折
/// `*-1`，整队零副作用（小形夭折已由 transaction_session_test 收口，此臂
/// 钉的是版本表在满容器规模下的校验完备性）
#[compio::test]
async fn large_txn_watch_abort_when_any_key_touched() {
  let size = 4096;
  let mut e = env();
  seed(&mut e.writer, size).await;
  watch_all(&mut e.watcher, size).await;

  // 只入队不打 EXEC：MULTI + 2*size 交替命令
  let (mut inp, _exec) = txn_queue_frames(size);
  inp.truncate(inp.len() - resp_frame(&[b"EXEC"]).len());
  let queued = "+QUEUED\r\n".repeat(size * 2);
  assert_eq!(
    send(&mut e.watcher, &inp).await,
    [b"+OK\r\n".as_slice(), queued.as_bytes()].concat(),
    "排队段必须逐条 +QUEUED"
  );

  // 外部写方触碰最后一个被监键（C# updateKey :582 形）
  assert_eq!(
    send(
      &mut e.writer,
      &resp_frame(&[b"SET", &key(size - 1), b"touched"])
    )
    .await,
    b"+OK\r\n",
    "外部 SET 被监键应成功"
  );
  assert_eq!(
    send(&mut e.watcher, &resp_frame(&[b"EXEC"])).await,
    b"*-1\r\n",
    "满容器规模下任一被监键失配，EXEC 必须夭折 *-1"
  );
  // 零副作用抽查：队首 SET 目标键不得存在
  assert_eq!(
    send(&mut e.writer, &resp_frame(&[b"GET", &key(size)])).await,
    b"$-1\r\n",
    "夭折后队列内 SET 严禁产生副作用"
  );
}

/// LargeTxn 无 WATCH 大队列臂（:153，取最大尺寸形 8192）：纯超长队列
/// EXEC 成功面（本仓缺口的「超长命令队列 + EXEC 尾语义」锁之一）
#[compio::test]
async fn large_txn_queue_exec_8192() {
  let size = 8192;
  let mut e = env();
  seed(&mut e.writer, size).await;

  let (inp, exec) = txn_queue_frames(size);
  let queued = "+QUEUED\r\n".repeat(size * 2);
  let expect = [b"+OK\r\n".as_slice(), queued.as_bytes(), exec.as_slice()].concat();
  assert_eq!(
    send(&mut e.watcher, &inp).await,
    expect,
    "8192×2 超长队列无 WATCH EXEC 必须整数组精确提交应答"
  );
}

/// 大队列 DISCARD 尾语义臂：4096 条入队后 DISCARD 回 +OK（C#
/// TxnRespCommands.cs:209-219），整队作废零副作用，且会话事务位复位
#[compio::test]
async fn large_txn_queue_discard_drops_everything() {
  let size = 4096;
  let mut e = env();
  let mut inp = resp_frame(&[b"MULTI"]);
  for i in 0..size {
    inp.extend_from_slice(&resp_frame(&[b"SET", &key(i), &val(i)]));
  }
  inp.extend_from_slice(&resp_frame(&[b"DISCARD"]));
  let queued = "+QUEUED\r\n".repeat(size);
  assert_eq!(
    send(&mut e.watcher, &inp).await,
    [b"+OK\r\n".as_slice(), queued.as_bytes(), b"+OK\r\n"].concat(),
    "MULTI + {size} 入队 + DISCARD 必须回 +OK×N 尾 +OK"
  );
  assert_eq!(
    send(&mut e.watcher, &resp_frame(&[b"GET", &key(0)])).await,
    b"$-1\r\n",
    "DISCARD 后整队严禁落库"
  );
  // 复位复核：DISCARD 收口后同会话可正常走完一轮新事务
  let replay = [
    resp_frame(&[b"MULTI"]).as_slice(),
    resp_frame(&[b"SET", &key(0), &val(0)]).as_slice(),
    resp_frame(&[b"EXEC"]).as_slice(),
  ]
  .concat();
  assert_eq!(
    send(&mut e.watcher, &replay).await,
    b"+OK\r\n+QUEUED\r\n*1\r\n+OK\r\n",
    "DISCARD 收口后会话事务位必须复位、可重新成套提交"
  );
  assert_eq!(
    send(&mut e.watcher, &resp_frame(&[b"GET", &key(0)])).await,
    bulk(&val(0)),
    "复位后新事务的写入应真实可见"
  );
}
