//! 字符串条件写族与位图族 found/notfound 记账矩阵锁（票
//! wnode-string-bitmap-found-notfound-accounting-matrix，P4 观测面）
//!
//! 案一（条件写族补账恰一帧）：C# SETNX / SET 条件写走
//! MainStoreOps.cs:SET_Conditional 无输出重载（:279 incr_session_notfound /
//! :284 incr_session_found / :273 WRONGTYPE 零计）与输出重载
//! （NetworkSET_Conditional getValue 臂 :339/:344），每命令恰一条；rust
//! 快臂原传 None 静默零计、慢臂走簿记档逐域探针（缺失键计 3、对象键计 2）
//! 双向失联。修复形态：快臂本地三态折叠 + 函数尾 fold_outcome 单点补账
//! （network_getex 先例同形），慢臂改静默对偶探针 + record_read_outcome
//! 终态单点补账；WRONGTYPE 与一切降级出口零入账交慢臂唯一出口收口。
//!
//! 案二（位图慢臂归零）：C# BitmapOps.cs:437-453 五命令（SETBIT/GETBIT/
//! BITCOUNT/BITPOS/BITFIELD）全走 RMW_MainStore / Read_MainStore
//! （AdvancedOps.cs:71/:87 零 incr_session_*）；rust 慢臂原走 read_cold /
//! read_and_frame 簿记漏斗各多计一帧。修复形态：写臂迁 read_cold_quiet
//! （SETRANGE/APPEND 既用），纯读臂增 read_and_frame_quiet 静默对偶
//! （内核复用 storage.read_user_quiet）；GETRANGE/STRLEN 入账臂不动。
//!
//! 案三（BITOP 快臂逐源补账）：C# ReadWithUnsafeContext
//! （MainStoreOps.cs:44/:76/:81）逐源恰一帧、dest 不计；rust 快臂原 None
//! 静默零计而慢臂 read_user_with_prefix 逐源簿记 N 帧——快慢臂同账失联。
//! 修复形态：快臂循环本地累加 found/notfound、成功收尾一次入账
//! （do_network_mget 先例）；WRONGTYPE 与降级出口零入账；慢臂 dest RI 门
//! 改静默口（gate 读虚增 1 帧会使逐源 N 帧变 N+1）。
//!
//! 测试全真存储真协议帧，无 mock：快臂经会话消费者 roundtrip（降级检测
//! 同 getex_getdel_read_accounting.rs 先例），慢臂经 SlowWait::for_command
//! 直驱；SET 族慢臂快照携 [`TtlResume`] 9 字节尾参（exec 降级快照恒追加
//! 同形，Full 态 = 全量重放）。

use std::str::from_utf8;

use compio::runtime::Runtime;
use itoa::Buffer;
use wnode::{
  MessageConsumerFace, RespSessionConsumer,
  resp::{TtlResume, garnet_api::GarnetApi, slow_path::SlowWait},
};
use wnode_test::{counts, metrics_env as env};
use wresp::command::RespCommand;
use wtest_base::resp_frame as frame;

/// 慢臂直驱的 RESP 协议版本入参
const RESP_V2: u8 = 2;

/// RESP2 bulk string 应答帧编码（GET 形旧值回显对拍用）
fn bulk_frame(val: &[u8]) -> Vec<u8> {
  let mut buf = Buffer::new();
  let mut out = Vec::new();
  out.push(b'$');
  out.extend_from_slice(buf.format(val.len()).as_bytes());
  out.extend_from_slice(b"\r\n");
  out.extend_from_slice(val);
  out.extend_from_slice(b"\r\n");
  out
}

/// 快臂单命令往返（降级挂起体由 block_on 承接闭环），并回报本命令是否降级
fn roundtrip(rt: &Runtime, c: &mut RespSessionConsumer, args: &[&[u8]]) -> (Vec<u8>, bool) {
  let mut scratch = c.take_recv_scratch();
  scratch.extend_from_slice(&frame(args));
  c.return_recv_scratch(scratch);
  let mut resp = Vec::new();
  let _ = c.try_consume_messages_into(&mut resp);
  match c.take_slow_wait() {
    Some(slow) => {
      resp.extend_from_slice(&rt.block_on(slow.resolve()));
      (resp, true)
    }
    None => (resp, false),
  }
}

/// 慢臂直驱（与降级快照投递同径，不经会话快路径）
fn slow_direct(rt: &Runtime, api: &GarnetApi, cmd: RespCommand, args: &[&[u8]]) -> Vec<u8> {
  let snapshot: Vec<Vec<u8>> = args.iter().map(|a| a.to_vec()).collect();
  rt.block_on(async {
    SlowWait::for_command(api, cmd, snapshot, RESP_V2)
      .resolve()
      .await
  })
}

/// SET 族慢臂快照的「提交前降级」尾参（exec 降级快照恒追加同款，Full =
/// 整命令全量重放；[`TtlResume::tail_bytes`] 单源编码，杜绝 9 双字面量）
fn set_tail_full() -> Vec<u8> {
  TtlResume::Full.tail_bytes()
}

/// INFO bulk 应答载荷文本
fn info_payload(resp: &[u8]) -> String {
  assert!(
    resp.starts_with(b"$"),
    "INFO 应答须为 bulk 帧，实得 {resp:?}"
  );
  let hdr_end = resp
    .windows(2)
    .position(|w| w == b"\r\n")
    .expect("bulk 头终止符");
  let len: usize = from_utf8(&resp[1..hdr_end]).unwrap().parse().unwrap();
  from_utf8(&resp[hdr_end + 2..hdr_end + 2 + len])
    .unwrap()
    .to_string()
}

/// INFO STATS 文本中的 (total_found, total_notfound) 读数
fn info_counts(resp: &[u8]) -> (u64, u64) {
  let (mut f, mut n) = (None, None);
  for line in info_payload(resp).lines() {
    if let Some(v) = line.trim().strip_prefix("total_found:") {
      f = Some(v.parse().unwrap());
    } else if let Some(v) = line.trim().strip_prefix("total_notfound:") {
      n = Some(v.parse().unwrap());
    }
  }
  (
    f.expect("STATS 段 total_found 行"),
    n.expect("STATS 段 total_notfound 行"),
  )
}

/// 案一主锁：SETNX 命中/缺席双臂各恰一帧（C# BasicCommands.cs:592 →
/// SET_Conditional 无输出重载 MainStoreOps.cs:279/:284 单帧口径）。
/// 快臂命中 found+1、缺席写成功 notfound+1（修复前快臂 None 静默恒零）；
/// 慢臂同形各恰一帧（修复前簿记档探针缺失键计 3、存活键 found 虚增）
#[test]
fn setnx_hit_absent_record_single_frame_both_arms() {
  let (rt, mut c, api, handle, _dir, _store) = env("strbitmap-setnx.db");

  assert_eq!(
    roundtrip(&rt, &mut c, &[b"SET", b"hit", b"v"]).0,
    b"+OK\r\n"
  );

  // 快臂命中（键在 → :0）：found 恰 1（修复前恒零为红灯）
  let (f0, n0) = counts(&handle);
  let (resp, degraded) = roundtrip(&rt, &mut c, &[b"SETNX", b"hit", b"w"]);
  assert_eq!(resp, b":0\r\n", "SETNX 命中应答 :0 零改动");
  assert!(!degraded, "常规存储域 SETNX 快臂不应降级");
  assert_eq!(counts(&handle), (f0 + 1, n0), "SETNX 快臂命中恰 1 found");

  // 快臂缺席（写成功 → :1）：notfound 恰 1
  let (f1, n1) = counts(&handle);
  let (resp, degraded) = roundtrip(&rt, &mut c, &[b"SETNX", b"absent", b"v"]);
  assert_eq!(resp, b":1\r\n", "SETNX 缺席应答 :1 零改动");
  assert!(!degraded);
  assert_eq!(
    counts(&handle),
    (f1, n1 + 1),
    "SETNX 快臂缺席写成功恰 1 notfound（修复前恒零为红灯）"
  );

  // 慢臂命中：found 恰 1
  assert_eq!(roundtrip(&rt, &mut c, &[b"SET", b"sh", b"v"]).0, b"+OK\r\n");
  let (f2, n2) = counts(&handle);
  assert_eq!(
    slow_direct(&rt, &api, RespCommand::Setnx, &[b"sh", b"w"]),
    b":0\r\n"
  );
  assert_eq!(
    counts(&handle),
    (f2 + 1, n2),
    "SETNX 慢臂命中恰 1 found（修复前簿记探针虚计为红灯）"
  );

  // 慢臂缺席：notfound 恰 1
  let (f3, n3) = counts(&handle);
  assert_eq!(
    slow_direct(&rt, &api, RespCommand::Setnx, &[b"sabsent", b"v"]),
    b":1\r\n"
  );
  assert_eq!(
    counts(&handle),
    (f3, n3 + 1),
    "SETNX 慢臂缺席恰 1 notfound（修复前缺失键三域探针计 3 为红灯）"
  );

  // 对象键臂（票面裁定「存活 found」，C# NX 语义以键在为断言终态）：
  // 快慢臂各恰一帧 found
  assert_eq!(
    roundtrip(&rt, &mut c, &[b"HSET", b"obj", b"f", b"v"]).0,
    b":1\r\n"
  );
  let (f4, n4) = counts(&handle);
  let (resp, _degraded) = roundtrip(&rt, &mut c, &[b"SETNX", b"obj", b"v"]);
  assert_eq!(resp, b":0\r\n", "SETNX 对象键命中应答 :0");
  assert_eq!(counts(&handle), (f4 + 1, n4), "SETNX 快臂对象键恰 1 found");
  let (f5, n5) = counts(&handle);
  assert_eq!(
    slow_direct(&rt, &api, RespCommand::Setnx, &[b"obj", b"w"]),
    b":0\r\n"
  );
  assert_eq!(
    counts(&handle),
    (f5 + 1, n5),
    "SETNX 慢臂对象键恰 1 found（静默探针终态折叠，修复前簿记虚计为红灯）"
  );
}

/// 案一扩锁：SET k v GET 与 GETSET 快慢同账（C# NetworkSET_Conditional
/// getValue 臂输出重载 MainStoreOps.cs:339/:344 恰一帧；GET 形快臂补账后
/// 与慢臂 read_cold 既有一帧对齐）。非 GET 形 SET NX 两态同锁
#[test]
fn set_get_form_and_getset_fast_slow_same_accounting() {
  let (rt, mut c, api, handle, _dir, _store) = env("strbitmap-setget.db");

  assert_eq!(
    roundtrip(&rt, &mut c, &[b"SET", b"k", b"old"]).0,
    b"+OK\r\n"
  );

  // SET k v GET 快臂命中：回旧值 + found 恰 1
  let (f0, n0) = counts(&handle);
  let (resp, degraded) = roundtrip(&rt, &mut c, &[b"SET", b"k", b"new", b"GET"]);
  assert_eq!(resp, bulk_frame(b"old"), "SET GET 命中回旧值零改动");
  assert!(!degraded);
  assert_eq!(counts(&handle), (f0 + 1, n0), "SET GET 快臂命中恰 1 found");

  // SET k v GET 快臂缺席：nil + notfound 恰 1
  let (f1, n1) = counts(&handle);
  let (resp, degraded) = roundtrip(&rt, &mut c, &[b"SET", b"gabsent", b"v", b"GET"]);
  assert_eq!(resp, b"$-1\r\n");
  assert!(!degraded);
  assert_eq!(
    counts(&handle),
    (f1, n1 + 1),
    "SET GET 快臂缺席恰 1 notfound"
  );

  // GETSET 快臂命中/缺席同锁
  let (f2, n2) = counts(&handle);
  let (resp, degraded) = roundtrip(&rt, &mut c, &[b"GETSET", b"k", b"x"]);
  assert_eq!(resp, bulk_frame(b"new"));
  assert!(!degraded);
  assert_eq!(counts(&handle), (f2 + 1, n2), "GETSET 快臂命中恰 1 found");
  let (f3, n3) = counts(&handle);
  let (resp, degraded) = roundtrip(&rt, &mut c, &[b"GETSET", b"gabsent2", b"v"]);
  assert_eq!(resp, b"$-1\r\n");
  assert!(!degraded);
  assert_eq!(
    counts(&handle),
    (f3, n3 + 1),
    "GETSET 快臂缺席恰 1 notfound"
  );

  // 慢臂 GETSET（read_cold 既有簿记一帧，修复前后对齐）：命中 found 恰 1、
  // 缺席 notfound 恰 1
  let (f4, n4) = counts(&handle);
  assert_eq!(
    slow_direct(&rt, &api, RespCommand::Getset, &[b"k", b"y"]),
    bulk_frame(b"x")
  );
  assert_eq!(counts(&handle), (f4 + 1, n4), "GETSET 慢臂命中恰 1 found");
  let (f5, n5) = counts(&handle);
  assert_eq!(
    slow_direct(&rt, &api, RespCommand::Getset, &[b"gabsent3", b"v"]),
    b"$-1\r\n"
  );
  assert_eq!(
    counts(&handle),
    (f5, n5 + 1),
    "GETSET 慢臂缺席恰 1 notfound"
  );

  // 慢臂 SET k v GET（快照携尾参，与 exec 降级投递同径）：快慢同账锁
  let tail = set_tail_full();
  let (f6, n6) = counts(&handle);
  assert_eq!(
    slow_direct(
      &rt,
      &api,
      RespCommand::Set,
      &[b"k", b"z", b"GET", tail.as_slice()]
    ),
    bulk_frame(b"y")
  );
  assert_eq!(counts(&handle), (f6 + 1, n6), "SET GET 慢臂命中恰 1 found");

  // 非 GET 形 SET NX 慢臂两态（无 NX/XX 裸写族非本票域不动，NX 条件臂在
  // slow_set_conditional 终态补账）：NX 命中键在 → nil + found 恰 1、
  // NX 缺席可写 → +OK + notfound 恰 1
  let (f7, n7) = counts(&handle);
  assert_eq!(
    slow_direct(
      &rt,
      &api,
      RespCommand::Set,
      &[b"k", b"never", b"NX", tail.as_slice()]
    ),
    b"$-1\r\n"
  );
  assert_eq!(
    counts(&handle),
    (f7 + 1, n7),
    "SET NX 慢臂命中键在恰 1 found（修复前簿记探针虚计为红灯）"
  );
  let (f8, n8) = counts(&handle);
  assert_eq!(
    slow_direct(
      &rt,
      &api,
      RespCommand::Set,
      &[b"nxabsent", b"v", b"NX", tail.as_slice()]
    ),
    b"+OK\r\n"
  );
  assert_eq!(
    counts(&handle),
    (f8, n8 + 1),
    "SET NX 慢臂缺席写成功恰 1 notfound（修复前缺失键计 3 为红灯）"
  );

  // 快臂 SET NX 两态同锁（函数尾 fold_outcome 单点补账）
  let (f9, n9) = counts(&handle);
  let (resp, _deg) = roundtrip(&rt, &mut c, &[b"SET", b"nxabsent", b"w", b"NX"]);
  assert_eq!(resp, b"$-1\r\n", "键已在 NX 回 nil");
  assert_eq!(counts(&handle), (f9 + 1, n9), "SET NX 快臂键在恰 1 found");
}

/// 案二主锁：位图五命令慢臂零入账（C# BitmapOps.cs:437-453 全走
/// RMW_MainStore / Read_MainStore，AdvancedOps.cs:71/:87 零
/// incr_session_*）。修复前 SETBIT/BITFIELD 走 read_cold、GETBIT/BITCOUNT/
/// BITPOS 走 read_and_frame 簿记漏斗各多计一帧为红灯
#[test]
fn bitmap_slow_arm_records_nothing() {
  let (rt, mut c, api, handle, _dir, _store) = env("strbitmap-bitmap-slow.db");

  assert_eq!(
    roundtrip(&rt, &mut c, &[b"SET", b"s", b"abc"]).0,
    b"+OK\r\n",
    "3 字节种子键"
  );
  let (f0, n0) = counts(&handle);

  // SETBIT 命中写臂 / 缺席写臂：零入账（命中臂写 0 于位 0 恒 0 处，种子串
  // 零改动，后续 BITCOUNT/BITPOS 期望不漂移）
  assert_eq!(
    slow_direct(&rt, &api, RespCommand::Setbit, &[b"s", b"0", b"0"]),
    b":0\r\n",
    "SETBIT 慢臂命中应答旧位零改动"
  );
  assert_eq!(
    slow_direct(&rt, &api, RespCommand::Setbit, &[b"sabsent", b"8", b"1"]),
    b":0\r\n",
    "SETBIT 慢臂缺席应答 :0 零改动"
  );
  assert_eq!(counts(&handle), (f0, n0), "SETBIT 慢臂写臂零入账");

  // GETBIT 命中/缺席：零入账（修复前 found/notfound 各 +1）
  assert_eq!(
    slow_direct(&rt, &api, RespCommand::Getbit, &[b"s", b"0"]),
    b":0\r\n"
  );
  assert_eq!(
    slow_direct(&rt, &api, RespCommand::Getbit, &[b"gone", b"0"]),
    b":0\r\n",
    "GETBIT 慢臂缺席 → :0（C# NOTFOUND 同形）"
  );
  assert_eq!(counts(&handle), (f0, n0), "GETBIT 慢臂零入账");

  // BITCOUNT 命中/缺席：零入账（缺席臂用从未写过的 gone，避免 SETBIT
  // 缺席臂已建键漂移期望值）
  assert_eq!(
    slow_direct(&rt, &api, RespCommand::Bitcount, &[b"s"]),
    b":10\r\n",
    "abc 全串 popcount=10"
  );
  assert_eq!(
    slow_direct(&rt, &api, RespCommand::Bitcount, &[b"gone"]),
    b":0\r\n"
  );
  assert_eq!(counts(&handle), (f0, n0), "BITCOUNT 慢臂零入账");

  // BITPOS 命中/缺席：零入账
  assert_eq!(
    slow_direct(&rt, &api, RespCommand::Bitpos, &[b"s", b"1"]),
    b":1\r\n",
    "'a'=01100001 首个 1 在位 1"
  );
  assert_eq!(
    slow_direct(&rt, &api, RespCommand::Bitpos, &[b"gone", b"1"]),
    b":-1\r\n",
    "C# NOTFOUND 找 1 → -1"
  );
  assert_eq!(counts(&handle), (f0, n0), "BITPOS 慢臂零入账");

  // BITFIELD 读子命令 / 写子命令与 BITFIELD_RO：零入账
  assert_eq!(
    slow_direct(
      &rt,
      &api,
      RespCommand::Bitfield,
      &[b"s", b"GET", b"u8", b"0"]
    ),
    b"*1\r\n:97\r\n",
    "BITFIELD GET 首字节 'a'=97"
  );
  assert_eq!(
    slow_direct(
      &rt,
      &api,
      RespCommand::Bitfield,
      &[b"s", b"SET", b"u8", b"0", b"255"]
    ),
    b"*1\r\n:97\r\n"
  );
  assert_eq!(
    slow_direct(
      &rt,
      &api,
      RespCommand::BitfieldRo,
      &[b"s", b"GET", b"u8", b"1"]
    ),
    b"*1\r\n:254\r\n",
    "BITFIELD_RO GET 位偏 1 取 8 位：0xFF 低 7 位 + 'b' 首位 0 = 254"
  );
  assert_eq!(
    counts(&handle),
    (f0, n0),
    "BITFIELD/BITFIELD_RO 慢臂零入账（修复前 read_cold 簿记各 +1 为红灯）"
  );

  // 对象键臂：WRONGTYPE 且零入账
  assert_eq!(
    roundtrip(&rt, &mut c, &[b"HSET", b"obj", b"f", b"v"]).0,
    b":1\r\n"
  );
  let obj = slow_direct(&rt, &api, RespCommand::Bitcount, &[b"obj"]);
  assert!(
    obj.starts_with(b"-WRONGTYPE"),
    "BITCOUNT 慢臂对象键 WRONGTYPE: {obj:?}"
  );
  assert_eq!(counts(&handle), (f0, n0), "位图慢臂对象键零入账");

  // 快臂对照零改动（既有 None 静默纪律回归锁）
  let cases: &[&[&[u8]]] = &[
    &[b"SETBIT", b"s", b"0", b"1"],
    &[b"GETBIT", b"s", b"0"],
    &[b"BITCOUNT", b"s"],
    &[b"BITPOS", b"s", b"1"],
    &[b"BITFIELD", b"s", b"GET", b"u8", b"0"],
    &[b"BITFIELD_RO", b"s", b"GET", b"u8", b"0"],
  ];
  for args in cases {
    let (resp, _deg) = roundtrip(&rt, &mut c, args);
    assert!(!resp.starts_with(b"-"), "{args:?} 快臂应答 {resp:?}");
    assert_eq!(counts(&handle), (f0, n0), "{args:?} 快臂零入账口径不动");
  }
}

/// 案三主锁：BITOP AND 快慢逐源同账（C# ReadWithUnsafeContext 逐源恰一
/// 帧、dest 不计；MainStoreOps.cs:44/:76/:81 + BitmapOps.cs）。混合命中 +
/// 缺席两源一缺：快臂收尾一次入账 (2,1)（修复前恒零为红灯）、慢臂逐源
/// 漏斗 (2,1) 同账（修复前快臂 0 帧慢臂 2+1 帧失联）
#[test]
fn bitop_fast_slow_per_source_same_accounting() {
  let (rt, mut c, api, handle, _dir, _store) = env("strbitmap-bitop.db");

  assert_eq!(
    roundtrip(&rt, &mut c, &[b"SET", b"a", b"\xff\xff"]).0,
    b"+OK\r\n"
  );
  assert_eq!(
    roundtrip(&rt, &mut c, &[b"SET", b"b", b"\x0f"]).0,
    b"+OK\r\n"
  );

  // 快臂：逐源 2 命中 + 1 缺席合计恰 (found 2, notfound 1)，dest 不计
  let (f0, n0) = counts(&handle);
  let (resp, degraded) = roundtrip(
    &rt,
    &mut c,
    &[b"BITOP", b"AND", b"dst1", b"a", b"b", b"absent"],
  );
  assert!(resp.starts_with(b":"), "BITOP 快臂应答整数帧 {resp:?}");
  assert!(!degraded, "常规存储域 BITOP 快臂不应降级");
  assert_eq!(
    counts(&handle),
    (f0 + 2, n0 + 1),
    "BITOP 快臂逐源补账恰 (2 found, 1 notfound)（修复前 (0,0) 为红灯）"
  );

  // 慢臂：同形逐源同账 (2,1)
  let (f1, n1) = counts(&handle);
  let resp = slow_direct(
    &rt,
    &api,
    RespCommand::BitopAnd,
    &[b"dst2", b"a", b"b", b"absent"],
  );
  assert!(resp.starts_with(b":"), "BITOP 慢臂应答整数帧 {resp:?}");
  assert_eq!(
    counts(&handle),
    (f1 + 2, n1 + 1),
    "BITOP 慢臂逐源 (2,1) 与快臂同账（dest 门静默，修复前 gate 虚增 1 帧为红灯）"
  );

  // 全命中臂对：两源命中 (2,0)
  let (f2, n2) = counts(&handle);
  let (resp, _deg) = roundtrip(&rt, &mut c, &[b"BITOP", b"AND", b"dst3", b"a", b"b"]);
  assert!(resp.starts_with(b":"), "{resp:?}");
  assert_eq!(counts(&handle), (f2 + 2, n2), "BITOP 快臂全命中 (2,0)");

  // 源对象键臂：WRONGTYPE 短路零入账（票面「WRONGTYPE 与降级出口零入账」）
  assert_eq!(
    roundtrip(&rt, &mut c, &[b"HSET", b"obj", b"f", b"v"]).0,
    b":1\r\n"
  );
  let (f3, n3) = counts(&handle);
  let wt = roundtrip(&rt, &mut c, &[b"BITOP", b"AND", b"dst4", b"a", b"obj"]);
  assert!(
    wt.0.starts_with(b"-WRONGTYPE"),
    "BITOP 对象源键应答 WRONGTYPE: {:?}",
    wt.0
  );
  assert_eq!(counts(&handle), (f3, n3), "BITOP WRONGTYPE 短路臂零入账");
}

/// 对照臂不动锁：GET/GETRANGE/STRLEN 慢臂各恰一帧簿记（C# GET 族本有计，
/// read_cold / read_and_frame 入账出口与 GET/GETEX 同线面，本票零改动）
#[test]
fn get_family_control_arms_keep_single_frame() {
  let (rt, mut c, api, handle, _dir, _store) = env("strbitmap-getcontrol.db");

  assert_eq!(
    roundtrip(&rt, &mut c, &[b"SET", b"s", b"abc"]).0,
    b"+OK\r\n"
  );
  let (f0, n0) = counts(&handle);

  // GET 慢臂命中/缺席各恰一帧
  assert_eq!(
    slow_direct(&rt, &api, RespCommand::Get, &[b"s"]),
    bulk_frame(b"abc")
  );
  assert_eq!(counts(&handle), (f0 + 1, n0), "GET 慢臂命中恰 1 found");
  assert_eq!(
    slow_direct(&rt, &api, RespCommand::Get, &[b"absent"]),
    b"$-1\r\n"
  );
  assert_eq!(
    counts(&handle),
    (f0 + 1, n0 + 1),
    "GET 慢臂缺席恰 1 notfound"
  );

  // GETRANGE 命中/缺席各恰一帧（read_and_frame 入账臂不动）
  assert_eq!(
    slow_direct(&rt, &api, RespCommand::Getrange, &[b"s", b"0", b"1"]),
    bulk_frame(b"ab")
  );
  assert_eq!(
    counts(&handle),
    (f0 + 2, n0 + 1),
    "GETRANGE 慢臂命中恰 1 found"
  );
  assert_eq!(
    slow_direct(&rt, &api, RespCommand::Getrange, &[b"absent", b"0", b"1"]),
    bulk_frame(b"")
  );
  assert_eq!(
    counts(&handle),
    (f0 + 2, n0 + 2),
    "GETRANGE 慢臂缺席恰 1 notfound"
  );

  // STRLEN 命中/缺席各恰一帧
  assert_eq!(
    slow_direct(&rt, &api, RespCommand::Strlen, &[b"s"]),
    b":3\r\n"
  );
  assert_eq!(
    counts(&handle),
    (f0 + 3, n0 + 2),
    "STRLEN 慢臂命中恰 1 found"
  );
  assert_eq!(
    slow_direct(&rt, &api, RespCommand::Strlen, &[b"absent"]),
    b":0\r\n"
  );
  assert_eq!(
    counts(&handle),
    (f0 + 3, n0 + 3),
    "STRLEN 慢臂缺席恰 1 notfound"
  );
}

/// 端到端对拍：会话 INFO STATS 的 total_found/total_notfound 行与句柄
/// 快照逐项相等（本票补账经 record_read_outcome/fold_outcome/收尾
/// incr_total_* 全部汇入同一会话共享句柄，INFO 面读取即口径终验）
#[test]
fn info_stats_matches_session_counters_end_to_end() {
  let (rt, mut c, api, handle, _dir, _store) = env("strbitmap-infostats.db");

  // 混打本票全族：SETNX 两态 + SET GET + GETSET + 位图慢臂零计 + BITOP 逐源
  assert_eq!(roundtrip(&rt, &mut c, &[b"SET", b"k", b"v"]).0, b"+OK\r\n");
  assert_eq!(roundtrip(&rt, &mut c, &[b"SETNX", b"k", b"w"]).0, b":0\r\n");
  assert_eq!(roundtrip(&rt, &mut c, &[b"SETNX", b"n", b"v"]).0, b":1\r\n");
  assert_eq!(
    roundtrip(&rt, &mut c, &[b"SET", b"k", b"x", b"GET"]).0,
    bulk_frame(b"v")
  );
  assert_eq!(
    slow_direct(&rt, &api, RespCommand::Setbit, &[b"bm", b"0", b"1"]),
    b":0\r\n"
  );
  assert_eq!(
    slow_direct(&rt, &api, RespCommand::Bitcount, &[b"bm"]),
    b":1\r\n"
  );
  let (resp, _deg) = roundtrip(
    &rt,
    &mut c,
    &[b"BITOP", b"AND", b"d", b"k", b"bm", b"absent"],
  );
  assert!(resp.starts_with(b":"), "{resp:?}");

  // 期望账：SETNX 命中 found+1、SETNX 缺席 notfound+1、SET GET 命中
  // found+1、位图慢臂 0、BITOP 逐源 k/bm 命中 2 found + absent 1 notfound
  // ——合计 (found, notfound) = (4, 2)（SET 裸写族不产计、GET 未打）
  let (f, n) = counts(&handle);
  assert_eq!((f, n), (4, 2), "混打账目锁定 (found, notfound) = (4, 2)");

  // INFO stats 端到端读取与会话句柄快照对拍
  let (info, _deg) = roundtrip(&rt, &mut c, &[b"INFO", b"stats"]);
  assert_eq!(
    info_counts(&info),
    (f, n),
    "INFO STATS total_found/total_notfound 与句柄快照相等"
  );
}
