//! DUMP 读臂命中/未命中入账单条口径回归（票 zcode-r131c-dumprest 案二，P3）
//!
//! 缺陷面：C# DUMP 经 storageApi.GET（KeyAdminCommands.cs:142），该 API 即
//! 计数 GET（MainStoreOps.cs:15-42：Found→incr_session_found、NotFound→
//! incr_session_notfound、WrongType 双臂均静默），快慢（pending）通道同口径
//! 各一条。rust 两臂俱脱轨：
//! - 快臂 network_dump 的 read_user_sync 第三参传 None → 命中/缺失零入账；
//! - 慢臂 read_user_with_prefix 三域逐探各经 read_tag_with_prefix 簿记口入账
//!   → 缺失键 String/Envelope/Meta 三探计 3 条 notfound、对象键计 1 notfound
//!   +1 found，虚报与漏报并存且双臂自异。
//!
//! 修复形态：入账自 read_tag_with / read_tag_with_prefix 尾部上提至薄包装
//! 入口（单域消费者如 read_string_with 现口径不动，防静默丢计），
//! read_user_with_prefix 三域探针改走不入账静默内核 read_tag_quiet*，漏斗
//! 出口按 UserReadAsync::record_outcome（与快路径 UserRead::record_outcome
//! 同一 fold_outcome 折叠规则单点）恰一条；快臂改传 self.session_metrics
//! （与 get.rs 同形）。
//!
//! 测试全真存储真协议帧，无 mock：快臂经会话消费者生产口驱动、慢臂
//! SlowWait::for_command 直驱（夹具先例 restore_10byte_payload_slice.rs
//! 慢臂同形段），断言 session 指标 total_found/total_notfound 在 DUMP
//! 命中/缺失/对象键三形下增量 1/0、0/1、0/0，双臂同构；顺带锁同漏斗
//! 其余消费臂（STRLEN 慢臂缺失恒一条 notfound，修复前三条）。

use compio::runtime::Runtime;
use wnode::resp::{garnet_api::GarnetApi, slow_path::SlowWait};
use wnode_test::{counts, metrics_env as env, roundtrip};
use wresp::command::RespCommand;

/// 慢臂 DUMP 帧型版本入参（与 RESP2 快臂同版）
const RESP_V2: u8 = 2;

/// 慢臂直驱（与降级快照投递同径，不经会话快路径）
fn slow_direct(rt: &Runtime, api: &GarnetApi, cmd: RespCommand, args: &[&[u8]]) -> Vec<u8> {
  let snapshot: Vec<Vec<u8>> = args.iter().map(|a| a.to_vec()).collect();
  rt.block_on(async {
    SlowWait::for_command(api, cmd, snapshot, RESP_V2)
      .resolve()
      .await
  })
}

/// DUMP 快臂三形入账：命中 found+1、缺失 notfound+1、对象键 0/0 静默
/// （对标 C# MainStoreOps GET 单条口径；修复前快臂传 None 恒零入账）
#[test]
fn dump_fast_arm_records_single_outcome_per_shape() {
  let (rt, mut c, _api, handle, _dir, _store) = env("dump-account-fast.db");

  assert_eq!(roundtrip(&rt, &mut c, &[b"SET", b"k", b"v"]), b"+OK\r\n");
  assert_eq!(
    roundtrip(&rt, &mut c, &[b"HSET", b"obj", b"f", b"v"]),
    b":1\r\n"
  );

  // 命中 → found+1
  let (f0, n0) = counts(&handle);
  let fast_hit = roundtrip(&rt, &mut c, &[b"DUMP", b"k"]);
  assert!(
    fast_hit.starts_with(b"$"),
    "命中 DUMP 应答必为 bulk: {fast_hit:?}"
  );
  assert_eq!(counts(&handle), (f0 + 1, n0), "DUMP 命中恰计 1 found");

  // 缺失 → notfound+1
  let (f1, n1) = counts(&handle);
  assert_eq!(roundtrip(&rt, &mut c, &[b"DUMP", b"missing"]), b"$-1\r\n");
  assert_eq!(
    counts(&handle),
    (f1, n1 + 1),
    "DUMP 缺失恰计 1 notfound（修复前零入账）"
  );

  // 对象键 → nil 且 0/0（C# WRONGTYPE 静默同口径）
  let (f2, n2) = counts(&handle);
  assert_eq!(roundtrip(&rt, &mut c, &[b"DUMP", b"obj"]), b"$-1\r\n");
  assert_eq!(
    counts(&handle),
    (f2, n2),
    "DUMP 对象键静默不计数（WrongType 折叠）"
  );
}

/// DUMP 慢臂三形入账与双臂同构：直驱 SlowWait 各恰一条（修复前缺失 3、
/// 对象 2、命中 1 偶合），应答与快臂逐字节同帧
#[test]
fn dump_slow_arm_records_single_outcome_and_matches_fast() {
  let (rt, mut c, api, handle, _dir, _store) = env("dump-account-slow.db");

  assert_eq!(roundtrip(&rt, &mut c, &[b"SET", b"k", b"v"]), b"+OK\r\n");
  assert_eq!(
    roundtrip(&rt, &mut c, &[b"HSET", b"obj", b"f", b"v"]),
    b":1\r\n"
  );

  // 命中 → found+1；双臂同输入同帧（双臂同构锁，载荷帧逐字节相等）
  let (f0, n0) = counts(&handle);
  let fast_hit = roundtrip(&rt, &mut c, &[b"DUMP", b"k"]);
  let slow_hit = slow_direct(&rt, &api, RespCommand::Dump, &[b"k"]);
  assert_eq!(
    counts(&handle),
    (f0 + 2, n0),
    "DUMP 命中快慢臂各恰 1 found（修复前快臂 0、慢臂 1 偶合）"
  );
  assert_eq!(
    slow_hit, fast_hit,
    "慢臂 DUMP 组帧与快臂逐字节同形（修复前后行为零漂移段）"
  );

  // 缺失 → notfound+1（修复前三域逐探计 3 条）
  let (f1, n1) = counts(&handle);
  assert_eq!(
    slow_direct(&rt, &api, RespCommand::Dump, &[b"missing"]),
    b"$-1\r\n"
  );
  assert_eq!(
    counts(&handle),
    (f1, n1 + 1),
    "慢臂 DUMP 缺失恰 1 notfound（修复前虚报 3 条）"
  );

  // 对象键 → 0/0（修复前 1 notfound + 1 found 虚报）
  let (f2, n2) = counts(&handle);
  assert_eq!(
    slow_direct(&rt, &api, RespCommand::Dump, &[b"obj"]),
    b"$-1\r\n"
  );
  assert_eq!(counts(&handle), (f2, n2), "慢臂 DUMP 对象键静默不计数");
}

/// 同漏斗其余消费臂收敛锁：STRLEN 慢臂（read_user 共用面）缺失键恰一条
/// notfound（修复前三探计 3 条）、对象键 WRONGTYPE 静默
#[test]
fn strlen_slow_arm_shares_folded_funnel() {
  let (rt, mut c, api, handle, _dir, _store) = env("dump-account-strlen.db");

  assert_eq!(
    roundtrip(&rt, &mut c, &[b"HSET", b"obj", b"f", b"v"]),
    b":1\r\n"
  );

  let (f0, n0) = counts(&handle);
  assert_eq!(
    slow_direct(&rt, &api, RespCommand::Strlen, &[b"missing"]),
    b":0\r\n"
  );
  assert_eq!(
    counts(&handle),
    (f0, n0 + 1),
    "STRLEN 慢臂缺失恰 1 notfound（同漏斗出口折叠，修复前 3 条）"
  );

  let (f1, n1) = counts(&handle);
  let out = slow_direct(&rt, &api, RespCommand::Strlen, &[b"obj"]);
  assert!(
    out.starts_with(b"-WRONGTYPE"),
    "STRLEN 对象键 WRONGTYPE: {out:?}"
  );
  assert_eq!(counts(&handle), (f1, n1), "STRLEN 对象键静默不计数");
}
