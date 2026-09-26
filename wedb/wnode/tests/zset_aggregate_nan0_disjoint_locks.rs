//! zset 聚合族语义锁（工单 doc-deviations-zset-aggregate-three-divergences，deviations.md §104）
//!
//! 纯登记票零行为改动，本档只锁 §104 两宗与 ZDIFF 翻案面的现行为：
//! 1. 宗一（§104 a）`nan0` 全产点归一：`ZADD k inf m` 后 `ZUNION 1 k WEIGHTS 0`
//!    加权产 NaN（0×inf），rust 归零恒回分值文本 "0"（C# SortedSetUnion 无门会直出 NaN）；
//! 2. 宗二（§104 b）交集收缩异常面消除：`ZINTER 2 a b`（a 含 b 外成员 x，真收缩）
//!    rust 正常回交集成员表且连接存活（C# foreach 体内 pairs.Remove 迭代删字典
//!    必抛 InvalidOperationException 掐连接；对拍段不复刻，skip 注记回指 §104 b）；
//! 3. ZDIFF 负 numkeys 翻案面锁（§104 观察句一，双侧同文，非偏差登记）：
//!    `ZDIFF -1` 回 wrong-number-of-args（C# :915 前置门 / rust :732 同位）、
//!    `ZDIFF -1 k` 回 syntax error（C# :926 对齐门 / rust :739-742 等位对称防御），
//!    锁死翻案裁决防回摆。

use std::sync::Arc;

use compio::runtime::Runtime;
use wnode::resp::{
  garnet_api::{GarnetApi, StoreGarnetApi},
  resp_server_session::{RespServerSession, RespServerSessionOptions},
};
use wnode_test::drain_output;
use wtest_base::open_test_store;

/// 会话级单命令往返（真解析环：帧进 → 分发 → 应答出）
fn roundtrip(session: &mut RespServerSession, frame: &[u8]) -> Vec<u8> {
  session.recv_buffer.extend_from_slice(frame);
  let mut resp_buf = Vec::new();
  let remaining = session.try_consume_messages();
  session.take_output_into(&mut resp_buf);
  Runtime::new()
    .unwrap()
    .block_on(wnode_test::drive_pending_parks(
      session,
      &mut resp_buf,
      true,
    ));
  session.output.extend_from_slice(&resp_buf);
  assert!(remaining.is_some(), "帧应被完整消费: {frame:?}");
  drain_output(session)
}

/// RESP2 数组帧拼装
fn frame(args: &[&str]) -> Vec<u8> {
  let mut buf = format!("*{}\r\n", args.len()).into_bytes();
  for a in args {
    buf.extend_from_slice(format!("${}\r\n{a}\r\n", a.len()).as_bytes());
  }
  buf
}

fn data_session(store: GarnetApi) -> RespServerSession {
  let mut session = RespServerSession::new(1, RespServerSessionOptions::default());
  session.set_garnet_api(store);
  session
}

/// 宗一锁：并集加权产点 NaN 归零，分值恒 "0"（RESP2 扁平 *2n + bulk 分值）
#[test]
fn zunion_nan_weight_zero_score_gate() {
  let (_dir, store) = open_test_store("agg-nan0-zunion.db").unwrap();
  let mut s = data_session(Arc::new(StoreGarnetApi::new(store.new_session().unwrap())));

  // inf 词形双侧同收（rust 按 §2 仅认 inf/+inf/-inf）
  assert_eq!(
    roundtrip(&mut s, &frame(&["ZADD", "k", "inf", "m"])),
    b":1\r\n"
  );
  // 0×inf=NaN 落并集加权产点（write.rs combine_sets :997），nan0 归一恒回 "0"；
  // C# 并集种子 :1222 无门直出 NaN——严禁按 C# 回改（§104 a）
  assert_eq!(
    roundtrip(
      &mut s,
      &frame(&["ZUNION", "1", "k", "WEIGHTS", "0", "WITHSCORES"])
    ),
    b"*2\r\n$1\r\nm\r\n$1\r\n0\r\n"
  );
}

/// 宗二锁：真收缩交集正常回成员表且连接存活（C# 掐连接面不复刻，§104 b）
#[test]
fn zinter_disjoint_no_abort() {
  let (_dir, store) = open_test_store("agg-disjoint-zinter.db").unwrap();
  let mut s = data_session(Arc::new(StoreGarnetApi::new(store.new_session().unwrap())));

  // a ⊄ b：keys[0]=a 含 b 外成员 x，该收缩在 C# 必触发 foreach 体内 Remove
  assert_eq!(
    roundtrip(&mut s, &frame(&["ZADD", "a", "1", "x", "2", "y"])),
    b":2\r\n"
  );
  assert_eq!(
    roundtrip(&mut s, &frame(&["ZADD", "b", "3", "y"])),
    b":1\r\n"
  );
  // 交集 {y: 2+3=5}，回成员表 *1
  assert_eq!(
    roundtrip(&mut s, &frame(&["ZINTER", "2", "a", "b"])),
    b"*1\r\n$1\r\ny\r\n"
  );
  // 连接存活锁：收缩命令后同连接继续正常应答（C# 现形为掐连接）
  assert_eq!(roundtrip(&mut s, &frame(&["PING"])), b"+PONG\r\n");
  assert_eq!(
    roundtrip(&mut s, &frame(&["ZINTER", "2", "a", "b", "WITHSCORES"])),
    b"*2\r\n$1\r\ny\r\n$1\r\n5\r\n"
  );
}

/// ZDIFF 负 numkeys 翻案面锁：与 C# 双门同文（非偏差登记，§104 观察句一）
#[test]
fn zdiff_negative_numkeys_same_text_locks() {
  let (_dir, store) = open_test_store("agg-negnumkeys-zdiff.db").unwrap();
  let mut s = data_session(Arc::new(StoreGarnetApi::new(store.new_session().unwrap())));

  // 「ZDIFF -1」：命令后纯参数数 1 <2 先撞 C# :915-918 前置门；
  // rust write.rs:732 check_arg_count!(2..) 同帧同文
  assert_eq!(
    roundtrip(&mut s, &frame(&["ZDIFF", "-1"])),
    b"-ERR wrong number of arguments for 'ZDIFF' command\r\n"
  );
  // 「ZDIFF -1 k」：过前置门后 C# :926 对齐门（Count-1=1 ≠ nKeys=-1 且 ≠ nKeys+1=0）
  // 必回 syntax error，:932 负界构造不可达；rust :739-742 拒绝臂等位对称防御同文
  assert_eq!(
    roundtrip(&mut s, &frame(&["ZDIFF", "-1", "k"])),
    b"-ERR syntax error\r\n"
  );
}
