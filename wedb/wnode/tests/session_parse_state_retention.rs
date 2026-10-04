//! 会话解析态根缓冲保留上限回归（PR #2157）
//!
//! 对标 garnet/test/standalone/Garnet.test/SessionBufferRetentionTests.cs 的
//! parse-state 臂（ParseStateDoesNotRetainAfterOneWideCommand /
//! WideCommandsRemainCorrectAcrossShrinkAndRegrow）：
//! libs/server/Resp/Parser/SessionParseState.cs 根缓冲按会话发过的最宽数组
//! 定容并钉死连接终身，一条超高元命令即永久放大会话、成本随连接数伸缩。
//! 收缩以批边界倒计数 trim 承接（RespServerSession.cs:442-480：
//! SessionTrimInterval=64、parseStateShrinkThreshold=1024）：容量高于帽且
//! 自上次 trim 未增长才释放——两次 trim 间隔内完成回收，宽命令会话保留
//! 其容量。

use std::str;

use wnode::resp::resp_server_session::{
  RespServerSession, SESSION_PARSE_STATE_MAX_RETAINED_ARGS, SESSION_TRIM_INTERVAL,
};
use wnode_test::drain_output;

/// 直填会话接收缓冲并泵完整解析分派
fn feed(s: &mut RespServerSession, bytes: &[u8]) -> Option<usize> {
  s.recv_buffer.extend_from_slice(bytes);
  let remaining = s.try_consume_messages();
  let mut out = Vec::new();
  s.take_output_into(&mut out, true);
  drain_output(s);
  remaining
}

/// 组装一条 n 参宽命令帧（EXISTS key1..keyN——解析态按数组元数定容）
fn wide_exists_frame(n: usize) -> Vec<u8> {
  let mut frame = format!("*{}\r\n$6\r\nEXISTS\r\n", n + 1).into_bytes();
  for i in 0..n {
    let key = format!("key{i}");
    frame.extend_from_slice(format!("${}\r\n{key}\r\n", key.len()).as_bytes());
  }
  frame
}

/// 单参小命令帧（PING）
const PING: &[u8] = b"*1\r\n$4\r\nPING\r\n";

/// ParseStateDoesNotRetainAfterOneWideCommand：一条宽命令后，trim 倒计数
///（最多两个间隔）把根缓冲收回帽内
#[test]
fn parse_state_does_not_retain_after_one_wide_command() {
  let mut s = RespServerSession::default();
  // 一条 2000 参宽命令：根缓冲按最宽元数定容（> 1024 帽）
  feed(&mut s, &wide_exists_frame(2000));
  let grown = s.parse_state.root_buffer.len();
  assert!(
    grown > SESSION_PARSE_STATE_MAX_RETAINED_ARGS,
    "前置：宽命令须先放大根缓冲（现 {grown}）"
  );

  // 第一个 trim 间隔（64 批小命令）：首次 trim 只记录水位不释放
  //（增长信号：上次 trim 后从未观察，容量 > 记录 0 不满足「未增长」）
  for _ in 0..SESSION_TRIM_INTERVAL {
    feed(&mut s, PING);
  }
  assert_eq!(
    s.parse_state.root_buffer.len(),
    grown,
    "首个间隔只记录不释放"
  );

  // 第二个 trim 间隔：容量未再增长 → 收缩回帽
  for _ in 0..SESSION_TRIM_INTERVAL {
    feed(&mut s, PING);
  }
  assert_eq!(
    s.parse_state.root_buffer.len(),
    SESSION_PARSE_STATE_MAX_RETAINED_ARGS,
    "两个间隔内必须收回帽内"
  );
}

/// 每批都需要容量的会话不被误缩：持续宽命令下容量保持（增长信号刷新记录）
#[test]
fn recurring_wide_commands_retain_capacity() {
  let mut s = RespServerSession::default();
  for _ in 0..(SESSION_TRIM_INTERVAL * 3) {
    feed(&mut s, &wide_exists_frame(2000));
  }
  assert!(
    s.parse_state.root_buffer.len() > SESSION_PARSE_STATE_MAX_RETAINED_ARGS,
    "每批都需要的容量必须保留（C# 迟滞设计意图）"
  );
}

/// WideCommandsRemainCorrectAcrossShrinkAndRegrow：收缩后再发宽命令，
/// 根缓冲按需重长、命令语义正确（EXISTS 缺执行域按错误应答仍可组帧）
#[test]
fn wide_commands_remain_correct_across_shrink_and_regrow() {
  let mut s = RespServerSession::default();
  feed(&mut s, &wide_exists_frame(2000));
  for _ in 0..(SESSION_TRIM_INTERVAL * 2 + 2) {
    feed(&mut s, PING);
  }
  assert_eq!(
    s.parse_state.root_buffer.len(),
    SESSION_PARSE_STATE_MAX_RETAINED_ARGS
  );

  // 重长：宽命令照常完整解析（此处无存储执行域，EXISTS 落错误应答路径，
  // 但解析须吃进全部 2001 元不残留半包）
  let remaining = feed(&mut s, &wide_exists_frame(2000));
  assert_eq!(remaining, Some(0), "收缩后宽命令须被完整消费");
  // 槽位仅承载参数（命令名不入槽）：2000 参 → 根缓冲 ≥ 2000
  assert!(
    s.parse_state.root_buffer.len() >= 2000,
    "重长须按需恢复容量（现 {}）",
    s.parse_state.root_buffer.len()
  );

  // 小命令不受影响
  let out = {
    s.recv_buffer.extend_from_slice(PING);
    s.try_consume_messages();
    drain_output(&mut s)
  };
  assert_eq!(out, b"+PONG\r\n");
  let _ = str::from_utf8(&out).unwrap();
}
