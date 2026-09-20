//! 服务端参数帧「违例 / 未到齐」二分回归
//!（票据 qw-net-parse-violation-single-length-header）
//!
//! C# 事实：libs/server/Resp/Parser/SessionParseState.cs:Read 对帧头非法、
//! 无数字、值尾终止符不符一律 `throw RespParsingException`，由上层 catch
//! 写 `ERR Protocol Error: {msg}` 后断连，仅 `ptr > end`（字节未到齐）返回
//! false 等待。rust 侧此前四类统一 false，`resp_command.rs` 装载解析态时据
//! false 回退游标等待 → 真畸形帧游标永不前进，会话既不回错也不断连
//!（永久挂死）。现与命令名侧（`get_command_range`）共用 `parse_violation`
//! 同一违例载体，帧头判定共用 `wresp::read` 单点。

use wnode::resp::resp_server_session::{RespServerSession, RespServerSessionOptions};
use wnode_test::drain_output;

fn session() -> RespServerSession {
  RespServerSession::new(0, RespServerSessionOptions::default())
}

/// 泵直填一批字节并消费（`None` = 应答已落输出、连接须断）
fn feed(s: &mut RespServerSession, bytes: &[u8]) -> Option<usize> {
  s.recv_buffer.extend_from_slice(bytes);
  s.try_consume_messages()
}

/// 畸形参数帧须在首个完整帧到达即断连：回 `None`、写
/// `ERR Protocol Error: {文案}`、违例哨兵消费复位
fn assert_frame_disconnects(frame: &[u8], expected: &[u8]) {
  let mut s = session();
  assert_eq!(feed(&mut s, frame), None, "{frame:?} 须以断连收口");
  assert_eq!(drain_output(&mut s), expected, "{frame:?}");
  assert!(s.parse_violation.is_none(), "哨兵应被消费复位");
}

#[test]
fn malformed_argument_frame_disconnects() {
  // 参数头非 `$` sigil
  assert_frame_disconnects(
    b"*2\r\n$4\r\nECHO\r\n:5\r\n",
    b"-ERR Protocol Error: Unexpected character ':'.\r\n",
  );
  // 参数头无数字（参数不作大写化，逐字节回显）
  assert_frame_disconnects(
    b"*2\r\n$4\r\nECHO\r\n$ab\r\n",
    b"-ERR Protocol Error: Unexpected character 'a'.\r\n",
  );
  // 长度头终止符不符
  assert_frame_disconnects(
    b"*2\r\n$4\r\nECHO\r\n$3\rXabc\r\n",
    b"-ERR Protocol Error: Unexpected character '\\x0d'.\r\n",
  );
  // 值尾终止符不符（旧路径按 false 回退等待 → 挂死）
  assert_frame_disconnects(
    b"*2\r\n$4\r\nECHO\r\n$3\r\nabcd\r\n",
    b"-ERR Protocol Error: Unexpected character 'd'.\r\n",
  );
  // 负长度（请求参数无 NULL 形态）
  assert_frame_disconnects(
    b"*2\r\n$4\r\nECHO\r\n$-1\r\n",
    b"-ERR Protocol Error: Invalid string length '-1'.\r\n",
  );
  // 长度数值溢出（文案回显数字串，C# ThrowIntegerOverflow 同源）
  assert_frame_disconnects(
    b"*2\r\n$4\r\nECHO\r\n$3000000000\r\n",
    b"-ERR Protocol Error: Unable to parse integer. The given number is larger than allowed: 3000000000\r\n",
  );
}

#[test]
fn leading_plus_argument_length_rejected() {
  // C# RespReadUtils.cs:362 帧头只认 '-'，`+` 前导落 digitsRead==0 抛
  // UnexpectedToken（:404）；rust 内联头旧实现额外吞 `+`，多接受了 C# 拒绝的输入
  assert_frame_disconnects(
    b"*2\r\n$4\r\nECHO\r\n$+3\r\nabc\r\n",
    b"-ERR Protocol Error: Unexpected character '+'.\r\n",
  );
  // 命令名侧同一单点判据（收敛后不再有第二套容度）
  assert_frame_disconnects(
    b"*1\r\n$+4\r\nECHO\r\n",
    b"-ERR Protocol Error: Unexpected character '+'.\r\n",
  );
  // 数组头同理（同形头，共用单点解码）
  assert_frame_disconnects(
    b"*+1\r\n$4\r\nPING\r\n",
    b"-ERR Protocol Error: Unexpected character '+'.\r\n",
  );
}

#[test]
fn half_packet_argument_frame_keeps_waiting() {
  let mut s = session();
  let half = &b"*2\r\n$4\r\nECHO\r\n$5\r\nabc"[..];
  // 负载未达：非违例，整批游标回退、零应答
  assert_eq!(feed(&mut s, half), Some(half.len()));
  assert!(drain_output(&mut s).is_empty());
  assert!(s.parse_violation.is_none());
  assert_eq!(s.read_head, 0, "半包不得吞字节");

  // 补齐后续字节即整帧消费（未到齐与违例两态行为分别可证）
  assert_eq!(feed(&mut s, b"de\r\n"), Some(0));
  assert_eq!(drain_output(&mut s), b"$5\r\nabcde\r\n");
}

#[test]
fn malformed_arg_frame_terminates_batch_after_prior_replies() {
  // 同批前序命令应答不丢，协议错误最后落位（C# catch 块序）
  let mut s = session();
  assert_eq!(
    feed(
      &mut s,
      b"*1\r\n$4\r\nPING\r\n*2\r\n$4\r\nECHO\r\n$3\r\nabcd\r\n"
    ),
    None
  );
  assert_eq!(
    drain_output(&mut s),
    b"+PONG\r\n-ERR Protocol Error: Unexpected character 'd'.\r\n"
  );
}
