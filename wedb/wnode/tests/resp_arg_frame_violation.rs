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
  // 长度头终止符不符：首个不符字节 'X'
  assert_frame_disconnects(
    b"*2\r\n$4\r\nECHO\r\n$3\rXabc\r\n",
    b"-ERR Protocol Error: Unexpected character 'X'.\r\n",
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

// ── 命令名侧无上限帽回归（票据 wnode-command-range-cap-permanent-hang）──
//
// C# 事实：GetCommand（RespServerSession.cs:1243-1275）亲验无 512MiB 帽臂，
// 头消费后仅查值收齐（ptr+2 > end → false 等待）与终止符（不符 →
// ThrowUnexpectedToken 断连）两臂；超限声明整帧收齐时正常取出名段、查表
// miss 写 `-ERR unknown command` 后连接存活。rust 侧自设帽臂（裸返 None
// 不置违例）致整帧收齐仍永久无应答挂起，已删除复原为 C# 单形态。

/// 小额等待臂负锁：声明 536870913（超 512MiB 帽常量、int 值域内）的命令名
/// 帧未收齐时须与普通半包同臂等待——返 None、游标不动、不置违例
#[test]
fn over_cap_command_name_header_keeps_waiting() {
  let mut s = session();
  s.recv_buffer.clear();
  s.recv_buffer.extend_from_slice(b"$536870913\r\n");
  s.bytes_read = s.recv_buffer.len();
  s.read_head = 0;
  assert!(
    s.get_command_range().is_none(),
    "值未达须按字节不足等待（C# :1259 ptr+2 > end 臂）"
  );
  assert_eq!(s.read_head, 0, "未到齐不得吞字节");
  assert!(s.parse_violation.is_none(), "等待形不得置违例");
}

/// 查表 miss 应答路径不回归：小额未知命令名整帧收齐时写
/// `-ERR unknown command` 错误帧后继续消费（C# ArrayParseCommand
/// :1335 writeErrorOnFailure 臂）
#[test]
fn unknown_command_reply_path_no_regression() {
  let mut s = session();
  assert_eq!(feed(&mut s, b"*1\r\n$3\r\nXYZ\r\n"), Some(0));
  assert_eq!(drain_output(&mut s), b"-ERR unknown command\r\n");
  assert!(s.parse_violation.is_none());
}

/// 全帧收齐行为锁 + C# 全帧形对拍：声明 ∈ [536870913, 2147483647] 且缓冲
/// 含全帧时，get_command_range 必须取出名段并推进 read_head 过名段
/// （C# GetCommand :1255/:1271 readHead 推进形）；旁路校验入口
/// （with_bypass_buffer 族，write_error_on_failure=false）零写输出、不置
/// 违例。537MB 级缓冲本地对拍专用，CI 默认不跑
#[test]
#[ignore = "537MB 全帧对拍（命令名帽差复原 C# 单形态），内存占用大，CI 默认跳过"]
fn over_cap_full_frame_is_consumed_not_hung() {
  const LEN: usize = 512 * 1024 * 1024 + 1; // 536870913，帽常量紧邻上界

  // 行为锁：直喂超限声明全帧，收齐即消费、游标推进过名段
  let mut s = session();
  s.recv_buffer.clear();
  s.recv_buffer.extend_from_slice(b"$536870913\r\n");
  s.recv_buffer.resize(s.recv_buffer.len() + LEN, b'X');
  s.recv_buffer.extend_from_slice(b"\r\n");
  s.bytes_read = s.recv_buffer.len();
  s.read_head = 0;
  let (start, len) = s
    .get_command_range()
    .expect("整帧收齐的超限命令名须正常取出（C# 无帽形）");
  assert_eq!((start, len), (13, LEN));
  assert_eq!(s.read_head, s.bytes_read, "收齐即推进游标过名段+终止符");
  assert!(s.parse_violation.is_none());
  s.recv_buffer.clear();

  // C# 全帧形经旁路入口：查表 miss → Invalid 滤除返 None，零违例零输出
  let mut frame = Vec::with_capacity(LEN + 32);
  frame.extend_from_slice(b"*1\r\n$536870913\r\n");
  frame.resize(frame.len() + LEN, b'X');
  frame.extend_from_slice(b"\r\n");
  assert!(s.parse_resp_command_buffer(&frame).is_none());
  assert!(s.parse_violation.is_none(), "旁路入口违例哨兵不外溢");
  assert!(
    drain_output(&mut s).is_empty(),
    "旁路校验入口（write_error_on_failure=false）零写输出"
  );
}
