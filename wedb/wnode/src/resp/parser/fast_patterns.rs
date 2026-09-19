//! 固定形状热命令模式表（对标 libs/server/Resp/Parser/RespCommandSimdPatterns.cs）

use wresp::command::RespCommand;

/// 固定形状热命令模式表：`( RESP 帧前缀, 命令, 参数个数 )`
///
/// 帧字节逐项对齐 RespCommandSimdPatterns.cs 的 RespPattern(argCount, cmd)：
/// `*N` 的 N = 参数个数 + 1（数组元素总数，含命令名）；13..15 字节模式在
/// C# 以掩码忽略模式长度之后的字节，16 字节模式（6 字符命令）为全等比较。
pub(crate) static FAST_PATTERN_TABLE: &[(&[u8], RespCommand, u8)] = &[
  // 13 字节：3 字符命令
  (b"*2\r\n$3\r\nGET\r\n", RespCommand::Get, 1),
  (b"*3\r\n$3\r\nSET\r\n", RespCommand::Set, 2),
  (b"*2\r\n$3\r\nDEL\r\n", RespCommand::Del, 1),
  (b"*2\r\n$3\r\nTTL\r\n", RespCommand::Ttl, 1),
  // 14 字节：4 字符命令
  (b"*1\r\n$4\r\nPING\r\n", RespCommand::Ping, 0),
  (b"*2\r\n$4\r\nINCR\r\n", RespCommand::Incr, 1),
  (b"*2\r\n$4\r\nDECR\r\n", RespCommand::Decr, 1),
  (b"*1\r\n$4\r\nEXEC\r\n", RespCommand::Exec, 0),
  (b"*2\r\n$4\r\nPTTL\r\n", RespCommand::Pttl, 1),
  // 15 字节：5 字符命令
  (b"*1\r\n$5\r\nMULTI\r\n", RespCommand::Multi, 0),
  (b"*3\r\n$5\r\nSETNX\r\n", RespCommand::Setnx, 2),
  (b"*4\r\n$5\r\nSETEX\r\n", RespCommand::Setex, 3),
  // 16 字节：6 字符命令（无掩码，全等）
  (b"*2\r\n$6\r\nEXISTS\r\n", RespCommand::Exists, 1),
  (b"*2\r\n$6\r\nGETDEL\r\n", RespCommand::Getdel, 1),
  (b"*3\r\n$6\r\nAPPEND\r\n", RespCommand::Append, 2),
  (b"*3\r\n$6\r\nINCRBY\r\n", RespCommand::Incrby, 2),
  (b"*3\r\n$6\r\nDECRBY\r\n", RespCommand::Decrby, 2),
  (b"*4\r\n$6\r\nPSETEX\r\n", RespCommand::Psetex, 3),
];

/// 模式比较（C# Vector128 载入 + 掩码 + EqualsAll 的标量等价：
/// 仅比较模式长度内的字节，模式之后的输入字节不作约束）
#[inline]
pub(crate) fn pattern_matches(buffer: &[u8], start: usize, pattern: &[u8]) -> bool {
  buffer.len() >= start + pattern.len() && &buffer[start..start + pattern.len()] == pattern
}
