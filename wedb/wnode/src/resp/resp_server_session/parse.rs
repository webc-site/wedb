//! 命令名与区间解析（对标 libs/server/Resp/RespServerSession.cs:GetCommand
//! 解析内核、MakeUpperCase 与 libs/common/Parsing/RespParsingException.cs
//! 违例臂；投机式 GET 前视对标 libs/server/Resp/BasicCommands.cs:
//! NextCommandMaybeGet / ParseGETAndKey）。

use wresp::{
  Error, argslice::ArgSlice, command::RespCommand, read::try_read_unsigned_length_header,
  session_parse_state::MAX_ARGUMENT_LENGTH_BYTES,
};

use super::core::RespServerSession;
use crate::resp::parser::resp_command::MAX_RESP_ARRAY_LENGTH;

/// RESP framing of `GET key` up to and including the command token: `*2\r\n$3\r\nGET\r\n`
/// (libs/server/Resp/BasicCommands.cs:GetCommandRespPrefix)
pub const GET_COMMAND_RESP_PREFIX: &[u8] = b"*2\r\n$3\r\nGET\r\n";

impl RespServerSession {
  /// libs/server/Resp/BasicCommands.cs:NextCommandMaybeGet
  ///
  /// 前视探查接收缓冲中下一条命令是否可能为 `GET key`（以 `*2\r\n$3\r\nGET\r\n` 快速前缀比对）
  #[inline]
  pub fn next_command_maybe_get(&self) -> bool {
    let head = self.end_read_head;
    let end = self.bytes_read;
    if head >= end {
      return false;
    }
    self
      .recv_buffer
      .get(head..end)
      .is_some_and(|buf| buf.starts_with(GET_COMMAND_RESP_PREFIX))
  }

  /// libs/server/Resp/BasicCommands.cs:ParseGETAndKey
  ///
  /// 投机式前视解析下一条命令是否为 `GET` 并提取其 `key` 参数。
  /// 命中时推进游标并返回其 key 的 ArgSlice；未命中或校验失败时回退游标并返回 None。
  pub fn parse_get_and_key(&mut self) -> Option<ArgSlice> {
    if !self.next_command_maybe_get() {
      return None;
    }
    let old_end_read_head = self.end_read_head;
    self.read_head = old_end_read_head;
    let Some(cmd) = self.parse_command() else {
      self.read_head = old_end_read_head;
      self.end_read_head = old_end_read_head;
      return None;
    };
    if cmd != RespCommand::Get || self.parse_state.count != 1 {
      self.read_head = old_end_read_head;
      self.end_read_head = old_end_read_head;
      return None;
    }
    if self.cluster_session.is_some() && !self.can_serve_slot_no_response(RespCommand::Get) {
      self.read_head = old_end_read_head;
      self.end_read_head = old_end_read_head;
      return None;
    }
    Some(self.parse_state.get_arg_slice_by_ref(0))
  }

  /// libs/server/Resp/RespServerSession.cs:MakeUpperCase
  ///
  /// 就地大写化首条命令名；返回是否发生改写。C# 位技巧快路径按
  /// "常见命令已大写" 假设跳过全扫描，语义保留。
  pub fn make_upper_case(&mut self, ptr: usize, len: usize) -> bool {
    let buffer = &mut self.recv_buffer;
    let end = (ptr + len).min(buffer.len());
    // 常见场景：命令名已全大写 → 不改写（返回 false）
    let mut changed = false;
    let mut i = ptr;
    while i < end {
      if buffer[i] > 64 {
        // 找到命令名起点
        while i < end && buffer[i] > 32 && buffer[i] < 123 {
          if buffer[i] > 96 {
            buffer[i] -= 32;
            changed = true;
          }
          i += 1;
        }
        return changed;
      }
      i += 1;
    }
    false
  }

  /// libs/common/Parsing/RespParsingException.cs:ThrowUnexpectedToken
  ///
  /// 置协议违规哨兵，文案 `Unexpected character '{escaped}'.`（控制字符
  /// 以 `\xNN` 转义；C# 以异常抛出，rust 以哨兵携带文案供消费入口写
  /// `ERR Protocol Error: {msg}`）
  pub(crate) fn violation_unexpected_token(&mut self, token: u8) {
    let escaped = if token.is_ascii_control() {
      format!("\\x{token:02x}")
    } else {
      (token as char).to_string()
    };
    self.parse_violation = Some(format!("Unexpected character '{escaped}'."));
  }

  /// libs/common/Parsing/RespParsingException.cs:ThrowInvalidStringLength
  ///
  /// 置协议违规哨兵，文案 `Invalid string length '{len}'.`
  pub(crate) fn violation_invalid_string_length(&mut self, len: i64) {
    self.parse_violation = Some(format!("Invalid string length '{len}'."));
  }

  /// libs/common/Parsing/RespParsingException.cs:ThrowExcessiveArgumentCount
  ///
  /// 置协议违规哨兵，文案
  /// `RESP array argument count '{count}' exceeds maximum allowed count of '{max}'.`
  pub(crate) fn violation_excessive_arg_count(&mut self, count: isize) {
    self.parse_violation = Some(format!(
      "RESP array argument count '{count}' exceeds maximum allowed count of '{}'.",
      MAX_RESP_ARRAY_LENGTH
    ));
  }

  /// libs/common/Parsing/RespParsingException.cs:ThrowIntegerOverflow
  ///
  /// 置协议违规哨兵，文案
  /// `Unable to parse integer. The given number is larger than allowed: {digits}`
  ///（C# 携 ASCII 数字串，即 [`wresp::Error::IntegerOverflow`] 的 digits 载荷）
  pub(crate) fn violation_integer_overflow(&mut self, digits: &str) {
    self.parse_violation = Some(format!(
      "Unable to parse integer. The given number is larger than allowed: {digits}"
    ));
  }

  /// 帧头解码单点（[`wresp::read`]）的违例 → 会话违例载体转接：
  /// C# `RespParsingException` 三抛臂逐一对位（文案单点见各 `violation_*`），
  /// 命令名侧与参数侧共用本口，全仓仅一套协议违例表达
  pub(crate) fn violation_parse_error(&mut self, err: Error) {
    match err {
      Error::UnexpectedToken(token) => self.violation_unexpected_token(token),
      Error::InvalidStringLength(len) => self.violation_invalid_string_length(i64::from(len)),
      Error::IntegerOverflow { digits } => self.violation_integer_overflow(&digits),
    }
  }

  /// libs/server/Resp/RespServerSession.cs:GetCommand 的区间解析内核（零拷贝
  /// 返回 recv_buffer 内 (start, len) 视图，对标 C# ReadOnlySpan 返回）。
  ///
  /// 从接收缓冲读取命令名（$len\r\n...\r\n）的区间 (start, len)，推进
  /// read_head；长度头解码复用 [`wresp::read::try_read_unsigned_length_header`]
  /// 单点（C# :1249 同一函数），三态与 C# 逐臂对齐：
  /// - 字节未到齐（头不足 3 字节 / 值或值尾未达）→ None 等待，游标不动
  ///   （断包可安全重试）
  /// - 协议违例（非法 sigil / 无数字 / 终止符不符 / 负长度含 `_\r\n`、
  ///   `$-1\r\n` / 数值溢出，C# 抛 RespParsingException 断连）→ 置
  ///   [`Self::parse_violation`] 后 None，经既有 violation 路径写
  ///   `ERR Protocol Error` 后断连
  /// - 超 [`MAX_ARGUMENT_LENGTH_BYTES`]：C# GetCommand 无超限断连
  ///   （SessionParseState.Read 同上限仅返回 false），按字节不足等待
  pub fn get_command_range(&mut self) -> Option<(usize, usize)> {
    let end = self.bytes_read;
    let mut head: &[u8] = &self.recv_buffer[self.read_head..end];
    let mut length = 0;
    let header = try_read_unsigned_length_header(&mut length, &mut head, b'$');
    // 头消费后的值起点（head 为 [值起点..end] 的余量视图）；取值即终结对
    // 接收缓冲的借用，其后违例转接才能可变借用会话
    let value_start = end - head.len();
    let length = length as usize;
    match header {
      Ok(true) => {}
      // 头部未到齐（C# ptr+3 > end / readHead+4 > end / 终止符两字节未达）
      Ok(false) => return None,
      Err(err) => {
        self.violation_parse_error(err);
        return None;
      }
    }

    // 超 512MB 上限：按字节不足等待（见文档注释）
    if length > MAX_ARGUMENT_LENGTH_BYTES {
      return None;
    }
    // 命令值 + 结尾（数据未收齐时不移动 read_head，保证断包可安全重试；
    // 值尾终止符不符 C# GetCommand:1267 ThrowUnexpectedToken 断连）
    if value_start + length + 2 > end {
      return None;
    }
    if &self.recv_buffer[value_start + length..value_start + length + 2] != b"\r\n" {
      let token = self.recv_buffer[value_start + length];
      self.violation_unexpected_token(token);
      return None;
    }
    self.read_head = value_start + length + 2;
    Some((value_start, length))
  }

  /// 同 get_command_range，并就地大写化
  #[inline]
  pub fn get_upper_case_command_range(&mut self) -> Option<(usize, usize)> {
    let (start, len) = self.get_command_range()?;
    self.recv_buffer[start..start + len].make_ascii_uppercase();
    Some((start, len))
  }
}
