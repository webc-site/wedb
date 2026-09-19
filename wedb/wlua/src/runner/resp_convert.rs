//! 在 garnet 中的相对路径:libs/server/Lua/LuaRunner.cs
//!
//! RESP 输出通道与响应转换：RESP 值 ↔ Lua 栈值的双向写出
//! （对标 LuaRunner.cs 响应写出/解析部分）。

use std::str;

use wresp::{
  read::{
    try_read_as_span, try_read_signed_array_length, try_read_signed_map_length,
    try_read_signed_set_length, try_read_span_with_length_header, try_read_unsigned_array_length,
    try_read_verbatim_string_length,
  },
  resp_memory_writer::{Resp3, RespWriter},
};

use super::LuaRunner;
use crate::{LuaState, strings::ConstantStrings};

/// RESP 输出通道（对标 IResponseAdapter：会话缓冲或 runner 内存缓冲；
/// 对齐 C# 侧 adapter 只暴露 BufferCur/BufferEnd/RespProtocolVersion 的形态，
/// RESP 写出由调用点就地构造 [`RespWriter`] 承接，不在本类型上镜像写出原语）。
pub struct RespOut<'a> {
  /// 响应写入缓冲。
  pub buf: &'a mut Vec<u8>,
  /// RESP 协议版本。
  pub protocol_version: u8,
}

impl<'a> RespOut<'a> {
  /// 会话侧输出。
  #[inline]
  pub fn session(buf: &'a mut Vec<u8>, protocol_version: u8) -> Self {
    Self {
      buf,
      protocol_version,
    }
  }

  /// runner 侧输出（恒 RESP2，对标 RunnerAdapter.RespProtocolVersion = 2）。
  #[inline]
  pub fn runner(buf: &'a mut Vec<u8>) -> Self {
    Self {
      buf,
      protocol_version: 2,
    }
  }

  /// 在 garnet 中的相对路径:libs/server/Lua/LuaRunner.cs:SendAndReset（IResponseAdapter 适配点）
  ///
  /// Vec 自增长，无缓冲翻转（C# 会话/runner 适配器的刷新点恒真成功）。
  pub fn send_and_reset(&mut self) {}
}

/// RunForRunner 的结构化返回（C# object 形态）。
#[derive(Debug, Clone, PartialEq)]
pub enum RespObject {
  /// `+` 简单串。
  SimpleString(String),
  /// `:` 整数。
  Integer(i64),
  /// `$` 批量串。
  BulkString(Vec<u8>),
  /// `$-1` 空。
  Null,
  /// `*` 数组。
  Array(Vec<RespObject>),
}

impl LuaRunner {
  /// 在 garnet 中的相对路径:libs/server/Lua/LuaRunner.cs:MapRespToObject
  pub(super) fn map_resp_to_object(&mut self, cursor: &mut &[u8]) -> Result<RespObject, String> {
    match cursor.first() {
      Some(b'+') => {
        let mut simple_str: &[u8] = &[];
        if !matches!(try_read_as_span(&mut simple_str, cursor), Ok(true)) {
          return Err("Unexpected simple string".into());
        }
        Ok(RespObject::SimpleString(
          String::from_utf8_lossy(simple_str).into_owned(),
        ))
      }
      Some(b':') => {
        let Some(int64) = read_resp_int(cursor) else {
          return Err("Unexpected integer".into());
        };
        Ok(RespObject::Integer(int64))
      }
      // Error ('-') is handled before call to MapRespToObject
      Some(b'$') => {
        if cursor.len() >= 5 && &cursor[1..5] == b"-1\r\n" {
          *cursor = &cursor[5..];
          return Ok(RespObject::Null);
        }
        let mut bulk_str: &[u8] = &[];
        if !matches!(
          try_read_span_with_length_header(&mut bulk_str, cursor),
          Ok(true)
        ) {
          return Err("Unexpected bulk string".into());
        }
        Ok(RespObject::BulkString(bulk_str.to_vec()))
      }
      Some(b'*') => {
        let mut item_count = 0i32;
        if !matches!(
          try_read_unsigned_array_length(&mut item_count, cursor),
          Ok(true)
        ) {
          return Err("Unexpected array".into());
        }
        let mut array = Vec::with_capacity(item_count as usize);
        for _ in 0..item_count {
          array.push(self.map_resp_to_object(cursor)?);
        }
        Ok(RespObject::Array(array))
      }
      other => Err(format!(
        "Unexpected sigil {}",
        other.map(|c| *c as char).unwrap_or('\0')
      )),
    }
  }

  /// 在 garnet 中的相对路径:libs/server/Lua/LuaRunner.cs:WriteResponse
  ///
  /// 栈顶（若有）值 → RESP 回复写入 `resp`。
  pub fn write_response(&mut self, resp: &mut RespOut) {
    if self.state.get_top() == 0 {
      // 顶层 null 无需栈空间，恒可写出。
      if resp.protocol_version == 3 {
        _ = Self::try_write_resp3_null(self, resp, &mut None);
      } else {
        _ = Self::try_write_resp2_null(self, resp, &mut None);
      }
      return;
    }

    // Copy the value in case of a trial serialization (Vec 单趟直写)
    self.state.push_value(1);

    let mut err: Option<&'static [u8]> = None;
    let written = Self::try_write_single_item(self, resp, &mut err);

    if err.is_none() && written {
      // Remove the extra value copy we pushed（原值随写出弹空）
      self.state.pop(1);
    }

    if let Some(err) = err {
      // An error was encountered, so write it out
      // 错误帧净化在 wresp 唯一成帧点内完成（见 RespWriter::write_error_bytes），
      // 调用侧不再预处理
      self.state.clear_stack();
      self.state.push_buffer(err);
      let err_buff = self.state.known_string_to_buffer(1).unwrap_or_default();
      RespWriter::new_ref(resp.buf).write_error_bytes(&err_buff);
      self.state.pop(1);
    }
  }

  /// 在 garnet 中的相对路径:libs/server/Lua/LuaRunner.cs:TryWriteSingleItem
  ///
  /// 写出栈顶单项并弹栈；返回是否完整写出（Vec 无界，恒真），
  /// 遭遇不可序列化错误时置 `err`。
  pub fn try_write_single_item(
    runner: &mut LuaRunner,
    resp: &mut RespOut,
    err: &mut Option<&'static [u8]>,
  ) -> bool {
    let cur_top = runner.state.get_top() as i32;
    let Some(ret_type) = runner.state.type_name(cur_top) else {
      *err = Some(ConstantStrings::UNEXPECTED_ERROR);
      return false;
    };
    let is_nullish = matches!(ret_type, "nil" | "userdata" | "function" | "thread");

    if is_nullish {
      return if resp.protocol_version == 3 {
        Self::try_write_resp3_null(runner, resp, err)
      } else {
        Self::try_write_resp2_null(runner, resp, err)
      };
    }

    match ret_type {
      "number" => Self::try_write_number(runner, resp, err),
      "string" => Self::try_write_string(runner, resp, err),
      "boolean" => {
        if resp.protocol_version == 3 {
          // RESP3 has a proper boolean type
          Self::try_write_resp3_boolean(runner, resp, err)
        } else {
          // RESP2 booleans are weird: false = null (bulk nil), true = 1
          if runner.state.to_boolean(cur_top) {
            runner.state.pop(1);
            runner.state.push_integer(1);
            Self::try_write_number(runner, resp, err)
          } else {
            Self::try_write_resp2_null(runner, resp, err)
          }
        }
      }
      "table" => {
        // Redis does not respect metatables, so RAW access is ok here

        runner.state.push_buffer(ConstantStrings::DOUBLE);
        runner.state.raw_get(cur_top);
        let is_double = runner.state.type_name(-1) == Some("number");
        if is_double {
          let fit = if resp.protocol_version == 3 {
            Self::try_write_double(runner, resp, err)
          } else {
            // Force double to string for RESP2
            if !runner.state.try_number_to_string() {
              *err = Some(ConstantStrings::OUT_OF_MEMORY);
              return false;
            }
            Self::try_write_string(runner, resp, err)
          };
          // Remove table from stack
          runner.state.pop(1);
          return fit;
        }
        // Remove whatever we read from the table under the "double" key
        runner.state.pop(1);

        runner.state.push_buffer(ConstantStrings::MAP);
        runner.state.raw_get(cur_top);
        let is_map = runner.state.type_name(-1) == Some("table");
        if is_map {
          let fit = Self::try_write_map(runner, resp, err);
          // remove table from stack
          runner.state.pop(1);
          return fit;
        }
        runner.state.pop(1);

        runner.state.push_buffer(ConstantStrings::SET);
        runner.state.raw_get(cur_top);
        let is_set = runner.state.type_name(-1) == Some("table");
        if is_set {
          let fit = Self::try_write_set(runner, resp, err);
          // remove table from stack
          runner.state.pop(1);
          return fit;
        }
        runner.state.pop(1);

        // If the key "ok" is in there, we need to short circuit
        runner.state.push_buffer(ConstantStrings::OK_LOWER);
        runner.state.raw_get(cur_top);
        let is_ok = runner.state.type_name(-1) == Some("string");
        if is_ok {
          let fit = Self::try_write_string(runner, resp, err);
          // Remove table from stack
          runner.state.pop(1);
          return fit;
        }
        runner.state.pop(1);

        // If the key "err" is in there, we need to short circuit
        runner.state.push_buffer(ConstantStrings::ERR);
        runner.state.raw_get(cur_top);
        let is_err = runner.state.type_name(-1) == Some("string");
        if is_err {
          let fit = Self::try_write_error(runner, resp, err);
          // Remove table from stack
          runner.state.pop(1);
          return fit;
        }
        runner.state.pop(1);

        // Map this table to an array
        Self::try_write_table_to_array(runner, resp, err)
      }
      _ => {
        // All types should have been handled
        *err = Some(ConstantStrings::UNEXPECTED_ERROR);
        false
      }
    }
  }

  /// 在 garnet 中的相对路径:libs/server/Lua/LuaRunner.cs:TryWriteResp2Null
  /// 保留 _err 形参以匹配 LuaRunner 统一响应写出函数签名规范
  pub fn try_write_resp2_null(
    runner: &mut LuaRunner,
    resp: &mut RespOut,
    _err: &mut Option<&'static [u8]>,
  ) -> bool {
    RespWriter::new_ref(resp.buf).write_resp2_null();
    runner.state.pop(1);
    true
  }

  /// 在 garnet 中的相对路径:libs/server/Lua/LuaRunner.cs:TryWriteResp3Null
  /// 保留 _err 形参以匹配 LuaRunner 统一响应写出函数签名规范
  pub fn try_write_resp3_null(
    runner: &mut LuaRunner,
    resp: &mut RespOut,
    _err: &mut Option<&'static [u8]>,
  ) -> bool {
    RespWriter::<&mut Vec<u8>, Resp3>::new_ref_p(resp.buf).write_resp3_null();
    runner.state.pop(1);
    true
  }

  /// 在 garnet 中的相对路径:libs/server/Lua/LuaRunner.cs:TryWriteNumber
  /// 保留 _err 形参以匹配 LuaRunner 统一响应写出函数签名规范
  pub fn try_write_number(
    runner: &mut LuaRunner,
    resp: &mut RespOut,
    _err: &mut Option<&'static [u8]>,
  ) -> bool {
    let cur_top = runner.state.get_top() as i32;
    // Redis unconditionally converts all "number" replies to integer replies
    let num = runner.state.check_number(cur_top).unwrap_or_default() as i64;
    RespWriter::new_ref(resp.buf).write_int64(num);
    runner.state.pop(1);
    true
  }

  /// 在 garnet 中的相对路径:libs/server/Lua/LuaRunner.cs:TryWriteString
  /// 保留 _err 形参以匹配 LuaRunner 统一响应写出函数签名规范
  /// 写出 RESP bulk string 并自栈顶弹出。
  pub fn try_write_string(
    runner: &mut LuaRunner,
    resp: &mut RespOut,
    _err: &mut Option<&'static [u8]>,
  ) -> bool {
    let cur_top = runner.state.get_top() as i32;
    let buf = runner
      .state
      .known_string_to_buffer(cur_top)
      .unwrap_or_default();
    RespWriter::new_ref(resp.buf).write_bulk_string(&buf);
    runner.state.pop(1);
    true
  }

  /// 在 garnet 中的相对路径:libs/server/Lua/LuaRunner.cs:TryWriteResp3Boolean
  /// 保留 _err 形参以匹配 LuaRunner 统一响应写出函数签名规范
  pub fn try_write_resp3_boolean(
    runner: &mut LuaRunner,
    resp: &mut RespOut,
    _err: &mut Option<&'static [u8]>,
  ) -> bool {
    let cur_top = runner.state.get_top() as i32;
    // In RESP3 there is a dedicated boolean type
    RespWriter::<&mut Vec<u8>, Resp3>::new_ref_p(resp.buf)
      .write_resp3_bool(runner.state.to_boolean(cur_top));
    runner.state.pop(1);
    true
  }

  /// 在 garnet 中的相对路径:libs/server/Lua/LuaRunner.cs:TryWriteDouble
  /// 保留 _err 形参以匹配 LuaRunner 统一响应写出函数签名规范
  pub fn try_write_double(
    runner: &mut LuaRunner,
    resp: &mut RespOut,
    _err: &mut Option<&'static [u8]>,
  ) -> bool {
    let cur_top = runner.state.get_top() as i32;
    let num = runner.state.check_number(cur_top).unwrap_or_default();
    if resp.protocol_version >= 3 {
      RespWriter::<&mut Vec<u8>, Resp3>::new_ref_p(resp.buf).write_double_numeric(num);
    } else {
      RespWriter::new_ref(resp.buf).write_double_numeric(num);
    }
    runner.state.pop(1);
    true
  }

  /// 在 garnet 中的相对路径:libs/server/Lua/LuaRunner.cs:TryWriteMap
  ///
  /// C# 缓冲受限发送器路径另有降级形态 TryWriteMapToArray
  /// （在 garnet 中的相对路径:libs/server/Lua/LuaRunner.cs:TryWriteMapToArray，
  /// map 逐对写成 key/value 交替数组并检查发送缓冲容量）；
  /// Rust 输出为 Vec 无界缓冲，无需容量判定与数组降级，直写 map 形态承接。
  pub fn try_write_map(
    runner: &mut LuaRunner,
    resp: &mut RespOut,
    err: &mut Option<&'static [u8]>,
  ) -> bool {
    let table_ix = runner.state.get_top() as i32;
    let mut map_size = 0usize;

    // Push nil key as "first key"
    runner.state.push_nil();
    while runner.state.lua_next() {
      // Now we have value at top of stack, and key one below it
      map_size += 1;
      // Remove value, we don't need it
      runner.state.pop(1);
    }

    // Write the map header
    if resp.protocol_version >= 3 {
      RespWriter::<&mut Vec<u8>, Resp3>::new_ref_p(resp.buf).write_map_length(map_size);
    } else {
      RespWriter::new_ref(resp.buf).write_map_length(map_size);
    }

    // Write the values out by traversing the table again
    runner.state.push_nil();
    while runner.state.lua_next() {
      // Copy key to top of stack
      runner.state.push_value(table_ix + 1);

      // Write (and remove) key out
      if !Self::try_write_single_item(runner, resp, err) && err.is_some() {
        return false;
      }

      // Write (and remove) value out
      if !Self::try_write_single_item(runner, resp, err) && err.is_some() {
        return false;
      }
    }

    // Remove the table
    runner.state.pop(1);
    true
  }

  /// 在 garnet 中的相对路径:libs/server/Lua/LuaRunner.cs:TryWriteSet
  ///
  /// C# 缓冲受限发送器路径另有降级形态 TryWriteSetToArray
  /// （在 garnet 中的相对路径:libs/server/Lua/LuaRunner.cs:TryWriteSetToArray，
  /// 仅写键集为数组并检查发送缓冲容量）；
  /// Rust 输出为 Vec 无界缓冲，无需容量判定与数组降级，直写 set 形态承接。
  pub fn try_write_set(
    runner: &mut LuaRunner,
    resp: &mut RespOut,
    err: &mut Option<&'static [u8]>,
  ) -> bool {
    let table_ix = runner.state.get_top() as i32;
    let mut set_size = 0usize;

    runner.state.push_nil();
    while runner.state.lua_next() {
      set_size += 1;
      runner.state.pop(1);
    }

    // Write the set header
    if resp.protocol_version >= 3 {
      RespWriter::<&mut Vec<u8>, Resp3>::new_ref_p(resp.buf).write_set_length(set_size);
    } else {
      RespWriter::new_ref(resp.buf).write_set_length(set_size);
    }

    runner.state.push_nil();
    while runner.state.lua_next() {
      // Remove the value, it's ignored
      runner.state.pop(1);

      // Make a copy of the key
      runner.state.push_value(table_ix + 1);

      // Write (and remove) key copy out
      if !Self::try_write_single_item(runner, resp, err) && err.is_some() {
        return false;
      }
    }

    runner.state.pop(1);
    true
  }

  /// 在 garnet 中的相对路径:libs/server/Lua/LuaRunner.cs:TryWriteError
  /// 保留 _err 形参以匹配 LuaRunner 统一响应写出函数签名规范
  ///
  /// `err_buff` 为栈上错误对象文本（脚本可控，可带 CRLF），成帧与净化统一由
  /// wresp 错误帧唯一成帧点承接，调用侧不预处理。
  pub fn try_write_error(
    runner: &mut LuaRunner,
    resp: &mut RespOut,
    _err: &mut Option<&'static [u8]>,
  ) -> bool {
    let cur_top = runner.state.get_top() as i32;
    let err_buff = runner
      .state
      .known_string_to_buffer(cur_top)
      .unwrap_or_default();
    RespWriter::new_ref(resp.buf).write_error_bytes(&err_buff);
    runner.state.pop(1);
    true
  }

  /// 在 garnet 中的相对路径:libs/server/Lua/LuaRunner.cs:TryWriteTableToArray
  pub fn try_write_table_to_array(
    runner: &mut LuaRunner,
    resp: &mut RespOut,
    err: &mut Option<&'static [u8]>,
  ) -> bool {
    let table_top = runner.state.get_top() as i32;

    // Lua # operator - this MAY stop at nils (raw length)
    let max_len = runner.state.raw_len(table_top) as usize;

    // Find the TRUE length by scanning for nils
    let mut true_len = 0usize;
    while true_len < max_len {
      // 表下标经类型分支先行校验，raw_get_integer 恒命中。
      runner.state.raw_get_integer(table_top, true_len as i64 + 1);
      let is_nil = runner.state.type_name(-1) == Some("nil");
      runner.state.pop(1);

      if is_nil {
        break;
      }
      true_len += 1;
    }

    RespWriter::new_ref(resp.buf).write_array_length(true_len);

    for i in 1..=true_len {
      // Push item at index i onto the stack
      runner.state.raw_get_integer(table_top, i as i64);

      // Write the item out, removing it from the stack
      if !Self::try_write_single_item(runner, resp, err) && err.is_some() {
        return false;
      }
    }

    // Remove the table
    runner.state.pop(1);
    true
  }
}

/// 直接写 RESP error 到输出缓冲（compile 路径）。
///
/// `msg` 可能是编译期回显的脚本文本（含 CRLF），净化在 wresp 错误帧唯一成帧点
/// 内生效，本口只负责清缓冲后成帧。
pub(super) fn resp_out_error(out: &mut Vec<u8>, msg: &[u8]) {
  out.clear();
  RespWriter::new_ref(out).write_error_bytes(msg);
}

/// RESP `:<int>\r\n` 解析。
pub(super) fn read_resp_int(cursor: &mut &[u8]) -> Option<i64> {
  let end = cursor.iter().position(|&b| b == b'\r')?;
  if cursor.get(end + 1) != Some(&b'\n') {
    return None;
  }
  let text = str::from_utf8(&cursor[1..end]).ok()?;
  let value = text.parse().ok()?;
  *cursor = &cursor[end + 2..];
  Some(value)
}

/// 查找 CRLF 位置。
fn find_crlf(data: &[u8]) -> Option<usize> {
  data.windows(2).position(|w| w == b"\r\n")
}

/// LuaWrappedError 栈视图形态（宿主回调侧入口）。
pub fn lua_wrapped_error_view(
  state: &mut LuaState,
  non_error_returns: usize,
  error_msg: &[u8],
) -> i32 {
  state.clear_stack();
  for _ in 0..non_error_returns {
    state.push_nil();
  }

  state.push_buffer(error_msg);

  (non_error_returns + 1) as i32
}

/// 对应 ProcessRespResponse 的栈视图底层实现
pub fn process_resp_response_view(
  state: &mut LuaState,
  resp_protocol_version: u8,
  resp: &[u8],
) -> i32 {
  let mut cursor = resp;
  let ret = process_single_resp_term_view(state, resp_protocol_version, &mut cursor);

  if !cursor.is_empty() {
    log::error!("RESP3 Response not fully consumed, this should never happen");
    return lua_wrapped_error_view(state, 1, ConstantStrings::UNEXPECTED_ERROR);
  }

  ret
}

/// 对应 ProcessSingleRespTerm 的栈视图底层实现
pub fn process_single_resp_term_view(
  state: &mut LuaState,
  resp_protocol_version: u8,
  cursor: &mut &[u8],
) -> i32 {
  let Some(&indicator) = cursor.first() else {
    log::error!("Unexpected response, this should never happen");
    return lua_wrapped_error_view(state, 1, ConstantStrings::UNEXPECTED_ERROR);
  };

  match indicator {
    // Simple reply (Common)
    b'+' => {
      *cursor = &cursor[1..];
      let mut result_span: &[u8] = &[];
      if matches!(try_read_as_span(&mut result_span, cursor), Ok(true)) {
        // Construct a table = { 'ok': value }
        state.create_table(0, 1);
        state.push_buffer(ConstantStrings::OK_LOWER);
        state.push_buffer(result_span);
        state.raw_set(1);
        return 1;
      }
      default_resp_term_view(state)
    }

    // Integer (Common)
    b':' => {
      if let Some(number) = read_resp_int(cursor) {
        state.push_integer(number);
        return 1;
      }
      default_resp_term_view(state)
    }

    // Error (Common)
    b'-' => {
      *cursor = &cursor[1..];
      let mut err_span: &[u8] = &[];
      if matches!(try_read_as_span(&mut err_span, cursor), Ok(true)) {
        if err_span == ConstantStrings::RESP_ERR_GENERIC_UNK_CMD {
          // Gets a special response
          return lua_wrapped_error_view(state, 1, ConstantStrings::ERR_UNKNOWN);
        }

        return lua_wrapped_error_view(state, 1, err_span);
      }
      default_resp_term_view(state)
    }

    // Bulk string or null bulk string (Common)
    b'$' => {
      // "$-1\r\n" → RESP2 null bulk → false
      if cursor.len() >= 5 && &cursor[1..5] == b"-1\r\n" {
        // Bulk null strings are mapped to FALSE
        state.push_boolean(false);
        *cursor = &cursor[5..];
        return 1;
      }
      let mut bulk_span: &[u8] = &[];
      if matches!(
        try_read_span_with_length_header(&mut bulk_span, cursor),
        Ok(true)
      ) {
        state.push_buffer(bulk_span);
        return 1;
      }
      default_resp_term_view(state)
    }

    // Array (Common)
    b'*' => {
      let mut array_item_count = 0i32;
      if matches!(
        try_read_signed_array_length(&mut array_item_count, cursor),
        Ok(true)
      ) {
        if array_item_count == -1 {
          state.push_boolean(false);
        } else {
          let count = array_item_count as usize;
          state.create_table(count, 0);
          let table_index = state.get_top() as i32;

          for item_ix in 0..count {
            // Pushes the item to the top of the stack
            _ = process_single_resp_term_view(state, resp_protocol_version, cursor);

            // Store the item into the table（值随 raw_set_integer 弹出）
            state.raw_set_integer(table_index, item_ix as i64 + 1);
          }
        }

        return 1;
      }
      default_resp_term_view(state)
    }

    // Map (RESP3 only)
    b'%' if resp_protocol_version == 3 => {
      let mut map_pair_count = 0i32;
      if matches!(
        try_read_signed_map_length(&mut map_pair_count, cursor),
        Ok(true)
      ) && map_pair_count >= 0
      {
        // Response is a two level table, where { map = { ... } }
        state.create_table(0, 1);
        let parent_index = state.get_top() as i32;

        state.push_buffer(ConstantStrings::MAP);
        state.create_table(0, map_pair_count as usize);
        let sub_index = parent_index + 2;

        for _ in 0..map_pair_count {
          // Read key
          _ = process_single_resp_term_view(state, resp_protocol_version, cursor);
          // Read value
          _ = process_single_resp_term_view(state, resp_protocol_version, cursor);

          // Set t[k] = v
          state.raw_set(sub_index);
        }

        // Store the sub-table into the parent table
        state.raw_set(parent_index);
        return 1;
      }
      default_resp_term_view(state)
    }

    // Null (RESP3 only)
    b'_' if resp_protocol_version == 3 => {
      if cursor.len() >= 3 && &cursor[1..3] == b"\r\n" {
        *cursor = &cursor[3..];
        state.push_nil();
        return 1;
      }
      default_resp_term_view(state)
    }

    // Set (RESP3 only)
    b'~' if resp_protocol_version == 3 => {
      let mut set_item_count = 0i32;
      if matches!(
        try_read_signed_set_length(&mut set_item_count, cursor),
        Ok(true)
      ) && set_item_count >= 0
      {
        // Response is a two level table, where { set = { ... } }
        state.create_table(0, 1);
        let parent_index = state.get_top() as i32;

        state.push_buffer(ConstantStrings::SET);
        state.create_table(0, set_item_count as usize);
        let sub_index = parent_index + 2;

        for _ in 0..set_item_count {
          // Read value, which we use as key
          _ = process_single_resp_term_view(state, resp_protocol_version, cursor);
          // Unconditionally the value under the key is true
          state.push_boolean(true);

          // Set t[value] = true
          state.raw_set(sub_index);
        }

        // Store the sub-table into the parent table
        state.raw_set(parent_index);
        return 1;
      }
      default_resp_term_view(state)
    }

    // Boolean (RESP3 only)
    b'#' if resp_protocol_version == 3 => {
      if cursor.len() >= 4 {
        let as_int = &cursor[0..4];
        if as_int == b"#t\r\n" {
          *cursor = &cursor[4..];
          state.push_boolean(true);
          return 1;
        } else if as_int == b"#f\r\n" {
          *cursor = &cursor[4..];
          state.push_boolean(false);
          return 1;
        }
      }
      default_resp_term_view(state)
    }

    // Double (RESP3 only)
    b',' if resp_protocol_version == 3 => {
      if let Some(end_of_double_ix) = find_crlf(cursor) {
        let double_span = &cursor[..end_of_double_ix + 2];
        let body = &double_span[1..double_span.len() - 2];
        let parsed = match body {
          b"inf" => Some(f64::INFINITY),
          b"nan" => Some(f64::NAN),
          b"-inf" => Some(f64::NEG_INFINITY),
          b"-nan" => Some(f64::NAN),
          text => str::from_utf8(text).ok().and_then(|t| t.parse().ok()),
        };
        if let Some(parsed) = parsed {
          *cursor = &cursor[double_span.len()..];

          // Create table like { double = <parsed> }
          state.create_table(0, 1);
          state.push_buffer(ConstantStrings::DOUBLE);
          state.push_number(parsed);
          state.raw_set(1);
          return 1;
        }
      }
      default_resp_term_view(state)
    }

    // Big number (RESP3 only)
    b'(' if resp_protocol_version == 3 => {
      if let Some(end_of_big_num) = find_crlf(cursor) {
        let big_num_span = &cursor[..end_of_big_num + 2];
        if big_num_span.len() >= 4 {
          let big_num_buf = &big_num_span[1..big_num_span.len() - 2];
          if big_num_buf.iter().all(u8::is_ascii_digit) {
            *cursor = &cursor[big_num_span.len()..];

            // Create table like { big_number = <bigNumBuf> }
            state.create_table(0, 1);
            state.push_buffer(ConstantStrings::BIG_NUMBER);
            state.push_buffer(big_num_buf);
            state.raw_set(1);
            return 1;
          }
        }
      }
      default_resp_term_view(state)
    }

    // Verbatim strings (RESP3 only)
    b'=' if resp_protocol_version == 3 => {
      let mut verbatim_string_length = 0i32;
      if matches!(
        try_read_verbatim_string_length(&mut verbatim_string_length, cursor),
        Ok(true)
      ) && verbatim_string_length >= 4
      {
        let verbatim = verbatim_string_length as usize;
        if cursor.len() >= verbatim + 2 {
          let format = &cursor[0..3];
          let data = &cursor[4..verbatim];

          let advanced = *cursor;
          *cursor = &cursor[verbatim..];
          if &cursor[0..2] != b"\r\n" {
            *cursor = advanced;
            return default_resp_term_view(state);
          }
          *cursor = &cursor[2..];

          // create table like { format = <format>, string = <data> }
          state.create_table(0, 2);

          state.push_buffer(ConstantStrings::FORMAT);
          state.push_buffer(format);
          state.raw_set(1);

          state.push_buffer(ConstantStrings::STRING);
          state.push_buffer(data);
          state.raw_set(1);

          return 1;
        }
      }
      default_resp_term_view(state)
    }

    _ => default_resp_term_view(state),
  }
}

/// default 分支（意外响应 → UnexpectedError）。
fn default_resp_term_view(state: &mut LuaState) -> i32 {
  log::error!("Unexpected response, this should never happen");
  lua_wrapped_error_view(state, 1, ConstantStrings::UNEXPECTED_ERROR)
}
