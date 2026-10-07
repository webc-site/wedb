//! 在 garnet 中的相对路径:libs/server/Lua/LuaRunner.cs
//!
//! RESP 输出通道与响应转换：RESP 值 ↔ Lua 栈值的双向写出
//! （对标 LuaRunner.cs 响应写出/解析部分）。
//!
//! 文件分组（纯移动，按方向与 RESP 类型族）：入口与公共形态在本件——
//! [`RespOut`] 输出通道、[`RespObject`] RunForRunner 形态、
//! [`LuaRunner::map_resp_to_object`] 读入口、[`LuaRunner::write_response`]
//! 写入口、宿主回调错误包装与 `RESP :int` 解析；标量族写出
//! （null/boolean/number/string/double/error）见 `write_scalar`；复合族
//! 写出（map/set/array 与写出单项调度）见 `write_table`；RESP 报文 →
//! Lua 栈值解析（RESP3 类型族 match 整块）见 `term_view`。

use std::str;

use wresp::{
  ext::RespVecExt,
  read::{try_read_as_span, try_read_unsigned_array_length, try_slice_with_length_header},
  resp_memory_writer::RespWriter,
};

use super::LuaRunner;
use crate::{LuaState, strings::ConstantStrings};

mod term_view;
mod write_scalar;
mod write_table;

/// RESP 输出通道（对标 IResponseAdapter：会话缓冲或 runner 内存缓冲；
/// 对齐 C# 侧 adapter 只暴露 BufferCur/BufferEnd/RespProtocolVersion 的形态，
/// RESP 写出由调用点就地构造 [`RespWriter`] 承接，不在本类型上镜像写出原语）。
pub struct RespOut<'a> {
  /// 响应写入缓冲。
  pub buf: &'a mut Vec<u8>,
  /// 连接侧成帧协议版本（C# RespResponseAdapter.RespProtocolVersion：恒为
  /// 连接入口版本，脚本不触碰）。取值域由会话侧收紧为 {2,3}：初值
  /// `DEFAULT_RESP_VERSION` = 2，唯一写入口 `update_resp_protocol_version`
  /// 的调用点各自校验（HELLO 走 `parse_hello_args` 的 `2..=3` 门，
  /// `redis.setresp` 先过滤 2.0/3.0），故与 [`is_resp3`] 阈判等价。
  pub protocol_version: u8,
  /// 脚本侧协议版本（C# TryWriteSingleItem 实时读的
  /// respServerSession.respProtocolVersion：RunCommon 起始压回 2，脚本内
  /// setresp 改写，响应写出时刻即脚本窗口终值）。
  pub script_version: u8,
}

impl<'a> RespOut<'a> {
  /// 会话侧输出（script_version 由 [`LuaRunner::run_common`] 在写出前回读填充）。
  #[inline]
  pub fn session(buf: &'a mut Vec<u8>, protocol_version: u8) -> Self {
    Self {
      buf,
      protocol_version,
      script_version: 2,
    }
  }

  /// runner 侧输出（恒 RESP2，对标 RunnerAdapter.RespProtocolVersion = 2 与
  /// respServerSession = null 的布尔臂双 2 象限）。
  #[inline]
  pub fn runner(buf: &'a mut Vec<u8>) -> Self {
    Self {
      buf,
      protocol_version: 2,
      script_version: 2,
    }
  }
}

/// RunForRunner 的结构化返回（C# object 形态）。
#[derive(Debug, PartialEq)]
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
          try_slice_with_length_header(&mut bulk_str, cursor),
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
      resp.buf.write_resp_null_ver(resp.protocol_version);
      return;
    }

    // Copy the value in case of a trial serialization (Vec 单趟直写)
    self.state.push_value(1);

    let mut err: Option<&'static [u8]> = None;
    let written = Self::try_write_single_item(self, resp, &mut err);

    if err.is_none() && written {
      // 成功写出首个返回值后，清空整个执行栈（消除多返回值残留，满足 Redis EVAL 仅取首返回值语义与栈清空断言）
      self.state.clear_stack();
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

/// libs/server/Lua/LuaRunner.cs:LuaWrappedError（宿主回调侧入口）。
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

/// libs/server/Lua/LuaRunner.cs:ProcessRespResponse
pub fn process_resp_response_view(
  state: &mut LuaState,
  resp_protocol_version: u8,
  resp: &[u8],
) -> i32 {
  let mut cursor = resp;
  let ret = term_view::process_single_resp_term_view(state, resp_protocol_version, &mut cursor);

  if !cursor.is_empty() {
    log::error!("RESP3 Response not fully consumed, this should never happen");
    return lua_wrapped_error_view(state, 1, ConstantStrings::UNEXPECTED_ERROR);
  }

  ret
}
