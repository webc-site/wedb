//! Lua 标量族 → RESP 写出（RESP2/RESP3 null、boolean、integer、string、
//! double、error）
//!
//! 在 garnet 中的相对路径:libs/server/Lua/LuaRunner.cs:TryWriteResp2Null
//! 在 garnet 中的相对路径:libs/server/Lua/LuaRunner.cs:TryWriteResp3Null
//! 在 garnet 中的相对路径:libs/server/Lua/LuaRunner.cs:TryWriteNumber
//! 在 garnet 中的相对路径:libs/server/Lua/LuaRunner.cs:TryWriteString
//! 在 garnet 中的相对路径:libs/server/Lua/LuaRunner.cs:TryWriteResp3Boolean
//! 在 garnet 中的相对路径:libs/server/Lua/LuaRunner.cs:TryWriteDouble
//! 在 garnet 中的相对路径:libs/server/Lua/LuaRunner.cs:TryWriteError

use std::ops::Range;

use wresp::{
  cmd_strings::write_double_numeric,
  resp_memory_writer::{Resp3, RespWriter},
};

use super::RespOut;
use crate::runner::LuaRunner;

/// x64 `cvttsd2si` 的合法转换域 `[-2^63, 2^63)`（两端皆 f64 精确可表示；
/// 上界 2^63 恰在域外故为排他——NaN 与域外值一律回硬件不定值 i64::MIN）。
const CVTTSD2SI_DOMAIN: Range<f64> = (i64::MIN as f64)..9_223_372_036_854_775_808.0;

impl LuaRunner {
  /// 在 garnet 中的相对路径:libs/server/Lua/LuaRunner.cs:TryWriteResp2Null
  /// 保留 _err 形参以匹配 LuaRunner 统一响应写出函数签名规范
  pub(super) fn try_write_resp2_null(
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
  pub(super) fn try_write_resp3_null(
    runner: &mut LuaRunner,
    resp: &mut RespOut,
    _err: &mut Option<&'static [u8]>,
  ) -> bool {
    RespWriter::<&mut Vec<u8>, Resp3>::new_ref_p(resp.buf).write_resp3_null();
    runner.state.pop(1);
    true
  }

  /// 布尔按整型写出：弹布尔、压 `:1`/`:0` 整数后走 [`Self::try_write_number`]
  /// （Redis 实际行为的布尔整型化形态，四象限中三臂共用）。
  pub(super) fn write_boolean_as_integer(
    runner: &mut LuaRunner,
    resp: &mut RespOut,
    err: &mut Option<&'static [u8]>,
  ) -> bool {
    let as_integer = i64::from(runner.state.to_boolean(runner.state.get_top() as i32));
    runner.state.pop(1);
    runner.state.push_integer(as_integer);
    Self::try_write_number(runner, resp, err)
  }

  /// 在 garnet 中的相对路径:libs/server/Lua/LuaRunner.cs:TryWriteNumber
  /// 保留 _err 形参以匹配 LuaRunner 统一响应写出函数签名规范
  pub(super) fn try_write_number(
    runner: &mut LuaRunner,
    resp: &mut RespOut,
    _err: &mut Option<&'static [u8]>,
  ) -> bool {
    let cur_top = runner.state.get_top() as i32;
    // Redis unconditionally converts all "number" replies to integer replies
    //
    // 截断基座显式对齐 x64 cvttsd2si（C# `(long)` 硬转换
    // libs/server/Lua/LuaRunner.cs:TryWriteNumber 的硬件语义）：NaN 与
    // [-2^63, 2^63) 值域外一律回 i64::MIN（硬件不定值）；Rust `as` 的饱和
    // 语义（inf → i64::MAX、NaN → 0）与之分叉，不得直接使用。NaN 经区间
    // 比较自然落域外（NaN 参与任何序比较恒 false），无需单独判定。
    let trunc = runner
      .state
      .check_number(cur_top)
      .unwrap_or_default()
      .trunc();
    let num = if CVTTSD2SI_DOMAIN.contains(&trunc) {
      trunc as i64
    } else {
      i64::MIN
    };
    RespWriter::new_ref(resp.buf).write_int64(num);
    runner.state.pop(1);
    true
  }

  /// 在 garnet 中的相对路径:libs/server/Lua/LuaRunner.cs:TryWriteString
  /// 保留 _err 形参以匹配 LuaRunner 统一响应写出函数签名规范
  /// 写出 RESP bulk string 并自栈顶弹出。
  pub(super) fn try_write_string(
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
  pub(super) fn try_write_resp3_boolean(
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
  pub(super) fn try_write_double(
    runner: &mut LuaRunner,
    resp: &mut RespOut,
    _err: &mut Option<&'static [u8]>,
  ) -> bool {
    let cur_top = runner.state.get_top() as i32;
    let num = runner.state.check_number(cur_top).unwrap_or_default();
    write_double_numeric(resp.buf, num, resp.protocol_version);
    runner.state.pop(1);
    true
  }

  /// 在 garnet 中的相对路径:libs/server/Lua/LuaRunner.cs:TryWriteError
  /// 保留 _err 形参以匹配 LuaRunner 统一响应写出函数签名规范
  ///
  /// `err_buff` 为栈上错误对象文本（脚本可控，可带 CRLF），成帧与净化统一由
  /// wresp 错误帧唯一成帧点承接，调用侧不预处理。
  pub(super) fn try_write_error(
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
}
