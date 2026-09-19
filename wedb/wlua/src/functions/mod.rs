//! 在 garnet 中的相对路径:libs/server/Lua/LuaRunner.Functions.cs:LuaRunner
//!
//! 宿主函数族：Lua → 宿主的全部回调实现。
//!
//! C# 以 `nint luaStatePtr` + trampoline 进出；Rust 以 wlua 的 C 蹦床
//! （见 LuaRunner::register）在真实栈上直调本域函数，函数签名统一为
//! `(state, host) -> i32`（返回栈上结果数）。无异常可逃逸：程序性错误以
//! panic 上抛并由蹦床转 Lua 错误（对标 FailOnException 兜底）。
//!
//! 域划分：本模块承接 trampoline 入口委托与 struct 转发；redis 命令族见
//! `redis`，数学与基础库见 `math`，位运算见 `bitop`，cjson 编解码见
//! `cjson`，cmsgpack 编解码见 `cmsgpack`。

use crate::{
  LuaState, functions_struct as struct_codec,
  runner::{
    HostShared, LuaRunner, clear_callback_context, lua_wrapped_error_view, set_callback_context,
  },
  strings::ConstantStrings,
};

pub struct LuaRunnerFunctions;

impl LuaRunnerFunctions {
  /// 在 garnet 中的相对路径:libs/server/Lua/LuaRunner.Functions.cs:UnsafeCompileForRunner
  ///
  /// 蹦床形态经 [`LuaRunner::compile_for_runner`]
  /// 直调，本入口保留给宿主测试路径。
  pub fn unsafe_compile_for_runner(
    runner: &mut LuaRunner,
    out: &mut Vec<u8>,
  ) -> Result<(), String> {
    runner.compile_for_runner(out)
  }

  /// 在 garnet 中的相对路径:libs/server/Lua/LuaRunner.Functions.cs:UnsafeCompileForSession
  ///
  /// 同上，会话形态委托 [`LuaRunner::compile_for_session`]。
  pub fn unsafe_compile_for_session(runner: &mut LuaRunner, out: &mut Vec<u8>) -> bool {
    runner.compile_for_session(out)
  }

  /// 在 garnet 中的相对路径:libs/server/Lua/LuaRunner.Functions.cs:UnsafeRunPreambleForRunner
  ///
  /// 委托 runner 的 runner 模式 preamble。
  pub fn unsafe_run_preamble_for_runner(runner: &mut LuaRunner) -> bool {
    runner.run_preamble_for_runner().is_ok()
  }

  /// 在 garnet 中的相对路径:libs/server/Lua/LuaRunner.Functions.cs:UnsafeRunPreambleForSession
  ///
  /// 委托 runner 的 session 模式 preamble。
  pub fn unsafe_run_preamble_for_session(runner: &mut LuaRunner) -> bool {
    runner.run_preamble_for_session().is_ok()
  }

  /// garnet_load：luau 适配宿主编译入口（C# 用 Lua 5.4 原生 load）。
  ///
  /// 编译源码为函数压栈；失败压 (nil, 错误串)。
  /// 满足 LuaCFunction 统一函数指针签名 (LuaState, HostShared) -> i32 规范，保留 _host 参数
  pub fn load_chunk(state: &mut LuaState, _host: &mut HostShared) -> i32 {
    let arg_count = state.get_top() as i32;
    if arg_count < 1 || state.type_name(1) != Some("string") {
      state.clear_stack();
      state.push_nil();
      state.push_buffer(b"bad argument to load");
      return 2;
    }

    let source = state.known_string_to_buffer(1).unwrap_or_default();
    state.clear_stack();
    match state.load_buffer(&source, "@user_script") {
      // 函数已由 load_buffer 压栈。
      Ok(()) => 1,
      Err(e) => {
        state.push_nil();
        state.push_buffer(e.to_string().as_bytes());
        2
      }
    }
  }

  /// 在 garnet 中的相对路径:libs/server/Lua/LuaRunner.Functions.cs:UnpackTrampoline
  ///
  /// 变参返回解包：rets 表（`rets[1]`=err, `rets[2]`=count, `rets[3..]`=值）+
  /// count → (nil 错误槽, 值...) 形态。
  /// 满足 LuaCFunction 统一函数指针签名 (LuaState, HostShared) -> i32 规范，保留 _host 参数
  pub fn unpack_trampoline(state: &mut LuaState, _host: &mut HostShared) -> i32 {
    let arg_count = state.get_top() as i32;
    if arg_count != 2 || state.type_name(1) != Some("table") {
      return lua_wrapped_error_view(state, 0, ConstantStrings::UNEXPECTED_ERROR);
    }

    let Some(count) = state.check_number(2).map(|n| n as i64) else {
      return lua_wrapped_error_view(state, 0, ConstantStrings::UNEXPECTED_ERROR);
    };
    state.pop(1);

    // Error slot, which is empty after the stack check
    state.push_nil();

    for ix in 1..=count {
      // + 2 to skip err and count
      state.raw_get_integer(1, ix + 2);
    }

    (count + 1) as i32
  }

  /// 转发至 set_callback_context
  pub fn set_callback_context(context: *mut HostShared) {
    set_callback_context(context);
  }

  /// 转发至 clear_callback_context
  pub fn clear_callback_context(context: *mut HostShared) {
    clear_callback_context(context);
  }

  /// 在 garnet 中的相对路径:libs/server/Lua/LuaRunner.Functions.cs:CompileForRunner（trampoline 形态）
  pub fn compile_for_runner(runner: &mut LuaRunner, out: &mut Vec<u8>) -> Result<(), String> {
    Self::unsafe_compile_for_runner(runner, out)
  }

  /// 在 garnet 中的相对路径:libs/server/Lua/LuaRunner.Functions.cs:CompileForSession（trampoline 形态）
  pub fn compile_for_session(runner: &mut LuaRunner, out: &mut Vec<u8>) -> bool {
    Self::unsafe_compile_for_session(runner, out)
  }

  /// 在 garnet 中的相对路径:libs/server/Lua/LuaRunner.Functions.cs:RunPreambleForRunner（trampoline 形态）
  pub fn run_preamble_for_runner(runner: &mut LuaRunner) -> bool {
    Self::unsafe_run_preamble_for_runner(runner)
  }

  /// 在 garnet 中的相对路径:libs/server/Lua/LuaRunner.Functions.cs:RunPreambleForSession（trampoline 形态）
  pub fn run_preamble_for_session(runner: &mut LuaRunner) -> bool {
    Self::unsafe_run_preamble_for_session(runner)
  }

  /// Lua 侧 struct.pack 回调，转发至 [`struct_codec::struct_pack`]。
  /// 满足 LuaCFunction 统一函数指针签名 (LuaState, HostShared) -> i32 规范，保留 _host 参数
  pub fn struct_pack(state: &mut LuaState, _host: &mut HostShared) -> i32 {
    let num_lua_args = state.get_top() as i32;
    if num_lua_args == 0 || state.type_name(1) != Some("string") {
      return lua_wrapped_error_view(state, 1, ConstantStrings::BAD_ARG_PACK);
    }

    let format = state.known_string_to_buffer(1).unwrap_or_default();
    let mut values: Vec<struct_codec::StructValue> =
      Vec::with_capacity((num_lua_args - 1).max(0) as usize);
    for ix in 2..=num_lua_args {
      match state.type_name(ix) {
        Some("number") => {
          values.push(struct_codec::StructValue::Number(
            state.check_number(ix).unwrap_or_default(),
          ));
        }
        Some("string") => {
          // 字节串原样参与（c/s 定长/原串编码，数值格式经 lua_tonumber 强转）。
          values.push(struct_codec::StructValue::Bytes(
            state.known_string_to_buffer(ix).unwrap_or_default(),
          ));
        }
        _ => return lua_wrapped_error_view(state, 1, ConstantStrings::BAD_ARG_PACK),
      }
    }

    match struct_codec::struct_pack(&format, &values) {
      Some(packed) => {
        state.clear_stack();
        state.push_buffer(&packed);
        1
      }
      None => lua_wrapped_error_view(state, 1, ConstantStrings::BAD_ARG_PACK),
    }
  }

  /// Lua 侧 struct.unpack 回调，转发至 [`struct_codec::struct_unpack`]。
  /// 满足 LuaCFunction 统一函数指针签名 (LuaState, HostShared) -> i32 规范，保留 _host 参数
  pub fn struct_unpack(state: &mut LuaState, _host: &mut HostShared) -> i32 {
    let num_lua_args = state.get_top() as i32;
    if num_lua_args < 2
      || state.type_name(1) != Some("string")
      || state.type_name(2) != Some("string")
    {
      return lua_wrapped_error_view(state, 0, ConstantStrings::BAD_ARG_UNPACK);
    }

    let format = state.known_string_to_buffer(1).unwrap_or_default();
    let mut data = state.known_string_to_buffer(2).unwrap_or_default();

    // C# 形态：第三参为 1 基偏移。
    if num_lua_args >= 3 {
      let Some(pos) = state
        .type_name(3)
        .filter(|t| *t == "number")
        .and_then(|_| state.check_number(3))
        .map(|n| n as i64)
      else {
        return lua_wrapped_error_view(state, 0, ConstantStrings::BAD_ARG_UNPACK);
      };
      if pos < 1 {
        return lua_wrapped_error_view(state, 0, ConstantStrings::BAD_ARG_UNPACK);
      }
      let offset = (pos - 1) as usize;
      if offset > data.len() {
        return lua_wrapped_error_view(state, 0, ConstantStrings::BAD_ARG_UNPACK);
      }
      data.drain(..offset);
    }

    let Some(out) = struct_codec::struct_unpack(&format, &data) else {
      return lua_wrapped_error_view(state, 0, ConstantStrings::BAD_ARG_UNPACK);
    };

    // 参数帧丢弃：仅留返回值。返回形态对齐 C#：
    // (err, count, 值..., 下一可读位置)，经 Rotate(1, 2) 归位。
    state.clear_stack();
    for value in &out.values {
      match value {
        struct_codec::StructValue::Number(n) => state.push_number(*n),
        struct_codec::StructValue::Bytes(bytes) => {
          state.push_buffer(bytes);
        }
      }
    }
    // 末位附加：消费后的 1 基读取位置（含对齐填充与变长项的实际消费量）。
    state.push_integer(out.consumed as i64 + 1);

    let decoded_count = state.get_top() as i64 - 1;
    state.push_nil();
    state.push_integer(decoded_count + 1);
    state.rotate(1, 2);

    // +1 位置 + (nil) 错误槽 + count
    (decoded_count + 3) as i32
  }

  /// Lua 侧 struct.size 回调，转发至 [`struct_codec::struct_size`]。
  /// 满足 LuaCFunction 统一函数指针签名 (LuaState, HostShared) -> i32 规范，保留 _host 参数
  pub fn struct_size(state: &mut LuaState, _host: &mut HostShared) -> i32 {
    let num_lua_args = state.get_top() as i32;
    if num_lua_args == 0 || state.type_name(1) != Some("string") {
      return lua_wrapped_error_view(state, 1, ConstantStrings::BAD_ARG_FORMAT);
    }

    let format = state.known_string_to_buffer(1).unwrap_or_default();
    match struct_codec::struct_size(&format) {
      Some(size) => {
        state.clear_stack();
        state.push_integer(size as i64);
        1
      }
      None => lua_wrapped_error_view(state, 1, ConstantStrings::BAD_ARG_FORMAT),
    }
  }
}

mod bitop;
mod cjson;
mod cmsgpack;
mod math;
mod redis;
pub use bitop::lua_number_to_bit_value;

#[cfg(test)]
mod tests {
  use crate::functions_struct as struct_codec;
  #[test]
  fn struct_pack_size_roundtrip() {
    // 底层 codec 形态核验（Lua 入口经 struct_pack/struct_size 承接）。
    use struct_codec::StructValue;
    assert_eq!(struct_codec::struct_size(b"<I"), Some(4));
    assert_eq!(
      struct_codec::struct_pack(b"<I", &[StructValue::Number(42.0)]),
      Some(vec![42, 0, 0, 0])
    );
    assert_eq!(
      struct_codec::struct_unpack(b"<I", &[42, 0, 0, 0]).map(|out| out.values),
      Some(vec![StructValue::Number(42.0)])
    );
  }
}
