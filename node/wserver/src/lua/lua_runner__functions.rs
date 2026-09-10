//! 宿主函数族：Lua → 宿主的全部回调实现
//! （对标 libs/server/Lua/LuaRunner.Functions.cs:LuaRunner）。
//!
//! C# 以 `nint luaStatePtr` + trampoline 进出；mlua 侧回调闭包（见
//! LuaRunner::register）把参数压入栈镜像后直调本文件函数，函数签名统一为
//! `(state, host) -> i32`（返回栈上结果数）。无异常可逃逸：程序性错误以
//! panic 上抛并由 mlua 转 Lua 错误（对标 FailOnException 兜底）。

use std::sync::LazyLock;

use gxhash::HashSet;
use sonic_rs::prelude::*;

use super::{
  lua_options::LuaLoggingMode,
  lua_runner::{HostShared, lua_wrapped_error_view, process_resp_response_view},
  lua_runner__functions__struct as struct_codec,
  lua_runner__strings::ConstantStrings,
  lua_state_wrapper::{LuaStateWrapper, now_monotonic_millis},
  scratch_buffer_network_sender::ScratchBufferBuilder,
  session_script_cache::SessionScriptCache,
};

/// redis.log 的合法级别数。
const LOG_LEVELS: [f64; 4] = [0.0, 1.0, 2.0, 3.0];

/// bitop 操作码（C# Bitop 内常量）。
const B_NOT: i32 = 0;
const B_OR: i32 = 1;
const B_AND: i32 = 2;
const B_XOR: i32 = 3;
const B_LSHIFT: i32 = 4;
const B_RSHIFT: i32 = 5;
const B_ARSHIFT: i32 = 6;
const B_ROL: i32 = 7;
const B_ROR: i32 = 8;

/// cjson 最大嵌套深度（对齐 Redis 解码深度上限）。
const MAX_ENCODE_DEPTH: i32 = 1000;

/// cmsgpack 最大嵌套深度（C# 写死 16 层后置 null）。
const MAX_MSGPACK_DEPTH: usize = 16;

/// msgpack 复合值的表容量提示上限（防恶意长度头触发巨量预分配；表按需自增长）。
const MSGPACK_TABLE_HINT_CAP: usize = u16::MAX as usize;

pub struct LuaRunner_Functions;

impl LuaRunner_Functions {
  /// libs/server/Lua/LuaRunner.Functions.cs:UnsafeCompileForRunner
  ///
  /// mlua 侧无 C 函数包装：编译经 [`super::lua_runner::LuaRunner::compile_for_runner`]
  /// 直调，本入口保留给宿主测试路径。
  pub fn unsafe_compile_for_runner(
    runner: &mut super::lua_runner::LuaRunner,
    out: &mut Vec<u8>,
  ) -> Result<(), String> {
    runner.compile_for_runner(out)
  }

  /// libs/server/Lua/LuaRunner.Functions.cs:UnsafeCompileForSession
  ///
  /// 同上，会话形态委托 [`super::lua_runner::LuaRunner::compile_for_session`]。
  pub fn unsafe_compile_for_session(
    runner: &mut super::lua_runner::LuaRunner,
    out: &mut Vec<u8>,
  ) -> bool {
    runner.compile_for_session(out)
  }

  /// libs/server/Lua/LuaRunner.Functions.cs:UnsafeRunPreambleForRunner
  ///
  /// 委托 runner 的 runner 模式 preamble。
  pub fn unsafe_run_preamble_for_runner(runner: &mut super::lua_runner::LuaRunner) -> bool {
    runner.run_preamble_for_runner().is_ok()
  }

  /// libs/server/Lua/LuaRunner.Functions.cs:PrepareString
  ///
  /// runner 模式参数按 UTF-8 就地编码（C# 经 ScratchBufferBuilder 复用缓冲；
  /// Rust 参数本就为字节，恒直返）。
  pub fn prepare_string<'a>(raw: &'a [u8], _buffer: &mut ScratchBufferBuilder) -> &'a [u8] {
    raw
  }

  /// libs/server/Lua/LuaRunner.Functions.cs:UnsafeRunPreambleForSession
  ///
  /// 委托 runner 的 session 模式 preamble。
  pub fn unsafe_run_preamble_for_session(runner: &mut super::lua_runner::LuaRunner) -> bool {
    runner.run_preamble_for_session().is_ok()
  }

  /// libs/server/Lua/LuaRunner.Functions.cs:NoSessionResponse
  ///
  /// 无会话的 redis.call（基准/测试路径）：吞参返回 nil。
  pub fn no_session_response(state: &mut LuaStateWrapper, _host: &mut HostShared) -> i32 {
    state.clear_stack();
    state.push_nil();
    1
  }

  /// libs/server/Lua/LuaRunner.Functions.cs:GarnetCallWithTransaction
  ///
  /// 事务模式 redis.call 入口（走同一命令处理面，事务窗口由
  /// RunInTransaction 包络）。
  pub fn garnet_call_with_transaction(state: &mut LuaStateWrapper, host: &mut HostShared) -> i32 {
    Self::process_command_from_scripting(state, host)
  }

  /// libs/server/Lua/LuaRunner.Functions.cs:GarnetCall
  ///
  /// 非事务 redis.call 入口。
  pub fn garnet_call(state: &mut LuaStateWrapper, host: &mut HostShared) -> i32 {
    if host.session.is_none() {
      // C# 构造时以 GarnetCallNoSession 注册的等价形态。
      return Self::no_session_response(state, host);
    }
    Self::process_command_from_scripting(state, host)
  }

  /// libs/server/Lua/LuaRunner.Functions.cs:RequestTimeout
  ///
  /// 超时触发点：置即时截止（中断钩子在下一检查点抛出超时错误）。
  pub fn request_timeout(state: &mut LuaStateWrapper, _host: &mut HostShared) -> i32 {
    state.clear_stack();
    state.try_set_hook(Some(now_monotonic_millis()));
    0
  }

  /// libs/server/Lua/LuaRunner.Functions.cs:RequestTimeout（garnet_request_timeout 全局）
  ///
  /// loader block 的 request_timeout() 委托点。
  pub fn request_timeout_fn(state: &mut LuaStateWrapper, host: &mut HostShared) -> i32 {
    Self::request_timeout(state, host)
  }

  /// garnet_load：luau 适配宿主编译入口（C# 用 Lua 5.4 原生 load）。
  ///
  /// 编译源码为函数压栈；失败压 (nil, 错误串)。
  pub fn load_chunk(state: &mut LuaStateWrapper, _host: &mut HostShared) -> i32 {
    let arg_count = state.get_top() as i32;
    if arg_count < 1 || state.type_name(1) != Some("string") {
      state.clear_stack();
      state.push_nil();
      state.push_constant_string(b"bad argument to load");
      return 2;
    }

    let source = state.known_string_to_buffer(1).unwrap_or_default();
    state.clear_stack();
    match state
      .lua()
      .load(String::from_utf8_lossy(&source).as_ref())
      .set_name("@user_script")
      .into_function()
    {
      Ok(function) => {
        state.push_c_function(function);
        1
      }
      Err(e) => {
        state.push_nil();
        state.push_constant_string(super::lua_state_wrapper::error_message(&e).as_bytes());
        2
      }
    }
  }

  /// libs/server/Lua/LuaRunner.Functions.cs:UnpackTrampoline
  ///
  /// 变参返回解包：rets 表（rets[1]=err, rets[2]=count, rets[3..]=值）+
  /// count → (nil 错误槽, 值...) 形态。
  pub fn unpack_trampoline(state: &mut LuaStateWrapper, _host: &mut HostShared) -> i32 {
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
      _ = state.raw_get_integer(1, ix + 2);
    }

    (count + 1) as i32
  }

  /// libs/server/Lua/LuaRunner.Functions.cs:SHA1Hex
  pub fn sha1_hex(state: &mut LuaStateWrapper, _host: &mut HostShared) -> i32 {
    let arg_count = state.get_top() as i32;
    if arg_count != 1 {
      return lua_wrapped_error_view(state, 1, ConstantStrings::ERR_WRONG_NUMBER_OF_ARGS);
    }

    let bytes: Vec<u8> = match state.type_name(1) {
      Some("string") => state.known_string_to_buffer(1).unwrap_or_default(),
      Some("number") => {
        let Some(bytes) = stack_bytes(state, 1) else {
          return lua_wrapped_error_view(state, 1, ConstantStrings::OUT_OF_MEMORY);
        };
        bytes
      }
      _ => Vec::new(),
    };

    // SessionScriptCache.GetScriptDigest：SHA1 → 40 字符小写 hex（一处定义）。
    let digest = SessionScriptCache::get_script_digest(&bytes);
    let hex_res = digest.as_str().as_bytes().to_vec();

    if !state.try_push_buffer(&hex_res) {
      return lua_wrapped_error_view(state, 1, ConstantStrings::OUT_OF_MEMORY);
    }

    1
  }

  /// libs/server/Lua/LuaRunner.Functions.cs:Log
  pub fn log(state: &mut LuaStateWrapper, host: &mut HostShared) -> i32 {
    let arg_count = state.get_top() as i32;
    if arg_count < 2 {
      return lua_wrapped_error_view(state, 0, ConstantStrings::ERR_REDIS_LOG_REQUIRED);
    }

    if state.type_name(1) != Some("number") {
      return lua_wrapped_error_view(state, 0, ConstantStrings::ERR_FIRST_ARG_MUST_BE_NUMBER);
    }

    let Some(raw_level) = state.check_number(1) else {
      return lua_wrapped_error_view(state, 0, ConstantStrings::ERR_FIRST_ARG_MUST_BE_NUMBER);
    };
    if !LOG_LEVELS.contains(&raw_level) {
      return lua_wrapped_error_view(state, 0, ConstantStrings::ERR_INVALID_DEBUG_LEVEL);
    }

    if host.log_mode == LuaLoggingMode::Disable {
      return lua_wrapped_error_view(state, 0, ConstantStrings::ERR_LOGGING_DISABLED);
    }

    // When shipped as a service, allowing arbitrary writes to logs is dangerous
    // so we support disabling it (while not breaking existing scripts)
    if host.log_mode == LuaLoggingMode::Silent {
      return 0;
    }

    // Construct and log the equivalent message
    let mut message = String::new();
    for arg_ix in 2..=arg_count {
      let buff = match state.type_name(arg_ix) {
        Some("string") => state.known_string_to_buffer(arg_ix),
        Some("number") => stack_bytes(state, arg_ix),
        _ => None,
      };
      let Some(buff) = buff else { continue };
      if !message.is_empty() {
        message.push(' ');
      }
      message.push_str(&String::from_utf8_lossy(&buff));
    }

    let log_level = match raw_level as i64 {
      0 => log::Level::Debug,
      1 => log::Level::Info,
      2 => log::Level::Warn,
      // We validated this above, so really it's just 3 but the switch needs to be exhaustive
      _ => log::Level::Error,
    };

    log::log!(log_level, "redis.log: {message}");

    0
  }

  /// libs/server/Lua/LuaRunner.Functions.cs:Atan2
  pub fn atan2(state: &mut LuaStateWrapper, _host: &mut HostShared) -> i32 {
    let lua_arg_count = state.get_top() as i32;
    if lua_arg_count != 2
      || state.type_name(1) != Some("number")
      || state.type_name(2) != Some("number")
    {
      return lua_wrapped_error_view(state, 1, ConstantStrings::BAD_ARG_ATAN2);
    }

    let x = state.check_number(1).unwrap_or_default();
    let y = state.check_number(2).unwrap_or_default();

    let res = x.atan2(y);
    state.pop(2);
    state.push_number(res);
    1
  }

  /// libs/server/Lua/LuaRunner.Functions.cs:Cosh
  pub fn cosh(state: &mut LuaStateWrapper, _host: &mut HostShared) -> i32 {
    let lua_arg_count = state.get_top() as i32;
    if lua_arg_count != 1 || state.type_name(1) != Some("number") {
      return lua_wrapped_error_view(state, 1, ConstantStrings::BAD_ARG_COSH);
    }

    let value = state.check_number(1).unwrap_or_default();

    let res = value.cosh();
    state.pop(1);
    state.push_number(res);
    1
  }

  /// libs/server/Lua/LuaRunner.Functions.cs:Frexp
  pub fn frexp(state: &mut LuaStateWrapper, _host: &mut HostShared) -> i32 {
    // Based on: https://github.com/MachineCognitis/C.math.NET/ (MIT License)
    const DBL_EXP_MASK: u64 = 0x7FF0_0000_0000_0000;
    const DBL_MANT_BITS: u32 = 52;
    const DBL_EXP_CLR_MASK: u64 = 0x800F_FFFF_FFFF_FFFF;

    let lua_arg_count = state.get_top() as i32;
    if lua_arg_count != 1 || state.type_name(1) != Some("number") {
      return lua_wrapped_error_view(state, 2, ConstantStrings::BAD_ARG_FREXP);
    }

    let mut number = state.check_number(1).unwrap_or_default();
    let mut exponent: i32 = 0;

    let bits = number.to_bits();
    let exp = ((bits & DBL_EXP_MASK) >> DBL_MANT_BITS) as i32;

    if exp == 0x7FF || number == 0.0 {
      number += number;
    } else {
      // Not zero and finite.
      exponent = exp - 1022;
      if exp == 0 {
        // Subnormal, scale number so that it is in [1, 2).
        number *= f64::from_bits(0x4350_0000_0000_0000); // 2^54
        let bits = number.to_bits();
        let exp = ((bits & DBL_EXP_MASK) >> DBL_MANT_BITS) as i32;
        exponent = exp - 1022 - 54;
      }
      // Set exponent to -1 so that number is in [0.5, 1).
      number = f64::from_bits((bits & DBL_EXP_CLR_MASK) | 0x3FE0_0000_0000_0000);
    }

    state.pop(1);

    let number_as_float = number as f32;

    if (number_as_float as i64) as f32 == number_as_float {
      state.push_integer(number_as_float as i64);
    } else {
      state.push_number(number);
    }

    state.push_integer(i64::from(exponent));

    2
  }

  /// libs/server/Lua/LuaRunner.Functions.cs:Ldexp
  pub fn ldexp(state: &mut LuaStateWrapper, _host: &mut HostShared) -> i32 {
    let lua_arg_count = state.get_top() as i32;
    if lua_arg_count != 2
      || state.type_name(1) != Some("number")
      || state.type_name(2) != Some("number")
    {
      return lua_wrapped_error_view(state, 1, ConstantStrings::BAD_ARG_LDEXP);
    }

    let m = state.check_number(1).unwrap_or_default();
    let e = state.check_number(2).unwrap_or_default() as i32;

    let res = m * 2.0_f64.powi(e);

    state.pop(2);

    if res.is_finite() && (res as i64) as f64 == res {
      state.push_integer(res as i64);
    } else {
      state.push_number(res);
    }

    1
  }

  /// libs/server/Lua/LuaRunner.Functions.cs:Log10
  pub fn log10(state: &mut LuaStateWrapper, _host: &mut HostShared) -> i32 {
    let lua_arg_count = state.get_top() as i32;
    if lua_arg_count != 1 || state.type_name(1) != Some("number") {
      return lua_wrapped_error_view(state, 1, ConstantStrings::BAD_ARG_LOG10);
    }

    let val = state.check_number(1).unwrap_or_default();

    let res = val.log10();

    state.pop(1);
    state.push_number(res);
    1
  }

  /// libs/server/Lua/LuaRunner.Functions.cs:Pow
  pub fn pow(state: &mut LuaStateWrapper, _host: &mut HostShared) -> i32 {
    let lua_arg_count = state.get_top() as i32;
    if lua_arg_count != 2
      || state.type_name(1) != Some("number")
      || state.type_name(2) != Some("number")
    {
      return lua_wrapped_error_view(state, 1, ConstantStrings::BAD_ARG_POW);
    }

    let x = state.check_number(1).unwrap_or_default();
    let y = state.check_number(2).unwrap_or_default();

    let res = x.powf(y);

    state.pop(2);
    state.push_number(res);
    1
  }

  /// libs/server/Lua/LuaRunner.Functions.cs:Sinh
  pub fn sinh(state: &mut LuaStateWrapper, _host: &mut HostShared) -> i32 {
    let lua_arg_count = state.get_top() as i32;
    if lua_arg_count != 1 || state.type_name(1) != Some("number") {
      return lua_wrapped_error_view(state, 1, ConstantStrings::BAD_ARG_SINH);
    }

    let val = state.check_number(1).unwrap_or_default();

    let res = val.sinh();

    state.pop(1);
    state.push_number(res);
    1
  }

  /// libs/server/Lua/LuaRunner.Functions.cs:Tanh
  pub fn tanh(state: &mut LuaStateWrapper, _host: &mut HostShared) -> i32 {
    let lua_arg_count = state.get_top() as i32;
    if lua_arg_count != 1 || state.type_name(1) != Some("number") {
      return lua_wrapped_error_view(state, 1, ConstantStrings::BAD_ARG_TANH);
    }

    let val = state.check_number(1).unwrap_or_default();

    let res = val.tanh();

    state.pop(1);
    state.push_number(res);
    1
  }

  /// libs/server/Lua/LuaRunner.Functions.cs:Maxn
  pub fn maxn(state: &mut LuaStateWrapper, _host: &mut HostShared) -> i32 {
    let lua_arg_count = state.get_top() as i32;
    if lua_arg_count != 1 || state.type_name(1) != Some("table") {
      return lua_wrapped_error_view(state, 1, ConstantStrings::BAD_ARG_MAXN);
    }

    let mut res: f64 = 0.0;

    // Initial key value onto stack
    state.push_nil();
    while state.next() {
      // Remove value
      state.pop(1);

      if state.type_name(2) == Some("number")
        && let Some(key_val) = state.check_number(2)
        && key_val > res
      {
        res = key_val;
      }
    }

    // Remove table, and push largest number
    state.pop(1);
    state.push_number(res);
    1
  }

  /// libs/server/Lua/LuaRunner.Functions.cs:LoadString
  pub fn load_string(state: &mut LuaStateWrapper, _host: &mut HostShared) -> i32 {
    let lua_arg_count = state.get_top() as i32;
    if (lua_arg_count == 1 && state.type_name(1) != Some("string"))
      || (lua_arg_count == 2
        && (state.type_name(1) != Some("string") || state.type_name(2) != Some("string")))
      || lua_arg_count > 2
    {
      return lua_wrapped_error_view(state, 2, ConstantStrings::BAD_ARG_LOAD_STRING);
    }

    // Ignore chunk name
    if lua_arg_count == 2 {
      state.pop(1);
    }

    let buff = state.known_string_to_buffer(1).unwrap_or_default();
    if buff.contains(&0) {
      return lua_wrapped_error_view(state, 2, ConstantStrings::BAD_ARG_LOAD_STRING_NULL_BYTE);
    }

    let res = state.load_string(&String::from_utf8_lossy(&buff));
    if res.is_err() {
      state.clear_stack();
      state.push_nil();
      state.push_constant_string(ConstantStrings::LOAD_STRING_ERROR);
      return 2;
    }

    1
  }

  /// libs/server/Lua/LuaRunner.Functions.cs:BitToBit
  pub fn bit_to_bit(state: &mut LuaStateWrapper, _host: &mut HostShared) -> i32 {
    let lua_arg_count = state.get_top() as i32;
    if lua_arg_count < 1 || state.type_name(1) != Some("number") {
      return lua_wrapped_error_view(state, 1, ConstantStrings::BAD_ARG_TO_BIT);
    }

    let raw_value = state.check_number(1).unwrap_or_default();

    // Make space on the stack
    state.pop(1);

    state.push_number(f64::from(lua_number_to_bit_value(raw_value)));

    1
  }

  /// libs/server/Lua/LuaRunner.Functions.cs:BitToHex
  pub fn bit_to_hex(state: &mut LuaStateWrapper, _host: &mut HostShared) -> i32 {
    let lua_arg_count = state.get_top() as i32;
    if lua_arg_count == 0 || state.type_name(1) != Some("number") {
      return lua_wrapped_error_view(state, 1, ConstantStrings::BAD_ARG_TO_HEX);
    }

    let mut num_digits: i32 = 8;

    if lua_arg_count == 2 {
      if state.type_name(2) != Some("number") {
        return lua_wrapped_error_view(state, 1, ConstantStrings::BAD_ARG_TO_HEX);
      }

      num_digits = state.check_number(2).unwrap_or_default() as i32;
    }

    let mut value = lua_number_to_bit_value(state.check_number(1).unwrap_or_default());

    let hex_bytes: &[u8; 16] = if num_digits == i32::MIN {
      num_digits = 8;
      b"0123456789ABCDEF"
    } else if num_digits < 0 {
      num_digits = -num_digits;
      b"0123456789ABCDEF"
    } else {
      b"0123456789abcdef"
    };

    let num_digits = num_digits.clamp(0, 8) as usize;

    let mut buff = vec![0u8; num_digits];
    for slot in buff.iter_mut().rev() {
      *slot = hex_bytes[(value & 0xF) as usize];
      value >>= 4;
    }

    // Free up space on stack
    state.pop(lua_arg_count as usize);

    if !state.try_push_buffer(&buff) {
      return lua_wrapped_error_view(state, 1, ConstantStrings::OUT_OF_MEMORY);
    }

    1
  }

  /// libs/server/Lua/LuaRunner.Functions.cs:BitBswap
  pub fn bit_bswap(state: &mut LuaStateWrapper, _host: &mut HostShared) -> i32 {
    let lua_arg_count = state.get_top() as i32;
    if lua_arg_count != 1 || state.type_name(1) != Some("number") {
      return lua_wrapped_error_view(state, 1, ConstantStrings::BAD_ARG_BSWAP);
    }

    let value = lua_number_to_bit_value(state.check_number(1).unwrap_or_default());

    // Free up space on stack
    state.pop(1);

    let swapped = value.swap_bytes();
    state.push_number(f64::from(swapped));
    1
  }

  /// libs/server/Lua/LuaRunner.Functions.cs:Bitop
  pub fn bitop(state: &mut LuaStateWrapper, _host: &mut HostShared) -> i32 {
    let lua_arg_count = state.get_top() as i32;
    if lua_arg_count == 0 || state.type_name(1) != Some("number") {
      log::error!("bitop was not indicated, should never happen");
      return lua_wrapped_error_view(state, 1, ConstantStrings::UNEXPECTED_ERROR);
    }

    let bitop = state.check_number(1).unwrap_or_default() as i32;
    if !(B_NOT..=B_ROR).contains(&bitop) {
      log::error!("invalid bitop was passed, should never happen");
      return lua_wrapped_error_view(state, 1, ConstantStrings::UNEXPECTED_ERROR);
    }

    // Handle bnot specially
    if bitop == B_NOT {
      if lua_arg_count < 2 || state.type_name(2) != Some("number") {
        return lua_wrapped_error_view(state, 1, ConstantStrings::BAD_ARG_BNOT);
      }

      let val = lua_number_to_bit_value(state.check_number(2).unwrap_or_default());
      let res = !val;
      state.pop(2);

      state.push_number(f64::from(res));
      return 1;
    }

    let bin_op_err = match bitop {
      B_OR => ConstantStrings::BAD_ARG_BOR,
      B_AND => ConstantStrings::BAD_ARG_BAND,
      B_XOR => ConstantStrings::BAD_ARG_BXOR,
      B_LSHIFT => ConstantStrings::BAD_ARG_LSHIFT,
      B_RSHIFT => ConstantStrings::BAD_ARG_RSHIFT,
      B_ARSHIFT => ConstantStrings::BAD_ARG_ARSHIFT,
      B_ROL => ConstantStrings::BAD_ARG_ROL,
      _ => ConstantStrings::BAD_ARG_ROR,
    };

    if lua_arg_count < 2 {
      return lua_wrapped_error_view(state, 1, bin_op_err);
    }

    if matches!(bitop, B_OR | B_AND | B_XOR) {
      let mut ret: i32 = if bitop == B_AND { -1 } else { 0 };

      for arg_ix in 2..=lua_arg_count {
        if state.type_name(arg_ix) != Some("number") {
          return lua_wrapped_error_view(state, 1, bin_op_err);
        }

        let next_value = lua_number_to_bit_value(state.check_number(arg_ix).unwrap_or_default());

        ret = match bitop {
          B_OR => ret | next_value,
          B_XOR => ret ^ next_value,
          _ => ret & next_value,
        };
      }

      state.pop(lua_arg_count as usize);
      state.push_number(f64::from(ret));

      return 1;
    }

    if lua_arg_count < 3
      || state.type_name(2) != Some("number")
      || state.type_name(3) != Some("number")
    {
      return lua_wrapped_error_view(state, 1, bin_op_err);
    }

    let x = lua_number_to_bit_value(state.check_number(2).unwrap_or_default());
    let n = (state.check_number(3).unwrap_or_default() as i32) & 0b1111;

    let shift_res = match bitop {
      B_LSHIFT => ((x as u32).wrapping_shl(n as u32)) as i32,
      B_RSHIFT => ((x as u32).wrapping_shr(n as u32)) as i32,
      B_ARSHIFT => x.wrapping_shr(n as u32),
      B_ROL => (x as u32).rotate_left(n as u32) as i32,
      _ => (x as u32).rotate_right(n as u32) as i32,
    };

    state.pop(lua_arg_count as usize);
    state.push_number(f64::from(shift_res));
    1
  }

  /// libs/server/Lua/LuaRunner.Functions.cs:CJsonEncode
  pub fn c_json_encode(state: &mut LuaStateWrapper, host: &mut HostShared) -> i32 {
    let lua_arg_count = state.get_top() as i32;
    if lua_arg_count != 1 {
      return lua_wrapped_error_view(state, 1, ConstantStrings::BAD_ARG_ENCODE);
    }

    host.scratch.reset();
    let ret = Self::encode(state, host, 0);

    if ret == 1 {
      // Encoding should leave nothing on the stack
      debug_assert!(state.expect_lua_stack_empty());

      // Push the encoded string
      let result = host.scratch.view_full_arg_slice().to_vec();
      if !state.try_push_buffer(&result) {
        return lua_wrapped_error_view(state, 1, ConstantStrings::OUT_OF_MEMORY);
      }
    }

    ret
  }

  /// libs/server/Lua/LuaRunner.Functions.cs:Encode
  #[allow(clippy::too_many_lines)]
  fn encode(state: &mut LuaStateWrapper, host: &mut HostShared, depth: i32) -> i32 {
    if depth > MAX_ENCODE_DEPTH {
      // Match Redis max decoding depth
      return lua_wrapped_error_view(state, 1, ConstantStrings::CANNOT_SERIALISE_NESTING);
    }

    let arg_type = state.type_name(-1);

    match arg_type {
      Some("boolean") => Self::encode_bool(state, host),
      Some("nil") => Self::encode_null(state, host),
      Some("number") => Self::encode_number(state, host),
      Some("string") => Self::encode_string(state, host),
      Some("table") => Self::encode_table(state, host, depth),
      _ => {
        log::error!("Cannot serialize {arg_type:?} to JSON");
        lua_wrapped_error_view(state, 1, ConstantStrings::CANNOT_SERIALISE_TO_JSON)
      }
    }
  }

  /// libs/server/Lua/LuaRunner.Functions.cs:EncodeBool
  fn encode_bool(state: &mut LuaStateWrapper, host: &mut HostShared) -> i32 {
    debug_assert_eq!(
      state.type_name(-1),
      Some("boolean"),
      "Expected boolean on top of stack"
    );

    let data: &[u8] = if state.to_boolean(-1) {
      b"true"
    } else {
      b"false"
    };
    host.scratch.append(data);
    state.pop(1);

    1
  }

  /// libs/server/Lua/LuaRunner.Functions.cs:EncodeNull
  fn encode_null(state: &mut LuaStateWrapper, host: &mut HostShared) -> i32 {
    debug_assert_eq!(
      state.type_name(-1),
      Some("nil"),
      "Expected nil on top of stack"
    );

    host.scratch.append(b"null");
    state.pop(1);

    1
  }

  /// libs/server/Lua/LuaRunner.Functions.cs:EncodeNumber
  fn encode_number(state: &mut LuaStateWrapper, host: &mut HostShared) -> i32 {
    debug_assert_eq!(
      state.type_name(-1),
      Some("number"),
      "Expected number on top of stack"
    );

    let number = state.check_number(-1).unwrap_or_default();
    host.scratch.append(format_number_g(number).as_bytes());
    state.pop(1);

    1
  }

  /// libs/server/Lua/LuaRunner.Functions.cs:EncodeString
  fn encode_string(state: &mut LuaStateWrapper, host: &mut HostShared) -> i32 {
    debug_assert_eq!(
      state.type_name(-1),
      Some("string"),
      "Expected string on top of stack"
    );

    let buff = state.known_string_to_buffer(-1).unwrap_or_default();

    host.scratch.append_byte(b'"');

    let mut rest: &[u8] = &buff;
    while let Some(escape_ix) = rest.iter().position(|b| *b == b'"' || *b == b'\\') {
      host.scratch.append(&rest[..escape_ix]);
      host.scratch.append_byte(b'\\');
      host.scratch.append_byte(rest[escape_ix]);
      rest = &rest[escape_ix + 1..];
    }
    host.scratch.append(rest);
    host.scratch.append_byte(b'"');

    state.pop(1);

    1
  }

  /// libs/server/Lua/LuaRunner.Functions.cs:EncodeTable
  fn encode_table(state: &mut LuaStateWrapper, host: &mut HostShared, depth: i32) -> i32 {
    debug_assert_eq!(
      state.type_name(-1),
      Some("table"),
      "Expected table on top of stack"
    );

    let table_index = state.get_top() as i32;

    let mut is_array = false;
    let mut array_length: i64 = 0;

    state.push_nil();
    while state.next() {
      // Pop value
      state.pop(1);

      let key_number = state.check_number(table_index + 1);
      let key_is_integral = key_number.is_some_and(|key_as_number| {
        key_as_number >= 1.0 && key_as_number == (key_as_number as i64) as f64
      });

      if key_is_integral {
        let key_as_number = key_number.unwrap_or_default();
        if key_as_number > array_length as f64 {
          // Need at least one integer key >= 1 to consider this an array
          is_array = true;
          array_length = key_as_number as i64;
        }
      } else {
        // Non-integer key, or integer <= 0, so it's not an array
        is_array = false;

        // Remove key
        state.pop(1);

        break;
      }
    }

    if is_array {
      Self::encode_array(state, host, array_length, depth)
    } else {
      Self::encode_object(state, host, depth)
    }
  }

  /// libs/server/Lua/LuaRunner.Functions.cs:EncodeArray
  fn encode_array(
    state: &mut LuaStateWrapper,
    host: &mut HostShared,
    length: i64,
    depth: i32,
  ) -> i32 {
    debug_assert_eq!(
      state.type_name(-1),
      Some("table"),
      "Expected table on top of stack"
    );

    let table_index = state.get_top() as i32;

    host.scratch.append_byte(b'[');

    for ix in 1..=length {
      if ix != 1 {
        host.scratch.append_byte(b',');
      }

      _ = state.raw_get_integer(table_index, ix);
      let r = Self::encode(state, host, depth + 1);
      if r != 1 {
        return r;
      }
    }

    host.scratch.append_byte(b']');

    // Remove table
    state.pop(1);

    1
  }

  /// libs/server/Lua/LuaRunner.Functions.cs:EncodeObject
  fn encode_object(state: &mut LuaStateWrapper, host: &mut HostShared, depth: i32) -> i32 {
    debug_assert_eq!(
      state.type_name(-1),
      Some("table"),
      "Expected table on top of stack"
    );

    let table_index = state.get_top() as i32;

    host.scratch.append_byte(b'{');

    let mut first_value = true;

    state.push_nil();
    while state.next() {
      let key_type = state.type_name(table_index + 1);
      if !matches!(key_type, Some("string") | Some("number")) {
        // Ignore non-string-ify-able keys

        // Remove value
        state.pop(1);

        continue;
      }

      if !first_value {
        host.scratch.append_byte(b',');
      }

      // Copy key to top of stack
      state.push_value(table_index + 1);

      // Force the _copy_ of the key to be a string if it is not already one.
      // We don't modify the original key value, so we can continue using it with Next.
      if key_type == Some("number") && !state.try_number_to_string() {
        return lua_wrapped_error_view(state, 1, ConstantStrings::OUT_OF_MEMORY);
      }

      // Encode key (the copy on top)
      let r1 = Self::encode(state, host, depth + 1);
      if r1 != 1 {
        return r1;
      }

      host.scratch.append_byte(b':');

      // Encode value
      let r2 = Self::encode(state, host, depth + 1);
      if r2 != 1 {
        return r2;
      }

      first_value = false;
    }

    host.scratch.append_byte(b'}');

    // Remove table
    state.pop(1);

    1
  }

  /// libs/server/Lua/LuaRunner.Functions.cs:CJsonDecode
  pub fn c_json_decode(state: &mut LuaStateWrapper, _host: &mut HostShared) -> i32 {
    let lua_arg_count = state.get_top() as i32;
    if lua_arg_count != 1 {
      return lua_wrapped_error_view(state, 1, ConstantStrings::BAD_ARG_DECODE);
    }

    let arg_type = state.type_name(1);
    if arg_type == Some("number") {
      // We'd coerce this to a string, and then decode it, so just pass it back as is
      return 1;
    }

    if arg_type != Some("string") {
      return lua_wrapped_error_view(state, 1, ConstantStrings::BAD_ARG_DECODE);
    }

    let buff = state.known_string_to_buffer(1).unwrap_or_default();
    let text = String::from_utf8_lossy(&buff);

    match sonic_rs::from_str::<sonic_rs::Value>(&text) {
      Ok(parsed) => Self::decode(state, &parsed),
      Err(e) => {
        let message = e.to_string().to_ascii_lowercase();
        if message.contains("depth") || message.contains("recursion") {
          // Maximum depth exceeded, munge to a compatible Redis error
          lua_wrapped_error_view(state, 1, ConstantStrings::FOUND_TOO_MANY_NESTED)
        } else {
          // Invalid token is implied (and matches Redis error replies)
          lua_wrapped_error_view(state, 1, ConstantStrings::EXPECTED_VALUE_BUT_FOUND)
        }
      }
    }
  }

  /// libs/server/Lua/LuaRunner.Functions.cs:Decode
  fn decode(state: &mut LuaStateWrapper, node: &sonic_rs::Value) -> i32 {
    if node.is_object() {
      Self::decode_object(state, node)
    } else if node.is_array() {
      Self::decode_array(state, node)
    } else if node.is_null() {
      state.push_nil();
      1
    } else if let Some(boolean) = node.as_bool() {
      state.push_boolean(boolean);
      1
    } else if let Some(number) = node.as_f64() {
      state.push_number(number);
      1
    } else if let Some(text) = node.as_str() {
      if !state.try_push_buffer(text.as_bytes()) {
        return lua_wrapped_error_view(state, 1, ConstantStrings::OUT_OF_MEMORY);
      }
      1
    } else {
      log::error!("Unexpected json node type");
      lua_wrapped_error_view(state, 1, ConstantStrings::UNEXPECTED_JSON_VALUE_KIND)
    }
  }

  /// libs/server/Lua/LuaRunner.Functions.cs:DecodeValue
  fn decode_value(state: &mut LuaStateWrapper, value: &sonic_rs::Value) -> i32 {
    if value.is_null() {
      state.push_nil();
    } else if let Some(boolean) = value.as_bool() {
      state.push_boolean(boolean);
    } else if let Some(number) = value.as_f64() {
      state.push_number(number);
    } else if let Some(text) = value.as_str() {
      if !state.try_push_buffer(text.as_bytes()) {
        return lua_wrapped_error_view(state, 1, ConstantStrings::OUT_OF_MEMORY);
      }
    } else {
      log::error!("Unexpected json value kind");
      return lua_wrapped_error_view(state, 1, ConstantStrings::UNEXPECTED_JSON_VALUE_KIND);
    }

    1
  }

  /// libs/server/Lua/LuaRunner.Functions.cs:DecodeArray
  fn decode_array(state: &mut LuaStateWrapper, arr: &sonic_rs::Value) -> i32 {
    let Some(items) = arr.as_array() else {
      return lua_wrapped_error_view(state, 1, ConstantStrings::UNEXPECTED_JSON_VALUE_KIND);
    };

    if !state.try_create_table(items.len(), 0) {
      return lua_wrapped_error_view(state, 1, ConstantStrings::OUT_OF_MEMORY);
    }

    let table_index = state.get_top() as i32;

    for (ix, item) in items.iter().enumerate() {
      // Places item on the stack
      let r = Self::decode_value(state, item);
      if r != 1 {
        // Propagate error return
        return r;
      }

      // Save into the table
      if let Some(value) = state.pop_value() {
        state.raw_set_integer(table_index, ix as i64 + 1, value);
      }
    }

    1
  }

  /// libs/server/Lua/LuaRunner.Functions.cs:DecodeObject
  fn decode_object(state: &mut LuaStateWrapper, obj: &sonic_rs::Value) -> i32 {
    let Some(entries) = obj.as_object() else {
      return lua_wrapped_error_view(state, 1, ConstantStrings::UNEXPECTED_JSON_VALUE_KIND);
    };

    if !state.try_create_table(0, entries.len()) {
      return lua_wrapped_error_view(state, 1, ConstantStrings::OUT_OF_MEMORY);
    }
    let table_index = state.get_top() as i32;

    for (key, value) in entries.iter() {
      // Decode key to string
      if !state.try_push_buffer(key.as_bytes()) {
        return lua_wrapped_error_view(state, 1, ConstantStrings::OUT_OF_MEMORY);
      }

      // Decode value
      let r = Self::decode_value(state, value);
      if r != 1 {
        return r;
      }

      state.raw_set(table_index);
    }

    1
  }

  /// libs/server/Lua/LuaRunner.Functions.cs:CMsgPackPack
  pub fn c_msg_pack_pack(state: &mut LuaStateWrapper, host: &mut HostShared) -> i32 {
    let num_lua_args = state.get_top() as i32;

    if num_lua_args == 0 {
      return lua_wrapped_error_view(state, 1, ConstantStrings::BAD_ARG_PACK);
    }

    // Redis concatenates all the message packs together if there are multiple.
    // Somewhat odd, but we match that behavior.
    host.scratch.reset();

    for _ in 0..num_lua_args {
      // Because each encode removes the encoded value we always encode position 1
      let mut err: Option<&'static [u8]> = None;
      if !Self::msgpack_try_encode(state, host, 1, 0, &mut err) {
        return lua_wrapped_error_view(state, 1, err.unwrap_or(ConstantStrings::UNEXPECTED_ERROR));
      }
    }

    // After all encoding, stack should be empty
    debug_assert!(state.expect_lua_stack_empty());

    let ret = host.scratch.view_full_arg_slice().to_vec();
    if !state.try_push_buffer(&ret) {
      return lua_wrapped_error_view(state, 1, ConstantStrings::OUT_OF_MEMORY);
    }

    1
  }

  /// libs/server/Lua/LuaRunner.Functions.cs:TryEncode
  fn msgpack_try_encode(
    state: &mut LuaStateWrapper,
    host: &mut HostShared,
    stack_index: i32,
    depth: usize,
    err: &mut Option<&'static [u8]>,
  ) -> bool {
    match state.type_name(stack_index) {
      Some("boolean") => Self::msgpack_try_encode_bool(state, host, stack_index),
      Some("number") => Self::msgpack_try_encode_number(state, host, stack_index),
      Some("string") => Self::msgpack_try_encode_bytes(state, host, stack_index),
      Some("table") => {
        if depth == MAX_MSGPACK_DEPTH {
          // Redis treats a too deeply nested table as a null. This is weird, but we match it.
          host.scratch.append_byte(0xC0);
          state.remove(stack_index);
          return true;
        }

        Self::msgpack_try_encode_table(state, host, stack_index, depth, err)
      }
      // Everything else maps to null, NOT an error
      _ => Self::msgpack_try_encode_null(state, host, stack_index),
    }
  }

  /// libs/server/Lua/LuaRunner.Functions.cs:TryEncodeNull
  fn msgpack_try_encode_null(
    state: &mut LuaStateWrapper,
    host: &mut HostShared,
    stack_index: i32,
  ) -> bool {
    host.scratch.append_byte(0xC0);
    state.remove(stack_index);
    true
  }

  /// libs/server/Lua/LuaRunner.Functions.cs:TryEncodeBool
  fn msgpack_try_encode_bool(
    state: &mut LuaStateWrapper,
    host: &mut HostShared,
    stack_index: i32,
  ) -> bool {
    let value: u8 = if state.to_boolean(stack_index) {
      0xC3
    } else {
      0xC2
    };
    host.scratch.append_byte(value);
    state.remove(stack_index);
    true
  }

  /// libs/server/Lua/LuaRunner.Functions.cs:TryEncodeNumber
  fn msgpack_try_encode_number(
    state: &mut LuaStateWrapper,
    host: &mut HostShared,
    stack_index: i32,
  ) -> bool {
    let num_raw = state.check_number(stack_index).unwrap_or_default();
    let is_int = num_raw == (num_raw as i64) as f64;

    if is_int {
      Self::msgpack_try_encode_integer(host, num_raw as i64);
    } else {
      Self::msgpack_try_encode_floating_point(host, num_raw);
    }

    state.remove(stack_index);
    true
  }

  /// libs/server/Lua/LuaRunner.Functions.cs:TryEncodeInteger
  fn msgpack_try_encode_integer(host: &mut HostShared, value: i64) -> bool {
    let out = &mut host.scratch;

    // positive 7-bit fixint
    if value >= 0 && (value & 0b0111_1111) == value {
      out.append_byte(value as u8);
      return true;
    }

    // negative 5-bit fixint
    if value < 0 && (value | 0b1110_0000_i64) == value {
      out.append_byte(value as u8);
      return true;
    }

    // 8-bit int
    if (i8::MIN as i64..=i8::MAX as i64).contains(&value) {
      out.append_byte(0xD0);
      out.append_byte(value as u8);
      return true;
    }

    // 8-bit uint
    if (0..=u8::MAX as i64).contains(&value) {
      out.append_byte(0xCC);
      out.append_byte(value as u8);
      return true;
    }

    // 16-bit int
    if (i16::MIN as i64..=i16::MAX as i64).contains(&value) {
      out.append_byte(0xD1);
      out.append(&(value as i16).to_be_bytes());
      return true;
    }

    // 16-bit uint
    if (0..=u16::MAX as i64).contains(&value) {
      out.append_byte(0xCD);
      out.append(&(value as u16).to_be_bytes());
      return true;
    }

    // 32-bit int
    if (i32::MIN as i64..=i32::MAX as i64).contains(&value) {
      out.append_byte(0xD2);
      out.append(&(value as i32).to_be_bytes());
      return true;
    }

    // 32-bit uint
    if (0..=u32::MAX as i64).contains(&value) {
      out.append_byte(0xCE);
      out.append(&(value as u32).to_be_bytes());
      return true;
    }

    // 64-bit uint
    if value > u32::MAX as i64 {
      out.append_byte(0xCF);
      out.append(&(value as u64).to_be_bytes());
      return true;
    }

    // 64-bit int
    out.append_byte(0xD3);
    out.append(&value.to_be_bytes());
    true
  }

  /// libs/server/Lua/LuaRunner.Functions.cs:TryEncodeFloatingPoint
  fn msgpack_try_encode_floating_point(host: &mut HostShared, value: f64) -> bool {
    // While Redis has code that attempts to pack doubles into floats
    // it doesn't appear to do anything, so we just always write a double
    host.scratch.append_byte(0xCB);
    host.scratch.append(&value.to_be_bytes());
    true
  }

  /// libs/server/Lua/LuaRunner.Functions.cs:TryEncodeBytes
  fn msgpack_try_encode_bytes(
    state: &mut LuaStateWrapper,
    host: &mut HostShared,
    stack_index: i32,
  ) -> bool {
    let data = state
      .known_string_to_buffer(stack_index)
      .unwrap_or_default();
    let out = &mut host.scratch;

    if data.len() < 32 {
      out.append_byte(0xA0 | data.len() as u8);
      out.append(&data);
    } else if data.len() <= u8::MAX as usize {
      out.append_byte(0xD9);
      out.append_byte(data.len() as u8);
      out.append(&data);
    } else if data.len() <= u16::MAX as usize {
      out.append_byte(0xDA);
      out.append(&(data.len() as u16).to_be_bytes());
      out.append(&data);
    } else {
      out.append_byte(0xDB);
      out.append(&(data.len() as u32).to_be_bytes());
      out.append(&data);
    }

    state.remove(stack_index);
    true
  }

  /// libs/server/Lua/LuaRunner.Functions.cs:TryEncodeTable
  fn msgpack_try_encode_table(
    state: &mut LuaStateWrapper,
    host: &mut HostShared,
    stack_index: i32,
    depth: usize,
    err: &mut Option<&'static [u8]>,
  ) -> bool {
    // A zero-length table is serialized as an array
    let mut is_array = true;
    let mut count = 0usize;
    let mut max: i64 = 0;

    let key_index = state.get_top() as i32 + 1;

    // Measure the table and figure out if we're creating a map or an array
    state.push_nil();
    while state.next() {
      count += 1;

      // Remove value
      state.pop(1);

      let key_as_num = state.check_number(key_index);
      match key_as_num {
        Some(key) if key > 0.0 && key == (key as i64) as f64 => {
          if key as i64 > max {
            max = key as i64;
          }
        }
        _ => {
          is_array = false;
        }
      }
    }

    if is_array && count as i64 == max {
      Self::msgpack_try_encode_array(state, host, stack_index, depth, count, err)
    } else {
      Self::msgpack_try_encode_map(state, host, stack_index, depth, count, err)
    }
  }

  /// libs/server/Lua/LuaRunner.Functions.cs:TryEncodeArray
  fn msgpack_try_encode_array(
    state: &mut LuaStateWrapper,
    host: &mut HostShared,
    stack_index: i32,
    depth: usize,
    count: usize,
    err: &mut Option<&'static [u8]>,
  ) -> bool {
    let table_index = stack_index;

    // Encode length
    if count <= 15 {
      host.scratch.append_byte(0b1001_0000 | count as u8);
    } else if count <= u16::MAX as usize {
      host.scratch.append_byte(0xDC);
      host.scratch.append(&(count as u16).to_be_bytes());
    } else {
      host.scratch.append_byte(0xDD);
      host.scratch.append(&(count as u32).to_be_bytes());
    }

    // Write each element out
    for ix in 1..=count {
      _ = state.raw_get_integer(table_index, ix as i64);
      if !Self::msgpack_try_encode(state, host, table_index + 1, depth + 1, err) {
        return false;
      }
    }

    state.remove(table_index);
    true
  }

  /// libs/server/Lua/LuaRunner.Functions.cs:TryEncodeMap
  fn msgpack_try_encode_map(
    state: &mut LuaStateWrapper,
    host: &mut HostShared,
    stack_index: i32,
    depth: usize,
    count: usize,
    err: &mut Option<&'static [u8]>,
  ) -> bool {
    let _table_index = stack_index;

    // Encode length
    if count <= 15 {
      host.scratch.append_byte(0b1000_0000 | count as u8);
    } else if count <= u16::MAX as usize {
      host.scratch.append_byte(0xDE);
      host.scratch.append(&(count as u16).to_be_bytes());
    } else {
      host.scratch.append_byte(0xDF);
      host.scratch.append(&(count as u32).to_be_bytes());
    }

    state.push_nil();
    while state.next() {
      // Now we have value on top, key one below it

      // Make a copy of the key (above the value)
      state.push_value(-2);

      // Write the key (the top copy)
      if !Self::msgpack_try_encode(state, host, -1, depth + 1, err) {
        return false;
      }

      // Write the value (now on top after key removed)
      if !Self::msgpack_try_encode(state, host, -1, depth + 1, err) {
        return false;
      }
    }

    state.remove(_table_index);
    true
  }

  /// libs/server/Lua/LuaRunner.Functions.cs:TryDecodeNull
  fn msgpack_try_decode_null(state: &mut LuaStateWrapper) -> bool {
    state.push_nil();
    true
  }

  /// libs/server/Lua/LuaRunner.Functions.cs:TryDecodeBoolean
  fn msgpack_try_decode_boolean(state: &mut LuaStateWrapper, b: bool) -> bool {
    state.push_boolean(b);
    true
  }

  /// libs/server/Lua/LuaRunner.Functions.cs:TryDecodeUInt8
  /// libs/server/Lua/LuaRunner.Functions.cs:TryDecodeUInt16
  /// libs/server/Lua/LuaRunner.Functions.cs:TryDecodeUInt32
  /// libs/server/Lua/LuaRunner.Functions.cs:TryDecodeUInt64
  fn msgpack_try_decode_uint(
    state: &mut LuaStateWrapper,
    cursor: &mut &[u8],
    width: usize,
    err: &mut Option<&'static [u8]>,
  ) -> bool {
    let Some(value) = read_be_uint(cursor, width) else {
      *err = Some(ConstantStrings::MISSING_BYTES_IN_INPUT);
      return false;
    };
    *cursor = &cursor[width..];
    state.push_number(value as f64);
    true
  }

  /// libs/server/Lua/LuaRunner.Functions.cs:TryDecodeTinyUInt
  fn msgpack_try_decode_tiny_uint(state: &mut LuaStateWrapper, sigil: u8) {
    state.push_number(f64::from(sigil));
  }

  /// libs/server/Lua/LuaRunner.Functions.cs:TryDecodeTinyInt
  fn msgpack_try_decode_tiny_int(state: &mut LuaStateWrapper, sigil: u8) {
    // 负 5 位 fixint 的符号扩展（0xFFFF_FF00 | sigil 形态）。
    let sign_extended = 0xFFFF_FF00u32 | u32::from(sigil);
    state.push_number(f64::from(sign_extended as i32));
  }

  /// libs/server/Lua/LuaRunner.Functions.cs:TryDecodeInt8
  /// libs/server/Lua/LuaRunner.Functions.cs:TryDecodeInt16
  /// libs/server/Lua/LuaRunner.Functions.cs:TryDecodeInt32
  /// libs/server/Lua/LuaRunner.Functions.cs:TryDecodeInt64
  fn msgpack_try_decode_int(
    state: &mut LuaStateWrapper,
    cursor: &mut &[u8],
    width: usize,
    err: &mut Option<&'static [u8]>,
  ) -> bool {
    let Some(raw) = read_be_bytes(cursor, width) else {
      *err = Some(ConstantStrings::MISSING_BYTES_IN_INPUT);
      return false;
    };
    *cursor = &cursor[width..];
    let value = match width {
      1 => i64::from(raw[0] as i8),
      2 => i64::from(i16::from_be_bytes([raw[0], raw[1]])),
      4 => i64::from(i32::from_be_bytes([raw[0], raw[1], raw[2], raw[3]])),
      _ => i64::from_be_bytes(raw.try_into().unwrap_or([0; 8])),
    };
    state.push_number(value as f64);
    true
  }

  /// libs/server/Lua/LuaRunner.Functions.cs:TryDecodeSingle
  fn msgpack_try_decode_single(state: &mut LuaStateWrapper, raw: [u8; 4]) {
    state.push_number(f64::from(f32::from_be_bytes(raw)));
  }

  /// libs/server/Lua/LuaRunner.Functions.cs:TryDecodeDouble
  fn msgpack_try_decode_double(state: &mut LuaStateWrapper, raw: [u8; 8]) {
    state.push_number(f64::from_be_bytes(raw));
  }

  /// libs/server/Lua/LuaRunner.Functions.cs:CMsgPackUnpack
  pub fn c_msg_pack_unpack(state: &mut LuaStateWrapper, _host: &mut HostShared) -> i32 {
    let num_lua_args = state.get_top() as i32;

    if num_lua_args == 0 || state.type_name(1) != Some("string") {
      // This method returns variable numbers of arguments, so the error goes in the first slot
      return lua_wrapped_error_view(state, 0, ConstantStrings::BAD_ARG_UNPACK);
    }

    let data = state.known_string_to_buffer(1).unwrap_or_default();

    let mut cursor: &[u8] = &data;
    let mut decoded_count: i64 = 0;
    while !cursor.is_empty() {
      let mut err: Option<&'static [u8]> = None;
      if !Self::msgpack_try_decode(state, &mut cursor, &mut err) {
        return lua_wrapped_error_view(
          state,
          0,
          err.unwrap_or(ConstantStrings::MISSING_BYTES_IN_INPUT),
        );
      }
      decoded_count += 1;
    }

    // Error and count for error_wrapper_rvar：输入串仍在栈 1 位，
    // (nil, count) 经 Rotate(2, 2) 移至返回区头部（对标 C# 原语义）。
    state.push_nil();
    state.push_integer(decoded_count);
    state.rotate(2, 2);

    // +2 for the (nil) error slot and the count
    (decoded_count + 2) as i32
  }

  /// libs/server/Lua/LuaRunner.Functions.cs:TryDecode
  fn msgpack_try_decode(
    state: &mut LuaStateWrapper,
    cursor: &mut &[u8],
    err: &mut Option<&'static [u8]>,
  ) -> bool {
    let Some(&sigil) = cursor.first() else {
      *err = Some(ConstantStrings::MISSING_BYTES_IN_INPUT);
      return false;
    };
    *cursor = &cursor[1..];

    match sigil {
      0xC0 => return Self::msgpack_try_decode_null(state),
      0xC2 => return Self::msgpack_try_decode_boolean(state, false),
      0xC3 => return Self::msgpack_try_decode_boolean(state, true),
      // 7-bit positive integers handled below
      // 5-bit negative integers handled below
      0xCC..=0xCF => {
        return Self::msgpack_try_decode_uint(state, cursor, 1usize << (sigil & 0b11), err);
      }
      0xD0..=0xD3 => {
        return Self::msgpack_try_decode_int(state, cursor, 1usize << (sigil & 0b11), err);
      }
      0xCA => {
        let Some(raw) = read_be_bytes(cursor, 4) else {
          *err = Some(ConstantStrings::MISSING_BYTES_IN_INPUT);
          return false;
        };
        *cursor = &cursor[4..];
        Self::msgpack_try_decode_single(state, raw.try_into().unwrap_or([0; 4]));
      }
      0xCB => {
        let Some(raw) = read_be_bytes(cursor, 8) else {
          *err = Some(ConstantStrings::MISSING_BYTES_IN_INPUT);
          return false;
        };
        *cursor = &cursor[8..];
        Self::msgpack_try_decode_double(state, raw.try_into().unwrap_or([0; 8]));
      }
      // <= 31 byte strings handled below
      0xD9 | 0xC4 => {
        let Some(&len) = cursor.first() else {
          *err = Some(ConstantStrings::MISSING_BYTES_IN_INPUT);
          return false;
        };
        *cursor = &cursor[1..];
        return Self::msgpack_push_string(state, cursor, u64::from(len), err);
      }
      0xDA | 0xC5 => {
        let Some(raw) = read_be_bytes(cursor, 2) else {
          *err = Some(ConstantStrings::MISSING_BYTES_IN_INPUT);
          return false;
        };
        *cursor = &cursor[2..];
        return Self::msgpack_push_string(
          state,
          cursor,
          u64::from(u16::from_be_bytes([raw[0], raw[1]])),
          err,
        );
      }
      0xDB | 0xC6 => {
        let Some(raw) = read_be_bytes(cursor, 4) else {
          *err = Some(ConstantStrings::MISSING_BYTES_IN_INPUT);
          return false;
        };
        *cursor = &cursor[4..];
        let len = u32::from_be_bytes([raw[0], raw[1], raw[2], raw[3]]);
        if len > i32::MAX as u32 {
          log::error!("String length is too long: {len}");
          *err = Some(ConstantStrings::MSGPACK_STRING_TOO_LONG);
          return false;
        }
        return Self::msgpack_push_string(state, cursor, u64::from(len), err);
      }
      // <= 15 element arrays are handled below
      0xDC => {
        let Some(raw) = read_be_bytes(cursor, 2) else {
          *err = Some(ConstantStrings::MISSING_BYTES_IN_INPUT);
          return false;
        };
        *cursor = &cursor[2..];
        return Self::msgpack_decode_array(
          state,
          cursor,
          u64::from(u16::from_be_bytes([raw[0], raw[1]])),
          err,
        );
      }
      0xDD => {
        let Some(raw) = read_be_bytes(cursor, 4) else {
          *err = Some(ConstantStrings::MISSING_BYTES_IN_INPUT);
          return false;
        };
        *cursor = &cursor[4..];
        let len = u32::from_be_bytes([raw[0], raw[1], raw[2], raw[3]]);
        if len > i32::MAX as u32 {
          log::error!("Array length is too long: {len}");
          *err = Some(ConstantStrings::MSGPACK_ARRAY_TOO_LONG);
          return false;
        }
        return Self::msgpack_decode_array(state, cursor, u64::from(len), err);
      }
      // <= 15 pair maps are handled below
      0xDE => {
        let Some(raw) = read_be_bytes(cursor, 2) else {
          *err = Some(ConstantStrings::MISSING_BYTES_IN_INPUT);
          return false;
        };
        *cursor = &cursor[2..];
        return Self::msgpack_decode_map(
          state,
          cursor,
          u64::from(u16::from_be_bytes([raw[0], raw[1]])),
          err,
        );
      }
      0xDF => {
        let Some(raw) = read_be_bytes(cursor, 4) else {
          *err = Some(ConstantStrings::MISSING_BYTES_IN_INPUT);
          return false;
        };
        *cursor = &cursor[4..];
        let len = u32::from_be_bytes([raw[0], raw[1], raw[2], raw[3]]);
        if len > i32::MAX as u32 {
          log::error!("Map length is too long: {len}");
          *err = Some(ConstantStrings::MSGPACK_MAP_TOO_LONG);
          return false;
        }
        return Self::msgpack_decode_map(state, cursor, u64::from(len), err);
      }

      _ => {
        if (sigil & 0b1000_0000) == 0 {
          Self::msgpack_try_decode_tiny_uint(state, sigil);
        } else if (sigil & 0b1110_0000) == 0b1110_0000 {
          Self::msgpack_try_decode_tiny_int(state, sigil);
        } else if (sigil & 0b1110_0000) == 0b1010_0000 {
          // Tiny string
          return Self::msgpack_push_string(state, cursor, u64::from(sigil & 0b0001_1111), err);
        } else if (sigil & 0b1111_0000) == 0b1001_0000 {
          // Small array
          return Self::msgpack_decode_array(state, cursor, u64::from(sigil & 0b0000_1111), err);
        } else if (sigil & 0b1111_0000) == 0b1000_0000 {
          // Small map
          return Self::msgpack_decode_map(state, cursor, u64::from(sigil & 0b0000_1111), err);
        } else {
          log::error!("Unexpected MsgPack sigil {sigil}");
          *err = Some(ConstantStrings::UNEXPECTED_MSGPACK_SIGIL);
          return false;
        }
      }
    }

    true
  }

  /// libs/server/Lua/LuaRunner.Functions.cs:TryDecodeSmallArray
  /// libs/server/Lua/LuaRunner.Functions.cs:TryDecodeMidArray
  /// libs/server/Lua/LuaRunner.Functions.cs:TryDecodeLargeArray
  fn msgpack_decode_array(
    state: &mut LuaStateWrapper,
    cursor: &mut &[u8],
    len: u64,
    err: &mut Option<&'static [u8]>,
  ) -> bool {
    // 容量提示封顶（防恶意长度头的巨量预分配；表按需自增长）。
    if !state.try_create_table((len as usize).min(MSGPACK_TABLE_HINT_CAP), 0) {
      *err = Some(ConstantStrings::OUT_OF_MEMORY);
      return false;
    }
    let array_index = state.get_top() as i32;

    for i in 1..=len {
      // Push the element onto the stack
      if !Self::msgpack_try_decode(state, cursor, err) {
        return false;
      }

      if let Some(value) = state.pop_value() {
        state.raw_set_integer(array_index, i as i64, value);
      }
    }

    true
  }

  /// libs/server/Lua/LuaRunner.Functions.cs:TryDecodeSmallMap
  /// libs/server/Lua/LuaRunner.Functions.cs:TryDecodeMidMap
  /// libs/server/Lua/LuaRunner.Functions.cs:TryDecodeLargeMap
  fn msgpack_decode_map(
    state: &mut LuaStateWrapper,
    cursor: &mut &[u8],
    len: u64,
    err: &mut Option<&'static [u8]>,
  ) -> bool {
    if !state.try_create_table(0, (len as usize).min(MSGPACK_TABLE_HINT_CAP)) {
      *err = Some(ConstantStrings::OUT_OF_MEMORY);
      return false;
    }
    let map_index = state.get_top() as i32;

    for _ in 0..len {
      // Push the key onto the stack
      if !Self::msgpack_try_decode(state, cursor, err) {
        return false;
      }

      // Push the value onto the stack
      if !Self::msgpack_try_decode(state, cursor, err) {
        return false;
      }

      state.raw_set(map_index);
    }

    true
  }

  /// libs/server/Lua/LuaRunner.Functions.cs:TryDecodeTinyString
  /// libs/server/Lua/LuaRunner.Functions.cs:TryDecodeSmallString
  /// libs/server/Lua/LuaRunner.Functions.cs:TryDecodeMidString
  /// libs/server/Lua/LuaRunner.Functions.cs:TryDecodeLargeString
  fn msgpack_push_string(
    state: &mut LuaStateWrapper,
    cursor: &mut &[u8],
    len: u64,
    err: &mut Option<&'static [u8]>,
  ) -> bool {
    let len = len as usize;
    if cursor.len() < len {
      *err = Some(ConstantStrings::MISSING_BYTES_IN_INPUT);
      return false;
    }
    if !state.try_push_buffer(&cursor[..len]) {
      *err = Some(ConstantStrings::OUT_OF_MEMORY);
      return false;
    }
    *cursor = &cursor[len..];
    true
  }

  /// libs/server/Lua/LuaRunner.Functions.cs:SetResp
  pub fn set_resp(state: &mut LuaStateWrapper, host: &mut HostShared) -> i32 {
    let lua_arg_count = state.get_top() as i32;
    if lua_arg_count != 1 {
      return lua_wrapped_error_view(state, 0, ConstantStrings::ERR_REDIS_SETRESP_ARG);
    }

    // C# 形态：栈 1 位须为数值类型，且取值为 2 或 3。
    if state.type_name(1) != Some("number") {
      return lua_wrapped_error_view(state, 0, ConstantStrings::ERR_RESP_VERSION);
    }
    let Some(num) = state.check_number(1).filter(|n| *n == 2.0 || *n == 3.0) else {
      return lua_wrapped_error_view(state, 0, ConstantStrings::ERR_RESP_VERSION);
    };

    if let Some(session) = host.session.as_mut() {
      session.get().update_resp_protocol_version(num as u8);
    }

    0
  }

  /// libs/server/Lua/LuaRunner.Functions.cs:AclCheckCommand
  pub fn acl_check_command(state: &mut LuaStateWrapper, host: &mut HostShared) -> i32 {
    let lua_arg_count = state.get_top() as i32;
    if lua_arg_count == 0 {
      return lua_wrapped_error_view(state, 1, ConstantStrings::PLEASE_SPECIFY_REDIS_CALL);
    }

    if state.type_name(1) != Some("string") {
      return lua_wrapped_error_view(state, 1, ConstantStrings::ERR_BAD_ARG);
    }

    let cmd_span = state.known_string_to_buffer(1).unwrap_or_default();

    // resp 域 RespCommandsInfo 为并行域：以大小写不敏感的已知命令名承接
    // 有效性判定（子命令信息待 resp 域就绪后接入）。
    let cmd_str = String::from_utf8_lossy(&cmd_span).to_ascii_uppercase();
    if !KNOWN_ACL_COMMANDS.contains(cmd_str.as_str()) {
      return lua_wrapped_error_view(state, 1, ConstantStrings::ERR_INVALID_COMMAND);
    }

    let provided_resp_arg_count = lua_arg_count - 1;

    let Some(session) = host.session.as_mut() else {
      // runner 模式无会话 ACL 面：视为允许（对标无 RespServerSession 形态）。
      state.pop(lua_arg_count as usize);
      state.push_boolean(true);
      return 1;
    };
    let session = session.get();

    // BITOP is _weird_: 无参形态需逐个子命令检查权限。
    let is_bit_op_parent = cmd_str == "BITOP" && provided_resp_arg_count == 0;

    let success = if is_bit_op_parent {
      const SUB_COMMANDS: &[&[u8]] = &[
        ConstantStrings::AND,
        ConstantStrings::OR,
        ConstantStrings::XOR,
        ConstantStrings::NOT,
        ConstantStrings::DIFF,
      ];
      let mut success = true;
      for sub_command in SUB_COMMANDS {
        // C# 以 BITOP_AND/BITOP_OR 等展开形态检查权限。
        let full_cmd = format!("BITOP_{}", String::from_utf8_lossy(sub_command));
        if !session.check_acl_permissions(&full_cmd) {
          success = false;
          break;
        }
      }
      success
    } else {
      session.check_acl_permissions(&cmd_str)
    };

    // We're done with these, so free up the space
    state.pop(lua_arg_count as usize);

    state.push_boolean(success);
    1
  }

  /// libs/server/Lua/LuaRunner.Functions.cs:PrepareAndCheckRespRequest
  ///
  /// 以 Lua 栈参数拼装 RESP 请求并校验参数类型（占位空参补齐最小元数）。
  pub fn prepare_and_check_resp_request(
    state: &mut LuaStateWrapper,
    scratch: &mut ScratchBufferBuilder,
    cmd_span: &[u8],
    lua_arg_count: i32,
  ) -> bool {
    let provided_resp_arg_count = lua_arg_count - 1;

    scratch.reset();
    scratch.start_command(cmd_span, provided_resp_arg_count.max(0) as usize);

    for i in 0..provided_resp_arg_count.max(0) {
      let stack_ix = 2 + i;
      match state.type_name(stack_ix) {
        Some("nil") => scratch.write_null_argument(),
        Some("string") => {
          let span = stack_bytes(state, stack_ix).unwrap_or_default();
          scratch.write_argument(&span);
        }
        Some("number") => {
          let Some(span) = stack_bytes(state, stack_ix) else {
            return false;
          };
          scratch.write_argument(&span);
        }
        _ => return false,
      }
    }

    true
  }

  /// libs/server/Lua/LuaRunner.Functions.cs:CompileCommon
  ///
  /// 编译为 [`super::lua_runner::LuaRunner::compile_for_session`] 的直调形态
  /// （mlua 侧无 C 函数包装）。
  pub fn compile_common(runner: &mut super::lua_runner::LuaRunner, out: &mut Vec<u8>) {
    runner.compile_for_session(out);
  }

  /// libs/server/Lua/LuaRunner.Functions.cs:ProcessCommandFromScripting
  #[allow(clippy::too_many_lines)]
  pub fn process_command_from_scripting(state: &mut LuaStateWrapper, host: &mut HostShared) -> i32 {
    let arg_count = state.get_top() as i32;

    if arg_count <= 0 {
      return lua_wrapped_error_view(state, 1, ConstantStrings::PLEASE_SPECIFY_REDIS_CALL);
    }

    if state.type_name(1) != Some("string") {
      return lua_wrapped_error_view(state, 1, ConstantStrings::ERR_BAD_ARG);
    }

    let cmd = state.known_string_to_buffer(1).unwrap_or_default();

    // We special-case a few performance-sensitive operations to directly invoke via the storage API
    if cmd.eq_ignore_ascii_case(b"SET") && arg_count == 3 {
      let Some(session) = host.session.as_mut() else {
        return lua_wrapped_error_view(state, 1, ConstantStrings::NO_SESSION_AVAILABLE);
      };
      if !session.get().check_acl_permissions("SET") {
        return lua_wrapped_error_view(state, 1, ConstantStrings::ERR_NO_PERM);
      }

      let Some(key) = stack_bytes(state, 2) else {
        return lua_wrapped_error_view(state, 1, ConstantStrings::ERR_BAD_ARG);
      };
      let Some(value) = stack_bytes(state, 3) else {
        return lua_wrapped_error_view(state, 1, ConstantStrings::ERR_BAD_ARG);
      };

      state.clear_stack();
      if let Err(err) = session.get().set(&key, &value) {
        return lua_wrapped_error_view(state, 1, err.as_bytes());
      }

      state.push_constant_string(ConstantStrings::OK);
      return 1;
    } else if cmd.eq_ignore_ascii_case(b"GET") && arg_count == 2 {
      let Some(session) = host.session.as_mut() else {
        return lua_wrapped_error_view(state, 1, ConstantStrings::ERR_NO_PERM);
      };
      if !session.get().check_acl_permissions("GET") {
        return lua_wrapped_error_view(state, 1, ConstantStrings::ERR_NO_PERM);
      }

      let Some(key) = stack_bytes(state, 2) else {
        return lua_wrapped_error_view(state, 1, ConstantStrings::ERR_BAD_ARG);
      };

      state.clear_stack();
      match session.get().get(&key) {
        Ok(Some(value)) => {
          if !state.try_push_buffer(&value) {
            return lua_wrapped_error_view(state, 1, ConstantStrings::OUT_OF_MEMORY);
          }
        }
        Ok(None) => {
          // Redis is weird, but false instead of Nil is correct here
          state.push_boolean(false);
        }
        Err(err) => {
          return lua_wrapped_error_view(state, 1, err.as_bytes());
        }
      }

      return 1;
    }

    // As fallback, we format a RESP request and dispatch it through the session.

    host.scratch.reset();
    host
      .scratch
      .start_command(&cmd, (arg_count - 1).max(0) as usize);

    for i in 0..(arg_count - 1) {
      let arg_ix = 2 + i;

      match state.type_name(arg_ix) {
        Some("nil") => host.scratch.write_null_argument(),
        Some("string") => {
          let span = state.known_string_to_buffer(arg_ix).unwrap_or_default();
          host.scratch.write_argument(&span);
        }
        Some("number") => {
          let Some(span) = stack_bytes(state, arg_ix) else {
            return lua_wrapped_error_view(state, 1, ConstantStrings::OUT_OF_MEMORY);
          };
          host.scratch.write_argument(&span);
        }
        _ => return lua_wrapped_error_view(state, 1, ConstantStrings::ERR_BAD_ARG),
      }
    }

    let request = host.scratch.view_full_arg_slice().to_vec();

    // Once the request is formatted, we can release all the args on the Lua stack
    state.pop(arg_count as usize);

    let Some(session) = host.session.as_mut() else {
      return lua_wrapped_error_view(state, 1, ConstantStrings::NO_SESSION_AVAILABLE);
    };
    // 响应字节落入 host.sender（与原始会话对象无重叠）。
    session.get().dispatch_resp(&request, &mut host.sender);

    let response = host.sender.get_response().to_vec();
    let resp_protocol_version = host
      .session
      .as_mut()
      .map_or(2, |session| session.get().resp_protocol_version());

    let result = process_resp_response_view(state, resp_protocol_version, &response);

    host.sender.reset();

    result
  }

  /// libs/server/Lua/LuaRunner.Functions.cs:SetCallbackContext
  pub fn set_callback_context(context: *mut HostShared) {
    super::lua_runner::set_callback_context(context);
  }

  /// libs/server/Lua/LuaRunner.Functions.cs:ClearCallbackContext
  pub fn clear_callback_context(context: *mut HostShared) {
    super::lua_runner::clear_callback_context(context);
  }

  /// libs/server/Lua/LuaRunner.Functions.cs:FailOnException
  ///
  /// 异常逃逸不变量被破坏时的兜底：记录后快速失败（C# Environment.FailFast）。
  pub fn fail_on_exception(error: &str, method: &str) -> ! {
    const FORMAT_STRING: &str = "Attempted to propogate exception back to Lua from {0}, this will corrupt the runtime.  Failing fast.";
    log::error!("{FORMAT_STRING} (method={method}, error={error})");
    panic!("{FORMAT_STRING} (method={method})")
  }

  /// libs/server/Lua/LuaRunner.Functions.cs:CompileForRunner（trampoline 形态）
  pub fn compile_for_runner(
    runner: &mut super::lua_runner::LuaRunner,
    out: &mut Vec<u8>,
  ) -> Result<(), String> {
    Self::unsafe_compile_for_runner(runner, out)
  }

  /// libs/server/Lua/LuaRunner.Functions.cs:CompileForSession（trampoline 形态）
  pub fn compile_for_session(runner: &mut super::lua_runner::LuaRunner, out: &mut Vec<u8>) -> bool {
    Self::unsafe_compile_for_session(runner, out)
  }

  /// libs/server/Lua/LuaRunner.Functions.cs:RunPreambleForRunner（trampoline 形态）
  pub fn run_preamble_for_runner(runner: &mut super::lua_runner::LuaRunner) -> bool {
    Self::unsafe_run_preamble_for_runner(runner)
  }

  /// libs/server/Lua/LuaRunner.Functions.cs:RunPreambleForSession（trampoline 形态）
  pub fn run_preamble_for_session(runner: &mut super::lua_runner::LuaRunner) -> bool {
    Self::unsafe_run_preamble_for_session(runner)
  }

  /// libs/server/Lua/LuaRunner.Functions.cs:GarnetCallNoSession（trampoline 形态）
  pub fn garnet_call_no_session(state: &mut LuaStateWrapper, host: &mut HostShared) -> i32 {
    Self::no_session_response(state, host)
  }

  /// libs/server/Lua/LuaRunner.Functions.cs:GarnetCallNoTransaction（trampoline 形态）
  pub fn garnet_call_no_transaction(state: &mut LuaStateWrapper, host: &mut HostShared) -> i32 {
    Self::garnet_call(state, host)
  }

  /// libs/server/Lua/LuaRunner.Functions.Struct.cs:StructPack
  ///
  /// Lua 侧 struct.pack：格式串 + 值序列（数值/字节串）→ 二进制串。
  pub fn struct_pack(state: &mut LuaStateWrapper, _host: &mut HostShared) -> i32 {
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
        if !state.try_push_buffer(&packed) {
          return lua_wrapped_error_view(state, 1, ConstantStrings::OUT_OF_MEMORY);
        }
        1
      }
      None => lua_wrapped_error_view(state, 1, ConstantStrings::BAD_ARG_PACK),
    }
  }

  /// libs/server/Lua/LuaRunner.Functions.Struct.cs:StructUnpack
  ///
  /// Lua 侧 struct.unpack：二进制串（+ 可选 1 基偏移）→ 值序列 + 消费位置。
  pub fn struct_unpack(state: &mut LuaStateWrapper, _host: &mut HostShared) -> i32 {
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
          if !state.try_push_buffer(bytes) {
            return lua_wrapped_error_view(state, 0, ConstantStrings::OUT_OF_MEMORY);
          }
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

  /// libs/server/Lua/LuaRunner.Functions.Struct.cs:StructSize
  ///
  /// Lua 侧 struct.size：格式串 → 打包尺寸。
  pub fn struct_size(state: &mut LuaStateWrapper, _host: &mut HostShared) -> i32 {
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

/// 已知命令名集（ACL 检查的有效性判定；resp 域就绪后切 RespCommandsInfo）。
static KNOWN_ACL_COMMANDS: LazyLock<HashSet<&'static str>> = LazyLock::new(|| {
  [
    "APPEND",
    "BITOP",
    "DECR",
    "DECRBY",
    "DEL",
    "ECHO",
    "EXISTS",
    "EXPIRE",
    "EXPIREAT",
    "EXPIRETIME",
    "FLUSHDB",
    "GET",
    "GETDEL",
    "GETRANGE",
    "GETSET",
    "INCR",
    "INCRBY",
    "INCRBYFLOAT",
    "MGET",
    "MSET",
    "PERSIST",
    "PEXPIRE",
    "PEXPIREAT",
    "PING",
    "PTTL",
    "RENAME",
    "SET",
    "SETEX",
    "SORT",
    "STRLEN",
    "TTL",
    "TYPE",
    "UNLINK",
  ]
  .into_iter()
  .collect()
});

/// 读栈上 string/number（number 就地强转字符串）为字节。
fn stack_bytes(state: &mut LuaStateWrapper, index: i32) -> Option<Vec<u8>> {
  match state.type_name(index) {
    Some("string") => state.known_string_to_buffer(index),
    Some("number") => {
      // 转换在栈顶副本上进行，不动原值。
      state.push_value(index);
      if !state.try_number_to_string() {
        state.pop(1);
        return None;
      }
      let bytes = state.known_string_to_buffer(-1);
      state.pop(1);
      bytes
    }
    _ => None,
  }
}

/// libs/server/Lua/LuaRunner.Functions.cs:LuaNumberToBitValue
pub fn lua_number_to_bit_value(value: f64) -> i32 {
  let scaled = value + 6_755_399_441_055_744.0;
  let as_ulong = scaled.to_bits();
  (as_ulong as u32) as i32
}

/// .NET "G"（invariant）形态的数值文本。
///
/// 有限数：|v| ∈ [1e-5, 1e15) 走十进制最短往返；越界走 15 位有效数字科学
/// 计数（.NET 指数带符号两位）；NaN/∞ 对齐 .NET Core 文案。
fn format_number_g(value: f64) -> String {
  if value.is_nan() {
    return "NaN".into();
  }
  if value.is_infinite() {
    return if value > 0.0 {
      "∞".into()
    } else {
      "-∞".into()
    };
  }
  if value == 0.0 {
    return if value.is_sign_negative() {
      "-0".into()
    } else {
      "0".into()
    };
  }

  let exponent = value.abs().log10().floor() as i32;
  if (-5..15).contains(&exponent) {
    return format!("{value}");
  }

  // 科学计数：15 位有效数字（尾数 1 位整数 + 14 位小数，C# G 去尾零）。
  let scientific = format!("{value:.14e}");
  let (mantissa, exp_part) = scientific
    .split_once('e')
    .unwrap_or((scientific.as_str(), "+00"));
  let mantissa = mantissa.trim_end_matches('0').trim_end_matches('.');
  let exp_value: i32 = exp_part.parse().unwrap_or(0);
  let sign = if exp_value < 0 { '-' } else { '+' };
  format!("{mantissa}e{sign}{:02}", exp_value.abs())
}

/// 大端无符号读（width = 1/2/4/8）。
fn read_be_uint(cursor: &[u8], width: usize) -> Option<u64> {
  let bytes = read_be_bytes(cursor, width)?;
  let mut value = 0u64;
  for byte in bytes {
    value = (value << 8) | u64::from(*byte);
  }
  Some(value)
}

/// 大端字节读。
fn read_be_bytes(cursor: &[u8], width: usize) -> Option<&[u8]> {
  if cursor.len() < width {
    return None;
  }
  Some(&cursor[..width])
}

#[cfg(test)]
mod tests {
  use gxhash::HashSet;

  use super::{LuaRunner_Functions, format_number_g, lua_number_to_bit_value};
  use crate::lua::{
    lua_options::LuaLoggingMode,
    lua_runner::{HostShared, LuaRunner, RespObject},
    lua_runner__functions__struct as struct_codec,
    lua_state_wrapper::LuaStateWrapper,
    session_script_cache::SessionScriptCache,
  };

  #[test]
  fn bit_value_conversion() {
    // C# 基准：+ 2^53+2^52 后取低 32 位。
    assert_eq!(lua_number_to_bit_value(0.0), 0);
    assert_eq!(lua_number_to_bit_value(-1.0), -1);
    assert_eq!(lua_number_to_bit_value(1.0), 1);
    assert_eq!(lua_number_to_bit_value(4_294_967_296.0), 0);
  }

  #[test]
  fn number_g_format() {
    assert_eq!(format_number_g(0.0), "0");
    assert_eq!(format_number_g(3.0), "3");
    assert_eq!(format_number_g(3.5), "3.5");
    assert_eq!(format_number_g(1.5e21), "1.5e+21");
    assert_eq!(format_number_g(f64::NAN), "NaN");
  }

  #[test]
  fn sha1_digest_matches() {
    // sha1("") = da39a3ee5e6b4b0d3255bfef95601890afd80709
    assert_eq!(
      SessionScriptCache::get_script_digest(b"").as_str(),
      "da39a3ee5e6b4b0d3255bfef95601890afd80709"
    );
  }

  #[test]
  fn runner_end_to_end_cmsgpack_and_bit() {
    // 端到端：loader block 沙箱 + 宿主回调（cmsgpack.pack / bit.tohex）。
    let mut runner = LuaRunner::new(
      LuaLoggingMode::Silent,
      None,
      HashSet::default(),
      b"return cmsgpack.pack(1, 'ab'), bit.tohex(255)".to_vec(),
      false,
      "0.0.0.0",
    )
    .unwrap();

    let mut out = Vec::new();
    runner.compile_for_runner(&mut out).unwrap();

    // Redis EVAL 语义：多返回值仅取首个（C# PCall(0, 1)）。
    let ret = runner.run_for_runner(None, None).unwrap();
    assert_eq!(ret, RespObject::BulkString(vec![0x01, 0xA2, b'a', b'b']));
  }

  #[test]
  fn runner_end_to_end_struct_pack_unpack() {
    // 端到端：struct.pack/unpack 走 loader block + 宿主栈契约
    //（i2 = 2 字节整数 + d = 8 字节浮点，消费 10 字节 → 位置 11）。
    let mut runner = LuaRunner::new(
      LuaLoggingMode::Silent,
      None,
      HashSet::default(),
      b"local a, b, c = struct.unpack('<i2d', struct.pack('<i2d', 7, 1.5)); assert(a == 7 and b == 1.5 and c == 11); return 'ok'".to_vec(),
      false,
      "0.0.0.0",
    )
    .unwrap();

    let mut out = Vec::new();
    runner.compile_for_runner(&mut out).unwrap();
    let ret = runner.run_for_runner(None, None).unwrap();
    assert_eq!(ret, RespObject::BulkString(b"ok".to_vec()));
  }

  #[test]
  fn runner_sandbox_hides_outer_globals() {
    // load_sandboxed 绑定 sandbox_env 后沙箱外全局（io）不可见。
    let mut runner = LuaRunner::new(
      LuaLoggingMode::Silent,
      None,
      HashSet::default(),
      b"return io".to_vec(),
      false,
      "7.4.0",
    )
    .unwrap();

    let mut out = Vec::new();
    assert!(runner.compile_for_runner(&mut out).is_ok());
    assert_eq!(runner.run_for_runner(None, None).unwrap(), RespObject::Null);
  }

  #[test]
  fn msgpack_decode_numbers_and_strings() {
    let mut state = LuaStateWrapper::new();
    let mut host = HostShared::new(LuaLoggingMode::Silent, false);

    // fixint 42
    let mut cursor: &[u8] = &[42];
    assert!(LuaRunner_Functions::msgpack_try_decode(
      &mut state,
      &mut cursor,
      &mut None
    ));
    assert_eq!(state.check_number(-1), Some(42.0));
    state.pop(1);

    // negative fixint -5 (0xFB)
    let mut cursor: &[u8] = &[0xFB];
    assert!(LuaRunner_Functions::msgpack_try_decode(
      &mut state,
      &mut cursor,
      &mut None
    ));
    assert_eq!(state.check_number(-1), Some(-5.0));
    state.pop(1);

    // fixstr "hi"
    let mut cursor: &[u8] = &[0xA2, b'h', b'i'];
    assert!(LuaRunner_Functions::msgpack_try_decode(
      &mut state,
      &mut cursor,
      &mut None
    ));
    assert_eq!(state.known_string_to_buffer(-1).unwrap(), b"hi");
    state.pop(1);

    // uint16 1000 (0xCD 0x03 0xE8)
    let mut cursor: &[u8] = &[0xCD, 0x03, 0xE8];
    assert!(LuaRunner_Functions::msgpack_try_decode(
      &mut state,
      &mut cursor,
      &mut None
    ));
    assert_eq!(state.check_number(-1), Some(1000.0));
    state.pop(1);

    let _ = &mut host;
  }

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
