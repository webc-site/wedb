//! 在 garnet 中的相对路径:libs/server/Lua/LuaRunner.Functions.cs
//!
//! 数学与基础库函数族：atan2 / cosh / frexp / ldexp / log10 / pow /
//! sinh / tanh / maxn / loadstring（对标 LuaRunner.Functions.cs 数学分支）。

use super::LuaRunnerFunctions;
use crate::{
  LuaState,
  runner::{HostShared, lua_wrapped_error_view},
  strings::ConstantStrings,
};

impl LuaRunnerFunctions {
  /// 在 garnet 中的相对路径:libs/server/Lua/LuaRunner.Functions.cs:Atan2
  /// 满足 LuaCFunction 统一函数指针签名 (LuaState, HostShared) -> i32 规范，保留 _host 参数
  pub fn atan2(state: &mut LuaState, _host: &mut HostShared) -> i32 {
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

  /// 在 garnet 中的相对路径:libs/server/Lua/LuaRunner.Functions.cs:Cosh
  /// 满足 LuaCFunction 统一函数指针签名 (LuaState, HostShared) -> i32 规范，保留 _host 参数
  pub fn cosh(state: &mut LuaState, _host: &mut HostShared) -> i32 {
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

  /// 在 garnet 中的相对路径:libs/server/Lua/LuaRunner.Functions.cs:Frexp
  /// 满足 LuaCFunction 统一函数指针签名 (LuaState, HostShared) -> i32 规范，保留 _host 参数
  pub fn frexp(state: &mut LuaState, _host: &mut HostShared) -> i32 {
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

  /// 在 garnet 中的相对路径:libs/server/Lua/LuaRunner.Functions.cs:Ldexp
  /// 满足 LuaCFunction 统一函数指针签名 (LuaState, HostShared) -> i32 规范，保留 _host 参数
  pub fn ldexp(state: &mut LuaState, _host: &mut HostShared) -> i32 {
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

  /// 在 garnet 中的相对路径:libs/server/Lua/LuaRunner.Functions.cs:Log10
  /// 满足 LuaCFunction 统一函数指针签名 (LuaState, HostShared) -> i32 规范，保留 _host 参数
  pub fn log10(state: &mut LuaState, _host: &mut HostShared) -> i32 {
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

  /// 在 garnet 中的相对路径:libs/server/Lua/LuaRunner.Functions.cs:Pow
  /// 满足 LuaCFunction 统一函数指针签名 (LuaState, HostShared) -> i32 规范，保留 _host 参数
  pub fn pow(state: &mut LuaState, _host: &mut HostShared) -> i32 {
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

  /// 在 garnet 中的相对路径:libs/server/Lua/LuaRunner.Functions.cs:Sinh
  /// 满足 LuaCFunction 统一函数指针签名 (LuaState, HostShared) -> i32 规范，保留 _host 参数
  pub fn sinh(state: &mut LuaState, _host: &mut HostShared) -> i32 {
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

  /// 在 garnet 中的相对路径:libs/server/Lua/LuaRunner.Functions.cs:Tanh
  /// 满足 LuaCFunction 统一函数指针签名 (LuaState, HostShared) -> i32 规范，保留 _host 参数
  pub fn tanh(state: &mut LuaState, _host: &mut HostShared) -> i32 {
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

  /// 在 garnet 中的相对路径:libs/server/Lua/LuaRunner.Functions.cs:Maxn
  /// 满足 LuaCFunction 统一函数指针签名 (LuaState, HostShared) -> i32 规范，保留 _host 参数
  pub fn maxn(state: &mut LuaState, _host: &mut HostShared) -> i32 {
    let lua_arg_count = state.get_top() as i32;
    if lua_arg_count != 1 || state.type_name(1) != Some("table") {
      return lua_wrapped_error_view(state, 1, ConstantStrings::BAD_ARG_MAXN);
    }

    let mut res: f64 = 0.0;

    // Initial key value onto stack
    state.push_nil();
    while state.lua_next() {
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

  /// 在 garnet 中的相对路径:libs/server/Lua/LuaRunner.Functions.cs:LoadString
  /// 满足 LuaCFunction 统一函数指针签名 (LuaState, HostShared) -> i32 规范，保留 _host 参数
  pub fn load_string(state: &mut LuaState, _host: &mut HostShared) -> i32 {
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

    let res = state.load_buffer(&buff, "=load_string");
    if res.is_err() {
      state.clear_stack();
      state.push_nil();
      state.push_buffer(ConstantStrings::LOAD_STRING_ERROR);
      return 2;
    }

    1
  }
}
