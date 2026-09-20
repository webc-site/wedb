//! 在 garnet 中的相对路径:libs/server/Lua/LuaRunner.Functions.cs
//!
//! bit 库函数族：bit.tobit / bit.tohex / bit.bswap / bitop
//! （对标 LuaRunner.Functions.cs BitOperations 分支）。

use wbase::hex::{HEX_CHARS_LOWER, HEX_CHARS_UPPER};

use crate::{
  LuaState,
  runner::{HostShared, lua_wrapped_error_view},
  strings::ConstantStrings,
};

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

/// 在 garnet 中的相对路径:libs/server/Lua/LuaRunner.Functions.cs:LuaNumberToBitValue
pub fn lua_number_to_bit_value(value: f64) -> i32 {
  let scaled = value + 6_755_399_441_055_744.0;
  let as_ulong = scaled.to_bits();
  (as_ulong as u32) as i32
}

use super::LuaRunnerFunctions;

impl LuaRunnerFunctions {
  /// 在 garnet 中的相对路径:libs/server/Lua/LuaRunner.Functions.cs:BitToBit
  /// 满足 LuaCFunction 统一函数指针签名 (LuaState, HostShared) -> i32 规范，保留 _host 参数
  pub fn bit_to_bit(state: &mut LuaState, _host: &mut HostShared) -> i32 {
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

  /// 在 garnet 中的相对路径:libs/server/Lua/LuaRunner.Functions.cs:BitToHex
  /// 满足 LuaCFunction 统一函数指针签名 (LuaState, HostShared) -> i32 规范，保留 _host 参数
  pub fn bit_to_hex(state: &mut LuaState, _host: &mut HostShared) -> i32 {
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
      HEX_CHARS_UPPER
    } else if num_digits < 0 {
      num_digits = -num_digits;
      HEX_CHARS_UPPER
    } else {
      HEX_CHARS_LOWER
    };

    let num_digits = num_digits.clamp(0, 8) as usize;

    let mut buff = [0u8; 8];
    let start = 8 - num_digits;
    for slot in buff[start..].iter_mut().rev() {
      *slot = hex_bytes[(value & 0xF) as usize];
      value >>= 4;
    }

    // Free up space on stack
    state.pop(lua_arg_count as usize);

    state.push_buffer(&buff[start..]);

    1
  }

  /// 在 garnet 中的相对路径:libs/server/Lua/LuaRunner.Functions.cs:BitBswap
  /// 满足 LuaCFunction 统一函数指针签名 (LuaState, HostShared) -> i32 规范，保留 _host 参数
  pub fn bit_bswap(state: &mut LuaState, _host: &mut HostShared) -> i32 {
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

  /// 在 garnet 中的相对路径:libs/server/Lua/LuaRunner.Functions.cs:Bitop
  /// 满足 LuaCFunction 统一函数指针签名 (LuaState, HostShared) -> i32 规范，保留 _host 参数
  pub fn bitop(state: &mut LuaState, _host: &mut HostShared) -> i32 {
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
}

#[cfg(test)]
mod tests {
  use super::lua_number_to_bit_value;

  #[test]
  fn bit_value_conversion() {
    // C# 基准：+ 2^53+2^52 后取低 32 位。
    assert_eq!(lua_number_to_bit_value(0.0), 0);
    assert_eq!(lua_number_to_bit_value(-1.0), -1);
    assert_eq!(lua_number_to_bit_value(1.0), 1);
    assert_eq!(lua_number_to_bit_value(4_294_967_296.0), 0);
  }
}
