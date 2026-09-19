//! 在 garnet 中的相对路径:libs/server/Lua/LuaRunner.Functions.cs
//!
//! cjson 编解码：cjson.encode / cjson.decode
//! （对标 LuaRunner.Functions.cs JsonEncoding/Decoding 分支）。

use sonic_rs::prelude::*;

use crate::{
  LuaState,
  runner::{HostShared, lua_wrapped_error_view},
  strings::ConstantStrings,
};

/// cjson 最大嵌套深度（对齐 Redis 解码深度上限）。
const MAX_ENCODE_DEPTH: i32 = 1000;

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

use super::LuaRunnerFunctions;

impl LuaRunnerFunctions {
  /// 在 garnet 中的相对路径:libs/server/Lua/LuaRunner.Functions.cs:CJsonEncode
  pub fn c_json_encode(state: &mut LuaState, host: &mut HostShared) -> i32 {
    let lua_arg_count = state.get_top() as i32;
    if lua_arg_count != 1 {
      return lua_wrapped_error_view(state, 1, ConstantStrings::BAD_ARG_ENCODE);
    }

    host.scratch.clear();
    let ret = Self::encode(state, host, 0);

    if ret == 1 {
      // Encoding should leave nothing on the stack
      debug_assert!(state.expect_lua_stack_empty());

      // Push the encoded string
      state.push_buffer(&host.scratch);
    }

    ret
  }

  /// 在 garnet 中的相对路径:libs/server/Lua/LuaRunner.Functions.cs:Encode
  fn encode(state: &mut LuaState, host: &mut HostShared, depth: i32) -> i32 {
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

  /// 在 garnet 中的相对路径:libs/server/Lua/LuaRunner.Functions.cs:EncodeBool
  fn encode_bool(state: &mut LuaState, host: &mut HostShared) -> i32 {
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
    host.scratch.extend_from_slice(data);
    state.pop(1);

    1
  }

  /// 在 garnet 中的相对路径:libs/server/Lua/LuaRunner.Functions.cs:EncodeNull
  fn encode_null(state: &mut LuaState, host: &mut HostShared) -> i32 {
    debug_assert_eq!(
      state.type_name(-1),
      Some("nil"),
      "Expected nil on top of stack"
    );

    host.scratch.extend_from_slice(b"null");
    state.pop(1);

    1
  }

  /// 在 garnet 中的相对路径:libs/server/Lua/LuaRunner.Functions.cs:EncodeNumber
  fn encode_number(state: &mut LuaState, host: &mut HostShared) -> i32 {
    debug_assert_eq!(
      state.type_name(-1),
      Some("number"),
      "Expected number on top of stack"
    );

    let number = state.check_number(-1).unwrap_or_default();
    host
      .scratch
      .extend_from_slice(format_number_g(number).as_bytes());
    state.pop(1);

    1
  }

  /// 在 garnet 中的相对路径:libs/server/Lua/LuaRunner.Functions.cs:EncodeString
  fn encode_string(state: &mut LuaState, host: &mut HostShared) -> i32 {
    debug_assert_eq!(
      state.type_name(-1),
      Some("string"),
      "Expected string on top of stack"
    );

    let buff = state.known_string_to_buffer(-1).unwrap_or_default();

    host.scratch.push(b'"');

    let mut rest: &[u8] = &buff;
    while let Some(escape_ix) = rest.iter().position(|b| *b == b'"' || *b == b'\\') {
      host.scratch.extend_from_slice(&rest[..escape_ix]);
      host.scratch.push(b'\\');
      host.scratch.push(rest[escape_ix]);
      rest = &rest[escape_ix + 1..];
    }
    host.scratch.extend_from_slice(rest);
    host.scratch.push(b'"');

    state.pop(1);

    1
  }

  /// 在 garnet 中的相对路径:libs/server/Lua/LuaRunner.Functions.cs:EncodeTable
  fn encode_table(state: &mut LuaState, host: &mut HostShared, depth: i32) -> i32 {
    debug_assert_eq!(
      state.type_name(-1),
      Some("table"),
      "Expected table on top of stack"
    );

    let table_index = state.get_top() as i32;

    let mut is_array = false;
    let mut array_length: i64 = 0;

    state.push_nil();
    while state.lua_next() {
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

  /// 在 garnet 中的相对路径:libs/server/Lua/LuaRunner.Functions.cs:EncodeArray
  fn encode_array(state: &mut LuaState, host: &mut HostShared, length: i64, depth: i32) -> i32 {
    debug_assert_eq!(
      state.type_name(-1),
      Some("table"),
      "Expected table on top of stack"
    );

    let table_index = state.get_top() as i32;

    host.scratch.push(b'[');

    for ix in 1..=length {
      if ix != 1 {
        host.scratch.push(b',');
      }

      state.raw_get_integer(table_index, ix);
      let r = Self::encode(state, host, depth + 1);
      if r != 1 {
        return r;
      }
    }

    host.scratch.push(b']');

    // Remove table
    state.pop(1);

    1
  }

  /// 在 garnet 中的相对路径:libs/server/Lua/LuaRunner.Functions.cs:EncodeObject
  fn encode_object(state: &mut LuaState, host: &mut HostShared, depth: i32) -> i32 {
    debug_assert_eq!(
      state.type_name(-1),
      Some("table"),
      "Expected table on top of stack"
    );

    let table_index = state.get_top() as i32;

    host.scratch.push(b'{');

    let mut first_value = true;

    state.push_nil();
    while state.lua_next() {
      let key_type = state.type_name(table_index + 1);
      if !matches!(key_type, Some("string") | Some("number")) {
        // Ignore non-string-ify-able keys

        // Remove value
        state.pop(1);

        continue;
      }

      if !first_value {
        host.scratch.push(b',');
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

      host.scratch.push(b':');

      // Encode value
      let r2 = Self::encode(state, host, depth + 1);
      if r2 != 1 {
        return r2;
      }

      first_value = false;
    }

    host.scratch.push(b'}');

    // Remove table
    state.pop(1);

    1
  }

  /// 在 garnet 中的相对路径:libs/server/Lua/LuaRunner.Functions.cs:CJsonDecode
  /// 满足 LuaCFunction 统一函数指针签名 (LuaState, HostShared) -> i32 规范，保留 _host 参数
  pub fn c_json_decode(state: &mut LuaState, _host: &mut HostShared) -> i32 {
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

  /// 在 garnet 中的相对路径:libs/server/Lua/LuaRunner.Functions.cs:Decode
  fn decode(state: &mut LuaState, node: &sonic_rs::Value) -> i32 {
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
      state.push_buffer(text.as_bytes());
      1
    } else {
      log::error!("Unexpected json node type");
      lua_wrapped_error_view(state, 1, ConstantStrings::UNEXPECTED_JSON_VALUE_KIND)
    }
  }

  /// 在 garnet 中的相对路径:libs/server/Lua/LuaRunner.Functions.cs:DecodeValue
  fn decode_value(state: &mut LuaState, value: &sonic_rs::Value) -> i32 {
    if value.is_null() {
      state.push_nil();
    } else if let Some(boolean) = value.as_bool() {
      state.push_boolean(boolean);
    } else if let Some(number) = value.as_f64() {
      state.push_number(number);
    } else if let Some(text) = value.as_str() {
      state.push_buffer(text.as_bytes());
    } else {
      log::error!("Unexpected json value kind");
      return lua_wrapped_error_view(state, 1, ConstantStrings::UNEXPECTED_JSON_VALUE_KIND);
    }

    1
  }

  /// 在 garnet 中的相对路径:libs/server/Lua/LuaRunner.Functions.cs:DecodeArray
  fn decode_array(state: &mut LuaState, arr: &sonic_rs::Value) -> i32 {
    let Some(items) = arr.as_array() else {
      return lua_wrapped_error_view(state, 1, ConstantStrings::UNEXPECTED_JSON_VALUE_KIND);
    };

    state.create_table(items.len(), 0);
    let table_index = state.get_top() as i32;

    for (ix, item) in items.iter().enumerate() {
      // Places item on the stack
      let r = Self::decode_value(state, item);
      if r != 1 {
        // Propagate error return
        return r;
      }

      // Save into the table（值随 raw_set_integer 弹出）
      state.raw_set_integer(table_index, ix as i64 + 1);
    }

    1
  }

  /// 在 garnet 中的相对路径:libs/server/Lua/LuaRunner.Functions.cs:DecodeObject
  fn decode_object(state: &mut LuaState, obj: &sonic_rs::Value) -> i32 {
    let Some(entries) = obj.as_object() else {
      return lua_wrapped_error_view(state, 1, ConstantStrings::UNEXPECTED_JSON_VALUE_KIND);
    };

    state.create_table(0, entries.len());
    let table_index = state.get_top() as i32;

    for (key, value) in entries.iter() {
      // Decode key to string
      state.push_buffer(key.as_bytes());

      // Decode value
      let r = Self::decode_value(state, value);
      if r != 1 {
        return r;
      }

      state.raw_set(table_index);
    }

    1
  }
}

#[cfg(test)]
mod tests {
  use super::format_number_g;

  #[test]
  fn number_g_format() {
    assert_eq!(format_number_g(0.0), "0");
    assert_eq!(format_number_g(3.0), "3");
    assert_eq!(format_number_g(3.5), "3.5");
    assert_eq!(format_number_g(1.5e21), "1.5e+21");
    assert_eq!(format_number_g(f64::NAN), "NaN");
  }
}
