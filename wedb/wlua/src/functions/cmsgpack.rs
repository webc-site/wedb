//! 在 garnet 中的相对路径:libs/server/Lua/LuaRunner.Functions.cs
//!
//! cmsgpack 编解码：cmsgpack.pack / cmsgpack.unpack
//! （对标 LuaRunner.Functions.cs MessagePackEncoding/Decoding 分支）。

use crate::{
  LuaState,
  runner::{HostShared, lua_wrapped_error_view},
  strings::ConstantStrings,
};

/// cmsgpack 最大嵌套深度（C# 写死 16 层后置 null）。
const MAX_MSGPACK_DEPTH: usize = 16;

/// msgpack 复合值的表容量提示上限（防恶意长度头触发巨量预分配；表按需自增长）。
const MSGPACK_TABLE_HINT_CAP: usize = u16::MAX as usize;

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

use super::LuaRunnerFunctions;

impl LuaRunnerFunctions {
  /// 在 garnet 中的相对路径:libs/server/Lua/LuaRunner.Functions.cs:CMsgPackPack
  pub fn c_msg_pack_pack(state: &mut LuaState, host: &mut HostShared) -> i32 {
    let num_lua_args = state.get_top() as i32;

    if num_lua_args == 0 {
      return lua_wrapped_error_view(state, 1, ConstantStrings::BAD_ARG_PACK);
    }

    // Redis concatenates all the message packs together if there are multiple.
    // Somewhat odd, but we match that behavior.
    host.scratch.clear();

    for _ in 0..num_lua_args {
      // Because each encode removes the encoded value we always encode position 1
      let mut err: Option<&'static [u8]> = None;
      if !Self::msgpack_try_encode(state, host, 1, 0, &mut err) {
        return lua_wrapped_error_view(state, 1, err.unwrap_or(ConstantStrings::UNEXPECTED_ERROR));
      }
    }

    // After all encoding, stack should be empty
    debug_assert!(state.expect_lua_stack_empty());

    state.push_buffer(&host.scratch);

    1
  }

  /// 在 garnet 中的相对路径:libs/server/Lua/LuaRunner.Functions.cs:TryEncode
  fn msgpack_try_encode(
    state: &mut LuaState,
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
          host.scratch.push(0xC0);
          state.remove(stack_index);
          return true;
        }

        Self::msgpack_try_encode_table(state, host, stack_index, depth, err)
      }
      // Everything else maps to null, NOT an error
      _ => Self::msgpack_try_encode_null(state, host, stack_index),
    }
  }

  /// 在 garnet 中的相对路径:libs/server/Lua/LuaRunner.Functions.cs:TryEncodeNull
  fn msgpack_try_encode_null(
    state: &mut LuaState,
    host: &mut HostShared,
    stack_index: i32,
  ) -> bool {
    host.scratch.push(0xC0);
    state.remove(stack_index);
    true
  }

  /// 在 garnet 中的相对路径:libs/server/Lua/LuaRunner.Functions.cs:TryEncodeBool
  fn msgpack_try_encode_bool(
    state: &mut LuaState,
    host: &mut HostShared,
    stack_index: i32,
  ) -> bool {
    let value: u8 = if state.to_boolean(stack_index) {
      0xC3
    } else {
      0xC2
    };
    host.scratch.push(value);
    state.remove(stack_index);
    true
  }

  /// 在 garnet 中的相对路径:libs/server/Lua/LuaRunner.Functions.cs:TryEncodeNumber
  fn msgpack_try_encode_number(
    state: &mut LuaState,
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

  /// 在 garnet 中的相对路径:libs/server/Lua/LuaRunner.Functions.cs:TryEncodeInteger
  fn msgpack_try_encode_integer(host: &mut HostShared, value: i64) -> bool {
    let out = &mut host.scratch;

    // positive 7-bit fixint
    if value >= 0 && (value & 0b0111_1111) == value {
      out.push(value as u8);
      return true;
    }

    // negative 5-bit fixint
    if value < 0 && (value | 0b1110_0000_i64) == value {
      out.push(value as u8);
      return true;
    }

    // 8-bit int
    if (i8::MIN as i64..=i8::MAX as i64).contains(&value) {
      out.push(0xD0);
      out.push(value as u8);
      return true;
    }

    // 8-bit uint
    if (0..=u8::MAX as i64).contains(&value) {
      out.push(0xCC);
      out.push(value as u8);
      return true;
    }

    // 16-bit int
    if (i16::MIN as i64..=i16::MAX as i64).contains(&value) {
      out.push(0xD1);
      out.extend_from_slice(&(value as i16).to_be_bytes());
      return true;
    }

    // 16-bit uint
    if (0..=u16::MAX as i64).contains(&value) {
      out.push(0xCD);
      out.extend_from_slice(&(value as u16).to_be_bytes());
      return true;
    }

    // 32-bit int
    if (i32::MIN as i64..=i32::MAX as i64).contains(&value) {
      out.push(0xD2);
      out.extend_from_slice(&(value as i32).to_be_bytes());
      return true;
    }

    // 32-bit uint
    if (0..=u32::MAX as i64).contains(&value) {
      out.push(0xCE);
      out.extend_from_slice(&(value as u32).to_be_bytes());
      return true;
    }

    // 64-bit uint
    if value > u32::MAX as i64 {
      out.push(0xCF);
      out.extend_from_slice(&(value as u64).to_be_bytes());
      return true;
    }

    // 64-bit int
    out.push(0xD3);
    out.extend_from_slice(&value.to_be_bytes());
    true
  }

  /// 在 garnet 中的相对路径:libs/server/Lua/LuaRunner.Functions.cs:TryEncodeFloatingPoint
  fn msgpack_try_encode_floating_point(host: &mut HostShared, value: f64) -> bool {
    // While Redis has code that attempts to pack doubles into floats
    // it doesn't appear to do anything, so we just always write a double
    host.scratch.push(0xCB);
    host.scratch.extend_from_slice(&value.to_be_bytes());
    true
  }

  /// 在 garnet 中的相对路径:libs/server/Lua/LuaRunner.Functions.cs:TryEncodeBytes
  fn msgpack_try_encode_bytes(
    state: &mut LuaState,
    host: &mut HostShared,
    stack_index: i32,
  ) -> bool {
    let data = state
      .known_string_to_buffer(stack_index)
      .unwrap_or_default();
    let out = &mut host.scratch;

    if data.len() < 32 {
      out.push(0xA0 | data.len() as u8);
      out.extend_from_slice(&data);
    } else if data.len() <= u8::MAX as usize {
      out.push(0xD9);
      out.push(data.len() as u8);
      out.extend_from_slice(&data);
    } else if data.len() <= u16::MAX as usize {
      out.push(0xDA);
      out.extend_from_slice(&(data.len() as u16).to_be_bytes());
      out.extend_from_slice(&data);
    } else {
      out.push(0xDB);
      out.extend_from_slice(&(data.len() as u32).to_be_bytes());
      out.extend_from_slice(&data);
    }

    state.remove(stack_index);
    true
  }

  /// 在 garnet 中的相对路径:libs/server/Lua/LuaRunner.Functions.cs:TryEncodeTable
  fn msgpack_try_encode_table(
    state: &mut LuaState,
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
    while state.lua_next() {
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

  /// 在 garnet 中的相对路径:libs/server/Lua/LuaRunner.Functions.cs:TryEncodeArray
  fn msgpack_try_encode_array(
    state: &mut LuaState,
    host: &mut HostShared,
    stack_index: i32,
    depth: usize,
    count: usize,
    err: &mut Option<&'static [u8]>,
  ) -> bool {
    let table_index = stack_index;

    // Encode length
    if count <= 15 {
      host.scratch.push(0b1001_0000 | count as u8);
    } else if count <= u16::MAX as usize {
      host.scratch.push(0xDC);
      host
        .scratch
        .extend_from_slice(&(count as u16).to_be_bytes());
    } else {
      host.scratch.push(0xDD);
      host
        .scratch
        .extend_from_slice(&(count as u32).to_be_bytes());
    }

    // Write each element out
    for ix in 1..=count {
      state.raw_get_integer(table_index, ix as i64);
      if !Self::msgpack_try_encode(state, host, table_index + 1, depth + 1, err) {
        return false;
      }
    }

    state.remove(table_index);
    true
  }

  /// 在 garnet 中的相对路径:libs/server/Lua/LuaRunner.Functions.cs:TryEncodeMap
  fn msgpack_try_encode_map(
    state: &mut LuaState,
    host: &mut HostShared,
    stack_index: i32,
    depth: usize,
    count: usize,
    err: &mut Option<&'static [u8]>,
  ) -> bool {
    // Encode length
    if count <= 15 {
      host.scratch.push(0b1000_0000 | count as u8);
    } else if count <= u16::MAX as usize {
      host.scratch.push(0xDE);
      host
        .scratch
        .extend_from_slice(&(count as u16).to_be_bytes());
    } else {
      host.scratch.push(0xDF);
      host
        .scratch
        .extend_from_slice(&(count as u32).to_be_bytes());
    }

    state.push_nil();
    while state.lua_next() {
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

    state.remove(stack_index);
    true
  }

  /// 在 garnet 中的相对路径:libs/server/Lua/LuaRunner.Functions.cs:TryDecodeNull
  fn msgpack_try_decode_null(state: &mut LuaState) -> bool {
    state.push_nil();
    true
  }

  /// 在 garnet 中的相对路径:libs/server/Lua/LuaRunner.Functions.cs:TryDecodeBoolean
  fn msgpack_try_decode_boolean(state: &mut LuaState, b: bool) -> bool {
    state.push_boolean(b);
    true
  }

  /// 在 garnet 中的相对路径:libs/server/Lua/LuaRunner.Functions.cs:TryDecodeUInt8
  /// 在 garnet 中的相对路径:libs/server/Lua/LuaRunner.Functions.cs:TryDecodeUInt16
  /// 在 garnet 中的相对路径:libs/server/Lua/LuaRunner.Functions.cs:TryDecodeUInt32
  /// 在 garnet 中的相对路径:libs/server/Lua/LuaRunner.Functions.cs:TryDecodeUInt64
  fn msgpack_try_decode_uint(
    state: &mut LuaState,
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

  /// 在 garnet 中的相对路径:libs/server/Lua/LuaRunner.Functions.cs:TryDecodeTinyUInt
  fn msgpack_try_decode_tiny_uint(state: &mut LuaState, sigil: u8) {
    state.push_number(f64::from(sigil));
  }

  /// 在 garnet 中的相对路径:libs/server/Lua/LuaRunner.Functions.cs:TryDecodeTinyInt
  fn msgpack_try_decode_tiny_int(state: &mut LuaState, sigil: u8) {
    // 负 5 位 fixint 的符号扩展（0xFFFF_FF00 | sigil 形态）。
    let sign_extended = 0xFFFF_FF00u32 | u32::from(sigil);
    state.push_number(f64::from(sign_extended as i32));
  }

  /// 在 garnet 中的相对路径:libs/server/Lua/LuaRunner.Functions.cs:TryDecodeInt8
  /// 在 garnet 中的相对路径:libs/server/Lua/LuaRunner.Functions.cs:TryDecodeInt16
  /// 在 garnet 中的相对路径:libs/server/Lua/LuaRunner.Functions.cs:TryDecodeInt32
  /// 在 garnet 中的相对路径:libs/server/Lua/LuaRunner.Functions.cs:TryDecodeInt64
  fn msgpack_try_decode_int(
    state: &mut LuaState,
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

  /// 在 garnet 中的相对路径:libs/server/Lua/LuaRunner.Functions.cs:TryDecodeSingle
  fn msgpack_try_decode_single(state: &mut LuaState, raw: [u8; 4]) {
    state.push_number(f64::from(f32::from_be_bytes(raw)));
  }

  /// 在 garnet 中的相对路径:libs/server/Lua/LuaRunner.Functions.cs:TryDecodeDouble
  fn msgpack_try_decode_double(state: &mut LuaState, raw: [u8; 8]) {
    state.push_number(f64::from_be_bytes(raw));
  }

  /// 在 garnet 中的相对路径:libs/server/Lua/LuaRunner.Functions.cs:CMsgPackUnpack
  /// 满足 LuaCFunction 统一函数指针签名 (LuaState, HostShared) -> i32 规范，保留 _host 参数
  pub fn c_msg_pack_unpack(state: &mut LuaState, _host: &mut HostShared) -> i32 {
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

  /// 在 garnet 中的相对路径:libs/server/Lua/LuaRunner.Functions.cs:TryDecode
  fn msgpack_try_decode(
    state: &mut LuaState,
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

  /// 在 garnet 中的相对路径:libs/server/Lua/LuaRunner.Functions.cs:TryDecodeSmallArray
  /// 在 garnet 中的相对路径:libs/server/Lua/LuaRunner.Functions.cs:TryDecodeMidArray
  /// 在 garnet 中的相对路径:libs/server/Lua/LuaRunner.Functions.cs:TryDecodeLargeArray
  fn msgpack_decode_array(
    state: &mut LuaState,
    cursor: &mut &[u8],
    len: u64,
    err: &mut Option<&'static [u8]>,
  ) -> bool {
    // 容量提示封顶（防恶意长度头的巨量预分配；表按需自增长）。
    state.create_table((len as usize).min(MSGPACK_TABLE_HINT_CAP), 0);
    let array_index = state.get_top() as i32;

    for i in 1..=len {
      // Push the element onto the stack
      if !Self::msgpack_try_decode(state, cursor, err) {
        return false;
      }

      // 值随 raw_set_integer 弹出
      state.raw_set_integer(array_index, i as i64);
    }

    true
  }

  /// 在 garnet 中的相对路径:libs/server/Lua/LuaRunner.Functions.cs:TryDecodeSmallMap
  /// 在 garnet 中的相对路径:libs/server/Lua/LuaRunner.Functions.cs:TryDecodeMidMap
  /// 在 garnet 中的相对路径:libs/server/Lua/LuaRunner.Functions.cs:TryDecodeLargeMap
  fn msgpack_decode_map(
    state: &mut LuaState,
    cursor: &mut &[u8],
    len: u64,
    err: &mut Option<&'static [u8]>,
  ) -> bool {
    state.create_table(0, (len as usize).min(MSGPACK_TABLE_HINT_CAP));
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

  /// 在 garnet 中的相对路径:libs/server/Lua/LuaRunner.Functions.cs:TryDecodeTinyString
  /// 在 garnet 中的相对路径:libs/server/Lua/LuaRunner.Functions.cs:TryDecodeSmallString
  /// 在 garnet 中的相对路径:libs/server/Lua/LuaRunner.Functions.cs:TryDecodeMidString
  /// 在 garnet 中的相对路径:libs/server/Lua/LuaRunner.Functions.cs:TryDecodeLargeString
  fn msgpack_push_string(
    state: &mut LuaState,
    cursor: &mut &[u8],
    len: u64,
    err: &mut Option<&'static [u8]>,
  ) -> bool {
    let len = len as usize;
    if cursor.len() < len {
      *err = Some(ConstantStrings::MISSING_BYTES_IN_INPUT);
      return false;
    }
    state.push_buffer(&cursor[..len]);
    *cursor = &cursor[len..];
    true
  }
}

#[cfg(test)]
mod tests {
  use crate::{LuaState, functions::LuaRunnerFunctions};

  #[test]
  fn msgpack_decode_numbers_and_strings() {
    let mut state = LuaState::new();

    // fixint 42
    let mut cursor: &[u8] = &[42];
    assert!(LuaRunnerFunctions::msgpack_try_decode(
      &mut state,
      &mut cursor,
      &mut None
    ));
    assert_eq!(state.check_number(-1), Some(42.0));
    state.pop(1);

    // negative fixint -5 (0xFB)
    let mut cursor: &[u8] = &[0xFB];
    assert!(LuaRunnerFunctions::msgpack_try_decode(
      &mut state,
      &mut cursor,
      &mut None
    ));
    assert_eq!(state.check_number(-1), Some(-5.0));
    state.pop(1);

    // fixstr "hi"
    let mut cursor: &[u8] = &[0xA2, b'h', b'i'];
    assert!(LuaRunnerFunctions::msgpack_try_decode(
      &mut state,
      &mut cursor,
      &mut None
    ));
    assert_eq!(state.known_string_to_buffer(-1).unwrap(), b"hi");
    state.pop(1);

    // uint16 1000 (0xCD 0x03 0xE8)
    let mut cursor: &[u8] = &[0xCD, 0x03, 0xE8];
    assert!(LuaRunnerFunctions::msgpack_try_decode(
      &mut state,
      &mut cursor,
      &mut None
    ));
    assert_eq!(state.check_number(-1), Some(1000.0));
    state.pop(1);
  }
}
