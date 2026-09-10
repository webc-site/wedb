//! 位图命令（SETBIT/GETBIT/BITCOUNT/BITPOS/BITOP/BITFIELD/BITFIELD_RO）
//!
//! 同步快路径：字符串域直读直写，磁盘候选等须异步裁决时返回 `Ok(false)`
//! 交调用方降级。位序对标 Redis/C#：bit 0 为首字节最高位。
//! C# 侧会话层经 `StringInput` 把参数传给存储回调；Rust 侧存储接口收敛到
//! 会话层直连，故 BITFIELD 子命令在解析期即固化为类型化参数。

use super::{
  super::{
    basic_commands::MAX_STRING_PAYLOAD_BYTES,
    cmd_strings as cs,
    cmd_strings::{abort_with_error_message, abort_with_wrong_number_of_arguments},
    parser::resp_ext::{RespSliceExt, RespVecExt},
    resp_server_session::RespServerSession,
  },
  bitmap_manager::{try_validate_bit_pos_offsets, try_validate_bitfield_offset},
  bitmap_manager_bit_op::{BitmapOperation, invoke_bit_operation_unsafe},
  bitmap_manager_bit_pos::bit_pos_driver,
  bitmap_manager_bitfield::{
    BitFieldCmdArgs, BitFieldOverflow, BitFieldSecondaryCommand, bit_field_execute,
    new_block_alloc_length_from_type,
  },
};

/// libs/server/Resp/CmdStrings.cs:RESP_ERR_WRONG_NUMBER_OF_ARGUMENTS
const RESP_ERR_WRONG_NUMBER_OF_ARGUMENTS: &str = "ERR wrong number of arguments";
/// libs/server/Resp/CmdStrings.cs:RESP_ERR_BITOP_KEY_LIMIT
const RESP_ERR_BITOP_KEY_LIMIT: &str = "ERR Bitop source key limit (64) exceeded";
/// libs/server/Resp/CmdStrings.cs:RESP_ERR_BITOP_DIFF_TWO_SOURCE_KEYS_REQUIRED
const RESP_ERR_BITOP_DIFF_TWO_SOURCE_KEYS_REQUIRED: &str =
  "ERR BITOP DIFF must be called with at least two source keys.";
/// libs/server/Resp/CmdStrings.cs:RESP_ERR_BITOP_NOT_SINGLE_SOURCE_KEY
const RESP_ERR_BITOP_NOT_SINGLE_SOURCE_KEY: &str =
  "ERR BITOP NOT must be called with a single source key.";
/// libs/server/Resp/CmdStrings.cs:RESP_ERR_INVALID_BITFIELD_TYPE
const RESP_ERR_INVALID_BITFIELD_TYPE: &str =
  "ERR Invalid bitfield type. Use something like i16 u8. Note that u64 is not supported but i64 is";
/// libs/server/Resp/CmdStrings.cs:RESP_ERR_INVALID_OVERFLOW_TYPE
const RESP_ERR_INVALID_OVERFLOW_TYPE: &str = "ERR Invalid OVERFLOW type specified";
/// libs/server/Resp/CmdStrings.cs:RESP_ERRNOTFOUND（RESP2 位域 nil 应答）
const RESP_ERRNOTFOUND: &[u8] = b"$-1\r\n";
/// libs/server/Resp/CmdStrings.cs:RESP3_NULL_REPLY（RESP3 位域 nil 应答）
const RESP3_NULL_REPLY: &[u8] = b"_\r\n";

/// 合法位偏移上限（BitmapManager.MaxOffsetForBitmapLength）
const MAX_BIT_OFFSET: i64 = (MAX_STRING_PAYLOAD_BYTES as i64 * 8) - 1;

/// SETBIT/GETBIT 的 offset 参数校验（对标 C# IsValidBitOffset 口径）
fn parse_bit_offset(raw: &[u8]) -> Option<i64> {
  let offset = raw.try_parse_i64()?;
  (0..=MAX_BIT_OFFSET).contains(&offset).then_some(offset)
}

/// 写位域 nil 应答（RESP2 `$-1` / RESP3 `_`；C# functionsState.nilResp）
fn write_bitfield_nil(session: &RespServerSession, output: &mut Vec<u8>) {
  if session.resp_protocol_version >= 3 {
    output.extend_from_slice(RESP3_NULL_REPLY);
  } else {
    output.extend_from_slice(RESP_ERRNOTFOUND);
  }
}

/// 位域 encoding 解析（C# TryGetBitfieldEncoding：`i<位宽>` / `u<位宽>`，
/// 位宽 > 0，有符号 ≤ 64，无符号 < 64）
///
/// 返回（位宽，是否带符号）
fn parse_bitfield_encoding(encoding: &[u8]) -> Option<(u8, bool)> {
  if encoding.len() <= 1 {
    return None;
  }
  let signed = match encoding[0] {
    b'i' => true,
    b'u' => false,
    _ => return None,
  };
  let bit_count = encoding[1..].try_parse_i64()?;
  (bit_count > 0
    && if signed {
      bit_count <= 64
    } else {
      bit_count < 64
    })
  .then_some((bit_count as u8, signed))
}

/// 位域 offset 解析（C# TryGetBitfieldOffset：`#<n>` 倍乘形式或裸位偏移，
/// 须 ≥ 0）
///
/// 返回（offset，是否倍乘）
fn parse_bitfield_offset(raw: &[u8]) -> Option<(i64, bool)> {
  let (digits, multiply_offset) = match raw {
    [b'#', rest @ ..] if !rest.is_empty() => (rest, true),
    _ => (raw, false),
  };
  let offset = digits.try_parse_i64()?;
  (offset >= 0).then_some((offset, multiply_offset))
}

/// 位域解析期联查：encoding + offset 校验并给出 typeInfo / 归一化 offset
///
/// C# 侧为 TryGetBitfieldEncoding + TryGetBitfieldOffset +
/// BitmapManager.TryValidateBitfieldOffset 三步；返回（typeInfo，offset）
fn parse_bitfield_type_offset(encoding: &[u8], offset_raw: &[u8]) -> Option<(u8, i64)> {
  let (bit_count, signed) = parse_bitfield_encoding(encoding)?;
  let (offset, multiply_offset) = parse_bitfield_offset(offset_raw)?;
  let (normalized_offset, _) = try_validate_bitfield_offset(offset, bit_count, multiply_offset)?;
  let type_info = if signed { 0x80 | bit_count } else { bit_count };
  Some((type_info, normalized_offset))
}

/// 解析溢出策略切片（C# TryGetBitFieldOverflow）
fn parse_bitfield_overflow(raw: &[u8]) -> Option<BitFieldOverflow> {
  if raw.eq_ignore_ascii_case(b"WRAP") {
    Some(BitFieldOverflow::Wrap)
  } else if raw.eq_ignore_ascii_case(b"SAT") {
    Some(BitFieldOverflow::Sat)
  } else if raw.eq_ignore_ascii_case(b"FAIL") {
    Some(BitFieldOverflow::Fail)
  } else {
    None
  }
}

impl RespServerSession {
  /// libs/server/Resp/Bitmap/BitmapCommands.cs:NetworkStringSetBit
  ///
  /// 键不存在时按需增长建值；应答改写前的原 bit 值
  pub fn network_string_set_bit<'a, D: wdev::Device>(
    &mut self,
    parse_state: &[&[u8]],
    store: &wkv::BatchStoreSession<'a, D>,
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    if parse_state.len() != 3 {
      abort_with_wrong_number_of_arguments(output, "SETBIT");
      return Ok(true);
    }
    let key = parse_state[0];
    let Some(offset) = parse_bit_offset(parse_state[1]) else {
      abort_with_error_message(output, cs::RESP_ERR_GENERIC_BITOFFSET_IS_NOT_INTEGER);
      return Ok(true);
    };
    // C#：bit 参数须为单字符 '0'/'1'
    if !matches!(parse_state[2], b"0" | b"1") {
      abort_with_error_message(output, cs::RESP_ERR_GENERIC_BIT_IS_NOT_INTEGER);
      return Ok(true);
    }
    let bit = parse_state[2][0] - b'0';

    let mut val = match store.try_read_sync(key, |v| v.to_vec()) {
      Ok(Some(Some(v))) => v,
      Ok(Some(None)) => Vec::new(),
      // 磁盘候选：降级
      Ok(None) => return Ok(false),
      Err(_) => {
        output.write_resp_error("generic error");
        return Ok(true);
      }
    };

    let byte_idx = (offset / 8) as usize;
    let bit_idx = 7 - (offset % 8) as u32;
    val.resize(byte_idx + 1, 0);
    let old_bit = (val[byte_idx] >> bit_idx) & 1;
    if bit == 1 {
      val[byte_idx] |= 1 << bit_idx;
    } else {
      val[byte_idx] &= !(1 << bit_idx);
    }

    match store.try_upsert_sync(key, &val) {
      Ok(Ok(_)) => output.write_resp_int(old_bit as i64),
      Ok(Err(_)) => return Ok(false),
      Err(_) => output.write_resp_error("generic error"),
    }
    Ok(true)
  }

  /// libs/server/Resp/Bitmap/BitmapCommands.cs:NetworkStringGetBit
  pub fn network_string_get_bit<'a, D: wdev::Device>(
    &mut self,
    parse_state: &[&[u8]],
    store: &wkv::BatchStoreSession<'a, D>,
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    if parse_state.len() != 2 {
      abort_with_wrong_number_of_arguments(output, "GETBIT");
      return Ok(true);
    }
    let key = parse_state[0];
    let Some(offset) = parse_bit_offset(parse_state[1]) else {
      abort_with_error_message(output, cs::RESP_ERR_GENERIC_BITOFFSET_IS_NOT_INTEGER);
      return Ok(true);
    };

    let bit = match store.try_read_sync(key, |v| v.to_vec()) {
      Ok(Some(Some(val))) => {
        let byte_idx = (offset / 8) as usize;
        if byte_idx < val.len() {
          (val[byte_idx] >> (7 - (offset % 8) as u32)) & 1
        } else {
          0
        }
      }
      // C# NOTFOUND → :0
      Ok(Some(None)) => 0,
      // 磁盘候选：降级
      Ok(None) => return Ok(false),
      Err(_) => {
        output.write_resp_error("generic error");
        return Ok(true);
      }
    };
    output.write_resp_int(bit as i64);
    Ok(true)
  }

  /// libs/server/Resp/Bitmap/BitmapCommands.cs:NetworkStringBitCount
  ///
  /// 形态：BITCOUNT key [start end [BYTE|BIT]]；缺省与 BYTE 均为字节区间，
  /// BIT 为位区间（对标 C# 存储侧 arg1 仅在带第 4 参时生效）
  pub fn network_string_bit_count<'a, D: wdev::Device>(
    &mut self,
    parse_state: &[&[u8]],
    store: &wkv::BatchStoreSession<'a, D>,
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    let count = parse_state.len();
    if count != 1 && count != 3 && count != 4 {
      abort_with_wrong_number_of_arguments(output, "BITCOUNT");
      return Ok(true);
    }
    let key = parse_state[0];

    // 缺省 start=0 / end=-1（全量）
    let mut start = 0i64;
    let mut end = -1i64;
    let mut use_bit_index = false;
    if count > 1 {
      let (Some(s), Some(e)) = (
        parse_state[1].try_parse_i64(),
        parse_state[2].try_parse_i64(),
      ) else {
        abort_with_error_message(output, cs::RESP_ERR_GENERIC_VALUE_IS_NOT_INTEGER);
        return Ok(true);
      };
      start = s;
      end = e;
      if count > 3 {
        let flag = parse_state[3];
        if flag.eq_ignore_ascii_case(b"BIT") {
          use_bit_index = true;
        } else if flag.eq_ignore_ascii_case(b"BYTE") {
          use_bit_index = false;
        } else {
          abort_with_error_message(output, cs::RESP_ERR_GENERIC_SYNTAX_ERROR);
          return Ok(true);
        }
      }
    }

    let val = match store.try_read_sync(key, |v| v.to_vec()) {
      Ok(Some(Some(v))) => v,
      // C# NOTFOUND → :0
      Ok(Some(None)) => {
        output.write_resp_int(0);
        return Ok(true);
      }
      // 磁盘候选：降级
      Ok(None) => return Ok(false),
      Err(_) => {
        output.write_resp_error("generic error");
        return Ok(true);
      }
    };

    let total = if use_bit_index {
      bit_index_count(&val, start, end)
    } else {
      byte_index_count(&val, start, end)
    };
    output.write_resp_int(total);
    Ok(true)
  }

  /// libs/server/Resp/Bitmap/BitmapCommands.cs:NetworkStringBitPosition
  pub fn network_string_bit_position<'a, D: wdev::Device>(
    &mut self,
    parse_state: &[&[u8]],
    store: &wkv::BatchStoreSession<'a, D>,
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    let count = parse_state.len();
    if !(2..=5).contains(&count) {
      abort_with_wrong_number_of_arguments(output, "BITPOS");
      return Ok(true);
    }
    let key = parse_state[0];

    // bit 参数须为单字符 '0'/'1'
    let bit_slice = parse_state[1];
    if bit_slice.len() != 1 || (bit_slice[0] != b'0' && bit_slice[0] != b'1') {
      abort_with_error_message(output, cs::RESP_ERR_GENERIC_BIT_IS_NOT_INTEGER);
      return Ok(true);
    }
    let search_for = bit_slice[0] - b'0';

    // 依次为 start、end、[BIT|BYTE]（缺省 start=0 / end=-1 / BYTE）
    let mut start_offset = 0i64;
    let mut end_offset = -1i64;
    let mut offset_type = 0x0u8;
    let mut has_start_offset = false;
    let mut has_end_offset = false;
    if count > 2 {
      let Some(start) = parse_state[2].try_parse_i64() else {
        abort_with_error_message(output, cs::RESP_ERR_GENERIC_VALUE_IS_NOT_INTEGER);
        return Ok(true);
      };
      start_offset = start;
      has_start_offset = true;

      if count > 3 {
        let Some(end) = parse_state[3].try_parse_i64() else {
          abort_with_error_message(output, cs::RESP_ERR_GENERIC_VALUE_IS_NOT_INTEGER);
          return Ok(true);
        };
        end_offset = end;
        has_end_offset = true;

        if count > 4 {
          let flag = parse_state[4];
          if flag.eq_ignore_ascii_case(b"BIT") {
            offset_type = 0x1;
          } else if !flag.eq_ignore_ascii_case(b"BYTE") {
            abort_with_error_message(output, cs::RESP_ERR_GENERIC_SYNTAX_ERROR);
            return Ok(true);
          }
        }
      }
    }

    // 区间越界直接 -1
    if try_validate_bit_pos_offsets(
      start_offset,
      end_offset,
      offset_type,
      has_start_offset,
      has_end_offset,
    ) {
      output.write_resp_int(-1);
      return Ok(true);
    }

    match store.try_read_sync(key, |v| v.to_vec()) {
      Ok(Some(Some(val))) => {
        let pos = bit_pos_driver(
          &val,
          val.len() as i64,
          start_offset,
          end_offset,
          search_for,
          offset_type,
        );
        output.write_resp_int(pos);
      }
      // C# NOTFOUND：找 0 回 0，找 1 回 -1
      Ok(Some(None)) => {
        let resp = if search_for == 0 {
          cs::RESP_RETURN_VAL_0
        } else {
          cs::RESP_RETURN_VAL_N1
        };
        output.extend_from_slice(resp);
      }
      // 磁盘候选：降级
      Ok(None) => return Ok(false),
      Err(_) => output.write_resp_error("generic error"),
    }
    Ok(true)
  }

  /// libs/server/Resp/Bitmap/BitmapCommands.cs:NetworkStringBitOperation
  ///
  /// C# 由分派器按 BITOP AND/OR/XOR/NOT/DIFF 传入 [`BitmapOperation`]。
  pub fn network_string_bit_operation<'a, D: wdev::Device>(
    &mut self,
    bit_op: BitmapOperation,
    parse_state: &[&[u8]],
    store: &wkv::BatchStoreSession<'a, D>,
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    let count = parse_state.len();
    // 参数过少（parse_state = [destkey, srckey...]）
    if count < 2 {
      abort_with_error_message(output, RESP_ERR_WRONG_NUMBER_OF_ARGUMENTS);
      return Ok(true);
    }
    // DIFF 至少两个源
    if bit_op == BitmapOperation::Diff && count < 3 {
      abort_with_error_message(output, RESP_ERR_BITOP_DIFF_TWO_SOURCE_KEYS_REQUIRED);
      return Ok(true);
    }
    // NOT 为一元：恰一个源键
    if bit_op == BitmapOperation::Not && count > 2 {
      abort_with_error_message(output, RESP_ERR_BITOP_NOT_SINGLE_SOURCE_KEY);
      return Ok(true);
    }
    // 源键上限（含 destkey 共 64）
    if count > 64 {
      abort_with_error_message(output, RESP_ERR_BITOP_KEY_LIMIT);
      return Ok(true);
    }

    let dest_key = parse_state[0];

    // 读源键，缺失键跳过（C# NOTFOUND continue）
    let mut srcs: Vec<Vec<u8>> = Vec::with_capacity(count - 1);
    for src_key in &parse_state[1..] {
      match store.try_read_sync(src_key, |v| v.to_vec()) {
        Ok(Some(Some(v))) => srcs.push(v),
        Ok(Some(None)) => {}
        Ok(None) => return Ok(false),
        Err(_) => {
          output.write_resp_error("generic error");
          return Ok(true);
        }
      }
    }

    let result = if srcs.is_empty() {
      // 全部缺失：即使无源也回 OK，结果 0（C# maxBitmapLen = 0）
      0i64
    } else {
      let shortest = srcs.iter().map(|s| s.len()).min().unwrap();
      let longest = srcs.iter().map(|s| s.len()).max().unwrap();
      let slices: Vec<&[u8]> = srcs.iter().map(|s| s.as_slice()).collect();
      let mut dst = vec![0u8; longest];
      match invoke_bit_operation_unsafe(bit_op, &slices, &mut dst, shortest) {
        Ok(()) => {
          if longest > 0 {
            match store.try_upsert_sync(dest_key, &dst) {
              Ok(Ok(_)) => longest as i64,
              Ok(Err(_)) => return Ok(false),
              Err(_) => {
                output.write_resp_error("generic error");
                return Ok(true);
              }
            }
          } else {
            0
          }
        }
        // C# GarnetException（源被吞并后 DIFF 单源）→ 通用错误应答
        Err(_) => {
          output.write_resp_error("generic error");
          return Ok(true);
        }
      }
    };
    output.write_resp_int(result);
    Ok(true)
  }

  /// libs/server/Resp/Bitmap/BitmapCommands.cs:StringBitField
  ///
  /// BITFIELD key [GET e o] [SET e o v] [INCRBY e o inc] [OVERFLOW WRAP|SAT|FAIL]
  pub fn string_bit_field<'a, D: wdev::Device>(
    &mut self,
    parse_state: &[&[u8]],
    store: &wkv::BatchStoreSession<'a, D>,
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    if parse_state.is_empty() {
      abort_with_wrong_number_of_arguments(output, "BITFIELD");
      return Ok(true);
    }

    // BITFIELD key [GET encoding offset] [SET encoding offset value]
    //            [INCRBY encoding offset increment] [OVERFLOW WRAP|SAT|FAIL]
    let key = parse_state[0];

    let mut overflow_type = BitFieldOverflow::Wrap;
    let mut secondary_command_args: Vec<BitFieldCmdArgs> = Vec::new();
    let mut has_write_sub_commands = false;

    let mut curr_token_idx = 1usize;
    while curr_token_idx < parse_state.len() {
      let command = parse_state[curr_token_idx];
      curr_token_idx += 1;

      // OVERFLOW：校验并应用到其后的全部子命令
      if command.eq_ignore_ascii_case(b"OVERFLOW") {
        let Some(next) = parse_state.get(curr_token_idx) else {
          abort_with_error_message(output, RESP_ERR_INVALID_OVERFLOW_TYPE);
          return Ok(true);
        };
        let Some(parsed) = parse_bitfield_overflow(next) else {
          abort_with_error_message(output, RESP_ERR_INVALID_OVERFLOW_TYPE);
          return Ok(true);
        };
        curr_token_idx += 1;
        overflow_type = parsed;
        continue;
      }

      // encoding（u<位宽> / i<位宽>）
      let Some(encoding_slice) = parse_state.get(curr_token_idx).copied() else {
        abort_with_error_message(output, RESP_ERR_INVALID_BITFIELD_TYPE);
        return Ok(true);
      };
      if parse_bitfield_encoding(encoding_slice).is_none() {
        abort_with_error_message(output, RESP_ERR_INVALID_BITFIELD_TYPE);
        return Ok(true);
      }
      curr_token_idx += 1;

      // offset（`#<n>` 倍乘或裸位偏移）
      let Some(offset_raw) = parse_state.get(curr_token_idx).copied() else {
        abort_with_error_message(output, cs::RESP_ERR_GENERIC_BITOFFSET_IS_NOT_INTEGER);
        return Ok(true);
      };
      let Some((type_info, offset)) = parse_bitfield_type_offset(encoding_slice, offset_raw) else {
        abort_with_error_message(output, cs::RESP_ERR_GENERIC_BITOFFSET_IS_NOT_INTEGER);
        return Ok(true);
      };
      curr_token_idx += 1;

      // GET 子命令取 encoding + offset
      if command.eq_ignore_ascii_case(b"GET") {
        secondary_command_args.push(BitFieldCmdArgs::new(
          BitFieldSecondaryCommand::Get,
          type_info,
          offset,
          0,
          overflow_type as u8,
        ));
        continue;
      }

      // SET / INCRBY 再取 value/increment
      let op = if command.eq_ignore_ascii_case(b"SET") {
        BitFieldSecondaryCommand::Set
      } else if command.eq_ignore_ascii_case(b"INCRBY") {
        BitFieldSecondaryCommand::IncrBy
      } else {
        let err = format!(
          "ERR Bitfield command {} not supported",
          command.as_str_safe()
        );
        abort_with_error_message(output, &err);
        return Ok(true);
      };
      has_write_sub_commands = true;

      let Some(value_slice) = parse_state.get(curr_token_idx).copied() else {
        abort_with_error_message(output, cs::RESP_ERR_GENERIC_VALUE_IS_NOT_INTEGER);
        return Ok(true);
      };
      let Some(value) = value_slice.try_parse_i64() else {
        abort_with_error_message(output, cs::RESP_ERR_GENERIC_VALUE_IS_NOT_INTEGER);
        return Ok(true);
      };
      curr_token_idx += 1;

      secondary_command_args.push(BitFieldCmdArgs::new(
        op,
        type_info,
        offset,
        value,
        overflow_type as u8,
      ));
    }

    self.string_bit_field_action(
      key,
      secondary_command_args,
      has_write_sub_commands,
      store,
      output,
    )
  }

  /// libs/server/Resp/Bitmap/BitmapCommands.cs:StringBitFieldReadOnly
  ///
  /// BITFIELD_RO key [GET encoding offset [GET encoding offset] ...]
  pub fn string_bit_field_read_only<'a, D: wdev::Device>(
    &mut self,
    parse_state: &[&[u8]],
    store: &wkv::BatchStoreSession<'a, D>,
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    if parse_state.is_empty() {
      abort_with_wrong_number_of_arguments(output, "BITFIELD_RO");
      return Ok(true);
    }

    let key = parse_state[0];
    let mut secondary_command_args: Vec<BitFieldCmdArgs> = Vec::new();

    let mut curr_token_idx = 1usize;
    while curr_token_idx < parse_state.len() {
      let command = parse_state[curr_token_idx];
      curr_token_idx += 1;

      // 只读变体仅支持 GET
      if !command.eq_ignore_ascii_case(b"GET") {
        abort_with_error_message(output, cs::RESP_ERR_GENERIC_SYNTAX_ERROR);
        return Ok(true);
      }

      // encoding
      let Some(encoding_slice) = parse_state.get(curr_token_idx).copied() else {
        abort_with_error_message(output, RESP_ERR_INVALID_BITFIELD_TYPE);
        return Ok(true);
      };
      curr_token_idx += 1;

      // offset
      let Some(offset_raw) = parse_state.get(curr_token_idx).copied() else {
        abort_with_error_message(output, cs::RESP_ERR_GENERIC_BITOFFSET_IS_NOT_INTEGER);
        return Ok(true);
      };
      let Some((type_info, offset)) = parse_bitfield_type_offset(encoding_slice, offset_raw) else {
        abort_with_error_message(output, cs::RESP_ERR_GENERIC_BITOFFSET_IS_NOT_INTEGER);
        return Ok(true);
      };
      curr_token_idx += 1;

      secondary_command_args.push(BitFieldCmdArgs::new(
        BitFieldSecondaryCommand::Get,
        type_info,
        offset,
        0,
        BitFieldOverflow::Wrap as u8,
      ));
    }

    self.string_bit_field_action(key, secondary_command_args, false, store, output)
  }

  /// libs/server/Resp/Bitmap/BitmapCommands.cs:StringBitFieldAction
  ///
  /// 执行已解析的位域子命令序列并按数组应答。C# 多子命令时经事务
  /// （写子命令排他锁）逐条 RMW；Rust 侧单会话直连，读一次、就地依次
  /// 应用、有写子命令时统一回写一次。WRONGTYPE 面在本层不可达（纯串
  /// 存储），首子命令错误短路语义经 [`Self::handle_first_sub_command`]
  /// 返回值保留。
  pub fn string_bit_field_action<'a, D: wdev::Device>(
    &mut self,
    key: &[u8],
    secondary_command_args: Vec<BitFieldCmdArgs>,
    has_write_commands: bool,
    store: &wkv::BatchStoreSession<'a, D>,
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    let _ = has_write_commands;

    // 应答数组长度
    output.write_resp_array_len(secondary_command_args.len());

    // 初始值快照（C# 首子命令经事务 API 读；缺失键记 None）
    let mut value: Option<Vec<u8>> = match store.try_read_sync(key, |v| v.to_vec()) {
      Ok(Some(Some(v))) => Some(v),
      Ok(Some(None)) => None,
      Ok(None) => return Ok(false),
      Err(_) => {
        output.write_resp_error("generic error");
        return Ok(true);
      }
    };

    let mut dirty = false;
    for (i, args) in secondary_command_args.iter().enumerate() {
      // 首子命令：数组长度已写、错误即整体短路（C# HandleFirstSubCommand）
      if i == 0 {
        if self.handle_first_sub_command(key, args, &mut value, &mut dirty, store, output)? {
          return Ok(true);
        }
        continue;
      }

      let is_get = args.secondary_command == BitFieldSecondaryCommand::Get;
      if is_get {
        match value.as_mut() {
          // NOTFOUND + GET → :0
          None => output.write_resp_int(0),
          Some(buf) => match bit_field_execute(args, buf) {
            Some((v, false)) => output.write_resp_int(v),
            // 只读 GET 不产生溢出
            _ => write_bitfield_nil(self, output),
          },
        }
      } else {
        // 写子命令：增长到位域所需长度后执行
        let need = new_block_alloc_length_from_type(args, 0) as usize;
        let buf = value.get_or_insert_with(|| Vec::with_capacity(need));
        if buf.len() < need {
          buf.resize(need, 0);
        }
        match bit_field_execute(args, buf) {
          Some((v, false)) => output.write_resp_int(v),
          Some((_, true)) => write_bitfield_nil(self, output),
          None => output.write_resp_error("generic error"),
        }
        dirty = true;
      }
    }

    if let Some(buf) = dirty.then_some(value).flatten() {
      match store.try_upsert_sync(key, &buf) {
        Ok(Ok(_)) => {}
        // 回写降级
        Ok(Err(_)) => return Ok(false),
        Err(_) => output.write_resp_error("generic error"),
      }
    }
    Ok(true)
  }

  /// libs/server/Resp/Bitmap/BitmapCommands.cs:HandleFirstSubCommand
  ///
  /// 首子命令特判：C# 须处理"数组长度已写但 WRONGTYPE 需回卷输出"；
  /// 返回 true 表示整体错误短路（本层 WRONGTYPE 不可达，恒 false，
  /// 保留签名以对齐调用形态）。
  pub fn handle_first_sub_command<'a, D: wdev::Device>(
    &mut self,
    _key: &[u8],
    args: &BitFieldCmdArgs,
    value: &mut Option<Vec<u8>>,
    dirty: &mut bool,
    _store: &wkv::BatchStoreSession<'a, D>,
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    let is_get = args.secondary_command == BitFieldSecondaryCommand::Get;
    if is_get {
      match value.as_mut() {
        // NOTFOUND + GET → :0
        None => output.write_resp_int(0),
        Some(buf) => match bit_field_execute(args, buf) {
          Some((v, false)) => output.write_resp_int(v),
          _ => write_bitfield_nil(self, output),
        },
      }
    } else {
      let need = new_block_alloc_length_from_type(args, 0) as usize;
      let buf = value.get_or_insert_with(|| Vec::with_capacity(need));
      if buf.len() < need {
        buf.resize(need, 0);
      }
      match bit_field_execute(args, buf) {
        Some((v, false)) => output.write_resp_int(v),
        Some((_, true)) => write_bitfield_nil(self, output),
        None => output.write_resp_error("generic error"),
      }
      *dirty = true;
    }
    Ok(false)
  }
}

/// 字节区间计数（负偏移按值长折算，end 截到值长，区间交集为空计 0）
fn byte_index_count(val: &[u8], start: i64, end: i64) -> i64 {
  let len = val.len() as i64;
  let s = if start < 0 { len + start } else { start };
  let e = if end < 0 { len + end } else { end };
  let e = e.min(len - 1);
  if s < 0 || e < 0 || s >= len || s > e {
    return 0;
  }
  val[s as usize..=e as usize]
    .iter()
    .map(|b| b.count_ones() as i64)
    .sum()
}

/// 位区间计数（负偏移按总位长折算，边界字节做首尾掩码）
fn bit_index_count(val: &[u8], start: i64, end: i64) -> i64 {
  let bit_len = val.len() as i64 * 8;
  if bit_len == 0 {
    return 0;
  }
  let s = if start < 0 { bit_len + start } else { start };
  let e = if end < 0 { bit_len + end } else { end };
  if s < 0 || e < 0 || s >= bit_len || s > e {
    return 0;
  }
  let e = e.min(bit_len - 1);
  let (fb, lb) = ((s / 8) as usize, (e / 8) as usize);
  // 首字节保留 s%8 起的位；末字节保留到 e%8（MSB 位序）
  let mask_first = 0xffu8 >> (s % 8);
  let mask_last = 0xffu8 << (7 - e % 8);
  if fb == lb {
    return (val[fb] & mask_first & mask_last).count_ones() as i64;
  }
  let mut total =
    (val[fb] & mask_first).count_ones() as i64 + (val[lb] & mask_last).count_ones() as i64;
  for b in &val[fb + 1..lb] {
    total += b.count_ones() as i64;
  }
  total
}

#[cfg(test)]
mod tests {
  use super::{super::super::batch_harness::with_batch, bit_index_count, byte_index_count};
  use crate::resp::bitmap::bitmap_manager_bit_op::BitmapOperation;

  #[test]
  fn byte_index_count_ranges() {
    // 0b1011_0001（4 个 1）/ 0b0100_1111（5 个 1）
    let val = [0b1011_0001u8, 0b0100_1111];
    assert_eq!(byte_index_count(&val, 0, -1), 4 + 5);
    assert_eq!(byte_index_count(&val, 0, 0), 4);
    assert_eq!(byte_index_count(&val, -1, -1), 5);
    assert_eq!(byte_index_count(&val, 1, 1), 5);
    // 区间为空 / 越界
    assert_eq!(byte_index_count(&val, 2, 3), 0);
    assert_eq!(byte_index_count(&val, -3, -3), 0);
    assert_eq!(byte_index_count(&val, 5, 9), 0);
    assert_eq!(byte_index_count(&[], 0, -1), 0);
    // end 越界截断（对标 C# clamp 到 len-1）
    assert_eq!(byte_index_count(&val, 0, 100), 4 + 5);
  }

  #[test]
  fn bit_index_count_ranges() {
    let val = [0b1011_0001u8];
    assert_eq!(bit_index_count(&val, 0, 7), 4);
    // 首位（bit0=MSB=1）
    assert_eq!(bit_index_count(&val, 0, 0), 1);
    // 末两位（bit6..bit7 = 0,1）
    assert_eq!(bit_index_count(&val, 6, 7), 1);
    assert_eq!(bit_index_count(&val, 1, 4), 2);
    // 跨字节边界：置位的是 bit0 与 bit15
    let v2 = [0b1000_0000, 0b0000_0001];
    assert_eq!(bit_index_count(&v2, 0, 15), 2);
    assert_eq!(bit_index_count(&v2, 7, 8), 0);
    assert_eq!(bit_index_count(&v2, 1, 14), 0);
    // 负偏移与越界
    assert_eq!(bit_index_count(&val, -8, -1), 4);
    assert_eq!(bit_index_count(&val, 8, 15), 0);
    assert_eq!(bit_index_count(&[], 0, -1), 0);
    // end 越界截断（对标 C# clamp 到 len-1）
    assert_eq!(bit_index_count(&val, 0, 100), 4);
  }

  #[test]
  fn setbit_getbit_roundtrip_and_growth() {
    with_batch(|s, batch| {
      // 键不存在自动增长
      let mut out = Vec::new();
      let _ = s
        .network_string_set_bit(&[b"bm", b"0", b"1"], batch, &mut out)
        .unwrap();
      assert_eq!(out, b":0\r\n");
      // 重复置位回旧值 1
      let mut out = Vec::new();
      let _ = s
        .network_string_set_bit(&[b"bm", b"0", b"1"], batch, &mut out)
        .unwrap();
      assert_eq!(out, b":1\r\n");
      // 位 9 跨字节增长
      let mut out = Vec::new();
      let _ = s
        .network_string_set_bit(&[b"bm", b"9", b"1"], batch, &mut out)
        .unwrap();
      assert_eq!(out, b":0\r\n");

      let mut out = Vec::new();
      let _ = s
        .network_string_get_bit(&[b"bm", b"0"], batch, &mut out)
        .unwrap();
      assert_eq!(out, b":1\r\n");
      let mut out = Vec::new();
      let _ = s
        .network_string_get_bit(&[b"bm", b"9"], batch, &mut out)
        .unwrap();
      assert_eq!(out, b":1\r\n");
      // 键缺失 / 越界位 → 0
      let mut out = Vec::new();
      let _ = s
        .network_string_get_bit(&[b"nk", b"3"], batch, &mut out)
        .unwrap();
      assert_eq!(out, b":0\r\n");
      let mut out = Vec::new();
      let _ = s
        .network_string_get_bit(&[b"bm", b"100"], batch, &mut out)
        .unwrap();
      assert_eq!(out, b":0\r\n");
    });
  }

  #[test]
  fn setbit_getbit_strict_validation() {
    with_batch(|s, batch| {
      // bit 非单字符 0/1
      let mut out = Vec::new();
      let _ = s
        .network_string_set_bit(&[b"bm", b"0", b"01"], batch, &mut out)
        .unwrap();
      assert_eq!(out, b"-ERR bit is not an integer or out of range\r\n");
      let mut out = Vec::new();
      let _ = s
        .network_string_set_bit(&[b"bm", b"0", b"2"], batch, &mut out)
        .unwrap();
      assert_eq!(out, b"-ERR bit is not an integer or out of range\r\n");
      // 负 / 非整数 / 越界 offset
      for bad in [b"-1".as_slice(), b"x".as_slice(), b"4294967296".as_slice()] {
        let mut out = Vec::new();
        let _ = s
          .network_string_set_bit(&[b"bm", bad, b"1"], batch, &mut out)
          .unwrap();
        assert_eq!(
          out,
          b"-ERR bit offset is not an integer or out of range\r\n"
        );
      }
      // 恰为上界值（512MB*8-1）的参数校验通过（不实际建 512MB 值，仅校验报错口径）
    });
  }

  #[test]
  fn bitcount_forms_byte_and_bit() {
    with_batch(|s, batch| {
      // 0xA5 = 1010_0101（4 个 1），0x0F（4 个 1）
      let _ = s
        .network_set(&[b"bm", &[0xA5, 0x0F]], batch, &mut Vec::new())
        .unwrap();

      let mut out = Vec::new();
      let _ = s
        .network_string_bit_count(&[b"bm"], batch, &mut out)
        .unwrap();
      assert_eq!(out, b":8\r\n");

      // 字节区间（缺省 BYTE 口径）
      let mut out = Vec::new();
      let _ = s
        .network_string_bit_count(&[b"bm", b"0", b"0"], batch, &mut out)
        .unwrap();
      assert_eq!(out, b":4\r\n");
      let mut out = Vec::new();
      let _ = s
        .network_string_bit_count(&[b"bm", b"-1", b"-1"], batch, &mut out)
        .unwrap();
      assert_eq!(out, b":4\r\n");

      // BIT 位区间：位序 MSB 在前，[0,7]=4；[1,2]=bit1,bit2=0,1；[3,4]=0,0
      let mut out = Vec::new();
      let _ = s
        .network_string_bit_count(&[b"bm", b"0", b"7", b"BIT"], batch, &mut out)
        .unwrap();
      assert_eq!(out, b":4\r\n");
      let mut out = Vec::new();
      let _ = s
        .network_string_bit_count(&[b"bm", b"1", b"2", b"BIT"], batch, &mut out)
        .unwrap();
      assert_eq!(out, b":1\r\n");
      let mut out = Vec::new();
      let _ = s
        .network_string_bit_count(&[b"bm", b"3", b"4", b"BIT"], batch, &mut out)
        .unwrap();
      assert_eq!(out, b":0\r\n");

      // 键缺失 → :0；arity 2 → 错误；非整数 → 错误；未知第 4 参 → 语法错
      let mut out = Vec::new();
      let _ = s
        .network_string_bit_count(&[b"nk"], batch, &mut out)
        .unwrap();
      assert_eq!(out, b":0\r\n");
      let mut out = Vec::new();
      let _ = s
        .network_string_bit_count(&[b"bm", b"1"], batch, &mut out)
        .unwrap();
      assert_eq!(
        out,
        b"-ERR wrong number of arguments for 'BITCOUNT' command\r\n"
      );
      let mut out = Vec::new();
      let _ = s
        .network_string_bit_count(&[b"bm", b"x", b"1"], batch, &mut out)
        .unwrap();
      assert_eq!(out, b"-ERR value is not an integer or out of range.\r\n");
      let mut out = Vec::new();
      let _ = s
        .network_string_bit_count(&[b"bm", b"0", b"1", b"BAD"], batch, &mut out)
        .unwrap();
      assert_eq!(out, b"-ERR syntax error\r\n");
    });
  }

  // —— BITPOS ——

  #[test]
  fn bitpos_forms_and_errors() {
    with_batch(|s, batch| {
      // 0x00 0x08：首个 1 在位 12
      let _ = s
        .network_set(&[b"bp", &[0x00, 0x08]], batch, &mut Vec::new())
        .unwrap();

      let mut out = Vec::new();
      let _ = s
        .network_string_bit_position(&[b"bp", b"1"], batch, &mut out)
        .unwrap();
      assert_eq!(out, b":12\r\n");

      let mut out = Vec::new();
      let _ = s
        .network_string_bit_position(&[b"bp", b"0"], batch, &mut out)
        .unwrap();
      assert_eq!(out, b":0\r\n");

      // BYTE 口径：仅查字节 0 → 无 1
      let mut out = Vec::new();
      let _ = s
        .network_string_bit_position(&[b"bp", b"1", b"0", b"0"], batch, &mut out)
        .unwrap();
      assert_eq!(out, b":-1\r\n");

      // BIT 口径：区间 [0,11] 不含位 12
      let mut out = Vec::new();
      let _ = s
        .network_string_bit_position(&[b"bp", b"1", b"0", b"11", b"BIT"], batch, &mut out)
        .unwrap();
      assert_eq!(out, b":-1\r\n");

      // 键缺失：找 0 → 0；找 1 → -1
      let mut out = Vec::new();
      let _ = s
        .network_string_bit_position(&[b"nk", b"0"], batch, &mut out)
        .unwrap();
      assert_eq!(out, b":0\r\n");
      let mut out = Vec::new();
      let _ = s
        .network_string_bit_position(&[b"nk", b"1"], batch, &mut out)
        .unwrap();
      assert_eq!(out, b":-1\r\n");

      // 区间越界 → -1
      let mut out = Vec::new();
      let _ = s
        .network_string_bit_position(&[b"bp", b"1", b"999999999999", b"-1"], batch, &mut out)
        .unwrap();
      assert_eq!(out, b":-1\r\n");

      // arity / bit / 非整数 / 未知第 4 参
      let mut out = Vec::new();
      let _ = s
        .network_string_bit_position(&[b"bp"], batch, &mut out)
        .unwrap();
      assert_eq!(
        out,
        b"-ERR wrong number of arguments for 'BITPOS' command\r\n"
      );
      let mut out = Vec::new();
      let _ = s
        .network_string_bit_position(&[b"bp", b"2"], batch, &mut out)
        .unwrap();
      assert_eq!(out, b"-ERR bit is not an integer or out of range\r\n");
      let mut out = Vec::new();
      let _ = s
        .network_string_bit_position(&[b"bp", b"1", b"x"], batch, &mut out)
        .unwrap();
      assert_eq!(out, b"-ERR value is not an integer or out of range.\r\n");
      let mut out = Vec::new();
      let _ = s
        .network_string_bit_position(&[b"bp", b"1", b"0", b"0", b"BAD"], batch, &mut out)
        .unwrap();
      assert_eq!(out, b"-ERR syntax error\r\n");
    });
  }

  // —— BITOP ——

  #[test]
  fn bitop_operations_and_guards() {
    with_batch(|s, batch| {
      let _ = s
        .network_set(&[b"a", &[0b1100_0011u8, 0x0F]], batch, &mut Vec::new())
        .unwrap();
      let _ = s
        .network_set(&[b"b", &[0b1010_1010u8]], batch, &mut Vec::new())
        .unwrap();

      // AND：与短源缺失位按 0
      let mut out = Vec::new();
      let _ = s
        .network_string_bit_operation(BitmapOperation::And, &[b"d", b"a", b"b"], batch, &mut out)
        .unwrap();
      assert_eq!(out, b":2\r\n");
      // AND 结果 [1000_0010, 0x00]
      let mut out = Vec::new();
      let _ = s.network_get(&[b"d"], batch, &mut out).unwrap();
      assert_eq!(out, b"$2\r\n\x82\x00\r\n");

      // OR
      let mut out = Vec::new();
      let _ = s
        .network_string_bit_operation(BitmapOperation::Or, &[b"d", b"a", b"b"], batch, &mut out)
        .unwrap();
      assert_eq!(out, b":2\r\n");

      // NOT：单源取反，长度不变
      let mut out = Vec::new();
      let _ = s
        .network_string_bit_operation(BitmapOperation::Not, &[b"d", b"b"], batch, &mut out)
        .unwrap();
      assert_eq!(out, b":1\r\n");

      // NOT 多源 → 错误
      let mut out = Vec::new();
      let _ = s
        .network_string_bit_operation(BitmapOperation::Not, &[b"d", b"a", b"b"], batch, &mut out)
        .unwrap();
      assert_eq!(
        out,
        b"-ERR BITOP NOT must be called with a single source key.\r\n"
      );

      // DIFF 少于两个源 → 错误
      let mut out = Vec::new();
      let _ = s
        .network_string_bit_operation(BitmapOperation::Diff, &[b"d", b"a"], batch, &mut out)
        .unwrap();
      assert_eq!(
        out,
        b"-ERR BITOP DIFF must be called with at least two source keys.\r\n"
      );

      // 参数过少
      let mut out = Vec::new();
      let _ = s
        .network_string_bit_operation(BitmapOperation::And, &[b"d"], batch, &mut out)
        .unwrap();
      assert_eq!(out, b"-ERR wrong number of arguments\r\n");

      // 全源缺失 → :0 且不建目标键
      let mut out = Vec::new();
      let _ = s
        .network_string_bit_operation(
          BitmapOperation::And,
          &[b"miss", b"n1", b"n2"],
          batch,
          &mut out,
        )
        .unwrap();
      assert_eq!(out, b":0\r\n");
      let mut out = Vec::new();
      let _ = s.network_get(&[b"miss"], batch, &mut out).unwrap();
      assert_eq!(out, b"$-1\r\n"); // 键不存在（GET 回 nil）
    });
  }

  // —— BITFIELD ——

  #[test]
  fn bitfield_full_option_matrix() {
    with_batch(|s, batch| {
      // SET u4 @1 = 15：键不存在自动增长，回旧值 0（位 1..5 = 1111 → 0x78）
      let mut out = Vec::new();
      let _ = s
        .string_bit_field(&[b"bf", b"SET", b"u4", b"1", b"15"], batch, &mut out)
        .unwrap();
      assert_eq!(out, b"*1\r\n:0\r\n");
      // 值字节：位 1..5 = 1111 → 0b0111_1000 = 0x78（'x'）
      let mut out = Vec::new();
      let _ = s.network_get(&[b"bf"], batch, &mut out).unwrap();
      assert_eq!(&out[4..5], b"x");

      // GET u4 / GET i4（0xF 符号扩展为 -1）
      let mut out = Vec::new();
      let _ = s
        .string_bit_field(&[b"bf", b"GET", b"u4", b"1"], batch, &mut out)
        .unwrap();
      assert_eq!(out, b"*1\r\n:15\r\n");
      let mut out = Vec::new();
      let _ = s
        .string_bit_field(&[b"bf", b"GET", b"i4", b"1"], batch, &mut out)
        .unwrap();
      assert_eq!(out, b"*1\r\n:-1\r\n");

      // WRAP（缺省策略）：15 + 1 → 0；0 - 1 → 15
      let mut out = Vec::new();
      let _ = s
        .string_bit_field(
          &[b"bf", b"INCRBY", b"u4", b"1", b"1", b"OVERFLOW", b"WRAP"],
          batch,
          &mut out,
        )
        .unwrap();
      assert_eq!(out, b"*1\r\n:0\r\n");
      let mut out = Vec::new();
      let _ = s
        .string_bit_field(&[b"bf", b"INCRBY", b"u4", b"1", b"-1"], batch, &mut out)
        .unwrap();
      assert_eq!(out, b"*1\r\n:15\r\n");

      // SAT：u8 @8 增长到 2 字节后 250 + 10 → 饱和 255
      let mut out = Vec::new();
      let _ = s
        .string_bit_field(
          &[b"bf", b"OVERFLOW", b"SAT", b"INCRBY", b"u8", b"8", b"250"],
          batch,
          &mut out,
        )
        .unwrap();
      assert_eq!(out, b"*1\r\n:250\r\n");
      let mut out = Vec::new();
      let _ = s
        .string_bit_field(
          &[b"bf", b"OVERFLOW", b"SAT", b"INCRBY", b"u8", b"8", b"10"],
          batch,
          &mut out,
        )
        .unwrap();
      assert_eq!(out, b"*1\r\n:255\r\n");

      // FAIL：溢出回 nil 且子命令照旧落盘（C# 语义）
      let mut out = Vec::new();
      let _ = s
        .string_bit_field(
          &[b"bf", b"OVERFLOW", b"FAIL", b"INCRBY", b"u8", b"8", b"10"],
          batch,
          &mut out,
        )
        .unwrap();
      assert_eq!(out, b"*1\r\n$-1\r\n");

      // `#` 倍乘 offset：SET u2 #4 3 → 位 8..10 = 11
      let mut out = Vec::new();
      let _ = s
        .string_bit_field(&[b"bf", b"SET", b"u2", b"#4", b"3"], batch, &mut out)
        .unwrap();
      assert_eq!(out, b"*1\r\n:0\r\n");
      let mut out = Vec::new();
      let _ = s
        .string_bit_field(&[b"bf", b"GET", b"u2", b"8"], batch, &mut out)
        .unwrap();
      assert_eq!(out, b"*1\r\n:3\r\n");

      // 多子命令数组：GET → INCRBY（WRAP）→ OVERFLOW SAT 后 INCRBY
      let mut out = Vec::new();
      let _ = s
        .string_bit_field(
          &[
            b"bf",
            b"GET",
            b"u4",
            b"1",
            b"INCRBY",
            b"u4",
            b"1",
            b"1",
            b"OVERFLOW",
            b"SAT",
            b"INCRBY",
            b"u4",
            b"1",
            b"5",
          ],
          batch,
          &mut out,
        )
        .unwrap();
      assert_eq!(out, b"*3\r\n:15\r\n:0\r\n:5\r\n");

      // i64 全宽
      let mut out = Vec::new();
      let _ = s
        .string_bit_field(
          &[b"i64k", b"SET", b"i64", b"0", b"-9223372036854775808"],
          batch,
          &mut out,
        )
        .unwrap();
      assert_eq!(out, b"*1\r\n:0\r\n");
      let mut out = Vec::new();
      let _ = s
        .string_bit_field(&[b"i64k", b"GET", b"i64", b"0"], batch, &mut out)
        .unwrap();
      assert_eq!(out, b"*1\r\n:-9223372036854775808\r\n");

      // 缺键 GET → :0
      let mut out = Vec::new();
      let _ = s
        .string_bit_field(&[b"nokey", b"GET", b"u4", b"0"], batch, &mut out)
        .unwrap();
      assert_eq!(out, b"*1\r\n:0\r\n");

      // 错误矩阵
      let mut out = Vec::new();
      let _ = s
        .string_bit_field(&[b"bf", b"SET", b"x4", b"0", b"1"], batch, &mut out)
        .unwrap();
      assert_eq!(out, b"-ERR Invalid bitfield type. Use something like i16 u8. Note that u64 is not supported but i64 is\r\n");
      let mut out = Vec::new();
      let _ = s
        .string_bit_field(&[b"bf", b"SET", b"u64", b"0", b"1"], batch, &mut out)
        .unwrap();
      assert_eq!(out, b"-ERR Invalid bitfield type. Use something like i16 u8. Note that u64 is not supported but i64 is\r\n");
      let mut out = Vec::new();
      let _ = s
        .string_bit_field(&[b"bf", b"FOO", b"u4", b"0"], batch, &mut out)
        .unwrap();
      assert_eq!(out, b"-ERR Bitfield command FOO not supported\r\n");
      let mut out = Vec::new();
      let _ = s
        .string_bit_field(&[b"bf", b"GET", b"u4"], batch, &mut out)
        .unwrap();
      assert_eq!(
        out,
        b"-ERR bit offset is not an integer or out of range\r\n"
      );
      let mut out = Vec::new();
      let _ = s
        .string_bit_field(&[b"bf", b"SET", b"u4", b"0", b"x"], batch, &mut out)
        .unwrap();
      assert_eq!(out, b"-ERR value is not an integer or out of range.\r\n");
      let mut out = Vec::new();
      let _ = s
        .string_bit_field(&[b"bf", b"OVERFLOW", b"BAD"], batch, &mut out)
        .unwrap();
      assert_eq!(out, b"-ERR Invalid OVERFLOW type specified\r\n");
      let mut out = Vec::new();
      let _ = s
        .string_bit_field(&[b"bf", b"OVERFLOW"], batch, &mut out)
        .unwrap();
      assert_eq!(out, b"-ERR Invalid OVERFLOW type specified\r\n");
      let mut out = Vec::new();
      let _ = s
        .string_bit_field(&[b"bf", b"GET", b"u4", b"-8"], batch, &mut out)
        .unwrap();
      assert_eq!(
        out,
        b"-ERR bit offset is not an integer or out of range\r\n"
      );
    });
  }

  #[test]
  fn bitfield_read_only() {
    with_batch(|s, batch| {
      let _ = s
        .string_bit_field(&[b"bf", b"SET", b"u4", b"1", b"15"], batch, &mut Vec::new())
        .unwrap();

      // 仅 GET
      let mut out = Vec::new();
      let _ = s
        .string_bit_field_read_only(&[b"bf", b"GET", b"u4", b"1"], batch, &mut out)
        .unwrap();
      assert_eq!(out, b"*1\r\n:15\r\n");

      // 多 GET
      let mut out = Vec::new();
      let _ = s
        .string_bit_field_read_only(
          &[b"bf", b"GET", b"u4", b"1", b"GET", b"u4", b"0"],
          batch,
          &mut out,
        )
        .unwrap();
      // u4 @0 = 0b0111 = 7
      assert_eq!(out, b"*2\r\n:15\r\n:7\r\n");

      // 写子命令 → 语法错误
      let mut out = Vec::new();
      let _ = s
        .string_bit_field_read_only(&[b"bf", b"SET", b"u4", b"1", b"1"], batch, &mut out)
        .unwrap();
      assert_eq!(out, b"-ERR syntax error\r\n");

      // OVERFLOW 不被只读变体接受 → 语法错误
      let mut out = Vec::new();
      let _ = s
        .string_bit_field_read_only(
          &[b"bf", b"OVERFLOW", b"SAT", b"GET", b"u4", b"1"],
          batch,
          &mut out,
        )
        .unwrap();
      assert_eq!(out, b"-ERR syntax error\r\n");
    });
  }
}
