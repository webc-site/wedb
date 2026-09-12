//! 位图命令（SETBIT/GETBIT/BITCOUNT/BITPOS/BITOP/BITFIELD/BITFIELD_RO）
//!
//! 同步快路径：字符串域直读直写，磁盘候选等须异步裁决时返回 `Ok(false)`
//! 交调用方降级。位序对标 Redis/C#：bit 0 为首字节最高位。
//! C# 侧会话层经 `StringInput` 把参数传给存储回调；Rust 侧存储接口收敛到
//! 会话层直连，故 BITFIELD 子命令在解析期即固化为类型化参数。

use wresp::{
  RespSliceExt, RespVecExt, cmd_strings as cs,
  cmd_strings::{abort_with_error_message, abort_with_wrong_number_of_arguments},
};

use super::{
  super::{basic_commands::MAX_STRING_PAYLOAD_BYTES, resp_server_session::RespServerSession},
  bitmap_manager::{try_validate_bit_pos_offsets, try_validate_bitfield_offset},
  bitmap_manager_bit_count::bit_count_driver,
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
    if val.len() <= byte_idx {
      val.resize(byte_idx + 1, 0);
    }
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

    let offset_type = if use_bit_index { 0x1 } else { 0x0 };
    let total = bit_count_driver(start, end, offset_type, &val, val.len() as i64);
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

    // 对标 C#：OVERFLOW 类型统一后置应用——最终解析出的策略附加到全部子命令
    // （含 OVERFLOW 之前入列者，BitmapCommands.cs:HandleFirstSubCommand 与
    // StringBitFieldAction 循环均以 SetArgument 追加 overflowTypeSlice）
    let mut is_overflow_type_set = false;
    let mut overflow_type = BitFieldOverflow::Wrap;
    let mut secondary_command_args: Vec<BitFieldCmdArgs> = Vec::new();
    let mut has_write_sub_commands = false;

    let mut curr_token_idx = 1usize;
    while curr_token_idx < parse_state.len() {
      let command = parse_state[curr_token_idx];
      curr_token_idx += 1;

      // OVERFLOW：校验并记录（覆盖既有策略，末值全局生效）
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
        is_overflow_type_set = true;
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
          BitFieldOverflow::Wrap as u8,
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
        BitFieldOverflow::Wrap as u8,
      ));
    }

    // OVERFLOW 末值后置全局生效（含 OVERFLOW 之前入列的子命令）
    if is_overflow_type_set {
      for args in &mut secondary_command_args {
        args.overflow_type = overflow_type as u8;
      }
    }

    if secondary_command_args.is_empty() {
      output.write_resp_array_len(0);
      return Ok(true);
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

    if secondary_command_args.is_empty() {
      output.write_resp_array_len(0);
      return Ok(true);
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
  /// 执行 BITFIELD 复合子命令；保留 _has_write_commands 以匹配 Garnet BitmapCommands.StringBitFieldAction 签名规范
  pub fn string_bit_field_action<'a, D: wdev::Device>(
    &mut self,
    key: &[u8],
    secondary_command_args: Vec<BitFieldCmdArgs>,
    _has_write_commands: bool,
    store: &wkv::BatchStoreSession<'a, D>,
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    if secondary_command_args.is_empty() {
      output.write_resp_array_len(0);
      return Ok(true);
    }

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
        if self.handle_first_sub_command(args, &mut value, &mut dirty, output)? {
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
  pub fn handle_first_sub_command(
    &mut self,
    args: &BitFieldCmdArgs,
    value: &mut Option<Vec<u8>>,
    dirty: &mut bool,
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
