use core::str;
//! 位图命令（SETBIT/GETBIT/BITCOUNT）
//!
//! 同步快路径：字符串域直读直写，磁盘候选等须异步裁决时返回 `Ok(false)`
//! 交调用方降级。位序对标 Redis/C#：bit 0 为首字节最高位。

use super::{
  super::{
    basic_commands::MAX_STRING_PAYLOAD_BYTES,
    cmd_strings as cs,
    cmd_strings::{abort_with_error_message, abort_with_wrong_number_of_arguments},
    parser::resp_ext::{RespSliceExt, RespVecExt},
    resp_server_session::RespServerSession,
  },
  bitmap_manager::BitmapManager,
  bitmap_manager_bit_op::{BitmapManagerBitOp, BitmapOperation},
  bitmap_manager_bit_pos::BitmapManagerBitPos,
  bitmap_manager_bitfield::{BitFieldType, BitmapManagerBitfield, OverflowType},
};

fn parse_bitfield_offset(raw: &[u8], bit_count: u8) -> Option<i64> {
  if raw.is_empty() {
    return None;
  }
  if raw[0] == b'#' {
    let index: i64 = str::from_utf8(&raw[1..]).ok()?.parse().ok()?;
    index.checked_mul(bit_count as i64)
  } else {
    raw.try_parse_i64()
  }
}

/// 合法位偏移上限（BitmapManager.MaxOffsetForBitmapLength）
const MAX_BIT_OFFSET: i64 = (MAX_STRING_PAYLOAD_BYTES as i64 * 8) - 1;

/// SETBIT/GETBIT 的 offset 参数校验（对标 C# IsValidBitOffset 口径）
fn parse_bit_offset(raw: &[u8]) -> Option<i64> {
  let offset = raw.try_parse_i64()?;
  (0..=MAX_BIT_OFFSET).contains(&offset).then_some(offset)
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
    let bit_bytes = parse_state[1];
    if bit_bytes.len() != 1 || (bit_bytes[0] != b'0' && bit_bytes[0] != b'1') {
      abort_with_error_message(output, cs::RESP_ERR_GENERIC_BIT_IS_NOT_INTEGER);
      return Ok(true);
    }
    let search_for = bit_bytes[0] - b'0';

    let mut start_offset = 0i64;
    let mut end_offset = -1i64;
    let mut offset_type = 0u8;
    let mut has_start = false;
    let mut has_end = false;

    if count > 2 {
      let Some(s) = parse_state[2].try_parse_i64() else {
        abort_with_error_message(output, cs::RESP_ERR_GENERIC_VALUE_IS_NOT_INTEGER);
        return Ok(true);
      };
      start_offset = s;
      has_start = true;

      if count > 3 {
        let Some(e) = parse_state[3].try_parse_i64() else {
          abort_with_error_message(output, cs::RESP_ERR_GENERIC_VALUE_IS_NOT_INTEGER);
          return Ok(true);
        };
        end_offset = e;
        has_end = true;

        if count > 4 {
          let t = parse_state[4];
          if t.eq_ignore_ascii_case(b"BIT") {
            offset_type = 0x1;
          } else if t.eq_ignore_ascii_case(b"BYTE") {
            offset_type = 0x0;
          } else {
            abort_with_error_message(output, cs::RESP_ERR_GENERIC_SYNTAX_ERROR);
            return Ok(true);
          }
        }
      }
    }

    if BitmapManager::try_validate_bit_pos_offsets(
      start_offset,
      end_offset,
      offset_type,
      has_start,
      has_end,
    ) {
      output.write_resp_int(-1);
      return Ok(true);
    }

    let val = match store.try_read_sync(key, |v| v.to_vec()) {
      Ok(Some(Some(v))) => v,
      Ok(Some(None)) => {
        let resp = if search_for == 0 { 0 } else { -1 };
        output.write_resp_int(resp);
        return Ok(true);
      }
      Ok(None) => return Ok(false),
      Err(_) => {
        output.write_resp_error("generic error");
        return Ok(true);
      }
    };

    let mut pos =
      BitmapManagerBitPos::bit_pos_driver(&val, start_offset, end_offset, search_for, offset_type);
    if pos == -1 && search_for == 0 && !has_end {
      pos = val.len() as i64 * 8;
    }

    output.write_resp_int(pos);
    Ok(true)
  }

  /// libs/server/Resp/Bitmap/BitmapCommands.cs:NetworkStringBitOperation
  pub fn network_string_bit_operation<'a, D: wdev::Device>(
    &mut self,
    parse_state: &[&[u8]],
    store: &wkv::BatchStoreSession<'a, D>,
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    if parse_state.len() < 3 {
      abort_with_wrong_number_of_arguments(output, "BITOP");
      return Ok(true);
    }

    let op = if parse_state[0].eq_ignore_ascii_case(b"AND") {
      BitmapOperation::And
    } else if parse_state[0].eq_ignore_ascii_case(b"OR") {
      BitmapOperation::Or
    } else if parse_state[0].eq_ignore_ascii_case(b"XOR") {
      BitmapOperation::Xor
    } else if parse_state[0].eq_ignore_ascii_case(b"NOT") {
      BitmapOperation::Not
    } else {
      abort_with_error_message(output, cs::RESP_ERR_GENERIC_SYNTAX_ERROR);
      return Ok(true);
    };

    let dest_key = parse_state[1];
    let src_keys = &parse_state[2..];

    if op == BitmapOperation::Not && src_keys.len() != 1 {
      abort_with_error_message(output, "BITOP NOT takes only one source key");
      return Ok(true);
    }

    let mut sources_data = Vec::with_capacity(src_keys.len());
    for &src_key in src_keys {
      match store.try_read_sync(src_key, |v| v.to_vec()) {
        Ok(Some(Some(v))) => sources_data.push(v),
        Ok(Some(None)) => sources_data.push(Vec::new()),
        Ok(None) => return Ok(false),
        Err(_) => {
          output.write_resp_error("generic error");
          return Ok(true);
        }
      }
    }

    let src_refs: Vec<&[u8]> = sources_data.iter().map(|v| v.as_slice()).collect();
    let result = match BitmapManagerBitOp::invoke_bit_operation(op, &src_refs) {
      Ok(res) => res,
      Err(msg) => {
        output.write_resp_error(msg);
        return Ok(true);
      }
    };

    match store.try_upsert_sync(dest_key, &result) {
      Ok(Ok(_)) => {
        output.write_resp_int(result.len() as i64);
        Ok(true)
      }
      Ok(Err(_)) => Ok(false),
      Err(_) => {
        output.write_resp_error("generic error");
        Ok(true)
      }
    }
  }

  /// libs/server/Resp/Bitmap/BitmapCommands.cs:StringBitField
  pub fn string_bit_field<'a, D: wdev::Device>(
    &mut self,
    parse_state: &[&[u8]],
    store: &wkv::BatchStoreSession<'a, D>,
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    self.string_bit_field_action(parse_state, store, output, false)
  }

  /// libs/server/Resp/Bitmap/BitmapCommands.cs:StringBitFieldReadOnly
  pub fn string_bit_field_read_only<'a, D: wdev::Device>(
    &mut self,
    parse_state: &[&[u8]],
    store: &wkv::BatchStoreSession<'a, D>,
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    self.string_bit_field_action(parse_state, store, output, true)
  }

  /// libs/server/Resp/Bitmap/BitmapCommands.cs:StringBitFieldAction
  pub fn string_bit_field_action<'a, D: wdev::Device>(
    &mut self,
    parse_state: &[&[u8]],
    store: &wkv::BatchStoreSession<'a, D>,
    output: &mut Vec<u8>,
    read_only: bool,
  ) -> wresp::Result<bool> {
    if parse_state.len() < 2 {
      abort_with_wrong_number_of_arguments(
        output,
        if read_only { "BITFIELD_RO" } else { "BITFIELD" },
      );
      return Ok(true);
    }
    let key = parse_state[0];
    let mut modified = false;

    let mut val = match store.try_read_sync(key, |v| v.to_vec()) {
      Ok(Some(Some(v))) => v,
      Ok(Some(None)) => Vec::new(),
      Ok(None) => return Ok(false),
      Err(_) => {
        output.write_resp_error("generic error");
        return Ok(true);
      }
    };

    let mut overflow = OverflowType::Wrap;
    let mut results: Vec<Option<i64>> = Vec::new();
    let mut idx = 1;

    while idx < parse_state.len() {
      let subcmd = parse_state[idx];
      idx += 1;

      if subcmd.eq_ignore_ascii_case(b"OVERFLOW") {
        if read_only {
          abort_with_error_message(output, cs::RESP_ERR_GENERIC_SYNTAX_ERROR);
          return Ok(true);
        }
        if idx >= parse_state.len() {
          abort_with_error_message(output, cs::RESP_ERR_GENERIC_SYNTAX_ERROR);
          return Ok(true);
        }
        let ov = parse_state[idx];
        idx += 1;
        if ov.eq_ignore_ascii_case(b"WRAP") {
          overflow = OverflowType::Wrap;
        } else if ov.eq_ignore_ascii_case(b"SAT") {
          overflow = OverflowType::Sat;
        } else if ov.eq_ignore_ascii_case(b"FAIL") {
          overflow = OverflowType::Fail;
        } else {
          abort_with_error_message(output, cs::RESP_ERR_GENERIC_SYNTAX_ERROR);
          return Ok(true);
        }
      } else if subcmd.eq_ignore_ascii_case(b"GET") {
        if idx + 2 > parse_state.len() {
          abort_with_error_message(output, cs::RESP_ERR_GENERIC_SYNTAX_ERROR);
          return Ok(true);
        }
        let Some(btype) = BitFieldType::parse(parse_state[idx]) else {
          abort_with_error_message(output, cs::RESP_ERR_GENERIC_SYNTAX_ERROR);
          return Ok(true);
        };
        let Some(offset) = parse_bitfield_offset(parse_state[idx + 1], btype.bit_count) else {
          abort_with_error_message(output, cs::RESP_ERR_GENERIC_BITOFFSET_IS_NOT_INTEGER);
          return Ok(true);
        };
        idx += 2;
        let v = BitmapManagerBitfield::get_value(&val, offset, btype);
        results.push(Some(v));
      } else if subcmd.eq_ignore_ascii_case(b"SET") {
        if read_only {
          abort_with_error_message(output, cs::RESP_ERR_GENERIC_SYNTAX_ERROR);
          return Ok(true);
        }
        if idx + 3 > parse_state.len() {
          abort_with_error_message(output, cs::RESP_ERR_GENERIC_SYNTAX_ERROR);
          return Ok(true);
        }
        let Some(btype) = BitFieldType::parse(parse_state[idx]) else {
          abort_with_error_message(output, cs::RESP_ERR_GENERIC_SYNTAX_ERROR);
          return Ok(true);
        };
        let Some(offset) = parse_bitfield_offset(parse_state[idx + 1], btype.bit_count) else {
          abort_with_error_message(output, cs::RESP_ERR_GENERIC_BITOFFSET_IS_NOT_INTEGER);
          return Ok(true);
        };
        let Some(new_val) = parse_state[idx + 2].try_parse_i64() else {
          abort_with_error_message(output, cs::RESP_ERR_GENERIC_VALUE_IS_NOT_INTEGER);
          return Ok(true);
        };
        idx += 3;

        let required_bytes = ((offset + btype.bit_count as i64 + 7) >> 3) as usize;
        if val.len() < required_bytes {
          val.resize(required_bytes, 0);
        }
        let old = BitmapManagerBitfield::set_value(&mut val, offset, btype, new_val);
        results.push(Some(old));
        modified = true;
      } else if subcmd.eq_ignore_ascii_case(b"INCRBY") {
        if read_only {
          abort_with_error_message(output, cs::RESP_ERR_GENERIC_SYNTAX_ERROR);
          return Ok(true);
        }
        if idx + 3 > parse_state.len() {
          abort_with_error_message(output, cs::RESP_ERR_GENERIC_SYNTAX_ERROR);
          return Ok(true);
        }
        let Some(btype) = BitFieldType::parse(parse_state[idx]) else {
          abort_with_error_message(output, cs::RESP_ERR_GENERIC_SYNTAX_ERROR);
          return Ok(true);
        };
        let Some(offset) = parse_bitfield_offset(parse_state[idx + 1], btype.bit_count) else {
          abort_with_error_message(output, cs::RESP_ERR_GENERIC_BITOFFSET_IS_NOT_INTEGER);
          return Ok(true);
        };
        let Some(incr) = parse_state[idx + 2].try_parse_i64() else {
          abort_with_error_message(output, cs::RESP_ERR_GENERIC_VALUE_IS_NOT_INTEGER);
          return Ok(true);
        };
        idx += 3;

        let required_bytes = ((offset + btype.bit_count as i64 + 7) >> 3) as usize;
        if val.len() < required_bytes {
          val.resize(required_bytes, 0);
        }
        let res =
          BitmapManagerBitfield::increment_bitfield(&mut val, offset, btype, incr, overflow);
        results.push(res);
        if res.is_some() {
          modified = true;
        }
      } else {
        abort_with_error_message(output, cs::RESP_ERR_GENERIC_SYNTAX_ERROR);
        return Ok(true);
      }
    }

    if modified && !read_only {
      match store.try_upsert_sync(key, &val) {
        Ok(Ok(_)) => {}
        Ok(Err(_)) => return Ok(false),
        Err(_) => {
          output.write_resp_error("generic error");
          return Ok(true);
        }
      }
    }

    output.write_resp_array_len(results.len());
    for item in results {
      match item {
        Some(v) => output.write_resp_int(v),
        None => output.write_resp_null(),
      }
    }
    Ok(true)
  }

  /// libs/server/Resp/Bitmap/BitmapCommands.cs:HandleFirstSubCommand
  pub fn handle_first_sub_command(&self) -> bool {
    true
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
}
