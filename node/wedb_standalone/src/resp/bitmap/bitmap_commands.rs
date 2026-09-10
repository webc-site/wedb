//! 位图命令（SETBIT/GETBIT/BITCOUNT）
//!
//! 同步快路径：字符串域直读直写，磁盘候选等须异步裁决时返回 `Ok(false)`
//! 交调用方降级。位序对标 Redis/C#：bit 0 为首字节最高位。

use super::super::{
  basic_commands::MAX_STRING_PAYLOAD_BYTES,
  cmd_strings as cs,
  cmd_strings::{abort_with_error_message, abort_with_wrong_number_of_arguments},
  parser::resp_ext::{RespSliceExt, RespVecExt},
  resp_server_session::RespServerSession,
};

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

  pub fn network_string_bit_position() {
    unimplemented!()
  }
  pub fn network_string_bit_operation() {
    unimplemented!()
  }
  pub fn string_bit_field() {
    unimplemented!()
  }
  pub fn string_bit_field_read_only() {
    unimplemented!()
  }
  pub fn string_bit_field_action() {
    unimplemented!()
  }
  pub fn handle_first_sub_command() {
    unimplemented!()
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
