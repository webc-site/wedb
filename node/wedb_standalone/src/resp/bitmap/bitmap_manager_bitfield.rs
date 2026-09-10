//! 位域命令（BITFIELD）底层算法
//! 对标 Garnet `libs/server/Resp/Bitmap/BitmapManagerBitfield.cs`

use super::bitmap_manager::BitmapManager;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OverflowType {
  Wrap,
  Sat,
  Fail,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BitFieldType {
  pub is_signed: bool,
  pub bit_count: u8,
}

impl BitFieldType {
  pub fn parse(s: &[u8]) -> Option<Self> {
    if s.is_empty() {
      return None;
    }
    let is_signed = match s[0] {
      b'i' | b'I' => true,
      b'u' | b'U' => false,
      _ => return None,
    };
    let count: u8 = core::str::from_utf8(&s[1..]).ok()?.parse().ok()?;
    if count == 0 || (is_signed && count > 64) || (!is_signed && count > 63) {
      return None;
    }
    Some(Self {
      is_signed,
      bit_count: count,
    })
  }
}

pub struct BitmapManagerBitfield;

impl BitmapManagerBitfield {
  /// libs/server/Resp/Bitmap/BitmapManagerBitfield.cs:IsLargeEnoughForType
  pub fn is_large_enough_for_type(offset: i64, bit_count: u8, vlen: usize) -> bool {
    let end_offset = offset + bit_count as i64 - 1;
    BitmapManager::is_large_enough(vlen, end_offset)
  }

  /// libs/server/Resp/Bitmap/BitmapManagerBitfield.cs:LengthFromType
  pub fn length_from_type(offset: i64, bit_count: u8) -> usize {
    let end_offset = offset + bit_count as i64 - 1;
    BitmapManager::length_in_bytes(end_offset).unwrap_or(0)
  }

  /// libs/server/Resp/Bitmap/BitmapManagerBitfield.cs:NewBlockAllocLengthFromType
  pub fn new_block_alloc_length_from_type(value_len: usize, offset: i64, bit_count: u8) -> usize {
    let required = Self::length_from_type(offset, bit_count);
    value_len.max(required)
  }

  /// libs/server/Resp/Bitmap/BitmapManagerBitfield.cs:CheckBitfieldOverflow
  pub fn check_bitfield_overflow(
    value: i64,
    incr_by: i64,
    bit_count: u8,
    overflow_type: OverflowType,
    signed: bool,
  ) -> (i64, bool) {
    if signed {
      Self::check_signed_bitfield_overflow(value, incr_by, bit_count, overflow_type)
    } else {
      let (nv, ov) =
        Self::check_unsigned_bitfield_overflow(value as u64, incr_by, bit_count, overflow_type);
      (nv as i64, ov)
    }
  }

  /// libs/server/Resp/Bitmap/BitmapManagerBitfield.cs:CheckUnsignedBitfieldOverflow
  pub fn check_unsigned_bitfield_overflow(
    value: u64,
    incr_by: i64,
    bit_count: u8,
    overflow_type: OverflowType,
  ) -> (u64, bool) {
    let max_val = if bit_count == 64 {
      u64::MAX
    } else {
      (1u64 << bit_count) - 1
    };
    let max_add = max_val.saturating_sub(value);
    let neg = incr_by < 0;
    let abs_incr = if neg {
      (!incr_by as u64).wrapping_add(1)
    } else {
      incr_by as u64
    };
    let overflow = abs_incr > max_add;
    let underflow = abs_incr > value && neg;
    let result = if neg {
      value.wrapping_sub(abs_incr)
    } else {
      value.wrapping_add(abs_incr)
    };
    let masked = result & max_val;
    match overflow_type {
      OverflowType::Wrap => (masked, overflow || underflow),
      OverflowType::Sat => {
        if overflow {
          (max_val, true)
        } else if underflow {
          (0, true)
        } else {
          (result, false)
        }
      }
      OverflowType::Fail => {
        if overflow || underflow {
          (0, true)
        } else {
          (masked, false)
        }
      }
    }
  }

  /// libs/server/Resp/Bitmap/BitmapManagerBitfield.cs:CheckSignedBitfieldOverflow
  pub fn check_signed_bitfield_overflow(
    value: i64,
    incr_by: i64,
    bit_count: u8,
    overflow_type: OverflowType,
  ) -> (i64, bool) {
    let signbit = 1i64 << (bit_count - 1);
    let mask = if bit_count == 64 { -1i64 } else { signbit - 1 };
    let result = value.wrapping_add(incr_by);
    let underflow = (result & signbit) == 0 && value < 0 && incr_by < 0;
    let overflow = if bit_count == 64 {
      result < 0 && value >= 0 && incr_by > 0
    } else {
      ((result & !mask) as u64) > 0 && value >= 0 && incr_by > 0
    };
    match overflow_type {
      OverflowType::Wrap => {
        if underflow || overflow {
          let mut res = result as u64;
          if bit_count < 64 {
            let msb = signbit as u64;
            let smask = mask as u64;
            res = if (res & msb) > 0 {
              res | !smask
            } else {
              res & smask
            };
          }
          (res as i64, true)
        } else {
          (result, false)
        }
      }
      OverflowType::Sat => {
        let max_val = if bit_count == 64 {
          i64::MAX
        } else {
          signbit - 1
        };
        if overflow {
          (max_val, true)
        } else if underflow {
          (-max_val - 1, true)
        } else {
          (result, false)
        }
      }
      OverflowType::Fail => {
        if underflow || overflow {
          (0, true)
        } else {
          (result, false)
        }
      }
    }
  }

  /// libs/server/Resp/Bitmap/BitmapManagerBitfield.cs:GetValue
  pub fn get_value(bitmap: &[u8], offset: i64, b_type: BitFieldType) -> i64 {
    let bit_count = b_type.bit_count;
    let mut raw_val = 0u64;

    for i in 0..bit_count {
      let cur_bit = offset + i as i64;
      let byte_idx = (cur_bit >> 3) as usize;
      let bit_idx = 7 - (cur_bit & 7) as u32;

      let b = if byte_idx < bitmap.len() {
        (bitmap[byte_idx] >> bit_idx) & 1
      } else {
        0
      };
      raw_val = (raw_val << 1) | (b as u64);
    }

    if b_type.is_signed {
      // 符号扩展
      if bit_count < 64 && (raw_val & (1 << (bit_count - 1))) != 0 {
        let mask = !0u64 << bit_count;
        (raw_val | mask) as i64
      } else {
        raw_val as i64
      }
    } else {
      raw_val as i64
    }
  }

  /// libs/server/Resp/Bitmap/BitmapManagerBitfield.cs:SetValue
  pub fn set_value(bitmap: &mut [u8], offset: i64, b_type: BitFieldType, value: i64) -> i64 {
    let old_val = Self::get_value(bitmap, offset, b_type);
    let bit_count = b_type.bit_count;
    let val_u64 = value as u64;

    for i in 0..bit_count {
      let cur_bit = offset + i as i64;
      let byte_idx = (cur_bit >> 3) as usize;
      let bit_idx = 7 - (cur_bit & 7) as u32;

      let bit_to_set = ((val_u64 >> (bit_count - 1 - i)) & 1) as u8;
      if byte_idx < bitmap.len() {
        if bit_to_set == 1 {
          bitmap[byte_idx] |= 1 << bit_idx;
        } else {
          bitmap[byte_idx] &= !(1 << bit_idx);
        }
      }
    }

    old_val
  }

  /// libs/server/Resp/Bitmap/BitmapManagerBitfield.cs:SetBitfield
  pub fn set_bitfield(
    bitmap: &mut [u8],
    offset: i64,
    b_type: BitFieldType,
    new_value: i64,
    overflow_type: OverflowType,
  ) -> (i64, bool) {
    let old_value = Self::get_value(bitmap, offset, b_type);
    if overflow_type == OverflowType::Fail {
      let (_, overflow) = Self::check_bitfield_overflow(
        old_value,
        0,
        b_type.bit_count,
        overflow_type,
        b_type.is_signed,
      );
      if overflow {
        return (0, true);
      }
    }
    Self::set_value(bitmap, offset, b_type, new_value);
    (old_value, false)
  }

  /// libs/server/Resp/Bitmap/BitmapManagerBitfield.cs:BitFieldExecute
  pub fn bit_field_execute(
    bitmap: &mut [u8],
    offset: i64,
    b_type: BitFieldType,
    sub_cmd: &[u8],
    arg: i64,
    overflow_type: OverflowType,
  ) -> (Option<i64>, bool) {
    if sub_cmd.eq_ignore_ascii_case(b"GET") {
      let val = Self::get_value(bitmap, offset, b_type);
      (Some(val), false)
    } else if sub_cmd.eq_ignore_ascii_case(b"SET") {
      let (old, ov) = Self::set_bitfield(bitmap, offset, b_type, arg, overflow_type);
      if ov { (None, true) } else { (Some(old), false) }
    } else if sub_cmd.eq_ignore_ascii_case(b"INCRBY") {
      let (new_val, ov) = Self::check_bitfield_overflow(
        Self::get_value(bitmap, offset, b_type),
        arg,
        b_type.bit_count,
        overflow_type,
        b_type.is_signed,
      );
      if ov && overflow_type == OverflowType::Fail {
        (None, true)
      } else {
        Self::set_value(bitmap, offset, b_type, new_val);
        (Some(new_val), ov)
      }
    } else {
      (None, true)
    }
  }

  /// libs/server/Resp/Bitmap/BitmapManagerBitfield.cs:BitFieldExecute_RO
  pub fn bit_field_execute_ro(bitmap: &[u8], offset: i64, b_type: BitFieldType) -> i64 {
    Self::get_value(bitmap, offset, b_type)
  }

  /// libs/server/Resp/Bitmap/BitmapManagerBitfield.cs:IncrementBitfield
  pub fn increment_bitfield(
    bitmap: &mut [u8],
    offset: i64,
    b_type: BitFieldType,
    incr: i64,
    overflow: OverflowType,
  ) -> Option<i64> {
    let old_val = Self::get_value(bitmap, offset, b_type);
    let (new_val, did_overflow) =
      Self::check_bitfield_overflow(old_val, incr, b_type.bit_count, overflow, b_type.is_signed);

    if did_overflow && overflow == OverflowType::Fail {
      return None;
    }

    Self::set_value(bitmap, offset, b_type, new_val);
    Some(new_val)
  }
}
