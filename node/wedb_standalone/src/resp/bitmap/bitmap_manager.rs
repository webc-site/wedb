//! 位图管理底层算法与工具函数
//! 对标 Garnet `libs/server/Resp/Bitmap/BitmapManager.cs`

pub struct BitmapManager;

/// 最大位图载荷字节数（512MB）
pub const MAX_BITMAP_PAYLOAD_BYTES: usize = 512 * 1024 * 1024;

/// 最大合法位偏移（(512MB * 8) - 1）
pub const MAX_OFFSET_FOR_BITMAP_LENGTH: i64 = (MAX_BITMAP_PAYLOAD_BYTES as i64 * 8) - 1;

impl BitmapManager {
  /// libs/server/Resp/Bitmap/BitmapManager.cs:IsValidBitOffset
  #[inline(always)]
  pub const fn is_valid_bit_offset(offset: i64) -> bool {
    offset >= 0 && offset <= MAX_OFFSET_FOR_BITMAP_LENGTH
  }

  /// libs/server/Resp/Bitmap/BitmapManager.cs:TryValidateLengthInBytes
  #[inline(always)]
  pub const fn try_validate_length_in_bytes(offset: i64) -> Option<usize> {
    if !Self::is_valid_bit_offset(offset) {
      return None;
    }
    Some(((offset >> 3) + 1) as usize)
  }

  /// libs/server/Resp/Bitmap/BitmapManager.cs:TryValidateBitfieldOffset
  #[inline]
  pub fn try_validate_bitfield_offset(
    offset: i64,
    bit_count: u8,
    multiply_offset: bool,
  ) -> Option<(i64, i64)> {
    if bit_count == 0 {
      return None;
    }

    let normalized_offset = if multiply_offset {
      offset.checked_mul(bit_count as i64)?
    } else {
      offset
    };

    if normalized_offset < 0 {
      return None;
    }

    let end_offset = normalized_offset.checked_add(bit_count as i64 - 1)?;

    if Self::is_valid_bit_offset(end_offset) {
      Some((normalized_offset, end_offset))
    } else {
      None
    }
  }

  /// libs/server/Resp/Bitmap/BitmapManager.cs:TryValidateBitPosOffsets
  #[inline]
  pub fn try_validate_bit_pos_offsets(
    start_offset: i64,
    end_offset: i64,
    offset_type: u8,
    has_start_offset: bool,
    has_end_offset: bool,
  ) -> bool {
    let max_offset = if offset_type == 0x1 {
      MAX_OFFSET_FOR_BITMAP_LENGTH
    } else {
      MAX_BITMAP_PAYLOAD_BYTES as i64 - 1
    };

    if has_start_offset && !(start_offset >= -max_offset && start_offset <= max_offset) {
      return true;
    }

    if has_end_offset && !(end_offset >= -max_offset && end_offset <= max_offset) {
      return true;
    }

    false
  }

  /// libs/server/Resp/Bitmap/BitmapManager.cs:NormalizeBitCountOffsets
  #[inline]
  pub fn normalize_bit_count_offsets(
    start_offset: &mut i64,
    end_offset: &mut i64,
    offset_type: u8,
  ) {
    let max_offset = if offset_type == 0x1 {
      MAX_OFFSET_FOR_BITMAP_LENGTH
    } else {
      MAX_BITMAP_PAYLOAD_BYTES as i64 - 1
    };

    if *start_offset < -max_offset {
      *start_offset = -max_offset;
    } else if *start_offset > max_offset {
      *start_offset = max_offset;
    }

    if *end_offset < -max_offset {
      *end_offset = -max_offset;
    } else if *end_offset > max_offset {
      *end_offset = max_offset;
    }
  }

  /// libs/server/Resp/Bitmap/BitmapManager.cs:LengthInBytes
  #[inline]
  pub const fn length_in_bytes(offset: i64) -> Option<usize> {
    Self::try_validate_length_in_bytes(offset)
  }

  /// libs/server/Resp/Bitmap/BitmapManager.cs:IsLargeEnough
  #[inline]
  pub fn is_large_enough(vlen: usize, offset: i64) -> bool {
    match Self::length_in_bytes(offset) {
      Some(len) => len <= vlen,
      None => false,
    }
  }

  /// libs/server/Resp/Bitmap/BitmapManager.cs:NewBlockAllocLength
  #[inline]
  pub fn new_block_alloc_length(value_len: usize, offset: i64) -> usize {
    let length_in_bytes = Self::length_in_bytes(offset).unwrap_or(0);
    value_len.max(length_in_bytes)
  }

  /// libs/server/Resp/Bitmap/BitmapManager.cs:UpdateBitmap
  #[inline]
  pub fn update_bitmap(value: &mut [u8], offset: i64, set: u8) -> u8 {
    let byte_index = (offset >> 3) as usize;
    let bit_index = 7 - (offset & 7) as u32;

    let byte_val = value[byte_index];
    let old_val = (byte_val >> bit_index) & 1;

    if set == 1 {
      value[byte_index] |= 1 << bit_index;
    } else {
      value[byte_index] &= !(1 << bit_index);
    }
    old_val
  }

  /// libs/server/Resp/Bitmap/BitmapManager.cs:ProcessNegativeOffset
  #[inline(always)]
  pub const fn process_negative_offset(offset: i64, val_len: i64) -> i64 {
    if val_len <= 0 {
      0
    } else {
      (offset % val_len) + val_len
    }
  }

  /// libs/server/Resp/Bitmap/BitmapManager.cs:reverse
  #[inline(always)]
  pub const fn reverse(n: u8) -> u8 {
    n.reverse_bits()
  }
}
