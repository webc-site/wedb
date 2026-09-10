//! 位图管理核心（对标 libs/server/Resp/Bitmap/BitmapManager.cs）
//!
//! C# 以 `byte*` 裸指针读写位图；Rust 侧统一以切片承接，位序语义不变：
//! bit 0 为首字节最高位（MSB 在前）。C# 越界即抛 `GarnetException` 的私有
//! 辅助（Index/LengthInBytes）在此退化为 `Option`——调用方均先经 `Try*`
//! 校验，None 仅在违约时出现（刻意差异：不 panic）。

/// 半字节位反转查找表（libs/server/Resp/Bitmap/BitmapManager.cs:lookup）
static LOOKUP: [u8; 16] = [
  0x0, 0x8, 0x4, 0xc, 0x2, 0xa, 0x6, 0xe, 0x1, 0x9, 0x5, 0xd, 0x3, 0xb, 0x7, 0xf,
];

/// 位图负载字节上限（libs/server/Resp/Bitmap/BitmapManager.cs:MaxBitmapPayloadBytes）
pub const MAX_BITMAP_PAYLOAD_BYTES: i64 = 512 * 1024 * 1024;

/// 合法位偏移上限（libs/server/Resp/Bitmap/BitmapManager.cs:MaxOffsetForBitmapLength）
pub const MAX_OFFSET_FOR_BITMAP_LENGTH: i64 = (MAX_BITMAP_PAYLOAD_BYTES * 8) - 1;

/// libs/server/Resp/Bitmap/BitmapManager.cs:IsValidBitOffset
#[inline]
pub fn is_valid_bit_offset(offset: i64) -> bool {
  (0..=MAX_OFFSET_FOR_BITMAP_LENGTH).contains(&offset)
}

/// libs/server/Resp/Bitmap/BitmapManager.cs:TryValidateLengthInBytes
///
/// 校验位偏移并给出覆盖该位所需的字节数（`offset >> 3 + 1`）
#[inline]
pub fn try_validate_length_in_bytes(offset: i64) -> Option<i32> {
  if !is_valid_bit_offset(offset) {
    return None;
  }
  Some(((offset >> 3) + 1) as i32)
}

/// libs/server/Resp/Bitmap/BitmapManager.cs:TryValidateBitfieldOffset
///
/// `multiplyOffset` 时 offset 先按位宽倍乘（`#<n>` 形式）；返回
/// （归一化 offset，域末端位 offset）。C# `checked` 溢出视为非法。
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
    offset.checked_mul(i64::from(bit_count))?
  } else {
    offset
  };
  if normalized_offset < 0 {
    return None;
  }
  let end_offset = normalized_offset.checked_add(i64::from(bit_count) - 1)?;

  is_valid_bit_offset(end_offset).then_some((normalized_offset, end_offset))
}

/// libs/server/Resp/Bitmap/BitmapManager.cs:TryValidateBitPosOffsets
///
/// BITPOS 区间粗校验：越界返回 true（会话层直接回 -1）。
/// BYTE 模式用字节界；BIT 模式用位界（`offsetType == 0x1`）。
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
    MAX_BITMAP_PAYLOAD_BYTES - 1
  };

  if has_start_offset && (start_offset < -max_offset || start_offset > max_offset) {
    return true;
  }

  if has_end_offset && (end_offset < -max_offset || end_offset > max_offset) {
    return true;
  }

  false
}

/// libs/server/Resp/Bitmap/BitmapManager.cs:NormalizeBitCountOffsets
///
/// BITCOUNT 区间钳制到 ±maxOffset（BYTE 字节界 / BIT 位界）；
/// C# 以 `ref` 就地改写，Rust 侧返回新值。
#[inline]
pub fn normalize_bit_count_offsets(
  start_offset: i64,
  end_offset: i64,
  offset_type: u8,
) -> (i64, i64) {
  let max_offset = if offset_type == 0x1 {
    MAX_OFFSET_FOR_BITMAP_LENGTH
  } else {
    MAX_BITMAP_PAYLOAD_BYTES - 1
  };

  let start = start_offset.clamp(-max_offset, max_offset);
  let end = end_offset.clamp(-max_offset, max_offset);
  (start, end)
}

/// libs/server/Resp/Bitmap/BitmapManager.cs:Index
///
/// C# 越界抛 GarnetException，此处以 None 表达（刻意差异：不 panic）
#[inline]
pub fn index(offset: i64) -> Option<usize> {
  if !is_valid_bit_offset(offset) {
    return None;
  }
  Some((offset >> 3) as usize)
}

/// libs/server/Resp/Bitmap/BitmapManager.cs:LengthInBytes
///
/// C# 越界抛 GarnetException，此处以 None 表达（刻意差异：不 panic）
#[inline]
pub fn length_in_bytes(offset: i64) -> Option<i32> {
  try_validate_length_in_bytes(offset)
}

/// libs/server/Resp/Bitmap/BitmapManager.cs:Length
#[inline]
pub fn length(offset: i64) -> Option<i32> {
  length_in_bytes(offset)
}

/// 检查位偏移是否落在值长之内
///
/// libs/server/Resp/Bitmap/BitmapManager.cs:IsLargeEnough
#[inline]
pub fn is_large_enough(vlen: i32, offset: i64) -> bool {
  // C# 对非法 offset 抛 GarnetException；调用方均先校验，此处退化 false
  length_in_bytes(offset).is_some_and(|len| len <= vlen)
}

/// 位图分配尺寸：值长与位偏移所需字节数取大
///
/// libs/server/Resp/Bitmap/BitmapManager.cs:NewBlockAllocLength
#[inline]
pub fn new_block_alloc_length(value_len: i32, offset: i64) -> i32 {
  // 调用方已校验 offset，length 必有值
  let length_in_bytes = length(offset).unwrap_or(i32::MAX);
  if value_len > length_in_bytes {
    value_len
  } else {
    length_in_bytes
  }
}

/// 按位偏移写入并返回原位值（调用方须保证值已增长到覆盖 offset）
///
/// libs/server/Resp/Bitmap/BitmapManager.cs:UpdateBitmap
pub fn update_bitmap(value: &mut [u8], offset: i64, set: u8) -> u8 {
  let Some(byte_index) = index(offset) else {
    return 0;
  };
  let bit_index = 7 - (offset & 7) as u32;

  let Some(byte_val) = value.get_mut(byte_index) else {
    return 0;
  };
  let old_val = (*byte_val >> bit_index) & 1;
  *byte_val = (*byte_val & !(1 << bit_index)) | ((set & 1) << bit_index);
  old_val
}

/// 读取位偏移处的位值；偏移落在已分配值之外恒为 0
///
/// libs/server/Resp/Bitmap/BitmapManager.cs:GetBit
pub fn get_bit(offset: i64, value: &[u8]) -> u8 {
  let Some(byte_index) = index(offset) else {
    return 0;
  };
  // 偏移超出已分配值大小时恒为 0
  let Some(byte_val) = value.get(byte_index) else {
    return 0;
  };
  let bit_index = 7 - (offset & 7) as u32;
  (byte_val >> bit_index) & 1
}

/// 负偏移折算：`(offset % len) + len`（len ≤ 0 时恒 0）
///
/// libs/server/Resp/Bitmap/BitmapManager.cs:ProcessNegativeOffset
#[inline]
pub(crate) fn process_negative_offset(offset: i64, val_len: i64) -> i64 {
  if val_len <= 0 {
    0
  } else {
    (offset % val_len) + val_len
  }
}

/// 单字节位反转
///
/// libs/server/Resp/Bitmap/BitmapManager.cs:reverse
#[inline]
pub(crate) fn reverse(n: u8) -> u8 {
  (LOOKUP[(n & 0b1111) as usize] << 4) | LOOKUP[(n >> 4) as usize]
}

#[cfg(test)]
mod tests {
  use super::{
    MAX_BITMAP_PAYLOAD_BYTES, MAX_OFFSET_FOR_BITMAP_LENGTH, index, is_large_enough,
    is_valid_bit_offset, length, new_block_alloc_length, normalize_bit_count_offsets,
    process_negative_offset, reverse, try_validate_bit_pos_offsets, try_validate_bitfield_offset,
    try_validate_length_in_bytes, update_bitmap,
  };

  #[test]
  fn offset_bounds() {
    assert!(is_valid_bit_offset(0));
    assert!(is_valid_bit_offset(MAX_OFFSET_FOR_BITMAP_LENGTH));
    assert!(!is_valid_bit_offset(-1));
    assert!(!is_valid_bit_offset(MAX_OFFSET_FOR_BITMAP_LENGTH + 1));
    assert_eq!(MAX_OFFSET_FOR_BITMAP_LENGTH, 512 * 1024 * 1024 * 8 - 1);
  }

  #[test]
  fn length_in_bytes_conversion() {
    assert_eq!(try_validate_length_in_bytes(0), Some(1));
    assert_eq!(try_validate_length_in_bytes(7), Some(1));
    assert_eq!(try_validate_length_in_bytes(8), Some(2));
    assert_eq!(try_validate_length_in_bytes(-1), None);
    assert_eq!(length(15), Some(2));
  }

  #[test]
  fn bitfield_offset_forms() {
    // 零位宽非法
    assert_eq!(try_validate_bitfield_offset(0, 0, false), None);
    // 普通 offset
    assert_eq!(try_validate_bitfield_offset(9, 8, false), Some((9, 16)));
    // # 倍乘：offset * bitCount
    assert_eq!(try_validate_bitfield_offset(2, 8, true), Some((16, 23)));
    // 负 offset 非法
    assert_eq!(try_validate_bitfield_offset(-1, 8, false), None);
    // 倍乘 i64 溢出（checked）
    assert_eq!(try_validate_bitfield_offset(i64::MAX / 2, 64, true), None);
    // 末端越位图上限
    assert_eq!(
      try_validate_bitfield_offset(MAX_OFFSET_FOR_BITMAP_LENGTH, 2, false),
      None
    );
  }

  #[test]
  fn bit_pos_offsets_bounds() {
    // BYTE 模式界为 MaxBitmapPayloadBytes - 1
    assert!(try_validate_bit_pos_offsets(
      MAX_BITMAP_PAYLOAD_BYTES,
      -1,
      0x0,
      true,
      true
    ));
    assert!(!try_validate_bit_pos_offsets(
      MAX_BITMAP_PAYLOAD_BYTES - 1,
      -1,
      0x0,
      true,
      true
    ));
    // BIT 模式界为位上限
    assert!(!try_validate_bit_pos_offsets(
      MAX_OFFSET_FOR_BITMAP_LENGTH,
      -1,
      0x1,
      true,
      true
    ));
    // 未提供的区间不参与校验
    assert!(!try_validate_bit_pos_offsets(
      -MAX_BITMAP_PAYLOAD_BYTES,
      0,
      0x0,
      false,
      false
    ));
  }

  #[test]
  fn normalize_clamps() {
    let (s, e) = normalize_bit_count_offsets(i64::MIN, i64::MAX, 0x0);
    assert_eq!(
      (s, e),
      (
        -(MAX_BITMAP_PAYLOAD_BYTES - 1),
        MAX_BITMAP_PAYLOAD_BYTES - 1
      )
    );
    let (s, e) = normalize_bit_count_offsets(-5, 5, 0x0);
    assert_eq!((s, e), (-5, 5));
  }

  #[test]
  fn index_and_alloc() {
    assert_eq!(index(0), Some(0));
    assert_eq!(index(8), Some(1));
    assert_eq!(index(-1), None);
    assert!(is_large_enough(1, 7));
    assert!(!is_large_enough(1, 8));
    assert!(is_large_enough(2, 8));
    assert_eq!(new_block_alloc_length(3, 8), 3);
    assert_eq!(new_block_alloc_length(1, 8), 2);
    assert_eq!(new_block_alloc_length(1, 0), 1);
  }

  #[test]
  fn update_and_get_bits() {
    let mut val = [0u8; 2];
    // bit0 = 首字节 MSB
    assert_eq!(update_bitmap(&mut val, 0, 1), 0);
    assert_eq!(val, [0x80, 0x00]);
    assert_eq!(update_bitmap(&mut val, 0, 1), 1);
    // 重复写同位
    assert_eq!(update_bitmap(&mut val, 9, 1), 0);
    assert_eq!(val, [0x80, 0x40]);
    // 清位回旧值
    assert_eq!(update_bitmap(&mut val, 9, 0), 1);
    assert_eq!(val, [0x80, 0x00]);

    assert_eq!(super::get_bit(0, &val), 1);
    assert_eq!(super::get_bit(7, &val), 0);
    // 越界恒 0
    assert_eq!(super::get_bit(16, &val), 0);
  }

  #[test]
  fn negative_offset_and_reverse() {
    assert_eq!(process_negative_offset(-1, 5), 4);
    // C# 口径：(-5 % 5) + 5 = 5
    assert_eq!(process_negative_offset(-5, 5), 5);
    assert_eq!(process_negative_offset(-6, 5), 4);
    assert_eq!(process_negative_offset(3, 0), 0);

    assert_eq!(reverse(0b0000_0001), 0b1000_0000);
    assert_eq!(reverse(0b1011_0001), 0b1000_1101);
    assert_eq!(reverse(0), 0);
    assert_eq!(reverse(0xff), 0xff);
  }
}
