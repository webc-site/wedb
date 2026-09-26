//! 位图管理核心（对标 libs/server/Resp/Bitmap/BitmapManager.cs）
//!
//! C# 以 `byte*` 裸指针读写位图；Rust 侧统一以切片承接，位序语义不变：
//! bit 0 为首字节最高位（MSB 在前）。C# 越界即抛 `GarnetException` 的私有
//! 辅助（Index/LengthInBytes）在此退化为 `Option`——调用方均先经 `Try*`
//! 校验，None 仅在违约时出现（刻意差异：不 panic）。
//!
//! 自研依据: bitmap 管理器（C# 对应 GarnetBitmap 命令引擎）

/// 位图负载字节上限（libs/server/Resp/Bitmap/BitmapManager.cs:MaxBitmapPayloadBytes）
pub const MAX_BITMAP_PAYLOAD_BYTES: i64 = 512 * 1024 * 1024;

/// 合法位偏移上限（libs/server/Resp/Bitmap/BitmapManager.cs:MaxOffsetForBitmapLength）
pub(crate) const MAX_OFFSET_FOR_BITMAP_LENGTH: i64 = (MAX_BITMAP_PAYLOAD_BYTES * 8) - 1;

/// libs/server/Resp/Bitmap/BitmapManager.cs:IsValidBitOffset
#[inline]
pub fn is_valid_bit_offset(offset: i64) -> bool {
  (0..=MAX_OFFSET_FOR_BITMAP_LENGTH).contains(&offset)
}

/// libs/server/Resp/Bitmap/BitmapManager.cs:TryValidateLengthInBytes
///
/// 校验位偏移并给出覆盖该位所需的字节数（`offset >> 3 + 1`）
#[inline]
fn try_validate_length_in_bytes(offset: i64) -> Option<i32> {
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
pub(crate) fn try_validate_bitfield_offset(
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
  let max_length = if offset_type == 0x1 {
    MAX_OFFSET_FOR_BITMAP_LENGTH + 1
  } else {
    MAX_BITMAP_PAYLOAD_BYTES
  };
  let max_offset = max_length - 1;

  if has_start_offset && (start_offset < -max_length || start_offset > max_offset) {
    return true;
  }

  if has_end_offset && (end_offset < -max_length || end_offset > max_offset) {
    return true;
  }

  false
}

/// libs/server/Resp/Bitmap/BitmapManager.cs:Index
///
/// C# 越界抛 GarnetException，此处以 None 表达（刻意差异：不 panic）
#[inline]
pub(crate) fn index(offset: i64) -> Option<usize> {
  if !is_valid_bit_offset(offset) {
    return None;
  }
  Some((offset >> 3) as usize)
}

/// libs/server/Resp/Bitmap/BitmapManager.cs:LengthInBytes
///
/// C# 越界抛 GarnetException，此处以 None 表达（刻意差异：不 panic）。
/// C# 会话层 SETBIT 增长经 BitmapManager.Length（= LengthInBytes）承接。
#[inline]
pub fn length_in_bytes(offset: i64) -> Option<i32> {
  try_validate_length_in_bytes(offset)
}

/// 在位图上置位并返回原 bit 值（0/1）
///
/// C# 越界抛 GarnetException；Rust 调用方（SETBIT 会话层）须先按
/// [`length_in_bytes`] 增长负载，违约仅 debug_assert（刻意差异：不 panic）。
///
/// libs/server/Resp/Bitmap/BitmapManager.cs:UpdateBitmap
#[inline]
pub fn update_bitmap(value: &mut [u8], offset: i64, set: u8) -> u8 {
  let byte_index = (offset >> 3) as usize;
  let bit_index = 7 - (offset & 7) as u32;
  debug_assert!(
    byte_index < value.len(),
    "SETBIT 越界：调用方须先按 length_in_bytes 增长"
  );
  let byte_val = &mut value[byte_index];
  let old_val = (*byte_val >> bit_index) & 1;
  *byte_val = (*byte_val & !(1 << bit_index)) | (set << bit_index);
  old_val
}

/// 读取位图指定位（0/1）；offset 越出负载界恒 0
///
/// libs/server/Resp/Bitmap/BitmapManager.cs:GetBit
/// （C# valLen 形参由切片长度承接）
#[inline]
pub fn get_bit(offset: i64, value: &[u8]) -> u8 {
  let byte_index = (offset >> 3) as usize;
  if byte_index >= value.len() {
    return 0;
  }
  (value[byte_index] >> (7 - (offset & 7) as u32)) & 1
}

/// 负偏移折算：越过值头部钳制为 0，否则 `val_len + offset`（val_len ≤ 0 时恒 0）
///
/// libs/server/Resp/Bitmap/BitmapManager.cs:ProcessNegativeOffset
#[inline]
pub(crate) fn process_negative_offset(offset: i64, val_len: i64) -> i64 {
  if val_len <= 0 || offset <= -val_len {
    0
  } else {
    val_len + offset
  }
}

/// 单字节位反转（硬件位反转原语，单周期指令并消除查表开销）
///
/// libs/server/Resp/Bitmap/BitmapManager.cs:reverse
#[inline(always)]
pub(crate) const fn reverse(n: u8) -> u8 {
  n.reverse_bits()
}

#[cfg(test)]
mod tests {
  use super::{
    MAX_BITMAP_PAYLOAD_BYTES, MAX_OFFSET_FOR_BITMAP_LENGTH, get_bit, index, is_valid_bit_offset,
    length_in_bytes, process_negative_offset, reverse, try_validate_bit_pos_offsets,
    try_validate_bitfield_offset, try_validate_length_in_bytes, update_bitmap,
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
    assert_eq!(length_in_bytes(15), Some(2));
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
    // BYTE 模式界为 MaxBitmapPayloadBytes - 1, 负界为 -MaxBitmapPayloadBytes
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
    assert!(!try_validate_bit_pos_offsets(
      -MAX_BITMAP_PAYLOAD_BYTES,
      -1,
      0x0,
      true,
      true
    ));
    assert!(try_validate_bit_pos_offsets(
      -MAX_BITMAP_PAYLOAD_BYTES - 1,
      -1,
      0x0,
      true,
      true
    ));
    // BIT 模式界为位上限，负界为 -(MAX_OFFSET_FOR_BITMAP_LENGTH + 1)
    assert!(!try_validate_bit_pos_offsets(
      MAX_OFFSET_FOR_BITMAP_LENGTH,
      -1,
      0x1,
      true,
      true
    ));
    assert!(!try_validate_bit_pos_offsets(
      -(MAX_OFFSET_FOR_BITMAP_LENGTH + 1),
      -1,
      0x1,
      true,
      true
    ));
    assert!(try_validate_bit_pos_offsets(
      -(MAX_OFFSET_FOR_BITMAP_LENGTH + 2),
      -1,
      0x1,
      true,
      true
    ));
    // 未提供的区间不参与校验
    assert!(!try_validate_bit_pos_offsets(
      -MAX_BITMAP_PAYLOAD_BYTES - 1,
      0,
      0x0,
      false,
      false
    ));
  }

  #[test]
  fn index_offset_to_byte() {
    assert_eq!(index(0), Some(0));
    assert_eq!(index(8), Some(1));
    assert_eq!(index(-1), None);
  }

  /// 单点位读写对拍：置位回旧值、MSB 在前、越值界恒 0，与朴素逐位实现一致
  #[test]
  fn update_get_bit_parity() {
    // 置位回旧值（MSB 在前：bit 0 → 0x80）
    let mut val = vec![0u8; 2];
    assert_eq!(update_bitmap(&mut val, 0, 1), 0);
    assert_eq!(val, vec![0x80, 0x00]);
    assert_eq!(update_bitmap(&mut val, 12, 1), 0);
    assert_eq!(val, vec![0x80, 0x08]);
    // 同位覆写回旧值
    assert_eq!(update_bitmap(&mut val, 12, 0), 1);
    assert_eq!(val, vec![0x80, 0x00]);
    assert_eq!(get_bit(0, &val), 1);
    // 越值界恒 0（C# GetBit byteIndex >= valLen）
    assert_eq!(get_bit(16, &val), 0);
    assert_eq!(get_bit(4096, &val), 0);

    // 与朴素逐位实现全偏移对拍
    let mut x: u64 = 0x9E37_79B9_7F4A_7C15;
    let mut buf = vec![0u8; 9];
    for offset in 0..(9 * 8) as i64 {
      x ^= x << 13;
      x ^= x >> 7;
      x ^= x << 17;
      let set = (x & 1) as u8;
      let naive_old = (buf[(offset >> 3) as usize] >> (7 - (offset & 7))) & 1;
      assert_eq!(
        update_bitmap(&mut buf, offset, set),
        naive_old,
        "offset={offset}"
      );
      assert_eq!(get_bit(offset, &buf), set, "offset={offset}");
    }
    assert_eq!(get_bit(9 * 8, &buf), 0);
  }

  #[test]
  fn negative_offset_and_reverse() {
    assert_eq!(process_negative_offset(-1, 5), 4);
    // 负偏移越过或等于 -val_len 时钳制为 0
    assert_eq!(process_negative_offset(-5, 5), 0);
    assert_eq!(process_negative_offset(-6, 5), 0);
    assert_eq!(process_negative_offset(3, 0), 0);

    assert_eq!(reverse(0b0000_0001), 0b1000_0000);
    assert_eq!(reverse(0b1011_0001), 0b1000_1101);
    assert_eq!(reverse(0), 0);
    assert_eq!(reverse(0xff), 0xff);
  }
}
