#![recursion_limit = "512"] // 泛型设备实例化下 async 状态机嵌套深（R27 CI 实证）
use wbitmap::{
  MAX_BITMAP_PAYLOAD_BYTES, MAX_OFFSET_FOR_BITMAP_LENGTH, get_bit, index, is_valid_bit_offset,
  length_in_bytes, process_negative_offset, try_validate_bit_pos_offsets,
  try_validate_bitfield_offset, update_bitmap,
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
  assert_eq!(length_in_bytes(0), Some(1));
  assert_eq!(length_in_bytes(7), Some(1));
  assert_eq!(length_in_bytes(8), Some(2));
  assert_eq!(length_in_bytes(-1), None);
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
fn negative_offset_clamps() {
  assert_eq!(process_negative_offset(-1, 5), 4);
  // 负偏移越过或等于 -val_len 时钳制为 0
  assert_eq!(process_negative_offset(-5, 5), 0);
  assert_eq!(process_negative_offset(-6, 5), 0);
  assert_eq!(process_negative_offset(3, 0), 0);
}
