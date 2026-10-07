//! 位图管理核心（对标 libs/server/Resp/Bitmap/BitmapManager.cs）
//!
//! C# 以 `byte*` 裸指针读写位图；Rust 侧统一以切片承接，位序语义不变：
//! bit 0 为首字节最高位（MSB 在前）。C# 越界即抛 `GarnetException` 的私有
//! 辅助（Index/LengthInBytes）在此退化为 `Option`——调用方均先经 `Try*`
//! 校验，None 仅在违约时出现（刻意差异：不 panic）。
//!
//! 自研依据: bitmap 管理器（C# 对应 GarnetBitmap 命令引擎）

/// 位偏移单位口径：字节（BITPOS/BITCOUNT 语法 `BYTE`，Redis offsetType = 0）
pub const OFFSET_UNIT_BYTE: u8 = 0;
/// 位偏移单位口径：位（BITPOS/BITCOUNT 语法 `BIT`，Redis offsetType = 1）
pub const OFFSET_UNIT_BIT: u8 = 1;

/// 位图负载字节上限（libs/server/Resp/Bitmap/BitmapManager.cs:MaxBitmapPayloadBytes）
pub const MAX_BITMAP_PAYLOAD_BYTES: i64 = 512 * 1024 * 1024;

/// 编译期位区间掩码表：\[start\]\[end\] 对应字节内 \[start, end) 位的掩码（MSB 序，0 <= start <= end <= 8）
pub const BIT_RANGE_MASKS: [[u8; 9]; 9] = {
  let mut table = [[0u8; 9]; 9];
  let mut s = 0;
  while s <= 8 {
    let mut e = 0;
    while e <= 8 {
      if s < e {
        let mut m = 0u8;
        let mut i = s;
        while i < e {
          m |= 1 << (7 - i);
          i += 1;
        }
        table[s][e] = m;
      }
      e += 1;
    }
    s += 1;
  }
  table
};

/// 合法位偏移上限（libs/server/Resp/Bitmap/BitmapManager.cs:MaxOffsetForBitmapLength）
pub const MAX_OFFSET_FOR_BITMAP_LENGTH: i64 = (MAX_BITMAP_PAYLOAD_BYTES * 8) - 1;

/// libs/server/Resp/Bitmap/BitmapManager.cs:IsValidBitOffset
#[inline]
pub const fn is_valid_bit_offset(offset: i64) -> bool {
  offset >= 0 && offset <= MAX_OFFSET_FOR_BITMAP_LENGTH
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

  if is_valid_bit_offset(end_offset) {
    Some((normalized_offset, end_offset))
  } else {
    None
  }
}

/// libs/server/Resp/Bitmap/BitmapManager.cs:TryValidateBitPosOffsets
///
/// BITPOS 区间粗校验：越界返回 true（会话层直接回 -1）。
/// BYTE 模式用字节界；BIT 模式用位界（`offsetType == 0x1`）。
#[inline]
pub const fn try_validate_bit_pos_offsets(
  start_offset: i64,
  end_offset: i64,
  offset_type: u8,
  has_start_offset: bool,
  has_end_offset: bool,
) -> bool {
  let max_length = if offset_type == OFFSET_UNIT_BIT {
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
pub const fn index(offset: i64) -> Option<usize> {
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
pub const fn length_in_bytes(offset: i64) -> Option<i32> {
  if !is_valid_bit_offset(offset) {
    return None;
  }
  Some(((offset >> 3) + 1) as i32)
}

/// 在位图上置位并返回原 bit值（0/1）
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
pub const fn process_negative_offset(offset: i64, val_len: i64) -> i64 {
  if val_len <= 0 || offset <= -val_len {
    0
  } else {
    val_len + offset
  }
}

/// BITPOS/BITCOUNT 两驱动共享的区间归一化（单点收口，禁止第三处解释同一口径）
///
/// `len` 为字节口径负载长度；BIT 口径（`offset_type == OFFSET_UNIT_BIT`）内部
/// 折算为位长 `len * 8`。步骤与 Redis bitops.c:1780-1811 同序：负偏移折算 →
/// end 钳制上限 `limit - 1` → 空区间早退（起点越界或 `start > end`）。
///
/// 返回 `Some((start, end))` 为归一化后的闭合扫描区间；`None` = 空区间，
/// 调用方按各自哨兵映射一行（BITPOS → -1，BITCOUNT → 0）。
#[inline]
pub const fn normalize_scan_range(
  offset_type: u8,
  start_offset: i64,
  end_offset: i64,
  len: i64,
) -> Option<(i64, i64)> {
  let limit = if offset_type == OFFSET_UNIT_BIT {
    len * 8
  } else {
    len
  };
  let start = if start_offset < 0 {
    process_negative_offset(start_offset, limit)
  } else {
    start_offset
  };
  let mut end = if end_offset < 0 {
    process_negative_offset(end_offset, limit)
  } else {
    end_offset
  };
  if end >= limit {
    end = limit - 1;
  }
  if start >= limit || start > end {
    return None;
  }
  Some((start, end))
}
