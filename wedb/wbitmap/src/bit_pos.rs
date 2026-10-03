//! BITPOS 位查找驱动（对标 libs/server/Resp/Bitmap/BitmapManagerBitPos.cs，
//! C# 为 BitmapManager partial）
//!
//! 搜索思路与 C# 一致：把区间负载移到 u64 高位后数前导零；全段命中无效
//! 负载（全 0 / 全 1）则整段跳过。
//!
//! 自研依据: 位定位算术（C# 对应 GarnetBitmap BITPOS 面 test/standalone/Garnet.test.complexstring/GarnetBitmapTests.cs）

use crate::manager::{OFFSET_UNIT_BYTE, normalize_scan_range};

/// BITPOS 主驱动：BYTE / BIT 两口径归一化区间后搜索
///
/// `has_end_offset` 语义即 Redis end_given（bitops.c:1726/1753）：调用方是否
/// 显式给出 end；仅 BYTE 口径找 0 无显式 end 时右尾零填充外延（见
/// [`bit_pos_byte_search`] 尾出口）。
///
/// libs/server/Resp/Bitmap/BitmapManagerBitPos.cs:BitPosDriver
pub fn bit_pos_driver(
  input: &[u8],
  input_len: i64,
  start_offset: i64,
  end_offset: i64,
  search_for: u8,
  offset_type: u8,
  has_end_offset: bool,
) -> i64 {
  // 区间归一化单点在 [`normalize_scan_range`]（负偏移、end 钳制、空区间早退）；
  // BIT 口径按位长折算后，原字节粒度早退（start_byte >= input_len、
  // start_byte > end_byte）与位粒度判定结果逐例一致——字节级倒序仅发生在
  // 同字节内，届时搜索循环为空、同样回落 -1
  let Some((start_offset, end_offset)) =
    normalize_scan_range(offset_type, start_offset, end_offset, input_len)
  else {
    // 空区间哨兵（Redis BITPOS 未命中恒 -1）
    return -1;
  };

  if offset_type == OFFSET_UNIT_BYTE {
    // BYTE 口径
    bit_pos_byte_search(input, start_offset, end_offset, search_for, has_end_offset)
  } else {
    // BIT 口径
    // 不变式（钉死）：parse 层文法（parse_bit_pos_args 中 count>4 蕴含 count>3）
    // 保证 unit（BIT/BYTE）仅与 start、end 同现——BIT 形 has_end_offset 恒真，
    // 恒为显式 end 闭合区间，本臂不消费 end_given、无匹配维持 -1，BIT 形行为
    // 零改动（Redis bitops.c:1764-1767 虚拟字节臂仅在带 end 的 BIT 形被钳回
    // totlen*8-1，两侧现形已等）；若未来扩「BIT 无 end」形态，虚拟零字节臂另案
    bit_pos_bit_search(input, start_offset, end_offset, search_for)
  }
}

/// 位区间搜索：逐字节裁剪出区间位，负载移位到高位后数前导零
///
/// libs/server/Resp/Bitmap/BitmapManagerBitPos.cs:BitPosBitSearch
fn bit_pos_bit_search(
  input: &[u8],
  start_bit_offset: i64,
  end_bit_offset: i64,
  search_for: u8,
) -> i64 {
  let search_bit = search_for == 1;
  let invalid_payload = if search_bit { 0x00u8 } else { 0xff };
  let mut current_bit_offset = start_bit_offset;
  while current_bit_offset <= end_bit_offset {
    let byte_index = (current_bit_offset >> 3) as usize;
    let left_bit_offset = (current_bit_offset & 7) as u32;
    let boundary = 8 - left_bit_offset as i64;
    let right_bit_offset = if current_bit_offset + boundary <= end_bit_offset {
      left_bit_offset + 8
    } else {
      (end_bit_offset & 7) as u32 + 1
    };

    // 裁剪字节到起止位
    let mask: u64 = ((0xffu64 >> left_bit_offset) ^ (0xffu64 >> right_bit_offset)) & 0xff;
    let payload = u64::from(input[byte_index] & mask as u8);

    // 全段命中无效负载则跳到下一字节
    let invalid_mask = u64::from(invalid_payload) & mask;
    if payload != invalid_mask {
      let payload = payload << (56 + left_bit_offset);
      let payload = if search_bit { payload } else { !payload };

      let lzcnt = payload.leading_zeros() as i64;
      return current_bit_offset + lzcnt;
    }

    current_bit_offset += boundary;
  }

  -1
}

/// 字节区间搜索：8/4/1 字节分段大端读入后数前导零
///
/// libs/server/Resp/Bitmap/BitmapManagerBitPos.cs:BitPosByteSearch
fn bit_pos_byte_search(
  input: &[u8],
  start_offset: i64,
  end_offset: i64,
  search_for: u8,
  has_end_offset: bool,
) -> i64 {
  // 初始化
  let search_bit = search_for == 1;
  let invalid_mask8 = if search_bit { 0x00u8 } else { 0xff };
  let invalid_mask32 = if search_bit { 0i32 } else { -1 };
  let invalid_mask64 = if search_bit { 0i64 } else { -1 };
  let mut current_start_offset = start_offset;

  while current_start_offset <= end_offset {
    let remainder = end_offset - current_start_offset + 1;
    if remainder >= 8 {
      let idx = current_start_offset as usize;
      debug_assert!(idx + 8 <= input.len());
      let payload = unsafe {
        (input.as_ptr().add(idx) as *const i64)
          .read_unaligned()
          .to_be()
      };

      if payload != invalid_mask64 {
        let payload = if search_bit { payload } else { !payload };
        let lzcnt = payload.leading_zeros() as i64;
        return (current_start_offset << 3) + lzcnt;
      }
      current_start_offset += 8;
    } else if remainder >= 4 {
      let idx = current_start_offset as usize;
      debug_assert!(idx + 4 <= input.len());
      let payload = unsafe {
        (input.as_ptr().add(idx) as *const i32)
          .read_unaligned()
          .to_be()
      };

      if payload != invalid_mask32 {
        let payload = if search_bit { payload } else { !payload };
        let lzcnt = payload.leading_zeros() as i64;
        return (current_start_offset << 3) + lzcnt;
      }
      current_start_offset += 4;
    } else {
      let byte = input[current_start_offset as usize];
      if byte != invalid_mask8 {
        // 当前字节移到最高字节位置后数前导零
        let raw = i64::from(byte) << 56;
        let payload = if search_bit { raw } else { !raw };
        let lzcnt = payload.leading_zeros() as i64;
        return (current_start_offset << 3) + lzcnt;
      }
      current_start_offset += 1;
    }
  }

  // 未命中出口（Redis 标准无界零尾回改，C# 上游缺形同此修）：
  // 找 0 且未显式给 end（全串/仅 start 形）→ 区间右侧视为零填充延伸，回首个
  // 不属于串的位（input_len*8）；显式 end（含 end=-1）闭合区间维持 -1；
  // 找 1 形永不外延。
  // 防御性说明：「区间非空且 input_len>0」由驱动既有早退（start>=input_len、
  // start>end）恒真保证，不写运行时条件；空串走早退保持 -1；缺失键臂
  // （找 0 回 0 找 1 回 -1）不入此路。
  // Redis 锚（unstable 实测行号）：无界零尾 1851-1854（条件 bit==0）、
  // 缺键臂 1800-1806、空区间前判 1808-1811
  if search_for == 0 && !has_end_offset {
    return input.len() as i64 * 8;
  }
  -1
}
