//! BITCOUNT 位计数驱动（对标 libs/server/Resp/Bitmap/BitmapManagerBitCount.cs，
//! C# 为 BitmapManager partial）
//!
//! C# 标量路径按 u64 四路展开 popcount，SIMD 路径经 SSSE3/AVX2 查表 + 累加；
//! Rust 侧三实现统一以 `u64::count_ones`（硬件 POPCNT/CNT 指令）承接，批处理
//! 宽度保持与 C# 一致（8/16/32 字节），结果逐位一致。

use super::bitmap_manager::{normalize_bit_count_offsets, process_negative_offset, reverse};

/// 单字节内计位：仅统计 `[startBitOffset, endBitOffset)` 位区间（MSB 序）
///
/// libs/server/Resp/Bitmap/BitmapManagerBitCount.cs:BitIndexCount(byte,int,int)
#[inline]
pub(crate) fn bit_index_count(payload: u8, start_bit_offset: u32, end_bit_offset: u32) -> i64 {
  // 反转后从最低位起数（MSB 在前）
  let payload = reverse(payload);
  let left_bit_index = 1u16 << start_bit_offset;
  let right_bit_index = 1u16 << end_bit_offset;

  let mask = (right_bit_index - left_bit_index) as u8;
  (mask & payload).count_ones() as i64
}

/// 位区间计数：首末字节做部分位计数，中间字节不计（由驱动按整段处理）
///
/// C# 同文件 BitIndexCount 的 `(byte*, long, long)` 重载
fn bit_index_count_range(value: &[u8], start_offset: i64, end_offset: i64) -> i64 {
  let start_byte = start_offset / 8;
  let end_byte = end_offset / 8;

  let left_bit_index = (start_offset & 7) as u32;
  let right_bit_index = (end_offset & 7) as u32 + 1;

  if start_byte == end_byte {
    bit_index_count(value[start_byte as usize], left_bit_index, right_bit_index)
  } else {
    // 首字节自 leftBitIndex 起数到字节尾；末字节自字节头数到 rightBitIndex
    bit_index_count(value[start_byte as usize], left_bit_index, 8)
      + bit_index_count(value[end_byte as usize], 0, right_bit_index)
  }
}

/// BITCOUNT 主驱动：归一化区间后分字节/位两口径计数
///
/// libs/server/Resp/Bitmap/BitmapManagerBitCount.cs:BitCountDriver
pub fn bit_count_driver(
  mut start_offset: i64,
  mut end_offset: i64,
  offset_type: u8,
  value: &[u8],
  val_len: i64,
) -> i64 {
  let mut count = 0;

  (start_offset, end_offset) = normalize_bit_count_offsets(start_offset, end_offset, offset_type);

  if offset_type == 0x0 {
    // BYTE 口径
    if start_offset < 0 {
      start_offset = process_negative_offset(start_offset, val_len);
    }
    if end_offset < 0 {
      end_offset = process_negative_offset(end_offset, val_len);
    }
    if end_offset >= val_len {
      end_offset = val_len - 1;
    }

    if start_offset >= val_len {
      return 0;
    }

    if start_offset > end_offset {
      return 0;
    }
  } else {
    // BIT 口径
    let bit_len = val_len * 8;
    if bit_len == 0 {
      return 0;
    }

    if start_offset < 0 {
      start_offset = process_negative_offset(start_offset, bit_len);
    }
    if end_offset < 0 {
      end_offset = process_negative_offset(end_offset, bit_len);
    }

    if start_offset >= bit_len {
      return 0;
    }

    if start_offset > end_offset {
      return 0;
    }

    if end_offset >= bit_len {
      end_offset = bit_len - 1;
    }

    count += bit_index_count_range(value, start_offset, end_offset);

    // 跳过首末字节后按整段字节计数
    start_offset = (start_offset / 8) + 1;
    end_offset = (end_offset / 8) - 1;

    if start_offset >= end_offset {
      return count;
    }
  }

  let (start, end) = (start_offset as usize, end_offset as usize);
  if end - start < 128 {
    count += __scalar_popc(value, start, end);
  } else {
    count += __simd_popc_x256(value, start, end);
  }
  count
}

/// 标量人口计数：u64 四路展开 + 单 u64 + 尾部逐位拼装
///
/// libs/server/Resp/Bitmap/BitmapManagerBitCount.cs:__scalar_popc
pub fn __scalar_popc(bitmap: &[u8], start: usize, end: usize) -> i64 {
  let mut count: u64 = 0;
  let mut batch_size: usize = 8 * 4;
  let mut len = (end - start) + 1;
  let mut tail = len & (batch_size - 1);
  let mut curr = start;
  let mut vend = curr + (len - (len & tail));

  // popc_4x8：每次 4 个 u64
  while curr < vend {
    let v00 = u64_read(bitmap, curr).count_ones() as u64;
    let v01 = u64_read(bitmap, curr + 8).count_ones() as u64;
    let v02 = u64_read(bitmap, curr + 16).count_ones() as u64;
    let v03 = u64_read(bitmap, curr + 24).count_ones() as u64;
    count += (v00 + v01) + (v02 + v03);
    curr += batch_size;
  }

  // popc_1x8：每次 1 个 u64
  len = tail;
  batch_size = 8;
  tail = len & (batch_size - 1);
  vend = curr + (len - (len & tail));
  while curr < vend {
    count += u64_read(bitmap, curr).count_ones() as u64;
    curr += batch_size;
  }

  // 尾部：按剩余字节数逐位拼进 u64（低位对齐）
  count += popc_tail(bitmap, curr, tail) as u64;

  count as i64
}

/// 16 字节宽度批处理人口计数（C# SSSE3 查表路径的等价承接）
///
/// libs/server/Resp/Bitmap/BitmapManagerBitCount.cs:__simd_popcX128
pub fn __simd_popc_x128(bitmap: &[u8], start: usize, end: usize) -> i64 {
  let mut count: u64 = 0;
  let mut batch_size: usize = 8 * 16;
  let mut len = (end - start) + 1;
  let mut tail = len & (batch_size - 1);
  let mut curr = start;
  let mut vend = curr + (len - (len & tail));

  // popc_8x16：每轮 8×16 字节
  while curr < vend {
    count += popc_bytes(bitmap, curr, 8 * 16);
    curr += batch_size;
  }
  if tail == 0 {
    return count as i64;
  }

  // popc_1x16：每次 16 字节
  len = tail;
  batch_size = 16;
  tail = len & (batch_size - 1);
  vend = curr + (len - (len & tail));
  while curr < vend {
    count += popc_bytes(bitmap, curr, 16);
    curr += batch_size;
  }
  if tail == 0 {
    return count as i64;
  }

  // 降级回 u64 标量路径收尾
  len = tail;
  batch_size = 4 * 8;
  tail = len & (batch_size - 1);
  vend = curr + (len - (len & tail));
  while curr < vend {
    count += u64_read(bitmap, curr).count_ones() as u64
      + u64_read(bitmap, curr + 8).count_ones() as u64
      + u64_read(bitmap, curr + 16).count_ones() as u64
      + u64_read(bitmap, curr + 24).count_ones() as u64;
    curr += batch_size;
  }
  if tail == 0 {
    return count as i64;
  }

  len = tail;
  batch_size = 8;
  tail = len & (batch_size - 1);
  vend = curr + (len - (len & tail));
  while curr < vend {
    count += u64_read(bitmap, curr).count_ones() as u64;
    curr += 8;
  }
  if tail == 0 {
    return count as i64;
  }

  count += popc_tail(bitmap, curr, tail) as u64;
  count as i64
}

/// 32 字节宽度批处理人口计数（C# AVX2 查表路径的等价承接）
///
/// libs/server/Resp/Bitmap/BitmapManagerBitCount.cs:__simd_popcX256
pub fn __simd_popc_x256(bitmap: &[u8], start: usize, end: usize) -> i64 {
  let mut count: u64 = 0;
  let mut batch_size: usize = 8 * 32;
  let mut len = (end - start) + 1;
  let mut tail = len & (batch_size - 1);
  let mut curr = start;
  let mut vend = curr + (len - (len & tail));

  // popc_8x32：每轮 8×32 字节
  while curr < vend {
    count += popc_bytes(bitmap, curr, 8 * 32);
    curr += batch_size;
  }
  if tail == 0 {
    return count as i64;
  }

  // popc_1x32：每次 32 字节
  len = tail;
  batch_size = 32;
  tail = len & (batch_size - 1);
  vend = curr + (len - (len & tail));
  while curr < vend {
    count += popc_bytes(bitmap, curr, 32);
    curr += batch_size;
  }
  if tail == 0 {
    return count as i64;
  }

  // 降级回 u64 标量路径收尾
  len = tail;
  batch_size = 4 * 8;
  tail = len & (batch_size - 1);
  vend = curr + (len - (len & tail));
  while curr < vend {
    count += u64_read(bitmap, curr).count_ones() as u64
      + u64_read(bitmap, curr + 8).count_ones() as u64
      + u64_read(bitmap, curr + 16).count_ones() as u64
      + u64_read(bitmap, curr + 24).count_ones() as u64;
    curr += batch_size;
  }
  if tail == 0 {
    return count as i64;
  }

  len = tail;
  batch_size = 8;
  tail = len & (batch_size - 1);
  vend = curr + (len - (len & tail));
  while curr < vend {
    count += u64_read(bitmap, curr).count_ones() as u64;
    curr += 8;
  }
  if tail == 0 {
    return count as i64;
  }

  count += popc_tail(bitmap, curr, tail) as u64;
  count as i64
}

/// 非对齐读取 `bitmap[idx..idx+8]` 的小端 u64（C# `*(ulong*)` 语义；
/// 调用方保证区间完整）
#[inline]
fn u64_read(bitmap: &[u8], idx: usize) -> u64 {
  u64::from_le_bytes(bitmap[idx..idx + 8].try_into().unwrap())
}

/// 计数 `bitmap[idx..idx+width]` 的置位数（width = 16/32，SIMD 宽度等效）
#[inline]
fn popc_bytes(bitmap: &[u8], idx: usize, width: usize) -> u64 {
  bitmap[idx..idx + width]
    .as_chunks::<8>()
    .0
    .iter()
    .map(|c| u64::from_le_bytes(*c).count_ones() as u64)
    .sum()
}

/// 尾部 1..=7 字节按 C# 位序拼装后计数
#[inline]
fn popc_tail(bitmap: &[u8], idx: usize, tail: usize) -> u32 {
  let mut tt: u64 = 0;
  if tail >= 7 {
    tt |= u64::from(bitmap[idx + 6]) << 48;
  }
  if tail >= 6 {
    tt |= u64::from(bitmap[idx + 5]) << 40;
  }
  if tail >= 5 {
    tt |= u64::from(bitmap[idx + 4]) << 32;
  }
  if tail >= 4 {
    tt |= u64::from(bitmap[idx + 3]) << 24;
  }
  if tail >= 3 {
    tt |= u64::from(bitmap[idx + 2]) << 16;
  }
  if tail >= 2 {
    tt |= u64::from(bitmap[idx + 1]) << 8;
  }
  if tail >= 1 {
    tt |= u64::from(bitmap[idx]);
  }
  tt.count_ones()
}

#[cfg(test)]
mod tests {
  use super::{__scalar_popc, __simd_popc_x128, __simd_popc_x256, bit_count_driver};

  /// 伪随机缓冲（固定种子，保证测试可复现）
  fn pseudo_random(len: usize) -> Vec<u8> {
    let mut x: u64 = 0x2545_F491_4F6C_DD1D;
    (0..len)
      .map(|_| {
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        x as u8
      })
      .collect()
  }

  fn naive(bitmap: &[u8], start: usize, end: usize) -> i64 {
    bitmap[start..=end]
      .iter()
      .map(|b| b.count_ones() as i64)
      .sum()
  }

  #[test]
  fn popc_variants_match_naive() {
    let bitmap = pseudo_random(1024);
    for &(s, e) in &[
      (0, 0),
      (0, 7),
      (0, 8),
      (5, 12),
      (0, 31),
      (3, 130),
      (0, 255),
      (1, 256),
      (0, 511),
      (7, 1023),
      (13, 777),
    ] {
      let want = naive(&bitmap, s, e);
      assert_eq!(__scalar_popc(&bitmap, s, e), want, "scalar [{s},{e}]");
      assert_eq!(__simd_popc_x128(&bitmap, s, e), want, "x128 [{s},{e}]");
      assert_eq!(__simd_popc_x256(&bitmap, s, e), want, "x256 [{s},{e}]");
    }
  }

  #[test]
  fn driver_byte_and_bit_modes() {
    // 0xA5 = 1010_0101，0x0F = 0000_1111
    let val = [0xA5u8, 0x0F];

    // BYTE 口径：全量 8 个；[0,0] 4 个；负区间自尾部折算
    assert_eq!(bit_count_driver(0, -1, 0x0, &val, 2), 8);
    assert_eq!(bit_count_driver(0, 0, 0x0, &val, 2), 4);
    assert_eq!(bit_count_driver(-1, -1, 0x0, &val, 2), 4);
    assert_eq!(bit_count_driver(2, 100, 0x0, &val, 2), 0);

    // BIT 口径：[0,7] 4 个；[1,2] bit1=0,bit2=1 → 1；区间交集为空 → 0
    assert_eq!(bit_count_driver(0, 7, 0x1, &val, 2), 4);
    assert_eq!(bit_count_driver(1, 2, 0x1, &val, 2), 1);
    assert_eq!(bit_count_driver(3, 4, 0x1, &val, 2), 0);
    // 跨字节：bit8..15 = 0x0F = 4
    assert_eq!(bit_count_driver(8, 15, 0x1, &val, 2), 4);
    // 起点越界 / 空值
    assert_eq!(bit_count_driver(16, 23, 0x1, &val, 2), 0);
    assert_eq!(bit_count_driver(0, -1, 0x1, &val, 0), 0);
  }
}
