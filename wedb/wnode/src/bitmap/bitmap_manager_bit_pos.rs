//! BITPOS 位查找驱动（对标 libs/server/Resp/Bitmap/BitmapManagerBitPos.cs，
//! C# 为 BitmapManager partial）
//!
//! 搜索思路与 C# 一致：把区间负载移到 u64 高位后数前导零；全段命中无效
//! 负载（全 0 / 全 1）则整段跳过。

use super::bitmap_manager::process_negative_offset;

/// BITPOS 主驱动：BYTE / BIT 两口径归一化区间后搜索
///
/// libs/server/Resp/Bitmap/BitmapManagerBitPos.cs:BitPosDriver
pub fn bit_pos_driver(
  input: &[u8],
  input_len: i64,
  mut start_offset: i64,
  mut end_offset: i64,
  search_for: u8,
  offset_type: u8,
) -> i64 {
  if offset_type == 0x0 {
    if start_offset < 0 {
      start_offset = process_negative_offset(start_offset, input_len);
    }
    if end_offset < 0 {
      end_offset = process_negative_offset(end_offset, input_len);
    }

    if start_offset >= input_len {
      return -1;
    }

    if start_offset > end_offset {
      return -1;
    }

    if end_offset >= input_len {
      end_offset = input_len - 1;
    }
    // BYTE 口径
    bit_pos_byte_search(input, start_offset, end_offset, search_for)
  } else {
    let bit_len = input_len * 8;
    if start_offset < 0 {
      start_offset = process_negative_offset(start_offset, bit_len);
    }
    if end_offset < 0 {
      end_offset = process_negative_offset(end_offset, bit_len);
    }

    let start_byte_index = start_offset >> 3;
    let end_byte_index = end_offset >> 3;

    if start_byte_index >= input_len {
      return -1;
    }

    if start_byte_index > end_byte_index {
      return -1;
    }

    if end_byte_index >= input_len {
      end_offset = bit_len - 1;
    }

    // BIT 口径
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
fn bit_pos_byte_search(input: &[u8], start_offset: i64, end_offset: i64, search_for: u8) -> i64 {
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

  // 未命中返回 -1
  -1
}

#[cfg(test)]
mod tests {
  use super::{bit_pos_driver, process_negative_offset};

  /// 按口径归一化后的朴素逐位搜索（BYTE：start/end 为字节界；BIT：为位界）
  fn naive(input: &[u8], start: i64, end: i64, bit: u8, offset_type: u8) -> i64 {
    let bit = bit == 1;
    if offset_type == 0x0 {
      // BYTE：字节区间，末字节含全部 8 位（C# 不裁剪末字节尾部）
      let len = input.len() as i64;
      let mut s = start;
      let mut e = end;
      if s < 0 {
        s = process_negative_offset(s, len);
      }
      if e < 0 {
        e = process_negative_offset(e, len);
      }
      if s >= len || s > e {
        return -1;
      }
      if e >= len {
        e = len - 1;
      }
      for b in s..=e {
        for k in 0..8 {
          if ((input[b as usize] >> (7 - k)) & 1) == u8::from(bit) {
            return b * 8 + k;
          }
        }
      }
      -1
    } else {
      // BIT：位区间
      let bit_len = input.len() as i64 * 8;
      let mut s = start;
      let mut e = end;
      if s < 0 {
        s = process_negative_offset(s, bit_len);
      }
      if e < 0 {
        e = process_negative_offset(e, bit_len);
      }
      let start_byte = s >> 3;
      let end_byte = e >> 3;
      if start_byte >= input.len() as i64 || start_byte > end_byte {
        return -1;
      }
      e = e.min(bit_len - 1);
      for i in s..=e {
        if ((input[(i / 8) as usize] >> (7 - (i % 8))) & 1) == u8::from(bit) {
          return i;
        }
      }
      -1
    }
  }

  #[test]
  fn byte_search_basic() {
    // 0x00 0x08：首个 1 在位 12；首个 0 在位 0
    let val = [0x00u8, 0x08];
    assert_eq!(bit_pos_driver(&val, 2, 0, -1, 1, 0x0), 12);
    assert_eq!(bit_pos_driver(&val, 2, 0, -1, 0, 0x0), 0);
    // BYTE 口径 end 为字节界：仅查字节 0 → 无 1
    assert_eq!(bit_pos_driver(&val, 2, 0, 0, 1, 0x0), -1);
    assert_eq!(bit_pos_driver(&val, 2, 1, 1, 1, 0x0), 12);
    // 全 1 找 0
    let ones = [0xffu8; 3];
    assert_eq!(bit_pos_driver(&ones, 3, 0, -1, 0, 0x0), -1);
    assert_eq!(bit_pos_driver(&ones, 3, 0, 0, 1, 0x0), 0);
    // 负区间自尾部折算：-1 → 字节 1 → 位 12
    assert_eq!(bit_pos_driver(&val, 2, -1, -1, 1, 0x0), 12);
    // 负偏移越界或等于 -len 时钳制为 0（字节 0 到 字节 1）→ 位 12
    assert_eq!(bit_pos_driver(&val, 2, -2, -1, 1, 0x0), 12);
    // 起点越界 / 空区间
    assert_eq!(bit_pos_driver(&val, 2, 2, 9, 1, 0x0), -1);
    assert_eq!(bit_pos_driver(&val, 2, 9, 8, 1, 0x0), -1);
    // 空值：start >= len → -1
    assert_eq!(bit_pos_driver(&val, 0, 0, -1, 1, 0x0), -1);
  }

  #[test]
  fn bit_search_basic() {
    let val = [0x00u8, 0x08];
    // BIT 口径 end 为位界：区间 [0,11] 不含位 12
    assert_eq!(bit_pos_driver(&val, 2, 0, 15, 1, 0x1), 12);
    assert_eq!(bit_pos_driver(&val, 2, 0, 15, 0, 0x1), 0);
    assert_eq!(bit_pos_driver(&val, 2, 0, 11, 1, 0x1), -1);
    // 区间钳制：end 超位长截断
    assert_eq!(bit_pos_driver(&val, 2, 0, 999, 1, 0x1), 12);
  }

  #[test]
  fn matches_naive_on_random_buffers() {
    // 覆盖 8/4/1 字节各分段与尾部裁剪路径
    let mut x: u64 = 0x9E37_79B9_7F4A_7C15;
    for len in [1usize, 3, 4, 5, 8, 9, 16, 17, 33] {
      let buf: Vec<u8> = (0..len)
        .map(|_| {
          x ^= x << 13;
          x ^= x >> 7;
          x ^= x << 17;
          x as u8
        })
        .collect();
      let byte_len = buf.len() as i64;
      let starts = [-1i64, 0, 1, 5, byte_len / 2, byte_len - 1, -(byte_len / 2)];
      let ends = [-1i64, 0, 7, byte_len / 3, byte_len - 1];
      for start in starts {
        for end in ends {
          for bit in [0u8, 1] {
            for offset_type in [0x0u8, 0x1] {
              assert_eq!(
                bit_pos_driver(&buf, byte_len, start, end, bit, offset_type),
                naive(&buf, start, end, bit, offset_type),
                "len={len} start={start} end={end} bit={bit} ot={offset_type}"
              );
            }
          }
        }
      }
    }
  }
}
