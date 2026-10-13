#![recursion_limit = "512"] // 泛型设备实例化下 async 状态机嵌套深（R27 CI 实证）
use wbitmap::{bit_pos_driver, process_negative_offset};

/// 按口径归一化后的朴素逐位搜索（BYTE：start/end 为字节界；BIT：为位界；
/// end_given 与 Redis 形对齐——找 0 无显式 end 时无界零尾外延回位长）
fn naive(input: &[u8], start: i64, end: i64, bit: u8, offset_type: u8, end_given: bool) -> i64 {
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
    // Redis 无界零尾：找 0 且未显式给 end → 回首个不属于串的位（位长）
    if !bit && !end_given {
      return len * 8;
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
    // BIT 形 parse 文法保证 end_given 恒真，无匹配维持 -1（与驱动等形）
    -1
  }
}

#[test]
fn byte_search_basic() {
  // 0x00 0x08：首个 1 在位 12；首个 0 在位 0
  let val = [0x00u8, 0x08];
  assert_eq!(bit_pos_driver(&val, 2, 0, -1, 1, 0x0, true), 12);
  assert_eq!(bit_pos_driver(&val, 2, 0, -1, 0, 0x0, true), 0);
  // BYTE 口径 end 为字节界：仅查字节 0 → 无 1
  assert_eq!(bit_pos_driver(&val, 2, 0, 0, 1, 0x0, true), -1);
  assert_eq!(bit_pos_driver(&val, 2, 1, 1, 1, 0x0, true), 12);
  // 全 1 找 0：无显式 end → 无界零尾外延回位长（Redis 8.10.1 实测 24）
  let ones = [0xffu8; 3];
  assert_eq!(bit_pos_driver(&ones, 3, 0, -1, 0, 0x0, false), 24);
  // 对照锁：显式 end（含 end=-1）闭合区间 → -1
  assert_eq!(bit_pos_driver(&ones, 3, 0, -1, 0, 0x0, true), -1);
  assert_eq!(bit_pos_driver(&ones, 3, 0, 0, 1, 0x0, true), 0);
  // 负区间自尾部折算：-1 → 字节 1 → 位 12
  assert_eq!(bit_pos_driver(&val, 2, -1, -1, 1, 0x0, true), 12);
  // 负偏移越界或等于 -len 时钳制为 0（字节 0 到 字节 1）→ 位 12
  assert_eq!(bit_pos_driver(&val, 2, -2, -1, 1, 0x0, true), 12);
  // 起点越界 / 空区间
  assert_eq!(bit_pos_driver(&val, 2, 2, 9, 1, 0x0, true), -1);
  assert_eq!(bit_pos_driver(&val, 2, 9, 8, 1, 0x0, true), -1);
  // 空值：start >= len → -1
  assert_eq!(bit_pos_driver(&val, 0, 0, -1, 1, 0x0, true), -1);
}

#[test]
fn bit_search_basic() {
  let val = [0x00u8, 0x08];
  // BIT 口径 end 为位界：区间 [0,11] 不含位 12
  assert_eq!(bit_pos_driver(&val, 2, 0, 15, 1, 0x1, true), 12);
  assert_eq!(bit_pos_driver(&val, 2, 0, 15, 0, 0x1, true), 0);
  assert_eq!(bit_pos_driver(&val, 2, 0, 11, 1, 0x1, true), -1);
  // 区间钳制：end 超位长截断
  assert_eq!(bit_pos_driver(&val, 2, 0, 999, 1, 0x1, true), 12);
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
            // end_given 维度与 Redis 形对齐：false 仅在 BYTE 找 0 走完区间
            // 时外延回位长；BIT 形两值同形（parse 文法保证恒真）
            for end_given in [false, true] {
              assert_eq!(
                bit_pos_driver(&buf, byte_len, start, end, bit, offset_type, end_given),
                naive(&buf, start, end, bit, offset_type, end_given),
                "len={len} start={start} end={end} bit={bit} ot={offset_type} eg={end_given}"
              );
            }
          }
        }
      }
    }
  }
}
