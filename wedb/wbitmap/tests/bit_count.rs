#![recursion_limit = "512"] // 泛型设备实例化下 async 状态机嵌套深（R27 CI 实证）
use wbitmap::{__scalar_popc, __simd_popc_x256, bit_count_driver};

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
  // 跨 3 字节（包含中间整字节）：bit 7 (1) + byte 1 (8) + bit 16 (1) = 10
  let three_bytes = [0x01u8, 0xFF, 0x80];
  assert_eq!(bit_count_driver(7, 16, 0x1, &three_bytes, 3), 10);
  // 起点越界 / 空值
  assert_eq!(bit_count_driver(16, 23, 0x1, &val, 2), 0);
  assert_eq!(bit_count_driver(0, -1, 0x1, &val, 0), 0);
}
