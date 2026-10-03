//! RoaringBitmapObject 位图操作测试（bit_pos rank 二分对拍与边界）
//!
//! 朴素神谕 vs rank 二分对拍与边界测试。

use roaring::RoaringBitmap;
use wext_roaring::roaring_bitmap_object::RoaringBitmapObject;

/// 朴素逐位神谕：原逐元素迭代语义，作稠密对拍基准
fn oracle_bit_pos(bitmap: &RoaringBitmap, bit: bool, from: u32) -> i64 {
  if bit {
    bitmap.range(from..).next().map_or(-1, |pos| pos as i64)
  } else {
    let mut current = from;
    for set_bit in bitmap.range(from..) {
      if set_bit > current {
        return current as i64;
      }
      if current == u32::MAX {
        return -1;
      }
      current += 1;
    }
    current as i64
  }
}

/// xorshift64 测试内伪随机，零新增依赖
struct Lcg(u64);

impl Lcg {
  fn draw(&mut self) -> u64 {
    let mut x = self.0;
    x ^= x << 13;
    x ^= x >> 7;
    x ^= x << 17;
    self.0 = x;
    x
  }
}

#[test]
fn full_span_no_hole_is_minus_one() {
  // 尾段 [u32::MAX-999, u32::MAX] 全满：[from, u32::MAX] 无洞回 -1
  let mut obj = RoaringBitmapObject::create();
  obj.bitmap.insert_range((u32::MAX - 999)..=(u32::MAX));
  assert_eq!(obj.bit_pos(false, u32::MAX - 999), -1);
  assert_eq!(obj.bit_pos(false, u32::MAX - 500), -1);
  // 单点全满
  let mut one = RoaringBitmapObject::create();
  one.set_bit(u32::MAX, true);
  assert_eq!(one.bit_pos(false, u32::MAX), -1);
}

#[test]
fn leading_dense_then_hole() {
  let mut obj = RoaringBitmapObject::create();
  for i in 0..=10u32 {
    obj.set_bit(i, true);
  }
  assert_eq!(obj.bit_pos(false, 0), 11);
  assert_eq!(obj.bit_pos(false, 3), 11);
  assert_eq!(obj.bit_pos(false, 11), 11);
}

#[test]
fn isolated_set_bit_before_from() {
  let mut obj = RoaringBitmapObject::create();
  obj.set_bit(5, true);
  assert_eq!(obj.bit_pos(false, 0), 0);
  assert_eq!(obj.bit_pos(false, 5), 6);
  assert_eq!(obj.bit_pos(false, 6), 6);
}

#[test]
fn u32_max_boundary_both_states() {
  // 位图含 u32::MAX
  let mut with = RoaringBitmapObject::create();
  with.set_bit(u32::MAX, true);
  assert_eq!(with.bit_pos(false, 0), 0);
  assert_eq!(with.bit_pos(true, u32::MAX), u32::MAX as i64);

  // 位图不含 u32::MAX：from 已置位但 u32::MAX 空洞兜底
  let mut without = RoaringBitmapObject::create();
  without.set_bit(u32::MAX - 1, true);
  assert_eq!(without.bit_pos(false, u32::MAX - 1), u32::MAX as i64);
  // from == u32::MAX 且未置位：洞即自身
  assert_eq!(without.bit_pos(false, u32::MAX), u32::MAX as i64);
  assert_eq!(without.bit_pos(true, u32::MAX), -1);
}

#[test]
fn empty_bitmap() {
  let obj = RoaringBitmapObject::create();
  assert_eq!(obj.bit_pos(false, 0), 0);
  assert_eq!(obj.bit_pos(false, 123), 123);
  assert_eq!(obj.bit_pos(false, u32::MAX), u32::MAX as i64);
  assert_eq!(obj.bit_pos(true, 0), -1);
}

#[test]
fn run_tail_full() {
  let mut obj = RoaringBitmapObject::create();
  for i in 0..=100u32 {
    obj.set_bit(i, true);
  }
  assert_eq!(obj.bit_pos(false, 0), 101);
  assert_eq!(obj.bit_pos(false, 50), 101);
  assert_eq!(obj.bit_pos(false, 100), 101);
}

#[test]
fn from_on_and_after_hole() {
  let mut obj = RoaringBitmapObject::create();
  for i in 0..=10u32 {
    obj.set_bit(i, true);
  }
  for i in 20..=30u32 {
    obj.set_bit(i, true);
  }
  // 洞 [11, 19]：from 恰在洞上 / 洞中 / 洞后已置位
  assert_eq!(obj.bit_pos(false, 11), 11);
  assert_eq!(obj.bit_pos(false, 15), 15);
  assert_eq!(obj.bit_pos(false, 19), 19);
  assert_eq!(obj.bit_pos(false, 20), 31);
  assert_eq!(obj.bit_pos(false, 25), 31);
}

/// 稠密对拍：朴素神谕 vs rank 二分，随机位图多 seed，双 bit 全域 from
#[test]
fn dense_randomized_oracle_crosscheck() {
  let mut rng = Lcg(0x9E37_79B9_7F4A_7C15);
  for seed_round in 0..16 {
    let mut obj = RoaringBitmapObject::create();
    // 小域稠密（跨容器边界 65536 不覆盖，另补大域稀疏）
    for _ in 0..1500 {
      let v = (rng.draw() % 2000) as u32;
      obj.set_bit(v, true);
    }
    // 大域稀疏：含 u32::MAX 附近点，覆盖高位容器与 u32::MAX 边界
    for _ in 0..40 {
      let v = u32::MAX - (rng.draw() % 3000) as u32;
      obj.set_bit(v, true);
    }
    for _ in 0..3000 {
      let from = rng.draw() as u32;
      assert_eq!(
        obj.bit_pos(true, from),
        oracle_bit_pos(&obj.bitmap, true, from),
        "bit=true round {seed_round} from {from}"
      );
      assert_eq!(
        obj.bit_pos(false, from),
        oracle_bit_pos(&obj.bitmap, false, from),
        "bit=false round {seed_round} from {from}"
      );
    }
    // from 落在已置位/边界附近，覆盖 base 特判路径
    for _ in 0..300 {
      let from = u32::MAX - (rng.draw() % 3001) as u32;
      assert_eq!(
        obj.bit_pos(false, from),
        oracle_bit_pos(&obj.bitmap, false, from),
        "tail round {seed_round} from {from}"
      );
    }
  }
}
