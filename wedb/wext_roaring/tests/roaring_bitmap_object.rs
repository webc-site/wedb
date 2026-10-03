//! RoaringBitmapObject 序列化与堆估算测试（自 src/roaring_bitmap_object.rs 外迁）

use wext_roaring::roaring_bitmap_object::{
  BITMAP_BASE, BITMAP_BODY, CONTAINER_HEADER, OBJECT_OVERHEAD, PER_CONTAINER, RoaringBitmapObject,
  heap_estimate,
};

#[test]
fn serialize_round_trip() {
  let mut obj = RoaringBitmapObject::create();
  assert!(obj.is_empty());
  obj.set_bit(1, true);
  obj.set_bit(1000, true);
  assert!(!obj.is_empty());

  let mut buf = Vec::new();
  obj.serialize_object(&mut buf).unwrap();
  let back = RoaringBitmapObject::deserialize(&mut &buf[..]).unwrap();
  assert_eq!(back.bit_count(), 2);
  assert!(!back.is_empty());
  assert!(back.get_bit(1) && back.get_bit(1000));
}

#[test]
fn corrupt_stream_is_error_not_panic() {
  assert!(RoaringBitmapObject::deserialize(&mut &b"garbage"[..]).is_err());
}

#[test]
fn bit_pos_boundaries() {
  let mut obj = RoaringBitmapObject::create();
  obj.set_bit(5, true);
  // 已置位：命中自身
  assert_eq!(obj.bit_pos(true, 5), 5);
  // 第一个 >= 6 的置位位不存在
  assert_eq!(obj.bit_pos(true, 6), -1);
  // 0..5 均未置位，首个未置位即 0
  assert_eq!(obj.bit_pos(false, 0), 0);
  // 5 已置位，首个未置位是 6
  assert_eq!(obj.bit_pos(false, 5), 6);
}

/// 空位图仅计对象壳 32 + 位图基座 32；稀疏双位走数组容器（头 16 + card*2B）
#[test]
fn heap_estimate_empty_and_sparse() {
  let obj = RoaringBitmapObject::create();
  let mut buf = Vec::new();
  obj.serialize_object(&mut buf).unwrap();
  assert_eq!(heap_estimate(&buf), OBJECT_OVERHEAD + BITMAP_BASE);

  let mut obj = RoaringBitmapObject::create();
  obj.set_bit(1, true);
  obj.set_bit(1000, true);
  let mut buf = Vec::new();
  obj.serialize_object(&mut buf).unwrap();
  assert_eq!(
    heap_estimate(&buf),
    OBJECT_OVERHEAD + BITMAP_BASE + PER_CONTAINER + CONTAINER_HEADER + 2 * 2
  );
}

/// 5000 连续位超数组容量上限：容器走位图存储（头 16 + 8KB body）
#[test]
fn heap_estimate_dense_bitmap_container() {
  let mut obj = RoaringBitmapObject::create();
  for i in 0..5000u32 {
    obj.set_bit(i, true);
  }
  let mut buf = Vec::new();
  obj.serialize_object(&mut buf).unwrap();
  assert_eq!(
    heap_estimate(&buf),
    OBJECT_OVERHEAD + BITMAP_BASE + PER_CONTAINER + CONTAINER_HEADER + BITMAP_BODY as i64
  );
}

/// 未知 cookie、空串、截断 body 一律 0 计（不 panic、不虚增）
#[test]
fn heap_estimate_corrupt_is_zero() {
  assert_eq!(heap_estimate(&[]), 0);
  assert_eq!(heap_estimate(&[0, 0, 0, 0]), 0);
  let mut obj = RoaringBitmapObject::create();
  obj.set_bit(1, true);
  let mut buf = Vec::new();
  obj.serialize_object(&mut buf).unwrap();
  buf.truncate(buf.len() - 1);
  assert_eq!(heap_estimate(&buf), 0);
}
