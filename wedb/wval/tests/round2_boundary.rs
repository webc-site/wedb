use std::{
  cmp::Ordering,
  mem::{align_of, size_of},
  ops::Deref,
};

use aok::{OK, Void};
use log::info;
use wval::{
  CompactHash, CompactHashCodec, CompactMetaValue, GarnetObjectType, KeyTag, META_VALUE_SIZE,
  MetaValue, StorageEncoding, SubKeyBuf,
};

#[ctor::ctor(unsafe)]
fn _log_init() {
  log_init::init();
}

// ============================================================================
// 3. MetaValue 与 CompactMetaValue 全局大端序测试
// ============================================================================
#[test]
fn test_round2_meta_value_and_compact_meta_value() -> Void {
  info!("开始测试: MetaValue 与 CompactMetaValue 全局大端序布局");

  // 1. MetaValue (32B)
  assert_eq!(size_of::<MetaValue>(), 32);
  assert_eq!(size_of::<MetaValue>(), META_VALUE_SIZE);
  assert_eq!(align_of::<MetaValue>(), 8);

  let meta = MetaValue::new(
    0x0102_0304_0506_0708,
    GarnetObjectType::SortedSet,
    0x1122_3344_5566_7788,
    500,
  )
  .with_encoding(StorageEncoding::Flattened);
  assert_eq!(meta.encoding(), StorageEncoding::Flattened);

  let raw_bytes = meta.to_bytes();
  assert_eq!(raw_bytes.len(), 32);
  // 逐字节验证大端序排布
  assert_eq!(
    &raw_bytes[0..8],
    &[0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08]
  );
  assert_eq!(raw_bytes[8], GarnetObjectType::SortedSet.as_u8());
  assert_eq!(raw_bytes[9], StorageEncoding::Flattened.as_u8());
  assert_eq!(
    &raw_bytes[16..24],
    &[0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77, 0x88]
  );
  assert_eq!(
    u64::from_be_bytes(raw_bytes[24..32].try_into().unwrap()),
    500
  );

  // 2. CompactMetaValue (16B)
  let cmeta = CompactMetaValue::new(
    GarnetObjectType::Hash,
    StorageEncoding::Compact,
    42,
    1_800_000_000_000,
  );
  let cbytes = cmeta.to_bytes();
  assert_eq!(cbytes.len(), 16);
  assert_eq!(cbytes[0], GarnetObjectType::Hash.as_u8());
  assert_eq!(cbytes[1], StorageEncoding::Compact.as_u8());
  assert_eq!(u32::from_be_bytes(cbytes[4..8].try_into().unwrap()), 42);
  assert_eq!(
    u64::from_be_bytes(cbytes[8..16].try_into().unwrap()),
    1_800_000_000_000
  );

  info!("MetaValue 与 CompactMetaValue 全局大端序测试通过");
  OK
}

// ============================================================================
// 4. SubKeyBuf 栈优先与堆回退契约测试
// ============================================================================
#[test]
fn test_round2_subkey_buf_stack_heap_contracts() -> Void {
  info!("开始测试: SubKeyBuf 栈优先与堆回退契约");

  // 1. SubKeyBuf 精确边界 (SUBKEY_STACK_CAP = 128)
  let payload_111 = vec![b'x'; 111]; // 17 header + 111 = 128 bytes (恰好满栈)
  let stack_buf = SubKeyBuf::encode(KeyTag::Hash, 1, 1, &payload_111)?;
  assert!(stack_buf.is_stack());
  assert!(!stack_buf.is_heap());
  assert_eq!(stack_buf.len(), 128);

  let payload_112 = vec![b'x'; 112]; // 17 header + 112 = 129 bytes (回退至堆)
  let heap_buf = SubKeyBuf::encode(KeyTag::Hash, 1, 1, &payload_112)?;
  assert!(!heap_buf.is_stack());
  assert!(heap_buf.is_heap());
  assert_eq!(heap_buf.len(), 129);

  // 2. Deref, AsRef, Borrow, Eq, Ord 跨变体一致性
  let slice_128 = stack_buf.as_slice();
  assert_eq!(stack_buf.deref(), slice_128);
  assert_eq!(stack_buf.as_ref(), slice_128);
  assert_eq!(stack_buf, slice_128);

  let heap_from_slice = SubKeyBuf::from(slice_128);
  assert_eq!(stack_buf, heap_from_slice);
  assert_eq!(stack_buf.cmp(&heap_from_slice), Ordering::Equal);

  info!("SubKeyBuf 栈优先与堆回退契约测试通过");
  OK
}

// ============================================================================

// ============================================================================
// 5. CompactHash 重复键批量编码与原地淘汰测试
// ============================================================================
#[test]
fn test_round2_compact_hash_duplicate_keys_and_large_scale() -> Void {
  info!("开始测试: CompactHash 重复键批量编码与原地淘汰");

  // 1. 批量编码包含大量重复键
  let mut entries: Vec<(&[u8], &[u8], Option<i64>)> = Vec::new();
  for i in 0..500 {
    let field = match i % 5 {
      0 => b"f0".as_slice(),
      1 => b"f1".as_slice(),
      2 => b"f2".as_slice(),
      3 => b"f3".as_slice(),
      _ => b"f4".as_slice(),
    };
    entries.push((field, b"val", None));
  }

  let encoded = CompactHashCodec::encode(entries)?;
  // 500 次写入仅生成 5 个独立字段
  assert_eq!(CompactHashCodec::count(&encoded)?, 5);

  let mut hash = CompactHash::from_vec(encoded)?;
  assert_eq!(hash.len(), 5);
  assert_eq!(hash.find_field(b"f0"), Some(b"val".as_slice()));
  assert_eq!(hash.find_field(b"f4"), Some(b"val".as_slice()));

  // 2. 原地 purge_expired 压缩测试（边界取严格小于：exp == now 视为未过期，
  //    对标 C# HashObject.IsExpired 的 expiration < now 口径）
  let now = 1_000_000_i64;
  hash.set_field(b"f0", b"expired", Some(now - 10))?;
  hash.set_field(b"f1", b"alive", Some(now + 100))?;
  hash.set_field(b"f2", b"boundary_alive", Some(now))?;
  hash.set_field(b"f3", b"no_expire", None)?;

  let purged = hash.purge_expired(now)?;
  assert_eq!(purged, 1); // 仅 f0 过期
  assert_eq!(hash.len(), 4);
  assert_eq!(hash.find_field(b"f0"), None);
  assert_eq!(hash.find_field(b"f1"), Some(b"alive".as_slice()));
  assert_eq!(hash.find_field(b"f2"), Some(b"boundary_alive".as_slice()));
  assert_eq!(hash.find_field(b"f3"), Some(b"no_expire".as_slice()));

  info!("CompactHash 重复键批量编码与原地淘汰测试通过");
  OK
}
