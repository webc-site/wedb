use std::mem::{align_of, size_of};

use aok::{OK, Void};
use log::info;
use wval::{Error, GarnetObjectType, META_VALUE_SIZE, MetaValue, Result, StorageEncoding};

#[test]
fn test_meta_value_memory_layout() -> Void {
  info!("测试 MetaValue 内存布局与对齐");

  // 严格断言定长 32 字节（8 字节自然对齐，4×64 位字容量）
  assert_eq!(size_of::<MetaValue>(), 32);
  assert_eq!(size_of::<MetaValue>(), META_VALUE_SIZE);
  // 严格断言 8 字节对齐（保证所有 u64 字段天然对齐，具备单总线周期原子访问能力）
  assert_eq!(align_of::<MetaValue>(), 8);

  let meta = MetaValue::new(1001, GarnetObjectType::Hash, 100);
  assert_eq!(meta.key_id, 1001);
  assert_eq!(meta.collection_type, GarnetObjectType::Hash);
  assert_eq!(meta.size, 100);
  assert_eq!(meta.next_expiry, i64::MAX);

  OK
}

#[test]
fn test_meta_value_roundtrip_and_serialization() -> Void {
  info!("测试 MetaValue 二进制序列化与往返恢复");

  let original = MetaValue::new(
    0x0123_4567_89ab_cdef,
    GarnetObjectType::SortedSet,
    10_000_000,
  );

  let bytes = original.to_bytes();
  assert_eq!(bytes.len(), 32);

  // 验证大端序字节排布
  assert_eq!(&bytes[0..8], &0x0123_4567_89ab_cdef_u64.to_be_bytes());
  assert_eq!(bytes[8], GarnetObjectType::SortedSet.as_u8());
  assert_eq!(
    &bytes[9..16],
    &[StorageEncoding::FlattenedTree as u8, 0, 0, 0, 0, 0, 0]
  );
  assert_eq!(&bytes[16..24], &10_000_000u64.to_be_bytes());
  assert_eq!(&bytes[24..32], &i64::MAX.to_be_bytes());

  // 从定长字节反序列化
  let restored = MetaValue::from_bytes(bytes)?;
  assert_eq!(restored, original);

  // 从切片反序列化
  let from_slice = MetaValue::from_slice(&bytes)?;
  assert_eq!(from_slice, original);

  // 防御性拦截：缓冲区不足
  assert!(matches!(
    MetaValue::from_slice(&bytes[..31]),
    Err(Error::BufferTooShort {
      expected: 32,
      actual: 31
    })
  ));

  // 非法 GarnetObjectType 拦截
  let mut corrupt_bytes = bytes;
  corrupt_bytes[8] = 0xff; // 非法类型
  assert!(matches!(
    MetaValue::from_bytes(corrupt_bytes),
    Err(Error::InvalidGarnetObjectType(0xff))
  ));

  // 非法 StorageEncoding 拦截（前向兼容：未知编码字节显式拒绝）
  let mut corrupt_encoding = bytes;
  corrupt_encoding[9] = 0x03; // 未来版本新编码
  assert!(matches!(
    MetaValue::from_bytes(corrupt_encoding),
    Err(Error::InvalidStorageEncoding(0x03))
  ));
  // 严格解码面：唯一合法值命中、未知值（含 0/1/3）一律 None
  assert_eq!(
    StorageEncoding::try_from_u8(2),
    Some(StorageEncoding::FlattenedTree)
  );
  assert_eq!(StorageEncoding::try_from_u8(0), None);
  assert_eq!(StorageEncoding::try_from_u8(1), None);
  assert_eq!(StorageEncoding::try_from_u8(3), None);
  assert_eq!(StorageEncoding::try_from_u8(0xff), None);

  OK
}

#[test]
fn test_meta_value_state_transitions() -> Void {
  info!("测试 MetaValue 状态机容量修改");

  let mut meta = MetaValue::new(8888, GarnetObjectType::Set, 0);
  assert_eq!(meta.size, 0);

  // 容量加减
  meta.inc_size(5);
  assert_eq!(meta.size, 5);
  meta.dec_size(2);
  assert_eq!(meta.size, 3);
  meta.dec_size(10); // 饱和减
  assert_eq!(meta.size, 0);

  OK
}

#[test]
fn test_meta_value_fast_field_readers() -> Void {
  info!("测试 MetaValue 快速字段读取方法与边界防御");

  let meta = MetaValue::new(9999, GarnetObjectType::List, 4321);
  let bytes = meta.to_bytes();

  // 1. 快速读取
  assert_eq!(MetaValue::read_size(&bytes)?, 4321);
  assert_eq!(
    MetaValue::read_collection_type(&bytes)?,
    GarnetObjectType::List
  );

  // 2. 边界截断防御
  assert!(matches!(
    MetaValue::read_size(&bytes[..23]),
    Err(Error::BufferTooShort {
      expected: 24,
      actual: 23
    })
  ));
  assert!(matches!(
    MetaValue::read_collection_type(&bytes[..8]),
    Err(Error::BufferTooShort {
      expected: 9,
      actual: 8
    })
  ));

  // 3. 非法类型防御
  let mut bad_type = bytes;
  bad_type[8] = 0xee;
  assert!(matches!(
    MetaValue::read_collection_type(&bad_type),
    Err(Error::InvalidGarnetObjectType(0xee))
  ));

  OK
}

#[test]
fn test_compile_time_const_evaluation() {
  const META: MetaValue = MetaValue::new(42, GarnetObjectType::Hash, 10);
  const BYTES: [u8; META_VALUE_SIZE] = META.to_bytes();
  const SIZE: Result<u64> = MetaValue::read_size(&BYTES);
  assert!(matches!(SIZE, Ok(10)));
  const TYPE: Result<GarnetObjectType> = MetaValue::read_collection_type(&BYTES);
  assert!(matches!(TYPE, Ok(GarnetObjectType::Hash)));
}
