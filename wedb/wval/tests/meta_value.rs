use std::mem::{align_of, size_of};

use aok::{OK, Void};
use log::info;
use wval::{
  COMPACT_META_VALUE_SIZE, CompactMetaValue, Error, GarnetObjectType, META_VALUE_SIZE, MetaValue,
  Result, StorageEncoding,
};

#[test]
fn test_meta_value_memory_layout() -> Void {
  info!("测试 MetaValue 内存布局与对齐");

  // 严格断言定长 32 字节（2^5，每条 64 字节缓存行恰好容纳 2 个）
  assert_eq!(size_of::<MetaValue>(), 32);
  assert_eq!(size_of::<MetaValue>(), META_VALUE_SIZE);
  // 严格断言 8 字节对齐（保证所有 u64 字段天然对齐，具备单总线周期原子访问能力）
  assert_eq!(align_of::<MetaValue>(), 8);

  let meta = MetaValue::new(1001, GarnetObjectType::Hash, 1, 100);
  assert_eq!(meta.key_id, 1001);
  assert_eq!(meta.collection_type, GarnetObjectType::Hash);
  assert_eq!(meta.version, 1);
  assert_eq!(meta.size, 100);
  assert_eq!(meta.reserved, [0u8; 7]);

  OK
}

#[test]
fn test_meta_value_roundtrip_and_serialization() -> Void {
  info!("测试 MetaValue 二进制序列化与往返恢复");

  let original = MetaValue::new(
    0x0123_4567_89ab_cdef,
    GarnetObjectType::SortedSet,
    0x0000_0000_0000_002a,
    10_000_000,
  );

  let bytes = original.to_bytes();
  assert_eq!(bytes.len(), 32);

  // 验证大端序字节排布
  assert_eq!(&bytes[0..8], &0x0123_4567_89ab_cdef_u64.to_be_bytes());
  assert_eq!(bytes[8], GarnetObjectType::SortedSet.as_u8());
  assert_eq!(&bytes[9..16], &[0u8; 7]);
  assert_eq!(&bytes[16..24], &42u64.to_be_bytes());
  assert_eq!(&bytes[24..32], &10_000_000u64.to_be_bytes());

  // 从定长字节反序列化
  let restored = MetaValue::from_bytes(bytes)?;
  assert_eq!(restored, original);

  // 从切片反序列化
  let from_slice = MetaValue::from_slice(&bytes)?;
  assert_eq!(from_slice, original);

  // 原位切片写入
  let mut dst = [0u8; 32];
  original.write_to_slice(&mut dst)?;
  assert_eq!(dst, bytes);

  // 防御性拦截：缓冲区不足
  assert!(matches!(
    MetaValue::from_slice(&bytes[..31]),
    Err(Error::BufferTooShort {
      expected: 32,
      actual: 31
    })
  ));

  let mut short_dst = [0u8; 30];
  assert!(matches!(
    original.write_to_slice(&mut short_dst),
    Err(Error::BufferTooShort {
      expected: 32,
      actual: 30
    })
  ));

  // 非法 GarnetObjectType 拦截
  let mut corrupt_bytes = bytes;
  corrupt_bytes[8] = 0xff; // 非法类型
  assert!(matches!(
    MetaValue::from_bytes(corrupt_bytes),
    Err(Error::InvalidCollectionType(0xff))
  ));

  // 非法 StorageEncoding 拦截（前向兼容：未知编码字节显式拒绝，
  // 绝不静默折叠为 Compact 按内联布局误解析）
  let mut corrupt_encoding = bytes;
  corrupt_encoding[9] = 0x03; // 未来版本新编码
  assert!(matches!(
    MetaValue::from_bytes(corrupt_encoding),
    Err(Error::InvalidStorageEncoding(0x03))
  ));
  // 严格解码面：已知两值逐一命中、未知值（含已删除的 Flattened=1）None
  assert_eq!(
    StorageEncoding::try_from_u8(0),
    Some(StorageEncoding::Compact)
  );
  assert_eq!(
    StorageEncoding::try_from_u8(2),
    Some(StorageEncoding::FlattenedTree)
  );
  assert_eq!(StorageEncoding::try_from_u8(1), None);
  assert_eq!(StorageEncoding::try_from_u8(3), None);
  assert_eq!(StorageEncoding::try_from_u8(0xff), None);

  OK
}

#[test]
fn test_meta_value_state_transitions() -> Void {
  info!("测试 MetaValue 状态机单调递增与容量修改");

  let mut meta = MetaValue::new(8888, GarnetObjectType::Set, 1, 0);
  assert_eq!(meta.version, 1);
  assert_eq!(meta.size, 0);

  // 版本单调递增
  assert_eq!(meta.bump_version(), 2);
  assert_eq!(meta.version, 2);
  assert_eq!(meta.bump_version(), 3);
  assert_eq!(meta.version, 3);

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

  let meta = MetaValue::new(9999, GarnetObjectType::List, 77, 4321);
  let bytes = meta.to_bytes();

  // 1. 快速读取
  assert_eq!(MetaValue::read_version(&bytes)?, 77);
  assert_eq!(MetaValue::read_size(&bytes)?, 4321);
  assert_eq!(
    MetaValue::read_collection_type(&bytes)?,
    GarnetObjectType::List
  );

  // 2. 边界截断防御
  assert!(matches!(
    MetaValue::read_version(&bytes[..23]),
    Err(Error::BufferTooShort {
      expected: 24,
      actual: 23
    })
  ));
  assert!(matches!(
    MetaValue::read_size(&bytes[..31]),
    Err(Error::BufferTooShort {
      expected: 32,
      actual: 31
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
    Err(Error::InvalidCollectionType(0xee))
  ));

  OK
}

#[test]
fn test_compile_time_const_evaluation() {
  const META: MetaValue = MetaValue::new(42, GarnetObjectType::Hash, 1, 10);
  const BYTES: [u8; 32] = META.to_bytes();
  const VERSION: Result<u64> = MetaValue::read_version(&BYTES);
  assert!(matches!(VERSION, Ok(1)));
  const SIZE: Result<u64> = MetaValue::read_size(&BYTES);
  assert!(matches!(SIZE, Ok(10)));
  const TYPE: Result<GarnetObjectType> = MetaValue::read_collection_type(&BYTES);
  assert!(matches!(TYPE, Ok(GarnetObjectType::Hash)));

  // CompactMetaValue 编译期求值与快速探针验证
  const CMETA: CompactMetaValue = CompactMetaValue::new(
    GarnetObjectType::SortedSet,
    StorageEncoding::FlattenedTree,
    888,
    1_700_000_000_000,
  );
  const C_BYTES: [u8; 16] = CMETA.to_bytes();
  const C_DEC: Result<CompactMetaValue> = CompactMetaValue::from_slice(&C_BYTES);
  assert!(matches!(C_DEC, Ok(cm) if cm.size == 888 && cm.expire_at_ticks == 1_700_000_000_000));
  const C_TYPE: Option<GarnetObjectType> = CompactMetaValue::read_collection_type(&C_BYTES);
  assert_eq!(C_TYPE, Some(GarnetObjectType::SortedSet));
  const C_ENC: Option<StorageEncoding> = CompactMetaValue::read_encoding(&C_BYTES);
  assert_eq!(C_ENC, Some(StorageEncoding::FlattenedTree));
  const C_SIZE: Option<u32> = CompactMetaValue::read_size(&C_BYTES);
  assert_eq!(C_SIZE, Some(888));
  const C_EXP: Option<i64> = CompactMetaValue::read_expire_at_ticks(&C_BYTES);
  assert_eq!(C_EXP, Some(1_700_000_000_000));
  const C_EXPIRED_NO: Option<bool> = CompactMetaValue::read_is_expired(&C_BYTES, 1_699_999_999_999);
  assert_eq!(C_EXPIRED_NO, Some(false));
  // 边界：expire == now 视为未过期（严格小于口径）
  const C_EXPIRED_BOUNDARY: Option<bool> =
    CompactMetaValue::read_is_expired(&C_BYTES, 1_700_000_000_000);
  assert_eq!(C_EXPIRED_BOUNDARY, Some(false));
  const C_EXPIRED_YES: Option<bool> =
    CompactMetaValue::read_is_expired(&C_BYTES, 1_700_000_000_001);
  assert_eq!(C_EXPIRED_YES, Some(true));
}

#[test]
fn test_compact_meta_value_16_bytes() -> Void {
  info!("测试 16 字节 CompactMetaValue 内存排布与功能");

  // 1. 严格断言定长 16 字节与 8 字节自然对齐
  assert_eq!(size_of::<CompactMetaValue>(), 16);
  assert_eq!(size_of::<CompactMetaValue>(), COMPACT_META_VALUE_SIZE);
  assert_eq!(align_of::<CompactMetaValue>(), 8);

  // 2. 构造与字段检查
  let mut cmeta = CompactMetaValue::new(
    GarnetObjectType::Hash,
    StorageEncoding::Compact,
    100,
    1_700_000_000_000,
  );
  assert_eq!(cmeta.collection_type, GarnetObjectType::Hash);
  assert_eq!(cmeta.encoding, StorageEncoding::Compact);
  assert_eq!(cmeta.size, 100);
  assert_eq!(cmeta.expire_at_ticks, 1_700_000_000_000);

  // 3. 过期检查（边界取严格小于：expire == now 视为未过期，
  //    对标 C# LogRecordUtils.cs:20 读路径口径）
  assert!(!cmeta.is_expired(1_699_999_999_999));
  assert!(!cmeta.is_expired(1_700_000_000_000));
  assert!(cmeta.is_expired(1_700_000_000_001));

  // 永不过期 (expire_at_ticks == 0)
  let no_exp = CompactMetaValue::new(GarnetObjectType::Set, StorageEncoding::FlattenedTree, 50, 0);
  assert!(!no_exp.is_expired(i64::MAX));

  // 4. 计数增减
  cmeta.inc_size(25);
  assert_eq!(cmeta.size, 125);
  cmeta.dec_size(50);
  assert_eq!(cmeta.size, 75);
  cmeta.dec_size(100); // 饱和减
  assert_eq!(cmeta.size, 0);

  // 5. 二进制大端往返
  let bytes = cmeta.to_bytes();
  assert_eq!(bytes.len(), 16);
  let restored = CompactMetaValue::from_bytes(bytes)?;
  assert_eq!(cmeta, restored);

  let from_slice = CompactMetaValue::from_slice(&bytes)?;
  assert_eq!(cmeta, from_slice);

  // 5.1 切片写入（零堆分配）
  let mut dst = [0u8; 16];
  cmeta.write_to_slice(&mut dst)?;
  assert_eq!(dst, bytes);
  let mut short_dst = [0u8; 15];
  assert_eq!(
    cmeta.write_to_slice(&mut short_dst),
    Err(Error::BufferTooShort {
      expected: 16,
      actual: 15,
    })
  );

  // 6. 切片长度不足错误拦截
  assert_eq!(
    CompactMetaValue::from_slice(&bytes[..15]),
    Err(Error::BufferTooShort {
      expected: 16,
      actual: 15,
    })
  );

  // 7. 快速字段探针验证
  assert_eq!(
    CompactMetaValue::read_collection_type(&bytes),
    Some(cmeta.collection_type)
  );
  assert_eq!(
    CompactMetaValue::read_encoding(&bytes),
    Some(cmeta.encoding)
  );
  assert_eq!(CompactMetaValue::read_size(&bytes), Some(cmeta.size));
  assert_eq!(
    CompactMetaValue::read_expire_at_ticks(&bytes),
    Some(cmeta.expire_at_ticks)
  );
  assert_eq!(
    CompactMetaValue::read_is_expired(&bytes, 1_699_999_999_999),
    Some(false)
  );
  // 边界：expire == now 视为未过期（严格小于口径）
  assert_eq!(
    CompactMetaValue::read_is_expired(&bytes, 1_700_000_000_000),
    Some(false)
  );
  assert_eq!(
    CompactMetaValue::read_is_expired(&bytes, 1_700_000_000_001),
    Some(true)
  );
  // 短切片探针安全返回 None
  assert_eq!(CompactMetaValue::read_collection_type(&bytes[..15]), None);
  assert_eq!(CompactMetaValue::read_size(&bytes[..15]), None);
  assert_eq!(CompactMetaValue::read_expire_at_ticks(&bytes[..15]), None);
  assert_eq!(
    CompactMetaValue::read_is_expired(&bytes[..15], 1_700_000_000_000),
    None
  );

  OK
}
