use aok::{OK, Void};
use log::info;
use wval::{
  CompactHash, CompactHashCodec, FieldValueRef, GarnetObjectType, HashEntryRef,
  META_VALUE_SIZE, MetaValue, StorageEncoding,
};

#[ctor::ctor(unsafe)]
fn _log_init() {
  log_init::init();
}

// ============================================================================
// 1. MetaValue 与 StorageEncoding 紧凑集成测试
// ============================================================================
#[test]
fn test_meta_value_storage_encoding() -> Void {
  info!("测试 MetaValue 的 StorageEncoding 扩展与二进制兼容性");

  // 1. 默认新建 MetaValue，默认编码必须为 Compact (0)
  let mut meta = MetaValue::new(1001, GarnetObjectType::Hash, 1, 0);
  assert_eq!(meta.encoding(), StorageEncoding::Compact);
  assert_eq!(meta.reserved[0], 0);

  // 2. 修改编码为 Flattened (1)
  meta.set_encoding(StorageEncoding::Flattened);
  assert_eq!(meta.encoding(), StorageEncoding::Flattened);
  assert_eq!(meta.reserved[0], 1);

  // 3. 序列化为 32 字节并反序列化验证往返一致性
  let bytes = meta.to_bytes();
  assert_eq!(bytes.len(), META_VALUE_SIZE);
  assert_eq!(bytes[9], 1); // reserved[0] 在大端布局中的绝对偏移为 9

  let restored = MetaValue::from_bytes(bytes)?;
  assert_eq!(restored.encoding(), StorageEncoding::Flattened);
  assert_eq!(restored, meta);

  // 4. 重置为 Compact 验证
  meta.set_encoding(StorageEncoding::Compact);
  assert_eq!(meta.encoding(), StorageEncoding::Compact);
  let bytes2 = meta.to_bytes();
  assert_eq!(bytes2[9], 0);
  let restored2 = MetaValue::from_bytes(bytes2)?;
  assert_eq!(restored2.encoding(), StorageEncoding::Compact);

  OK
}

// ============================================================================

// ============================================================================
// 2. CompactHashCodec 测试套件
// ============================================================================
#[test]
fn test_compact_hash_encode_decode() -> Void {
  info!("测试 CompactHashCodec 基础编码与解码（含过期时间）");

  let entries = [
    (b"user".as_slice(), b"alice".as_slice(), None),
    (
      b"token".as_slice(),
      b"jwt_secret_token_value".as_slice(),
      Some(1_700_000_000_000_i64),
    ),
    (b"role".as_slice(), b"admin".as_slice(), None),
  ];

  // 编码
  let encoded = CompactHashCodec::encode(entries.iter().copied())?;
  assert_eq!(CompactHashCodec::count(&encoded)?, 3);

  // 解码迭代
  let decoded: Vec<HashEntryRef> = CompactHashCodec::iter_fields(&encoded).collect();
  assert_eq!(decoded.len(), 3);

  assert_eq!(decoded[0].field, b"user");
  assert_eq!(decoded[0].value, b"alice");
  assert_eq!(decoded[0].expire_at_ticks, None);

  assert_eq!(decoded[1].field, b"token");
  assert_eq!(decoded[1].value, b"jwt_secret_token_value");
  assert_eq!(decoded[1].expire_at_ticks, Some(1_700_000_000_000_i64));

  assert_eq!(decoded[2].field, b"role");
  assert_eq!(decoded[2].value, b"admin");
  assert_eq!(decoded[2].expire_at_ticks, None);

  OK
}

#[test]
fn test_compact_hash_find_field_zero_alloc() -> Void {
  info!("测试 CompactHashCodec 零堆分配切片就地查找");

  let mut buf = Vec::new();
  CompactHashCodec::set_field(&mut buf, b"k1", b"v1", None)?;
  CompactHashCodec::set_field(&mut buf, b"k2", b"v2_with_expire", Some(999_888_777))?;
  CompactHashCodec::set_field(&mut buf, b"k3", b"v3", None)?;

  // 1. find_field -> Option<&[u8]>
  assert_eq!(
    CompactHashCodec::find_field(&buf, b"k1"),
    Some(b"v1".as_slice())
  );
  assert_eq!(
    CompactHashCodec::find_field(&buf, b"k2"),
    Some(b"v2_with_expire".as_slice())
  );
  assert_eq!(
    CompactHashCodec::find_field(&buf, b"k3"),
    Some(b"v3".as_slice())
  );
  assert_eq!(CompactHashCodec::find_field(&buf, b"k4"), None);

  // 2. find -> Option<FieldValueRef>
  let ref1: FieldValueRef<'_> = CompactHashCodec::find(&buf, b"k1").expect("k1 应该存在");
  assert_eq!(ref1.value, b"v1");
  assert_eq!(ref1.expire_at_ticks, None);
  // 测试 Deref 支持
  assert_eq!(&*ref1, b"v1");

  let ref2: FieldValueRef<'_> = CompactHashCodec::find(&buf, b"k2").expect("k2 应该存在");
  assert_eq!(ref2.value, b"v2_with_expire");
  assert_eq!(ref2.expire_at_ticks, Some(999_888_777));

  assert!(CompactHashCodec::find(&buf, b"not_found").is_none());

  OK
}

#[test]
fn test_compact_hash_set_field_update_and_append() -> Void {
  info!("测试 CompactHash 字段追加与原地/变长更新");

  let mut hash = CompactHash::new();
  assert!(hash.is_empty());
  assert_eq!(hash.len(), 0);

  // 1. 连续追加
  assert!(hash.set_field(b"name", b"alice", None)?); // true 表示新插入
  assert_eq!(hash.len(), 1);
  assert_eq!(hash.find_field(b"name"), Some(b"alice".as_slice()));

  assert!(hash.set_field(b"city", b"beijing", None)?);
  assert_eq!(hash.len(), 2);

  // 2. 等长更新
  assert!(!hash.set_field(b"name", b"clark", None)?); // false 表示原地/更新
  assert_eq!(hash.len(), 2);
  assert_eq!(hash.find_field(b"name"), Some(b"clark".as_slice()));

  // 3. 扩容变长更新 (短 -> 长)
  assert!(!hash.set_field(b"name", b"alexander_the_great", None)?);
  assert_eq!(hash.len(), 2);
  assert_eq!(
    hash.find_field(b"name"),
    Some(b"alexander_the_great".as_slice())
  );
  assert_eq!(hash.find_field(b"city"), Some(b"beijing".as_slice()));

  // 4. 缩容变长更新 (长 -> 短)
  assert!(!hash.set_field(b"name", b"al", None)?);
  assert_eq!(hash.len(), 2);
  assert_eq!(hash.find_field(b"name"), Some(b"al".as_slice()));
  assert_eq!(hash.find_field(b"city"), Some(b"beijing".as_slice()));

  // 5. 更新过期时间
  assert!(!hash.set_field(b"city", b"shanghai", Some(12345678))?);
  let city_ref = hash.find(b"city").expect("city 应存在");
  assert_eq!(city_ref.value, b"shanghai");
  assert_eq!(city_ref.expire_at_ticks, Some(12345678));

  OK
}

#[test]
fn test_compact_hash_delete_field() -> Void {
  info!("测试 CompactHash 字段删除");

  let mut hash = CompactHash::new();
  hash.set_field(b"k1", b"v1", None)?;
  hash.set_field(b"k2", b"v2", None)?;
  hash.set_field(b"k3", b"v3", None)?;
  assert_eq!(hash.len(), 3);

  // 删除不存在的字段
  assert!(!hash.delete_field(b"k_none")?);
  assert_eq!(hash.len(), 3);

  // 删除中间字段 k2
  assert!(hash.delete_field(b"k2")?);
  assert_eq!(hash.len(), 2);
  assert_eq!(hash.find_field(b"k1"), Some(b"v1".as_slice()));
  assert_eq!(hash.find_field(b"k2"), None);
  assert_eq!(hash.find_field(b"k3"), Some(b"v3".as_slice()));

  // 删除首部字段 k1
  assert!(hash.delete_field(b"k1")?);
  assert_eq!(hash.len(), 1);
  assert_eq!(hash.find_field(b"k1"), None);
  assert_eq!(hash.find_field(b"k3"), Some(b"v3".as_slice()));

  // 删除尾部字段 k3
  assert!(hash.delete_field(b"k3")?);
  assert_eq!(hash.len(), 0);
  assert!(hash.is_empty());
  assert_eq!(hash.find_field(b"k3"), None);

  OK
}

#[test]
fn test_compact_hash_iter_fields() -> Void {
  info!("测试 CompactHash 全量迭代");

  let mut buf = Vec::new();
  for i in 0..10 {
    let k = format!("key_{:02}", i);
    let v = format!("val_{:02}", i);
    CompactHashCodec::set_field(&mut buf, k.as_bytes(), v.as_bytes(), None)?;
  }

  let mut count = 0;
  for (idx, entry) in CompactHashCodec::iter_fields(&buf).enumerate() {
    let expected_k = format!("key_{:02}", idx);
    let expected_v = format!("val_{:02}", idx);
    assert_eq!(entry.field, expected_k.as_bytes());
    assert_eq!(entry.value, expected_v.as_bytes());
    assert_eq!(entry.expire_at_ticks, None);
    count += 1;
  }
  assert_eq!(count, 10);

  OK
}

#[test]
fn test_compact_hash_boundary_limits() -> Void {
  info!("测试 CompactHash 极限边界：空、单字段、512 满载、重复覆盖");

  // 1. 空 hash
  let empty_buf = vec![0, 0];
  assert_eq!(CompactHashCodec::count(&empty_buf)?, 0);
  assert_eq!(CompactHashCodec::find_field(&empty_buf, b"any"), None);
  assert_eq!(CompactHashCodec::iter_fields(&empty_buf).count(), 0);

  // 2. 单字段
  let mut hash = CompactHash::new();
  hash.set_field(b"solo", b"value", None)?;
  assert_eq!(hash.len(), 1);
  assert_eq!(hash.find_field(b"solo"), Some(b"value".as_slice()));

  // 3. 512 字段满载
  let mut big_hash = CompactHash::with_capacity(16384);
  for i in 0..512 {
    let k = format!("f_{:04}", i);
    let v = format!("v_{:04}", i);
    let inserted = big_hash.set_field(k.as_bytes(), v.as_bytes(), None)?;
    assert!(inserted);
  }
  assert_eq!(big_hash.len(), 512);

  // 随机与边界抽检
  assert_eq!(big_hash.find_field(b"f_0000"), Some(b"v_0000".as_slice()));
  assert_eq!(big_hash.find_field(b"f_0255"), Some(b"v_0255".as_slice()));
  assert_eq!(big_hash.find_field(b"f_0511"), Some(b"v_0511".as_slice()));
  assert_eq!(big_hash.find_field(b"f_0512"), None);
  assert_eq!(big_hash.iter_fields().count(), 512);

  // 4. 重复 field 覆盖 100 次
  for i in 0..100 {
    let v = format!("updated_{}", i);
    let inserted = big_hash.set_field(b"f_0000", v.as_bytes(), None)?;
    assert!(!inserted); // 均为更新
  }
  assert_eq!(big_hash.len(), 512);
  assert_eq!(
    big_hash.find_field(b"f_0000"),
    Some(b"updated_99".as_slice())
  );

  OK
}

#[test]
fn test_compact_hash_from_vec_corruption_defense() -> Void {
  info!("测试 CompactHash::from_vec 完整遍历校验：残缺与尾部脏数据拦截");

  // 1. 合法数据往返
  let mut hash = CompactHash::new();
  hash.set_field(b"a", b"1", None)?;
  hash.set_field(b"b", b"2", Some(123))?;
  let restored = CompactHash::from_vec(hash.as_slice().to_vec())?;
  assert_eq!(restored.as_slice(), hash.as_slice());

  // 2. 残缺条目（value 长度声明超出缓冲区）
  let mut truncated = hash.as_slice().to_vec();
  truncated.truncate(truncated.len() - 1);
  assert!(CompactHash::from_vec(truncated).is_err());

  // 3. 尾部脏数据（条目总长度与缓冲区不一致）
  let mut dirty = hash.as_slice().to_vec();
  dirty.extend_from_slice(&[0xFF, 0xFF]);
  assert!(CompactHash::from_vec(dirty).is_err());

  // 4. 少于计数前缀
  assert!(CompactHash::from_vec(vec![0]).is_err());

  OK
}
