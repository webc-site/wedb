use aok::{OK, Void};
use log::info;
use wval::{
  CollectionType, CompactHash, CompactHashCodec, CompactSet, CompactSetCodec, CompactZSet,
  CompactZSetCodec, FieldValueRef, HashEntryRef, META_VALUE_SIZE, MetaValue, StorageEncoding,
  ZSetEntryRef,
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
  let mut meta = MetaValue::new(1001, CollectionType::Hash, 1, 0);
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
      Some(1_700_000_000_000_u64),
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
  assert_eq!(decoded[0].expire_at_ms, None);

  assert_eq!(decoded[1].field, b"token");
  assert_eq!(decoded[1].value, b"jwt_secret_token_value");
  assert_eq!(decoded[1].expire_at_ms, Some(1_700_000_000_000_u64));

  assert_eq!(decoded[2].field, b"role");
  assert_eq!(decoded[2].value, b"admin");
  assert_eq!(decoded[2].expire_at_ms, None);

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
  assert_eq!(ref1.expire_at_ms, None);
  // 测试 Deref 支持
  assert_eq!(&*ref1, b"v1");

  let ref2: FieldValueRef<'_> = CompactHashCodec::find(&buf, b"k2").expect("k2 应该存在");
  assert_eq!(ref2.value, b"v2_with_expire");
  assert_eq!(ref2.expire_at_ms, Some(999_888_777));

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
  assert_eq!(city_ref.expire_at_ms, Some(12345678));

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
    assert_eq!(entry.expire_at_ms, None);
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

// ============================================================================
// 3. CompactSetCodec 测试套件
// ============================================================================
#[test]
fn test_compact_set_ordered_insert_and_dedup() -> Void {
  info!("测试 CompactSetCodec 有序插入与去重");

  let mut set = CompactSet::new();
  assert!(set.is_empty());
  assert_eq!(set.len(), 0);

  // 乱序插入
  assert!(set.insert(b"banana")?);
  assert!(set.insert(b"apple")?);
  assert!(set.insert(b"date")?);
  assert!(set.insert(b"cherry")?);
  assert_eq!(set.len(), 4);

  // 重复插入相同元素（去重）
  assert!(!set.insert(b"apple")?);
  assert!(!set.insert(b"banana")?);
  assert_eq!(set.len(), 4);

  // 验证内部严格保持字典序排列
  let members: Vec<&[u8]> = set.iter_members().collect();
  assert_eq!(
    members,
    vec![
      b"apple".as_slice(),
      b"banana".as_slice(),
      b"cherry".as_slice(),
      b"date".as_slice()
    ]
  );

  OK
}

#[test]
fn test_compact_set_binary_search_contains() -> Void {
  info!("测试 CompactSetCodec 零堆分配二分查找 contains");

  let mut buf = Vec::new();
  CompactSetCodec::insert(&mut buf, b"car")?;
  CompactSetCodec::insert(&mut buf, b"boat")?;
  CompactSetCodec::insert(&mut buf, b"plane")?;
  CompactSetCodec::insert(&mut buf, b"train")?;

  // 验证存在
  assert!(CompactSetCodec::contains(&buf, b"boat"));
  assert!(CompactSetCodec::contains(&buf, b"car"));
  assert!(CompactSetCodec::contains(&buf, b"plane"));
  assert!(CompactSetCodec::contains(&buf, b"train"));

  // 验证不存在（首部之前、中间穿插、尾部之后）
  assert!(!CompactSetCodec::contains(&buf, b"airplane"));
  assert!(!CompactSetCodec::contains(&buf, b"bus"));
  assert!(!CompactSetCodec::contains(&buf, b"rocket"));
  assert!(!CompactSetCodec::contains(&buf, b"zebra"));

  OK
}

#[test]
fn test_compact_set_remove() -> Void {
  info!("测试 CompactSetCodec 成员二分定位与删除");

  let mut set = CompactSet::new();
  set.insert(b"m1")?;
  set.insert(b"m2")?;
  set.insert(b"m3")?;
  set.insert(b"m4")?;
  assert_eq!(set.len(), 4);

  // 删除不存在的元素
  assert!(!set.remove(b"m0")?);
  assert!(!set.remove(b"m5")?);
  assert_eq!(set.len(), 4);

  // 删除中间元素 m2
  assert!(set.remove(b"m2")?);
  assert_eq!(set.len(), 3);
  assert!(!set.contains(b"m2"));
  assert!(set.contains(b"m1"));
  assert!(set.contains(b"m3"));
  assert!(set.contains(b"m4"));

  // 删除首部元素 m1
  assert!(set.remove(b"m1")?);
  assert_eq!(set.len(), 2);
  assert!(!set.contains(b"m1"));

  // 删除尾部元素 m4
  assert!(set.remove(b"m4")?);
  assert_eq!(set.len(), 1);
  assert!(!set.contains(b"m4"));
  assert!(set.contains(b"m3"));

  // 删除最后元素 m3
  assert!(set.remove(b"m3")?);
  assert_eq!(set.len(), 0);
  assert!(set.is_empty());

  OK
}

#[test]
fn test_compact_set_iter_members() -> Void {
  info!("测试 CompactSetCodec 全量迭代");

  let mut buf = Vec::new();
  for i in (0..20).rev() {
    let s = format!("user_{:03}", i);
    CompactSetCodec::insert(&mut buf, s.as_bytes())?;
  }

  assert_eq!(CompactSetCodec::count(&buf)?, 20);

  // 迭代输出必须严格单调递增有序
  let members: Vec<String> = CompactSetCodec::iter_members(&buf)
    .map(|m| String::from_utf8(m.to_vec()).unwrap())
    .collect();

  for (i, member) in members.iter().enumerate().take(20) {
    let expected = format!("user_{:03}", i);
    assert_eq!(member, &expected);
  }

  OK
}

#[test]
fn test_compact_set_boundary_limits() -> Void {
  info!("测试 CompactSet 极限边界：空、单元素、128 满载");

  // 1. 空 set
  let empty = vec![0, 0];
  assert_eq!(CompactSetCodec::count(&empty)?, 0);
  assert!(!CompactSetCodec::contains(&empty, b"x"));
  assert_eq!(CompactSetCodec::iter_members(&empty).count(), 0);

  // 2. 单元素
  let mut single = CompactSet::new();
  assert!(single.insert(b"only_one")?);
  assert_eq!(single.len(), 1);
  assert!(single.contains(b"only_one"));
  assert!(!single.contains(b"other"));
  assert!(single.remove(b"only_one")?);
  assert_eq!(single.len(), 0);

  // 3. 128 满载
  let mut full_set = CompactSet::with_capacity(4096);
  for i in 0..128 {
    let s = format!("member_{:03}", i);
    assert!(full_set.insert(s.as_bytes())?);
  }
  assert_eq!(full_set.len(), 128);

  for i in 0..128 {
    let s = format!("member_{:03}", i);
    assert!(full_set.contains(s.as_bytes()));
  }
  assert!(!full_set.contains(b"member_128"));
  assert_eq!(full_set.iter_members().count(), 128);

  OK
}

#[test]
fn test_compact_set_binary_search_accuracy() -> Void {
  info!("测试 CompactSetCodec 与 CompactSet 二分查找准确率与插入点定位");

  let mut set = CompactSet::new();
  set.insert(b"banana")?;
  set.insert(b"apple")?;
  set.insert(b"date")?;
  set.insert(b"cherry")?;

  // 内部字典序: ["apple", "banana", "cherry", "date"]
  assert_eq!(set.binary_search(b"apple")?, Ok(0));
  assert_eq!(set.binary_search(b"banana")?, Ok(1));
  assert_eq!(set.binary_search(b"cherry")?, Ok(2));
  assert_eq!(set.binary_search(b"date")?, Ok(3));

  // 未命中的插入点检查
  assert_eq!(set.binary_search(b"aaa")?, Err(0));
  assert_eq!(set.binary_search(b"bb")?, Err(2));
  assert_eq!(set.binary_search(b"cz")?, Err(3));
  assert_eq!(set.binary_search(b"zzz")?, Err(4));

  OK
}

#[test]
fn test_compact_set_validate_and_batch_encode() -> Void {
  info!("测试 CompactSetCodec validate 异常数据拦截与 batch encode 排序去重");

  // 1. batch encode
  let raw_members = vec![
    b"zebra".as_slice(),
    b"apple".as_slice(),
    b"banana".as_slice(),
    b"apple".as_slice(), // 重复
    b"cherry".as_slice(),
    b"banana".as_slice(), // 重复
  ];
  let encoded = CompactSetCodec::encode(raw_members)?;
  let decoded = CompactSet::from_vec(encoded)?;
  assert_eq!(decoded.len(), 4);
  let decoded_items: Vec<&[u8]> = decoded.iter_members().collect();
  assert_eq!(
    decoded_items,
    vec![
      b"apple".as_slice(),
      b"banana".as_slice(),
      b"cherry".as_slice(),
      b"zebra".as_slice(),
    ]
  );

  // 2. validate 拦截倒序数据
  let mut bad_buf = Vec::new();
  bad_buf.extend_from_slice(&2u16.to_be_bytes());
  bad_buf.extend_from_slice(&(b"zoo".len() as u16).to_be_bytes());
  bad_buf.extend_from_slice(b"zoo");
  bad_buf.extend_from_slice(&(b"ant".len() as u16).to_be_bytes());
  bad_buf.extend_from_slice(b"ant");
  assert!(CompactSet::from_vec(bad_buf).is_err());

  // 3. validate 拦截长度不足的残缺数据
  let truncated_buf = vec![0, 1, 0, 5, b'a', b'b'];
  assert!(CompactSet::from_vec(truncated_buf).is_err());

  OK
}

// ============================================================================
// 4. CompactZSetCodec 测试套件
// ============================================================================
#[test]
fn test_compact_zset_score_ordering_and_tie_breaker() -> Void {
  info!("测试 CompactZSet 按分值递增及同分字典序（tie-breaker）排序");

  let mut zset = CompactZSet::new();

  // 插入不同分数
  zset.insert(10.0, b"charlie")?;
  zset.insert(5.0, b"bob")?;
  zset.insert(20.0, b"david")?;

  // 插入相同分数 (10.0) 的不同 member
  zset.insert(10.0, b"alice")?;
  zset.insert(10.0, b"zach")?;

  assert_eq!(zset.len(), 5);

  // 预期顺序: // 1. 5.0, "bob"
  // 2. 10.0, "alice" // 3. 10.0, "charlie"
  // 4. 10.0, "zach" // 5. 20.0, "david"
  let entries: Vec<ZSetEntryRef> = zset.iter_members().collect();
  assert_eq!(entries.len(), 5);

  assert_eq!(entries[0].score, 5.0);
  assert_eq!(entries[0].member, b"bob");

  assert_eq!(entries[1].score, 10.0);
  assert_eq!(entries[1].member, b"alice");

  assert_eq!(entries[2].score, 10.0);
  assert_eq!(entries[2].member, b"charlie");

  assert_eq!(entries[3].score, 10.0);
  assert_eq!(entries[3].member, b"zach");

  assert_eq!(entries[4].score, 20.0);
  assert_eq!(entries[4].member, b"david");

  OK
}

#[test]
fn test_compact_zset_rank_and_key_at_rank() -> Void {
  info!("测试 CompactZSet 排名计算 (rank_of) 与按排名提取 (key_at_rank)");

  let mut zset = CompactZSet::new();
  zset.insert(100.0, b"p1")?;
  zset.insert(200.0, b"p2")?;
  zset.insert(300.0, b"p3")?;
  zset.insert(400.0, b"p4")?;
  zset.insert(500.0, b"p5")?;

  // rank_of 测试 (0-indexed)
  assert_eq!(zset.rank_of(b"p1"), Some(0));
  assert_eq!(zset.rank_of(b"p2"), Some(1));
  assert_eq!(zset.rank_of(b"p3"), Some(2));
  assert_eq!(zset.rank_of(b"p4"), Some(3));
  assert_eq!(zset.rank_of(b"p5"), Some(4));
  assert_eq!(zset.rank_of(b"p_unknown"), None);

  // CompactZSetCodec 静态方法测试
  assert_eq!(CompactZSetCodec::rank_of(&zset, b"p1"), Some(0));
  assert_eq!(
    CompactZSetCodec::key_at_rank(&zset, 0),
    Some(b"p1".as_slice())
  );
  assert_eq!(CompactZSetCodec::count_range(&zset, 150.0, 450.0), 3);

  // key_at_rank 测试
  assert_eq!(zset.key_at_rank(0), Some(b"p1".as_slice()));
  assert_eq!(zset.key_at_rank(1), Some(b"p2".as_slice()));
  assert_eq!(zset.key_at_rank(2), Some(b"p3".as_slice()));
  assert_eq!(zset.key_at_rank(3), Some(b"p4".as_slice()));
  assert_eq!(zset.key_at_rank(4), Some(b"p5".as_slice()));
  assert_eq!(zset.key_at_rank(5), None);

  // score_of 测试
  assert_eq!(zset.score_of(b"p1"), Some(100.0));
  assert_eq!(zset.score_of(b"p3"), Some(300.0));
  assert_eq!(zset.score_of(b"none"), None);

  OK
}

#[test]
fn test_compact_zset_insert_update_and_delete() -> Void {
  info!("测试 CompactZSet 插入、分值更新导致的重排序以及删除");

  let mut zset = CompactZSet::new();
  assert!(zset.insert(10.0, b"player")?);
  assert!(zset.insert(20.0, b"other")?);
  assert_eq!(zset.rank_of(b"player"), Some(0));

  // 1. 将 player 的分数更新为 30.0，排名应移动到最后 (rank 1)
  assert!(!zset.insert(30.0, b"player")?); // 更新返回 false
  assert_eq!(zset.len(), 2);
  assert_eq!(zset.score_of(b"player"), Some(30.0));
  assert_eq!(zset.rank_of(b"player"), Some(1));
  assert_eq!(zset.key_at_rank(0), Some(b"other".as_slice()));
  assert_eq!(zset.key_at_rank(1), Some(b"player".as_slice()));

  // 2. 将 player 的分数更新为 5.0，排名应移动到最前 (rank 0)
  assert!(!zset.insert(5.0, b"player")?);
  assert_eq!(zset.score_of(b"player"), Some(5.0));
  assert_eq!(zset.rank_of(b"player"), Some(0));
  assert_eq!(zset.key_at_rank(0), Some(b"player".as_slice()));

  // 3. 删除 player
  assert!(zset.remove(b"player")?);
  assert_eq!(zset.len(), 1);
  assert_eq!(zset.rank_of(b"player"), None);
  assert_eq!(zset.score_of(b"player"), None);

  // 删除不存在的元素
  assert!(!zset.remove(b"player")?);
  assert_eq!(zset.len(), 1);

  OK
}

#[test]
fn test_compact_zset_count_range_and_streaming_iter() -> Void {
  info!("测试 CompactZSet count_range 区间统计与流式切片迭代");

  let mut zset = CompactZSet::new();
  zset.insert(10.0, b"m10")?;
  zset.insert(20.0, b"m20")?;
  zset.insert(30.0, b"m30")?;
  zset.insert(40.0, b"m40")?;
  zset.insert(50.0, b"m50")?;

  // 1. 范围计数统计
  assert_eq!(zset.count_range(20.0, 40.0), 3);
  assert_eq!(zset.count_range(15.0, 45.0), 3);
  assert_eq!(zset.count_range(25.0, 35.0), 1);
  assert_eq!(zset.count_range(0.0, 100.0), 5);
  assert_eq!(zset.count_range(60.0, 100.0), 0);
  assert_eq!(zset.count_range(50.0, 100.0), 1);
  assert_eq!(zset.count_range(50.0, 10.0), 0); // 反向区间

  // 2. 切片流式区间迭代
  let range_items: Vec<ZSetEntryRef> = zset.range(20.0, 40.0).collect();
  assert_eq!(range_items.len(), 3);
  assert_eq!(range_items[0].member, b"m20");
  assert_eq!(range_items[1].member, b"m30");
  assert_eq!(range_items[2].member, b"m40");

  OK
}

#[test]
fn test_compact_zset_extreme_floats_and_boundaries() -> Void {
  info!("测试 CompactZSet 极端浮点数（负无穷、正无穷、-0.0 vs +0.0）与 128 满载");

  let mut zset = CompactZSet::new();

  // 插入各类极值
  zset.insert(f64::INFINITY, b"pos_inf")?;
  zset.insert(f64::NEG_INFINITY, b"neg_inf")?;
  zset.insert(0.0, b"plus_zero")?;
  zset.insert(-0.0, b"minus_zero")?;
  zset.insert(-1000.0, b"neg_thousand")?;
  zset.insert(1000.0, b"pos_thousand")?;

  assert_eq!(zset.len(), 6);

  // 验证全局有序性：
  // 0: neg_inf (-inf)
  // 1: neg_thousand (-1000.0) // 2: minus_zero (-0.0)
  // 3: plus_zero (+0.0) // 4: pos_thousand (1000.0)
  // 5: pos_inf (+inf)
  let items: Vec<ZSetEntryRef> = zset.iter_members().collect();
  assert_eq!(items[0].member, b"neg_inf");
  assert_eq!(items[0].score, f64::NEG_INFINITY);

  assert_eq!(items[1].member, b"neg_thousand");
  assert_eq!(items[1].score, -1000.0);

  assert_eq!(items[2].member, b"minus_zero");
  assert_eq!(items[2].score.to_bits(), (-0.0_f64).to_bits());

  assert_eq!(items[3].member, b"plus_zero");
  assert_eq!(items[3].score.to_bits(), 0.0_f64.to_bits());

  assert_eq!(items[4].member, b"pos_thousand");
  assert_eq!(items[4].score, 1000.0);

  assert_eq!(items[5].member, b"pos_inf");
  assert_eq!(items[5].score, f64::INFINITY);

  // 128 满载测试
  let mut full_zset = CompactZSet::with_capacity(4096);
  for i in 0..128 {
    let m = format!("z_{:03}", i);
    let score = (i as f64) * 1.5;
    assert!(full_zset.insert(score, m.as_bytes())?);
  }
  assert_eq!(full_zset.len(), 128);
  assert_eq!(full_zset.rank_of(b"z_000"), Some(0));
  assert_eq!(full_zset.rank_of(b"z_127"), Some(127));
  assert_eq!(full_zset.key_at_rank(0), Some(b"z_000".as_slice()));
  assert_eq!(full_zset.key_at_rank(127), Some(b"z_127".as_slice()));
  assert_eq!(full_zset.count_range(0.0, 150.0), 101);

  OK
}

// ============================================================================
// 5. CompactZSet Redis 命令对齐 libs/storage/Tsavorite/cs/benchmark/KV.benchmark/KvBenchmark.cs:API（ZCARD/ZRANK/ZREVRANK/ZSCORE/ZPOPMIN/ZPOPMAX/ZRANGE/Bitcode）
// ============================================================================
#[test]
fn test_compact_zset_redis_style_api() -> Void {
  info!("测试 CompactZSet Redis 风格 API 与 Bitcode 往返");

  let mut zset = CompactZSet::new();
  for i in 0..5 {
    let score = (i as f64) * 10.0;
    let member = format!("m{i}").into_bytes();
    assert!(zset.insert(score, &member)?);
  }

  // from_bytes / as_bytes / zcard
  let snapshot = CompactZSet::from_bytes(zset.as_bytes())?;
  assert_eq!(snapshot.as_bytes(), zset.as_bytes());
  assert_eq!(snapshot.zcard(), 5);
  assert_eq!(snapshot.zcard(), snapshot.len());

  // zrank / zrevrank / zscore
  assert_eq!(zset.zrank(b"m0"), Some(0));
  assert_eq!(zset.zrank(b"m4"), Some(4));
  assert_eq!(zset.zrevrank(b"m0"), Some(4));
  assert_eq!(zset.zrevrank(b"m4"), Some(0));
  assert_eq!(zset.zrevrank(b"missing"), None);
  assert_eq!(zset.zscore(b"m2"), Some(20.0));
  assert_eq!(zset.zscore(b"missing"), None);

  // zrange 正序与反序（含负索引）
  let names = |v: Vec<(Vec<u8>, f64)>| -> Vec<String> {
    v.iter()
      .map(|(m, _)| String::from_utf8_lossy(m).to_string())
      .collect()
  };
  assert_eq!(
    names(zset.zrange(0, -1, false)),
    vec!["m0", "m1", "m2", "m3", "m4"]
  );
  assert_eq!(
    names(zset.zrange(0, -1, true)),
    vec!["m4", "m3", "m2", "m1", "m0"]
  );
  assert_eq!(names(zset.zrange(-2, -1, false)), vec!["m3", "m4"]);
  assert_eq!(names(zset.zrange(2, 10, false)), vec!["m2", "m3", "m4"]);
  assert!(zset.zrange(5, 9, false).is_empty());

  // pop_min / pop_max
  let (min_m, min_s) = zset.pop_min().expect("非空必可弹");
  assert_eq!((min_m.as_slice(), min_s), (b"m0".as_slice(), 0.0));
  let (max_m, max_s) = zset.pop_max().expect("非空必可弹");
  assert_eq!((max_m.as_slice(), max_s), (b"m4".as_slice(), 40.0));
  assert_eq!(zset.zcard(), 3);
  assert_eq!(zset.zrank(b"m4"), None);

  // 空集合弹出
  let mut empty = CompactZSet::new();
  assert!(empty.pop_min().is_none());
  assert!(empty.pop_max().is_none());

  OK
}

#[test]
fn test_compact_zset_batch_encode_dedup_and_order() -> Void {
  info!("测试 CompactZSetCodec::encode 同成员覆盖与批量有序构建");

  // 同成员重复出现：后写覆盖先写（对齐逐条 insert 的更新语义）
  let entries = vec![
    (5.0, b"a".as_slice(), None),
    (1.0, b"b".as_slice(), None),
    (9.0, b"a".as_slice(), None),
    (3.0, b"c".as_slice(), None),
  ];
  let buf = CompactZSetCodec::encode(entries)?;
  let zset = CompactZSet::from_vec(buf)?;
  assert_eq!(zset.len(), 3);
  let items: Vec<(f64, &[u8])> = zset.iter_members().map(|e| (e.score, e.member)).collect();
  assert_eq!(
    items,
    vec![
      (1.0, b"b".as_slice()),
      (3.0, b"c".as_slice()),
      (9.0, b"a".as_slice())
    ]
  );

  // 空 batch 仅含计数前缀
  let empty = CompactZSetCodec::encode(Vec::<(f64, &[u8], Option<u64>)>::new())?;
  assert_eq!(empty.len(), 2);
  assert_eq!(CompactZSetCodec::count(&empty)?, 0);

  // 条目级过期批量编码往返（对标 C# SortedSetObject ExpirationBitMask 序列化）
  let exp_entries = vec![
    (5.0, b"a".as_slice(), Some(1_700_000_000_000u64)),
    (1.0, b"b".as_slice(), None),
    (5.0, b"a".as_slice(), Some(1_800_000_000_000u64)), // 后写覆盖过期时间
  ];
  let exp_buf = CompactZSetCodec::encode(exp_entries)?;
  let exp_zset = CompactZSet::from_vec(exp_buf)?;
  let entries: Vec<ZSetEntryRef> = exp_zset.iter_members().collect();
  assert_eq!(entries.len(), 2);
  assert_eq!(entries[0].member, b"b");
  assert_eq!(entries[0].expire_at_ms, None);
  assert_eq!(entries[1].member, b"a");
  assert_eq!(entries[1].expire_at_ms, Some(1_800_000_000_000u64));

  OK
}

#[test]
fn test_compact_zset_member_expiration() -> Void {
  info!("测试 CompactZSet 成员级过期：写入往返、purge_expired 淘汰与原地更新");

  let now = 1_000_000u64;
  let mut zset = CompactZSet::new();

  // 1. 写入带过期与不过期成员，迭代零拷贝还原过期时间戳
  assert!(zset.insert_with_expire(10.0, b"expired", Some(now - 1))?);
  assert!(zset.insert_with_expire(20.0, b"alive", Some(now + 100))?);
  assert!(zset.insert_with_expire(30.0, b"eternal", None)?);
  assert_eq!(zset.len(), 3);

  let entries: Vec<ZSetEntryRef> = zset.iter_members().collect();
  assert_eq!(entries[0].expire_at_ms, Some(now - 1));
  assert_eq!(entries[1].expire_at_ms, Some(now + 100));
  assert_eq!(entries[2].expire_at_ms, None);

  // 2. 同分值同过期重复写入：零写放大短路（无新增无更新）
  assert!(!zset.insert_with_expire(10.0, b"expired", Some(now - 1))?);
  assert_eq!(zset.len(), 3);

  // 3. 仅更新过期时间（分值不变）：原位重排并保留
  assert!(!zset.insert_with_expire(20.0, b"alive", None)?);
  assert_eq!(zset.len(), 3);
  assert_eq!(zset.score_of(b"alive"), Some(20.0));
  assert_eq!(
    zset
      .iter_members()
      .find(|e| e.member == b"alive")
      .unwrap()
      .expire_at_ms,
    None
  );

  // 4. purge_expired 单遍淘汰过期成员并压缩
  assert_eq!(zset.purge_expired(now)?, 1);
  assert_eq!(zset.len(), 2);
  assert_eq!(zset.rank_of(b"expired"), None);
  assert_eq!(zset.rank_of(b"alive"), Some(0));
  assert_eq!(zset.rank_of(b"eternal"), Some(1));
  CompactZSetCodec::validate(zset.as_slice())?;

  // 5. 全部过期后清空
  let mut all_exp = CompactZSet::new();
  all_exp.insert_with_expire(1.0, b"a", Some(1))?;
  all_exp.insert_with_expire(2.0, b"b", Some(2))?;
  assert_eq!(all_exp.purge_expired(10)?, 2);
  assert!(all_exp.is_empty());
  assert_eq!(all_exp.purge_expired(10)?, 0);

  OK
}
