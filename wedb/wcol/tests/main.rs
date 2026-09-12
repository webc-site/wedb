use std::{env, fs, ops::Deref, path::PathBuf};

use aok::{OK, Result};
use wbftree::BfTreeService;
use wcol::{
  CollectionError, HashTreeOps, LIST_STUB_SIZE, ListStub, ListTree, ListTreeOps, RiTreeOps,
  SetTreeOps, TreePrefix, ZSetTreeOps, decode_order_score, encode_order_score, i64_from_order_idx,
  normalize_range, order_idx_from_i64,
};

/// 隔离的临时测试树 RAII 守卫
struct TempTree {
  path: PathBuf,
  tree: BfTreeService,
}

impl TempTree {
  fn new(name: &str) -> Result<Self> {
    let path = env::temp_dir().join(format!("bftree_coll_{}_{}.bftree", name, fastrand::u64(..)));
    let tree = BfTreeService::open_disk(&path, 4)?;
    Ok(Self { path, tree })
  }
}

impl Deref for TempTree {
  type Target = BfTreeService;
  #[inline]
  fn deref(&self) -> &Self::Target {
    &self.tree
  }
}

impl Drop for TempTree {
  fn drop(&mut self) {
    if self.path.exists() {
      let _ = fs::remove_file(&self.path);
    }
  }
}

// ============================================================================
// 1. Hash 集合测试
// ============================================================================

#[test]
fn test_hash_crud_and_empty_value_tolerance() -> Result<()> {
  let tree = TempTree::new("hash_crud")?;

  // 1. 正常插入与读取
  assert!(tree.hset(b"user:name", b"Alice")?);
  assert_eq!(tree.hget(b"user:name")?, Some(b"Alice".to_vec()));
  assert_eq!(
    tree.hget_callback(b"user:name", |opt| opt.map(|v| v.len()))?,
    Some(5)
  );
  assert!(tree.hexists(b"user:name")?);
  assert_eq!(tree.hlen()?, 1);

  // 2. 覆盖更新 (返回 false)
  assert!(!tree.hset(b"user:name", b"Bob")?);
  assert_eq!(tree.hget(b"user:name")?, Some(b"Bob".to_vec()));
  assert_eq!(
    tree.hget_callback(b"user:name", |opt| opt.map(|v| v.to_vec()))?,
    Some(b"Bob".to_vec())
  );
  assert_eq!(tree.hlen()?, 1);

  // 3. 空值容错 (tag=0 空串)
  assert!(tree.hset(b"user:bio", b"")?);
  assert_eq!(tree.hget(b"user:bio")?, Some(Vec::new()));
  assert_eq!(
    tree.hget_callback(b"user:bio", |opt| opt.map(|v| v.is_empty()))?,
    Some(true)
  );
  assert!(tree.hexists(b"user:bio")?);
  assert_eq!(tree.hlen()?, 2);

  // 4. 读取不存在的字段
  assert_eq!(tree.hget(b"user:nonexistent")?, None);
  assert!(!tree.hexists(b"user:nonexistent")?);

  // 5. 删除字段
  assert!(tree.hdel(b"user:name")?);
  assert!(!tree.hdel(b"user:name")?); // 再次删除返回 false
  assert_eq!(tree.hget(b"user:name")?, None);
  assert_eq!(tree.hlen()?, 1);

  // 6. 删除空串字段
  assert!(tree.hdel(b"user:bio")?);
  assert_eq!(tree.hlen()?, 0);

  OK
}

#[test]
fn test_hash_streaming_scan() -> Result<()> {
  let tree = TempTree::new("hash_scan")?;

  for i in 0..100 {
    let key = format!("field:{:03}", i);
    let val = format!("val:{:03}", i);
    tree.hset(key.as_bytes(), val.as_bytes())?;
  }
  assert_eq!(tree.hlen()?, 100);

  // 全量流式扫描
  let mut scanned = 0;
  tree.hscan(b"", usize::MAX, |k, v| {
    let expected_key = format!("field:{:03}", scanned);
    let expected_val = format!("val:{:03}", scanned);
    assert_eq!(k, expected_key.as_bytes());
    assert_eq!(v, expected_val.as_bytes());
    scanned += 1;
    true
  })?;
  assert_eq!(scanned, 100);

  // 提前终止扫描 (仅扫描前 10 条)
  let mut count = 0;
  let res = tree.hscan(b"", 50, |_k, _v| {
    count += 1;
    count < 10
  })?;
  assert_eq!(res, 10);
  assert_eq!(count, 10);

  // 起始键扫描
  let mut from_50_count = 0;
  tree.hscan(b"field:050", 20, |k, _v| {
    let expected_key = format!("field:{:03}", 50 + from_50_count);
    assert_eq!(k, expected_key.as_bytes());
    from_50_count += 1;
    true
  })?;
  assert_eq!(from_50_count, 20);

  OK
}

// ============================================================================
// 2. Set 集合测试
// ============================================================================

#[test]
fn test_set_crud_and_card() -> Result<()> {
  let tree = TempTree::new("set_crud")?;

  // 1. 基础添加
  assert!(tree.sadd(b"member_a")?);
  assert!(tree.sadd(b"member_b")?);
  assert_eq!(tree.scard()?, 2);

  // 2. 重复添加返回 false
  assert!(!tree.sadd(b"member_a")?);
  assert_eq!(tree.scard()?, 2);

  // 3. 成员存在性校验与全量成员获取
  assert!(tree.sismember(b"member_a")?);
  assert!(tree.sismember(b"member_b")?);
  assert!(!tree.sismember(b"member_c")?);
  assert_eq!(
    tree.smembers()?,
    vec![b"member_a".to_vec(), b"member_b".to_vec()]
  );

  // 4. 移除成员
  assert!(tree.srem(b"member_a")?);
  assert!(!tree.srem(b"member_a")?); // 重复删除返回 false
  assert!(!tree.sismember(b"member_a")?);
  assert_eq!(tree.scard()?, 1);

  assert!(tree.srem(b"member_b")?);
  assert_eq!(tree.scard()?, 0);

  OK
}

#[test]
fn test_set_streaming_scan() -> Result<()> {
  let tree = TempTree::new("set_scan")?;

  for i in 0..50 {
    let member = format!("m:{:03}", i);
    tree.sadd(member.as_bytes())?;
  }
  assert_eq!(tree.scard()?, 50);

  let mut collected = Vec::new();
  tree.sscan(b"", usize::MAX, |m| {
    collected.push(m.to_vec());
    true
  })?;
  assert_eq!(collected.len(), 50);

  // 提前终止
  let mut early_count = 0;
  tree.sscan(b"", 30, |_| {
    early_count += 1;
    early_count < 15
  })?;
  assert_eq!(early_count, 15);

  OK
}

// ============================================================================
// 3. ZSet 集合测试
// ============================================================================

#[test]
fn test_zset_float_order_monotonicity() {
  let scores = [
    f64::NEG_INFINITY,
    -10_000_000.0,
    -100.5,
    -1.0,
    -0.0001,
    -0.0,
    0.0,
    0.0001,
    1.0,
    100.5,
    10_000_000.0,
    f64::INFINITY,
  ];

  for i in 0..scores.len() - 1 {
    let s1 = scores[i];
    let s2 = scores[i + 1];
    let order1 = encode_order_score(s1);
    let order2 = encode_order_score(s2);
    assert!(
      order1 < order2,
      "单调性校验失败: score1={} ({:?}), score2={} ({:?})",
      s1,
      order1,
      s2,
      order2
    );

    // 往返精确还原校验
    let dec1 = decode_order_score(order1);
    let dec2 = decode_order_score(order2);
    if s1.is_infinite() {
      assert_eq!(s1.is_sign_positive(), dec1.is_sign_positive());
    } else {
      assert_eq!(s1.to_bits(), dec1.to_bits());
    }
    if s2.is_infinite() {
      assert_eq!(s2.is_sign_positive(), dec2.is_sign_positive());
    } else {
      assert_eq!(s2.to_bits(), dec2.to_bits());
    }
  }
}

#[test]
fn test_zset_crud_and_score_update() -> Result<()> {
  let tree = TempTree::new("zset_crud")?;

  // 1. 新增元素
  assert!(tree.zadd(b"player1", 100.0)?);
  assert!(tree.zadd(b"player2", -50.5)?);
  assert_eq!(tree.zscore(b"player1")?, Some(100.0));
  assert_eq!(tree.zscore(b"player2")?, Some(-50.5));
  assert_eq!(tree.zscore(b"player_none")?, None);
  assert_eq!(tree.zcard()?, 2);

  // 2. 更新分值 (返回 false，且清理旧分值索引)
  assert!(!tree.zadd(b"player1", 200.0)?);
  assert_eq!(tree.zscore(b"player1")?, Some(200.0));
  assert_eq!(tree.zcard()?, 2);

  // 验证旧分值 100.0 不再出现在范围查询中
  let mut in_old_range = 0;
  tree.zrange_by_score(90.0, 110.0, |_m, _s| {
    in_old_range += 1;
    true
  })?;
  assert_eq!(in_old_range, 0);

  // 3. 删除元素
  assert!(tree.zrem(b"player1")?);
  assert!(!tree.zrem(b"player1")?);
  assert_eq!(tree.zscore(b"player1")?, None);
  assert_eq!(tree.zcard()?, 1);

  // 4. 非法分值 NaN 拦截
  assert!(matches!(
    tree.zadd(b"player_nan", f64::NAN),
    Err(CollectionError::InvalidArgument(_))
  ));

  OK
}

#[test]
fn test_zset_range_by_score_and_count() -> Result<()> {
  let tree = TempTree::new("zset_range")?;

  let test_data = [
    (b"m_neg_inf".as_slice(), f64::NEG_INFINITY),
    (b"m_neg_100".as_slice(), -100.0),
    (b"m_neg_10".as_slice(), -10.0),
    (b"m_zero".as_slice(), 0.0),
    (b"m_pos_10".as_slice(), 10.0),
    (b"m_pos_100".as_slice(), 100.0),
    (b"m_pos_inf".as_slice(), f64::INFINITY),
  ];

  for (m, s) in test_data {
    assert!(tree.zadd(m, s)?);
  }
  assert_eq!(tree.zcard()?, 7);

  // 计数区间
  assert_eq!(tree.zcount(-15.0, 15.0)?, 3); // -10, 0, 10
  assert_eq!(tree.zcount(0.0, 100.0)?, 3); // 0, 10, 100
  assert_eq!(tree.zcount(200.0, 300.0)?, 0);
  assert_eq!(tree.zcount(50.0, 10.0)?, 0); // min > max

  // 开闭区间 zcount_ext
  assert_eq!(tree.zcount_ext(-10.0, true, 10.0, true)?, 3); // [-10, 10]: -10, 0, 10
  assert_eq!(tree.zcount_ext(-10.0, false, 10.0, true)?, 2); // (-10, 10]: 0, 10
  assert_eq!(tree.zcount_ext(-10.0, true, 10.0, false)?, 2); // [-10, 10): -10, 0
  assert_eq!(tree.zcount_ext(-10.0, false, 10.0, false)?, 1); // (-10, 10): 0
  assert_eq!(tree.zcount_ext(0.0, false, 0.0, false)?, 0); // (0, 0)
  assert_eq!(tree.zcount_ext(f64::NAN, true, 10.0, true)?, 0);
  assert_eq!(tree.zcount_ext(10.0, true, f64::NAN, true)?, 0);

  // 流式范围获取并验证顺序
  let mut items = Vec::new();
  tree.zrange_by_score(-10.0, 100.0, |m, s| {
    items.push((m.to_vec(), s));
    true
  })?;
  assert_eq!(items.len(), 4);
  assert_eq!(items[0], (b"m_neg_10".to_vec(), -10.0));
  assert_eq!(items[1], (b"m_zero".to_vec(), 0.0));
  assert_eq!(items[2], (b"m_pos_10".to_vec(), 10.0));
  assert_eq!(items[3], (b"m_pos_100".to_vec(), 100.0));

  // 同分值按 member 字典序排列
  assert!(tree.zadd(b"same_b", 42.0)?);
  assert!(tree.zadd(b"same_a", 42.0)?);
  assert!(tree.zadd(b"same_c", 42.0)?);

  let mut same_items = Vec::new();
  tree.zrange_by_score(42.0, 42.0, |m, _| {
    same_items.push(m.to_vec());
    true
  })?;
  assert_eq!(
    same_items,
    vec![b"same_a".to_vec(), b"same_b".to_vec(), b"same_c".to_vec()]
  );

  OK
}

#[test]
fn test_zset_negative_zero_boundary() -> Result<()> {
  let tree = TempTree::new("zset_neg_zero")?;

  // 插入 -0.0
  assert!(tree.zadd(b"neg_zero_member", -0.0)?);
  // 插入 +0.0 (应被识别为同一分值更新返回 false)
  assert!(!tree.zadd(b"neg_zero_member", 0.0)?);

  // 验证分值归一化返回 0.0
  let score = tree.zscore(b"neg_zero_member")?.unwrap();
  assert_eq!(score, 0.0);
  assert!(score.is_sign_positive());

  // 验证范围查询 [0.0, 0.0] 能正确检索到
  let mut results = Vec::new();
  tree.zrange_by_score(0.0, 0.0, |m, s| {
    results.push((m.to_vec(), s));
    true
  })?;
  assert_eq!(results.len(), 1);
  assert_eq!(results[0].0, b"neg_zero_member");

  // 验证 zcount(0.0, 0.0) 和 zcount(-0.0, 0.0)
  assert_eq!(tree.zcount(0.0, 0.0)?, 1);
  assert_eq!(tree.zcount(-0.0, 0.0)?, 1);

  // 验证反序 min > max 返回 0
  assert_eq!(tree.zcount(10.0, 0.0)?, 0);
  assert_eq!(tree.zrange_by_score(10.0, 0.0, |_, _| true)?, 0);

  OK
}

// ============================================================================
// 4. List 集合测试
// ============================================================================

#[test]
fn test_list_order_idx_monotonicity_and_stub_codec() -> Result<()> {
  // 序号保序单调性
  let indices = [
    i64::MIN,
    i64::MIN + 1,
    -10_000_000,
    -1,
    0,
    1,
    10_000_000,
    i64::MAX - 1,
    i64::MAX,
  ];
  for i in 0..indices.len() - 1 {
    let idx1 = indices[i];
    let idx2 = indices[i + 1];
    let order1 = order_idx_from_i64(idx1);
    let order2 = order_idx_from_i64(idx2);
    assert!(
      order1 < order2,
      "order_idx 单调性失效: {} >= {}",
      idx1,
      idx2
    );
    assert_eq!(i64_from_order_idx(order1), idx1);
    assert_eq!(i64_from_order_idx(order2), idx2);
  }

  // 51 字节存根编解码往返
  let stub = ListStub {
    range_stub: wbftree::RangeIndexStub {
      cache_size: 1048576,
      min_record_size: 4,
      max_record_size: 4096,
      ..Default::default()
    },
    head: -12345,
    tail: 67890,
  };

  let encoded = stub.encode();
  assert_eq!(encoded.len(), LIST_STUB_SIZE);
  assert_eq!(encoded.len(), 51);

  let decoded = ListStub::decode_opt(&encoded).expect("解码存根失败");
  let decoded_res = ListStub::decode(&encoded)?;
  assert_eq!(decoded_res, decoded);
  assert_eq!(decoded.head, -12345);
  assert_eq!(decoded.tail, 67890);
  assert_eq!(decoded.range_stub.cache_size, 1048576);
  assert_eq!(decoded.range_stub.min_record_size, 4);
  assert_eq!(decoded.range_stub.max_record_size, 4096);

  // 区间标准化函数校验
  assert_eq!(normalize_range(10, 0, -1), Some((0, 9)));
  assert_eq!(normalize_range(10, -3, -1), Some((7, 9)));
  assert_eq!(normalize_range(10, 5, 2), None);
  assert_eq!(normalize_range(10, 15, 20), None);
  assert_eq!(normalize_range(0, 0, 0), None);

  OK
}

#[test]
fn test_list_fifo_lifo_and_index_range() -> Result<()> {
  let tree = TempTree::new("list_fifo")?;
  let mut stub = ListStub::default();

  // 空列表操作
  assert_eq!(stub.len(), 0);
  assert!(stub.is_empty());
  assert_eq!(tree.lpop(&mut stub)?, None);
  assert_eq!(tree.rpop(&mut stub)?, None);
  assert_eq!(tree.lindex(&stub, 0)?, None);
  assert_eq!(tree.lindex(&stub, -1)?, None);
  assert!(tree.lrange(&stub, 0, -1)?.is_empty());

  // 空值容错：禁止空元素
  assert!(matches!(
    tree.lpush(&mut stub, b""),
    Err(CollectionError::EmptyValue)
  ));
  assert!(matches!(
    tree.rpush(&mut stub, b""),
    Err(CollectionError::EmptyValue)
  ));

  // RPUSH 3 个元素: ["one", "two", "three"]
  assert_eq!(tree.rpush(&mut stub, b"one")?, 1);
  assert_eq!(tree.rpush(&mut stub, b"two")?, 2);
  assert_eq!(tree.rpush(&mut stub, b"three")?, 3);
  assert_eq!(stub.len(), 3);

  // LPUSH 1 个元素: ["zero", "one", "two", "three"]
  assert_eq!(tree.lpush(&mut stub, b"zero")?, 4);
  assert_eq!(stub.len(), 4);

  // LINDEX 正向与倒数索引
  assert_eq!(tree.lindex(&stub, 0)?, Some(b"zero".to_vec()));
  assert_eq!(
    tree.lindex_callback(&stub, 0, |opt| opt.map(|v| v.len()))?,
    Some(4)
  );
  assert_eq!(tree.lindex(&stub, 1)?, Some(b"one".to_vec()));
  assert_eq!(tree.lindex(&stub, 2)?, Some(b"two".to_vec()));
  assert_eq!(tree.lindex(&stub, 3)?, Some(b"three".to_vec()));
  assert_eq!(tree.lindex(&stub, 4)?, None); // 越界
  assert!(!tree.lindex_callback(&stub, 4, |opt| opt.is_some())?);

  assert_eq!(tree.lindex(&stub, -1)?, Some(b"three".to_vec()));
  assert_eq!(tree.lindex(&stub, -2)?, Some(b"two".to_vec()));
  assert_eq!(tree.lindex(&stub, -3)?, Some(b"one".to_vec()));
  assert_eq!(tree.lindex(&stub, -4)?, Some(b"zero".to_vec()));
  assert_eq!(tree.lindex(&stub, -5)?, None); // 越界

  // LRANGE 全量读取
  assert_eq!(
    tree.lrange(&stub, 0, -1)?,
    vec![
      b"zero".to_vec(),
      b"one".to_vec(),
      b"two".to_vec(),
      b"three".to_vec()
    ]
  );

  // LRANGE 子区间
  assert_eq!(
    tree.lrange(&stub, 1, 2)?,
    vec![b"one".to_vec(), b"two".to_vec()]
  );
  assert_eq!(
    tree.lrange(&stub, -2, -1)?,
    vec![b"two".to_vec(), b"three".to_vec()]
  );
  assert_eq!(tree.lrange(&stub, 5, 10)?, Vec::<Vec<u8>>::new());

  // LPOP
  assert_eq!(tree.lpop(&mut stub)?, Some(b"zero".to_vec()));
  assert_eq!(stub.len(), 3);

  // RPOP
  assert_eq!(tree.rpop(&mut stub)?, Some(b"three".to_vec()));
  assert_eq!(stub.len(), 2);

  // 全部弹空
  assert_eq!(tree.lpop(&mut stub)?, Some(b"one".to_vec()));
  assert_eq!(tree.rpop(&mut stub)?, Some(b"two".to_vec()));
  assert_eq!(stub.len(), 0);
  assert!(stub.is_empty());
  assert_eq!(tree.lpop(&mut stub)?, None);

  // 使用 ListTree 包装结构体验证
  let mut list = ListTree::new(&tree, &mut stub);
  list.rpush(b"item1")?;
  list.lpush(b"item0")?;
  assert_eq!(list.len(), 2);
  assert_eq!(
    list.lindex_callback(0, |opt| opt.map(|v| v.to_vec()))?,
    Some(b"item0".to_vec())
  );
  assert_eq!(
    list.lrange(0, -1)?,
    vec![b"item0".to_vec(), b"item1".to_vec()]
  );
  let mut count_cb = 0;
  list.lrange_callback(0, -1, |_| {
    count_cb += 1;
    true
  })?;
  assert_eq!(count_cb, 2);
  assert_eq!(list.lpop()?, Some(b"item0".to_vec()));
  assert_eq!(list.rpop()?, Some(b"item1".to_vec()));
  assert!(list.is_empty());

  // LSET 测试
  list.rpush(b"elem0")?;
  list.rpush(b"elem1")?;
  list.rpush(b"elem2")?;
  list.lset(1, b"modified1")?;
  assert_eq!(list.lindex(1)?, Some(b"modified1".to_vec()));
  list.lset(-1, b"modified2")?;
  assert_eq!(list.lindex(2)?, Some(b"modified2".to_vec()));
  assert!(list.lset(3, b"invalid").is_err());
  assert!(list.lset(-4, b"invalid").is_err());
  assert!(list.lset(0, b"").is_err());

  // LTRIM 测试
  list.rpush(b"elem3")?;
  list.rpush(b"elem4")?;
  // list 目前有 5 个元素: [elem0, modified1, modified2, elem3, elem4]
  assert_eq!(list.len(), 5);
  // 保留 [1, 3] -> [modified1, modified2, elem3]
  assert_eq!(list.ltrim(1, 3)?, 3);
  assert_eq!(list.len(), 3);
  assert_eq!(
    list.lrange(0, -1)?,
    vec![
      b"modified1".to_vec(),
      b"modified2".to_vec(),
      b"elem3".to_vec()
    ]
  );
  // 裁空测试
  assert_eq!(list.ltrim(2, 1)?, 0);
  assert!(list.is_empty());

  OK
}

// ============================================================================
// 5. 千万级高并发与极端边界循环测试
// ============================================================================

#[test]
fn test_large_scale_and_extreme_boundary() -> Result<()> {
  // 1. ZSet 千万级分值边界与多数据循环
  let zset_tree = TempTree::new("boundary_zset")?;
  for i in 0..2000 {
    let score = (i as f64 - 1000.0) * 10000.0; // 分值覆盖 [-10,000,000, 10,000,000]
    let member = format!("user:{:05}", i);
    zset_tree.zadd(member.as_bytes(), score)?;
  }
  assert_eq!(zset_tree.zcard()?, 2000);

  // 验证千万级正负端点分值
  assert_eq!(zset_tree.zscore(b"user:00000")?, Some(-10_000_000.0));
  assert_eq!(zset_tree.zscore(b"user:01999")?, Some(9_990_000.0));

  // 统计在 [-500,000, 500,000] 范围内的数量
  let count_mid = zset_tree.zcount(-500_000.0, 500_000.0)?;
  assert!(count_mid > 80 && count_mid < 120);

  // 2. List 跨越 0 坐标边界的高频推入弹出
  let list_tree = TempTree::new("boundary_list")?;
  let mut stub = ListStub::default();
  // 从 0 往两边同时推入 1000 个
  for i in 0..1000 {
    let elem = format!("elem:{:04}", i);
    list_tree.lpush(&mut stub, elem.as_bytes())?;
    list_tree.rpush(&mut stub, elem.as_bytes())?;
  }
  assert_eq!(stub.len(), 2000);
  assert_eq!(stub.head, -1000);
  assert_eq!(stub.tail, 1000);

  // 验证正负跨度边界读取
  let first = list_tree.lindex(&stub, 0)?;
  assert_eq!(first, Some(b"elem:0999".to_vec()));
  let last = list_tree.lindex(&stub, -1)?;
  assert_eq!(last, Some(b"elem:0999".to_vec()));
  let center_left = list_tree.lindex(&stub, 999)?;
  let center_right = list_tree.lindex(&stub, 1000)?;
  assert_eq!(center_left, Some(b"elem:0000".to_vec()));
  assert_eq!(center_right, Some(b"elem:0000".to_vec()));

  // 3. Hash 批量循环与覆盖校验
  let hash_tree = TempTree::new("boundary_hash")?;
  for i in 0..2000 {
    let field = format!("f:{:05}", i);
    let val = format!("v:{:05}", i);
    hash_tree.hset(field.as_bytes(), val.as_bytes())?;
  }
  assert_eq!(hash_tree.hlen()?, 2000);

  OK
}

#[test]
fn test_hash_short_field_and_padded_value_roundtrip() -> Result<()> {
  let tree = TempTree::new("hash_padded")?;

  // 1 字节键 + 1 字节值 (field_len + 1 + value.len() = 3 < 4, 触发 TAG_PADDED)
  assert!(tree.hset(b"k", b"v")?);
  assert_eq!(tree.hget(b"k")?, Some(b"v".to_vec()));
  assert!(tree.hexists(b"k")?);

  // 覆盖为多字节值
  assert!(!tree.hset(b"k", b"longer_value")?);
  assert_eq!(tree.hget(b"k")?, Some(b"longer_value".to_vec()));

  // 再次覆盖为空值
  assert!(!tree.hset(b"k", b"")?);
  assert_eq!(tree.hget(b"k")?, Some(Vec::new()));

  OK
}

#[test]
fn test_collection_corruption_and_extreme_index_guards() -> Result<()> {
  let tree = TempTree::new("coll_guards")?;

  // 1. List 极端索引防溢出
  let mut stub = ListStub::default();
  tree.rpush(&mut stub, b"item0")?;
  tree.rpush(&mut stub, b"item1")?;
  assert_eq!(tree.llen(&stub), 2);

  let list = ListTree::new(&tree, &mut stub);
  assert_eq!(list.llen(), 2);

  // 索引极端负数与正数 (必须安全返回 None，严禁 panic)
  assert_eq!(tree.lindex(&stub, i64::MIN)?, None);
  assert_eq!(tree.lindex(&stub, i64::MAX)?, None);
  assert_eq!(tree.lindex(&stub, -100)?, None);
  assert_eq!(tree.lindex(&stub, 100)?, None);

  // 范围极端索引 (严禁溢出 panic)
  assert_eq!(tree.lrange(&stub, i64::MIN, i64::MAX)?.len(), 2);
  assert_eq!(tree.lrange(&stub, i64::MIN, 0)?.len(), 1);
  assert!(tree.lrange(&stub, 10, i64::MAX)?.is_empty());

  // 2. Hash 非法损坏标签检测 (直接向底层插入非法标签)
  let mut corrupt_key = vec![TreePrefix::HashField as u8];
  corrupt_key.extend_from_slice(b"corrupt_field");
  tree.insert(&corrupt_key, &[0xFF, 0x01, 0x02, 0x03]);
  assert!(matches!(
    tree.hget(b"corrupt_field"),
    Err(CollectionError::Corrupted(_))
  ));
  assert!(matches!(
    tree.hscan(b"", 10, |_, _| true),
    Err(CollectionError::Corrupted(_))
  ));

  // 3. ZSet 非法反查索引检测
  tree.insert(&[TreePrefix::ZSetMember as u8, b'x'], &[0x01, 0x02]); // 长度为 2 != 8
  assert!(matches!(
    tree.zscore(b"x"),
    Err(CollectionError::Corrupted(_))
  ));
  assert!(matches!(
    tree.zadd(b"x", 1.0),
    Err(CollectionError::Corrupted(_))
  ));
  assert!(matches!(
    tree.zrem(b"x"),
    Err(CollectionError::Corrupted(_))
  ));

  OK
}

// ============================================================================
// 6. RangeIndex (RiTreeOps) 与防穿透全面测试
// ============================================================================

#[test]
fn test_ri_crud_and_scanning() -> Result<()> {
  let tree = TempTree::new("ri_crud")?;

  // 1. 插入与单点读取
  assert!(tree.ri_set(b"k1", b"val1")?);
  assert_eq!(tree.ri_get(b"k1")?, Some(b"val1".to_vec()));
  assert_eq!(
    tree.ri_get_callback(b"k1", |opt| opt.map(|v| v.len()))?,
    Some(4)
  );
  assert!(tree.ri_exists(b"k1")?);
  assert_eq!(tree.ri_len()?, 1);

  // 2. 覆盖更新 (返回 false)
  assert!(!tree.ri_set(b"k1", b"val1_updated")?);
  assert_eq!(tree.ri_get(b"k1")?, Some(b"val1_updated".to_vec()));
  assert_eq!(tree.ri_len()?, 1);

  // 3. 多键插入
  assert!(tree.ri_set(b"k2", b"val2")?);
  assert!(tree.ri_set(b"k3", b"val3")?);
  assert_eq!(tree.ri_len()?, 3);

  // 4. 流式扫描
  let mut scanned = Vec::new();
  let count = tree.ri_scan(b"k1", 10, |k, v| {
    scanned.push((k.to_vec(), v.to_vec()));
    true
  })?;
  assert_eq!(count, 3);
  assert_eq!(scanned.len(), 3);
  assert_eq!(scanned[0], (b"k1".to_vec(), b"val1_updated".to_vec()));
  assert_eq!(scanned[1], (b"k2".to_vec(), b"val2".to_vec()));
  assert_eq!(scanned[2], (b"k3".to_vec(), b"val3".to_vec()));

  // 5. 闭区间范围扫描
  let mut ranged = Vec::new();
  let rcount = tree.ri_range(b"k1", b"k2", |k, v| {
    ranged.push((k.to_vec(), v.to_vec()));
    true
  })?;
  assert_eq!(rcount, 2);
  assert_eq!(ranged.len(), 2);
  assert_eq!(ranged[0].0, b"k1");
  assert_eq!(ranged[1].0, b"k2");

  // 6. 删除键
  assert!(tree.ri_del(b"k2")?);
  assert!(!tree.ri_del(b"k2")?); // 重复删除返回 false
  assert_eq!(tree.ri_get(b"k2")?, None);
  assert_eq!(tree.ri_len()?, 2);

  OK
}

#[test]
fn test_anti_penetration_binary_and_prefix_isolation() -> Result<()> {
  let tree = TempTree::new("anti_penetration")?;

  // 1. 同名键跨五大集合类型存入同一棵树，物理隔离互不干扰
  let common_key = b"same_name";
  assert!(tree.hset(common_key, b"hash_val")?);
  assert!(tree.sadd(common_key)?);
  assert!(tree.zadd(common_key, 99.5)?);
  assert!(tree.ri_set(common_key, b"ri_val")?);

  let mut stub = ListStub::default();
  tree.rpush(&mut stub, common_key)?;

  // 验证各自的数据与长度隔离
  assert_eq!(tree.hget(common_key)?, Some(b"hash_val".to_vec()));
  assert!(tree.sismember(common_key)?);
  assert_eq!(tree.zscore(common_key)?, Some(99.5));
  assert_eq!(tree.ri_get(common_key)?, Some(b"ri_val".to_vec()));
  assert_eq!(tree.lindex(&stub, 0)?, Some(common_key.to_vec()));

  assert_eq!(tree.hlen()?, 1);
  assert_eq!(tree.scard()?, 1);
  assert_eq!(tree.zcard()?, 1);
  assert_eq!(tree.ri_len()?, 1);
  assert_eq!(tree.llen(&stub), 1);

  // 2. 用户键包含二进制 \0 与特殊前缀字节 (0x01..=0x06)，杜绝前缀穿透
  let special_keys: &[&[u8]] = &[
    b"\0",
    b"\0\0\0\0",
    b"\x01",
    b"\x02",
    b"\x03",
    b"\x04",
    b"\x05",
    b"\x06",
    b"\x01prefix_trick",
    b"\x05range_trick",
    b"user\0name\0with\0zeros",
  ];

  for &k in special_keys {
    assert!(tree.hset(k, b"h_val")?);
    assert!(tree.sadd(k)?);
    assert!(tree.zadd(k, 123.0)?);
    assert!(tree.ri_set(k, b"r_val")?);

    assert_eq!(tree.hget(k)?, Some(b"h_val".to_vec()));
    assert!(tree.sismember(k)?);
    assert_eq!(tree.zscore(k)?, Some(123.0));
    assert_eq!(tree.ri_get(k)?, Some(b"r_val".to_vec()));
  }

  // 3. 扫描隔离验证：hscan 不会扫到 set / zset / ri 的键
  let mut h_scanned_keys = Vec::new();
  tree.hscan(b"", 100, |k, _| {
    h_scanned_keys.push(k.to_vec());
    true
  })?;
  assert_eq!(h_scanned_keys.len(), 1 + special_keys.len());
  assert!(h_scanned_keys.contains(&common_key.to_vec()));

  let mut s_scanned_keys = Vec::new();
  tree.sscan(b"", 100, |k| {
    s_scanned_keys.push(k.to_vec());
    true
  })?;
  assert_eq!(s_scanned_keys.len(), 1 + special_keys.len());
  assert!(s_scanned_keys.contains(&common_key.to_vec()));

  let mut ri_scanned_keys = Vec::new();
  tree.ri_scan(b"", 100, |k, _| {
    ri_scanned_keys.push(k.to_vec());
    true
  })?;
  assert_eq!(ri_scanned_keys.len(), 1 + special_keys.len());
  assert!(ri_scanned_keys.contains(&common_key.to_vec()));

  OK
}

#[test]
fn test_zset_extended_zrange_and_iter_all() -> Result<()> {
  let tree = TempTree::new("zset_ext")?;

  // 插入 5 个成员
  tree.zadd(b"m1", 10.0)?;
  tree.zadd(b"m2", 20.0)?;
  tree.zadd(b"m3", 30.0)?;
  tree.zadd(b"m4", 40.0)?;
  tree.zadd(b"m5", 50.0)?;

  // 1. zrange_by_score_ext: 开区间 (10.0, 40.0) -> m2, m3
  let mut items = Vec::new();
  tree.zrange_by_score_ext(
    wcol::ZRangeByScoreOpt::new(10.0, false, 40.0, false, 0, usize::MAX),
    |m, s| {
      items.push((m.to_vec(), s));
      true
    },
  )?;
  assert_eq!(items.len(), 2);
  assert_eq!(items[0], (b"m2".to_vec(), 20.0));
  assert_eq!(items[1], (b"m3".to_vec(), 30.0));

  // 2. LIMIT offset 1, limit 2
  let mut items = Vec::new();
  tree.zrange_by_score_ext(
    wcol::ZRangeByScoreOpt::new(10.0, true, 50.0, true, 1, 2),
    |m, s| {
      items.push((m.to_vec(), s));
      true
    },
  )?;
  assert_eq!(items.len(), 2);
  assert_eq!(items[0], (b"m2".to_vec(), 20.0));
  assert_eq!(items[1], (b"m3".to_vec(), 30.0));

  // 3. zrange_by_index [1, 3] -> m2, m3, m4
  let mut items = Vec::new();
  tree.zrange_by_index(1, 3, |m, s| {
    items.push((m.to_vec(), s));
    true
  })?;
  assert_eq!(items.len(), 3);
  assert_eq!(items[0], (b"m2".to_vec(), 20.0));
  assert_eq!(items[1], (b"m3".to_vec(), 30.0));
  assert_eq!(items[2], (b"m4".to_vec(), 40.0));

  // 4. ziter_all 导出全部成员供自动合并使用
  let mut all_members = Vec::new();
  let count = tree.ziter_all(|m, s| {
    all_members.push((m.to_vec(), s));
    true
  })?;
  assert_eq!(count, 5);
  assert_eq!(all_members.len(), 5);
  // 反查索引顺序（按 member 字典序）
  assert!(all_members.iter().any(|(m, _)| m == b"m1"));
  assert!(all_members.iter().any(|(m, _)| m == b"m5"));

  OK
}
