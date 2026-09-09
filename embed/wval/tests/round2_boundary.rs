use std::{
  cmp::Ordering,
  mem::{align_of, size_of},
  ops::Deref,
};

use aok::{OK, Void};
use log::info;
use wrecord::{
  ADDRESS_MASK, Error, HEADER_SIZE, RecordHeader, RecordMut, RecordRef, fast_key_eq,
  try_encode_to_vec,
};
use wval::{
  CollectionType, CompactHash, CompactHashCodec, CompactMetaValue, CompactSet, CompactSetCodec,
  CompactZSet, KeyTag, META_VALUE_SIZE, MetaValue, StorageEncoding, SubKeyBuf, ZSetEntryRef,
  ZSetSubKeyBuf,
};

#[ctor::ctor(unsafe)]
fn _log_init() {
  log_init::init();
}

// ============================================================================
// 1. SIMD fast_key_eq 极端边界与非对齐指针专项测试
// ============================================================================
#[test]
fn test_round2_simd_fast_key_eq_unaligned_and_overlapping() -> Void {
  info!("开始测试: SIMD fast_key_eq 极端边界、非对齐指针与重叠切片");

  // 1. 空切片边界测试（0B）
  assert!(fast_key_eq(b"", b""));
  let empty1: &[u8] = &[];
  let empty2: &[u8] = &[];
  assert!(fast_key_eq(empty1, empty2));
  assert!(fast_key_eq(&[], b""));

  // 2. 非对齐内存切片测试（从奇数地址偏移切片）
  let mut raw_buf = vec![0u8; 256];
  for (i, b) in raw_buf.iter_mut().enumerate() {
    *b = (i % 251) as u8;
  }
  // 奇数偏移 1 vs 奇数偏移 1
  assert!(fast_key_eq(&raw_buf[1..17], &raw_buf[1..17]));
  // 奇数偏移 3 vs 奇数偏移 19（内容相同）
  let mut mirror_buf = raw_buf.clone();
  assert!(fast_key_eq(&raw_buf[3..35], &mirror_buf[3..35]));
  // 仅在最后一个字节不同
  mirror_buf[34] ^= 0x01;
  assert!(!fast_key_eq(&raw_buf[3..35], &mirror_buf[3..35]));

  // 3. 所有步长阶梯（0, 1, 2, 3, 4, 7, 8, 9, 15, 16, 17, 31, 32, 33, 63, 64, 65, 127, 128, 129）
  let step_lengths = [
    0, 1, 2, 3, 4, 7, 8, 9, 15, 16, 17, 31, 32, 33, 63, 64, 65, 127, 128, 129,
  ];
  for &len in &step_lengths {
    let a = vec![0xA5u8; len];
    let b = vec![0xA5u8; len];
    assert!(
      fast_key_eq(&a, &b),
      "等长相同数据 fast_key_eq 必须返回 true, len={len}"
    );

    if len > 0 {
      // 头部变异
      let mut c_head = a.clone();
      c_head[0] ^= 0xFF;
      assert!(!fast_key_eq(&a, &c_head), "头部变异未检测出, len={len}");

      // 尾部变异
      let mut c_tail = a.clone();
      c_tail[len - 1] ^= 0xFF;
      assert!(!fast_key_eq(&a, &c_tail), "尾部变异未检测出, len={len}");

      // 中部变异
      let mut c_mid = a.clone();
      c_mid[len / 2] ^= 0xFF;
      assert!(!fast_key_eq(&a, &c_mid), "中部变异未检测出, len={len}");
    }
  }

  info!("SIMD fast_key_eq 极端边界、非对齐指针与重叠切片测试通过");
  OK
}

// ============================================================================
// 2. RecordHeader / RecordMut 探针与生命周期一致性测试
// ============================================================================
#[test]
fn test_round2_record_header_and_mut_in_place_lifecycle() -> Void {
  info!("开始测试: RecordHeader / RecordMut 探针与生命周期一致性");

  // 1. 定长排布与内存对齐
  assert_eq!(size_of::<RecordHeader>(), 16);
  assert_eq!(align_of::<RecordHeader>(), 8);
  assert_eq!(HEADER_SIZE, 16);

  // 2. 地址掩码与溢出检查
  let max_addr = ADDRESS_MASK;
  let mut header = RecordHeader::new(max_addr, 10, 20, false)?;
  assert_eq!(header.address(), max_addr);
  assert!(!header.is_tombstone());

  // 超过 48 位地址拦截
  assert_eq!(
    RecordHeader::new(max_addr + 1, 10, 20, false),
    Err(Error::AddressOverflow(max_addr + 1))
  );
  assert_eq!(
    header.set_address(max_addr + 1),
    Err(Error::AddressOverflow(max_addr + 1))
  );

  // 3. 墓碑翻转与 can_update_in_place 探针状态联动
  assert!(header.can_update_in_place(20));
  assert!(!header.can_update_in_place(19));
  assert!(!header.can_update_in_place(21));

  // 翻转为墓碑 -> 禁止原位更新
  assert!(header.flip_tombstone());
  assert!(header.is_tombstone());
  assert!(!header.can_update_in_place(20));
  assert_eq!(header.address(), max_addr);

  // 再次翻转清除墓碑 -> 恢复原位更新
  assert!(!header.flip_tombstone());
  assert!(!header.is_tombstone());
  assert!(header.can_update_in_place(20));

  // 4. RecordMut 原位更新与生命周期归还 (into_slice, into_ref, as_ref)
  let key = b"bench_counter";
  let initial_val = b"0000000100";
  let mut buf = try_encode_to_vec(0x1000, key, initial_val, false)?;

  {
    let mut rec_mut = RecordMut::from_slice_mut(&mut buf)?;
    assert_eq!(rec_mut.key(), key);
    assert_eq!(rec_mut.value(), initial_val);
    assert!(rec_mut.can_update_in_place(10));

    // 原位更新值
    let new_val = b"0000000200";
    rec_mut.update_value_in_place(new_val)?;
    assert_eq!(rec_mut.value(), new_val);

    // 转换为 RecordRef (as_ref)
    let ref_view = rec_mut.as_ref();
    assert_eq!(ref_view.key(), key);
    assert_eq!(ref_view.value(), new_val);
    assert_eq!(ref_view.prev_address(), 0x1000);

    // 归还为切片 (into_slice)
    let raw_slice = rec_mut.into_slice();
    assert_eq!(raw_slice.len(), HEADER_SIZE + key.len() + 10);
  }

  // 5. 重新以 RecordMut 构造并消耗为拥有原生命周期的 RecordRef (into_ref)
  {
    let rec_mut = RecordMut::from_slice_mut(&mut buf)?;
    let rec_ref: RecordRef<'_> = rec_mut.into_ref();
    assert_eq!(rec_ref.key(), key);
    assert_eq!(rec_ref.value(), b"0000000200");
    assert_eq!(rec_ref.total_size(), buf.len());
  }

  info!("RecordHeader / RecordMut 探针与生命周期一致性测试通过");
  OK
}

// ============================================================================
// 3. MetaValue 与 CompactMetaValue 全局大端序与 bitcode 往返测试
// ============================================================================
#[test]
fn test_round2_meta_value_and_compact_meta_value() -> Void {
  info!("开始测试: MetaValue 与 CompactMetaValue 全局大端序与 bitcode 往返");

  // 1. MetaValue (32B)
  assert_eq!(size_of::<MetaValue>(), 32);
  assert_eq!(size_of::<MetaValue>(), META_VALUE_SIZE);
  assert_eq!(align_of::<MetaValue>(), 8);

  let meta = MetaValue::new(
    0x0102_0304_0506_0708,
    CollectionType::ZSet,
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
  assert_eq!(raw_bytes[8], CollectionType::ZSet.as_u8());
  assert_eq!(raw_bytes[9], StorageEncoding::Flattened.as_u8());
  assert_eq!(
    &raw_bytes[16..24],
    &[0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77, 0x88]
  );
  assert_eq!(
    u64::from_be_bytes(raw_bytes[24..32].try_into().unwrap()),
    500
  );

  // bitcode 往返
  let bc = meta.encode_bitcode();
  let decoded_meta = MetaValue::decode_bitcode(&bc)?;
  assert_eq!(meta, decoded_meta);

  // 2. CompactMetaValue (16B)
  let cmeta = CompactMetaValue::new(
    CollectionType::Hash,
    StorageEncoding::Compact,
    42,
    1_800_000_000_000,
  );
  let cbytes = cmeta.to_bytes();
  assert_eq!(cbytes.len(), 16);
  assert_eq!(cbytes[0], CollectionType::Hash.as_u8());
  assert_eq!(cbytes[1], StorageEncoding::Compact.as_u8());
  assert_eq!(u32::from_be_bytes(cbytes[4..8].try_into().unwrap()), 42);
  assert_eq!(
    u64::from_be_bytes(cbytes[8..16].try_into().unwrap()),
    1_800_000_000_000
  );

  let c_bc = cmeta.encode_bitcode();
  let decoded_cmeta = CompactMetaValue::decode_bitcode(&c_bc)?;
  assert_eq!(cmeta, decoded_cmeta);

  info!("MetaValue 与 CompactMetaValue 全局大端序与 bitcode 往返测试通过");
  OK
}

// ============================================================================
// 4. SubKeyBuf 与 ZSetSubKeyBuf 栈优先与堆回退契约测试
// ============================================================================
#[test]
fn test_round2_subkey_buf_stack_heap_contracts() -> Void {
  info!("开始测试: SubKeyBuf 与 ZSetSubKeyBuf 栈优先与堆回退契约");

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

  // 3. ZSetSubKeyBuf 精确边界 (ZSET_SUBKEY_STACK_CAP = 128) // Member key: 17 header + 111 = 128 bytes
  let zstack = ZSetSubKeyBuf::from_member(1, 1, &[b'z'; 111])?;
  assert!(zstack.is_stack());
  assert_eq!(zstack.len(), 128);

  // Member key: 17 header + 112 = 129 bytes
  let zheap = ZSetSubKeyBuf::from_member(1, 1, &[b'z'; 112])?;
  assert!(zheap.is_heap());
  assert_eq!(zheap.len(), 129);

  // Score key: 25 header + 103 = 128 bytes
  let zscore_stack = ZSetSubKeyBuf::from_score(1, 1, 3.25, &[b's'; 103])?;
  assert!(zscore_stack.is_stack());
  assert_eq!(zscore_stack.len(), 128);

  let zscore_heap = ZSetSubKeyBuf::from_score(1, 1, 3.25, &[b's'; 104])?;
  assert!(zscore_heap.is_heap());
  assert_eq!(zscore_heap.len(), 129);

  info!("SubKeyBuf 与 ZSetSubKeyBuf 栈优先与堆回退契约测试通过");
  OK
}

// ============================================================================
// 5. CompactHash 重复键批量编码与原地淘汰测试
// ============================================================================
#[test]
fn test_round2_compact_hash_duplicate_keys_and_large_scale() -> Void {
  info!("开始测试: CompactHash 重复键批量编码与原地淘汰");

  // 1. 批量编码包含大量重复键
  let mut entries: Vec<(&[u8], &[u8], Option<u64>)> = Vec::new();
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

  // 2. 原地 purge_expired 压缩测试
  let now = 1_000_000_u64;
  hash.set_field(b"f0", b"expired", Some(now - 10))?;
  hash.set_field(b"f1", b"alive", Some(now + 100))?;
  hash.set_field(b"f2", b"expired_exact", Some(now))?;
  hash.set_field(b"f3", b"no_expire", None)?;

  let purged = hash.purge_expired(now)?;
  assert_eq!(purged, 2); // f0 和 f2 已过期
  assert_eq!(hash.len(), 3);
  assert_eq!(hash.find_field(b"f0"), None);
  assert_eq!(hash.find_field(b"f1"), Some(b"alive".as_slice()));
  assert_eq!(hash.find_field(b"f2"), None);
  assert_eq!(hash.find_field(b"f3"), Some(b"no_expire".as_slice()));

  info!("CompactHash 重复键批量编码与原地淘汰测试通过");
  OK
}

// ============================================================================
// 6. CompactSet 大规模堆回退 (> 512 元素) 测试
// ============================================================================
#[test]
fn test_round2_compact_set_large_scale_heap_fallback() -> Void {
  info!("开始测试: CompactSet 大规模堆回退 (> 512 元素) 与二分查找");

  let mut set = CompactSet::new();
  const TOTAL: usize = 600; // 超过 STACK_CAP 512，触发 heap_offsets

  for i in 0..TOTAL {
    let member = format!("key_{:05}", i);
    assert!(set.insert(member.as_bytes())?);
  }
  assert_eq!(set.len(), TOTAL);

  // 再次插入重复项 -> 返回 false
  assert!(!set.insert(b"key_00100")?);
  assert_eq!(set.len(), TOTAL);

  // 二分查找测试
  assert_eq!(set.binary_search(b"key_00000")?, Ok(0));
  assert_eq!(set.binary_search(b"key_00599")?, Ok(599));
  assert!(set.contains(b"key_00350"));
  assert!(!set.contains(b"key_99999"));

  // 删除元素
  assert!(set.remove(b"key_00350")?);
  assert_eq!(set.len(), TOTAL - 1);
  assert!(!set.contains(b"key_00350"));

  // 验证有序单调递增性
  CompactSetCodec::validate(set.as_slice())?;

  info!("CompactSet 大规模堆回退与二分查找测试通过");
  OK
}

// ============================================================================
// 7. CompactZSet 大规模堆回退 (> 256 元素) 与精细区间选项测试
// ============================================================================
#[test]
fn test_round2_compact_zset_large_scale_heap_fallback_and_ranges() -> Void {
  info!("开始测试: CompactZSet 大规模堆回退 (> 256 元素) 与精细区间控制");

  let mut zset = CompactZSet::new();
  const TOTAL: usize = 300; // 超过 STACK_CAP 256，触发 heap_offsets

  for i in 0..TOTAL {
    let member = format!("player_{:04}", i);
    let score = i as f64;
    assert!(zset.insert(score, member.as_bytes())?);
  }
  assert_eq!(zset.len(), TOTAL);

  // 1. 开闭区间组合测试
  // [10.0, 20.0] -> 11 个元素 (10..=20)
  assert_eq!(zset.count_score_range(10.0, true, 20.0, true), 11);
  // (10.0, 20.0] -> 10 个元素 (11..=20)
  assert_eq!(zset.count_score_range(10.0, false, 20.0, true), 10);
  // [10.0, 20.0) -> 10 个元素 (10..20)
  assert_eq!(zset.count_score_range(10.0, true, 20.0, false), 10);
  // (10.0, 20.0) -> 9 个元素 (11..20)
  assert_eq!(zset.count_score_range(10.0, false, 20.0, false), 9);

  // 2. 单点退化区间
  // [15.0, 15.0] -> 1 个元素
  assert_eq!(zset.count_score_range(15.0, true, 15.0, true), 1);
  // (15.0, 15.0] -> 0 个元素
  assert_eq!(zset.count_score_range(15.0, false, 15.0, true), 0);
  // [15.0, 15.0) -> 0 个元素
  assert_eq!(zset.count_score_range(15.0, true, 15.0, false), 0);
  // (15.0, 15.0) -> 0 个元素
  assert_eq!(zset.count_score_range(15.0, false, 15.0, false), 0);

  // 3. 反向与越界区间
  assert_eq!(zset.count_score_range(50.0, true, 20.0, true), 0);
  assert_eq!(zset.count_score_range(500.0, true, 600.0, true), 0);

  // 4. 流式切片迭代器与 options 联动
  let range_res: Vec<ZSetEntryRef> = zset.range_with_options(10.0, false, 13.0, true).collect();
  assert_eq!(range_res.len(), 3); // 11.0, 12.0, 13.0
  assert_eq!(range_res[0].score, 11.0);
  assert_eq!(range_res[1].score, 12.0);
  assert_eq!(range_res[2].score, 13.0);

  // 5. 排名与分值查询在大集合下的正确性
  assert_eq!(zset.rank_of(b"player_0000"), Some(0));
  assert_eq!(zset.rank_of(b"player_0299"), Some(299));
  assert_eq!(zset.score_of(b"player_0150"), Some(150.0));
  assert_eq!(zset.key_at_rank(150), Some(b"player_0150".as_slice()));

  // 6. 删除元素
  assert!(zset.remove(b"player_0150")?);
  assert_eq!(zset.len(), TOTAL - 1);
  assert_eq!(zset.rank_of(b"player_0150"), None);

  info!("CompactZSet 大规模堆回退与精细区间控制测试通过");
  OK
}
