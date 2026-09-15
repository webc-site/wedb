use std::mem::{align_of, size_of};

use aok::{OK, Void};
use log::info;
use windex::{
  DATA_ENTRIES, ENTRIES_PER_BUCKET, Error, HashBucket, HashBucketEntry, HashIndex, OVERFLOW_INDEX,
  OverflowPool,
};

/// 验证 HashBucket 严格 64 字节 Cacheline 对齐与内存步长
/// 对标 Tsavorite `HashBucket.cs`
#[test]
fn test_cacheline_alignment_and_stride() -> Void {
  info!("验证 HashBucket 严格 64 字节 Cacheline 对齐与内存步长");

  assert_eq!(size_of::<HashBucket>(), 64);
  assert_eq!(align_of::<HashBucket>(), 64);
  assert_eq!(size_of::<HashBucketEntry>(), 8);
  assert_eq!(align_of::<HashBucketEntry>(), 8);
  assert_eq!(ENTRIES_PER_BUCKET, 8);
  assert_eq!(OVERFLOW_INDEX, 7);
  assert_eq!(DATA_ENTRIES, 7);

  // 验证主桶切片物理地址对齐与步长
  let index = HashIndex::new(32)?;
  assert_eq!(index.buckets.len(), 32);

  for (i, b) in index.buckets.iter().enumerate() {
    let ptr = b as *const HashBucket as usize;
    assert_eq!(
      ptr % 64,
      0,
      "主桶 {i} 的内存地址 {ptr:#x} 未满足 64 字节 Cacheline 对齐"
    );
  }

  for pair in index.buckets.windows(2) {
    let p1 = &pair[0] as *const HashBucket as usize;
    let p2 = &pair[1] as *const HashBucket as usize;
    assert_eq!(p2 - p1, 64, "相邻主桶步长必须严格为 64 字节");
  }

  // 验证 OverflowPool 分块分配的溢出桶物理对齐
  let pool = OverflowPool::new();
  let mut ids = Vec::with_capacity(2048);
  for _ in 0..2048 {
    ids.push(pool.allocate()?);
  }

  for &id in &ids {
    let bucket = pool.get(id).expect("已分配的溢出桶必须能成功读取");
    let ptr = bucket as *const HashBucket as usize;
    assert_eq!(
      ptr % 64,
      0,
      "溢出桶 {id} 地址 {ptr:#x} 未满足 64 字节 Cacheline 对齐"
    );
  }

  // 块内相邻桶步长断言（连续 64 字节）
  for i in 1..1023 {
    let b1 = pool.get(i).unwrap();
    let b2 = pool.get(i + 1).unwrap();
    let p1 = b1 as *const HashBucket as usize;
    let p2 = b2 as *const HashBucket as usize;
    assert_eq!(p2 - p1, 64, "块内相邻溢出桶步长必须为 64 字节");
  }

  OK
}

/// 验证 HashBucketEntry 48 位地址、15 位 Tag 与 1 位试探标记编解码
/// 对标 Tsavorite `HashBucketEntry.cs`
#[test]
fn test_entry_bit_packing_and_tentative() -> Void {
  info!("验证 HashBucketEntry 位排布、Tag 提取与 Tentative 语义");

  let test_address = 0x0000_1234_5678_9ABC_u64;
  let test_tag = 0x5A5A_u16 & 0x7FFF;
  let entry_committed = HashBucketEntry::new(test_address, test_tag, false);

  assert_eq!(entry_committed.address(), test_address);
  assert_eq!(entry_committed.tag(), test_tag);
  assert!(!entry_committed.is_tentative());
  assert!(!entry_committed.is_empty());
  assert!(entry_committed.is_valid());

  // 底层原始 64 位值验证
  let expected_raw =
    (test_address & HashBucketEntry::ADDRESS_MASK) | (((test_tag as u64) & 0x7FFF) << 48);
  assert_eq!(entry_committed.as_raw(), expected_raw);

  // 验证试探性标记位（第 63 位）
  let entry_tentative = entry_committed.with_tentative(true);
  let expected_tentative_raw = expected_raw | (1u64 << 63);
  assert_eq!(entry_tentative.as_raw(), expected_tentative_raw);
  assert_eq!(entry_tentative.address(), test_address);
  assert_eq!(entry_tentative.tag(), test_tag);
  assert!(entry_tentative.is_tentative());
  assert!(!entry_tentative.is_valid());

  // 验证 with_tag 替换
  let new_tag = 0x1234_u16;
  let entry_new_tag = entry_tentative.with_tag(new_tag);
  assert_eq!(entry_new_tag.tag(), new_tag);
  assert_eq!(entry_new_tag.address(), test_address);
  assert!(entry_new_tag.is_tentative());

  // 48 位最大地址边界验证（256TB）
  let max_addr = HashBucketEntry::ADDRESS_MASK;
  let max_tag = 0x7FFF_u16;
  let max_entry = HashBucketEntry::new(max_addr, max_tag, false);
  assert_eq!(max_entry.address(), max_addr);
  assert_eq!(max_entry.tag(), max_tag);
  assert_eq!(max_entry.as_raw(), 0x7FFF_FFFF_FFFF_FFFF);

  // 对标 Tsavorite `HashBucketEntry.GetTag(long hashCode)`
  let hash_samples = [
    0x0000_0000_0000_0000_u64,
    0xFFFF_FFFF_FFFF_FFFF_u64,
    0x8000_0000_0000_0000_u64,
    0x1234_5678_9ABC_DEF0_u64,
    0xFEDC_BA98_7654_3210_u64,
    0xABCD_EF01_2345_6789_u64,
  ];

  for &h in &hash_samples {
    let expected = ((h >> 49) & 0x7FFF) as u16;
    assert_eq!(HashBucketEntry::tag_from_hash(h), expected);
  }

  OK
}

/// 极端 Tag (0 / 0x7FFF) 与逻辑地址 (1 / 2^48 - 1) 边界全组合对抗测试
/// 对标 Tsavorite `HashBucketEntry.cs` 与 `LogAddress`
#[test]
fn test_extreme_tag_and_address_boundaries() -> Void {
  info!("极端 Tag (0 / 0x7FFF) 与逻辑地址 (1 / 2^48 - 1) 边界全组合对抗测试");

  let max_addr = HashBucketEntry::ADDRESS_MASK;
  let min_addr = 1_u64;
  let min_tag = 0_u16;
  let max_tag = HashBucketEntry::TAG_MASK as u16;

  let boundary_cases = [
    (min_addr, min_tag, false),
    (min_addr, min_tag, true),
    (max_addr, min_tag, false),
    (max_addr, min_tag, true),
    (min_addr, max_tag, false),
    (min_addr, max_tag, true),
    (max_addr, max_tag, false),
    (max_addr, max_tag, true),
  ];

  for &(addr, tag, tent) in &boundary_cases {
    let entry = HashBucketEntry::new(addr, tag, tent);
    assert_eq!(entry.address(), addr, "地址解码失真: addr={addr:#x}");
    assert_eq!(entry.tag(), tag, "Tag 解码失真: tag={tag:#x}");
    assert_eq!(entry.is_tentative(), tent, "Tentative 解码失真");
    assert!(!entry.is_empty(), "边界非零条目绝不能判定为 empty");
    if !tent {
      assert!(entry.is_valid(), "已提交的边界条目必须有效");
    }
  }

  // 验证边界地址的索引级输入拦截
  let index = HashIndex::new(8)?;

  let zero_res = index.insert(b"zero_address_key", HashBucketEntry::INVALID_ADDRESS);
  assert!(
    matches!(zero_res, Err(Error::InvalidAddress(0))),
    "Address = 0 必须返回 InvalidAddress(0)"
  );
  assert!(!index.update_address(b"any_key", 100, HashBucketEntry::INVALID_ADDRESS));
  assert!(!index.update_address(b"any_key", HashBucketEntry::INVALID_ADDRESS, 200));
  assert!(!index.delete(b"any_key", HashBucketEntry::INVALID_ADDRESS));

  let overflow_addr = 1u64 << 48;
  let ov_res = index.insert(b"overflow_key", overflow_addr);
  assert!(
    matches!(ov_res, Err(Error::AddressOverflow(a)) if a == overflow_addr),
    "Address = 2^48 必须返回 AddressOverflow"
  );
  assert!(!index.update_address(b"any_key", 100, overflow_addr));

  // 极端边界值插入索引验证
  let test_pairs = [
    (b"boundary_k1".as_slice(), min_addr),
    (b"boundary_k2".as_slice(), max_addr),
    (b"boundary_k3".as_slice(), 0x0000_1234_5678_9ABC_u64),
    (b"boundary_k4".as_slice(), 0x0000_EDCB_A987_6543_u64),
  ];

  for &(k, addr) in &test_pairs {
    index.insert(k, addr)?;
    let res = index.lookup(k);
    assert!(res.contains(&addr), "边界地址插入后 lookup 必须能定位");
  }

  // RCU 更新至另一个极端边界
  assert!(index.update_address(b"boundary_k1", min_addr, max_addr));
  assert_eq!(index.lookup(b"boundary_k1"), vec![max_addr]);

  assert!(index.update_address(b"boundary_k2", max_addr, min_addr));
  assert_eq!(index.lookup(b"boundary_k2"), vec![min_addr]);

  // 删除
  assert!(index.delete(b"boundary_k1", max_addr));
  assert!(!index.lookup(b"boundary_k1").contains(&max_addr));
  assert!(index.delete(b"boundary_k2", min_addr));
  assert!(!index.lookup(b"boundary_k2").contains(&min_addr));

  // 底层 Bucket 极端 Tag (Tag 0 与 Tag 0x7FFF) 碰撞隔离验证
  let raw_bucket = HashBucket::new();
  assert!(raw_bucket.try_insert(0, 0, 100));
  assert!(raw_bucket.try_insert(1, 0, 200));
  assert!(raw_bucket.try_insert(2, 0x7FFF, 300));
  assert!(raw_bucket.try_insert(3, 0x7FFF, 400));

  assert_eq!(raw_bucket.find_entry_by_address(0, 100).unwrap().0, 0);
  assert_eq!(raw_bucket.find_entry_by_address(0, 200).unwrap().0, 1);
  assert_eq!(raw_bucket.find_entry_by_address(0x7FFF, 300).unwrap().0, 2);
  assert_eq!(raw_bucket.find_entry_by_address(0x7FFF, 400).unwrap().0, 3);

  assert!(raw_bucket.find_entry_by_address(0, 300).is_none());
  assert!(raw_bucket.find_entry_by_address(0x7FFF, 100).is_none());
  assert!(raw_bucket.find_entry_by_address(0x1234, 100).is_none());

  OK
}

/// 验证 Tag 匹配的防御性掩码行为
/// 对标 Tsavorite `HashBucketEntry.cs`
#[test]
fn test_matches_tag_mask_defense() -> Void {
  info!("验证 Tag 匹配的防御性掩码行为");

  let addr = 0x1234_5678_u64;
  let tag_15 = 0x3ABC_u16;
  let entry = HashBucketEntry::new(addr, tag_15, false);

  // 即使外部传入高位带杂质的 tag（如 0x8000 标记位未清除），也能正确匹配
  let dirty_tag_15 = tag_15 | 0x8000;
  assert!(entry.matches_tag(dirty_tag_15));
  assert!(entry.matches_tag(tag_15));
  assert!(!entry.matches_tag(tag_15 + 1));

  // 试探态条目不可匹配
  let tent_entry = entry.with_tentative(true);
  assert!(!tent_entry.matches_tag(tag_15));

  OK
}

/// 验证非法容量与地址越界错误处理
/// 对标 Tsavorite 初始化与输入边界验证
#[test]
fn test_invalid_capacity_and_overflow_limit() -> Void {
  info!("验证非法容量与地址越界错误处理");

  // 非 2 的幂
  assert!(
    matches!(HashIndex::new(10), Err(Error::InvalidBucketCount(10))),
    "容量为 10 应返回 InvalidBucketCount 错误"
  );

  // 容量为 0
  assert!(
    matches!(HashIndex::new(0), Err(Error::InvalidBucketCount(0))),
    "容量为 0 应返回 InvalidBucketCount 错误"
  );

  let index = HashIndex::new(16)?;
  // 插入保留的无效逻辑地址 0
  assert!(
    matches!(
      index.insert(b"test_zero_addr", 0),
      Err(Error::InvalidAddress(0))
    ),
    "逻辑地址 0 应返回 InvalidAddress 错误"
  );

  // 插入超过 48 位的非法逻辑地址
  let bad_addr = 1u64 << 49;
  assert!(
    matches!(
      index.insert(b"test_overflow_key", bad_addr),
      Err(Error::AddressOverflow(a)) if a == bad_addr
    ),
    "超过 48 位地址应返回 AddressOverflow 错误"
  );

  OK
}
