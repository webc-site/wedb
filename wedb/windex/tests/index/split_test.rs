//! 自研依据: 增量分裂（C# 对应 libs/storage/Tsavorite/cs/src/core/Index/Tsavorite/Implementation/SplitBlocks）
use std::sync::atomic::Ordering;

use aok::{OK, Void};
use windex::{
  DATA_ENTRIES, HashBucket, HashBucketEntry, HashIndex, chunk_count, chunk_offset_for_hash,
  split_chunk, split_single_bucket,
};

use super::support::HashIndexTestOps;

/// 沿指定主桶下标的桶链（含溢出链）检查是否存在承载该逻辑地址的槽位
fn bucket_chain_has_addr(index: &HashIndex, bucket_idx: usize, addr: u64) -> bool {
  let mut bucket: &HashBucket = index.bucket(bucket_idx);
  loop {
    if bucket.entries[..DATA_ENTRIES].iter().any(|e| {
      let raw = e.load(Ordering::Acquire);
      raw != 0 && HashBucketEntry::from_raw(raw).address() == addr
    }) {
      return true;
    }
    let next = bucket.overflow_index();
    if next == 0 {
      return false;
    }
    bucket = index
      .overflow_pool
      .get(next)
      .expect("链上挂载的溢出桶必须可解析");
  }
}

#[test]
fn test_split_bucket_and_chunk() -> Void {
  let old_size = 1024;
  let new_size = 2048;
  let old_index = HashIndex::new(old_size)?;
  let new_index = HashIndex::new(new_size)?;

  let key1 = b"key_left_branch";
  let hash1 = HashIndex::hash_key(key1);
  let addr1 = 100u64;
  old_index.insert(key1, addr1)?;

  let key2 = b"key_right_branch_2";
  let hash2 = HashIndex::hash_key(key2);
  let addr2 = 200u64;
  old_index.insert(key2, addr2)?;

  let num_chunks = chunk_count(old_size);
  assert_eq!(num_chunks, 1);

  let offset1 = chunk_offset_for_hash(hash1, old_index.mask);
  assert_eq!(offset1, 0);

  split_chunk(
    &old_index,
    &new_index,
    0,
    num_chunks,
    0,
    |addr| {
      if addr == addr1 {
        Some((hash1, 0))
      } else if addr == addr2 {
        Some((hash2, 0))
      } else {
        None
      }
    },
    |_prev, _bit| None,
  )?;

  assert_eq!(new_index.find_tag(key1), Some(addr1));
  assert_eq!(new_index.find_tag(key2), Some(addr2));

  OK
}

#[test]
fn test_split_trace_back() -> Void {
  let old_size = 512;
  let new_size = 1024;
  let old_index = HashIndex::new(old_size)?;
  let new_index = HashIndex::new(new_size)?;

  let key_v2 = b"key_v2";
  let hash_v2 = HashIndex::hash_key(key_v2);
  let addr_v2 = 500u64;
  let addr_v1 = 400u64;

  old_index.insert(key_v2, addr_v2)?;

  let target_b = (hash_v2 as usize) & old_index.mask;

  split_single_bucket(
    &old_index,
    &new_index,
    target_b,
    0,
    |addr| {
      if addr == addr_v2 {
        Some((hash_v2, addr_v1))
      } else {
        None
      }
    },
    |prev_addr, _target_bit| {
      if prev_addr == addr_v1 {
        Some(addr_v1)
      } else {
        None
      }
    },
  )?;

  assert_eq!(new_index.find_tag(key_v2), Some(addr_v2));

  OK
}

/// 验证分裂迁移按 begin 门跳过物理截断死条目（主桶与溢出链条目一并覆盖）：
/// 开启门控（min_valid_addr=100）后左右子桶两侧均不含死地址、零溢出桶伪分配；
/// 关闭门控（min_valid_addr=0）的对照运行中同一夹具的死条目按「不可解析冷记录」
/// 双写进左右两子桶——正反双断言锁定过滤即本函数门控的直接后果
///
/// 此为登记在案的分裂期防膨胀有意偏差（doc/zh/deviations.md「分裂期按 begin
/// 过滤死条目」）：C# SplitChunk 只门 HeadAddress、不做此项过滤，本测试断言的是
/// rust 侧偏差口径而非 C# 原生契约
#[test]
fn test_split_skips_truncated_dead_entries() -> Void {
  let tag_shift = HashBucketEntry::HASH_TAG_SHIFT;
  // 单桶旧表 → 双桶新表：全部条目挂在旧桶 0，左子桶 0 / 右子桶 1
  let old_index = HashIndex::new(1)?;

  // 确定性铺链：tag 1..=7 为活条目（地址 ≥ 100，locator 可解析），
  // 恰好铺满主桶 7 槽；tag 8..=10 为死条目（地址 < 100，locator 不可解析），
  // 必然落入溢出桶 1 的前 3 槽——溢出链过滤与主桶过滤同被覆盖
  let live_tags = 1..=7u64;
  let dead_tags = 8..=10u64;
  let mut live = Vec::new();
  for tag in live_tags.clone() {
    let addr = 100 + tag * 10;
    old_index.insert_by_hash((tag << tag_shift) | tag, addr)?;
    live.push(((tag << tag_shift) | tag, addr));
  }
  let mut dead = Vec::new();
  for tag in dead_tags.clone() {
    let addr = tag;
    old_index.insert_by_hash((tag << tag_shift) | tag, addr)?;
    dead.push(((tag << tag_shift) | tag, addr));
  }
  assert_eq!(
    old_index.overflow_pool.allocated_count(),
    1,
    "夹具：10 个条目必须主桶 7 + 溢出桶 3"
  );

  let locator = |addr: u64| -> Option<(u64, u64)> {
    live.iter().find(|(_, a)| *a == addr).map(|(h, _)| (*h, 0))
  };
  let no_trace = |_prev: u64, _bit: usize| None;

  // 1. 开启 begin 门：死条目两侧均不落，活条目按哈希低位精确落侧
  let new_index = HashIndex::new(2)?;
  split_chunk(&old_index, &new_index, 0, 1, 100, &locator, &no_trace)?;
  for (_, addr) in &dead {
    assert!(
      !bucket_chain_has_addr(&new_index, 0, *addr),
      "死条目 {addr} 严禁迁入左子桶"
    );
    assert!(
      !bucket_chain_has_addr(&new_index, 1, *addr),
      "死条目 {addr} 严禁迁入右子桶"
    );
  }
  for (hash, addr) in &live {
    let bit = (*hash & 1) as usize;
    assert!(
      bucket_chain_has_addr(&new_index, bit, *addr),
      "活条目 {addr} 必须迁入位判定侧 {bit}"
    );
    assert!(
      !bucket_chain_has_addr(&new_index, 1 - bit, *addr),
      "可解析活条目 {addr} 不得双写对侧"
    );
  }
  assert_eq!(
    new_index.overflow_pool.allocated_count(),
    0,
    "过滤死条目后新表 3 活条目入左、4 活条目入右，绝不允许溢出桶伪分配"
  );

  // 2. 对照：关闭 begin 门，同一夹具的死条目按不可解析冷记录双写两侧
  //    （证伪锚：若分裂内核本就不会写死条目，此段断言即失守，说明 1 的
  //    干净结果并非本门控之功）
  let control = HashIndex::new(2)?;
  split_chunk(&old_index, &control, 0, 1, 0, &locator, &no_trace)?;
  for (_, addr) in &dead {
    assert!(
      bucket_chain_has_addr(&control, 0, *addr) && bucket_chain_has_addr(&control, 1, *addr),
      "门控关闭时死条目 {addr} 必须按冷记录双写左右两子桶（C# SplitChunk 原生口径）"
    );
  }

  OK
}
