use std::{
  sync::{Arc, atomic::Ordering},
  thread,
};

use aok::{OK, Void};
use log::info;
use windex::{DATA_ENTRIES, Error, HashBucket, HashIndex, OverflowPool};

use super::support::{make_address, make_key};

/// 验证超过 1024 个溢出桶跨 Chunk 并发分配与寻址稳定性
/// 对标 Tsavorite 溢出桶 Chunk 分块管理架构
#[test]
fn test_overflow_pool_cross_chunk_concurrent_allocation() -> Void {
  info!("超过 1024 个溢出桶跨 Chunk 并发分配与寻址稳定性对抗测试");

  let pool = Arc::new(OverflowPool::new());
  let thread_count = 16;
  let allocs_per_thread = 200; // 16 * 200 = 3200 个溢出桶，跨越 Chunk 0, 1, 2, 3
  let total_allocations = thread_count * allocs_per_thread;

  let mut handles = Vec::new();

  for tid in 0..thread_count {
    let p = Arc::clone(&pool);
    handles.push(thread::spawn(move || {
      let mut my_ids = Vec::with_capacity(allocs_per_thread);
      for i in 0..allocs_per_thread {
        let id = p.allocate().expect("并发跨块分配溢出桶成功");
        let bucket = p.get(id).expect("新分配的桶必须立即可见");
        assert_eq!(
          bucket as *const HashBucket as usize % 64,
          0,
          "跨 Chunk 桶地址必须严格满足 64 字节对齐"
        );
        let test_raw = make_address(tid as u64 + 1, i as u64 + 1);
        bucket.entries[0].store(test_raw, Ordering::Release);
        my_ids.push((id, test_raw));
      }
      my_ids
    }));
  }

  let mut all_ids = Vec::with_capacity(total_allocations);
  for h in handles {
    let thread_ids = h.join().unwrap();
    all_ids.extend(thread_ids);
  }

  assert_eq!(pool.allocated_count(), total_allocations as u64);
  assert_eq!(all_ids.len(), total_allocations);

  all_ids.sort_unstable_by_key(|&(id, _)| id);
  for (idx, &(id, expected_val)) in all_ids.iter().enumerate() {
    let expected_id = (idx + 1) as u64;
    assert_eq!(id, expected_id, "分配的溢出桶 ID 必须严格单调连续无重复");

    let bucket = pool.get(id).expect("溢出桶引用获取");
    let actual_val = bucket.entries[0].load(Ordering::Acquire);
    assert_eq!(
      actual_val, expected_val,
      "溢出桶 {id} 内存数据被并发脏写破坏"
    );
  }

  for &boundary_id in &[1024, 1025, 2048, 2049, 3072, 3073] {
    let b = pool.get(boundary_id).expect("边界桶必须有效");
    assert_eq!(
      b as *const HashBucket as usize % 64,
      0,
      "边界桶 {boundary_id} 必须 64 字节对齐"
    );
  }

  assert!(pool.get(0).is_none());
  assert!(pool.get((total_allocations + 1) as u64).is_none());
  assert!(pool.get(999_999).is_none());

  OK
}

/// 验证极限哈希冲突极深溢出链表操作（1024+ 溢出桶深度）、RCU 与槽位复用
/// 对标 Tsavorite 单桶高冲突深链表设计
#[test]
fn test_extreme_overflow_depth_1024_plus_buckets() -> Void {
  info!("极限哈希冲突极深溢出链表操作（1024+ 溢出桶深度）、RCU 与槽位复用");

  let index = HashIndex::new(1)?;
  assert_eq!(index.bucket_count(), 1);

  let total_keys = 7500;
  for i in 1..=total_keys {
    let key = make_key("extreme_deep_key", i);
    let addr = (i as u64) * 10 + 3;
    index.insert(&key, addr)?;
  }

  let overflow_count = index.overflow_bucket_count();
  assert!(
    overflow_count >= 1071,
    "溢出桶深度必须超过 1071 桶，实际为: {overflow_count}"
  );

  assert!(!index.bucket(0).is_latched());
  assert_eq!(index.bucket(0).num_latched_shared(), 0);

  let test_ids = [1, 7, 3500, 5000, 7168, 7169, 7499, 7500];
  for &id in &test_ids {
    let key = make_key("extreme_deep_key", id);
    let expected_addr = (id as u64) * 10 + 3;
    let addrs = index.lookup(&key);
    assert!(
      addrs.contains(&expected_addr),
      "深层链表检索失败: id={id}, expected_addr={expected_addr}"
    );
  }

  let deep_id = 7169;
  let deep_key = make_key("extreme_deep_key", deep_id);
  let old_addr = (deep_id as u64) * 10 + 3;
  let new_addr = 999_888_777_u64;

  assert!(
    index.update_address(&deep_key, old_addr, new_addr),
    "极深层溢出桶条目 RCU 地址替换应成功"
  );
  let updated_addrs = index.lookup(&deep_key);
  assert!(updated_addrs.contains(&new_addr));
  assert!(!updated_addrs.contains(&old_addr));

  let tail_id = 7500;
  let tail_key = make_key("extreme_deep_key", tail_id);
  let tail_addr = (tail_id as u64) * 10 + 3;
  assert!(index.delete(&tail_key, tail_addr), "极深层尾部删除应成功");
  assert!(!index.lookup(&tail_key).contains(&tail_addr));

  let reuse_key = b"reused_slot_deep_entry";
  let reuse_addr = 888_777_666_u64;
  index.insert(reuse_key, reuse_addr)?;
  assert!(index.lookup(reuse_key).contains(&reuse_addr));

  assert!(index.try_lock_shared(b"arbitrary"));
  assert_eq!(index.bucket(0).num_latched_shared(), 1);
  index.unlock_shared(b"arbitrary");

  assert!(index.try_lock_exclusive(b"arbitrary"));
  assert!(index.bucket(0).is_latched_exclusive());
  index.unlock_exclusive(b"arbitrary");

  OK
}

/// 验证溢出桶链表级联挂接与锁位保护
/// 对标 Tsavorite `OverflowBucketLockTableTests`
#[test]
fn test_overflow_bucket_cascade() -> Void {
  info!("验证溢出桶级联挂接与锁位保护");

  let index = HashIndex::new(1)?;

  // 1. 锁位保护验证：在挂接溢出桶前持有主桶共享锁
  assert!(index.try_lock_shared(b"arbitrary_key"));
  assert_eq!(index.bucket(0).num_latched_shared(), 1);

  // 插入 25 个条目：主桶 7 个，溢出桶各 7 个，级联挂接
  let total_entries = 25;
  for i in 1..=total_entries {
    let key = make_key("cascade_key", i);
    let addr = (i * 100) as u64;
    index.insert(&key, addr)?;
  }

  assert_eq!(
    index.bucket(0).num_latched_shared(),
    1,
    "溢出桶指针安装严禁破坏主桶锁状态"
  );
  index.unlock_shared(b"arbitrary_key");
  assert_eq!(index.bucket(0).num_latched_shared(), 0);

  assert_eq!(index.overflow_bucket_count(), 3);

  let ov1_id = index.buckets[0].overflow_index();
  assert_eq!(ov1_id, 1);
  let ov1 = index.overflow_pool.get(ov1_id).unwrap();

  let ov2_id = ov1.overflow_index();
  assert_eq!(ov2_id, 2);
  let ov2 = index.overflow_pool.get(ov2_id).unwrap();

  let ov3_id = ov2.overflow_index();
  assert_eq!(ov3_id, 3);
  let ov3 = index.overflow_pool.get(ov3_id).unwrap();
  assert_eq!(ov3.overflow_index(), 0);

  for i in 1..=total_entries {
    let key = make_key("cascade_key", i);
    let expected_addr = (i * 100) as u64;
    assert!(index.lookup(&key).contains(&expected_addr));
  }

  // 测试置零删除与查找更新
  let del_targets = [1, 8, 25];
  for &id in &del_targets {
    let key = make_key("cascade_key", id);
    let addr = (id * 100) as u64;
    assert!(index.delete(&key, addr));
    assert!(!index.delete(&key, addr));
    assert!(!index.lookup(&key).contains(&addr));
  }

  for i in 1..=total_entries {
    if !del_targets.contains(&i) {
      let key = make_key("cascade_key", i);
      let expected_addr = (i * 100) as u64;
      assert!(index.lookup(&key).contains(&expected_addr));
    }
  }

  OK
}

/// 验证哈希桶溢出链回环检测与防死锁机制 (Floyd 判环算法)
/// 对标 Tsavorite 溢出链安全设计
#[test]
fn test_overflow_chain_cycle_detection() -> Void {
  info!("验证哈希桶溢出链回环检测与防死循环机制");

  let index = HashIndex::new(1)?;

  // 填满主桶 7 个槽位
  for i in 1..=DATA_ENTRIES {
    index.insert(&make_key("key", i), i as u64)?;
  }

  // 分配第 1 个溢出桶并填满 7 个槽位
  for i in 1..=DATA_ENTRIES {
    index.insert(&make_key("overflow_key", i), 100 + i as u64)?;
  }

  // 此时 ov1 已分配但无后续溢出桶 (overflow_index == 0)，人为构造回环 (1 -> 1)
  let ov1 = index.overflow_pool.get(1).unwrap();
  assert!(ov1.set_overflow_index(1));

  // 1. lookup 不陷入死循环，安全退出
  let candidates = index.lookup(b"non_existent_key");
  assert!(candidates.is_empty());

  // 2. insert 检测到回环并返回 OverflowCycleDetected
  let res = index.insert(b"cycle_trigger_key", 999);
  assert!(matches!(res, Err(Error::OverflowCycleDetected)));

  OK
}

/// 验证 OverflowPool 溢出桶回收复用与连续分配
/// 对标 Tsavorite 溢出池回收机制
#[test]
fn test_overflow_pool_free_and_recycle() -> Void {
  info!("验证 OverflowPool 溢出桶回收复用与连续分配");

  let pool = OverflowPool::new();
  assert!(!pool.has_free());
  assert_eq!(pool.allocated_count(), 0);

  let id1 = pool.allocate()?;
  let id2 = pool.allocate()?;
  let id3 = pool.allocate()?;
  assert_eq!(id1, 1);
  assert_eq!(id2, 2);
  assert_eq!(id3, 3);
  assert_eq!(pool.allocated_count(), 3);
  assert!(!pool.has_free());

  pool.free(id2);
  assert!(pool.has_free());

  let reused = pool.allocate()?;
  assert_eq!(reused, id2, "必须优先复用已回收的溢出桶 id2");
  assert!(!pool.has_free());
  assert_eq!(pool.allocated_count(), 3, "复用回收桶不应增加总分配计数");

  let id4 = pool.allocate()?;
  assert_eq!(id4, 4);
  assert_eq!(pool.allocated_count(), 4);

  OK
}
