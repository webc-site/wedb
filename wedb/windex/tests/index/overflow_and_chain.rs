//! 自研依据: 溢出桶池与链扩展（C# 对应 OverflowBucketLockTableTests.cs + MallocFixedPageSizeTests.cs）
use std::{
  sync::{Arc, Barrier, atomic::Ordering},
  thread,
};

use aok::{OK, Void};
use log::info;
use windex::{DATA_ENTRIES, Error, HashBucket, HashBucketEntry, HashIndex, OverflowPool};

use super::support::{HashIndexTestOps, make_address, make_key};

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

  let total_keys = 7500;
  for i in 1..=total_keys {
    let key = make_key("extreme_deep_key", i);
    let addr = (i as u64) * 10 + 3;
    index.insert(&key, addr)?;
  }

  let overflow_count = index.overflow_pool.allocated_count();
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
    let addrs = index.lookup_vec(&key);
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
  let updated_addrs = index.lookup_vec(&deep_key);
  assert!(updated_addrs.contains(&new_addr));
  assert!(!updated_addrs.contains(&old_addr));

  let tail_id = 7500;
  let tail_key = make_key("extreme_deep_key", tail_id);
  let tail_addr = (tail_id as u64) * 10 + 3;
  assert!(index.delete(&tail_key, tail_addr), "极深层尾部删除应成功");
  assert!(!index.lookup_vec(&tail_key).contains(&tail_addr));

  let reuse_key = b"reused_slot_deep_entry";
  let reuse_addr = 888_777_666_u64;
  index.insert(reuse_key, reuse_addr)?;
  assert!(index.lookup_vec(reuse_key).contains(&reuse_addr));

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

  assert_eq!(index.overflow_pool.allocated_count(), 3);

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
    assert!(index.lookup_vec(&key).contains(&expected_addr));
  }

  // 测试置零删除与查找更新
  let del_targets = [1, 8, 25];
  for &id in &del_targets {
    let key = make_key("cascade_key", id);
    let addr = (id * 100) as u64;
    assert!(index.delete(&key, addr));
    assert!(!index.delete(&key, addr));
    assert!(!index.lookup_vec(&key).contains(&addr));
  }

  for i in 1..=total_entries {
    if !del_targets.contains(&i) {
      let key = make_key("cascade_key", i);
      let expected_addr = (i * 100) as u64;
      assert!(index.lookup_vec(&key).contains(&expected_addr));
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
  let candidates = index.lookup_vec(b"non_existent_key");
  assert!(candidates.is_empty());

  // 2. insert 检测到回环并返回 OverflowCycleDetected
  let res = index.insert(b"cycle_trigger_key", 999);
  assert!(matches!(res, Err(Error::OverflowCycleDetected)));

  OK
}

/// 采集一条溢出链的结构指纹：`(挂载溢出桶序号序列, 每桶已占用数据槽位数)`，主桶在首
fn chain_profile(index: &HashIndex) -> (Vec<u64>, Vec<usize>) {
  let occupied = |bucket: &HashBucket| -> usize {
    bucket.entries[..DATA_ENTRIES]
      .iter()
      .filter(|e| e.load(Ordering::Acquire) != 0)
      .count()
  };

  let mut ids = Vec::new();
  let mut occupancy = vec![occupied(index.bucket(0))];
  let mut next = index.bucket(0).overflow_index();
  while next != 0 {
    assert!(ids.len() < 1_000, "结构指纹采集必须在有限链上终止");
    let bucket = index
      .overflow_pool
      .get(next)
      .expect("链上挂载的溢出桶必须可解析");
    ids.push(next);
    occupancy.push(occupied(bucket));
    next = bucket.overflow_index();
  }
  (ids, occupancy)
}

/// 写入口标识（两条生产写路径 + 测试支撑层哈希形态便捷口，同归
/// ChainWalker::advance_or_extend 单点内核）
#[derive(Clone, Copy, Debug)]
enum WritePath {
  /// 测试支撑 HashIndexTestOps::insert_by_hash（由定向追加口 insert_to_bucket 等价复现）
  InsertByHash,
  /// insert_to_bucket：定向主桶插入（扩容分裂迁移）
  InsertToBucket,
  /// find_or_create_tag_by_hash_with_min_addr + HashEntryInfo::try_cas：探针式插入
  Probed,
}

const WRITE_PATHS: [WritePath; 3] = [
  WritePath::InsertByHash,
  WritePath::InsertToBucket,
  WritePath::Probed,
];

/// 用指定写路径把同一冲突序列灌入单主桶表（Tag 互异、哈希同桶、地址互异非零）
fn fill_by_path(index: &HashIndex, tag_count: u64, path: WritePath) -> Void {
  let tag_shift = HashBucketEntry::HASH_TAG_SHIFT;
  for tag in 1..=tag_count {
    let hash = (tag << tag_shift) | tag;
    let addr = tag * 100 + 7;
    match path {
      WritePath::InsertByHash => index.insert_by_hash(hash, addr)?,
      WritePath::InsertToBucket => {
        index.insert_to_bucket(0, HashBucketEntry::tag_from_hash(hash), addr)?
      }
      WritePath::Probed => {
        let mut hei = index.find_or_create_tag_by_hash_with_min_addr(hash, 0)?;
        assert!(!hei.is_found(), "互异 Tag 序列不得命中既有槽位");
        assert!(hei.try_cas(addr), "探针句柄定点 CAS 发布必须成功");
      }
    }
  }
  OK
}

/// 锁定「三个写入口经同一溢出链挂载内核」：同一哈希冲突序列分别走
/// 测试支撑哈希便捷口 / 生产定向追加 insert_to_bucket / 生产探针
/// find_or_create_tag_by_hash_with_min_addr，
/// 断言链结构指纹、溢出桶分配次数、空闲栈残留与链环错误类别逐字全等
#[test]
fn test_write_paths_share_overflow_extension_kernel() -> Void {
  info!("三个写入口共用溢出链挂载内核：同序列下链结构/分配次数/错误类型全等");

  // 单主桶表（mask=0）下任意哈希皆落桶 0；21 个互异 Tag 恰好铺满主桶 7 + 溢出桶 7×2，
  // 整链零空槽——扩链挂载点与三个入口的推进次数全部可精确预期
  const TAGS: u64 = 21;
  const EXTENSIONS: usize = 2;

  let mut profiles = Vec::new();
  let mut pools = Vec::new();
  for path in WRITE_PATHS {
    let index = HashIndex::new(1)?;
    fill_by_path(&index, TAGS, path)?;
    profiles.push(chain_profile(&index));
    pools.push((
      index.overflow_pool.allocated_count(),
      index.overflow_pool.free_count(),
    ));

    // 同序列下逐 Tag 候选地址集合一致（探针路径同样可检索到另两条路径的条目）
    let tag_shift = HashBucketEntry::HASH_TAG_SHIFT;
    for tag in 1..=TAGS {
      let hash = (tag << tag_shift) | tag;
      let cands = index.lookup_candidates_by_hash(hash);
      assert!(
        cands.contains(tag * 100 + 7),
        "写路径 {path:?} 灌入的 tag {tag} 必须可检索"
      );
      assert_eq!(
        cands.len(),
        1,
        "写路径 {path:?} 的互异 Tag 序列不得产生多候选"
      );
    }
  }

  // 1. 链结构指纹全等：挂载序号序列 + 每桶占用槽位数
  assert_eq!(
    profiles[0], profiles[1],
    "insert_to_bucket 的链结构必须与哈希便捷口全等"
  );
  assert_eq!(
    profiles[0], profiles[2],
    "探针写路径的链结构必须与哈希便捷口全等"
  );
  assert_eq!(
    profiles[0].0,
    vec![1, 2],
    "21 个条目必须恰级联序号 1、2 两个溢出桶"
  );
  assert_eq!(profiles[0].1, vec![7, 7, 7], "每桶槽位占用必须为 7/7/7");

  // 2. 溢出桶分配次数与空闲栈残留全等（内核多分配一次、或少归还一次都会在此暴露）
  for (path, &(allocated, free)) in WRITE_PATHS.iter().zip(&pools) {
    assert_eq!(
      allocated, EXTENSIONS as u64,
      "写路径 {path:?} 的扩链分配次数必须恰为 {EXTENSIONS}"
    );
    assert_eq!(free, 0, "写路径 {path:?} 单线程无竞争不得残留空闲溢出桶");
  }

  // 3. 错误类型一致：链尾自环后，三个写入口必须同样报 OverflowCycleDetected
  //    （整链满槽 + 自环 = 无空槽可收口，只能由内核步数上限转译退出）
  let index = HashIndex::new(1)?;
  fill_by_path(&index, TAGS, WritePath::InsertByHash)?;
  let tail_id = *profiles[0]
    .0
    .last()
    .expect("两桶级联链的链尾溢出桶序号必存在");
  let tail = index
    .overflow_pool
    .get(tail_id)
    .expect("链尾溢出桶必须可解析");
  assert!(tail.set_overflow_index(tail_id), "满载链尾自环挂载必须成功");

  let tag_shift = HashBucketEntry::HASH_TAG_SHIFT;
  let fresh_hash = (9_000u64 << tag_shift) | 9_000;
  let by_hash = index.insert_by_hash(fresh_hash, 4_242);
  // insert_to_bucket 只收 Tag 不收哈希，9_001 同为链上不存在的新 Tag
  let to_bucket = index.insert_to_bucket(0, 9_001, 4_242);
  let probed = index.find_or_create_tag_by_hash_with_min_addr(fresh_hash, 0);
  assert!(
    matches!(by_hash, Err(Error::OverflowCycleDetected)),
    "哈希便捷口必须报链环错误，实际 {by_hash:?}"
  );
  assert!(
    matches!(to_bucket, Err(Error::OverflowCycleDetected)),
    "insert_to_bucket 必须报链环错误，实际 {to_bucket:?}"
  );
  assert!(
    matches!(probed, Err(Error::OverflowCycleDetected)),
    "探针写路径必须报链环错误，实际 {probed:?}"
  );
  assert_eq!(
    index.overflow_pool.allocated_count(),
    EXTENSIONS as u64,
    "三个入口的环检测全程不得新增溢出桶分配"
  );

  OK
}

/// 锁定链尾收口与扩链内核的边界：探针在整链留有可复用空槽时绝不伪分配，
/// 而溢出指针非零（链未到底/已挂载）时绝不提前收口
#[test]
fn test_tail_free_slot_reuse_never_fakes_overflow_allocation() -> Void {
  info!("验证链尾可复用空槽收口不伪分配溢出桶、指针非零时不提前收口");

  let tag_shift = HashBucketEntry::HASH_TAG_SHIFT;
  let index = HashIndex::new(1)?;

  // 1. 铺满 21 槽（主桶 7 + 两溢出桶 7），链尾无空槽
  for tag in 1..=21u64 {
    index.insert_by_hash((tag << tag_shift) | tag, tag * 64)?;
  }
  assert_eq!(chain_profile(&index).0, vec![1, 2]);
  assert_eq!(index.overflow_pool.allocated_count(), 2);

  // 2. 第 22 个 Tag 整链无空槽 → 必须合法扩链挂载第 3 个溢出桶
  let stale_hash = (300u64 << tag_shift) | 300;
  index.insert_by_hash(stale_hash, 50)?;
  assert_eq!(
    index.overflow_pool.allocated_count(),
    3,
    "整链无空槽时唯一写路径内核必须扩链挂载"
  );
  assert_eq!(index.overflow_pool.free_count(), 0);

  // 3. 截断清退使链尾槽 0 成为可复用空槽：探针必须就地收口复用，零新增分配
  let mut hei = index.find_or_create_tag_by_hash_with_min_addr(stale_hash, 100)?;
  assert!(
    !hei.is_found(),
    "低于截断线的陈旧槽位必须被原位清退为空闲槽"
  );
  assert_eq!(
    index.overflow_pool.allocated_count(),
    3,
    "链尾已有可复用空槽时严禁进入扩链内核伪分配"
  );
  assert_eq!(
    index.overflow_pool.free_count(),
    0,
    "复用收口不得产生待归还冗余桶"
  );
  assert!(hei.try_cas(500), "复用空槽句柄定点 CAS 必须成功");
  let cands = index.lookup_candidates_by_hash(stale_hash);
  assert!(cands.contains(500) && !cands.contains(50));

  // 4. 链尾指针非零（此处以自环模拟并发抢先挂载）：探针不得提前收口，必须沿链深入
  let tail = index.overflow_pool.get(3).expect("链尾溢出桶必须可解析");
  assert!(tail.set_overflow_index(3), "链尾自环挂载必须成功");
  let other_hash = (777u64 << tag_shift) | 777;
  let probed = index.find_or_create_tag_by_hash_with_min_addr(other_hash, 0);
  assert!(
    matches!(probed, Err(Error::OverflowCycleDetected)),
    "指针非零即链未到底，必须沿链深入并由内核判环，实际 {probed:?}"
  );

  OK
}

/// 锁定内核的败者归还协议：多线程在同一链尾竞争挂载，静止态池必须守恒
/// `allocated == 链上挂载数 + 空闲栈长`（少 free 即泄漏、多 free 即重复回收）
#[test]
fn test_concurrent_chain_extension_conserves_overflow_slots() -> Void {
  info!("验证并发扩链挂载竞争后溢出桶池位守恒、无泄漏");

  let tag_shift = HashBucketEntry::HASH_TAG_SHIFT;
  let index = Arc::new(HashIndex::new(1)?);
  let threads = 8usize;
  let per_thread = 150usize;
  let total = threads * per_thread;
  // 屏障对齐起跑线：每次链尾挂载点都是全线程同时抵达，制造真实的 CAS 挂载败者
  let barrier = Arc::new(Barrier::new(threads));

  let mut handles = Vec::with_capacity(threads);
  for tid in 0..threads {
    let idx = Arc::clone(&index);
    let bar = Arc::clone(&barrier);
    handles.push(thread::spawn(move || {
      bar.wait();
      let mut written = Vec::with_capacity(per_thread);
      for j in 0..per_thread {
        let tag = (tid * per_thread + j + 1) as u64;
        let addr = tag * 16;
        idx
          .insert_by_hash((tag << tag_shift) | tag, addr)
          .expect("并发扩链插入不得失败");
        written.push((tag, addr));
      }
      written
    }));
  }
  let mut all = Vec::with_capacity(total);
  for h in handles {
    all.extend(h.join().unwrap());
  }
  assert_eq!(all.len(), total);

  let (ids, occupancy) = chain_profile(&index);
  let expected = (total - DATA_ENTRIES).div_ceil(DATA_ENTRIES);
  assert_eq!(
    ids.len(),
    expected,
    "挂载桶数必须恰等于容量需求：多一次挂载即内核重复扩链"
  );
  assert_eq!(
    occupancy.iter().sum::<usize>(),
    total,
    "全部条目必须落在链上且无重复计数"
  );
  assert!(
    occupancy.iter().all(|&n| n <= DATA_ENTRIES),
    "单桶数据槽位占用不得超过 {DATA_ENTRIES}"
  );

  let allocated = index.overflow_pool.allocated_count();
  let free = index.overflow_pool.free_count();
  assert_eq!(
    allocated,
    ids.len() as u64 + free,
    "挂载竞争败者必须归还槽位：池位守恒 allocated == 挂载数 + 空闲栈长"
  );

  for (tag, addr) in all.iter().step_by(97) {
    let hash = (tag << tag_shift) | tag;
    assert!(
      index.lookup_candidates_by_hash(hash).contains(*addr),
      "并发插入的 tag {tag} 必须可检索"
    );
  }

  OK
}

/// 验证 OverflowPool 溢出桶回收复用与连续分配
/// 对标 Tsavorite 溢出池回收机制
#[test]
fn test_overflow_pool_free_and_recycle() -> Void {
  info!("验证 OverflowPool 溢出桶回收复用与连续分配");

  let pool = OverflowPool::new();
  assert_eq!(pool.allocated_count(), 0);

  let id1 = pool.allocate()?;
  let id2 = pool.allocate()?;
  let id3 = pool.allocate()?;
  assert_eq!(id1, 1);
  assert_eq!(id2, 2);
  assert_eq!(id3, 3);
  assert_eq!(pool.allocated_count(), 3);

  pool.free(id2);

  let reused = pool.allocate()?;
  assert_eq!(reused, id2, "必须优先复用已回收的溢出桶 id2");
  assert_eq!(pool.allocated_count(), 3, "复用回收桶不应增加总分配计数");

  let id4 = pool.allocate()?;
  assert_eq!(id4, 4);
  assert_eq!(pool.allocated_count(), 4);

  OK
}

/// 验证纯查找句柄探针（对标 C# InternalDelete 的 FindTag 语义）只查不建：
/// 键不存在时沿链探针绝不分配溢出桶；命中句柄可定点 try_cas/try_elide；
/// min_valid_addr 截断死槽位清退口径与 find_or_create 完全一致（同一分类内核）
#[test]
fn test_find_tag_entry_never_allocates_and_supports_cas_elide() -> Void {
  info!("验证 find_tag_entry 纯查找语义：未命中零分配 + 命中句柄 CAS/脱钩");

  let index = HashIndex::new(1)?;
  let tag_shift = HashBucketEntry::HASH_TAG_SHIFT;

  // 铺满一条三桶链（主桶 7 + 溢出桶 7×2 = 21 个互异 Tag），链上零空闲槽位
  let full_chain_tags = 21usize;
  for (i, t) in (1u64..).take(full_chain_tags).enumerate() {
    let hash = (t << tag_shift) | (i as u64);
    index.insert_by_hash(hash, ((i + 1) * 64) as u64)?;
  }
  assert_eq!(
    index.overflow_pool.allocated_count(),
    2,
    "21 个条目应恰好级联 2 个溢出桶且全链铺满"
  );

  // 1. 未命中探针（构造链上不存在的 Tag）：零分配
  let miss_hash = (9_999u64 << tag_shift) | 1;
  assert!(
    index
      .find_tag_entry_by_hash_with_min_addr(miss_hash, 0)
      .is_none()
  );
  assert_eq!(
    index.overflow_pool.allocated_count(),
    2,
    "满载链上的删除未命中（FindTag 纯查找）绝不允许分配新溢出桶"
  );

  // 2. 真实键（经调用方预哈希）探针未命中同样零分配（键 Tag 恰与铺链 Tag 重合时顺延候选键）
  let miss_key = (0..100usize)
    .map(|i| make_key("find_entry_miss", i))
    .find(|k| {
      let tag = HashBucketEntry::tag_from_hash(HashIndex::hash_key(k)) as usize;
      !(1..=full_chain_tags).contains(&tag)
    })
    .expect("候选键搜索不可能耗尽");
  assert!(
    index
      .find_tag_entry_by_hash_with_min_addr(HashIndex::hash_key(&miss_key), 0)
      .is_none()
  );
  assert_eq!(index.overflow_pool.allocated_count(), 2);

  // 3. 命中句柄：定点 CAS 替换与原子脱钩（try_elide）
  let hit_tag = 7u64;
  let hit_hash = (hit_tag << tag_shift) | 7;
  let old_addr = (7 * 64) as u64;
  let mut hei = index
    .find_tag_entry_by_hash_with_min_addr(hit_hash, 0)
    .expect("铺链条目必须命中");
  assert_eq!(hei.address(), old_addr);
  let new_addr = 987_654_321u64;
  assert!(hei.try_cas(new_addr), "命中句柄定点 CAS 必须成功");
  let mut hei = index
    .find_tag_entry_by_hash_with_min_addr(hit_hash, 0)
    .expect("CAS 后必须命中新地址");
  assert_eq!(hei.address(), new_addr);
  assert!(hei.try_elide(), "命中句柄原子脱钩必须成功");
  assert!(
    index
      .find_tag_entry_by_hash_with_min_addr(hit_hash, 0)
      .is_none()
  );

  // 4. min_valid_addr 清退：同 Tag 多候选（陈旧截断 + 有效新版）时必须清退陈旧槽位
  //    并继续推进命中有效新版——与 find_or_create_tag_with_min_addr 口径一致
  let multi_tag = 30u64;
  let multi_hash = (multi_tag << tag_shift) | 30;
  index.insert_by_hash(multi_hash, 50)?; // 陈旧版本（低于截断线 100）
  index.insert_by_hash(multi_hash, 200)?; // 有效版本（合法建槽扩链）
  let multi_filled = index.overflow_pool.allocated_count();
  let hei = index
    .find_tag_entry_by_hash_with_min_addr(multi_hash, 100)
    .expect("有效新版本必须命中");
  assert_eq!(hei.address(), 200, "截断陈旧候选必须被清退并推进至有效版本");
  // 陈旧槽位已被原位 CAS 置零回收：候选地址列表只剩有效新版本
  let cands = index.lookup_candidates_by_hash(multi_hash);
  assert!(
    !cands.contains(50),
    "陈旧候选槽位必须已被 min_addr 探针原位清退回收"
  );
  assert!(cands.contains(200), "有效新版本保持可达");
  assert_eq!(
    index.overflow_pool.allocated_count(),
    multi_filled,
    "探针全程零新增溢出桶（建槽扩链仅来自显式 insert）"
  );

  OK
}
