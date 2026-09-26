//! 自研依据: 共享/独占闩与晋升（C# 对应 OverflowBucketLockTableTests.cs 锁面）
use std::{
  hint::spin_loop,
  sync::{
    Arc, Barrier,
    atomic::{AtomicU64, Ordering},
  },
  thread::{self, yield_now},
};

use aok::{OK, Void};
use log::info;
use windex::{BucketExclusiveGuard, BucketSharedGuard, HashBucket, HashBucketEntry, HashIndex};

use super::support::{HashIndexTestOps, make_key};

/// 验证共享自旋锁（S-Latch）并发读者递增与独占自旋锁（X-Latch）互斥生命周期
/// 对标 Tsavorite `HashBucket.cs` 与 `OverflowBucketLockTableTests`: `SingleKeyTest` / `ThreeKeyTest`
#[test]
fn test_bucket_shared_exclusive_latch_lifecycle() -> Void {
  info!("验证共享读锁并发递增与独占写锁严格互斥生命周期");

  let bucket = HashBucket::new();

  assert!(!bucket.is_latched());
  assert_eq!(bucket.num_latched_shared(), 0);
  assert!(!bucket.is_latched_exclusive());
  assert!(!bucket.is_latched_shared());

  // 1. 读者递增
  assert!(bucket.try_lock_shared());
  assert_eq!(bucket.num_latched_shared(), 1);
  assert!(bucket.is_latched_shared());
  assert!(!bucket.is_latched_exclusive());
  assert!(bucket.is_latched());

  assert!(bucket.try_lock_shared());
  assert_eq!(bucket.num_latched_shared(), 2);

  // 存在读者时，独占写锁尝试必须失败
  assert!(
    !bucket.try_lock_exclusive(),
    "活跃读者未排空时独占写锁必须失败"
  );
  assert_eq!(bucket.num_latched_shared(), 2);
  assert!(!bucket.is_latched_exclusive());

  // 逐步释放读者
  bucket.unlock_shared();
  assert_eq!(bucket.num_latched_shared(), 1);
  assert!(bucket.is_latched_shared());

  bucket.unlock_shared();
  assert_eq!(bucket.num_latched_shared(), 0);
  assert!(!bucket.is_latched());

  // 读者全部释放后，独占写锁必须成功
  assert!(bucket.try_lock_exclusive());
  assert!(bucket.is_latched_exclusive());
  assert_eq!(bucket.num_latched_shared(), 0);
  assert!(bucket.is_latched());

  // 独占写锁持有期间，任何读者或其他写者加锁均必须失败
  assert!(!bucket.try_lock_shared());
  assert!(!bucket.try_lock_exclusive());

  // 释放独占写锁
  bucket.unlock_exclusive();
  assert!(!bucket.is_latched());
  assert!(!bucket.is_latched_exclusive());
  assert_eq!(bucket.num_latched_shared(), 0);

  // 2. 3 读者并发加锁与排空
  assert!(bucket.try_lock_shared());
  assert!(bucket.try_lock_shared());
  assert!(bucket.try_lock_shared());
  assert_eq!(bucket.num_latched_shared(), 3);
  assert!(!bucket.try_lock_exclusive());

  bucket.unlock_shared();
  bucket.unlock_shared();
  bucket.unlock_shared();
  assert_eq!(bucket.num_latched_shared(), 0);
  assert!(!bucket.is_latched());

  // 3. RAII 守卫验证
  {
    let guard = BucketSharedGuard::new(&bucket).expect("获取共享锁守卫成功");
    assert!(bucket.is_latched_shared());
    drop(guard);
  }
  assert!(!bucket.is_latched());

  {
    let guard = BucketExclusiveGuard::new(&bucket).expect("获取独占锁守卫成功");
    assert!(bucket.is_latched_exclusive());
    drop(guard);
  }
  assert!(!bucket.is_latched());

  OK
}

/// 验证锁冲突自旋退避、读者排空超时回退与原子锁升级
/// 对标 Tsavorite `HashBucket.TryAcquireExclusiveLatch` 与 `TryPromoteLatch`
#[test]
fn test_latch_conflict_spin_drain_and_promote() -> Void {
  info!("验证锁冲突自旋、读者排空超时回退与原子锁升级");

  let bucket = Arc::new(HashBucket::new());

  // 1. 写者排空读者路径（Writer Drains Readers）
  {
    assert!(bucket.try_lock_shared());
    assert_eq!(bucket.num_latched_shared(), 1);

    let b = Arc::clone(&bucket);
    let attempts = Arc::new(AtomicU64::new(0));
    let att = Arc::clone(&attempts);

    let writer_handle = thread::spawn(move || {
      for _ in 0..10_000 {
        att.fetch_add(1, Ordering::Relaxed);
        if b.try_lock_exclusive() {
          return true;
        }
        yield_now();
      }
      false
    });

    // 保证写者已至少发起一次冲突加锁尝试，确实处于排空等待中
    while attempts.load(Ordering::Relaxed) == 0 {
      yield_now();
    }
    bucket.unlock_shared();

    let writer_res = writer_handle.join().unwrap();
    assert!(writer_res, "写者在读者释放后应成功排空并获取独占锁");
    assert!(bucket.is_latched_exclusive());
    assert_eq!(bucket.num_latched_shared(), 0);

    bucket.unlock_exclusive();
    assert!(!bucket.is_latched());
  }

  // 2. 排空超时回退路径（Drain Timeout & Rollback）
  {
    assert!(bucket.try_lock_shared());
    assert_eq!(bucket.num_latched_shared(), 1);

    assert!(
      !bucket.try_lock_exclusive(),
      "读者不退出的情况下独占锁排空超时必须返回 false"
    );

    assert!(
      !bucket.is_latched_exclusive(),
      "超时后独占锁标记位必须已回退"
    );
    assert_eq!(
      bucket.num_latched_shared(),
      1,
      "读者的共享锁计数在写者超时回退后必须保持为 1"
    );

    bucket.unlock_shared();
    assert!(!bucket.is_latched());
  }

  // 3. 单读者原子升级独占锁
  {
    assert!(bucket.try_lock_shared());
    assert_eq!(bucket.num_latched_shared(), 1);

    assert!(
      bucket.try_promote_latch(),
      "单读者持有共享锁时应能直接原子升级为独占锁"
    );
    assert!(bucket.is_latched_exclusive());
    assert_eq!(bucket.num_latched_shared(), 0);

    bucket.unlock_exclusive();
    assert!(!bucket.is_latched());
  }

  // 4. 多读者等待其他读者排空后升级
  {
    assert!(bucket.try_lock_shared());
    assert!(bucket.try_lock_shared());
    assert_eq!(bucket.num_latched_shared(), 2);

    let b = Arc::clone(&bucket);
    let attempts = Arc::new(AtomicU64::new(0));
    let att = Arc::clone(&attempts);

    let promoter_handle = thread::spawn(move || {
      for _ in 0..10_000 {
        att.fetch_add(1, Ordering::Relaxed);
        if b.try_promote_latch() {
          return true;
        }
        yield_now();
      }
      false
    });

    // 保证升级者已至少发起一次冲突升级尝试，确实处于排空等待中
    while attempts.load(Ordering::Relaxed) == 0 {
      yield_now();
    }
    bucket.unlock_shared();

    let promote_res = promoter_handle.join().unwrap();
    assert!(promote_res, "其他读者释放后应成功升级为独占锁");
    assert!(bucket.is_latched_exclusive());
    assert_eq!(bucket.num_latched_shared(), 0);

    bucket.unlock_exclusive();
    assert!(!bucket.is_latched());
  }

  // 5. 验证 RAII 守卫 BucketSharedGuard::try_promote 升级
  {
    let guard = BucketSharedGuard::new(&bucket).expect("获取共享锁守卫");
    assert!(bucket.is_latched_shared());

    let exclusive_guard = guard.try_promote().expect("升级为独占锁守卫应成功");
    assert!(bucket.is_latched_exclusive());
    assert_eq!(bucket.num_latched_shared(), 0);

    drop(exclusive_guard);
    assert!(!bucket.is_latched());
  }

  OK
}

/// 验证锁并发升级互斥防幽灵写者、锁计数守恒与原子降级
/// 对标 Tsavorite `OverflowBucketLockTableTests` 与 `HashBucket.downgrade_latch`
#[test]
fn test_latch_promotion_and_atomic_downgrade_concurrency() -> Void {
  info!("验证锁并发升级互斥防幽灵写者、锁计数守恒与原子降级");

  let bucket = Arc::new(HashBucket::new());

  // 1. 无共享锁时调用 try_promote_latch 必须被拒绝
  assert!(!bucket.try_promote_latch());
  assert!(!bucket.is_latched());
  assert_eq!(bucket.num_latched_shared(), 0);

  // 2. 多读者并发竞争锁升级防幽灵写者
  let reader_threads = 8;
  let barrier = Arc::new(Barrier::new(reader_threads));
  let promoted_count = Arc::new(AtomicU64::new(0));
  let mut handles = Vec::new();

  for _ in 0..reader_threads {
    let b = Arc::clone(&bucket);
    let bar = Arc::clone(&barrier);
    let p_count = Arc::clone(&promoted_count);

    handles.push(thread::spawn(move || {
      let guard = BucketSharedGuard::new(&b).expect("获取共享锁守卫");
      bar.wait();

      match guard.try_promote() {
        Ok(exclusive_guard) => {
          let current_writers = p_count.fetch_add(1, Ordering::SeqCst);
          assert_eq!(
            current_writers, 0,
            "检测到幽灵写者并发侵入！独占锁互斥性被破坏！"
          );

          for _ in 0..100 {
            spin_loop();
          }

          p_count.fetch_sub(1, Ordering::SeqCst);
          drop(exclusive_guard);
        }
        Err(shared_guard) => {
          drop(shared_guard);
        }
      }
    }));
  }

  for h in handles {
    h.join().unwrap();
  }

  assert!(!bucket.is_latched());
  assert_eq!(bucket.num_latched_shared(), 0);
  assert!(!bucket.is_latched_exclusive());

  // 3. 独占锁原子降级为共享锁（Exclusive Guard -> Shared Guard）
  {
    let exclusive_guard = BucketExclusiveGuard::new(&bucket).expect("获取独占锁守卫");
    assert!(bucket.is_latched_exclusive());
    assert_eq!(bucket.num_latched_shared(), 0);

    let shared_guard = exclusive_guard.downgrade();
    assert!(
      !bucket.is_latched_exclusive(),
      "降级后独占标记必须被原子清除"
    );
    assert_eq!(
      bucket.num_latched_shared(),
      1,
      "降级后共享读者计数必须原子置为 1"
    );
    assert!(bucket.is_latched_shared());

    assert!(bucket.try_lock_shared());
    assert_eq!(bucket.num_latched_shared(), 2);
    bucket.unlock_shared();
    assert_eq!(bucket.num_latched_shared(), 1);

    drop(shared_guard);
    assert!(!bucket.is_latched());
    assert_eq!(bucket.num_latched_shared(), 0);
  }

  // 4. HashIndex 顶层降级 API 测试
  {
    let index = HashIndex::new(4)?;
    let key = b"downgrade_test_key";

    assert!(index.try_lock_exclusive(key));
    assert!(
      index
        .bucket(index.bucket_index_for_key(key))
        .is_latched_exclusive()
    );

    index.downgrade(key);
    assert!(
      !index
        .bucket(index.bucket_index_for_key(key))
        .is_latched_exclusive()
    );
    assert!(
      index
        .bucket(index.bucket_index_for_key(key))
        .is_latched_shared()
    );
    assert_eq!(
      index
        .bucket(index.bucket_index_for_key(key))
        .num_latched_shared(),
      1
    );

    index.unlock_shared(key);
    assert!(!index.bucket(index.bucket_index_for_key(key)).is_latched());
  }

  OK
}

/// 验证单键独占闩守卫的取闩互斥、RAII 放闩与异桶零干扰
/// 对标 Tsavorite 单键 ephemeral 独占闩（`Implementation/InternalRMW.cs` 首段调
/// `FindOrCreateTagAndTryEphemeralXLock`，其转 `Locking/TransientLocking.cs` 的
/// `TryEphemeralXLock`：一次尝试、取不到即返回状态，无批量编排、无逆序回滚、无索引层超时）
#[test]
fn test_key_latch_exclusive_take_and_raii_release() -> Void {
  info!("验证单键独占闩取闩互斥、Drop 放闩与异桶零干扰");

  let index = HashIndex::new(64)?;
  let key = b"latch_key_alpha";
  let neighbor = b"latch_key_beta";

  // 同键不可重入：持闩期间任何再次取闩（守卫入口与裸桶闩入口同一把锁）必须失败
  {
    let latch = index.try_lock_key_exclusive(key).expect("首次取闩成功");
    let bucket = index.bucket(index.bucket_index_for_key(key));
    assert!(
      bucket.is_latched_exclusive(),
      "取闩后本键主桶必须处于独占态"
    );

    assert!(
      index.try_lock_key_exclusive(key).is_none(),
      "持闩期间同键再取闩必须一次失败返回 None"
    );
    assert!(!index.try_lock_shared(key), "持闩期间共享取闩必须失败");
    assert!(!index.try_lock_exclusive(key), "持闩期间裸独占取闩必须失败");

    // 不同桶的键各自独立：索引层锁面只剩这一把按键定位的桶闩，无任何跨键编排
    // （夹具两键必须落在不同主桶，否则本段形同虚设——直接断言前提而非静默跳过）
    assert_ne!(
      index.bucket_index_for_key(neighbor),
      index.bucket_index_for_key(key),
      "夹具键 {neighbor:?} 与 {key:?} 落在同一主桶，异桶独立性段未覆盖"
    );
    {
      let other = index
        .try_lock_key_exclusive(neighbor)
        .expect("异桶取闩不受影响");
      assert!(
        index
          .bucket(index.bucket_index_for_key(neighbor))
          .is_latched_exclusive(),
        "异桶闩必须独立持有"
      );
      assert!(
        index
          .bucket(index.bucket_index_for_key(key))
          .is_latched_exclusive(),
        "释放异桶前本键闩不受影响"
      );
      drop(other);
      assert!(!index.is_locked(neighbor), "异桶闩 Drop 后必须放闩");
    }

    drop(latch);
  }

  // RAII Drop 放闩后即可重取，且无锁残留（放闩只由守卫 Drop 承担，无手动解锁面）
  assert!(!index.is_locked(key), "Drop 后本键必须完全放闩");
  {
    let retaken = index.try_lock_key_exclusive(key);
    assert!(retaken.is_some(), "放闩后必须可重新取闩");
    assert!(
      index.try_lock_key_exclusive(key).is_none(),
      "重取的闩持有期间同键仍不可重入"
    );
    drop(retaken);
  }
  assert!(!index.is_locked(key), "二次 Drop 后本键仍零锁残留");

  OK
}

/// 验证多线程同键独占闩竞争的互斥性与零残留（无自旋驱动，失败方自行让步重试）
/// 对标 Tsavorite `ThreadedLockStressTest` 的单键形态
#[test]
fn test_key_latch_concurrent_exclusion() -> Void {
  info!("验证多线程同键独占闩互斥与结束后零残留");

  let index = Arc::new(HashIndex::new(64)?);
  let key = b"latch_key_contended";
  // 同一键的 64 桶索引下取不同键：竞争同一把闩的线程数
  let thread_count = 8usize;
  let iterations = 200u64;
  let counter = Arc::new(AtomicU64::new(0));
  let barrier = Arc::new(Barrier::new(thread_count));

  let mut handles = Vec::new();
  for _ in 0..thread_count {
    let idx = Arc::clone(&index);
    let cnt = Arc::clone(&counter);
    let bar = Arc::clone(&barrier);
    handles.push(thread::spawn(move || {
      bar.wait();
      for _ in 0..iterations {
        // 取闩失败按 C# RETRY_LATER 口径由调用方让步重试（索引层不自旋不回滚）；
        // 重试不消耗本轮预算，故总临界区次数恒为 thread_count * iterations，
        // 一旦闩失效丢更新即显式变红
        let mut guard = idx.try_lock_key_exclusive(key);
        let mut yields = 0u32;
        while guard.is_none() {
          yields += 1;
          assert!(yields <= 1_000_000, "同键独占闩长期不可得，放闩链有漏");
          yield_now();
          guard = idx.try_lock_key_exclusive(key);
        }
        let _latch = guard.expect("取闩成功");
        // 临界区：非原子化的读-改-写序列，若闩失效必然丢更新
        let curr = cnt.load(Ordering::Relaxed);
        spin_loop();
        cnt.store(curr + 1, Ordering::Relaxed);
      }
    }));
  }

  for h in handles {
    h.join().expect("同键竞争线程无死锁完成");
  }

  assert_eq!(
    counter.load(Ordering::Relaxed),
    (thread_count as u64) * iterations,
    "单键独占闩的临界区必须零丢更新"
  );
  assert!(!index.is_locked(key), "压力结束后本键闩必须完全释放");

  OK
}

/// 验证多线程全冲突锁竞争压力测试
/// 对标 Tsavorite `ThreadedLockStressTestMultiThreadsFullContention`
#[test]
fn test_threaded_lock_stress_full_contention() -> Void {
  info!("验证多线程全冲突锁竞争压力与排空");

  let thread_count = 8;
  let iterations_per_thread = 200;
  let shared_bucket = Arc::new(HashBucket::new());
  let mut handles = Vec::new();

  for tid in 0..thread_count {
    let b = Arc::clone(&shared_bucket);
    handles.push(thread::spawn(move || {
      for i in 0..iterations_per_thread {
        if (tid + i) % 3 == 0 {
          while !b.try_lock_exclusive() {
            yield_now();
          }
          spin_loop();
          b.unlock_exclusive();
        } else {
          while !b.try_lock_shared() {
            yield_now();
          }
          spin_loop();
          b.unlock_shared();
        }
      }
    }));
  }

  for h in handles {
    h.join().unwrap();
  }

  assert!(!shared_bucket.is_latched());
  assert_eq!(shared_bucket.num_latched_shared(), 0);
  assert!(!shared_bucket.is_latched_exclusive());

  OK
}

/// 验证桶寻址掩码分布正确性：任意容量（含最小 1 桶）下桶下标恒在界内、
/// 掩码环绕寻址与键/哈希两路一致性——单键取闩 `get_unchecked` 裸寻址的分布前提
#[test]
fn test_bucket_index_mask_distribution() -> Void {
  info!("验证桶寻址掩码分布正确性与容量边界");

  for &cap in &[1usize, 2, 4, 16, 64, 1024, 4096] {
    let index = HashIndex::new(cap)?;

    // 1. 大样本哈希的桶下标恒在界内（hash & mask 截断不变量）
    for i in 0..2000u64 {
      let hash = HashIndex::hash_key(format!("dist_probe_{cap}_{i}").as_bytes());
      let idx = index.bucket_index_for_hash(hash);
      assert!(idx < cap, "容量 {cap} 下桶下标 {idx} 越界");

      // 键路径与哈希路径寻址一致
      let key = make_key("dist_key", i as usize);
      assert_eq!(
        index.bucket_index_for_key(&key),
        index.bucket_index_for_hash(HashIndex::hash_key(&key)),
        "键路径与哈希路径寻址必须一致"
      );
    }

    // 2. get_bucket 掩码环绕：bucket(i) 与 bucket(i + cap) 必须命中同一物理桶
    for i in 0..cap {
      let p1 = index.bucket(i) as *const HashBucket as usize;
      let p2 = index.bucket(i + cap) as *const HashBucket as usize;
      assert_eq!(p1, p2, "容量 {cap} 下掩码环绕寻址失真");
    }
  }

  // 3. 最小容量 1 桶的全链路退化：插入、查找、删除均收敛于唯一桶
  let tiny = HashIndex::new(1)?;
  tiny.insert(b"only_bucket_key", 7)?;
  assert_eq!(tiny.find_tag(b"only_bucket_key"), Some(7));
  assert_eq!(tiny.bucket_index_for_key(b"any_key"), 0);
  assert!(tiny.delete(b"only_bucket_key", 7));

  OK
}

/// 本组夹具键：单桶表（mask=0）下任意键恒属主桶 0，按键寻址锁口与句柄锁口必同锁
fn key_slot0() -> &'static [u8] {
  b"entry_info_latch_key"
}

/// 验证 `HashEntryInfo` 句柄的 ephemeral 桶闩一律落在链首主桶而非槽位所在溢出桶
/// 对标 `Implementation/Locking/OverflowBucketLockTable.cs:TryLockExclusive/TryLockShared`
/// （两者取锁对象均为 `hei.firstBucket`）与 `OverflowBucketLockTableTests` 的
/// `UseSingleBucketComparer` 全键同桶夹具（rust 侧以单桶表 `HashIndex::new(1)` 等价复现）
#[test]
fn test_entry_info_latch_locks_first_bucket_not_overflow() -> Void {
  info!("验证溢出桶条目与链尾复用空槽句柄的取闩均落主桶、零触碰溢出桶锁字");

  let index = HashIndex::new(1)?;
  let tag_shift = HashBucketEntry::HASH_TAG_SHIFT;

  // 确定性铺链：21 个互异 Tag 恰铺满主桶 7 槽 + 溢出桶 1、2 各 7 槽，
  // tag 8 的条目必落溢出桶 1 槽 0（结构无任何随机成分）
  for tag in 1..=21u64 {
    index.insert_by_hash((tag << tag_shift) | tag, tag * 100 + 7)?;
  }
  let ov1 = index.overflow_pool.get(1).expect("溢出桶 1 必须已挂载");

  // 1. 溢出桶条目命中句柄：独占闩落主桶，溢出桶锁字（与溢出指针同字）零触碰
  let hit_hash = (8u64 << tag_shift) | 8;
  let hei = index
    .find_tag_entry_by_hash_with_min_addr(hit_hash, 0)
    .expect("铺链条目必须命中");
  {
    let _guard = hei
      .lock_exclusive_guard()
      .expect("句柄取独占闩必须成功（空载无竞争）");
    assert!(
      index.bucket(0).is_latched_exclusive(),
      "溢出桶条目句柄的独占闩必须作用于链首主桶"
    );
    assert!(
      !ov1.is_latched(),
      "严禁在溢出桶上取闩：其锁位与溢出链指针同字，取闩即污染链指针"
    );
    // 与按键寻址锁同源互斥：外部经主桶检查必须感知本闩
    assert!(index.is_locked(key_slot0()), "按键寻址视角必须感知句柄闩");
    assert!(
      index.try_lock_key_exclusive(key_slot0()).is_none(),
      "句柄持闩期间按键寻址独占取闩必须失败"
    );
    assert!(
      !index.try_lock_shared(key_slot0()),
      "句柄持独占闩期间按键寻址共享取闩必须失败"
    );
  }
  assert!(!index.bucket(0).is_latched(), "守卫析构后主桶必须放闩");
  assert!(!index.is_locked(key_slot0()));

  // 2. 同句柄共享闩：与按键寻址共享口同锁互通（第二读者可进、写者被拒），析构计数归零
  {
    let _s_guard = hei.lock_shared_guard().expect("句柄取共享闩必须成功");
    assert_eq!(index.bucket(0).num_latched_shared(), 1);
    assert!(!ov1.is_latched(), "共享闩同样严禁触碰溢出桶锁字");
    assert!(index.try_lock_shared(key_slot0()), "主桶同源共享闩应可叠加");
    assert_eq!(index.bucket(0).num_latched_shared(), 2);
    assert!(
      !index.try_lock_exclusive(key_slot0()),
      "读者未排空时独占必须失败"
    );
    index.unlock_shared(key_slot0());
  }
  assert_eq!(
    index.bucket(0).num_latched_shared(),
    0,
    "守卫析构后读者计数归零"
  );

  // 3. 链尾复用空槽句柄（bucket 为溢出桶、raw==0 未命中态）：取闩同样落主桶
  //    （对标 C# FindOrCreateTagAndTryEphemeralXLock 对新键插入锁 firstBucket）
  let stale_hash = (300u64 << tag_shift) | 300;
  index.insert_by_hash(stale_hash, 50)?; // 合法扩链入溢出桶 3 槽 0
  let mut free_hei = index
    .find_or_create_tag_by_hash_with_min_addr(stale_hash, 100)
    .expect("探针必须成功");
  assert!(
    !free_hei.is_found(),
    "低于截断线的陈旧槽位必须被原位清退为复用空槽"
  );
  let ov3 = index.overflow_pool.get(3).expect("溢出桶 3 必须已挂载");
  {
    let _x_guard = free_hei
      .lock_exclusive_guard()
      .expect("空槽句柄取独占闩必须成功");
    assert!(
      index.bucket(0).is_latched_exclusive(),
      "复用空槽句柄的闩同样必须落链首主桶"
    );
    assert!(!ov3.is_latched(), "空槽所在溢出桶锁字必须零触碰");
    assert!(
      index.try_lock_key_exclusive(key_slot0()).is_none(),
      "空槽句柄持闩期间按键寻址取闩必须失败"
    );
  }
  assert!(free_hei.try_cas(500), "放闩后句柄定点 CAS 复用空槽必须成功");
  assert!(index.lookup_candidates_by_hash(stale_hash).contains(500));

  OK
}

/// 句柄取闩与按键取闩的双线程闭环互斥证伪：两条锁口若落在不同桶（修复前
/// 句柄锁溢出桶、按键锁主桶）则临界区互不阻挡、读-改-写必然丢更新
/// 对标 `OverflowBucketLockTableTests:ThreadedLockStressTest` 的同桶竞争形态
#[test]
fn test_entry_info_and_key_latch_mutual_exclusion() -> Void {
  info!("验证句柄闩与按键闩双线程同锁互斥、零丢更新");

  let index = Arc::new(HashIndex::new(1)?);
  let tag_shift = HashBucketEntry::HASH_TAG_SHIFT;
  // 铺 21 项使 tag 8 条目确定位于溢出桶，两类线程此后只读定位、不改链
  for tag in 1..=21u64 {
    index.insert_by_hash((tag << tag_shift) | tag, tag * 100 + 7)?;
  }
  let hit_hash = (8u64 << tag_shift) | 8;

  let threads_per_side = 4usize;
  let iterations = 200u64;
  let counter = Arc::new(AtomicU64::new(0));
  let barrier = Arc::new(Barrier::new(threads_per_side * 2));

  let mut handles = Vec::new();
  // 写者 A：经溢出桶条目句柄取独占闩
  for _ in 0..threads_per_side {
    let idx = Arc::clone(&index);
    let cnt = Arc::clone(&counter);
    let bar = Arc::clone(&barrier);
    handles.push(thread::spawn(move || {
      bar.wait();
      for _ in 0..iterations {
        let hei = idx
          .find_tag_entry_by_hash_with_min_addr(hit_hash, 0)
          .expect("铺链条目恒定命中");
        let mut guard = hei.lock_exclusive_guard();
        let mut yields = 0u32;
        while guard.is_none() {
          yields += 1;
          assert!(yields <= 1_000_000, "句柄独占闩长期不可得，放闩链有漏");
          yield_now();
          guard = hei.lock_exclusive_guard();
        }
        let _latch = guard.expect("取闩成功");
        // 临界区：非原子读-改-写，闩互斥失效即丢更新
        let curr = cnt.load(Ordering::Relaxed);
        spin_loop();
        cnt.store(curr + 1, Ordering::Relaxed);
      }
    }));
  }
  // 写者 B：经按键寻址（主桶）取独占闩
  for _ in 0..threads_per_side {
    let idx = Arc::clone(&index);
    let cnt = Arc::clone(&counter);
    let bar = Arc::clone(&barrier);
    handles.push(thread::spawn(move || {
      bar.wait();
      for _ in 0..iterations {
        let mut guard = idx.try_lock_key_exclusive(key_slot0());
        let mut yields = 0u32;
        while guard.is_none() {
          yields += 1;
          assert!(yields <= 1_000_000, "按键独占闩长期不可得，放闩链有漏");
          yield_now();
          guard = idx.try_lock_key_exclusive(key_slot0());
        }
        let _latch = guard.expect("取闩成功");
        let curr = cnt.load(Ordering::Relaxed);
        spin_loop();
        cnt.store(curr + 1, Ordering::Relaxed);
      }
    }));
  }

  for h in handles {
    h.join().expect("双锁口竞争线程无死锁完成");
  }

  assert_eq!(
    counter.load(Ordering::Relaxed),
    (threads_per_side as u64) * iterations * 2,
    "句柄闩与按键闩必须互斥同锁，否则读-改-写丢更新"
  );
  assert!(!index.is_locked(key_slot0()), "压力结束后主桶闩必须零残留");

  OK
}
