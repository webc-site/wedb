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
use windex::{BucketExclusiveGuard, BucketSharedGuard, HashBucket, HashIndex};

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

/// 验证 lock_key_exclusive 单键独占桶锁并发互斥与自动释放
#[test]
fn test_lock_key_exclusive_concurrency() -> Void {
  info!("验证 lock_key_exclusive 单键独占桶锁并发互斥");

  let index = Arc::new(HashIndex::new(64)?);
  let key = b"exclusive_test_key";

  // 基础加锁与互斥
  {
    let guard = index.lock_key_exclusive(key)?;
    assert!(!index.try_lock_shared(key));
    assert!(!index.try_lock_exclusive(key));
    drop(guard);
  }

  // Drop 之后自动释放
  assert!(index.try_lock_shared(key));
  index.unlock_shared(key);

  // 高并发多线程竞争同一键的排他锁
  let counter = Arc::new(AtomicU64::new(0));
  let mut handles = Vec::new();
  for _ in 0..8 {
    let idx = Arc::clone(&index);
    let cnt = Arc::clone(&counter);
    handles.push(thread::spawn(move || {
      for _ in 0..100 {
        let guard = idx.lock_key_exclusive(key).expect("加锁成功");
        cnt.fetch_add(1, Ordering::Relaxed);
        spin_loop();
        drop(guard);
      }
    }));
  }

  for h in handles {
    h.join().expect("并发线程完成");
  }

  assert_eq!(counter.load(Ordering::Relaxed), 800);
  assert!(!index.is_locked(key));

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

  OK
}

/// 验证桶寻址掩码分布正确性：任意容量（含最小 1 桶）下桶下标恒在界内、
/// 掩码环绕寻址与键/哈希两路一致性——多键加锁 get_unchecked 裸寻址的分布前提
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
