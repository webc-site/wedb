use std::{
  mem,
  sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
  },
  thread,
};

use aok::{OK, Result};
use wbftree::{CacheAlignedLock, NUM_LOCK_STRIPES, RangeIndexLocks};

/// 测试条带锁并发获取
#[test]
fn test_range_index_locks() -> Result<()> {
  let locks = RangeIndexLocks::new();

  // 共享锁并发读
  let _r1 = locks.read(100);
  let _r2 = locks.read(100);

  // 释放后获取互斥写锁
  drop(_r1);
  drop(_r2);
  let _w = locks.write(100);
  drop(_w);

  OK
}

/// 测试条带锁 128 字节缓存行对齐 (消除 ARM64/Apple Silicon 及 x86-64 上的 CPU 伪共享)
#[test]
fn test_cache_aligned_lock_alignment() -> Result<()> {
  assert_eq!(mem::align_of::<CacheAlignedLock>(), 128);

  let locks = RangeIndexLocks::new();
  {
    let _r1 = locks.read(12345);
    let _r2 = locks.read(12345);
  }
  {
    let _w = locks.write(12345);
  }
  OK
}

/// 测试条带数量常量与实际条带数一致 (对标 Garnet rangeIndexLocks 128 分段)
#[test]
fn test_lock_stripe_count() -> Result<()> {
  assert_eq!(NUM_LOCK_STRIPES, 128);
  // 不同哈希值命中各自条带，写锁可同时持有 (互不阻塞)
  let locks = RangeIndexLocks::new();
  let _w1 = locks.write(0);
  let _w2 = locks.write(u64::MAX);
  OK
}

/// 并发争用探测：同条带写锁必须互斥，临界区内「load → store」计数序列零丢失
#[test]
fn test_lock_write_exclusion_under_contention() -> Result<()> {
  let locks = Arc::new(RangeIndexLocks::new());
  let counter = Arc::new(AtomicUsize::new(0));
  const THREADS: usize = 4;
  const ITERS: usize = 500;

  let mut handles = Vec::new();
  for _ in 0..THREADS {
    let locks = Arc::clone(&locks);
    let counter = Arc::clone(&counter);
    handles.push(thread::spawn(move || {
      for _ in 0..ITERS {
        let _w = locks.write(7);
        // 非原子的 load→store 序列：若互斥失效将丢失更新导致终值偏小
        let v = counter.load(Ordering::Relaxed);
        counter.store(v + 1, Ordering::Relaxed);
      }
    }));
  }
  for h in handles {
    h.join().unwrap();
  }
  assert_eq!(counter.load(Ordering::Relaxed), THREADS * ITERS);
  OK
}
