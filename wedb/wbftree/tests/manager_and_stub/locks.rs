use std::{
  sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
  },
  thread,
};

use aok::{OK, Result};
use wbftree::RangeIndexManager;

use super::common::ManagerEnvGuard;

/// 同条带写锁必须互斥，临界区内「load → store」计数序列零丢失
/// (条带槽位 128 字节缓存行对齐断言归 wbase::striped 职责，此处仅测条带锁语义)
#[test]
fn test_lock_write_exclusion_under_contention() -> Result<()> {
  let env = ManagerEnvGuard::new("locks");
  let manager = Arc::new(RangeIndexManager::new(&env.ri_root.path, &env.cpr_root.path).unwrap());
  let counter = Arc::new(AtomicUsize::new(0));
  const THREADS: usize = 4;
  const ITERS: usize = 500;

  let mut handles = Vec::new();
  for _ in 0..THREADS {
    let manager = Arc::clone(&manager);
    let counter = Arc::clone(&counter);
    handles.push(thread::spawn(move || {
      for _ in 0..ITERS {
        let _w = manager.locks().write(7);
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

/// 同哈希读锁共享可同时持有；不同哈希命中各自条带，写锁互不阻塞
#[test]
fn test_lock_read_shared_and_cross_stripe_independent() -> Result<()> {
  let env = ManagerEnvGuard::new("locks_rw");
  let manager = RangeIndexManager::new(&env.ri_root.path, &env.cpr_root.path).unwrap();
  let locks = manager.locks();
  {
    let _r1 = locks.read(12345);
    let _r2 = locks.read(12345);
  }
  {
    let _w1 = locks.write(0);
    let _w2 = locks.write(u64::MAX);
  }
  OK
}
