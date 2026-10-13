use parking_lot::RwLock;
use wbase::{
  align::CachePadded64,
  striped::{StripedRwLock, StripedTable},
};

#[test]
fn test_striped_rwlock_basic() {
  let lock = StripedRwLock::<i32, 16>::new();
  assert_eq!(lock.len(), 16);
  assert!(!lock.is_empty());
  {
    let mut w = lock.write(0);
    *w = 42;
  }
  {
    let r1 = lock.read(0);
    let r2 = lock.read_at(16); // 回绕到 0
    assert_eq!(*r1, 42);
    assert_eq!(*r2, 42);
  }
}

#[test]
fn test_striped_rwlock_try_lock() {
  let lock = StripedRwLock::<(), 16>::new();
  let idx = StripedRwLock::<(), 16>::stripe_index(1);
  let r = lock.try_read_at(idx);
  assert!(r.is_some());
  // 读锁存在时，写锁失败
  assert!(lock.try_write_at(idx).is_none());
  drop(r);
  // 释放后写锁成功
  let w = lock.try_write_at(idx);
  assert!(w.is_some());
  assert!(lock.try_read_at(idx).is_none());
  drop(w);
  assert!(lock.try_read_at(idx).is_some());
}

#[test]
fn test_striped_rwlock_padded64_slot() {
  // 泛型槽位：64 字节对齐 CachePadded64
  let lock = StripedRwLock::<i32, 8, CachePadded64<RwLock<i32>>>::new();
  assert_eq!(lock.len(), 8);
  {
    let mut w = lock.write_at(3);
    *w = 7;
  }
  assert_eq!(*lock.read_at(3), 7);
  // 默认槽位：128 字节对齐 CacheAlignedLock
  let default_slot_lock = StripedRwLock::<(), 8>::new();
  let r = default_slot_lock.try_read_at(2);
  assert!(r.is_some());
  // 读锁存续期间写锁互斥
  assert!(default_slot_lock.try_write_at(2).is_none());
  drop(r);
  // 释放后写锁成功
  assert!(default_slot_lock.try_write_at(2).is_some());
}

// 以下两用例自 tests/main.rs 迁入（原 test_striped_rwlock / test_striped_counter）

#[test]
fn test_striped_rwlock() {
  use std::{
    sync::{
      Arc,
      atomic::{AtomicUsize, Ordering},
    },
    thread,
  };

  use wbase::striped::*;

  // 1. 默认构造与容量断言
  let locks: StripedRwLock<(), 128> = StripedRwLock::new();
  assert_eq!(locks.len(), 128);
  assert!(!locks.is_empty());
  assert_eq!(StripedRwLock::<(), 128>::STRIPE_MASK, 127);

  // 2. 自定义初始化与索引访问
  let data_locks: StripedRwLock<usize, 64> = StripedRwLock::with_initializer(|i| i * 10);
  assert_eq!(data_locks.len(), 64);
  assert_eq!(*data_locks.read_at(3), 30);
  assert_eq!(*data_locks.read_at(67), 30); // 67 & 63 == 3

  // 3. 读写锁基本语义
  {
    let mut w = data_locks.write_at(3);
    *w = 999;
  }
  assert_eq!(*data_locks.read_at(3), 999);

  // 4. 哈希寻址
  let hash1 = 0x1234_5678_u64;
  let hash2 = hash1 + 64;
  assert_eq!(
    StripedRwLock::<(), 64>::stripe_index(hash1),
    StripedRwLock::<(), 64>::stripe_index(hash2)
  );

  // 5. 多线程并发读写压力测试
  let counter_locks = Arc::new(StripedRwLock::<AtomicUsize, 32>::new());
  let mut handles = Vec::new();

  for t in 0..16 {
    let cl = Arc::clone(&counter_locks);
    handles.push(thread::spawn(move || {
      for i in 0..100 {
        let hash = (t * 1000 + i) as u64;
        let guard = cl.read(hash);
        guard.fetch_add(1, Ordering::Relaxed);
      }
    }));
  }

  for h in handles {
    h.join().unwrap();
  }

  let total: usize = (0..32)
    .map(|idx| counter_locks.read_at(idx).load(Ordering::Relaxed))
    .sum();
  assert_eq!(total, 16 * 100);
}

#[test]
fn test_striped_table_base() {
  use std::mem::size_of;

  // 条带表通用基座：掩码 / 寻址 / 堆上就地构造（条带数 2 的幂约束经
  // StripedTable::with_initializer 编译期断言单点承载，非法 N 无法编译）
  let table: StripedTable<u8, 16> = StripedTable::with_initializer(|i| i as u8);
  assert_eq!(StripedTable::<u8, 16>::STRIPE_MASK, 15);
  assert_eq!(StripedTable::<u8, 16>::stripe_index(16), 0);
  assert_eq!(table.len(), 16);
  assert!(!table.is_empty());
  assert_eq!(*table.slot_at(3), 3);
  assert_eq!(*table.slot_at(19), 3); // 19 & 15 == 3，掩码回绕一致
  assert_eq!(
    format!("{:?}", StripedTable::<u8, 16>::with_initializer(|_| 0)),
    "StripedTable { len: 16 }"
  );

  // Arc 薄指针存储：仅一个机器字
  assert_eq!(size_of::<StripedTable<u8, 16>>(), size_of::<usize>());
}

#[test]
fn test_striped_counter() {
  use std::thread;

  use wbase::striped::StripedCounter;

  // 1. 本地实例测试：隔离性、容量、掩码与基础增减
  let counter = StripedCounter::<16>::new();
  assert_eq!(counter.len(), 16);
  assert!(!counter.is_empty());
  assert_eq!(counter.slots().len(), 16);
  assert_eq!(StripedCounter::<16>::STRIPE_MASK, 15);

  counter.add(0, 100);
  counter.add(16, 50); // 16 自动模 16 回绕到 0
  assert_eq!(counter.get(), 150);

  counter.add(1, 200);
  counter.sub(17, 30); // 17 模 16 回绕到 1
  assert_eq!(counter.get(), 320);
  assert_eq!(counter.get_positive(), 320);

  // 负值截断
  counter.sub(0, 500);
  assert_eq!(counter.get(), -180);
  assert_eq!(counter.get_positive(), 0);

  // 重置
  counter.reset();
  assert_eq!(counter.get(), 0);
  assert_eq!(counter.get_positive(), 0);

  let dbg_str = format!("{counter:?}");
  assert!(dbg_str.contains("StripedCounter"));
  assert!(dbg_str.contains("len: 16"));

  // 2. 静态常量构造与多线程高并发原子争用测试
  static GLOBAL_COUNTER: StripedCounter<32> = StripedCounter::new();
  GLOBAL_COUNTER.reset();
  assert_eq!(GLOBAL_COUNTER.len(), 32);
  assert_eq!(StripedCounter::<32>::STRIPE_MASK, 31);

  let threads: Vec<_> = (0..16)
    .map(|tid| {
      thread::spawn(move || {
        for i in 0..500 {
          let stripe = tid * 37 + i;
          GLOBAL_COUNTER.add(stripe, 10);
          GLOBAL_COUNTER.sub(stripe, 5);
        }
      })
    })
    .collect();

  for t in threads {
    t.join().unwrap();
  }

  // 16 线程 * 500 轮 * (10 - 5) = 40000
  assert_eq!(GLOBAL_COUNTER.get(), 16 * 500 * 5);
  assert_eq!(GLOBAL_COUNTER.get_positive(), 16 * 500 * 5);

  GLOBAL_COUNTER.reset();
  assert_eq!(GLOBAL_COUNTER.get(), 0);
}
