#![recursion_limit = "512"] // 泛型设备实例化下 async 状态机嵌套深（R27 CI 实证）
//! 分片日志 CAS 位图锁集成与并发冲突测试
//! （对应 libs/server/AOF/ShardedLog.cs:lockMap）

use std::{
  sync::{
    Arc,
    atomic::{AtomicU64, Ordering},
  },
  thread,
};

use wnode::aof::sharded_log::ShardedLogLockMap;

#[test]
fn lock_unlock_roundtrip() {
  let map = ShardedLogLockMap::new();
  map.lock_sublogs(0b101);
  assert_eq!(map.lock_map.load(Ordering::Relaxed), 0b101);
  map.unlock_sublogs(0b101);
  assert_eq!(map.lock_map.load(Ordering::Relaxed), 0);
}

#[test]
fn contended_bits_block() {
  let map = Arc::new(ShardedLogLockMap::new());
  map.lock_sublogs(0b1);

  // 线程尝试锁含冲突位的集合：持锁期间不得成功
  let map2 = map.clone();
  let acquired = Arc::new(AtomicU64::new(0));
  let acquired2 = acquired.clone();
  let handle = thread::spawn(move || {
    map2.lock_sublogs(0b11);
    acquired2.store(1, Ordering::Release);
  });
  while map.event.total_listeners() == 0 {
    thread::yield_now();
  }
  assert_eq!(acquired.load(Ordering::Acquire), 0);

  map.unlock_sublogs(0b1);
  handle.join().unwrap();
  assert_eq!(acquired.load(Ordering::Acquire), 1);
}

#[test]
fn non_contended_bits_proceed_concurrently() {
  let map = Arc::new(ShardedLogLockMap::new());
  map.lock_sublogs(0b01);

  // 不冲突的位图可直接并发获得
  let map2 = map.clone();
  let handle = thread::spawn(move || {
    map2.lock_sublogs(0b10);
    assert_eq!(map2.lock_map.load(Ordering::Relaxed) & 0b10, 0b10);
    map2.unlock_sublogs(0b10);
  });
  handle.join().unwrap();
  assert_eq!(map.lock_map.load(Ordering::Relaxed), 0b01);
  map.unlock_sublogs(0b01);
  assert_eq!(map.lock_map.load(Ordering::Relaxed), 0);
}
