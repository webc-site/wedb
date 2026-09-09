//! 物理子日志集合的访问位图锁（对标 libs/server/AOF/ShardedLog.cs:ShardedLog
//! 的 LockSublogs / UnlockSublogs 子集）。

use std::sync::atomic::{AtomicU64, Ordering};

/// CAS 位图锁：`log_access_bitmap` 的每个置位位对应一个物理子日志；
/// 请求的位全空闲（与 lockMap 无交集）时原子占位。
pub struct ShardedLogLockMap {
  /// 子日志占用位图。
  lock_map: AtomicU64,
}

impl Default for ShardedLogLockMap {
  fn default() -> Self {
    Self::new()
  }
}

impl ShardedLogLockMap {
  /// libs/server/AOF/ShardedLog.cs:ShardedLog（lockMap 字段初始化）。
  pub fn new() -> Self {
    Self {
      lock_map: AtomicU64::new(0),
    }
  }

  /// libs/server/AOF/ShardedLog.cs:LockSublogs
  ///
  /// 占用 `log_access_bitmap` 标记的子日志集合；与既有占用冲突时自旋重试。
  /// 子日志数 <= 64，故 u64 位图可全覆盖。
  pub fn lock_sublogs(&self, log_access_bitmap: u64) {
    loop {
      let current = self.lock_map.load(Ordering::Acquire);
      if current & log_access_bitmap == 0 {
        let new_map = current | log_access_bitmap;
        if self
          .lock_map
          .compare_exchange_weak(current, new_map, Ordering::AcqRel, Ordering::Acquire)
          .is_ok()
        {
          return;
        }
      }
      std::hint::spin_loop();
    }
  }

  /// libs/server/AOF/ShardedLog.cs:UnlockSublogs
  ///
  /// 释放 `log_access_bitmap` 标记的子日志集合（必须先已占用）。
  pub fn unlock_sublogs(&self, mut log_access_bitmap: u64) {
    debug_assert!(self.lock_map.load(Ordering::Relaxed) & log_access_bitmap > 0);
    log_access_bitmap = !log_access_bitmap;
    self.lock_map.fetch_and(log_access_bitmap, Ordering::Release);
  }
}

#[cfg(test)]
mod tests {
  use std::sync::{
    atomic::{AtomicU64, Ordering},
    Arc,
  };

  use super::ShardedLogLockMap;

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

    // 线程尝试锁含冲突位的集合：持锁期间不得成功。
    let map2 = map.clone();
    let acquired = Arc::new(AtomicU64::new(0));
    let acquired2 = acquired.clone();
    let handle = std::thread::spawn(move || {
      map2.lock_sublogs(0b11);
      acquired2.store(1, Ordering::Release);
    });
    std::thread::sleep(std::time::Duration::from_millis(5));
    assert_eq!(acquired.load(Ordering::Acquire), 0);

    map.unlock_sublogs(0b1);
    handle.join().unwrap();
    assert_eq!(acquired.load(Ordering::Acquire), 1);
  }
}
