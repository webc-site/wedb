//! 物理子日志集合与访问位图锁（对标 libs/server/AOF/ShardedLog.cs:ShardedLog）。

use std::{
  hint,
  sync::{
    Arc,
    atomic::{AtomicU64, Ordering},
  },
};

use waof::AofAddress;

use super::sublog::Sublog;

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
  /// 初始化子日志位图锁
  pub fn new() -> Self {
    Self {
      lock_map: AtomicU64::new(0),
    }
  }

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
      hint::spin_loop();
    }
  }

  /// 释放 `log_access_bitmap` 标记的子日志集合（必须先已占用）。
  pub fn unlock_sublogs(&self, mut log_access_bitmap: u64) {
    debug_assert_eq!(
      self.lock_map.load(Ordering::Relaxed) & log_access_bitmap,
      log_access_bitmap
    );
    log_access_bitmap = !log_access_bitmap;
    self
      .lock_map
      .fetch_and(log_access_bitmap, Ordering::Release);
  }
}

/// 分片日志抽象（对标 libs/server/AOF/ShardedLog.cs:ShardedLog）。
pub struct ShardedLog {
  /// 物理子日志实例集合（对标 C# TsavoriteLog[] sublog）。
  pub sublog: Vec<Arc<Sublog>>,
  /// 子日志位图锁。
  lock_map: ShardedLogLockMap,
}

impl ShardedLog {
  /// libs/server/AOF/ShardedLog.cs:ShardedLog
  pub fn new(sublogs: Vec<Arc<Sublog>>) -> Self {
    Self {
      sublog: sublogs,
      lock_map: ShardedLogLockMap::new(),
    }
  }

  /// 物理子日志数（对标 C# Length）。
  #[inline]
  pub fn len(&self) -> usize {
    self.sublog.len()
  }

  #[inline]
  pub fn is_empty(&self) -> bool {
    self.sublog.is_empty()
  }

  /// libs/server/AOF/ShardedLog.cs:LockSublogs
  #[inline]
  pub fn lock_sublogs(&self, log_access_bitmap: u64) {
    debug_assert!(log_access_bitmap.count_ones() as usize <= self.len());
    self.lock_map.lock_sublogs(log_access_bitmap);
  }

  /// libs/server/AOF/ShardedLog.cs:UnlockSublogs
  #[inline]
  pub fn unlock_sublogs(&self, log_access_bitmap: u64) {
    debug_assert!(log_access_bitmap.count_ones() as usize <= self.len());
    self.lock_map.unlock_sublogs(log_access_bitmap);
  }

  /// libs/server/AOF/ShardedLog.cs:BeginAddress
  pub fn begin_address(&self) -> AofAddress {
    let mut result = AofAddress::create(self.len() as i32, 0);
    for (i, log) in self.sublog.iter().enumerate() {
      result[i] = log.begin_address();
    }
    result
  }

  /// libs/server/AOF/ShardedLog.cs:TailAddress
  pub fn tail_address(&self) -> AofAddress {
    let mut result = AofAddress::create(self.len() as i32, 0);
    for (i, log) in self.sublog.iter().enumerate() {
      result[i] = log.tail_address();
    }
    result
  }

  /// libs/server/AOF/ShardedLog.cs:CommittedUntilAddress
  pub fn committed_until_address(&self) -> AofAddress {
    let mut result = AofAddress::create(self.len() as i32, 0);
    for (i, log) in self.sublog.iter().enumerate() {
      result[i] = log.committed_until_address();
    }
    result
  }

  /// libs/server/AOF/ShardedLog.cs:CommittedBeginAddress
  pub fn committed_begin_address(&self) -> AofAddress {
    let mut result = AofAddress::create(self.len() as i32, 0);
    for (i, log) in self.sublog.iter().enumerate() {
      result[i] = log.begin_address();
    }
    result
  }

  /// libs/server/AOF/ShardedLog.cs:FlushedUntilAddress
  pub fn flushed_until_address(&self) -> AofAddress {
    let mut result = AofAddress::create(self.len() as i32, 0);
    for (i, log) in self.sublog.iter().enumerate() {
      result[i] = log.flushed_until_address();
    }
    result
  }

  /// libs/server/AOF/ShardedLog.cs:HeaderSize
  #[inline]
  pub fn header_size(&self) -> i64 {
    24
  }

  /// libs/server/AOF/ShardedLog.cs:MaxMemorySizeBytes
  pub fn max_memory_size_bytes(&self) -> AofAddress {
    let mut result = AofAddress::create(self.len() as i32, 0);
    for (i, log) in self.sublog.iter().enumerate() {
      result[i] = log.memory_size_bytes();
    }
    result
  }

  /// libs/server/AOF/ShardedLog.cs:MemorySizeBytes
  pub fn memory_size_bytes(&self) -> AofAddress {
    let mut result = AofAddress::create(self.len() as i32, 0);
    for (i, log) in self.sublog.iter().enumerate() {
      result[i] = log.memory_size_bytes();
    }
    result
  }

  /// libs/server/AOF/ShardedLog.cs:RecoverAsync
  pub async fn recover_async(&self) {
    for log in &self.sublog {
      log.recover_async().await;
    }
  }

  /// libs/server/AOF/ShardedLog.cs:Reset
  pub fn reset(&self) {
    for log in &self.sublog {
      log.reset();
    }
  }

  /// libs/server/AOF/ShardedLog.cs:Initialize
  pub fn initialize(
    &self,
    begin_address: &AofAddress,
    committed_until_address: &AofAddress,
    last_commit_num: i64,
  ) {
    for (i, log) in self.sublog.iter().enumerate() {
      log.safe_initialize(
        begin_address[i],
        committed_until_address[i],
        last_commit_num,
      );
    }
  }

  /// libs/server/AOF/ShardedLog.cs:SafeInitialize
  #[inline]
  pub fn safe_initialize(
    &self,
    sublog_idx: usize,
    begin_address: i64,
    committed_until_address: i64,
    last_commit_num: i64,
  ) {
    self.sublog[sublog_idx].safe_initialize(
      begin_address,
      committed_until_address,
      last_commit_num,
    );
  }

  #[inline]
  pub fn get_sub_log(&self, sublog_idx: usize) -> &Arc<Sublog> {
    &self.sublog[sublog_idx]
  }

  #[inline]
  pub fn get_tail_address(&self, sublog_idx: usize) -> i64 {
    self.sublog[sublog_idx].tail_address()
  }

  #[inline]
  pub fn get_begin_address(&self, sublog_idx: usize) -> i64 {
    self.sublog[sublog_idx].begin_address()
  }
}

#[cfg(test)]
mod tests {
  use std::{
    sync::{
      Arc,
      atomic::{AtomicU64, Ordering},
    },
    thread,
    time::Duration,
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
    let handle = thread::spawn(move || {
      map2.lock_sublogs(0b11);
      acquired2.store(1, Ordering::Release);
    });
    thread::sleep(Duration::from_millis(5));
    assert_eq!(acquired.load(Ordering::Acquire), 0);

    map.unlock_sublogs(0b1);
    handle.join().unwrap();
    assert_eq!(acquired.load(Ordering::Acquire), 1);
  }
}
