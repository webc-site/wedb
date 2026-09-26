//! 物理子日志集合与访问位图锁（对标 libs/server/AOF/ShardedLog.cs:ShardedLog）。

use std::sync::{
  Arc,
  atomic::{AtomicU64, Ordering},
};

use event_listener::{Event, Listener};
use waof::AofAddress;
use wbase::backoff::Backoff;

use super::waof_sublog::AofSublog;

/// libs/server/AOF/ShardedLog.cs:lockMap
///
/// CAS 位图锁：`log_access_bitmap` 的每个置位位对应一个物理子日志；
/// 请求的位全空闲（与 lockMap 无交集）时原子占位。
pub struct ShardedLogLockMap {
  /// 子日志占用位图。
  lock_map: AtomicU64,
  /// 锁释放事件通知器（消除死循环忙自旋）。
  event: Event,
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
      event: Event::new(),
    }
  }

  /// 占用 `log_access_bitmap` 标记的子日志集合；短自旋后转入事件等待，消除 CPU 忙等待。
  /// 子日志数 <= 64，故 u64 位图可全覆盖。
  pub fn lock_sublogs(&self, log_access_bitmap: u64) {
    // 1. 短自旋快路径（复用 wbase::backoff::Backoff 自适应退避）
    let mut backoff = Backoff::new();
    while backoff.stage().is_spin() {
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
      backoff.snooze();
    }

    // 2. 慢路径：事件驱动被动挂起，零 CPU 占用
    loop {
      let listener = self.event.listen();
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
      listener.wait();
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
    self.event.notify(usize::MAX);
  }
}

/// 分片日志抽象（对标 libs/server/AOF/ShardedLog.cs:ShardedLog）。
///
/// 成员集与 C# 一致：Length / sublog 集合 / 位图锁 / 7 个地址属性 /
/// RecoverAsync / Reset / Initialize。逐子日志的下标操作（GetSubLog、
/// GetTailAddress、SafeInitialize、Scan、Truncate…）由 GarnetLog 直接经
/// `sublog` 字段完成，对标 C# `shardedLog.sublog[idx]`
/// （GarnetLog.cs:260、:280、:300、:401、:429、:468），本类型不设索引转发。
pub struct ShardedLog {
  /// 物理子日志实例集合（对标 C# TsavoriteLog[] sublog）。
  pub sublog: Vec<Arc<AofSublog>>,
  /// 子日志位图锁。
  lock_map: ShardedLogLockMap,
}

impl ShardedLog {
  /// libs/server/AOF/ShardedLog.cs:ShardedLog
  pub fn new(sublogs: Vec<Arc<AofSublog>>) -> Self {
    Self {
      sublog: sublogs,
      lock_map: ShardedLogLockMap::new(),
    }
  }

  /// libs/server/AOF/ShardedLog.cs:Length
  ///
  /// 物理子日志数。
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

  /// 各子日志地址快照的单点收集器（C# 各属性各自展开的同构循环，
  /// rust 侧收敛：AofAddress::create(len,0) 后逐槽填充）。
  fn gather(&self, f: impl Fn(&AofSublog) -> i64) -> AofAddress {
    let mut result = AofAddress::create(self.len() as i32, 0);
    for (i, log) in self.sublog.iter().enumerate() {
      result[i] = f(log);
    }
    result
  }

  /// libs/server/AOF/ShardedLog.cs:BeginAddress
  pub fn begin_address(&self) -> AofAddress {
    self.gather(AofSublog::begin_address)
  }

  /// libs/server/AOF/ShardedLog.cs:TailAddress
  pub fn tail_address(&self) -> AofAddress {
    self.gather(AofSublog::tail_address)
  }

  /// libs/server/AOF/ShardedLog.cs:CommittedUntilAddress
  pub fn committed_until_address(&self) -> AofAddress {
    self.gather(AofSublog::committed_until_address)
  }

  /// libs/server/AOF/ShardedLog.cs:CommittedBeginAddress
  ///
  /// 各子日志已提交 begin 快照（TsavoriteLog.CommittedBeginAddress，
  /// TsavoriteLog.cs:120；非实时 begin）。
  pub fn committed_begin_address(&self) -> AofAddress {
    self.gather(AofSublog::committed_begin_address)
  }

  /// libs/server/AOF/ShardedLog.cs:FlushedUntilAddress
  pub fn flushed_until_address(&self) -> AofAddress {
    self.gather(AofSublog::flushed_until_address)
  }

  /// libs/server/AOF/ShardedLog.cs:MaxMemorySizeBytes
  pub fn max_memory_size_bytes(&self) -> AofAddress {
    self.gather(AofSublog::max_memory_size_bytes)
  }

  /// libs/server/AOF/ShardedLog.cs:MemorySizeBytes
  pub fn memory_size_bytes(&self) -> AofAddress {
    self.gather(AofSublog::memory_size_bytes)
  }

  /// libs/server/AOF/ShardedLog.cs:RecoverAsync
  ///
  /// 逐子日志串行恢复（C# :170-174 foreach await 同形）：首个 Err 即 `?`
  /// 中止——脏位点子日志绝不带病续跑后续子日志（续行门控同 SingleLog）。
  pub async fn recover_async(&self) -> waof::Result<()> {
    for log in &self.sublog {
      log.recover_async().await?;
    }
    Ok(())
  }

  /// libs/server/AOF/ShardedLog.cs:Reset
  pub async fn reset_async(&self) {
    for log in &self.sublog {
      log.reset_async().await;
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
}

#[cfg(test)]
mod tests {
  use std::{
    sync::{
      Arc,
      atomic::{AtomicU64, Ordering},
    },
    thread,
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
    while map.event.total_listeners() == 0 {
      thread::yield_now();
    }
    assert_eq!(acquired.load(Ordering::Acquire), 0);

    map.unlock_sublogs(0b1);
    handle.join().unwrap();
    assert_eq!(acquired.load(Ordering::Acquire), 1);
  }
}
