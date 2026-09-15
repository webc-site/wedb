//! 副本一致读会话上下文与状态机（对标 libs/server/AOF/ReadConsistency/
//! ReplicaReadSessionContext.cs:ReplicaReadSessionContext + ReadSessionState）
//!
//! C# 以 StructLayout 显式布局承载会话上下文，以 ReadSessionState 统一承接
//! 会话级读锁防护、批处理哈希缓存管理及上下游协议回调。
//! `cached_sublog_max` 以 `Arc<[AtomicI64]>` 承接 C# 跨上下文副本共享同一底层数组语义，
//! 实现零全局锁读写。各会话标量字段经 AtomicI64 / AtomicI32 实现单线程与并发安全无锁读写，
//! 彻底消除读一致性每次点查时的互斥锁（Mutex）开销。

use std::{
  sync::{
    Arc,
    atomic::{AtomicI32, AtomicI64, Ordering},
  },
  time::Duration,
};

use parking_lot::RwLock;

use super::{
  read_consistency_manager::ReadConsistencyManager, virtual_sublog_replay_state::ReadSessionWaiter,
};

/// libs/server/AOF/ReadConsistency/ReplicaReadSessionContext.cs:ReplicaReadSessionContext
///
/// 副本一致读会话上下文。
pub struct ReplicaReadSessionContext {
  /// 会话版本（manager 版本变更即重置；-1 = 首次）。
  session_version: AtomicI64,
  /// 所有已读 key 建立的最大会话序列号。
  maximum_session_sequence_number: AtomicI64,
  /// 最近一次读取的 key 哈希。
  last_hash: AtomicI64,
  /// 最近一次读取的虚拟子日志下标（-1 = 无）。
  last_virtual_sublog_idx: AtomicI32,
  /// 各虚拟子日志缓存的最大序列号（跨上下文副本共享）。
  cached_sublog_max: Arc<[AtomicI64]>,
  /// 会话自有的可复用等待节点（顺序等待，无争用）。
  waiter: Arc<ReadSessionWaiter>,
}

impl Default for ReplicaReadSessionContext {
  fn default() -> Self {
    Self::new(0)
  }
}

impl Clone for ReplicaReadSessionContext {
  fn clone(&self) -> Self {
    Self {
      session_version: AtomicI64::new(self.session_version.load(Ordering::Acquire)),
      maximum_session_sequence_number: AtomicI64::new(
        self.maximum_session_sequence_number.load(Ordering::Acquire),
      ),
      last_hash: AtomicI64::new(self.last_hash.load(Ordering::Acquire)),
      last_virtual_sublog_idx: AtomicI32::new(self.last_virtual_sublog_idx.load(Ordering::Acquire)),
      cached_sublog_max: Arc::clone(&self.cached_sublog_max),
      waiter: Arc::clone(&self.waiter),
    }
  }
}

impl ReplicaReadSessionContext {
  /// 构造指定虚拟子日志规模的会话上下文。
  pub fn new(virtual_sublog_count: usize) -> Self {
    let count = virtual_sublog_count.max(1);
    let mut slots = Vec::with_capacity(count);
    slots.resize_with(count, || AtomicI64::new(0));
    Self {
      session_version: AtomicI64::new(-1),
      maximum_session_sequence_number: AtomicI64::new(0),
      last_hash: AtomicI64::new(0),
      last_virtual_sublog_idx: AtomicI32::new(-1),
      cached_sublog_max: slots.into(),
      waiter: Arc::new(ReadSessionWaiter::new()),
    }
  }

  /// 复制标量会话状态（序列号/前驱子日志/哈希），避免克隆 Arc 数组与等待节点句柄的原子引用计数开销
  #[inline]
  pub fn copy_state_from(&self, other: &Self) {
    self.session_version.store(
      other.session_version.load(Ordering::Acquire),
      Ordering::Release,
    );
    self.maximum_session_sequence_number.store(
      other
        .maximum_session_sequence_number
        .load(Ordering::Acquire),
      Ordering::Release,
    );
    self
      .last_hash
      .store(other.last_hash.load(Ordering::Acquire), Ordering::Release);
    self.last_virtual_sublog_idx.store(
      other.last_virtual_sublog_idx.load(Ordering::Acquire),
      Ordering::Release,
    );
  }

  /// 会话版本。
  #[inline]
  pub fn session_version(&self) -> i64 {
    self.session_version.load(Ordering::Acquire)
  }

  /// 会话版本写入。
  #[inline]
  pub fn set_session_version(&self, version: i64) {
    self.session_version.store(version, Ordering::Release);
  }

  /// 最大会话序列号。
  #[inline]
  pub fn maximum_session_sequence_number(&self) -> i64 {
    self.maximum_session_sequence_number.load(Ordering::Acquire)
  }

  /// 最大会话序列号推进（单调；C# 直写字段的收敛形态）。
  #[inline]
  pub fn advance_maximum_session_sequence_number(&self, value: i64) {
    if self.maximum_session_sequence_number.load(Ordering::Relaxed) < value {
      self
        .maximum_session_sequence_number
        .fetch_max(value, Ordering::AcqRel);
    }
  }

  /// 最大会话序列号直写（C# 字段赋值形态）。
  #[inline]
  pub fn set_maximum_session_sequence_number(&self, value: i64) {
    self
      .maximum_session_sequence_number
      .store(value, Ordering::Release);
  }

  /// 最近读取的 key 哈希。
  #[inline]
  pub fn last_hash(&self) -> i64 {
    self.last_hash.load(Ordering::Acquire)
  }

  /// 最近读取的 key 哈希写入。
  #[inline]
  pub fn set_last_hash(&self, hash: i64) {
    self.last_hash.store(hash, Ordering::Release);
  }

  /// 最近读取的虚拟子日志下标。
  #[inline]
  pub fn last_virtual_sublog_idx(&self) -> i32 {
    self.last_virtual_sublog_idx.load(Ordering::Acquire)
  }

  /// 最近读取的虚拟子日志下标写入。
  #[inline]
  pub fn set_last_virtual_sublog_idx(&self, idx: i32) {
    self.last_virtual_sublog_idx.store(idx, Ordering::Release);
  }

  /// 等待节点句柄（零原子克隆引用）。
  #[inline]
  pub fn waiter(&self) -> &Arc<ReadSessionWaiter> {
    &self.waiter
  }

  /// 缓存下标读取（越界回 0：缓存未热身的良性路径）。
  #[inline]
  pub fn cached_sublog_max(&self, idx: usize) -> i64 {
    self
      .cached_sublog_max
      .get(idx)
      .map_or(0, |slot| slot.load(Ordering::Acquire))
  }

  /// 缓存下标写入。
  #[inline]
  pub fn set_cached_sublog_max(&self, idx: usize, value: i64) {
    if let Some(slot) = self.cached_sublog_max.get(idx) {
      slot.store(value, Ordering::Release);
    }
  }

  /// libs/server/AOF/ReadConsistency/ReplicaReadSessionContext.cs:ResetCachedSublogMax
  ///
  /// 清零缓存最大值（版本变更时）。
  #[inline]
  pub fn reset_cached_sublog_max(&self) {
    for slot in self.cached_sublog_max.iter() {
      slot.store(0, Ordering::Release);
    }
  }
}

/// libs/server/AOF/ReadConsistency/ReplicaReadSessionContext.cs:ReadSessionState
///
/// 读会话状态机。内部持有的标量状态机字段与批缓存完全消除互斥锁，
/// 单 key 点查路径零锁争用。
pub struct ReadSessionState {
  manager: Arc<ReadConsistencyManager>,
  replica_read_context: ReplicaReadSessionContext,
  batch_read_context: ReplicaReadSessionContext,
  read_timeout: Duration,
  in_progress: RwLock<()>,
  key_hash_cache: RwLock<Vec<i64>>,
}

impl ReadSessionState {
  /// libs/server/AOF/ReadConsistency/ReplicaReadSessionContext.cs:ReadSessionState
  ///
  /// 构造读会话状态机。
  pub fn new(
    manager: Arc<ReadConsistencyManager>,
    virtual_sublog_count: usize,
    read_timeout: Duration,
  ) -> Self {
    let replica_read_context = ReplicaReadSessionContext::new(virtual_sublog_count);
    let batch_read_context = replica_read_context.clone();
    Self {
      manager,
      replica_read_context,
      batch_read_context,
      read_timeout,
      in_progress: RwLock::new(()),
      key_hash_cache: RwLock::new(Vec::new()),
    }
  }

  /// libs/server/AOF/ReadConsistency/ReplicaReadSessionContext.cs:GetPowerOfTwoSize
  ///
  /// 向上取 2 的幂。
  #[inline]
  pub fn get_power_of_two_size(value: usize) -> usize {
    value.max(1).next_power_of_two()
  }

  /// libs/server/AOF/ReadConsistency/ReplicaReadSessionContext.cs:ExpandKeyHashCache
  ///
  /// 扩容批处理 key 哈希缓存。
  pub fn expand_key_hash_cache(&self, key_count: usize) {
    let new_size = Self::get_power_of_two_size(key_count);
    let mut cache = self.key_hash_cache.write();
    if cache.len() < new_size {
      cache.resize(new_size, 0);
    }
  }

  /// libs/server/AOF/ReadConsistency/ReplicaReadSessionContext.cs:ShrinkKeyHashCache
  ///
  /// 缩容批处理 key 哈希缓存。
  pub fn shrink_key_hash_cache(&self, key_count: usize) {
    let new_size = Self::get_power_of_two_size(key_count);
    let mut cache = self.key_hash_cache.write();
    cache.resize(new_size, 0);
  }

  /// libs/server/AOF/ReadConsistency/ReplicaReadSessionContext.cs:PreSingleKeyConsistentRead
  ///
  /// 单 key 读前新鲜度同步校验。零互斥锁开销。
  pub fn pre_single_key_consistent_read(&self, hash: i64) {
    let guard = self.in_progress.read();
    self.manager.pre_single_key_consistent_read(
      hash & i64::MAX,
      &self.replica_read_context,
      self.read_timeout,
    );
    drop(guard);
  }

  /// libs/server/AOF/ReadConsistency/ReplicaReadSessionContext.cs:PostSingleKeyConsistentReadCallback
  ///
  /// 单 key 读后推进会话序列号回调。零互斥锁开销。
  pub fn post_single_key_consistent_read_callback(&self) {
    let guard = self.in_progress.read();
    self
      .manager
      .post_single_key_consistent_read(&self.replica_read_context);
    drop(guard);
  }

  /// libs/server/AOF/ReadConsistency/ReplicaReadSessionContext.cs:PreBatchKeyConsistentReadCallback
  ///
  /// 批量键一致读前半协议。
  pub fn pre_batch_key_consistent_read_callback(&self, keys: &[&[u8]]) {
    let guard = self.in_progress.read();
    let key_count = keys.len();
    self
      .manager
      .check_consistency_manager_version(&self.replica_read_context);

    let mut cache = self.key_hash_cache.write();
    if cache.len() < key_count || (key_count > 0 && (key_count << 2) < cache.len()) {
      let new_size = Self::get_power_of_two_size(key_count);
      cache.resize(new_size, 0);
    }

    self
      .batch_read_context
      .copy_state_from(&self.replica_read_context);

    for (i, &key) in keys.iter().enumerate() {
      let hash = self.manager.pre_batch_key_consistent_read(
        key,
        &self.batch_read_context,
        self.read_timeout,
      );
      cache[i] = hash;
    }
    drop(guard);
  }

  /// libs/server/AOF/ReadConsistency/ReplicaReadSessionContext.cs:PostBatchKeyConsistentReadCallback
  ///
  /// 批量键读后校验。
  pub fn post_batch_key_consistent_read_callback(&self, key_count: usize) -> bool {
    let guard = self.in_progress.read();
    let cache = self.key_hash_cache.read();

    for &hash in cache.iter().take(key_count) {
      if !self
        .manager
        .post_batch_key_consistent_read_validate(hash, &self.batch_read_context)
      {
        drop(guard);
        return false;
      }
    }

    // 校验通过：同步回传主会话上下文以维持前缀一致
    self
      .replica_read_context
      .copy_state_from(&self.batch_read_context);
    drop(guard);
    true
  }

  /// 会话上下文快照（测试/诊断）。
  pub fn replica_context_snapshot(&self) -> ReplicaReadSessionContext {
    self.replica_read_context.clone()
  }
}

impl wkv::ConsistentReadFunctions for ReadSessionState {
  fn pre_single_key_consistent_read(&self, hash: i64) {
    ReadSessionState::pre_single_key_consistent_read(self, hash);
  }

  fn post_single_key_consistent_read_callback(&self) {
    ReadSessionState::post_single_key_consistent_read_callback(self);
  }

  fn pre_batch_key_consistent_read_callback(&self, keys: &[&[u8]]) {
    ReadSessionState::pre_batch_key_consistent_read_callback(self, keys);
  }

  fn post_batch_key_consistent_read_callback(&self, key_count: usize) -> bool {
    ReadSessionState::post_batch_key_consistent_read_callback(self, key_count)
  }
}

#[cfg(test)]
mod tests {
  use super::*;

  #[test]
  fn power_of_two_sizes() {
    assert_eq!(ReadSessionState::get_power_of_two_size(0), 1);
    assert_eq!(ReadSessionState::get_power_of_two_size(1), 1);
    assert_eq!(ReadSessionState::get_power_of_two_size(5), 8);
    assert_eq!(ReadSessionState::get_power_of_two_size(16), 16);
  }

  #[test]
  fn expand_shrink_and_cache_roundtrip() {
    let manager = Arc::new(ReadConsistencyManager::new(1, 2, 2, -1, 0));
    let state = ReadSessionState::new(manager, 4, Duration::from_millis(100));

    state.expand_key_hash_cache(5);
    assert_eq!(state.key_hash_cache.read().len(), 8);

    state.shrink_key_hash_cache(5);
    assert_eq!(state.key_hash_cache.read().len(), 8);

    state.shrink_key_hash_cache(3);
    assert_eq!(state.key_hash_cache.read().len(), 4);
  }

  #[test]
  fn session_state_monotonic_sequence_and_reset() {
    let ctx = ReplicaReadSessionContext::new(4);
    assert_eq!(ctx.last_virtual_sublog_idx(), -1);
    ctx.set_maximum_session_sequence_number(10);
    ctx.advance_maximum_session_sequence_number(7);
    assert_eq!(
      ctx.maximum_session_sequence_number(),
      10,
      "单调推进：较小序列号不得倒退"
    );
    ctx.advance_maximum_session_sequence_number(15);
    assert_eq!(
      ctx.maximum_session_sequence_number(),
      15,
      "单调推进：较大序列号成功前移"
    );

    ctx.set_cached_sublog_max(1, 42);
    assert_eq!(ctx.cached_sublog_max(1), 42);
    ctx.reset_cached_sublog_max();
    assert_eq!(ctx.cached_sublog_max(1), 0, "reset 后缓存子日志序列号归零");
  }

  #[test]
  fn read_session_state_lifecycle() {
    let manager = Arc::new(ReadConsistencyManager::new(1, 2, 2, -1, 0));
    let state = Arc::new(ReadSessionState::new(
      manager,
      4,
      Duration::from_millis(100),
    ));

    // 单 key 前置与后置
    state.pre_single_key_consistent_read(0x1234);
    state.post_single_key_consistent_read_callback();

    // 批量读流程
    let keys: &[&[u8]] = &[b"key1", b"key2", b"key3"];
    state.pre_batch_key_consistent_read_callback(keys);
    assert!(state.post_batch_key_consistent_read_callback(keys.len()));
  }
}
