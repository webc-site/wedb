//! 副本一致读会话上下文与状态机（对标 libs/server/AOF/ReadConsistency/
//! ReplicaReadSessionContext.cs:ReplicaReadSessionContext + ReadSessionState）
//!
//! C# 以 StructLayout 显式布局承载会话上下文，以 ReadSessionState 统一承接
//! 会话级读锁防护、批处理哈希缓存管理及上下游协议回调。
//! `cached_sublog_max` 以 `Arc<[AtomicI64]>` 承接 C# 跨上下文副本共享同一底层数组语义，
//! 实现零全局锁读写。

use std::{
  sync::{
    Arc,
    atomic::{AtomicI64, Ordering},
  },
  time::Duration,
};

use parking_lot::{Mutex, RwLock};

use super::{
  read_consistency_manager::ReadConsistencyManager, virtual_sublog_replay_state::ReadSessionWaiter,
};

/// 副本一致读会话上下文（对标 libs/server/AOF/ReadConsistency/ReplicaReadSessionContext.cs:ReplicaReadSessionContext）
#[derive(Clone)]
pub struct ReplicaReadSessionContext {
  /// 会话版本（manager 版本变更即重置；-1 = 首次）。
  session_version: i64,
  /// 所有已读 key 建立的最大会话序列号。
  maximum_session_sequence_number: i64,
  /// 最近一次读取的 key 哈希。
  last_hash: i64,
  /// 最近一次读取的虚拟子日志下标（-1 = 无）。
  last_virtual_sublog_idx: i32,
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

impl ReplicaReadSessionContext {
  /// 构造指定虚拟子日志规模的会话上下文。
  pub fn new(virtual_sublog_count: usize) -> Self {
    let count = virtual_sublog_count.max(1);
    let mut slots = Vec::with_capacity(count);
    slots.resize_with(count, || AtomicI64::new(0));
    Self {
      session_version: -1,
      maximum_session_sequence_number: 0,
      last_hash: 0,
      last_virtual_sublog_idx: -1,
      cached_sublog_max: slots.into(),
      waiter: Arc::new(ReadSessionWaiter::new()),
    }
  }

  /// 复制标量会话状态（序列号/前驱子日志/哈希），避免克隆 Arc 数组与等待节点句柄的原子引用计数开销
  #[inline]
  pub fn copy_state_from(&mut self, other: &Self) {
    self.session_version = other.session_version;
    self.maximum_session_sequence_number = other.maximum_session_sequence_number;
    self.last_hash = other.last_hash;
    self.last_virtual_sublog_idx = other.last_virtual_sublog_idx;
  }

  /// 会话版本。
  #[inline]
  pub fn session_version(&self) -> i64 {
    self.session_version
  }

  /// 会话版本写入。
  #[inline]
  pub fn set_session_version(&mut self, version: i64) {
    self.session_version = version;
  }

  /// 最大会话序列号。
  #[inline]
  pub fn maximum_session_sequence_number(&self) -> i64 {
    self.maximum_session_sequence_number
  }

  /// 最大会话序列号推进（单调；C# 直写字段的收敛形态）。
  #[inline]
  pub fn advance_maximum_session_sequence_number(&mut self, value: i64) {
    self.maximum_session_sequence_number = self.maximum_session_sequence_number.max(value);
  }

  /// 最大会话序列号直写（C# 字段赋值形态）。
  #[inline]
  pub fn set_maximum_session_sequence_number(&mut self, value: i64) {
    self.maximum_session_sequence_number = value;
  }

  /// 最近读取的 key 哈希。
  #[inline]
  pub fn last_hash(&self) -> i64 {
    self.last_hash
  }

  /// 最近读取的 key 哈希写入。
  #[inline]
  pub fn set_last_hash(&mut self, hash: i64) {
    self.last_hash = hash;
  }

  /// 最近读取的虚拟子日志下标。
  #[inline]
  pub fn last_virtual_sublog_idx(&self) -> i32 {
    self.last_virtual_sublog_idx
  }

  /// 最近读取的虚拟子日志下标写入。
  #[inline]
  pub fn set_last_virtual_sublog_idx(&mut self, idx: i32) {
    self.last_virtual_sublog_idx = idx;
  }

  /// 等待节点句柄。
  pub fn waiter(&self) -> Arc<ReadSessionWaiter> {
    Arc::clone(&self.waiter)
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

  /// 缓存数组长度（测试/校验面）。
  #[inline]
  pub fn cached_len(&self) -> usize {
    self.cached_sublog_max.len()
  }
}

/// 读会话状态机（对标 libs/server/AOF/ReadConsistency/ReplicaReadSessionContext.cs:ReadSessionState）
pub struct ReadSessionState {
  manager: Arc<ReadConsistencyManager>,
  replica_read_context: Mutex<ReplicaReadSessionContext>,
  batch_read_context: Mutex<ReplicaReadSessionContext>,
  read_timeout: Duration,
  in_progress: RwLock<()>,
  key_hash_cache: Mutex<Vec<i64>>,
}

impl ReadSessionState {
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
      replica_read_context: Mutex::new(replica_read_context),
      batch_read_context: Mutex::new(batch_read_context),
      read_timeout,
      in_progress: RwLock::new(()),
      key_hash_cache: Mutex::new(Vec::new()),
    }
  }

  /// 向上取 2 的幂（对标 C# GetPowerOfTwoSize）。
  #[inline]
  pub fn get_power_of_two_size(value: usize) -> usize {
    value.max(1).next_power_of_two()
  }

  /// 扩容批处理 key 哈希缓存（对标 C# ExpandKeyHashCache）。
  pub fn expand_key_hash_cache(&self, key_count: usize) {
    let new_size = Self::get_power_of_two_size(key_count);
    let mut cache = self.key_hash_cache.lock();
    if cache.len() < new_size {
      cache.resize(new_size, 0);
    }
  }

  /// 缩容批处理 key 哈希缓存（对标 C# ShrinkKeyHashCache）。
  pub fn shrink_key_hash_cache(&self, key_count: usize) {
    let new_size = Self::get_power_of_two_size(key_count);
    let mut cache = self.key_hash_cache.lock();
    cache.resize(new_size, 0);
  }

  /// 批缓存当前容量（测试/诊断）。
  pub fn key_hash_cache_len(&self) -> usize {
    self.key_hash_cache.lock().len()
  }

  /// 单 key 读前新鲜度同步校验（对标 C# PreSingleKeyConsistentRead）。
  pub fn pre_single_key_consistent_read(&self, hash: i64) {
    let guard = self.in_progress.read();
    let mut ctx = self.replica_read_context.lock();
    self
      .manager
      .pre_single_key_consistent_read(hash & i64::MAX, &mut ctx, self.read_timeout);
    drop(guard);
  }

  /// 单 key 读后推进会话序列号回调（对标 C# PostSingleKeyConsistentReadCallback）。
  pub fn post_single_key_consistent_read_callback(&self) {
    let guard = self.in_progress.read();
    let mut ctx = self.replica_read_context.lock();
    self.manager.post_single_key_consistent_read(&mut ctx);
    drop(guard);
  }

  /// 批量键一致读前半协议（对标 C# PreBatchKeyConsistentReadCallback）。
  pub fn pre_batch_key_consistent_read_callback(&self, keys: &[&[u8]]) {
    let guard = self.in_progress.read();
    let key_count = keys.len();
    let mut replica_ctx = self.replica_read_context.lock();
    self
      .manager
      .check_consistency_manager_version(&mut replica_ctx);

    let mut cache = self.key_hash_cache.lock();
    if cache.len() < key_count || (key_count > 0 && (key_count << 2) < cache.len()) {
      let new_size = Self::get_power_of_two_size(key_count);
      cache.resize(new_size, 0);
    }

    let mut batch_ctx = self.batch_read_context.lock();
    batch_ctx.copy_state_from(&replica_ctx);

    for (i, &key) in keys.iter().enumerate() {
      let hash = self
        .manager
        .pre_batch_key_consistent_read(key, &mut batch_ctx, self.read_timeout);
      cache[i] = hash;
    }
    drop(guard);
  }

  /// 批量键读后校验（对标 C# PostBatchKeyConsistentReadCallback）。
  pub fn post_batch_key_consistent_read_callback(&self, key_count: usize) -> bool {
    let guard = self.in_progress.read();
    let cache = self.key_hash_cache.lock();
    let batch_ctx = self.batch_read_context.lock();

    for &hash in cache.iter().take(key_count) {
      if !self
        .manager
        .post_batch_key_consistent_read_validate(hash, &batch_ctx)
      {
        drop(guard);
        return false;
      }
    }

    // 校验通过：同步回传主会话上下文以维持前缀一致
    let mut replica_ctx = self.replica_read_context.lock();
    replica_ctx.copy_state_from(&batch_ctx);
    drop(guard);
    true
  }

  /// 会话上下文快照（测试/诊断）。
  pub fn replica_context_snapshot(&self) -> ReplicaReadSessionContext {
    self.replica_read_context.lock().clone()
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
    assert_eq!(state.key_hash_cache_len(), 8);

    state.shrink_key_hash_cache(5);
    assert_eq!(state.key_hash_cache_len(), 8);

    state.shrink_key_hash_cache(3);
    assert_eq!(state.key_hash_cache_len(), 4);
  }

  #[test]
  fn session_state_defaults_and_setters() {
    let mut ctx = ReplicaReadSessionContext::new(4);
    assert_eq!(ctx.last_virtual_sublog_idx(), -1);
    ctx.set_session_version(3);
    ctx.set_last_hash(0xdead);
    ctx.set_last_virtual_sublog_idx(2);
    ctx.set_maximum_session_sequence_number(10);
    ctx.advance_maximum_session_sequence_number(7);
    assert_eq!(ctx.maximum_session_sequence_number(), 10, "单调推进");
    ctx.advance_maximum_session_sequence_number(15);
    assert_eq!(ctx.maximum_session_sequence_number(), 15);
    assert_eq!(ctx.last_hash(), 0xdead);
    assert_eq!(ctx.last_virtual_sublog_idx(), 2);

    ctx.set_cached_sublog_max(1, 42);
    assert_eq!(ctx.cached_sublog_max(1), 42);
    ctx.reset_cached_sublog_max();
    assert_eq!(ctx.cached_sublog_max(1), 0);
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
