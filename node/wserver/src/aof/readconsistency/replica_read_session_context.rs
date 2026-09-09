//! 副本一致读会话上下文（对标 libs/server/AOF/ReadConsistency/
//! ReplicaReadSessionContext.cs:ReplicaReadSessionContext + ReadSessionState
//! 的上下文子集）
//!
//! C# 以 StructLayout 显式布局的单结构承载；rust 侧以普通结构承接同字段面。
//! `cached_sublog_max` 以 Arc 共享数组承接 C# “批/会话上下文副本共享同一
//! 底层数组”的语义。

use std::sync::{
  Arc,
  atomic::{AtomicI64, Ordering},
};

use parking_lot::RwLock;

use super::virtual_sublog_replay_state::ReadSessionWaiter;

/// 副本一致读会话上下文。
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
  cached_sublog_max: Arc<RwLock<Vec<AtomicI64>>>,
  /// 会话自有的可复用等待节点（顺序等待，无争用）。
  waiter: Arc<ReadSessionWaiter>,
}

impl Default for ReplicaReadSessionContext {
  fn default() -> Self {
    Self {
      session_version: -1,
      maximum_session_sequence_number: 0,
      last_hash: 0,
      last_virtual_sublog_idx: -1,
      cached_sublog_max: Arc::new(RwLock::new(Vec::new())),
      waiter: Arc::new(ReadSessionWaiter::new()),
    }
  }
}

impl ReplicaReadSessionContext {
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

  /// libs/server/AOF/ReadConsistency/ReplicaReadSessionContext.cs:GetPowerOfTwoSize
  ///
  /// 向上取 2 的幂（1 → 1）。
  pub fn get_power_of_two_size(value: usize) -> usize {
    value.max(1).next_power_of_two()
  }

  /// libs/server/AOF/ReadConsistency/ReplicaReadSessionContext.cs:ExpandKeyHashCache
  ///
  /// 扩容缓存数组至 2 的幂（共享句柄：所有副本同步可见）。
  pub fn expand_key_hash_cache(&self, key_count: usize) {
    let new_size = Self::get_power_of_two_size(key_count);
    let mut cache = self.cached_sublog_max.write();
    if cache.len() >= new_size {
      return;
    }
    *cache = (0..new_size).map(|_| AtomicI64::new(0)).collect();
  }

  /// libs/server/AOF/ReadConsistency/ReplicaReadSessionContext.cs:ShrinkKeyHashCache
  ///
  /// 缩容缓存数组至 2 的幂下界（不高于 key_count 的最大 2 幂；1 保底）。
  pub fn shrink_key_hash_cache(&self, key_count: usize) {
    let mut cache = self.cached_sublog_max.write();
    let new_size = if key_count == 0 {
      0
    } else {
      Self::get_power_of_two_size(key_count) / 2
    };
    cache.truncate(new_size);
  }

  /// 缓存下标读取（越界回 0：缓存未热身的良性路径）。
  pub fn cached_sublog_max(&self, idx: usize) -> i64 {
    self
      .cached_sublog_max
      .read()
      .get(idx)
      .map_or(0, |slot| slot.load(Ordering::Acquire))
  }

  /// 缓存下标写入。
  pub fn set_cached_sublog_max(&self, idx: usize, value: i64) {
    let cache = self.cached_sublog_max.read();
    if let Some(slot) = cache.get(idx) {
      slot.store(value, Ordering::Release);
    }
  }

  /// libs/server/AOF/ReadConsistency/ReplicaReadSessionContext.cs:ResetCachedSublogMax
  ///
  /// 清零缓存最大值（版本变更时）。
  pub fn reset_cached_sublog_max(&self) {
    for slot in self.cached_sublog_max.read().iter() {
      slot.store(0, Ordering::Release);
    }
  }

  /// 缓存数组长度（测试/校验面）。
  pub fn cached_len(&self) -> usize {
    self.cached_sublog_max.read().len()
  }
}

#[cfg(test)]
mod tests {
  use super::*;

  #[test]
  fn power_of_two_sizes() {
    assert_eq!(ReplicaReadSessionContext::get_power_of_two_size(0), 1);
    assert_eq!(ReplicaReadSessionContext::get_power_of_two_size(1), 1);
    assert_eq!(ReplicaReadSessionContext::get_power_of_two_size(5), 8);
    assert_eq!(ReplicaReadSessionContext::get_power_of_two_size(16), 16);
  }

  #[test]
  fn expand_shrink_and_cache_roundtrip() {
    let ctx = ReplicaReadSessionContext::default();
    assert_eq!(ctx.session_version(), -1);
    ctx.expand_key_hash_cache(5);
    assert_eq!(ctx.cached_len(), 8);
    ctx.set_cached_sublog_max(3, 99);
    assert_eq!(ctx.cached_sublog_max(3), 99);
    // 未热身下标良性回 0
    assert_eq!(ctx.cached_sublog_max(100), 0);

    // 缩容至 4：下标 3 仍在界内，值保留
    ctx.shrink_key_hash_cache(5);
    assert_eq!(ctx.cached_len(), 4);
    assert_eq!(ctx.cached_sublog_max(3), 99);
    // 进一步缩容至 2：下标 3 出界丢弃
    ctx.shrink_key_hash_cache(3);
    assert_eq!(ctx.cached_len(), 2);
    assert_eq!(ctx.cached_sublog_max(3), 0);

    ctx.reset_cached_sublog_max();
    ctx.set_cached_sublog_max(1, 7);
    ctx.reset_cached_sublog_max();
    assert_eq!(ctx.cached_sublog_max(1), 0);
  }

  #[test]
  fn session_state_defaults_and_setters() {
    let mut ctx = ReplicaReadSessionContext::default();
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
    // 等待节点可用
    let _ = ctx.waiter();
  }
}
