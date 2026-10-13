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
use crate::primary_tasks::PrimaryTasks;

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
  pub cached_sublog_max: Arc<[AtomicI64]>,
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
  fn copy_state_from(&self, other: &Self) {
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
  pub manager: Arc<ReadConsistencyManager>,
  pub replica_read_context: ReplicaReadSessionContext,
  pub batch_read_context: ReplicaReadSessionContext,
  pub read_timeout: Option<Duration>,
  /// 角色门（对齐 C# EnforceConsistentRead = 配置位 && clusterProvider.IsReplica()，
  /// StoreWrapper.cs:903-904 的动态分量）：pre 协议入口按当前角色判定，非副本
  /// 零 manager 开销直通；post 无门（推进为单调安全余量）
  pub role_gate: Option<Arc<PrimaryTasks>>,
  pub in_progress: RwLock<()>,
  pub key_hash_cache: RwLock<Vec<i64>>,
}

impl ReadSessionState {
  /// libs/server/AOF/ReadConsistency/ReplicaReadSessionContext.cs:ReadSessionState
  ///
  /// 构造读会话状态机（无角色门形态，测试用）。
  pub fn new(
    manager: Arc<ReadConsistencyManager>,
    virtual_sublog_count: usize,
    read_timeout: Option<Duration>,
  ) -> Self {
    let replica_read_context = ReplicaReadSessionContext::new(virtual_sublog_count);
    let batch_read_context = replica_read_context.clone();
    Self {
      manager,
      replica_read_context,
      batch_read_context,
      read_timeout,
      role_gate: None,
      in_progress: RwLock::new(()),
      key_hash_cache: RwLock::new(Vec::new()),
    }
  }

  /// 生产装配口：拓扑与超时自 manager 读取，挂接角色门（对标 C# 建会话时
  /// 按 EnableCluster+EnableAOF+MultiLogEnabled 创建 ReadSessionState，
  /// RespServerSession.cs:298-300；角色动态性由 pre 入口 is_replica 判定承接）
  pub fn attach(
    manager: Arc<ReadConsistencyManager>,
    role_gate: Option<Arc<PrimaryTasks>>,
  ) -> Self {
    let virtual_sublog_count = manager.virtual_sublog_count();
    let read_timeout = manager.read_timeout();
    Self::new(manager, virtual_sublog_count, read_timeout).with_role_gate(role_gate)
  }

  /// 挂接角色门（链式形态）。
  fn with_role_gate(mut self, role_gate: Option<Arc<PrimaryTasks>>) -> Self {
    self.role_gate = role_gate;
    self
  }

  /// libs/server/AOF/ReadConsistency/ReplicaReadSessionContext.cs:GetPowerOfTwoSize
  ///
  /// 向上取 2 的幂。
  #[inline]
  pub const fn get_power_of_two_size(value: usize) -> usize {
    if value <= 1 {
      1
    } else {
      1usize << (usize::BITS - (value - 1).leading_zeros())
    }
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
  /// 单 key 读前新鲜度同步校验。零互斥锁开销；角色门非副本直通，超时上抛。
  pub fn pre_single_key_consistent_read(&self, hash: i64) -> wkv::Result<()> {
    if self.role_gate.as_ref().is_some_and(|g| !g.is_replica()) {
      return Ok(());
    }
    let guard = self.in_progress.read();
    self.manager.pre_single_key_consistent_read(
      hash & i64::MAX,
      &self.replica_read_context,
      self.read_timeout,
    )?;
    drop(guard);
    Ok(())
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
  /// 批量键一致读前半协议；角色门非副本直通，超时上抛。
  pub fn pre_batch_key_consistent_read_callback(&self, keys: &[&[u8]]) -> wkv::Result<()> {
    if self.role_gate.as_ref().is_some_and(|g| !g.is_replica()) {
      return Ok(());
    }
    let guard = self.in_progress.read();
    let key_count = keys.len();
    self
      .manager
      .check_consistency_manager_version(&self.replica_read_context);

    // 对标 C# 两臂分派：缓存缺失/过小走扩容、占比过低走缩容，
    // expand/shrink 是伸缩策略唯一入口（peek 读锁语句末即释放，与后续写锁无重叠）
    let cache_len = self.key_hash_cache.read().len();
    if cache_len < key_count {
      self.expand_key_hash_cache(key_count);
    } else if (key_count << 2) < cache_len {
      self.shrink_key_hash_cache(key_count);
    }

    let mut cache = self.key_hash_cache.write();
    self
      .batch_read_context
      .copy_state_from(&self.replica_read_context);

    for (i, &key) in keys.iter().enumerate() {
      let hash = self.manager.pre_batch_key_consistent_read(
        key,
        &self.batch_read_context,
        self.read_timeout,
      )?;
      cache[i] = hash;
    }
    drop(guard);
    Ok(())
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
}

impl wkv::ConsistentReadFunctions for ReadSessionState {
  fn pre_single_key_consistent_read(&self, hash: i64) -> wkv::Result<()> {
    ReadSessionState::pre_single_key_consistent_read(self, hash)
  }

  fn post_single_key_consistent_read_callback(&self) {
    ReadSessionState::post_single_key_consistent_read_callback(self);
  }

  fn pre_batch_key_consistent_read_callback(&self, keys: &[&[u8]]) -> wkv::Result<()> {
    ReadSessionState::pre_batch_key_consistent_read_callback(self, keys)
  }

  fn post_batch_key_consistent_read_callback(&self, key_count: usize) -> bool {
    ReadSessionState::post_batch_key_consistent_read_callback(self, key_count)
  }
}
