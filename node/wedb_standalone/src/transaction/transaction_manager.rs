//! 事务管理器（对标 libs/server/Transaction/TransactionManager.cs:TransactionManager）
//!
//! C# 事务闭包 = 上下文锁面（Tsavorite 事务上下文）+ AOF 事务条目 +
//! WATCH 校验 + 键集排序加锁；Rust 侧锁面由本域 [`TxnLockTable`] 承接，
//! WATCH 校验经 [`WatchVersionMap`]（与存储会话尾地址代理同向），AOF 事务
//! 条目直连 `GarnetLog::enqueue_txn`。上下文 Begin/End/LocksAcquired 等
//! Tsavorite 会话面在 wkv 纪元会话模型下为结构性空操作（文档就地标注）。
//!
//! C# 的 partial 拆分对应关系：
//! - TxnKeyManager.cs / TxnClusterSlotCheck.cs / TxnRespCommands.cs
//!   → 本结构在其他域文件的跨文件 `impl` 块；
//! - TxnState.cs → [`TxnState`]。

use std::{sync::Arc, time::Duration};

use bitflags::bitflags;

use super::{
  txn_key_entry::{LockType, TxnKeyEntries},
  txn_watched_keys_container::TxnWatchedKeysContainer,
  watch_version_map::WatchVersionMap,
};
use crate::{
  aof::{aof_entry_type::AofEntryType, garnet_log::GarnetLog},
  storage::session::storage_session::StoreType,
};

/// libs/server/Transaction/TxnState.cs:TxnState
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TxnState {
  /// 非事务模式
  None,
  /// MULTI 已入队、命令进入跳过（排队）模式
  Started,
  /// EXEC 后的执行中（IsSkippingOperations 为 false 的窗口）
  Running,
  /// 槽校验失败等导致的中止（EXEC 时报 EXECABORT）
  Aborted,
}

bitflags! {
  /// 事务触达的存储面（libs/server/Transaction/TransactionManager.cs:TransactionStoreTypes）
  #[derive(Debug, Clone, Copy, PartialEq, Eq)]
  pub struct TransactionStoreTypes: u8 {
    /// 无
    const None = 0;
    /// 主存
    const Main = 1;
    /// 对象存
    const Object = 1 << 1;
    /// 统一存
    const Unified = 1 << 2;
  }
}

/// 集群槽校验输入（C# ClusterSlotVerificationInput 的本域投影；完整校验面
/// 由 cluster 域承载）
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ClusterSlotVerificationInput {
  /// 事务是否只读（全共享锁）
  pub read_only: bool,
  /// 发起会话的 ASKING 计数
  pub session_asking: u8,
}

/// 自定义事务过程的最小面（C# CustomTransactionProcedure 的本域投影；
/// Prepare/Main/Finalize 三段式与失败快速路径开关对齐 C# 签名语义）
pub trait TxnProcedure {
  /// 过程 ID（AOF StoredProcedure 条目归属）
  fn id(&self) -> u8;
  /// 锁失败是否快速失败（C# FailFastOnKeyLockFailure）
  fn fail_fast_on_key_lock_failure(&self) -> bool {
    false
  }
  /// 锁超时（C# KeyLockTimeout）
  fn key_lock_timeout(&self) -> Duration {
    Duration::ZERO
  }
  /// 准备段：登记键与只读操作（C# Prepare，入参为只读 API）
  fn prepare(&mut self, txn_manager: &mut TransactionManager) -> bool;
  /// 主段：锁内执行（C# Main）
  fn main(&mut self, txn_manager: &mut TransactionManager, output: &mut Vec<u8>);
  /// 收尾段（C# Finalize；AOF 回放期间跳过）
  fn finalize(&mut self, txn_manager: &mut TransactionManager, output: &mut Vec<u8>);
}

/// 事务提升守卫：drop 即提交（C# TransactionGuard.Dispose → Commit(true)）
///
/// 由 [`TransactionManager::promote_to_transaction`] 产出，用于把两条及以上
/// 子命令固化为原子的 using 块。
pub struct TransactionGuard<'a> {
  /// 守卫的事务管理器；None = Null 守卫（已在事务内，无须提升）
  txn_manager: Option<&'a mut TransactionManager>,
}

impl TransactionGuard<'_> {
  /// C# TransactionGuard.Null：不承载管理器的空守卫
  fn null() -> Self {
    Self { txn_manager: None }
  }

  /// 守卫存活期内的事务状态（Null 守卫报告 None）
  pub fn state(&self) -> TxnState {
    self
      .txn_manager
      .as_deref()
      .map_or(TxnState::None, |txn_manager| txn_manager.state)
  }
}

impl Drop for TransactionGuard<'_> {
  fn drop(&mut self) {
    if let Some(txn_manager) = self.txn_manager.as_deref_mut() {
      txn_manager.commit(true);
    }
  }
}

/// 事务管理器
pub struct TransactionManager {
  /// 事务状态（C# state）
  pub state: TxnState,
  /// 事务键加锁集合（C# keyEntries）
  pub key_entries: TxnKeyEntries,
  /// 被监视键容器（C# watchContainer）
  pub watch_container: TxnWatchedKeysContainer,
  /// 事务触达的存储面（C# storeTypes）
  pub store_types: TransactionStoreTypes,
  /// 集群模式开关（C# clusterEnabled）
  pub cluster_enabled: bool,
  /// MULTI 起点（缓冲偏移；C# txnStartHead，EXEC 回退重放用）
  pub txn_start_head: usize,
  /// 事务内排队命令数（C# operationCntTxn）
  pub operation_cnt_txn: usize,
  /// 事务含写操作（决定 AOF 事务条目是否记录；C# PerformWrites）
  pub perform_writes: bool,
  /// 当前事务版本（C# txnVersion）
  pub txn_version: i64,
  /// 事务版本序列发生器（C# StateMachineDriver.Acquire/
  /// VerifyTransactionVersion 的本域代理：单发生器递增即校验通过）
  pub version_sequence: u64,
  /// 集群槽校验键列表（C# txnKeysParseState 的自有键副本列表）
  pub txn_keys: Vec<Box<[u8]>>,
  /// 上次登记键列表的接收缓冲标识（C# saveKeyRecvBufferPtr；托管缓冲
  /// 不做中途重分配，恒 None）
  pub save_key_recv_buffer_ptr: Option<usize>,
  /// AOF 事务日志（C# appendOnlyFile.Log；None = AofEnabled false）
  pub aof_log: Option<Arc<GarnetLog>>,
  /// 会话 ID（AOF 事务条目归属；C# stringBasicContext.Session.ID）
  pub session_id: i32,
  /// 存储过程模式（C# functionsState.StoredProcMode）
  pub stored_proc_mode: bool,
  /// 处于 AOF 回放（C# IsReplaying；回放期跳过 Finalize）
  pub is_replaying: bool,
}

impl TransactionManager {
  /// 构造事务管理器
  ///
  /// C# 构造从存储会话拆出各 store 上下文并建 WATCH 容器；Rust 侧存储面
  /// 由命令层经存储会话完成，此处仅持 WATCH 版本表、AOF 日志与集群开关。
  pub fn new(
    watch_version_map: Arc<WatchVersionMap>,
    aof_log: Option<Arc<GarnetLog>>,
    cluster_enabled: bool,
  ) -> Self {
    Self {
      state: TxnState::None,
      key_entries: TxnKeyEntries::new(16),
      watch_container: TxnWatchedKeysContainer::new(watch_version_map),
      store_types: TransactionStoreTypes::None,
      cluster_enabled,
      txn_start_head: 0,
      operation_cnt_txn: 0,
      perform_writes: false,
      txn_version: 0,
      version_sequence: 0,
      txn_keys: Vec::new(),
      save_key_recv_buffer_ptr: None,
      aof_log,
      session_id: 0,
      stored_proc_mode: false,
      is_replaying: false,
    }
  }

  /// 是否启用 AOF（libs/server/Transaction/TransactionManager.cs:AofEnabled）
  pub fn aof_enabled(&self) -> bool {
    self.aof_log.is_some()
  }

  /// 无参重置（libs/server/Transaction/TransactionManager.cs:Reset()）
  pub fn reset_current(&mut self) {
    let is_running = self.state == TxnState::Running;
    self.reset(is_running);
  }

  /// 重置事务状态（libs/server/Transaction/TransactionManager.cs:Reset(bool)）
  ///
  /// 运行中先解锁并释放上下文（wkv 纪元会话下上下文释放为结构性空操作，
  /// 锁位经守卫 drop 归还）。
  pub fn reset(&mut self, is_running: bool) {
    if is_running {
      self.key_entries.unlock_all_keys();
      // C# 按 storeTypes 依次 EndTransaction；wkv 批处理会话无独立事务
      // 上下文，锁面释放即收尾。
    }
    self.txn_version = 0;
    self.txn_start_head = 0;
    self.operation_cnt_txn = 0;
    self.state = TxnState::None;
    self.store_types = TransactionStoreTypes::None;
    self.stored_proc_mode = false;
    self.perform_writes = false;

    // 集群键解析态重置
    if self.cluster_enabled {
      self.txn_keys.clear();
      self.save_key_recv_buffer_ptr = None;
    }
  }

  /// 开启事务（上下文面）
  ///
  /// libs/server/Transaction/TransactionManager.cs:BeginTransaction
  ///
  /// C# 按 storeTypes 依次开启各 store 事务上下文；wkv 纪元会话无独立
  /// 事务上下文，此处为结构性空操作（锁的获取在 LockAllKeys 落地）。
  pub fn begin_transaction(&mut self) {}

  /// 锁就绪通知
  ///
  /// libs/server/Transaction/TransactionManager.cs:LocksAcquired
  ///
  /// C# 将事务版本注入各 store 上下文；托管面版本随管理器状态推进，
  /// 无需逐上下文登记。
  pub fn locks_acquired(&mut self, _txn_version: i64) {}

  /// 运行事务（libs/server/Transaction/TransactionManager.cs:Run）
  ///
  /// 保存 WATCH 锁集 → 取事务版本 → 开上下文 → 加锁 → 校验 WATCH →
  /// 记录 TxnStart → 置 Running。
  pub fn run(
    &mut self,
    internal_txn: bool,
    fail_fast_on_lock: bool,
    lock_timeout: Duration,
  ) -> bool {
    // WATCH 键并入锁集（字段拆借免克隆，登记内核同 SaveKeyEntryToLock）
    if !internal_txn {
      let Self {
        watch_container,
        key_entries,
        perform_writes,
        ..
      } = self;
      for key in watch_container.save_keys_to_lock() {
        Self::register_key_lock(key_entries, perform_writes, key, LockType::Shared);
      }
    }

    // 取事务版本（C# StateMachineDriver.AcquireTransactionVersion；
    // 单发生器递增代理，Verify 同值通过）
    self.version_sequence = self.version_sequence.wrapping_add(1);
    self.txn_version = self.version_sequence as i64;

    self.begin_transaction();

    let lock_success = if fail_fast_on_lock {
      self.key_entries.try_lock_all_keys(lock_timeout)
    } else {
      self.key_entries.lock_all_keys();
      true
    };

    if !lock_success || (!internal_txn && !self.watch_container.validate_watch_version()) {
      if !lock_success {
        log::error!("Transaction failed to acquire all the locks on keys to proceed.");
      }
      self.reset(true);
      if !internal_txn {
        self.watch_container.reset();
      }
      return false;
    }

    // 校验事务版本（C# VerifyTransactionVersion；单发生器代理恒通过）
    self.locks_acquired(self.txn_version);

    // TxnStart 标记
    if self.perform_writes
      && !self.stored_proc_mode
      && let Some(log) = &self.aof_log
    {
      let (physical_sublog_access_vector, _) = self.compute_sublog_access_vector();
      log.enqueue_txn(
        AofEntryType::TxnStart,
        self.txn_version,
        self.session_id,
        &[],
        physical_sublog_access_vector,
      );
    }

    self.state = TxnState::Running;
    true
  }

  /// 提交事务（libs/server/Transaction/TransactionManager.cs:Commit）
  ///
  /// 非存储过程路径记 TxnCommit 条目；非内部事务清空 WATCH；随后重置。
  pub fn commit(&mut self, internal_txn: bool) {
    if self.perform_writes
      && !self.stored_proc_mode
      && let Some(log) = &self.aof_log
    {
      let (physical_sublog_access_vector, _) = self.compute_sublog_access_vector();
      log.enqueue_txn(
        AofEntryType::TxnCommit,
        self.txn_version,
        self.session_id,
        &[],
        physical_sublog_access_vector,
      );
    }
    if !internal_txn {
      self.watch_container.reset();
    }
    self.reset(true);
  }

  /// 中止事务（libs/server/Transaction/TransactionManager.cs:Abort）
  pub fn abort(&mut self) {
    self.state = TxnState::Aborted;
  }

  /// 跳过（排队）模式判定
  ///
  /// libs/server/Transaction/TransactionManager.cs:IsSkippingOperations
  #[inline]
  pub fn is_skipping_operations(&self) -> bool {
    self.state == TxnState::Started || self.state == TxnState::Aborted
  }

  /// 监视键
  ///
  /// libs/server/Transaction/TransactionManager.cs:Watch
  ///
  /// C# 随后按 storeTypes 对各上下文 ResetModified（撤销本会话对该键的
  /// 挂起修改标记）；wkv 无挂起修改缓冲，监视登记即完整语义。
  pub fn watch(&mut self, key: &[u8]) {
    self.watch_container.add_watch(key);
  }

  /// 并入事务存储面
  ///
  /// libs/server/Transaction/TransactionManager.cs:AddTransactionStoreTypes
  pub fn add_transaction_store_types(&mut self, transaction_store_types: TransactionStoreTypes) {
    self.store_types |= transaction_store_types;
  }

  /// 按存储类别并入事务存储面
  ///
  /// libs/server/Transaction/TransactionManager.cs:AddTransactionStoreType
  pub fn add_transaction_store_type(&mut self, store_type: StoreType) {
    let transaction_store_types = match store_type {
      StoreType::Main => TransactionStoreTypes::Main,
      StoreType::Object => TransactionStoreTypes::Object,
      StoreType::All => TransactionStoreTypes::Unified,
      StoreType::None => TransactionStoreTypes::None,
    };
    self.store_types |= transaction_store_types;
  }

  /// 锁集展示串（libs/server/Transaction/TransactionManager.cs:GetLockset）
  pub fn get_lockset(&self) -> String {
    self.key_entries.get_lockset()
  }

  /// 取集群槽校验输入
  ///
  /// libs/server/Transaction/TransactionManager.cs:GetSlotVerificationInput
  ///
  /// 先把 WATCH 键并入槽校验键列表（SaveKeyArgSlice 主体就地展开：集群
  /// 门禁 + 键副本入列，字段拆借免克隆）；托管缓冲无指针失效问题，
  /// C# 的 saveKeyRecvBufferPtr 比对为恒等短路。
  pub fn get_slot_verification_input(
    &mut self,
    session_asking: u8,
  ) -> ClusterSlotVerificationInput {
    let Self {
      watch_container,
      cluster_enabled,
      txn_keys,
      key_entries,
      ..
    } = self;
    for key in watch_container.save_keys_to_key_list() {
      if *cluster_enabled {
        txn_keys.push(key.into());
      }
    }
    // 槽校验按本上下文全键迭代（C# 注释：不指定 key specs）
    ClusterSlotVerificationInput {
      read_only: key_entries.is_read_only(),
      session_asking,
    }
  }

  /// 把单命令窗口提升为原子事务（C# PromoteToTransaction）
  ///
  /// 已在 Running 事务内返回 Null 守卫；否则登记键锁并以内部事务运行，
  /// 返回的守卫 drop 时提交（C# using 块语义）。
  pub fn promote_to_transaction(
    &mut self,
    store_types: TransactionStoreTypes,
    key: &[u8],
    lock_type: LockType,
  ) -> TransactionGuard<'_> {
    if self.state == TxnState::Running {
      return TransactionGuard::null();
    }

    self.add_transaction_store_types(store_types);
    self.save_key_entry_to_lock(key, lock_type);
    let _ = self.run(true, false, Duration::ZERO);
    TransactionGuard {
      txn_manager: Some(self),
    }
  }

  /// 计算自定义过程的多日志回放访问元数据
  ///
  /// libs/server/Transaction/TransactionManager.cs:ComputeCustomProcShardedLogAccess
  ///
  /// 托管 AOF 恒单日志拓扑（MultiLog 未启用），与 C# 单日志路径一样在此
  /// 早退；分片回放位图随 waof 多日志面一并接线。
  pub fn compute_custom_proc_sharded_log_access(&self, _key: &[u8]) {}

  /// 计算事务的多子日志访问向量
  ///
  /// libs/server/Transaction/TransactionManager.cs:ComputeSublogAccessVector
  ///
  /// 返回 `(physicalSublogAccessVector, virtualSublogParticipantCount)`；
  /// 单日志拓扑无需计算（C# 同路径恒零）。
  pub fn compute_sublog_access_vector(&self) -> (u64, u32) {
    (0, 0)
  }

  /// AOF 记录自定义过程条目
  ///
  /// libs/server/Transaction/TransactionManager.cs:Log
  fn log_proc(&mut self, proc: &dyn TxnProcedure) {
    debug_assert!(self.stored_proc_mode);
    if self.perform_writes
      && let Some(log) = &self.aof_log
    {
      let (physical_sublog_access_vector, _) = self.compute_sublog_access_vector();
      log.enqueue_stored_proc(
        AofEntryType::StoredProcedure,
        self.txn_version,
        self.session_id,
        proc.id(),
        // 过程输入负载由 custom 域序列化接管；单日志拓扑恒空体直通
        &[],
        physical_sublog_access_vector,
      );
    }
  }

  /// 运行自定义事务过程三段式
  ///
  /// libs/server/Transaction/TransactionManager.cs:RunTransactionProc /
  /// RunTransactionProcInternal
  ///
  /// 准备失败 / 中止 / 锁失败均重置并返回 false；主段后记 AOF、提交，
  /// 收尾段仅在非回放路径执行（C# 同款 try/finally 语义）。
  pub fn run_transaction_proc(
    &mut self,
    proc: &mut dyn TxnProcedure,
    output: &mut Vec<u8>,
    is_replaying: bool,
  ) -> bool {
    let running = false;
    self.is_replaying = is_replaying;

    // 集群启用时重置槽校验缓存（ResetCacheSlotVerificationResult）
    self.reset_cache_slot_verification_result();

    self.stored_proc_mode = true;

    // 准备段
    if !proc.prepare(self) {
      self.reset(running);
      return false;
    }

    if self.state == TxnState::Aborted {
      self.write_cached_slot_verification_message(output);
      self.reset(running);
      return false;
    }

    // 运行事务（锁失败快速路径随过程开关）
    if !self.run(
      false,
      proc.fail_fast_on_key_lock_failure(),
      proc.key_lock_timeout(),
    ) {
      self.reset(running);
      return false;
    }

    // 主段：锁内执行
    proc.main(self, output);

    // AOF 记录过程条目：C# Log 无条件调用、靠回放期 appendOnlyFile 缺席
    // 短路；托管宿主回放期可能仍持日志句柄，故显式以 is_replaying 短路，
    // 网络效果同 C#（回放不重复落盘）
    if !is_replaying {
      self.log_proc(proc);
    }

    // 提交（隐含 Reset(true)，C# running 标记至此闭环）
    self.commit(false);

    // 收尾段：AOF 回放跳过
    if !is_replaying {
      proc.finalize(self, output);
    }

    true
  }
}

#[cfg(test)]
mod tests {
  use super::*;
  use crate::transaction::{
    txn_key_entry_comparison::TxnKeyEntryComparison, watch_version_map::WatchVersionMap,
  };

  fn manager() -> TransactionManager {
    TransactionManager::new(Arc::new(WatchVersionMap::new(64)), None, false)
  }

  #[test]
  fn state_machine_skip_window() {
    let mut txn = manager();
    assert_eq!(txn.state, TxnState::None);
    assert!(!txn.is_skipping_operations());
    txn.state = TxnState::Started;
    assert!(txn.is_skipping_operations());
    txn.state = TxnState::Aborted;
    assert!(txn.is_skipping_operations());
    txn.state = TxnState::Running;
    assert!(!txn.is_skipping_operations());
  }

  #[test]
  fn run_commit_cycle_locks_and_resets() {
    let mut txn = manager();
    txn.save_key_entry_to_lock(b"alpha", LockType::Exclusive);
    assert!(txn.run(false, false, Duration::ZERO));
    assert_eq!(txn.state, TxnState::Running);
    txn.perform_writes = true;
    txn.commit(false);
    assert_eq!(txn.state, TxnState::None);
    // 锁集随重置清空
    assert_eq!(txn.key_entries.count(), 0);
  }

  #[test]
  fn watch_invalidation_aborts_run() {
    let map = Arc::new(WatchVersionMap::new(64));
    let mut txn = TransactionManager::new(Arc::clone(&map), None, false);
    txn.watch(b"key");
    // 写方推进被监视键版本
    map.increment_version(TxnKeyEntryComparison::key_hash(b"key") as u64);
    txn.save_key_entry_to_lock(b"other", LockType::Exclusive);
    assert!(!txn.run(false, false, Duration::ZERO));
    // 失败路径已清 WATCH 并重置
    assert_eq!(txn.state, TxnState::None);
  }

  #[test]
  fn store_type_mapping() {
    let mut txn = manager();
    txn.add_transaction_store_type(StoreType::Main);
    txn.add_transaction_store_type(StoreType::Object);
    assert!(txn.store_types.contains(TransactionStoreTypes::Main));
    assert!(txn.store_types.contains(TransactionStoreTypes::Object));
    assert!(!txn.store_types.contains(TransactionStoreTypes::Unified));
    txn.add_transaction_store_type(StoreType::All);
    assert!(txn.store_types.contains(TransactionStoreTypes::Unified));
  }

  #[test]
  fn guard_commits_on_drop() {
    let mut txn = manager();
    {
      let guard =
        txn.promote_to_transaction(TransactionStoreTypes::Main, b"k", LockType::Exclusive);
      assert_eq!(guard.state(), TxnState::Running);
    }
    assert_eq!(txn.state, TxnState::None);
  }

  #[test]
  fn guard_is_null_when_already_running() {
    let mut txn = manager();
    txn.save_key_entry_to_lock(b"k", LockType::Shared);
    assert!(txn.run(false, false, Duration::ZERO));
    {
      // 已在 Running：Null 守卫不承载管理器（状态报告 None），drop 不提交
      let guard = txn.promote_to_transaction(TransactionStoreTypes::Main, b"j", LockType::Shared);
      assert_eq!(guard.state(), TxnState::None);
    }
    // Running 保持
    assert_eq!(txn.state, TxnState::Running);
  }

  /// 三段式最小过程：主段写键，收尾计数
  struct CountingProc {
    prepared: bool,
    finalized: bool,
  }

  impl TxnProcedure for CountingProc {
    fn id(&self) -> u8 {
      9
    }
    fn prepare(&mut self, txn: &mut TransactionManager) -> bool {
      txn.save_key_entry_to_lock(b"proc-key", LockType::Exclusive);
      self.prepared = true;
      true
    }
    fn main(&mut self, _txn: &mut TransactionManager, output: &mut Vec<u8>) {
      output.extend_from_slice(b"main");
    }
    fn finalize(&mut self, _txn: &mut TransactionManager, _output: &mut Vec<u8>) {
      self.finalized = true;
    }
  }

  #[test]
  fn run_transaction_proc_full_flow() {
    let mut txn = manager();
    let mut proc = CountingProc {
      prepared: false,
      finalized: false,
    };
    let mut output = Vec::new();
    assert!(txn.run_transaction_proc(&mut proc, &mut output, false));
    assert!(proc.prepared && proc.finalized);
    assert_eq!(output, b"main");
    assert_eq!(txn.state, TxnState::None);
  }

  #[test]
  fn run_transaction_proc_skips_finalize_on_replay() {
    let mut txn = manager();
    let mut proc = CountingProc {
      prepared: false,
      finalized: false,
    };
    let mut output = Vec::new();
    assert!(txn.run_transaction_proc(&mut proc, &mut output, true));
    assert!(proc.prepared);
    assert!(!proc.finalized);
  }
}
