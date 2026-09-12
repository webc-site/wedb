//! 事务管理器（对标 libs/server/Transaction/TransactionManager.cs:TransactionManager）
//!
//! C# 事务闭包 = 上下文锁面（Tsavorite 事务上下文）+ AOF 事务条目 +
//! WATCH 校验 + 键集排序加锁；Rust 侧锁面由本域 [`crate::transaction::txn_lock_table::TxnLockTable`] 承接，
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
use smallvec::SmallVec;
use waof::AofEntryType;

use crate::{
  StoreType, TxnState,
  txn_key_entry::{LockType, TxnKeyEntries},
  txn_watched_keys_container::TxnWatchedKeysContainer,
  watch_version_map::WatchVersionMap,
};

/// 虚拟回放任务访问位图字节数（对标 Garnet AofHeader.ReplayTaskAccessVector）
pub const REPLAY_TASK_ACCESS_VECTOR_BYTES: usize = 32;

/// 虚拟子日志回放任务位图列表（内联 4 物理子日志，对齐绝大多数物理日志分片配置，消除堆分配）
pub type SublogVirtualVectors = SmallVec<[[u8; REPLAY_TASK_ACCESS_VECTOR_BYTES]; 4]>;

/// 协调条目（事务 / 存储过程）的子日志参与面
#[derive(Clone, Copy, Default, Debug)]
pub struct SublogAccess<'a> {
  /// 物理子日志访问位图（位 = sublogIdx）。
  pub physical_vector: u64,
  /// 各物理子日志的回放任务位图（单日志拓扑为空）。
  pub virtual_vectors: &'a [[u8; REPLAY_TASK_ACCESS_VECTOR_BYTES]],
  /// 参与协调操作的回放任务总数。
  pub participant_count: usize,
}

/// 事务 AOF 日志接口（解耦底层事务管理与物理日志实现）
pub trait TxnAofLog: Send + Sync {
  /// 物理日志分片数
  fn size(&self) -> usize;
  /// 回放任务数
  fn replay_task_count(&self) -> usize;
  /// 物理子日志下标
  fn get_physical_sublog_idx(&self, key_hash: i64) -> usize;
  /// 回放任务下标
  fn get_replay_task_idx(&self, key_hash: i64) -> usize;
  /// 追加事务标记条目（TxnStart / TxnCommit）
  fn enqueue_txn(
    &self,
    op_type: AofEntryType,
    txn_version: i64,
    session_id: i32,
    access: &SublogAccess<'_>,
  );
  /// 追加存储过程条目
  fn enqueue_stored_proc(
    &self,
    op_type: AofEntryType,
    txn_version: i64,
    session_id: i32,
    proc_id: u8,
    payload: &[u8],
    access: &SublogAccess<'_>,
  );
}

impl<T: TxnAofLog + ?Sized> TxnAofLog for Arc<T> {
  #[inline]
  fn size(&self) -> usize {
    (**self).size()
  }

  #[inline]
  fn replay_task_count(&self) -> usize {
    (**self).replay_task_count()
  }

  #[inline]
  fn get_physical_sublog_idx(&self, key_hash: i64) -> usize {
    (**self).get_physical_sublog_idx(key_hash)
  }

  #[inline]
  fn get_replay_task_idx(&self, key_hash: i64) -> usize {
    (**self).get_replay_task_idx(key_hash)
  }

  #[inline]
  fn enqueue_txn(
    &self,
    op_type: AofEntryType,
    txn_version: i64,
    session_id: i32,
    access: &SublogAccess<'_>,
  ) {
    (**self).enqueue_txn(op_type, txn_version, session_id, access);
  }

  #[inline]
  fn enqueue_stored_proc(
    &self,
    op_type: AofEntryType,
    txn_version: i64,
    session_id: i32,
    proc_id: u8,
    payload: &[u8],
    access: &SublogAccess<'_>,
  ) {
    (**self).enqueue_stored_proc(op_type, txn_version, session_id, proc_id, payload, access);
  }
}

impl TxnAofLog for () {
  #[inline]
  fn size(&self) -> usize {
    0
  }

  #[inline]
  fn replay_task_count(&self) -> usize {
    0
  }

  #[inline]
  fn get_physical_sublog_idx(&self, _key_hash: i64) -> usize {
    0
  }

  #[inline]
  fn get_replay_task_idx(&self, _key_hash: i64) -> usize {
    0
  }

  #[inline]
  fn enqueue_txn(
    &self,
    _op_type: AofEntryType,
    _txn_version: i64,
    _session_id: i32,
    _access: &SublogAccess<'_>,
  ) {
  }

  #[inline]
  fn enqueue_stored_proc(
    &self,
    _op_type: AofEntryType,
    _txn_version: i64,
    _session_id: i32,
    _proc_id: u8,
    _payload: &[u8],
    _access: &SublogAccess<'_>,
  ) {
  }
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

/// 自定义事务过程的最小面（C# CustomTransactionProcedure 的本域投影；
/// Prepare/Main/Finalize 三段式与失败快速路径开关对齐 C# 签名语义）
pub trait TxnProcedure<L: TxnAofLog = ()> {
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
  fn prepare(&mut self, txn_manager: &mut TransactionManager<L>) -> bool;
  /// 主段：锁内执行（C# Main）
  fn main(&mut self, txn_manager: &mut TransactionManager<L>, output: &mut Vec<u8>);
  /// 收尾段（C# Finalize；AOF 回放期间跳过）
  fn finalize(&mut self, txn_manager: &mut TransactionManager<L>, output: &mut Vec<u8>);
}

/// 事务提升守卫：drop 即提交（C# TransactionGuard.Dispose → Commit(true)）
///
/// 由 [`TransactionManager::promote_to_transaction`] 产出，用于把两条及以上
/// 子命令固化为原子的 using 块。
pub struct TransactionGuard<'a, L: TxnAofLog = ()> {
  /// 守卫的事务管理器；None = Null 守卫（已在事务内，无须提升）
  txn_manager: Option<&'a mut TransactionManager<L>>,
}

impl<L: TxnAofLog> TransactionGuard<'_, L> {
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

impl<L: TxnAofLog> TransactionGuard<'_, L> {
  /// 提交并销毁守卫（对标 C# TransactionGuard.Dispose）
  ///
  /// libs/server/Transaction/TransactionManager.cs:Dispose
  pub fn dispose(&mut self) {
    if let Some(txn_manager) = self.txn_manager.take() {
      txn_manager.commit(true);
    }
  }
}

impl<L: TxnAofLog> Drop for TransactionGuard<'_, L> {
  fn drop(&mut self) {
    self.dispose();
  }
}

impl<L: TxnAofLog> Drop for TransactionManager<L> {
  fn drop(&mut self) {
    if self.state == TxnState::Running {
      self.reset(true);
    }
  }
}

/// 事务管理器
pub struct TransactionManager<L: TxnAofLog = ()> {
  /// 事务状态（C# state）
  pub state: TxnState,
  /// 事务键加锁集合（C# keyEntries）
  pub key_entries: TxnKeyEntries,
  /// 被监视键容器（C# watchContainer）
  pub watch_container: TxnWatchedKeysContainer,
  /// 事务触达的存储面（C# storeTypes）
  pub store_types: TransactionStoreTypes,
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
  /// AOF 事务日志（C# appendOnlyFile.Log；None = AofEnabled false）
  pub aof_log: Option<L>,
  /// 会话 ID（AOF 事务条目归属；C# stringBasicContext.Session.ID）
  pub session_id: i32,
  /// 存储过程模式（C# functionsState.StoredProcMode）
  pub stored_proc_mode: bool,
  /// 处于 AOF 回放（C# IsReplaying；回放期跳过 Finalize）
  pub is_replaying: bool,
}

impl<L: TxnAofLog> TransactionManager<L> {
  /// 构造泛型事务管理器（静态单态化消除虚表间接寻址）
  pub fn new(watch_version_map: Arc<WatchVersionMap>, aof_log: Option<L>) -> Self {
    Self::with_aof_log(watch_version_map, aof_log)
  }

  /// 构造泛型事务管理器（静态单态化消除虚表间接寻址）
  pub fn with_aof_log(watch_version_map: Arc<WatchVersionMap>, aof_log: Option<L>) -> Self {
    Self {
      state: TxnState::None,
      key_entries: TxnKeyEntries::new(16),
      watch_container: TxnWatchedKeysContainer::new(watch_version_map),
      store_types: TransactionStoreTypes::None,
      txn_start_head: 0,
      operation_cnt_txn: 0,
      perform_writes: false,
      txn_version: 0,
      version_sequence: 0,
      aof_log,
      session_id: 0,
      stored_proc_mode: false,
      is_replaying: false,
    }
  }

  /// 设置会话标识（对齐 C# Session.ID 绑定）
  #[inline]
  pub fn set_session_id(&mut self, session_id: i32) {
    self.session_id = session_id;
  }

  /// 链式绑定会话标识
  #[inline]
  pub fn with_session_id(mut self, session_id: i32) -> Self {
    self.session_id = session_id;
    self
  }

  /// 是否启用 AOF（libs/server/Transaction/TransactionManager.cs:AofEnabled）
  pub fn aof_enabled(&self) -> bool {
    self.aof_log.is_some()
  }

  /// TransactionManager.cs:Reset()（internal 无参重载）
  ///
  /// 以当前事务态决定复位后的运行标志（`Reset(state == TxnState.Running)`）
  pub fn reset_current(&mut self) {
    let is_running = self.state == TxnState::Running;
    self.reset(is_running);
  }

  /// 重置事务状态
  ///
  /// libs/server/Transaction/TransactionManager.cs:Reset
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
  }

  /// 运行事务（libs/server/Transaction/TransactionManager.cs:Run）
  ///
  /// 保存 WATCH 锁集 → 取事务版本 → 加锁 → 校验 WATCH →
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

    // TxnStart 标记
    if self.perform_writes
      && !self.stored_proc_mode
      && let Some(log) = &self.aof_log
    {
      self.enqueue_txn_marker(log, AofEntryType::TxnStart);
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
      self.enqueue_txn_marker(log, AofEntryType::TxnCommit);
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

  /// 事务触达存储面登记（libs/server/Transaction/TransactionManager.cs:AddTransactionStoreTypes）
  #[inline]
  pub fn add_transaction_store_types(&mut self, types: TransactionStoreTypes) {
    self.store_types |= types;
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

  /// 把单命令窗口提升为原子事务
  ///
  /// libs/server/Transaction/TransactionManager.cs:PromoteToTransaction
  ///
  /// 已在 Running 事务内返回 Null 守卫；否则登记键锁并以内部事务运行，
  /// 返回的守卫 drop 时提交（C# using 块语义）。
  pub fn promote_to_transaction(
    &mut self,
    store_types: TransactionStoreTypes,
    key: &[u8],
    lock_type: LockType,
  ) -> TransactionGuard<'_, L> {
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

  /// AOF 记录事务标记条目（TxnStart / TxnCommit）
  #[inline]
  fn enqueue_txn_marker(&self, log: &L, op_type: AofEntryType) {
    let (physical_vector, virtual_vectors, participant_count) = self.compute_sublog_access_vector();
    log.enqueue_txn(
      op_type,
      self.txn_version,
      self.session_id,
      &SublogAccess {
        physical_vector,
        virtual_vectors: &virtual_vectors,
        participant_count: participant_count as usize,
      },
    );
  }

  /// 计算事务的多子日志访问向量
  ///
  /// libs/server/Transaction/TransactionManager.cs:ComputeSublogAccessVector
  ///
  /// 返回 `(physical_sublog_access_vector, virtual_sublog_access_vector, participant_count)`。
  /// 对标 C# 1939 号提交（Fix ComputeSublogAccessVector to use keyEntries）：
  /// 在 standalone 与 cluster 模式下统一经事务已加锁的 `key_entries` 计算物理子日志
  /// 访问位图与回放任务位图，避免 standalone 分片 AOF 恢复时丢失事务标记。
  /// 单日志单任务拓扑无需计算（返回恒零与空位图）。
  pub fn compute_sublog_access_vector(&self) -> (u64, SublogVirtualVectors, u32) {
    let Some(log) = &self.aof_log else {
      return (0, SublogVirtualVectors::new(), 0);
    };
    if log.size() <= 1 && log.replay_task_count() <= 1 {
      return (0, SublogVirtualVectors::new(), 0);
    }

    let sublog_size = log.size();
    let mut virtual_vectors = SublogVirtualVectors::with_capacity(sublog_size);
    virtual_vectors.resize(sublog_size, [0u8; REPLAY_TASK_ACCESS_VECTOR_BYTES]);
    let mut physical_vector = 0u64;
    let mut participant_count = 0u32;

    // 对标 C# 1939：从事务加锁键 key_entries 路由，standalone 与 cluster 均已填充；
    // 键哈希等同于 GarnetLog.HASH，直接使用无需重算哈希。
    for key_hash in self.key_entries.key_hashes() {
      let physical_idx = log.get_physical_sublog_idx(key_hash);
      let replay_idx = log.get_replay_task_idx(key_hash);

      if physical_idx < 64 {
        physical_vector |= 1u64 << physical_idx;
      }

      let byte_idx = replay_idx / 8;
      let bit_mask = 1u8 << (replay_idx % 8);
      if let Some(sublog_vector) = virtual_vectors.get_mut(physical_idx)
        && let Some(slot) = sublog_vector.get_mut(byte_idx)
        && (*slot & bit_mask) == 0
      {
        *slot |= bit_mask;
        participant_count += 1;
      }
    }

    (physical_vector, virtual_vectors, participant_count)
  }

  /// AOF 记录自定义过程条目
  ///
  /// libs/server/Transaction/TransactionManager.cs:Log
  fn log_proc(&mut self, proc: &(impl TxnProcedure<L> + ?Sized)) {
    debug_assert!(self.stored_proc_mode);
    if self.perform_writes
      && let Some(log) = &self.aof_log
    {
      let (physical_vector, virtual_vectors, participant_count) =
        self.compute_sublog_access_vector();
      log.enqueue_stored_proc(
        AofEntryType::StoredProcedure,
        self.txn_version,
        self.session_id,
        proc.id(),
        // 过程输入负载由 custom 域序列化接管；单日志拓扑恒空体直通
        &[],
        &SublogAccess {
          physical_vector,
          virtual_vectors: &virtual_vectors,
          participant_count: participant_count as usize,
        },
      );
    }
  }

  /// 运行自定义事务过程三段式
  ///
  /// libs/server/Transaction/TransactionManager.cs:RunTransactionProc
  /// libs/server/Transaction/TransactionManager.cs:RunTransactionProcInternal
  ///
  /// 准备失败 / 中止 / 锁失败均重置并返回 false；主段后记 AOF、提交，
  /// 收尾段仅在非回放路径执行（C# 同款 try/finally 语义）。
  pub fn run_transaction_proc(
    &mut self,
    proc: &mut (impl TxnProcedure<L> + ?Sized),
    output: &mut Vec<u8>,
    is_replaying: bool,
  ) -> bool {
    let running = false;
    self.is_replaying = is_replaying;

    self.stored_proc_mode = true;

    // 准备段
    if !proc.prepare(self) {
      self.reset(running);
      return false;
    }

    if self.state == TxnState::Aborted {
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
