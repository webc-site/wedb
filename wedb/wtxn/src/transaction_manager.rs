//! 事务管理器（对标 libs/server/Transaction/TransactionManager.cs:TransactionManager）
//!
//! C# 事务闭包 = 上下文锁面（Tsavorite 事务上下文）+ AOF 事务条目 +
//! WATCH 校验 + 键集排序加锁；Rust 侧锁面由本域 [`crate::txn_lock_table::TxnLockTable`] 承接，
//! WATCH 校验经 [`WatchVersionMap`]（与存储会话尾地址代理同向），AOF 事务
//! 条目直连 `GarnetLog::enqueue_txn`。上下文 Begin/End/LocksAcquired 等
//! Tsavorite 会话面在 wkv 纪元会话模型下为结构性空操作（文档就地标注）。
//!
//! C# 的 partial 拆分对应关系：
//! - TxnKeyManager.cs / TxnClusterSlotCheck.cs / TxnRespCommands.cs
//!   → 本结构在其他域文件的跨文件 `impl` 块；
//! - TxnState.cs → [`TxnState`]。

use std::{
  sync::{
    Arc,
    atomic::{AtomicU64, Ordering},
  },
  time::Duration,
};

use bitflags::bitflags;
use smallvec::SmallVec;
use wbase::store_type::{REPLAY_TASK_ACCESS_VECTOR_BYTES, StoreType};

use crate::{
  TxnState,
  txn_key_entry::{LockType, TxnKeyEntries},
  txn_keys_buffer::TxnKeysBuffer,
  txn_lock_table::TxnLockTable,
  txn_watched_keys_container::TxnWatchedKeysContainer,
  watch_version_map::WatchVersionMap,
};

/// 全局单调递增事务版本发生器（对标 C# StateMachineDriver.AcquireTransactionVersion）
static GLOBAL_TXN_VERSION_SEQ: AtomicU64 = AtomicU64::new(1);

/// 事务 AOF 条目类型端口（事务域不感知上层 AOF 判别值，物理编码由
/// 日志实现域经单一映射函数承接；对标 C# TransactionManager 直写的
/// AofEntryType.TxnStart / TxnCommit / StoredProcedure 三值）
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TxnEntryType {
  /// 事务开始（AOF 事务条目开标记）
  TxnStart,
  /// 事务提交（AOF 事务条目提交标记）
  TxnCommit,
  /// 存储过程条目（含过程输入负载）
  StoredProcedure,
}

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
  /// 追加事务标记条目（TxnStart / TxnCommit）；失败透传日志入队错误
  ///（对标 C# EnqueueTxn 异常沿调用栈上抛）
  fn enqueue_txn(
    &self,
    op_type: TxnEntryType,
    txn_version: i64,
    session_id: i32,
    access: &SublogAccess<'_>,
  ) -> waof::Result<()>;
  /// 追加存储过程条目（`payload` 为过程输入负载，主侧全量写入）；失败透传
  fn enqueue_stored_proc(
    &self,
    op_type: TxnEntryType,
    txn_version: i64,
    session_id: i32,
    proc_id: u8,
    payload: &[u8],
    access: &SublogAccess<'_>,
  ) -> waof::Result<()>;
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

impl Drop for TransactionManager {
  fn drop(&mut self) {
    self.reset();
  }
}

/// 事务管理器
pub struct TransactionManager {
  /// 事务状态（C# state）
  pub state: TxnState,
  /// 事务键加锁集合（C# keyEntries，持所属引擎实例的锁表句柄）
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
  /// AOF 事务日志（C# appendOnlyFile.Log；None = AofEnabled false）
  pub aof_log: Option<Arc<dyn TxnAofLog>>,
  /// 会话 ID（AOF 事务条目归属；C# stringBasicContext.Session.ID）
  pub session_id: i32,
  /// 存储过程模式（C# functionsState.StoredProcMode）
  pub stored_proc_mode: bool,
  /// 处于 AOF 回放（C# IsReplaying；回放期跳过 Finalize）
  pub is_replaying: bool,
  /// 事务涉及的键集合（EXEC 多键槽校验用，扁平连续缓冲自动去重；对标 C# txnKeysParseState）
  pub txn_keys: TxnKeysBuffer,
  /// 集群模式是否启用（对标 C# TransactionManager.cs:clusterEnabled；
  /// txn_keys / SaveKeyArgSlice 仅在 cluster_enabled 时登记，
  /// 单机形态零登记零堆分配）
  pub cluster_enabled: bool,
}

impl TransactionManager {
  /// 构造事务管理器
  ///
  /// `lock_table` 为所属引擎实例的锁表句柄（对标 C# 事务管理器随会话构造、
  /// 锁面取该 store 的 `LockTable`），键集加锁集合持同一句柄。
  /// `aof_log` 为 AOF 日志句柄（C# 构造注入的 `GarnetAppendOnlyFile`；
  /// None = 该库未启用 AOF）。
  pub fn new(
    lock_table: TxnLockTable,
    watch_version_map: Arc<WatchVersionMap>,
    aof_log: Option<Arc<dyn TxnAofLog>>,
  ) -> Self {
    Self {
      state: TxnState::None,
      key_entries: TxnKeyEntries::new(16, lock_table),
      watch_container: TxnWatchedKeysContainer::new(watch_version_map),
      store_types: TransactionStoreTypes::None,
      txn_start_head: 0,
      operation_cnt_txn: 0,
      perform_writes: false,
      txn_version: 0,
      aof_log,
      session_id: 0,
      stored_proc_mode: false,
      is_replaying: false,
      txn_keys: TxnKeysBuffer::new(),
      cluster_enabled: false,
    }
  }

  /// 是否启用 AOF（libs/server/Transaction/TransactionManager.cs:AofEnabled）
  pub fn aof_enabled(&self) -> bool {
    self.aof_log.is_some()
  }

  /// 重置事务状态
  ///
  /// libs/server/Transaction/TransactionManager.cs:Reset
  /// 无条件解锁全部键并清理锁集，杜绝中止/丢弃事务的锁泄漏与跨事务键污染。
  pub fn reset(&mut self) {
    self.key_entries.unlock_all_keys();
    self.txn_version = 0;
    self.txn_start_head = 0;
    self.operation_cnt_txn = 0;
    self.state = TxnState::None;
    self.store_types = TransactionStoreTypes::None;
    self.stored_proc_mode = false;
    self.is_replaying = false;
    self.perform_writes = false;
    self.txn_keys.clear();
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

    // 取全局单调递增事务版本（对标 C# StateMachineDriver.AcquireTransactionVersion）
    self.txn_version = GLOBAL_TXN_VERSION_SEQ.fetch_add(1, Ordering::Relaxed) as i64;

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
      self.reset();
      if !internal_txn {
        self.watch_container.reset();
      }
      return false;
    }

    // TxnStart 标记（C# EnqueueTxn 异常上抛的 rust 投影：并入锁失败同款
    // 收尾后返回 false；标记未落盘，AOF 无残缺事务组）
    if self.perform_writes
      && !self.stored_proc_mode
      && let Some(log) = self.aof_log.as_deref()
      && self
        .enqueue_txn_marker(log, TxnEntryType::TxnStart)
        .is_err()
    {
      self.reset();
      if !internal_txn {
        self.watch_container.reset();
      }
      return false;
    }

    self.state = TxnState::Running;
    true
  }

  /// 提交事务（libs/server/Transaction/TransactionManager.cs:Commit）
  ///
  /// 非存储过程路径记 TxnCommit 条目；非内部事务清空 WATCH；随后重置。
  /// TxnCommit 入队失败经 Err 上抛（C# 异常传播），但状态收尾仍先做——
  /// rust 无 C# 会话 GC 兜底，显式释放锁防泄漏；客户端经 EXEC 响应感知未确认。
  pub fn commit(&mut self, internal_txn: bool) -> waof::Result<()> {
    let enqueued = if self.perform_writes
      && !self.stored_proc_mode
      && let Some(log) = self.aof_log.as_deref()
    {
      self.enqueue_txn_marker(log, TxnEntryType::TxnCommit)
    } else {
      Ok(())
    };
    if !internal_txn {
      self.watch_container.reset();
    }
    self.reset();
    enqueued
  }

  /// 中止事务（libs/server/Transaction/TransactionManager.cs:Abort）
  pub fn abort(&mut self) {
    self.state = TxnState::Aborted;
  }

  /// 事务是否只读（C# keyEntries.IsReadOnly）
  #[inline]
  pub fn is_read_only(&self) -> bool {
    self.key_entries.is_read_only()
  }

  /// 登记事务涉及的键（自动去重；对标 C# txnKeysParseState 与
  /// libs/server/Transaction/TxnClusterSlotCheck.cs:SaveKeyArgSlice）
  ///
  /// `!cluster_enabled` 短路，单机形态零登记零堆分配。
  #[inline]
  pub fn add_txn_key(&mut self, key: &[u8]) {
    if !self.cluster_enabled {
      return;
    }
    self.txn_keys.push(key);
  }

  /// 监视键
  ///
  /// libs/server/Transaction/TransactionManager.cs:Watch
  ///
  /// 保存键版本，并为键加独占锁（C# SetLockType(true) 并向 ScratchBuffer
  /// 挂起修改标记）；wkv 无挂起修改缓冲，监视登记即完整语义。WATCH 键入
  /// EXEC 多键面（C# SaveKeysToKeyList，仅集群启用时同步登记），不触迭代槽校验。
  pub fn watch(&mut self, key: &[u8]) {
    self.watch_container.add_watch(key);
    self.add_txn_key(key);
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

  /// AOF 记录事务标记条目（TxnStart / TxnCommit）；失败上抛
  #[inline]
  fn enqueue_txn_marker(&self, log: &dyn TxnAofLog, op_type: TxnEntryType) -> waof::Result<()> {
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
    )
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
    let Some(log) = self.aof_log.as_deref() else {
      return (0, SublogVirtualVectors::new(), 0);
    };
    if log.size() <= 1 && log.replay_task_count() <= 1 {
      return (0, SublogVirtualVectors::new(), 0);
    }

    let sublog_size = log.size();
    let mut virtual_vectors: SublogVirtualVectors =
      smallvec::smallvec![[0u8; REPLAY_TASK_ACCESS_VECTOR_BYTES]; sublog_size];
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
}
