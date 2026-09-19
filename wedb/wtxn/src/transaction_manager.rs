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
  txn_slot_verify::SlotVerifyHandle,
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

/// 事务过程存储读写视图（main/finalize 段共用；C# IGarnetApi : IGarnetReadApi
/// 的过程域投影，原语面取过程族实际触达的最小集）
///
/// C# TransactionManager.cs:188-190 装配的 `TransactionalGarnetApi`（main
/// 事务视图）与 `BasicGarnetApi`（finalize 直连视图）在 rust 合一：无
/// lockable version store，事务隔离由 windex 哈希桶内嵌闩（[`TxnLockTable`]）完成，
/// 两面落到同一物理入口。返回类型用 rust 形状，不搬运 C# GarnetStatus。
///
/// 映射口径：本版 C# 的 IGarnetReadApi 与 IGarnetApi 同文件声明（过程体经
/// GarnetApi 多态跳板转 storageSession 同名原语），跳板与接口层按
/// js/check/ignore/server.yml 的甄别不建映射；本 trait 各方法只是派发面，
/// 故只述语义，真实落点锚点单点挂在 wnode StorageSession 的对应原语与
/// RESP 层 rmw 骨架上（一处定义，见 wnode storage/session/txn_proc_view.rs）。
pub trait TxnProcApi {
  /// 读字符串键（双域读：对象键 / 缺失折叠为 None）
  fn get(&mut self, key: &[u8]) -> Option<Vec<u8>>;
  /// 写字符串键（SET 语义清既有 TTL）；false = 存储失败
  fn set(&mut self, key: &[u8], val: &[u8]) -> bool;
  /// 写值并设相对过期（`expiry_ticks` 为 .NET TimeSpan ticks 口径）；
  /// false = 存储失败
  fn setex(&mut self, key: &[u8], val: &[u8], expiry_ticks: i64) -> bool;
  /// 删除键（返回键是否存在）
  fn delete(&mut self, key: &[u8]) -> bool;
  /// 整型增减（RMW 写回保留既有 TTL）；None = 键缺失 / 非整数 / 存储失败
  fn increment(&mut self, key: &[u8], delta: i64) -> Option<i64>;
  /// 有序集加成员（返回操作是否成功，键不存在时自动新建）
  fn sorted_set_add(&mut self, key: &[u8], score: f64, member: &[u8]) -> bool;
  /// 有序集删成员（返回是否实际移除）
  fn sorted_set_remove(&mut self, key: &[u8], member: &[u8]) -> bool;
}

/// prepare 段只读视图（C# `Prepare<TGarnetReadApi>` 只读界的本域投影）
pub trait TxnProcReadApi {
  /// 读键并登记 WATCH（C# GarnetWatchApi.GET：WATCH(key, storeType) 后
  /// 透传 GET，使 prepare 读集进入 WATCH 冲突检测）
  fn get(&mut self, txn: &mut TransactionManager, key: &[u8]) -> Option<Vec<u8>>;
}

/// prepare 段读即 WATCH 包装（libs/server/API/GarnetWatchApi.cs:
/// GarnetWatchApi<TGarnetApi> 的本域投影：只读界包装读写视图，每次读键
/// 先登记进 WATCH 集合再透传；包装点与 C# TransactionManager 装配
/// `GarnetWatchApi<BasicGarnetApi>` 同位——由 [`TransactionManager::
/// run_transaction_proc`] 在 prepare 段单点构造）
pub struct TxnWatchApi<'a> {
  /// 被包装的读写视图
  api: &'a mut (dyn TxnProcApi + 'a),
}

impl TxnProcReadApi for TxnWatchApi<'_> {
  fn get(&mut self, txn: &mut TransactionManager, key: &[u8]) -> Option<Vec<u8>> {
    txn.watch(key);
    self.api.get(key)
  }
}

/// 自定义事务过程的最小面（C# CustomTransactionProcedure 的本域投影；
/// Prepare/Main/Finalize 三段式与失败快速路径开关对齐 C# 签名语义）
///
/// 三段签名对标 libs/server/Custom/CustomTransactionProcedure.cs:73-90：
/// prepare 收只读视图（[`TxnProcReadApi`]，由本域包装读即 WATCH）、
/// main/finalize 收读写视图（[`TxnProcApi`]）。
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
  fn prepare(
    &mut self,
    txn_manager: &mut TransactionManager,
    api: &mut dyn TxnProcReadApi,
    verifier: Option<&SlotVerifyHandle<'_>>,
  ) -> bool;
  /// 主段：锁内执行（C# Main，入参为读写 API）
  fn main(
    &mut self,
    txn_manager: &mut TransactionManager,
    api: &mut dyn TxnProcApi,
    output: &mut Vec<u8>,
  );
  /// 收尾段（C# Finalize；AOF 回放期间跳过）
  fn finalize(
    &mut self,
    txn_manager: &mut TransactionManager,
    api: &mut dyn TxnProcApi,
    output: &mut Vec<u8>,
  );
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

  /// 提交并销毁守卫（对标 C# TransactionGuard.Dispose）
  ///
  /// libs/server/Transaction/TransactionManager.cs:Dispose
  ///
  /// Drop 语义无法传播 Err（C# Dispose 异常可沿显式 using 块上抛），
  /// 失败显式告警可见；internal 提升路径，锁已随 commit 内 reset 释放。
  pub fn dispose(&mut self) {
    if let Some(txn_manager) = self.txn_manager.take()
      && let Err(err) = txn_manager.commit(true)
    {
      log::error!("事务提升守卫提交失败: {err:?}");
    }
  }
}

impl Drop for TransactionGuard<'_> {
  fn drop(&mut self) {
    self.dispose();
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

  /// 设置会话标识（对齐 C# Session.ID 绑定）
  #[inline]
  pub fn set_session_id(&mut self, session_id: i32) {
    self.session_id = session_id;
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

  /// AOF 记录自定义过程条目；失败上抛（C# Log 异常传播）
  ///
  /// `proc_input` 为过程输入负载（C# procInput），主侧全量写入
  ///（经日志实现域同名存储过程入队口承载 `ref procInput`，映射归位
  /// wnode garnet_log 单一定义）
  ///
  /// libs/server/Transaction/TransactionManager.cs:Log
  fn log_proc(
    &mut self,
    proc: &(impl TxnProcedure + ?Sized),
    proc_input: &[u8],
  ) -> waof::Result<()> {
    debug_assert!(self.stored_proc_mode);
    if self.perform_writes
      && let Some(log) = self.aof_log.as_deref()
    {
      let (physical_vector, virtual_vectors, participant_count) =
        self.compute_sublog_access_vector();
      return log.enqueue_stored_proc(
        TxnEntryType::StoredProcedure,
        self.txn_version,
        self.session_id,
        proc.id(),
        proc_input,
        &SublogAccess {
          physical_vector,
          virtual_vectors: &virtual_vectors,
          participant_count: participant_count as usize,
        },
      );
    }
    Ok(())
  }

  /// 运行自定义事务过程三段式
  ///
  /// `proc_input` 为过程输入负载（C# procInput），主段后随 AOF 条目全量落盘
  ///
  /// `api` 为过程体存储读写视图（C# TransactionManager.cs:188-190 装配的
  /// `TransactionalGarnetApi` / `BasicGarnetApi` 两面在 rust 合一）；prepare
  /// 段由本函数包装为读即 WATCH 的只读视图（对标 C# `GarnetWatchApi<
  /// BasicGarnetApi>` 装配点）
  ///
  /// `verifier` 为迭代式槽位校验直穿句柄（None = 单机 / 回放宿主无集群面，
  /// 全部校验短路——同 C# !clusterEnabled || IsReplaying 判定。RUNTXP 执行前由会话
  /// 侧栈上借传入，prepare 参数直穿逐键内联校验，无登记无堆分配）
  ///
  /// libs/server/Transaction/TransactionManager.cs:RunTransactionProc
  /// libs/server/Transaction/TransactionManager.cs:RunTransactionProcInternal
  ///
  /// 准备失败 / 中止 / 锁失败均重置并返回 false；主段后记 AOF、提交。
  /// 收尾段对标 C# finally：正常与三条早退路径均跑，仅 AOF 回放期整体跳过。
  pub fn run_transaction_proc(
    &mut self,
    proc: &mut (impl TxnProcedure + ?Sized),
    proc_input: &[u8],
    output: &mut Vec<u8>,
    is_replaying: bool,
    verifier: Option<&SlotVerifyHandle<'_>>,
    api: &mut dyn TxnProcApi,
  ) -> bool {
    self.is_replaying = is_replaying;

    // 集群启用时重置迭代槽位校验缓存（C# ResetCacheSlotVerificationResult；
    // 切面缺席即 clusterEnabled false 短路）
    if let Some(v) = verifier {
      v.reset_cached_slot_verification_result();
    }

    self.stored_proc_mode = true;

    // prepare 段只读视图（对标 C# GarnetWatchApi<BasicGarnetApi> 装配：
    // 读即 WATCH 后透传读写视图，单点包装零堆分配）
    let mut watch_api = TxnWatchApi { api };

    // 准备段（直穿 verifier 供 add_key 逐键内联校验，对标 C# AddKey VerifyKeyOwnership）
    let ran = if !proc.prepare(self, &mut watch_api, verifier) {
      false
    } else if self.state == TxnState::Aborted {
      // 写出缓存槽位验证错误（C# WriteCachedSlotVerificationMessage）：
      // 已落线的错误留在 output 里，收尾段若再写则串在其后
      if let Some(v) = verifier {
        v.write_cached_slot_verification_message(output);
      }
      false
    } else if !self.run(
      false,
      proc.fail_fast_on_key_lock_failure(),
      proc.key_lock_timeout(),
    ) {
      // 运行事务（锁失败快速路径随过程开关）
      false
    } else {
      // 主段：锁内执行
      proc.main(self, api, output);

      // AOF 记录过程条目 + 提交（C# Log 后随 Commit，均无 try/catch）：
      // 入队或提交失败对齐 C# 异常传播的 bool 投影——log_proc 失败短路
      // 跳过 commit（C# 异常跳过后续语句），ran=false；收尾段照跑
      //（finally 语义）。回放期整体跳过：C# 靠回放宿主 appendOnlyFile
      // 缺席短路，托管宿主可能仍持日志句柄，显式以 is_replaying 短路，
      // 网络效果同 C#（回放不重复落盘）
      if is_replaying {
        true
      } else {
        self.log_proc(proc, proc_input).is_ok() && self.commit(false).is_ok()
      }
    };

    // 早退出口：Reset 在前、收尾段在后（C# 同款顺序——Reset(running) 先于
    // finally 里的 Finalize，故收尾段跑在无锁、TxnState::None 态上）
    if !ran {
      self.reset();
    }

    // 收尾段：C# finally 语义，早退路径同样必跑；AOF 回放整体跳过
    if !is_replaying {
      proc.finalize(self, api, output);
    }

    ran
  }
}
