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

use smallvec::SmallVec;
use wbase::store_type::REPLAY_TASK_ACCESS_VECTOR_BYTES;

use crate::{
  TxnKeyEntryComparison, TxnState,
  txn_key_entry::{LockType, TxnKeyEntries, TxnKeyEntry},
  txn_keys_buffer::TxnKeysBuffer,
  txn_lock_table::{TxnBarrierTicket, TxnLockTable},
  txn_watched_keys_container::TxnWatchedKeysContainer,
  watch_version_map::WatchVersionMap,
};

/// 全局单调递增事务版本发生器（对标 C# StateMachineDriver.AcquireTransactionVersion）
static GLOBAL_TXN_VERSION_SEQ: AtomicU64 = AtomicU64::new(1);

/// 事务 AOF 条目类型端口（事务域不感知上层 AOF 判别值，物理编码由
/// 日志实现域经单一映射函数承接；对标 C# TransactionManager 直写的
/// AofEntryType.TxnStart / TxnCommit 两值，另接 C# 恢复/副本侧既有
/// AofEntryType.TxnAbort 判别值——C# 该值零生产写入方（NetworkEXEC 在单
/// 网络线程内联重放、批中途不可打断，组恒闭合至 Commit），rust 泵竞速
/// 废弃臂是唯一写入方，见 [`TransactionManager::finish_abandoned`]）
#[derive(Clone, Copy, PartialEq, Eq)]
pub enum TxnEntryType {
  /// 事务开始（AOF 事务条目开标记）
  TxnStart,
  /// 事务提交（AOF 事务条目提交标记）
  TxnCommit,
  /// 事务中止（AOF 事务条目废弃标记：恢复/副本侧据此弃组，解组缓冲滞留）
  TxnAbort,
}

/// 虚拟子日志回放任务位图列表（内联 4 物理子日志，对齐绝大多数物理日志分片配置，消除堆分配）
pub type SublogVirtualVectors = SmallVec<[[u8; REPLAY_TASK_ACCESS_VECTOR_BYTES]; 4]>;

/// 协调条目（事务 / 存储过程）的子日志参与面
#[derive(Clone, Copy)]
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
}

impl Drop for TransactionManager {
  fn drop(&mut self) {
    self.reset();
  }
}

/// [`TransactionManager::run_exec`] 的三态结果（外部 MULTI/EXEC 的 compio 安全事务起点）
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExecRun {
  /// 取锁成功且锁后收尾完成（WATCH 校验通过、TxnStart 已记），事务已置 Running。
  Started,
  /// 取锁争用：屏障注册或单次取闩尝试失败，键集/缓存计划/钉定索引原样保留、未复位。
  /// 两种争用面共用本档：扩容 PREPARE_GROW 屏障占用（首轮注册被拦，对标 C#
  /// AcquireTransactionVersion 的拦截自旋在 compio 态的让步投影）、桶闩争用。
  /// 宿主会话登记既有慢臂（`pending_slow` 单次让步 + `pending_rearm` 重驱本 EXEC 命令），
  /// 下一轮复入 [`TransactionManager::run_exec`] 再单次尝试，直至成功——保留 C# 无界等待契约。
  Contended,
  /// 锁后收尾失败（WATCH 被并发推进 / TxnStart 落盘错误），事务已复位。
  /// 对应 C# `Run` 返回 false：EXEC 回空数组。
  Aborted,
}

/// 事务管理器
pub struct TransactionManager {
  /// 事务状态（C# state）
  pub state: TxnState,
  /// 事务键加锁集合（C# keyEntries，持所属引擎实例的锁表句柄）
  pub key_entries: TxnKeyEntries,
  /// 被监视键容器（C# watchContainer）
  pub watch_container: TxnWatchedKeysContainer,
  /// MULTI 起点（缓冲偏移；C# txnStartHead，EXEC 回退重放用）
  pub txn_start_head: usize,
  /// 事务内排队命令数（C# operationCntTxn）
  pub operation_cnt_txn: usize,
  /// 事务含写操作（决定 AOF 事务条目是否记录；C# PerformWrites）
  pub perform_writes: bool,
  /// 当前事务版本（C# txnVersion）
  pub txn_version: i64,
  /// 本笔事务的全事务屏障注册票据（对标 C# 事务随 store 绑定的
  /// `stateMachineDriver` 字段 + `txnVersion != 0` 的「已注册」判据）：
  /// [`Self::run`] / [`Self::run_exec`] 起手注册成功即持有，[`Self::reset`] 于
  /// 桶闩尽释后注销并清空——票据在位即为「已注册」唯一真值源，
  /// 杜绝重复注册漏计与漏注销挂死扩容排空。
  txn_barrier: Option<TxnBarrierTicket>,
  /// AOF 事务日志（C# appendOnlyFile.Log；None = AofEnabled false）
  pub aof_log: Option<Arc<dyn TxnAofLog>>,
  /// 会话 ID（AOF 事务条目归属；C# stringBasicContext.Session.ID）
  pub session_id: i32,
  /// 事务涉及的键集合（EXEC 多键槽校验用，扁平连续缓冲 O(1) 尾追；对标 C# txnKeysParseState）
  pub txn_keys: TxnKeysBuffer,
  /// 集群模式是否启用（对标 C# TransactionManager.cs:clusterEnabled；
  /// txn_keys / SaveKeyArgSlice 仅在 cluster_enabled 时登记，
  /// 单机形态零登记零堆分配）
  pub cluster_enabled: bool,
  /// 本笔外部事务的锁集登记/版本获取是否已就绪（对标 C# Run 一次性前置：
  /// WATCH 键并入 + 取版本仅在首轮做一次）。compio 态外部 EXEC 走
  /// [`Self::run_exec`] 的异步臂时，取锁争用会多次复入本方法，此标志杜绝
  /// 重试轮次重复登记 WATCH 键（重复增长键集）与重复消耗全局事务版本。
  /// 由 [`Self::reset`] 复位。
  exec_lock_armed: bool,
  /// 集群模式下 WATCH 键是否已合并进 txn_keys（对标 C# TxnRespCommands.cs:40
  /// SaveKeysToKeyList 单次消费契约）：compio 态外部 EXEC 争用重驱会多次
  /// 复入 network_exec，此标志将「并入过」与「起锁成功（armed）」解耦，
  /// 杜绝重驱轮次重复推入 WATCH 键导致 txn_keys 线性膨胀与重算槽校验放大。
  /// 由 [`Self::reset`] 复位。
  pub watch_merged_into_txn_keys: bool,
  /// EXEC 展开期物理前缀快照（用于检测重放期换代）
  pub lock_prefix: Option<Box<[u8]>>,
}

impl TransactionManager {
  /// 构造事务管理器
  ///
  /// `lock_table` 为所属引擎实例的锁表句柄（对标 C# 事务管理器随会话构造、
  /// 锁面取该 store 的 `LockTable`），键集加锁集合持同一句柄。
  /// `aof_log` 为 AOF 日志句柄（C# 构造注入的 `GarnetAppendOnlyFile`；
  /// None = 该库未启用 AOF）。trait 擦除收在本构造边界单点（wtxn 边界），
  /// 装配点持具体日志类型（产线侧恒 `GarnetLog`）隐式强转，零显式 cast。
  pub fn new(
    lock_table: TxnLockTable,
    watch_version_map: Arc<WatchVersionMap>,
    aof_log: Option<Arc<dyn TxnAofLog>>,
  ) -> Self {
    Self {
      state: TxnState::None,
      key_entries: TxnKeyEntries::new(16, lock_table),
      watch_container: TxnWatchedKeysContainer::new(watch_version_map),
      txn_start_head: 0,
      operation_cnt_txn: 0,
      perform_writes: false,
      txn_version: 0,
      txn_barrier: None,
      aof_log,
      session_id: 0,
      txn_keys: TxnKeysBuffer::new(),
      cluster_enabled: false,
      exec_lock_armed: false,
      watch_merged_into_txn_keys: false,
      lock_prefix: None,
    }
  }

  /// 重置事务状态
  ///
  /// libs/server/Transaction/TransactionManager.cs:Reset
  /// 无条件解锁全部键并清理锁集，杜绝中止/丢弃事务的锁泄漏与跨事务键污染。
  /// 桶闩尽释后经注册票据注销活跃事务计数（对标 C# Reset 中 `if (txnVersion != 0)
  /// stateMachineDriver.EndTransaction(txnVersion)`——票据在位即本笔已注册，
  /// 且注销必打在注册时那个驱动实例上）：提交/中止/加锁失败/丢弃/析构全部退出
  /// 路径经本函数单点收口，不漏减、不重减、不错减他引擎实例。
  pub fn reset(&mut self) {
    self.key_entries.unlock_all_keys();
    if let Some(barrier) = self.txn_barrier.take() {
      barrier.end_txn();
    }
    self.txn_version = 0;
    self.txn_start_head = 0;
    self.operation_cnt_txn = 0;
    self.state = TxnState::None;
    self.perform_writes = false;
    self.txn_keys.clear();
    self.exec_lock_armed = false;
    self.watch_merged_into_txn_keys = false;
    self.lock_prefix = None;
  }

  /// 运行事务（libs/server/Transaction/TransactionManager.cs:Run）
  ///
  /// 保存 WATCH 锁集 → 取事务版本 → 加锁 → 校验 WATCH →
  /// 记录 TxnStart → 置 Running。
  ///
  /// 线程/内部事务上下文专用：非快速失败臂走 [`TxnKeyEntries::lock_all_keys`]
  /// 的抢占式自旋（真线程让步）。compio 单核 worker 上的外部 MULTI/EXEC 不得走
  /// 此自旋（会饿死同 worker 挂起持闩的阻塞命令），改走 [`Self::run_exec`] 异步臂。
  ///
  /// `lock_prefix` 为 EXEC 执行时刻的会话**物理**前缀（锁轨种子，WATCH 键
  /// 并入锁集按其现算，对位 C# SaveKeysToLock→GetKeyHash 运行期取值；
  /// 版本轨=逻辑域种子，两轨分置见 [`TxnWatchedKeysContainer::save_lock_hashes`]）
  pub fn run(
    &mut self,
    lock_prefix: &[u8],
    internal_txn: bool,
    fail_fast_on_lock: bool,
    lock_timeout: Duration,
  ) -> bool {
    self.begin_run_preamble(lock_prefix, internal_txn);

    let lock_success = if fail_fast_on_lock {
      self.key_entries.try_lock_all_keys(lock_timeout)
    } else {
      self.key_entries.lock_all_keys();
      true
    };

    if !lock_success {
      log::error!("Transaction failed to acquire all the locks on keys to proceed.");
      self.reset();
      if !internal_txn {
        self.watch_container.reset();
      }
      return false;
    }

    self.finish_run_postlock(internal_txn)
  }

  /// 外部 MULTI/EXEC 的 compio 安全事务起点（对标 [`Self::run`] 的外部队，
  /// 但屏障注册与取锁均为「单次尝试」而非线程自旋）。
  ///
  /// 首轮经 [`TxnLockTable::try_acquire_txn`] 单次注册全事务屏障（扩容
  /// PREPARE_GROW 占用即回 [`ExecRun::Contended`] 交慢臂重驱，绝不自旋占死
  /// reactor，对标 C# AcquireTransactionVersion 拦截的 compio 投影），再登记
  /// WATCH 键并取版本后单次尝试取闩：
  /// - 成功（无争用快路径，与 C# 内联直取零额外开销同形）→ 续跑 [`Self::run`]
  ///   的锁后收尾（WATCH 校验 + TxnStart + 置 Running），返回 [`ExecRun::Started`]；
  /// - 争用 → 键集/计划/钉定索引原样保留、不 reset，返回 [`ExecRun::Contended`]，
  ///   交宿主会话登记既有慢臂（`pending_slow`/`SlowWait` 单次让步 + 重驱本命令），
  ///   下一轮复入本方法时 [`Self::exec_lock_armed`] 已置位，跳过登记不重复、
  ///   仅复调 [`TxnKeyEntries::try_lock_all_keys_once`] 再单次尝试，直至成功——
  ///   保留 C# `TransactionalContext.Lock` 的无界等待契约，且不丢键集。
  /// - 锁获取后收尾失败（WATCH 被并发推进 / TxnStart 落盘错误）→ 复位，
  ///   返回 [`ExecRun::Aborted`]（对应 C# `run` 返回 false，EXEC 回空数组）。
  ///
  /// 门控仅在本异步臂生效：前置屏障注册、WATCH 键登记与版本获取只在争用重试链
  /// 首轮做一次，复入轮据 [`Self::exec_lock_armed`] 跳过（杜绝重复登记键集、
  /// 重复消耗版本与重复注册计数）。[`Self::run`] 的线程臂不受本门控，每次调用
  /// 完整前置（与 C# `Run` 同构）；门控位与屏障注册均由 [`Self::reset`] 随事务
  /// 代际清除。
  ///
  /// `lock_prefix` 口径同 [`Self::run`]（EXEC 时刻会话物理前缀，锁轨现算种子；
  /// 复入轮宿主会话域未变则逐轮同值，首轮门控下仅消费一次）。
  pub fn run_exec(&mut self, lock_prefix: &[u8]) -> ExecRun {
    if !self.exec_lock_armed {
      if !self.try_acquire_barrier() {
        return ExecRun::Contended;
      }
      self.register_run_preamble(lock_prefix, false);
      self.exec_lock_armed = true;
    }
    if !self.key_entries.try_lock_all_keys_once() {
      return ExecRun::Contended;
    }
    if !self.finish_run_postlock(false) {
      return ExecRun::Aborted;
    }
    ExecRun::Started
  }

  /// 事务前置全序（线程臂）：阻塞过 PREPARE_GROW 全事务屏障并注册活跃事务 →
  /// WATCH 键并入锁集 → 取全局事务版本（对标 C# `Run` 起手的
  /// `txnVersion = stateMachineDriver.AcquireTransactionVersion()` 全套前置）。
  ///
  /// 票据在位即直通取锁，绝不重复注册（C# 靠 `Reset` 先行同型约束，rust 以
  /// 票据判据兜底，杜绝重复注册漏计把扩容排空永久挂死）。
  fn begin_run_preamble(&mut self, lock_prefix: &[u8], internal_txn: bool) {
    // 线程上下文阻塞注册（对标 C# AcquireTransactionVersion 的
    // while (Phase == PREPARE_GROW) { ProtectAndDrain(); Thread.Yield(); }）
    if self.txn_barrier.is_none() {
      self.txn_barrier = Some(self.key_entries.lock_table().acquire_txn());
    }
    self.register_run_preamble(lock_prefix, internal_txn);
  }

  /// 屏障单次非阻塞注册尝试（compio 异步臂）：已注册即直通（争用重试链复入
  /// 由 [`Self::exec_lock_armed`] 门控，本判据为其兜底双保险）；未注册且扩容
  /// 处于 PrepareGrow 返回 false，交调用方回 [`ExecRun::Contended`] 让步重驱。
  #[must_use]
  fn try_acquire_barrier(&mut self) -> bool {
    if self.txn_barrier.is_some() {
      return true;
    }
    self.txn_barrier = self.key_entries.lock_table().try_acquire_txn();
    self.txn_barrier.is_some()
  }

  /// 屏障注册后的前置登记（WATCH 键并入锁集 + 取全局事务版本），每次调用完整执行
  /// （对标 C# `Run` 函数体内的前置段）。异步臂复入去重由唯一置位方
  /// [`Self::run_exec`] 的门控调用承担，本函数自身不设判据。
  ///
  /// 测试握手：生产消费面仅限本域 [`Self::begin_run_preamble`] / [`Self::run_exec`]，
  /// pub 可见性仅供 wtxn / wnode 集成测试直驱（同 [`TxnKeysBuffer::len`] 先例）。
  #[doc(hidden)]
  pub fn register_run_preamble(&mut self, lock_prefix: &[u8], internal_txn: bool) {
    self.lock_prefix = Some(lock_prefix.into());

    // WATCH 键并入锁集（字段拆借免克隆）：锁轨=物理域，按 EXEC 时刻会话前缀
    // 对裸键体现算（对标 C# SaveKeysToLock → AddKey → GetKeyHash 运行期取值；
    // Shared 恒不置位 perform_writes，免走登记内核）；路由轨=裸键哈希，对齐 AOF 子日志
    if !internal_txn {
      let Self {
        watch_container,
        key_entries,
        ..
      } = self;
      for (hash, routing_hash) in watch_container.save_lock_hashes(lock_prefix) {
        key_entries.add_key(hash, routing_hash, LockType::Shared);
      }
    }

    // 排队键展开：按 EXEC/Run 时刻当前物理前缀统一重展开入 key_entries，
    // 杜绝排队期冻结代际哈希导致换号穿透；写标记于展开期原子置位；
    // 裸键在手处单次现算 whasher::fast_hash_i64(key) 作为 routing_hash 存入 entry
    let Self {
      txn_keys,
      key_entries,
      perform_writes,
      ..
    } = self;
    for (key, lock_type) in txn_keys.iter_with_lock() {
      *perform_writes |= lock_type == LockType::Exclusive;
      let hash = TxnKeyEntryComparison::scoped_key_hash(lock_prefix, key);
      let routing_hash = whasher::fast_hash_i64(key);
      key_entries.add_key(hash, routing_hash, lock_type);
    }

    // 取全局单调递增事务版本（对标 C# StateMachineDriver.AcquireTransactionVersion）
    self.txn_version = GLOBAL_TXN_VERSION_SEQ.fetch_add(1, Ordering::Relaxed) as i64;
  }

  /// 重放段换号重展开：按当前物理前缀重算哈希（含 WATCH 键与排队键）并入 key_entries，
  /// 对新增桶增量 try 闩、对旧代仅持 Shared 而新代要求 Exclusive 的同桶升闩
  /// （强度口径与 lock_plan/归并步「同桶取最强」对齐，杜绝 Shared 旧持桶吞掉
  /// Exclusive 新条目）。成功返回 true 并更新 lock_prefix；争用返回 false，交慢臂让步重驱。
  pub fn reexpand_for_generation_swap(&mut self, new_prefix: &[u8]) -> bool {
    let mut new_entries = SmallVec::<[TxnKeyEntry; 8]>::with_capacity(self.txn_keys.len());
    for (hash, routing_hash) in self.watch_container.save_lock_hashes(new_prefix) {
      new_entries.push(TxnKeyEntry::new(hash, routing_hash, LockType::Shared));
    }
    for (key, lock_type) in self.txn_keys.iter_with_lock() {
      self.perform_writes |= lock_type == LockType::Exclusive;
      let hash = TxnKeyEntryComparison::scoped_key_hash(new_prefix, key);
      let routing_hash = whasher::fast_hash_i64(key);
      new_entries.push(TxnKeyEntry::new(hash, routing_hash, lock_type));
    }

    if !self.key_entries.try_lock_incremental_entries(&new_entries) {
      return false;
    }

    self.lock_prefix = Some(new_prefix.into());
    true
  }

  /// 锁获取后收尾（WATCH 版本校验 → TxnStart 标记 → 置 Running）。
  ///
  /// 任一失败按 C# `Run` 锁失败同款收尾（reset + WATCH 容器复位）后返回 false；
  /// 成功置 [`TxnState::Running`] 返回 true。
  fn finish_run_postlock(&mut self, internal_txn: bool) -> bool {
    if !internal_txn && !self.watch_container.validate_watch_version() {
      self.reset();
      self.watch_container.reset();
      return false;
    }

    // TxnStart 标记（C# EnqueueTxn 异常上抛的 rust 投影：并入锁失败同款
    // 收尾后返回 false；标记未落盘，AOF 无残缺事务组）
    if self.perform_writes
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

  /// 废弃事务收口单点（rust 侧形态，C# 无对应：NetworkEXEC 重放在单网络
  /// 线程内联执行、批中途不可被外部打断，CLIENT KILL 仅令后续读失败
  /// （libs/common/Networking/GarnetTcpNetworkSender.cs:TryClose 的
  /// socket.Close()），重放必达 Commit，AOF 组恒闭合，故 C# 不存在需要
  /// 补投终结符的废弃面）
  ///
  /// rust 泵把 EXEC 重放段（Running 直通）命令的挂起交竞速驱动（wnode
  /// net/handler/drive.rs 的 probe_race 三臂），终止广播或对端脱机胜出即
  /// RaceEnd::Disposed 丢弃执行体、重放中途废弃：TxnStart 已落 AOF、组内前缀
  /// 写已生效，而尾帧 EXEC 不再被消费、TxnCommit 永不再达，AOF 留孤儿残组。
  /// 本单点对 Running 态事务经既有 `enqueue_txn_marker`（与
  /// [`Self::commit`] 同一入队内核）补投显式 [`TxnEntryType::TxnAbort`]
  /// 终结符，接通 waof 既有 `AofEntryType::TxnAbort` 判别值与恢复/副本协调器
  /// 既有弃组臂，随后走现行 [`Self::reset`]——锁释放与屏障注销路径零改动。
  ///
  /// 终结符取 Abort 不取 Commit：半组提交会把已废弃的尾命令钉成永久缺失、
  /// 并令副本整组重放半组（原子性破坏更深）；TxnAbort 与恢复期「无提交
  /// 终结符整组弃置」的现行为同结果，只是显式化，同时解副本组缓冲滞留。
  /// 无 AOF 或未落 TxnStart 的只读事务无组可收，直复位。
  ///
  /// ⚠️ 重放废弃组 = 刻意弃置面（严禁改判为补提交）：组内已生效的前缀写随
  /// 弃组在重启恢复后消失，是废弃语义的既定代价，对标 C# 恢复侧
  /// AofReplayCoordinator 对未闭合组仅在恢复末尾整组弃置、从不半组重放
  /// （libs/server/AOF/ReplayCoordinator/AofReplayCoordinator.cs:activeTxns）。
  /// 后续「恢复丢写」类报告的正解是消灭竞速废弃窗口本身，不是在此处改写
  /// 终结符。
  ///
  /// 入队失败透传（C# 无此面；rust 由调用方落日志——客户端已断连无从应答），
  /// 但状态收尾仍先做，杜绝锁与屏障票据随废弃泄漏。
  pub fn finish_abandoned(&mut self) -> waof::Result<()> {
    if self.state != TxnState::Running {
      return Ok(());
    }
    let enqueued = if self.perform_writes
      && let Some(log) = self.aof_log.as_deref()
    {
      self.enqueue_txn_marker(log, TxnEntryType::TxnAbort)
    } else {
      Ok(())
    };
    self.reset();
    enqueued
  }

  /// 事务是否处于排队/中止在途态
  ///（libs/server/Transaction/TransactionManager.cs:IsSkippingOperations）
  ///
  /// C# 唯一消费面为 RespServerSession.TryConsumeMessages 批尾采样 `txnSkip`：
  /// Started（MULTI 排队中）/ Aborted（排队期出错）时禁平移接收缓冲，保全排队
  /// 字节与 txnStartHead 偏移供 EXEC 回退重放；rust 消费面为
  /// `RespServerSession::try_consume_messages_body` 的缓冲清零门。Running 不在列：
  /// EXEC 重放同步完成后批内必达提交复位，不跨批驻留。
  #[inline]
  pub fn is_skipping_operations(&self) -> bool {
    matches!(self.state, TxnState::Started | TxnState::Aborted)
  }

  /// 本笔外部事务的锁集登记/版本获取是否已就绪（对标 C# Run 一次性前置：争用重试轮门控）
  #[inline]
  pub fn is_exec_lock_armed(&self) -> bool {
    self.exec_lock_armed
  }

  /// 事务是否只读（C# keyEntries.IsReadOnly 与 txn_keys.is_read_only 同构）
  #[inline]
  pub fn is_read_only(&self) -> bool {
    self.key_entries.is_read_only() && self.txn_keys.is_read_only()
  }

  /// 监视键
  ///
  /// libs/server/Transaction/TransactionManager.cs:Watch
  ///
  /// `prefix` 为会话**逻辑**归属前缀（`[NsVarint][DbVarint]` 逻辑域真值投影，
  /// 与写面推进的 `StoreSession::session_logical_prefix` 同一单点）：版本轨=
  /// 逻辑域种子，FLUSHDB/FLUSHNS/SWAPDB 换号不改逻辑身份，换号前后同逻辑键
  /// bump 与核验恒落同槽，改后写必 abort；锁轨另置——EXEC 并入锁集按当前
  /// **物理**前缀现算（[`TxnWatchedKeysContainer::save_lock_hashes`]），
  /// 两轨分置两单点、禁共口互染，杜绝跨租户/跨库同名键假性中止与假性互斥；
  /// txn_keys 仍按用户键裸字节登记（C# 每库独持槽校验表、天然库内裸键口径）。
  ///
  /// 保存键版本，并为键加独占锁（C# SetLockType(true) 并向 ScratchBuffer
  /// 挂起修改标记）；wkv 无挂起修改缓冲，监视登记即完整语义。WATCH 键入
  /// EXEC 多键面（C# SaveKeysToKeyList，仅集群启用时由 network_exec 首轮同步登记），不触迭代槽校验。
  pub fn watch(&mut self, prefix: &[u8], key: &[u8]) {
    self.watch_container.add_watch(prefix, key);
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
  ///
  /// 测试握手：生产消费面仅限本域 [`Self::enqueue_txn_marker`]，pub 可见性仅供
  /// wnode 集成测试断言向量展开（同 [`TxnKeysBuffer::len`] 先例）。
  #[doc(hidden)]
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
    // 直读 entry 的裸路由哈希 routing_hash（等同于 GarnetLog.HASH），与 AOF 数据条目路由恒对齐。
    for routing_hash in self.key_entries.routing_hashes() {
      let physical_idx = log.get_physical_sublog_idx(routing_hash);
      let replay_idx = log.get_replay_task_idx(routing_hash);

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
