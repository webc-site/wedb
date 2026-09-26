//! AOF 重放处理器（对标 libs/server/AOF/AofProcessor.cs:AofProcessor）
//!
//! 恢复回放与复制回放共用的条目处理内核：按条目头分发（检查点标记 /
//! FLUSH / 存储过程 / 事务 / 数据操作），数据操作经 [`ReplayTarget`] 落入
//! wkv 存储会话。rust 侧重放应用为异步（wkv 面天然异步），拓扑预处理
//! （key 哈希 / 一致性时间戳推进）保持同步快路径。
//!
//! C# 的拓扑特化预处理结构（SingleLogPreprocessKey 等）折叠为
//! `prepare_key`：按拓扑更新一致性时间戳并产出 key/payload 视图。

use std::{
  borrow::Cow,
  future::Future,
  sync::{
    Arc,
    atomic::{AtomicI64, Ordering},
  },
};

use futures_util::future::LocalBoxFuture;
use parking_lot::RwLock;
use waof::{AofEntryType, AofHeader};
use wdev::Device;
use wkv::{CollectionError, Error, WedbStore};
use wval::{KeyTag, NamespaceDbCodec};

use super::{
  aof_processor_object_replay::{object_store_delete, object_store_rmw, object_store_upsert},
  aof_processor_store_ops::{replay_dbmeta, store_delete, store_rmw, store_upsert},
  garnet_append_only_file::GarnetAppendOnlyFile,
  garnet_log::GarnetLog,
  readconsistency::read_consistency_manager::ReadConsistencyManager,
  record_gate,
  replay_input::ReplayInput,
  replaycoordinator::{
    aof_replay_context::{ReplayOperation, TransactionGroup},
    aof_replay_coordinator::{AofReplayCoordinator, LeaderBarrierType},
  },
};
use crate::{
  range_index::range_index_manager_replication::{RangeIndexManagerReplication, ReplicationError},
  resp::vector::vector_manager::RegistryReclaim,
  storage::session::storage_session::StorageSession,
};

/// AOF 重放域错误（C# GarnetException 回放路径的 rust 形态）。
#[derive(Debug, thiserror::Error)]
pub enum AofReplayError {
  /// 重放语义错误（损坏条目 / 未接线子域 / 存储失败）。
  #[error("AOF replay: {0}")]
  Replay(String),
  /// 存储面错误（wkv 透传）。
  #[error(transparent)]
  Store(#[from] wkv::Error),
  /// 日志判读错误（waof 透传：未知回放头型 / 头损坏 / 未知操作类型）。
  #[error(transparent)]
  Log(#[from] waof::Error),
  /// 并行恢复 worker 回放体 panic（监督隔离捕获的展开载荷文本；对标 C#
  /// RecoverReplayTask.cs RecoverReplayTaskAsync try/catch(Exception) 全捕
  /// 形态——C# 无 panic 穿透错误臂的形态，rust 以本变体归入既有错误链）。
  #[error("AOF replay worker panicked: {0}")]
  Panicked(String),
}

impl From<String> for AofReplayError {
  fn from(message: String) -> Self {
    Self::Replay(message)
  }
}

impl From<&str> for AofReplayError {
  fn from(message: &str) -> Self {
    Self::Replay(message.to_string())
  }
}

/// RI 复制族类型化错误归位（票 zcode-r135c 案二）：重放族错误带变体原样
/// 上浮至分派面，经既有 Store/Log 透明通道收敛，杜绝分派臂再套一段
/// `format!` 把类型化错误砸进 Replay(String) 击穿透明链（对标 C#
/// AofProcessor.cs 重放 catch 记 ex.ToString() 的 cause 链保全形态）；
/// Msg（携带上下文快照的损坏条目/状态机违例）系天然无类型源，留 Replay
impl From<ReplicationError> for AofReplayError {
  fn from(e: ReplicationError) -> Self {
    use ReplicationError as R;
    match e {
      R::Msg(message) => Self::Replay(message),
      R::Io(io) => Self::Store(Error::Io(io)),
      R::BfTree(tree) => Self::Store(Error::Collection(CollectionError::Tree(tree))),
      R::RangeIndex(index) => Self::Store(Error::RangeIndex(index)),
      R::Aof(log) => Self::Log(log),
    }
  }
}

/// 预处理产出：key、key 哈希与负载起点（C# PreparedParameters）。
pub struct PreparedParameters<'a> {
  /// key 字节。
  pub key: Cow<'a, [u8]>,
  /// key 哈希（GarnetLog::HASH）。
  pub key_hash: i64,
  /// 负载（key 之后的首个组件起点）。
  pub payload: Cow<'a, [u8]>,
}

/// 重放落点：存储会话 + 存储句柄（FLUSH 面）。
pub struct ReplayTarget<'a, 'b, D: Device> {
  /// 存储会话（读写应用面）。
  pub session: &'b StorageSession<'a, D>,
  /// 存储句柄（FLUSH ALL/DB 应用面）。
  pub store: Arc<WedbStore<D>>,
  /// 恢复面 AOF 重放位点下界向量（恢复检查点元数据的覆盖边界，按物理子日志
  /// 逐位保存，缺位/空 = 无下界；C# RecoveredSafeAofAddress 全向量形态）。
  /// [`record_gate::should_skip_record`] 的位点闸按条目所属物理子日志经
  /// [`Self::aof_floor_of`] 逐位取值，跳过快照已物化条目（截断前异常宕机纵深
  /// 防御）；副本面由闸内 `!as_replica` 位判天然禁用。绝不以子日志 0 标量
  /// 广播全维——子日志间地址空间独立，标量广播会把小写入量子日志的合法
  /// 区间整段误跳
  pub aof_floor: Vec<i64>,
}

impl<'a, 'b, D: Device> ReplayTarget<'a, 'b, D> {
  /// 装配重放落点（位点下界向量取自存储恢复面，装配点统一经此构造）。
  pub fn new(session: &'b StorageSession<'a, D>, store: &Arc<WedbStore<D>>) -> Self {
    Self {
      session,
      store: Arc::clone(store),
      aof_floor: store
        .recovered_aof_floor()
        .iter()
        .map(|&a| a as i64)
        .collect(),
    }
  }

  /// 按物理子日志取位点下界（缺位 = 无下界 0）
  #[inline]
  pub fn aof_floor_of(&self, sublog_idx: usize) -> i64 {
    self.aof_floor.get(sublog_idx).copied().unwrap_or(0)
  }

  /// 当前存储版本（SkipRecord 版本判定，逐记录动态读取；C#
  /// storeWrapper.store.CurrentVersion——检查点推进后即刻生效，绝不缓存快照）。
  #[inline]
  pub fn store_version(&self) -> i64 {
    self.store.current_version()
  }
}

/// 副本本地检查点钩子：类型擦除的无参异步动作，产出 [`wkv::Result`]。
///
/// 对标 C# 检查点结束臂 `storeWrapper.TakeCheckpointAsync`（AofProcessor.cs:302-317）
/// 下达面的 rust 注入形态。future 用 [`LocalBoxFuture`]（非 `Send`）：本仓 compio
/// 运行时线程本地驱动，重放钩子仅由背景重放线程同线程 await，与复制装配只加
/// `'static` 不加 `Send` 的既有约定同源；闭包对象只需 `Send + Sync`（宿主仅捕获
/// `Arc` 句柄），方可存入跨线程移动的 [`AofProcessor`]。
pub type ReplicaCheckpointHook = dyn Fn() -> LocalBoxFuture<'static, wkv::Result<()>> + Send + Sync;

/// libs/server/AOF/AofProcessor.cs:AofProcessor
///
/// AOF 重放处理器。
pub struct AofProcessor {
  /// 追加日志文件（一致性管理器 / 序列号 / 拓扑入口）。
  append_only_file: Arc<GarnetAppendOnlyFile>,
  /// 回放协调器（事务 / 模糊区 / 栅栏）。
  coordinator: AofReplayCoordinator,
  /// 活跃库 id。
  active_db_id: AtomicI64,
  /// 范围索引重放面（C# activeRangeIndexManager；RangeIndexPreview 关闭 /
  /// 未注入为 None，RI 族条目重放按 C# 同文案失败）
  range_index: Option<Arc<RangeIndexManagerReplication>>,
  /// 副本本地检查点钩子（C# 检查点结束臂 storeWrapper.TakeCheckpointAsync
  /// 下达面；AofProcessor 不持库管理器句柄、且非 [`Device`] 泛型，故宿主
  /// 装配期以类型擦除闭包注入。未注入为 None：恢复/单机重放臂据此维持
  /// 无本地打点的原语义，仅副本稳态重放链注入）。
  ///
  /// libs/server/AOF/AofProcessor.cs:ProcessAofRecordInternal（case
  /// CheckpointEndCommit 臂的 storeWrapper.TakeCheckpointAsync 触发）
  checkpoint_hook: RwLock<Option<Arc<ReplicaCheckpointHook>>>,
}

impl AofProcessor {
  /// libs/server/AOF/AofProcessor.cs:AofProcessor（构造）
  ///
  /// 依拓扑装配预处理路径并初始化回放上下文。多回放拓扑判定单源取
  /// GarnetAppendOnlyFile::multi_log_enabled（C# usingShardedLog 与
  /// MultiLogEnabled 全等，rust 不再复刻第二对字段）。
  pub fn new(append_only_file: Arc<GarnetAppendOnlyFile>) -> Self {
    let coordinator = AofReplayCoordinator::new(
      append_only_file.virtual_sublog_count(),
      append_only_file.multi_log_enabled(),
      // 栅栏会合时限与副本一致读同源（C# serverOptions.ReplicaSyncTimeout
      // 单一配置位投影，不建第二来源）
      append_only_file.read_timeout(),
    );
    if let Some(manager) = append_only_file.read_consistency_manager() {
      coordinator.set_consistency_manager(manager);
    }
    Self {
      append_only_file,
      coordinator,
      active_db_id: AtomicI64::new(0),
      range_index: None,
      checkpoint_hook: RwLock::new(None),
    }
  }
  /// 注入范围索引重放面（C# 由 storeWrapper.activeRangeIndexManager 装配）。
  pub fn set_range_index_manager(&mut self, manager: Arc<RangeIndexManagerReplication>) {
    self.range_index = Some(manager);
  }

  /// 范围索引重放面句柄。
  pub fn range_index_manager(&self) -> Option<&Arc<RangeIndexManagerReplication>> {
    self.range_index.as_ref()
  }

  /// 注入副本本地检查点钩子（宿主装配期调用；C# 由 storeWrapper 反查
  /// TakeCheckpointAsync 下达，rust 以类型擦除闭包承接）。仅副本稳态重放链
  /// 注入，恢复/单机驱动面不注入 → 检查点结束臂以钩子在场为门维持原语义。
  pub fn set_checkpoint_hook(&self, hook: Arc<ReplicaCheckpointHook>) {
    *self.checkpoint_hook.write() = Some(hook);
  }

  /// 副本本地检查点钩子快照。
  pub fn checkpoint_hook(&self) -> Option<Arc<ReplicaCheckpointHook>> {
    self.checkpoint_hook.read().clone()
  }

  /// 回放协调器句柄。
  pub fn coordinator(&self) -> &AofReplayCoordinator {
    &self.coordinator
  }

  /// 追加日志文件句柄。
  pub fn append_only_file(&self) -> &Arc<GarnetAppendOnlyFile> {
    &self.append_only_file
  }

  /// 读取一致性管理器快捷入口。
  pub fn read_consistency_manager(&self) -> Option<Arc<ReadConsistencyManager>> {
    self.append_only_file.read_consistency_manager()
  }

  /// libs/server/AOF/AofProcessor.cs:SwitchActiveDatabaseContext
  pub fn switch_active_database_context(&self, db_id: i64) {
    self.active_db_id.store(db_id, Ordering::Release);
  }

  /// 活跃库 id。
  pub fn active_db_id(&self) -> i64 {
    self.active_db_id.load(Ordering::Acquire)
  }

  /// libs/server/AOF/AofProcessor.cs:PrepareKey
  ///
  /// 拓扑预处理（C# IPreprocessKey.PrepareKey 三实现 :28/:44/:66 的折叠）：
  /// 解出 key / 哈希 / 负载并按拓扑推进一致性 key 时间戳（零堆分配与零 Arc 克隆）。
  pub fn prepare_key<'a>(
    &self,
    virtual_sublog_idx: usize,
    entry: &'a [u8],
    log_address_sequence_number: i64,
  ) -> Option<PreparedParameters<'a>> {
    // 帧游标单点：条目体起点与序列号一律经 waof 头面唯一口径取数
    //（`AofHeader::skip_header` 按头型定长、`sequence_number_of` 按头型取内嵌
    // 序号），与 record_gate 的条目键速览面同一函数；本函数不自带第二套
    //「头尺寸 + 序列号」判定——未知/截断头型即判损坏上抛，绝不按 16B 误读体
    let header_size = AofHeader::skip_header(entry)?;
    let sequence_number = AofHeader::sequence_number_of(entry, log_address_sequence_number)?;
    let payload = entry.get(header_size..)?;
    // 键段切分走 record_gate 单点（与 peek_entry_key / 值段共用），越界即 None
    let (key, rest) = record_gate::split_len_prefixed(payload)?;
    let key_hash = GarnetLog::hash(key);

    // 多回放拓扑（分片 / 单物理多回放）：按序列号推进一致性时间戳（零 Arc 克隆）
    if self.append_only_file.multi_log_enabled() {
      self
        .append_only_file
        .with_read_consistency_manager(|manager| {
          manager.update_virtual_sublog_key_sequence_number(
            virtual_sublog_idx,
            key_hash,
            sequence_number,
          );
        });
    }
    Some(PreparedParameters {
      key: Cow::Borrowed(key),
      key_hash,
      payload: Cow::Borrowed(rest),
    })
  }

  /// 从记录负载解析 value（长度前缀形态）与剩余 input。
  #[inline]
  pub(crate) fn split_value_input(payload: &[u8]) -> Option<(&[u8], &[u8])> {
    record_gate::split_len_prefixed(payload)
  }

  /// libs/server/AOF/ReplayCoordinator/AofReplayCoordinator.cs:ReplayStoredProc
  ///
  /// 存储过程回放包装（单 / 分片日志统一入口）：
  /// - 单物理日志：直接重放（无收集器，对齐 C# `tracker: null`）；
  /// - 多日志拓扑：经同步栅栏（栅栏键为会话 id + 序列号，与 C#
  ///   ReplayStoredProc 传 sessionId 一致；负值类别段仅 FLUSH / 检查点族使用），
  ///   仅领导者执行；领导者以
  ///   [`CustomProcedureKeyHashCollection`] 收集过程触达键哈希，回放后
  ///   推进其读一致性时间戳（顺序差异见该类型文档）。
  pub async fn replay_stored_proc<D: Device>(
    &self,
    _virtual_sublog_idx: usize,
    _entry: &[u8],
    _log_address_sequence_number: i64,
    _target: &ReplayTarget<'_, '_, D>,
  ) -> Result<(), AofReplayError> {
    Err("已移除自定义事务过程支持，无法回放存储过程条目".into())
  }

  fn replay_task_count(&self) -> usize {
    self.append_only_file.virtual_sublog_per_sublog()
  }

  /// 同步操作回放栅栏编排（C# ProcessAofRecordInternal 各
  /// GetSynchronizedOperationParams + ProcessSynchronizedOperation 支的合流入口：
  /// FLUSH 族清空、副本检查点结束臂本地打点共用同一栅栏机制，不留第二套）：
  /// 按条目头取 (序列号, 参与者数) 交协调器异步栅栏入口，Leader 独占段内 await
  /// 传入动作，全员对齐 → 独占执行 → 清栏放行 → 虚拟子日志最大序列号推进一体化
  /// 承接，非多回放形态由入口内部直执行 + 推进（对标 C# `!usingShardedLog`
  /// 的 BlockingWait 直调）。动作闭包自带其上下文（FLUSH 闭包克隆 store、
  /// 检查点闭包克隆钩子），本编排不持任何域句柄。
  async fn synchronized_under_barrier<F, Fut, R>(
    &self,
    virtual_sublog_idx: usize,
    entry: &[u8],
    log_address_sequence_number: i64,
    barrier_type: LeaderBarrierType,
    op: F,
  ) -> Result<(), AofReplayError>
  where
    F: FnOnce() -> Fut,
    Fut: Future<Output = wkv::Result<R>>,
  {
    let (sequence_number, participant_count) = record_gate::get_synchronized_operation_params(
      self.replay_task_count(),
      entry,
      log_address_sequence_number,
    )
    .ok_or("同步操作条目缺少栅栏参数")?;
    self
      .coordinator
      .process_synchronized_operation_async(
        virtual_sublog_idx,
        sequence_number,
        participant_count,
        barrier_type as i32,
        Some(move || async move { op().await.map_err(AofReplayError::Store) }),
      )
      .await
      .map(|_| ())
  }

  /// libs/server/AOF/AofProcessor.cs:ProcessAofRecordInternal
  ///
  /// 共享回放入口（恢复回放与复制回放同一路径）。返回是否遇到检查点
  /// 起始标记（复制侧据此记录 ReplicationCheckpointStartOffset）。
  pub async fn process_aof_record_internal<D: Device>(
    &self,
    virtual_sublog_idx: usize,
    entry: &[u8],
    as_replica: bool,
    log_address_sequence_number: i64,
    target: &ReplayTarget<'_, '_, D>,
  ) -> Result<bool, AofReplayError> {
    // 逐记录纪元让步（对标 C# 重放会话逐记录常规 context 的 enter/exit——
    // Tsavorite 重放不经 UnsafeContext 持整轮保护）：重放会话批守卫整轮在场，
    // 重入臂只递增计数不刷新公布纪元，不让步即把会话槽位公布纪元钉死入场值，
    // 全量重放期间目标引擎 safe_head/closed_until/检查点 fence 的排空屏障全部
    // 停摆（前台写环形回绕 evict_pages_for 乃至秒级停摆）。在上一条已完整落库
    // 的记录边界瞬时释放并重入（重入即公布最新纪元），单点收口于本共享入口，
    // 单任务快路径/页级并行/副本回放全拓扑覆盖
    target.session.batch.epoch_yield();
    // commit 元数据帧（waof 提交边界标记）非 AOF 条目，重放面跳过；
    // 复制推流面不过滤——帧随流保真传输，从侧恢复得以同样收敛
    if waof::is_commit_frame(entry) {
      return Ok(false);
    }

    // 顺序布局与并行恢复同构：分块帧每帧携带完整帧头，统一经
    // AofHeader::is_chunked 分发进重组读取器（裸续块旁路已随写端线协议
    // 对齐 C# WriteOneRecord 而移除，见 enqueue_span_chunked）

    let header = AofHeader::parse(entry).ok_or("AOF 条目头损坏")?;
    // 版本域自持的等值门（C# MaxSupportedAofHeaderVersion 上界臂的收窄）：
    // 本仓 AOF 载荷与 C# 异构且不做向下兼容，凡非本构建代际一律显式拒绝，
    // 使跨仓搬运文件在此判止而非被键域/输入区静默误读
    if header.aof_header_version != AofHeader::AOF_FORMAT_VERSION {
      return Err(
        format!(
          "Unsupported AOF format version {}; this build only accepts {}; \
           cross-repo (C# garnet) or foreign-generation AOF files are rejected",
          header.aof_header_version,
          AofHeader::AOF_FORMAT_VERSION
        )
        .into(),
      );
    }

    // 分块记录：累积至完成即直接分派（不物化连续镜像）
    if header.is_chunked() {
      let completed = self
        .coordinator
        .context(virtual_sublog_idx)
        .chunked_reader
        .read_chunk(entry);
      if let Some(acc) = completed {
        return super::aof_processor_chunk_replay::process_chunked_record(
          self,
          virtual_sublog_idx,
          acc,
          as_replica,
          log_address_sequence_number,
          target,
        )
        .await;
      }
      return Ok(false);
    }

    // C# 直接以枚举转型分发；未知判别值为损坏条目，显式拒绝
    //（replay_op_dispatch 的 default 分支同文案报错）
    let op_type = AofEntryType::try_from(header.op_type)
      .map_err(|_| format!("Unknown AOF header operation type {}", header.op_type))?;

    // 事务处理：TxnStart/TxnAbort/TxnCommit 及组内操作由协调器消化
    let action = self.coordinator.add_or_replay_transaction_operation(
      virtual_sublog_idx,
      entry,
      log_address_sequence_number,
    );
    match action {
      super::replaycoordinator::aof_replay_coordinator::TxnAction::Handled => return Ok(false),
      super::replaycoordinator::aof_replay_coordinator::TxnAction::Commit { group } => {
        let (sequence_number, participant_count) = record_gate::get_synchronized_operation_params(
          self.replay_task_count(),
          entry,
          log_address_sequence_number,
        )
        .unwrap_or((log_address_sequence_number, group.participant_count as i16));
        self
          .process_transaction_group(
            virtual_sublog_idx,
            sequence_number,
            participant_count,
            &group,
            as_replica,
            target,
          )
          .await?;
        return Ok(false);
      }
      // 非法事务流（活动组在位撞嵌套 TxnStart / 组内 StoredProcedure）：
      // 携协调器拒绝文案上抛，恢复即中止（C# 同位两臂 throw GarnetException）
      super::replaycoordinator::aof_replay_coordinator::TxnAction::Reject(reason) => {
        return Err(reason.into());
      }
      super::replaycoordinator::aof_replay_coordinator::TxnAction::None => {}
    }

    let mut is_checkpoint_start = false;
    match op_type {
      AofEntryType::CheckpointStartCommit => {
        is_checkpoint_start = true;
        // C# 的 aofHeaderVersion > 1 是 v1 旧文件兼容臂；本仓单版本域等值门
        // 之下过门条目恒为本代际，模糊区跟踪无条件启用
        if self
          .coordinator
          .context(virtual_sublog_idx)
          .in_fuzzy_region()
        {
          // C# AofProcessor.cs:276 同款 Information 留痕：上一模糊区未遇
          // CheckpointEndCommit 即遭新 CheckpointStartCommit，此处将静默
          // 丢弃其缓冲重放条目，丢弃条数为排障唯一信号
          let discarded = self
            .coordinator
            .fuzzy_region_buffer_count(virtual_sublog_idx);
          log::info!(
            "上一模糊区未遇 CheckpointEndCommit 即遭新 CheckpointStartCommit，丢弃先前模糊区缓冲 {discarded} 条"
          );
          self
            .coordinator
            .clear_fuzzy_region_buffer(virtual_sublog_idx);
        }
        self
          .coordinator
          .context(virtual_sublog_idx)
          .set_in_fuzzy_region(true);
        if self.append_only_file.multi_log_enabled() {
          let sequence_number = record_gate::get_synchronized_operation_params(
            self.replay_task_count(),
            entry,
            log_address_sequence_number,
          )
          .map_or(0, |(seq, _)| seq);
          self
            .append_only_file
            .with_read_consistency_manager(|manager| {
              manager
                .update_virtual_sublog_max_sequence_number(virtual_sublog_idx, sequence_number);
            });
        }
      }
      AofEntryType::CheckpointEndCommit => {
        // C# 的 aofHeaderVersion > 1 是 v1 旧文件兼容臂；过门条目在本仓
        // 单版本域等值门之下恒为本代际，直接判读模糊区状态
        if !self
          .coordinator
          .context(virtual_sublog_idx)
          .in_fuzzy_region()
        {
          // 无起始标记的结束标记：忽略（C# LogInformation 分支）
        } else {
          self
            .coordinator
            .context(virtual_sublog_idx)
            .set_in_fuzzy_region(false);
          // 副本遇主端更新版本检查点结束标记：拍本地一次检查点，序次在
          // 重放模糊区缓冲条目之前（C# AofProcessor.cs:301-319 检查点在
          // ProcessFuzzyRegionOperations 之前）——非多回放形态
          // !usingShardedLog 直接 BlockingWait(TakeCheckpointAsync)，
          // 多回放形态 ProcessSynchronizedOperation(CHECKPOINT) 让 Leader
          // 独占拍；两形态由 synchronized_under_barrier 内 process_synchronized
          // _operation_async 单一入口按 multi_log_enabled 分派。
          // 判定复用 record_gate::is_new_version_record 单点（header.store_version
          // > 当前版本，与 C# :302 逐字对齐；钩子未注入 = 恢复/单机重放臂，
          // 维持原语义，不新增打点面），副本截断点由内核经
          // on_checkpoint_initiated / add_new_checkpoint_entry 既有单机制承接
          // 驱动面须在纪元保护区外（wcpr 检查点入口 ensure_epoch_unprotected 以
          // CheckpointWhileEpochProtected fail-fast：重放循环自带批会话纪元守卫，
          // 自钉使内核排空屏障永假），故在本条记录处（两记录之间、上一条会话
          // 操作已完整落库）挂起自持保护、拍完由守卫 Drop 按原深度重入，对标
          // C# 长 I/O 的 UnsafeSuspendThread/ResumeThread 协议
          if as_replica
            && record_gate::is_new_version_record(&header, target.store.current_version())
            && let Some(hook) = self.checkpoint_hook()
          {
            let _epoch_suspend = target.session.batch.suspend_epoch();
            self
              .synchronized_under_barrier(
                virtual_sublog_idx,
                entry,
                log_address_sequence_number,
                LeaderBarrierType::Checkpoint,
                move || async move { hook().await },
              )
              .await?;
          }
          // 模糊区结束后统一重放缓冲的 (v+1) 条目
          self
            .process_fuzzy_region_operations(virtual_sublog_idx, as_replica, target)
            .await?;
          self
            .coordinator
            .clear_fuzzy_region_buffer(virtual_sublog_idx);
        }
      }
      AofEntryType::FlushAll => {
        // libs/server/AOF/AofProcessor.cs:ProcessAofRecordInternal（case FlushAll →
        // GetSynchronizedOperationParams + ProcessSynchronizedOperation
        // (LeaderBarrierType.FLUSH_DB_ALL) 栅栏内 StoreWrapper.FlushAllDatabases
        // (unsafeTruncateLog)）：全部库用户域清空。多回放拓扑下全员在条目序列号
        // 对齐后由 Leader 独占清空，杜绝其它任务在途的 FLUSH 前记录清空后落库
        // 复活被清数据；虚拟最大序列号推进随栅栏尾步生效（FLUSH 条目无 key，
        // 走不到 keyed 记录的时间戳推进面）。非多回放形态由栅栏入口直执行 +
        // 推进，保持原单任务语义。unsafeTruncateLog flag 经回放臂消费，
        // 截断单点 store.truncate()，受 delete_floor 钳制。
        // 登记表全域回收联动（换号后旧域条目在新域不可达，回收即清库；
        // 主端 flush_all_databases 同臂，域值与广播条目同源）
        if let Some(vm) = self.append_only_file.vector_manager() {
          vm.reclaim_registry_domain(RegistryReclaim::All).await;
        }
        // 纪元让渡（对标检查点臂同款 suspend_epoch 单机制）：FLUSH 独占段
        // （flush_all_databases 的 shift 链含排空屏障）在栅栏内执行时，其余
        // 参与者正钉在其批会话保护区内的栅栏等待上——不让渡即排空互锁
        // （我等他放栏、他等我退区），恢复冻结。让渡须覆盖整段栅栏跨度
        // （先让渡再签到），检查点臂同款时序
        let _epoch_suspend = target.session.batch.suspend_epoch();
        let unsafe_truncate = header.unsafe_truncate_log();
        let store = Arc::clone(&target.store);
        self
          .synchronized_under_barrier(
            virtual_sublog_idx,
            entry,
            log_address_sequence_number,
            LeaderBarrierType::FlushDbAll,
            move || async move {
              store.flush_all_databases().await?;
              if unsafe_truncate {
                store.truncate().await?;
              }
              Ok(())
            },
          )
          .await?;
      }
      AofEntryType::FlushDb => {
        // libs/server/AOF/AofProcessor.cs:ProcessAofRecordInternal（case FlushDb →
        // ProcessSynchronizedOperation(LeaderBarrierType.FLUSH_DB) 栅栏）：
        // 仅清条目域指定库，其它库数据不受影响；栅栏与序列号推进同 FlushAll 支。
        // 域载荷 (vns, 换号前旧 vdb) 取条目本身（与数据条目物理键前缀同域），
        // 绝不取重放会话当前上下文——多租户共享单 AOF 下会话语域随上一条数据
        // 条目漂移，误读必错域清库（C# 对应 databaseId 1 字节，rust u64 全宽
        // 无截断）。换号映射与旧域判死已由先行的 DbMeta 镜像条目应用
        //（主库 commit_swap 落盘先于本条目入队，副本绝不在回放面本地取号
        // 换格——本地二次映射即主从分叉），条目臂只余栅栏对齐与登记表回收。
        // unsafeTruncateLog flag 经回放臂消费，截断单点 store.truncate()，
        // 受 delete_floor 钳制
        let (vns, old_vdb) = parse_flush_domain(entry)?;
        // 登记表域回收联动（载荷 (vns, 换号前旧 vdb) 即死亡域）
        if let Some(vm) = self.append_only_file.vector_manager() {
          vm.reclaim_registry_domain(RegistryReclaim::Database {
            vns,
            vdb: old_vdb,
            slot: None,
          })
          .await;
        }
        // 纪元让渡：与 FlushAll 臂同款（栅栏独占段先让渡本会话纪元，防排空互锁）
        let _epoch_suspend = target.session.batch.suspend_epoch();
        let unsafe_truncate = header.unsafe_truncate_log();
        let store = Arc::clone(&target.store);
        self
          .synchronized_under_barrier(
            virtual_sublog_idx,
            entry,
            log_address_sequence_number,
            LeaderBarrierType::FlushDb,
            move || async move {
              store.retire_dead_domain(vns, old_vdb);
              if unsafe_truncate {
                store.truncate().await?;
              }
              Ok(())
            },
          )
          .await?;
      }
      AofEntryType::FlushNs => {
        // rust 多租户扩展（C# 无此形态）：整命名空间虚拟换号清库，域值取
        // 条目载荷 (旧 vns, 0)，防多租户错域清库。与 FlushDb 同族局部清空，
        // 复用其栅栏类别接同一跨回放任务同步（不另造 C# 没有的新类别）；
        // 换号由先行 DbMeta 镜像条目承接（零本地换号取号），条目臂对齐栅栏、
        // 回收登记表并兜底本地判死旧空间。
        // unsafeTruncateLog flag 经回放臂消费，截断单点 store.truncate()，
        // 受 delete_floor 钳制
        let (old_vns, _) = parse_flush_domain(entry)?;
        // 登记表域回收联动（载荷 vns 即换号前旧命名空间域）
        if let Some(vm) = self.append_only_file.vector_manager() {
          vm.reclaim_registry_domain(RegistryReclaim::Namespace { vns: old_vns })
            .await;
        }
        // 纪元让渡：与 FlushAll 臂同款（栅栏独占段先让渡本会话纪元，防排空互锁）
        let _epoch_suspend = target.session.batch.suspend_epoch();
        let unsafe_truncate = header.unsafe_truncate_log();
        let store = Arc::clone(&target.store);
        self
          .synchronized_under_barrier(
            virtual_sublog_idx,
            entry,
            log_address_sequence_number,
            LeaderBarrierType::FlushDb,
            move || async move {
              store.retire_dead_namespace(old_vns);
              if unsafe_truncate {
                store.truncate().await?;
              }
              Ok(())
            },
          )
          .await?;
      }
      AofEntryType::StoredProcedure => {
        // 存储过程重放（C# ReplayStoredProc）：经注册表工厂重建过程实例，
        // 走 wtxn 事务三段式落库（过程存储视图包回放落点存储会话）
        self
          .replay_stored_proc(
            virtual_sublog_idx,
            entry,
            log_address_sequence_number,
            target,
          )
          .await?;
      }
      // 事务提交无本地分支：TxnCommit 已在函数头部
      // add_or_replay_transaction_operation 闭环消化——非模糊区经
      // TxnAction::Commit 立即整组重放，模糊区经 add_to_fuzzy_region_buffer
      // 登记提交标记 + txn_group_buffer 入队，清算期由
      // process_fuzzy_region_operations 识别标记出组重放（C# AofProcessor.cs:397
      // 的 TxnCommit 臂即 C# 侧不可达死支，rust 不再复刻）
      _ => {
        self
          .replay_op_dispatch(
            virtual_sublog_idx,
            header,
            entry,
            as_replica,
            log_address_sequence_number,
            target,
          )
          .await?;
      }
    }
    Ok(is_checkpoint_start)
  }

  /// libs/server/AOF/AofProcessor.cs:ProcessFuzzyRegionOperations
  /// libs/server/AOF/ReplayCoordinator/AofReplayCoordinator.cs:ProcessFuzzyRegionOperations
  ///
  /// 模糊区操作统一重放：rust 将 C# 处理器侧重放循环（AofProcessor.cs）与
  /// 协调器侧取缓冲 + 逐条分派（AofReplayCoordinator.cs，GetReplayContext +
  /// foreach fuzzyRegionOps → ReplayOpDispatch）折叠为同一体——缓冲所有面
  /// 即 [`AofReplayCoordinator::take_fuzzy_region_operations`]，分派复用
  /// [`Self::replay_op_dispatch`] / [`replay_chunk`](super::aof_processor_chunk_replay::replay_chunk)。
  ///
  /// 条目类型分支（对标 C# AddToFuzzyRegionBuffer 压入的 TxnCommit 提交标记
  /// 与 AddOrReplayTransactionOperation 模糊区支的成对入队）：缓冲条目中
  /// TxnCommit 头型是事务组提交标记（无键载荷），绝不可当常规数据条目派发
  /// （prepare_key 无键可解即判损坏崩溃），必须经
  /// [`Self::process_fuzzy_region_transaction_group`] 从 txn_group_buffer
  /// FIFO 出队整组顺序重放；as_replica 经 CheckpointEndCommit 调用处透传
  ///（对标 C# ProcessFuzzyRegionOperations(sublogIdx, storeVersion, asReplica)），
  /// 决定组重放的加锁/免锁形态。
  pub async fn process_fuzzy_region_operations<D: Device>(
    &self,
    sublog_idx: usize,
    as_replica: bool,
    target: &ReplayTarget<'_, '_, D>,
  ) -> Result<(), AofReplayError> {
    let operations = self.coordinator.take_fuzzy_region_operations(sublog_idx);
    for op in operations {
      match op {
        ReplayOperation::Record(entry) => {
          let header = AofHeader::parse(&entry).ok_or("模糊区条目头损坏")?;
          let op_type = AofEntryType::try_from(header.op_type).map_err(|_| "未知 AOF 操作类型")?;
          if op_type == AofEntryType::TxnCommit {
            // 提交标记：携带 TxnCommit 头内提交序列号，交模糊区事务组重放
            self
              .process_fuzzy_region_transaction_group(sublog_idx, &entry, as_replica, target)
              .await?;
            continue;
          }
          // 模糊区缓冲条目入队时已通过新代版本判定，快照完成后清算无条件落库
          //（与下分支 Chunk 直调 replay_chunk 对齐；若再走 replay_op_dispatch 将被
          // 刚拍完检查点推高的 store_version 误判为旧代条目丢弃）
          let prepared = self
            .prepare_key(sublog_idx, &entry, 0)
            .ok_or("AOF 条目负载损坏")?;
          self.replay_op(op_type, prepared, target).await?;
        }
        ReplayOperation::Chunk(acc) => {
          super::aof_processor_chunk_replay::replay_chunk(self, &acc, target).await?;
        }
      }
    }
    Ok(())
  }

  /// libs/server/AOF/AofProcessor.cs:ProcessFuzzyRegionTransactionGroup
  /// libs/server/AOF/ReplayCoordinator/AofReplayCoordinator.cs:ProcessFuzzyRegionTransactionGroup
  ///
  /// 模糊区事务组重放：rust 将处理器侧重放体（AofProcessor.cs）与协调器侧
  /// FIFO 出队 + ProcessTransactionGroup 转调（AofReplayCoordinator.cs）折叠
  /// 为同一体——出队即 [`AofReplayCoordinator::dequeue_txn_group`]，重放交
  /// [`Self::process_transaction_group`]。栅栏参数自 commit 条目头解析
  ///（提交序列号 + 参与者数，对标 C# ProcessTransactionGroup 直读 ptr 头），
  /// as_replica 透传消除单机恢复免锁 / 副本加锁两形态的硬编码分叉。
  pub async fn process_fuzzy_region_transaction_group<D: Device>(
    &self,
    sublog_idx: usize,
    commit_entry: &[u8],
    as_replica: bool,
    target: &ReplayTarget<'_, '_, D>,
  ) -> Result<(), AofReplayError> {
    let Some(group) = self.coordinator.dequeue_txn_group(sublog_idx) else {
      return Ok(());
    };
    let (sequence_number, participant_count) =
      record_gate::get_synchronized_operation_params(self.replay_task_count(), commit_entry, 0)
        .unwrap_or((group.start_sequence_number, group.participant_count as i16));
    self
      .process_transaction_group(
        sublog_idx,
        sequence_number,
        participant_count,
        &group,
        as_replica,
        target,
      )
      .await?;
    Ok(())
  }

  /// libs/server/AOF/ReplayCoordinator/AofReplayCoordinator.cs:ProcessTransactionGroup
  ///
  /// 事务组同步回放（归组键自本票修复后数据条目与标记同 session_id，组非空）：
  /// - 崩溃恢复（非副本）或单日志拓扑直接顺序重放：恢复期读不会暴露局部中
  ///   间态事务，无需加锁；子日志写入顺序已在入队时确定，无需跨流同步栅栏
  ///   （多日志串行恢复下跨分片组若等栅栏，其余参与者所在子日志的回放驱动
  ///   被当前 await 永久阻塞，确定性死锁）；
  /// - 副本多日志拓扑经 Acquire/Release 序列号栅栏保证跨子日志组提交全序，
  ///   但不持 C# 逐键锁集（SaveTransactionGroupKeysToLock+Run/Commit）——组内
  ///   操作顺序重放期间的读者中间态暴露窗口为刻意取舍，与 C# 锁集读者隔离的
  ///   差集及其后果见 doc/zh/deviations.md 第 88 条登记；
  /// - 两形态均顺序重放事务组全部操作并清理会话事务。
  pub async fn process_transaction_group<D: Device>(
    &self,
    virtual_sublog_idx: usize,
    sequence_number: i64,
    participant_count: i16,
    group: &TransactionGroup,
    as_replica: bool,
    target: &ReplayTarget<'_, '_, D>,
  ) -> Result<(), AofReplayError> {
    let session_id = group.session_id;
    if !as_replica || !self.append_only_file.multi_log_enabled() {
      let res = self
        .process_transaction_group_operations(virtual_sublog_idx, group, as_replica, target)
        .await;
      self
        .coordinator
        .clear_session_txn(virtual_sublog_idx, session_id);
      return res;
    }

    // 前置 Acquire 栅栏（TxnStart 序列号）
    self.coordinator.process_synchronized_operation(
      virtual_sublog_idx,
      group.start_sequence_number,
      participant_count,
      session_id,
      None::<fn() -> Result<(), AofReplayError>>,
    )?;

    // 重放组内操作
    let res = self
      .process_transaction_group_operations(virtual_sublog_idx, group, as_replica, target)
      .await;

    // 后置 Release 栅栏（TxnCommit 序列号）
    self.coordinator.process_synchronized_operation(
      virtual_sublog_idx,
      sequence_number,
      participant_count,
      session_id,
      None::<fn() -> Result<(), AofReplayError>>,
    )?;

    self
      .coordinator
      .clear_session_txn(virtual_sublog_idx, session_id);
    res
  }

  /// libs/server/AOF/ReplayCoordinator/AofReplayCoordinator.cs:ProcessTransactionGroupOperations
  ///
  /// 顺序重放事务组全部操作（组提交原子性由恢复免锁 / 副本锁集保障）；
  /// 组内条目失败即上抛（C# 无 catch，异常沿 Recover 传播至恢复失败）。
  pub async fn process_transaction_group_operations<D: Device>(
    &self,
    sublog_idx: usize,
    group: &TransactionGroup,
    _as_replica: bool,
    target: &ReplayTarget<'_, '_, D>,
  ) -> Result<(), AofReplayError> {
    for op in &group.operations {
      match op {
        ReplayOperation::Record(entry) => match AofHeader::parse(entry) {
          Some(header) => {
            let op_type =
              AofEntryType::try_from(header.op_type).map_err(|_| "未知 AOF 操作类型")?;
            let prepared = self
              .prepare_key(sublog_idx, entry, group.start_sequence_number)
              .ok_or("AOF 条目负载损坏")?;
            self.replay_op(op_type, prepared, target).await?;
          }
          None => return Err("模糊区条目头损坏".into()),
        },
        ReplayOperation::Chunk(acc) => {
          super::aof_processor_chunk_replay::replay_chunk(self, acc, target).await?
        }
      };
    }
    Ok(())
  }

  /// libs/server/AOF/AofProcessor.cs:ReplayOpDispatch
  ///
  /// 按拓扑选择预处理路径并分派到 [`Self::replay_op`]。
  pub async fn replay_op_dispatch<D: Device>(
    &self,
    virtual_sublog_idx: usize,
    header: AofHeader,
    entry: &[u8],
    as_replica: bool,
    log_address_sequence_number: i64,
    target: &ReplayTarget<'_, '_, D>,
  ) -> Result<(), AofReplayError> {
    let op_type = AofEntryType::try_from(header.op_type).map_err(|_| "未知 AOF 操作类型")?;
    if record_gate::should_skip_record(
      &self.coordinator,
      virtual_sublog_idx,
      entry,
      as_replica,
      target.store_version(),
      log_address_sequence_number,
      // 位点闸按条目所属物理子日志逐位取下界（虚拟下标 → 物理下标换算，
      // 公式真源 GetVirtualSublogIdx 的逆映射）
      target.aof_floor_of(virtual_sublog_idx / self.replay_task_count()),
    ) {
      return Ok(());
    }
    let prepared = self
      .prepare_key(virtual_sublog_idx, entry, log_address_sequence_number)
      .ok_or("AOF 条目负载损坏")?;
    self.replay_op(op_type, prepared, target).await
  }

  /// libs/server/AOF/AofProcessor.cs:ReplayOp
  ///
  /// 数据操作应用：按条目类型分发到主存 / 对象存 / 统一存应用面。
  /// 条目 key 为物理键，经 `KeyContextGuard` 直设条目虚拟域 `(vns, vdb)` 后
  /// 以用户键应用（drop 时恢复重放会话进入前的虚拟域，逻辑槽全程不动）。
  pub async fn replay_op<'a, D: Device>(
    &self,
    op_type: AofEntryType,
    prepared: PreparedParameters<'a>,
    target: &ReplayTarget<'_, '_, D>,
  ) -> Result<(), AofReplayError> {
    let PreparedParameters {
      key,
      key_hash: _,
      payload,
    } = prepared;
    let guard = KeyContextGuard::enter(target.session, &key)?;
    let key: &[u8] = guard.user_key;
    let tag = guard.tag;
    // DbMeta 镜像条目（0x0E）：映射体系应用单点，不落用户数据域——条目 key 为
    // 根域记录键载荷、val 为定长记录值，交 wkv 应用（doc/zh/db.md「从库完全
    // 继承主库的映射体系，不进行本地二次映射」；C# 单租户 databaseId 无此面）
    if tag == KeyTag::DbMeta {
      let vm = self.append_only_file.vector_manager();
      return replay_dbmeta(target, op_type, key, &payload, vm.as_deref()).await;
    }
    match op_type {
      AofEntryType::StoreUpsert => {
        let (value, _) = Self::split_value_input(&payload).ok_or("StoreUpsert 负载损坏")?;
        store_upsert(target.session, tag, key, value).await
      }
      AofEntryType::StoreRMW => store_rmw(self, target.session, key, &payload).await,
      AofEntryType::StoreDelete => store_delete(target.session, tag, key).await,
      AofEntryType::ObjectStoreUpsert => {
        let (value, _) = Self::split_value_input(&payload).ok_or("ObjectStoreUpsert 负载损坏")?;
        object_store_upsert(target.session, key, value).await
      }
      AofEntryType::ObjectStoreRMW => object_store_rmw(target.session, tag, key, &payload).await,
      AofEntryType::ObjectStoreDelete => object_store_delete(target.session, key).await,
      AofEntryType::UnifiedStoreStringUpsert => {
        let (value, _) =
          Self::split_value_input(&payload).ok_or("UnifiedStoreStringUpsert 负载损坏")?;
        store_upsert(target.session, KeyTag::String, key, value).await
      }
      AofEntryType::UnifiedStoreObjectUpsert => {
        let (value, _) =
          Self::split_value_input(&payload).ok_or("UnifiedStoreObjectUpsert 负载损坏")?;
        object_store_upsert(target.session, key, value).await
      }
      // C# UnifiedStoreRMW / UnifiedStoreDelete：wkv 统一面下与主存 RMW /
      // delete 同形
      AofEntryType::UnifiedStoreRMW => store_rmw(self, target.session, key, &payload).await,
      AofEntryType::UnifiedStoreDelete => store_delete(target.session, tag, key).await,
      // libs/server/AOF/AofProcessor.cs:HandleRangeIndexStreamChunk
      //（迁移索引流块重放；未注入 RI 面即按 C# 同文案失败）
      AofEntryType::RangeIndexStreamChunk => {
        let Some(ri) = &self.range_index else {
          return Err(
            "RangeIndexPreview disabled; Replay failed"
              .to_string()
              .into(),
          );
        };
        let input = ReplayInput::deserialize(&payload).ok_or("StreamChunk input 损坏")?;
        ri.handle_range_index_stream_replay(&target.session.batch, key, &input)
          .await
          .map_err(AofReplayError::from)
      }
      _ => Err(format!("Unknown AOF header operation type {op_type:?}").into()),
    }
  }

  /// libs/server/AOF/AofProcessor.cs:Dispose
  ///
  /// 释放回放执行器并清理未完成的范围索引流重组状态
  pub fn dispose(&self) {
    if let Some(ri) = &self.range_index {
      ri.dispose_incomplete_stream_reassembly();
    }
  }
}

impl Drop for AofProcessor {
  fn drop(&mut self) {
    self.dispose();
  }
}

/// FLUSH 族条目域载荷长度（[ns: u64 LE][db: u64 LE]）
const FLUSH_DOMAIN_LEN: usize = 16;

/// FLUSH 族条目域载荷解析（enqueue_safe_flush_aof 的对称读端）
///
/// 完整头之后 16B [ns: u64 LE][db: u64 LE]；载荷缺失或截断为损坏条目显式失败
///（恢复路径显式暴露原则，绝不静默按零域清库）。
///
/// 头尺寸经 waof 头面唯一口径 [`AofHeader::skip_header`] 按头型定长，本函数不
/// 自带第二套帧格式：FLUSH 广播条目在写侧 [`GarnetLog::enqueue_broadcast_entry`]
/// 按拓扑改写头型（对标 C# GarnetLog.cs:1196-1225——单物理日志 + 多重放任务落
/// SingleLogTransactionHeader、分片拓扑落 ShardedLogTransactionHeader，仅单日志
/// 拓扑保持 BasicHeader），载荷起点随头型移动；写侧 C# 把库号放在头字段
/// `databaseId`（GarnetLog.cs:1263 EnqueueSafeFlushAOF）故与头型无关，rust 以
/// 载荷承载 ns/db 全宽，读端必须按头型定位载荷，否则多重放/分片拓扑下清错域。
pub fn parse_flush_domain(entry: &[u8]) -> Result<(u64, u64), AofReplayError> {
  let at = AofHeader::skip_header(entry).ok_or("FLUSH 条目头损坏")?;
  let tail = entry
    .get(at..at + FLUSH_DOMAIN_LEN)
    .ok_or("FLUSH 条目域载荷缺失")?;
  let ns: [u8; 8] = tail[0..8].try_into().map_err(|_| "FLUSH 条目域载荷损坏")?;
  let db: [u8; 8] = tail[8..16].try_into().map_err(|_| "FLUSH 条目域载荷损坏")?;
  Ok((u64::from_le_bytes(ns), u64::from_le_bytes(db)))
}

/// 物理键 context 切换守卫（构造时解出 `(vns, vdb, 标签, 用户键)` 并**直设**
/// 会话虚拟域**与逻辑域**，drop 时复原进入前的虚拟域与逻辑域）。
///
/// 条目 key 统一为 wkv 物理键（`[NsVarint][DbVarint][KeyTag][用户键]`，
/// 等价 C# 单库 AOF 的用户键 + databaseId 组合，且跨 ns/db 域无损）；前缀两
/// 段是入账会话当时的**虚拟号**（`StoreSession::virtual_domain` 单点，与记录
/// 落域同值），故重放应用面只经 [`StoreSession::set_virtual_context`] 直设回
/// 条目原域——零虚号分配、零 DbMeta 落盘（doc/zh/db.md「从库完全继承主库的
/// 映射体系，不进行本地二次映射」）。C# 对位是
/// `AofProcessor.cs:SwitchActiveDatabaseContext` 按子日志切到**既有**库实例，
/// 从不重解析域号：本守卫取相同口径（切域即指物理解析完成后的域）。
///
/// 逻辑槽 `namespace`/`active_db` 随本守卫一并直设：版本轨种子=逻辑域
/// （wkv `bump_watch_version` 取 `session_logical_prefix` 单点），回放条目
/// 的写须落条目**逻辑域**槽位方能与副本在途 WATCH 的登记核验同槽——逻辑域
/// 于 enter 期经 `VirtualDbManager::version_domain_of` 对条目物理域做**一次**
/// 映射换算并固着进会话逻辑槽（set 期换算、bump 期零反查，禁逐次热路径
/// 反查），映射缺席（旧代死域条目）回条目物理号作孤域替身（该代内容已随
/// 换号销毁，至多数值重合碰撞面、只多 abort 不少 abort，属安全侧）。
pub(crate) struct KeyContextGuard<'a, D: Device> {
  batch: &'a wkv::BatchStoreSession<'a, D>,
  /// 进入前的会话虚拟域（drop 侧对称复原）
  prev: (u64, u64),
  /// 进入前的会话逻辑域（drop 侧对称复原，版本轨槽随逻辑槽回位）
  prev_logic: (u64, u64),
  /// 条目物理键标签（回放应用面据此选择值域：String / ObjectEnvelope / Acl）
  pub(crate) tag: wval::KeyTag,
  /// 用户键（物理键剥前缀）
  pub(crate) user_key: &'a [u8],
}

impl<'a, D: Device> KeyContextGuard<'a, D> {
  pub(crate) fn enter(
    session: &'a StorageSession<'_, D>,
    key: &'a [u8],
  ) -> Result<Self, AofReplayError> {
    let (vns, vdb, tag, user_key) =
      NamespaceDbCodec::decode_tagged_key(key).map_err(|e| format!("AOF 条目物理键损坏: {e}"))?;
    let batch = &session.batch;
    let prev = batch.virtual_domain();
    let prev_logic = (batch.namespace(), batch.active_db());
    let (lns, ldb) = batch.store().vdb.version_domain_of(vns, vdb);
    batch.set_virtual_context(vns, vdb, lns, ldb);
    Ok(Self {
      batch,
      prev,
      prev_logic,
      tag,
      user_key,
    })
  }
}

impl<D: Device> Drop for KeyContextGuard<'_, D> {
  fn drop(&mut self) {
    self.batch.set_virtual_context(
      self.prev.0,
      self.prev.1,
      self.prev_logic.0,
      self.prev_logic.1,
    );
  }
}
