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
use wbase::{convert::expire_at_milliseconds_to_ticks, hash_slot::slot_of};
use wdev::Device;
use wkv::{DbMetaRecord, WedbStore};
use wresp::command::RespCommand;
use wval::{KeyTag, NO_ETAG, NamespaceDbCodec};

use super::{
  aof_processor_object_replay::{
    object_store_delete, object_store_rmw, object_store_upsert, unified_store_string_upsert,
  },
  garnet_append_only_file::GarnetAppendOnlyFile,
  garnet_log::GarnetLog,
  readconsistency::{
    custom_procedure_key_hash_collection::CustomProcedureKeyHashCollection,
    read_consistency_manager::ReadConsistencyManager,
  },
  record_gate,
  replay_input::ReplayInput,
  replaycoordinator::{
    aof_replay_context::{ReplayOperation, TransactionGroup},
    aof_replay_coordinator::{AofReplayCoordinator, LeaderBarrierType},
    stored_proc_replay::{StoredProcRegistryReplayer, stored_proc_args, stored_proc_payload},
  },
};
use crate::{
  rangeindex::range_index_manager_replication::RangeIndexManagerReplication,
  resp::vector::vector_manager::{RECORD_TYPE, RegistryReclaim, VSETINDEX_APPEND_LOG_ARG},
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

/// 预处理产出：key、key 哈希与负载起点（C# PreparedParameters）。
pub struct PreparedParameters<'a> {
  /// key 字节。
  pub key: Cow<'a, [u8]>,
  /// key 哈希（GarnetLog::HASH）。
  pub key_hash: i64,
  /// 负载（key 之后的首个组件起点）。
  pub payload: Cow<'a, [u8]>,
}

/// 重放落点：存储会话 + 存储句柄（FLUSH 面）+ 当前存储版本。
pub struct ReplayTarget<'a, 'b, D: Device> {
  /// 存储会话（读写应用面）。
  pub session: &'b StorageSession<'a, D>,
  /// 存储句柄（FLUSH ALL/DB 应用面）。
  pub store: Arc<WedbStore<D>>,
  /// 当前存储版本（SkipRecord 版本判定；C# storeWrapper.store.CurrentVersion）。
  pub store_version: i64,
}

impl<'a, 'b, D: Device> ReplayTarget<'a, 'b, D> {
  /// 装配重放落点（版本取自存储当前值，装配点统一经此构造）。
  pub fn new(session: &'b StorageSession<'a, D>, store: &Arc<WedbStore<D>>) -> Self {
    Self {
      session,
      store: Arc::clone(store),
      store_version: store.current_version(),
    }
  }
}

/// 副本本地检查点钩子：类型擦除的无参异步动作，产出 [`wkv::Result`]。
///
/// 对标 C# 检查点结束臂 `storeWrapper.TakeCheckpointAsync`（AofProcessor.cs:302-317）
/// 下达面的 rust 注入形态。future 用 [`LocalBoxFuture`]（非 `Send`）：本仓 compio
/// 运行时线程本地驱动，重放钩子仅由背景重放线程同线程 await，与复制装配只加
/// `'static` 不加 `Send` 的既有约定同源；闭包对象只需 `Send + Sync`（宿主仅捕获
/// `Arc` 句柄），方可存入跨线程移动的 [`AofProcessor`]。
pub type ReplicaCheckpointHook =
  dyn Fn() -> LocalBoxFuture<'static, wkv::Result<()>> + Send + Sync;

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
  /// 存储过程重放执行面（C# 经 replayContext.respServerSession 承接；
  /// rust 回放驱动经注册面装配，未注入为 None）。
  stored_proc_replayer: RwLock<Option<Arc<StoredProcRegistryReplayer>>>,
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
    );
    if let Some(manager) = append_only_file.read_consistency_manager() {
      coordinator.set_consistency_manager(manager);
    }
    Self {
      append_only_file,
      coordinator,
      active_db_id: AtomicI64::new(0),
      range_index: None,
      stored_proc_replayer: RwLock::new(None),
      checkpoint_hook: RwLock::new(None),
    }
  }

  /// 注入存储过程重放执行面（C# 由 storeWrapper.customCommandManager 经
  /// 回放会话承接；rust 恢复/复制驱动经注册面装配）。
  pub fn set_stored_proc_replayer(&mut self, replayer: Arc<StoredProcRegistryReplayer>) {
    *self.stored_proc_replayer.write() = Some(replayer);
  }

  /// 存储过程重放执行面快照。
  pub fn stored_proc_replayer(&self) -> Option<Arc<StoredProcRegistryReplayer>> {
    self.stored_proc_replayer.read().clone()
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

  /// 拓扑预处理（C# IPreprocessKey.PrepareKey 三实现的折叠）：
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
    let (len_bytes, after_len) = payload.split_first_chunk::<4>()?;
    let key_len = u32::from_le_bytes(*len_bytes) as usize;
    if after_len.len() < key_len {
      return None;
    }
    let (key, rest) = after_len.split_at(key_len);
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
  fn split_value_input(payload: &[u8]) -> Option<(&[u8], &[u8])> {
    let (len_bytes, after_len) = payload.split_first_chunk::<4>()?;
    let len = u32::from_le_bytes(*len_bytes) as usize;
    if after_len.len() < len {
      return None;
    }
    Some(after_len.split_at(len))
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
    virtual_sublog_idx: usize,
    entry: &[u8],
    log_address_sequence_number: i64,
    target: &ReplayTarget<'_, '_, D>,
  ) -> Result<(), AofReplayError> {
    let Some(replayer) = self.stored_proc_replayer() else {
      return Err(
        "AOF 存储过程条目回放未装配执行面（custom 注册表未注入）"
          .to_string()
          .into(),
      );
    };
    let header = AofHeader::parse(entry).ok_or("存储过程条目头损坏")?;
    let proc_id = header.procedure_id;
    let session_id = header.session_id;
    let args =
      stored_proc_args::decode(stored_proc_payload(entry)?).ok_or("存储过程输入载荷损坏")?;

    if !self.append_only_file.multi_log_enabled() {
      let mut hashes = Vec::new();
      return replayer.replay(proc_id, session_id, &args, &mut hashes, target);
    }

    let Some((sequence_number, participant_count)) = record_gate::get_synchronized_operation_params(
      self.replay_task_count(),
      entry,
      log_address_sequence_number,
    ) else {
      return Err("存储过程条目缺少同步操作参数".into());
    };

    self.coordinator.process_synchronized_operation(
      virtual_sublog_idx,
      sequence_number,
      participant_count,
      session_id,
      Some(|| {
        self.run_stored_proc_with_tracker(
          &replayer,
          proc_id,
          session_id,
          &args,
          sequence_number,
          target,
        )
      }),
    )?;
    Ok(())
  }

  /// libs/server/AOF/ReplayCoordinator/AofReplayCoordinator.cs:StoredProcRunnerWrapper
  /// libs/server/AOF/ReplayCoordinator/AofReplayCoordinator.cs:StoredProcRunnerBase
  ///
  /// 多日志存储过程重放（C# StoredProcRunnerWrapper + StoredProcRunnerBase
  /// 合流）：收集器登记过程触达键哈希并推进读一致性时间戳。
  fn run_stored_proc_with_tracker<D: Device>(
    &self,
    replayer: &StoredProcRegistryReplayer,
    proc_id: u8,
    session_id: i32,
    args: &[Vec<u8>],
    sequence_number: i64,
    target: &ReplayTarget<'_, '_, D>,
  ) -> Result<(), AofReplayError> {
    let mut tracker = CustomProcedureKeyHashCollection::new(&self.append_only_file);
    let mut hashes = Vec::new();
    let result = replayer.replay(proc_id, session_id, args, &mut hashes, target);
    if result.is_ok() && !hashes.is_empty() {
      tracker.extend(hashes);
      tracker.update_sequence_number(sequence_number);
    }
    result
  }

  fn replay_task_count(&self) -> usize {
    self.append_only_file.virtual_sublog_count() / self.append_only_file.log().size().max(1)
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
    // commit 元数据帧（waof 提交边界标记）非 AOF 条目，重放面跳过；
    // 复制推流面不过滤——帧随流保真传输，从侧恢复得以同样收敛
    if waof::is_commit_frame(entry) {
      return Ok(false);
    }

    // 顺序布局：存在进行中分块记录时，本条目必为纯数据续块
    let in_progress_or_completed = {
      let mut ctx = self.coordinator.context(virtual_sublog_idx);
      if ctx.has_in_progress_chunk() {
        Some(ctx.chunked_reader.read_chunk(entry))
      } else {
        None
      }
    };
    if let Some(completed) = in_progress_or_completed {
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

    let header = AofHeader::parse(entry).ok_or("AOF 条目头损坏")?;
    if header.aof_header_version > AofHeader::MAX_SUPPORTED_AOF_HEADER_VERSION {
      return Err(
        format!(
          "Unsupported AOF header version {}; this build supports up to version {}",
          header.aof_header_version,
          AofHeader::MAX_SUPPORTED_AOF_HEADER_VERSION
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
      super::replaycoordinator::aof_replay_coordinator::TxnAction::None => {}
    }

    let mut is_checkpoint_start = false;
    match op_type {
      AofEntryType::CheckpointStartCommit => {
        is_checkpoint_start = true;
        if header.aof_header_version > 1 {
          if self
            .coordinator
            .context(virtual_sublog_idx)
            .in_fuzzy_region()
          {
            self
              .coordinator
              .clear_fuzzy_region_buffer(virtual_sublog_idx);
          }
          self
            .coordinator
            .context(virtual_sublog_idx)
            .set_in_fuzzy_region(true);
        }
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
        if header.aof_header_version > 1 {
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
            if as_replica
              && record_gate::is_new_version_record(&header, target.store.current_version())
              && let Some(hook) = self.checkpoint_hook()
            {
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
              .process_fuzzy_region_operations(virtual_sublog_idx, target)
              .await?;
            self
              .coordinator
              .clear_fuzzy_region_buffer(virtual_sublog_idx);
          }
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
        // 推进，保持原单任务语义。unsafeTruncateLog 直传底层 Flush（C# 无告警
        // 分支，安全截断为正常形态；rust wkv 层物理截断恒含该语义，头 flag
        // 保真不另施为）
        // 登记表全域回收联动（换号后旧域条目在新域不可达，回收即清库；
        // 主端 flush_all_databases 同臂，域值与广播条目同源）
        if let Some(vm) = self.append_only_file.vector_manager() {
          vm.reclaim_registry_domain(RegistryReclaim::All);
        }
        let store = Arc::clone(&target.store);
        self
          .synchronized_under_barrier(
            virtual_sublog_idx,
            entry,
            log_address_sequence_number,
            LeaderBarrierType::FlushDbAll,
            move || async move { store.flush_all_databases().await },
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
        // 换格——本地二次映射即主从分叉），条目臂只余栅栏对齐与登记表回收
        let (vns, old_vdb) = parse_flush_domain(entry)?;
        // 登记表域回收联动（载荷 (vns, 换号前旧 vdb) 即死亡域）
        if let Some(vm) = self.append_only_file.vector_manager() {
          vm.reclaim_registry_domain(RegistryReclaim::Database { vns, vdb: old_vdb });
        }
        let store = Arc::clone(&target.store);
        self
          .synchronized_under_barrier(
            virtual_sublog_idx,
            entry,
            log_address_sequence_number,
            LeaderBarrierType::FlushDb,
            move || async move {
              store.retire_dead_domain(vns, old_vdb);
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
        // 回收登记表并兜底本地判死旧空间
        let (old_vns, _) = parse_flush_domain(entry)?;
        // 登记表域回收联动（载荷 vns 即换号前旧命名空间域）
        if let Some(vm) = self.append_only_file.vector_manager() {
          vm.reclaim_registry_domain(RegistryReclaim::Namespace { vns: old_vns });
        }
        let store = Arc::clone(&target.store);
        self
          .synchronized_under_barrier(
            virtual_sublog_idx,
            entry,
            log_address_sequence_number,
            LeaderBarrierType::FlushDb,
            move || async move {
              store.retire_dead_namespace(old_vns);
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
      AofEntryType::TxnCommit => {
        // 模糊区事务组重放（FIFO）
        self
          .process_fuzzy_region_transaction_group(virtual_sublog_idx, target)
          .await?;
      }
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
  ///
  /// 模糊区操作统一重放（C# ProcessFuzzyRegionOperations 的处理器侧）。
  pub async fn process_fuzzy_region_operations<D: Device>(
    &self,
    sublog_idx: usize,
    target: &ReplayTarget<'_, '_, D>,
  ) -> Result<(), AofReplayError> {
    let operations = self.coordinator.take_fuzzy_region_operations(sublog_idx);
    for op in operations {
      match op {
        ReplayOperation::Record(entry) => {
          let header = AofHeader::parse(&entry).ok_or("模糊区条目头损坏")?;
          self
            .replay_op_dispatch(sublog_idx, header, &entry, true, 0, target)
            .await?;
        }
        ReplayOperation::Chunk(acc) => {
          super::aof_processor_chunk_replay::replay_chunk(self, &acc, target).await?;
        }
      }
    }
    Ok(())
  }

  /// libs/server/AOF/AofProcessor.cs:ProcessFuzzyRegionTransactionGroup
  ///
  /// 模糊区事务组重放（C# ProcessFuzzyRegionTransactionGroup）。
  pub async fn process_fuzzy_region_transaction_group<D: Device>(
    &self,
    sublog_idx: usize,
    target: &ReplayTarget<'_, '_, D>,
  ) -> Result<(), AofReplayError> {
    let Some(group) = self.coordinator.dequeue_txn_group(sublog_idx) else {
      return Ok(());
    };
    self
      .process_transaction_group(
        sublog_idx,
        group.start_sequence_number,
        group.participant_count as i16,
        &group,
        true,
        target,
      )
      .await?;
    Ok(())
  }

  /// libs/server/AOF/ReplayCoordinator/AofReplayCoordinator.cs:ProcessTransactionGroup
  ///
  /// 事务组同步回放：
  /// - 多日志拓扑下经 Acquire/Release 栅栏与条带锁协调；
  /// - 顺序重放事务组全部操作并清理会话事务。
  pub async fn process_transaction_group<D: Device>(
    &self,
    virtual_sublog_idx: usize,
    sequence_number: i64,
    participant_count: i16,
    group: &TransactionGroup,
    as_replica: bool,
    target: &ReplayTarget<'_, '_, D>,
  ) -> Result<(), AofReplayError> {
    if !self.append_only_file.multi_log_enabled() {
      return self
        .process_transaction_group_operations(virtual_sublog_idx, group, as_replica, target)
        .await;
    }

    let session_id = group.session_id;
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
    as_replica: bool,
    target: &ReplayTarget<'_, '_, D>,
  ) -> Result<(), AofReplayError> {
    for op in &group.operations {
      match op {
        ReplayOperation::Record(entry) => match AofHeader::parse(entry) {
          Some(header) => {
            self
              .replay_op_dispatch(
                sublog_idx,
                header,
                entry,
                as_replica,
                group.start_sequence_number,
                target,
              )
              .await?
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
      target.store_version,
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
      return Self::replay_dbmeta(target, op_type, key, &payload).await;
    }
    match op_type {
      AofEntryType::StoreUpsert => {
        let (value, _) = Self::split_value_input(&payload).ok_or("StoreUpsert 负载损坏")?;
        Self::store_upsert(target.session, tag, key, value).await
      }
      AofEntryType::StoreRMW => self.store_rmw(target.session, key, &payload).await,
      AofEntryType::StoreDelete => Self::store_delete(target.session, tag, key).await,
      AofEntryType::ObjectStoreUpsert => {
        let (value, _) = Self::split_value_input(&payload).ok_or("ObjectStoreUpsert 负载损坏")?;
        object_store_upsert(target.session, key, value).await
      }
      AofEntryType::ObjectStoreRMW => object_store_rmw(target.session, key, &payload).await,
      AofEntryType::ObjectStoreDelete => object_store_delete(target.session, key).await,
      AofEntryType::UnifiedStoreStringUpsert => {
        let (value, _) =
          Self::split_value_input(&payload).ok_or("UnifiedStoreStringUpsert 负载损坏")?;
        unified_store_string_upsert(target.session, key, value).await
      }
      AofEntryType::UnifiedStoreObjectUpsert => {
        let (value, _) =
          Self::split_value_input(&payload).ok_or("UnifiedStoreObjectUpsert 负载损坏")?;
        object_store_upsert(target.session, key, value).await
      }
      // C# UnifiedStoreRMW / UnifiedStoreDelete：wkv 统一面下与主存 RMW /
      // delete 同形
      AofEntryType::UnifiedStoreRMW => self.store_rmw(target.session, key, &payload).await,
      AofEntryType::UnifiedStoreDelete => Self::store_delete(target.session, tag, key).await,
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
          .map_err(|e| AofReplayError::from(format!("RangeIndexStreamChunk replay failed: {e}")))
      }
      _ => Err(format!("Unknown AOF header operation type {op_type:?}").into()),
    }
  }

  /// DbMeta 镜像条目回放应用（主从映射体系同步的唯一通道）
  ///
  /// 主库全部 DbMeta 落盘经存储事件镜像为 StoreUpsert / StoreDelete 条目
  ///（service.rs:on_aof_store_event 放行 KeyTag::DbMeta）：upsert 复原完整
  /// 记录交 [`WedbStore::apply_dbmeta_record`]（映射装载 + 水位抬升 + 判死
  /// 联动回收 + 本节点落盘）；墓碑删除只需键自足信息，交
  /// [`WedbStore::apply_dbmeta_tombstone`] 注销。条目先于同事务的 FlushDb /
  /// FlushNs 广播条目入队（落盘先于条目入队），回放按序到达即映射已就位。
  /// 回放全程 pause_aof_listeners，应用落盘不再镜像，无自激放大
  async fn replay_dbmeta<D: Device>(
    target: &ReplayTarget<'_, '_, D>,
    op_type: AofEntryType,
    key: &[u8],
    payload: &[u8],
  ) -> Result<(), AofReplayError> {
    match op_type {
      AofEntryType::StoreUpsert => {
        let (value, _) = Self::split_value_input(payload).ok_or("StoreUpsert 负载损坏")?;
        let rec = DbMetaRecord::decode(key, value)
          .ok_or("DbMeta 镜像条目记录损坏（布局错位或未知子类型）")?;
        target
          .store
          .apply_dbmeta_record(rec)
          .await
          .map_err(AofReplayError::Store)
      }
      AofEntryType::StoreDelete => target
        .store
        .apply_dbmeta_tombstone(key)
        .await
        .map_err(AofReplayError::Store),
      // 写侧镜像端口只产这两种形态，其余条目类型携 DbMeta 键即损坏流
      _ => Err(format!("DbMeta 条目非法操作类型 {op_type:?}").into()),
    }
  }

  /// libs/server/AOF/AofProcessor.cs:StoreUpsert
  ///
  /// 条目物理键标签决定值域：ACL 旁路标签（0x0D）走标签直写（String 域
  /// SET 语义会清 TTL/信封，误伤旁路记录）；其余仍为字符串域 upsert
  pub async fn store_upsert<D: Device>(
    session: &StorageSession<'_, D>,
    tag: wval::KeyTag,
    key: &[u8],
    value: &[u8],
  ) -> Result<(), AofReplayError> {
    // 条件写形态（EX/NX 等经 StoreRMW 路径回放）；upsert 直写
    if tag == KeyTag::Acl {
      return session
        .upsert_tag(key, KeyTag::Acl, value)
        .await
        .map_err(AofReplayError::Store);
    }
    session
      .upsert_string(key, value)
      .await
      .map_err(AofReplayError::Store)
  }

  /// libs/server/AOF/AofProcessor.cs:StoreRMW
  ///
  /// RMW 命令重放：RI 族交范围索引重放面实际执行（C#
  /// RangeIndexManager.HandleRangeIndex*Replay）；DELIFEXPIM 为 TTL 物理清除
  /// 确定性单条目（C# ExpireAndResume + Expired|Deterministic 标志的回放面）；
  /// 键管理族只保留写侧真实产出的 Pexpireat / Persist 两形态，其余命令形态
  /// 写侧不可产，按 C# MainStore/RMWMethods default 尾部口径显式失败。
  pub async fn store_rmw<D: Device>(
    &self,
    session: &StorageSession<'_, D>,
    key: &[u8],
    input: &[u8],
  ) -> Result<(), AofReplayError> {
    let input = ReplayInput::deserialize(input).ok_or("StoreRMW input 损坏")?;
    // 向量族 RMW 子分派（VADD/VREM/VSETATTR 与 RENAME 向量哨兵条目）交向量
    // 域真实重放（对标 C# AofProcessor.StoreRMW 的 VADD/VREM/VSETATTR 分派 +
    // UnifiedStoreStringUpsert 的 RENAME+RecordType 特判：经 VectorManager
    // 重建索引/增删元素/改属性/迁移登记项；context 已由 KeyContextGuard 切至
    // 条目域）
    if matches!(
      input.cmd,
      RespCommand::Vadd | RespCommand::Vrem | RespCommand::Vsetattr
    ) || (input.cmd == RespCommand::Rename && input.arg1 == i64::from(RECORD_TYPE))
    {
      let Some(vm) = self.append_only_file.vector_manager() else {
        return Err(
          "Vector Set (preview) commands are not enabled; Replay failed"
            .to_string()
            .into(),
        );
      };
      // 库级定槽（doc/zh/db.md 4.1）：槽位口径与在线面同源——`slot_of` 取
      // **逻辑域** (namespace, active_db)。回放会话逻辑槽属本机连接面、与条目
      // 无关，守卫又绝不重解析映射（否则即从库本地二次映射），故按条目物理域
      // (vns, vdb) 经 vdb 管理面逆向表反查真逻辑域，与在线面逐值同值；
      // 登记表域随条目域收敛（session_prefix 与条目物理键前缀同域）
      let (vns, vdb) = session.batch.virtual_domain();
      let (slot_ns, slot_db) = session.batch.store().vdb.logic_domain_of(vns, vdb);
      let repl_slot = slot_of(slot_ns, slot_db);
      let repl_prefix = session.batch.session_prefix();
      let repl_prefix = repl_prefix.as_slice();
      let result = match input.cmd {
        // 迁移索引条目（arg1 哨兵区分）：按几何参补建登记表与内存索引，
        // 零元素插入（空向量集重启重建面）
        RespCommand::Vadd if input.arg1 == VSETINDEX_APPEND_LOG_ARG => {
          vm.replay_vector_set_index(repl_prefix, key, repl_slot, &input)
        }
        RespCommand::Vadd => vm.replay_vector_set_add(repl_prefix, key, repl_slot, &input),
        RespCommand::Vrem => vm.replay_vector_set_remove(repl_prefix, key, &input),
        RespCommand::Vsetattr => vm.replay_vector_set_set_attribute(repl_prefix, key, &input),
        // 条目键=新名，参数=[旧名, 新名]（C# parseState 同布局）
        RespCommand::Rename => vm.replay_vector_set_rename(repl_prefix, key, &input),
        _ => unreachable!("向量族判别已收敛"),
      };
      // 错误文案对齐 RangeIndex 分支的原格式
      return result.map_err(|e| AofReplayError::from(format!("Vector Set replay failed: {e}")));
    }
    // 范围索引族须实际执行（C# AofProcessor.StoreRMW 的 RICREATE/RISET/RIDEL
    // 分派）：context 已由 KeyContextGuard 切至条目域，直接交 RI 复制面
    if matches!(
      input.cmd,
      RespCommand::Ricreate | RespCommand::Riset | RespCommand::Ridel
    ) {
      let Some(ri) = &self.range_index else {
        return Err(
          "RangeIndexPreview disabled; Replay failed"
            .to_string()
            .into(),
        );
      };
      let result = match input.cmd {
        RespCommand::Ricreate => {
          ri.handle_range_index_create_replay(&session.batch, key, &input)
            .await
        }
        RespCommand::Riset => {
          ri.handle_range_index_set_replay(&session.batch, key, &input)
            .await
        }
        RespCommand::Ridel => {
          ri.handle_range_index_del_replay(&session.batch, key, &input)
            .await
        }
        _ => unreachable!("RI 族判别已收敛"),
      };
      // 错误文案（"RangeIndex replay failed: …"）对齐原格式
      return result.map_err(|e| AofReplayError::from(format!("RangeIndex replay failed: {e}")));
    }
    // TTL 物理清除确定性重放（C# DELIFEXPIM：Expired|Deterministic 标志下
    // CheckExpiry 恒真 → 删除）；主端已判定到期，重放端幂等执行统一 DEL
    //（先清随键 TTL 再清数据，context 已切至条目域）
    if input.cmd == RespCommand::Delifexpim {
      session
        .batch
        .delete(key)
        .await
        .map_err(|e| format!("Delifexpim replay failed: {e}"))?;
      return Ok(());
    }
    // ETag 旁路记录确定性直设/清除（wnode ETag 写监听端口入队的两跳等价
    // 条目；对标 C# RMWMethods.Etags 族 etag 内嵌记录随 RMW input 重放
    // 确定性重现——rust 侧 etag 为独立旁路记录，条目 arg1 携带主端线性化
    // 后的绝对 etag 值直设，不重算）：arg1 > NO_ETAG 直设旁路记录；
    // arg1 == NO_ETAG(0) 为清除墓碑（合法 etag 恒 >= 1，0 无歧义），物理
    // 删除旁路记录，杜绝 DEL 级联后盘上残留令恢复复活旧 etag。
    //
    // 原子性边界：C# etag 与值同记录一体；rust 拆「值记录 + etag 旁路
    // 记录」两记录，写入端 apply 序恒为「值 upsert → etag 推进」，AOF 全序
    // 保证值条目先于 etag 条目，恢复同序重放即收敛（检查点基线过滤对两
    // 记录同代生效，不产生跨代错序）
    if input.cmd == RespCommand::Setwithetag {
      if input.arg1 > NO_ETAG {
        session
          .batch
          .put_etag(key, input.arg1)
          .await
          .map_err(|e| format!("Setwithetag replay failed: {e}"))?;
      } else {
        session
          .batch
          .del_etag(key)
          .await
          .map_err(|e| format!("Setwithetag replay failed: {e}"))?;
      }
      return Ok(());
    }
    // 主存 RMW 的逐命令手写臂已整族删除（C# 无这一层：AofProcessor.cs:StoreRMW 只对
    // VADD/VREM/VSETATTR 与 RICREATE/RISET/RIDEL 特判，其余 input 直通
    // stringContext.RMW，按 cmd 的分发只存在于 MainStore/RMWMethods 算子内部）。
    // rust 侧 StoreRMW 条目的生产者全集是 service.rs:on_aof_store_event 的七个镜像臂
    //（Pexpireat/Persist/Setwithetag/Riset/Ridel/Ricreate/Delifexpim）加
    // range_index_manager_replication.rs 与 vector_manager_replication.rs 的 RI/Vector
    // 族入队点，全部已在上面各臂落位；主存字符串 RMW 的语义在命令端就地折成终值
    //（resp/basic_commands/incr.rs 的 network_increment、set.rs 的 network_append /
    // network_set_range 走 read_user_sync + try_rmw_sync），AOF 只经 wkv 物理写漏斗
    // 镜像写效果（StoreUpsert 终值 / StoreDelete 墓碑），故
    // Incr/Incrby/Decr/Decrby/Incrbyfloat/Append/Setrange 形态条目恒不可产，
    // rmw_main_store 收口同批删除。Expireat/Getdel/Setex/Psetex 四臂同批删除，
    // 口径逐条如下：
    // - Expireat：写侧唯一 TTL 事件源 TtlWrite 恒以 Pexpireat + 主端线性化绝对毫秒
    //   （或 Persist）入条目，客户端 EXPIRE/PEXPIRE/EXPIREAT/PEXPIREAT 的相对秒/
    //   毫秒与绝对时间戳在命令端即换算为物理 TTL 记录；C# AOF 亦从不存在相对时长
    //   条目形态（主端 KeyAdminCommands.cs:423-432 统一线性化为绝对 ticks 打包
    //   ExpirationWithOption.Word）
    // - Setex/Psetex：C# NetworkSETEX 在命令端把时长折成绝对 ticks 随条目下发
    //   （BasicCommands.cs:552-559 的 valMetadata = UtcNow.Ticks + 时长），rust 命令
    //   端折成「值 upsert + Pexpireat 绝对毫秒」两跳（set.rs:network_setex_impl）；
    //   被删臂却按相对时长解释 arg1（session.setex → expire_in_ticks），与写侧唯一
    //   TTL 条目源口径正好相反——条目一旦出现即按错误语义执行，相对重算叠加复制
    //   延迟使从库 TTL 系统性偏短，严禁静默执行
    // - Getdel：命令端 read_user_sync 取值 + try_delete_sync 删除
    //   （key_admin_commands/keys.rs:network_getdel），净效果由 StoreDelete 承载
    // 遇上述任一形态即条目损坏或写/放两端演化失配，一律落 default 臂显式失败
    match input.cmd {
      RespCommand::Pexpireat => {
        // 绝对 Unix 毫秒 → ticks（KeyAdminCommands.cs:426 的 PEXPIREAT，
        // 负夹 0/上界钳制单点与命令端同函数）。
        // 刻意差异声明：C# EXPIRE 族条件（NX/XX/GT/LT）经 ExpirationWithOption
        // word 低 4 位随 AOF 完整携带、副本端重评估（UnifiedStore/PrivateMethods.cs:
        // 108 WriteLogRMW 置 Deterministic 后条件语义仍在）；rust AOF 条目由
        // TtlWrite 镜像产出，仅携带主端线性化后的绝对毫秒、不携带 ExpireOption，
        // 故重放按无条件绝对过期执行（TtlOpt::NONE）——主端写事件已按条件裁决，
        // 副本端丢失条件不影响镜像一致性，但携带面弱于 C# word 编码。
        // AOF TTL 口径恒为本臂（绝对毫秒）+ Persist 双形态，Expire/Pexpire/Expireat/
        // Setex/Psetex 五臂已删（重放按接收时刻重算会叠加复制延迟致 TTL 漂移）；
        // 未来若写入端补相对形态条目，必须同步改回绝对口径后再入重放面
        session
          .expire_at_ticks(key, expire_at_milliseconds_to_ticks(input.arg1))
          .await
          .map_err(|e| format!("Pexpireat replay failed: {e}"))?;
        Ok(())
      }
      RespCommand::Persist => {
        session
          .persist_key(key)
          .await
          .map_err(|e| format!("Persist replay failed: {e}"))?;
        Ok(())
      }
      _ => {
        // C# RMW 重放面对未覆盖命令抛 GarnetException("Unsupported
        // operation on input")（MainStore/RMWMethods.cs InPlaceUpdaterWorker
        // default 尾部）——恢复显式失败，杜绝未知命令静默吞没恢复数据
        //（写入端新增 RMW 编码而重放端漏配时立即暴露）
        Err(
          format!(
            "StoreRMW replay failed: unsupported cmd {:?} (key length {})",
            input.cmd,
            key.len()
          )
          .into(),
        )
      }
    }
  }

  /// libs/server/AOF/AofProcessor.cs:StoreDelete
  ///
  /// ACL 旁路标签走标签墓碑删除；其余仍为字符串域删除
  pub async fn store_delete<D: Device>(
    session: &StorageSession<'_, D>,
    tag: wval::KeyTag,
    key: &[u8],
  ) -> Result<(), AofReplayError> {
    if tag == KeyTag::ObjectEnvelope {
      // 信封域墓碑条目：仅摘信封物理记录（store-scoped 删，对位 C#
      // ObjectStoreDelete 只触对象域），勿跑 delete_string 全键删——就地升阶臂
      // 的数据流 publish 先行入账，全键删会抹掉副本刚重建的树态元记录；DEL 族
      // 全键语义由其 String 域墓碑条目与 RangeIndexDrop 条目各自承担
      session
        .delete_tag(key, KeyTag::ObjectEnvelope)
        .await
        .map_err(|e| format!("StoreDelete replay failed: {e}"))?;
      return Ok(());
    }
    if tag == KeyTag::Acl {
      session
        .delete_tag(key, KeyTag::Acl)
        .await
        .map_err(|e| format!("StoreDelete replay failed: {e}"))?;
      return Ok(());
    }
    session
      .delete_string(key)
      .await
      .map_err(|e| format!("StoreDelete replay failed: {e}"))?;
    Ok(())
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
/// 会话虚拟域，drop 时复原进入前的虚拟域）。
///
/// 条目 key 统一为 wkv 物理键（`[NsVarint][DbVarint][KeyTag][用户键]`，
/// 等价 C# 单库 AOF 的用户键 + databaseId 组合，且跨 ns/db 域无损）；前缀两
/// 段是入账会话当时的**虚拟号**（`StoreSession::virtual_domain` 单点，与记录
/// 落域同值），故重放应用面只经 [`StoreSession::set_virtual_context`] 直设回
/// 条目原域——零逻辑解析、零虚号分配、零 DbMeta 落盘（doc/zh/db.md「从库完全
/// 继承主库的映射体系，不进行本地二次映射」）。C# 对位是
/// `AofProcessor.cs:SwitchActiveDatabaseContext` 按子日志切到**既有**库实例，
/// 从不重解析域号：本守卫取相同口径（切域即指物理解析完成后的域）。
///
/// 逻辑槽 `namespace`/`active_db` 全程不触碰：回放会话的逻辑上下文属本机连接
/// 面，条目域与之无关；需要逻辑域的槽位口径经
/// [`wkv::VirtualDbManager::logic_domain_of`] 逆向表反查真值。
pub(crate) struct KeyContextGuard<'a, D: Device> {
  batch: &'a wkv::BatchStoreSession<'a, D>,
  /// 进入前的会话虚拟域（drop 侧对称复原）
  prev: (u64, u64),
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
    batch.set_virtual_context(vns, vdb);
    Ok(Self {
      batch,
      prev,
      tag,
      user_key,
    })
  }
}

impl<D: Device> Drop for KeyContextGuard<'_, D> {
  fn drop(&mut self) {
    self.batch.set_virtual_context(self.prev.0, self.prev.1);
  }
}
