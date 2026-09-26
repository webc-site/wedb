//! Garnet 追加日志文件（对标 libs/server/AOF/GarnetAppendOnlyFile.cs:
//! GarnetAppendOnlyFile）
//!
//! C# 侧聚合 TsavoriteLog 拓扑（GarnetLog）、序列号生成器、读取一致性管理器
//! 与背压闸门；rust 侧背压与路由已由周期 1 的 [`GarnetLog`] 承载，本类型承接
//! 总尺寸 / 虚拟子日志换算 / 序列号生成与复位 / 一致性管理器代际更替。
//!（副本同步重放决策与数据丢失检查收敛于 ReplicationManager 单点）

use std::{
  io,
  sync::{Arc, OnceLock},
  time::Duration,
};

use parking_lot::RwLock;
use waof::{AofAddress, AofEntryType, SequenceNumberGenerator};
use wconf::RuntimeServerOptions;
use wdev::Device;
use wkv::Error;

use super::{
  AofProcessor,
  aof_backpressure::AofBackpressure,
  aof_processor::ReplayTarget,
  garnet_log::{AofWriteContext, GarnetLog, RecordShape, virtual_sublog_idx},
  readconsistency::read_consistency_manager::ReadConsistencyManager,
  recover::aof_recover::AofRecover,
  replay_input::{ReplayInput, ReplayInputSlice},
};
use crate::{
  database::GarnetDatabase,
  primary_tasks::PrimaryTasks,
  resp::vector::{vector_manager::VectorManager, vector_store_callbacks::ActiveVectorSessionGuard},
  storage::StorageSession,
};

/// libs/server/AOF/GarnetAppendOnlyFile.cs:GarnetAppendOnlyFile
///
/// Garnet 追加日志文件。
pub struct GarnetAppendOnlyFile {
  /// 日志拓扑（单日志 / 分片路由层）。
  log: Arc<GarnetLog>,
  /// 服务器选项子集。
  physical_sublog_count: usize,
  replay_task_count: usize,
  replay_drift_threshold: i64,
  replay_drift_check_freq: i64,
  /// 副本一致读等待超时（C# serverOptions.ReplicaSyncTimeout 投影）。
  read_timeout: Duration,
  /// 多物理日志或单物理多回放拓扑（C# MultiLogEnabled）。
  multi_log_enabled: bool,
  /// 序列号生成器（仅多物理日志模式；C# seqNumGen，与 GarnetLog 共享）。
  seq_num_gen: Option<Arc<SequenceNumberGenerator>>,
  /// 读取一致性管理器（代际更替；C# readConsistencyManager）。
  read_consistency_manager: RwLock<Option<Arc<ReadConsistencyManager>>>,
  /// 向量集合管理器（重放面；AOF 点亮装配期注入，VADD/VREM/VSETATTR
  /// 条目重放经 AofProcessor 在此取承接面）。
  vector_manager: RwLock<Option<Arc<VectorManager>>>,
  /// 副本角色位（停机收口 [`Self::dispose_async`] 角色分派用，服务装配期
  /// 注入；与 database_manager attach_primary_tasks 同款注入、同一 Arc，
  /// 勿造第二角色状态源。None = 裸构造形态恒视为主角色）
  primary_tasks: OnceLock<Arc<PrimaryTasks>>,
}

impl GarnetAppendOnlyFile {
  /// libs/server/AOF/GarnetAppendOnlyFile.cs:GarnetAppendOnlyFile
  ///
  /// 构造：绑定日志拓扑并按选项装配一致性管理器（C# 构造子的装配子集；
  /// 设备面日志设置由 GarnetLog 的后端注入承接）。
  pub fn new(
    log: Arc<GarnetLog>,
    server_options: &RuntimeServerOptions,
    seq_num_gen: Option<Arc<SequenceNumberGenerator>>,
  ) -> Self {
    let physical_sublog_count = server_options.aof_physical_sublog_count.max(1) as usize;
    let replay_task_count = server_options.aof_replay_task_count.max(1) as usize;
    // C# MultiLogEnabled = physical > 1 || replay > 1
    let multi_log_enabled = physical_sublog_count > 1 || replay_task_count > 1;
    let aof = Self {
      log: Arc::clone(&log),
      physical_sublog_count,
      replay_task_count,
      replay_drift_threshold: server_options.replay_drift_threshold,
      replay_drift_check_freq: server_options.replay_drift_check_freq,
      read_timeout: Duration::from_secs(server_options.replica_sync_timeout_secs),
      multi_log_enabled,
      // 仅多物理日志模式持有（C# 构造同款条件），并与 GarnetLog 共享
      seq_num_gen: if physical_sublog_count > 1 {
        seq_num_gen
      } else {
        None
      },
      read_consistency_manager: RwLock::new(None),
      vector_manager: RwLock::new(None),
      primary_tasks: OnceLock::new(),
    };
    if let Some(bp) = aof.log.backpressure() {
      bp.set_weak_log(Arc::downgrade(&aof.log));
    }
    // C# 构造即建一致性管理器（CreateOrUpdateKeySequenceManager）
    aof.create_or_update_key_sequence_manager();
    aof
  }

  /// libs/server/AOF/GarnetAppendOnlyFile.cs:Log
  ///
  /// 日志拓扑句柄。
  pub fn log(&self) -> &Arc<GarnetLog> {
    &self.log
  }

  /// 主库门控广播 FLUSH 条目：仅 `primary` 为真时向共享 AOF 追加安全 flush
  /// 广播条目（C# `clusterProvider.IsPrimary()` 门控 + `EnqueueSafeFlushAOF`）。
  /// 副本与嵌入式无 AOF 面为静默跳过——副本清库经回放条目承接，绝不二次
  /// 入队（防自激放大；回放面另有 pause_aof_listeners 双保险）。
  ///
  /// C# 对偶为 SafeFlushAOF 内联的「IsPrimary 门控 + Log.EnqueueSafeFlushAOF」
  /// 主库入队段，非函数级映射；SafeFlushAOF 的函数级映射唯一见
  /// SingleDatabaseManager::safe_flush_aof（其无条件 SafeTruncateAOF 截断段
  /// 由 flush_all_databases 的「先截断后入队」承接）。
  pub fn enqueue_safe_flush_aof_if_primary(
    &self,
    primary: bool,
    op_type: AofEntryType,
    unsafe_truncate_log: bool,
    ns: u64,
    db: u64,
  ) -> waof::Result<()> {
    if !primary {
      return Ok(());
    }
    self
      .log
      .enqueue_safe_flush_aof(op_type, unsafe_truncate_log, ns, db)?;
    Ok(())
  }

  /// 原始底层条目入队（统一装配 RecordShape 并写入日志，对标 C# 各 WriteLog* 共用单一 RecordShape 组装形态）
  #[inline]
  pub fn enqueue_raw(
    &self,
    op_type: AofEntryType,
    ctx: impl Into<AofWriteContext>,
    key: &[u8],
    value: &[u8],
    input: &[u8],
  ) -> waof::Result<i64> {
    let ctx = ctx.into();
    self
      .log
      .enqueue(&RecordShape::from_context(op_type, ctx, key, value, input))
  }

  /// 切片零分配/低分配条目入队统一出口
  #[inline]
  pub fn enqueue_slices<T: AsRef<[u8]>>(
    &self,
    op_type: AofEntryType,
    ctx: impl Into<AofWriteContext>,
    key: &[u8],
    value: &[u8],
    input: &ReplayInputSlice<'_, T>,
  ) -> waof::Result<i64> {
    let ctx = ctx.into();
    ReplayInput::with_encoded_slices(input, |serialized| {
      self.enqueue_raw(op_type, ctx, key, value, serialized)
    })
  }

  /// RMW 切片零分配条目入队统一出口（value 恒空）
  ///
  /// C# 统一存 RMW 日志单点（PostRMWOperation 按 NeedAofLog 位转调入队，
  /// Deterministic 置位 + StoredProcMode 跳过两道门由 RecordShape/调用方门承担）：
  /// libs/server/Storage/Functions/UnifiedStore/PrivateMethods.cs:WriteLogRMW
  #[inline]
  pub fn enqueue_rmw_slices<T: AsRef<[u8]>>(
    &self,
    op_type: AofEntryType,
    ctx: impl Into<AofWriteContext>,
    key: &[u8],
    input: &ReplayInputSlice<'_, T>,
  ) -> waof::Result<i64> {
    self.enqueue_slices(op_type, ctx, key, &[], input)
  }

  /// libs/server/AOF/GarnetAppendOnlyFile.cs:backpressure
  ///
  /// 主侧复制背压闸门。
  pub fn backpressure(&self) -> Option<&Arc<AofBackpressure>> {
    self.log.backpressure()
  }

  /// libs/server/AOF/GarnetAppendOnlyFile.cs:Dispose
  ///
  /// AOF 关停对偶（唯一显式入口）：先置位背压闸门放行全部滞留追加方
  ///（C# `backpressure?.Dispose()`——Release all stalled appenders
  /// permanently），后按角色分派收口：副本走纯刷盘（不写本地 commit 元数据
  /// 帧——副本 AOF 须为主端流严格镜像，本地帧不在主端流内，重启增量协商
  /// 位点漂出主端帧边界令扫描判 Invalid 死锁；C# 对偶：副本 Dispose 链无
  /// CommitAsync，TrueDispose 仅释放资源），主端维持写帧收口
  ///（[`GarnetLog::commit_async`]）。直驱路径不经常驻提交协程通道——停机期
  /// worker 运行时随 join 析构、协程已不在，直驱刷盘不受影响。
  pub async fn dispose_async(&self) {
    if let Some(bp) = self.backpressure() {
      bp.dispose();
    }
    if self.is_replica() {
      self.log().commit_flush_only_async().await;
    } else {
      self.log().commit_async().await;
    }
  }

  /// 注入副本角色位（服务装配期一次；from_parts 与 AOF 点亮、--recover
  /// 重放两处装点注入同一 Arc，保证任何装配形态下停机分派读到同一角色位；
  /// 同源转发全部物理子日志——提交落盘角色闸与停机分派读同一角色源）
  pub fn attach_primary_tasks(&self, tasks: Arc<PrimaryTasks>) {
    if self.primary_tasks.set(Arc::clone(&tasks)).is_ok() {
      self.log.attach_primary_tasks(tasks);
    }
  }

  /// 当前是否副本角色（角色位缺省裸构造 = 主角色）
  fn is_replica(&self) -> bool {
    self
      .primary_tasks
      .get()
      .is_some_and(|tasks| tasks.is_replica())
  }

  /// libs/server/AOF/GarnetAppendOnlyFile.cs:ReadConsistencyManager
  ///
  /// 读取一致性管理器快照。
  pub fn read_consistency_manager(&self) -> Option<Arc<ReadConsistencyManager>> {
    self.read_consistency_manager.read().clone()
  }

  /// 读锁借用读取一致性管理器，执行闭包并返回值（零 Arc 克隆）
  #[inline]
  pub fn with_read_consistency_manager<R>(
    &self,
    f: impl FnOnce(&ReadConsistencyManager) -> R,
  ) -> Option<R> {
    self.read_consistency_manager.read().as_deref().map(f)
  }

  /// 注入向量集合管理器（AOF 点亮装配期；C# 由 storeWrapper.activeVectorManager
  /// 随数据库上下文激活承接）。
  pub fn set_vector_manager(&self, manager: Arc<VectorManager>) {
    *self.vector_manager.write() = Some(manager);
  }

  /// 向量集合管理器快照（未注入为 None，向量条目重放按无承接面失败）。
  pub fn vector_manager(&self) -> Option<Arc<VectorManager>> {
    self.vector_manager.read().clone()
  }

  /// libs/server/AOF/GarnetAppendOnlyFile.cs:MultiLogEnabled
  ///
  /// 多物理日志模式是否启用。
  pub fn multi_log_enabled(&self) -> bool {
    self.multi_log_enabled
  }

  /// 副本一致读等待超时只读出口（C# serverOptions.ReplicaSyncTimeout 投影的
  /// 单一配置位；回放栅栏 LeaderBarrier 会合时限同源复用，不建第二配置来源）。
  pub fn read_timeout(&self) -> Duration {
    self.read_timeout
  }

  /// libs/server/AOF/GarnetAppendOnlyFile.cs:TotalSize
  ///
  /// 全拓扑 begin..tail 字节总量。
  pub fn total_size(&self) -> i64 {
    self
      .log
      .tail_address()
      .aggregate_diff(&self.log.begin_address())
  }

  /// libs/server/AOF/GarnetAppendOnlyFile.cs:GetVirtualSublogIdx
  ///
  /// 物理子日志 × 回放任务 → 虚拟子日志下标（公式单点转引域共享内核）。
  pub fn get_virtual_sublog_idx(&self, sublog_idx: usize, replay_idx: usize) -> usize {
    virtual_sublog_idx(sublog_idx, replay_idx, self.replay_task_count)
  }

  /// libs/server/Servers/GarnetServerOptions.cs:AofVirtualSublogCount
  ///
  /// 虚拟子日志总数。
  pub fn virtual_sublog_count(&self) -> usize {
    self.physical_sublog_count * self.replay_task_count
  }

  /// libs/server/Servers/GarnetServerOptions.cs:AofReplayTaskCount
  ///
  /// 单物理子日志所辖虚拟子日志数（= 回放任务数，与
  /// [`Self::get_virtual_sublog_idx`] 的槽算式同源，路由换算单点）。
  pub fn virtual_sublog_per_sublog(&self) -> usize {
    self.replay_task_count
  }

  /// libs/server/AOF/GarnetAppendOnlyFile.cs:InvalidAofAddress
  ///
  /// 无效地址向量（C# InvalidAofAddress：全槽 -1）。
  pub fn invalid_aof_address(&self) -> AofAddress {
    AofAddress::create(self.physical_sublog_count as i32, -1)
  }

  /// libs/server/AOF/GarnetAppendOnlyFile.cs:GetLargerThanMaximumSequenceNumber
  ///
  /// 严格大于已观测尾部全部序列号的新序列号（调用方须先读尾地址）。
  ///（C# GetLargerThanMaximumSequenceNumber = GetSequenceNumber() + 1）
  pub fn get_larger_than_maximum_sequence_number(&self) -> i64 {
    self.get_sequence_number() + 1
  }

  /// libs/server/AOF/GarnetAppendOnlyFile.cs:GetSequenceNumber
  ///
  /// 取序列号（C# SequenceNumberGenerator.GetSequenceNumber；仅多物理
  /// 日志模式构造，单物理日志模式恒 0——排序由日志地址承担）。
  pub fn get_sequence_number(&self) -> i64 {
    self
      .seq_num_gen
      .as_ref()
      .map_or(0, |g| g.get_sequence_number())
  }

  /// libs/server/AOF/GarnetAppendOnlyFile.cs:CreateOrUpdateKeySequenceManager
  ///
  /// 代际更替一致性管理器（版本 = 前代 + 1）；旧管理器栅栏禁用放行。
  pub fn create_or_update_key_sequence_manager(&self) {
    if !self.multi_log_enabled {
      // C# MultiLogEnabled 未启用时不建管理器
      return;
    }
    let previous = self.read_consistency_manager();
    let current_version = previous.as_ref().map_or(0, |m| m.current_version());
    let fresh = Arc::new(ReadConsistencyManager::new(
      current_version + 1,
      self.physical_sublog_count,
      self.replay_task_count,
      self.replay_drift_threshold,
      self.replay_drift_check_freq,
      self.read_timeout,
    ));
    if let Some(m) = previous {
      m.replay_barrier.disable();
    }
    *self.read_consistency_manager.write() = Some(fresh);
  }

  /// libs/server/AOF/GarnetAppendOnlyFile.cs:ResetSequenceNumberGenerator
  ///
  /// 恢复/故障转移后按已回放最大序列号抬升生成器起点（仅多物理日志模式；
  /// 时间前进保证后续取号 > 起点）。
  pub fn reset_sequence_number_generator(&self) {
    if self.physical_sublog_count <= 1 {
      return;
    }
    let Some(manager) = self.read_consistency_manager() else {
      return;
    };
    let Some(seq_num_gen) = &self.seq_num_gen else {
      return;
    };
    let start = manager
      .get_physical_sublog_max_replayed_sequence_number()
      .into_iter()
      .max()
      .unwrap_or(0);
    seq_num_gen.set_starting_offset(start);
  }

  /// C# DatabaseManagerBase ReplayDatabaseAOF 编排对应（精确锚点见 database_manager_base.rs）
  ///
  /// 重放 AOF 条目至指定地址
  pub async fn replay_database_aof<D: Device>(
    self: Arc<Self>,
    db: &GarnetDatabase<D>,
    until: u64,
  ) -> wkv::Result<u64> {
    // 单次取口：重放执行域会话与重放目标必须绑定同一在线引擎实例（取口间
    // 被副本全量恢复换持将致会话与目标跨引擎错位）
    let store = db.store();
    let session = store.new_session()?;
    session.set_active_db(db.id.max(0) as u64);
    // 重放趟自持执行域会话绑定（对标 C# 恢复趟由重放驱动自备
    // ActiveThreadSession）：向量族条目应用（store_rmw 向量分支经
    // VectorManager 读索引/写透登记表）须见当前执行域会话。绑定跨整趟
    // `.await` 存活为刻意形态——重放臂属主线程即绑定线程，同线程其他
    // 任务的向量段各自叠绑/还原（LIFO 栈纪律），且本趟在装配/重放收口段
    // 执行，缺席删除钩子等他任务触达的登记表/清扫写面对会话落位域无感
    // （键自带记录域，见 vector_registry_recovery 模块头），无错域风险
    let _vector_domain = ActiveVectorSessionGuard::bind(&session);
    let batch = session.enter_batch();
    let storage = StorageSession::new(batch);
    let aof_clone = Arc::clone(&self);
    // 拓扑面先取（AofProcessor 构造消耗 Arc）
    let multi_log_enabled = self.multi_log_enabled;
    let physical_sublog_count = self.physical_sublog_count;
    let invalid = self.invalid_aof_address();
    let processor = AofProcessor::new(self);
    let target = ReplayTarget {
      session: &storage,
      store: Arc::clone(&store),
      // 恢复重放位点下界：恢复检查点元数据持久化的 AOF 覆盖边界（位点
      // 闸跳过快照已物化条目，截断前异常宕机纵深防御；冷启动 0 无下界）。
      // 与会话同读自单次取口实例，重放趟全程绑定同一在线引擎
      aof_floor: store
        .recovered_aof_floor()
        .iter()
        .map(|&a| a as i64)
        .collect(),
    };
    // C# AofRecover.cs:Recover 分派（RecoverReplayDriver）：MultiLogEnabled
    // 时按 untilAddress 向量逐物理子日志收敛恢复，否则单子日志恢复
    let replayed = if multi_log_enabled {
      let mut until_vector = invalid;
      for i in 0..physical_sublog_count {
        let value = if until == u64::MAX {
          -1
        } else {
          until.min(aof_clone.log().get_tail_address(i) as u64) as i64
        };
        until_vector.set(i, value);
      }
      AofRecover::multi_log_recover(&processor, &aof_clone, db.id, &until_vector, &target)
        .await
        .map_err(|e| Error::Io(io::Error::other(e.to_string())))
    } else {
      let until_address = if until == u64::MAX {
        -1
      } else {
        until.min(aof_clone.log().get_tail_address(0) as u64) as i64
      };
      AofRecover::single_log_recover(&processor, &aof_clone, db.id, 0, until_address, &target)
        .await
        .map_err(|e| Error::Io(io::Error::other(e.to_string())))
    };
    // C# AofProcessor.Recover 的 finally 收口（AofRecover.cs:35-37）：两分支、
    // 成败两态一律抬升取号起点——起点取回放期累积进一致性管理器的各物理子日志
    // 最大已回放序列号，保证「time moves forward」：重启后新序列号 > 崩溃前历史
    // 序列号。缺此收口则取号回拨，record_gate 的 `sequence_number >
    // until_sequence_number` 上界判定错位，新写入有效记录被当越界帧丢弃致发散
    aof_clone.reset_sequence_number_generator();
    replayed
  }
}

#[cfg(test)]
mod tests {
  use std::sync::Arc;

  use compio::runtime::Runtime;
  use waof::AofEntryType;

  use super::*;
  use crate::aof::{test_support::test_sublog, waof_sublog::AofSublog};

  fn aof_with(
    sublogs: usize,
    replay_tasks: i32,
  ) -> (Vec<tempfile::TempDir>, Arc<GarnetAppendOnlyFile>) {
    let options = RuntimeServerOptions {
      aof_physical_sublog_count: sublogs as i32,
      aof_replay_task_count: replay_tasks,
      ..RuntimeServerOptions::default()
    };
    let dirs = (0..sublogs.max(1))
      .map(|i| {
        let (dir, backend) = test_sublog(&format!("aof_{i}"));
        (dir, backend)
      })
      .collect::<Vec<_>>();
    let backends: Vec<Arc<AofSublog>> = dirs.iter().map(|(_, b)| Arc::clone(b)).collect();
    let seq_num_gen = (sublogs > 1).then(|| Arc::new(SequenceNumberGenerator::new(0)));
    let aof = Arc::new(GarnetAppendOnlyFile::new(
      Arc::new(GarnetLog::new(&options, backends, seq_num_gen.clone()).expect("构造 GarnetLog")),
      &options,
      seq_num_gen,
    ));
    (dirs.into_iter().map(|(d, _)| d).collect(), aof)
  }

  #[test]
  fn total_size_and_virtual_index() {
    let (_dirs, aof) = aof_with(1, 1);
    assert_eq!(aof.total_size(), 0);
    let _ = aof.enqueue_raw(AofEntryType::StoreUpsert, (1, 1), b"k", b"vv", &[]);
    assert!(aof.total_size() > 0);
    assert_eq!(aof.get_virtual_sublog_idx(0, 0), 0);
    assert_eq!(aof.virtual_sublog_count(), 1);
  }

  /// 关停对偶：dispose 收口未提交帧并放行闸门（C# GarnetAppendOnlyFile.
  /// Dispose 的 backpressure?.Dispose + Log.Dispose 对位）
  #[test]
  fn dispose_async_flushes_frames_and_releases_gate() {
    let (_dirs, aof) = aof_with(1, 1);
    let _ = aof.enqueue_raw(AofEntryType::StoreUpsert, (1, 1), b"k", b"vv", &[]);
    let tail = aof.log().tail_address().max();
    let sublog = aof.log().get_sub_log(0);
    assert!(
      sublog.flushed_until_address() < tail,
      "预置：dispose 前未刷盘"
    );

    Runtime::new().unwrap().block_on(aof.dispose_async());

    assert!(
      sublog.flushed_until_address() >= tail,
      "dispose 后未提交帧已落设备"
    );
    // 主端分派写 commit 元数据帧：尾位点随帧推进（与副本分派用例的
    // 「尾位点不动」断言对偶，锁死双向角色分派）
    assert!(
      aof.log().tail_address().max() > tail,
      "主端 dispose 随 commit 帧推进尾位点"
    );
    // 闸门放行：dispose 置位后任何滞后快照均判释放
    assert!(aof.backpressure().unwrap().is_released(0, i64::MAX));
  }

  /// 停机分派：副本角色纯刷盘不写本地 commit 元数据帧（已收记录落设备、
  /// 尾位点不动——副本 AOF 为主端流严格镜像，本地帧会令重启增量协商位点
  /// 漂出主端帧边界）；主端分派对偶断言（尾位点随 commit 帧推进）见上例。
  /// 角色装配先于入队（自动提交面在角色闸内）；停机收口前先经异步提交入口
  /// （commit_aof/检查点/周期任务同源生产面）驱动一次副本落盘——r57 红即
  /// 该面常驻协程 commit_to 写帧的交错竞态，此驱动把竞窗折叠为确定路径
  #[test]
  fn dispose_async_replica_flushes_without_commit_frame() {
    let (_dirs, aof) = aof_with(1, 1);
    let tasks = Arc::new(PrimaryTasks::default());
    tasks.suspend();
    aof.attach_primary_tasks(tasks);
    let _ = aof.enqueue_raw(AofEntryType::StoreUpsert, (1, 1), b"k", b"vv", &[]);
    let tail = aof.log().tail_address().max();
    let sublog = aof.log().get_sub_log(0);

    let rt = Runtime::new().unwrap();
    rt.block_on(aof.log().commit_async());
    rt.block_on(aof.dispose_async());

    assert!(
      sublog.flushed_until_address() >= tail,
      "副本 dispose 后已收记录须落设备"
    );
    assert_eq!(
      aof.log().tail_address().max(),
      tail,
      "副本纯刷盘不写本地 commit 帧，尾位点不动"
    );
  }

  #[test]
  fn sequence_numbers_strictly_increase_past_tail() {
    let (_dirs, aof) = aof_with(2, 1);
    let first = aof.get_sequence_number();
    let larger = aof.get_larger_than_maximum_sequence_number();
    assert!(larger > first);
    assert!(aof.get_larger_than_maximum_sequence_number() >= larger);
  }

  #[test]
  fn consistency_manager_generation_bump() {
    let (_dirs, aof) = aof_with(2, 1);
    let v1 = aof.read_consistency_manager().unwrap();
    assert_eq!(v1.current_version(), 1);
    aof.create_or_update_key_sequence_manager();
    let v2 = aof.read_consistency_manager().unwrap();
    assert_eq!(v2.current_version(), 2, "代际 = 前代 + 1");
  }

  #[test]
  fn reset_sequence_generator_after_replay() {
    let (_dirs, aof) = aof_with(2, 1);
    let manager = aof.read_consistency_manager().unwrap();
    manager.update_physical_sublog_max_sequence_number(0, 77);
    manager.update_physical_sublog_max_sequence_number(1, 42);
    aof.reset_sequence_number_generator();
    // 抬升后取号不小于已回放最大序列号（时钟推进后严格增大）
    assert!(aof.get_sequence_number() >= 77, "恢复后时间前进");
  }

  #[test]
  fn invalid_address_shape() {
    let (_dirs, aof) = aof_with(2, 1);
    let invalid = aof.invalid_aof_address();
    assert_eq!(invalid.length(), 2);
    assert_eq!(invalid.get(0), Some(-1));
  }

  #[test]
  fn multi_sublog_probe_methods() {
    // TempDir 绑定持目录存活至测试结束，段文件随 Drop 清理
    let (_dirs, aof) = aof_with(2, 1);
    let sub0 = Arc::clone(aof.log().get_sub_log(0));
    let sub1 = Arc::clone(aof.log().get_sub_log(1));

    // 初始状态：双子日志空日志（真实段设备首地址 0：begin=tail=flushed=committed=0）
    assert_eq!(aof.log().tail_address().max(), 0);

    // 仅向 sublog 1 写入数据，0 号保持空闲
    let addr = sub1.enqueue(b"sublog_1_record").unwrap();
    assert_eq!(addr, 0, "首条记录落设备首地址");
    assert!(sub1.tail_address() > 0);
    assert_eq!(sub0.tail_address(), 0);

    // 尾地址取所有子日志的最大尾地址
    assert_eq!(aof.log().tail_address().max(), sub1.tail_address());

    // 提交 sub1
    let sub1_tail = sub1.tail_address();
    sub1.commit(sub1_tail, 0);

    // 向 sub0 写入更大长度数据并提交
    let _ = sub0.enqueue(b"sublog_0_longer_payload_data");
    let sub0_tail = sub0.tail_address();
    sub0.commit(sub0_tail, 0);

    // 尾地址应取各子日志尾地址的最大值
    let expected_max_tail = sub0.tail_address().max(sub1.tail_address());
    assert_eq!(aof.log().tail_address().max(), expected_max_tail);
  }

  #[test]
  fn drift_options_forwarded_to_consistency_manager() {
    let options = RuntimeServerOptions {
      aof_physical_sublog_count: 2,
      aof_replay_task_count: 1,
      replay_drift_threshold: 10,
      replay_drift_check_freq: 2,
      ..RuntimeServerOptions::default()
    };
    let (dir0, b0) = test_sublog("aof_drift_0");
    let (dir1, b1) = test_sublog("aof_drift_1");
    let seq_num_gen = Some(Arc::new(SequenceNumberGenerator::new(0)));
    let aof = Arc::new(GarnetAppendOnlyFile::new(
      Arc::new(
        GarnetLog::new(
          &options,
          vec![Arc::clone(&b0), Arc::clone(&b1)],
          seq_num_gen.clone(),
        )
        .expect("构造 GarnetLog"),
      ),
      &options,
      seq_num_gen,
    ));
    let rcm = aof.read_consistency_manager().expect("存在一致性管理器");
    assert_eq!(rcm.current_version(), 1);
    assert_eq!(rcm.virtual_sublog_count(), 2);
    assert_eq!(rcm.vsr(0).next_drift_check_window_lower_bound(), 0);
    assert_eq!(rcm.vsr(1).next_drift_check_window_lower_bound(), 20);
    drop(dir0);
    drop(dir1);
  }
}
