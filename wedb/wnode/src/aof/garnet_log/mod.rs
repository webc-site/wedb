//! Garnet 分布式 AOF 日志：单日志 / 分片多子日志双拓扑的路由层
//! （对标 libs/server/AOF/GarnetLog.cs:GarnetLog）。
//!
//! C# 底层为 TsavoriteLog（单）或 TsavoriteLog[]（分片）；Rust 侧设备面
//! 为具体类型 [`super::waof_sublog::AofSublog`]（单测走轻量真实段设备），
//! 本类型保留全部路由 / 头编码 / 背压 / 地址向量语义。
//!
//! 分片路由：`物理子日志 = hash % physicalSublogCount`，
//! `回放任务 = hash / physicalSublogCount % replayTaskCount`，
//! `虚拟子日志 = 物理子日志 * replayTaskCount + 回放任务`。
//!
//! 目录布局（单文件面拆分，C# GarnetLog.cs 段落映射）：
//! - [`addresses`]：地址向量 / 哈希索引换算 / Unsafe / Scan 段；
//! - [`commit`]：Recover / Reset / Initialize 段 + Commit / WaitForCommit 段；
//! - [`single_log_branch`]：BackpressureWait 段 + Enqueue 段（单物理日志分支）。

use crate::{Error, primary_tasks::PrimaryTasks};
mod addresses;
mod commit;
mod single_log_branch;

use std::{slice::from_ref, sync::Arc};

pub(crate) use addresses::virtual_sublog_idx;
use waof::{AofEntryType, SequenceNumberGenerator};
use wconf::RuntimeServerOptions;

use super::{
  aof_backpressure::AofBackpressure, sharded_log::ShardedLog, single_log::SingleLog,
  waof_sublog::AofSublog,
};

/// 单日志 / 分片双拓扑容器（对标 libs/server/AOF/GarnetLog.cs:GarnetLog）。
///
/// 后端字段拓扑与 C# 同形（GarnetLog.cs:25-26 双可空字段，:66-71 构造期二选一），
/// 拓扑判定的重复写法经 [`GarnetLog::route`] 收敛为单点定义。
pub struct GarnetLog {
  /// 单物理日志包装（AofPhysicalSublogCount == 1 拓扑；对标 C# :25 singleLog）。
  single_log: Option<SingleLog>,
  /// 分片物理日志集合（AofPhysicalSublogCount > 1 拓扑；对标 C# :26 shardedLog）。
  sharded_log: Option<ShardedLog>,
  /// 物理子日志数。
  physical_sublog_count: usize,
  /// 回放任务数。
  replay_task_count: usize,
  /// 单日志拓扑标志（1 物理 × 1 回放；对标 C# :28 usingSingleLog）。
  using_single_log: bool,
  /// 单物理日志拓扑标志（1 物理子日志；对标 C# :29 usingSinglePhysicalLog）。
  using_single_physical_log: bool,
  /// 主侧背压闸门（可选）。
  backpressure: Option<Arc<AofBackpressure>>,
  /// 分片模式序列号生成器（C# appendOnlyFile.seqNumGen 共享引用）。
  seq_num_gen: Option<Arc<SequenceNumberGenerator>>,
  /// AofAutoCommit（C# 派生属性 CommitFrequencyMs == 0）。
  auto_commit: bool,
}

impl GarnetLog {
  /// libs/server/AOF/GarnetLog.cs:GarnetLog（构造）。
  ///
  /// 依选项决定单日志（1 物理 1 回放）或分片拓扑，并构造背压闸门。
  /// `seq_num_gen` 仅多物理日志模式传入（C# 由 appendOnlyFile 构造并共享）。
  /// 单物理日志拓扑空后端集为构造期参数错误（C# 构造期 ArgumentException 对位）。
  /// 本口不做选项体检：C# 构造子同位零校验，三尺寸旋钮组合体检唯一在
  /// [`super::AofSettings::from_options`]（boot 装配口先于设备分配执行一次），
  /// 此处不设第二道闸。
  pub fn new(
    server_options: &RuntimeServerOptions,
    backends: Vec<Arc<AofSublog>>,
    seq_num_gen: Option<Arc<SequenceNumberGenerator>>,
  ) -> crate::Result<Self> {
    let physical_sublog_count = server_options.aof_physical_sublog_count.max(1) as usize;
    let replay_task_count = server_options.aof_replay_task_count.max(1) as usize;
    let using_single_log = physical_sublog_count == 1 && replay_task_count == 1;
    let using_single_physical_log = physical_sublog_count == 1;

    let (single_log, sharded_log) = if using_single_physical_log {
      let Some(single) = backends.into_iter().next() else {
        return Err(Error::InvalidArgument(
          "单物理日志拓扑需至少 1 个后端".into(),
        ));
      };
      (Some(SingleLog::new(single)), None)
    } else {
      (None, Some(ShardedLog::new(backends)))
    };

    Ok(Self {
      single_log,
      sharded_log,
      physical_sublog_count,
      replay_task_count,
      using_single_log,
      using_single_physical_log,
      backpressure: Some(Arc::new(AofBackpressure::new(
        physical_sublog_count,
        server_options.aof_sync_max_lag_bytes,
      ))),
      seq_num_gen: if physical_sublog_count > 1 {
        seq_num_gen
      } else {
        None
      },
      auto_commit: server_options.commit_frequency_ms == 0,
    })
  }

  /// 分片拓扑后端（单/双拓扑构造期二选一固化，sharded 分支必非空；
  /// 不变量单点 expect，全部分片路由转发自此）。
  #[inline]
  fn sharded(&self) -> &ShardedLog {
    self
      .sharded_log
      .as_ref()
      .expect("分片拓扑后端必在构造期就位")
  }

  /// 单/分片拓扑二选一的单点判定（C# 每个成员各自重复
  /// `singleLog != null ? singleLog.X : shardedLog.X`，GarnetLog.cs:82-83、109-177、
  /// 180-214、250-302、396-430、436-593；rust 把 null 判定收敛为这一处，
  /// 各成员只交出两个后端上的动作）。
  #[inline]
  fn route<'a, T>(
    &'a self,
    on_single: impl FnOnce(&'a SingleLog) -> T,
    on_sharded: impl FnOnce(&'a ShardedLog) -> T,
  ) -> T {
    match &self.single_log {
      Some(single) => on_single(single),
      None => on_sharded(self.sharded()),
    }
  }

  /// 分片序列号取号（C# appendOnlyFile.seqNumGen.GetSequenceNumber）。
  fn next_sequence_number(&self) -> i64 {
    self
      .seq_num_gen
      .as_ref()
      .map_or(0, |g| g.get_sequence_number())
  }

  /// 背压闸门句柄（C# 经 appendOnlyFile.backpressure 共享）。
  #[inline]
  pub fn backpressure(&self) -> Option<&Arc<AofBackpressure>> {
    self.backpressure.as_ref()
  }

  /// 注入角色状态源到全部物理子日志（提交落盘角色闸的下游装配；同一
  /// Arc<PrimaryTasks> 转发，勿造第二角色状态源）。
  pub fn attach_primary_tasks(&self, tasks: Arc<PrimaryTasks>) {
    for sublog in self.sublogs() {
      sublog.attach_primary_tasks(Arc::clone(&tasks));
    }
  }

  /// libs/server/AOF/GarnetLog.cs:GetSubLog
  ///
  /// 指定子日志后端。
  #[inline]
  pub fn get_sub_log(&self, sublog_idx: usize) -> &Arc<AofSublog> {
    self.route(
      |single| {
        debug_assert_eq!(sublog_idx, 0);
        &single.log
      },
      |sharded| &sharded.sublog[sublog_idx],
    )
  }

  /// 全部物理子日志切片视图
  #[inline]
  pub fn sublogs(&self) -> &[Arc<AofSublog>] {
    self.route(
      |single| from_ref(&single.log),
      |sharded| sharded.sublog.as_slice(),
    )
  }

  /// 拓扑的物理子日志总数（C# Size 属性）。
  #[inline]
  pub fn size(&self) -> usize {
    self.route(|_| 1, |sharded| sharded.len())
  }

  /// 回放任务数（C# ReplayTaskCount）。
  #[inline]
  pub fn replay_task_count(&self) -> usize {
    self.replay_task_count
  }

  /// libs/server/AOF/GarnetLog.cs:LockSublogs
  ///
  /// 入队操作前的子日志位图锁（慢路径，慎用）。仅多物理日志拓扑的广播/事务
  /// 分支调用（C# GarnetLog.cs:227-232 无条件用 shardedLog）。
  #[inline]
  pub fn lock_sublogs(&self, log_access_bitmap: u64) {
    self.sharded().lock_sublogs(log_access_bitmap);
  }

  /// libs/server/AOF/GarnetLog.cs:UnlockSublogs
  #[inline]
  pub fn unlock_sublogs(&self, log_access_bitmap: u64) {
    self.sharded().unlock_sublogs(log_access_bitmap);
  }
}

/// 分块写入形状：记录形状 + 组件选择标志（仅 garnet_log 模块内部分块支
/// 使用；大记录经统一 `GarnetLog::enqueue` 口自动进入，无对外第二入口）。
#[derive(Clone)]
struct ChunkedShape<'a> {
  /// 记录形状。
  record: RecordShape<'a>,
  /// 是否写 value 组件。
  write_value: bool,
  /// 是否写 input 组件。
  write_input: bool,
}

/// AOF 写入上下文元数据（存储版本 + 会话 ID）
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AofWriteContext {
  /// 存储版本
  pub version: i64,
  /// 会话 id
  pub session_id: i32,
}

impl AofWriteContext {
  /// 显式构造版本与会话上下文
  #[inline]
  pub const fn new(version: i64, session_id: i32) -> Self {
    Self {
      version,
      session_id,
    }
  }

  /// 仅携带存储版本构造（会话 id 默认 0）
  #[inline]
  pub const fn from_version(version: i64) -> Self {
    Self {
      version,
      session_id: 0,
    }
  }
}

impl From<i64> for AofWriteContext {
  #[inline]
  fn from(version: i64) -> Self {
    Self::from_version(version)
  }
}

impl From<(i64, i32)> for AofWriteContext {
  #[inline]
  fn from((version, session_id): (i64, i32)) -> Self {
    Self::new(version, session_id)
  }
}

/// 入队记录形状（头字段 + 负载组件）。
#[derive(Clone)]
pub struct RecordShape<'a> {
  /// 操作类型。
  pub op_type: AofEntryType,
  /// 存储版本。
  pub version: i64,
  /// 会话 id。
  pub session_id: i32,
  /// key。
  pub key: &'a [u8],
  /// value。
  pub value: &'a [u8],
  /// input。
  pub input: &'a [u8],
  /// 数据库 id。
  pub database_id: u8,
}

impl<'a> RecordShape<'a> {
  /// 构造统一 RecordShape（对标 C# 各 WriteLog* 单一形态，database_id 默认 0）
  #[inline]
  pub const fn new(
    op_type: AofEntryType,
    version: i64,
    session_id: i32,
    key: &'a [u8],
    value: &'a [u8],
    input: &'a [u8],
  ) -> Self {
    Self {
      op_type,
      version,
      session_id,
      key,
      value,
      input,
      database_id: 0,
    }
  }

  /// 基于 AOF 上下文元数据构造 RecordShape（对标 C# 各 WriteLog* 单一形态）
  #[inline]
  pub const fn from_context(
    op_type: AofEntryType,
    ctx: AofWriteContext,
    key: &'a [u8],
    value: &'a [u8],
    input: &'a [u8],
  ) -> Self {
    Self::new(op_type, ctx.version, ctx.session_id, key, value, input)
  }
}

use wtxn::{SublogAccess, TxnEntryType};

/// 事务端口枚举 → AOF 条目类型的单一映射点
///
/// wtxn 不感知物理判别值，全部事务 / 存储过程条目经此翻译后入队
///（物理判别值单源 [`waof::AofEntryType`]，在 garnet 中的相对路径:
/// libs/server/AOF/AofEntryType.cs:AofEntryType）
fn txn_entry_type_to_aof(entry: TxnEntryType) -> AofEntryType {
  match entry {
    TxnEntryType::TxnStart => AofEntryType::TxnStart,
    TxnEntryType::TxnCommit => AofEntryType::TxnCommit,
    TxnEntryType::StoredProcedure => AofEntryType::StoredProcedure,
  }
}

impl wtxn::TxnAofLog for GarnetLog {
  #[inline]
  fn size(&self) -> usize {
    self.size()
  }

  #[inline]
  fn replay_task_count(&self) -> usize {
    self.replay_task_count()
  }

  #[inline]
  fn get_physical_sublog_idx(&self, key_hash: i64) -> usize {
    self.get_physical_sublog_idx(key_hash)
  }

  #[inline]
  fn get_replay_task_idx(&self, key_hash: i64) -> usize {
    self.get_replay_task_idx(key_hash)
  }

  /// 事务标记入队：透传底层入队错误（对标 C# EnqueueTxn 异常上抛），
  /// 由 wtxn 事务状态机决定传播路径（EXEC 响应可感知未确认）
  #[inline]
  fn enqueue_txn(
    &self,
    op_type: TxnEntryType,
    txn_version: i64,
    session_id: i32,
    access: &SublogAccess<'_>,
  ) -> waof::Result<()> {
    self
      .enqueue_txn(
        txn_entry_type_to_aof(op_type),
        txn_version,
        session_id,
        access,
      )
      .map(|_| ())
  }

  /// 存储过程入队：透传底层入队错误（见 [`Self::enqueue_txn`]）
  #[inline]
  fn enqueue_stored_proc(
    &self,
    op_type: TxnEntryType,
    txn_version: i64,
    session_id: i32,
    proc_id: u8,
    payload: &[u8],
    access: &SublogAccess<'_>,
  ) -> waof::Result<()> {
    self
      .enqueue_stored_proc(
        txn_entry_type_to_aof(op_type),
        txn_version,
        session_id,
        proc_id,
        payload,
        access,
      )
      .map(|_| ())
  }
}

/// TsavoriteLog.MinPartialAllocSize 的等价常量（超过即分块；C# = 1 << 20）。
pub const MIN_PARTIAL_ALLOC_SIZE: i64 = 1 << 20;
