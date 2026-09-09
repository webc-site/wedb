//! 逻辑数据库管理接口（对标 libs/server/Databases/IDatabaseManager.cs:IDatabaseManager）
//!
//! Rust 侧以泛型 trait 承载（实现：[`SingleDatabaseManager`](super::single_database_manager::SingleDatabaseManager)
//! / [`MultiDatabaseManager`](super::multi_database_manager::MultiDatabaseManager)）；
//! wkv 的读写同步闭环特性使异步完成语义内嵌于各方法内。

use std::{future::Future, sync::Arc};

use wdev::Device;

use super::garnet_database::GarnetDatabase;
use crate::storage::functions::functions_state::FunctionsState;

/// 混合日志分布统计（对标 CollectHybridLogStats 返回的 HybridLogScanMetrics 对）
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct HybridLogStats {
  /// 日志起始有效地址
  pub begin_address: u64,
  /// 只读区起始地址
  pub read_only_address: u64,
  /// 内存区起始地址
  pub head_address: u64,
  /// 日志尾地址
  pub tail_address: u64,
  /// 键数量估计
  pub key_count: u64,
  /// 过期键数量估计
  pub expire_count: u64,
}

/// 数据库管理接口
pub trait IDatabaseManager<D: Device>: Send + Sync {
  /// 取库（不存在则新建），返回 (库, 是否新建)
  ///
  /// libs/server/Databases/IDatabaseManager.cs:TryGetOrAddDatabase
  fn try_get_or_add_database(&self, db_id: i64) -> wkv::Result<(Arc<GarnetDatabase<D>>, bool)>;

  /// 取库（不新建）
  ///
  /// libs/server/Databases/IDatabaseManager.cs:TryGetDatabase
  fn try_get_database(&self, db_id: i64) -> Option<Arc<GarnetDatabase<D>>>;

  /// 尝试占用检查点锁（暂停后台检查点）
  ///
  /// libs/server/Databases/IDatabaseManager.cs:TryPauseCheckpoints
  fn try_pause_checkpoints(&self, db_id: i64) -> bool;

  /// 释放检查点锁
  ///
  /// libs/server/Databases/IDatabaseManager.cs:ResumeCheckpoints
  fn resume_checkpoints(&self, db_id: i64);

  /// 恢复检查点（磁盘候选异步闭环）
  ///
  /// libs/server/Databases/IDatabaseManager.cs:RecoverCheckpointAsync
  fn recover_checkpoint_async(
    &self,
    replica_recover: bool,
    recover_from_token: Option<u128>,
  ) -> impl Future<Output = wkv::Result<()>>;

  /// 对指定库（-1 为全部）拍检查点，返回是否真正执行
  ///
  /// libs/server/Databases/IDatabaseManager.cs:TakeCheckpointAsync
  fn take_checkpoint_async(
    &self,
    background: bool,
    db_id: i64,
  ) -> impl Future<Output = wkv::Result<bool>>;

  /// 若距上次保存早于 `entry_ms` 则拍检查点
  ///
  /// libs/server/Databases/IDatabaseManager.cs:TakeOnDemandCheckpointAsync
  fn take_on_demand_checkpoint_async(
    &self,
    entry_ms: u64,
    db_id: i64,
  ) -> impl Future<Output = wkv::Result<()>>;

  /// AOF 达到字节上限的库拍检查点
  ///
  /// libs/server/Databases/IDatabaseManager.cs:TaskCheckpointBasedOnAofSizeLimitAsync
  fn task_checkpoint_based_on_aof_size_limit_async(
    &self,
    aof_size_limit: u64,
  ) -> impl Future<Output = wkv::Result<()>>;

  /// 提交 AOF（刷盘 + 推进提交地址）
  ///
  /// libs/server/Databases/IDatabaseManager.cs:CommitToAofAsync
  fn commit_to_aof_async(&self, db_id: i64) -> wkv::Result<()>;

  /// 等待 AOF 提交完成（闭环模型下为状态确认）
  ///
  /// libs/server/Databases/IDatabaseManager.cs:WaitForCommitToAofAsync
  fn wait_for_commit_to_aof_async(&self, db_id: i64) -> wkv::Result<bool>;

  /// 恢复 AOF（重放至当前尾），返回重放条数
  ///
  /// libs/server/Databases/IDatabaseManager.cs:RecoverAOFAsync
  fn recover_aof_async(&self) -> impl Future<Output = wkv::Result<u64>>;

  /// 重放 AOF 至 `until` 地址，返回重放条数
  ///
  /// libs/server/Databases/IDatabaseManager.cs:ReplayAOF
  fn replay_aof(&self, until: u64) -> impl Future<Output = wkv::Result<u64>>;

  /// 按需增长存储索引
  ///
  /// libs/server/Databases/IDatabaseManager.cs:GrowIndexesIfNeededAsync
  fn grow_indexes_if_needed_async(&self) -> wkv::Result<bool>;

  /// 执行对象收集扫描，返回遍历对象数
  ///
  /// libs/server/Databases/IDatabaseManager.cs:ExecuteObjectCollection
  fn execute_object_collection(&self, db_id: i64) -> wkv::Result<usize>;

  /// 启动大小追踪器
  ///
  /// libs/server/Databases/IDatabaseManager.cs:StartSizeTrackers
  fn start_size_trackers(&self);

  /// 重置复活化统计
  ///
  /// libs/server/Databases/IDatabaseManager.cs:ResetRevivificationStats
  fn reset_revivification_stats(&self);

  /// 入队 AOF 提交请求（`until` 地址）
  ///
  /// libs/server/Databases/IDatabaseManager.cs:EnqueueCommit
  fn enqueue_commit(&self, db_id: i64, until: u64);

  /// 取全部活跃库快照
  ///
  /// libs/server/Databases/IDatabaseManager.cs:GetDatabasesSnapshot
  fn get_databases_snapshot(&self) -> Vec<Arc<GarnetDatabase<D>>>;

  /// 清空指定库数据
  ///
  /// libs/server/Databases/IDatabaseManager.cs:FlushDatabase
  fn flush_database(&self, db_id: i64) -> impl Future<Output = wkv::Result<()>>;

  /// 清空全部库数据
  ///
  /// libs/server/Databases/IDatabaseManager.cs:FlushAllDatabases
  fn flush_all_databases(&self) -> impl Future<Output = wkv::Result<()>>;

  /// 交换两个库编号，成功返回 true
  ///
  /// libs/server/Databases/IDatabaseManager.cs:TrySwapDatabases
  fn try_swap_databases(&self, db_id1: i64, db_id2: i64) -> bool;

  /// 创建会话函数状态
  ///
  /// libs/server/Databases/IDatabaseManager.cs:CreateFunctionsState
  fn create_functions_state(&self, db_id: i64) -> FunctionsState;

  /// 采集混合日志分布统计（-1 为全部库）
  ///
  /// libs/server/Databases/IDatabaseManager.cs:CollectHybridLogStats
  fn collect_hybrid_log_stats(
    &self,
  ) -> impl Future<Output = wkv::Result<Vec<(i64, HybridLogStats)>>>;

  /// 恢复向量集合
  ///
  /// 缺口说明：向量引擎域（见 storage 域 vector 模块级缺口）未转写完成，
  /// 本方法返回 0。
  ///
  /// libs/server/Databases/IDatabaseManager.cs:RecoverVectorSets
  fn recover_vector_sets(&self, db_id: i64) -> wkv::Result<u64>;
}
