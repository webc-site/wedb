//! 逻辑数据库管理接口（对标 libs/server/Databases/IDatabaseManager.cs:IDatabaseManager）
//!
//! Rust 侧以泛型 trait 承载（实现：[`SingleDatabaseManager`](super::single_database_manager::SingleDatabaseManager)）；
//! wkv 的读写同步闭环特性使异步完成语义内嵌于各方法内。
//!
//! 接口面只收录 C# 无域参（或域参恒定为默认库）的管理动作；清库族
//! （FlushDatabase / FlushNamespace / FlushAllDatabases）携 (ns, db,
//! unsafeTruncateLog) 实参，唯一漏斗在具体
//! [`SingleDatabaseManager`](super::single_database_manager::SingleDatabaseManager)
//! 面上，本 trait 不设同形旁路（无调用方的接口投影即第二套入口）。

use std::{future::Future, sync::Arc};

use wdev::Device;
use wkv::HybridLogScanMetrics;

use super::garnet_database::GarnetDatabase;

/// 获取或新建数据库返回类型
pub type GetOrAddResult<D> = wkv::Result<(Arc<GarnetDatabase<D>>, bool)>;

/// 数据库管理接口
pub trait IDatabaseManager<D: Device>: Send + Sync {
  /// 取库（不存在则新建），返回 (库, 是否新建)
  ///
  /// libs/server/Databases/IDatabaseManager.cs:TryGetOrAddDatabase
  fn try_get_or_add_database(&self) -> impl Future<Output = GetOrAddResult<D>>;

  /// 取库（不新建）
  ///
  /// libs/server/Databases/IDatabaseManager.cs:TryGetDatabase
  fn try_get_database(&self) -> Option<Arc<GarnetDatabase<D>>>;

  /// 上次保存时间（毫秒 Unix 时间戳，0 表示从未保存）
  ///
  /// libs/server/Databases/IDatabaseManager.cs:LastSaveTime
  fn last_save_ms(&self) -> u64;

  /// 尝试占用检查点锁（暂停后台检查点）
  ///
  /// libs/server/Databases/IDatabaseManager.cs:TryPauseCheckpoints
  fn try_pause_checkpoints(&self) -> bool;

  /// 释放检查点锁
  ///
  /// libs/server/Databases/IDatabaseManager.cs:ResumeCheckpoints
  fn resume_checkpoints(&self);

  /// 恢复检查点（磁盘候选异步闭环）
  ///
  /// libs/server/Databases/IDatabaseManager.cs:RecoverCheckpointAsync
  fn recover_checkpoint_async(
    &self,
    replica_recover: bool,
    recover_from_token: Option<u128>,
  ) -> impl Future<Output = wkv::Result<()>>;

  /// 对指定库拍检查点，返回是否真正执行
  ///
  /// libs/server/Databases/IDatabaseManager.cs:TakeCheckpointAsync
  fn take_checkpoint_async(&self, background: bool) -> impl Future<Output = wkv::Result<bool>>;

  /// 若距上次保存早于 `entry_ms` 则拍检查点
  ///
  /// libs/server/Databases/IDatabaseManager.cs:TakeOnDemandCheckpointAsync
  fn take_on_demand_checkpoint_async(&self, entry_ms: u64)
  -> impl Future<Output = wkv::Result<()>>;

  /// AOF 体积超限检查点驱动入口（含尺寸预判、暂停闸门与副本角色门控，
  /// 周期任务循环直调本方法，对标 C# StoreWrapper 循环仅 Delay + 本调用）
  ///
  /// libs/server/Databases/IDatabaseManager.cs:TaskCheckpointBasedOnAofSizeLimitAsync
  fn task_checkpoint_based_on_aof_size_limit_async(
    &self,
    aof_size_limit: u64,
  ) -> impl Future<Output = wkv::Result<()>>;

  /// 提交 AOF（刷盘 + 推进提交地址）
  ///
  /// libs/server/Databases/IDatabaseManager.cs:CommitToAofAsync
  fn commit_to_aof_async(&self) -> impl Future<Output = wkv::Result<()>>;

  /// 等待 AOF 提交完成（事件驱动无锁等待）
  ///
  /// libs/server/Databases/IDatabaseManager.cs:WaitForCommitToAofAsync
  fn wait_for_commit_to_aof_async(&self) -> impl Future<Output = wkv::Result<bool>>;

  /// 恢复 AOF（重放至当前尾），返回重放条数
  ///
  /// libs/server/Databases/IDatabaseManager.cs:RecoverAOFAsync
  fn recover_aof_async(&self) -> impl Future<Output = wkv::Result<u64>>;

  /// 重放 AOF 至 `until` 地址，返回重放条数
  ///
  /// libs/server/Databases/IDatabaseManager.cs:ReplayAOF
  fn replay_aof(&self, until: u64) -> impl Future<Output = wkv::Result<u64>>;

  /// 重置复活化统计
  ///
  /// libs/server/Databases/IDatabaseManager.cs:ResetRevivificationStats
  fn reset_revivification_stats(&self);

  /// 入队 AOF 提交请求（`until` 地址）
  ///
  /// libs/server/Databases/IDatabaseManager.cs:EnqueueCommit
  fn enqueue_commit(&self, until: u64);

  /// 取全部活跃库快照
  ///
  /// libs/server/Databases/IDatabaseManager.cs:GetDatabasesSnapshot
  fn get_databases_snapshot(&self) -> Vec<Arc<GarnetDatabase<D>>>;

  /// 重置指定库（数据清空 + AOF 位点归零 + 保存点复位）
  ///
  /// libs/server/Databases/IDatabaseManager.cs:Reset
  fn reset(&self) -> impl Future<Output = wkv::Result<()>>;

  /// 清空全部库数据
  ///
  /// libs/server/Databases/IDatabaseManager.cs:FlushAllDatabases
  fn flush_all_databases(&self) -> impl Future<Output = wkv::Result<()>>;

  /// 采集混合日志内存分布统计（`库 id, 分布桶`；-1 形参语义由单库实现承接）
  ///
  /// C# 对主/对象存储各扫一遍；wedb 单物理日志统一值域，仅 main store
  /// 形态（分布容器 [`HybridLogScanMetrics`]，区域 × 状态 × (条数, 字节)）。
  ///
  /// libs/server/Databases/IDatabaseManager.cs:CollectHybridLogStats
  fn collect_hybrid_log_stats(
    &self,
  ) -> impl Future<Output = wkv::Result<Vec<(i64, HybridLogScanMetrics)>>>;

  /// 恢复向量集合（扫描回建登记表与上下文元数据 + 恢复期簿记收口）
  ///
  /// libs/server/Databases/IDatabaseManager.cs:RecoverVectorSets
  fn recover_vector_sets(&self) -> impl Future<Output = wkv::Result<u64>>;

  /// 检查并在溢出超阈值时自动扩容主存储索引（全活跃库扫描，达上限返回 true）
  ///
  /// libs/server/Databases/IDatabaseManager.cs:GrowIndexesIfNeededAsync
  fn grow_indexes_if_needed_async(
    &self,
    index_max_size: usize,
    resize_threshold: i64,
  ) -> impl Future<Output = wkv::Result<bool>>;
}
