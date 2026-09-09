//! 存储包装器（对标 libs/server/StoreWrapper.cs:StoreWrapper）
//!
//! C# 侧 StoreWrapper 聚合 TsavoriteKV / 检查点管理 / AOF / 任务管理器 /
//! 集群提供者；Rust 侧存储与检查点面由 [`DatabaseManager`]（databases 域）
//! 承载，任务调度（taskmanager 域）与集群拓扑（cluster 域）为并行转写域，
//! 相关入口以一致性缺省返回并注明缺口。

use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering::Relaxed};

use waof::AofEntryType;
use wdev::Device;

use crate::databases::database_manager_factory::DatabaseManager;
use crate::databases::garnet_database::GarnetDatabase;
use crate::databases::i_database_manager::{HybridLogStats, IDatabaseManager};
use crate::storage::functions::functions_state::FunctionsState;
use crate::storage::session::common::array_key_iteration_functions::cluster_slot;
use crate::storage::session::storage_session::StorageSession;

/// 存储包装器
pub struct StoreWrapper<D: Device> {
  /// 数据库管理器（单库 / 多库）
  pub database_manager: DatabaseManager<D>,
  /// 是否多库模式
  pub multi_database: bool,
  /// 是否启用 AOF
  pub enable_aof: bool,
  /// AOF 提交水位地址（EnqueueCommit / WaitForCommit 的闭环凭证）
  commit_head_address: AtomicU64,
}

/// 遍历单 / 多库管理器的统一分派宏（仅可调用 trait 面方法）
macro_rules! for_manager {
  ($self:expr, |$m:ident| $body:expr) => {
    match &$self.database_manager {
      DatabaseManager::Single($m) => $body,
      DatabaseManager::Multi($m) => $body,
    }
  };
}

impl<D: Device> StoreWrapper<D> {
  /// 创建存储包装器
  pub fn new(database_manager: DatabaseManager<D>, multi_database: bool, enable_aof: bool) -> Self {
    Self {
      database_manager,
      multi_database,
      enable_aof,
      commit_head_address: AtomicU64::new(0),
    }
  }

  /// AOF 当前总字节数（全部库聚合）
  ///
  /// libs/server/StoreWrapper.cs:AofSize
  pub fn aof_size(&self) -> u64 {
    self
      .get_databases_snapshot()
      .iter()
      .map(GarnetDatabase::aof_size)
      .sum()
  }

  /// 集群通告端点
  ///
  /// 缺口说明：C# 依赖 GarnServerTcp 监听端点与集群通告配置（servers /
  /// cluster 域）；本域无 TCP 面信息，返回 None，由调用方按配置域解析。
  ///
  /// libs/server/StoreWrapper.cs:GetClusterEndpoint
  pub fn get_cluster_endpoint(&self) -> Option<String> {
    None
  }

  /// 拍检查点（`db_id` -1 为全部库），返回是否真正执行
  ///
  /// libs/server/StoreWrapper.cs:TakeCheckpointAsync
  pub async fn take_checkpoint_async(&self, background: bool, db_id: i64) -> wkv::Result<bool> {
    for_manager!(self, |m| m.take_checkpoint_async(background, db_id).await)
  }

  /// 按需检查点：距上次保存早于 `entry_ms` 才执行
  ///
  /// libs/server/StoreWrapper.cs:TakeOnDemandCheckpointAsync
  pub async fn take_on_demand_checkpoint_async(&self, entry_ms: u64, db_id: i64) -> wkv::Result<()> {
    for_manager!(self, |m| m
      .take_on_demand_checkpoint_async(entry_ms, db_id)
      .await)
  }

  /// 恢复检查点（可选指定令牌）
  ///
  /// libs/server/StoreWrapper.cs:RecoverCheckpointAsync
  pub async fn recover_checkpoint_async(
    &self,
    replica_recover: bool,
    recover_from_token: Option<u128>,
  ) -> wkv::Result<()> {
    for_manager!(self, |m| m
      .recover_checkpoint_async(replica_recover, recover_from_token)
      .await)
  }

  /// 尝试暂停检查点调度
  ///
  /// libs/server/StoreWrapper.cs:TryPauseCheckpoints
  pub fn try_pause_checkpoints(&self, db_id: i64) -> bool {
    for_manager!(self, |m| m.try_pause_checkpoints(db_id))
  }

  /// 恢复检查点调度
  ///
  /// libs/server/StoreWrapper.cs:ResumeCheckpoints
  pub fn resume_checkpoints(&self, db_id: i64) {
    for_manager!(self, |m| m.resume_checkpoints(db_id));
  }

  /// 恢复 AOF（从上次保存点重放到当前尾）
  ///
  /// libs/server/StoreWrapper.cs:RecoverAOFAsync
  pub async fn recover_aof_async(&self) -> wkv::Result<u64> {
    for_manager!(self, |m| m.recover_aof_async().await)
  }

  /// 重放 AOF 至指定地址
  ///
  /// libs/server/StoreWrapper.cs:ReplayAOF
  pub async fn replay_aof(&self, until: u64) -> wkv::Result<u64> {
    for_manager!(self, |m| m.replay_aof(until).await)
  }

  /// 入队一次 AOF 提交请求（`version` 为提交水位地址）
  ///
  /// libs/server/StoreWrapper.cs:EnqueueCommit
  pub fn enqueue_commit(&self, _entry_type: AofEntryType, version: u64, db_id: i64) {
    self.commit_head_address.store(version, Relaxed);
    for_manager!(self, |m| m.enqueue_commit(db_id, version));
  }

  /// 等待提交水位被 AOF 刷盘闭环
  ///
  /// 缺口说明：waof 写路径同步闭环（enqueue 即持久化排队），无独立后台
  /// 提交器可等待；此处仅校验水位合法性并返回闭环结果。
  ///
  /// libs/server/StoreWrapper.cs:WaitForCommitAsync
  pub fn wait_for_commit_async(&self) -> bool {
    let head = self.commit_head_address.load(Relaxed);
    self
      .get_databases_snapshot()
      .iter()
      .all(|db| db.aof.as_ref().is_none_or(|aof| aof.tail_address() >= head))
  }

  /// 提交 AOF（刷盘 + 推进提交地址）
  ///
  /// libs/server/StoreWrapper.cs:CommitAOFAsync
  pub fn commit_aof_async(&self, db_id: i64) -> wkv::Result<()> {
    for_manager!(self, |m| m.commit_to_aof_async(db_id))
  }

  /// 创建会话函数状态（`db_id` 非零要求多库模式）
  ///
  /// libs/server/StoreWrapper.cs:CreateFunctionsState
  pub fn create_functions_state(&self, db_id: i64) -> wkv::Result<FunctionsState> {
    if db_id != 0 && !self.check_multi_database_compatibility() {
      return Err(wkv::Error::InvalidConfig(format!(
        "CreateFunctionsState 需要多库模式: db_id={db_id}"
      )));
    }
    Ok(for_manager!(self, |m| m.create_functions_state(db_id)))
  }

  /// 全部活跃库快照
  ///
  /// libs/server/StoreWrapper.cs:GetDatabasesSnapshot
  pub fn get_databases_snapshot(&self) -> Vec<Arc<GarnetDatabase<D>>> {
    for_manager!(self, |m| m.get_databases_snapshot())
  }

  /// 取库（`db_id` 非零要求多库模式）
  ///
  /// libs/server/StoreWrapper.cs:TryGetDatabase
  pub fn try_get_database(&self, db_id: i64) -> Option<Arc<GarnetDatabase<D>>> {
    if db_id != 0 && !self.check_multi_database_compatibility() {
      return None;
    }
    for_manager!(self, |m| m.try_get_database(db_id))
  }

  /// 取库或新建（`db_id` 非零要求多库模式）
  ///
  /// libs/server/StoreWrapper.cs:TryGetOrAddDatabase
  pub fn try_get_or_add_database(
    &self,
    db_id: i64,
  ) -> wkv::Result<Option<(Arc<GarnetDatabase<D>>, bool)>> {
    if db_id != 0 && !self.check_multi_database_compatibility() {
      return Ok(None);
    }
    for_manager!(self, |m| m.try_get_or_add_database(db_id).map(Some))
  }

  /// 清空指定库（`unsafe_truncate_log` 直通管理器清空语义）
  ///
  /// libs/server/StoreWrapper.cs:FlushDatabase
  pub async fn flush_database(&self, _unsafe_truncate_log: bool, db_id: i64) -> wkv::Result<()> {
    for_manager!(self, |m| m.flush_database(db_id).await)
  }

  /// 清空全部库
  ///
  /// libs/server/StoreWrapper.cs:FlushAllDatabases
  pub async fn flush_all_databases(&self, _unsafe_truncate_log: bool) -> wkv::Result<()> {
    for_manager!(self, |m| m.flush_all_databases().await)
  }

  /// 交换两个库（单库模式恒 false）
  ///
  /// libs/server/StoreWrapper.cs:TrySwapDatabases
  pub async fn try_swap_databases(&self, db_id1: i64, db_id2: i64) -> bool {
    for_manager!(self, |m| m.try_swap_databases(db_id1, db_id2).await)
  }

  /// 重置复活化统计
  ///
  /// libs/server/StoreWrapper.cs:ResetRevivificationStats
  pub fn reset_revivification_stats(&self) {
    for_manager!(self, |m| m.reset_revivification_stats());
  }

  /// AOF 达限自动检查点
  ///
  /// libs/server/StoreWrapper.cs:AutoCheckpointBasedOnAofSizeLimitAsync
  pub async fn auto_checkpoint_based_on_aof_size_limit_async(
    &self,
    aof_size_limit: u64,
  ) -> wkv::Result<()> {
    for_manager!(self, |m| m
      .task_checkpoint_based_on_aof_size_limit_async(aof_size_limit)
      .await)
  }

  /// 提交任务单次迭代（提交任务体由 taskmanager 域调度）
  ///
  /// libs/server/StoreWrapper.cs:CommitTaskAsync
  pub fn commit_task_async(&self) -> wkv::Result<()> {
    self.commit_aof_async(-1)
  }

  /// 过期键删除扫描任务单次迭代，返回 (删除数, 扫描记录数)
  ///
  /// libs/server/StoreWrapper.cs:ExpiredKeyDeletionScanTaskAsync
  pub async fn expired_key_deletion_scan_task_async(&self) -> wkv::Result<(u64, u64)> {
    let mut deleted = 0u64;
    let mut scanned = 0u64;
    for db in self.get_databases_snapshot() {
      // 库基座入口：经管理器具体类型访问共享基座
      let (d, s) = match &self.database_manager {
        DatabaseManager::Single(m) => m.base.store_expired_key_deletion_scan(&db).await?,
        DatabaseManager::Multi(m) => m.base.store_expired_key_deletion_scan(&db).await?,
      };
      deleted += d;
      scanned += s;
    }
    Ok((deleted, scanned))
  }

  /// 索引自动增长任务单次迭代，返回是否发生增长
  ///
  /// libs/server/StoreWrapper.cs:IndexAutoGrowTaskAsync
  pub fn index_auto_grow_task_async(&self) -> wkv::Result<bool> {
    for_manager!(self, |m| m.grow_indexes_if_needed_async())
  }

  /// 混合日志分布扫描（各库地址分布统计）
  ///
  /// libs/server/StoreWrapper.cs:HybridLogDistributionScan
  pub async fn hybrid_log_distribution_scan(&self) -> wkv::Result<Vec<(i64, HybridLogStats)>> {
    for_manager!(self, |m| m.collect_hybrid_log_stats().await)
  }

  /// 启动大小追踪器
  ///
  /// libs/server/StoreWrapper.cs:StartSizeTrackers
  pub fn start_size_trackers(&self) {
    for_manager!(self, |m| m.start_size_trackers());
  }

  /// 是否存在键落在给定集群槽位
  ///
  /// libs/server/StoreWrapper.cs:HasKeysInSlots
  pub fn has_keys_in_slots(&self, slots: &[u16]) -> bool {
    for db in self.get_databases_snapshot() {
      let Ok(session) = db.store.new_session() else {
        continue;
      };
      session.set_active_db(db.id.max(0) as u64);
      let batch = session.enter_batch();
      let ss = StorageSession::new(batch);
      let mut found = false;
      let _ = ss.iterate_store(|user_key, _| {
        if slots.contains(&cluster_slot(user_key)) {
          found = true;
          return false;
        }
        true
      });
      if found {
        return true;
      }
    }
    false
  }

  /// 多库兼容性检查（db_id 非零操作的前置门槛）
  ///
  /// libs/server/StoreWrapper.cs:CheckMultiDatabaseCompatibility
  pub fn check_multi_database_compatibility(&self) -> bool {
    self.multi_database
  }

  /// 一致性读强制开关
  ///
  /// 缺口说明：C# 由 serverOptions.DisableConsistentRead 决定；server
  /// options 域为并行转写域，本域按集群语义缺省返回 true（强制一致读）。
  ///
  /// libs/server/StoreWrapper.cs:EnforceConsistentRead
  pub fn enforce_consistent_read(&self) -> bool {
    true
  }

  /// 挂起仅主节点任务
  ///
  /// 缺口说明：任务调度面属 taskmanager 域；存储侧无在途主任务需挂起，
  /// 返回 true（已无任务）。
  ///
  /// libs/server/StoreWrapper.cs:SuspendPrimaryOnlyTasksAsync
  pub fn suspend_primary_only_tasks_async(&self) -> bool {
    true
  }

  /// 挂起仅副本任务
  ///
  /// 缺口说明：同 [`Self::suspend_primary_only_tasks_async`]。
  ///
  /// libs/server/StoreWrapper.cs:SuspendReplicaOnlyTasksAsync
  pub fn suspend_replica_only_tasks_async(&self) -> bool {
    true
  }

  /// 启动主节点任务组
  ///
  /// 缺口说明：任务循环属 taskmanager 域；提交 / 对象回收 / 过期扫描均
  /// 提供单次迭代入口（commit_task_async 等）供调度域驱动。
  ///
  /// libs/server/StoreWrapper.cs:StartPrimaryTasks
  pub fn start_primary_tasks(&self) {}

  /// 尝试启动提交任务
  ///
  /// 缺口说明：taskmanager 域未接线，返回 false（未启动）。
  ///
  /// libs/server/StoreWrapper.cs:TryStartCommitTask
  pub fn try_start_commit_task(&self) -> bool {
    false
  }

  /// 尝试启动对象回收任务
  ///
  /// 缺口说明：taskmanager 域未接线，返回 false（未启动）。
  ///
  /// libs/server/StoreWrapper.cs:TryStartObjectCollectTask
  pub fn try_start_object_collect_task(&self) -> bool {
    false
  }

  /// 尝试启动过期键删除任务
  ///
  /// 缺口说明：taskmanager 域未接线，返回 false（未启动）。
  ///
  /// libs/server/StoreWrapper.cs:TryStartExpiredKeyDeletionTask
  pub fn try_start_expired_key_deletion_task(&self) -> bool {
    false
  }

  /// 主任务调和（任务退出后的重启判定）
  ///
  /// 缺口说明：调度策略属 taskmanager 域，本域空操作。
  ///
  /// libs/server/StoreWrapper.cs:ReconcilePrimaryTask
  pub fn reconcile_primary_task(&self) {}

  /// 应用 AOF 同步最大滞后字节数（复制面配置热更新）
  ///
  /// 缺口说明：复制链路属 aof/replication 域；本域仅接受配置值，空操作。
  ///
  /// libs/server/StoreWrapper.cs:ApplyAofSyncMaxLagBytes
  pub fn apply_aof_sync_max_lag_bytes(&self, _new_value_bytes: u64) {}

  /// 启动副本任务组
  ///
  /// 缺口说明：复制调度属 aof / cluster 域，本域空操作。
  ///
  /// libs/server/StoreWrapper.cs:StartReplicaTasks
  pub fn start_replica_tasks(&self) {}

  /// 启动通用节点任务组
  ///
  /// 缺口说明：同 [`Self::start_replica_tasks`]。
  ///
  /// libs/server/StoreWrapper.cs:StartGenericNodeTasks
  pub fn start_generic_node_tasks(&self) {}

  /// 检查点目录便捷构造（默认库目录）
  pub fn default_checkpoint_dir(root: &PathBuf) -> PathBuf {
    root.join("checkpoints")
  }
}
