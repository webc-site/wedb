//! 单库管理器（对标 libs/server/Databases/SingleDatabaseManager.cs）
//!
//! 固定 DB 0 的管理模式：检查点 / AOF / 恢复全部落在唯一数据库上。

use std::{
  path::PathBuf,
  sync::{Arc, atomic::Ordering::Relaxed},
};

use wdev::Device;

use super::{
  database_manager_base::DatabaseManagerBase,
  garnet_database::GarnetDatabase,
  i_database_manager::{HybridLogStats, IDatabaseManager},
};
use crate::storage::functions::functions_state::FunctionsState;

/// 单库管理器
pub struct SingleDatabaseManager<D: Device> {
  /// 共享基座（检查点管理器等）
  pub base: DatabaseManagerBase<D>,
  /// 唯一数据库（DB 0）
  pub db: Arc<GarnetDatabase<D>>,
}

impl<D: Device> SingleDatabaseManager<D> {
  /// 创建单库管理器
  pub fn new(checkpoint_dir: PathBuf, db: Arc<GarnetDatabase<D>>) -> Self {
    Self {
      base: DatabaseManagerBase::new(checkpoint_dir),
      db,
    }
  }

  /// 单库 TryGetOrAddDatabase（恒 db0，不新建）
  ///
  /// libs/server/Databases/SingleDatabaseManager.cs:TryGetOrAddDatabase
  pub fn try_get_or_add_database(&self) -> wkv::Result<(Arc<GarnetDatabase<D>>, bool)> {
    self.base.try_get_or_add_database(&self.db)
  }

  /// 单库恢复检查点
  ///
  /// libs/server/Databases/SingleDatabaseManager.cs:RecoverCheckpointAsync
  /// 保留 _replica_recover 形参以匹配 SingleDatabaseManager.RecoverCheckpointAsync 签名规范
  pub async fn recover_checkpoint(
    &self,
    _replica_recover: bool,
    recover_from_token: Option<u128>,
  ) -> wkv::Result<()> {
    // 副本恢复与主恢复同以快照为基准（差异由 AOF 追平承担）
    self
      .base
      .recover_database_checkpoint_async(&self.db, recover_from_token)
      .await
      .map(|_checkpoint| ())
  }

  /// 单库暂停检查点
  ///
  /// libs/server/Databases/SingleDatabaseManager.cs:TryPauseCheckpoints
  pub fn try_pause_checkpoints(&self) -> bool {
    self.base.try_pause_checkpoints(&self.db)
  }

  /// 单库恢复检查点调度
  ///
  /// libs/server/Databases/SingleDatabaseManager.cs:ResumeCheckpoints
  pub fn resume_checkpoints(&self) {
    self.base.resume_checkpoints(&self.db);
  }

  /// 单库拍检查点（带暂停互斥）
  ///
  /// libs/server/Databases/SingleDatabaseManager.cs:TakeCheckpointAsync
  /// 保留 _background 形参以匹配 SingleDatabaseManager.TakeCheckpointAsync 签名规范
  pub async fn take_checkpoint(&self, _background: bool) -> wkv::Result<bool> {
    self.base.take_database_checkpoint_async(&self.db).await
  }

  /// 检查点辅助（与按需入口共用内核）
  ///
  /// libs/server/Databases/SingleDatabaseManager.cs:TakeCheckpointHelperAsync
  pub async fn take_checkpoint_helper(&self, entry_ms: u64) -> wkv::Result<bool> {
    self
      .base
      .take_checkpoint_helper_async(&self.db, entry_ms)
      .await
  }

  /// 单库按需检查点
  ///
  /// libs/server/Databases/SingleDatabaseManager.cs:TakeOnDemandCheckpointAsync
  pub async fn take_on_demand_checkpoint(&self, entry_ms: u64) -> wkv::Result<()> {
    self
      .base
      .take_on_demand_checkpoint_async(&self.db, entry_ms)
      .await
  }

  /// AOF 达限检查点
  ///
  /// libs/server/Databases/SingleDatabaseManager.cs:TaskCheckpointBasedOnAofSizeLimitAsync
  pub async fn task_checkpoint_based_on_aof_size_limit(&self, limit: u64) -> wkv::Result<()> {
    self.base.checkpoint_if_aof_exceeds(&self.db, limit).await?;
    Ok(())
  }

  /// 单库 AOF 提交
  ///
  /// libs/server/Databases/SingleDatabaseManager.cs:CommitToAofAsync
  pub async fn commit_to_aof(&self) -> wkv::Result<()> {
    self.base.commit_aof(&self.db).await
  }

  /// 等待单库 AOF 提交完成
  ///
  /// libs/server/Databases/SingleDatabaseManager.cs:WaitForCommitToAofAsync
  pub fn wait_for_commit_to_aof(&self) -> wkv::Result<bool> {
    Ok(self.db.aof.as_ref().is_none_or(|aof| aof.wait_for_commit()))
  }

  /// 单库恢复 AOF
  ///
  /// libs/server/Databases/SingleDatabaseManager.cs:RecoverAOFAsync
  pub async fn recover_aof(&self) -> wkv::Result<u64> {
    self.base.recover_database_aof_async(&self.db).await
  }

  /// 单库重放 AOF
  ///
  /// libs/server/Databases/SingleDatabaseManager.cs:ReplayAOF
  pub async fn replay_aof(&self, until: u64) -> wkv::Result<u64> {
    self.base.replay_database_aof(&self.db, until).await
  }

  /// 单库索引增长
  ///
  /// libs/server/Databases/SingleDatabaseManager.cs:GrowIndexesIfNeededAsync
  pub fn grow_indexes_if_needed(&self) -> wkv::Result<bool> {
    self.base.grow_index_if_needed_async(&self.db)
  }

  /// 单库对象收集（对象信封扫描统计）
  ///
  pub async fn execute_object_collection(&self) -> wkv::Result<usize> {
    self.base.execute_object_collection(&self.db).await
  }

  /// 启动大小追踪器
  ///
  /// libs/server/Databases/SingleDatabaseManager.cs:StartSizeTrackers
  pub fn start_size_trackers(&self) {
    self.db.size_tracker.restart();
  }

  /// 重置复活化统计
  ///
  /// libs/server/Databases/SingleDatabaseManager.cs:ResetRevivificationStats
  pub fn reset_revivification_stats(&self) {
    // wkv 无复活化统计面（wkv index 内部化），空操作
  }

  /// 单库 AOF 提交请求入队
  ///
  /// libs/server/Databases/SingleDatabaseManager.cs:EnqueueCommit
  pub fn enqueue_commit(&self, until: u64) {
    self.db.last_save_store_tail_address.store(until, Relaxed);
  }

  /// 单库快照
  ///
  /// libs/server/Databases/SingleDatabaseManager.cs:GetDatabasesSnapshot
  pub fn get_databases_snapshot(&self) -> Vec<Arc<GarnetDatabase<D>>> {
    vec![Arc::clone(&self.db)]
  }

  /// 单库清空
  ///
  /// libs/server/Databases/SingleDatabaseManager.cs:FlushDatabase
  pub async fn flush_database(&self) -> wkv::Result<()> {
    self.base.reset_database(&self.db).await
  }

  /// 全部清空（单库即 db0）
  ///
  /// libs/server/Databases/SingleDatabaseManager.cs:FlushAllDatabases
  pub async fn flush_all_databases(&self) -> wkv::Result<()> {
    self.flush_database().await
  }

  /// 单库不支持交换（恒 false）
  /// 满足 C# TrySwapDatabases(int db1, int db2) 签名，单库模式保留形参
  ///
  /// libs/server/Databases/SingleDatabaseManager.cs:TrySwapDatabases
  pub async fn try_swap_databases(&self, _db_id1: i64, _db_id2: i64) -> bool {
    false
  }

  /// 创建会话函数状态
  ///
  /// libs/server/Databases/SingleDatabaseManager.cs:CreateFunctionsState
  pub fn create_functions_state(&self) -> FunctionsState {
    FunctionsState::new()
  }

  /// 检查点连续暂停尝试（后台任务挂起语义）
  ///
  /// libs/server/Databases/SingleDatabaseManager.cs:TryPauseCheckpointsContinuousAsync
  pub fn try_pause_checkpoints_continuous(&self) -> bool {
    self.try_pause_checkpoints()
  }

  /// 单库混合日志统计
  ///
  /// libs/server/Databases/SingleDatabaseManager.cs:CollectHybridLogStats
  pub async fn collect_hybrid_log_stats(&self) -> wkv::Result<Vec<(i64, HybridLogStats)>> {
    self.base.collect_hybrid_log_stats(&self.db).await
  }

  /// 安全刷 AOF（推进提交地址到已刷地址）
  ///
  /// libs/server/Databases/SingleDatabaseManager.cs:SafeFlushAOF
  pub fn safe_flush_aof(&self) -> wkv::Result<bool> {
    Ok(match &self.db.aof {
      Some(aof) => {
        if let Some(flushed) = aof.safe_flush_address() {
          self.db.last_save_store_tail_address.store(flushed, Relaxed);
          true
        } else {
          false
        }
      }
      None => false,
    })
  }

  /// 单库恢复向量集合
  ///
  /// 缺口说明：向量引擎域未转写完成（见 storage 域缺口总述），返回 0。
  ///
  /// libs/server/Databases/SingleDatabaseManager.cs:RecoverVectorSets
  pub fn recover_vector_sets(&self) -> wkv::Result<u64> {
    Ok(0)
  }
}

impl<D: Device> IDatabaseManager<D> for SingleDatabaseManager<D> {
  /// 满足 IDatabaseManager trait 接口规范，单库实现始终操作默认 db，保留 _db_id
  async fn try_get_or_add_database(
    &self,
    _db_id: i64,
  ) -> wkv::Result<(Arc<GarnetDatabase<D>>, bool)> {
    SingleDatabaseManager::try_get_or_add_database(self)
  }

  /// 满足 IDatabaseManager trait 接口规范，单库实现始终操作默认 db，保留 _db_id
  fn try_get_database(&self, _db_id: i64) -> Option<Arc<GarnetDatabase<D>>> {
    Some(Arc::clone(&self.db))
  }

  /// 满足 IDatabaseManager trait 接口规范，单库实现始终操作默认 db，保留 _db_id
  fn try_pause_checkpoints(&self, _db_id: i64) -> bool {
    SingleDatabaseManager::try_pause_checkpoints(self)
  }

  /// 满足 IDatabaseManager trait 接口规范，单库实现始终操作默认 db，保留 _db_id
  fn resume_checkpoints(&self, _db_id: i64) {
    SingleDatabaseManager::resume_checkpoints(self);
  }

  async fn recover_checkpoint_async(
    &self,
    replica_recover: bool,
    recover_from_token: Option<u128>,
  ) -> wkv::Result<()> {
    SingleDatabaseManager::recover_checkpoint(self, replica_recover, recover_from_token).await
  }

  /// 满足 IDatabaseManager trait 接口规范，单库实现始终操作默认 db，保留 _db_id
  async fn take_checkpoint_async(&self, background: bool, _db_id: i64) -> wkv::Result<bool> {
    SingleDatabaseManager::take_checkpoint(self, background).await
  }

  /// 满足 IDatabaseManager trait 接口规范，单库实现始终操作默认 db，保留 _db_id
  async fn take_on_demand_checkpoint_async(&self, entry_ms: u64, _db_id: i64) -> wkv::Result<()> {
    SingleDatabaseManager::take_on_demand_checkpoint(self, entry_ms).await
  }

  async fn task_checkpoint_based_on_aof_size_limit_async(
    &self,
    aof_size_limit: u64,
  ) -> wkv::Result<()> {
    SingleDatabaseManager::task_checkpoint_based_on_aof_size_limit(self, aof_size_limit).await
  }

  /// 满足 IDatabaseManager trait 接口规范，单库实现始终操作默认 db，保留 _db_id
  async fn commit_to_aof_async(&self, _db_id: i64) -> wkv::Result<()> {
    SingleDatabaseManager::commit_to_aof(self).await
  }

  /// 满足 IDatabaseManager trait 接口规范，单库实现始终操作默认 db，保留 _db_id
  fn wait_for_commit_to_aof_async(&self, _db_id: i64) -> wkv::Result<bool> {
    SingleDatabaseManager::wait_for_commit_to_aof(self)
  }

  async fn recover_aof_async(&self) -> wkv::Result<u64> {
    SingleDatabaseManager::recover_aof(self).await
  }

  async fn replay_aof(&self, until: u64) -> wkv::Result<u64> {
    SingleDatabaseManager::replay_aof(self, until).await
  }

  fn grow_indexes_if_needed_async(&self) -> wkv::Result<bool> {
    SingleDatabaseManager::grow_indexes_if_needed(self)
  }

  /// 满足 IDatabaseManager trait 接口规范，单库实现始终操作默认 db，保留 _db_id
  async fn execute_object_collection(&self, _db_id: i64) -> wkv::Result<usize> {
    SingleDatabaseManager::execute_object_collection(self).await
  }

  fn start_size_trackers(&self) {
    SingleDatabaseManager::start_size_trackers(self);
  }

  fn reset_revivification_stats(&self) {
    SingleDatabaseManager::reset_revivification_stats(self);
  }

  /// 满足 IDatabaseManager trait 接口规范，单库实现始终操作默认 db，保留 _db_id
  fn enqueue_commit(&self, _db_id: i64, until: u64) {
    SingleDatabaseManager::enqueue_commit(self, until);
  }

  fn get_databases_snapshot(&self) -> Vec<Arc<GarnetDatabase<D>>> {
    SingleDatabaseManager::get_databases_snapshot(self)
  }

  /// 满足 IDatabaseManager trait 接口规范，单库实现始终操作默认 db，保留 _db_id
  async fn flush_database(&self, _db_id: i64) -> wkv::Result<()> {
    SingleDatabaseManager::flush_database(self).await
  }

  async fn flush_all_databases(&self) -> wkv::Result<()> {
    SingleDatabaseManager::flush_all_databases(self).await
  }

  async fn try_swap_databases(&self, db_id1: i64, db_id2: i64) -> bool {
    SingleDatabaseManager::try_swap_databases(self, db_id1, db_id2).await
  }

  /// 满足 IDatabaseManager trait 接口规范，单库实现始终操作默认 db，保留 _db_id
  fn create_functions_state(&self, _db_id: i64) -> FunctionsState {
    SingleDatabaseManager::create_functions_state(self)
  }

  async fn collect_hybrid_log_stats(&self) -> wkv::Result<Vec<(i64, HybridLogStats)>> {
    SingleDatabaseManager::collect_hybrid_log_stats(self).await
  }

  /// 满足 IDatabaseManager trait 接口规范，单库实现始终操作默认 db，保留 _db_id
  fn recover_vector_sets(&self, _db_id: i64) -> wkv::Result<u64> {
    SingleDatabaseManager::recover_vector_sets(self)
  }
}
