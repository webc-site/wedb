//! 单库管理器（对标 libs/server/Databases/SingleDatabaseManager.cs）
//!
//! 固定 DB 0 的管理模式：检查点 / AOF / 恢复全部落在唯一数据库上。

use std::{
  path::PathBuf,
  sync::{Arc, OnceLock, atomic::Ordering::Relaxed},
};

use waof::AofEntryType;
use wdev::Device;
use wkv::Error;

use super::{
  database_manager_base::DatabaseManagerBase, garnet_database::GarnetDatabase,
  i_database_manager::IDatabaseManager,
};
use crate::{
  cluster_provider::ClusterProviderHandle,
  primary_tasks::PrimaryTasks,
  resp::{
    slow_path::SlowFuture,
    vector::vector_manager::{RegistryReclaim, VectorManager},
  },
};

/// 单库管理器
pub struct SingleDatabaseManager<D: Device> {
  /// 共享基座（检查点管理器等）
  pub base: DatabaseManagerBase<D>,
  /// 唯一数据库（DB 0）
  pub db: Arc<GarnetDatabase<D>>,
  /// 清库广播门（集群提供者句柄，装配期注入；None = 单机形态恒视为主库，
  /// 对标 C# standalone provider IsPrimary == true）
  flush_gate: OnceLock<ClusterProviderHandle>,
  /// AOF 体积超限检查点的副本角色门（PrimaryTasks 角色域，装配期注入；
  /// None = 裸构造形态恒视为主角色，与 flush_gate 缺省语义一致）。
  /// 角色位取 PrimaryTasks.replica 而非 ClusterProvider::is_replica()：
  /// 前者挂起点更宽（全量同步 attach、REPLICAOF 指向主端等窗口内 config
  /// 角色可能仍是主），换成后者会放宽保护，副本截断本地 AOF 直接破坏
  /// 主从推流衔接
  primary_tasks: OnceLock<Arc<PrimaryTasks>>,
  /// 向量集合管理器（FLUSH 族登记表域回收联动用，装配期注入；None =
  /// 向量预览未点亮，回收静默跳过）
  vector_manager: OnceLock<Arc<VectorManager>>,
}

impl<D: Device> SingleDatabaseManager<D> {
  /// 创建单库管理器
  pub fn new(checkpoint_dir: PathBuf, db: Arc<GarnetDatabase<D>>) -> Self {
    Self {
      base: DatabaseManagerBase::new(checkpoint_dir),
      db,
      flush_gate: OnceLock::new(),
      primary_tasks: OnceLock::new(),
      vector_manager: OnceLock::new(),
    }
  }

  /// 注入清库广播门（集群装配期一次；boot.rs set_store 同点时序）
  ///
  /// 同一句柄双用：清库广播门（本管理器 flush_gate）与检查点版本切换标记出口
  /// （base.attach_cluster_provider，对标 C# provider 既是 flush 门控源又是
  /// checkpointVersionShift 委托宿主）
  pub fn attach_flush_gate(&self, gate: ClusterProviderHandle) {
    self.base.attach_cluster_provider(gate.clone());
    let _ = self.flush_gate.set(gate);
  }

  /// 注入副本角色门（服务装配期一次；from_parts 与 AOF 点亮、--recover
  /// 重放两处装点注入同一 Arc，保证任何装配形态下臂内读到同一角色位）
  pub fn attach_primary_tasks(&self, tasks: Arc<PrimaryTasks>) {
    let _ = self.primary_tasks.set(tasks);
  }

  /// 注入向量集合管理器（服务装配期一次；FLUSH 族域回收联动）
  pub fn attach_vector_manager(&self, vm: Arc<VectorManager>) {
    let _ = self.vector_manager.set(vm);
  }

  /// 登记表域回收联动（域值与广播条目载荷同源；未注入即静默跳过）
  fn reclaim_registry(&self, reclaim: RegistryReclaim) {
    if let Some(vm) = self.vector_manager.get() {
      vm.reclaim_registry_domain(reclaim);
    }
  }

  /// 当前是否副本角色（角色门缺省裸构造 = 主角色）
  fn is_replica(&self) -> bool {
    self
      .primary_tasks
      .get()
      .is_some_and(|tasks| tasks.is_replica())
  }

  /// 是否主库（广播门缺省单机 = true）
  fn is_primary(&self) -> bool {
    self.flush_gate.get().is_none_or(|gate| gate.is_primary())
  }

  /// libs/server/Databases/SingleDatabaseManager.cs:SafeFlushAOF（Enqueue 段）
  ///
  /// 清库执行段后原子补写 FLUSH 族广播条目：仅主库入队（副本经回放条目
  /// 承接清库，绝不二次入队），载荷 (ns, db) 与数据条目物理键前缀同域。
  /// 入队失败以 AofEnqueue 上抛拒绝命令（清库已生效而 AOF 缺条目 = 主从
  /// 发散面，调用方不得静默吞错）
  ///
  /// C# SafeFlushAOF 的无条件 SafeTruncateAOF 截断段（主副均做）在本层不重复
  /// 出现：FLUSHALL 由 flush_all_databases 的「先截断后入队」承接；Enqueue 段
  /// 对偶即本函数（AOF 侧助手 enqueue_safe_flush_aof_if_primary 为其内联
  /// IsPrimary 门控臂，非独立函数级映射）。
  fn safe_flush_aof(
    &self,
    op_type: AofEntryType,
    unsafe_truncate_log: bool,
    ns: u64,
    db: u64,
  ) -> wkv::Result<()> {
    if let Some(aof) = &self.db.aof {
      aof
        .enqueue_safe_flush_aof_if_primary(self.is_primary(), op_type, unsafe_truncate_log, ns, db)
        .map_err(|e| Error::AofEnqueue(e.to_string()))?;
    }
    Ok(())
  }

  /// 单库 TryGetOrAddDatabase（恒 db0，不新建）
  ///
  /// libs/server/Databases/SingleDatabaseManager.cs:TryGetOrAddDatabase
  pub fn try_get_or_add_database(&self) -> wkv::Result<(Arc<GarnetDatabase<D>>, bool)> {
    self.base.try_get_or_add_database(&self.db)
  }

  /// 单库 TryGetDatabase（恒 db0）
  ///
  /// libs/server/Databases/SingleDatabaseManager.cs:TryGetDatabase
  pub fn try_get_database(&self) -> Option<Arc<GarnetDatabase<D>>> {
    Some(Arc::clone(&self.db))
  }

  /// 上次保存时间（毫秒 Unix 时间戳，0 表示从未保存）
  ///
  /// libs/server/Databases/SingleDatabaseManager.cs:LastSaveTime
  #[inline]
  pub fn last_save_ms(&self) -> u64 {
    self.db.last_save_ms()
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

  /// 重置复活化统计
  ///
  /// libs/server/Databases/SingleDatabaseManager.cs:ResetRevivificationStats
  ///
  /// 单库形态唯一存储句柄直下（C# `Store.ResetRevivificationStats()` 同构）：
  /// 复活账目在 wkv 侧由 `WedbStore::reviv_pool`（wreviv FreeRecordPool 四计数）
  /// 单一承载，本口即 INFO RESETSTAT 的 reviv 臂落点
  pub fn reset_revivification_stats(&self) {
    self.db.store.reset_revivification_stats();
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

  /// 单库恢复向量集合
  ///
  /// 缺口说明：向量引擎域未转写完成（见 storage 域缺口总述），返回 0。
  ///
  /// libs/server/Databases/SingleDatabaseManager.cs:RecoverVectorSets
  pub fn recover_vector_sets(&self) -> wkv::Result<u64> {
    Ok(0)
  }
}

/// 检查点暂停锁 RAII 守卫（作用域结束或 panic 时自动还锁）
pub struct CheckpointPauseGuard<D: Device> {
  manager: Arc<SingleDatabaseManager<D>>,
}

impl<D: Device> Drop for CheckpointPauseGuard<D> {
  fn drop(&mut self) {
    self.manager.resume_checkpoints();
  }
}

impl<D: Device> SingleDatabaseManager<D> {
  /// 尝试获取检查点互斥守卫（RAII 释放），被占用时返回 None
  pub fn try_acquire_checkpoint_guard(self: &Arc<Self>) -> Option<CheckpointPauseGuard<D>> {
    if self.try_pause_checkpoints() {
      Some(CheckpointPauseGuard {
        manager: Arc::clone(self),
      })
    } else {
      None
    }
  }
}

impl<D: Device> SingleDatabaseManager<D> {
  /// 单库拍检查点（带暂停互斥）
  ///
  /// libs/server/Databases/SingleDatabaseManager.cs:TakeCheckpointAsync
  /// 保留 _background 形参以匹配 SingleDatabaseManager.TakeCheckpointAsync 签名规范；
  /// C# 尾段 RunPostCheckpointCleanup 仅清对象堆序列化区间（rust 统一检查点
  /// 模型无对位）。快照磁盘淘汰按形态二分且两轨互斥（对标 C# removeOutdated =
  /// !EnableCluster）：单机形态由检查点内核尾段按代回收（其第 7 步），集群
  /// 形态归复制面 CheckpointStore 读者闸门
  pub async fn take_checkpoint(&self, _background: bool) -> wkv::Result<bool> {
    let ret = self.base.take_database_checkpoint_async(&self.db).await?;
    Ok(ret)
  }

  /// 暂停闸门内的 AOF 超限打点段（副本角色门 + 检查点内核）
  ///
  /// 承接 C# 臂内 try 块闸门段（映射挂在
  /// [`Self::task_checkpoint_based_on_aof_size_limit_async`]，此处不重复
  /// 登记）。独立函数即 C# try 块形态：早退（副本轮空）不跨还闸——还闸
  /// 由调用方无条件执行。LastSave / LastSaveStoreTailAddress 回填由内核
  /// update_last_save 单点承接，此处不重复
  async fn checkpoint_within_pause_gate(
    &self,
    aof_size: u64,
    aof_size_limit: u64,
  ) -> wkv::Result<bool> {
    // 检查点由 AOF 重放侧触发的窗口（副本角色）绝不本地打点
    if self.is_replica() {
      log::info!("Replica skipping TaskCheckpointBasedOnAofSizeLimitAsync");
      return Ok(false);
    }
    log::info!(
      "Enforcing AOF size limit currentAofSize: {aof_size} > AofSizeLimit: {aof_size_limit}"
    );
    self.base.take_database_checkpoint_async(&self.db).await
  }

  /// 等待单库 AOF 提交完成（事件驱动无锁等待）
  ///
  /// libs/server/Databases/SingleDatabaseManager.cs:WaitForCommitToAofAsync
  pub async fn wait_for_commit_to_aof_async(&self) -> wkv::Result<bool> {
    if let Some(aof) = &self.db.aof {
      aof.wait_for_commit_async(0).await;
    }
    Ok(true)
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

  /// 单库清空（换号 + 广播族）
  ///
  /// libs/server/Databases/SingleDatabaseManager.cs:FlushDatabase
  /// （接口成员 IDatabaseManager.cs:FlushDatabase 即本函数承接，rust 不在
  /// [`IDatabaseManager`](super::i_database_manager::IDatabaseManager) 上另开
  /// 零参投影臂）
  ///
  /// RESP FLUSHDB 主路径唯一清库入口（慢路径执行段
  /// `StoreGarnetApi::flush_command_slow`）：
  ///
  /// C# `FlushDatabase(unsafeTruncateLog, dbId)` + `SafeFlushAOF(FlushDb, ...)`：
  /// wkv O(1) 虚拟换号清库后原子补写 FlushDb 广播条目（仅主库入队），载荷
  /// (vns, 旧 vdb) 与数据条目物理键前缀同域。rust 共享单 AOF 不物理截断
  /// （换号模型旧数据由延时 GC 回收，截断将抹除其他库未检查点历史；C# 集群
  /// safeTruncateAof == false 分支同形）。换号映射批次（新 DbMap + 旧域墓碑 +
  /// 分配水位）经 commit_swap 落盘时镜像为 DbMeta 条目先行入队，副本回放按
  /// 主库号直设映射格，绝无本地二次取号（doc/zh/db.md 主从映射继承）
  pub async fn flush_database(
    &self,
    ns: u64,
    db_id: u64,
    unsafe_truncate_log: bool,
  ) -> wkv::Result<()> {
    let (vns, domain_db) = self.db.store.flush_database(ns, db_id).await?;
    // 换号回收：死亡域 (vns, 旧 vdb) 的登记条目与底层 context 一并联动清理
    self.reclaim_registry(RegistryReclaim::Database {
      vns,
      vdb: domain_db,
    });
    self.safe_flush_aof(AofEntryType::FlushDb, unsafe_truncate_log, vns, domain_db)?;
    Ok(())
  }

  /// 清空整命名空间（多租户隔离清库 + 广播）
  ///
  /// C# 无 ns 维度（每实例单租户）；rust 非 0 租户 FLUSHALL 执行段，
  /// FlushNs 广播条目携带 (旧 vns, 0) 域载荷
  pub async fn flush_namespace(&self, ns: u64, unsafe_truncate_log: bool) -> wkv::Result<()> {
    let (domain_vns, _) = self.db.store.flush_namespace(ns).await?;
    self.reclaim_registry(RegistryReclaim::Namespace { vns: domain_vns });
    self.safe_flush_aof(AofEntryType::FlushNs, unsafe_truncate_log, domain_vns, 0)?;
    Ok(())
  }

  /// 全租户换号广播门转发（doc/zh/db.md 4.5 集群总线广播）
  ///
  /// None = 单机形态（flush_gate 未装配，本地换号即完成）；Some 为协调者
  /// 应答闭环字节（收齐全部 Primary ack 才 +OK，失败回错误），入口以
  /// 其结果作为客户端最终应答，严禁先 +OK 再异步广播
  pub fn flushall_broadcast(&self, ns: u64) -> Option<SlowFuture> {
    self
      .flush_gate
      .get()
      .and_then(|gate| gate.flushall_broadcast(ns))
  }

  /// 全部清空（全域物理截断 + 广播）
  ///
  /// libs/server/Databases/SingleDatabaseManager.cs:FlushAllDatabases
  ///
  /// C# `FlushAllDatabases(unsafeTruncateLog)` + `SafeFlushAOF(FlushAll, ...)`：
  /// wkv 物理截断 O(1) 全域清空 + AOF 截断至尾后补写 FlushAll 广播条目
  /// （先截断后入队，与 C# SafeFlushAOF 的 SafeTruncateAOF → Enqueue 同序；
  /// 全域清空无他库存活，共享 AOF 截断安全）
  pub async fn flush_all_databases(&self, unsafe_truncate_log: bool) -> wkv::Result<()> {
    self.base.flush_all_databases(&self.db).await?;
    self.reclaim_registry(RegistryReclaim::All);
    self.safe_flush_aof(AofEntryType::FlushAll, unsafe_truncate_log, 0, 0)?;
    Ok(())
  }

  /// 单库重置（拆除重建族：数据清空 + AOF 位点归零 + 保存点复位）
  ///
  /// libs/server/Databases/SingleDatabaseManager.cs:Reset
  pub async fn reset(&self) -> wkv::Result<()> {
    let ret = self.base.reset_database(&self.db).await;
    self.reclaim_registry(RegistryReclaim::All);
    ret
  }

  /// 单库混合日志内存分布统计
  ///
  /// libs/server/Databases/SingleDatabaseManager.cs:CollectHybridLogStats
  pub async fn collect_hybrid_log_stats(
    &self,
  ) -> wkv::Result<Vec<(i64, wkv::HybridLogScanMetrics)>> {
    self.base.collect_hybrid_log_stats(&self.db).await
  }

  /// 检查并按需自动扩容单库主存储索引
  ///
  /// libs/server/Databases/SingleDatabaseManager.cs:GrowIndexesIfNeededAsync
  pub async fn grow_indexes_if_needed(
    &self,
    index_max_size: usize,
    resize_threshold: i64,
  ) -> wkv::Result<bool> {
    self
      .base
      .grow_indexes_if_needed(&self.db, index_max_size, resize_threshold)
      .await
  }
}

impl<D: Device> IDatabaseManager<D> for SingleDatabaseManager<D> {
  async fn try_get_or_add_database(&self) -> wkv::Result<(Arc<GarnetDatabase<D>>, bool)> {
    SingleDatabaseManager::try_get_or_add_database(self)
  }

  fn try_get_database(&self) -> Option<Arc<GarnetDatabase<D>>> {
    SingleDatabaseManager::try_get_database(self)
  }

  fn last_save_ms(&self) -> u64 {
    SingleDatabaseManager::last_save_ms(self)
  }

  fn try_pause_checkpoints(&self) -> bool {
    SingleDatabaseManager::try_pause_checkpoints(self)
  }

  fn resume_checkpoints(&self) {
    SingleDatabaseManager::resume_checkpoints(self);
  }

  async fn recover_checkpoint_async(
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

  async fn take_checkpoint_async(&self, background: bool) -> wkv::Result<bool> {
    SingleDatabaseManager::take_checkpoint(self, background).await
  }

  async fn take_on_demand_checkpoint_async(&self, entry_ms: u64) -> wkv::Result<()> {
    self
      .base
      .take_on_demand_checkpoint_async(&self.db, entry_ms)
      .await
  }

  /// AOF 体积超限检查点唯一驱动入口（service 周期任务循环直调本臂）
  ///
  /// libs/server/Databases/SingleDatabaseManager.cs:TaskCheckpointBasedOnAofSizeLimitAsync
  /// libs/server/Databases/DatabaseManagerBase.cs:TaskCheckpointBasedOnAofSizeLimitAsync（抽象基类段，实现由本臂覆写）
  ///
  /// 四段次序对标 C#：尺寸预判（严格大于才打点，`aofSize <= aofSizeLimit`
  /// 早退，未超限轮不触碰暂停闸门）→ 取暂停闸门 → 闸门内段（副本角色门 +
  /// 打点）→ 无条件还闸。闸门用 try_pause_checkpoints/resume_checkpoints
  /// 显式对（同名 C# 两法）；Arc 版 CheckpointPauseGuard 保留给 SAVE/BGSAVE
  /// （BGSAVE 需把守卫 move 进 spawn 任务，必须持有 Arc）。
  /// 闸门占用中本轮轮空而非 C# TryPauseCheckpointsContinuousAsync 忙等续等：
  /// 续等由下一轮周期重投承接，避免在 compio 单线程 reactor 上做无界忙等，
  /// 最坏延迟一个检查周期
  async fn task_checkpoint_based_on_aof_size_limit_async(
    &self,
    aof_size_limit: u64,
  ) -> wkv::Result<()> {
    let aof_size = self.db.aof_size();
    if aof_size <= aof_size_limit {
      return Ok(());
    }
    if !self.try_pause_checkpoints() {
      log::debug!("AofSizeLimitTask: checkpoint in progress, skip this round");
      return Ok(());
    }
    let ret = self
      .checkpoint_within_pause_gate(aof_size, aof_size_limit)
      .await;
    self.resume_checkpoints();
    ret?;
    Ok(())
  }

  async fn commit_to_aof_async(&self) -> wkv::Result<()> {
    self.base.commit_aof(&self.db).await
  }

  async fn wait_for_commit_to_aof_async(&self) -> wkv::Result<bool> {
    SingleDatabaseManager::wait_for_commit_to_aof_async(self).await
  }

  async fn recover_aof_async(&self) -> wkv::Result<u64> {
    SingleDatabaseManager::recover_aof(self).await
  }

  async fn replay_aof(&self, until: u64) -> wkv::Result<u64> {
    SingleDatabaseManager::replay_aof(self, until).await
  }

  fn reset_revivification_stats(&self) {
    SingleDatabaseManager::reset_revivification_stats(self);
  }

  fn enqueue_commit(&self, until: u64) {
    SingleDatabaseManager::enqueue_commit(self, until);
  }

  fn get_databases_snapshot(&self) -> Vec<Arc<GarnetDatabase<D>>> {
    SingleDatabaseManager::get_databases_snapshot(self)
  }

  async fn reset(&self) -> wkv::Result<()> {
    SingleDatabaseManager::reset(self).await
  }

  async fn flush_all_databases(&self) -> wkv::Result<()> {
    SingleDatabaseManager::flush_all_databases(self, false).await
  }

  async fn collect_hybrid_log_stats(&self) -> wkv::Result<Vec<(i64, wkv::HybridLogScanMetrics)>> {
    SingleDatabaseManager::collect_hybrid_log_stats(self).await
  }

  fn recover_vector_sets(&self) -> wkv::Result<u64> {
    SingleDatabaseManager::recover_vector_sets(self)
  }

  async fn grow_indexes_if_needed_async(
    &self,
    index_max_size: usize,
    resize_threshold: i64,
  ) -> wkv::Result<bool> {
    SingleDatabaseManager::grow_indexes_if_needed(self, index_max_size, resize_threshold).await
  }
}
