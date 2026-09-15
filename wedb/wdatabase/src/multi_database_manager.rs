//! 多库管理器（对标 libs/server/Databases/MultiDatabaseManager.cs）
//!
//! wkv 单库模型下多逻辑库共享同一 [`WedbStore`]（键前缀隔离）；库注册表为
//! papaya 并发映射，结构换库（SWAPDB）经内容写锁串行化。

use std::{
  fs, io,
  path::PathBuf,
  sync::{Arc, atomic::Ordering::Relaxed},
};

use async_lock::RwLock;
use parking_lot::Mutex;
use wbase::map::{ConcurrentMap, new_concurrent_map};
use wdev::Device;
use wkv::{CheckpointManager, Error, WedbStore};

use super::{
  database_manager_base::DatabaseManagerBase,
  garnet_database::GarnetDatabase,
  i_database_manager::{HybridLogStats, IDatabaseManager},
};
use crate::{aof::DatabaseAof, functions_state::FunctionsState};

/// 多库管理器
pub struct MultiDatabaseManager<D: Device, A: DatabaseAof<D> = ()> {
  /// 共享基座（检查点管理器等）
  pub base: DatabaseManagerBase<D>,
  /// 共享存储引擎
  pub store: Arc<WedbStore<D>>,
  /// 库注册表：db_id -> 库
  pub databases: ConcurrentMap<i64, Arc<GarnetDatabase<D, A>>>,
  /// 库表结构变更锁（SWAPDB / 批量恢复；异步感知，允许持锁跨越内部 await）
  pub content_lock: RwLock<()>,
  /// 共享 AOF 域（None 表示未启用 AOF；对标 C# StoreWrapper 构造期一次性
  /// 创建 appendOnlyFile、全部 GarnetDatabase 共享同一实例——多库单一物理 AOF 域）
  aof_factory: Mutex<Option<Arc<A>>>,
  /// 库检查点目录基路径（实际目录 = base/<db_id>）
  pub checkpoint_root: PathBuf,
}

impl<D: Device, A: DatabaseAof<D>> MultiDatabaseManager<D, A> {
  /// 创建多库管理器（DB 0 立即注册）
  pub fn new(store: Arc<WedbStore<D>>, checkpoint_root: PathBuf) -> Self {
    Self {
      base: DatabaseManagerBase::new(checkpoint_root.join("0")),
      store,
      databases: new_concurrent_map(),
      content_lock: RwLock::new(()),
      aof_factory: Mutex::new(None),
      checkpoint_root,
    }
  }

  /// 挂载共享 AOF 域（后续新建库均启用 AOF，全部共享同一物理日志实例）
  pub fn enable_aof(&self, aof: Arc<A>) {
    *self.aof_factory.lock() = Some(aof);
  }

  /// 库检查点目录
  pub fn checkpoint_dir_of(&self, db_id: i64) -> PathBuf {
    self.checkpoint_root.join(db_id.to_string())
  }

  /// 注册库
  ///
  /// libs/server/Databases/MultiDatabaseManager.cs:TryAddDatabase
  pub fn try_add_database(
    &self,
    db_id: i64,
    store: Arc<WedbStore<D>>,
  ) -> wkv::Result<Arc<GarnetDatabase<D, A>>> {
    let db = Arc::new(GarnetDatabase::new(
      db_id,
      store,
      Arc::clone(&self.store.device),
      self.checkpoint_dir_of(db_id),
      self.create_aof(),
    ));
    self.databases.pin().insert(db_id, Arc::clone(&db));
    Ok(db)
  }

  /// 新库 AOF 域句柄（全库共享同一实例）
  fn create_aof(&self) -> Option<Arc<A>> {
    self.aof_factory.lock().clone()
  }

  /// 新库挂载事件（记录保存基线）
  ///
  /// libs/server/Databases/MultiDatabaseManager.cs:HandleDatabaseAdded
  pub fn handle_database_added(&self, db: &GarnetDatabase<D, A>) {
    db.last_save_store_tail_address
      .store(db.store.tail_address(), Relaxed);
  }

  /// 库表内容写锁（持锁期间注册表串行变更）
  ///
  /// libs/server/Databases/MultiDatabaseManager.cs:TryGetDatabasesContentWriteLock
  pub fn try_get_databases_content_write_lock(
    &self,
  ) -> Option<async_lock::RwLockWriteGuard<'_, ()>> {
    self.content_lock.try_write()
  }

  /// 库表内容读锁
  ///
  /// libs/server/Databases/MultiDatabaseManager.cs:TryGetDatabasesContentReadLock
  pub fn try_get_databases_content_read_lock(&self) -> Option<async_lock::RwLockReadGuard<'_, ()>> {
    self.content_lock.try_read()
  }

  /// 取已持久化的库编号集合（检查点目录下有子目录的编号）
  ///
  /// 两分支对标 libs/server/Databases/MultiDatabaseManager.cs:TryGetSavedDatabaseIds
  /// 与 :RecoverCheckpointAsync / :RecoverAOFAsync 中包裹它的 try/catch：
  /// - 根目录不存在属良性全新启动态，此处返回空集而非报错（debug 级日志留痕）。
  ///   注意上游的良性守卫 `Directory.Exists` 对 stat 级失败一律返回 false——除不存在
  ///   外还包括路径被普通文件占用（NotADirectory）、父目录不可穿越（PermissionDenied）
  ///   等，上游均静默跳过恢复；本实现有意把良性集收窄为仅 NotFound，上述其余 stat 级
  ///   失败落入 fail-loud 分支（部署错误必须可见，而非静默零恢复）；
  /// - 其余枚举失败必须显式报错而非静默返回空集：空集会让 `--recover` 在没有任何
  ///   库被恢复的情况下看似健康地启动（上游 d20d63993 修复的"静默错误恢复"类缺陷，
  ///   日志从 LogInformation 提级到 LogError 并尊重 FailOnRecoveryError 抛出）。
  ///   Rust 侧错误即返回值，记录 error 日志后向调用方传播（恒为 fail-loud 语义，
  ///   强于上游 FailOnRecoveryError=false 时"记日志后放弃恢复"的默认分支）。
  pub fn try_get_saved_database_ids(&self) -> wkv::Result<Vec<i64>> {
    let mut ids = Vec::new();
    let entries = match fs::read_dir(&self.checkpoint_root) {
      Ok(entries) => entries,
      // 根目录尚未创建：无任何已持久化库可枚举，良性空集（上游 Directory.Exists 守卫；
      // 上游对其它 stat 级失败也静默，本实现有意报错，见函数文档的良性集收窄说明）
      Err(e) if e.kind() == io::ErrorKind::NotFound => {
        log::debug!(
          "检查点根目录不存在，按空集处理: checkpoint_root = {}",
          self.checkpoint_root.display()
        );
        return Ok(ids);
      }
      Err(e) => {
        log::error!(
          "枚举已持久化库编号失败: checkpoint_root = {}; err = {e}",
          self.checkpoint_root.display()
        );
        return Err(Error::from(e));
      }
    };
    for entry in entries.flatten() {
      if entry.path().is_dir()
        && let Ok(n) = entry.file_name().into_string()
        && let Ok(id) = n.parse::<i64>()
      {
        ids.push(id);
      }
    }
    ids.sort_unstable();
    Ok(ids)
  }

  /// 暂停全部检查点（成功则返回释放闭包所需标志）
  ///
  /// libs/server/Databases/MultiDatabaseManager.cs:RunPausedCheckpointsAndReleaseLocksAsync
  pub async fn run_paused_checkpoints_and_release_locks(&self) -> wkv::Result<()> {
    let pin = self.databases.pin();
    for (_, db) in pin.iter() {
      self.base.resume_checkpoints(db);
    }
    Ok(())
  }

  /// 对单个库拍检查点
  ///
  /// 对单个库拍检查点
  ///
  /// libs/server/Databases/MultiDatabaseManager.cs:TakeOneCheckpointAsync
  pub async fn take_one_checkpoint(&self, db: &GarnetDatabase<D, A>) -> wkv::Result<bool> {
    self.base.take_database_checkpoint_async(db).await
  }

  /// 按 id 取库
  ///
  /// libs/server/Databases/MultiDatabaseManager.cs:GetDbById
  pub fn get_db_by_id(&self, db_id: i64) -> Option<Arc<GarnetDatabase<D, A>>> {
    self.databases.pin().get(&db_id).cloned()
  }

  /// 异步等待多库 AOF 提交落盘（若 db_id < 0 则等待所有活跃库；对标 C# MultiDatabaseManager.WaitForCommitToAofAsync）
  pub async fn wait_for_commit_to_aof_async(&self, db_id: i64) -> wkv::Result<bool> {
    let aofs: Vec<Arc<A>> = {
      let _guard = self.content_lock.read().await;
      let pin = self.databases.pin();
      if db_id >= 0 {
        pin
          .get(&db_id)
          .and_then(|db| db.aof.as_ref().map(Arc::clone))
          .into_iter()
          .collect()
      } else {
        let mut list = Vec::new();
        for (_, db) in pin.iter() {
          if let Some(aof) = &db.aof
            && !list.iter().any(|existing| Arc::ptr_eq(existing, aof))
          {
            list.push(Arc::clone(aof));
          }
        }
        list
      }
    };
    for aof in aofs {
      aof.wait_for_commit_async(0).await;
    }
    Ok(true)
  }
}

impl<D: Device, A: DatabaseAof<D>> IDatabaseManager<D> for MultiDatabaseManager<D, A> {
  type A = A;

  async fn try_get_or_add_database(
    &self,
    db_id: i64,
  ) -> wkv::Result<(Arc<GarnetDatabase<D, A>>, bool)> {
    {
      let pin = self.databases.pin();
      if let Some(db) = pin.get(&db_id) {
        return Ok((Arc::clone(db), false));
      }
    }
    let _guard = self.content_lock.write().await;
    // 双检：写锁期间可能已被并发注册
    let pin = self.databases.pin();
    if let Some(db) = pin.get(&db_id) {
      return Ok((Arc::clone(db), false));
    }
    drop(pin);
    let db = self.try_add_database(db_id, Arc::clone(&self.store))?;
    self.handle_database_added(&db);
    Ok((db, true))
  }

  fn try_get_database(&self, db_id: i64) -> Option<Arc<GarnetDatabase<D, A>>> {
    self.get_db_by_id(db_id)
  }

  fn try_pause_checkpoints(&self, db_id: i64) -> bool {
    self
      .get_db_by_id(db_id)
      .is_some_and(|db| self.base.try_pause_checkpoints(&db))
  }

  fn resume_checkpoints(&self, db_id: i64) {
    if let Some(db) = self.get_db_by_id(db_id) {
      self.base.resume_checkpoints(&db);
    }
  }

  /// 保留 _replica_recover 形参以匹配 MultiDatabaseManager.RecoverCheckpointAsync 签名规范
  async fn recover_checkpoint_async(
    &self,
    _replica_recover: bool,
    recover_from_token: Option<u128>,
  ) -> wkv::Result<()> {
    // 登记已持久化库并逐库恢复 + AOF 追平。
    // 目录不存在的良性空集已在 try_get_saved_database_ids 源头消化；其余枚举失败
    // 向上传播，不允许静默空集恢复——对标上游 d20d63993 后 RecoverCheckpointAsync
    // 对 ids 枚举错误的 Error 级日志 + FailOnRecoveryError 抛出语义。
    for db_id in self.try_get_saved_database_ids()? {
      let (db, _) = self.try_get_or_add_database(db_id).await?;
      if let Some(token) = recover_from_token.or_else(|| {
        wkv::CheckpointManager::<D>::find_latest_checkpoint(&db.checkpoint_dir)
          .ok()
          .flatten()
      }) {
        CheckpointManager::recover(&db.checkpoint_dir, token, Arc::clone(&db.device))
          .await
          .map_err(Error::from)?;
      }
      self.base.recover_database_aof_async(&db).await?;
    }
    Ok(())
  }

  /// 满足 IDatabaseManager trait 接口规范，多库检查点保留 _background 参数
  async fn take_checkpoint_async(&self, _background: bool, db_id: i64) -> wkv::Result<bool> {
    let mut taken = false;
    if db_id < 0 {
      for db in self.get_databases_snapshot() {
        taken |= self.take_one_checkpoint(&db).await?;
        self.base.run_post_checkpoint_cleanup(&db)?;
      }
    } else if let Some(db) = self.get_db_by_id(db_id) {
      taken = self.take_one_checkpoint(&db).await?;
      self.base.run_post_checkpoint_cleanup(&db)?;
    }
    Ok(taken)
  }

  async fn take_on_demand_checkpoint_async(&self, entry_ms: u64, db_id: i64) -> wkv::Result<()> {
    if let Some(db) = self.get_db_by_id(db_id) {
      self
        .base
        .take_on_demand_checkpoint_async(&db, entry_ms)
        .await?;
    }
    Ok(())
  }

  async fn task_checkpoint_based_on_aof_size_limit_async(
    &self,
    aof_size_limit: u64,
  ) -> wkv::Result<()> {
    for (_, db) in self.databases.pin().iter() {
      self
        .base
        .checkpoint_if_aof_exceeds(db, aof_size_limit)
        .await?;
    }
    Ok(())
  }

  async fn commit_to_aof_async(&self, db_id: i64) -> wkv::Result<()> {
    if db_id < 0 {
      for (_, db) in self.databases.pin().iter() {
        self.base.commit_aof(db).await?;
      }
    } else if let Some(db) = self.get_db_by_id(db_id) {
      self.base.commit_aof(&db).await?;
    }
    Ok(())
  }

  async fn wait_for_commit_to_aof_async(&self, db_id: i64) -> wkv::Result<bool> {
    MultiDatabaseManager::wait_for_commit_to_aof_async(self, db_id).await
  }

  async fn recover_aof_async(&self) -> wkv::Result<u64> {
    let mut total = 0u64;
    for (_, db) in self.databases.pin().iter() {
      total += self.base.recover_database_aof_async(db).await?;
    }
    Ok(total)
  }

  async fn replay_aof(&self, until: u64) -> wkv::Result<u64> {
    let mut total = 0u64;
    for (_, db) in self.databases.pin().iter() {
      total += self.base.replay_database_aof(db, until).await?;
    }
    Ok(total)
  }

  fn grow_indexes_if_needed_async(&self) -> wkv::Result<bool> {
    self.databases.pin().iter().try_fold(false, |acc, (_, db)| {
      self.base.grow_index_if_needed_async(db).map(|g| acc | g)
    })
  }

  async fn execute_object_collection(&self, db_id: i64) -> wkv::Result<usize> {
    let mut n = 0usize;
    for (_, db) in self.databases.pin().iter() {
      if db_id < 0 || db.id == db_id {
        n += self.base.execute_object_collection(db).await?;
      }
    }
    Ok(n)
  }

  fn start_size_trackers(&self) {
    for (_, db) in self.databases.pin().iter() {
      db.size_tracker.restart();
    }
  }

  fn reset_revivification_stats(&self) {
    // wkv 无复活化统计面（引擎内部化），空操作
  }

  fn enqueue_commit(&self, db_id: i64, until: u64) {
    if let Some(db) = self.get_db_by_id(db_id) {
      db.last_save_store_tail_address.store(until, Relaxed);
    }
  }

  fn get_databases_snapshot(&self) -> Vec<Arc<GarnetDatabase<D, A>>> {
    self
      .databases
      .pin()
      .iter()
      .map(|(_, v)| Arc::clone(v))
      .collect()
  }

  async fn flush_database(&self, db_id: i64) -> wkv::Result<()> {
    if let Some(db) = self.get_db_by_id(db_id) {
      self.base.reset_database(&db).await?;
    }
    Ok(())
  }

  async fn flush_all_databases(&self) -> wkv::Result<()> {
    let dbs = self.get_databases_snapshot();
    for db in dbs {
      self.base.reset_database(&db).await?;
    }
    Ok(())
  }

  async fn try_swap_databases(&self, db_id1: i64, db_id2: i64) -> bool {
    swap_impl::swap(self, db_id1, db_id2).await
  }

  /// 满足 IDatabaseManager trait 接口规范，多库实现保留 _db_id 参数
  fn create_functions_state(&self, _db_id: i64) -> FunctionsState {
    FunctionsState::new()
  }

  async fn collect_hybrid_log_stats(&self) -> wkv::Result<Vec<(i64, HybridLogStats)>> {
    let mut out = Vec::new();
    for (_, db) in self.databases.pin().iter() {
      let stats = self.base.collect_hybrid_log_stats_for_db(db).await?;
      out.push((db.id, stats));
    }
    out.sort_by_key(|(id, _)| *id);
    Ok(out)
  }

  /// 满足 IDatabaseManager trait 接口规范，多库实现保留 _db_id 参数
  fn recover_vector_sets(&self, _db_id: i64) -> wkv::Result<u64> {
    Ok(0)
  }
}

/// SWAPDB 共享实现（trait impl 与固有方法共用）
mod swap_impl {
  use std::sync::atomic::Ordering::Relaxed;

  use wdev::Device;

  use super::MultiDatabaseManager;
  use crate::aof::DatabaseAof;

  /// 交换两库（共享存储模型：数据以 db 前缀物理隔离，须真实搬移键值）
  ///
  /// 数据面内核见 wkv `StoreSession::swap_databases`（全 tag 域搬移：字符串 /
  /// 对象信封 / RangeIndex 元记录 + 随键 TTL/ETag；物理写经写端口自动镜像
  /// AOF，重放端与主端收敛）。本层职责对齐 C# TrySwapDatabases 的容器面：
  /// content_lock 写锁串行化库表结构变更 + lastSave 元数据随容器交换
  /// （C# `copyLastSaveData: true`）。
  ///
  /// 活跃会话门控在命令面（wnode，对齐 C# 遍历 ActiveConsumers 统计
  /// RespServerSession 数 > 1 时回错拒绝）；本 trait 面为存储执行域。
  ///
  /// libs/server/Databases/MultiDatabaseManager.cs:TrySwapDatabases
  pub(super) async fn swap<D: Device, A: DatabaseAof<D>>(
    manager: &MultiDatabaseManager<D, A>,
    db_id1: i64,
    db_id2: i64,
  ) -> bool {
    if db_id1 == db_id2 {
      return true;
    }
    let db1 = manager.get_db_by_id(db_id1);
    let db2 = manager.get_db_by_id(db_id2);
    let (Some(db1), Some(db2)) = (db1, db2) else {
      return false;
    };
    let _guard = manager.content_lock.write().await;
    // 真实搬移（C# 交换 GarnetDatabase 容器指针的共享存储面等价）：
    // 收集 → 双库清源 → 交叉重写，任一存储错误即失败口径（C# return false）
    let Ok(session) = manager.store.new_session() else {
      return false;
    };
    if session.swap_databases(db_id1, db_id2).await.is_err() {
      return false;
    }
    // lastSave 元数据随容器交换（C# copyLastSaveData: true）
    let (last_save1, tail1) = (
      db1.last_save_ms.load(Relaxed),
      db1.last_save_store_tail_address.load(Relaxed),
    );
    let (last_save2, tail2) = (
      db2.last_save_ms.load(Relaxed),
      db2.last_save_store_tail_address.load(Relaxed),
    );
    db1.last_save_ms.store(last_save2, Relaxed);
    db1.last_save_store_tail_address.store(tail2, Relaxed);
    db2.last_save_ms.store(last_save1, Relaxed);
    db2.last_save_store_tail_address.store(tail1, Relaxed);
    true
  }
}
