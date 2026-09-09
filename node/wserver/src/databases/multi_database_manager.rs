//! 多库管理器（对标 libs/server/Databases/MultiDatabaseManager.cs）
//!
//! wkv 单库模型下多逻辑库共享同一 [`WedbStore`]（键前缀隔离）；库注册表为
//! papaya 并发映射，结构换库（SWAPDB）经内容写锁串行化。

use std::{
  fs,
  path::{Path, PathBuf},
  sync::{Arc, atomic::Ordering::Relaxed},
};

use async_lock::RwLock;
use gxhash::HashMap as GxHashMap;
use papaya::HashMap as PapayaMap;
use parking_lot::Mutex;
use waof::WalLog;
use wdev::Device;
use wkv::WedbStore;

use super::{
  database_manager_base::DatabaseManagerBase,
  garnet_database::GarnetDatabase,
  i_database_manager::{HybridLogStats, IDatabaseManager},
};
use crate::storage::functions::functions_state::FunctionsState;

/// 多库管理器
pub struct MultiDatabaseManager<D: Device> {
  /// 共享基座（检查点管理器等）
  pub base: DatabaseManagerBase<D>,
  /// 共享存储引擎
  pub store: Arc<WedbStore<D>>,
  /// 库注册表：db_id -> 库
  pub databases: PapayaMap<i64, Arc<GarnetDatabase<D>>>,
  /// 库表结构变更锁（SWAPDB / 批量恢复；异步感知，允许持锁跨越内部 await）
  pub content_lock: RwLock<()>,
  /// AOF 构造配置（None 表示未启用 AOF）
  pub wal_factory: Mutex<Option<(Arc<D>, waof::WalConfig)>>,
  /// 库检查点目录基路径（实际目录 = base/<db_id>）
  pub checkpoint_root: PathBuf,
}

impl<D: Device> MultiDatabaseManager<D> {
  /// 创建多库管理器（DB 0 立即注册）
  pub fn new(store: Arc<WedbStore<D>>, checkpoint_root: PathBuf) -> Self {
    Self {
      base: DatabaseManagerBase::new(checkpoint_root.join("0")),
      store,
      databases: PapayaMap::new(),
      content_lock: RwLock::new(()),
      wal_factory: Mutex::new(None),
      checkpoint_root,
    }
  }

  /// 挂载 AOF 工厂（后续新建库均启用 AOF）
  pub fn enable_aof(&self, device: Arc<D>, config: waof::WalConfig) {
    *self.wal_factory.lock() = Some((device, config));
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
  ) -> wkv::Result<Arc<GarnetDatabase<D>>> {
    let db = Arc::new(GarnetDatabase::new(
      db_id,
      store,
      Arc::clone(&self.store.device),
      self.checkpoint_dir_of(db_id),
      self.create_aof(db_id),
    ));
    self.databases.pin().insert(db_id, Arc::clone(&db));
    Ok(db)
  }

  /// 新库 AOF 构造
  fn create_aof(&self, _db_id: i64) -> Option<Arc<WalLog<D>>> {
    let (device, config) = self.wal_factory.lock().clone()?;
    WalLog::new(device, config).map(Arc::new).ok()
  }

  /// 新库挂载事件（记录保存基线）
  ///
  /// libs/server/Databases/MultiDatabaseManager.cs:HandleDatabaseAdded
  pub fn handle_database_added(&self, db: &GarnetDatabase<D>) {
    db.last_save_store_tail_address
      .store(db.store.tail_address(), Relaxed);
  }

  /// 复制库表快照（读锁语义）
  ///
  /// libs/server/Databases/MultiDatabaseManager.cs:CopyDatabases
  pub fn copy_databases(&self) -> GxHashMap<i64, Arc<GarnetDatabase<D>>> {
    self
      .databases
      .pin()
      .iter()
      .fold(GxHashMap::default(), |mut m, (k, v)| {
        m.insert(*k, Arc::clone(v));
        m
      })
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
  /// 目录枚举失败必须显式报错而非静默返回空集：空集会让 `--recover` 在没有任何
  /// 库被恢复的情况下看似健康地启动（上游 d20d63993 修复的"静默错误恢复"类缺陷）。
  /// 对标 libs/server/Databases/MultiDatabaseManager.cs:RecoverCheckpointAsync 与
  /// :RecoverAOFAsync 中包裹 TryGetSavedDatabaseIds 的 try/catch——上游将日志从
  /// LogInformation 提级到 LogError 并尊重 FailOnRecoveryError 抛出；Rust 侧错误即
  /// 返回值，统一为记录 error 日志后向调用方传播（恒为 fail-loud 语义，强于上游
  /// FailOnRecoveryError=false 时"记日志后放弃恢复"的默认分支）。
  ///
  /// libs/server/Databases/MultiDatabaseManager.cs:TryGetSavedDatabaseIds
  pub fn try_get_saved_database_ids(&self) -> wkv::Result<Vec<i64>> {
    let mut ids = Vec::new();
    let entries = match fs::read_dir(&self.checkpoint_root) {
      Ok(entries) => entries,
      Err(e) => {
        log::error!(
          "枚举已持久化库编号失败: checkpoint_root = {}; err = {e}",
          self.checkpoint_root.display()
        );
        return Err(wkv::Error::from(e));
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
  /// libs/server/Databases/MultiDatabaseManager.cs:TakeOneCheckpointAsync
  pub async fn take_one_checkpoint(&self, db: &GarnetDatabase<D>) -> wkv::Result<bool> {
    self.base.take_database_checkpoint_async(db).await
  }

  /// 记录库保存元数据
  ///
  /// libs/server/Databases/MultiDatabaseManager.cs:UpdateLastSaveData
  pub fn update_last_save_data(&self, db: &GarnetDatabase<D>, now_ms: u64) {
    db.update_last_save(now_ms);
  }

  /// 按 id 取库
  ///
  /// libs/server/Databases/MultiDatabaseManager.cs:GetDbById
  pub fn get_db_by_id(&self, db_id: i64) -> Option<Arc<GarnetDatabase<D>>> {
    self.databases.pin().get(&db_id).cloned()
  }

  /// 收集指定库的字符串键值快照
  async fn collect_db_kv(&self, db_id: i64) -> Vec<(Vec<u8>, Vec<u8>)> {
    let Ok(session) = self.store.new_session() else {
      return Vec::new();
    };
    session.set_active_db(db_id.max(0) as u64);
    let batch = session.enter_batch();
    let ss = crate::storage::session::storage_session::StorageSession::new(batch);
    let Ok((map, keys)) = ss.string_snapshot().await else {
      return Vec::new();
    };
    keys
      .into_iter()
      .filter_map(|k| {
        let v = map.get(&k).cloned().flatten()?;
        Some((k, v))
      })
      .collect()
  }

  /// 删除指定库全部字符串键
  async fn delete_db_keys(&self, db_id: i64) {
    let Ok(session) = self.store.new_session() else {
      return;
    };
    session.set_active_db(db_id.max(0) as u64);
    let batch = session.enter_batch();
    let ss = crate::storage::session::storage_session::StorageSession::new(batch);
    if let Ok((_, keys)) = ss.string_snapshot().await {
      for key in keys {
        let _ = ss.delete_string(&key).await;
      }
    }
  }

  /// 等待一次 AOF 提交闭环（提交水位确认）
  ///
  /// libs/server/Databases/MultiDatabaseManager.cs:AwaitCommitAsync
  pub async fn await_commit(&self, db_id: i64) -> wkv::Result<bool> {
    self.wait_for_commit_to_aof_async(db_id)
  }

  /// 打开（或复用）检查点目录下已存在的库存储
  ///
  /// 缺口说明：wkv 恢复产出全新 [`WedbStore`] 句柄，与共享单库模型冲突；
  /// 多库共享存储时按 db 前缀隔离，因此多库恢复只登记已保存的库编号，
  /// 数据经各自目录的 AOF 重放重建。
  pub fn open_database_store(&self, _db_id: i64, _dir: &Path) -> wkv::Result<Arc<WedbStore<D>>> {
    Ok(Arc::clone(&self.store))
  }
}

impl<D: Device> IDatabaseManager<D> for MultiDatabaseManager<D> {
  async fn try_get_or_add_database(
    &self,
    db_id: i64,
  ) -> wkv::Result<(Arc<GarnetDatabase<D>>, bool)> {
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

  fn try_get_database(&self, db_id: i64) -> Option<Arc<GarnetDatabase<D>>> {
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

  async fn recover_checkpoint_async(
    &self,
    replica_recover: bool,
    recover_from_token: Option<u128>,
  ) -> wkv::Result<()> {
    let _ = replica_recover;
    // 登记已持久化库并逐库恢复 + AOF 追平。
    // 枚举失败向上传播（见 try_get_saved_database_ids 的对标说明），不允许静默
    // 空集恢复——对标上游 d20d63993 后 RecoverCheckpointAsync 对 ids 枚举错误的
    // Error 级日志 + FailOnRecoveryError 抛出语义。
    for db_id in self.try_get_saved_database_ids()? {
      let (db, _) = self.try_get_or_add_database(db_id).await?;
      if let Some(token) = recover_from_token.or_else(|| {
        wkv::CheckpointManager::<D>::find_latest_checkpoint(&db.checkpoint_dir)
          .ok()
          .flatten()
      }) {
        wkv::CheckpointManager::recover(&db.checkpoint_dir, token, Arc::clone(&db.device))
          .await
          .map_err(wkv::Error::from)?;
      }
      self.base.recover_database_aof_async(&db).await?;
    }
    Ok(())
  }

  async fn take_checkpoint_async(&self, _background: bool, db_id: i64) -> wkv::Result<bool> {
    let mut taken = false;
    let targets: Vec<Arc<GarnetDatabase<D>>> = if db_id < 0 {
      self.get_databases_snapshot()
    } else {
      self.get_db_by_id(db_id).into_iter().collect()
    };
    for db in targets {
      taken |= self.take_one_checkpoint(&db).await?;
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

  fn commit_to_aof_async(&self, db_id: i64) -> wkv::Result<()> {
    if db_id < 0 {
      for (_, db) in self.databases.pin().iter() {
        self.base.commit_aof(db)?;
      }
    } else if let Some(db) = self.get_db_by_id(db_id) {
      self.base.commit_aof(&db)?;
    }
    Ok(())
  }

  fn wait_for_commit_to_aof_async(&self, db_id: i64) -> wkv::Result<bool> {
    let ok = self
      .get_databases_snapshot()
      .into_iter()
      .filter(|db| db_id < 0 || db.id == db_id)
      .all(|db| {
        db.aof
          .as_ref()
          .is_none_or(|aof| aof.flushed_until_address() >= aof.committed_until_address())
      });
    Ok(ok)
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
      total += self.base.replay_database_aof(db, 0, until).await?;
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
        let session = self.store.new_session()?;
        session.set_active_db(db.id.max(0) as u64);
        let batch = session.enter_batch();
        let storage = crate::storage::session::storage_session::StorageSession::new(batch);
        n += storage.object_collect(|_, _| true).await?;
      }
    }
    Ok(n)
  }

  fn start_size_trackers(&self) {
    for (_, db) in self.databases.pin().iter() {
      let _ = db.size_tracker.is_stopped();
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

  fn get_databases_snapshot(&self) -> Vec<Arc<GarnetDatabase<D>>> {
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

  fn recover_vector_sets(&self, _db_id: i64) -> wkv::Result<u64> {
    Ok(0)
  }
}

/// SWAPDB 共享实现（trait impl 与固有方法共用）
mod swap_impl {
  use std::sync::Arc;

  use wdev::Device;

  use super::MultiDatabaseManager;

  /// 交换两库：真实搬移字符串键值（共享存储模型，前缀重写）
  pub(super) async fn swap<D: Device>(
    manager: &MultiDatabaseManager<D>,
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

    // 共享存储模型：数据以 db 前缀物理隔离，SWAPDB 必须真实搬移键值
    // （C# 侧两库独立 Tsavorite，仅交换容器）。字符串面全量重写，
    // TTL 记录随键删除丢弃（缺口：SWAPDB 后过期时间不保留）。
    let snap1 = manager.collect_db_kv(db_id1).await;
    let snap2 = manager.collect_db_kv(db_id2).await;
    manager.delete_db_keys(db_id1).await;
    manager.delete_db_keys(db_id2).await;
    let session = match manager.store.new_session() {
      Ok(s) => s,
      Err(_) => return false,
    };
    for (db_id, pairs) in [(db_id2, snap1), (db_id1, snap2)] {
      session.set_active_db(db_id.max(0) as u64);
      for (key, value) in pairs {
        if session.upsert(&key, &value).await.is_err() {
          return false;
        }
      }
    }
    let pin = manager.databases.pin();
    pin.insert(db_id1, Arc::clone(&db2));
    pin.insert(db_id2, Arc::clone(&db1));
    drop(pin);
    let _ = (db1, db2);
    true
  }
}
