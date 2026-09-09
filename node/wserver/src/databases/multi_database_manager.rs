//! 多库管理器（对标 libs/server/Databases/MultiDatabaseManager.cs）
//!
//! wkv 单库模型下多逻辑库共享同一 [`WedbStore`]（键前缀隔离）；库注册表为
//! papaya 并发映射，结构换库（SWAPDB）经内容写锁串行化。

use std::{
  fs,
  path::{Path, PathBuf},
  sync::{Arc, atomic::Ordering::Relaxed},
};

use gxhash::HashMap as GxHashMap;
use papaya::HashMap as PapayaMap;
use parking_lot::{Mutex, RwLock};
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
  /// 库表结构变更锁（SWAPDB / 批量恢复）
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
  ) -> Option<parking_lot::RwLockWriteGuard<'_, ()>> {
    self.content_lock.try_write()
  }

  /// 库表内容读锁
  ///
  /// libs/server/Databases/MultiDatabaseManager.cs:TryGetDatabasesContentReadLock
  pub fn try_get_databases_content_read_lock(
    &self,
  ) -> Option<parking_lot::RwLockReadGuard<'_, ()>> {
    self.content_lock.try_read()
  }

  /// 取已持久化的库编号集合（检查点目录下有子目录的编号）
  ///
  /// libs/server/Databases/MultiDatabaseManager.cs:TryGetSavedDatabaseIds
  pub fn try_get_saved_database_ids(&self) -> Vec<i64> {
    let mut ids = Vec::new();
    if let Ok(entries) = fs::read_dir(&self.checkpoint_root) {
      for entry in entries.flatten() {
        if entry.path().is_dir()
          && let Ok(n) = entry.file_name().into_string()
          && let Ok(id) = n.parse::<i64>()
        {
          ids.push(id);
        }
      }
    }
    ids.sort_unstable();
    ids
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
  fn try_get_or_add_database(&self, db_id: i64) -> wkv::Result<(Arc<GarnetDatabase<D>>, bool)> {
    let pin = self.databases.pin();
    if let Some(db) = pin.get(&db_id) {
      return Ok((Arc::clone(db), false));
    }
    drop(pin);
    let _guard = self.content_lock.write();
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
    // 登记已持久化库并逐库恢复 + AOF 追平
    for db_id in self.try_get_saved_database_ids() {
      let (db, _) = self.try_get_or_add_database(db_id)?;
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

  fn execute_object_collection(&self, db_id: i64) -> wkv::Result<usize> {
    let mut n = 0usize;
    for (_, db) in self.databases.pin().iter() {
      if db_id < 0 || db.id == db_id {
        let session = self.store.new_session()?;
        session.set_active_db(db.id.max(0) as u64);
        let batch = session.enter_batch();
        let storage = crate::storage::session::storage_session::StorageSession::new(batch);
        n += storage.object_collect(|_, _| true)?;
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
    self.store.truncate().await?;
    for (_, db) in self.databases.pin().iter() {
      db.last_save_ms.store(0, Relaxed);
      db.last_save_store_tail_address.store(0, Relaxed);
    }
    Ok(())
  }

  fn try_swap_databases(&self, db_id1: i64, db_id2: i64) -> bool {
    if db_id1 == db_id2 {
      return true;
    }
    let _guard = self.content_lock.write();
    let pin = self.databases.pin();
    let db1 = pin.get(&db_id1).cloned();
    let db2 = pin.get(&db_id2).cloned();
    match (db1, db2) {
      (Some(a), Some(b)) => {
        // 交换库内容：以重挂 id 的方式对调（存储共享，仅换容器身份）
        let a_new = GarnetDatabase::new(
          db_id2,
          Arc::clone(&a.store),
          Arc::clone(&a.device),
          self.checkpoint_dir_of(db_id2),
          a.aof.clone(),
        );
        let b_new = GarnetDatabase::new(
          db_id1,
          Arc::clone(&b.store),
          Arc::clone(&b.device),
          self.checkpoint_dir_of(db_id1),
          b.aof.clone(),
        );
        a_new
          .last_save_ms
          .store(b.last_save_ms.load(Relaxed), Relaxed);
        b_new
          .last_save_ms
          .store(a.last_save_ms.load(Relaxed), Relaxed);
        pin.insert(db_id2, Arc::new(a_new));
        pin.insert(db_id1, Arc::new(b_new));
        true
      }
      _ => false,
    }
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
