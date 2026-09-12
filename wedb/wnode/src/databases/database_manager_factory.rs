//! 数据库管理器工厂（对标 libs/server/Databases/DatabaseManagerFactory.cs）

use std::{path::PathBuf, sync::Arc};

use wdev::Device;
use wkv::WedbStore;

use super::{
  garnet_database::GarnetDatabase, i_database_manager::IDatabaseManager,
  multi_database_manager::MultiDatabaseManager, single_database_manager::SingleDatabaseManager,
};

/// 数据库管理器（单库 / 多库二选一，静态分发）
pub enum DatabaseManager<D: Device> {
  /// 单库模式
  Single(SingleDatabaseManager<D>),
  /// 多库模式
  Multi(MultiDatabaseManager<D>),
}

impl<D: Device> DatabaseManager<D> {
  /// 库数量
  pub fn database_count(&self) -> usize {
    match self {
      Self::Single(_) => 1,
      Self::Multi(m) => m.databases.pin().len(),
    }
  }

  /// 默认库（DB 0；多库模式未注册时返回 None）
  pub fn try_default_database(&self) -> Option<Arc<GarnetDatabase<D>>> {
    match self {
      Self::Single(s) => Some(Arc::clone(&s.db)),
      Self::Multi(m) => m.try_get_database(0),
    }
  }
}

impl DatabaseManagerFactory {
  /// 数据库管理器构造
  fn build<D: Device>(
    multi: bool,
    store: Arc<WedbStore<D>>,
    checkpoint_root: PathBuf,
    default_db: Arc<GarnetDatabase<D>>,
  ) -> DatabaseManager<D> {
    if multi {
      let manager = MultiDatabaseManager::new(store, checkpoint_root);
      manager.databases.pin().insert(0, default_db);
      DatabaseManager::Multi(manager)
    } else {
      DatabaseManager::Single(SingleDatabaseManager::new(checkpoint_root, default_db))
    }
  }
}

/// 数据库管理器工厂
pub struct DatabaseManagerFactory;

impl DatabaseManagerFactory {
  /// 创建数据库管理器（单库 / 多库由 `multi` 决定）
  ///
  /// libs/server/Databases/DatabaseManagerFactory.cs:CreateDatabaseManager
  pub fn create_database_manager<D: Device>(
    multi: bool,
    store: Arc<WedbStore<D>>,
    checkpoint_root: PathBuf,
    default_db: Arc<GarnetDatabase<D>>,
  ) -> DatabaseManager<D> {
    Self::build(multi, store, checkpoint_root, default_db)
  }

  /// 是否应创建多库管理器（数据库数 > 1 判定）
  ///
  /// libs/server/Databases/DatabaseManagerFactory.cs:ShouldCreateMultipleDatabaseManager
  pub fn should_create_multiple_database_manager(num_databases: usize) -> bool {
    num_databases > 1
  }
}
