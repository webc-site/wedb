use std::sync::Arc;

use gxhash::HashSet;
use wbase::pool::{DEFAULT_BUFFER_SIZE, DEFAULT_MAX_POOL_SIZE, LimitedFixedBufferPool};

use crate::server::{
  cluster_provider::ClusterProvider,
  migration::{
    migrate_session::{MigrateSession, MigrateTaskSpec},
    migrate_session_task_store::MigrateSessionTaskStore,
    sketch::Sketch,
  },
};

/// libs/cluster/Server/Migration/MigrationManager.cs:TransferOption
#[derive(Clone, Copy, PartialEq, Eq)]
pub enum TransferOption {
  Slots,
  Keys,
}

/// libs/cluster/Server/Migration/MigrationManager.cs:MigrationManager
pub struct MigrationManager {
  cluster_provider: Arc<ClusterProvider>,
  migration_task_store: MigrateSessionTaskStore,
  /// 迁移网络缓冲池（对标 C# networkPool：NetworkBufferSettings.CreateBufferPool）
  network_pool: Arc<LimitedFixedBufferPool>,
}

impl MigrationManager {
  /// libs/cluster/Server/Migration/MigrationManager.cs:MigrationManager
  pub fn new(cluster_provider: Arc<ClusterProvider>) -> Self {
    Self {
      cluster_provider,
      migration_task_store: MigrateSessionTaskStore::new(),
      network_pool: LimitedFixedBufferPool::new(DEFAULT_BUFFER_SIZE, DEFAULT_MAX_POOL_SIZE),
    }
  }

  /// libs/cluster/Server/Migration/MigrationManager.cs:Purge
  ///
  /// 清空迁移网络缓冲池内全部闲置缓冲区（C# networkPool.Purge()）
  pub fn purge(&self) {
    self.network_pool.purge();
  }

  /// libs/cluster/Server/Migration/MigrationManager.cs:GetBufferPoolStats
  ///
  /// 迁移网络缓冲池统计（C# networkPool.GetStats()）
  pub fn get_buffer_pool_stats(&self) -> String {
    format!(
      "max_pool_size={} free_buffers={} borrowed_buffers={} allocated_buffers={}",
      self.network_pool.max_pool_size(),
      self.network_pool.free_count(),
      self.network_pool.borrowed_count(),
      self.network_pool.allocated_count(),
    )
  }

  /// libs/cluster/Server/Migration/MigrationManager.cs:Dispose
  pub fn dispose(&self) {
    self.migration_task_store.dispose();
  }

  /// libs/cluster/Server/Migration/MigrationManager.cs:GetMigrationTaskCount
  pub fn get_migration_task_count(&self) -> usize {
    self.migration_task_store.get_num_sessions()
  }

  /// libs/cluster/Server/Migration/MigrationManager.cs:TryAddMigrationTask
  pub fn try_add_migration_task(
    &self,
    spec: MigrateTaskSpec<'_>,
    slots: HashSet<i32>,
    sketch: Sketch,
  ) -> Option<Arc<MigrateSession>> {
    self.migration_task_store.try_add_migrate_session(
      Arc::clone(&self.cluster_provider),
      spec,
      slots,
      sketch,
    )
  }

  /// libs/cluster/Server/Migration/MigrationManager.cs:TryRemoveMigrationTask
  pub fn try_remove_migration_task_session(&self, m_session: Arc<MigrateSession>) -> bool {
    self.migration_task_store.try_remove(m_session)
  }

  /// Overload of [Self::try_remove_migration_task_session] taking target_node_id (MigrationManager.cs:TryRemoveMigrationTask)
  pub fn try_remove_migration_task_node(&self, target_node_id: &str) -> bool {
    self.migration_task_store.try_remove_node(target_node_id)
  }

  /// libs/cluster/Server/Migration/MigrationManager.cs:CanAccessKey
  pub fn can_access_key(&self, key: &[u8], slot: i32, read_only: bool) -> bool {
    self
      .migration_task_store
      .can_access_key(key, slot, read_only)
  }
}
