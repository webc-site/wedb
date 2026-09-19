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

/// 发送缓冲单批/单块载荷保留额：批头（CLUSTER MIGRATE 6 元 RESP 数组 +
/// 载荷长度头）与逐记录帧头（kind 字节 + 4 字节块长）的余量
/// （对标 garnet/libs/common/NetworkBufferSettings.cs:SendBufferOverheadReserve）
pub const SEND_BUFFER_OVERHEAD_RESERVE: usize = 256;

/// 发送缓冲内容上限缺省值：网络缓冲缺省规格扣保留额，与默认配置下
/// [`MigrationManager::max_send_buffer_content_size`] 同源派生，供
/// migration_manager 未装配时兜底（对标 C#
/// NetworkBufferSettings.MaxSendBufferContentSize = sendBufferSize -
/// SendBufferOverheadReserve）
pub const DEFAULT_MAX_SEND_BUFFER_CONTENT_SIZE: usize =
  DEFAULT_BUFFER_SIZE - SEND_BUFFER_OVERHEAD_RESERVE;

/// libs/cluster/Server/Migration/MigrationManager.cs:MigrationManager
pub struct MigrationManager {
  cluster_provider: Arc<ClusterProvider>,
  migration_task_store: MigrateSessionTaskStore,
  /// 迁移网络发送缓冲区尺寸（对标 C# networkBufferSettings.sendBufferSize，
  /// C# 取 1 << serverOptions.PageSizeBits()；rust 与 network_pool 同源取
  /// 网络缓冲默认规格）
  send_buffer_size: usize,
  /// 迁移网络缓冲池（对标 C# networkPool：NetworkBufferSettings.CreateBufferPool）
  network_pool: Arc<LimitedFixedBufferPool>,
}

impl MigrationManager {
  /// libs/cluster/Server/Migration/MigrationManager.cs:MigrationManager
  pub fn new(cluster_provider: Arc<ClusterProvider>) -> Self {
    let send_buffer_size = DEFAULT_BUFFER_SIZE;
    Self {
      cluster_provider,
      migration_task_store: MigrateSessionTaskStore::new(),
      send_buffer_size,
      network_pool: LimitedFixedBufferPool::new(send_buffer_size, DEFAULT_MAX_POOL_SIZE),
    }
  }

  /// 发送缓冲内容上限：扣除保留额后单条记录/单块的最大字节数
  /// （对标 garnet/libs/common/NetworkBufferSettings.cs:MaxSendBufferContentSize；
  /// C# MigrationManager.GetNetworkBufferSettings.MaxSendBufferContentSize 同源，
  /// 迁移装批与大记录分块共用该阈值）
  #[inline]
  pub fn max_send_buffer_content_size(&self) -> usize {
    self.send_buffer_size - SEND_BUFFER_OVERHEAD_RESERVE
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
    spec: MigrateTaskSpec,
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
  pub fn try_remove_migration_task_node(&self, target_node_id: u128) -> bool {
    self.migration_task_store.try_remove_node(target_node_id)
  }

  /// libs/cluster/Server/Migration/MigrationManager.cs:CanAccessKey
  pub fn can_access_key(&self, key: &[u8], slot: i32, read_only: bool) -> bool {
    self
      .migration_task_store
      .can_access_key(key, slot, read_only)
  }
}
