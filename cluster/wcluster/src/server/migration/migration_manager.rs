use std::sync::Arc;

use gxhash::HashSet;

use crate::server::{
  cluster_provider::ClusterProvider,
  migration::{
    migrate_session::{MigrateSession, MigrateTaskSpec},
    migrate_session_task_store::MigrateSessionTaskStore,
    sketch::Sketch,
  },
};

#[derive(Clone, Copy, PartialEq, Eq)]
pub enum TransferOption {
  Slots,
  Keys,
}

/// libs/cluster/Server/Migration/MigrationManager.cs:MigrationManager
pub struct MigrationManager {
  cluster_provider: Arc<ClusterProvider>,
  migration_task_store: MigrateSessionTaskStore,
}

impl MigrationManager {
  /// libs/cluster/Server/Migration/MigrationManager.cs:MigrationManager
  pub fn new(cluster_provider: Arc<ClusterProvider>) -> Self {
    Self {
      cluster_provider,
      migration_task_store: MigrateSessionTaskStore::new(),
    }
  }

  /// libs/cluster/Server/Migration/MigrationManager.cs:Dispose
  pub fn dispose(&self) {
    self.migration_task_store.dispose();
  }

  /// libs/cluster/Server/Migration/MigrationManager.cs:Purge
  pub fn purge(&self) {}

  /// libs/cluster/Server/Migration/MigrationManager.cs:GetBufferPoolStats
  pub fn get_buffer_pool_stats(&self) -> String {
    String::new()
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

#[cfg(test)]
mod tests {
  use super::*;

  /// 默认入参：目标节点 dst
  fn spec() -> MigrateTaskSpec<'static> {
    MigrateTaskSpec {
      source_node_id: "src",
      target_address: "10.0.0.2",
      target_port: 7002,
      target_node_id: "dst",
      username: "",
      passwd: "",
      copy_option: false,
      replace_option: false,
      timeout: 0,
    }
  }

  /// 门面生命周期：任务计数随 add/remove 演进，dispose 后拒绝新任务
  #[test]
  fn manager_facade_lifecycle() {
    let mgr = MigrationManager::new(Arc::new(ClusterProvider {}));
    assert_eq!(mgr.get_migration_task_count(), 0);

    let slots: HashSet<i32> = [1, 2].into_iter().collect();
    let session = mgr
      .try_add_migration_task(spec(), slots, Sketch::new())
      .unwrap();
    assert_eq!(mgr.get_migration_task_count(), 1, "多槽会话按会话去重计数");
    assert!(mgr.can_access_key(b"k", 3, false));

    // 按节点摘除
    assert!(mgr.try_remove_migration_task_node("dst"));
    assert_eq!(mgr.get_migration_task_count(), 0);
    // 会话句柄已失效：摘除为安全空操作且报告成功（槽位清理语义）
    assert!(mgr.try_remove_migration_task_session(session));

    // dispose 后拒绝新任务，键访问放行
    mgr.dispose();
    let slots: HashSet<i32> = [5].into_iter().collect();
    assert!(
      mgr
        .try_add_migration_task(spec(), slots, Sketch::new())
        .is_none()
    );
    assert!(mgr.can_access_key(b"k", 5, false));
  }
}
