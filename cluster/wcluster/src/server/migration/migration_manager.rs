use std::sync::Arc;
use gxhash::HashSet;
use crate::server::cluster_provider::ClusterProvider;
use crate::server::migration::migrate_session_task_store::MigrateSessionTaskStore;
use crate::server::migration::migrate_session::MigrateSession;
use crate::server::migration::sketch::Sketch;

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
    pub fn dispose(&mut self) {
        self.migration_task_store.dispose();
    }

    /// libs/cluster/Server/Migration/MigrationManager.cs:Purge
    pub fn purge(&self) {}

    /// libs/cluster/Server/Migration/MigrationManager.cs:GetBufferPoolStats
    pub fn get_buffer_pool_stats(&self) -> String { String::new() }

    /// libs/cluster/Server/Migration/MigrationManager.cs:GetMigrationTaskCount
    pub fn get_migration_task_count(&self) -> usize {
        self.migration_task_store.get_num_sessions()
    }

    /// libs/cluster/Server/Migration/MigrationManager.cs:TryAddMigrationTask
    pub fn try_add_migration_task(
        &self,
        source_node_id: &str,
        target_address: &str,
        target_port: i32,
        target_node_id: &str,
        username: &str,
        passwd: &str,
        copy_option: bool,
        replace_option: bool,
        timeout: i32,
        slots: HashSet<i32>,
        sketch: Sketch,
        transfer_option: TransferOption,
    ) -> Option<Arc<MigrateSession>> {
        self.migration_task_store.try_add_migrate_session(
            self.cluster_provider.clone(),
            source_node_id, target_address, target_port, target_node_id,
            username, passwd, copy_option, replace_option, timeout,
            slots, sketch, transfer_option
        )
    }

    /// libs/cluster/Server/Migration/MigrationManager.cs:TryRemoveMigrationTask
    pub fn try_remove_migration_task_session(&self, m_session: Arc<MigrateSession>) -> bool {
        self.migration_task_store.try_remove(m_session)
    }

    /// libs/cluster/Server/Migration/MigrationManager.cs:TryRemoveMigrationTask
    pub fn try_remove_migration_task_node(&self, target_node_id: &str) -> bool {
        self.migration_task_store.try_remove_node(target_node_id)
    }

    /// libs/cluster/Server/Migration/MigrationManager.cs:CanAccessKey
    pub fn can_access_key(&self, key: &[u8], slot: i32, read_only: bool) -> bool {
        self.migration_task_store.can_access_key(key, slot, read_only)
    }
}
