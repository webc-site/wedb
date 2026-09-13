use crate::{
  error::{Error, Result},
  server::{
    cluster_config::{ClusterConfig, LOCAL_WORKER_ID},
    cluster_manager::ClusterManager,
    replication::recovery_status::RecoveryStatus,
    worker::{LocalWorkerSpec, NodeRole},
  },
};

/// libs/cluster/Server/ClusterManagerWorkerState.cs:ClusterManager
impl ClusterManager {
  /// libs/cluster/Server/ClusterManagerWorkerState.cs:TryRemoveWorker
  pub fn try_remove_worker(&self, node_id: &str, expiry_seconds: u64) -> Result<()> {
    let _guard = self.suspend_config_merge();
    {
      let mut current = self.current_config.write();
      if current
        .local_node_id()
        .is_some_and(|id| id.eq_ignore_ascii_case(node_id))
      {
        return Err(Error::CannotForgetMyself);
      }
      if current.get_node_role_from_node_id(node_id) == NodeRole::Unassigned {
        return Err(Error::NodeNotFound(node_id.to_string()));
      }
      if current.local_node_role() == NodeRole::Replica
        && current
          .local_node_primary_id()
          .is_some_and(|pid| pid.eq_ignore_ascii_case(node_id))
      {
        return Err(Error::CannotForgetPrimary);
      }
      let new_config = current.remove_worker(node_id);
      *current = new_config;
    }
    self.ban_node(node_id, expiry_seconds);
    self.flush_config();
    Ok(())
  }

  /// libs/cluster/Server/ClusterManagerWorkerState.cs:TryReset
  /// 保留 _expiry_seconds 形参以对标 ClusterManagerWorkerState.TryReset 签名规范
  pub fn try_reset(&self, soft: bool, _expiry_seconds: u64) -> Result<()> {
    let _guard = self.suspend_config_merge();
    if let Some(repl_mgr) = self.cluster_provider.replication_manager() {
      repl_mgr.reset_recovery();
    }
    if let Some(gm) = self.cluster_provider.gossip_manager() {
      gm.connection_store.close_all();
    }
    {
      let mut current = self.current_config.write();
      let new_node_id = if soft {
        current.local_node_id().unwrap_or_default().to_string()
      } else {
        super::cluster_manager::create_hex_id()
      };
      let address = current.local_node_ip().to_string();
      let port = current.local_node_port();
      let config_epoch = if soft {
        current.local_node_config_epoch()
      } else {
        0
      };

      let mut new_config = ClusterConfig::new();
      new_config.initialize_local_worker(LocalWorkerSpec {
        node_id: &new_node_id,
        address: &address,
        port,
        config_epoch,
        role: NodeRole::Primary,
        replica_of_node_id: None,
        hostname: None,
      });
      *current = new_config;
    }
    if !soft {
      self.worker_ban_list.write().clear();
    }
    self.flush_config();
    Ok(())
  }

  /// libs/cluster/Server/ClusterManagerWorkerState.cs:TryAddReplicaAsync
  pub async fn try_add_replica_async(
    &self,
    node_id: &str,
    force: bool,
    upgrade_lock: bool,
  ) -> Result<()> {
    {
      let current = self.current_config.read();
      if current
        .local_node_id()
        .is_some_and(|id| id.eq_ignore_ascii_case(node_id))
      {
        return Err(Error::MigrateToMyself);
      }
      if !force && current.local_node_role() != NodeRole::Primary {
        return Err(Error::TargetNotPrimary("local node is not primary".into()));
      }
      if !force && current.has_assigned_slots(LOCAL_WORKER_ID as u16) {
        return Err(Error::SlotAlreadyScheduled(0));
      }
      let worker_id = current.get_worker_id_from_node_id(node_id);
      if worker_id == 0 {
        return Err(Error::NodeNotFound(node_id.to_string()));
      }
      if current.get_node_role_from_node_id(node_id) != NodeRole::Primary {
        return Err(Error::TargetNotPrimary(node_id.to_string()));
      }
    }

    if let Some(repl_mgr) = self.cluster_provider.replication_manager()
      && !repl_mgr.begin_recovery(RecoveryStatus::ClusterReplicate, upgrade_lock)
    {
      return Err(Error::CannotAcquireRecoveryLock);
    }

    {
      let mut current = self.current_config.write();
      current
        .make_replica_of(Some(node_id))
        .bump_local_node_config_epoch();
    }
    self.flush_config();
    Ok(())
  }

  /// libs/cluster/Server/ClusterManagerWorkerState.cs:ListReplicas
  pub fn list_replicas(&self, node_id: &str) -> Vec<String> {
    self.current_config.read().get_replica_ids(node_id)
  }
}
