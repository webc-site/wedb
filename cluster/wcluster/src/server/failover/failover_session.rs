use std::{
  sync::Arc,
  time::{Duration, Instant},
};

use crate::{
  client::GarnetClient,
  server::{
    cluster_config::ClusterConfig,
    cluster_provider::ClusterProvider,
    failover::{failover_option::FailoverOption, failover_status::FailoverStatus},
  },
};

/// libs/cluster/Server/Failover/FailoverSession.cs:FailoverSession
pub struct FailoverSession {
  _cluster_provider: Arc<ClusterProvider>,
  _cluster_timeout: Duration,
  _failover_timeout: Duration,
  option: FailoverOption,
  clients: Vec<Option<Arc<GarnetClient>>>,
  failover_deadline: Instant,
  pub status: FailoverStatus,
  old_config: ClusterConfig,
  primary_client: Option<Arc<GarnetClient>>,
}

impl FailoverSession {
  /// libs/cluster/Server/Failover/FailoverSession.cs:FailoverSession
  pub fn new(
    cluster_provider: Arc<ClusterProvider>,
    option: FailoverOption,
    cluster_timeout: Duration,
    failover_timeout: Duration,
    is_replica_session: bool,
    _host_address: &str,
    _host_port: i32,
  ) -> Self {
    let old_config = ClusterConfig::new(); // mock

    let mut clients = Vec::new();
    if !is_replica_session {
      clients.push(Some(Arc::new(GarnetClient::new())));
    }

    let failover_timeout = if failover_timeout.is_zero() {
      Duration::from_secs(600)
    } else {
      failover_timeout
    };

    Self {
      _cluster_provider: cluster_provider,
      _cluster_timeout: cluster_timeout,
      _failover_timeout: failover_timeout,
      option,
      clients,
      failover_deadline: Instant::now() + failover_timeout,
      status: FailoverStatus::BeginFailover,
      old_config,
      primary_client: None,
    }
  }

  pub fn failover_timeout_reached(&self) -> bool {
    Instant::now() > self.failover_deadline
  }

  /// libs/cluster/Server/Failover/FailoverSession.cs:Dispose
  pub fn dispose(&mut self) {
    self.dispose_connections();
  }

  /// libs/cluster/Server/Failover/FailoverSession.cs:DisposeConnections
  fn dispose_connections(&mut self) {
    for client in self.clients.iter_mut() {
      if let Some(c) = client.take() {
        c.dispose();
      }
    }
    if let Some(c) = self.primary_client.take() {
      c.dispose();
    }
  }

  // --- PrimaryFailoverSession.cs ---

  /// libs/cluster/Server/Failover/PrimaryFailoverSession.cs:CheckReplicaSyncAsync
  #[allow(dead_code)]
  async fn check_replica_sync_async(&self, gclient: Arc<GarnetClient>) -> Option<String> {
    if !gclient.is_connected {
      gclient.connect_async().await;
    }
    Some(
      gclient
        .execute_cluster_fail_replication_offset_async(0)
        .await,
    )
  }

  /// libs/cluster/Server/Failover/PrimaryFailoverSession.cs:WaitForFirstReplicaSyncAsync
  async fn wait_for_first_replica_sync_async(&self) -> Option<Arc<GarnetClient>> {
    if !self.clients.is_empty() {
      self.clients[0].clone()
    } else {
      None
    }
  }

  /// libs/cluster/Server/Failover/PrimaryFailoverSession.cs:InitiateReplicaTakeOverAsync
  async fn initiate_replica_take_over_async(&self, gclient: Arc<GarnetClient>) -> bool {
    if !gclient.is_connected {
      gclient.connect_async().await;
    }
    gclient.failover(FailoverOption::Takeover).await
  }

  /// libs/cluster/Server/Failover/PrimaryFailoverSession.cs:BeginAsyncPrimaryFailoverAsync
  pub async fn begin_async_primary_failover_async(&mut self) -> bool {
    self.status = FailoverStatus::IssuingPauseWrites;
    // mock
    self.status = FailoverStatus::WaitingForSync;

    let new_primary = self.wait_for_first_replica_sync_async().await;
    if let Some(np) = new_primary {
      self.status = FailoverStatus::TakingOverAsPrimary;
      if !self.initiate_replica_take_over_async(np).await {
        self.status = FailoverStatus::NoFailover;
        return false;
      }
    } else {
      self.status = FailoverStatus::NoFailover;
      return false;
    }

    self.status = FailoverStatus::NoFailover;
    true
  }

  // --- ReplicaFailoverSession.cs ---

  /// libs/cluster/Server/Failover/ReplicaFailoverSession.cs:CreateConnectionAsync
  async fn create_connection_async(&self, _node_id: &str) -> Option<Arc<GarnetClient>> {
    let client = Arc::new(GarnetClient::new());
    if !client.is_connected {
      client.reconnect_async().await;
    }
    Some(client)
  }

  /// libs/cluster/Server/Failover/ReplicaFailoverSession.cs:GetConnectionAsync
  async fn get_connection_async(&self, node_id: &str) -> Option<Arc<GarnetClient>> {
    self.create_connection_async(node_id).await
  }

  /// libs/cluster/Server/Failover/ReplicaFailoverSession.cs:PauseWritesAndWaitForSyncAsync
  async fn pause_writes_and_wait_for_sync_async(&mut self) -> bool {
    let primary_id = self
      .old_config
      .local_node_primary_id()
      .unwrap_or("")
      .to_string();
    let client = self.get_connection_async(&primary_id).await;

    if let Some(c) = client {
      self.primary_client = Some(c.clone());
      self.status = FailoverStatus::IssuingPauseWrites;
      let local_id = self.old_config.local_node_id().unwrap_or("").as_bytes();
      let _resp = c.execute_cluster_fail_stop_writes_async(local_id).await;

      self.status = FailoverStatus::WaitingForSync;
      true
    } else {
      false
    }
  }

  /// libs/cluster/Server/Failover/ReplicaFailoverSession.cs:TakeOverAsPrimaryAsync
  async fn take_over_as_primary_async(&mut self) -> bool {
    self.status = FailoverStatus::TakingOverAsPrimary;
    true
  }

  /// libs/cluster/Server/Failover/ReplicaFailoverSession.cs:BroadcastConfigAndRequestAttachAsync
  async fn broadcast_config_and_request_attach_async(
    &self,
    replica_id: &str,
    config_byte_array: &[u8],
  ) {
    let old_primary_id = self.old_config.local_node_primary_id().unwrap_or("");
    let client = if old_primary_id == replica_id && self.primary_client.is_some() {
      self.primary_client.clone().unwrap()
    } else {
      if let Some(c) = self.get_connection_async(replica_id).await {
        c
      } else {
        return;
      }
    };

    let _resp = client.gossip_async(config_byte_array).await;
    let local_address = self.old_config.local_node_ip();
    let local_port = self.old_config.local_node_port();
    let _ = client.replica_of(local_address, local_port).await;
  }

  /// libs/cluster/Server/Failover/ReplicaFailoverSession.cs:IssueAttachReplicasAsync
  async fn issue_attach_replicas_async(&self) {
    let old_primary_id = self
      .old_config
      .local_node_primary_id()
      .unwrap_or("")
      .to_string();
    let mut replica_ids = vec![];
    let config_byte_array = vec![];

    if self.option == FailoverOption::Default {
      replica_ids.push(old_primary_id);
    }

    for replica_id in replica_ids {
      self
        .broadcast_config_and_request_attach_async(&replica_id, &config_byte_array)
        .await;
    }
  }

  /// libs/cluster/Server/Failover/ReplicaFailoverSession.cs:PrimaryNeedsReset
  fn primary_needs_reset(&self) -> bool {
    self.status == FailoverStatus::WaitingForSync
      || self.status == FailoverStatus::TakingOverAsPrimary
  }

  /// libs/cluster/Server/Failover/ReplicaFailoverSession.cs:BeginAsyncReplicaFailoverAsync
  pub async fn begin_async_replica_failover_async(&mut self) -> bool {
    let mut failover_succeeded = false;

    if self.option == FailoverOption::Default && !self.pause_writes_and_wait_for_sync_async().await
    {
      self.reset_if_needed(failover_succeeded).await;
      return false;
    }

    if !self.take_over_as_primary_async().await {
      self.reset_if_needed(failover_succeeded).await;
      return false;
    }

    failover_succeeded = true;
    self.issue_attach_replicas_async().await;
    self.reset_if_needed(failover_succeeded).await;
    true
  }

  async fn reset_if_needed(&mut self, failover_succeeded: bool) {
    if self.primary_needs_reset()
      && !failover_succeeded
      && let Some(ref c) = self.primary_client
    {
      let _ = c.execute_cluster_fail_stop_writes_async(&[]).await;
    }
    self.status = FailoverStatus::NoFailover;
  }
}
