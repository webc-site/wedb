use std::{sync::Arc, time::Duration};

use parking_lot::RwLock;

use crate::server::{
  cluster_provider::ClusterProvider,
  failover::{
    failover_option::FailoverOption, failover_session::FailoverSession,
    failover_status::FailoverStatus,
  },
};

/// libs/cluster/Server/Failover/FailoverManager.cs:FailoverManager
pub struct FailoverManager {
  cluster_provider: Arc<ClusterProvider>,
  current_failover_session: RwLock<Option<FailoverSession>>,
  failover_task_lock: RwLock<()>,
  pub last_failover_status: RwLock<FailoverStatus>,
}

impl FailoverManager {
  /// libs/cluster/Server/Failover/FailoverManager.cs:FailoverManager
  pub fn new(cluster_provider: Arc<ClusterProvider>) -> Self {
    Self {
      cluster_provider,
      current_failover_session: RwLock::new(None),
      failover_task_lock: RwLock::new(()),
      last_failover_status: RwLock::new(FailoverStatus::NoFailover),
    }
  }

  /// libs/cluster/Server/Failover/FailoverManager.cs:Dispose
  pub fn dispose(&self) {
    self.reset();
  }

  /// libs/cluster/Server/Failover/FailoverManager.cs:TryAbortReplicaFailover
  pub fn try_abort_replica_failover(&self) {
    self.reset();
  }

  /// libs/cluster/Server/Failover/FailoverManager.cs:Reset
  fn reset(&self) {
    let mut session = self.current_failover_session.write();
    if let Some(mut s) = session.take() {
      s.dispose();
    }
  }

  /// libs/cluster/Server/Failover/FailoverManager.cs:GetFailoverStatus
  pub fn get_failover_status(&self) -> String {
    let session = self.current_failover_session.read();
    if let Some(ref s) = *session {
      s.status.get_failover_status().to_string()
    } else {
      FailoverStatus::NoFailover.get_failover_status().to_string()
    }
  }

  /// libs/cluster/Server/Failover/FailoverManager.cs:GetLastFailoverStatus
  pub fn get_last_failover_status(&self) -> String {
    self
      .last_failover_status
      .read()
      .get_failover_status()
      .to_string()
  }

  /// libs/cluster/Server/Failover/FailoverManager.cs:TryStartReplicaFailover
  pub fn try_start_replica_failover(
    &self,
    option: FailoverOption,
    failover_timeout: Duration,
  ) -> bool {
    let _guard = match self.failover_task_lock.try_write() {
      Some(g) => g,
      None => return false,
    };

    *self.last_failover_status.write() = FailoverStatus::BeginFailover;

    let cluster_timeout = Duration::from_secs(60); // mock
    let session = FailoverSession::new(
      self.cluster_provider.clone(),
      option,
      cluster_timeout,
      failover_timeout,
      true,
      "",
      -1,
    );
    *self.current_failover_session.write() = Some(session);

    // spawn task mock
    true
  }

  /// libs/cluster/Server/Failover/FailoverManager.cs:TryStartPrimaryFailover
  pub fn try_start_primary_failover(
    &self,
    replica_address: &str,
    replica_port: i32,
    option: FailoverOption,
    timeout: Duration,
  ) -> bool {
    let _guard = match self.failover_task_lock.try_write() {
      Some(g) => g,
      None => return false,
    };

    let cluster_timeout = Duration::from_secs(60); // mock
    let session = FailoverSession::new(
      self.cluster_provider.clone(),
      option,
      cluster_timeout,
      timeout,
      false,
      replica_address,
      replica_port,
    );
    *self.current_failover_session.write() = Some(session);

    // spawn task mock
    true
  }
}
