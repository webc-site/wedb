use std::fmt::{self, Display, Formatter};

use strum::{EnumString, FromRepr, IntoStaticStr};

/// garnet相对路径:Server:NodeRole
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, FromRepr, EnumString, IntoStaticStr)]
#[repr(u8)]
pub enum NodeRole {
  Primary = 0x0,
  Replica = 0x1,
  #[default]
  Unassigned = 0x2,
}

/// garnet相对路径:Server:Worker
#[derive(Debug, Clone, Default)]
pub struct Worker {
  pub nodeid: Option<String>,
  pub address: String,
  pub port: i32,
  pub config_epoch: i64,
  pub role: NodeRole,
  pub replica_of_node_id: Option<String>,
  pub replication_offset: i64,
  pub hostname: Option<String>,
}

impl Display for Worker {
  /// garnet相对路径:Server:Worker:ToString
  fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
    write!(
      f,
      "{} {} {} {} {:?} {}",
      self.nodeid.as_deref().unwrap_or(""),
      self.address,
      self.port,
      self.config_epoch,
      self.role,
      self.replica_of_node_id.as_deref().unwrap_or("")
    )
  }
}

/// 本地 worker 身份入参（ClusterConfig:InitializeLocalWorker 的散参聚合）
pub struct LocalWorkerSpec<'a> {
  pub node_id: &'a str,
  pub address: &'a str,
  pub port: i32,
  pub config_epoch: i64,
  pub role: NodeRole,
  pub replica_of_node_id: Option<&'a str>,
  pub hostname: Option<&'a str>,
}
