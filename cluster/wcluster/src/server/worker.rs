use std::fmt::{self, Display, Formatter};

use bitcode::{Decode, Encode};
use strum::{EnumString, FromRepr, IntoStaticStr};

// 与 Worker 一同作为配置线格式载荷（bitcode 变体序即枚举序，两端同版无兼容负担）

/// garnet相对路径:Server:NodeRole
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, FromRepr, EnumString, IntoStaticStr)]
#[derive(Encode, Decode)]
#[repr(u8)]
pub enum NodeRole {
  Primary = 0x0,
  Replica = 0x1,
  #[default]
  Unassigned = 0x2,
}

/// garnet相对路径:Server:Worker
///
/// 派生 bitcode 编码直接作为集群配置线格式载荷（worker 自 1 号起序列化）
#[derive(Debug, Clone, Default, Encode, Decode)]
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
