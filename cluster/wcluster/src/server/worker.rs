use bitcode::{Decode, Encode};
use strum::{EnumString, FromRepr, IntoStaticStr};

// 与 Worker 一同作为配置线格式载荷（bitcode 变体序即枚举序，两端同版无兼容负担）

/// 保留 worker 位（0 号），永不承载节点
pub const RESERVED_WORKER_ID: usize = 0;
/// 本地 worker 位（1 号），紧跟保留位
pub const LOCAL_WORKER_ID: usize = 1;

/// 在 garnet 中的相对路径:Server:NodeRole
#[derive(
  Debug, Clone, Copy, PartialEq, Eq, Default, FromRepr, EnumString, IntoStaticStr, Encode, Decode,
)]
#[repr(u8)]
pub enum NodeRole {
  Primary = 0x0,
  Replica = 0x1,
  #[default]
  Unassigned = 0x2,
}

/// 在 garnet 中的相对路径:Server:Worker
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
