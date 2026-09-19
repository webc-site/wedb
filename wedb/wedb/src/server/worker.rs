use bitcode::{Decode, Encode};
use hipstr::HipStr;
use strum::{EnumString, FromRepr, IntoStaticStr};

// 与 Worker 一同作为配置线格式载荷（bitcode 变体序即枚举序，两端同版无兼容负担）

/// 保留 worker 位（0 号），永不承载节点
pub const RESERVED_WORKER_ID: usize = 0;
/// 本地 worker 位（1 号），紧跟保留位
pub const LOCAL_WORKER_ID: usize = 1;
/// 0 号未分配保留节点地址标识
pub const UNASSIGNED_ADDRESS: &str = "unassigned";

/// libs/cluster/Server/Worker.cs:NodeRole
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

/// libs/cluster/Server/Worker.cs:Worker
///
/// 集群工作节点元数据。节点 ID 按 transpile 规范收敛为 u128 纯二进制
/// （C# Generator.CreateHexId(40) 的字符串形态仅在 RESP/命令帧渲染点转
/// 十六进制），地址等非身份字符串字段仍用 HipStr 优化高频克隆；
/// 字段按对齐大小降序排列以最小化结构体填充（padding）
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Worker {
  pub nodeid: Option<u128>,
  pub address: HipStr<'static>,
  pub replica_of_node_id: Option<u128>,
  pub hostname: Option<HipStr<'static>>,
  pub config_epoch: i64,
  pub replication_offset: i64,
  pub port: i32,
  pub role: NodeRole,
}

impl Worker {
  /// 创建 0 号未分配保留节点
  #[inline]
  pub fn unassigned() -> Self {
    Self {
      address: HipStr::borrowed(UNASSIGNED_ADDRESS),
      ..Self::default()
    }
  }
}

/// 本地 worker 身份入参（ClusterConfig:InitializeLocalWorker 的散参聚合）
pub struct LocalWorkerSpec<'a> {
  pub node_id: u128,
  pub address: &'a str,
  pub port: i32,
  pub config_epoch: i64,
  pub role: NodeRole,
  pub replica_of_node_id: Option<u128>,
  pub hostname: Option<&'a str>,
}
