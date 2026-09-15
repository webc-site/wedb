//! 集群节点角色信息（libs/server/Cluster/RoleInfo.cs）
//!
//! 节点角色枚举统一由 wedb 域的 `crate::server::worker::NodeRole` 承载（对标
//! libs/cluster/Server/Worker.cs:NodeRole），一处定义全链路复用。

/// 节点角色元数据条目
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct RoleInfo {
  /// 副本复制偏移量（C# replication_offset）
  pub replication_offset: i64,
  /// 最大发送时间戳（C# sequenceNumber）
  pub sequence_number: i64,
  /// 复制延迟（C# replication_lag）
  pub replication_lag: i64,
  /// 复制状态（ROLE 命令用 connect/connecting/sync/connected；指标用 online/offline）
  pub replication_state: String,
  /// 实例地址（C# address）
  pub address: String,
  /// 实例端口（C# port）
  pub port: i32,
}
