//! 集群优选端点类型（对标 libs/server/Cluster/ClusterPreferredEndpointType.cs）

/// 集群优选端点类型
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum ClusterPreferredEndpointType {
  /// 主机名
  Hostname,
  /// IP 地址
  #[default]
  Ip,
  /// 客户端自身已知形式
  Unknown,
}
