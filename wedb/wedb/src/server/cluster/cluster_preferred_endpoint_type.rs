//! 集群优选端点类型（对标 libs/server/Cluster/ClusterPreferredEndpointType.cs）

/// libs/server/Cluster/ClusterPreferredEndpointType.cs:ClusterPreferredEndpointType
///
/// 集群优选端点类型（判别值对齐 C# 声明序：MOVED/ASK 重定向与 CLUSTER
/// SLOTS/SHARDS 输出的地址形态偏好；命令行取值 ip/hostname/unknown）
#[derive(
  Debug, Clone, Copy, PartialEq, Eq, Default, clap::ValueEnum, strum::Display, toml_spanner::Toml,
)]
#[toml(rename_all = "lowercase")]
pub enum ClusterPreferredEndpointType {
  /// IP 地址（ex -MOVED 12182 127.0.0.1:7000）
  #[default]
  #[value(name = "ip")]
  Ip = 0,
  /// 主机名（ex -MOVED 12182 localhost:7000；无 hostname 时为 ?:7000）
  #[value(name = "hostname")]
  Hostname = 1,
  /// 客户端自身已知形式（ex -MOVED 12182 ?:7000）
  #[value(name = "unknown")]
  Unknown = 2,
}
