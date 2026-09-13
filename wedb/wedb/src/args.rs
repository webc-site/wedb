//! WeDB 分布式集群命令行与服务参数配置

use clap::Parser;
use wnode::{NodeArgs, ServerArgs};

/// 默认集群节点心跳与故障检测超时毫秒数
pub const DEFAULT_CLUSTER_NODE_TIMEOUT_MS: u64 = 15000;
/// 集群总线端口偏移量（对标 Redis Cluster 协议规范：PORT + 10000）
pub const CLUSTER_BUS_PORT_OFFSET: u16 = 10000;

/// WeDB 分布式集群节点参数
#[derive(Debug, Clone, Parser)]
#[command(author, version, about = "WeDB 高性能分布式集群节点")]
pub struct ClusterArgs {
  /// 节点通用参数（端口、工作目录、线程、WAL路径等）
  #[command(flatten)]
  pub node: NodeArgs,

  /// 集群总线监听端口（缺省为业务端口 + 10000）
  #[arg(long)]
  pub cluster_port: Option<u16>,

  /// 集群拓扑配置文件存储路径（缺省为 <dir>/nodes.conf）
  #[arg(long)]
  pub cluster_config_file: Option<String>,

  /// 集群节点心跳与故障检测超时毫秒数
  #[arg(long, default_value_t = DEFAULT_CLUSTER_NODE_TIMEOUT_MS)]
  pub cluster_node_timeout_ms: u64,
}

impl ServerArgs for ClusterArgs {
  #[inline]
  fn node_args(&self) -> &NodeArgs {
    &self.node
  }
}

impl ClusterArgs {
  /// 获取集群总线通信端口（未指定时自动推导为业务端口 + 10000）
  #[inline]
  pub fn cluster_bus_port(&self) -> u16 {
    self
      .cluster_port
      .unwrap_or(self.node.port + CLUSTER_BUS_PORT_OFFSET)
  }

  /// 获取集群配置文件存储路径（未指定时默认为 <dir>/nodes.conf）
  pub fn cluster_config_path(&self) -> String {
    self
      .cluster_config_file
      .clone()
      .unwrap_or_else(|| self.node.dir.join("nodes.conf").display().to_string())
  }
}
