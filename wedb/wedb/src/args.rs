//! WeDB 分布式集群命令行与服务参数配置

use clap::Parser;
use wconf::{NodeArgs, ServerArgs};

/// 默认集群节点心跳与故障检测超时毫秒数
pub const DEFAULT_CLUSTER_NODE_TIMEOUT_MS: u64 = 15000;
/// 默认集群拓扑配置文件名（node dir 内）
pub const DEFAULT_CLUSTER_CONFIG_FILE: &str = "nodes.conf";

/// WeDB 分布式集群节点参数
#[derive(Debug, Clone, Parser)]
#[command(author, version, about = "WeDB 高性能分布式集群节点")]
pub struct ClusterArgs {
  /// 节点通用参数（端口、工作目录、线程、WAL路径等）
  #[command(flatten)]
  pub node: NodeArgs,

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
  /// 获取集群配置文件存储路径（未指定时默认为 <dir>/nodes.conf）
  pub fn cluster_config_path(&self) -> String {
    self.cluster_config_file.as_deref().map_or_else(
      || {
        self
          .node
          .dir
          .join(DEFAULT_CLUSTER_CONFIG_FILE)
          .display()
          .to_string()
      },
      ToOwned::to_owned,
    )
  }
}

#[cfg(test)]
mod tests {
  use clap::Parser;
  use log::LevelFilter;
  use wconf::ServerArgs;
  use wnode::LoggingBuilder;

  use super::*;

  #[test]
  fn test_cluster_args_custom_logging() {
    let args = ClusterArgs::try_parse_from([
      "wedb",
      "--file-logger",
      "/tmp/cluster.log",
      "--log-level",
      "debug",
    ])
    .unwrap();
    assert_eq!(
      args.node_args().file_logger.as_deref(),
      Some("/tmp/cluster.log")
    );
    assert_eq!(args.node_args().log_level.as_deref(), Some("debug"));
    assert_eq!(args.node_args().minimum_log_level(), LevelFilter::Debug);

    const DEFAULT_LOG_FLUSH_INTERVAL: i32 = 0;
    let mut logging =
      LoggingBuilder::new().with_minimum_level(args.node_args().minimum_log_level());
    if let Some(file) = &args.node_args().file_logger {
      logging = logging.add_file(file, DEFAULT_LOG_FLUSH_INTERVAL);
    }
    drop(logging);
  }

  #[test]
  fn test_cluster_args_log_level_variants() {
    let cases = [
      ("trace", LevelFilter::Trace),
      ("TRACE", LevelFilter::Trace),
      ("debug", LevelFilter::Debug),
      ("Debug", LevelFilter::Debug),
      ("info", LevelFilter::Info),
      ("INFO", LevelFilter::Info),
      ("warn", LevelFilter::Warn),
      ("warning", LevelFilter::Warn),
      ("WARNING", LevelFilter::Warn),
      ("error", LevelFilter::Error),
      ("ERROR", LevelFilter::Error),
      ("off", LevelFilter::Off),
      ("OFF", LevelFilter::Off),
      ("unknown_level", LevelFilter::Info),
    ];

    for (level_str, expected) in cases {
      let args = ClusterArgs::try_parse_from(["wedb", "--log-level", level_str]).unwrap();
      assert_eq!(args.node_args().minimum_log_level(), expected);
    }
  }
}
