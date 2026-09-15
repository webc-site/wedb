//! WeDB 分布式集群命令行与服务参数配置

use clap::{ArgMatches, Parser};
use wconf::{ConfigFileArgs, NodeArgs, NodeOptionsError, ServerArgs};

/// 默认集群节点心跳与故障检测超时毫秒数
pub const DEFAULT_CLUSTER_NODE_TIMEOUT_MS: u64 = 15000;
/// 默认集群 gossip 周期秒数（garnet/libs/server/Servers/GarnetServerOptions.cs:246 GossipDelay）
pub const DEFAULT_GOSSIP_DELAY_SECS: u64 = 5;
/// 默认 gossip 抽样百分比（garnet/libs/server/Servers/GarnetServerOptions.cs:241 GossipSamplePercent）
pub const DEFAULT_GOSSIP_SAMPLE_PERCENT: i32 = 100;
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

  /// 集群 gossip 协议每节点发送更新配置的周期秒数
  #[arg(long, default_value_t = DEFAULT_GOSSIP_DELAY_SECS)]
  pub gossip_delay_secs: u64,

  /// 每轮 gossip 与多少百分比的集群节点通信（0-100，100 = 全量广播）
  #[arg(long, default_value_t = DEFAULT_GOSSIP_SAMPLE_PERCENT)]
  pub gossip_sample_percent: i32,
}

impl ServerArgs for ClusterArgs {
  #[inline]
  fn node_args(&self) -> &NodeArgs {
    &self.node
  }
}

impl ConfigFileArgs for ClusterArgs {
  fn from_layered_matches(matches: &ArgMatches) -> Result<Self, NodeOptionsError> {
    let mut cli = <Self as clap::FromArgMatches>::from_arg_matches(matches)?;
    // node 域三层合并（文件为基、CLI 显式覆盖）；集群扩展参数仅命令行面
    cli.node = NodeArgs::from_layered_matches(matches)?;
    Ok(cli)
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
  use std::fs;

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

  #[test]
  fn test_cluster_args_config_file_and_cli_extras() {
    // 文件为基（node 域）+ CLI 显式覆盖；集群扩展参数仅命令行面
    // （serde 忽略文件中的未知键，嵌套文本只承载 NodeArgs 字段）
    let file = std::env::temp_dir().join("wedb-cluster-args-config.nt");
    fs::write(
      &file,
      "port: 7010\nslow_log_threshold: 3000\nunknown_key: 1\n",
    )
    .unwrap();
    let args = ClusterArgs::from_args_iter([
      "wedb",
      "--config",
      file.to_str().unwrap(),
      "--port",
      "7011",
      "--gossip-delay-secs",
      "9",
      "--cluster-node-timeout-ms",
      "30000",
    ])
    .unwrap();
    fs::remove_file(&file).ok();
    // node 域：CLI 覆盖 + 文件值生效
    assert_eq!(args.node_args().port, 7011);
    assert_eq!(args.node_args().slow_log_threshold, 3000);
    // 集群扩展参数取 CLI 值
    assert_eq!(args.gossip_delay_secs, 9);
    assert_eq!(args.cluster_node_timeout_ms, 30000);
    assert_eq!(args.cluster_config_path(), "./data/nodes.conf");
  }
}
