//! WeDB 分布式集群命令行与服务参数配置

use std::fs::read_to_string;

use clap::{ArgMatches, Parser};
use wconf::{
  ConfigFileArgs, NodeArgs, NodeOptionsError, ServerArgs,
  runtime_server_options::DEFAULT_CLUSTER_TIMEOUT,
};

use crate::server::cluster::ClusterPreferredEndpointType;

/// 默认集群节点心跳与故障检测超时毫秒数
/// （garnet/libs/server/Servers/GarnetServerOptions.cs:251 ClusterTimeout = 60 秒；
/// 0 = 无限超时哨兵，见 cluster_provider::cluster_node_timeout）
pub const DEFAULT_CLUSTER_NODE_TIMEOUT_MS: u64 = DEFAULT_CLUSTER_TIMEOUT as u64 * 1000;
/// 默认集群 gossip 周期秒数（garnet/libs/server/Servers/GarnetServerOptions.cs:246 GossipDelay）
pub const DEFAULT_GOSSIP_DELAY_SECS: u64 = 5;
/// 默认集群 gossip 周期毫秒数（= C# GossipDelay 5 秒 * 1000，秒→毫秒换算单点在此）
pub const DEFAULT_GOSSIP_DELAY_MS: u64 = DEFAULT_GOSSIP_DELAY_SECS * 1000;
/// 默认 gossip 抽样百分比（garnet/libs/server/Servers/GarnetServerOptions.cs:241 GossipSamplePercent）
pub const DEFAULT_GOSSIP_SAMPLE_PERCENT: i32 = 100;
/// 默认集群拓扑配置文件名（node dir 内）
pub const DEFAULT_CLUSTER_CONFIG_FILE: &str = "nodes.conf";

#[derive(Debug, Clone, toml_spanner::Toml, Default)]
#[toml(ignore_unknown_fields)]
struct ClusterFileOptions {
  cluster_config_file: Option<String>,
  cluster_config_flush_frequency_ms: Option<i32>,
  clean_cluster_config: Option<bool>,
  cluster_node_timeout_ms: Option<u64>,
  gossip_delay_secs: Option<u64>,
  gossip_sample_percent: Option<i32>,
  cluster_announce_ip: Option<String>,
  cluster_announce_port: Option<u16>,
  cluster_preferred_endpoint_type: Option<ClusterPreferredEndpointType>,
}

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

  /// 集群拓扑刷盘频率毫秒数（-1 = 纯内存永不落盘；0 = 每变更即时刷盘；
  /// >0 = 周期刷盘；对标 garnet/libs/host/Configuration/Options.cs
  /// > cluster-config-flush-frequency，GarnetServerOptions 默认 0）
  #[arg(
    long = "cluster-config-flush-frequency",
    default_value_t = 0,
    allow_negative_numbers = true
  )]
  pub cluster_config_flush_frequency_ms: i32,

  /// 以清洁集群配置启动，跳过拓扑盘恢复
  /// （对标 garnet/libs/host/Configuration/Options.cs:CleanClusterConfig）
  #[arg(long = "clean-cluster-config", default_value_t = false)]
  pub clean_cluster_config: bool,

  /// 集群节点心跳与故障检测超时毫秒数（0 = 无限超时；
  /// 对标 GarnetServerOptions.cs:251 ClusterTimeout，默认 60 秒）
  #[arg(long, default_value_t = DEFAULT_CLUSTER_NODE_TIMEOUT_MS)]
  pub cluster_node_timeout_ms: u64,

  /// 集群 gossip 协议每节点发送更新配置的周期秒数
  #[arg(long, default_value_t = DEFAULT_GOSSIP_DELAY_SECS)]
  pub gossip_delay_secs: u64,

  /// 每轮 gossip 与多少百分比的集群节点通信（0-100，100 = 全量广播）
  #[arg(long, default_value_t = DEFAULT_GOSSIP_SAMPLE_PERCENT)]
  pub gossip_sample_percent: i32,

  /// 集群 gossip 向其他节点宣告的连接 IP（对标 C# Options.cs:53-54
  /// > --cluster-announce-ip IpAddressValidation）。配置后经启动期校验：
  /// > 须为 IP 字面量 / `localhost` / 本机主机名，且与监听端点匹配（端口
  /// > 相等、地址相等或监听为 Any），不匹配即拒启；未配置时按监听端点宣告。
  /// > 校验与 Any 绑定出口探测见 [`crate::server::announce`]
  #[arg(long = "cluster-announce-ip")]
  pub cluster_announce_ip: Option<String>,

  /// 集群 gossip 向其他节点宣告的连接端口（0 = 随监听端口，对标 C#
  /// Options.cs:49-52 > --cluster-announce-port IntRangeValidation(0, 65535)
  /// 的 0 哨兵）。语义登记：C# :805 匹配校验要求宣告端口等于某监听端点
  /// 端口，而监听端点恒以单一 port 展开，故显式值不等于监听端口即拒启
  /// ——「仅宣告端口与监听端口解耦」的容器映射能力在 C# 现行校验下不
  /// 存在，本字段 1:1 保持该约束
  #[arg(long = "cluster-announce-port", default_value_t = 0)]
  pub cluster_announce_port: u16,

  /// 集群重定向（MOVED/ASK）与 CLUSTER SLOTS/SHARDS 输出向客户端通告的
  /// 端点形态（值 ip/hostname/unknown；对标 C# Options.cs:60 选项
  /// cluster-preferred-endpoint-type，默认 ip）
  #[arg(long = "cluster-preferred-endpoint-type", default_value = "ip")]
  pub cluster_preferred_endpoint_type: ClusterPreferredEndpointType,
}

impl ServerArgs for ClusterArgs {
  #[inline]
  fn node_args(&self) -> &NodeArgs {
    &self.node
  }
}

impl ConfigFileArgs for ClusterArgs {
  fn from_layered_matches(matches: &ArgMatches) -> Result<Self, NodeOptionsError> {
    let cli = <Self as clap::FromArgMatches>::from_arg_matches(matches)?;
    let (import_path, export_path) = (cli.node.config.clone(), cli.node.config_export_path.clone());
    let mut merged = match import_path.as_deref() {
      Some(path) => {
        let content = read_to_string(path)?;
        let node = NodeArgs::from_toml_str(&content)?;
        let arena = toml_spanner::Arena::new();
        let mut doc = toml_spanner::parse(&content, &arena)?;
        let cluster_opts: ClusterFileOptions = doc.to()?;
        let mut def = Self {
          node,
          ..Self::default()
        };
        if let Some(v) = cluster_opts.cluster_config_file {
          def.cluster_config_file = Some(v);
        }
        if let Some(v) = cluster_opts.cluster_config_flush_frequency_ms {
          def.cluster_config_flush_frequency_ms = v;
        }
        if let Some(v) = cluster_opts.clean_cluster_config {
          def.clean_cluster_config = v;
        }
        if let Some(v) = cluster_opts.cluster_node_timeout_ms {
          def.cluster_node_timeout_ms = v;
        }
        if let Some(v) = cluster_opts.gossip_delay_secs {
          def.gossip_delay_secs = v;
        }
        if let Some(v) = cluster_opts.gossip_sample_percent {
          def.gossip_sample_percent = v;
        }
        if let Some(v) = cluster_opts.cluster_announce_ip {
          def.cluster_announce_ip = Some(v);
        }
        if let Some(v) = cluster_opts.cluster_announce_port {
          def.cluster_announce_port = v;
        }
        if let Some(v) = cluster_opts.cluster_preferred_endpoint_type {
          def.cluster_preferred_endpoint_type = v;
        }
        def
      }
      None => Self::default(),
    };

    merged.node.override_explicit(matches, cli.node);

    use clap::parser::ValueSource;
    macro_rules! over {
      ($($f:ident),+ $(,)?) => {
        $(if matches.value_source(stringify!($f)) == Some(ValueSource::CommandLine) {
          merged.$f = cli.$f.clone();
        })+
      };
    }
    over![
      cluster_config_file,
      cluster_config_flush_frequency_ms,
      clean_cluster_config,
      cluster_node_timeout_ms,
      gossip_delay_secs,
      gossip_sample_percent,
      cluster_announce_ip,
      cluster_announce_port,
      cluster_preferred_endpoint_type,
    ];

    merged.node.config = import_path;
    merged.node.config_export_path = export_path;
    merged.node.validate()?;
    if let Some(path) = &merged.node.config_export_path
      && let Err(err) = merged.node.export_config(path)
    {
      log::warn!("导出配置到 {} 失败: {err}，继续启动", path.display());
    }
    Ok(merged)
  }
}

impl Default for ClusterArgs {
  fn default() -> Self {
    Self {
      node: NodeArgs::default(),
      cluster_config_file: None,
      cluster_config_flush_frequency_ms: 0,
      clean_cluster_config: false,
      cluster_node_timeout_ms: DEFAULT_CLUSTER_NODE_TIMEOUT_MS,
      gossip_delay_secs: DEFAULT_GOSSIP_DELAY_SECS,
      gossip_sample_percent: DEFAULT_GOSSIP_SAMPLE_PERCENT,
      cluster_announce_ip: None,
      cluster_announce_port: 0,
      cluster_preferred_endpoint_type: ClusterPreferredEndpointType::Ip,
    }
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
  use std::{env::temp_dir, fs, process};

  use clap::Parser;
  use log::LevelFilter;
  use wconf::ServerArgs;

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
    // 装配段不在此重抄（原第三份手抄）：单点见 wnode `LoggingBuilder::from_node`，
    // 其构造断言随码在 wnode logging 单测
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
      ("critical", LevelFilter::Error),
      ("CRITICAL", LevelFilter::Error),
      ("off", LevelFilter::Off),
      ("OFF", LevelFilter::Off),
      ("none", LevelFilter::Off),
      ("NONE", LevelFilter::Off),
      ("information", LevelFilter::Info),
      ("INFORMATION", LevelFilter::Info),
      ("unknown_level", LevelFilter::Info),
    ];

    for (level_str, expected) in cases {
      let args = ClusterArgs::try_parse_from(["wedb", "--log-level", level_str]).unwrap();
      assert_eq!(args.node_args().minimum_log_level(), expected);
    }
  }

  #[test]
  fn test_cluster_args_preferred_endpoint_type() {
    // 默认 ip（C# enum 首成员 Ip = 默认值）
    let args = ClusterArgs::try_parse_from(["wedb"]).unwrap();
    assert_eq!(
      args.cluster_preferred_endpoint_type,
      ClusterPreferredEndpointType::Ip
    );

    // 显式 hostname（重定向通告主机名形态）
    let args =
      ClusterArgs::try_parse_from(["wedb", "--cluster-preferred-endpoint-type", "hostname"])
        .unwrap();
    assert_eq!(
      args.cluster_preferred_endpoint_type,
      ClusterPreferredEndpointType::Hostname
    );

    // unknown / 非法值
    let args =
      ClusterArgs::try_parse_from(["wedb", "--cluster-preferred-endpoint-type", "unknown"])
        .unwrap();
    assert_eq!(
      args.cluster_preferred_endpoint_type,
      ClusterPreferredEndpointType::Unknown
    );
    assert!(
      ClusterArgs::try_parse_from(["wedb", "--cluster-preferred-endpoint-type", "dnswithcare"])
        .is_err()
    );
  }

  #[test]
  fn test_cluster_args_announce_ip_port_parse() {
    // 缺省：未配宣告 IP、宣告端口 0 哨兵（随监听端口，对标 C# 默认值）
    let args = ClusterArgs::try_parse_from(["wedb"]).unwrap();
    assert_eq!(args.cluster_announce_ip, None);
    assert_eq!(args.cluster_announce_port, 0);

    // 显式配置（对标 C# --cluster-announce-ip / --cluster-announce-port）
    let args = ClusterArgs::try_parse_from([
      "wedb",
      "--cluster-announce-ip",
      "10.1.2.3",
      "--cluster-announce-port",
      "7000",
    ])
    .unwrap();
    assert_eq!(args.cluster_announce_ip.as_deref(), Some("10.1.2.3"));
    assert_eq!(args.cluster_announce_port, 7000);

    // 端口越界拒解析（对标 C# IntRangeValidation(0, 65535)，u16 界内单点）
    assert!(ClusterArgs::try_parse_from(["wedb", "--cluster-announce-port", "65536"]).is_err());
  }

  #[test]
  fn test_cluster_args_config_file_and_cli_extras() {
    // 路径掺进程 id：temp_dir 用户级跨进程共享，固定名会与并发跑套件的其他
    // 进程互踩（一进程 remove、另一进程 read_to_string 即 Io 红）
    let file = temp_dir().join(format!("wedb-cluster-args-config-{}.toml", process::id()));
    fs::write(
      &file,
      "port = 7010\nslow_log_threshold = 3000\nunknown_key = 1\ngossip_delay_secs = 10\n",
    )
    .unwrap();
    let args = ClusterArgs::from_args_iter([
      "wedb",
      "--config",
      file.to_str().unwrap(),
      "--port",
      "7011",
      "--cluster-node-timeout-ms",
      "30000",
    ])
    .unwrap();
    fs::remove_file(&file).ok();
    // node 域：CLI 覆盖 + 文件值生效
    assert_eq!(args.node_args().port, 7011);
    assert_eq!(args.node_args().slow_log_threshold, 3000);
    // 集群扩展参数取 CLI/文件值
    assert_eq!(args.gossip_delay_secs, 10);
    assert_eq!(args.cluster_node_timeout_ms, 30000);
    assert_eq!(args.cluster_config_path(), "./data/nodes.conf");
    // 拓扑持久化选项：缺省即时刷盘 + 非清洁启动（对标 C# 默认值）
    assert_eq!(args.cluster_config_flush_frequency_ms, 0);
    assert!(!args.clean_cluster_config);

    // 显式覆盖：纯内存模式 + 清洁启动 + 自定义拓扑文件路径
    let args = ClusterArgs::try_parse_from([
      "wedb",
      "--cluster-config-flush-frequency",
      "-1",
      "--clean-cluster-config",
      "--cluster-config-file",
      "/var/lib/wedb/topology.conf",
    ])
    .unwrap();
    assert_eq!(args.cluster_config_flush_frequency_ms, -1);
    assert!(args.clean_cluster_config);
    assert_eq!(args.cluster_config_path(), "/var/lib/wedb/topology.conf");
  }
}
