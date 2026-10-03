//! WeDB 分布式集群命令行与服务参数配置

use std::fs::read_to_string;

use clap::{ArgMatches, Parser};
use toml_spanner::Arena;
use wconf::{
  ConfigFileArgs, NodeArgs, NodeOptionsError, ServerArgs,
  runtime_server_options::{
    DEFAULT_CLUSTER_CONFIG_FILE, DEFAULT_CLUSTER_NODE_TIMEOUT_MS, DEFAULT_GOSSIP_DELAY_SECS,
    DEFAULT_GOSSIP_SAMPLE_PERCENT,
  },
};

use crate::server::cluster::ClusterPreferredEndpointType;

#[derive(Debug, toml_spanner::Toml, Default)]
#[toml(ignore_unknown_fields)]
pub struct ClusterFileOptions {
  pub cluster_config_file: Option<String>,
  pub cluster_config_flush_frequency_ms: Option<i32>,
  pub clean_cluster_config: Option<bool>,
  pub cluster_node_timeout_ms: Option<u64>,
  pub gossip_delay_secs: Option<u64>,
  pub gossip_sample_percent: Option<i32>,
  pub cluster_announce_ip: Option<String>,
  pub cluster_announce_port: Option<u16>,
  pub cluster_preferred_endpoint_type: Option<ClusterPreferredEndpointType>,
}

/// WeDB 分布式集群节点参数
#[derive(Debug, Parser)]
#[command(author, version, about = "WeDB 高性能分布式集群节点")]
pub struct ClusterArgs {
  /// 节点通用参数（端口、工作目录、线程、WAL路径等）
  #[command(flatten)]
  pub node: NodeArgs,

  /// 集群拓扑配置文件存储路径（缺省为 `<dir>`/nodes.conf）
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
  #[arg(long = "clean-cluster-config", default_value_t = false, action = clap::ArgAction::Set, num_args = 0..=1, default_missing_value = "true")]
  pub clean_cluster_config: bool,

  /// 集群节点心跳与故障检测超时毫秒数（0 = 无限超时；
  /// 对标 GarnetServerOptions.cs:251 ClusterTimeout，默认 60 秒）。
  /// 亚秒值（1..=999）启动期拒启——CONFIG 秒槽整秒粒度承载不了亚秒，
  /// 见 server::boot 亚秒门
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
        let arena = Arena::new();
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
  /// 获取集群配置文件存储路径（未指定时默认为 `<dir>`/nodes.conf）
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
