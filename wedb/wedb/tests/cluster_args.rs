#![recursion_limit = "512"] // 泛型设备实例化下 async 状态机嵌套深（R27 CI 实证）
use std::{env::temp_dir, fs, path::Path, process};

use clap::Parser;
use log::LevelFilter;
use toml_spanner::Arena;
use wconf::{ConfigFileArgs, ServerArgs};
use wedb::{
  args::{ClusterArgs, ClusterFileOptions},
  server::cluster::ClusterPreferredEndpointType,
};

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
    ("verbose", LevelFilter::Trace),
    ("notice", LevelFilter::Info),
    ("nothing", LevelFilter::Off),
    ("4", LevelFilter::Error),
  ];

  for (level_str, expected) in cases {
    let args = ClusterArgs::try_parse_from(["wedb", "--log-level", level_str]).unwrap();
    assert_eq!(args.node_args().minimum_log_level(), expected);
  }

  assert!(ClusterArgs::try_parse_from(["wedb", "--log-level", "unknown_level"]).is_err());
}

#[test]
fn test_cluster_args_preferred_endpoint_type() {
  // 默认 ip（C# enum 首成员 Ip = 默认值）
  let args = ClusterArgs::try_parse_from(["wedb"]).unwrap();
  assert_eq!(
    args.cluster_preferred_endpoint_type,
    ClusterPreferredEndpointType::Ip
  );

  // 显式 hostname 与大小写混形支持（Hostname / HOSTNAME 与 hostname 同位）
  for name in ["hostname", "Hostname", "HOSTNAME", "HostName"] {
    let args =
      ClusterArgs::try_parse_from(["wedb", "--cluster-preferred-endpoint-type", name]).unwrap();
    assert_eq!(
      args.cluster_preferred_endpoint_type,
      ClusterPreferredEndpointType::Hostname,
      "未能解析 {name}"
    );
  }

  // unknown 小写形回归不破及大小写混形
  for name in ["unknown", "Unknown", "UNKNOWN"] {
    let args =
      ClusterArgs::try_parse_from(["wedb", "--cluster-preferred-endpoint-type", name]).unwrap();
    assert_eq!(
      args.cluster_preferred_endpoint_type,
      ClusterPreferredEndpointType::Unknown,
      "未能解析 {name}"
    );
  }

  // ip 小写形回归不破及大小写混形
  for name in ["ip", "Ip", "IP"] {
    let args =
      ClusterArgs::try_parse_from(["wedb", "--cluster-preferred-endpoint-type", name]).unwrap();
    assert_eq!(
      args.cluster_preferred_endpoint_type,
      ClusterPreferredEndpointType::Ip,
      "未能解析 {name}"
    );
  }

  // 数字 0/1/2 分别入位 Ip/Hostname/Unknown（对位 C# Enum.Parse+IsDefinedEx）
  for (num, want) in [
    ("0", ClusterPreferredEndpointType::Ip),
    ("1", ClusterPreferredEndpointType::Hostname),
    ("2", ClusterPreferredEndpointType::Unknown),
  ] {
    let args =
      ClusterArgs::try_parse_from(["wedb", "--cluster-preferred-endpoint-type", num]).unwrap();
    assert_eq!(
      args.cluster_preferred_endpoint_type, want,
      "数字 {num} 未能映射到期望的变体"
    );
  }

  // 非法值（dnswithcare）与越界数字（3 等）维持拒
  for invalid in ["dnswithcare", "3", "255", "-1", "foo"] {
    assert!(
      ClusterArgs::try_parse_from(["wedb", "--cluster-preferred-endpoint-type", invalid]).is_err(),
      "{invalid} 应被拒绝"
    );
  }

  // TOML 侧同名单源不破
  let arena = Arena::new();
  let mut doc =
    toml_spanner::parse("cluster_preferred_endpoint_type = 'Hostname'", &arena).unwrap();
  let opts: ClusterFileOptions = doc.to().unwrap();
  assert_eq!(
    opts.cluster_preferred_endpoint_type,
    Some(ClusterPreferredEndpointType::Hostname)
  );
  let mut doc = toml_spanner::parse("cluster_preferred_endpoint_type = '1'", &arena).unwrap();
  let opts: ClusterFileOptions = doc.to().unwrap();
  assert_eq!(
    opts.cluster_preferred_endpoint_type,
    Some(ClusterPreferredEndpointType::Hostname)
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
  assert_eq!(
    args.cluster_config_path(),
    Path::new("./data").join("nodes.conf").display().to_string()
  );
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
