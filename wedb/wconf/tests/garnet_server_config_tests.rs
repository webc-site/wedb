//! 服务器配置集成测试（对标 test/standalone/Garnet.test/GarnetServerConfigTests.cs）
//!
//! 针对活配置面（RuntimeServerConfig / RuntimeServerOptions / NodeArgs / wconf::size）
//! 的默认值覆盖、配置槽位播种与尺寸换算进行验证。
//!
//! 在 garnet 中的相对路径: test/standalone/Garnet.test/GarnetServerConfigTests.cs + libs/host/ServerSettingsManager.cs

use std::path::PathBuf;

use wbase::cfg::LogCompactionType;
use wconf::{
  DEFAULT_DIR, DEFAULT_PORT, DEFAULT_RESP_VERSION, NodeArgs, RuntimeServerConfig,
  RuntimeServerOptions, ServerConfigType,
  size::{
    MIN_PAGE_SIZE_BYTES, log2_exact, next_power_of_2, parse_size, parse_size_bytes, pretty_size,
    previous_power_of_2, try_parse_size, try_parse_size_bytes,
  },
};

// ======================== 默认值覆盖（DefaultConfigurationOptionsCoverage） ========================

/// 活配置面（RuntimeServerOptions + NodeArgs）默认值落位对齐
#[test]
fn default_configuration_options_coverage() {
  let o = RuntimeServerOptions::default();
  assert_eq!(o.cluster_timeout, 60);
  assert_eq!(o.replica_sync_delay_ms, 5);
  assert_eq!(o.aof_replay_max_lag_bytes, -1);
  assert_eq!(o.aof_tail_witness_freq_ms, 10);
  assert_eq!(o.aof_sync_max_lag_bytes, -1);
  assert_eq!(o.replica_diskless_sync_delay, 5);
  assert_eq!(o.replica_attach_timeout_secs, 60);
  assert_eq!(o.cluster_replication_reestablishment_timeout, 0);
  assert_eq!(o.compaction_max_segments, 32);
  assert_eq!(o.compaction_type, LogCompactionType::None);
  assert_eq!(o.slow_log_threshold, 0);
  assert_eq!(o.object_scan_count_limit, 1000);
  assert!(o.enable_scatter_gather_get);
  assert_eq!(o.aof_size_limit_enforce_frequency_secs, 5);
  assert_eq!(o.commit_frequency_ms, 0);
  assert_eq!(o.expired_object_collection_frequency_secs, 0);
  assert_eq!(o.expired_key_deletion_scan_frequency_secs, -1);
  assert!(!o.enable_aof);
  assert_eq!(o.max_databases, 16);
  assert!(o.checkpoint_base_directory.is_empty());
  assert!(o.log_dir.is_none());
  assert!(o.unix_socket_path.is_none());
  assert!(!o.enable_cluster);
  assert_eq!(o.aof_memory_size.as_deref(), Some("128m"));
  assert_eq!(o.aof_page_size.as_deref(), Some("32m"));
  assert_eq!(o.aof_segment_size.as_deref(), Some("1g"));
  assert_eq!(o.aof_physical_sublog_count, 1);
  assert_eq!(o.aof_replay_task_count, 1);
  assert_eq!(o.replay_drift_threshold, -1);
  assert_eq!(o.replay_drift_check_freq, 1);
  assert!(!o.wait_for_commit);
  assert_eq!(o.aof_size_limit.as_deref(), Some(""));
  assert!(!o.fast_aof_truncate);
  // 按需检查点默认真（对标 C# defaults.conf:346 与 GarnetServerOptions.cs:405）
  assert!(o.on_demand_checkpoint);

  // 节点通用参数默认值
  let args = NodeArgs::default();
  // 保护模式默认开，bind 未显式 → 端点回环回退
  assert_eq!(args.bind, None);
  assert!(args.protected_mode);
  assert_eq!(
    args.endpoints().unwrap(),
    vec![
      format!("127.0.0.1:{DEFAULT_PORT}"),
      format!("[::1]:{DEFAULT_PORT}"),
    ]
  );
  assert_eq!(args.slow_log_max_entries, 128);
  assert_eq!(args.max_databases, 16);
  assert_eq!(args.object_scan_count_limit, 1000);
  assert_eq!(args.metrics_sampling_frequency_secs, 0);
  assert_eq!(args.dir, PathBuf::from(DEFAULT_DIR));
  assert_eq!(args.wal_dir(), PathBuf::from("./data/wal"));
  assert_eq!(DEFAULT_RESP_VERSION, 2);
  assert!(!args.aof);
  assert!(!args.disable_pubsub);
  assert!(!args.recover);
  // 无盘同步默认关闭（对标 defaults.conf:349 ReplicaDisklessSync false）
  assert!(!args.repl_diskless_sync);
  // AOF 截断与按需检查点两旋钮默认值（defaults.conf:343 false / :346 true）
  assert!(!args.fast_aof_truncate);
  assert!(args.on_demand_checkpoint);
  assert!(!args.enable_lua);
  assert_eq!(args.lua_script_timeout_ms, 0);
  // TLS 入站默认要求客户端证书（mTLS，对标 defaults.conf:250
  // ClientCertificateRequired: true 生效默认，zcode-r30-defaults 立项二）
  assert!(args.tls_client_cert_required);
  assert_eq!(args.tls_cert, None);
  assert_eq!(args.tls_key, None);
  assert_eq!(args.tls_issuer_cert, None);
}

/// 入站 mTLS 旋钮命令行与配置文件两路装配（Options.cs:333
/// client-certificate-required 与 :339 issuer-certificate-path 的对位面）
#[test]
fn test_tls_client_cert_required_knobs() {
  use std::{env::temp_dir, fs};

  use wconf::{ConfigFileArgs, NodeArgs};

  // 命令行旗标翻转
  let args = NodeArgs::from_args_iter(["wedb", "--tls-client-cert-required"]).unwrap();
  assert!(args.tls_client_cert_required);

  // 配置文件同路生效（toml 漏斗）
  let path = temp_dir().join("wedb-wconf-mtls-knob.toml");
  fs::write(&path, "tls_client_cert_required = true\n").unwrap();
  let args = NodeArgs::from_args_iter(["wedb", "--config", path.to_str().unwrap()]).unwrap();
  fs::remove_file(&path).ok();
  assert!(args.tls_client_cert_required);

  // issuer 路径旋钮同路可配
  let args = NodeArgs::from_args_iter([
    "wedb",
    "--tls-issuer-cert",
    "/tmp/ca.pem",
    "--tls-client-cert-required",
  ])
  .unwrap();
  assert_eq!(args.tls_issuer_cert, Some(PathBuf::from("/tmp/ca.pem")));
  assert!(args.tls_client_cert_required);

  // 命令行显式关闭回落单向 TLS（zcode-r30-defaults 立项二验证点）
  let args = NodeArgs::from_args_iter(["wedb", "--tls-client-cert-required", "false"]).unwrap();
  assert!(!args.tls_client_cert_required);

  // 配置文件显式 false 同路回落
  let path = temp_dir().join("wedb-wconf-mtls-off.toml");
  fs::write(&path, "tls_client_cert_required = false\n").unwrap();
  let args = NodeArgs::from_args_iter(["wedb", "--config", path.to_str().unwrap()]).unwrap();
  fs::remove_file(&path).ok();
  assert!(!args.tls_client_cert_required);
}

// ======================== 运行时配置播种（RuntimeServerConfig Seeding） ========================

/// 验证 RuntimeServerConfig 从 RuntimeServerOptions 播种后的槽位值
#[test]
fn runtime_server_config_seeding() {
  let cfg = RuntimeServerConfig::with_defaults();

  // 时长/频率类
  assert_eq!(cfg.get_seconds(ServerConfigType::ClusterNodeTimeout), 60);
  assert_eq!(cfg.get_milliseconds(ServerConfigType::ReplicaSyncDelay), 5);
  assert_eq!(
    cfg.get_milliseconds(ServerConfigType::AofTailWitnessFreq),
    10
  );
  assert_eq!(cfg.get_seconds(ServerConfigType::ReplDisklessSyncDelay), 5);
  assert_eq!(cfg.get_seconds(ServerConfigType::ReplAttachTimeout), 60);
  assert_eq!(
    cfg.get_seconds(ServerConfigType::ClusterReplicationReestablishmentTimeout),
    0
  );
  assert_eq!(
    cfg.get_int(ServerConfigType::AofSizeLimitEnforceFrequency),
    5
  );
  assert_eq!(cfg.get_int(ServerConfigType::AofCommitFreq), 0);
  assert_eq!(
    cfg.get_int(ServerConfigType::ExpiredObjectCollectionFreq),
    0
  );
  assert_eq!(
    cfg.get_int(ServerConfigType::ExpiredKeyDeletionScanFreq),
    -1
  );

  // 数值与上限
  assert_eq!(cfg.get_int(ServerConfigType::AofReplayMaxLagBytes), -1);
  assert_eq!(cfg.get_long(ServerConfigType::AofSyncMaxLagBytes), -1);
  assert_eq!(cfg.get_int(ServerConfigType::CompactionMaxSegments), 32);
  assert_eq!(cfg.get_int(ServerConfigType::SlowlogLogSlowerThan), 0);
  assert_eq!(cfg.get_int(ServerConfigType::ObjectScanCountLimit), 1000);

  // 布尔开关
  assert!(cfg.get_bool(ServerConfigType::SgGet));

  // 只读回落项
  assert_eq!(cfg.resp_format(ServerConfigType::Databases), "16");
  assert_eq!(cfg.resp_format(ServerConfigType::ClusterEnabled), "no");
  assert_eq!(cfg.resp_format(ServerConfigType::AppendOnly), "no");
}

// ======================== 尺寸解析与换算（SizeParsingAndConversion） ========================

/// 验证 wconf::size 尺寸解析与 2 的幂工具
#[test]
fn size_parsing_and_power_of_2_helpers() {
  // 纯数字与带后缀
  assert_eq!(parse_size("128"), (128, 3));
  assert_eq!(parse_size("0"), (0, 1));
  assert_eq!(parse_size("4k"), (4 * 1024, 2));
  assert_eq!(parse_size("4Kb"), (4 * 1024, 3));
  assert_eq!(parse_size("32m"), (32 * 1024 * 1024, 3));
  assert_eq!(parse_size("1g"), (1024 * 1024 * 1024, 2));
  assert_eq!(parse_size("2T"), (2i64 * 1024 * 1024 * 1024 * 1024, 2));
  assert_eq!(parse_size("1p"), (1024i64.pow(5), 2));

  // try_parse_size 全量消费校验
  assert_eq!(try_parse_size("16"), Some(16));
  assert_eq!(try_parse_size("16m"), Some(16 * 1024 * 1024));
  assert_eq!(try_parse_size(""), Some(0));
  assert_eq!(try_parse_size("x16"), None);
  assert_eq!(try_parse_size("16x"), None);

  // 字节切片形态
  assert_eq!(parse_size_bytes(b"32m"), (32 * 1024 * 1024, 3));
  assert_eq!(try_parse_size_bytes(b"4kb"), Some(4 * 1024));
  assert_eq!(try_parse_size_bytes(b"4kbx"), None);

  // 人类可读格式化
  assert_eq!(pretty_size(16 * 1024 * 1024 * 1024), "16g");
  assert_eq!(pretty_size(32 * 1024 * 1024), "32m");
  assert_eq!(pretty_size(4 * 1024), "4k");
  assert_eq!(pretty_size(512), "512");
  assert_eq!(pretty_size(1000), "0.9765625k");

  // 2 的幂辅助
  assert_eq!(previous_power_of_2(1024), 1024);
  assert_eq!(previous_power_of_2(1000), 512);
  assert_eq!(previous_power_of_2(1), 1);
  assert_eq!(previous_power_of_2(3), 2);
  assert_eq!(next_power_of_2(1000), 1024);
  assert_eq!(next_power_of_2(1024), 1024);
  assert_eq!(next_power_of_2(1), 1);
  assert_eq!(log2_exact(1024), 10);
  assert_eq!(log2_exact(16 * 1024 * 1024 * 1024), 34);

  // 最小页大小常量
  assert_eq!(MIN_PAGE_SIZE_BYTES, 512);
}
