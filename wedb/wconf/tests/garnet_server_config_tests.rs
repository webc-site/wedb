//! 服务器配置集成测试（对标 test/standalone/Garnet.test/GarnetServerConfigTests.cs）
//!
//! 针对活配置面（RuntimeServerConfig / RuntimeServerOptions / NodeArgs / wconf::size）
//! 的默认值覆盖、配置槽位播种、路径推导与尺寸换算进行验证。

use std::path::PathBuf;

use wconf::{
  DEFAULT_BIND, DEFAULT_DIR, DEFAULT_PORT, DEFAULT_PUBSUB_PAGE_SIZE, DEFAULT_RESP_VERSION,
  LogCompactionType, NodeArgs, RuntimeServerConfig, RuntimeServerOptions, ServerConfigType,
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
  assert_eq!(o.aof_tail_witness_freq_ms, 100);
  assert_eq!(o.aof_sync_max_lag_bytes, -1);
  assert_eq!(o.replica_diskless_sync_delay, 5);
  assert_eq!(o.replica_attach_timeout_secs, 60);
  assert_eq!(o.cluster_replication_reestablishment_timeout, 0);
  assert_eq!(o.compaction_max_segments, 32);
  assert!(!o.compaction_force_delete);
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
  assert!(!o.wait_for_commit);
  assert_eq!(o.aof_size_limit.as_deref(), Some(""));
  assert!(!o.fast_aof_truncate);

  // 节点通用参数默认值
  let args = NodeArgs::default();
  // 保护模式默认开，bind 未显式 → 端点回环回退
  assert_eq!(args.bind, None);
  assert!(args.protected_mode);
  assert_eq!(
    args.endpoints(),
    vec![format!("{DEFAULT_BIND}:{DEFAULT_PORT}")]
  );
  assert_eq!(args.slow_log_max_entries, 128);
  assert_eq!(args.max_databases, 16);
  assert_eq!(args.object_scan_count_limit, 1000);
  assert_eq!(args.metrics_sampling_frequency_secs, 0);
  assert_eq!(args.dir, PathBuf::from(DEFAULT_DIR));
  assert_eq!(args.wal_dir(), PathBuf::from("./data/wal"));
  assert_eq!(args.pubsub_page_size, DEFAULT_PUBSUB_PAGE_SIZE);
  assert_eq!(DEFAULT_RESP_VERSION, 2);
  assert!(!args.aof);
  assert!(!args.disable_pubsub);
  assert!(!args.recover);
  assert!(!args.enable_lua);
  assert_eq!(args.lua_script_timeout_ms, 0);
  assert!(!args.lua_transaction_mode);
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
    100
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
  assert!(!cfg.get_bool(ServerConfigType::CompactionForceDelete));
  assert!(cfg.get_bool(ServerConfigType::SgGet));

  // 只读回落项
  assert_eq!(cfg.resp_format(ServerConfigType::Databases), "16");
  assert_eq!(cfg.resp_format(ServerConfigType::ClusterEnabled), "no");
  assert_eq!(cfg.resp_format(ServerConfigType::AppendOnly), "no");
}

// ======================== 路径推导（PathDerivation） ========================

/// 验证 RuntimeServerOptions 检查点与 AOF 目录推导
#[test]
fn runtime_server_options_directory_paths() {
  let opts = RuntimeServerOptions {
    checkpoint_base_directory: "/data".to_string(),
    ..Default::default()
  };

  // 目录名对标
  assert_eq!(
    RuntimeServerOptions::get_checkpoint_directory_name(0),
    "checkpoints"
  );
  assert_eq!(
    RuntimeServerOptions::get_checkpoint_directory_name(3),
    "checkpoints_3"
  );
  assert_eq!(
    RuntimeServerOptions::get_append_only_file_directory_name(0),
    "AOF"
  );
  assert_eq!(
    RuntimeServerOptions::get_append_only_file_directory_name(2),
    "AOF_2"
  );

  // 完整路径推导
  assert_eq!(
    opts.get_store_checkpoint_directory(1),
    PathBuf::from("/data/Store/checkpoints_1")
  );
  assert_eq!(
    opts.get_append_only_file_directory(0),
    PathBuf::from("/data/AOF")
  );
  assert_eq!(
    opts.get_append_only_file_directory(2),
    PathBuf::from("/data/AOF_2")
  );

  // 缺省回落测试
  let bare = RuntimeServerOptions::default();
  assert_eq!(
    bare.get_store_checkpoint_directory(0),
    PathBuf::from("Store/checkpoints")
  );
  assert_eq!(bare.get_append_only_file_directory(0), PathBuf::from("AOF"));
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
