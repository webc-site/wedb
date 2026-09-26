//! 票 wconf-cli-override-explicit-fields-omission：命令行显式项覆盖 11 个关键
//! 配置字段的全链用例。
//!
//! 对标 garnet/test/standalone/Garnet.test/GarnetServerConfigTests.cs 的
//! DeviceIoContexts / DeviceAioMaxDevicesOption 三段式断言（无显式项取默认、
//! 显式项解析生效、经 GetServerOptions 投影进运行时装配口），此处第三臂另经
//! RuntimeServerConfig::new 槽位播种核对「CLI 显式项 → 运行时真消费」。
//!
//! 修复前必红：NodeArgs::override_explicit 的 over! 清单遗漏本组 11 字段，
//! 配合 --config 时 CLI 显式值被文件基线静默丢弃，第一段断言即失败。
//!
//! 自研依据: 命令行覆盖仅显式字段（toml 配置契约，C# 无对应）

use std::{env::temp_dir, fmt, fs, path::PathBuf, process};

use wconf::{
  ConfigFileArgs, NodeArgs, RuntimeServerConfig, ServerArgs, ServerConfigType,
  node_options::{
    DEFAULT_AOF_SYNC_MAX_LAG_BYTES, DEFAULT_AOF_TAIL_WITNESS_FREQ_MS,
    DEFAULT_CLUSTER_REPLICATION_REESTABLISHMENT_TIMEOUT,
    DEFAULT_EXPIRED_KEY_DELETION_SCAN_FREQUENCY_SECS,
    DEFAULT_EXPIRED_OBJECT_COLLECTION_FREQUENCY_SECS, DEFAULT_REPLICA_ATTACH_TIMEOUT_SECS,
    DEFAULT_REPLICA_SYNC_DELAY_MS, DEFAULT_REPLICA_SYNC_TIMEOUT_SECS,
    DEFAULT_VECTOR_SET_QUANTIZATION_TASK_COUNT,
  },
};

/// 写临时 toml 配置文件（路径掺测试名与进程 id，杜绝并发互踩）
fn temp_config(name: &str, content: &str) -> PathBuf {
  let path = temp_dir().join(format!("wedb-cli-override-{name}-{}.toml", process::id()));
  fs::write(&path, content).unwrap();
  path
}

/// 11 个字段的文件基线旧值（与默认值、CLI 新值三方互异，覆盖丢失即红）
const FILE_BASELINE: &str = "\
replica_sync_timeout_secs = 11
replica_attach_timeout_secs = 61
replica_sync_delay_ms = 20
aof_sync_max_lag_bytes = 2048
aof_tail_witness_freq_ms = 40
cluster_replication_reestablishment_timeout = 22
vector_set_quantization_task_count = 4
enable_vector_set_preview = false
cluster_announce_hostname = \"file.example\"
expired_object_collection_frequency_secs = 30
expired_key_deletion_scan_frequency_secs = -1
";

fn cli_overrides() -> Vec<&'static str> {
  vec![
    "--repl-sync-timeout",
    "77",
    "--repl-attach-timeout",
    "120",
    "--replica-sync-delay",
    "25",
    "--aof-sync-max-lag-bytes",
    "4096",
    "--aof-tail-witness-freq",
    "50",
    "--cluster-replication-reestablishment-timeout",
    "33",
    "--vector-set-quantization-task-count",
    "8",
    "--enable-vector-set-preview",
    "--cluster-announce-hostname",
    "cli.example",
    "--expired-object-collection-freq",
    "60",
    "--expired-key-deletion-scan-freq",
    "45",
  ]
}

/// 逐一断言 11 字段等于期望值（消息点名字段，红时直指漏项）
macro_rules! assert_fields {
  ($args:expr, [$(($f:ident, $want:expr)),+ $(,)?]) => {
    {$($crate::assert_one(stringify!($f), $args.$f, $want);)+}
  };
}

fn assert_one<T: fmt::Debug + PartialEq>(field: &str, got: T, want: T) {
  assert_eq!(got, want, "字段 {field} 未按预期取值");
}

/// 第一段：--config 文件基线 + 11 个 CLI 显式项 → 显式给出即生效
#[test]
fn test_cli_explicit_overrides_file_baseline_all_11_fields() {
  let file = temp_config("ovr-base", FILE_BASELINE);
  let mut argv: Vec<&str> = vec!["wedb", "--config", file.to_str().unwrap()];
  argv.extend(cli_overrides());
  let args = NodeArgs::from_args_iter(&argv).unwrap();
  fs::remove_file(&file).ok();

  assert_fields!(
    args,
    [
      (replica_sync_timeout_secs, 77),
      (replica_attach_timeout_secs, 120),
      (replica_sync_delay_ms, 25),
      (aof_sync_max_lag_bytes, 4096),
      (aof_tail_witness_freq_ms, 50),
      (cluster_replication_reestablishment_timeout, 33),
      (vector_set_quantization_task_count, 8),
      (enable_vector_set_preview, true),
      (cluster_announce_hostname, "cli.example".to_string()),
      (expired_object_collection_frequency_secs, 60),
      (expired_key_deletion_scan_frequency_secs, 45),
    ]
  );
}

/// 第二段：仅 --config 无 CLI 显式项 → 文件值原样保留（证覆盖臂不吃文件值）
#[test]
fn test_file_baseline_kept_without_cli_explicit() {
  let file = temp_config("ovr-file", FILE_BASELINE);
  let args = NodeArgs::from_args_iter(["wedb", "--config", file.to_str().unwrap()]).unwrap();
  fs::remove_file(&file).ok();

  assert_fields!(
    args,
    [
      (replica_sync_timeout_secs, 11),
      (replica_attach_timeout_secs, 61),
      (replica_sync_delay_ms, 20),
      (aof_sync_max_lag_bytes, 2048),
      (aof_tail_witness_freq_ms, 40),
      (cluster_replication_reestablishment_timeout, 22),
      (vector_set_quantization_task_count, 4),
      (enable_vector_set_preview, false),
      (cluster_announce_hostname, "file.example".to_string()),
      (expired_object_collection_frequency_secs, 30),
      (expired_key_deletion_scan_frequency_secs, -1),
    ]
  );
}

/// 第三段：无文件无显式项 → 未给即取默认（对标 C# 默认段）
#[test]
fn test_defaults_without_file_or_cli() {
  let args = NodeArgs::from_args_iter(["wedb"]).unwrap();

  assert_fields!(
    args,
    [
      (replica_sync_timeout_secs, DEFAULT_REPLICA_SYNC_TIMEOUT_SECS),
      (
        replica_attach_timeout_secs,
        DEFAULT_REPLICA_ATTACH_TIMEOUT_SECS
      ),
      (replica_sync_delay_ms, DEFAULT_REPLICA_SYNC_DELAY_MS),
      (aof_sync_max_lag_bytes, DEFAULT_AOF_SYNC_MAX_LAG_BYTES),
      (aof_tail_witness_freq_ms, DEFAULT_AOF_TAIL_WITNESS_FREQ_MS),
      (
        cluster_replication_reestablishment_timeout,
        DEFAULT_CLUSTER_REPLICATION_REESTABLISHMENT_TIMEOUT
      ),
      (
        vector_set_quantization_task_count,
        DEFAULT_VECTOR_SET_QUANTIZATION_TASK_COUNT
      ),
      (enable_vector_set_preview, false),
      (cluster_announce_hostname, String::new()),
      (
        expired_object_collection_frequency_secs,
        DEFAULT_EXPIRED_OBJECT_COLLECTION_FREQUENCY_SECS
      ),
      (
        expired_key_deletion_scan_frequency_secs,
        DEFAULT_EXPIRED_KEY_DELETION_SCAN_FREQUENCY_SECS
      ),
    ]
  );
}

/// 第四段：CLI 显式项经 runtime_server_options → RuntimeServerConfig 槽位
/// 播种全链生效（对标 C# GetServerOptions 投影臂；证明覆盖值真落运行时装配口，
/// 而非只存字段）。cluster_announce_hostname / enable_vector_set_preview 的
/// 运行时消费口分别在 wedb boot（NodeArgs 直读）与 wnode service（ServerArgs
///  trait 直读），此段直断其 NodeArgs 取值臂。
#[test]
fn test_cli_explicit_projects_into_runtime_slots() {
  let file = temp_config("ovr-slot", FILE_BASELINE);
  let mut argv: Vec<&str> = vec!["wedb", "--config", file.to_str().unwrap()];
  argv.extend(cli_overrides());
  let args = NodeArgs::from_args_iter(&argv).unwrap();
  fs::remove_file(&file).ok();

  let opts = args.runtime_server_options();
  assert_eq!(opts.replica_attach_timeout_secs, 120);
  assert_eq!(opts.replica_sync_delay_ms, 25);
  assert_eq!(opts.aof_sync_max_lag_bytes, 4096);
  assert_eq!(opts.aof_tail_witness_freq_ms, 50);
  assert_eq!(opts.cluster_replication_reestablishment_timeout, 33);
  assert_eq!(opts.vector_set_quantization_task_count, 8);
  assert_eq!(opts.expired_object_collection_frequency_secs, 60);
  assert_eq!(opts.expired_key_deletion_scan_frequency_secs, 45);

  let config = RuntimeServerConfig::new(opts);
  assert_eq!(
    config.get_int(ServerConfigType::ReplicaSyncDelay),
    25,
    "replica-sync-delay 槽位须取 CLI 显式值"
  );
  assert_eq!(
    config.get_int(ServerConfigType::AofTailWitnessFreq),
    50,
    "aof-tail-witness-freq 槽位须取 CLI 显式值"
  );
  assert_eq!(
    config.get_long(ServerConfigType::AofSyncMaxLagBytes),
    4096,
    "aof-sync-max-lag-bytes 槽位须取 CLI 显式值"
  );
  assert_eq!(
    config.get_int(ServerConfigType::ReplAttachTimeout),
    120,
    "repl-attach-timeout 槽位须取 CLI 显式值"
  );
  assert_eq!(
    config.get_int(ServerConfigType::ClusterReplicationReestablishmentTimeout),
    33,
    "cluster-replication-reestablishment-timeout 槽位须取 CLI 显式值"
  );
  assert_eq!(
    config.get_int(ServerConfigType::ExpiredObjectCollectionFreq),
    60,
    "expired-object-collection-freq 槽位须取 CLI 显式值"
  );
  assert_eq!(
    config.get_int(ServerConfigType::ExpiredKeyDeletionScanFreq),
    45,
    "expired-key-deletion-scan-freq 槽位须取 CLI 显式值"
  );

  // ServerArgs 直读臂（wnode service.rs 向量预览开关消费面）
  assert!(args.enable_vector_set_preview());
  // NodeArgs 直读臂（wedb boot.rs 集群宣告主机名消费面）
  assert_eq!(args.cluster_announce_hostname, "cli.example");
}
