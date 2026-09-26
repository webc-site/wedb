//! 票 wconf-five-knobs-missing-startup-wiring：五旋钮启动段真接线全链用例。
//!
//! 对标 garnet test/standalone 的 GetServerOptions 三段式（无显式项取默认、
//! 显式项解析生效、经投影落运行时装配口），覆盖 CLI 显式项、toml 文件
//! 导入、导出-回导往返三面，并以 RuntimeServerConfig::new 槽位播种核对「启动值
//! → CONFIG 现取真值」，杜绝「改了 CONFIG SET 当下生效、重启无声回落默认」的
//! 假旋钮。
//!
//! 修复前必红：NodeArgs 无这五字段、runtime_server_options() 投影不触碰这五项，
//! 无论 CLI/文件如何设值，槽位播种恒取 RuntimeServerOptions::default() 常量，
//! 显式值断言即失败。
//!
//! C# 一手锚：libs/host/Configuration/Options.cs:271/:279/:430/:438/:461 旗标与
//! GetServerOptions :939/:941/:987/:989/:994 投影。

use std::{env::temp_dir, fs, path::PathBuf, process};

use wbase::cfg::LogCompactionType;
use wconf::{
  ConfigFileArgs, NodeArgs, RuntimeServerConfig, ServerConfigType,
  runtime_server_options::{
    DEFAULT_AOF_REPLAY_MAX_LAG_BYTES, DEFAULT_COMPACTION_MAX_SEGMENTS,
    DEFAULT_ENABLE_SCATTER_GATHER_GET, DEFAULT_REPLICA_DISKLESS_SYNC_DELAY_SECS,
  },
};

/// 写临时 toml 配置文件（路径掺测试名与进程 id，杜绝并发互踩）
fn temp_config(name: &str, content: &str) -> PathBuf {
  let path = temp_dir().join(format!("wedb-five-knobs-{name}-{}.toml", process::id()));
  fs::write(&path, content).unwrap();
  path
}

/// 五旋钮的文件基线旧值（与默认值、CLI 新值三方互异，覆盖丢失或投影缺项即红）
const FILE_BASELINE: &str = "\
compaction_type = \"Shift\"
compaction_max_segments = 12
enable_scatter_gather_get = false
aof_replay_max_lag_bytes = 4096
replica_diskless_sync_delay = 9
";

/// 五旋钮的 CLI 显式新值（kebab 长名与 C# 同名）
fn cli_overrides() -> Vec<&'static str> {
  vec![
    "--compaction-type",
    "Lookup",
    "--compaction-max-segments",
    "64",
    "--sg-get",
    "false",
    "--aof-replay-max-lag-bytes",
    "8192",
    "--repl-diskless-sync-delay",
    "30",
  ]
}

/// 第一段：--config 文件基线 + 5 个 CLI 显式项 → 显式给出即覆盖
#[test]
fn test_cli_explicit_overrides_file_baseline() {
  let file = temp_config("cli-ovr", FILE_BASELINE);
  let mut argv: Vec<&str> = vec!["wedb", "--config", file.to_str().unwrap()];
  argv.extend(cli_overrides());
  let args = NodeArgs::from_args_iter(&argv).unwrap();
  fs::remove_file(&file).ok();

  assert_eq!(args.compaction_type, LogCompactionType::Lookup);
  assert_eq!(args.compaction_max_segments, 64);
  assert!(!args.enable_scatter_gather_get);
  assert_eq!(args.aof_replay_max_lag_bytes, 8192);
  assert_eq!(args.replica_diskless_sync_delay, 30);
}

/// 第二段：仅 --config 无 CLI 显式项 → toml 蛇形键导入原样生效（证文件
/// 面接线，非仅 CLI 面）
#[test]
fn test_toml_file_import() {
  let file = temp_config("file-only", FILE_BASELINE);
  let args = NodeArgs::from_args_iter(["wedb", "--config", file.to_str().unwrap()]).unwrap();
  fs::remove_file(&file).ok();

  assert_eq!(args.compaction_type, LogCompactionType::Shift);
  assert_eq!(args.compaction_max_segments, 12);
  assert!(!args.enable_scatter_gather_get);
  assert_eq!(args.aof_replay_max_lag_bytes, 4096);
  assert_eq!(args.replica_diskless_sync_delay, 9);
}

/// 第三段：导出-回导往返 → 五旋钮经 --config-export-path 落盘再回导逐值全等
/// （证导出面与导入面同用一份字段集，无缺键）
#[test]
fn test_export_reimport_roundtrip() {
  let file = temp_config("roundtrip", FILE_BASELINE);
  let args = NodeArgs::from_args_iter(["wedb", "--config", file.to_str().unwrap()]).unwrap();
  fs::remove_file(&file).ok();

  let out = temp_dir().join(format!("wedb-five-knobs-export-{}.toml", process::id()));
  args.export_config(&out).expect("导出失败");
  let reloaded = NodeArgs::from_file(&out).expect("回导失败");
  fs::remove_file(&out).ok();

  assert_eq!(reloaded.compaction_type, args.compaction_type);
  assert_eq!(
    reloaded.compaction_max_segments,
    args.compaction_max_segments
  );
  assert_eq!(
    reloaded.enable_scatter_gather_get,
    args.enable_scatter_gather_get
  );
  assert_eq!(
    reloaded.aof_replay_max_lag_bytes,
    args.aof_replay_max_lag_bytes
  );
  assert_eq!(
    reloaded.replica_diskless_sync_delay,
    args.replica_diskless_sync_delay
  );
}

/// 第四段：CLI 显式项经 runtime_server_options 投影 → RuntimeServerConfig Init
/// 槽位播种全链生效（对标 C# GetServerOptions 投影臂；证明启动值真落 CONFIG
/// 现取口，而非只存字段）
#[test]
fn test_cli_explicit_projects_into_runtime_slots() {
  let file = temp_config("slot-proj", FILE_BASELINE);
  let mut argv: Vec<&str> = vec!["wedb", "--config", file.to_str().unwrap()];
  argv.extend(cli_overrides());
  let args = NodeArgs::from_args_iter(&argv).unwrap();
  fs::remove_file(&file).ok();

  let opts = args.runtime_server_options();
  assert_eq!(opts.compaction_type, LogCompactionType::Lookup);
  assert_eq!(opts.compaction_max_segments, 64);
  assert!(!opts.enable_scatter_gather_get);
  assert_eq!(opts.aof_replay_max_lag_bytes, 8192);
  assert_eq!(opts.replica_diskless_sync_delay, 30);

  let config = RuntimeServerConfig::new(opts);
  assert_eq!(
    config.get_enum(ServerConfigType::CompactionType).unwrap(),
    LogCompactionType::Lookup,
    "compaction-type 槽位须取启动值"
  );
  assert_eq!(
    config.get_int(ServerConfigType::CompactionMaxSegments),
    64,
    "compaction-max-segments 槽位须取启动值"
  );
  assert!(
    !config.get_bool(ServerConfigType::SgGet),
    "sg-get 槽位须取启动值"
  );
  assert_eq!(
    config.get_int(ServerConfigType::AofReplayMaxLagBytes),
    8192,
    "aof-replay-max-lag-bytes 槽位须取启动值"
  );
  assert_eq!(
    config.get_int(ServerConfigType::ReplDisklessSyncDelay),
    30,
    "repl-diskless-sync-delay 槽位须取启动值"
  );
}

/// 缺省态零漂移锁：不传五旗标时投影与槽位播种逐项等于既有 DEFAULT_* 常量
/// （防 default 常量被顺手改，亦证未给即取缺省）
#[test]
fn test_defaults_no_drift() {
  let args = NodeArgs::from_args_iter(["wedb"]).unwrap();
  let opts = args.runtime_server_options();
  assert_eq!(opts.compaction_type, LogCompactionType::None);
  assert_eq!(
    opts.compaction_max_segments,
    DEFAULT_COMPACTION_MAX_SEGMENTS
  );
  assert_eq!(
    opts.enable_scatter_gather_get,
    DEFAULT_ENABLE_SCATTER_GATHER_GET
  );
  assert_eq!(
    opts.aof_replay_max_lag_bytes,
    DEFAULT_AOF_REPLAY_MAX_LAG_BYTES
  );
  assert_eq!(
    opts.replica_diskless_sync_delay,
    DEFAULT_REPLICA_DISKLESS_SYNC_DELAY_SECS
  );

  let config = RuntimeServerConfig::new(opts);
  assert_eq!(
    config.get_enum(ServerConfigType::CompactionType).unwrap(),
    LogCompactionType::None
  );
  assert_eq!(
    config.get_int(ServerConfigType::CompactionMaxSegments),
    DEFAULT_COMPACTION_MAX_SEGMENTS
  );
  assert!(config.get_bool(ServerConfigType::SgGet));
  assert_eq!(
    config.get_int(ServerConfigType::AofReplayMaxLagBytes),
    DEFAULT_AOF_REPLAY_MAX_LAG_BYTES
  );
  assert_eq!(
    config.get_int(ServerConfigType::ReplDisklessSyncDelay),
    DEFAULT_REPLICA_DISKLESS_SYNC_DELAY_SECS
  );
}

/// 非法档名拒启锁：CLI 与 toml 两面越界档位皆须解析期报错（对标
/// Options.cs:271 Enum.Parse 失败即启动失败）
#[test]
fn test_invalid_compaction_type_rejects() {
  assert!(NodeArgs::from_args_iter(["wedb", "--compaction-type", "Bogus"]).is_err());
  let file = temp_config("bad-enum", "compaction_type = \"Bogus\"\n");
  let parsed = NodeArgs::from_args_iter(["wedb", "--config", file.to_str().unwrap()]);
  fs::remove_file(&file).ok();
  assert!(parsed.is_err(), "toml 非法 compaction_type 须拒启");
}
