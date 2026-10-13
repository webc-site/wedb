#![recursion_limit = "512"] // 泛型设备实例化下 async 状态机嵌套深（R27 CI 实证）
use std::{env::temp_dir, fs, path::Path};

use clap::Parser;
use wconf::ConfigFileArgs;
use wedb_standalone::StandaloneArgs;

#[test]
fn test_standalone_cli_aof_and_wal_dir() {
  let args =
    StandaloneArgs::try_parse_from(["wedb-standalone", "--aof", "--wal-dir", "/data/wal"]).unwrap();
  assert!(args.node.aof);
  assert_eq!(args.node.wal_dir.as_deref(), Some(Path::new("/data/wal")));

  let args_default = StandaloneArgs::try_parse_from(["wedb-standalone"]).unwrap();
  assert!(!args_default.node.aof);
  assert_eq!(args_default.node.wal_dir, None);
}

#[test]
fn test_standalone_config_file_cli_override_projection() {
  // 端到端：toml 文件加载 → CLI 覆盖 → 运行时选项投影生效
  let file = temp_dir().join("wedb-standalone-e2e.toml");
  fs::write(
    &file,
    "port = 7020\nslow_log_threshold = 2500\nslow_log_max_entries = 64\nmax_databases = 8\nobject_scan_count_limit = 777\n",
  )
  .unwrap();
  let args = StandaloneArgs::from_args_iter([
    "wedb-standalone",
    "--config",
    file.to_str().unwrap(),
    "--port",
    "7021",
  ])
  .unwrap();
  fs::remove_file(&file).ok();
  // CLI 覆盖文件值
  assert_eq!(args.node.port, 7021);
  // 文件值生效
  assert_eq!(args.node.slow_log_threshold, 2500);
  assert_eq!(args.node.slow_log_max_entries, 64);
  assert_eq!(args.node.max_databases, 8);
  assert_eq!(args.node.object_scan_count_limit, 777);

  // 运行时选项投影（provider.with_runtime_server_options 播种源）
  let opts = args.node.runtime_server_options();
  assert_eq!(opts.slow_log_threshold, 2500);
  assert_eq!(opts.slow_log_max_entries, 64);
  assert_eq!(opts.max_databases, 8);
  assert_eq!(opts.object_scan_count_limit, 777);
}
