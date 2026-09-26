//! node_options 装配面集成测试（自 src/node_options.rs 内联 mod tests 迁出）

use std::{env::temp_dir, fs, path::PathBuf, process};

use clap::Parser;
use log::LevelFilter;
use wconf::node_options::*;

/// 写临时 toml 配置文件（路径掺入进程 id：temp_dir 用户级跨进程共享，
/// 固定名会与并发跑套件的其他进程互踩——一进程删除、另一进程读取即红）
fn temp_config(name: &str, content: &str) -> PathBuf {
  let path = temp_dir().join(format!("wedb-node-options-{name}-{}.toml", process::id()));
  fs::write(&path, content).unwrap();
  path
}

/// 三层合并解析的测试口（对标 ServerSettingsManager：文件为基 → CLI 显式项
/// 覆盖）：落临时 toml、跑一次 `from_args_iter`（含 validate 拒启面）、读完即删
fn merged(name: &str, file_toml: &str, cli: &[&str]) -> NodeArgs {
  let file = temp_config(name, file_toml);
  let mut argv = vec!["wedb", "--config", file.to_str().unwrap()];
  argv.extend_from_slice(cli);
  let args = NodeArgs::from_args_iter(argv).unwrap();
  fs::remove_file(&file).ok();
  args
}

#[test]
fn test_validation_and_priority() {
  let file = temp_config(
    "prio",
    "port = 9000\naof_size_limit = \"128m\"\nslow_log_threshold = 0\n",
  );
  // invalid size string rejects
  let args = NodeArgs::from_args_iter([
    "wedb",
    "--config",
    file.to_str().unwrap(),
    "--aof-size-limit",
    "invalid",
  ]);
  assert!(matches!(
    args.unwrap_err(),
    NodeOptionsError::InvalidSizeStr(_, _)
  ));

  // slow log range rejects
  let args = NodeArgs::from_args_iter([
    "wedb",
    "--config",
    file.to_str().unwrap(),
    "--slow-log-threshold",
    "99",
  ]);
  let err = args.unwrap_err();
  assert!(
    matches!(err, NodeOptionsError::ValueOutOfRange(_, _, _, _)),
    "actual error: {:?}",
    err
  );

  // valid priority
  let args =
    NodeArgs::from_args_iter(["wedb", "--config", file.to_str().unwrap(), "--port", "9001"])
      .unwrap();
  assert_eq!(args.port, 9001); // CLI wins
  assert_eq!(args.aof_size_limit_bytes().unwrap(), 128 * 1024 * 1024); // file fallback

  fs::remove_file(&file).ok();
}

#[test]
fn test_node_args_ipv6_endpoints() {
  let mut args = NodeArgs {
    bind: Some("::1".into()),
    ..Default::default()
  };
  assert_eq!(args.endpoints().unwrap(), vec!["[::1]:6379"]);

  args.bind = Some("[::1]".into());
  assert_eq!(args.endpoints().unwrap(), vec!["[::1]:6379"]);

  args.bind = Some("::".into());
  assert_eq!(args.endpoints().unwrap(), vec!["[::]:6379"]);

  args.bind = Some("127.0.0.1, ::1".into());
  assert_eq!(
    args.endpoints().unwrap(),
    vec!["127.0.0.1:6379", "[::1]:6379"]
  );

  args.bind = Some("127.0.0.1 [::1] 2001:db8::1".into());
  assert_eq!(
    args.endpoints().unwrap(),
    vec!["127.0.0.1:6379", "[::1]:6379", "[2001:db8::1]:6379"]
  );
}

#[test]
fn test_node_args_defaults() {
  let args = NodeArgs::default();
  // 保护模式默认开，bind 未显式 → 回环回退
  assert_eq!(args.bind, None);
  assert!(args.protected_mode);
  assert_eq!(
    args.endpoints().unwrap(),
    vec!["127.0.0.1:6379", "[::1]:6379"]
  );
  assert_eq!(args.port, 6379);
  assert_eq!(args.dir, PathBuf::from("./data"));
  assert_eq!(args.wal_dir(), PathBuf::from("./data/wal"));
  assert!(!args.aof);
  // 慢日志 / 扫描限额 / 数据库数默认对齐 C#
  assert_eq!(args.slow_log_threshold, 0);
  assert_eq!(args.slow_log_max_entries, 128);
  assert_eq!(args.max_databases, 16);
  assert_eq!(args.object_scan_count_limit, 1000);
  assert_eq!(args.metrics_sampling_frequency_secs, 0);
  // C# defaults.conf:304 NetworkConnectionLimit = -1：连接上限默认不限
  assert_eq!(args.network_connection_limit, -1);
  // C# 默认链：EnableLua=false、LuaScriptTimeoutMs=0（无限）。
  assert!(!args.enable_lua);
  assert_eq!(args.lua_script_timeout_ms, 0);
  // C# defaults.conf:533 EnableVectorSetPreview=false：向量预览默认关
  assert!(!args.enable_vector_set_preview);
  // 后台任务默认链：AofSizeLimit=""（关闭）、EnforceFrequencySecs=5、
  // IndexMaxMemorySize=""（关闭）、IndexResizeFrequencySecs=60、
  // IndexResizeThreshold=50（GarnetServerOptions.cs:167/172/201/206）
  assert_eq!(args.aof_size_limit, None);
  assert_eq!(args.aof_size_limit_enforce_frequency_secs, 5);
  assert_eq!(args.index_max_size, None);
  assert_eq!(args.index_resize_frequency_secs, 60);
  assert_eq!(args.index_resize_threshold, 50);
  assert_eq!(args.aof_size_limit_bytes(), None);
  assert_eq!(args.index_max_size_buckets(), None);
  // C# Options.cs:685 UnixSocketPermission 默认 0 = 不设置（跳过 chmod 臂）
  assert_eq!(args.unixsocket_perm, None);
  assert_eq!(args.unix_socket_mode(), None);
}

#[test]
fn test_background_task_size_parsing() {
  // 尺寸换算对标 GarnetServerOptions.AofSizeLimitSizeBits（向下取 2 的幂）
  // 与 ServerOptions.IndexSizeCachelines（向下取 2 的幂再按 64B/桶折算）
  let args = NodeArgs {
    aof_size_limit: Some("64mb".into()),
    index_max_size: Some("16k".into()),
    ..Default::default()
  };
  assert_eq!(args.aof_size_limit_bytes(), Some(64 * 1024 * 1024));
  // 非 2 的幂输入向下取整（"100m" → 64m）
  let args = NodeArgs {
    aof_size_limit: Some("100m".into()),
    ..Default::default()
  };
  assert_eq!(args.aof_size_limit_bytes(), Some(64 * 1024 * 1024));
  // 索引上限："16k" 字节 → 2^14/64 = 256 桶
  let args = NodeArgs {
    index_max_size: Some("16k".into()),
    ..Default::default()
  };
  assert_eq!(args.index_max_size_buckets(), Some(256));
  // 低于 64B 最小界拒绝（C# adjustedSize < 64 throw）
  let args = NodeArgs {
    index_max_size: Some("32".into()),
    ..Default::default()
  };
  assert_eq!(args.index_max_size_buckets(), None);
  // 非法尺寸字符串拒绝
  let args = NodeArgs {
    aof_size_limit: Some("abc".into()),
    ..Default::default()
  };
  assert_eq!(args.aof_size_limit_bytes(), None);
}

#[test]
fn test_node_args_lua_cli_flags() {
  let args =
    NodeArgs::try_parse_from(["wedb", "--enable-lua", "--lua-script-timeout-ms", "5000"])
      .unwrap();
  assert!(args.enable_lua);
  assert_eq!(args.lua_script_timeout_ms, 5000);
  // lua 事务模式选项已整链删除（task/done/lua-txn-mode-drop-placeholder.md），
  // 传入即未知选项硬失败，不做旧配置兼容
  assert!(NodeArgs::try_parse_from(["wedb", "--lua-transaction-mode"]).is_err());
}

#[test]
fn test_vector_set_preview_flag_and_toml() {
  // CLI 旗标开启
  let args = NodeArgs::try_parse_from(["wedb", "--enable-vector-set-preview"]).unwrap();
  assert!(args.enable_vector_set_preview);
  // toml 导入面（键名 = 字段名）
  let toml = "enable_vector_set_preview = true\n";
  let conf = NodeArgs::from_toml_str(toml).unwrap();
  assert!(conf.enable_vector_set_preview);
  // toml 导出面（ConfigExportPath 落盘内容含该开关，缺省 false）
  let exported = NodeArgs::default().to_toml_string().unwrap();
  assert!(exported.contains("enable_vector_set_preview = false"));
}

#[test]
fn test_node_args_cli_aof_and_wal_dir() {
  let args = NodeArgs::try_parse_from([
    "wedb",
    "--aof",
    "--wal-dir",
    "/mnt/wal",
    "--aof-commit-wait",
  ])
  .unwrap();
  assert!(args.aof);
  assert_eq!(args.wal_dir, Some(PathBuf::from("/mnt/wal")));
  assert_eq!(args.wal_dir(), PathBuf::from("/mnt/wal"));
  // WAIT-FOR-COMMIT 档参数源（C# Options.cs:253 [Option("aof-commit-wait")]）
  assert!(args.aof_commit_wait);
  assert!(args.runtime_server_options().wait_for_commit);

  let args_no_aof = NodeArgs::try_parse_from(["wedb"]).unwrap();
  assert!(!args_no_aof.aof);
  assert_eq!(args_no_aof.wal_dir, None);
  assert_eq!(args_no_aof.wal_dir(), PathBuf::from("./data/wal"));
  assert!(!args_no_aof.aof_commit_wait);
  assert!(!args_no_aof.runtime_server_options().wait_for_commit);
}

/// checkpoint-dir 旋钮三面贯通（CLI / toml / 投影）：显式项直达
/// checkpoint_base_dir() 与 runtime_server_options 投影，缺省回落数据目录
/// （对标 C# -c/--checkpointdir Options.cs:134-136 与
/// CheckpointBaseDirectory = CheckpointDir ?? LogDir，GarnetServerOptions.cs:625）
#[test]
fn test_checkpoint_dir_knob_parse_fallback_project() {
  // 显式 CLI 项
  let args = NodeArgs::try_parse_from([
    "wedb",
    "--dir",
    "/data/main",
    "--checkpoint-dir",
    "/mnt/backup",
  ])
  .unwrap();
  assert_eq!(args.checkpoint_dir, Some(PathBuf::from("/mnt/backup")));
  assert_eq!(args.checkpoint_base_dir(), PathBuf::from("/mnt/backup"));
  let opts = args.runtime_server_options();
  assert_eq!(opts.checkpoint_base_directory, "/mnt/backup");

  // toml 形态（CONFIG 静态面同源）
  let from_file =
    NodeArgs::from_toml_str("dir = \"/data/main\"\ncheckpoint_dir = \"/mnt/backup2\"\n").unwrap();
  assert_eq!(
    from_file.checkpoint_base_dir(),
    PathBuf::from("/mnt/backup2")
  );
  assert_eq!(
    from_file.runtime_server_options().checkpoint_base_directory,
    "/mnt/backup2"
  );

  // 缺省回落数据目录（现网行为零漂移）
  let args_default = NodeArgs::try_parse_from(["wedb", "--dir", "/data/main"]).unwrap();
  assert_eq!(args_default.checkpoint_dir, None);
  assert_eq!(
    args_default.checkpoint_base_dir(),
    PathBuf::from("/data/main")
  );
  assert_eq!(
    args_default
      .runtime_server_options()
      .checkpoint_base_directory,
    "/data/main"
  );
}

/// fast-aof-truncate 与 on-demand-checkpoint 两旋钮的三面（CLI / toml /
/// 导出）与 runtime_server_options 投影（对标 C# Options.cs:991-992 落入
/// GarnetServerOptions，供 ClusterProvider::allow_data_loss 派生单点读取）
#[test]
fn test_truncate_odc_knobs_parse_store_project() {
  // 缺省即 C# defaults.conf:343 false / :346 true
  let args = NodeArgs::try_parse_from(["wedb"]).unwrap();
  assert!(!args.fast_aof_truncate);
  assert!(args.on_demand_checkpoint);
  let opts = args.runtime_server_options();
  assert!(!opts.fast_aof_truncate);
  assert!(opts.on_demand_checkpoint);

  // CLI 显式置位（默认真值旋钮取 ArgAction::Set，须带值）
  let args = NodeArgs::try_parse_from([
    "wedb",
    "--fast-aof-truncate",
    "--on-demand-checkpoint",
    "false",
  ])
  .unwrap();
  let opts = args.runtime_server_options();
  assert!(opts.fast_aof_truncate);
  assert!(!opts.on_demand_checkpoint);

  // toml 配置面（键名 = 字段名）；未写项回落 C# 默认
  let conf =
    NodeArgs::from_toml_str("fast_aof_truncate = true\non_demand_checkpoint = false\n").unwrap();
  let opts = conf.runtime_server_options();
  assert!(opts.fast_aof_truncate);
  assert!(!opts.on_demand_checkpoint);
  let conf = NodeArgs::from_toml_str("port = 7100\n").unwrap();
  assert!(!conf.fast_aof_truncate);
  assert!(conf.on_demand_checkpoint);

  // 三层合并：文件为基、CLI 显式覆盖
  let args = merged(
    "truncate-odc",
    "port = 7100\nfast_aof_truncate = true\non_demand_checkpoint = false\n",
    &["--on-demand-checkpoint", "true"],
  );
  let opts = args.runtime_server_options();
  assert!(opts.fast_aof_truncate, "文件值未被显式覆盖项须保留");
  assert!(opts.on_demand_checkpoint, "CLI 显式项覆盖文件值");

  // 导出面含两旋钮
  let exported = NodeArgs::default().to_toml_string().unwrap();
  assert!(exported.contains("fast_aof_truncate = false"));
  assert!(exported.contains("on_demand_checkpoint = true"));
}

/// C# GarnetServer.cs:508 提交组合互校验（未开 AOF 却配提交节拍/等待档
/// 即拒启；节拍显式 0 与 C# 缺省同值，不拒）。
///
/// 走 `ConfigFileArgs::from_args_iter` 而非 `try_parse_from`：validate 在
/// 后者之外的分层合并末端（from_layered_matches）才跑，用前者才真触拒启面
#[test]
fn test_aof_commit_combination_validation() {
  let err = NodeArgs::from_args_iter(["wedb", "--aof-commit-wait"]).unwrap_err();
  assert!(
    matches!(err, NodeOptionsError::AofCommitWithoutAof),
    "{err:?}"
  );

  let err = NodeArgs::from_args_iter(["wedb", "--aof-commit-ms", "5"]).unwrap_err();
  assert!(
    matches!(err, NodeOptionsError::AofCommitWithoutAof),
    "{err:?}"
  );

  // 开 AOF 即两档皆合法（会话门 enable_aof && wait_for_commit 自此可开）
  assert!(
    NodeArgs::from_args_iter(["wedb", "--aof", "--aof-commit-wait"]).is_ok(),
    "开 AOF 时等待档须合法"
  );
  // 显式 0 即 C# defaults.conf:182 缺省值，`!= 0` 判据不成立
  assert!(
    NodeArgs::from_args_iter(["wedb", "--aof-commit-ms", "0"]).is_ok(),
    "缺省同值的显式 0 节拍不拒"
  );
}

/// C# GarnetServerOptions.cs:839-840「LatencyMonitor requires
/// MetricsSamplingFrequency to be set」的启动期拒启面（全仓唯一校验点，
/// 装配侧不再各判一次）。同走 `from_args_iter` 以触达分层合并末端的 validate
#[test]
fn test_latency_monitor_requires_metrics_sampling_frequency() {
  let err = NodeArgs::from_args_iter(["wedb", "--latency-monitor", "true"]).unwrap_err();
  assert!(
    matches!(err, NodeOptionsError::LatencyMonitorWithoutMetrics),
    "{err:?}"
  );

  // 配了采样节拍即合法；关闭延迟监视时节拍缺省 0 亦不拒
  assert!(
    NodeArgs::from_args_iter([
      "wedb",
      "--latency-monitor",
      "true",
      "--metrics-sampling-freq",
      "5"
    ])
    .is_ok(),
    "有采样节拍时延迟监视须合法"
  );
  assert!(NodeArgs::from_args_iter(["wedb", "--latency-monitor", "false"]).is_ok());
}

#[test]
fn test_node_args_with_custom_wal_dir() {
  let args = NodeArgs {
    wal_dir: Some(PathBuf::from("/mnt/fast_ssd/wal")),
    ..Default::default()
  };
  assert_eq!(args.wal_dir(), PathBuf::from("/mnt/fast_ssd/wal"));
}

/// 数据路径唯一真源：仅由 dir 派生，不含任何模式维度（单机与集群共用同一
/// 物理件，对标 C# 单一命名方案 GarnetServer.cs:479-484 两臂共用
/// defaultNamingScheme、Options.cs:790-793 LogDir/CheckpointDir 单套）
#[test]
fn test_data_path_is_mode_agnostic_single_source() {
  // 默认目录回落 {dir}/wedb.db
  let args = NodeArgs::default();
  assert_eq!(args.data_path(), PathBuf::from(DEFAULT_DIR).join(DATA_FILE));
  assert_eq!(args.data_path(), PathBuf::from("./data/wedb.db"));
  // 显式 dir：数据文件、WAL 同根派生，一套物理布局
  let args = NodeArgs {
    dir: PathBuf::from("/srv/wedb"),
    ..Default::default()
  };
  assert_eq!(args.data_path(), PathBuf::from("/srv/wedb/wedb.db"));
  assert_eq!(args.wal_dir(), PathBuf::from("/srv/wedb/wal"));
}

#[test]
fn test_node_args_with_unixsocket() {
  let cfg = NodeArgs {
    unixsocket: Some("/tmp/wedb.sock".to_string()),
    ..Default::default()
  };
  assert_eq!(
    cfg.endpoints().unwrap(),
    vec!["127.0.0.1:6379", "[::1]:6379", "unix:/tmp/wedb.sock"]
  );
}

/// unixsocketperm 旋钮全链（对标 C# Options.cs:684 选项 + :816-817
/// 八进制折算 + GarnetServerTcp.cs:149-151 跳默认臂）：CLI / toml
/// 双入口同口径、权限值不进端点字符串、非法值在 validate 期拒启
#[test]
fn test_unixsocketperm_knob() {
  // CLI 八进制字面量口径：660 → 0o660；权限值不落端点字符串
  let args = NodeArgs::from_args_iter([
    "wedb",
    "--unixsocket",
    "/tmp/wedb.sock",
    "--unixsocketperm",
    "660",
  ])
  .unwrap();
  assert_eq!(args.unixsocket_perm, Some(660));
  assert_eq!(args.unix_socket_mode(), Some(0o660));
  assert_eq!(
    args.endpoints().unwrap(),
    vec!["127.0.0.1:6379", "[::1]:6379", "unix:/tmp/wedb.sock"]
  );

  // toml 单配置机制同口径
  let conf =
    NodeArgs::from_toml_str("unixsocket = \"/tmp/wedb.sock\"\nunixsocket_perm = 600\n").unwrap();
  assert_eq!(conf.unix_socket_mode(), Some(0o600));

  // C# 默认 0 即不设置（unixSocketPermission != default 跳过臂）
  let args = NodeArgs::from_args_iter(["wedb", "--unixsocketperm", "0"]).unwrap();
  assert_eq!(args.unix_socket_mode(), None);

  // 越界（C# IntRangeValidation(0, 777)）与非法八进制位（Convert 失败面）
  // 皆在 validate 期拒启，不落 bind
  let err = NodeArgs::from_args_iter(["wedb", "--unixsocketperm", "778"]).unwrap_err();
  assert!(
    matches!(err, NodeOptionsError::ValueOutOfRange(_, 0, 777, 778)),
    "{err:?}"
  );
  let err = NodeArgs::from_args_iter(["wedb", "--unixsocketperm", "80"]).unwrap_err();
  assert!(
    matches!(err, NodeOptionsError::UnixSocketPermDigits(80)),
    "{err:?}"
  );
  // 负值直构 validate（clap 负号输入属命令行语法层，非本校验面）
  let err = NodeArgs {
    unixsocket_perm: Some(-1),
    ..Default::default()
  }
  .validate()
  .err()
  .unwrap();
  assert!(
    matches!(err, NodeOptionsError::ValueOutOfRange(_, 0, 777, -1)),
    "{err:?}"
  );
}

#[test]
fn test_toml_parse() {
  let toml = r#"
bind = "192.168.1.100"
port = 6380
dir = "/tmp/wedb_data"
"#;
  let conf = NodeArgs::from_toml_str(toml).unwrap();
  assert_eq!(conf.bind.as_deref(), Some("192.168.1.100"));
  assert_eq!(conf.port, 6380);
  assert_eq!(conf.dir, PathBuf::from("/tmp/wedb_data"));
}

#[test]
fn test_protected_mode_bind_fallback() {
  // C# Format.TryParseAddressList：保护模式 + 空 bind → 回环；
  // 非保护 + 空 bind → 全接口
  let args = NodeArgs::try_parse_from(["wedb", "--protected-mode", "false"]).unwrap();
  assert_eq!(args.endpoints().unwrap(), vec!["0.0.0.0:6379", "[::]:6379"]);

  let args =
    NodeArgs::try_parse_from(["wedb", "--protected-mode", "false", "--bind", "10.0.0.8"])
      .unwrap();
  assert_eq!(args.endpoints().unwrap(), vec!["10.0.0.8:6379"]);

  let args = NodeArgs::try_parse_from(["wedb"]).unwrap();
  assert_eq!(
    args.endpoints().unwrap(),
    vec!["127.0.0.1:6379", "[::1]:6379"]
  );
}

#[test]
fn test_multi_bind_split() {
  // C# Format.cs:64：bind 按逗号与空格切分多地址，逐地址组合 port 成端点
  let args =
    NodeArgs::try_parse_from(["wedb", "--bind", "127.0.0.1, 10.0.0.8 ,192.168.1.8"]).unwrap();
  assert_eq!(
    args.endpoints().unwrap(),
    vec!["127.0.0.1:6379", "10.0.0.8:6379", "192.168.1.8:6379"]
  );
  // 全分隔符/全空白输入：条目剔除后端点列表为空（对应 C# endpoints.Length==0 拒启臂）
  let args = NodeArgs {
    bind: Some(" , ".to_string()),
    ..Default::default()
  };
  assert_eq!(args.endpoints().unwrap(), Vec::<String>::new());
  // 纯空白 bind 视同未指定，走保护模式回退臂（C# IsNullOrWhiteSpace）
  let args = NodeArgs {
    bind: Some("   ".to_string()),
    ..Default::default()
  };
  assert_eq!(
    args.endpoints().unwrap(),
    vec!["127.0.0.1:6379", "[::1]:6379"]
  );
}

#[test]
fn test_bind_rejects_uds_path() {
  // C# OptionsValidators.cs:364-378：bind 列表路径形态条目显式拒启，UDS 仅允许走 unixsocket 独立选项
  let args = NodeArgs {
    bind: Some("/tmp/wedb.sock".to_string()),
    ..Default::default()
  };
  let err = args.endpoints().unwrap_err();
  assert!(
    matches!(&err, NodeOptionsError::InvalidAddress(a) if a == "/tmp/wedb.sock"),
    "{err:?}"
  );
  assert_eq!(
    err.to_string(),
    "Expected string in IPv4 / IPv6 format (e.g. 127.0.0.1 / 0:0:0:0:0:0:0:1) or 'localhost' or valid hostname. Actual value: /tmp/wedb.sock"
  );

  let args_unix_prefix = NodeArgs {
    bind: Some("127.0.0.1, unix:/tmp/wedb.sock".to_string()),
    ..Default::default()
  };
  let err_prefix = args_unix_prefix.endpoints().unwrap_err();
  assert!(
    matches!(&err_prefix, NodeOptionsError::InvalidAddress(a) if a == "unix:/tmp/wedb.sock"),
    "{err_prefix:?}"
  );

  // validate() 同样拦截
  assert!(args.validate().is_err());
}

#[test]
fn test_config_file_cli_override() {
  // 端到端：文件为基 → CLI 显式覆盖（对标 ServerSettingsManager 三层合并）
  let args = merged(
    "override",
    "port = 7000\nslow_log_threshold = 5000\nmax_databases = 4\nbind = \"192.168.1.100\"\n",
    &["--port", "7001"],
  );
  // CLI 覆盖文件值
  assert_eq!(args.port, 7001);
  // 文件值生效
  assert_eq!(args.slow_log_threshold, 5000);
  assert_eq!(args.max_databases, 4);
  assert_eq!(args.bind.as_deref(), Some("192.168.1.100"));
  // 未涉及项取默认
  assert_eq!(args.object_scan_count_limit, 1000);
  assert_eq!(args.endpoints().unwrap(), vec!["192.168.1.100:7001"]);
}

#[test]
fn test_config_file_kebab_field_override() {
  // kebab-case 长名（--wal-dir）字段的显式判定：CLI 覆盖文件值，
  // 文件里的其余项（slow_log_max_entries）原样保留
  let args = merged(
    "kebab",
    "wal_dir = \"/file/wal\"\nslow_log_max_entries = 8192\n",
    &["--wal-dir", "/cli/wal"],
  );
  assert_eq!(args.wal_dir, Some(PathBuf::from("/cli/wal")));
  assert_eq!(args.slow_log_max_entries, 8192);
}

/// 连接上限三面：CLI 显式项、toml snake_case 覆盖、越界拒启
///（C# Options.cs:398 IntRangeValidation(-1, int.MaxValue) 的启动期拒绝面）
#[test]
fn test_network_connection_limit_surfaces() {
  let args = NodeArgs::try_parse_from(["wedb", "--network-connection-limit", "512"]).unwrap();
  assert_eq!(args.network_connection_limit, 512);

  // toml 蛇形键覆盖（-1 显式不限与 C# defaults.conf 同形态）
  let args = merged("net-limit", "network_connection_limit = 32\n", &[]);
  assert_eq!(args.network_connection_limit, 32);

  // 越界（< -1）拒启；validate 在 from_args_iter 分层合并末端才跑
  let err = NodeArgs::from_args_iter(["wedb", "--network-connection-limit", "-2"]).unwrap_err();
  assert!(
    matches!(
      err,
      NodeOptionsError::ValueOutOfRange("network-connection-limit", -1, i32::MAX, -2)
    ),
    "{err:?}"
  );
  // -1 边界合法（缺省不限）
  assert!(NodeArgs::from_args_iter(["wedb", "--network-connection-limit", "-1"]).is_ok());
}

#[test]
fn test_config_export_round_trip() {
  // 导出合并后配置 → 重新加载一致
  let export = temp_dir().join("wedb-node-options-export-out.toml");
  let args = merged(
    "export-src",
    "port = 7002\nslow_log_max_entries = 64\ndir = \"/file/data\"\nunixsocket = \"/file/x.sock\"\naof = true\naof_size_limit = \"32mb\"\n",
    &[
      "--config-export-path",
      export.to_str().unwrap(),
      "--object-scan-count-limit",
      "256",
    ],
  );
  let reloaded = NodeArgs::from_file(&export).unwrap();
  fs::remove_file(&export).ok();
  assert_eq!(reloaded.port, 7002);
  assert_eq!(reloaded.slow_log_max_entries, 64);
  assert_eq!(reloaded.object_scan_count_limit, 256);
  assert_eq!(reloaded.port, args.port);
  // --config-file 形态下只读回显五字段经投影等于入参，且导出/重载往返一致
  let opts = args.runtime_server_options();
  assert_eq!(opts.checkpoint_base_directory, "/file/data");
  assert_eq!(opts.log_dir.as_deref(), Some("/file/data/wal"));
  assert_eq!(opts.unix_socket_path.as_deref(), Some("/file/x.sock"));
  assert!(opts.enable_aof);
  assert_eq!(opts.aof_size_limit.as_deref(), Some("32mb"));
  assert_eq!(
    reloaded.runtime_server_options().unix_socket_path,
    opts.unix_socket_path
  );
  assert_eq!(
    reloaded.runtime_server_options().aof_size_limit,
    opts.aof_size_limit
  );
}

#[test]
fn test_runtime_server_options_projection() {
  // 对标 C# Options.GetServerOptions 选项装配段
  let args = NodeArgs {
    aof_commit_ms: Some(20),
    aof_commit_wait: true,
    slow_log_threshold: 800,
    slow_log_max_entries: 32,
    max_databases: 8,
    object_scan_count_limit: 512,
    dir: PathBuf::from("/data/ro"),
    wal_dir: Some(PathBuf::from("/mnt/wal")),
    unixsocket: Some("/tmp/x.sock".into()),
    aof: true,
    aof_size_limit: Some("64mb".into()),
    ..Default::default()
  };
  let opts = args.runtime_server_options();
  assert_eq!(opts.commit_frequency_ms, 20);
  assert!(opts.wait_for_commit);
  assert_eq!(opts.slow_log_threshold, 800);
  assert_eq!(opts.slow_log_max_entries, 32);
  assert_eq!(opts.max_databases, 8);
  assert_eq!(opts.object_scan_count_limit, 512);
  // 只读回显五字段（C# Options.cs:909-910、:921、:935、:1030 投影同源）
  assert_eq!(opts.checkpoint_base_directory, "/data/ro");
  assert_eq!(opts.log_dir.as_deref(), Some("/mnt/wal"));
  assert_eq!(opts.unix_socket_path.as_deref(), Some("/tmp/x.sock"));
  assert!(opts.enable_aof);
  assert_eq!(opts.aof_size_limit.as_deref(), Some("64mb"));

  // 未设置项保持 C# 默认：dir 恒回落 ./data（非空），wal_dir 落 <dir>/wal，
  // unixsocket/aof_size_limit 未配置为 None（格式器吐 ""，与 C# 空串/false 口径一致）
  let opts = NodeArgs::default().runtime_server_options();
  assert_eq!(opts.commit_frequency_ms, 0);
  assert!(!opts.wait_for_commit);
  assert_eq!(opts.slow_log_threshold, 0);
  assert_eq!(opts.slow_log_max_entries, 128);
  assert_eq!(opts.max_databases, 16);
  assert_eq!(opts.object_scan_count_limit, 1000);
  assert_eq!(opts.checkpoint_base_directory, DEFAULT_DIR);
  assert_eq!(opts.log_dir.as_deref(), Some("./data/wal"));
  assert!(opts.unix_socket_path.is_none());
  assert!(!opts.enable_aof);
  assert!(opts.aof_size_limit.is_none());
}

#[test]
fn test_hlog_section_defaults_and_projection() {
  // 默认全 None + read_cache 关闭：不覆盖装配基线（页容量交由内存预算规划器推导）
  let args = NodeArgs::default();
  assert_eq!(args.hlog, HlogOptions::default());
  let p = args.hlog.validated().unwrap();
  assert_eq!(p.page_size, None);
  assert_eq!(p.memory_size, None);
  assert_eq!(p.mutable_fraction, None);
  assert!(!p.read_cache);
  assert_eq!(p.read_cache_memory_size, None);
  assert_eq!(p.tree_cache_budget, None);
  // reviv 三旋钮默认态 = 现状硬编码基线（对标 C# defaults：reviv 关、
  // reviv-fraction 未配置、copy-reads-to-tail 关）
  assert!(!p.reviv);
  assert_eq!(p.reviv_fraction, None);
  assert!(!p.copy_reads_to_tail);

  // 显式项投影：对标 C# GetSettings（page 16m / memory 4g / mutable 50）
  let args = NodeArgs {
    hlog: HlogOptions {
      page_size: Some(DEFAULT_HLOG_PAGE_SIZE),
      memory_size: Some(4 * 1024 * 1024 * 1024),
      mutable_percent: Some(50),
      read_cache: true,
      read_cache_memory_size: Some(512 * 1024 * 1024),
      // 0 = 不设限为合法显式值，validated 纯透传
      tree_cache_budget: Some(0),
      reviv: true,
      reviv_fraction: Some(0.5),
      copy_reads_to_tail: true,
    },
    ..Default::default()
  };
  let (page, memory, fraction, read_cache, rc_memory, tree_budget, reviv, reviv_fraction, crt) = {
    let p = args.hlog.validated().unwrap();
    (
      p.page_size,
      p.memory_size,
      p.mutable_fraction,
      p.read_cache,
      p.read_cache_memory_size,
      p.tree_cache_budget,
      p.reviv,
      p.reviv_fraction,
      p.copy_reads_to_tail,
    )
  };
  assert_eq!(page, Some(16 * 1024 * 1024));
  assert_eq!(memory, Some(4 * 1024 * 1024 * 1024));
  assert_eq!(fraction, Some(0.5));
  assert!(read_cache);
  assert_eq!(rc_memory, Some(512 * 1024 * 1024));
  assert_eq!(tree_budget, Some(0));
  // 三旋钮透传：validated 不复校 reviv_fraction（单点在 StoreConfig::validate）
  assert!(reviv);
  assert_eq!(reviv_fraction, Some(0.5));
  assert!(crt);
}

#[test]
fn test_hlog_section_validation_rejects() {
  // 对标 GarnetServerOptions.GetSettings：MutablePercent < 10 或 > 95 → throw
  let bad = HlogOptions {
    mutable_percent: Some(9),
    ..HlogOptions::default()
  };
  assert!(bad.validated().is_err());
  let bad = HlogOptions {
    mutable_percent: Some(96),
    ..HlogOptions::default()
  };
  assert!(bad.validated().is_err());
  // 边界 10 / 95 合法
  assert!(
    HlogOptions {
      mutable_percent: Some(10),
      ..HlogOptions::default()
    }
    .validated()
    .is_ok()
  );
  assert!(
    HlogOptions {
      mutable_percent: Some(95),
      ..HlogOptions::default()
    }
    .validated()
    .is_ok()
  );
  // 页容量必须为 2 的幂
  let bad = HlogOptions {
    page_size: Some(4095),
    ..HlogOptions::default()
  };
  assert!(bad.validated().is_err());
  // 下限校验核在场：256 页容量须被点名拒（证伪「小页静默接受」，对标 C#
  // C# ServerOptions.ValidatedPageSizeBits 的 MIN_PAGE_SIZE_BYTES 判定）
  let bad = HlogOptions {
    page_size: Some(256),
    ..HlogOptions::default()
  };
  assert!(
    matches!(
      bad.validated(),
      Err(NodeOptionsError::Hlog(msg))
        if msg.contains("hlog-page-size") && msg.contains("512") && msg.contains("256")
    ),
    "256 页容量须被下限校验核点名拒绝: {bad:?}"
  );
  // 向下取幂后跌破下限：511 取幂归 256 才判负，文案给出取幂生效值
  let bad = HlogOptions {
    page_size: Some(511),
    ..HlogOptions::default()
  };
  assert!(
    matches!(
      bad.validated(),
      Err(NodeOptionsError::Hlog(msg)) if msg.contains("生效 256 字节") && msg.contains("512")
    ),
    "取幂后跌破下限须报生效值 256: {bad:?}"
  );
  // 页容量扇区口径与 wkv 一致（4096 整数倍）：2048 为 2 的幂但非 4KB 扇区
  // 倍数，同样拒绝；4096 恰为一扇区，合法
  let bad = HlogOptions {
    page_size: Some(2048),
    ..HlogOptions::default()
  };
  assert!(matches!(
    bad.validated(),
    Err(NodeOptionsError::Hlog(msg)) if msg.contains("4096")
  ));
  assert!(
    HlogOptions {
      page_size: Some(4096),
      ..HlogOptions::default()
    }
    .validated()
    .is_ok()
  );
  // 内存预算必须为正
  let bad = HlogOptions {
    memory_size: Some(0),
    ..HlogOptions::default()
  };
  assert!(matches!(bad.validated(), Err(NodeOptionsError::Hlog(_))));
  // ReadCache 内存预算必须为正
  let bad = HlogOptions {
    read_cache_memory_size: Some(0),
    ..HlogOptions::default()
  };
  assert!(matches!(bad.validated(), Err(NodeOptionsError::Hlog(_))));
}

#[test]
fn test_hlog_read_cache_section_parse() {
  // CLI 开关 + 预算显式覆盖（对标 C# EnableReadCache / ReadCacheMemorySize）
  let args = NodeArgs::from_args_iter([
    "wedb",
    "--read-cache",
    "--read-cache-memory-size",
    "536870912",
  ])
  .unwrap();
  assert!(args.hlog.read_cache);
  assert_eq!(args.hlog.read_cache_memory_size, Some(512 * 1024 * 1024));

  // 缺省关闭且预算透传 None（装配侧取 DEFAULT_READ_CACHE_MEMORY_SIZE 推导）
  let args = NodeArgs::from_toml_str("port = 7100\n").unwrap();
  assert!(!args.hlog.read_cache);
  assert_eq!(args.hlog.read_cache_memory_size, None);
  assert_eq!(
    DEFAULT_READ_CACHE_MEMORY_SIZE,
    1024 * 1024 * 1024,
    "对标 C# GarnetServerOptions ReadCacheMemorySize = \"1g\""
  );
}

/// reviv 三旋钮（reviv / reviv-fraction / copy-reads-to-tail）CLI / toml
/// 三面（对标 C# Options.cs:564-567、:559、:128 命令行长名）；override_explicit
/// 合并 id 与 clap Args 字段名同源，显式项不静默丢失
#[test]
fn test_hlog_reviv_knobs_parse_and_merge() {
  // CLI 显式项
  let args = NodeArgs::from_args_iter([
    "wedb",
    "--reviv",
    "--reviv-fraction",
    "0.25",
    "--copy-reads-to-tail",
  ])
  .unwrap();
  assert!(args.hlog.reviv);
  assert_eq!(args.hlog.reviv_fraction, Some(0.25));
  assert!(args.hlog.copy_reads_to_tail);

  // 缺省未配置（默认 false/None/false = 现状基线）
  let args = NodeArgs::from_toml_str("port = 7100\n").unwrap();
  assert!(!args.hlog.reviv);
  assert_eq!(args.hlog.reviv_fraction, None);
  assert!(!args.hlog.copy_reads_to_tail);

  // toml hlog 嵌套节 + validated 透传
  let args = merged(
    "hlog-reviv",
    "[hlog]\nreviv = true\nreviv_fraction = 0.5\ncopy_reads_to_tail = true\n",
    &[],
  );
  let p = args.hlog.validated().unwrap();
  assert!(p.reviv);
  assert_eq!(p.reviv_fraction, Some(0.5));
  assert!(p.copy_reads_to_tail);
}

#[test]
fn test_hlog_toml_parse_and_cli_override() {
  // toml 嵌套节 `[hlog]` 解析 + CLI 显式覆盖
  let args = merged(
    "hlog",
    "port = 7100\n[hlog]\npage_size = 8388608\nmemory_size = 268435456\nmutable_percent = 60\n",
    &["--hlog-page-size", "16777216"],
  );
  // CLI 覆盖文件值
  assert_eq!(args.hlog.page_size, Some(16 * 1024 * 1024));
  // 文件值生效
  assert_eq!(args.hlog.memory_size, Some(256 * 1024 * 1024));
  assert_eq!(args.hlog.mutable_percent, Some(60));
  // hlog 段缺省 → 全 None（旧配置文件向后兼容）
  let args = NodeArgs::from_toml_str("port = 7100\n").unwrap();
  assert_eq!(args.hlog, HlogOptions::default());
}

#[test]
fn test_hlog_config_export_round_trip() {
  // 导出合并后配置 → 重新加载一致（hlog 嵌套节全量往返）
  let export = temp_dir().join("wedb-node-options-hlog-export.toml");
  let _args = NodeArgs::from_args_iter([
    "wedb",
    "--config-export-path",
    export.to_str().unwrap(),
    "--hlog-page-size",
    "16777216",
    "--hlog-mutable-percent",
    "55",
  ])
  .unwrap();
  let reloaded = NodeArgs::from_file(&export).unwrap();
  fs::remove_file(&export).ok();
  assert_eq!(reloaded.hlog.page_size, Some(DEFAULT_HLOG_PAGE_SIZE));
  assert_eq!(reloaded.hlog.mutable_percent, Some(55));
  assert_eq!(reloaded.hlog.memory_size, None);
}

#[test]
fn test_fast_aof_truncate_and_commit_wait_validations() {
  // 1. fast_aof_truncate + aof + aof_commit_ms != -1 => 拒启
  let args = NodeArgs {
    fast_aof_truncate: true,
    aof: true,
    aof_commit_ms: Some(20),
    ..Default::default()
  };
  let err = args.validate().unwrap_err();
  assert!(
    matches!(err, NodeOptionsError::FastAofTruncateRequiresManualCommit),
    "{err:?}"
  );

  // fast_aof_truncate + aof + aof_commit_ms == -1 => 合法
  let args = NodeArgs {
    fast_aof_truncate: true,
    aof: true,
    aof_commit_ms: Some(-1),
    ..Default::default()
  };
  assert!(args.validate().is_ok());

  // 2. aof_commit_ms < 0 + aof_commit_wait => 拒启
  let args = NodeArgs {
    aof: true,
    aof_commit_ms: Some(-1),
    aof_commit_wait: true,
    ..Default::default()
  };
  let err = args.validate().unwrap_err();
  assert!(
    matches!(err, NodeOptionsError::CommitWaitWithManualCommit),
    "{err:?}"
  );

  // aof_commit_ms >= 0 + aof_commit_wait => 合法
  let args = NodeArgs {
    aof: true,
    aof_commit_ms: Some(20),
    aof_commit_wait: true,
    ..Default::default()
  };
  assert!(args.validate().is_ok());

  // 命令行解析 --aof-commit-ms -1 负数
  let parsed = NodeArgs::from_args_iter(["wedb", "--aof", "--aof-commit-ms", "-1"]).unwrap();
  assert_eq!(parsed.aof_commit_ms, Some(-1));
  assert_eq!(parsed.runtime_server_options().commit_frequency_ms, -1);
}

#[test]
fn test_minimum_log_level_alignment() {
  let test_cases = [
    ("critical", LevelFilter::Error),
    ("CRITICAL", LevelFilter::Error),
    ("Critical", LevelFilter::Error),
    ("none", LevelFilter::Off),
    ("NONE", LevelFilter::Off),
    ("None", LevelFilter::Off),
    ("information", LevelFilter::Info),
    ("INFORMATION", LevelFilter::Info),
    ("Information", LevelFilter::Info),
    ("error", LevelFilter::Error),
    ("warn", LevelFilter::Warn),
    ("warning", LevelFilter::Warn),
    ("debug", LevelFilter::Debug),
    ("trace", LevelFilter::Trace),
    ("off", LevelFilter::Off),
    ("info", LevelFilter::Info),
    ("unknown", LevelFilter::Info),
  ];
  for (level_str, expected) in test_cases {
    let args = NodeArgs {
      log_level: Some(level_str.into()),
      ..Default::default()
    };
    assert_eq!(args.minimum_log_level(), expected, "testing {level_str}");
  }

  let default_args = NodeArgs::default();
  // 未配置（log_level 缺省 None）缺省 Warning（defaults.conf:280 LogLevel
  // 生效默认，zcode-r30-defaults 立项四）
  assert_eq!(default_args.minimum_log_level(), LevelFilter::Warn);
}

/// quiet / disable-console-logger 双旋钮三面（CLI 短选项与别名、toml、
/// 三层合并；对标 C# Options.cs:363-364 QuietMode 与 :374-375
/// DisableConsoleLogger，C# bool? 缺省经 GetValueOrDefault 折 false）
#[test]
fn test_quiet_and_disable_console_logger_knobs() {
  // 缺省 false（C# bool? null → false）
  let args = NodeArgs::default();
  assert!(!args.quiet);
  assert!(!args.disable_console_logger);

  // CLI：短选项 -q、长选项 --quiet、别名 quiet_mode / DisableConsoleLogger 同入口
  assert!(NodeArgs::from_args_iter(["wedb", "-q"]).unwrap().quiet);
  assert!(NodeArgs::from_args_iter(["wedb", "--quiet"]).unwrap().quiet);
  assert!(
    NodeArgs::from_args_iter(["wedb", "--quiet_mode"])
      .unwrap()
      .quiet
  );
  let args = NodeArgs::from_args_iter(["wedb", "--disable-console-logger"]).unwrap();
  assert!(args.disable_console_logger);
  let args = NodeArgs::from_args_iter(["wedb", "--DisableConsoleLogger"]).unwrap();
  assert!(args.disable_console_logger);

  // toml 导入面（键名 = 字段名）
  let conf = NodeArgs::from_toml_str("quiet = true\ndisable_console_logger = true\n").unwrap();
  assert!(conf.quiet);
  assert!(conf.disable_console_logger);

  // 三层合并：文件为基 → CLI 显式项覆盖；CLI 未显式项保留文件值
  let args = merged(
    "quiet-knobs",
    "port = 7101\nquiet = false\ndisable_console_logger = true\n",
    &["-q"],
  );
  assert!(args.quiet, "CLI 显式项覆盖文件值");
  assert!(args.disable_console_logger, "CLI 未显式覆盖时文件值保留");

  // 导出面含双旋钮（派生自动纳入 toml 导出）
  let exported = NodeArgs::default().to_toml_string().unwrap();
  assert!(exported.contains("quiet = false"));
  assert!(exported.contains("disable_console_logger = false"));
}
