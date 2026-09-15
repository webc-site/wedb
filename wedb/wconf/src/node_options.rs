//! 统一节点通用命令行与配置参数
//!
//! 包含通用网络端点、存储路径、工作线程与认证密码配置，供单机与集群模式共同复用。
//! 配置文件格式钦定 nested_text（C# GarnetConf/RedisConf 双格式不转写）。

use std::{
  env::args_os,
  ffi::OsString,
  fs,
  io::Error,
  path::{Path, PathBuf},
  sync::Arc,
};

use clap::{ArgMatches, Parser};
use log::LevelFilter;
use serde::{Deserialize, Serialize};

use crate::runtime_server_options::RuntimeServerOptions;

/// 默认监听端口
pub const DEFAULT_PORT: u16 = 6379;
/// 默认监听地址（保护模式回环回退；对标 Format.cs:defaultBindLoopBack）
pub const DEFAULT_BIND: &str = "127.0.0.1";
/// 非保护模式监听地址（对标 Format.cs:defaultBindAny；C# 为 IPv4/IPv6 双栈
/// any，Rust 收敛 IPv4 any）
pub const DEFAULT_BIND_ANY: &str = "0.0.0.0";
/// 默认工作目录
pub const DEFAULT_DIR: &str = "./data";
/// 默认周期自动紧缩间隔秒数
pub const DEFAULT_COMPACTION_FREQ_SECS: u64 = 60;

/// 默认发布订阅分发日志页大小字节（C# PubSubPageSize = "4k"）
pub const DEFAULT_PUBSUB_PAGE_SIZE: usize = 4096;

/// 默认 RESP 协议版本（对标 ServerOptions.cs:DEFAULT_RESP_VERSION）
pub const DEFAULT_RESP_VERSION: u8 = 2;

/// 慢日志记录阈值微秒（0 = 禁用；对标 C# Options.cs:351 SlowLogThreshold）
pub const DEFAULT_SLOW_LOG_THRESHOLD: i32 = 0;
/// 慢日志容量上限（对标 GarnetServerOptions.cs:292 SlowLogMaxEntries）
pub const DEFAULT_SLOW_LOG_MAX_ENTRIES: i32 = 128;
/// 默认逻辑数据库数量上限（对标 GarnetServerOptions.cs:615 MaxDatabases）
pub const DEFAULT_MAX_DATABASES: i32 = 16;
/// 默认保护模式（对标 defaults.conf ProtectedMode = "yes"）
pub const DEFAULT_PROTECTED_MODE: bool = true;
/// 默认 *SCAN 单次迭代返回项数上限（对标 ObjectScanCountLimit 默认 1000）
pub const DEFAULT_OBJECT_SCAN_COUNT_LIMIT: i32 = 1000;
/// 默认指标监视器采样周期秒数（0 = 禁用采样任务）
pub const DEFAULT_METRICS_SAMPLING_FREQUENCY_SECS: u64 = 0;

/// 节点参数加载错误（命令行解析、NestedText 配置解析与文件读取；依赖库错误透明转发）。
#[derive(Debug, thiserror::Error)]
pub enum NodeOptionsError {
  /// 命令行解析失败。
  #[error(transparent)]
  Cli(#[from] clap::Error),
  /// NestedText 配置解析失败。
  #[error(transparent)]
  NestedText(#[from] nested_text::Error),
  /// 配置文件读取失败。
  #[error(transparent)]
  Io(#[from] Error),
}

/// 统一节点基础参数配置
#[derive(Debug, Clone, Serialize, Deserialize, Parser)]
#[command(author, version, about = "WeDB 高性能分布式数据库服务")]
pub struct NodeArgs {
  /// 绑定监听 IP 地址（未指定时按 protected-mode 回退：保护回环 / 非保护全接口）
  #[arg(short = 'b', long)]
  #[serde(default)]
  pub bind: Option<String>,

  /// 业务监听端口
  #[arg(short = 'p', long, default_value_t = DEFAULT_PORT)]
  #[serde(default = "default_port")]
  pub port: u16,

  /// Unix 域套接字路径
  #[arg(long)]
  pub unixsocket: Option<String>,

  /// 数据与持久化存储工作目录
  #[arg(short = 'd', long, default_value = DEFAULT_DIR)]
  #[serde(default = "default_dir")]
  pub dir: PathBuf,

  /// WAL / AOF 物理日志存储路径（未显式指定时默认为 <dir>/wal）
  #[arg(long)]
  pub wal_dir: Option<PathBuf>,

  /// 访问认证密码
  #[arg(long)]
  pub requirepass: Option<String>,

  /// TLS 证书文件路径（PEM 格式）
  #[arg(long)]
  pub tls_cert: Option<PathBuf>,

  /// TLS 私钥文件路径（PEM 格式）
  #[arg(long)]
  pub tls_key: Option<PathBuf>,

  /// 工作线程数（默认按可用 CPU 物理核心数）
  #[arg(short = 't', long)]
  pub threads: Option<usize>,

  /// 周期自动紧缩间隔秒数（0 表示禁用）
  #[arg(long, default_value_t = DEFAULT_COMPACTION_FREQ_SECS)]
  #[serde(default = "default_compaction_freq")]
  pub compaction_freq_secs: u64,

  /// 是否启用 AOF 持久化日志（对标 C# Options.cs:209 EnableAOF）
  #[arg(long, default_value_t = false)]
  #[serde(default)]
  pub aof: bool,

  /// 是否禁用发布订阅功能（对标 C# ServerOptions.cs:107 DisablePubSub，
  /// C# 默认 false 即默认启用 pubsub）
  #[arg(long = "disable-pubsub", default_value_t = false)]
  #[serde(default)]
  pub disable_pubsub: bool,

  /// 发布订阅分发日志页大小字节（取 2 的幂；对标 C# ServerOptions.cs:112
  /// PubSubPageSize = "4k" → PubSubPageSizeBytes() = 4096）
  #[arg(long = "pubsub-page-size", default_value_t = DEFAULT_PUBSUB_PAGE_SIZE)]
  #[serde(default = "default_pubsub_page_size")]
  pub pubsub_page_size: usize,

  /// 启动时从最新检查点与 AOF 日志恢复（若存在；对标 C# Options.cs:139 Recover）
  #[arg(short = 'r', long, default_value_t = false)]
  #[serde(default)]
  pub recover: bool,

  /// AOF 周期提交毫秒数
  #[arg(long)]
  pub aof_commit_ms: Option<u8>,

  /// 日志追加文件路径（设置后日志同步落文件；对标 serverSettings.FileLogger）
  #[arg(long)]
  pub file_logger: Option<String>,

  /// 控制台日志最低级别（trace/debug/info/warn/error；对标 serverSettings.LogLevel）
  #[arg(long)]
  pub log_level: Option<String>,

  /// 慢日志记录阈值微秒（0 = 禁用记录；对标 C# Options.cs:351 SlowLogThreshold）
  #[arg(
    long = "slowlog-log-slower-than",
    default_value_t = DEFAULT_SLOW_LOG_THRESHOLD
  )]
  #[serde(default = "default_slow_log_threshold")]
  pub slow_log_threshold: i32,

  /// 慢日志容量上限（对标 C# Options.cs:355 SlowLogMaxEntries，默认 128 =
  /// GarnetServerOptions.cs:292）
  #[arg(long = "slowlog-max-len", default_value_t = DEFAULT_SLOW_LOG_MAX_ENTRIES)]
  #[serde(default = "default_slow_log_max_entries")]
  pub slow_log_max_entries: i32,

  /// 逻辑数据库数量上限（对标 C# Options.cs:688 MaxDatabases，默认 16；
  /// 集群模式恒为 2 = C# AllowMultiDb = !EnableCluster）
  #[arg(long = "max-databases", default_value_t = DEFAULT_MAX_DATABASES)]
  #[serde(default = "default_max_databases")]
  pub max_databases: i32,

  /// 保护模式：bind 未显式指定时回退回环监听（true）或监听全部接口（false；
  /// 对标 C# Options.cs:602 ProtectedMode，默认 yes，Format.cs:TryParseAddressList）
  #[arg(
    long = "protected-mode",
    default_value_t = DEFAULT_PROTECTED_MODE,
    action = clap::ArgAction::Set
  )]
  #[serde(default = "default_protected_mode")]
  pub protected_mode: bool,

  /// *SCAN 命令单次迭代返回项数上限（对标 C# Options.cs:590
  /// ObjectScanCountLimit，默认 1000）
  #[arg(
    long = "object-scan-count-limit",
    default_value_t = DEFAULT_OBJECT_SCAN_COUNT_LIMIT
  )]
  #[serde(default = "default_object_scan_count_limit")]
  pub object_scan_count_limit: i32,

  /// 指标监视器采样周期秒数（0 = 禁用采样任务；对标 C# Options.cs:359
  /// MetricsSamplingFrequency）
  #[arg(
    long = "metrics-sampling-freq",
    default_value_t = DEFAULT_METRICS_SAMPLING_FREQUENCY_SECS
  )]
  #[serde(default = "default_metrics_sampling_frequency_secs")]
  pub metrics_sampling_frequency_secs: u64,

  /// 是否启用延迟监视（跟踪各事件类别延迟分布；对标 C# Options.cs:344
  /// LatencyMonitor）
  #[arg(
    long = "latency-monitor",
    default_value_t = false,
    action = clap::ArgAction::Set
  )]
  #[serde(default)]
  pub latency_monitor: bool,

  /// 是否启用逐命令使用统计（calls / failed / rejected，经 INFO COMMANDSTATS
  /// 输出；对标 C# Options.cs:348 CommandStatsMonitor）
  #[arg(
    long = "commandstats-monitor",
    default_value_t = false,
    action = clap::ArgAction::Set
  )]
  #[serde(default)]
  pub commandstats_monitor: bool,

  /// 是否启用 Lua 脚本（对标 C# Options.cs:284 EnableLua，GarnetServerOptions.cs:91
  /// 默认 false）
  #[arg(long, default_value_t = false)]
  #[serde(default)]
  pub enable_lua: bool,

  /// Lua 脚本单次执行超时毫秒数（0 = 无限；对标 C# Options.cs LuaScriptTimeoutMs，
  /// 0 映射 Timeout.InfiniteTimeSpan 即不建超时管理器）
  #[arg(long, default_value_t = 0)]
  #[serde(default)]
  pub lua_script_timeout_ms: i64,

  /// Lua 脚本事务模式（对标 C# Options.cs:288 LuaTransactionMode，
  /// GarnetServerOptions.cs:96 默认 false）
  #[arg(long, default_value_t = false)]
  #[serde(default)]
  pub lua_transaction_mode: bool,

  /// nested_text 配置文件路径（对标 C# Options.cs:483 ConfigImportPath；
  /// 配置文件自身不可嵌套本项，serde skip 同 C# JsonIgnore 语义）
  #[arg(long, value_name = "FILE")]
  #[serde(skip)]
  pub config: Option<PathBuf>,

  /// 导出合并后的生效配置到 nested_text 文件（对标 C# Options.cs:501
  /// ConfigExportPath；C# 仅导出非默认项，Rust 导出全量字段）
  #[arg(long, value_name = "FILE")]
  #[serde(skip)]
  pub config_export_path: Option<PathBuf>,
}

fn default_port() -> u16 {
  DEFAULT_PORT
}

fn default_dir() -> PathBuf {
  PathBuf::from(DEFAULT_DIR)
}

const fn default_compaction_freq() -> u64 {
  DEFAULT_COMPACTION_FREQ_SECS
}

const fn default_pubsub_page_size() -> usize {
  DEFAULT_PUBSUB_PAGE_SIZE
}

const fn default_slow_log_threshold() -> i32 {
  DEFAULT_SLOW_LOG_THRESHOLD
}

const fn default_slow_log_max_entries() -> i32 {
  DEFAULT_SLOW_LOG_MAX_ENTRIES
}

const fn default_max_databases() -> i32 {
  DEFAULT_MAX_DATABASES
}

const fn default_protected_mode() -> bool {
  DEFAULT_PROTECTED_MODE
}

const fn default_object_scan_count_limit() -> i32 {
  DEFAULT_OBJECT_SCAN_COUNT_LIMIT
}

const fn default_metrics_sampling_frequency_secs() -> u64 {
  DEFAULT_METRICS_SAMPLING_FREQUENCY_SECS
}

impl Default for NodeArgs {
  fn default() -> Self {
    Self {
      bind: None,
      port: default_port(),
      unixsocket: None,
      dir: default_dir(),
      wal_dir: None,
      requirepass: None,
      tls_cert: None,
      tls_key: None,
      threads: None,
      compaction_freq_secs: default_compaction_freq(),
      aof: false,
      disable_pubsub: false,
      pubsub_page_size: default_pubsub_page_size(),
      recover: false,
      aof_commit_ms: None,
      file_logger: None,
      log_level: None,
      slow_log_threshold: default_slow_log_threshold(),
      slow_log_max_entries: default_slow_log_max_entries(),
      max_databases: default_max_databases(),
      protected_mode: default_protected_mode(),
      object_scan_count_limit: default_object_scan_count_limit(),
      metrics_sampling_frequency_secs: default_metrics_sampling_frequency_secs(),
      latency_monitor: false,
      commandstats_monitor: false,
      enable_lua: false,
      lua_script_timeout_ms: 0,
      lua_transaction_mode: false,
      config: None,
      config_export_path: None,
    }
  }
}

impl NodeArgs {
  /// 解析最低日志级别（serverSettings.LogLevel；缺省 Information，
  /// 未知级别回退 Information）
  ///
  /// libs/host/Configuration/CommandLineTypes.cs:LogLevel 解析投影
  pub fn minimum_log_level(&self) -> LevelFilter {
    match self.log_level.as_deref() {
      Some(s) if s.eq_ignore_ascii_case("trace") => LevelFilter::Trace,
      Some(s) if s.eq_ignore_ascii_case("debug") => LevelFilter::Debug,
      Some(s) if s.eq_ignore_ascii_case("warn") || s.eq_ignore_ascii_case("warning") => {
        LevelFilter::Warn
      }
      Some(s) if s.eq_ignore_ascii_case("error") => LevelFilter::Error,
      Some(s) if s.eq_ignore_ascii_case("off") => LevelFilter::Off,
      _ => LevelFilter::Info,
    }
  }

  /// 生成网络端点定义列表（bind 未显式指定时按 protected-mode 回退，
  /// 对标 Format.cs:TryParseAddressList 的保护模式绑定域）
  pub fn endpoints(&self) -> Vec<String> {
    let bind = self.bind.as_deref().unwrap_or(if self.protected_mode {
      DEFAULT_BIND
    } else {
      DEFAULT_BIND_ANY
    });
    let mut eps = vec![format!("{bind}:{}", self.port)];
    if let Some(ref u) = self.unixsocket {
      eps.push(format!("unix:{u}"));
    }
    eps
  }

  /// 获取计算后的 WAL / AOF 日志工作目录（优先使用显式 wal_dir，否则为 dir/wal）
  pub fn wal_dir(&self) -> PathBuf {
    self.wal_dir.clone().unwrap_or_else(|| self.dir.join("wal"))
  }

  /// 投影运行时服务选项（对标 Options.cs:GetServerOptions 的服务选项装配段；
  /// RuntimeServerOptions 为 RuntimeServerConfig 播种的运行时单一真源）
  pub fn runtime_server_options(&self) -> RuntimeServerOptions {
    let mut opts = RuntimeServerOptions::default();
    if let Some(ms) = self.aof_commit_ms {
      opts.commit_frequency_ms = i32::from(ms);
    }
    opts.slow_log_threshold = self.slow_log_threshold;
    opts.slow_log_max_entries = self.slow_log_max_entries;
    opts.max_databases = self.max_databases;
    opts.object_scan_count_limit = self.object_scan_count_limit;
    opts
  }

  /// 从 NestedText 字符串解析配置（唯一的配置文件格式）
  pub fn from_nested_text_str(s: &str) -> Result<Self, NodeOptionsError> {
    nested_text::from_str(s).map_err(Into::into)
  }

  /// 从 NestedText 文件加载配置
  pub fn from_file(path: impl AsRef<Path>) -> Result<Self, NodeOptionsError> {
    let content = fs::read_to_string(path.as_ref())?;
    Self::from_nested_text_str(&content)
  }

  /// 命令行显式项覆盖文件/默认基线
  ///
  /// clap_derive 的 arg id 为字段名原样（long 才是 kebab-case），故以
  /// stringify!(字段名) 作 value_source 查询键
  fn override_explicit(&mut self, matches: &ArgMatches, cli: Self) {
    use clap::parser::ValueSource;
    macro_rules! over {
      ($($f:ident),+ $(,)?) => {
        $(if matches.value_source(stringify!($f)) == Some(ValueSource::CommandLine) {
          self.$f = cli.$f.clone();
        })+
      };
    }
    over![
      bind,
      port,
      unixsocket,
      dir,
      wal_dir,
      requirepass,
      tls_cert,
      tls_key,
      threads,
      compaction_freq_secs,
      aof,
      disable_pubsub,
      pubsub_page_size,
      recover,
      aof_commit_ms,
      file_logger,
      log_level,
      slow_log_threshold,
      slow_log_max_entries,
      max_databases,
      protected_mode,
      object_scan_count_limit,
      metrics_sampling_frequency_secs,
      latency_monitor,
      commandstats_monitor,
      enable_lua,
      lua_script_timeout_ms,
      lua_transaction_mode,
    ];
  }

  /// 导出生效配置到 nested_text 文件（对标 Options.cs:501 ConfigExportPath；
  /// C# 仅导出非默认项，Rust 导出全量字段，差异登记）
  fn export_config(&self, path: &Path) -> Result<(), NodeOptionsError> {
    fs::write(path, nested_text::to_string(self)?)?;
    Ok(())
  }
}

/// 三层配置合并解析契约：结构体默认值 → --config nested_text 文件 → 命令行显式项覆盖
///
/// 对标 libs/host/ServerSettingsManager.cs:TryParseCommandLineArguments：C#
/// 命令行解析两遍，第二遍以文件合并后的对象为工厂仅覆盖显式给出的项；
/// Rust 以 ArgMatches::value_source 判定显式项一次合并完成。
pub trait ConfigFileArgs: clap::CommandFactory + clap::FromArgMatches + Sized {
  /// 从 matches 物化：配置文件基线 + 命令行显式项覆盖
  fn from_layered_matches(matches: &ArgMatches) -> Result<Self, NodeOptionsError>;

  /// 从命令行参数迭代器解析（首个元素为程序名）
  fn from_args_iter<I, T>(itr: I) -> Result<Self, NodeOptionsError>
  where
    I: IntoIterator<Item = T>,
    T: Into<OsString> + Clone,
  {
    let matches = Self::command().try_get_matches_from(itr)?;
    Self::from_layered_matches(&matches)
  }

  /// 从进程命令行参数解析（C# Options 命令行解析入口的对标形态）
  fn from_args() -> Result<Self, NodeOptionsError> {
    Self::from_args_iter(args_os())
  }
}

impl ConfigFileArgs for NodeArgs {
  fn from_layered_matches(matches: &ArgMatches) -> Result<Self, NodeOptionsError> {
    let cli = <Self as clap::FromArgMatches>::from_arg_matches(matches)?;
    // config / config_export_path 为 CLI 专属（serde skip，文件基线恒 None），
    // 合并前先行拷出
    let (import_path, export_path) = (cli.config.clone(), cli.config_export_path.clone());
    let mut merged = match import_path.as_deref() {
      Some(path) => Self::from_file(path)?,
      None => Self::default(),
    };
    merged.override_explicit(matches, cli);
    merged.config = import_path;
    merged.config_export_path = export_path;
    if let Some(path) = &merged.config_export_path {
      merged.export_config(path)?;
    }
    Ok(merged)
  }
}

/// 服务端参数多态契约（泛型解耦单机与集群扩展参数）
pub trait ServerArgs: Send + Sync + 'static {
  /// 获取通用节点参数引用
  fn node_args(&self) -> &NodeArgs;

  /// 获取配置的所有网络监听端点
  #[inline]
  fn endpoints(&self) -> Vec<String> {
    self.node_args().endpoints()
  }

  /// 获取服务监听端口
  #[inline]
  fn port(&self) -> u16 {
    self.node_args().port
  }

  /// 获取工作线程数
  #[inline]
  fn threads(&self) -> Option<usize> {
    self.node_args().threads
  }

  /// 获取 WAL 日志存储目录
  #[inline]
  fn wal_dir(&self) -> PathBuf {
    self.node_args().wal_dir()
  }

  /// 是否启用 AOF 持久化日志
  #[inline]
  fn aof(&self) -> bool {
    self.node_args().aof
  }
}

impl ServerArgs for NodeArgs {
  #[inline]
  fn node_args(&self) -> &NodeArgs {
    self
  }
}

impl<T: ServerArgs> ServerArgs for Arc<T> {
  #[inline]
  fn node_args(&self) -> &NodeArgs {
    (**self).node_args()
  }
}

#[cfg(test)]
mod tests {
  use std::env::temp_dir;

  use super::*;

  /// 写临时 nested_text 配置文件（进程内唯一名，测试结束自清理）
  fn temp_config(name: &str, content: &str) -> PathBuf {
    let path = temp_dir().join(format!("wedb-node-options-{name}.nt"));
    fs::write(&path, content).unwrap();
    path
  }

  #[test]
  fn test_node_args_defaults() {
    let args = NodeArgs::default();
    // 保护模式默认开，bind 未显式 → 回环回退
    assert_eq!(args.bind, None);
    assert!(args.protected_mode);
    assert_eq!(args.endpoints(), vec!["127.0.0.1:6379"]);
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
    // C# 默认链：EnableLua=false、LuaScriptTimeoutMs=0（无限）、
    // LuaTransactionMode=false。
    assert!(!args.enable_lua);
    assert_eq!(args.lua_script_timeout_ms, 0);
    assert!(!args.lua_transaction_mode);
  }

  #[test]
  fn test_node_args_lua_cli_flags() {
    let args = NodeArgs::try_parse_from([
      "wedb",
      "--enable-lua",
      "--lua-script-timeout-ms",
      "5000",
      "--lua-transaction-mode",
    ])
    .unwrap();
    assert!(args.enable_lua);
    assert_eq!(args.lua_script_timeout_ms, 5000);
    assert!(args.lua_transaction_mode);
  }

  #[test]
  fn test_node_args_cli_aof_and_wal_dir() {
    let args = NodeArgs::try_parse_from(["wedb", "--aof", "--wal-dir", "/mnt/wal"]).unwrap();
    assert!(args.aof);
    assert_eq!(args.wal_dir, Some(PathBuf::from("/mnt/wal")));
    assert_eq!(args.wal_dir(), PathBuf::from("/mnt/wal"));

    let args_no_aof = NodeArgs::try_parse_from(["wedb"]).unwrap();
    assert!(!args_no_aof.aof);
    assert_eq!(args_no_aof.wal_dir, None);
    assert_eq!(args_no_aof.wal_dir(), PathBuf::from("./data/wal"));
  }

  #[test]
  fn test_node_args_with_custom_wal_dir() {
    let args = NodeArgs {
      wal_dir: Some(PathBuf::from("/mnt/fast_ssd/wal")),
      ..Default::default()
    };
    assert_eq!(args.wal_dir(), PathBuf::from("/mnt/fast_ssd/wal"));
  }

  #[test]
  fn test_node_args_with_unixsocket() {
    let cfg = NodeArgs {
      unixsocket: Some("/tmp/wedb.sock".to_string()),
      ..Default::default()
    };
    assert_eq!(
      cfg.endpoints(),
      vec!["127.0.0.1:6379", "unix:/tmp/wedb.sock"]
    );
  }

  #[test]
  fn test_nested_text_parse() {
    let nt = r#"
bind: 192.168.1.100
port: 6380
dir: /tmp/wedb_data
"#;
    let conf = NodeArgs::from_nested_text_str(nt).unwrap();
    assert_eq!(conf.bind.as_deref(), Some("192.168.1.100"));
    assert_eq!(conf.port, 6380);
    assert_eq!(conf.dir, PathBuf::from("/tmp/wedb_data"));
  }

  #[test]
  fn test_protected_mode_bind_fallback() {
    // C# Format.cs:TryParseAddressList：保护模式 + 空 bind → 回环；
    // 非保护 + 空 bind → 全接口
    let args = NodeArgs::try_parse_from(["wedb", "--protected-mode", "false"]).unwrap();
    assert_eq!(args.endpoints(), vec!["0.0.0.0:6379"]);

    let args =
      NodeArgs::try_parse_from(["wedb", "--protected-mode", "false", "--bind", "10.0.0.8"])
        .unwrap();
    assert_eq!(args.endpoints(), vec!["10.0.0.8:6379"]);

    let args = NodeArgs::try_parse_from(["wedb"]).unwrap();
    assert_eq!(args.endpoints(), vec!["127.0.0.1:6379"]);
  }

  #[test]
  fn test_config_file_cli_override() {
    // 端到端：文件为基 → CLI 显式覆盖（对标 ServerSettingsManager 三层合并）
    let file = temp_config(
      "override",
      "port: 7000\nslow_log_threshold: 5000\nmax_databases: 4\nbind: 192.168.1.100\n",
    );
    let args =
      NodeArgs::from_args_iter(["wedb", "--config", file.to_str().unwrap(), "--port", "7001"])
        .unwrap();
    fs::remove_file(&file).ok();
    // CLI 覆盖文件值
    assert_eq!(args.port, 7001);
    // 文件值生效
    assert_eq!(args.slow_log_threshold, 5000);
    assert_eq!(args.max_databases, 4);
    assert_eq!(args.bind.as_deref(), Some("192.168.1.100"));
    // 未涉及项取默认
    assert_eq!(args.object_scan_count_limit, 1000);
    assert_eq!(args.endpoints(), vec!["192.168.1.100:7001"]);
  }

  #[test]
  fn test_config_file_kebab_field_override() {
    // kebab-case 长名（--wal-dir）字段的显式判定：CLI 覆盖文件值
    let file = temp_config("kebab", "wal_dir: /file/wal\npubsub_page_size: 8192\n");
    let args = NodeArgs::from_args_iter([
      "wedb",
      "--config",
      file.to_str().unwrap(),
      "--wal-dir",
      "/cli/wal",
    ])
    .unwrap();
    fs::remove_file(&file).ok();
    assert_eq!(args.wal_dir, Some(PathBuf::from("/cli/wal")));
    assert_eq!(args.pubsub_page_size, 8192);
  }

  #[test]
  fn test_config_export_round_trip() {
    // 导出合并后配置 → 重新加载一致
    let file = temp_config("export-src", "port: 7002\nslow_log_max_entries: 64\n");
    let export = temp_dir().join("wedb-node-options-export-out.nt");
    let args = NodeArgs::from_args_iter([
      "wedb",
      "--config",
      file.to_str().unwrap(),
      "--config-export-path",
      export.to_str().unwrap(),
      "--object-scan-count-limit",
      "256",
    ])
    .unwrap();
    fs::remove_file(&file).ok();
    let reloaded = NodeArgs::from_file(&export).unwrap();
    fs::remove_file(&export).ok();
    assert_eq!(reloaded.port, 7002);
    assert_eq!(reloaded.slow_log_max_entries, 64);
    assert_eq!(reloaded.object_scan_count_limit, 256);
    assert_eq!(reloaded.port, args.port);
  }

  #[test]
  fn test_runtime_server_options_projection() {
    // 对标 Options.cs:GetServerOptions 选项装配段
    let args = NodeArgs {
      aof_commit_ms: Some(20),
      slow_log_threshold: 800,
      slow_log_max_entries: 32,
      max_databases: 8,
      object_scan_count_limit: 512,
      ..Default::default()
    };
    let opts = args.runtime_server_options();
    assert_eq!(opts.commit_frequency_ms, 20);
    assert_eq!(opts.slow_log_threshold, 800);
    assert_eq!(opts.slow_log_max_entries, 32);
    assert_eq!(opts.max_databases, 8);
    assert_eq!(opts.object_scan_count_limit, 512);

    // 未设置项保持 C# 默认
    let opts = NodeArgs::default().runtime_server_options();
    assert_eq!(opts.commit_frequency_ms, 0);
    assert_eq!(opts.slow_log_threshold, 0);
    assert_eq!(opts.slow_log_max_entries, 128);
    assert_eq!(opts.max_databases, 16);
    assert_eq!(opts.object_scan_count_limit, 1000);
  }
}
