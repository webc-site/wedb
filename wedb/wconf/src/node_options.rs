//! 统一节点通用命令行与配置参数
//!
//! 包含通用网络端点、存储路径、工作线程与认证密码配置，供单机与集群模式共同复用。

use std::{
  fs,
  io::Error,
  path::{Path, PathBuf},
  sync::Arc,
};

use clap::Parser;
use log::LevelFilter;
use serde::{Deserialize, Serialize};

/// 默认监听端口
pub const DEFAULT_PORT: u16 = 6379;
/// 默认监听地址
pub const DEFAULT_BIND: &str = "127.0.0.1";
/// 默认工作目录
pub const DEFAULT_DIR: &str = "./data";
/// 默认周期自动紧缩间隔秒数
pub const DEFAULT_COMPACTION_FREQ_SECS: u64 = 60;

/// 发布订阅分发日志默认页大小字节（C# PubSubPageSize = "4k"）
pub const DEFAULT_PUBSUB_PAGE_SIZE: usize = 4096;

/// 默认 RESP 协议版本（对标 ServerOptions.cs:DEFAULT_RESP_VERSION）
pub const DEFAULT_RESP_VERSION: u8 = 2;

/// 节点参数加载错误（NestedText 配置解析与文件读取；依赖库错误透明转发）。
#[derive(Debug, thiserror::Error)]
pub enum NodeOptionsError {
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
  /// 绑定监听 IP 地址
  #[arg(short = 'b', long, default_value = DEFAULT_BIND)]
  #[serde(default = "default_bind")]
  pub bind: String,

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
}

fn default_bind() -> String {
  DEFAULT_BIND.to_string()
}

const fn default_port() -> u16 {
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

impl Default for NodeArgs {
  fn default() -> Self {
    Self {
      bind: default_bind(),
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
      enable_lua: false,
      lua_script_timeout_ms: 0,
      lua_transaction_mode: false,
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
}

impl NodeArgs {
  /// 生成网络端点定义列表
  pub fn endpoints(&self) -> Vec<String> {
    let mut eps = vec![format!("{}:{}", self.bind, self.port)];
    if let Some(ref u) = self.unixsocket {
      eps.push(format!("unix:{u}"));
    }
    eps
  }

  /// 获取计算后的 WAL / AOF 日志工作目录（优先使用显式 wal_dir，否则为 dir/wal）
  pub fn wal_dir(&self) -> PathBuf {
    self.wal_dir.clone().unwrap_or_else(|| self.dir.join("wal"))
  }

  /// 从命令行参数解析配置
  pub fn from_args() -> Self {
    Self::parse()
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
  use super::*;

  #[test]
  fn test_node_args_defaults() {
    let args = NodeArgs::default();
    assert_eq!(args.bind, "127.0.0.1");
    assert_eq!(args.port, 6379);
    assert_eq!(args.dir, PathBuf::from("./data"));
    assert_eq!(args.wal_dir(), PathBuf::from("./data/wal"));
    assert_eq!(args.endpoints(), vec!["127.0.0.1:6379"]);
    assert!(!args.aof);
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
    assert_eq!(conf.bind, "192.168.1.100");
    assert_eq!(conf.port, 6380);
    assert_eq!(conf.dir, PathBuf::from("/tmp/wedb_data"));
  }
}
