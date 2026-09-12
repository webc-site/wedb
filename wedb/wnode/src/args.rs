//! 统一节点通用命令行与配置参数
//!
//! 包含通用网络端点、存储路径、工作线程与认证密码配置，供单机与集群模式共同复用。

use std::{
  fs,
  path::{Path, PathBuf},
  sync::Arc,
};

use clap::Parser;
use serde::{Deserialize, Serialize};

use crate::error::{Error, Result};

/// 默认监听端口
pub const DEFAULT_PORT: u16 = 6379;
/// 默认监听地址
pub const DEFAULT_BIND: &str = "127.0.0.1";
/// 默认工作目录
pub const DEFAULT_DIR: &str = "./data";
/// 默认周期自动紧缩间隔秒数
pub const DEFAULT_COMPACTION_FREQ_SECS: u64 = 60;

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

  /// 工作线程数（默认按可用 CPU 物理核心数）
  #[arg(short = 't', long)]
  pub threads: Option<usize>,

  /// 周期自动紧缩间隔秒数（0 表示禁用）
  #[arg(long, default_value_t = DEFAULT_COMPACTION_FREQ_SECS)]
  #[serde(default = "default_compaction_freq")]
  pub compaction_freq_secs: u64,

  /// AOF 周期提交毫秒数
  #[arg(long)]
  pub aof_commit_ms: Option<u64>,
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

impl Default for NodeArgs {
  fn default() -> Self {
    Self {
      bind: default_bind(),
      port: default_port(),
      unixsocket: None,
      dir: default_dir(),
      wal_dir: None,
      requirepass: None,
      threads: None,
      compaction_freq_secs: default_compaction_freq(),
      aof_commit_ms: None,
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
  pub fn from_nested_text_str(s: &str) -> Result<Self> {
    nested_text::from_str(s).map_err(|e| Error::Custom(format!("NestedText 配置解析失败: {e}")))
  }

  /// 从 NestedText 文件加载配置
  pub fn from_file(path: impl AsRef<Path>) -> Result<Self> {
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
  }

  #[test]
  fn test_node_args_with_custom_wal_dir() {
    let args = NodeArgs {
      wal_dir: Some(PathBuf::from("/mnt/fast_ssd/wal")),
      ..Default::default()
    };
    assert_eq!(args.wal_dir(), PathBuf::from("/mnt/fast_ssd/wal"));
  }
}
