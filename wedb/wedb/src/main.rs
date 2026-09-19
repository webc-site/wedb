//! WeDB 分布式集群服务端主程序入口
//!
//! 集群与单机共用同一套 wnode::RespServerSession 会话体系、存储执行域与
//! StorageSessionProvider 装配基座（功能一致：存储 + 经纪 + 向量三件套同源）：
//! 集群差异仅为 decorate 钩子构造 with_cluster 会话消费者（挂
//! ClusterSession 切面，库数上限与单机同口径读 max_databases 配置、全模式
//! 自由切库），槽位验证、MOVED/ASK 重定向、
//! CLUSTER 命令族、ROLE/HELLO 集群分支均由会话主循环经切面驱动。

use std::env::args_os;

use wconf::{ConfigFileArgs, ServerArgs};
use wedb::{ClusterArgs, server::boot::run_cluster_server};
use wnode::{Error, LoggingBuilder, MemoryForwardLogger, Result};

fn main() -> Result<()> {
  // 先行缓冲安装（对标 C# GarnetServer 构造器 :86-88 的 initLogger 段）：正式
  // 日志面要等参数解析完才谈得上装配，期间记录先进内存，装配时回灌
  let pre_parse = MemoryForwardLogger::install().map_err(|e| Error::LogInstall(e.to_string()))?;

  // 三层配置合并解析：默认值 → --config nested_text 文件 → 命令行显式项
  // （对标 ServerSettingsManager.cs:TryParseCommandLineArguments）
  let args =
    ClusterArgs::from_args_iter(args_os()).map_err(|e| Error::InvalidArgument(e.to_string()))?;

  // 日志装配收口 wnode 单点 LoggingBuilder::from_node + flush_into（对标 C#
  // GarnetServer 构造器 loggerFactory 段与 FlushMemoryLogger 回灌）
  LoggingBuilder::from_node(args.node_args()).flush_into(&pre_parse);

  run_cluster_server(args, None)
}
