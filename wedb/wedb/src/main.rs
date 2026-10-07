#![recursion_limit = "256"]
//! WeDB 分布式集群服务端主程序入口
//!
//! 集群与单机共用同一套 wnode::RespServerSession 会话体系、存储执行域与
//! StorageSessionProvider 装配基座（功能一致：存储 + 经纪 + 向量三件套同源）：
//! 集群差异仅为 decorate 钩子构造 with_cluster 会话消费者（挂
//! ClusterSession 切面，库数上限与单机同口径读 max_databases 配置、全模式
//! 自由切库），槽位验证、MOVED/ASK 重定向、
//! CLUSTER 命令族、ROLE/HELLO 集群分支均由会话主循环经切面驱动。

use std::{env::args_os, error};

use wedb::server::boot::run_cluster_server;
use wnode::logging::bootstrap_args_with_logging;

/// 进程边界错误域为 `Box<dyn std::error::Error>`：前段日志装配为 wnode 错误
/// 域（LogInstall），尾段服务运行为 wedb 错误域（Node 透明变体收口），两域
/// 均经 StdError 自动装箱，进程退出码语义不变
fn main() -> Result<(), Box<dyn error::Error>> {
  // 前段三步（先行缓冲安装 → 三层配置解析 → 日志装配回灌）收口 wnode 单点
  //（与单机宿主同一份，对标 C# GarnetServer 构造器 initLogger 段）
  let args = bootstrap_args_with_logging(args_os())?;

  run_cluster_server(args, None)?;
  Ok(())
}
