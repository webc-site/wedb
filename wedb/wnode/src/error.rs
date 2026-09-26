//! wnode 统一错误定义

use std::{io, result};

use thiserror::Error;
use wkv::RangeIndexError;

use crate::aof::AofReplayError;

#[derive(Debug, Error)]
pub enum Error {
  #[error(transparent)]
  Io(#[from] io::Error),

  /// 存储设备错误（wdev 透传）
  #[error(transparent)]
  Device(#[from] wdev::Error),

  /// 存储引擎错误（wkv 透传）
  #[error(transparent)]
  Store(#[from] wkv::Error),

  /// 范围索引操作错误（wkv 透传）
  #[error(transparent)]
  RangeIndex(#[from] RangeIndexError),

  /// WAL 物理层错误（waof 透传）
  #[error(transparent)]
  Wal(#[from] waof::Error),

  /// AOF 重放错误
  #[error(transparent)]
  Aof(#[from] AofReplayError),

  /// 节点参数装配错误（wconf 透传）
  #[error(transparent)]
  Options(#[from] wconf::NodeOptionsError),

  #[error("等待停机信号的通道已断开: {0}")]
  SignalChannelBroken(String),

  #[error("地址解析失败: {0}")]
  AddrParse(String),

  /// 日志器装配失败（C# GarnetServer 构造器日志装配段；log SetLoggerError 场景）
  #[error("日志器安装失败: {0}")]
  LogInstall(String),

  #[error("服务已停机")]
  Stopped,

  /// 启动参数非法（对标 C# GarnetServer 构造期 ArgumentException，
  /// 如 ClusterProvider.cs:60 GossipSamplePercent 越界校验）
  #[error("参数非法: {0}")]
  InvalidArgument(String),

  /// 数据目录排他锁被占：另一实例正在同一数据目录上运行
  ///（SO_REUSEPORT 偏差下防多实例并发双写的唯一防线，deviations 第 11 条；
  /// 见 `datadir_lock` 模块）
  #[error("数据目录已被其他实例占用，拒绝启动（数据目录: {dir}，锁文件: {lock_file}）")]
  DataDirLocked { dir: String, lock_file: String },
}

pub type Result<T> = result::Result<T, Error>;
