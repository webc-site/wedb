//! wnode 统一错误定义

use std::{io, result};

use thiserror::Error;

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

  #[error("等待停机信号的通道已断开: {0}")]
  SignalChannelBroken(String),

  #[error("地址解析失败: {0}")]
  AddrParse(String),

  /// 日志器装配失败（C# GarnetServer 构造器日志装配段；log SetLoggerError 场景）
  #[error("日志器安装失败: {0}")]
  LogInstall(String),

  #[error("服务已停机")]
  Stopped,
}

pub type Result<T> = result::Result<T, Error>;
