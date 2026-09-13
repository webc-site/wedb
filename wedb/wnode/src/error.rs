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

  /// NestedText 配置解析错误（nested-text 透传）
  #[error(transparent)]
  Config(#[from] nested_text::Error),

  #[error("等待停机信号的通道已断开: {0}")]
  SignalChannelBroken(String),

  #[error("地址解析失败: {0}")]
  AddrParse(String),

  #[error("服务已停机")]
  Stopped,
}

pub type Result<T> = result::Result<T, Error>;
