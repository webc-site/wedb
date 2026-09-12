//! wnode 统一错误定义

use std::{io, result};

use thiserror::Error;

#[derive(Debug, Error)]
pub enum Error {
  #[error(transparent)]
  Io(#[from] io::Error),

  #[error("等待停机信号的通道已断开: {0}")]
  SignalChannelBroken(String),

  #[error("地址解析失败: {0}")]
  AddrParse(String),

  #[error("服务已停机")]
  Stopped,

  #[error("{0}")]
  Custom(String),
}

pub type Result<T> = result::Result<T, Error>;
