use std::result;

use thiserror::Error;

#[derive(Error, Debug)]
pub enum Error {
  #[error("Not implemented")]
  NotImplemented,
  /// 序列化/反序列化底层 I/O 或格式错误（roaring 经 io::Error 上抛）
  #[error(transparent)]
  Io(#[from] std::io::Error),
  #[error("Invalid bit")]
  InvalidBit,
  #[error("Invalid offset")]
  InvalidOffset,
}

pub type Result<T> = result::Result<T, Error>;
