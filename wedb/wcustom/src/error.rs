use core::result;

use thiserror::Error;

/// 自定义扩展注册与命令管理错误
#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum Error {
  #[error("Out of registration space")]
  OutOfSpace,

  #[error("Type already registered with ID")]
  TypeAlreadyRegistered,
}

pub type Result<T> = result::Result<T, Error>;
