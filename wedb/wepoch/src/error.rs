use std::result;

use thiserror::Error;

/// wepoch 错误类型
#[derive(Error, Debug, Clone, PartialEq, Eq)]
pub enum Error {
  /// 参与者数量达到上限
  #[error("已达到最大参与者数量上限: {0}")]
  ExceededMaxThreads(usize),
}

/// wepoch 结果类型
pub type Result<T> = result::Result<T, Error>;
