use std::result;

use thiserror::Error;

/// wepoch 错误类型
#[derive(Error, Debug, Clone, PartialEq, Eq)]
pub enum Error {
  /// 参与者数量达到上限
  #[error("已达到最大参与者数量上限: {0}")]
  ExceededMaxThreads(usize),
  /// 用户字槽位数量达到上限
  #[error("已达到最大用户字槽位上限: {0}")]
  ExceededMaxUserWords(usize),
  /// 非法的用户字槽位索引
  #[error("非法的用户字槽位索引: {0}")]
  InvalidUserWordIndex(usize),
  /// 当前线程未处于纪元保护区
  #[error("当前线程未处于纪元保护区")]
  NotProtected,
}

/// wepoch 结果类型
pub type Result<T> = result::Result<T, Error>;
