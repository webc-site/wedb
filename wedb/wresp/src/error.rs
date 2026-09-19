use std::result;

use thiserror::Error;

#[derive(Error, Debug, PartialEq, Eq)]
pub enum Error {
  #[error("RESP 解析整数溢出，偏移量: {offset}")]
  IntegerOverflow { offset: usize },
  #[error("非预期标记: {0}")]
  UnexpectedToken(u8),
  #[error("无效字符串长度: {0}")]
  InvalidStringLength(i32),
  #[error("非有效数字")]
  NotANumber,
}

pub type Result<T> = result::Result<T, Error>;
