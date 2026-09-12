use std::result;

use thiserror::Error;

#[derive(Error, Debug, PartialEq, Eq)]
pub enum Error {
  #[error("Integer overflow during RESP parsing at offset {offset}")]
  IntegerOverflow { offset: usize },
  #[error("Unexpected token {0}")]
  UnexpectedToken(u8),
  #[error("Invalid string length {0}")]
  InvalidStringLength(i32),
  #[error("Not a number")]
  NotANumber,
}

pub type Result<T> = result::Result<T, Error>;
