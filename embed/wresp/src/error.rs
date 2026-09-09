use std::result;

use thiserror::Error;

#[derive(Error, Debug, PartialEq, Eq)]
pub enum Error {
  #[error("Integer overflow during RESP parsing at offset {offset}")]
  IntegerOverflow { offset: usize },
  #[error("Invalid integer during RESP parsing")]
  InvalidInteger,
  #[error("Unexpected end of RESP input")]
  UnexpectedEnd,
  #[error("Unexpected token {0}")]
  UnexpectedToken(u8),
  #[error("Invalid string length {0}")]
  InvalidStringLength(i32),
  #[error("Not a number")]
  NotANumber,
}

pub type Result<T> = result::Result<T, Error>;
