use std::result;

use thiserror::Error;

#[derive(Error, Debug)]
pub enum Error {
  #[error("Integer overflow during RESP parsing at offset {offset}")]
  IntegerOverflow { offset: usize },
  #[error("Invalid integer during RESP parsing")]
  InvalidInteger,
  #[error("Unexpected end of RESP input")]
  UnexpectedEnd,
}

pub type Result<T> = result::Result<T, Error>;
