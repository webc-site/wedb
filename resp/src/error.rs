use std::io;
use std::result;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum Error {
  #[error("I/O error: {0}")]
  Io(#[from] io::Error),

  #[error("Protocol error: {0}")]
  Protocol(String),

  #[error("Invalid data: {0}")]
  InvalidData(String),
}

impl Error {
  #[inline]
  pub fn protocol(msg: impl Into<String>) -> Self {
    Self::Protocol(msg.into())
  }

  #[inline]
  pub fn invalid_data(msg: impl Into<String>) -> Self {
    Self::InvalidData(msg.into())
  }
}

impl From<Error> for io::Error {
  #[inline]
  fn from(err: Error) -> Self {
    match err {
      Error::Io(e) => e,
      Error::Protocol(msg) | Error::InvalidData(msg) => {
        io::Error::new(io::ErrorKind::InvalidData, msg)
      }
    }
  }
}

pub type Result<T> = result::Result<T, Error>;
