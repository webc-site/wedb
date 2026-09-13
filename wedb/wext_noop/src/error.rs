use std::result;

use thiserror::Error;

#[derive(Error, Debug)]
pub enum Error {
  #[error("Not implemented")]
  NotImplemented,
  #[error("Invalid operation")]
  InvalidOperation,
}

pub type Result<T> = result::Result<T, Error>;
