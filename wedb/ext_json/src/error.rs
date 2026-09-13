use std::result;

use thiserror::Error;

#[derive(Error, Debug)]
pub enum Error {
  #[error("Not implemented")]
  NotImplemented,
  #[error("JSON error: {0}")]
  Json(String),
}

pub type Result<T> = result::Result<T, Error>;
