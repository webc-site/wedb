use std::error::Error as StdError;
use std::fmt::Display;
use std::io;
use std::result::Result as StdResult;

use thiserror::Error;
use zenoh_raft::StorageError;
use zenoh_raft::error::{Fatal, RaftError};

use crate::types::TypeConfig;

pub type Result<T, E = Error> = StdResult<T, E>;

#[derive(Error, Debug)]
pub enum Error {
  #[error("Storage error: {0}")]
  Storage(String),

  #[error("Raft error: {0}")]
  Raft(String),

  #[error("Config error: {0}")]
  Config(String),

  #[error("Internal error: {0}")]
  Internal(String),

  #[error("Retryable error: {0}")]
  Retryable(#[source] io::Error),

  #[error("IO error: {0}")]
  Io(#[from] io::Error),

  #[error("Fjall error: {0}")]
  Fjall(#[from] fjall::Error),

  #[error("Embed error: {0}")]
  Embed(#[from] wedb_embed::Error),

  #[error("Serialization error: {0}")]
  Serialization(String),

  #[error("Network error: {0}")]
  Network(String),

  #[error("Not found: {0}")]
  NotFound(String),

  #[error("Timeout: {0}")]
  Timeout(String),
}

impl Error {
  pub fn conf(msg: impl Into<String>) -> Self {
    Self::Config(msg.into())
  }

  pub fn internal(msg: impl Into<String>) -> Self {
    Self::Internal(msg.into())
  }

  pub fn internal_with_source<E: StdError + Send + Sync + 'static>(
    msg: impl Into<String>,
    src: E,
  ) -> Self {
    let msg_str: String = msg.into();
    Self::Internal(format!("{msg_str}: {src}"))
  }

  pub fn raft(msg: impl Into<String>) -> Self {
    Self::Raft(msg.into())
  }

  pub fn retryable(src: io::Error) -> Self {
    Self::Retryable(src)
  }

  pub fn not_found(msg: impl Into<String>) -> Self {
    Self::NotFound(msg.into())
  }

  pub fn storage(msg: impl Into<String>) -> Self {
    Self::Storage(msg.into())
  }

  pub fn redis(msg: impl Into<String>) -> Self {
    Self::Internal(msg.into())
  }

  pub fn invalid_data(msg: impl Into<String>) -> Self {
    Self::Internal(msg.into())
  }

  pub fn is_retryable(&self) -> bool {
    matches!(self, Self::Retryable(_))
  }
}

impl<E: Display> From<RaftError<TypeConfig, E>> for Error {
  fn from(e: RaftError<TypeConfig, E>) -> Self {
    Self::Raft(e.to_string())
  }
}

impl From<Fatal<TypeConfig>> for Error {
  fn from(e: Fatal<TypeConfig>) -> Self {
    Self::Raft(e.to_string())
  }
}

impl From<StorageError<TypeConfig>> for Error {
  fn from(e: StorageError<TypeConfig>) -> Self {
    Self::Storage(e.to_string())
  }
}
