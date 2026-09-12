use std::result;

use thiserror::Error;
use wkv::RangeIndexError;
use wnode::aof::AofReplayError;

#[derive(Error, Debug)]
pub enum Error {
  /// 存储引擎错误（含会话创建失败）
  #[error(transparent)]
  Store(#[from] wkv::Error),
  /// 范围索引操作错误
  #[error(transparent)]
  RangeIndex(#[from] RangeIndexError),
  /// WAL 物理层错误
  #[error(transparent)]
  Wal(#[from] waof::Error),
  /// AOF 重放错误
  #[error(transparent)]
  Aof(#[from] AofReplayError),
}

pub type Result<T> = result::Result<T, Error>;
