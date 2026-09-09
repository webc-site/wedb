use std::{io, result};

use thiserror::Error;

use crate::range_index::RangeIndexError;

/// wedb_store 统一错误类型
#[derive(Error, Debug)]
pub enum Error {
  #[error(transparent)]
  Device(#[from] wdev::Error),

  #[error(transparent)]
  Epoch(#[from] wepoch::Error),

  #[error(transparent)]
  HLog(#[from] whlog::Error),

  #[error(transparent)]
  Index(#[from] windex::Error),

  #[error(transparent)]
  Record(#[from] wrecord::Error),

  #[error(transparent)]
  Value(#[from] wval::Error),

  #[error(transparent)]
  Mem(#[from] wram::Error),

  #[error("配置错误: {0}")]
  InvalidConfig(String),

  /// 索引容量与配置不一致（恢复组件装配预检：索引打开时定容，无在线扩容）
  #[error(
    "index_size 配置与实际索引容量不一致: 配置 {config} 桶, 实际 {actual} 桶; 索引打开时按容量定表且运行期无在线扩容, 恢复组件装配禁止缩表或漂移, 请将 index_size 设为 {actual} 与索引快照一致, 或全新建库"
  )]
  IndexSizeMismatch {
    /// 配置声称的哈希索引桶数
    config: usize,
    /// 实际构建/恢复出的哈希索引桶数
    actual: usize,
  },

  #[error(transparent)]
  Io(#[from] io::Error),

  #[error(transparent)]
  BfTree(#[from] wbftree::Error),

  #[error(transparent)]
  RangeIndex(#[from] RangeIndexError),

  #[error(transparent)]
  Compact(#[from] wcompact::Error),

  #[error(transparent)]
  Cpr(#[from] wcpr::Error),
}

impl Error {
  /// 判断是否为数据类型不匹配（WRONGTYPE）错误
  #[inline]
  pub fn is_wrong_type(&self) -> bool {
    matches!(self, Self::Value(wval::Error::InvalidCollectionType(_)))
  }
}

pub type Result<T> = result::Result<T, Error>;
