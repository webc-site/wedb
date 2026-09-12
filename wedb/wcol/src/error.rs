use std::result;

use thiserror::Error;

#[derive(Error, Debug)]
pub enum Error {
  /// 底层 BfTree 操作失败
  #[error(transparent)]
  Tree(#[from] wbftree::Error),

  /// 数据损坏或格式不合法
  #[error("数据格式损坏: {0}")]
  Corrupted(&'static str),

  /// 参数非法
  #[error("参数非法: {0}")]
  InvalidArgument(&'static str),

  /// 空值非法（底层树约束）
  #[error("空值非法")]
  EmptyValue,

  /// 键或字段超长
  #[error("键或字段超长")]
  KeyTooLong,
}

pub type CollectionError = Error;
pub type Result<T> = result::Result<T, Error>;
