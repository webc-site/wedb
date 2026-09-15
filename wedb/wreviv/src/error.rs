use std::result;

use thiserror::Error;

use crate::record::FreeRecord;

/// wreviv 错误类型
#[derive(Error, Debug, PartialEq, Eq)]
pub enum Error {
  /// 分桶尺寸列表不能为空
  #[error("分桶尺寸列表不能为空")]
  EmptyBinSizes,

  /// 分桶容量必须大于 0
  #[error("分桶容量必须大于 0")]
  InvalidCapacity,

  /// 分桶尺寸必须严格递增且大于 0
  #[error("分桶尺寸必须严格递增且大于 0")]
  UnsortedBinSizes,

  /// 记录尺寸溢出最大内联尺寸限制
  #[error("记录尺寸溢出最大内联尺寸限制: {0} > {max}", max = FreeRecord::MAX_INLINE_SIZE)]
  SizeOverflow(u32),
}

/// wreviv Result 类型别名
pub type Result<T> = result::Result<T, Error>;
