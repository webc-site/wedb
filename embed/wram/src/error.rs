use std::{
  alloc::{Layout, LayoutError},
  io::Error as IoError,
  result,
};

use thiserror::Error;
use wbase::align::{MIN_SECTOR_SIZE, SectorRangeError};

#[derive(Error, Debug)]
pub enum Error {
  #[error("无效对齐大小: {0}，必须为 2 的幂且 >= {1}")]
  InvalidAlignment(usize, usize),

  #[error("无效大小: {0}")]
  InvalidSize(usize),

  #[error("设置长度 {len} 超出容量 {capacity}")]
  SetLenExceeded { len: usize, capacity: usize },

  #[error("内存分配失败: {0:?}")]
  AllocFailed(Layout),

  #[error("直接虚拟内存分配失败 (大小: {size} 字节): {source}")]
  DirectVmAllocFailed { size: usize, source: IoError },

  #[error("范围计算溢出")]
  Overflow,

  #[error(transparent)]
  Layout(#[from] LayoutError),
}

impl From<SectorRangeError> for Error {
  fn from(err: SectorRangeError) -> Self {
    match err {
      SectorRangeError::InvalidSectorSize(s) => Error::InvalidAlignment(s, MIN_SECTOR_SIZE),
      SectorRangeError::Overflow => Error::Overflow,
    }
  }
}

pub type Result<T> = result::Result<T, Error>;
