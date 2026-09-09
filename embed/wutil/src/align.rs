//! 扇区/缓存行对齐原语再导出 (1:1 对标 libs/storage/Tsavorite/cs/src/core/Utilities/Utility.cs 对齐数学)
//!
//! 对齐常数本体位于 wbase（基础原语层），本模块按 C# Tsavorite core 单程序集内
//! `Utilities` 直接引用 `Utility` 的形态原样再导出，供 pool / aligned_buf 就地使用。

pub use wbase::align::{
  DEFAULT_SECTOR_SIZE, MIN_SECTOR_SIZE, SectorRange, SectorRangeError, align_down, align_up,
  checked_align_up, is_aligned, is_valid_sector_size,
};

use crate::error::{Error, Result};

/// 校验扇区大小为 2 的幂且不小于 [`MIN_SECTOR_SIZE`] (池 / 缓冲区 / 范围换算共用)
#[inline]
pub(crate) fn validate_sector_size(size: usize) -> Result<()> {
  if !is_valid_sector_size(size) {
    return Err(Error::InvalidAlignment(size, MIN_SECTOR_SIZE));
  }
  Ok(())
}
