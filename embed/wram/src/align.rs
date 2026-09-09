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
