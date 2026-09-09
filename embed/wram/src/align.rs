//! 扇区对齐原语再导出 (对齐数学本体对标 libs/storage/Tsavorite/cs/src/core/Utilities/Utility.cs)
//!
//! 仅为存量调用方的兼容门面；校验与池内算术已随 BufferPool 下沉至 wutil::align。

pub use wbase::align::{
  DEFAULT_SECTOR_SIZE, MIN_SECTOR_SIZE, SectorRange, SectorRangeError, align_down, align_up,
  checked_align_up, is_aligned, is_valid_sector_size,
};
