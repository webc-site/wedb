//! 公共工具层：garnet `libs/common` 工具与 Tsavorite `core/Utilities` 缓冲原语
//!
//! 对应关系：
//! - `ascii` / `num` / `convert` / `crc64` / `hash` / `hash_slot`
//!   ← `libs/common/AsciiUtils.cs`、`NumUtils.cs`、`Crc64.cs`、`HashUtils.cs`
//! - `pool` / [`AlignedBuf`] ← `libs/storage/Tsavorite/cs/src/core/Utilities/BufferPool*.cs`
//!   (SectorAlignedBufferPool Origin-Return 三级缓存) 与 `SectorAlignedMemory`
//!
//! 在 C# 中 `Utilities` 位于依赖图最底层，被 Device、Allocator(Native)、TsavoriteLog、
//! CheckpointManagement 共同引用；本 crate 同样只依赖 wbase，供 wdev / whlog / wram
//! 平行引用，杜绝「设备层反向依赖内存分配层」的拓扑违例。

pub mod ascii;
pub mod convert;
pub mod crc64;
pub mod hash;
pub mod hash_slot;
pub mod num;
mod tests;

mod align;
mod aligned_buf;
mod error;
mod pool;

pub use align::{
  DEFAULT_SECTOR_SIZE, MIN_SECTOR_SIZE, SectorRange, SectorRangeError, align_down, align_up,
  checked_align_up, is_aligned, is_valid_sector_size,
};
pub use aligned_buf::AlignedBuf;
pub use error::{Error, Result};
pub use pool::{
  BufferPool, CLASS_CAPACITIES_SECTORS, DEFAULT_LARGE_BUDGET_BYTES, DEFAULT_SMALL_BUDGET_BYTES,
  DEPOT_STRIPE_CAP, LARGE_TIER_MIN_BYTES, MAX_LOCAL_PER_CLASS, MAX_POOLED_SECTORS, NUM_CLASSES,
  PoolStats, class_capacity_bytes, class_capacity_sectors, class_of_sectors, current_thread_id,
};
