#![cfg_attr(docsrs, feature(doc_cfg))]

mod align;
mod aligned_buf;
mod direct_vm;
mod error;
mod pool;
mod tracker;

pub use align::{
  DEFAULT_SECTOR_SIZE, MIN_SECTOR_SIZE, SectorRange, SectorRangeError, align_down, align_up,
  checked_align_up, is_aligned, is_valid_sector_size,
};
pub use aligned_buf::AlignedBuf;
pub use direct_vm::{DirectVirtualMemory, DirectVmBlock, system_page_size};
pub use error::{Error, Result};
pub use pool::{
  BufferPool, CLASS_CAPACITIES_SECTORS, DEFAULT_LARGE_BUDGET_BYTES, DEFAULT_SMALL_BUDGET_BYTES,
  DEPOT_STRIPE_CAP, LARGE_TIER_MIN_BYTES, MAX_LOCAL_PER_CLASS, MAX_POOLED_SECTORS, NUM_CLASSES,
  PoolStats, class_capacity_bytes, class_capacity_sectors, class_of_sectors, current_thread_id,
};
pub use tracker::NativeMemoryTracker;
