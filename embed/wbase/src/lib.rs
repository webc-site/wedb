#![cfg_attr(docsrs, feature(doc_cfg))]

#[cfg(feature = "addr")]
pub mod addr;

#[cfg(feature = "align")]
pub mod align;
#[cfg(feature = "align")]
pub use align::{
  DEFAULT_SECTOR_SIZE, MIN_SECTOR_SIZE, SectorRange, SectorRangeError, align_down, align_up,
  checked_align_up, is_aligned, is_valid_sector_size,
};

#[cfg(feature = "ascii")]
pub mod ascii;

#[cfg(feature = "backoff")]
pub mod backoff;

#[cfg(feature = "base32")]
pub mod base32;

#[cfg(feature = "buf")]
pub mod buf;

#[cfg(feature = "convert")]
pub mod convert;

#[cfg(feature = "crc")]
pub mod crc;

#[cfg(feature = "crc64")]
pub mod crc64;

#[cfg(feature = "error")]
pub mod error;
#[cfg(feature = "error")]
pub use error::{Error, Result};

#[cfg(feature = "float")]
pub mod float;

#[cfg(feature = "glob")]
pub mod glob;

#[cfg(feature = "hash")]
pub mod hash;

#[cfg(feature = "hash_slot")]
pub mod hash_slot;

#[cfg(any(feature = "map", feature = "set"))]
pub mod map;

#[cfg(feature = "num")]
pub mod num;

#[cfg(feature = "pool")]
pub mod pool;
#[cfg(feature = "pool")]
pub use pool::{
  AlignedBuf, BufferPool, CLASS_CAPACITIES_SECTORS, DEFAULT_LARGE_BUDGET_BYTES,
  DEFAULT_SMALL_BUDGET_BYTES, DEPOT_STRIPE_CAP, LARGE_TIER_MIN_BYTES, MAX_LOCAL_PER_CLASS,
  MAX_POOLED_SECTORS, NUM_CLASSES, PoolStats, class_capacity_bytes, class_capacity_sectors,
  class_of_sectors,
};

#[cfg(feature = "simd")]
pub mod simd;

#[cfg(feature = "striped")]
pub mod striped;

#[cfg(feature = "thread")]
pub mod thread;
#[cfg(feature = "thread")]
pub use thread::current_thread_id;

#[cfg(feature = "time")]
pub mod time;

#[cfg(feature = "varint")]
pub mod varint;
