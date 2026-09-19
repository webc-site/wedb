#![cfg_attr(docsrs, feature(doc_cfg))]

#[cfg(feature = "addr")]
pub mod addr;

#[cfg(feature = "align")]
pub mod align;

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

#[cfg(feature = "endpoint")]
pub mod endpoint;

#[cfg(feature = "error")]
pub mod error;

#[cfg(feature = "future")]
pub mod future;

#[cfg(feature = "glob")]
pub mod glob;

#[cfg(feature = "group-commit")]
pub mod group_commit;

#[cfg(feature = "hash")]
pub mod hash;

pub mod heap;

#[cfg(feature = "hash_slot")]
pub mod hash_slot;

#[cfg(feature = "hex")]
pub mod hex;

#[cfg(any(feature = "map", feature = "set"))]
pub mod map;

#[cfg(feature = "num")]
pub mod num;

#[cfg(feature = "pool")]
pub mod pool;
#[cfg(feature = "pool")]
pub mod throttle;

#[cfg(feature = "simd")]
pub mod simd;

#[cfg(feature = "striped")]
pub mod striped;

#[cfg(feature = "thread")]
pub mod thread;

#[cfg(feature = "time")]
pub mod time;

#[cfg(feature = "store_type")]
pub mod store_type;

#[cfg(feature = "varint")]
pub mod varint;
