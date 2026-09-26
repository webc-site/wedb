#![cfg_attr(docsrs, feature(doc_cfg))]

pub mod ascii;
pub use ascii::{ascii_sanitize, eq_ascii_case};

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

/// 跨 crate 基座配置定义（互不应依赖的消费方共用同一判据：紧缩档位、逻辑库界限），
/// 按 task/review.md 规则以特性启用，消费方按需添加 wbase 的 cfg 特性依赖
#[cfg(feature = "cfg")]
pub mod cfg;

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

pub mod keyfmt;

#[cfg(any(feature = "map", feature = "set"))]
pub mod map;

#[cfg(feature = "num")]
pub mod num;

pub mod ns_prefix;

#[cfg(feature = "pool")]
pub mod pool;
#[cfg(feature = "primed")]
pub mod primed;
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

#[cfg(feature = "supervise")]
pub mod supervise;

#[cfg(feature = "varint")]
pub mod varint;
