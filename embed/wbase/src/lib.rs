#![cfg_attr(docsrs, feature(doc_cfg))]

#[cfg(feature = "addr")]
pub mod addr;

#[cfg(feature = "align")]
pub mod align;

#[cfg(feature = "backoff")]
pub mod backoff;

#[cfg(feature = "crc")]
pub mod crc;

#[cfg(feature = "float")]
pub mod float;

#[cfg(feature = "base32")]
pub mod base32;

#[cfg(feature = "simd")]
pub mod simd;

#[cfg(feature = "thread")]
pub mod thread;

#[cfg(feature = "striped")]
pub mod striped;

#[cfg(feature = "time")]
pub mod time;

#[cfg(feature = "buf")]
pub mod buf;

#[cfg(feature = "glob")]
pub mod glob;

#[cfg(feature = "varint")]
pub mod varint;
