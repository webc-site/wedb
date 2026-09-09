#![cfg_attr(docsrs, feature(doc_cfg))]

//! 记录格式层：记录头、键值编解码、分块框架与零拷贝记录视图
//!
//! 纯记录格式，不感知任何 Redis 值层语义——zset/set/hash 紧凑编解码、
//! 打平子键、集合元数据均位于上层 wval（对标 Garnet 中 Tsavorite core
//! 不依赖任何 Garnet 对象类型的分层约束）。whlog / windex 仅依赖本层。

mod chunk;
mod codec;
mod error;
mod header;
mod record_mut;
mod record_ref;
mod simd;

pub use chunk::{CHUNK_LEN_PREFIX_SIZE, ChunkCodec, ChunkIter};
pub use codec::{checked_record_size, encode_to_slice, record_size, try_encode_to_vec};
pub use error::{Error, Result};
pub use header::{
  ADDRESS_MASK, HEADER_READ_CACHE_BIT, HEADER_SIZE, IN_NEW_VERSION_BIT, MAX_FILLER_BYTES,
  MODIFIED_BIT, PAD_KEY_LEN, RecordHeader, SEALED_BIT, TOMBSTONE_BIT,
};
pub use record_mut::RecordMut;
pub use record_ref::RecordRef;
pub use simd::fast_key_eq;
