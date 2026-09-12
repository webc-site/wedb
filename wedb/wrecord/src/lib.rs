#![cfg_attr(docsrs, feature(doc_cfg))]

//! 记录格式层：记录头、键值编解码、分块框架与零拷贝记录视图
//!
//! 纯记录格式，不感知任何 Redis 值层语义——zset/set/hash 紧凑编解码、
//! 打平子键、集合元数据均位于上层 wval（对标 Garnet 中 Tsavorite core
//! 不依赖任何 Garnet 对象类型的分层约束）。whlog / windex 仅依赖本层。
//!
//! C# RecordDataHeader 的 RecordType 判别字节（byte 6，Garnet 侧解释：
//! VectorManager.RecordType=1、RangeIndexManager.RangeIndexRecordType=2）与 Namespace
//! 字节（byte 7）不在本层承载：记录类型判别由 wval 键前缀 KeyTag（enum u8）与
//! MetaValue.collection_type（值载荷）承载——如 RangeIndex 存根记录 = wval
//! `CollectionType::RangeIndex` 元数据 + wbftree 存根载荷（wkv::range_index），
//! 命名空间由 wval 会话前缀（ns+db varint）编入物理键。与 C# 中 RecordInfo/RDH 属
//! Tsavorite 核心、RecordType 语义由 Garnet 调用方解释的分层等价。

mod chunk;
mod codec;
mod error;
mod header;
mod record_mut;
mod record_ref;
mod simd;

pub use chunk::{CHUNK_LEN_PREFIX_SIZE, ChunkCodec, ChunkIter};
pub use codec::{
  MAX_KEY_LEN, checked_record_size, encode_to_slice, record_size, try_encode_to_vec,
};
pub use error::{Error, Result};
pub use header::{
  ADDRESS_MASK, HEADER_READ_CACHE_BIT, HEADER_SIZE, IN_NEW_VERSION_BIT, MAX_FILLER_BYTES,
  MODIFIED_BIT, PAD_KEY_LEN, RECORD_ALIGNMENT, RecordHeader, SEALED_BIT, TOMBSTONE_BIT,
};
pub use record_mut::RecordMut;
pub use record_ref::RecordRef;
pub use simd::fast_key_eq;
