#![cfg_attr(docsrs, feature(doc_cfg))]

//! Redis 值层编解码：命名空间子键、集合元数据与紧凑容器
//!
//! 定位为领域编码层：zset/set/hash 紧凑编解码、打平子键与命名空间编解码、
//! 集合元数据与无重复抽样。纯值域编解码，不感知任何引擎层类型
//! （对标 Garnet 中 Tsavorite core 与 libs/server 值对象层的分层约束）。

mod compact_hash;
mod compact_set;
mod compact_zset;
mod error;
mod meta;
mod ns_codec;
mod sample;
mod tag;
mod ttl;

pub use compact_hash::{
  COMPACT_HASH_COUNT_SIZE, COMPACT_HASH_EXPIRE_FLAG_SIZE, COMPACT_HASH_EXPIRE_TIME_SIZE,
  COMPACT_HASH_LEN_SIZE, CompactHash, CompactHashCodec, CompactHashIter, FieldValueRef,
  HashEntryRef,
};
pub use compact_set::{
  COMPACT_SET_COUNT_SIZE, COMPACT_SET_LEN_SIZE, CompactSet, CompactSetCodec, CompactSetIter,
};
pub use compact_zset::{
  COMPACT_ZSET_COUNT_SIZE, COMPACT_ZSET_ENTRY_HEADER_SIZE, COMPACT_ZSET_EXPIRE_FLAG_SIZE,
  COMPACT_ZSET_EXPIRE_TIME_SIZE, COMPACT_ZSET_LEN_SIZE, COMPACT_ZSET_SCORE_SIZE, CompactZSet,
  CompactZSetCodec, CompactZSetIter, ZSetEntryRef,
};
pub use error::{Error, Result};
pub use meta::{
  COMPACT_META_VALUE_SIZE, CompactMetaValue, META_VALUE_SIZE, MetaValue, SUBKEY_HEADER_SIZE,
  SUBKEY_STACK_CAP, StorageEncoding, SubKeyBuf, SubKeyCodec, SubKeyRef,
};
pub use ns_codec::{
  CHUNK_ID_LEN, DecodedSubKey, KeyBufRepr, MAX_SESSION_PREFIX_LEN, MIN_CHUNK_KEY_LEN,
  MIN_SESSION_PREFIX_LEN, MIN_SUBKEY_LEN, NamespaceDbCodec, STACK_KEY_CAP, SUBKEY_META_HEADER_LEN,
  SessionPrefixBuf, TaggedKeyBuf, U64_BYTE_LEN,
};
pub use sample::{SAMPLE_STACK_CAP, sample_distinct_indices};
pub use tag::{GarnetObjectType, KeyTag};
pub use ttl::{TTL_VAL_LEN, TtlCodec};
