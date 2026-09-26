#![cfg_attr(docsrs, feature(doc_cfg))]

//! Redis 值层编解码：命名空间物理键、集合元数据与紧凑容器
//!
//! 定位为领域编码层：zset/set/hash 紧凑编解码、命名空间标签键编解码、
//! 集合元数据与无重复抽样。纯值域编解码，不感知任何引擎层类型
//! （对标 Garnet 中 Tsavorite core 与 libs/server 值对象层的分层约束）。

mod codec;
mod error;
mod etag;
mod meta;
mod ns_codec;
mod tag;

pub use codec::{I64_VAL_LEN, I64Codec};
pub use error::{Error, Result};
pub use etag::NO_ETAG;
pub use meta::{META_VALUE_SIZE, MetaValue, StorageEncoding};
pub use ns_codec::{NamespaceDbCodec, STACK_KEY_CAP, SessionPrefixBuf, TaggedKeyBuf};
pub use tag::{
  CUSTOM_OBJECT_TYPE_BASE, CustomObjectType, GarnetObjectType, KeyTag, LAST_RESERVED_BUILTIN_TYPE,
  VectorRegistrySubTag,
};
