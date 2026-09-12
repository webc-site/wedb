#![cfg_attr(docsrs, feature(doc_cfg))]

pub mod error;
pub mod hash;
pub mod list;
pub mod prefix;
pub mod ri;
pub mod set;
pub mod zset;

pub use error::{CollectionError, Error, Result};
pub use hash::{HashTreeOps, TAG_EMPTY, TAG_NON_EMPTY, TAG_PADDED};
pub use list::{
  LIST_STUB_SIZE, ListStub, ListTree, ListTreeOps, i64_from_list_key, i64_from_order_idx,
  list_key_from_i64, normalize_range, order_idx_from_i64,
};
pub use prefix::{STACK_KEY_BUF_SIZE, TreePrefix, with_prefixed_key, with_prefixed_key2};
pub use ri::RiTreeOps;
pub use set::{SET_VAL_PLACEHOLDER, SetTreeOps};
pub use zset::{
  PREFIX_MEMBER, PREFIX_SCORE, ZRangeByScoreOpt, ZSetTreeOps, decode_order_score,
  encode_order_score,
};
