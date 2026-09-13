#![cfg_attr(docsrs, feature(doc_cfg))]

pub mod error;
pub mod geo;
pub mod hash;
pub mod itembroker;
pub mod list;
pub mod object_store_utils;
pub mod parse_utils;
pub mod prefix;
pub mod resp;
pub mod ri;
pub mod set;
pub mod types;
pub mod zset;

pub use error::{Error, Result};
pub use geo::{GeoAddOptions, GeoDistanceUnitType, GeoHash, GeoOrder, GeoOriginType};
pub use hash::hash_object::{HashObject, HashOperation};
pub use itembroker::{
  CollectionItemBroker, CollectionItemBrokerEvent, CollectionItemBrokerEventType,
  CollectionItemObserver, CollectionItemResult, CollectionItemStore, CompioTaskSpawner,
  ItemBrokerFinisher, ObserverStatus, SharedItemBroker, TaskSpawner, TryGetOutcome,
};
pub use list::{
  LIST_STUB_SIZE, ListStub, ListTree, ListTreeOps, OperationDirection, i64_from_list_key,
  i64_from_order_idx, list_key_from_i64,
  list_object::{ListObject, ListOperation},
  normalize_range, order_idx_from_i64,
};
pub use prefix::{STACK_KEY_BUF_SIZE, TreePrefix, with_prefixed_key, with_prefixed_key2};
pub use resp::{
  ObjectInput, ObjectOutput, ObjectOutputFlags, RespInputFlags, RespInputHeader, ScanInput,
};
pub use ri::RiTreeOps;
pub use set::{
  SET_VAL_PLACEHOLDER, SetTreeOps,
  set_object::{SetObject, SetOperation},
};
pub use zset as sortedset;
pub use zset::{
  PREFIX_MEMBER, PREFIX_SCORE, SortedSetComparer, ZRangeByScoreOpt, ZSetTreeOps,
  decode_order_score, encode_order_score,
  sorted_set_object::{SortedSetObject, SortedSetOperation},
};
pub mod sortedsetgeo {
  pub use crate::geo::*;
  pub mod sorted_set_geo_object_impl {
    pub use crate::zset::geo_impl::*;
  }
}
