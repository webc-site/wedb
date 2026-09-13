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
pub use list::list_object::{ListObject, ListOperation, OperationDirection};
pub use prefix::{STACK_KEY_BUF_SIZE, TreePrefix, with_prefixed_key, with_prefixed_key2};
pub use resp::{
  ObjectInput, ObjectOutput, ObjectOutputFlags, RespInputFlags, RespInputHeader, ScanInput,
};
pub use ri::RiTreeOps;
pub use set::set_object::{SetObject, SetOperation};
pub use zset as sortedset;
pub use zset::sorted_set_object::{SortedSetObject, SortedSetOperation};
pub mod sortedsetgeo {
  pub use crate::geo::*;
  pub mod sorted_set_geo_object_impl {
    pub use crate::zset::geo_impl::*;
  }
}
