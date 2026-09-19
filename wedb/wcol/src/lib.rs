#![cfg_attr(docsrs, feature(doc_cfg))]

pub mod geo;
pub mod hash;
pub mod itembroker;
pub mod list;
pub mod object_payload;
pub mod parse_utils;
pub mod resp;
pub mod set;
pub mod types;
pub mod zset;

pub use geo::{GeoAddOptions, GeoDistanceUnitType, GeoHash, GeoOrder, GeoOriginType};
pub use hash::hash_object::{HashObject, HashOperation};
pub use itembroker::{
  CollectionItemBroker, CollectionItemBrokerEvent, CollectionItemBrokerEventType,
  CollectionItemObserver, CollectionItemResult, CollectionItemStore, CompioTaskSpawner,
  ItemBrokerFinisher, ObserverStatus, SharedItemBroker, TaskSpawner, TryGetOutcome,
};
pub use list::list_object::{ListObject, ListOperation, OperationDirection};
pub use object_payload::ObjLoad;
pub use resp::{ObjectOutput, ObjectOutputFlags, RespInputFlags};
pub use set::set_object::{SetObject, SetOperation};
pub use types::IGarnetObject;
pub use zset::sorted_set_object::{SortedSetObject, SortedSetOperation};

/// Set 集合底层或树化存储成员记录的统一哑值（4 字节 b"1111"）
pub const SET_MEMBER_DUMMY_VALUE: &[u8] = b"1111";

/// 集合自适应分层：自动升阶为 BfTree 独立分层树的条目数高水位阈值（65,536 条目）
pub const TIERED_PROMOTE_THRESHOLD: usize = 65_536;

/// 集合自适应分层：满足降阶回退为内存信封的条目数低水位阈值（32,768 条目）
pub const TIERED_DEMOTE_THRESHOLD: usize = 32_768;

/// 集合自适应分层：自动升阶为 BfTree 独立分层树的内存字节高水位阈值（4MB）
///
/// 体积维吃 [`types::GarnetObject::heap_memory_size`]，按其 rust 自定记账口径标定
/// （口径单点见 [`wbase::heap`]，非 .NET GC 绝对值，刻意差异）
pub const TIERED_PROMOTE_BYTES: usize = 4 * 1024 * 1024;

/// 集合自适应分层：满足降阶回退为内存信封的内存字节低水位阈值（2MB）
///
/// 同 [`TIERED_PROMOTE_BYTES`]，按 [`wbase::heap`] 的 rust 记账口径标定
pub const TIERED_DEMOTE_BYTES: usize = 2 * 1024 * 1024;

/// 判定是否满足升阶触发条件（双维度 OR 逻辑：条目数 >= 65536 或 内存体积 >= 4MB）
#[inline]
pub const fn should_promote(count: usize, heap_bytes: usize) -> bool {
  count >= TIERED_PROMOTE_THRESHOLD || heap_bytes >= TIERED_PROMOTE_BYTES
}

/// 判定是否满足降阶触发条件（双维度 AND 逻辑：条目数 <= 32768 且 内存体积 <= 2MB）
#[inline]
pub const fn should_demote(count: usize, heap_bytes: usize) -> bool {
  count <= TIERED_DEMOTE_THRESHOLD && heap_bytes <= TIERED_DEMOTE_BYTES
}
