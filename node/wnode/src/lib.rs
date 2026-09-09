#![cfg_attr(docsrs, feature(doc_cfg))]
#![doc = include_str!("../README.md")]

pub mod aof;
pub mod resp;
pub mod service;

pub use aof::{AofEntryRef, AofOp, AofResult, Error as AofError, Replay, encode_entry};
pub use service::{Error as NodeError, NodeService, Result as NodeResult, SharedStore};
/// 下游所需的引擎类型出口：调用 NodeService 公共签名所需的 wkv/waof/wdev
/// 类型统一经本包透传，下游（含集群层）无需直接依赖 embed 引擎 crate
pub use waof::{Error as WalError, WalConfig, WalLog, WalRecord, WalScanIterator};
pub use wdev::Device;
pub use wkv::{StorageBackend, StoreSession, TreeTuning, WedbStore};
