#![cfg_attr(docsrs, feature(doc_cfg))]

mod compact;
mod config;
mod error;
mod etag;
mod gc;
mod range_index;
mod read_cache;
mod ri;
mod session;
pub mod store;
mod ttl;
pub mod vdb;
pub use config::{
  DEFAULT_DB_GC_RECLAIM_DELAY_SECS, DEFAULT_GC_MAX_BATCH_DELETES, DEFAULT_GC_MAX_SEGMENTS,
  GcConfig, INDEX_BUCKET_BYTES, INDEX_BUCKET_DATA_SLOTS, MAX_INDEX_SIZE, MIN_ADAPTIVE_BUDGET_BYTES,
  MIN_INDEX_SIZE, StoreConfig,
};
pub use error::{CollectionError, Error, Result};
/// 换号物理回收常驻驱动（实例产生点必带的挂载口：恢复臂与副本换入收口用）
pub use gc::{GcManager, spawn_bftree_reclaimer};
pub use range_index::{RangeIndexError, SwapInWindowGuard, TreeGuard, validate_bftree_record};
pub use read_cache::{RcVisit, ReadCache};
/// set_context 冷检窗口测试留钩（仅集成测试基础设施，见 session 模块文档）
#[doc(hidden)]
pub use session::TEST_COLD_WINDOW_HOOK;
pub use session::{
  BatchStoreSession, ConsistentReadContext, ConsistentReadFunctions, DeleteMissHook,
  EngineHookSlots, KeyRef, RMW_PLAN_ACQUIRE_MISS, RMW_PLAN_BUILD_COUNT, RMW_PLAN_PINNED_INDEX,
  RecordRead, RmwGrow, RmwWindow, SessionLocking, StoreResult, StoreSession, WatchHook,
  consistent_read::record_key_hash,
};
pub use store::{
  HybridLogScanMetrics, KEY_ID_ASSIGN_MARGIN, ObjectRmwNotification, SerialLockGuard, StoreEvent,
  StoreEventSink, TieredCollectionNotification, WedbStore,
};
pub use ttl::{TtlGate, TtlOpt, is_expired, is_expired_or_now};
/// DbMeta 系统记录单点编解码布局（AOF 镜像条目的记录复原口径，wnode 回放面用）
pub use vdb::DbMetaRecord;
/// 紧缩与检查点参数型单门面 re-export（公开签名的参数型，bench 等宿主经 wkv
/// 取用免直挂底层 crate，对标 wkv/tests/compact/basic.rs:10-11 的同口径依赖）
pub use wcompact::CompactionType;
pub use wcpr::CheckpointType;
pub use whlog::{VERSION_MASK, VERSION_SHIFT_OPEN_BIT};
