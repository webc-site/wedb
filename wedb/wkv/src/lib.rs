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
pub use compact::WedbCompactionFunctions;
pub use config::{
  DEFAULT_GC_MAX_BATCH_DELETES, DEFAULT_GC_MAX_SEGMENTS, DEFAULT_INDEX_SIZE, DEFAULT_MAX_SESSIONS,
  DEFAULT_MEMORY_PERCENT, DEFAULT_REVIVIFIABLE_FRACTION, GcConfig, INDEX_BUCKET_BYTES,
  INDEX_BUCKET_DATA_SLOTS, MAX_DEFAULT_MEMORY_BUDGET_BYTES, MAX_INDEX_SIZE,
  MIN_ADAPTIVE_BUDGET_BYTES, MIN_INDEX_SIZE, MIN_MEMORY_BUDGET_BYTES, StoreConfig,
};
pub use error::{CollectionError, CollectionResult, Error, Result};
pub use gc::{GcHandle, GcManager, GcStatsSnapshot};
pub use range_index::{
  RangeIndexError, RangeIndexMetrics, TreeGuard, TreeReadGuard, TreeWriteGuard,
  encode_meta_stub_record, validate_bftree_record,
};
pub use read_cache::{RcVisit, ReadCache};
pub use session::{
  BatchStoreSession, ConsistentReadContext, ConsistentReadFunctions, DeleteMissHook, RecordRead,
  StoreResult, StoreSession, WatchHook,
};
pub use store::{
  DefaultWedbStore, HybridLogScanMetrics, KEY_ID_ASSIGN_MARGIN, ObjectRmwNotification, StoreEvent,
  StoreEventSink, WedbStore,
};
/// DbMeta 系统记录单点编解码布局（AOF 镜像条目的记录复原口径，wnode 回放面用）
pub use vdb::DbMetaRecord;
pub use ttl::{TtlCarrier, TtlGate, TtlOpt, is_expired, is_expired_or_now};
/// 批量读预取窗口：单点定义在 windex 索引层，此处仅原样透出路径供引擎上层分块对齐
/// （对标 C# `ContextReadWithPrefetch` 的 `PrefetchSize = 12`）
pub use windex::PREFETCH_WINDOW;
