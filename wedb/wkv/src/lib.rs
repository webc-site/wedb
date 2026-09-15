#![cfg_attr(docsrs, feature(doc_cfg))]

mod checkpoint;
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

pub use checkpoint::CheckpointManager;
pub use config::{
  DEFAULT_GC_COMPACTION_INTERVAL_MS, DEFAULT_GC_MAX_BATCH_DELETES, DEFAULT_GC_MAX_SEGMENTS,
  DEFAULT_GC_NUM_SEGMENTS, DEFAULT_GC_SCAN_INTERVAL_MS, DEFAULT_INDEX_SIZE, DEFAULT_MAX_SESSIONS,
  DEFAULT_MEMORY_PERCENT, GcConfig, INDEX_BUCKET_BYTES, INDEX_BUCKET_DATA_SLOTS,
  MAX_DEFAULT_MEMORY_BUDGET_BYTES, MAX_INDEX_SIZE, MIN_ADAPTIVE_BUDGET_BYTES, MIN_INDEX_SIZE,
  MIN_MEMORY_BUDGET_BYTES, StoreConfig,
};
pub use error::{CollectionError, CollectionResult, Error, Result};
pub use etag::ETAG_VALUE_LEN;
pub use gc::{GcHandle, GcManager, GcStatsSnapshot};
pub use range_index::{RangeIndexError, RangeIndexMetrics, TreeReadGuard};
pub use read_cache::ReadCache;
pub use session::{
  BatchStoreSession, ConsistentReadContext, ConsistentReadFunctions, RecordRead, StoreSession,
};
pub use store::{
  DefaultWedbStore, KEY_ID_ASSIGN_MARGIN, ObjectRmwNotification, StoreEvent, StoreEventSink,
  WedbStore,
};
pub use ttl::{TTL_VALUE_LEN, TtlOpt, TtlProbe};
