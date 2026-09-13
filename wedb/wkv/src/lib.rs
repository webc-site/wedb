#![cfg_attr(docsrs, feature(doc_cfg))]

mod checkpoint;
mod compact;
mod config;
mod error;
mod gc;
mod range_index;
mod read_cache;
mod session;
mod store;
mod ttl;

pub use checkpoint::{
  CheckpointManager, VersionShiftFn, recover_cpr_snapshots, take_cpr_snapshots,
};
pub use config::{
  DEFAULT_GC_COMPACTION_INTERVAL_MS, DEFAULT_GC_MAX_BATCH_DELETES, DEFAULT_GC_MAX_SEGMENTS,
  DEFAULT_GC_NUM_SEGMENTS, DEFAULT_GC_SCAN_INTERVAL_MS, DEFAULT_INDEX_SIZE, DEFAULT_MAX_SESSIONS,
  DEFAULT_MEMORY_PERCENT, GcConfig, INDEX_BUCKET_BYTES, INDEX_BUCKET_DATA_SLOTS,
  MAX_DEFAULT_MEMORY_BUDGET_BYTES, MAX_INDEX_SIZE, MIN_ADAPTIVE_BUDGET_BYTES, MIN_INDEX_SIZE,
  MIN_MEMORY_BUDGET_BYTES, StoreConfig,
};
pub use error::{Error, Result};
pub use gc::{GcHandle, GcManager, GcStatsSnapshot, RunGuard};
pub use range_index::{RangeIndexError, RangeIndexMetrics, TreeReadGuard};
pub use read_cache::{ReadCache, is_read_cache_addr};
pub use session::{BatchStoreSession, ConsistentReadContext, ConsistentReadFunctions, StoreSession};
pub use store::{
  DefaultWedbStore, KEY_ID_ASSIGN_MARGIN, ObjectRmwListenerFn, ObjectRmwNotification,
  RangeIndexCreateListenerFn, RangeIndexDropListenerFn, RangeIndexListenerFn, TtlPurgeListenerFn,
  TtlWriteListenerFn, WedbStore, WriteListenerFn,
};
pub use ttl::{TTL_VALUE_LEN, TtlOpt, TtlProbe};
