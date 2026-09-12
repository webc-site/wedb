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
pub use session::{
  BatchStoreSession, ConsistentReadContext, ConsistentReadFunctions, HASH_DOWNGRADE_BYTE_THRESHOLD,
  HASH_DOWNGRADE_ITEM_THRESHOLD, HASH_MAX_COMPACT_ENTRIES, HASH_MAX_COMPACT_VALUE,
  HASH_UPGRADE_BYTE_THRESHOLD, HASH_UPGRADE_ITEM_THRESHOLD, MAX_COMPACT_TOTAL_BYTES,
  RawCollectionRead, SET_MAX_COMPACT_ENTRIES, SET_MAX_COMPACT_VALUE, StoreSession,
  ZSET_MAX_COMPACT_ENTRIES, ZSET_MAX_COMPACT_MEMBER, should_downgrade_hash, should_upgrade_hash,
};
pub use store::{
  DefaultWedbStore, KEY_ID_ASSIGN_MARGIN, ObjectRmwListenerFn, ObjectRmwNotification,
  RangeIndexCreateListenerFn, RangeIndexDropListenerFn, RangeIndexListenerFn, TtlPurgeListenerFn,
  WedbStore, WriteListenerFn,
};
pub use ttl::{TTL_VALUE_LEN, TtlOpt, TtlProbe};
pub use wbftree::{
  BfTreeService, RANGE_INDEX_STUB_SIZE, RangeIndexChunkedDeserializer, RangeIndexChunkedSerializer,
  RangeIndexFileEntry, RangeIndexManager, RangeIndexMigrationReader, RangeIndexStub, ScanRecord,
  ScanReturnField, StorageBackend, StorageBackendType, TreeTuning, compute_checksum,
  compute_checksum_with_seed,
};
pub use wcompact::{CompactSession, CompactStore, CompactionStats, CompactionType, LogCompactor};
pub use wcpr::{CheckpointMeta, CheckpointType, CprRecover, CprStore, StoreMeta};
pub use wval::{KeyTag, NamespaceDbCodec, StorageEncoding, TaggedKeyBuf};
