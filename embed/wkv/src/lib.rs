#![cfg_attr(docsrs, feature(doc_cfg))]

mod checkpoint;
pub mod compact;
mod config;
mod error;
pub mod gc;
mod range_index;
pub mod read_cache;
mod session;
mod store;
mod ttl;

pub use checkpoint::{
  BFTREE_SNAPSHOT_DIR, BFTREE_SNAPSHOT_FILE, CheckpointManager, recover_cpr_snapshots,
  recover_shared_bftree, take_cpr_snapshots, take_shared_bftree_snapshot,
};
pub use config::{
  DEFAULT_GC_COMPACTION_INTERVAL_MS, DEFAULT_GC_MAX_BATCH_DELETES, DEFAULT_GC_MAX_SEGMENTS,
  DEFAULT_GC_NUM_SEGMENTS, DEFAULT_GC_SCAN_INTERVAL_MS, DEFAULT_INDEX_SIZE, DEFAULT_MAX_SESSIONS,
  DEFAULT_MEMORY_PERCENT, GcConfig, INDEX_BUCKET_BYTES, INDEX_BUCKET_DATA_SLOTS,
  MAX_DEFAULT_MEMORY_BUDGET_BYTES, MAX_INDEX_SIZE, MIN_INDEX_SIZE, MIN_MEMORY_BUDGET_BYTES,
  StoreConfig,
};
pub use error::{Error, Result};
pub use gc::{GcHandle, GcManager, GcStatsSnapshot, RunGuard};
pub use range_index::RangeIndexError;
pub use read_cache::{
  READ_CACHE_BIT, ReadCache, absolute_address, is_read_cache_addr, tag_read_cache_addr,
};
pub use session::{
  BatchStoreSession, HASH_MAX_COMPACT_ENTRIES, HASH_MAX_COMPACT_VALUE, MAX_COMPACT_TOTAL_BYTES,
  RawCollectionRead, SET_MAX_COMPACT_ENTRIES, SET_MAX_COMPACT_VALUE, StoreSession,
  ZSET_MAX_COMPACT_ENTRIES, ZSET_MAX_COMPACT_MEMBER,
};
pub use store::{KEY_ID_ASSIGN_MARGIN, RangeIndexListenerFn, WedbStore, WriteListenerFn};
pub use ttl::{TTL_VALUE_LEN, TtlOpt, TtlProbe};
pub use wbftree::{
  BfTreeService, RANGE_INDEX_STUB_SIZE, RangeIndexChunkedDeserializer, RangeIndexChunkedSerializer,
  RangeIndexFileEntry, RangeIndexManager, RangeIndexMigrationReader, RangeIndexStub, ScanRecord,
  ScanReturnField, StorageBackend, StorageBackendType, TreeTuning, compute_checksum,
  compute_checksum_with_seed,
};
pub use wcompact::{CompactSession, CompactStore, CompactionStats, CompactionType, LogCompactor};
pub use wcpr::{CheckpointMeta, CheckpointType, CprRecover, CprStore, StoreMeta};
pub use wval::{StorageEncoding, TaggedKeyBuf};
