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
/// 换号物理回收常驻驱动（实例产生点必带的挂载口：恢复臂与副本换入收口用）
pub use gc::{GcHandle, GcManager, GcStatsSnapshot, spawn_bftree_reclaimer};
pub use range_index::{
  RangeIndexError, RangeIndexMetrics, SwapInWindowGuard, TreeGuard, TreeReadGuard, TreeWriteGuard,
  validate_bftree_record,
};
pub use read_cache::{RcVisit, ReadCache};
/// set_context 冷检窗口测试留钩（仅集成测试基础设施，见 session 模块文档）
#[doc(hidden)]
pub use session::TEST_COLD_WINDOW_HOOK;
pub use session::{
  BatchStoreSession, ConsistentReadContext, ConsistentReadFunctions, DeleteMissHook, RecordRead,
  RmwGrow, RmwWindow, SessionLocking, SessionLockingGuard, StoreResult, StoreSession, WatchHook,
};
/// 全量活跃域枚举条目（跨域扫描原语的产出形态，wedb 复制/迁移扇出面用）
pub use store::vdb_load::ActiveDomain;
pub use store::{
  DefaultWedbStore, HybridLogScanMetrics, MetricEntry, ScanRegion, ScanState, KEY_ID_ASSIGN_MARGIN,
  ObjectRmwNotification, StoreEvent, StoreEventSink, TieredCollectionNotification, WedbStore,
};
pub use ttl::{TtlCarrier, TtlGate, TtlOpt, is_expired, is_expired_or_now};
/// DbMeta 系统记录单点编解码布局（AOF 镜像条目的记录复原口径，wnode 回放面用）
pub use vdb::DbMetaRecord;
