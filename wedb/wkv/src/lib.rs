#![recursion_limit = "256"]
#![cfg_attr(docsrs, feature(doc_cfg))]

mod compact;
#[cfg(debug_assertions)]
pub use compact::{
  HOST_EXISTS_ERR_INJECT, ON_DROPPED_PAUSE_INJECT, ON_DROPPED_PAUSED, ON_DROPPED_RESUME,
};
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
#[doc(hidden)]
pub use gc::{MIN_SCAN_INTERVAL_MS, enabled_by_config, scan_interval_ms};
/// RI.CREATE 建树窗停车注入钩子族（仅 debug 测试装配消费，见 range_index::ops
/// 建树闭包停车注入注）
#[cfg(debug_assertions)]
pub use range_index::{
  RI_CREATE_WINDOW_PAUSE_INJECT, RI_CREATE_WINDOW_PAUSED, RI_CREATE_WINDOW_RESUME,
};
pub use range_index::{
  RangeIndexError, SwapInWindowGuard, TreeGuard, range_index_blocking, validate_bftree_record,
};
/// 分层存根装载停车注入钩子族（仅 debug 测试装配消费，见 range_index::stub）
#[cfg(debug_assertions)]
pub use range_index::{
  STUB_LOAD_PAUSE_INJECT, STUB_LOAD_PAUSED, STUB_LOAD_RESUME, STUB_WIN_LOAD_PAUSE_INJECT,
  STUB_WIN_LOAD_PAUSED, STUB_WIN_LOAD_RESUME,
};
#[doc(hidden)]
pub use range_index::{
  STUB_WINDOW_END, STUB_WINDOW_START, clear_flushed_patch, drain_guard_ok, encode_meta_stub_record,
  mark_recovered_patch, patch_stub_record, range_index_stub_of, recreate_patch, transfer_out_patch,
};
/// 建链/换代窗测试留钩（仅 debug 测试装配消费，见 range_index 模块文档）
#[cfg(debug_assertions)]
pub use range_index::{TEST_DOMAIN_PIN_HOOK, TEST_TRANSFER_OUT_HOOK};
#[doc(hidden)]
pub use read_cache::INFLIGHT_CLOSED;
pub use read_cache::{RcVisit, ReadCache};
#[cfg(debug_assertions)]
#[doc(hidden)]
pub use session::TEST_COLD_WINDOW_HOOK;
pub use session::{
  BatchStoreSession, ConsistentReadContext, ConsistentReadFunctions, DeleteMissHook,
  EngineHookSlots, RMW_KEY_LATCH_ATTEMPTS, RMW_PLAN_ACQUIRE_MISS, RMW_PLAN_BUILD_COUNT,
  RMW_PLAN_PINNED_INDEX, RecordRead, RmwGrow, RmwWindow, SessionLocking, StoreResult, StoreSession,
  WatchHook,
};
/// set_context 冷检窗口测试留钩（仅集成测试基础设施，见 session 模块文档）
#[doc(hidden)]
pub use session::{CONTEXT_TERM_MASK, matches_vector_context};
pub use store::{
  HybridLogScanMetrics, KEY_ID_ASSIGN_MARGIN, ObjectRmwNotification, ScanRegion, ScanState,
  SerialLock, SerialLockGuard, StoreEvent, StoreEventSink, TieredCollectionNotification, WedbStore,
};
pub use ttl::{PurgeNotifyGuard, TtlGate, TtlOpt, is_expired, is_expired_or_now};
/// DbMeta 系统记录单点编解码布局（AOF 镜像条目的记录复原口径，wnode 回放面用）
pub use vdb::DbMetaRecord;
/// 紧缩与检查点参数型单门面 re-export（公开签名的参数型，bench 等宿主经 wkv
/// 取用免直挂底层 crate，对标 wkv/tests/compact/basic.rs:10-11 的同口径依赖）
pub use wcompact::CompactionType;
pub use wcpr::CheckpointType;
pub use whlog::VERSION_MASK;
