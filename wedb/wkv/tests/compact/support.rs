//! Compact 紧缩测试公共辅助方法与 Fixture

use std::{
  sync::{
    Arc,
    atomic::{AtomicI64, Ordering},
  },
  thread::yield_now,
};

use aok::Void;
use tempfile::{TempDir, tempdir};
use wdev::SegmentedDevice;
use windex::{HashIndex, SPLIT_UNSTARTED, chunk_count};
use wkv::{StoreConfig, StoreSession, WedbStore, store::ResizePhase};

const DEFAULT_NUM_BUCKETS: usize = 4096;
const DEFAULT_PAGE_SIZE: usize = 64 * 1024;
const DEFAULT_NUM_PAGES: usize = 16;
const DEFAULT_MUTABLE_FRACTION: f64 = 0.5;

/// 测试库 RangeIndex 旁表子目录名（挂于调用方临时目录内，与 tests/support
/// 主套件支撑层目录组织一致）
const RANGE_INDEXES: &str = "range_indexes";

/// 挂载 RangeIndex 旁表子目录的测试配置（旁表目录落在调用方持有的临时
/// 目录内随其保活；紧缩面用例的 GC 关闭由调用方对返回值置位）
pub fn range_index_config(
  dir: &TempDir,
  index_size: usize,
  page_size: usize,
) -> aok::Result<StoreConfig> {
  Ok(
    StoreConfig::new(
      index_size,
      page_size,
      DEFAULT_NUM_PAGES,
      DEFAULT_MUTABLE_FRACTION,
    )?
    .with_range_index_dir(dir.path().join(RANGE_INDEXES)),
  )
}

/// 在指定临时目录中打开存储实例（目录与数据文件路径由调用方持有，便于
/// 检查点目录挂载与崩溃后重开同一路径）
pub fn open_store_in(
  dir: &TempDir,
  name: &str,
  config: StoreConfig,
) -> aok::Result<Arc<WedbStore<SegmentedDevice>>> {
  let device = Arc::new(SegmentedDevice::single_file(dir.path().join(name))?);
  Ok(Arc::new(WedbStore::open(config, device)?))
}

/// 创建默认测试存储引擎实例（单页 64KB，16 页环形缓冲，可变区比例 0.5）
pub fn create_test_store(
  db_name: &str,
) -> aok::Result<(
  TempDir,
  Arc<WedbStore<SegmentedDevice>>,
  StoreSession<SegmentedDevice>,
)> {
  create_custom_store(
    db_name,
    DEFAULT_NUM_BUCKETS,
    DEFAULT_PAGE_SIZE,
    DEFAULT_NUM_PAGES,
    DEFAULT_MUTABLE_FRACTION,
  )
}

/// 创建自定义参数存储引擎实例
pub fn create_custom_store(
  db_name: &str,
  num_buckets: usize,
  page_size: usize,
  num_pages: usize,
  mutable_fraction: f64,
) -> aok::Result<(
  TempDir,
  Arc<WedbStore<SegmentedDevice>>,
  StoreSession<SegmentedDevice>,
)> {
  let dir = tempdir()?;
  let db_path = dir.path().join(db_name);
  let device = Arc::new(SegmentedDevice::single_file(&db_path)?);
  let config = StoreConfig::new(num_buckets, page_size, num_pages, mutable_fraction)?;
  let store = Arc::new(WedbStore::open(config, device)?);
  let session = store.new_session()?;
  Ok((dir, store, session))
}

/// 创建启用 ReadCache 的存储引擎实例
pub fn create_read_cache_store(
  db_name: &str,
  num_buckets: usize,
  page_size: usize,
  num_pages: usize,
) -> aok::Result<(
  TempDir,
  Arc<WedbStore<SegmentedDevice>>,
  StoreSession<SegmentedDevice>,
)> {
  let dir = tempdir()?;
  let db_path = dir.path().join(db_name);
  let device = Arc::new(SegmentedDevice::single_file(&db_path)?);
  let config = StoreConfig::new(num_buckets, page_size, num_pages, DEFAULT_MUTABLE_FRACTION)?
    .with_read_cache(true);
  let store = Arc::new(WedbStore::open(config, device)?);
  let session = store.new_session()?;
  Ok((dir, store, session))
}

/// 批量回读校验辅助函数：验证区间记录与删除断言（对标 libs/storage/Tsavorite/cs/test/test.hlog/SpanByteLogCompactionTests.cs:VerifyRead）
pub async fn verify_records<F>(
  session: &StoreSession<SegmentedDevice>,
  prefix: &str,
  total: usize,
  mut is_deleted: F,
) -> Void
where
  F: FnMut(usize) -> bool,
{
  use std::fmt::Write;

  let mut k = String::with_capacity(prefix.len() + 8);
  let mut expected = String::with_capacity(prefix.len() + 16);

  for i in 0..total {
    k.clear();
    let _ = write!(&mut k, "{prefix}:{i:05}");
    let val = session.read(k.as_bytes()).await?;
    if is_deleted(i) {
      assert!(val.is_none(), "已删除或淘汰记录必须返回 None: {k}");
    } else {
      expected.clear();
      let _ = write!(&mut expected, "{prefix}:{i:05}:payload");
      assert_eq!(
        val.as_deref(),
        Some(expected.as_bytes()),
        "存活记录读取不一致: {k}"
      );
    }
  }
  aok::OK
}

// ===================== grow 迁移窗装配（紧缩面回归专用） =====================

/// 装配确定性 IN_PROGRESS_GROW 迁移窗（与 `tests/support` 的 stage_resize
/// 同源形态：2 倍容量新表在场、全部分块 SPLIT_UNSTARTED、条目仅存旧表——
/// 紧缩探针未过协同门即对新表采得空候选）
pub fn stage_grow_window(store: &WedbStore<SegmentedDevice>) {
  let old_index = store.active_index();
  let count = chunk_count(old_index.size);
  store.resize.split_status.store(Arc::new(
    (0..count)
      .map(|_| AtomicI64::new(SPLIT_UNSTARTED))
      .collect(),
  ));
  store
    .resize
    .num_pending_chunks
    .store(count, Ordering::Release);
  store.resize.old_index.store(Some(Arc::clone(&old_index)));
  store
    .index
    .store(Arc::new(HashIndex::new(old_index.size * 2).unwrap()));
  store
    .resize
    .phase
    .store(ResizePhase::InProgressGrow as u8, Ordering::Release);
}

/// 确定性收窗（严格复刻 grow_index 步 4/5 合法完成序：全量分块迁移至 pending
/// 归零后注销迁移源、翻回 Rest）
pub fn finish_grow_window(store: &WedbStore<SegmentedDevice>) {
  let old_index = store
    .resize
    .old_index
    .load_full()
    .expect("装配窗口必有迁移源");
  let count = chunk_count(old_index.size);
  for i in 0..count {
    store.split_single_chunk(i, count, &old_index).unwrap();
  }
  while store.resize.num_pending_chunks.load(Ordering::Acquire) > 0 {
    yield_now();
  }
  store.resize.old_index.store(None);
  store.resize.split_status.store(Arc::new(Vec::new()));
  store
    .resize
    .phase
    .store(ResizePhase::Rest as u8, Ordering::Release);
}
