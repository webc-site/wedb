//! 升阶树页缓存总闸 wkv 装配面测试（task/ing/wbftree-tree-cache-global-budget.md）
//!
//! 验证 StoreConfig.tree_cache_budget_bytes 经装配注入后的会话层语义：
//! RI.CREATE 超预算显式拒绝（wbftree CacheBudgetExhausted 透明透传）、
//! 索引删除后预算归还、cache_reserved 记账与在线树环容量和一致。
//!
//! 自研依据: wbftree 页缓存预算（C# 无对应机制，BfTree 页级缓存为本仓分层架构组件，见 doc/zh/collection.md）

use std::sync::Arc;

use aok::{OK, Result, Void};
use tempfile::tempdir;
use wbftree::{Error, StorageBackendType, TreeTuning};
use wdev::SegmentedDevice;
use wkv::{RangeIndexError, StoreConfig};

use crate::support::{open_store_in, tree_id_key};

/// 与 C# 测试一致的树调参，页环压至 64KiB 便于预算封顶
const TUNE: TreeTuning = TreeTuning {
  cache_size: 64 * 1024,
  min_record_size: 8,
  max_record_size: 1024,
  max_key_len: 128,
  leaf_page_size: 0,
};

fn open_store(
  dir: &tempfile::TempDir,
  name: &str,
  budget: usize,
) -> Result<Arc<wkv::WedbStore<SegmentedDevice>>> {
  let config = StoreConfig::new(1024, 64 * 1024, 16, 0.5)?
    .with_range_index_dir(dir.path().join("range_indexes"))
    .with_tree_cache_budget(budget);
  open_store_in(dir, name, config)
}

#[compio::test]
async fn test_ri_create_budget_exhausted_and_release() -> Void {
  let dir = tempdir()?;
  let store = open_store(&dir, "ri_budget.db", 64 * 1024 * 2)?;

  // 1. 预算内创建两棵（2 × 64KiB 恰满）
  let session = store.new_session()?;
  session
    .range_index_create(b"idx_a", StorageBackendType::Disk, TUNE)
    .await?;
  session
    .range_index_create(b"idx_b", StorageBackendType::Disk, TUNE)
    .await?;
  assert_eq!(
    store.range_index().cache_reserved(),
    2 * TUNE.cache_size,
    "记账与在线树环容量和一致"
  );
  // INFO STORE 披露口径锁：store_snapshot 两行直读总闸，reserved 与在线树
  // 环容量和一致、budget 为装配定额（wconf tree-cache-budget 旋钮注入源）
  let snap = store.store_snapshot();
  assert_eq!(snap.tree_cache_reserved_bytes as usize, 2 * TUNE.cache_size);
  assert_eq!(
    snap.tree_cache_budget_bytes as usize,
    64 * 1024 * 2,
    "披露定额必须等于装配注入值"
  );

  // 2. 超预算第 3 棵：显式拒绝（wbftree CacheBudgetExhausted 透明透传），记账不变
  let err = session
    .range_index_create(b"idx_c", StorageBackendType::Disk, TUNE)
    .await;
  assert!(
    matches!(
      err,
      Err(RangeIndexError::Wbftree(Error::CacheBudgetExhausted))
    ),
    "超预算 RI.CREATE 必须回 CacheBudgetExhausted"
  );
  assert_eq!(store.range_index().cache_reserved(), 2 * TUNE.cache_size);

  // 3. 摘除一棵：预算归还，后续创建可继续执行（验收指标 2）
  assert!(
    store
      .range_index()
      .dispose_tree_under_lock(&tree_id_key(0, 0, b"idx_a"), false)?
  );
  assert_eq!(store.range_index().cache_reserved(), TUNE.cache_size);
  session
    .range_index_create(b"idx_c", StorageBackendType::Disk, TUNE)
    .await?;
  assert_eq!(
    store.range_index().cache_reserved(),
    2 * TUNE.cache_size,
    "归还后新树登记记账恢复"
  );
  OK
}
