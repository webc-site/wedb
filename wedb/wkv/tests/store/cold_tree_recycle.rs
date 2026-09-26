//! 冷树缓存耗尽自愈路径锁测（task/ing/wbftree-cold-tree-cache-no-evict-recycle.md）
//!
//! 对标 C#：libs/server/Storage/Functions/GarnetRecordTriggers.cs:OnEvict →
//! libs/server/Resp/RangeIndex/RangeIndexManager.Index.cs:DisposeTreeUnderLock
//! (deleteFiles:false)——页驱逐联动释放冷 RI 树常驻页环、访问时 RestoreTree
//! (rust get_or_open_tree) 懒重开。rust 侧驱逐链无记录级钩子（既定改良），回收
//! 由 wkv 常驻轮询内核 [`wkv::WedbStore::recycle_cold_bftrees`] 承接（判据：
//! 在册 ∧ Meta 存根越 head ∧ 冷满迟滞窗），本文件按票面验证点锁测：
//! 1. 预算耗尽且无可回收时显式拒绝（r15-perf 既定行为不变）；
//! 2. 热键（存根在内存）跨轮不释放；存根刚逐出（迟滞窗内）不释放——防抖；
//! 3. 冷满窗口后回收：cache_reserved 回落、数据文件保留、新树创建成功（自愈）；
//! 4. 冷键再读走 get_or_open_tree 懒重开，淘汰前已确认写入逐字段无损；
//!    再轮回收幂等零摘除。
//!
//! 自研依据: doc/zh/collection.md 分层树缓存预算与页驱逐联动回收

use std::{sync::Arc, time::Duration};

use aok::{OK, Void};
use compio::time::sleep;
use tempfile::tempdir;
use wbase::align::DEFAULT_SECTOR_SIZE;
use wbftree::{Error, StorageBackendType, TreeTuning};
use wkv::{RangeIndexError, StoreConfig};

use crate::support::{open_store_in, tree_id_key};

/// 页环 64KiB 小调参（与 tree_cache_budget 锁测同量级，便于预算封顶）
const TUNE: TreeTuning = TreeTuning {
  cache_size: 64 * 1024,
  min_record_size: 8,
  max_record_size: 1024,
  max_key_len: 128,
  leaf_page_size: 0,
};

/// 迟滞窗口（wkv gc/cold_tree.rs COLD_TREE_HYSTERESIS_TICKS = 1s）之上的等待余量
const HYSTERESIS_WAIT_MS: u64 = 1_200;

fn open_store(
  dir: &tempfile::TempDir,
  name: &str,
  budget: usize,
) -> aok::Result<Arc<wkv::WedbStore<wdev::SegmentedDevice>>> {
  let config = StoreConfig::new(1024, DEFAULT_SECTOR_SIZE, 16, 0.5)?
    .with_range_index_dir(dir.path().join("range_indexes"))
    .with_tree_cache_budget(budget);
  open_store_in(dir, name, config)
}

#[compio::test]
async fn test_cold_tree_recycle_self_heals_exhausted_budget() -> Void {
  let dir = tempdir()?;
  // 两环预算：默认 256MiB/16MiB=16 棵耗尽形态的等比例缩小
  let store = open_store(&dir, "cold_recycle.db", 2 * TUNE.cache_size)?;
  let session = store.new_session()?;

  session
    .range_index_create(b"cold_a", StorageBackendType::Disk, TUNE)
    .await?;
  session
    .range_index_create(b"cold_b", StorageBackendType::Disk, TUNE)
    .await?;
  session
    .range_index_set(b"cold_a", b"field", b"value")
    .await?;
  session
    .range_index_set(b"cold_a", b"field2", b"value2")
    .await?;
  let ring = store
    .range_index
    .get_tree(&tree_id_key(0, 0, b"cold_a"))
    .unwrap()
    .cache_bytes();
  assert_eq!(store.range_index.cache_reserved(), 2 * ring, "两树占满预算");

  // 验证点 1：预算耗尽且全部为热树（无可回收面）——拒绝臂同步回收一轮亦
  // 零摘除，显式拒绝行为保持（r15-perf 裁定不变量）
  let err = session
    .range_index_create(b"cold_c", StorageBackendType::Disk, TUNE)
    .await;
  assert!(
    matches!(
      err,
      Err(RangeIndexError::Wbftree(Error::CacheBudgetExhausted))
    ),
    "热树占满预算时新树必须显式拒绝，实得: {err:?}"
  );

  // 验证点 2a：热键（存根记录在内存窗）反复驱动回收轮也不释放
  assert_eq!(store.recycle_cold_bftrees(), 0);
  assert_eq!(store.range_index.cache_reserved(), 2 * ring);

  // 驱逐驱动：填充记录把存根页甩出内存窗（页池回绕同判据，对标 flush_evict 形态）
  let pad = vec![b'P'; 480];
  session.upsert(b"pad_key", &pad).await?;
  store.flush_all().await?;
  let addr_a = store
    .index
    .load()
    .find_tag(&tree_id_key(0, 0, b"cold_a"))
    .expect("cold_a 存根在册");
  let addr_b = store
    .index
    .load()
    .find_tag(&tree_id_key(0, 0, b"cold_b"))
    .expect("cold_b 存根在册");
  let evict_to = (addr_a.max(addr_b) / DEFAULT_SECTOR_SIZE as u64 + 1) * DEFAULT_SECTOR_SIZE as u64;
  store.shift_read_only_address(evict_to);
  store.shift_head_address(evict_to);
  assert!(
    store.hlog.is_on_disk(addr_a) && store.hlog.is_on_disk(addr_b),
    "存根页已逐出"
  );

  // 验证点 2b：刚逐出（迟滞窗内）不释放——防热键释放-重开抖动
  assert_eq!(store.recycle_cold_bftrees(), 0, "迟滞窗内不得摘除");
  assert_eq!(store.range_index.cache_reserved(), 2 * ring);

  // 验证点 3：冷满迟滞窗后回收——cache_reserved 回落、文件保留（deleteFiles:false）
  sleep(Duration::from_millis(HYSTERESIS_WAIT_MS)).await;
  assert_eq!(store.recycle_cold_bftrees(), 2, "两棵冷树一轮摘除");
  assert_eq!(
    store.range_index.cache_reserved(),
    0,
    "常驻页环预算全额归还"
  );
  assert!(
    store
      .range_index
      .get_tree(&tree_id_key(0, 0, b"cold_a"))
      .is_none()
  );
  assert!(
    store
      .range_index
      .data_file_path_for_key(&tree_id_key(0, 0, b"cold_a"))
      .exists(),
    "懒恢复形态数据文件必须保留"
  );

  // 预算腾出后新树创建成功（自愈闭环；拒绝臂无需再触发）
  session
    .range_index_create(b"cold_c", StorageBackendType::Disk, TUNE)
    .await?;
  assert_eq!(store.range_index.cache_reserved(), ring);

  // 验证点 4：冷键再读走 get_or_open_tree 懒重开，淘汰前全部已确认写入无损
  // （detach 在条带写锁下收口快照——field/field2 均须随文件落盘）
  assert_eq!(
    session.range_index_get(b"cold_a", b"field").await?,
    Some(b"value".to_vec()),
    "收口快照承接淘汰前已确认写入"
  );
  assert_eq!(
    session.range_index_get(b"cold_a", b"field2").await?,
    Some(b"value2".to_vec()),
    "收口快照承接淘汰前已确认写入（多字段）"
  );
  assert!(
    store
      .range_index
      .is_registered(&tree_id_key(0, 0, b"cold_a"))
  );
  assert_eq!(
    store.range_index.cache_reserved(),
    2 * ring,
    "重开树强制预留如实回账"
  );
  // 幂等：无新增冷树时再轮零摘除，热树（cold_c/cold_a 重开）不释放
  assert_eq!(store.recycle_cold_bftrees(), 0);
  assert_eq!(store.range_index.cache_reserved(), 2 * ring);
  OK
}
