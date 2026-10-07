//! 冷树观察账换号摘域销账锁测（task/ing/wkv-cold-bftree-observed-orphan-domain-leak.md）
//!
//! 缺陷形：回收轮（[`wkv::WedbStore::recycle_cold_bftrees`]）观察账的销账点全部
//! 要求域仍在旁表快照内；FLUSHDB/FLUSHNS 换号 take 摘域后该域永不再进快照、
//! 死域分支亦不可达，其冷观察条目（key_id→首轮 ticks，虚号不重用故永不复现）
//! 单调滞留。本文件按票面验证点锁测修复三面：
//! 1. FLUSHDB 族换号回收唯一投递入口（reclaim_bftree_keys）顺带逐键销账——
//!    多轮换号下账目不增（回退修复即红：每轮滞留 1 条）；
//! 2. flush_all_databases 的 clear_bftree_domains 同族摘路径连带全清观察账
//!    （回退修复即红）；
//! 3. 换号回收后同名重建索引正常（销账动作零副作用旁证）。
//!
//! 观察账条目登记形态复现同 cold_tree_recycle 锁测：建树 + 写存根 + pad 填充
//! 逐出内存窗 + 迟滞窗内驱动一轮回收（首轮冷观察入账、零摘除）。跨轮统一
//! 树名：每轮开头在换号后的新域同名重建，既是下轮登记形态的前置又构成
//! 验证点 3 的重建旁证；旧域树随 take 摘注册，全仓任一时刻至多一棵在册树，
//! 观察账断言数目确定。测试经 WedbStore::open 直开（非 open_shared），
//! 无常驻回收轮并发，时序确定。
//!
//! 自研依据: doc/zh/db.md FLUSHDB/FLUSHALL 秒级换号 + doc/zh/collection.md 冷树回收

use std::sync::Arc;

use aok::{OK, Void};
use tempfile::tempdir;
use wbase::align::DEFAULT_SECTOR_SIZE;
use wbftree::{StorageBackendType, TreeTuning};
use wdev::SegmentedDevice;
use wkv::StoreConfig;

use crate::support::{open_store_in, tree_id_key};

/// 与 cold_tree_recycle 锁测同量级的小页环调参
const TUNE: TreeTuning = TreeTuning {
  cache_size: 64 * 1024,
  min_record_size: 8,
  max_record_size: 1024,
  max_key_len: 128,
  leaf_page_size: 0,
};

/// 跨轮统一树名：换号后同名重建即 FLUSHDB 旁表回收的既定语义面
const TREE: &[u8] = b"obs_tree";

/// 换号轮数：覆盖 FLUSHDB 族多轮换号，泄漏形下每轮滞留 1 条、账目单调增
const FLUSH_CYCLES: usize = 4;

fn open_store(
  dir: &tempfile::TempDir,
  name: &str,
) -> aok::Result<Arc<wkv::WedbStore<SegmentedDevice>>> {
  let config = StoreConfig::new(1024, DEFAULT_SECTOR_SIZE, 16, 0.5)?
    .with_range_index_dir(dir.path().join("range_indexes"));
  open_store_in(dir, name, config)
}

/// 建域内冷树并逐出存根页后驱动一轮回收：观察账入账 1 条、迟滞窗内零摘除
/// （复现登记形态，随后置断言只对换号销账动作敏感）
async fn register_cold_observed(store: &Arc<wkv::WedbStore<SegmentedDevice>>) -> Void {
  let session = store.new_session()?;
  session
    .range_index_create(TREE, StorageBackendType::Disk, TUNE)
    .await?;
  session.range_index_set(TREE, b"field", b"value").await?;
  // 登记域以旁表快照为准（注册面即快照，杜绝硬编码换号后的新域号）
  let (vns, vdb) = store
    .snapshot_bftree_domains()
    .into_iter()
    .find(|(_, _, keys)| keys.iter().any(|k| k.as_ref() == TREE))
    .map(|(vns, vdb, _)| (vns, vdb))
    .expect("注册面已落旁表快照");
  // 驱逐驱动：pad 记录把存根页甩出内存窗（同 cold_tree_recycle 形态）
  let pad = vec![b'P'; 480];
  session.upsert(b"pad_key", &pad).await?;
  store.flush_all().await?;
  let addr = store
    .index
    .load()
    .find_tag(&tree_id_key(vns, vdb, TREE))
    .expect("存根在册");
  let evict_to = (addr / DEFAULT_SECTOR_SIZE as u64 + 1) * DEFAULT_SECTOR_SIZE as u64;
  store.shift_read_only_address(evict_to);
  store.shift_head_address(evict_to);
  assert!(store.hlog.is_on_disk(addr), "存根页已逐出");
  // 迟滞窗内首轮冷观察：入账一条、零摘除（全仓至多一棵在册树，数目确定）
  assert_eq!(store.recycle_cold_bftrees(), 0, "迟滞窗内不得摘除");
  assert_eq!(store.cold_bftree_observed_len(), 1, "首轮冷观察入账");
  OK
}

/// 验证点 1+3：FLUSHDB 族多轮换号，观察账随摘域逐键销账、账目不单调增；
/// 每轮开头的同名重建承接换号语义面，末轮再补一次空树断言
#[compio::test]
async fn test_flushdb_reclaim_clears_cold_observed_ledger() -> Void {
  let dir = tempdir()?;
  let store = open_store(&dir, "obs_flushdb.db")?;
  for cycle in 0..FLUSH_CYCLES {
    register_cold_observed(&store).await?;
    // 换号回收投递：唯一投递入口顺带销账，观察账归零（泄漏形下滞留 1 条）
    let (vns, old_vdb) = store.flush_database(0, 0).await?;
    assert_eq!(vns, 0);
    assert!(old_vdb.is_some(), "既有域换号必有旧域退役");
    assert_eq!(
      store.cold_bftree_observed_len(),
      0,
      "FLUSHDB 摘域必须同步销观察账（第 {cycle} 轮）"
    );
  }
  // 末轮换号后同名重建并确认可读空树（验证点 3 收口旁证）
  let session = store.new_session()?;
  session
    .range_index_create(TREE, StorageBackendType::Disk, TUNE)
    .await?;
  assert_eq!(
    session.range_index_get(TREE, b"field").await?,
    None,
    "换号后同名新树为空树"
  );
  OK
}

/// 验证点 2：flush_all_databases 的 clear_bftree_domains 同族摘路径连带全清观察账
#[compio::test]
async fn test_flushall_clears_cold_observed_ledger() -> Void {
  let dir = tempdir()?;
  let store = open_store(&dir, "obs_flushall.db")?;
  register_cold_observed(&store).await?;
  store.flush_all_databases().await?;
  assert_eq!(
    store.cold_bftree_observed_len(),
    0,
    "flush_all 全域摘除必须连带全清观察账"
  );
  OK
}
