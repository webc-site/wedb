//! 从库全量快照恢复置换与单 Database Manager 失同步回归
//!
//! C# 契约（libs/server/StoreWrapper.cs:41 `Store => databaseManager.Store`
//! 单计算属性动态转发；libs/server/Databases/SingleDatabaseManager.cs:
//! RecoverCheckpointAsync 恢复后的引擎全生命周期一致）：从库全量快照恢复
//! 置换在线引擎后，管理面周期检查点/BGSAVE、FLUSHDB/FLUSHALL、索引自适应
//! 扩容一律作用于恢复后的最新在线引擎。
//!
//! rust 恢复产出全新 [`WedbStore`] 实例（replica_diskbased_sync 换库段经
//! `ClusterProvider::swap_online_store` 收口），若管理面引用不联动换持，
//! `SingleDatabaseManager.db` 悬挂旧引擎：BGSAVE 拍出置换前的旧数据（主库
//! 同步数据全部丢失）、清库与扩容在旧引擎空转。三个用例分别锁定三条链。

use std::{
  fs::create_dir_all,
  sync::{Arc, atomic::Ordering},
};

use compio::runtime::Runtime;
use wdev::SegmentedDevice;
use wedb::server::cluster_provider::ClusterProvider;
use wkv::{StoreConfig, WedbStore};
use wnode::database::{GarnetDatabase, SingleDatabaseManager};
use wtest_base::open_test_store;

type AssembleHarness = (
  tempfile::TempDir,
  Arc<WedbStore<SegmentedDevice>>,
  Arc<GarnetDatabase<SegmentedDevice>>,
  Arc<SingleDatabaseManager<SegmentedDevice>>,
  Arc<ClusterProvider>,
);

/// 装配集群形态宿主（provider + 旧引擎宿主库 + 单库管理器，对齐
/// checkpoint_wiring 生产装配链样板：attach_flush_gate + set_database_manager）
fn assemble(tag: &str) -> aok::Result<AssembleHarness> {
  let (dir, old_store) = open_test_store(tag)?;
  let cp_dir = dir.path().join("checkpoints");
  create_dir_all(&cp_dir)?;
  let db = Arc::new(GarnetDatabase::new(
    0,
    Arc::clone(&old_store),
    Arc::clone(&old_store.device),
    cp_dir,
    None,
  ));
  let dm = Arc::new(SingleDatabaseManager::new(
    dir.path().join("checkpoints"),
    Arc::clone(&db),
  ));
  let provider = ClusterProvider::new();
  provider.set_checkpoint_dir(dir.path().join("checkpoints"));
  dm.attach_flush_gate(provider.clone());
  provider.set_database_manager(Arc::clone(&dm));
  Ok((dir, old_store, db, dm, provider))
}

/// 模拟主库快照恢复产出的新引擎（独立实例，携带主库同步数据）
async fn recovered_engine(
  tag: &str,
  key: &[u8],
) -> aok::Result<(tempfile::TempDir, Arc<WedbStore<SegmentedDevice>>)> {
  let (dir, store) = open_test_store(tag)?;
  let session = store.new_session()?;
  session.upsert(key, b"from-primary").await?;
  Ok((dir, store))
}

/// BGSAVE 链：swap_online_store 后管理面检查点必须拍摄新引擎数据
///
/// 修复前：dm 悬挂旧引擎，快照物化置换前残留（stale-key），主库同步数据
///（recovered-key）全部丢失——故障转移升主后持久化即灾难性回滚
#[test]
fn bgsave_after_swap_snapshots_recovered_engine() -> aok::Result<()> {
  let rt = Runtime::new()?;
  rt.block_on(async {
    let (_dir, old_store, db, dm, provider) = assemble("swap_bgsave")?;

    // 置换前旧引擎残留数据
    let s_old = old_store.new_session()?;
    s_old.upsert(b"stale-key", b"stale").await?;

    // 从库全量快照恢复置换（新引擎携带主库同步数据）
    let (_new_dir, new_store) = recovered_engine("swap_bgsave_new", b"recovered-key").await?;
    provider.swap_online_store(Arc::clone(&new_store));

    // 管理面引用须与新引擎同一实例（换持联动单点）
    assert!(
      Arc::ptr_eq(&dm.db.store(), &new_store),
      "swap_online_store 须联动换持 SingleDatabaseManager 引擎引用"
    );

    // BGSAVE 内核（SAVE/BGSAVE 慢路径同款收口入口）
    assert!(dm.take_checkpoint(true).await?, "快照须发布");

    // 从最新快照恢复出引擎（设备须与检查点同源——新引擎自身设备， flushed
    // 前缀配套），断言物化的是新引擎数据
    let token = *wcpr::list_checkpoints(&db.checkpoint_dir)?
      .last()
      .expect("快照已发布");
    let snapshotted =
      WedbStore::recover(&db.checkpoint_dir, token, Arc::clone(&new_store.device)).await?;
    let snapshotted = Arc::new(snapshotted);
    let s_snap = snapshotted.new_session()?;
    assert_eq!(
      s_snap.read(b"recovered-key").await?,
      Some(b"from-primary".to_vec()),
      "检查点必须物化主库同步数据（新引擎）"
    );
    assert_eq!(
      s_snap.read(b"stale-key").await?,
      None,
      "检查点不得物化被淘汰旧引擎的置换前残留"
    );
    aok::OK
  })
}

/// FLUSHDB/FLUSHALL 链：swap 后清库必须确切作用于新引擎
///
/// 修复前：清库在旧引擎空转，在线新引擎数据原封不动（清库失效 +
/// 双引擎分叉）
#[test]
fn flush_after_swap_targets_recovered_engine() -> aok::Result<()> {
  let rt = Runtime::new()?;
  rt.block_on(async {
    let (_dir, old_store, _db, dm, provider) = assemble("swap_flush")?;

    let (_new_dir, new_store) = recovered_engine("swap_flush_new", b"recovered-key").await?;
    provider.swap_online_store(Arc::clone(&new_store));

    // FLUSHDB：新引擎上数据须被清
    dm.flush_database(0, 0, false).await?;
    let s_new = new_store.new_session()?;
    assert_eq!(
      s_new.read(b"recovered-key").await?,
      None,
      "FLUSHDB 必须清掉在线新引擎数据"
    );

    // FLUSHALL：新引擎重新写入后再清，仍须作用同一在线实例
    s_new.upsert(b"post-flush", b"v").await?;
    dm.flush_all_databases(false).await?;
    let s_new = new_store.new_session()?;
    assert_eq!(
      s_new.read(b"post-flush").await?,
      None,
      "FLUSHALL 必须清掉在线新引擎数据"
    );
    assert!(
      !Arc::ptr_eq(&dm.db.store(), &old_store),
      "管理面引用不得回退悬挂旧引擎"
    );
    aok::OK
  })
}

/// 索引自适应扩容链：swap 后 GrowIndexesIfNeeded 必须读新引擎索引规模
///
/// 新旧引擎索引规模可区分（不同内存预算装配），maxed 判定以在线引擎
/// index size 为准；修复前判定读旧引擎 size，上限判定与扩容全部错位
#[test]
fn grow_index_after_swap_reads_recovered_engine() -> aok::Result<()> {
  let rt = Runtime::new()?;
  rt.block_on(async {
    let (_dir, old_store, db, dm, provider) = assemble("swap_grow")?;

    // 新引擎以显式小索引装配（2048 桶，宿主为 65536 桶——索引规模可区分）
    let new_dir = tempfile::tempdir()?;
    let new_device = Arc::new(SegmentedDevice::single_file(
      new_dir.path().join("swap_grow_new.db"),
    )?);
    let mut new_config = StoreConfig::new(2048, 64 * 1024, 16, 0.5)?;
    new_config.gc.enabled = false;
    let new_store = Arc::new(WedbStore::open(new_config, new_device)?);
    provider.swap_online_store(Arc::clone(&new_store));

    let size_new = new_store.active_index().size;
    let size_old = old_store.active_index().size;
    assert_ne!(
      size_new, size_old,
      "前置条件：新旧引擎索引规模须可区分，否则用例失去判别力"
    );

    // maxed 判定（index_max_size = 新引擎实际 size）：读新引擎 → 已达上限
    let maxed = dm.grow_indexes_if_needed(size_new, 50).await?;
    assert!(
      maxed,
      "扩容判定须读在线新引擎 index size（size == max → true）；\
       读到旧引擎 size 时该判定错位为 false"
    );
    assert!(
      db.store_index_maxed_out.load(Ordering::Acquire),
      "上限结论须落在新引擎判定之上"
    );
    aok::OK
  })
}
