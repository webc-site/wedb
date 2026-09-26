//! 票2 回归：树态集合删空自愈排空未级联清理 ETag 旁路记录
//!
//! 对标 C# 对象记录删空触发 HasRemoveKey → ExpireAndStop 时，记录连同尾随
//! 可选字段（HasExpiration / HasETag）一体消亡
//! （libs/server/Storage/Functions/ObjectStore/RMWMethods.cs:InPlaceUpdaterWorker）。
//! wedb 以 KeyTag::Ttl / KeyTag::Etag 两旁路记录建模，删空自愈臂 drain 单点
//! drain_and_delete_collection_meta(keep_ttl=false) 此前只级联 del_ttl、漏调
//! del_etag，导致带 ETag 的集合删空后残留无主孤儿 ETag。
//!
//! 探针一律以物理键（session.etag_key）查询索引，逻辑键会假绿。
//!
//! 自研依据: 删空自愈原子墓碑与孤儿 TTL 清退（transpile 契约，杜绝幽灵空元记录）

use std::{fs::create_dir_all, sync::Arc};

use aok::{OK, Void};
use tempfile::tempdir;
use wbftree::{StorageBackendType, TreeTuning};
use wdev::SegmentedDevice;
use wkv::{StoreConfig, WedbStore};

const TUNE: TreeTuning = TreeTuning {
  cache_size: 65536,
  min_record_size: 8,
  max_record_size: 1024,
  max_key_len: 128,
  leaf_page_size: 0,
};

/// 打开带 RangeIndex 目录的测试存储
async fn open_ri_store(
  name: &str,
) -> aok::Result<(tempfile::TempDir, Arc<WedbStore<SegmentedDevice>>)> {
  let dir = tempdir()?;
  let ri_dir = dir.path().join("range_indexes");
  create_dir_all(&ri_dir)?;
  let config = StoreConfig::new(1024, 64 * 1024, 16, 0.5)?.with_range_index_dir(&ri_dir);
  let device = Arc::new(SegmentedDevice::single_file(dir.path().join(name))?);
  let store = Arc::new(WedbStore::open(config, device)?);
  Ok((dir, store))
}

/// 删空自愈臂（keep_ttl=false）：RangeIndex 删至空后，ETag 旁路记录须随键级联清除
#[compio::test]
async fn drain_empty_collection_clears_etag_orphan() -> Void {
  let (_dir, store) = open_ri_store("drain_etag.db").await?;
  let session = store.new_session()?;

  let key = b"ri:drain";
  session
    .range_index_create(key, StorageBackendType::Disk, TUNE)
    .await?;
  session.range_index_set(key, b"field01", b"value01").await?;
  assert_eq!(session.range_index_count(key).await?, 1, "预置单字段");

  session.put_etag(key, 99).await?;
  // 造两版链：单版记录被删时按 C# CanElide（Helpers.cs:83-90，prev < BeginAddress）
  // 整槽脱链，本用例的「键位保留指向墓碑」判据只对不可脱链的多版记录成立
  store.flush_and_evict_all().await?;
  session.put_etag(key, 99).await?;
  assert_eq!(session.etag_of(key).await?, Some(99), "删空前 ETag 在场");
  assert!(
    store
      .index
      .load()
      .find_tag(session.etag_key(key).as_slice())
      .is_some(),
    "删空前 ETag 物理键在索引中"
  );

  // 删至空 → 触发自愈排空 drain(keep_ttl=false)
  assert!(session.range_index_del(key, b"field01").await?);

  // 判别断言：修复前排空臂只 del_ttl、漏 del_etag，孤儿 ETag 残留（本两行必红）
  assert_eq!(
    session.etag_of(key).await?,
    None,
    "删空自愈须级联清 ETag，杜绝无主孤儿残留"
  );
  // 索引面判据：删空走 O(1) 逻辑秒删，ETag 键位保留但必须指向墓碑记录，
  // 物理摘除交由后台 Compaction（对位票1 wkv_compact_etag_orphan 的回收臂）
  let etag_addr = store
    .index
    .load()
    .find_tag(session.etag_key(key).as_slice())
    .expect("删空后 ETag 键位仍在索引，待 Compaction 物理回收");
  assert!(
    store.hlog.read_record(etag_addr).await?.is_tombstone()?,
    "删空自愈须落 ETag 墓碑记录，而非仅会话侧读空"
  );
  OK
}

/// 对照：降阶迁移臂（keep_ttl=true）绝不级联清 ETag，键全程存活旁路保留
#[compio::test]
async fn drain_keep_ttl_preserves_etag() -> Void {
  let (_dir, store) = open_ri_store("keep_ttl_etag.db").await?;
  let session = store.new_session()?;

  let key = b"ri:migrate";
  session
    .range_index_create(key, StorageBackendType::Disk, TUNE)
    .await?;
  // field+value 须满足本测试自设存根契约 min_record_size=8，否则被 InvalidKV 提前拒
  session.range_index_set(key, b"field01", b"value01").await?;
  session.put_etag(key, 7).await?;
  assert_eq!(session.etag_of(key).await?, Some(7));

  // 迁移臂（keep_ttl=true）：仅墓碑元记录，绝不清 TTL/ETag
  session.handle_bftree_drain_and_delete(key, true).await?;

  assert_eq!(
    session.etag_of(key).await?,
    Some(7),
    "keep_ttl=true 迁移臂不得误删 ETag（若 del_etag 误置于 keep_ttl 守卫外，本行必红）"
  );
  assert!(
    store
      .index
      .load()
      .find_tag(session.etag_key(key).as_slice())
      .is_some(),
    "迁移臂后 ETag 物理键须仍在索引中"
  );
  OK
}
