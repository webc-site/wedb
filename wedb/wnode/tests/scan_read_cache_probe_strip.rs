//! SCAN 族活键探针剥 ReadCache 位回归（票 wkv-scan-live-key-probe-missing-read-cache-skip）
//!
//! ReadCache 开启下冷键点读回填以链首 CAS 挂载 `READ_CACHE_BIT` 虚拟地址，
//! 活键判定单点（[`StorageSession`] 的 `active_user_key_at` 链首探针）若不先
//! 剥位再比对主日志记录地址，预热冷键整批误判 Dead：SCAN/KEYS/DBSIZE 漏键
//! 漏计（SCAN 游标固化不可恢复）、无盘复制快照键集缺键发散、delete_slot_keys
//! 漏删残留、双域新者胜恒判他域新。对标 C# FindRecord.cs:84-85 `SkipReadCache`
//! （剥位仅限扫描判定消费面，`find_tag_cooperative` 单点本体不动）。

use std::sync::Arc;

use tempfile::tempdir;
use wbase::{addr::is_read_cache, hash_slot::slot_of};
use wdev::SegmentedDevice;
use wkv::WedbStore;
use wnode_test::storage_session;
use wtest_base::test_store_config;
use wval::{GarnetObjectType, KeyTag};

/// RC 开启小库（flush_and_evict_all 后点读全落磁盘冷路径回填 RC）
fn open_read_cache_store(
  tag: &str,
) -> aok::Result<(tempfile::TempDir, Arc<WedbStore<SegmentedDevice>>)> {
  let dir = tempdir()?;
  let device = Arc::new(SegmentedDevice::single_file(
    dir.path().join(format!("{tag}.db")),
  )?);
  let store = Arc::new(WedbStore::open(
    test_store_config().with_read_cache(true),
    device,
  )?);
  Ok((dir, store))
}

/// 前提守护：物理键链头确挂 RC 位（回填未发生则本文件用例对剥位回归失去判别力）；
/// `key` 须为带会话前缀 + 标签的物理键（索引键域，经 `session_string_key` /
/// `session_tag_key` 构造）
fn assert_head_is_read_cache(store: &WedbStore<SegmentedDevice>, key: &[u8]) {
  let head = store.index.load().find_tag(key);
  assert!(
    head.is_some_and(is_read_cache),
    "{key:?} 冷读回填后链头应挂 ReadCache 位，实测 {head:?}（rc_enabled={}）",
    store.read_cache.is_enabled,
  );
}

/// RC 预热冷键后扫描族口径不漏键不漏计：SCAN / KEYS / DBSIZE / 槽位计数 /
/// 槽位键集（无盘复制快照 GETKEYSINSLOT 消费面）与写入键集逐键全等
#[compio::test]
async fn scan_family_after_read_cache_prewarm_no_miss() -> aok::Void {
  let (_dir, store) = open_read_cache_store("scan_rc_prewarm.db")?;
  let session = store.new_session()?;
  let ss = storage_session(&session);
  let slot = slot_of(0, 0);

  let keys: Vec<Vec<u8>> = (0..8).map(|i| format!("key:{i}").into_bytes()).collect();
  for k in &keys {
    ss.upsert_string(k, b"v").await?;
  }

  // 整段内存日志落盘驱逐后逐键冷读回填
  store.flush_and_evict_all().await?;
  for k in &keys {
    assert_eq!(ss.read_string(k).await?.as_deref(), Some(b"v".as_slice()));
    assert_head_is_read_cache(&store, &session.session_string_key(k));
  }

  // 修复前：链头挂 RC 位与主日志记录地址裸比必然失配，整批误判 Dead 全漏
  assert_eq!(ss.db_size().await?, keys.len(), "DBSIZE 漏计预热冷键");
  assert_eq!(ss.db_keys(b"*").await?, keys, "KEYS 漏键");
  // 页上界取 N+1 保证扫尽归零（恰好 N 触发页满返回推进游标）
  let (cursor, scan_keys) = ss.scan_cursor(b"*", true, 0, keys.len() + 1, None).await?;
  assert_eq!(cursor, 0, "单页应扫尽归零");
  assert_eq!(scan_keys, keys, "SCAN 漏键");

  // 槽位口径（COUNTKEYSINSLOT / 复制快照以 usize::MAX 全量收集）键集逐键全等
  assert_eq!(ss.count_keys_in_slot(slot).await?, keys.len());
  assert_eq!(ss.get_keys_in_slot(slot, usize::MAX).await?, keys);
  Ok(())
}

/// RC 预热冷键后 delete_slot_keys 删净无残留（修复前漏判 Dead 漏删残留）
#[compio::test]
async fn delete_slot_keys_after_read_cache_prewarm_no_residue() -> aok::Void {
  let (_dir, store) = open_read_cache_store("scan_rc_del.db")?;
  let session = store.new_session()?;
  let ss = storage_session(&session);
  let slot = slot_of(0, 0);

  let keys: Vec<Vec<u8>> = (0..4).map(|i| format!("del:{i}").into_bytes()).collect();
  for k in &keys {
    ss.upsert_string(k, b"v").await?;
  }
  store.flush_and_evict_all().await?;
  for k in &keys {
    assert_eq!(ss.read_string(k).await?.as_deref(), Some(b"v".as_slice()));
    assert_head_is_read_cache(&store, &session.session_string_key(k));
  }

  let deleted = ss.delete_slot_keys(&[slot]).await?;
  assert_eq!(deleted, keys.len() as u64, "delete_slot_keys 漏删预热冷键");
  assert_eq!(ss.db_size().await?, 0, "删除后仍有残留计数");
  assert!(ss.get_keys_in_slot(slot, 10).await?.is_empty());
  for k in &keys {
    assert_eq!(session.read(k).await?, None, "{k:?} 删除后残留");
  }
  Ok(())
}

/// String/信封双域同名键 RC 预热后 SCAN 计数正确：双域仅计一次且不整批漏键
///
/// 修复前双域探针裸比 RC 地址：跨域探针 `a > addr` 对 RC 地址恒真（1<<47 远
/// 超主日志地址）恒判他域新，主探针亦链首失配，双域键整批漏判
#[compio::test]
async fn dual_domain_key_scan_count_after_read_cache_prewarm() -> aok::Void {
  let (_dir, store) = open_read_cache_store("scan_rc_dual.db")?;
  let session = store.new_session()?;
  let ss = storage_session(&session);

  // 双域同名键（信封后写为新者）+ 单域参照键
  ss.upsert_string(b"dual", b"s").await?;
  ss.upsert_tag(
    b"dual",
    KeyTag::ObjectEnvelope,
    &[GarnetObjectType::Set.as_u8(), 1],
  )
  .await?;
  ss.upsert_string(b"solo", b"v").await?;

  store.flush_and_evict_all().await?;
  // 双域各自冷读回填：两域链头均挂 RC 位，主/跨域两探针剥位臂同时受力
  assert!(
    ss.read_tag_with(b"dual", KeyTag::ObjectEnvelope, |_| ())
      .await?
      .is_some(),
    "信封域冷读应命中"
  );
  assert_eq!(
    ss.read_string(b"dual").await?.as_deref(),
    Some(b"s".as_slice())
  );
  assert_eq!(
    ss.read_string(b"solo").await?.as_deref(),
    Some(b"v".as_slice())
  );
  assert_head_is_read_cache(&store, &session.session_string_key(b"dual"));
  assert_head_is_read_cache(
    &store,
    &session.session_tag_key(KeyTag::ObjectEnvelope, b"dual"),
  );

  assert_eq!(ss.db_size().await?, 2, "双域键须计一次，单域键计一次");
  assert_eq!(
    ss.db_keys(b"*").await?,
    vec![b"dual".to_vec(), b"solo".to_vec()]
  );
  let (cursor, scan_keys) = ss.scan_cursor(b"*", true, 0, 10, None).await?;
  assert_eq!(cursor, 0);
  assert_eq!(scan_keys, vec![b"dual".to_vec(), b"solo".to_vec()]);
  Ok(())
}
