//! ReadCache 槽位指纹（tag）保真回归
//!
//! 缺陷背景：快照写侧 `resolve_read_cache` 曾将解析闭包返回值（裸主日志地址，
//! 高 16 位全零）整体替换槽位字，导致快照槽位 tag 被清零——恢复后 `find_tag`
//! 按 `matches_tag` 比对高 16 位永失配，该键不可见。
//!
//! C# 正确语义：`SkipReadCacheBucket` 仅回写 Address 字段、指纹位原样保留
//! （Tsavorite ReadCache.cs:159 `SkipReadCacheBucket` 逐跳 `entry->Address =
//! logicalAddress` + HashBucketEntry.cs:49 Address setter 掩码换写）。

use std::sync::{Arc, atomic};

use aok::{OK, Void};
use compio::runtime::Runtime;
use tempfile::tempdir;
use wcpr::{CheckpointManager, CheckpointType};
use wdev::SegmentedDevice;
use wepoch::Participant;
use windex::{HashBucket, HashBucketEntry, HashIndex};

use super::support::{MiniStore, RC_VIRTUAL_BASE};

/// 挑选首个 15 位指纹非零的键（指纹取自键哈希高位，几乎必然首个即命中）
fn pick_nonzero_tag_key() -> Vec<u8> {
  (0u32..1024)
    .map(|i| format!("rc:tag:{i}").into_bytes())
    .find(|k| HashBucketEntry::tag_from_hash(HashIndex::hash_key(k)) != 0)
    .expect("1024 个候选键必有非零指纹")
}

/// 将指定键槽位改写为断链 RC 形态（虚拟地址映射到主日志地址 0，解析闭包必返回 0）
fn install_broken_rc_entry(store: &MiniStore, p: &Participant, key: &[u8]) -> Void {
  let _guard = p.enter();
  let main = store
    .index
    .find_tag(key)
    .ok_or_else(|| aok::anyhow!("install_broken_rc_entry: 键不存在: {key:?}"))?;
  let bucket = store.index.bucket_for_key(key);
  for slot in &bucket.entries[..HashBucket::DATA_ENTRIES] {
    let cur = slot.load(atomic::Ordering::Acquire);
    if cur & HashBucketEntry::ADDRESS_MASK == main {
      let broken = RC_VIRTUAL_BASE | HashBucketEntry::READ_CACHE_BIT;
      let new_raw = (cur & !HashBucketEntry::ADDRESS_MASK) | broken;
      if slot
        .compare_exchange(
          cur,
          new_raw,
          atomic::Ordering::AcqRel,
          atomic::Ordering::Acquire,
        )
        .is_ok()
      {
        return OK;
      }
    }
  }
  aok::bail!("install_broken_rc_entry: 未定位到匹配槽位: {key:?}")
}

/// RC 位条目 + 非零 tag：checkpoint → 销毁重建 → find_tag 必须命中（修复前 tag
/// 被清零致此键永失配不可见），且槽位无 RC 残留、回写主日志真实地址
#[test]
fn rc_entry_tag_survives_checkpoint_roundtrip() -> Void {
  let rt = Runtime::new()?;
  rt.block_on(async {
    let dir = tempdir()?;
    let ckpt_dir = dir.path().join("checkpoints");
    let db_path = dir.path().join("rc_tag.db");
    let store = MiniStore::open(&db_path)?;
    let p = store.session()?;

    // 哨兵键（普通主日志条目）：验证相邻键不受解析流程影响
    store.put(&p, b"user:sentinel", b"safe").await?;
    // RC 位条目：正常插入后改写槽位为读缓存虚拟形态（tag 保留在高 16 位）
    let key = pick_nonzero_tag_key();
    store.put(&p, &key, b"hot_value").await?;
    store.install_read_cache_entry(&p, &key)?;
    drop(p);

    let tag = HashBucketEntry::tag_from_hash(HashIndex::hash_key(&key));
    assert_ne!(tag, 0, "前置条件：测试键指纹必须非零");
    let rc_addr = store.index.find_tag(&key).expect("插入后必须可见");
    assert!(
      rc_addr & HashBucketEntry::READ_CACHE_BIT != 0,
      "前置条件：槽位必须是 RC 虚拟形态"
    );
    let expected_main = (rc_addr & !HashBucketEntry::READ_CACHE_BIT) - RC_VIRTUAL_BASE;
    assert_ne!(expected_main, 0, "前置条件：主日志真实地址必须非零");

    let mgr = CheckpointManager::<SegmentedDevice>::new();
    let meta = mgr
      .create_checkpoint(&store, &ckpt_dir, CheckpointType::FoldOver)
      .await?;

    // 销毁重建：全新设备句柄 + 全新引擎实例
    drop(store);
    let device = Arc::new(SegmentedDevice::single_file(&db_path)?);
    let restored = CheckpointManager::recover::<MiniStore>(&ckpt_dir, meta.token, device).await?;
    let p2 = restored.session()?;

    // 核心回归点：tag 保真 → find_tag 必须命中（修复前 tag 清零，此处为 None）
    let recovered = restored
      .index
      .find_tag(&key)
      .expect("RC 条目恢复后必须可见（tag 保真，find_tag 不得失配）");
    assert!(
      recovered & HashBucketEntry::READ_CACHE_BIT == 0,
      "恢复槽位不得残留 ReadCache 指示位"
    );
    assert_eq!(recovered, expected_main, "必须回写主日志真实地址");
    assert_eq!(
      restored.get(&p2, &key).await?.as_deref(),
      Some(b"hot_value".as_slice()),
      "恢复后键值必须可见"
    );
    assert_eq!(
      restored.get(&p2, b"user:sentinel").await?.as_deref(),
      Some(b"safe".as_slice()),
      "哨兵键不受解析流程影响"
    );

    aok::Result::<()>::Ok(())
  })?;
  OK
}

/// 断链 RC 条目（解析闭包返回 0）：槽位必须整体净化归零（该键不可见），
/// 其余键不受影响
#[test]
fn broken_rc_chain_slot_sanitized_on_roundtrip() -> Void {
  let rt = Runtime::new()?;
  rt.block_on(async {
    let dir = tempdir()?;
    let ckpt_dir = dir.path().join("checkpoints");
    let db_path = dir.path().join("rc_broken.db");
    let store = MiniStore::open(&db_path)?;
    let p = store.session()?;

    store.put(&p, b"user:keep", b"alive").await?;
    let key = pick_nonzero_tag_key();
    store.put(&p, &key, b"doomed").await?;
    // 改写为断链 RC 形态：虚拟地址映射主日志地址 0，解析闭包返回 0
    install_broken_rc_entry(&store, &p, &key)?;
    drop(p);

    assert!(
      store
        .index
        .find_tag(&key)
        .is_some_and(|a| a & HashBucketEntry::READ_CACHE_BIT != 0),
      "前置条件：断链条目必须是 RC 虚拟形态"
    );

    let mgr = CheckpointManager::<SegmentedDevice>::new();
    let meta = mgr
      .create_checkpoint(&store, &ckpt_dir, CheckpointType::FoldOver)
      .await?;

    drop(store);
    let device = Arc::new(SegmentedDevice::single_file(&db_path)?);
    let restored = CheckpointManager::recover::<MiniStore>(&ckpt_dir, meta.token, device).await?;
    let p2 = restored.session()?;

    // 断链条目：槽位净化归零，键不可见（绝不将物理偏移误当主日志地址）
    assert_eq!(
      restored.index.find_tag(&key),
      None,
      "断链 RC 条目恢复后必须不可见"
    );
    assert_eq!(
      restored.get(&p2, &key).await?,
      None,
      "断链 RC 条目恢复后必须无值"
    );
    // 其余键不受影响
    assert_eq!(
      restored.get(&p2, b"user:keep").await?.as_deref(),
      Some(b"alive".as_slice()),
      "断链净化不得伤及相邻键"
    );

    // 全桶扫描：恢复索引不得残留 RC 位与试探态标记
    for bucket in restored.index.buckets.iter() {
      for slot in &bucket.entries[..HashBucket::DATA_ENTRIES] {
        let raw = slot.load(atomic::Ordering::Acquire);
        assert!(
          raw & (HashBucketEntry::READ_CACHE_BIT | HashBucketEntry::TENTATIVE_MASK) == 0,
          "恢复索引槽位残留瞬态标记: {raw:#x}"
        );
      }
    }

    aok::Result::<()>::Ok(())
  })?;
  OK
}
