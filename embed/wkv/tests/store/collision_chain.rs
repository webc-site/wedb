//! 哈希指纹碰撞与磁盘冷链回溯测试（对标 Tsavorite InternalRead/TraceBackForKeyMatch 语义）
//!
//! 索引哈希输入是会话前缀物理键（ns + db + KeyTag + 用户键），碰撞搜索必须基于物理键
//! 哈希（与 defense.rs 的 test_find_tag_probe_traceback_and_collision 口径一致）：
//! 构造确定性碰撞对（同桶 + 同 15 位 tag），先写 victim 后写 twin 使 twin 成为槽位头、
//! victim 记录被链入 prev 链；全量驱逐至磁盘区后验证：
//! 1. 冷读必须沿 prev 链跨磁盘回溯命中被掩埋的 victim；
//! 2. 对被掩埋的 victim 执行 DELETE 必须真实生效（读闭环为 None 且重写可恢复）。

use std::sync::Arc;

use aok::{OK, Void};
use compio::runtime::Runtime;
use tempfile::tempdir;
use wdev::{Device, SegmentedDevice};
use whlog::SECTOR_ALIGNMENT;
use windex::HashIndex;
use wkv::{StoreConfig, StoreSession, WedbStore};

/// 搜索与 `anchor` 物理键同桶同 tag（对给定掩码全等）的碰撞键
fn find_collision<D: Device>(session: &StoreSession<D>, anchor: &[u8], mask: u64) -> String {
  let anchor_phys = session.session_string_key(anchor);
  let target = HashIndex::hash_key(anchor_phys.as_slice()) & mask;
  for i in 0u64.. {
    let key = format!("collision-twin-{i}");
    let phys = session.session_string_key(key.as_bytes());
    if HashIndex::hash_key(phys.as_slice()) & mask == target {
      return key;
    }
  }
  unreachable!("碰撞键搜索不可能耗尽")
}

#[test]
fn test_disk_collision_chain_read_and_delete() -> Void {
  let rt = Runtime::new()?;
  rt.block_on(async {
    let dir = tempdir()?;
    let device = Arc::new(SegmentedDevice::single_file(
      dir.path().join("collision.db"),
    )?);
    // index_size=64：桶位 6 位 + tag 15 位，中间 43 位哈希位不参与索引判定，
    // 碰撞键可在 2^21 期望步数内确定性搜索得到
    let config = StoreConfig::new(64, SECTOR_ALIGNMENT, 16, 0.5)?;
    let store = Arc::new(WedbStore::open(config, device)?);
    let session = store.new_session()?;

    let mask = (store.index.mask as u64) | (0x7fffu64 << windex::HashBucketEntry::HASH_TAG_SHIFT);
    let victim = b"victim-key".to_vec();
    let twin = find_collision(&session, &victim, mask);
    let victim_val = b"victim-buried-value".to_vec();

    // 先写 victim（占据碰撞槽位），后写 twin（盲插链住 victim 并 CAS 抢占槽位头）
    session.upsert(&victim, &victim_val).await?;
    session.upsert(twin.as_bytes(), b"twin-value").await?;

    // 碰撞构造有效性自证：两物理键必须同桶同 tag
    let phys_v = session.session_string_key(&victim);
    let phys_t = session.session_string_key(twin.as_bytes());
    assert_eq!(
      HashIndex::hash_key(phys_v.as_slice()) & mask,
      HashIndex::hash_key(phys_t.as_slice()) & mask,
      "碰撞键构造必须基于物理键哈希（与索引判定一致）"
    );

    // 双向确认内存态读取正确（碰撞链回溯在内存区已由既定路径覆盖）
    assert_eq!(session.read(&victim).await?, Some(victim_val.clone()));
    assert_eq!(
      session.read(twin.as_bytes()).await?,
      Some(b"twin-value".to_vec())
    );

    // 全量刷盘并驱逐：所有记录进入磁盘区，链头与被掩埋记录均需磁盘 I/O 回溯
    store.flush_and_evict_all().await?;
    assert!(store.hlog.is_on_disk(store.tail_address() - 1));

    // 冷读 twin（槽位头直查）与 victim（必须沿 prev 链跨记录磁盘回溯）
    assert_eq!(
      session.read(twin.as_bytes()).await?,
      Some(b"twin-value".to_vec()),
      "槽位头冷读失败"
    );
    assert_eq!(
      session.read(&victim).await?,
      Some(victim_val.clone()),
      "碰撞掩埋键磁盘链回溯冷读失败"
    );
    assert!(
      session.contains_key(&victim).await?,
      "碰撞掩埋键存在性判定失败"
    );

    // DELETE 被掩埋的 victim：必须真实删除（删除后读闭环为 None）
    assert!(session.delete(&victim).await?, "碰撞掩埋键删除未生效");
    assert_eq!(session.read(&victim).await?, None, "删除后仍可读到 victim");
    assert!(!session.contains_key(&victim).await?);
    // twin 不受 victim 删除影响（链在墓碑处仍保持可达）
    assert_eq!(
      session.read(twin.as_bytes()).await?,
      Some(b"twin-value".to_vec())
    );

    // 删除后重写 victim 恢复可见
    session.upsert(&victim, b"reborn").await?;
    assert_eq!(session.read(&victim).await?, Some(b"reborn".to_vec()));

    OK
  })?;

  OK
}

/// ReadCache 链头与 Tag 碰撞链交叉场景（对标 C# InternalDelete 的 HasReadCacheSrc 分流
/// 与 AllocatorBase.AsyncGetFromDiskCallback 沿 PreviousAddress 链跳过碰撞键）：
/// 1. RC 链头脱钩盲插（upsert）后，被掩埋键冷读沿磁盘链回溯仍命中；
/// 2. 槽位头为碰撞键 RC 缓存条目时删除被掩埋键，必须真实生效且碰撞键保持可达。
#[test]
fn test_rc_collision_head_buried_delete() -> Void {
  let rt = Runtime::new()?;
  rt.block_on(async {
    let dir = tempdir()?;
    let device = Arc::new(SegmentedDevice::single_file(
      dir.path().join("rc_collision.db"),
    )?);
    let config = StoreConfig::new(64, SECTOR_ALIGNMENT, 16, 0.5)?.with_read_cache(true);
    let store = Arc::new(WedbStore::open(config, device)?);
    let session = store.new_session()?;

    let mask = (store.index.mask as u64) | (0x7fffu64 << windex::HashBucketEntry::HASH_TAG_SHIFT);
    let victim = b"rc-victim-key".to_vec();
    let twin = find_collision(&session, &victim, mask);
    let victim_val = b"victim-cold-value".to_vec();

    // 写 victim 后全量驱逐，冷读触发磁盘回填，victim 条目挂入 ReadCache 成为槽位头
    session.upsert(&victim, &victim_val).await?;
    store.flush_and_evict_all().await?;
    assert_eq!(session.read(&victim).await?, Some(victim_val.clone()));
    let phys_v = session.session_string_key(&victim);
    let victim_slot = store.index.find_tag(phys_v.as_slice()).unwrap();
    assert!(
      wkv::is_read_cache_addr(victim_slot),
      "冷读回填后 victim 槽位头必须为 ReadCache 条目"
    );

    // 1. upsert 碰撞键 twin：RC 链头脱钩（顺链解析主日志地址）盲插并 CAS 抢占槽位头；
    //    victim 冷数据经墓碑化的主日志链仍必须可达（磁盘链回溯）
    session.upsert(twin.as_bytes(), b"twin-warm-value").await?;
    assert_eq!(
      session.read(twin.as_bytes()).await?,
      Some(b"twin-warm-value".to_vec()),
      "upsert RC 链头脱钩盲插后 twin 读取失败"
    );
    assert_eq!(
      session.read(&victim).await?,
      Some(victim_val.clone()),
      "upsert RC 链头脱钩盲插后被掩埋 victim 磁盘链回溯冷读失败"
    );

    // 2. 驱逐后冷读 twin，使其 RC 缓存条目成为槽位头（victim 被掩埋在链深处）
    store.flush_and_evict_all().await?;
    assert_eq!(
      session.read(twin.as_bytes()).await?,
      Some(b"twin-warm-value".to_vec())
    );
    let phys_t = session.session_string_key(twin.as_bytes());
    assert!(
      wkv::is_read_cache_addr(store.index.find_tag(phys_t.as_slice()).unwrap()),
      "冷读回填后 twin 槽位头必须为 ReadCache 条目"
    );

    // 3. 删除被 twin 的 RC 缓存条目掩埋的 victim：
    //    槽位头为碰撞键的 RC 条目（Tag 碰撞），elide 会令 twin 不可达，
    //    必须脱钩顺链回溯主日志找到 victim 后盲墓碑挂载
    assert!(
      session.delete(&victim).await?,
      "RC 链头碰撞下删除被掩埋 victim 未生效"
    );
    assert_eq!(
      session.read(&victim).await?,
      None,
      "RC 链头碰撞删除后仍可读到 victim"
    );
    assert!(!session.contains_key(&victim).await?);
    // twin 不受 victim 删除影响（墓碑前驱保持碰撞链可达）
    assert_eq!(
      session.read(twin.as_bytes()).await?,
      Some(b"twin-warm-value".to_vec()),
      "victim 删除后 twin 必须保持可达"
    );

    OK
  })?;

  OK
}
