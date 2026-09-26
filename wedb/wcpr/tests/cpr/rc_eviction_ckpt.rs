//! 索引检查点相位 ReadCache 换页驱逐过渡态回归
//! （缺陷：CPR 面把三态契约的 `None` 折成 0 哨兵，与紧缩面探针口径分叉）
//!
//! 缺陷形态：换页单临界区自 head 推进起、至 `cleanse_page` 及本槽止，该页 RC 记录
//! 顺链解析不可判读（宿主 skip 端口报 `None`），但槽位值未变、记录未失效、键存活。
//! 检查点面此前把这一过渡态连同链尽一起归零 ⇒ 存活键在**快照文件**里整槽消失，
//! 而 cleanse 随后修复的是 live 槽位（系统侧无痕），恢复重插域又不含其前驱主日志
//! 记录（地址恒在 `index_start` 之下），重启后该键永久不可见。
//!
//! 链尽（`Some(0)`，RC 专属记录无主日志对应）的整槽净化语义由 `rc_tag` 套件覆盖。
//!
//! 对标 C#：快照面 `IndexCheckpoint.cs:146-157` 在 `epoch.Resume()` 下整块 bucket
//! 拷贝后经 `ReadCache.cs:159-179 SkipReadCacheBucket` 在拷贝上直走链，纪元保护
//! 冻结驱逐进度，故 C# 检查点面**不存在** None/0 判读分叉；本 port 不冻结驱逐，
//! 改走紧缩面同一套「三态 + 等待重探」（`ReadCache.cs:ReadCacheNeedToWaitForEviction`
//! + `EpochOperations.cs:SpinWaitUntilRecordIsClosed`），两套口径就此收敛为一套。

use std::{
  sync::{Arc, atomic::Ordering},
  time::Duration,
};

use aok::{OK, Void};
use tempfile::tempdir;
use wcpr::{CheckpointType, CkptGateState, index_filename, read_index_checkpoint_truncated};
use wdev::SegmentedDevice;
use windex::{HashBucket, HashBucketEntry, HashIndex};

use super::support::{MiniStore, Watchdog, pick_nonzero_tag_key};

/// 本用例键名前缀（与 rc_tag 套件的键空间互不相交）
const EVICT_MARK: &str = "rc:evict:";

/// 在快照重建的索引中按指纹定位该键承载的原始槽位字（含高 16 位指纹位）
fn snapshot_raw_slot(index: &HashIndex, key: &[u8], tag: u64) -> u64 {
  let bucket = index.bucket(index.bucket_index_for_key(key));
  bucket.entries[..HashBucket::DATA_ENTRIES]
    .iter()
    .map(|slot| slot.load(Ordering::Acquire))
    .find(|raw| {
      *raw != 0
        && (raw & HashBucketEntry::TAG_POS_MASK) >> HashBucketEntry::TAG_SHIFT == tag
        && raw & HashBucketEntry::ADDRESS_MASK != 0
    })
    .unwrap_or_else(|| panic!("快照中未找到指纹 {tag:#04x} 的非零槽位: {key:?}"))
}

/// 时序注入「head 已过、cleanse 未及」的 None 过渡态：断言快照该槽落主日志地址
/// 且指纹保留、等待一轮即收敛、恢复后 GET 可见
#[compio::test]
async fn live_key_survives_read_cache_eviction_at_index_checkpoint() -> Void {
  // 等待重探环若退化（永不落定）会让本用例挂死，看门狗负责暴露
  let _watchdog = Watchdog::start("rc_eviction_ckpt", Duration::from_secs(20));
  let dir = tempdir()?;
  let ckpt_dir = dir.path().join("checkpoints");
  let db_path = dir.path().join("rc_evict.db");
  let store = MiniStore::open(&db_path)?;
  let gate = CkptGateState::default();
  let p = store.session()?;

  // 哨兵键：验证相邻键不受驱逐等待环影响
  store.put(&p, b"user:evict:sentinel", b"safe").await?;
  // 靶键：普通插入后槽位改指 ReadCache 记录（模拟读缓存晋升回写索引槽位），
  // 再布防换页驱逐过渡态——此际该地址顺链解析恒报 None
  let key = pick_nonzero_tag_key(EVICT_MARK);
  store.put(&p, &key, b"hot_value").await?;
  store.install_read_cache_entry(&p, &key)?;
  store.arm_read_cache_eviction(&key)?;
  drop(p);

  let tag = HashBucketEntry::tag_from_hash(HashIndex::hash_key(&key)) as u64;
  assert_ne!(tag, 0, "前置条件：测试键指纹必须非零");
  let rc_slot = store
    .index
    .find_tag(&key)
    .expect("前置条件：布防后槽位必须可见");
  assert_ne!(
    rc_slot & HashBucketEntry::READ_CACHE_BIT,
    0,
    "前置条件：槽位必须是 ReadCache 形态"
  );
  let expected_main = store.resolve_main(rc_slot);
  assert_ne!(expected_main, 0, "前置条件：主日志真实地址必须非零");

  let meta = wcpr::create_checkpoint(&store, &gate, &ckpt_dir, CheckpointType::FoldOver).await?;
  assert!(
    expected_main < meta.index_start_logical_address,
    "前置条件：靶记录必须落在模糊区地板之下（恢复重插域不含它，快照归零即永久漏键）"
  );

  // ① 核心判据：快照文件该槽落主日志地址（修复前 None 折 0 → 整槽归零）
  let (snapshot, _) =
    read_index_checkpoint_truncated(ckpt_dir.join(index_filename(meta.token)), meta.token, None)
      .await?;
  assert_eq!(
    snapshot.find_tag(&key),
    Some(expected_main),
    "驱逐过渡态的存活键必须回写主日志地址入快照，绝不可整槽归零"
  );

  // ② 指纹保真：仅换写地址字段，高 16 位原样保留（C# Address setter 掩码换写语义），
  //    且 ReadCache 指示位不得残留
  let raw = snapshot_raw_slot(&snapshot, &key, tag);
  assert_eq!(
    raw & HashBucketEntry::ADDRESS_MASK,
    expected_main,
    "快照槽位地址字段必须是主日志真实地址"
  );
  assert_eq!(
    (raw & HashBucketEntry::TAG_POS_MASK) >> HashBucketEntry::TAG_SHIFT,
    tag,
    "快照槽位指纹必须原样保留（否则恢复期 find_tag 永失配）"
  );
  assert_eq!(
    raw & HashBucketEntry::READ_CACHE_BIT,
    0,
    "快照槽位不得残留 ReadCache 指示位"
  );

  // ③ 等待有界收敛：一轮清洗发布 ClosedUntil 即落定，检查点不被拖成长自旋
  assert_eq!(
    store.rc_evict_waits(),
    1,
    "等待重探环必须在驱逐方清洗落定的那一轮收敛（0 = 未走等待环，>1 = 空转）"
  );
  // live 槽位由注入侧复现的 cleanse 修复——缺陷只打快照面，系统侧无痕
  assert_eq!(
    store.index.find_tag(&key),
    Some(expected_main),
    "清洗落定后 live 槽位必须指向主日志地址"
  );

  // ④ 恢复面：销毁重建后该键必须可见（修复前此处 GET 永久为空）
  drop(store);
  let device = Arc::new(SegmentedDevice::single_file(&db_path)?);
  let restored = wcpr::recover::<_, MiniStore>(&ckpt_dir, meta.token, device).await?;
  let p2 = restored.session()?;
  assert_eq!(
    restored.index.find_tag(&key),
    Some(expected_main),
    "恢复索引必须携回该键"
  );
  assert_eq!(
    restored.get(&p2, &key).await?.as_deref(),
    Some(b"hot_value".as_slice()),
    "恢复后 GET 必须可见该存活键"
  );
  assert_eq!(
    restored.get(&p2, b"user:evict:sentinel").await?.as_deref(),
    Some(b"safe".as_slice()),
    "哨兵键不得受等待重探环影响"
  );
  OK
}
