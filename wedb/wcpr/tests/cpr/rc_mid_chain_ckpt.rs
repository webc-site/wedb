//! 链中段滑出形态的索引快照回归
//! （缺陷：ReadCache 驱逐等待锚在**链头**，中段滑出被误判链尽、整槽归零落盘）
//!
//! 缺陷形态：同一键反复命中读缓存会晋升出多代 RC 记录（槽位指最新代，逐代 prev 串
//! 更旧一代，最末一代的 prev 才落主日志真身）。驱逐自最老侧推进，「链上靠旧侧已滑出
//! 环形窗口、槽头（最新代）仍在窗」是换页进行中的**常态**形态（对标 wkv
//! `read_cache/cleanse.rs` 自述「被驱逐记录多在链深处」）。旧实现以槽头地址判定是否
//! 须等待，槽头在窗 ⇒ 判为不必等待 ⇒ 按链尽归零，存活键在**快照文件**里整槽消失；
//! 随后清洗方修复的是 live 槽位（系统侧无痕），主日志真身又恒在 `index_start` 之下、
//! 不被恢复期重插，重启后该键永久不可见、无自愈通道。
//!
//! C# 正确语义：`ReadCache.cs:101-115 ReadCacheNeedToWaitForEviction` 判定对象是
//! **走查当前位置**（`stackCtx.recSrc.LatestLogicalAddress`），`:119-155 SkipReadCache`
//! 每步先判定当前位置、命中滑出即 `SpinWaitUntilRecordIsClosed` 后经
//! `UpdateRecordSourceToCurrentHashEntry` 重读哈希项回链头重探（`goto RestartChain`）。
//! C# 两条路径都不存在「以链头地址判定链中段滑出」的形态。
//!
//! 自研依据: 快照面与紧缩面共用同一带等待走查内核
//! （`wkv::ReadCache::skip_read_cache_with_wait`，见 `wkv/tests/store/read_cache.rs` 真实环形窗口面回归）

use std::{
  sync::{Arc, atomic::Ordering},
  time::Duration,
};

use aok::{OK, Result, Void};
use tempfile::tempdir;
use wcpr::{
  CheckpointType, CkptGateState, CprStore, index_filename, read_index_checkpoint_truncated,
};
use wdev::SegmentedDevice;
use wepoch::Participant;
use windex::{HashBucket, HashBucketEntry, HashIndex};

use super::support::{MiniStore, Watchdog, pick_nonzero_tag_key};

/// 本用例键名前缀（与 rc_tag / rc_eviction_ckpt 套件的键空间互不相交）
const MID_MARK: &str = "rc:mid:";

/// 靶键的链形（主日志真身地址 + 自槽头向旧逐代的 RC 地址表 + 靶槽坐标）
struct RcChain {
  /// 靶键
  key: Vec<u8>,
  /// 主日志真身地址（快照该槽应折成的目标值，不随代际变化）
  main: u64,
  /// 逐代 RC 打标地址：索引 0 为**最旧**代（prev 即主日志真身），末位为槽头（最新代）
  gens: Vec<u64>,
  /// 靶槽在靶桶内的下标（快照与 live 索引同布局，按坐标取原始槽位字）
  slot: usize,
}

impl RcChain {
  /// 槽头（最新代）地址，即索引槽位当前携带的 ReadCache 打标值
  fn head(&self) -> u64 {
    *self.gens.last().expect("至少一代")
  }
}

/// 靶键灌主日志记录后逐代晋升出 `gens` 代 ReadCache 记录链，返回链形与靶槽坐标
async fn seed_rc_chain(store: &MiniStore, p: &Participant, gens: usize) -> Result<RcChain> {
  let key = pick_nonzero_tag_key(MID_MARK);
  let main = store.put(p, &key, b"hot_value").await?;
  assert_ne!(main, 0, "前置条件：靶记录主日志地址必须非零");
  // 首代晋升：槽位改指 `main + 基址` 打标地址（其 prev 即主日志真身）
  store.install_read_cache_entry(p, &key)?;
  let mut chain = vec![slot_address(store, p, &key)?];
  for _ in 1..gens {
    chain.push(store.push_read_cache_generation(p, &key)?);
  }
  assert_ne!(
    chain.last().copied().expect("至少一代") & HashBucketEntry::READ_CACHE_BIT,
    0,
    "前置条件：槽头必须是 ReadCache 形态"
  );
  let bucket = store.index.bucket_index_for_key(&key);
  let head = *chain.last().expect("至少一代");
  let slot = store.index.bucket(bucket).entries[..HashBucket::DATA_ENTRIES]
    .iter()
    .position(|s| s.load(Ordering::Acquire) & HashBucketEntry::ADDRESS_MASK == head)
    .expect("前置条件：槽头必须落在主桶数据槽区（本套件构造不触及溢出链）");
  Ok(RcChain {
    key,
    main,
    gens: chain,
    slot,
  })
}

/// 取该键槽位当前携带的地址字（含 ReadCache 指示位，已剥离高 16 位指纹）
fn slot_address(store: &MiniStore, p: &Participant, key: &[u8]) -> Result<u64> {
  let _guard = p.enter();
  store
    .index
    .find_tag(key)
    .ok_or_else(|| aok::anyhow!("槽位读取：键不存在: {key:?}"))
}

/// 在靶桶内自 `after` 下标起按地址定位槽位下标（同指纹碰撞候选并存时唯一可靠的定位方式）
fn slot_index_of_after(store: &MiniStore, key: &[u8], after: usize, addr: u64) -> usize {
  let bucket = store.index.bucket_index_for_key(key);
  (after + 1..HashBucket::DATA_ENTRIES)
    .find(|&i| {
      let raw = store.index.bucket(bucket).entries[i].load(Ordering::Acquire);
      raw & HashBucketEntry::ADDRESS_MASK == addr
    })
    .expect("槽位定位失败")
}

/// 在指定键的靶桶坐标取原始槽位字（快照与 live 索引逐槽位同布局）
fn raw_slot(index: &HashIndex, key: &[u8], slot: usize) -> u64 {
  index.bucket(index.bucket_index_for_key(key)).entries[slot].load(Ordering::Acquire)
}

/// 链中段（槽头之更旧一侧）滑出、槽头仍在窗：快照该槽必须折成主日志真身地址且
/// 指纹保留，等待恰一轮收敛，恢复后键可见。同时挂一条**同桶同指纹**碰撞兄弟候选，
/// 断言重读槽位重探的靶点不漂移、兄弟槽原样落盘。
///
/// 锚定链头的旧实现在此形态下等待数为 0（链头在窗直判「不必等待」）并把槽位归零，
/// 故「等待恰一轮」与「快照槽非零」两条断言即缺陷判别面
#[compio::test]
async fn mid_chain_eviction_snapshot_keeps_live_key_visible() -> Void {
  // 等待重探环若退化（永不落定）会让本用例挂死，看门狗负责暴露
  let _watchdog = Watchdog::start("rc_mid_chain", Duration::from_secs(20));
  let dir = tempdir()?;
  let ckpt_dir = dir.path().join("checkpoints");
  let db_path = dir.path().join("rc_mid_chain.db");
  let store = MiniStore::open(&db_path)?;
  let gate = CkptGateState::default();
  let p = store.session()?;

  // 哨兵键既作相邻键回归，又作同指纹碰撞兄弟项指向的真实记录
  let sibling_key = b"user:mid:sentinel".to_vec();
  let sibling_addr = store.put(&p, &sibling_key, b"safe").await?;
  let chain = seed_rc_chain(&store, &p, 3).await?;
  // 同指纹碰撞兄弟项：把哨兵键的真实记录以靶键指纹再挂一条候选进靶桶
  // （对标真实索引里指纹撞车后共存于同桶的同 tag 条目集；空槽分配取最低空闲位，
  // 故必落在靶槽之后）
  let tag = HashBucketEntry::tag_from_hash(HashIndex::hash_key(&chain.key));
  assert_ne!(tag, 0, "前置条件：测试键指纹必须非零");
  store.index.insert_to_bucket(
    store.index.bucket_index_for_key(&chain.key),
    tag,
    sibling_addr,
  )?;
  let sib_slot = slot_index_of_after(&store, &chain.key, chain.slot, sibling_addr);
  assert_eq!(
    slot_address(&store, &p, &chain.key)?,
    chain.head(),
    "构造前提：碰撞兄弟项不得抢占靶键的 find_tag 命中位"
  );
  // 滑出点取最旧代（驱逐自最老侧推进的必然首站），它相对槽头即「链中段」
  let gone = chain.gens[0];
  assert_ne!(
    gone,
    chain.head(),
    "构造前提：滑出位置必须落在链中段而非槽头"
  );
  store.arm_read_cache_eviction_at(gone);
  drop(p);

  let meta = wcpr::create_checkpoint(&store, &gate, &ckpt_dir, CheckpointType::FoldOver).await?;
  assert!(
    chain.main < meta.index_start_logical_address,
    "前置条件：靶记录必须落在模糊区地板之下（恢复重插域不含它，快照归零即永久漏键）"
  );

  // ① 核心判据：快照槽折成主日志真身地址（旧实现锚链头判不出中段滑出 → 整槽归零）
  let (snapshot, _) =
    read_index_checkpoint_truncated(ckpt_dir.join(index_filename(meta.token)), meta.token, None)
      .await?;
  assert_eq!(
    snapshot.find_tag(&chain.key),
    Some(chain.main),
    "链中段滑出的存活键必须回写主日志地址入快照，绝不可整槽归零"
  );

  // ② 槽位字逐字段校验：仅换写低 48 位，高 16 位指纹原样保留、ReadCache 位无残留
  let raw = raw_slot(&snapshot, &chain.key, chain.slot);
  assert_eq!(
    raw & HashBucketEntry::ADDRESS_MASK,
    chain.main,
    "快照靶槽地址字段必须是主日志真实地址"
  );
  assert_eq!(
    (raw & HashBucketEntry::TAG_POS_MASK) >> HashBucketEntry::TAG_SHIFT,
    tag as u64,
    "快照靶槽指纹必须原样保留（否则恢复期 find_tag 永失配）"
  );
  assert_eq!(
    raw & HashBucketEntry::READ_CACHE_BIT,
    0,
    "快照靶槽不得残留 ReadCache 指示位"
  );
  // 碰撞兄弟槽原样透传：等待重探只针对靶槽，绝不牵连同桶同指纹兄弟项
  let sib_raw = raw_slot(&snapshot, &chain.key, sib_slot);
  assert_eq!(
    sib_raw & HashBucketEntry::ADDRESS_MASK,
    sibling_addr,
    "同指纹碰撞兄弟槽必须原样落盘（重读槽位重探的靶点不得漂移）"
  );
  assert_eq!(
    (sib_raw & HashBucketEntry::TAG_POS_MASK) >> HashBucketEntry::TAG_SHIFT,
    tag as u64,
    "同指纹碰撞兄弟槽指纹不得被改写"
  );

  // ③ 等待有界：中段清洗落定的那一轮即收敛，链头不被重复等待
  assert_eq!(
    store.rc_evict_waits(),
    1,
    "中段滑出必须恰好触发一轮驱逐等待（0 = 仍按链头锚定，>1 = 重探空转）"
  );
  // live 槽位仍指链头（清洗方只缝中段断口），缝链后的再走查零等待收敛到主日志真身
  let bucket = store.index.bucket_index_for_key(&chain.key);
  let slot = &store.index.bucket(bucket).entries[chain.slot];
  assert_eq!(
    slot.load(Ordering::Acquire) & HashBucketEntry::ADDRESS_MASK,
    chain.head(),
    "清洗只缝链中断口，live 槽位仍应指链头"
  );
  let waits_before = store.rc_evict_waits();
  assert_eq!(
    store.skip_read_cache_with_wait(slot),
    chain.main,
    "缝链后的走查必须解析到主日志真身"
  );
  assert_eq!(
    store.rc_evict_waits(),
    waits_before,
    "已落定链形的再走查不得产生任何等待（快照不被拖成长自旋）"
  );

  // ④ 恢复面：销毁重建后该键必须可见（旧缺陷下此处 GET 永久为空）
  drop(store);
  let device = Arc::new(SegmentedDevice::single_file(&db_path)?);
  let restored = wcpr::recover::<_, MiniStore>(&ckpt_dir, meta.token, device).await?;
  let p2 = restored.session()?;
  assert_eq!(
    restored.index.find_tag(&chain.key),
    Some(chain.main),
    "恢复索引必须携回该键"
  );
  assert_eq!(
    restored.get(&p2, &chain.key).await?.as_deref(),
    Some(b"hot_value".as_slice()),
    "恢复后 GET 必须可见该存活键"
  );
  assert_eq!(
    restored.get(&p2, &sibling_key).await?.as_deref(),
    Some(b"safe".as_slice()),
    "哨兵键不得受等待重探环影响"
  );
  OK
}

/// 驱逐同时推进过链上多代（靠旧侧整段滑出）：单口内「等待→回链头重读槽位→再走查」
/// 环必须逐处落定并有界收敛（每处恰一轮），快照仍折成主日志真身
#[compio::test]
async fn multi_position_mid_chain_eviction_converges_bounded() -> Void {
  let _watchdog = Watchdog::start("rc_mid_chain_multi", Duration::from_secs(20));
  let dir = tempdir()?;
  let ckpt_dir = dir.path().join("checkpoints");
  let db_path = dir.path().join("rc_mid_chain_multi.db");
  let store = MiniStore::open(&db_path)?;
  let gate = CkptGateState::default();
  let p = store.session()?;

  let sentinel_key = b"user:mid:multi:sentinel".to_vec();
  store.put(&p, &sentinel_key, b"safe").await?;
  let chain = seed_rc_chain(&store, &p, 4).await?;
  // 最旧的两代同时滑出（环形窗口整体前移的常态形态），槽头及其紧邻代仍在窗
  let evicted = &chain.gens[..chain.gens.len() - 2];
  assert_eq!(evicted.len(), 2, "构造前提：滑出集合为链上中段两处");
  for &gone in evicted {
    store.arm_read_cache_eviction_at(gone);
  }
  drop(p);

  let meta = wcpr::create_checkpoint(&store, &gate, &ckpt_dir, CheckpointType::FoldOver).await?;
  assert!(
    chain.main < meta.index_start_logical_address,
    "前置条件：靶记录必须落在模糊区地板之下"
  );

  let (snapshot, _) =
    read_index_checkpoint_truncated(ckpt_dir.join(index_filename(meta.token)), meta.token, None)
      .await?;
  assert_eq!(
    snapshot.find_tag(&chain.key),
    Some(chain.main),
    "多处中段滑出同样必须折成主日志地址，不得整槽归零"
  );
  assert_eq!(
    store.rc_evict_waits(),
    evicted.len(),
    "每处滑出恰一轮等待（重探环不得对已落定地址空转，也不可漏判第二处）"
  );

  drop(store);
  let device = Arc::new(SegmentedDevice::single_file(&db_path)?);
  let restored = wcpr::recover::<_, MiniStore>(&ckpt_dir, meta.token, device).await?;
  let p2 = restored.session()?;
  assert_eq!(
    restored.get(&p2, &chain.key).await?.as_deref(),
    Some(b"hot_value".as_slice()),
    "恢复后 GET 必须可见该存活键"
  );
  assert_eq!(
    restored.get(&p2, &sentinel_key).await?.as_deref(),
    Some(b"safe".as_slice()),
    "哨兵键不得受等待重探环影响"
  );
  OK
}
