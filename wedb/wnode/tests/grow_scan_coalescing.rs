//! 扩容迁移窗 SCAN / KEYS / DBSIZE / 槽位枚举活键判定分裂协同收口回归
//! （票 zcode-r135c-rehash 案一）
//!
//! 自研依据: 增量扫描游标臂活键探针经 ensure_split_by_hash 协同单点收口
//! （对标 C# libs/storage/Tsavorite/cs/src/core/Index/Tsavorite/
//! Implementation/InternalRead.cs:70-73 入口铁律「phase == IN_PROGRESS_GROW →
//! SplitBuckets(hei.hash) 先协同、后 FindTag」与
//! libs/server/Storage/Session/Common/ArrayKeyIterationFunctions.cs:DbScan /
//! AllocatorScan 消费面；C# 上游 ConditionalScanPush 链首探针同缺协同系
//! 既有缺陷面，本仓扫描族收口为修复型改进）
//!
//! 证伪口径（修复前必红）：InProgressGrow 装配下后台迁移未启动，活跃新表
//! 全桶恒空；scan_cursor / db_size / db_keys / 槽位枚举的存活判定单点以裸
//! find_tag 采得 None，全部活键被误判链首不符剔除，SCAN 游标按地址推进
//! 固化全程漏键（每页空扫至 tail 终态、DBSIZE/KEYS 恒 0）。修复后探针经
//! [`wkv::StoreSession::find_tag_cooperative`] 协同单点（与点读 read_probe /
//! TTL has_ttl_key_unprotected 同一套 `ensure_split_by_hash` 机制，非第二套），
//! 首触分块即被会话协同抢占迁移，全族命令零漏键收敛。

use std::{
  collections::HashSet,
  sync::{
    Arc,
    atomic::{AtomicI64, Ordering},
  },
};

use aok::Void;
use tempfile::tempdir;
use wbase::{align::DEFAULT_SECTOR_SIZE, hash_slot::slot_of};
use wdev::SegmentedDevice;
use windex::{HashIndex, SPLIT_COMPLETED, SPLIT_UNSTARTED, chunk_count, chunk_offset_for_hash};
use wkv::{Error as WkvError, StoreConfig, WedbStore, store::ResizePhase};
use wnode::storage::session::storage_session::StorageSession;

/// 扩容全态装配（严格对齐 grow_index 第 2~3 步发布次序：split_status /
/// num_pending_chunks / old_index 就绪 → 先切新表、后发相位），不发后台
/// 迁移驱动——全部分块留待前台会话协同按需迁移
fn assemble_in_progress_grow(store: &Arc<WedbStore<SegmentedDevice>>) -> Result<(), WkvError> {
  let old_index = store.active_index();
  let num_chunks = chunk_count(old_index.size);
  store.resize.split_status.store(Arc::new(
    (0..num_chunks)
      .map(|_| AtomicI64::new(SPLIT_UNSTARTED))
      .collect(),
  ));
  store
    .resize
    .num_pending_chunks
    .store(num_chunks, Ordering::Release);
  store.resize.old_index.store(Some(Arc::clone(&old_index)));
  store
    .index
    .store(Arc::new(HashIndex::new(old_index.size * 2)?));
  store
    .resize
    .phase
    .store(ResizePhase::InProgressGrow as u8, Ordering::Release);
  Ok(())
}

/// 扩容迁移窗收尾复位（与 wkv resize 装配 teardown 同形：先相位收口、后资源回收）
fn teardown_resize_state(store: &WedbStore<SegmentedDevice>) {
  store
    .resize
    .phase
    .store(ResizePhase::Rest as u8, Ordering::Release);
  store.resize.old_index.store(None);
  store.resize.split_status.store(Arc::new(Vec::new()));
}

/// grow×SCAN 分裂协同：迁移窗内有界分页游标全量覆盖 + 无重复 + 全族命令
/// 收敛一致；协同迁移确由既有 split_buckets 单点完成（分块状态推进至
/// SPLIT_COMPLETED、待迁计数清零，不造第二套机制）；收口复位后稳态复扫
/// 视图逐键一致
#[compio::test]
async fn test_scan_during_grow_covers_all_keys_via_split_coalescing() -> Void {
  let dir = tempdir()?;
  let device = Arc::new(SegmentedDevice::single_file(
    dir.path().join("grow_scan_coop.db"),
  )?);
  // 32768 桶 = 2 分块（CHUNK_BITS=14），新表 65536 桶
  let config = StoreConfig::new(32768, DEFAULT_SECTOR_SIZE, 16, 0.5)?;
  let store = Arc::new(WedbStore::open(config, device)?);
  let session = store.new_session()?;
  let ss = StorageSession::new(session.enter_batch());

  const N: usize = 2000;
  let mut users: Vec<Vec<u8>> = Vec::new();
  let mut phys: Vec<Vec<u8>> = Vec::new();
  for i in 0..N {
    let user = format!("coop_key:{i}").into_bytes();
    ss.upsert_string(&user, b"v").await?;
    phys.push(session.session_string_key(&user).as_slice().to_vec());
    users.push(user);
  }

  // 夹具前提：键集横跨两分块（未迁滞留面真实存在）
  let old_index = store.active_index();
  let mut in_chunk0 = 0usize;
  for p in &phys {
    if chunk_offset_for_hash(HashIndex::hash_key(p), old_index.mask) == 0 {
      in_chunk0 += 1;
    }
  }
  assert!(in_chunk0 > 0 && in_chunk0 < N, "夹具键集必须横跨两分块");

  // 手工装配 InProgressGrow 且不发后台驱动：全部条目滞留旧表，活跃新表
  // 对应桶恒空（修复前裸探针面即在此黑洞）
  assemble_in_progress_grow(&store)?;
  assert_eq!(store.active_index().size, 65536);
  assert!(store.is_growing());

  // 1. SCAN 有界分页全覆盖：小页游标推进，联合视图恰为全键集、无重复
  let mut seen: HashSet<Vec<u8>> = HashSet::new();
  let mut cursor = 0u64;
  let mut pages = 0usize;
  loop {
    let (next, page) = ss.scan_cursor(b"*", true, cursor, 50, None).await?;
    for k in page {
      assert!(seen.insert(k), "SCAN 跨页重复键（游标寻址撕裂）");
    }
    pages += 1;
    if next == 0 {
      break;
    }
    cursor = next;
    assert!(pages < 200, "SCAN 分页必须在有限轮内终态");
  }
  assert_eq!(
    seen.len(),
    N,
    "修复前裸 find_tag 将未迁分块活键全数误判剔除，SCAN 全程漏键"
  );

  // 2. DBSIZE / KEYS / 槽位枚举同判据收敛（同一存活判定单点的多消费面）
  assert_eq!(ss.db_size().await?, N);
  assert_eq!(ss.db_keys(b"*").await?.len(), N);
  let slot = slot_of(0, 0);
  assert_eq!(ss.count_keys_in_slot(slot).await?, N);
  assert_eq!(ss.get_keys_in_slot(slot, usize::MAX).await?.len(), N);

  // 3. 协同单点证据：探针经既有 split_buckets 机制按需迁移，两分块均推进
  //    至 SPLIT_COMPLETED、待迁计数清零——未造第二套协同旁路
  let status = store.resize.split_status.load();
  for s in status.iter() {
    assert_eq!(s.load(Ordering::Acquire), SPLIT_COMPLETED);
  }
  assert_eq!(store.resize.num_pending_chunks.load(Ordering::Acquire), 0);

  // 4. 点读一致：协同迁移后全部键可逐点读回（扫描面与点读面同源收敛）
  for user in &users {
    assert_eq!(session.read(user).await?, Some(b"v".to_vec()));
  }

  // 5. 收口复位后稳态回归：大页复扫与迁移窗视图逐键一致
  teardown_resize_state(&store);
  let mut cursor = 0u64;
  let mut seen2: HashSet<Vec<u8>> = HashSet::new();
  loop {
    let (next, page) = ss.scan_cursor(b"*", true, cursor, 700, None).await?;
    seen2.extend(page);
    if next == 0 {
      break;
    }
    cursor = next;
  }
  assert_eq!(seen2, seen, "稳态复扫必须与迁移窗视图逐键一致");
  Ok(())
}
