//! grow 迁移窗并发紧缩活键保全回归（wkv-growwindow-compact-index-probe-miss-live-drop）
//!
//! 危害链对位 C#：紧缩面 NOTFOUND 保守补拷臂（TsavoriteCompaction.cs:45-55 →
//! FindRecord.cs:40-43 → ConditionalCopyToTail.cs:111-113）保证探针未命中绝不弃迁；
//! 修复前本仓紧缩探针直查活跃表（grow 窗内未迁分块键查空）判 superseded 弃迁，
//! begin 截断越过即成批静默丢键；TTL/ETag 宿主探查同窗误判孤儿（TTL 静默消失）。
//! 修复后探针与宿主探查同过会话侧协同门（ensure_split / find_tag_cooperative，
//! 与读面/扫描面同构）。本回归以真实 IN_PROGRESS_GROW 装配窗（stage_grow_window
//! 确定性切新表、全分块未迁）驱动 Lookup/Scan 两档紧缩锁定：活键零弃迁、
//! 旁路记录零误杀、收窗后数据完好。

use aok::{OK, Void};
use wbase::{convert::TICKS_PER_SECOND, time::now_ticks};
use wcompact::CompactionType;
use wtest_base::open_test_store;

use super::support::{finish_grow_window, stage_grow_window};

/// 两档紧缩在 grow 迁移窗内保全全部活键：探针协同先行，零弃迁零保留
async fn grow_window_preserves_live_keys(comp_type: CompactionType, tag: &str) -> Void {
  let (_dir, store) = open_test_store(tag)?;
  let session = store.new_session()?;

  const TOTAL: usize = 8;
  for i in 0..TOTAL {
    session
      .upsert(format!("gw:{i}").as_bytes(), b"payload")
      .await?;
  }
  store.flush_all().await?;
  let tail = store.tail_address();
  store.shift_read_only_address(tail);

  // 装配 IN_PROGRESS_GROW：2 倍容量新表在场、全部分块未迁移
  stage_grow_window(&store);

  let stats = store.compact(tail, comp_type).await?;
  assert_eq!(
    stats.superseded, 0,
    "未迁分块活键不得因探针空候选被误判 superseded 弃迁: {stats:?}"
  );
  assert_eq!(stats.dead_dropped, 0, "活键不得被误判死亡: {stats:?}");
  assert_eq!(
    stats.live_copied, TOTAL,
    "全部活键必须经协同门保全迁移: {stats:?}"
  );

  finish_grow_window(&store);
  for i in 0..TOTAL {
    assert_eq!(
      session.read(format!("gw:{i}").as_bytes()).await?.as_deref(),
      Some(b"payload".as_slice()),
      "grow 窗紧缩收窗后活键必须完好可见（begin 截断绝未越过弃迁记录）"
    );
  }
  OK
}

/// TTL/ETag 旁路记录 grow 窗内不误判孤儿：宿主条目未迁即新表查空，
/// 宿主探查经 find_tag_cooperative 协同先行，旁路记录随宿主同荣
async fn grow_window_keeps_sidecars(comp_type: CompactionType, tag: &str) -> Void {
  let (_dir, store) = open_test_store(tag)?;
  let session = store.new_session()?;

  let key = b"gw:sidecar";
  session.upsert(key, b"v").await?;
  // 免 sleep 挂远期 TTL 与 ETag 对偶旁路记录（宿主存活，紧缩不得误杀）
  session
    .put_ttl(key, now_ticks() + 60 * TICKS_PER_SECOND)
    .await?;
  session.put_etag(key, 9).await?;
  store.flush_all().await?;
  let tail = store.tail_address();
  store.shift_read_only_address(tail);

  stage_grow_window(&store);

  let stats = store.compact(tail, comp_type).await?;
  assert_eq!(
    stats.dead_dropped, 0,
    "grow 窗内 TTL/ETag 宿主未迁不得判孤儿丢弃（TTL 静默消失洞）: {stats:?}"
  );
  assert_eq!(stats.superseded, 0, "旁路与宿主记录均须保全: {stats:?}");
  assert_eq!(
    stats.live_copied, 3,
    "String + Ttl + Etag 三条活记录全量迁移: {stats:?}"
  );

  finish_grow_window(&store);
  assert_eq!(session.read(key).await?.as_deref(), Some(b"v".as_slice()));
  assert!(
    session.ttl_of(key).await?.is_some(),
    "TTL 旁路记录不得因宿主条目未迁被误判孤儿"
  );
  assert_eq!(
    session.etag_of(key).await?,
    Some(9),
    "ETag 对偶记录须随宿主保全"
  );
  OK
}

#[compio::test]
async fn compact_lookup_grow_window_preserves_live_keys() -> Void {
  grow_window_preserves_live_keys(CompactionType::Lookup, "growwin_lookup").await
}

#[compio::test]
async fn compact_scan_grow_window_preserves_live_keys() -> Void {
  grow_window_preserves_live_keys(CompactionType::Scan, "growwin_scan").await
}

#[compio::test]
async fn compact_lookup_grow_window_keeps_ttl_etag_sidecars() -> Void {
  grow_window_keeps_sidecars(CompactionType::Lookup, "growwin_sidecar_lookup").await
}

#[compio::test]
async fn compact_scan_grow_window_keeps_ttl_etag_sidecars() -> Void {
  grow_window_keeps_sidecars(CompactionType::Scan, "growwin_sidecar_scan").await
}
