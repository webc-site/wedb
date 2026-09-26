//! grow 索引扩容迁移窗内紧缩探针回归（wkv-growwindow-compact-index-probe-miss-live-drop）
//!
//! 对标 C# 紧缩面 NOTFOUND 保守补拷臂（TsavoriteCompaction.cs:45-55 逐活记录
//! CompactionCopyToTail → FindRecord.cs:40-43 FindTag 未命中落 NOTFOUND →
//! ConditionalCopyToTail.cs:111-113 无条件补拷插回，绝不因探针未命中弃迁记录）；
//! 本仓以读面/扫描面同构的会话侧分裂协同门收口紧缩面（探针 `find_latest_address`
//! 入口 `CompactSession::ensure_split` 先于 `lookup_candidates`）。
//!
//! 锁定不变式：探针 growing 期不得直采空候选判 superseded 弃迁活键。注入钩仿读面
//! `test_read_gap_hook` 形态（wcompact 探针独立留钩，非复用读面钩子）：协同门若失效，
//! 钩体在采样间隙内确定性收窗翻回 Rest，陈旧空候选即被上判 superseded——回归以
//! 「钩零触发 + 活键零弃迁 + 数据完好」三断言当场捕获。

use std::sync::{
  Arc,
  atomic::{AtomicUsize, Ordering},
};

use aok::{OK, Void};
use tempfile::tempdir;
use wcompact::{CompactionType, LogCompactor};

use super::support::{FixtureStore, str_key};

/// Lookup 档：grow 窗内未迁分块活键经协同门保全迁移，不收窗即永久不可见
#[compio::test]
async fn lookup_probe_grow_window_no_stale_supersede() -> Void {
  let dir = tempdir()?;
  let store = FixtureStore::open(dir.path().join("grow_lookup.db"))?;
  let s = store.session()?;

  let k1 = str_key(b"gw_a");
  let k2 = str_key(b"gw_b");
  store.put(&s, &k1, b"alpha").await?;
  store.put(&s, &k2, b"beta").await?;
  let tail = store.hlog.tail_address();
  store.seal_read_only(tail);

  // 装配 grow 迁移窗：两键索引条目摘入未迁暂存，活跃表（新表）探得空候选
  store.stage_grow_window(&[&k1, &k2]);

  let fired = Arc::new(AtomicUsize::new(0));
  let compactor = LogCompactor::new(Arc::clone(&store));
  {
    let store_hook = Arc::clone(&store);
    let fired_hook = Arc::clone(&fired);
    compactor.arm_probe_gap_hook(move || {
      fired_hook.fetch_add(1, Ordering::Relaxed);
      store_hook.finish_grow_window();
    });
  }

  let stats = compactor.compact(tail, CompactionType::Lookup).await?;

  assert_eq!(
    fired.load(Ordering::Relaxed),
    0,
    "探针 growing 期不得直采空候选（触发即协同门失守，陈旧空候选被直判 superseded）"
  );
  assert_eq!(
    stats.superseded, 0,
    "未迁分块活键不得判 superseded 弃迁: {stats:?}"
  );
  assert_eq!(
    stats.live_copied, 2,
    "两活键必须经协同门迁移保全: {stats:?}"
  );

  // 收窗后数据完好（对位真实危害链：begin 截断越过弃迁记录即永久不可见）
  store.finish_grow_window();
  assert_eq!(
    store.get(&s, &k1).await?.as_deref(),
    Some(b"alpha".as_slice())
  );
  assert_eq!(
    store.get(&s, &k2).await?.as_deref(),
    Some(b"beta".as_slice())
  );
  OK
}

/// Scan 档：阶段 3 幸存候选探针同过协同门，未迁分块活键同样保全
#[compio::test]
async fn scan_probe_grow_window_no_stale_supersede() -> Void {
  let dir = tempdir()?;
  let store = FixtureStore::open(dir.path().join("grow_scan.db"))?;
  let s = store.session()?;

  let k1 = str_key(b"gw_c");
  let k2 = str_key(b"gw_d");
  store.put(&s, &k1, b"gamma").await?;
  store.put(&s, &k2, b"delta").await?;
  let tail = store.hlog.tail_address();
  store.seal_read_only(tail);

  store.stage_grow_window(&[&k1, &k2]);

  let fired = Arc::new(AtomicUsize::new(0));
  let compactor = LogCompactor::new(Arc::clone(&store));
  {
    let store_hook = Arc::clone(&store);
    let fired_hook = Arc::clone(&fired);
    compactor.arm_probe_gap_hook(move || {
      fired_hook.fetch_add(1, Ordering::Relaxed);
      store_hook.finish_grow_window();
    });
  }

  let stats = compactor.compact(tail, CompactionType::Scan).await?;

  assert_eq!(
    fired.load(Ordering::Relaxed),
    0,
    "探针 growing 期不得直采空候选（Scan 阶段 3 复核臂同过协同门）"
  );
  assert_eq!(
    stats.superseded, 0,
    "未迁分块活键不得判 superseded 弃迁: {stats:?}"
  );
  assert_eq!(
    stats.live_copied, 2,
    "两活键必须经协同门迁移保全: {stats:?}"
  );

  store.finish_grow_window();
  assert_eq!(
    store.get(&s, &k1).await?.as_deref(),
    Some(b"gamma".as_slice())
  );
  assert_eq!(
    store.get(&s, &k2).await?.as_deref(),
    Some(b"delta".as_slice())
  );
  OK
}

/// 对照负形：真并发新版本已生效（非扩容窗）时空候选照常判 superseded，
/// 协同门与注入钩零干扰，杜绝修复过度保守化阻断正常弃迁
#[compio::test]
async fn supersede_outside_grow_window_unaffected() -> Void {
  let dir = tempdir()?;
  let store = FixtureStore::open(dir.path().join("grow_neg.db"))?;
  let s = store.session()?;

  let k = str_key(b"gw_e");
  store.put(&s, &k, b"old").await?;
  // 并发新版本已生效：索引 CAS 顶掉旧槽位引用，旧记录探针空候选照常弃迁
  let other = store.session()?;
  store.put(&other, &k, b"new_value").await?;
  let tail = store.hlog.tail_address();
  store.seal_read_only(tail);

  let fired = Arc::new(AtomicUsize::new(0));
  let compactor = LogCompactor::new(Arc::clone(&store));
  let fired_hook = Arc::clone(&fired);
  let store_hook = Arc::clone(&store);
  compactor.arm_probe_gap_hook(move || {
    fired_hook.fetch_add(1, Ordering::Relaxed);
    store_hook.finish_grow_window();
  });

  let stats = compactor.compact(tail, CompactionType::Lookup).await?;
  assert_eq!(
    fired.load(Ordering::Relaxed),
    0,
    "非扩容窗空候选不得触碰注入钩"
  );
  assert!(
    stats.superseded >= 1,
    "真并发覆盖旧版本须照常弃迁: {stats:?}"
  );
  assert_eq!(
    store.get(&s, &k).await?.as_deref(),
    Some(b"new_value".as_slice())
  );
  OK
}
