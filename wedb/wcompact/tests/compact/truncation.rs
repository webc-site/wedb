//! 截断边界：until 落点对齐记录边界、记录跨出只读区快照的起始边界回退、
//! CAS 预算耗尽的保守保留回退与惰性紧缩窗口

use std::sync::Arc;

use aok::{OK, Void};
use compio::runtime::Runtime;
use tempfile::tempdir;
use wcompact::{CompactionType, LogCompactor};

use super::support::FixtureStore;

/// until 落在记录中部：截断点自动对齐至该记录的结束边界（整记录处理）
#[test]
fn until_mid_record_aligns_to_record_end() -> Void {
  let rt = Runtime::new()?;
  rt.block_on(async {
    let dir = tempdir()?;
    let store = FixtureStore::open(dir.path().join("align.db"))?;
    let s = store.session()?;

    store.put(&s, b"key:first", b"v1").await?;
    let addr_k2 = store.put(&s, b"key:second", b"v2").await?;
    let tail = store.hlog.tail_address();
    store.seal_read_only(tail);

    // until 深入第二条记录内部：紧缩必须覆盖至其结束边界（记录不可截半）
    let compactor = LogCompactor::new(Arc::clone(&store));
    let stats = compactor
      .compact(addr_k2 + 3, CompactionType::Lookup)
      .await?;

    assert_eq!(
      stats.scanned_records, 2,
      "落点内侧的两条记录都必须处理: {stats:?}"
    );
    assert_eq!(stats.live_copied, 2);
    assert_eq!(
      stats.new_begin_address, tail,
      "截断点必须对齐到末记录结束边界（即 tail）"
    );
    assert_eq!(
      store.get(&s, b"key:first").await?.as_deref(),
      Some(b"v1".as_slice())
    );
    assert_eq!(
      store.get(&s, b"key:second").await?.as_deref(),
      Some(b"v2".as_slice())
    );

    aok::Result::<()>::Ok(())
  })?;
  OK
}

/// 记录跨出只读区快照：截断点回退至该记录起始边界，记录原位保留、数据无损
#[test]
fn record_crossing_read_only_falls_back_to_start() -> Void {
  let rt = Runtime::new()?;
  rt.block_on(async {
    let dir = tempdir()?;
    let store = FixtureStore::open(dir.path().join("crossing.db"))?;
    let s = store.session()?;

    store.put(&s, b"key:first", b"v1").await?;
    let addr_k2 = store.put(&s, b"key:second", b"v2").await?;

    // 只读边界封印在第二条记录中部：其结束边界越出快照
    store.seal_read_only(addr_k2 + 2);

    let compactor = LogCompactor::new(Arc::clone(&store));
    let stats = compactor
      .compact(addr_k2 + 2, CompactionType::Lookup)
      .await?;

    // 跨界记录不计入扫描，截断点回退至其起始边界
    assert_eq!(stats.scanned_records, 1, "跨界记录不得计入扫描: {stats:?}");
    assert_eq!(stats.live_copied, 1, "区间内记录照常迁移");
    assert_eq!(
      stats.new_begin_address, addr_k2,
      "截断点必须回退至跨界记录起始边界"
    );

    // 回退记录原位保留：数据完好可读
    assert_eq!(
      store.get(&s, b"key:first").await?.as_deref(),
      Some(b"v1".as_slice())
    );
    assert_eq!(
      store.get(&s, b"key:second").await?.as_deref(),
      Some(b"v2".as_slice())
    );

    aok::Result::<()>::Ok(())
  })?;
  OK
}

/// CAS 预算归零：全部存活记录保守保留（Retain），截断点回退至最早存活记录起始边界，
/// 孤儿归还为零（预算耗尽时不产生任何迁移副本），数据绝无误删
#[test]
fn zero_retry_budget_retains_and_rolls_back_truncation() -> Void {
  let rt = Runtime::new()?;
  rt.block_on(async {
    let dir = tempdir()?;
    let store = FixtureStore::open(dir.path().join("retain.db"))?;
    let s = store.session()?;

    let addr_k1 = store.put(&s, b"key:a", b"va").await?;
    store.put(&s, b"key:b", b"vb").await?;
    store.put(&s, b"key:c", b"vc").await?;
    let tail = store.hlog.tail_address();
    store.seal_read_only(tail);

    // CAS 重试预算为 0：存活记录不迁移、直接保守保留
    let compactor = LogCompactor::with_cas_retries(Arc::clone(&store), 0);
    let stats = compactor.compact(tail, CompactionType::Lookup).await?;

    assert_eq!(stats.scanned_records, 3);
    assert_eq!(stats.retained, 3, "全部存活记录必须保守保留: {stats:?}");
    assert_eq!(stats.live_copied, 0);
    assert_eq!(stats.superseded, 0);
    assert!(
      store.reviv_puts_snapshot().is_empty(),
      "保守保留路径不得产生任何孤儿副本"
    );
    assert_eq!(
      stats.new_begin_address, addr_k1,
      "截断点必须回退至最早存活记录起始边界"
    );

    // 三键数据绝无误删
    assert_eq!(
      store.get(&s, b"key:a").await?.as_deref(),
      Some(b"va".as_slice())
    );
    assert_eq!(
      store.get(&s, b"key:b").await?.as_deref(),
      Some(b"vb".as_slice())
    );
    assert_eq!(
      store.get(&s, b"key:c").await?.as_deref(),
      Some(b"vc".as_slice())
    );

    aok::Result::<()>::Ok(())
  })?;
  OK
}

/// 惰性紧缩窗口：预算 0 空转、预算充足时按只读区全量推进
#[test]
fn lazy_compaction_window_bounded_by_budget() -> Void {
  let rt = Runtime::new()?;
  rt.block_on(async {
    let dir = tempdir()?;
    let store = FixtureStore::open(dir.path().join("lazy.db"))?;
    let s = store.session()?;

    store.put(&s, b"key:a", b"va").await?;
    store.put(&s, b"key:b", b"vb").await?;
    let tail = store.hlog.tail_address();
    store.seal_read_only(tail);

    let compactor = LogCompactor::new(Arc::clone(&store));

    // 单轮预算 0：等价紧缩关闭，空统计直返
    let stats = compactor.compact_lazy(0).await?;
    assert!(stats.is_empty(), "预算 0 必须空转: {stats:?}");
    assert_eq!(stats.new_begin_address, store.hlog.begin_address());

    // 预算充足：窗口推进至只读区（min(RO, begin+max)），全部记录迁移
    let stats = compactor.compact_lazy(u64::MAX).await?;
    assert_eq!(stats.scanned_records, 2);
    assert_eq!(stats.live_copied, 2);
    assert_eq!(stats.new_begin_address, tail, "全量预算必须推进至只读区");
    assert_eq!(
      store.get(&s, b"key:a").await?.as_deref(),
      Some(b"va".as_slice())
    );
    assert_eq!(
      store.get(&s, b"key:b").await?.as_deref(),
      Some(b"vb".as_slice())
    );

    aok::Result::<()>::Ok(())
  })?;
  OK
}

/// until 位于历史记录之后但小于只读区：截断点绝不错误跳页越过未扫描记录
#[test]
fn compaction_does_not_overextend_past_until_address() -> Void {
  let rt = Runtime::new()?;
  rt.block_on(async {
    let dir = tempdir()?;
    let store = FixtureStore::open(dir.path().join("overextend.db"))?;
    let s = store.session()?;

    let _addr1 = store.put(&s, b"k1", b"v1").await?;
    let addr2 = store.put(&s, b"k2", b"v2").await?;
    let _addr3 = store.put(&s, b"k3", b"v3").await?;
    let tail = store.hlog.tail_address();
    store.seal_read_only(tail);

    let compactor = LogCompactor::new(Arc::clone(&store));
    // 仅要求紧缩到 addr2（即只紧缩 k1）
    let stats = compactor.compact(addr2, CompactionType::Lookup).await?;
    assert_eq!(stats.scanned_records, 1);
    assert_eq!(stats.live_copied, 1);
    assert_eq!(
      stats.new_begin_address, addr2,
      "新起始地址必须精确落在 k1 结束边界即 addr2"
    );

    // 全部记录依然完好可读
    assert_eq!(
      store.get(&s, b"k1").await?.as_deref(),
      Some(b"v1".as_slice())
    );
    assert_eq!(
      store.get(&s, b"k2").await?.as_deref(),
      Some(b"v2".as_slice())
    );
    assert_eq!(
      store.get(&s, b"k3").await?.as_deref(),
      Some(b"v3".as_slice())
    );

    // 同样验证 Scan 模式
    let stats_scan = compactor.compact(tail, CompactionType::Scan).await?;
    assert_eq!(stats_scan.scanned_records, 2);
    assert_eq!(stats_scan.new_begin_address, tail);
    assert_eq!(
      store.get(&s, b"k1").await?.as_deref(),
      Some(b"v1".as_slice())
    );
    assert_eq!(
      store.get(&s, b"k2").await?.as_deref(),
      Some(b"v2".as_slice())
    );
    assert_eq!(
      store.get(&s, b"k3").await?.as_deref(),
      Some(b"v3".as_slice())
    );

    aok::Result::<()>::Ok(())
  })?;
  OK
}
