//! 并发紧缩与 upsert 竞争：CAS 冲突良性重试路径、真覆盖（Superseded）不丢新值，
//! 以及写者与紧缩器并行推进的有界压力验证（看门狗保护）

use std::{sync::Arc, time::Duration};

use aok::{OK, Void};
use compio::runtime::Runtime;
use tempfile::tempdir;
use wcompact::{CompactStore, CompactionType, LogCompactor};
use wrecord::record_size;

use super::support::{FixtureStore, Inject, Watchdog, str_key};

const WATCHDOG_BUDGET: Duration = Duration::from_secs(60);

/// 良性竞争：紧缩迁移追加途中索引槽位被并发改写为 ReadCache 形态（主地址不变），
/// CAS 失败后必须换新期望槽位重试并最终复制成功，孤儿副本归还复活池
#[test]
fn benign_slot_rewrite_retries_and_copies() -> Void {
  let rt = Runtime::new()?;
  rt.block_on(async {
    let dir = tempdir()?;
    let store = FixtureStore::open(dir.path().join("benign.db"))?;
    let s = store.session()?;

    let k = str_key(b"benign");
    store.put(&s, &k, b"stable_value").await?;

    let tail = store.hlog.tail_address();
    store.seal_read_only(tail);

    // 注入：紧缩迁移的追加路径上，索引槽位被原位置上 ReadCache 位（良性改写）
    store.arm_inject(Inject::BenignReadCache { key: k.clone() });

    let compactor = LogCompactor::new(Arc::clone(&store));
    let stats = compactor.compact(tail, CompactionType::Lookup).await?;

    assert_eq!(stats.scanned_records, 1);
    assert_eq!(
      stats.live_copied, 1,
      "良性竞争重试后必须迁移成功: {stats:?}"
    );
    assert_eq!(stats.superseded, 0);
    assert_eq!(stats.retained, 0);

    // CAS 失败产生的孤儿副本必须归还复活池，尺寸精确
    let puts = store.reviv_puts_snapshot();
    assert_eq!(
      puts.len(),
      1,
      "良性 CAS 冲突必须恰好归还一个孤儿副本: {puts:?}"
    );
    let expected_size = record_size(k.len(), b"stable_value".len()) as u32;
    assert_eq!(puts[0].1, expected_size, "归还槽位尺寸必须与记录尺寸一致");
    assert!(puts[0].0 >= tail, "孤儿副本地址必须位于紧缩区间之外的尾部");

    // 数据完好：迁移后读取一致，且最终槽位为干净的新地址
    assert_eq!(
      store.get(&s, &k).await?.as_deref(),
      Some(b"stable_value".as_slice())
    );
    let candidates = store.index.lookup_candidates(&k);
    assert_eq!(candidates.len(), 1);
    assert!(
      !store.is_read_cache_addr(candidates.first().expect("非空")),
      "迁移完成后索引槽位不得残留 ReadCache 位"
    );

    aok::Result::<()>::Ok(())
  })?;
  OK
}

/// 真覆盖竞争：紧缩迁移途中索引槽位被并发新版本抢占（Superseded），
/// 旧版本安全弃迁、绝不反向覆盖新值，孤儿副本归还复活池
#[test]
fn steal_slot_superseded_never_loses_new_value() -> Void {
  let rt = Runtime::new()?;
  rt.block_on(async {
    let dir = tempdir()?;
    let store = FixtureStore::open(dir.path().join("steal.db"))?;
    let s = store.session()?;

    let k = str_key(b"contended");
    store.put(&s, &k, b"old_version").await?;

    let tail = store.hlog.tail_address();
    store.seal_read_only(tail);

    // 并发写者先行追加新版本（未发布索引，模拟「已写尾未挂链」窗口）
    let writer_addr = store.append_raw(&s, &k, b"new_version_wins").await?;
    assert!(writer_addr >= tail, "并发新版本必须写在紧缩区间之外");

    // 注入：紧缩迁移追加途中把索引槽位抢占至新版本地址
    store.arm_inject(Inject::StealSlot {
      key: k.clone(),
      to: writer_addr,
    });

    let compactor = LogCompactor::new(Arc::clone(&store));
    let stats = compactor.compact(tail, CompactionType::Lookup).await?;

    assert_eq!(stats.scanned_records, 1);
    assert_eq!(stats.live_copied, 0, "旧版本不得迁移");
    assert_eq!(stats.superseded, 1, "真并发覆盖必须计为弃迁: {stats:?}");
    assert_eq!(stats.retained, 0);

    // 新值完好无损，孤儿副本归还复活池
    assert_eq!(
      store.get(&s, &k).await?.as_deref(),
      Some(b"new_version_wins".as_slice()),
      "并发写入的新值绝不能被紧缩旧值覆盖"
    );
    let puts = store.reviv_puts_snapshot();
    assert_eq!(puts.len(), 1, "CAS 失败的孤儿副本必须归还复活池: {puts:?}");

    aok::Result::<()>::Ok(())
  })?;
  OK
}

/// 有界压力：写者持续覆盖与紧缩器并行推进，紧缩绝不复活旧值、绝不丢新值
#[test]
fn concurrent_upsert_and_compaction_bounded_stress() -> Void {
  let _watchdog = Watchdog::start("concurrent_upsert_and_compaction", WATCHDOG_BUDGET);
  let rt = Runtime::new()?;
  rt.block_on(async {
    const KEYS: usize = 8;
    const HISTORY_PER_KEY: usize = 5;
    const NEW_PER_KEY: usize = 5;

    let dir = tempdir()?;
    let store = FixtureStore::open(dir.path().join("stress.db"))?;
    let s = store.session()?;

    // 历史：每键多版本覆盖，旧版本留在紧缩区间
    for round in 0..HISTORY_PER_KEY {
      for i in 0..KEYS {
        let k = str_key(format!("hot:{i}").as_bytes());
        store
          .put(&s, &k, format!("h{}_v{round}", i).as_bytes())
          .await?;
      }
    }
    let seal = store.hlog.tail_address();
    store.seal_read_only(seal);

    // 并行：写者向尾部追加新版本；紧缩器消费 [begin, seal) 紧缩区间
    let writer_store = Arc::clone(&store);
    let writer = rt.spawn(async move {
      let p = writer_store.session()?;
      for round in 0..NEW_PER_KEY {
        for i in 0..KEYS {
          let k = str_key(format!("hot:{i}").as_bytes());
          writer_store
            .put(&p, &k, format!("w{}_v{round}", i).as_bytes())
            .await?;
        }
      }
      aok::Result::<()>::Ok(())
    });
    let compactor_store = Arc::clone(&store);
    let compactor_task = rt.spawn(async move {
      let compactor = LogCompactor::new(Arc::clone(&compactor_store));
      // 逐轮推进紧缩区间直至越过封印点（区间终点固定为 seal 快照）
      loop {
        let begin = compactor_store.begin_address();
        if begin >= seal {
          break;
        }
        compactor.compact(seal, CompactionType::Scan).await?;
      }
      aok::Result::<()>::Ok(())
    });
    writer
      .await
      .map_err(|e| aok::anyhow!("写者任务异常结束: {e}"))??;
    compactor_task
      .await
      .map_err(|e| aok::anyhow!("紧缩任务异常结束: {e}"))??;

    // 终态：每键读值必须精确等于写者最后一笔（紧缩绝不复活旧值）
    let v = store.session()?;
    for i in 0..KEYS {
      let k = str_key(format!("hot:{i}").as_bytes());
      let expected = format!("w{}_v{}", i, NEW_PER_KEY - 1);
      let actual = store.get(&v, &k).await?;
      assert_eq!(
        actual.as_deref(),
        Some(expected.as_bytes()),
        "键 {k:?} 终值必须为写者最后一笔"
      );
    }

    // 紧缩区间已推进至封印点（Scan 阶段 2 剔除语义保证无保守保留残留）
    assert_eq!(
      store.begin_address(),
      seal,
      "无竞争残留时紧缩区间必须推进至封印点"
    );

    aok::Result::<()>::Ok(())
  })?;
  OK
}
