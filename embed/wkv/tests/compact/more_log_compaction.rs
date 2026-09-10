//! 对标 Garnet Tsavorite MoreLogCompactionTests 紧缩测试套件
//!
//! 覆盖场景：
//! 1. 大规模批量删除后的日志紧缩（DeleteCompactLookup 对标）；
//! 2. 全量日志紧缩至尾部（迁移全部活跃记录）；
//! 3. 跨多代连续覆盖更新与逐代紧缩一致性；
//! 4. 单键高频多版本更新的链式塌缩；
//! 5. Scan 模式阶段 2 候选集全量覆盖时的提前跳出优化。

use std::{fmt::Write, sync::Arc};

use aok::{OK, Void};
use compio::runtime::Runtime;
use log::info;
use wcompact::{CompactionType, LogCompactor};

use super::support::create_test_store;

const TOTAL_RECORDS: usize = 2000;
const DELETE_COUNT: usize = 1000;
const CUT_RECORD_INDEX: usize = 1010;

/// 对标 libs/storage/Tsavorite/cs/test/test.hlog/MoreLogCompactionTests.cs:DeleteCompactLookup (Lookup 模式)
///
/// 写入 2000 条记录，在第 1010 条记录处记录紧缩点，随后删除前半部分 1000 条记录；
/// 刷盘并驱逐后执行 Lookup 紧缩，验证前 1000 条全部读空，未删除记录准确完整。
#[test]
fn more_log_compaction_delete_lookup() -> Void {
  let rt = Runtime::new()?;
  rt.block_on(async {
    let (_dir, store, session) = create_test_store("more_log_delete_lookup.db")?;

    let mut compact_until = 0u64;
    let mut k = String::with_capacity(20);
    let mut v = String::with_capacity(16);

    for i in 0..TOTAL_RECORDS {
      if i == CUT_RECORD_INDEX {
        compact_until = store.tail_address();
      }
      k.clear();
      let _ = write!(&mut k, "long_key:{i:06}");
      v.clear();
      let _ = write!(&mut v, "{i}");
      session.upsert(k.as_bytes(), v.as_bytes()).await?;
    }

    for i in 0..DELETE_COUNT {
      k.clear();
      let _ = write!(&mut k, "long_key:{i:06}");
      session.delete(k.as_bytes()).await?;
    }

    store.flush_and_evict_all().await?;

    let compactor = LogCompactor::new(Arc::clone(&store));
    let stats = compactor
      .compact(compact_until, CompactionType::Lookup)
      .await?;

    assert_eq!(store.begin_address(), compact_until);
    assert_eq!(stats.scanned_records, CUT_RECORD_INDEX);
    assert_eq!(stats.live_copied, CUT_RECORD_INDEX - DELETE_COUNT);
    assert_eq!(
      stats.superseded, DELETE_COUNT,
      "被区外墓碑取代的旧版计为弃迁"
    );
    assert_eq!(stats.dead_dropped, 0);

    let mut expected = String::with_capacity(16);
    for i in 0..TOTAL_RECORDS {
      k.clear();
      let _ = write!(&mut k, "long_key:{i:06}");
      let val = session.read(k.as_bytes()).await?;
      if i < DELETE_COUNT {
        assert!(val.is_none(), "已被删除的键必须返回 None: {k}");
      } else {
        expected.clear();
        let _ = write!(&mut expected, "{i}");
        assert_eq!(val.as_deref(), Some(expected.as_bytes()));
      }
    }

    info!("MoreLogCompactionTests.DeleteCompactLookup (Lookup) 验证通过");
    aok::Result::<()>::Ok(())
  })?;

  OK
}

/// 测试 Scan 模式下的 DeleteCompactLookup 紧缩推进及全量数据准确性
#[test]
fn more_log_compaction_delete_scan() -> Void {
  let rt = Runtime::new()?;
  rt.block_on(async {
    let (_dir, store, session) = create_test_store("more_log_delete_scan.db")?;

    let mut compact_until = 0u64;
    let mut k = String::with_capacity(20);
    let mut v = String::with_capacity(16);

    for i in 0..TOTAL_RECORDS {
      if i == CUT_RECORD_INDEX {
        compact_until = store.tail_address();
      }
      k.clear();
      let _ = write!(&mut k, "long_key:{i:06}");
      v.clear();
      let _ = write!(&mut v, "{i}");
      session.upsert(k.as_bytes(), v.as_bytes()).await?;
    }

    for i in 0..DELETE_COUNT {
      k.clear();
      let _ = write!(&mut k, "long_key:{i:06}");
      session.delete(k.as_bytes()).await?;
    }

    store.flush_and_evict_all().await?;

    let compactor = LogCompactor::new(Arc::clone(&store));
    let stats = compactor
      .compact(compact_until, CompactionType::Scan)
      .await?;

    assert_eq!(store.begin_address(), compact_until);
    assert_eq!(stats.scanned_records, CUT_RECORD_INDEX);
    assert_eq!(stats.live_copied, CUT_RECORD_INDEX - DELETE_COUNT);
    assert_eq!(
      stats.superseded, DELETE_COUNT,
      "阶段 2 剔除：候选被区外墓碑取代计为弃迁"
    );
    assert_eq!(stats.dead_dropped, 0);

    let mut expected = String::with_capacity(16);
    for i in 0..TOTAL_RECORDS {
      k.clear();
      let _ = write!(&mut k, "long_key:{i:06}");
      let val = session.read(k.as_bytes()).await?;
      if i < DELETE_COUNT {
        assert!(val.is_none(), "已被删除的键必须返回 None: {k}");
      } else {
        expected.clear();
        let _ = write!(&mut expected, "{i}");
        assert_eq!(val.as_deref(), Some(expected.as_bytes()));
      }
    }

    info!("MoreLogCompactionTests.DeleteCompactLookup (Scan) 验证通过");
    aok::Result::<()>::Ok(())
  })?;

  OK
}

/// 对标 Garnet MoreLogCompactionTests: 全量日志紧缩至尾部极限用例
///
/// 写入 300 条记录后落盘驱逐，紧缩目标设为 store.tail_address()，
/// 验证全量活跃记录完整迁移至新尾部，有效起始地址推进至旧尾部。
#[test]
fn more_log_compaction_entire_log_to_tail() -> Void {
  let rt = Runtime::new()?;
  rt.block_on(async {
    let (_dir, store, session) = create_test_store("more_log_entire_tail.db")?;

    const TOTAL: usize = 300;
    let mut k = String::with_capacity(16);
    let mut v = String::with_capacity(16);

    for i in 0..TOTAL {
      k.clear();
      let _ = write!(&mut k, "entire_k:{i:04}");
      v.clear();
      let _ = write!(&mut v, "entire_v:{i:04}");
      session.upsert(k.as_bytes(), v.as_bytes()).await?;
    }

    store.flush_and_evict_all().await?;
    let tail = store.tail_address();

    let compactor = LogCompactor::new(Arc::clone(&store));
    let stats = compactor.compact(tail, CompactionType::Scan).await?;

    assert_eq!(stats.scanned_records, TOTAL);
    assert_eq!(stats.live_copied, TOTAL);
    assert_eq!(stats.dead_dropped, 0);
    assert_eq!(store.begin_address(), tail);
    assert_eq!(stats.new_begin_address, tail);

    for i in 0..TOTAL {
      k.clear();
      let _ = write!(&mut k, "entire_k:{i:04}");
      v.clear();
      let _ = write!(&mut v, "entire_v:{i:04}");
      let val = session.read(k.as_bytes()).await?;
      assert_eq!(val.as_deref(), Some(v.as_bytes()));
    }

    info!("全量日志紧缩至尾部极限用例验证通过");
    aok::Result::<()>::Ok(())
  })?;

  OK
}

/// 对标 Garnet MoreLogCompactionTests: 多版本连续覆盖更新与逐代紧缩一致性验证
#[test]
fn more_log_compaction_multigeneration_continuous_updates() -> Void {
  let rt = Runtime::new()?;
  rt.block_on(async {
    let (_dir, store, session) = create_test_store("more_log_multigen.db")?;
    let compactor = LogCompactor::new(Arc::clone(&store));

    let k_a = b"key:alpha";
    let k_b = b"key:beta";
    let k_c = b"key:gamma";

    // 代次 1：初始写入三条记录
    session.upsert(k_a, b"v1_alpha").await?;
    session.upsert(k_b, b"v1_beta").await?;
    session.upsert(k_c, b"v1_gamma").await?;

    store.shift_read_only_address(store.tail_address());

    // 覆盖更新 alpha 与 beta
    session.upsert(k_a, b"v2_alpha").await?;
    session.upsert(k_b, b"v2_beta").await?;

    let gen1_cut = store.tail_address();
    store.shift_read_only_address(gen1_cut);

    // 第一代紧缩（Lookup 模式）
    let stats1 = compactor.compact(gen1_cut, CompactionType::Lookup).await?;
    assert_eq!(stats1.scanned_records, 5);
    assert_eq!(stats1.live_copied, 3);
    assert_eq!(
      stats1.superseded, 2,
      "alpha/beta v1 被区间内 v2 取代计为弃迁"
    );
    assert_eq!(stats1.dead_dropped, 0);

    assert_eq!(
      session.read(k_a).await?.as_deref(),
      Some(b"v2_alpha".as_slice())
    );
    assert_eq!(
      session.read(k_b).await?.as_deref(),
      Some(b"v2_beta".as_slice())
    );
    assert_eq!(
      session.read(k_c).await?.as_deref(),
      Some(b"v1_gamma".as_slice())
    );

    // 代次 2：进一步更新 alpha 至 v3，墓碑删除 beta
    session.upsert(k_a, b"v3_alpha").await?;
    session.delete(k_b).await?;

    let gen2_cut = store.tail_address();
    store.shift_read_only_address(gen2_cut);

    // 第二代紧缩（Scan 模式）
    let stats2 = compactor.compact(gen2_cut, CompactionType::Scan).await?;
    assert_eq!(stats2.scanned_records, 4);
    assert_eq!(stats2.live_copied, 2);
    assert_eq!(stats2.superseded, 1, "b_v2 被区间内墓碑取代计为弃迁");
    assert_eq!(stats2.dead_dropped, 1, "beta 墓碑判死丢弃");

    assert_eq!(
      session.read(k_a).await?.as_deref(),
      Some(b"v3_alpha".as_slice())
    );
    assert!(session.read(k_b).await?.is_none());
    assert_eq!(
      session.read(k_c).await?.as_deref(),
      Some(b"v1_gamma".as_slice())
    );

    // 代次 3：再次更新 alpha 至 v4，全量紧缩至尾部
    session.upsert(k_a, b"v4_alpha").await?;
    store.shift_read_only_address(store.tail_address());

    let stats3 = compactor
      .compact(store.tail_address(), CompactionType::Lookup)
      .await?;
    assert_eq!(stats3.live_copied, 2);
    assert_eq!(
      session.read(k_a).await?.as_deref(),
      Some(b"v4_alpha".as_slice())
    );
    assert!(session.read(k_b).await?.is_none());
    assert_eq!(
      session.read(k_c).await?.as_deref(),
      Some(b"v1_gamma".as_slice())
    );

    info!("多版本连续覆盖更新与逐代紧缩一致性验证通过");
    aok::Result::<()>::Ok(())
  })?;

  OK
}

/// 对标 Garnet MoreLogCompactionTests: 单键高频多次连续覆盖更新版本塌缩验证
#[test]
fn more_log_compaction_multiversion_single_key_many_updates() -> Void {
  let rt = Runtime::new()?;
  rt.block_on(async {
    let (_dir, store, session) = create_test_store("more_log_single_key.db")?;

    let k = b"key:hotspot:chain";

    // 写入 10 个递增版本强制形成日志多版本链
    let mut val = String::with_capacity(48);
    for v in 1..=10usize {
      val.clear();
      let _ = write!(&mut val, "v{v:02}_");
      for _ in 0..v * 4 {
        val.push('x');
      }
      session.upsert(k, val.as_bytes()).await?;
    }

    let compact_until = store.tail_address();
    store.shift_read_only_address(compact_until);

    let compactor = LogCompactor::new(Arc::clone(&store));
    let stats = compactor
      .compact(compact_until, CompactionType::Lookup)
      .await?;

    assert_eq!(stats.scanned_records, 10);
    assert_eq!(stats.live_copied, 1);
    assert_eq!(stats.superseded, 9, "v1-v9 被区间内 v10 取代计为弃迁");
    assert_eq!(stats.dead_dropped, 0);
    assert_eq!(store.begin_address(), compact_until);

    assert_eq!(
      session.read(k).await?.as_deref(),
      Some(val.as_bytes()),
      "紧缩后必须保留最终版本 v10"
    );

    info!("单键连续覆盖更新版本塌缩验证通过");
    aok::Result::<()>::Ok(())
  })?;

  OK
}

/// 对标 libs/storage/Tsavorite/cs/test/test.hlog/MoreLogCompactionTests.cs:Scan 模式阶段 2 全覆盖提前终止优化
#[test]
fn more_log_compaction_scan_mode_stage2_early_termination() -> Void {
  let rt = Runtime::new()?;
  rt.block_on(async {
    let (_dir, store, session) = create_test_store("more_log_stage2_break.db")?;

    const COUNT: usize = 50;
    let mut k = String::with_capacity(16);
    let mut v = String::with_capacity(32);

    // 阶段 1 区间：写入 50 个键的 v1 版本
    for i in 0..COUNT {
      k.clear();
      let _ = write!(&mut k, "k_{i:03}");
      v.clear();
      let _ = write!(&mut v, "v1_{i:03}");
      session.upsert(k.as_bytes(), v.as_bytes()).await?;
    }

    let compact_until = store.tail_address();

    // 阶段 2 区间：全部覆盖为 v2 版本
    for i in 0..COUNT {
      k.clear();
      let _ = write!(&mut k, "k_{i:03}");
      v.clear();
      let _ = write!(&mut v, "v2_{i:03}_overwritten");
      session.upsert(k.as_bytes(), v.as_bytes()).await?;
    }

    let read_only_cut = store.tail_address();
    store.shift_read_only_address(read_only_cut);

    let compactor = LogCompactor::new(Arc::clone(&store));
    let stats = compactor
      .compact(compact_until, CompactionType::Scan)
      .await?;

    assert_eq!(stats.scanned_records, COUNT);
    assert_eq!(stats.live_copied, 0, "全部键已覆盖，紧缩区 0 条存活");
    assert_eq!(
      stats.superseded, COUNT,
      "阶段 2 全量剔除：候选被区外 v2 取代计为弃迁"
    );
    assert_eq!(stats.dead_dropped, 0);
    assert_eq!(store.begin_address(), compact_until);

    let mut expected = String::with_capacity(32);
    for i in 0..COUNT {
      k.clear();
      let _ = write!(&mut k, "k_{i:03}");
      expected.clear();
      let _ = write!(&mut expected, "v2_{i:03}_overwritten");
      assert_eq!(
        session.read(k.as_bytes()).await?.as_deref(),
        Some(expected.as_bytes())
      );
    }

    info!("Scan 模式阶段 2 全覆盖提前终止验证通过");
    aok::Result::<()>::Ok(())
  })?;

  OK
}

/// 验证集合历史子键/已删集合子键在紧缩时判定为死记录，直接淘汰并清理哈希索引（Lookup 与 Scan 双模式）
#[test]
fn more_log_compaction_stale_subkeys_dead_dropped() -> Void {
  let rt = Runtime::new()?;
  rt.block_on(async {
    for comp_type in [CompactionType::Lookup, CompactionType::Scan] {
      let (_dir, store, session) = create_test_store("compact_stale_subkey.db")?;

      let key_id = 99999u64;
      let old_ver = 1u64;

      // 1. 模拟写入 10 条打平集合子键（方案 A 规范编码）
      for i in 0..10 {
        let field = format!("field_{i:02}");
        let sub_k = wval::NamespaceDbCodec::encode_sub_key(
          0,
          0,
          wval::KeyTag::Hash,
          key_id,
          old_ver,
          field.as_bytes(),
        );
        session.upsert_raw(&sub_k, b"some_val").await?;
      }

      // 2. 模拟该集合被删除（Fast Drop）或版本升级，当前版本变为 2
      store.update_key_id_meta(key_id, old_ver + 1, false);

      let compact_until = store.tail_address();
      store.shift_read_only_address(compact_until);

      let compactor = LogCompactor::new(Arc::clone(&store));
      let stats = compactor.compact(compact_until, comp_type).await?;

      // 验证所有历史子键都被淘汰丢弃，无一条死循环保活
      assert_eq!(stats.scanned_records, 10);
      assert_eq!(stats.live_copied, 0, "历史子键绝不搬迁保活");
      assert_eq!(stats.dead_dropped, 10, "历史子键全量作为垃圾丢弃");

      // 验证索引中已完全无该历史子键
      let probe_sub = wval::NamespaceDbCodec::encode_sub_key(
        0,
        0,
        wval::KeyTag::Hash,
        key_id,
        old_ver,
        b"field_00",
      );
      assert!(store.index.find_tag(&probe_sub).is_none());

      info!("历史子键 GC 淘汰与索引清理验证通过: {comp_type:?}");
    }
    aok::Result::<()>::Ok(())
  })?;

  OK
}
