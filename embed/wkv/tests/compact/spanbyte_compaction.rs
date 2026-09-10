//! 对标 Garnet Tsavorite SpanByteLogCompactionTests 紧缩测试套件
//!
//! 覆盖场景：
//! 1. 基础全量数据推进紧缩（Lookup 与 Scan 模式）；
//! 2. 多层冷热分级存储紧缩（Level 1 内存覆写 + Level 2 磁盘冷读）；
//! 3. 穿插删除与墓碑清理紧缩；
//! 4. 自定义过期过滤谓词紧缩；
//! 5. 单键多版本覆盖更新紧缩至尾部。

use std::{fmt::Write, sync::Arc};

use aok::{OK, Void};
use compio::runtime::Runtime;
use log::info;
use wcompact::{CompactionType, LogCompactor};

use super::support::{create_test_store, verify_records};

const TOTAL_RECORDS: usize = 2000;
const CUT_OFFSET: usize = 1000;
const OVERWRITE_COUNT: usize = 500;

/// 对标 libs/storage/Tsavorite/cs/test/test.hlog/SpanByteLogCompactionTests.cs:SpanByteLogCompactionTest1 (Lookup 模式)
///
/// 写入 2000 条记录，在 1000 条处记录紧缩点，刷盘并驱逐至磁盘，
/// 执行 Lookup 紧缩后验证 begin_address 推进至截断点，2000 条记录全部完整可读。
#[test]
fn spanbyte_compaction_test1_lookup() -> Void {
  let rt = Runtime::new()?;
  rt.block_on(async {
    let (_dir, store, session) = create_test_store("spanbyte_test1_lookup.db")?;

    let mut compact_until = 0u64;
    let mut k = String::with_capacity(16);
    let mut v = String::with_capacity(24);

    for i in 0..TOTAL_RECORDS {
      if i == CUT_OFFSET {
        compact_until = store.tail_address();
      }
      k.clear();
      let _ = write!(&mut k, "kfield1:{i:05}");
      v.clear();
      let _ = write!(&mut v, "kfield1:{i:05}:payload");
      session.upsert(k.as_bytes(), v.as_bytes()).await?;
    }

    store.flush_and_evict_all().await?;

    let compactor = LogCompactor::new(Arc::clone(&store));
    let stats = compactor
      .compact(compact_until, CompactionType::Lookup)
      .await?;

    assert_eq!(store.begin_address(), stats.new_begin_address);
    assert_eq!(store.begin_address(), compact_until);
    assert_eq!(stats.scanned_records, CUT_OFFSET);
    assert_eq!(stats.live_copied, CUT_OFFSET);
    assert_eq!(stats.dead_dropped, 0);

    verify_records(&session, "kfield1", TOTAL_RECORDS, |_| false).await?;

    info!("SpanByteLogCompactionTest1 (Lookup) 验证通过");
    aok::Result::<()>::Ok(())
  })?;

  OK
}

/// 测试 Scan 模式下的 SpanByteLogCompactionTest1 紧缩推进及全量数据准确性
///
/// 相同 2000 条记录落盘流程，采用 Scan 模式紧缩，验证紧缩推进及全量数据准确性。
#[test]
fn spanbyte_compaction_test1_scan() -> Void {
  let rt = Runtime::new()?;
  rt.block_on(async {
    let (_dir, store, session) = create_test_store("spanbyte_test1_scan.db")?;

    let mut compact_until = 0u64;
    let mut k = String::with_capacity(16);
    let mut v = String::with_capacity(24);

    for i in 0..TOTAL_RECORDS {
      if i == CUT_OFFSET {
        compact_until = store.tail_address();
      }
      k.clear();
      let _ = write!(&mut k, "kfield1:{i:05}");
      v.clear();
      let _ = write!(&mut v, "kfield1:{i:05}:payload");
      session.upsert(k.as_bytes(), v.as_bytes()).await?;
    }

    store.flush_and_evict_all().await?;

    let compactor = LogCompactor::new(Arc::clone(&store));
    let stats = compactor
      .compact(compact_until, CompactionType::Scan)
      .await?;

    assert_eq!(store.begin_address(), stats.new_begin_address);
    assert_eq!(store.begin_address(), compact_until);
    assert_eq!(stats.scanned_records, CUT_OFFSET);
    assert_eq!(stats.live_copied, CUT_OFFSET);
    assert_eq!(stats.dead_dropped, 0);

    verify_records(&session, "kfield1", TOTAL_RECORDS, |_| false).await?;

    info!("SpanByteLogCompactionTest1 (Scan) 验证通过");
    aok::Result::<()>::Ok(())
  })?;

  OK
}

/// 对标 libs/storage/Tsavorite/cs/test/test.hlog/SpanByteLogCompactionTests.cs:SpanByteLogCompactionTest2 (Lookup 多层冷热模式)
///
/// 写入 2000 条后落盘，在内存中覆写前 500 条（Level 1 内存热区 + Level 2 磁盘冷区），
/// 紧缩后验证旧版本安全丢弃，新版本与未修改记录全部准确回读。
#[test]
fn spanbyte_compaction_test2_multilevel_lookup() -> Void {
  let rt = Runtime::new()?;
  rt.block_on(async {
    let (_dir, store, session) = create_test_store("spanbyte_test2_lookup.db")?;

    let mut compact_until = 0u64;
    let mut k = String::with_capacity(16);
    let mut v = String::with_capacity(36);

    for i in 0..TOTAL_RECORDS {
      if i == CUT_OFFSET {
        compact_until = store.tail_address();
      }
      k.clear();
      let _ = write!(&mut k, "kfield1:{i:05}");
      v.clear();
      let _ = write!(&mut v, "vfield1:{i:05}:level2_disk");
      session.upsert(k.as_bytes(), v.as_bytes()).await?;
    }

    store.flush_and_evict_all().await?;

    // 内存覆写前 500 条记录为 Level 1 最新版本
    for i in 0..OVERWRITE_COUNT {
      k.clear();
      let _ = write!(&mut k, "kfield1:{i:05}");
      v.clear();
      let _ = write!(&mut v, "vfield1:{i:05}:level1_memory_fresh");
      session.upsert(k.as_bytes(), v.as_bytes()).await?;
    }

    store.shift_read_only_address(store.tail_address());

    let compactor = LogCompactor::new(Arc::clone(&store));
    let stats = compactor
      .compact(compact_until, CompactionType::Lookup)
      .await?;

    assert_eq!(store.begin_address(), compact_until);
    assert_eq!(stats.scanned_records, CUT_OFFSET);
    assert_eq!(stats.live_copied, OVERWRITE_COUNT);
    assert_eq!(
      stats.superseded, OVERWRITE_COUNT,
      "被内存区新版本取代的旧版计为弃迁"
    );
    assert_eq!(stats.dead_dropped, 0);

    let mut expected = String::with_capacity(36);
    for i in 0..TOTAL_RECORDS {
      k.clear();
      let _ = write!(&mut k, "kfield1:{i:05}");
      let val = session.read(k.as_bytes()).await?;
      expected.clear();
      if i < OVERWRITE_COUNT {
        let _ = write!(&mut expected, "vfield1:{i:05}:level1_memory_fresh");
      } else {
        let _ = write!(&mut expected, "vfield1:{i:05}:level2_disk");
      }
      assert_eq!(val.as_deref(), Some(expected.as_bytes()));
    }

    info!("SpanByteLogCompactionTest2 (Lookup 多层冷热) 验证通过");
    aok::Result::<()>::Ok(())
  })?;

  OK
}

/// 测试 Scan 模式下的 SpanByteLogCompactionTest2 多层冷热数据紧缩推进与全量校验
#[test]
fn spanbyte_compaction_test2_multilevel_scan() -> Void {
  let rt = Runtime::new()?;
  rt.block_on(async {
    let (_dir, store, session) = create_test_store("spanbyte_test2_scan.db")?;

    let mut compact_until = 0u64;
    let mut k = String::with_capacity(16);
    let mut v = String::with_capacity(36);

    for i in 0..TOTAL_RECORDS {
      if i == CUT_OFFSET {
        compact_until = store.tail_address();
      }
      k.clear();
      let _ = write!(&mut k, "kfield1:{i:05}");
      v.clear();
      let _ = write!(&mut v, "vfield1:{i:05}:level2_disk");
      session.upsert(k.as_bytes(), v.as_bytes()).await?;
    }

    store.flush_and_evict_all().await?;

    for i in 0..OVERWRITE_COUNT {
      k.clear();
      let _ = write!(&mut k, "kfield1:{i:05}");
      v.clear();
      let _ = write!(&mut v, "vfield1:{i:05}:level1_memory_fresh");
      session.upsert(k.as_bytes(), v.as_bytes()).await?;
    }

    store.shift_read_only_address(store.tail_address());

    let compactor = LogCompactor::new(Arc::clone(&store));
    let stats = compactor
      .compact(compact_until, CompactionType::Scan)
      .await?;

    assert_eq!(store.begin_address(), compact_until);
    assert_eq!(stats.scanned_records, CUT_OFFSET);
    assert_eq!(stats.live_copied, OVERWRITE_COUNT);
    assert_eq!(
      stats.superseded, OVERWRITE_COUNT,
      "被内存区新版本取代的旧版计为弃迁"
    );
    assert_eq!(stats.dead_dropped, 0);

    let mut expected = String::with_capacity(36);
    for i in 0..TOTAL_RECORDS {
      k.clear();
      let _ = write!(&mut k, "kfield1:{i:05}");
      let val = session.read(k.as_bytes()).await?;
      expected.clear();
      if i < OVERWRITE_COUNT {
        let _ = write!(&mut expected, "vfield1:{i:05}:level1_memory_fresh");
      } else {
        let _ = write!(&mut expected, "vfield1:{i:05}:level2_disk");
      }
      assert_eq!(val.as_deref(), Some(expected.as_bytes()));
    }

    info!("SpanByteLogCompactionTest2 (Scan 多层冷热) 验证通过");
    aok::Result::<()>::Ok(())
  })?;

  OK
}

/// 对标 libs/storage/Tsavorite/cs/test/test.hlog/SpanByteLogCompactionTests.cs:SpanByteLogCompactionTest3 (Lookup 穿插删除模式)
///
/// 写入过程中穿插删除键，紧缩后验证被删除键读空，其余记录完整存活。
#[test]
fn spanbyte_compaction_test3_with_deletions_lookup() -> Void {
  let rt = Runtime::new()?;
  rt.block_on(async {
    let (_dir, store, session) = create_test_store("spanbyte_test3_lookup.db")?;

    let mut compact_until = 0u64;
    let mut k = String::with_capacity(16);
    let mut v = String::with_capacity(16);
    let mut del_k = String::with_capacity(16);

    for i in 0..TOTAL_RECORDS {
      if i == CUT_OFFSET {
        compact_until = store.tail_address();
      }
      k.clear();
      let _ = write!(&mut k, "key:{i:04}");
      v.clear();
      let _ = write!(&mut v, "val:{i:04}");
      session.upsert(k.as_bytes(), v.as_bytes()).await?;

      if i % 8 == 0 {
        let del_j = i / 4;
        del_k.clear();
        let _ = write!(&mut del_k, "key:{del_j:04}");
        session.delete(del_k.as_bytes()).await?;
      }
    }

    store.flush_and_evict_all().await?;

    let compactor = LogCompactor::new(Arc::clone(&store));
    let stats = compactor
      .compact(compact_until, CompactionType::Lookup)
      .await?;

    assert_eq!(store.begin_address(), compact_until);

    let mut expected = String::with_capacity(16);
    for i in 0..TOTAL_RECORDS {
      k.clear();
      let _ = write!(&mut k, "key:{i:04}");
      let val = session.read(k.as_bytes()).await?;
      let is_deleted = (i < TOTAL_RECORDS / 4) && (i % 2 == 0);
      if is_deleted {
        assert!(val.is_none(), "已被删除的键必须返回 None: {k}");
      } else {
        expected.clear();
        let _ = write!(&mut expected, "val:{i:04}");
        assert_eq!(val.as_deref(), Some(expected.as_bytes()));
      }
    }

    info!(
      "SpanByteLogCompactionTest3 (Lookup 穿插删除) 验证通过, 丢弃数={}",
      stats.dead_dropped
    );
    aok::Result::<()>::Ok(())
  })?;

  OK
}

/// 测试 Scan 模式下的 SpanByteLogCompactionTest3 穿插删除数据紧缩推进与全量校验
#[test]
fn spanbyte_compaction_test3_with_deletions_scan() -> Void {
  let rt = Runtime::new()?;
  rt.block_on(async {
    let (_dir, store, session) = create_test_store("spanbyte_test3_scan.db")?;

    let mut compact_until = 0u64;
    let mut k = String::with_capacity(16);
    let mut v = String::with_capacity(16);
    let mut del_k = String::with_capacity(16);

    for i in 0..TOTAL_RECORDS {
      if i == CUT_OFFSET {
        compact_until = store.tail_address();
      }
      k.clear();
      let _ = write!(&mut k, "key:{i:04}");
      v.clear();
      let _ = write!(&mut v, "val:{i:04}");
      session.upsert(k.as_bytes(), v.as_bytes()).await?;

      if i % 8 == 0 {
        let del_j = i / 4;
        del_k.clear();
        let _ = write!(&mut del_k, "key:{del_j:04}");
        session.delete(del_k.as_bytes()).await?;
      }
    }

    store.flush_and_evict_all().await?;

    let compactor = LogCompactor::new(Arc::clone(&store));
    let stats = compactor
      .compact(compact_until, CompactionType::Scan)
      .await?;

    assert_eq!(store.begin_address(), compact_until);

    let mut expected = String::with_capacity(16);
    for i in 0..TOTAL_RECORDS {
      k.clear();
      let _ = write!(&mut k, "key:{i:04}");
      let val = session.read(k.as_bytes()).await?;
      let is_deleted = (i < TOTAL_RECORDS / 4) && (i % 2 == 0);
      if is_deleted {
        assert!(val.is_none(), "已被删除的键必须返回 None: {k}");
      } else {
        expected.clear();
        let _ = write!(&mut expected, "val:{i:04}");
        assert_eq!(val.as_deref(), Some(expected.as_bytes()));
      }
    }

    info!(
      "SpanByteLogCompactionTest3 (Scan 穿插删除) 验证通过, 丢弃数={}",
      stats.dead_dropped
    );
    aok::Result::<()>::Ok(())
  })?;

  OK
}

/// 对标 libs/storage/Tsavorite/cs/test/test.hlog/SpanByteLogCompactionTests.cs:SpanByteLogCompactionCustomFunctionsTest1 (Lookup 模式)
///
/// 自定义谓词：奇数值记录判定为已删除。紧缩后奇数项清除，偶数项存活迁移，紧缩区外不受影响。
#[test]
fn spanbyte_compaction_custom_filter_test1_lookup() -> Void {
  let rt = Runtime::new()?;
  rt.block_on(async {
    let (_dir, store, session) = create_test_store("spanbyte_custom1_lookup.db")?;

    let mut compact_until = 0u64;
    let mut k = String::with_capacity(16);
    let mut v = String::with_capacity(16);

    for i in 0..TOTAL_RECORDS {
      if i == CUT_OFFSET {
        compact_until = store.tail_address();
      }
      k.clear();
      let _ = write!(&mut k, "k:{i:05}");
      v.clear();
      let _ = write!(&mut v, "{i}");
      session.upsert(k.as_bytes(), v.as_bytes()).await?;
    }

    store.flush_and_evict_all().await?;

    let compactor = LogCompactor::new(Arc::clone(&store));
    let stats = compactor
      .compact_with_filter(compact_until, CompactionType::Lookup, |_key, val| {
        val
          .last()
          .is_some_and(|&b| b.is_ascii_digit() && b % 2 != 0)
      })
      .await?;

    assert_eq!(store.begin_address(), compact_until);
    assert_eq!(stats.scanned_records, CUT_OFFSET);
    assert_eq!(stats.live_copied, 500);
    assert_eq!(stats.dead_dropped, 500);

    let mut expected = String::with_capacity(16);
    for i in 0..TOTAL_RECORDS {
      k.clear();
      let _ = write!(&mut k, "k:{i:05}");
      let val = session.read(k.as_bytes()).await?;
      if i < CUT_OFFSET && i % 2 != 0 {
        assert!(val.is_none(), "紧缩区奇数记录应被自定义过滤删除: {k}");
      } else {
        expected.clear();
        let _ = write!(&mut expected, "{i}");
        assert_eq!(val.as_deref(), Some(expected.as_bytes()));
      }
    }

    info!("SpanByteLogCompactionCustomFunctionsTest1 (Lookup) 验证通过");
    aok::Result::<()>::Ok(())
  })?;

  OK
}

/// 测试 Scan 模式下的 SpanByteLogCompactionCustomFunctionsTest1 自定义过滤紧缩推进与全量校验
#[test]
fn spanbyte_compaction_custom_filter_test1_scan() -> Void {
  let rt = Runtime::new()?;
  rt.block_on(async {
    let (_dir, store, session) = create_test_store("spanbyte_custom1_scan.db")?;

    let mut compact_until = 0u64;
    let mut k = String::with_capacity(16);
    let mut v = String::with_capacity(16);

    for i in 0..TOTAL_RECORDS {
      if i == CUT_OFFSET {
        compact_until = store.tail_address();
      }
      k.clear();
      let _ = write!(&mut k, "k:{i:05}");
      v.clear();
      let _ = write!(&mut v, "{i}");
      session.upsert(k.as_bytes(), v.as_bytes()).await?;
    }

    store.flush_and_evict_all().await?;

    let compactor = LogCompactor::new(Arc::clone(&store));
    let stats = compactor
      .compact_with_filter(compact_until, CompactionType::Scan, |_key, val| {
        val
          .last()
          .is_some_and(|&b| b.is_ascii_digit() && b % 2 != 0)
      })
      .await?;

    assert_eq!(store.begin_address(), compact_until);
    assert_eq!(stats.scanned_records, CUT_OFFSET);
    assert_eq!(stats.live_copied, 500);
    assert_eq!(stats.dead_dropped, 500);

    let mut expected = String::with_capacity(16);
    for i in 0..TOTAL_RECORDS {
      k.clear();
      let _ = write!(&mut k, "k:{i:05}");
      let val = session.read(k.as_bytes()).await?;
      if i < CUT_OFFSET && i % 2 != 0 {
        assert!(val.is_none(), "紧缩区奇数记录应被自定义过滤删除: {k}");
      } else {
        expected.clear();
        let _ = write!(&mut expected, "{i}");
        assert_eq!(val.as_deref(), Some(expected.as_bytes()));
      }
    }

    info!("SpanByteLogCompactionCustomFunctionsTest1 (Scan) 验证通过");
    aok::Result::<()>::Ok(())
  })?;

  OK
}

/// 对标 libs/storage/Tsavorite/cs/test/test.hlog/SpanByteLogCompactionTests.cs:SpanByteLogCompactionCustomFunctionsTest2 (Lookup 模式)
///
/// 写入同一 Key 的多个版本并刷盘落盘，紧缩至 TailAddress，确保最终读取到的是最新版本值。
#[test]
fn spanbyte_compaction_custom_functions_test2_lookup() -> Void {
  let rt = Runtime::new()?;
  rt.block_on(async {
    let (_dir, store, session) = create_test_store("spanbyte_custom2_lookup.db")?;

    let k = b"key:custom:test2";
    let v_old = b"vfield1=10,vfield2=20";
    session.upsert(k, v_old).await?;
    store.flush_all().await?;
    store.shift_read_only_address(store.tail_address());

    let v_new = b"vfield1=11,vfield2=21";
    session.upsert(k, v_new).await?;
    store.flush_and_evict_all().await?;

    let tail = store.tail_address();
    let compactor = LogCompactor::new(Arc::clone(&store));
    let stats = compactor.compact(tail, CompactionType::Lookup).await?;

    assert_eq!(store.begin_address(), tail);
    assert_eq!(stats.scanned_records, 2);
    assert_eq!(stats.live_copied, 1);
    assert_eq!(stats.superseded, 1, "v_old 被区间内 v_new 取代计为弃迁");
    assert_eq!(stats.dead_dropped, 0);

    let val = session.read(k).await?;
    assert_eq!(
      val.as_deref(),
      Some(v_new.as_slice()),
      "紧缩后必须正确读取到最新版本值"
    );

    info!("SpanByteLogCompactionCustomFunctionsTest2 (Lookup) 验证通过");
    aok::Result::<()>::Ok(())
  })?;

  OK
}

/// 测试 Scan 模式下的 SpanByteLogCompactionCustomFunctionsTest2 自定义函数紧缩推进与全量校验
#[test]
fn spanbyte_compaction_custom_functions_test2_scan() -> Void {
  let rt = Runtime::new()?;
  rt.block_on(async {
    let (_dir, store, session) = create_test_store("spanbyte_custom2_scan.db")?;

    let k = b"key:custom:test2";
    let v_old = b"vfield1=10,vfield2=20";
    session.upsert(k, v_old).await?;
    store.flush_all().await?;
    store.shift_read_only_address(store.tail_address());

    let v_new = b"vfield1=11,vfield2=21";
    session.upsert(k, v_new).await?;
    store.flush_and_evict_all().await?;

    let tail = store.tail_address();
    let compactor = LogCompactor::new(Arc::clone(&store));
    let stats = compactor.compact(tail, CompactionType::Scan).await?;

    assert_eq!(store.begin_address(), tail);
    assert_eq!(stats.scanned_records, 2);
    assert_eq!(stats.live_copied, 1);
    assert_eq!(stats.superseded, 1, "v_old 被区间内 v_new 取代计为弃迁");
    assert_eq!(stats.dead_dropped, 0);

    let val = session.read(k).await?;
    assert_eq!(
      val.as_deref(),
      Some(v_new.as_slice()),
      "紧缩后必须正确读取到最新版本值"
    );

    info!("SpanByteLogCompactionCustomFunctionsTest2 (Scan) 验证通过");
    aok::Result::<()>::Ok(())
  })?;

  OK
}
