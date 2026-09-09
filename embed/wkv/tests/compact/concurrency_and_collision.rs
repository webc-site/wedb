//! 并发写入安全、哈希碰撞链重构、空键与大记录极端边界测试
//!
//! 覆盖场景：
//! 1. 紧缩期间并发写入安全性（CAS 原子防护，旧记录绝不反向覆盖并发新版本）；
//! 2. 强哈希碰撞与深度溢出桶链场景下的紧缩正确性；
//! 3. 空键（b""）与大尺寸记录（32KB）极端边界；
//! 4. 零长合法值（val == b"" 且非墓碑）紧缩存活验证；
//! 5. ReadCache 启用下的冷读、标记剥离与紧缩迁移验证。

use std::{
  fmt::Write,
  sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
  },
  thread,
};

use aok::{OK, Void};
use compio::runtime::Runtime;
use log::info;
use tempfile::tempdir;
use wcompact::{CompactionType, LogCompactor};
use wdev::SegmentedDevice;
use wkv::{StoreConfig, WedbStore};

use super::support::{create_read_cache_store, create_test_store};

const COLLISION_TOTAL: usize = 1000;
const COLLISION_CUT: usize = 500;
const COLLISION_OVERWRITE: usize = 250;
const RC_TOTAL: usize = 50;
const RC_READ_COUNT: usize = 25;
/// 并发写入安全验证（Lookup 模式）：原子 CAS 保证绝不以旧数据覆盖并发新数据
#[test]
fn concurrent_write_safety_lookup() -> Void {
  let rt = Runtime::new()?;
  rt.block_on(async {
    let (_dir, store, session) = create_test_store("concurrent_write_lookup.db")?;

    let k = b"key:concurrent:race";
    let v_old = b"version_1_old_value";
    session.upsert(k, v_old).await?;

    let stage1_tail = store.tail_address();
    store.shift_read_only_address(stage1_tail);

    // 模拟并发新事务在尾部写入新版本
    let v_new_concurrent = b"version_2_concurrent_latest_value";
    session.upsert(k, v_new_concurrent).await?;

    let compactor = LogCompactor::new(Arc::clone(&store));
    let stats = compactor
      .compact(stage1_tail, CompactionType::Lookup)
      .await?;

    assert_eq!(stats.scanned_records, 1);
    assert_eq!(stats.live_copied, 0, "旧版本不得被迁移至 Tail");
    assert_eq!(stats.superseded, 1, "旧版本被并发新版本取代计为弃迁");
    assert_eq!(stats.dead_dropped, 0);

    let current_val = session.read(k).await?;
    assert_eq!(
      current_val.as_deref(),
      Some(v_new_concurrent.as_slice()),
      "并发写入的新数据必须绝对保留，绝不能被旧紧缩覆盖"
    );

    info!("Lookup 模式并发写入安全测试验证通过");
    aok::Result::<()>::Ok(())
  })?;

  OK
}

/// 并发写入安全验证（Scan 模式）：验证并发新版本在 Scan 阶段绝不会被旧版本覆写
#[test]
fn concurrent_write_safety_scan() -> Void {
  let rt = Runtime::new()?;
  rt.block_on(async {
    let (_dir, store, session) = create_test_store("concurrent_write_scan.db")?;

    let k = b"key:concurrent:scan:race";
    let v_old = b"version_1_scan_old";
    session.upsert(k, v_old).await?;

    let stage1_tail = store.tail_address();
    store.shift_read_only_address(stage1_tail);

    let v_new_concurrent = b"version_2_scan_concurrent_latest";
    session.upsert(k, v_new_concurrent).await?;

    let compactor = LogCompactor::new(Arc::clone(&store));
    let stats = compactor.compact(stage1_tail, CompactionType::Scan).await?;

    assert_eq!(stats.scanned_records, 1);
    assert_eq!(stats.live_copied, 0, "旧版本不得迁移");
    assert_eq!(stats.superseded, 1, "旧版本被并发新版本取代计为弃迁");
    assert_eq!(stats.dead_dropped, 0);

    let current_val = session.read(k).await?;
    assert_eq!(
      current_val.as_deref(),
      Some(v_new_concurrent.as_slice()),
      "并发写入的新数据必须绝对保留"
    );

    info!("Scan 模式并发写入安全测试验证通过");
    aok::Result::<()>::Ok(())
  })?;

  OK
}

/// 强哈希碰撞链重构验证：使用极小桶数（64 桶）强制产生大量碰撞与溢出桶链
#[test]
fn forced_hash_collision_chaining() -> Void {
  let rt = Runtime::new()?;
  rt.block_on(async {
    let dir = tempdir()?;
    let db_path = dir.path().join("hash_collision.db");
    let device = Arc::new(SegmentedDevice::single_file(&db_path)?);

    // 仅 64 个哈希桶，承载 1000 条记录
    let config = StoreConfig::new(64, 64 * 1024, 16, 0.5)?;
    let store = Arc::new(WedbStore::open(config, device)?);
    let session = store.new_session()?;

    let mut compact_until = 0u64;
    let mut k = String::with_capacity(20);
    let mut v = String::with_capacity(16);

    for i in 0..COLLISION_TOTAL {
      if i == COLLISION_CUT {
        compact_until = store.tail_address();
      }
      k.clear();
      let _ = write!(&mut k, "collision_key:{i:04}");
      v.clear();
      let _ = write!(&mut v, "orig_val:{i:04}");
      session.upsert(k.as_bytes(), v.as_bytes()).await?;
    }

    store.flush_and_evict_all().await?;

    // 覆写前 250 条记录
    for i in 0..COLLISION_OVERWRITE {
      k.clear();
      let _ = write!(&mut k, "collision_key:{i:04}");
      v.clear();
      let _ = write!(&mut v, "updated_val:{i:04}");
      session.upsert(k.as_bytes(), v.as_bytes()).await?;
    }

    store.shift_read_only_address(store.tail_address());

    let compactor = LogCompactor::new(Arc::clone(&store));
    let stats = compactor
      .compact(compact_until, CompactionType::Lookup)
      .await?;

    assert_eq!(store.begin_address(), compact_until);
    assert_eq!(stats.scanned_records, COLLISION_CUT);
    assert_eq!(stats.live_copied, COLLISION_OVERWRITE);
    assert_eq!(
      stats.superseded, COLLISION_OVERWRITE,
      "区外更新版本取代的旧版计为弃迁"
    );
    assert_eq!(stats.dead_dropped, 0);

    let mut expected = String::with_capacity(16);
    for i in 0..COLLISION_TOTAL {
      k.clear();
      let _ = write!(&mut k, "collision_key:{i:04}");
      let val = session.read(k.as_bytes()).await?;
      expected.clear();
      if i < COLLISION_OVERWRITE {
        let _ = write!(&mut expected, "updated_val:{i:04}");
      } else {
        let _ = write!(&mut expected, "orig_val:{i:04}");
      }
      assert_eq!(val.as_deref(), Some(expected.as_bytes()));
    }

    info!("强哈希碰撞链紧缩验证通过");
    aok::Result::<()>::Ok(())
  })?;

  OK
}

/// 极限边界：空键（b""）与大负载数据（32KB）紧缩与版本更新测试
#[test]
fn empty_key_and_large_payload() -> Void {
  let rt = Runtime::new()?;
  rt.block_on(async {
    let (_dir, store, session) = create_test_store("empty_large_compact.db")?;

    let empty_k = b"";
    session.upsert(empty_k, b"empty_key_v1").await?;
    session.upsert(empty_k, b"empty_key_v2_updated").await?;

    let large_k = b"large_key_32k";
    let large_v = vec![b'L'; 32 * 1024];
    session.upsert(large_k, &large_v).await?;

    let compact_until = store.tail_address();
    store.flush_and_evict_all().await?;

    session.upsert(b"normal_key", b"normal_val").await?;

    let compactor = LogCompactor::new(Arc::clone(&store));
    let stats = compactor
      .compact(compact_until, CompactionType::Scan)
      .await?;

    assert_eq!(stats.scanned_records, 3);
    assert_eq!(stats.live_copied, 2);
    assert_eq!(stats.superseded, 1, "区间内被 v2 取代的 v1 计为弃迁");
    assert_eq!(stats.dead_dropped, 0);
    assert_eq!(store.begin_address(), compact_until);

    assert_eq!(
      session.read(empty_k).await?.as_deref(),
      Some(b"empty_key_v2_updated".as_slice())
    );
    assert_eq!(session.read(large_k).await?.as_deref(), Some(&large_v[..]));
    assert_eq!(
      session.read(b"normal_key").await?.as_deref(),
      Some(b"normal_val".as_slice())
    );

    info!("空键与大尺寸负载紧缩验证通过");
    aok::Result::<()>::Ok(())
  })?;

  OK
}

/// 零长合法值（val == b"" 且非墓碑）在 Scan 模式下的存活与回读正确性
#[test]
fn zero_length_value_live() -> Void {
  let rt = Runtime::new()?;
  rt.block_on(async {
    let (_dir, store, session) = create_test_store("zero_val_live.db")?;

    let k_empty_val = b"key:empty:val";
    session.upsert(k_empty_val, b"").await?;

    let k_normal = b"key:normal";
    session.upsert(k_normal, b"hello_world").await?;

    let k_tombstone = b"key:tombstone";
    session.upsert(k_tombstone, b"will_be_deleted").await?;
    session.delete(k_tombstone).await?;

    let compact_until = store.tail_address();
    store.shift_read_only_address(compact_until);

    let compactor = LogCompactor::new(Arc::clone(&store));
    let stats = compactor
      .compact(compact_until, CompactionType::Scan)
      .await?;

    assert_eq!(stats.scanned_records, 4);
    assert_eq!(stats.live_copied, 2);
    assert_eq!(stats.superseded, 1, "初版被区间内墓碑取代计为弃迁");
    assert_eq!(stats.dead_dropped, 1, "墓碑计为判死丢弃");
    assert_eq!(store.begin_address(), compact_until);

    assert_eq!(
      session.read(k_empty_val).await?.as_deref(),
      Some(&[][..]),
      "零长度合法记录紧缩后必须正常读出 Some([])"
    );
    assert_eq!(
      session.read(k_normal).await?.as_deref(),
      Some(&b"hello_world"[..])
    );
    assert!(session.read(k_tombstone).await?.is_none());

    info!("零长值记录紧缩存活验证通过");
    aok::Result::<()>::Ok(())
  })?;

  OK
}

/// 验证启用 ReadCache 场景下的紧缩正确性（Lookup 模式）
#[test]
fn read_cache_compaction_lookup() -> Void {
  let rt = Runtime::new()?;
  rt.block_on(async {
    let (_dir, store, session) = create_read_cache_store("rc_lookup.db", 64, 4096, 8)?;
    assert!(store.read_cache.is_enabled);

    let mut k = String::with_capacity(16);
    let mut v = String::with_capacity(16);

    for i in 0..RC_TOTAL {
      k.clear();
      let _ = write!(&mut k, "rc_key:{i:03}");
      v.clear();
      let _ = write!(&mut v, "rc_val:{i:03}");
      session.upsert(k.as_bytes(), v.as_bytes()).await?;
    }

    let compact_until = store.tail_address();
    store.flush_and_evict_all().await?;

    // 冷读前 25 条记录，挂载进入 ReadCache
    for i in 0..RC_READ_COUNT {
      k.clear();
      let _ = write!(&mut k, "rc_key:{i:03}");
      v.clear();
      let _ = write!(&mut v, "rc_val:{i:03}");
      let res = session.read(k.as_bytes()).await?;
      assert_eq!(res.as_deref(), Some(v.as_bytes()));
    }

    let compactor = LogCompactor::new(Arc::clone(&store));
    let stats = compactor
      .compact(compact_until, CompactionType::Lookup)
      .await?;

    assert_eq!(stats.scanned_records, RC_TOTAL);
    assert_eq!(stats.live_copied, RC_TOTAL);
    assert_eq!(stats.dead_dropped, 0);
    assert_eq!(store.begin_address(), compact_until);

    for i in 0..RC_TOTAL {
      k.clear();
      let _ = write!(&mut k, "rc_key:{i:03}");
      v.clear();
      let _ = write!(&mut v, "rc_val:{i:03}");
      let res = session.read(k.as_bytes()).await?;
      assert_eq!(res.as_deref(), Some(v.as_bytes()));
    }

    info!("ReadCache 启用下的 Lookup 紧缩测试验证通过");
    aok::Result::<()>::Ok(())
  })?;

  OK
}

/// 验证启用 ReadCache 场景下的紧缩正确性（Scan 模式）
#[test]
fn read_cache_compaction_scan() -> Void {
  let rt = Runtime::new()?;
  rt.block_on(async {
    let (_dir, store, session) = create_read_cache_store("rc_scan.db", 64, 4096, 8)?;
    assert!(store.read_cache.is_enabled);

    let mut k = String::with_capacity(16);
    let mut v = String::with_capacity(16);

    for i in 0..RC_TOTAL {
      k.clear();
      let _ = write!(&mut k, "rc_key:{i:03}");
      v.clear();
      let _ = write!(&mut v, "rc_val:{i:03}");
      session.upsert(k.as_bytes(), v.as_bytes()).await?;
    }

    let compact_until = store.tail_address();
    store.flush_and_evict_all().await?;

    for i in 0..RC_READ_COUNT {
      k.clear();
      let _ = write!(&mut k, "rc_key:{i:03}");
      v.clear();
      let _ = write!(&mut v, "rc_val:{i:03}");
      let res = session.read(k.as_bytes()).await?;
      assert_eq!(res.as_deref(), Some(v.as_bytes()));
    }

    let compactor = LogCompactor::new(Arc::clone(&store));
    let stats = compactor
      .compact(compact_until, CompactionType::Scan)
      .await?;

    assert_eq!(stats.scanned_records, RC_TOTAL);
    assert_eq!(stats.live_copied, RC_TOTAL);
    assert_eq!(stats.dead_dropped, 0);
    assert_eq!(store.begin_address(), compact_until);

    for i in 0..RC_TOTAL {
      k.clear();
      let _ = write!(&mut k, "rc_key:{i:03}");
      v.clear();
      let _ = write!(&mut v, "rc_val:{i:03}");
      let res = session.read(k.as_bytes()).await?;
      assert_eq!(res.as_deref(), Some(v.as_bytes()));
    }

    info!("ReadCache 启用下的 Scan 紧缩测试验证通过");
    aok::Result::<()>::Ok(())
  })?;

  OK
}

/// 良性 CAS 竞争回归压力测试：紧缩期间并发读触发 ReadCache 挂链提升与驱逐回写，
/// 索引槽位被良性改写（主日志地址 <-> RC 虚拟地址）时存活记录必须经复核重试成功迁移，
/// 绝不随截断物理丢弃（对标 C# ConditionalCopyToTail 重试环的语义：仅真并发覆盖才放弃）
#[test]
fn compaction_with_concurrent_read_rc_index_rewrite() -> Void {
  let rt = Runtime::new()?;
  rt.block_on(async {
    let (_dir, store, session) = create_read_cache_store("rc_race_compact.db", 1024, 4096, 4)?;

    const N: usize = 400;
    let mut k = String::with_capacity(16);
    let mut v = String::with_capacity(24);

    for i in 0..N {
      k.clear();
      let _ = write!(&mut k, "race:{i:04}");
      v.clear();
      let _ = write!(&mut v, "race_val:{i:04}:payload");
      session.upsert(k.as_bytes(), v.as_bytes()).await?;
    }

    store.flush_and_evict_all().await?;
    let compact_until = store.tail_address();
    store.shift_read_only_address(compact_until);

    // 并发读线程：独立运行时反复冷读紧缩区间键，持续触发 RC 挂链提升与窗口驱逐，
    // 在紧缩"索引探查 -> 尾部追加 -> CAS 替换"窗口内良性改写索引槽位
    let store_bg = Arc::clone(&store);
    let stop = Arc::new(AtomicBool::new(false));
    let stop_bg = Arc::clone(&stop);
    let reader = thread::spawn(move || {
      let rt2 = Runtime::new()?;
      rt2.block_on(async {
        let s2 = store_bg.new_session()?;
        let mut k2 = String::with_capacity(16);
        while !stop_bg.load(Ordering::Relaxed) {
          for i in 0..N {
            k2.clear();
            let _ = write!(&mut k2, "race:{i:04}");
            let _ = s2.read(k2.as_bytes()).await?;
          }
        }
        aok::Result::<()>::Ok(())
      })
    });

    let compactor = LogCompactor::new(Arc::clone(&store));
    let stats = compactor
      .compact(compact_until, CompactionType::Lookup)
      .await?;

    stop.store(true, Ordering::Relaxed);
    reader.join().unwrap()?;

    // 紧缩区间全部记录必须存活迁移（无并发写，良性竞争经复核重试后不得弃迁）
    assert_eq!(stats.scanned_records, N, "全部记录必须参与扫描");
    assert_eq!(stats.live_copied, N, "存活记录必须全部迁移");
    assert_eq!(stats.dead_dropped, 0);

    for i in 0..N {
      k.clear();
      let _ = write!(&mut k, "race:{i:04}");
      v.clear();
      let _ = write!(&mut v, "race_val:{i:04}:payload");
      let val = session.read(k.as_bytes()).await?;
      assert_eq!(val.as_deref(), Some(v.as_bytes()), "记录不得丢失: {k}");
    }

    info!(
      "并发读良性索引改写下的紧缩存活验证通过, 释放字节={}",
      stats.bytes_freed
    );
    aok::Result::<()>::Ok(())
  })?;

  OK
}
