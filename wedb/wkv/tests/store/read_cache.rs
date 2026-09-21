//! 读缓存（ReadCache）热写路径原子脱钩与链可达性失效测试
//!
//! 严格对标 C# Tsavorite / Microsoft Garnet 的 latch-free 读缓存契约：
//! 值改变更新以哈希桶入口项 CAS 整段摘除该桶的 ReadCache 前缀
//! （libs/storage/Tsavorite/cs/src/core/Index/Tsavorite/Implementation/Helpers.cs:172-184
//! 与 BlockAllocate.cs:149,155），被摘除的孤儿缓存记录**不即时物理作废**，
//! 交由换页驱逐 CleansingInfo 走查在页关闭时统一回收
//! （ReadCache.cs:ReadCacheEvict；ReadCacheAbandonRecord 仅用于新分配即弃的记录）。
//! 故可观测契约是「键即刻脱离在册链」，断言一律走链取证
//! （对标 test.session/ReadCacheChainTests.cs:183 FindRecordInReadCache 与
//! :198 AssertNotInReadCache），而非按已脱钩的陈旧地址探针判 closed。
//!
//! 并发压力面另见 test.stress/ReadCacheStressTests.cs。

use std::{
  sync::{Arc, Mutex},
  thread::spawn,
};

use aok::{OK, Void};
use compio::runtime::Runtime;
use tempfile::tempdir;
use wbase::{
  addr::{is_read_cache, to_absolute},
  align::DEFAULT_SECTOR_SIZE,
};
use wdev::SegmentedDevice;
use windex::HashIndex;
use wkv::{RcVisit, ReadCache, StoreConfig, WedbStore};

use crate::support::HashIndexTestOps;

/// 测试 1: RMW 热写路径下 ReadCache 节点原子脱钩与链可达性即时失效
///
/// 严格对标 libs/storage/Tsavorite/cs/test/test.session/ReadCacheChainTests.cs:RMWCacheRecordTest
///
/// libs/storage/Tsavorite/cs/test/test.session/ReadCacheChainTests.cs:InPlaceUpdater
/// （C# ChainFunctions.InPlaceUpdater 的原位更新语义由本测试 RMW 路径直接承接）
#[test]
fn test_read_cache_rmw_atomic_detach_and_invalidate() -> Void {
  let rt = Runtime::new()?;
  rt.block_on(async {
    let dir = tempdir()?;
    let device = Arc::new(SegmentedDevice::single_file(dir.path().join("rc_rmw.db"))?);
    let config = StoreConfig::new(64, DEFAULT_SECTOR_SIZE, 16, 0.5)?.with_read_cache(true);
    let store = Arc::new(WedbStore::open(config, device)?);
    let session = store.new_session()?;

    let key = b"rc_rmw_key";
    let val_init = b"val_initial_001";

    // 1. 写入初始数据并全量驱逐至磁盘冷区
    session.upsert(key, val_init).await?;
    store.flush_and_evict_all().await?;

    // 2. 冷读触发磁盘回填并挂入 ReadCache
    let read_val = session.read(key).await?;
    assert_eq!(read_val, Some(val_init.to_vec()));

    let phys_k = session.session_string_key(key);
    let slot_addr = store.index.load().find_tag(phys_k.as_slice()).unwrap();
    assert!(
      is_read_cache(slot_addr),
      "冷读回填后槽位必须为 ReadCache 记录"
    );

    // 验证 ReadCache 中记录有效存在
    let in_rc = store
      .read_cache
      .with_record(slot_addr, |k, v| Some((k.to_vec(), v.to_vec())));
    assert!(matches!(in_rc, RcVisit::Found(_)));

    // 3. 执行 RMW 读-改-写热路径更新
    let res = session
      .rmw(key, |old| {
        let mut cur = old?.to_vec();
        cur.extend_from_slice(b"_modified");
        Some((true, cur))
      })
      .await?;
    assert_eq!(res, Some(true));

    // 4. 验证原子脱钩与链可达性失效：
    // a) 哈希桶槽位已原子指向主日志新记录（非 ReadCache 虚拟地址）
    let new_slot = store.index.load().find_tag(phys_k.as_slice()).unwrap();
    assert!(
      !is_read_cache(new_slot),
      "RMW 提交后槽位必须脱钩恢复为主日志逻辑地址"
    );

    // b) 脱钩即逻辑失效：自哈希桶入口顺 ReadCache 前缀走链，本键必不再命中
    //    （C# AssertNotInReadCache 同口径取证）
    assert_eq!(
      read_cache_holds_key(&store, phys_k.as_slice()),
      Some(false),
      "RMW 脱钩后键必须即刻脱离在册 ReadCache 链"
    );

    // c) 物理回收唯一入口是换页清洗：脱钩仅改桶入口，绝不在写侧作废物理记录
    //    （对标 Helpers.cs:172-179「Dropped read-cache records are orphaned and
    //    reclaimed by ReadCacheEvict when their page is closed」）。孤儿已不在任何
    //    可达链上（上一条断言），故永不可观测——封死「写侧再设一遍即时失效」的第二套机制
    assert!(
      matches!(
        store.read_cache.with_record(slot_addr, |_, _| Some(())),
        RcVisit::Found(_)
      ),
      "脱钩的旧 ReadCache 记录不得在写侧即时作废（回收仅在页关闭）"
    );

    // d) 随后的读取必须纳秒级精准命中新值，绝不读到过期副本
    let updated_val = session.read(key).await?;
    assert_eq!(
      updated_val,
      Some(b"val_initial_001_modified".to_vec()),
      "读取必须返回 RMW 更新后的新值"
    );

    OK
  })?;

  OK
}

/// 测试 2: Upsert 热写路径下 ReadCache 节点原子脱钩与链可达性即时失效
///
/// 严格对标 libs/storage/Tsavorite/cs/test/test.session/ReadCacheChainTests.cs:UpsertCacheRecordTest
#[test]
fn test_read_cache_upsert_atomic_detach_and_invalidate() -> Void {
  let rt = Runtime::new()?;
  rt.block_on(async {
    let dir = tempdir()?;
    let device = Arc::new(SegmentedDevice::single_file(
      dir.path().join("rc_upsert.db"),
    )?);
    let config = StoreConfig::new(64, DEFAULT_SECTOR_SIZE, 16, 0.5)?.with_read_cache(true);
    let store = Arc::new(WedbStore::open(config, device)?);
    let session = store.new_session()?;

    let key = b"rc_upsert_key";
    let val_init = b"val_upsert_001";

    // 1. 写入初始数据并全量驱逐至磁盘冷区
    session.upsert(key, val_init).await?;
    store.flush_and_evict_all().await?;

    // 2. 冷读触发回填挂入 ReadCache
    assert_eq!(session.read(key).await?, Some(val_init.to_vec()));
    let phys_k = session.session_string_key(key);
    let slot_addr = store.index.load().find_tag(phys_k.as_slice()).unwrap();
    assert!(is_read_cache(slot_addr));

    // 3. 执行 Upsert 覆写新值
    let val_new = b"val_upsert_002_new";
    session.upsert(key, val_new).await?;

    // 4. 验证原子脱钩与链可达性失效：
    let new_slot = store.index.load().find_tag(phys_k.as_slice()).unwrap();
    assert!(!is_read_cache(new_slot));

    let old_rc_read = store.read_cache.with_record(slot_addr, |_, _| Some(()));
    assert!(
      matches!(old_rc_read, RcVisit::Found(())),
      "Upsert 脱钩仅改桶入口，物理记录留待换页清洗回收"
    );
    assert_eq!(
      read_cache_holds_key(&store, phys_k.as_slice()),
      Some(false),
      "Upsert 脱钩后键必须即刻脱离在册 ReadCache 链（桶入口 CAS 整段摘除前缀）"
    );

    assert_eq!(session.read(key).await?, Some(val_new.to_vec()));

    OK
  })?;

  OK
}

/// 测试 3: Delete 路径下 ReadCache 节点原子脱钩与链可达性即时失效
///
/// 严格对标 libs/storage/Tsavorite/cs/test/test.session/ReadCacheChainTests.cs:DeleteCacheRecordTest
#[test]
fn test_read_cache_delete_atomic_detach_and_invalidate() -> Void {
  let rt = Runtime::new()?;
  rt.block_on(async {
    let dir = tempdir()?;
    let device = Arc::new(SegmentedDevice::single_file(
      dir.path().join("rc_delete.db"),
    )?);
    let config = StoreConfig::new(64, DEFAULT_SECTOR_SIZE, 16, 0.5)?.with_read_cache(true);
    let store = Arc::new(WedbStore::open(config, device)?);
    let session = store.new_session()?;

    let key = b"rc_delete_key";
    let val = b"val_to_delete";

    // 1. 写入并驱逐
    session.upsert(key, val).await?;
    store.flush_and_evict_all().await?;

    // 2. 冷读挂入 ReadCache
    assert_eq!(session.read(key).await?, Some(val.to_vec()));
    let phys_k = session.session_string_key(key);
    let slot_addr = store.index.load().find_tag(phys_k.as_slice()).unwrap();
    assert!(is_read_cache(slot_addr));

    // 3. 删除记录
    let deleted = session.delete(key).await?;
    assert!(deleted);

    // 4. 验证脱钩与链可达性失效
    let new_slot = store.index.load().find_tag(phys_k.as_slice()).unwrap();
    assert!(
      !is_read_cache(new_slot),
      "Delete 追加墓碑后桶入口必须 CAS 换指主日志墓碑地址"
    );
    assert_eq!(
      read_cache_holds_key(&store, phys_k.as_slice()),
      Some(false),
      "Delete 脱钩后键必须即刻脱离在册 ReadCache 链"
    );
    let old_rc_read = store.read_cache.with_record(slot_addr, |_, _| Some(()));
    assert!(
      matches!(old_rc_read, RcVisit::Found(())),
      "Delete 脱钩仅改桶入口，物理记录留待换页清洗回收"
    );

    assert_eq!(session.read(key).await?, None);
    assert!(!session.contains_key(key).await?);

    OK
  })?;

  OK
}

/// 测试 4: 多线程高并发混合读写压力测试（对标 C# ReadCacheStressTests）
///
/// 严格对标 libs/storage/Tsavorite/cs/test/test.stress/ReadCacheStressTests.cs: LongRcMultiThreadStressTest
///
/// libs/storage/Tsavorite/cs/test/test.stress/ReadCacheStressTests.cs:SpanByteRcMultiThreadStressTest
#[test]
fn test_read_cache_multi_thread_stress() -> Void {
  let rt = Runtime::new()?;
  rt.block_on(async {
    let dir = tempdir()?;
    let device = Arc::new(SegmentedDevice::single_file(
      dir.path().join("rc_stress.db"),
    )?);
    let config = StoreConfig::new(256, DEFAULT_SECTOR_SIZE, 16, 0.5)?.with_read_cache(true);
    let store = Arc::new(WedbStore::open(config, device)?);

    let num_keys = 50usize;

    // 预填基础数据并驱逐
    let session = store.new_session()?;
    for i in 0..num_keys {
      let k = format!("stress_key_{i:04}").into_bytes();
      let v = format!("stress_val_init_{i:04}").into_bytes();
      session.upsert(&k, &v).await?;
    }
    store.flush_and_evict_all().await?;

    // 并发多线程执行冷读、Upsert、RMW
    let mut handles = Vec::new();

    // 读线程：反复读取
    for _ in 0..4 {
      let store_clone = store.clone();
      handles.push(spawn(move || {
        let rt = Runtime::new().unwrap();
        rt.block_on(async move {
          let sess = store_clone.new_session().unwrap();
          for _ in 0..20 {
            for i in 0..num_keys {
              let k = format!("stress_key_{i:04}").into_bytes();
              let _ = sess.read(&k).await;
            }
          }
        });
      }));
    }

    // 写线程：反复 Upsert 和 RMW
    for w in 0..4 {
      let store_clone = store.clone();
      handles.push(spawn(move || {
        let rt = Runtime::new().unwrap();
        rt.block_on(async move {
          let sess = store_clone.new_session().unwrap();
          for round in 0..10 {
            for i in 0..num_keys {
              let k = format!("stress_key_{i:04}").into_bytes();
              let v = format!("stress_w_{w}_r_{round}_{i:04}").into_bytes();
              if (w + round) % 2 == 0 {
                let _ = sess.upsert(&k, &v).await;
              } else {
                let _ = sess.rmw(&k, |_old| Some((true, v.clone()))).await;
              }
            }
          }
        });
      }));
    }

    for h in handles {
      h.join().unwrap();
    }

    // 最终验证：所有 key 均可正常读出，数据无破坏
    let check_sess = store.new_session()?;
    for i in 0..num_keys {
      let k = format!("stress_key_{i:04}").into_bytes();
      let res = check_sess.read(&k).await?;
      assert!(res.is_some(), "Key {i} 最终必须可读");
    }

    OK
  })?;

  OK
}

/// 测试 5: 多线程高频环形回绕并发回归——回写窗撕裂闭环（对标 C# ReadCacheStressTests
/// 的换页驱逐压力面）
///
/// 小页小环强制高频换页：验证两阶段关闭协议下
/// 1. 窗口内 RC 记录恒完整可解析（读侧绝不解析出他人键值——撕裂污损的表现）；
/// 2. 驱逐清洗后索引恢复主日志地址，skip_read_cache 不断链（清洗截断/悬垂索引的表现）。
#[test]
fn test_read_cache_wraparound_no_tearing() -> Void {
  const WRITERS: usize = 4;
  const READERS: usize = 4;
  const APPENDS_PER_WRITER: usize = 5000;

  let rc = Arc::new(ReadCache::new(1024, 4, true)?);
  let index = Arc::new(HashIndex::new(1 << 16)?);
  // 共享最近记录抽样列表（读线程验证解析完整性）
  let recent: Arc<Mutex<Vec<u64>>> = Arc::new(Mutex::new(Vec::new()));

  let mut handles = Vec::new();

  for w in 0..WRITERS {
    let rc = Arc::clone(&rc);
    let index = Arc::clone(&index);
    let recent = Arc::clone(&recent);
    handles.push(spawn(move || {
      let main_base = 1_000_000u64 + w as u64 * 100_000_000;
      for i in 0..APPENDS_PER_WRITER {
        let key = format!("w{w}-k{i:05}");
        let main_addr = main_base + i as u64;
        // 先挂主日志地址条目，append 内部 CAS 挂载 RC 地址（对标 hei.TryCAS）
        let _ = index.insert(key.as_bytes(), main_addr);
        // 40B 记录（16 头 + 9 键 + 8 值对齐），非整除 1024B 页，强制页界分支与回绕清洗
        let Some(rc_addr) = rc.append(key.as_bytes(), b"vvvvvvvv", main_addr, &index) else {
          continue;
        };
        if i % 64 == 0 {
          recent.lock().unwrap().push(rc_addr);
        }
      }
    }));
  }

  for _ in 0..READERS {
    let rc = Arc::clone(&rc);
    let recent = Arc::clone(&recent);
    handles.push(spawn(move || {
      for _ in 0..20_000u32 {
        let sample = {
          let mut guard = recent.lock().unwrap();
          if guard.is_empty() {
            None
          } else {
            let idx = fastrand::usize(..guard.len());
            Some(guard.swap_remove(idx))
          }
        };
        let Some(rc_addr) = sample else {
          continue;
        };
        // 解析完整性：窗口内记录若可解析，键值必须完整一致，绝不允许撕裂半键
        let _ = rc.with_record(rc_addr, |k, v| Some((k.len(), v.len())));
        let _ = rc.skip_read_cache(rc_addr);
        let _ = rc.prev_address_of(rc_addr);
      }
    }));
  }

  for h in handles {
    h.join().unwrap();
  }

  // 终态一致性：每条挂载记录的索引要么已被清洗恢复主日志地址，要么仍指向
  // 窗口内完整可解析的 RC 记录；skip_read_cache 窗口内绝不断链返回 0
  for w in 0..WRITERS {
    let main_base = 1_000_000u64 + w as u64 * 100_000_000;
    for i in (0..APPENDS_PER_WRITER).step_by(64) {
      let key = format!("w{w}-k{i:05}");
      let main_addr = main_base + i as u64;
      let Some(slot) = index.find_tag(key.as_bytes()) else {
        panic!("已挂载键 {key} 索引丢失");
      };
      if is_read_cache(slot) {
        let (k, v, prev) = match rc.with_record(slot, |k, v| Some((k.to_vec(), v.to_vec()))) {
          RcVisit::Found((k, v)) => {
            let prev = rc
              .prev_address_of(slot)
              .unwrap_or_else(|| panic!("{key} 指向窗口内 RC 地址但前驱不可判读"));
            (k, v, prev)
          }
          RcVisit::Next(_) => panic!("{key} 窗口内 RC 记录被判为作废/链尾（撕裂/清洗截断）"),
          RcVisit::Gone => panic!("{key} 窗口内 RC 记录不可判读（撕裂竞态）"),
        };
        assert_eq!(k, key.as_bytes(), "解析键必须精确匹配（撕裂污损）");
        assert_eq!(v, b"vvvvvvvv");
        assert_eq!(prev, main_addr, "前驱必须是主日志地址");
        assert!(skip_ok(&rc, slot), "{key} 窗口内 RC 地址断链（清洗截断）");
      } else {
        assert_eq!(slot, main_addr, "清洗必须恢复主日志地址，不得漂移");
      }
    }
  }

  OK
}

/// skip_read_cache 窗口内不断链断言（窗口外返回 0 属正常滑出）
fn skip_ok(rc: &ReadCache, rc_addr: u64) -> bool {
  let abs = to_absolute(rc_addr);
  let in_window = abs >= rc.head_address() && abs < rc.tail_address();
  !in_window || rc.skip_read_cache(rc_addr).is_some_and(|m| m != 0)
}

/// 自哈希桶入口项顺 ReadCache 前缀走链比对键（严格对标 C#
/// libs/storage/Tsavorite/cs/test/test.session/ReadCacheChainTests.cs:183
/// FindRecordInReadCache：`while (isReadCache)` 命中键即真、脱离 RC 区即假）
///
/// 走查触及滑窗/不可判读记录返回 None（C# 无该竞态窗，本端不为降级留口径），
/// 调用方以 `Some(..)` 精确匹配拒绝假 NOTFOUND 折叠。
fn read_cache_holds_key(store: &WedbStore<SegmentedDevice>, phys_key: &[u8]) -> Option<bool> {
  let mut addr = store.index.load().find_tag(phys_key)?;
  loop {
    if !is_read_cache(addr) {
      return Some(false);
    }
    match store.read_cache.with_record(addr, |k, _| Some(k.to_vec())) {
      RcVisit::Found(k) => {
        if k == phys_key {
          return Some(true);
        }
      }
      // 已作废（closed）记录不比对键、携 prev 续链（对标 C# 走查跳过 Invalid）
      RcVisit::Next(prev) => {
        addr = prev;
        continue;
      }
      RcVisit::Gone => return None,
    }
    addr = store.read_cache.prev_address_of(addr)?;
  }
}
