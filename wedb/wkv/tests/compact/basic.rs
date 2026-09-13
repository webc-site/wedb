//! Wedb 存储引擎级紧缩集成测试（物理段文件截断、PadRecord 换页、极端边界与混合负载）

use std::{fmt::Write, sync::Arc};

use aok::{OK, Void};
use compio::runtime::Runtime;
use log::info;
use tempfile::tempdir;
use wcompact::{CompactionType, Error, LogCompactor};
use wdev::SegmentedDevice;
use wkv::{StoreConfig, WedbStore};

const MULTI_SEG_RECORDS: usize = 600;

/// 跨多页多段文件物理截断与磁盘空间回收验证
#[test]
fn multi_segment_physical_truncation() -> Void {
  let rt = Runtime::new()?;
  rt.block_on(async {
    let dir = tempdir()?;
    let base_path = dir.path().join("seg_db");
    let segment_size = 8192u64;
    let page_size = 4096usize;
    let device = Arc::new(SegmentedDevice::segmented(&base_path, segment_size)?);

    let config = StoreConfig::new(1024, page_size, 16, 0.5)?;
    let store = Arc::new(WedbStore::open(config, Arc::clone(&device))?);
    let session = store.new_session()?;

    let mut k = String::with_capacity(16);
    let mut v = String::with_capacity(64);

    // 写入多条记录跨越至少 4 个段
    for i in 0..MULTI_SEG_RECORDS {
      k.clear();
      let _ = write!(&mut k, "key_{i:04}");
      v.clear();
      let _ = write!(
        &mut v,
        "val_{i:04}_long_payload_padding_data_to_fill_segments"
      );
      session.upsert(k.as_bytes(), v.as_bytes()).await?;
    }

    store.flush_all().await?;
    let tail = store.tail_address();
    store.shift_read_only_address(tail);

    let seg0_path = device.segment_path(0);
    let seg1_path = device.segment_path(1);
    let seg2_path = device.segment_path(2);
    assert!(seg0_path.exists());
    assert!(seg1_path.exists());
    assert!(seg2_path.exists());

    // 紧缩目标地址设置为段 2 的起始边界（2 * 8192 = 16384）
    let until_address = 2 * segment_size;
    let compactor = LogCompactor::new(Arc::clone(&store));
    let stats = compactor
      .compact(until_address, CompactionType::Scan)
      .await?;

    assert_eq!(stats.new_begin_address, until_address);
    assert_eq!(store.begin_address(), until_address);

    // 物理截断断言：段 0 与段 1 必须被物理删除回收，段 2 保留
    assert!(!seg0_path.exists(), "段 0 文件紧缩后必须已被物理截断删除");
    assert!(!seg1_path.exists(), "段 1 文件紧缩后必须已被物理截断删除");
    assert!(seg2_path.exists(), "段 2 文件必须保留");

    for i in 0..MULTI_SEG_RECORDS {
      k.clear();
      let _ = write!(&mut k, "key_{i:04}");
      v.clear();
      let _ = write!(
        &mut v,
        "val_{i:04}_long_payload_padding_data_to_fill_segments"
      );
      let val = session.read(k.as_bytes()).await?;
      assert_eq!(val.as_deref(), Some(v.as_bytes()));
    }

    info!("跨多页多段文件物理截断与空间回收验证通过");
    aok::Result::<()>::Ok(())
  })?;

  OK
}

/// 边界条件与容错测试（超出只读区拒绝、空紧缩与幂等性）
#[test]
fn edge_cases() -> Void {
  let rt = Runtime::new()?;
  rt.block_on(async {
    let dir = tempdir()?;
    let db_path = dir.path().join("compact_edge.db");
    let device = Arc::new(SegmentedDevice::single_file(&db_path)?);

    let config = StoreConfig::new(1024, 64 * 1024, 16, 0.5)?;
    let store = Arc::new(WedbStore::open(config, device)?);
    let session = store.new_session()?;

    session.upsert(b"foo", b"bar").await?;
    let tail = store.tail_address();
    let compactor = LogCompactor::new(Arc::clone(&store));

    // 尝试紧缩超过 read_only_address 的地址应直接返回错误
    let err = compactor.compact(tail + 100, CompactionType::Lookup).await;
    assert!(
      matches!(err, Err(Error::UntilAddressOutOfRange { .. })),
      "预期 UntilAddressOutOfRange 错误"
    );

    // until_address <= begin_address 应直接返回空统计且不报错
    let begin = store.begin_address();
    let stats = compactor.compact(begin, CompactionType::Lookup).await?;
    assert!(stats.is_empty());
    assert_eq!(stats.bytes_freed, 0);

    info!("边界条件与容错测试验证通过");
    aok::Result::<()>::Ok(())
  })?;

  OK
}

/// 紧缩目标地址落在记录中部时的记录边界对齐验证（对标 C# "Ensure address is at record boundary"）
#[test]
fn mid_record_until_snaps_to_boundary() -> Void {
  let rt = Runtime::new()?;
  rt.block_on(async {
    let dir = tempdir()?;
    let db_path = dir.path().join("compact_mid_record.db");
    let device = Arc::new(SegmentedDevice::single_file(&db_path)?);

    let config = StoreConfig::new(1024, 64 * 1024, 16, 0.5)?;
    let store = Arc::new(WedbStore::open(config, device)?);
    let session = store.new_session()?;

    session.upsert(b"r0", b"val_0").await?;
    let addr_r1 = store.tail_address();
    session.upsert(b"r1", b"val_1").await?;
    let addr_r2 = store.tail_address();
    session.upsert(b"r2", b"val_2").await?;

    store.flush_all().await?;
    store.shift_read_only_address(store.tail_address());

    // 紧缩目标落在第 2 条记录（r1）内部：r1 必须完整参与紧缩，截断点对齐推进至 r2 起始边界
    let compactor = LogCompactor::new(Arc::clone(&store));
    let stats = compactor
      .compact(addr_r1 + 1, CompactionType::Lookup)
      .await?;

    assert_eq!(stats.scanned_records, 2);
    assert_eq!(stats.live_copied, 2);
    assert_eq!(stats.dead_dropped, 0);
    assert_eq!(stats.new_begin_address, addr_r2);
    assert_eq!(store.begin_address(), addr_r2);

    assert_eq!(
      session.read(b"r0").await?.as_deref(),
      Some(b"val_0".as_slice())
    );
    assert_eq!(
      session.read(b"r1").await?.as_deref(),
      Some(b"val_1".as_slice())
    );
    assert_eq!(
      session.read(b"r2").await?.as_deref(),
      Some(b"val_2".as_slice())
    );

    info!("记录中部紧缩目标边界对齐验证通过");
    aok::Result::<()>::Ok(())
  })?;

  OK
}

/// 空日志与零写入状态下的紧缩极端边界测试
#[test]
fn pure_empty_store() -> Void {
  let rt = Runtime::new()?;
  rt.block_on(async {
    let dir = tempdir()?;
    let db_path = dir.path().join("compact_empty.db");
    let device = Arc::new(SegmentedDevice::single_file(&db_path)?);

    let config = StoreConfig::new(1024, 64 * 1024, 16, 0.5)?;
    let store = Arc::new(WedbStore::open(config, device)?);
    let compactor = LogCompactor::new(Arc::clone(&store));
    let begin = store.begin_address();

    // 零写入存储引擎紧缩 begin_address
    let stats_lookup = compactor.compact(begin, CompactionType::Lookup).await?;
    assert!(stats_lookup.is_empty());
    assert_eq!(stats_lookup.new_begin_address, begin);
    assert_eq!(stats_lookup.bytes_freed, 0);

    let stats_scan = compactor.compact(begin, CompactionType::Scan).await?;
    assert!(stats_scan.is_empty());
    assert_eq!(stats_scan.new_begin_address, begin);
    assert_eq!(stats_scan.bytes_freed, 0);

    // 传入小于 begin_address 的地址返回空统计
    let stats_zero = compactor.compact(0, CompactionType::Lookup).await?;
    assert!(stats_zero.is_empty());

    // 传入大于 read_only_address 的地址必须报错
    let err = compactor.compact(begin + 1, CompactionType::Lookup).await;
    assert!(
      matches!(err, Err(Error::UntilAddressOutOfRange { .. })),
      "预期 UntilAddressOutOfRange 错误"
    );

    info!("空日志与零写入状态下的紧缩边界测试验证通过");
    aok::Result::<()>::Ok(())
  })?;

  OK
}

/// 跨物理页与 PadRecord 换页边界紧缩验证
#[test]
fn exact_page_boundary_and_pad() -> Void {
  let rt = Runtime::new()?;
  rt.block_on(async {
    let dir = tempdir()?;
    let db_path = dir.path().join("compact_page_pad.db");
    let device = Arc::new(SegmentedDevice::single_file(&db_path)?);

    let page_size = 4096usize;
    let config = StoreConfig::new(1024, page_size, 16, 0.5)?;
    let store = Arc::new(WedbStore::open(config, device)?);
    let session = store.new_session()?;

    const P0_PAYLOAD: [u8; 100] = [b'x'; 100];
    const P1_PAYLOAD: [u8; 100] = [b'y'; 100];

    let mut k = String::with_capacity(16);

    // 填充第 0 页至 4024 字节，剩余 72 字节无法放入 120 字节记录，产生 PadRecord 并换至页 1
    for i in 0..31 {
      k.clear();
      let _ = write!(&mut k, "{i:04}");
      session.upsert_raw(k.as_bytes(), &P0_PAYLOAD).await?;
    }

    session.upsert_raw(b"p1_0", &P1_PAYLOAD).await?;
    session.upsert_raw(b"p1_1", &P1_PAYLOAD).await?;

    for i in 2..6 {
      k.clear();
      let _ = write!(&mut k, "p1_{i}");
      session.upsert_raw(k.as_bytes(), &P1_PAYLOAD).await?;
    }

    store.flush_all().await?;
    let tail = store.tail_address();
    store.shift_read_only_address(tail);

    // 紧缩目标地址设在 PadRecord 区间内（4050），验证紧缩器自动对齐跳过 PadRecord
    let compactor = LogCompactor::new(Arc::clone(&store));
    let stats = compactor.compact(4050, CompactionType::Lookup).await?;

    assert_eq!(stats.new_begin_address, 4096);
    assert_eq!(store.begin_address(), 4096);
    assert_eq!(stats.scanned_records, 33);
    assert_eq!(stats.live_copied, 33);
    assert_eq!(stats.dead_dropped, 0);

    for i in 0..31 {
      k.clear();
      let _ = write!(&mut k, "{i:04}");
      let val = session.read_raw(k.as_bytes()).await?;
      assert_eq!(val.as_deref(), Some(&P0_PAYLOAD[..]));
    }
    for i in 0..6 {
      k.clear();
      let _ = write!(&mut k, "p1_{i}");
      let val = session.read_raw(k.as_bytes()).await?;
      assert_eq!(val.as_deref(), Some(&P1_PAYLOAD[..]));
    }

    info!("跨物理页与 PadRecord 换页边界紧缩验证通过");
    aok::Result::<()>::Ok(())
  })?;

  OK
}

/// 大尺寸（16KB）、小尺寸（10B）、零长值（0B）混合负载多代连续紧缩
#[test]
fn mixed_large_small_zero_payloads() -> Void {
  let rt = Runtime::new()?;
  rt.block_on(async {
    let dir = tempdir()?;
    let db_path = dir.path().join("compact_mixed.db");
    let device = Arc::new(SegmentedDevice::single_file(&db_path)?);

    let config = StoreConfig::new(1024, 64 * 1024, 16, 0.5)?;
    let store = Arc::new(WedbStore::open(config, device)?);
    let session = store.new_session()?;

    let k_large = b"mix:large";
    let v_large = vec![b'L'; 16 * 1024];
    let k_small = b"mix:small";
    let v_small = b"small_10b_";
    let k_zero = b"mix:zero";
    let v_zero = b"";

    session.upsert(k_large, &v_large).await?;
    session.upsert(k_small, v_small).await?;
    session.upsert(k_zero, v_zero).await?;

    let cut1 = store.tail_address();
    store.shift_read_only_address(cut1);

    let compactor = LogCompactor::new(Arc::clone(&store));
    let stats1 = compactor.compact(cut1, CompactionType::Lookup).await?;
    assert_eq!(stats1.scanned_records, 3);
    assert_eq!(stats1.live_copied, 3);
    assert_eq!(stats1.dead_dropped, 0);

    assert_eq!(
      session.read(k_large).await?.as_deref(),
      Some(v_large.as_slice())
    );
    assert_eq!(
      session.read(k_small).await?.as_deref(),
      Some(v_small.as_slice())
    );
    assert_eq!(session.read(k_zero).await?.as_deref(), Some(&[][..]));

    // 更新大值与零长值，删除小值
    let v_large_v2 = vec![b'M'; 8 * 1024];
    session.upsert(k_large, &v_large_v2).await?;
    session.upsert(k_zero, b"now_not_empty").await?;
    session.delete(k_small).await?;

    let cut2 = store.tail_address();
    store.shift_read_only_address(cut2);

    let stats2 = compactor.compact(cut2, CompactionType::Scan).await?;
    assert_eq!(
      session.read(k_large).await?.as_deref(),
      Some(v_large_v2.as_slice())
    );
    assert!(session.read(k_small).await?.is_none());
    assert_eq!(
      session.read(k_zero).await?.as_deref(),
      Some(b"now_not_empty".as_slice())
    );

    info!(
      "大/小/零混合负载多代连续紧缩验证通过, 释放字节={}",
      stats2.bytes_freed
    );
    aok::Result::<()>::Ok(())
  })?;

  OK
}

/// CAS 竞争重试预算耗尽（预算 0 = 耗尽）且记录仍为最新存活版本时，
/// 截断点必须回退至该存活记录起始边界：绝不截断存活数据，墓碑等死记录照常退役
#[test]
fn cas_retry_exhausted_retains_live_record() -> Void {
  let rt = Runtime::new()?;
  rt.block_on(async {
    for comp_type in [CompactionType::Lookup, CompactionType::Scan] {
      let dir = tempdir()?;
      let device = Arc::new(SegmentedDevice::single_file(dir.path().join("retain.db"))?);
      let config = StoreConfig::new(1024, 64 * 1024, 16, 0.5)?;
      let store = Arc::new(WedbStore::open(config, device)?);
      let session = store.new_session()?;

      // k0 写入后墓碑删除（可安全退役的死记录），k1 为仍存活的最新版本
      session.upsert(b"retain:k0", b"dead_value").await?;
      store.shift_read_only_address(store.tail_address());
      session.delete(b"retain:k0").await?;
      let until = store.tail_address();
      session.upsert(b"retain:k1", b"live_value").await?;

      store.flush_all().await?;
      let compact_until = store.tail_address();
      store.shift_read_only_address(compact_until);

      // 零重试预算：存活记录无法 CAS 迁移，必须触发保守保留（截断点回退）
      let compactor = LogCompactor::with_cas_retries(Arc::clone(&store), 0);
      let stats = compactor.compact(compact_until, comp_type).await?;

      assert_eq!(stats.scanned_records, 3, "墓碑与两条记录全部参与扫描");
      assert_eq!(stats.live_copied, 0, "零预算下存活记录不得迁移");
      assert_eq!(stats.superseded, 1, "k0 旧版本被墓碑取代计为弃迁");
      assert_eq!(stats.dead_dropped, 1, "墓碑计为判死丢弃");
      assert_eq!(stats.retained, 1, "零预算下存活记录保守保留");
      // 截断点回退至存活记录 k1 的起始边界，墓碑区段照常退役
      assert_eq!(stats.new_begin_address, until);
      assert_eq!(store.begin_address(), until);

      // 存活记录必须原位完整保留可读（绝不误删），死记录照常读空
      assert_eq!(
        session.read(b"retain:k1").await?.as_deref(),
        Some(b"live_value".as_slice()),
        "重试耗尽的存活记录绝不能随截断丢失: {comp_type:?}"
      );
      assert!(session.read(b"retain:k0").await?.is_none());
    }
    aok::Result::<()>::Ok(())
  })?;

  OK
}
