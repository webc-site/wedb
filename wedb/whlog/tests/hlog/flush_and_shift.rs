use std::sync::Arc;

use aok::{OK, Void};
use compio::runtime::Runtime;
use log::info;
use tempfile::tempdir;
use wdev::SegmentedDevice;
use wepoch::LightEpoch;
use whlog::{
  DEFAULT_INITIAL_ADDRESS, HybridLog, HybridLogConfig, PageFlushRange, PendingFlushList,
  SECTOR_ALIGNMENT,
};

/// 测试 6: PendingFlushList 贪心双向区间合并（严格PendingFlushList.cs）
#[test]
fn test_pending_flush_list_coalesce() -> Void {
  let list = PendingFlushList::new();
  assert!(list.is_empty());

  // 插入两个不相邻区间：[100, 200) 与 [300, 400)
  list.add(PageFlushRange::new(100, 200));
  list.add(PageFlushRange::new(300, 400));
  assert_eq!(list.len(), 2);

  // 插入连接桥梁 [200, 300)，执行 coalesce 应当同时将 [100, 200) 与 [300, 400) 双向贪心合并为 [100, 400)
  let merged = list.coalesce(PageFlushRange::new(200, 300));
  assert_eq!(merged, PageFlushRange::new(100, 400));
  assert!(list.is_empty(), "合并后队列中原本的相邻区间应已被取出");

  OK
}

/// 测试 7: flush_pages_range 批量合并落盘与冷数据回读
#[test]
fn test_flush_pages_range_coalesced_direct_io() -> Void {
  let rt = Runtime::new()?;
  rt.block_on(async {
    let dir = tempdir()?;
    let db_path = dir.path().join("hlog_flush_range.db");
    let device = Arc::new(SegmentedDevice::single_file(&db_path)?);
    let epoch = Arc::new(LightEpoch::new(16));

    let page_size = 4096usize;
    let config = HybridLogConfig {
      page_size,
      num_pages: 8,
      mutable_fraction: 0.5,
      ro_lag_num: whlog::ro_lag_num_from_fraction(0.5),
      initial_address: DEFAULT_INITIAL_ADDRESS,
    };

    let hlog = HybridLog::new(config, device, epoch)?;

    // 分别在第 0 页、第 1 页、第 2 页各写入一条记录
    let addr0 = hlog.append(b"k0", b"v0_page0", 0, false)?;
    assert_eq!(hlog.config.page_id(addr0), 0);

    // 填充至第 1 页
    let pad_val1 = vec![b'A'; 4000];
    let _ = hlog.append(b"fill1", &pad_val1, 0, false)?;
    let addr1 = hlog.append(b"k1", b"v1_page1", 0, false)?;
    assert_eq!(hlog.config.page_id(addr1), 1);

    // 填充至第 2 页
    let pad_val2 = vec![b'B'; 4000];
    let _ = hlog.append(b"fill2", &pad_val2, 0, false)?;
    let addr2 = hlog.append(b"k2", b"v2_page2", 0, false)?;
    assert_eq!(hlog.config.page_id(addr2), 2);

    // 聚合落盘 [0..=2] 连续三页 (单次 Direct I/O 写入)

    hlog.flush_pages_range(0, 2).await?;
    hlog.sync().await?;

    // 推进 HeadAddress 将 0..=2 页全部驱逐出内存
    let new_head = (page_size * 3) as u64;
    hlog.shift_read_only_address(new_head);
    hlog.shift_head_address(new_head);

    assert!(hlog.is_on_disk(addr0));
    assert!(hlog.is_on_disk(addr1));
    assert!(hlog.is_on_disk(addr2));

    // 回读验证三页冷数据全部正确
    let out0 = hlog.read_record(addr0).await?;
    assert_eq!(out0.key()?, b"k0");
    assert_eq!(out0.value()?, b"v0_page0");

    let out1 = hlog.read_record(addr1).await?;
    assert_eq!(out1.key()?, b"k1");
    assert_eq!(out1.value()?, b"v1_page1");

    let out2 = hlog.read_record(addr2).await?;
    assert_eq!(out2.key()?, b"k2");
    assert_eq!(out2.value()?, b"v2_page2");

    aok::Result::<()>::Ok(())
  })?;

  OK
}

/// 测试 14: shift_read_only_to_tail 与 flush_all
#[test]
fn test_shift_read_only_to_tail_and_flush_all() -> Void {
  let rt = Runtime::new()?;
  rt.block_on(async {
    let dir = tempdir()?;
    let db_path = dir.path().join("hlog_flush_all.db");
    let device = Arc::new(SegmentedDevice::single_file(&db_path)?);
    let epoch = Arc::new(LightEpoch::new(16));

    let config = HybridLogConfig::new(SECTOR_ALIGNMENT, 16, 0.5)?;
    let hlog = HybridLog::new(config, device, epoch)?;

    let addr = hlog.append(b"freeze_k", b"freeze_v", 0, false)?;
    let tail = hlog.tail_address();

    // 推进 ReadOnly 至 Tail
    let frozen_tail = hlog.shift_read_only_to_tail();
    assert_eq!(frozen_tail, tail);
    assert!(hlog.is_read_only(addr));
    assert!(!hlog.is_mutable(addr));

    // 刷写所有脏页
    let flushed = hlog.flush_all().await?;
    assert!(flushed >= tail);
    assert_eq!(hlog.flushed_until_address(), flushed);

    info!("shift_read_only_to_tail 与 flush_all 测试通过");
    aok::Result::<()>::Ok(())
  })?;

  OK
}

/// 测试 15: AddressSnapshot 与 PageFlushRange bitcode 序列化往返
#[test]
fn test_bitcode_roundtrip() -> Void {
  use whlog::AddressSnapshot;

  let snap = AddressSnapshot::new(64, 4096, 4096, 8192, 8192, 16384, 8192);
  let encoded = bitcode::encode(&snap);
  let decoded: AddressSnapshot = bitcode::decode(&encoded)?;
  assert_eq!(snap, decoded);

  let range = PageFlushRange::new(4096, 8192);
  let encoded_range = bitcode::encode(&range);
  let decoded_range: PageFlushRange = bitcode::decode(&encoded_range)?;
  assert_eq!(range, decoded_range);

  info!("bitcode 序列化往返测试通过");
  OK
}

/// 测试 16: shift_read_only_to_tail 之后调用 shift_begin_address，单调状态机不变式严格保持
#[test]
fn test_shift_begin_after_shift_read_only_to_tail_preserves_invariants() -> Void {
  let rt = Runtime::new()?;
  rt.block_on(async {
    let dir = tempdir()?;
    let db_path = dir.path().join("shift_begin_inv.db");
    let device = Arc::new(SegmentedDevice::single_file(&db_path)?);
    let epoch = Arc::new(LightEpoch::new(16));

    let config = HybridLogConfig::new(SECTOR_ALIGNMENT, 8, 0.5)?;
    let hlog = HybridLog::new(config, device, epoch.clone())?;

    // 写入多条记录
    let mut addrs = Vec::new();
    for i in 0..64 {
      let key = format!("k{i:03}").into_bytes();
      let addr = hlog.append(&key, &[b'v'; 64], 0, false)?;
      addrs.push(addr);
    }
    hlog.flush_all().await?;
    hlog.sync().await?;

    // 先将只读边界迅速推至尾部并排空
    hlog.shift_read_only_to_tail();
    epoch.bump_epoch();
    epoch.drain();
    assert!(hlog.safe_read_only_address() >= hlog.tail_address());

    // 随后推进 begin 地址
    let cut = addrs[16];
    hlog.shift_begin_address(cut).await?;

    // 验证状态机单调不变式依然完全成立，safe_head 必须赶上 cut
    assert_eq!(hlog.begin_address(), cut);
    assert!(
      hlog.safe_head_address() >= cut,
      "safe_head ({:#x}) 必须 >= cut ({cut:#x})",
      hlog.safe_head_address()
    );
    assert!(
      hlog.addresses.validate_invariants(),
      "地址状态机单调不变式校验失败: {:?}",
      hlog.addresses.snapshot()
    );

    info!("shift_begin_after_shift_read_only_to_tail 不变式保持测试通过");
    aok::Result::<()>::Ok(())
  })?;

  OK
}

/// 测试 17: shift_addresses_with_wait 推进与排空等待 (对标 C# AllocatorBase.ShiftAddressesWithWait)
#[test]
fn test_shift_addresses_with_wait() -> Void {
  let rt = Runtime::new()?;
  rt.block_on(async {
    let dir = tempdir()?;
    let db_path = dir.path().join("shift_addrs_wait.db");
    let device = Arc::new(SegmentedDevice::single_file(&db_path)?);
    let epoch = Arc::new(LightEpoch::new(16));
    let config = HybridLogConfig::new(SECTOR_ALIGNMENT, 8, 0.5)?;
    let hlog = HybridLog::new(config, device, epoch.clone())?;

    let _ = hlog.append(b"k0", b"v0", 0, false)?;
    let pad = vec![b'A'; 4000];
    let _ = hlog.append(b"fill0", &pad, 0, false)?;
    let _ = hlog.append(b"k1", b"v1", 0, false)?;
    let _ = hlog.append(b"fill1", &pad, 0, false)?;
    let _ = hlog.append(b"k2", b"v2", 0, false)?;

    let target_ro = hlog.config.page_start_address(2);
    let target_head = hlog.config.page_start_address(1);

    hlog
      .shift_addresses_with_wait(target_ro, target_head, true)
      .await?;

    assert!(hlog.flushed_until_address() >= target_ro);
    assert!(hlog.read_only_address() >= target_ro);
    assert!(hlog.head_address() >= target_head);
    assert!(
      hlog.safe_head_address() >= target_head,
      "safe_head 必须 >= target_head"
    );
    assert!(
      hlog.addresses.validate_invariants(),
      "不变式校验失败: {:?}",
      hlog.addresses.snapshot()
    );

    info!("shift_addresses_with_wait 测试通过");
    aok::Result::<()>::Ok(())
  })?;

  OK
}

/// 测试 18: shift_read_only_address_with_wait (wait=false 与 wait=true)
#[test]
fn test_shift_read_only_address_with_wait() -> Void {
  let rt = Runtime::new()?;
  rt.block_on(async {
    let dir = tempdir()?;
    let db_path = dir.path().join("shift_ro_wait.db");
    let device = Arc::new(SegmentedDevice::single_file(&db_path)?);
    let epoch = Arc::new(LightEpoch::new(16));
    let config = HybridLogConfig::new(SECTOR_ALIGNMENT, 8, 0.5)?;
    let hlog = HybridLog::new(config, device, epoch.clone())?;

    let addr0 = hlog.append(b"k0", b"v0", 0, false)?;
    let pad = vec![b'A'; 4000];
    let _ = hlog.append(b"fill0", &pad, 0, false)?;
    let addr1 = hlog.append(b"k1", b"v1", 0, false)?;
    let _ = hlog.append(b"fill1", &pad, 0, false)?;
    let _ = hlog.append(b"k2", b"v2", 0, false)?;

    let target_ro = hlog.config.page_start_address(1);

    // wait=false：只推进 read_only_address，不强制落盘
    hlog
      .shift_read_only_address_with_wait(target_ro, false)
      .await?;
    assert!(hlog.read_only_address() >= target_ro);
    assert!(hlog.is_read_only(addr0));

    // wait=true：推进并确保 flushed_until 达到目标
    let target_ro2 = hlog.config.page_start_address(2);
    hlog
      .shift_read_only_address_with_wait(target_ro2, true)
      .await?;
    assert!(hlog.read_only_address() >= target_ro2);
    assert!(hlog.flushed_until_address() >= target_ro2);
    assert!(hlog.is_read_only(addr1));
    assert!(hlog.addresses.validate_invariants());

    info!("shift_read_only_address_with_wait 测试通过");
    aok::Result::<()>::Ok(())
  })?;

  OK
}

/// 测试 19: shift_begin_address 越界截断被拦截
#[test]
fn test_shift_begin_address_out_of_range() -> Void {
  let rt = Runtime::new()?;
  rt.block_on(async {
    let dir = tempdir()?;
    let db_path = dir.path().join("shift_begin_oor.db");
    let device = Arc::new(SegmentedDevice::single_file(&db_path)?);
    let epoch = Arc::new(LightEpoch::new(16));
    let config = HybridLogConfig::new(SECTOR_ALIGNMENT, 8, 0.5)?;
    let hlog = HybridLog::new(config, device, epoch)?;

    let _ = hlog.append(b"k0", b"v0", 0, false)?;
    let tail = hlog.tail_address();

    // 超过 tail 的截断必须被拦截并返回 AddressOutOfRange
    let res = hlog.shift_begin_address(tail + 1000).await;
    match res {
      Err(whlog::Error::AddressOutOfRange { addr, .. }) => {
        assert_eq!(addr, tail + 1000);
      }
      other => panic!("预期返回 AddressOutOfRange，实际: {other:?}"),
    }

    info!("shift_begin_address 越界拦截测试通过");
    aok::Result::<()>::Ok(())
  })?;

  OK
}

/// 测试 20: 持纪元守卫下调用 shift_addresses_with_wait (waitForEviction=true)
#[test]
fn test_shift_addresses_with_wait_under_epoch_guard() -> Void {
  let rt = Runtime::new()?;
  rt.block_on(async {
    let dir = tempdir()?;
    let db_path = dir.path().join("shift_addrs_guard.db");
    let device = Arc::new(SegmentedDevice::single_file(&db_path)?);
    let epoch = Arc::new(LightEpoch::new(16));
    let config = HybridLogConfig::new(SECTOR_ALIGNMENT, 8, 0.5)?;
    let hlog = HybridLog::new(config, device, epoch.clone())?;

    let _ = hlog.append(b"k0", b"v0", 0, false)?;
    let pad = vec![b'A'; 4000];
    let _ = hlog.append(b"fill0", &pad, 0, false)?;
    let _ = hlog.append(b"k1", b"v1", 0, false)?;
    let _ = hlog.append(b"fill1", &pad, 0, false)?;
    let _ = hlog.append(b"fill2", &pad, 0, false)?;
    let _ = hlog.append(b"k2", b"v2", 0, false)?;

    let target_ro = hlog.config.page_start_address(2);
    let target_head = hlog.config.page_start_address(1);

    // TLS 保护区下调用：屏障自动解钉并恢复保护，绝不死锁
    {
      let _guard = epoch.protected_scope();
      hlog
        .shift_addresses_with_wait(target_ro, target_head, true)
        .await?;
    }

    assert!(hlog.flushed_until_address() >= target_ro);
    assert!(hlog.safe_head_address() >= target_head);
    assert!(hlog.addresses.validate_invariants());

    info!("持纪元守卫 shift_addresses_with_wait 测试通过");
    aok::Result::<()>::Ok(())
  })?;

  OK
}
