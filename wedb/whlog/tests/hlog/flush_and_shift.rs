//! 在 garnet 中的相对路径: libs/storage/Tsavorite/cs/test/test.hlog/LogTests.cs（Flush/ShiftTail/Truncate）
use std::sync::Arc;

use aok::{OK, Void};
use compio::runtime::Runtime;
use log::info;
use tempfile::tempdir;
use wdev::SegmentedDevice;
use wepoch::LightEpoch;
use whlog::{
  AddressSnapshot, DEFAULT_INITIAL_ADDRESS, Error, HybridLog, HybridLogConfig, SECTOR_ALIGNMENT,
};

// 原测试 6/6b（PendingFlushList 合并内核直测）随导出面收敛移入
// whlog/src/flush.rs 的 #[cfg(test)] mod tests，就近守护同一定判体。

/// 测试 7: flush_addr_range 批量合并落盘与冷数据回读
#[test]
fn test_flush_addr_range_coalesced_direct_io() -> Void {
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
    let (addr0, _) = hlog.append(b"k0", b"v0_page0", 0, false)?;
    assert_eq!(hlog.config.page_id(addr0), 0);

    // 填充至第 1 页
    let pad_val1 = vec![b'A'; 4000];
    hlog.append(b"fill1", &pad_val1, 0, false)?;
    let (addr1, _) = hlog.append(b"k1", b"v1_page1", 0, false)?;
    assert_eq!(hlog.config.page_id(addr1), 1);

    // 填充至第 2 页
    let pad_val2 = vec![b'B'; 4000];
    hlog.append(b"fill2", &pad_val2, 0, false)?;
    let (addr2, _) = hlog.append(b"k2", b"v2_page2", 0, false)?;
    assert_eq!(hlog.config.page_id(addr2), 2);

    // 聚合落盘第 0~2 页对应的连续地址区间 (单次 Direct I/O 写入)

    hlog
      .flush_addr_range(
        hlog.config.page_start_address(0),
        hlog.config.page_start_address(3),
      )
      .await?;
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

/// 测试 14: ReadOnly 推进至 Tail 与 flush_all
#[test]
fn test_freeze_read_only_to_tail_and_flush_all() -> Void {
  let rt = Runtime::new()?;
  rt.block_on(async {
    let dir = tempdir()?;
    let db_path = dir.path().join("hlog_flush_all.db");
    let device = Arc::new(SegmentedDevice::single_file(&db_path)?);
    let epoch = Arc::new(LightEpoch::new(16));

    let config = HybridLogConfig::new(SECTOR_ALIGNMENT, 16, 0.5)?;
    let hlog = HybridLog::new(config, device, epoch)?;

    let (addr, _) = hlog.append(b"freeze_k", b"freeze_v", 0, false)?;
    let tail = hlog.tail_address();

    // 推进 ReadOnly 至 Tail（生产冻结通道：shift_read_only_address 显式推进）
    let frozen_tail = hlog.tail_address();
    hlog.shift_read_only_address(frozen_tail);
    assert_eq!(frozen_tail, tail);
    assert!(AddressSnapshot::region_read_only(
      addr,
      hlog.head_address(),
      hlog.read_only_address()
    ));
    assert!(!hlog.is_mutable(addr));

    // 刷写所有脏页
    let flushed = hlog.flush_all().await?;
    assert!(flushed >= tail);
    assert_eq!(hlog.flushed_until_address(), flushed);

    info!("ReadOnly 推进至 Tail 与 flush_all 测试通过");
    aok::Result::<()>::Ok(())
  })?;

  OK
}

/// 测试 16: ReadOnly 推进至 Tail 之后调用 shift_begin_address，单调状态机不变式严格保持
#[test]
fn test_shift_begin_after_read_only_tail_freeze_preserves_invariants() -> Void {
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
      let (addr, _) = hlog.append(&key, &[b'v'; 64], 0, false)?;
      addrs.push(addr);
    }
    hlog.flush_all().await?;
    hlog.sync().await?;

    // 先将只读边界迅速推至尾部并排空
    hlog.shift_read_only_address(hlog.tail_address());
    epoch.bump_current_epoch();
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
      hlog.addresses.snapshot().validate(),
      "地址状态机单调不变式校验失败: {:?}",
      hlog.addresses.snapshot()
    );

    info!("shift_begin_after_read_only_tail_freeze 不变式保持测试通过");
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

    let (addr0, _) = hlog.append(b"k0", b"v0", 0, false)?;
    let pad = vec![b'A'; 4000];
    hlog.append(b"fill0", &pad, 0, false)?;
    let (addr1, _) = hlog.append(b"k1", b"v1", 0, false)?;
    hlog.append(b"fill1", &pad, 0, false)?;
    hlog.append(b"k2", b"v2", 0, false)?;

    let target_ro = hlog.config.page_start_address(1);

    // wait=false：只推进 read_only_address，不强制落盘
    hlog
      .shift_read_only_address_with_wait(target_ro, false)
      .await?;
    assert!(hlog.read_only_address() >= target_ro);
    assert!(AddressSnapshot::region_read_only(
      addr0,
      hlog.head_address(),
      hlog.read_only_address()
    ));

    // wait=true：推进并确保 flushed_until 达到目标
    let target_ro2 = hlog.config.page_start_address(2);
    hlog
      .shift_read_only_address_with_wait(target_ro2, true)
      .await?;
    assert!(hlog.read_only_address() >= target_ro2);
    assert!(hlog.flushed_until_address() >= target_ro2);
    assert!(AddressSnapshot::region_read_only(
      addr1,
      hlog.head_address(),
      hlog.read_only_address()
    ));
    assert!(hlog.addresses.snapshot().validate());

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

    hlog.append(b"k0", b"v0", 0, false)?;
    let tail = hlog.tail_address();

    // 超过 tail 的截断必须被拦截并返回 AddressOutOfRange
    let res = hlog.shift_begin_address(tail + 1000).await;
    match res {
      Err(Error::AddressOutOfRange { addr, .. }) => {
        assert_eq!(addr, tail + 1000);
      }
      other => panic!("预期返回 AddressOutOfRange，实际: {other:?}"),
    }

    info!("shift_begin_address 越界拦截测试通过");
    aok::Result::<()>::Ok(())
  })?;

  OK
}
