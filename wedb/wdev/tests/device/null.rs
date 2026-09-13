//! 空设备语义：假读写、零填充、单调截断与无界假段。
//!
//! 对标 C# 测试文件：
//! `garnet/libs/storage/Tsavorite/cs/src/core/Device/NullDevice.cs`
//! （实现语义：ReadAsync/WriteAsync 即时假成功回调；Garnet
//! `UseAofNullDevice` 与 `SubscribeBroker` 的内存日志场景）。

use aok::{OK, Void};
use compio::runtime::Runtime;
use log::info;
use wbase::AlignedBuf;
use wdev::{Device, Error, NullDevice};

/// 对标 C# `NullDevice` 语义：写入即时假成功返回完整长度，读取即时假成功且零填充；
/// 刷盘/删除/重置均为安全无操作；截断单调推进 start_segment；
/// get_file_size 报告单一无界假段；扇区尺寸校验拒绝非法值。
#[test]
fn null_device_fakes_io_without_persistence() -> Void {
  let rt = Runtime::new()?;
  rt.block_on(async {
    let device = NullDevice::new()?;
    assert_eq!(device.sector_size(), 4096);
    assert_eq!(device.segment_size(), None);
    assert_eq!(device.start_segment(), 0, "初始 start_segment 应为 0");
    assert!(!device.direct_io(), "空设备无物理 I/O，Direct 恒为 false");

    // 写入即时假成功，返回完整逻辑长度（含非对齐长度——无物理写入无需对齐约束）
    let pattern = vec![0x7Fu8; 4096];
    let (res, _) = device
      .write_aligned(0, AlignedBuf::from_slice(&pattern, 4096)?)
      .await;
    assert_eq!(res?, 4096);

    // 读取即时假成功且零填充：池化缓冲脏数据绝不冒充有效数据
    let mut rbuf = AlignedBuf::from_slice(&pattern, 4096)?;
    rbuf.as_mut_slice().fill(0xEE);
    let (res, rbuf) = device.read_aligned(4096, rbuf).await;
    assert_eq!(res?, 4096);
    assert!(
      rbuf.as_slice().iter().all(|&b| b == 0),
      "空设备读取必须零填充，严禁脏数据泄漏"
    );

    // 便捷读取路由（direct_io 为 false 走 read_raw 精确直读）同样零填充
    let range = device.read_range(123, 456).await?;
    assert_eq!(range.len(), 456);
    assert!(range.as_slice().iter().all(|&b| b == 0));

    // 刷盘/数据刷盘/删除/重置均为安全无操作
    device.sync().await?;
    device.sync_data().await?;
    device.remove_segment(0).await?;
    device.reset();
    assert_eq!(device.end_segment(), None, "无界假设备不跟踪 end_segment");

    // 截断单调推进 start_segment，回退为无操作（对标 C# MonotonicUpdate）
    device.truncate_until_segment(3).await?;
    device.truncate_until_segment(2).await?;
    assert_eq!(device.start_segment(), 3, "截断单调推进，回退为无操作");
    assert_eq!(device.get_file_size(0)?, u64::MAX, "单一无界假段");

    // 扇区尺寸校验与 SegmentedDevice 一致：非 2 的幂与小于 512 拒绝
    assert!(matches!(
      NullDevice::with_sector_size(3000),
      Err(Error::InvalidSectorSize { .. })
    ));
    assert!(matches!(
      NullDevice::with_sector_size(256),
      Err(Error::InvalidSectorSize { .. })
    ));
    let small = NullDevice::with_sector_size(512)?;
    assert_eq!(small.sector_size(), 512);

    info!("空设备假 I/O、零填充与单调截断校验通过 (NullDevice)");
    aok::Result::<()>::Ok(())
  })?;

  OK
}
