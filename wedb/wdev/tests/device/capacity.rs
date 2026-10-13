//! 有界容量与系统能力语义：构造期容量校验、写满自动逐出最老段、段尺寸上限与硬件探测。
//!
//! 对标 C# 测试文件：
//! `garnet/libs/storage/Tsavorite/cs/test/test.hlog/DeviceTests.cs`
//! 与实现 `garnet/libs/storage/Tsavorite/cs/src/core/Device/StorageDeviceBase.cs`
//! （`HandleCapacity`：写新段时按容量自动截断最老段；容量口径校验
//! "Capacity must be a multiple of segment size"）；
//! 硬件探测用例对标 C# `IDevice` 实现的系统内存/CPU 探测，Rust 以
//! `detect_system_memory` / `detect_cpu_cores` 暴露。
//!
//! 在 garnet 中的相对路径: libs/storage/Tsavorite/cs/test/test.hlog/DeviceTests.cs（容量上限）

use std::fs::read_dir;

use aok::{OK, Void};
use compio::runtime::Runtime;
use log::info;
use tempfile::tempdir;
use wbase::{align::DEFAULT_SECTOR_SIZE, pool::AlignedBuf};
use wdev::{
  Device, DeviceParams, Error, MAX_SEGMENT_SIZE, SegmentedDevice, detect_cpu_cores,
  detect_system_memory,
};

/// 容量校验收归构造期（对标 C# 设备构造形参 capacity 的口径校验）：非法容量在
/// 构造即拒且不留任何文件产物，合法容量注入后透过 `Device::capacity` 立即可见。
#[test]
fn construction_rejects_non_multiple_of_segment_size() -> Void {
  let dir = tempdir()?;
  let seg_size: u64 = 64 * 1024;

  // 0 与非整数倍容量都必须在构造期拒绝
  for bad in [0u64, seg_size * 2 + 123] {
    let err = SegmentedDevice::with_params(
      dir.path().join(format!("cap_invalid_{bad}.log")),
      seg_size,
      DEFAULT_SECTOR_SIZE,
      DeviceParams {
        capacity: Some(bad),
        ..DeviceParams::default()
      },
    )
    .err()
    .expect("非法容量必须构造失败");
    assert!(
      matches!(err, Error::InvalidCapacity { capacity } if capacity == bad),
      "非法容量 {bad} 实际报 {err:?}"
    );
  }
  // 构造失败不得留下任何段文件/目录产物：容量口径校验前置于目录创建，
  // 杜绝"设备已建、目录已存之后才拒"的事后声明形态
  assert_eq!(read_dir(dir.path())?.count(), 0, "非法容量构造必须零产物");

  // 合法容量经构造注入即生效
  let device = SegmentedDevice::with_params(
    dir.path().join("cap_valid.log"),
    seg_size,
    DEFAULT_SECTOR_SIZE,
    DeviceParams {
      capacity: Some(seg_size * 2),
      ..DeviceParams::default()
    },
  )?;
  assert_eq!(device.capacity(), Some(seg_size * 2));
  assert_eq!(Device::capacity(&device), Some(seg_size * 2));

  info!("容量上限整数倍校验通过 (构造期 capacity 校验)");
  OK
}

/// 对标 C# `StorageDeviceBase.HandleCapacity`：有界容量设备写入新段时自动
/// 截断最老段腾出空间；end_segment 单调推进；被逐出段物理删除且数据不可访问。
#[test]
fn handle_capacity_evicts_oldest_segments_when_bounded() -> Void {
  let rt = Runtime::new()?;
  rt.block_on(async {
    let dir = tempdir()?;
    let seg_size: u64 = 64 * 1024;
    // 构造期定容 2 段：写入段 0..4 时，写段 3 起自动逐出最老段
    let device = SegmentedDevice::with_params(
      dir.path().join("cap_evict.log"),
      seg_size,
      DEFAULT_SECTOR_SIZE,
      DeviceParams {
        capacity: Some(seg_size * 2),
        ..DeviceParams::default()
      },
    )?;
    assert_eq!(
      device.end_segment(),
      None,
      "初始 end_segment 应为 None (C# -1)"
    );

    for seg_id in 0..4u32 {
      let buf = AlignedBuf::from_slice(&[(seg_id * 17 + 1) as u8; 4096], 4096)?;
      let (res, _) = device.write_aligned((seg_id as u64) * seg_size, buf).await;
      assert_eq!(res?, 4096);
    }

    // 段 0 被自动逐出：start_segment 推进至 1，物理文件删除
    assert_eq!(
      device.start_segment(),
      1,
      "段 0 被逐出后 start_segment 应为 1"
    );
    assert_eq!(device.end_segment(), Some(3));
    assert!(
      !device.segment_path(0).exists(),
      "超容量的最老段必须被 HandleCapacity 自动物理删除"
    );
    assert_eq!(device.get_file_size(0)?, 0);

    // 段 1..3 必须完好且可回读
    for seg_id in 1..4u32 {
      assert!(device.segment_path(seg_id).exists(), "段 {seg_id} 应保留");
      let expected = (seg_id * 17 + 1) as u8;
      let check = AlignedBuf::new(4096, 4096)?;
      let (res, check) = device.read_aligned((seg_id as u64) * seg_size, check).await;
      assert_eq!(res?, 4096);
      assert!(check.as_slice().iter().all(|&b| b == expected));
    }

    info!("有界容量自动逐出最老段校验通过 (HandleCapacity)");
    aok::Result::<()>::Ok(())
  })?;

  OK
}

/// 对标 C# 段尺寸上界防御：超过 `MAX_SEGMENT_SIZE` 的段尺寸必须拒绝，
/// 上界本身必须被接受。
#[test]
fn segment_size_above_max_is_rejected() -> Void {
  let rt = Runtime::new()?;
  rt.block_on(async {
    let dir = tempdir()?;

    // 超出上界的段尺寸一律拒绝
    assert!(matches!(
      SegmentedDevice::new(dir.path().join("seg_max1.log"), 1u64 << 63, 4096),
      Err(Error::InvalidSegmentSize(_))
    ));
    assert!(matches!(
      SegmentedDevice::new(dir.path().join("seg_max2.log"), MAX_SEGMENT_SIZE + 1, 4096),
      Err(Error::InvalidSegmentSize(_))
    ));

    // 上界本身合法（构造期不发生物理分配）
    let device = SegmentedDevice::new(dir.path().join("seg_max_ok.log"), MAX_SEGMENT_SIZE, 4096)?;
    assert_eq!(device.segment_size(), MAX_SEGMENT_SIZE);

    info!("段尺寸上界防御校验通过 (MAX_SEGMENT_SIZE)");
    aok::Result::<()>::Ok(())
  })?;

  OK
}

/// Rust 扩展语义：系统物理内存与 CPU 核心数探测必须返回可用值
/// （对标 C# 设备层环境探测，Rust 以独立函数暴露供上层容量自 tuning）。
#[test]
fn system_probes_return_usable_values() -> Void {
  let sys_mem = detect_system_memory();
  assert!(sys_mem > 0, "系统内存探测值必须大于 0");

  let cores = detect_cpu_cores();
  assert!(cores >= 1, "可用 CPU 核心数必须至少为 1");

  info!("系统内存/核心数探测校验通过");
  OK
}
