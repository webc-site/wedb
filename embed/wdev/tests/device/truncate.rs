//! 截断与段清理语义：按段截断、按地址截断、单段删除、重置与文件尺寸查询。
//!
//! 对标 C# 测试文件：
//! `/Users/z/git/db/garnet/libs/storage/Tsavorite/cs/test/test.hlog/DeviceTests.cs`
//! （方法 `Native_RemoveSegment_RemovesPersistedData`、
//! `Native_Reset_ClosesSegments_DeviceRemainsUsable`、
//! `Native_GetFileSize_ReflectsWrites`）；
//! 按段/按地址截断对标 C# 实现 `src/core/Device/StorageDeviceBase.cs` 的
//! `TruncateUntilSegment` / `TruncateUntilAddress`（含 `Utility.MonotonicUpdate`
//! 单调语义与物理删除保证）。

use aok::{OK, Void};
use compio::runtime::Runtime;
use log::info;
use tempfile::tempdir;
use wdev::{Device, Error, SegmentedDevice};
use wutil::AlignedBuf;

/// 对标 C# `StorageDeviceBase.TruncateUntilSegment`：截断后小于目标段的文件必须
/// 从文件系统物理删除、get_file_size 返回 0、保留段数据完好；
/// 单调回退截断为安全无操作（对标 libs/client/Utility.cs:MonotonicUpdate）；
/// 单文件无界模式截断为无操作且不删除主文件。
#[test]
fn truncate_until_segment_removes_prior_segments() -> Void {
  let rt = Runtime::new()?;
  rt.block_on(async {
    let dir = tempdir()?;
    let seg_size: u64 = 64 * 1024;
    let device = SegmentedDevice::segmented(dir.path().join("trunc_seg.log"), seg_size)?;

    // 向段 0..5 各写入一个扇区，内容按段号区分
    for seg_id in 0..5u32 {
      let buf = AlignedBuf::from_slice(&[(seg_id * 17 + 1) as u8; 4096], 4096)?;
      let (res, _) = device.write_aligned((seg_id as u64) * seg_size, buf).await;
      assert_eq!(res?, 4096);
      assert!(device.segment_path(seg_id).exists());
    }

    // 截断至段 2：段 0、1 物理删除，段 2..4 完好
    device.truncate_until_segment(2).await?;
    assert_eq!(device.start_segment(), 2);

    for seg_id in 0..2u32 {
      assert!(
        !device.segment_path(seg_id).exists(),
        "段 {seg_id} 必须已物理删除"
      );
      assert_eq!(
        device.get_file_size(seg_id)?,
        0,
        "已删除段 get_file_size 必须为 0"
      );
    }
    for seg_id in 2..5u32 {
      assert!(device.segment_path(seg_id).exists(), "段 {seg_id} 应保留");
      let expected = (seg_id * 17 + 1) as u8;
      let check = AlignedBuf::new(4096, 4096)?;
      let (res, check) = device.read_aligned((seg_id as u64) * seg_size, check).await;
      assert_eq!(res?, 4096);
      assert!(check.as_slice().iter().all(|&b| b == expected));
    }

    // 单调回退：截断到更小的段号是无操作，start_segment 不回退
    device.truncate_until_segment(1).await?;
    assert_eq!(
      device.start_segment(),
      2,
      "回退截断不得推进或回退 start_segment"
    );

    // 单文件无界模式：截断是安全无操作，主文件保留
    let single_path = dir.path().join("single_trunc.log");
    let single = SegmentedDevice::single_file(&single_path)?;
    let buf = AlignedBuf::from_slice(&[0x11u8; 4096], 4096)?;
    let (res, _) = single.write_aligned(0, buf).await;
    assert_eq!(res?, 4096);
    single.truncate_until_segment(10).await?;
    assert!(single_path.exists(), "单文件模式截断不应删除主文件");

    info!("按段截断物理删除与单调语义校验通过 (TruncateUntilSegment)");
    aok::Result::<()>::Ok(())
  })?;

  OK
}

/// 对标 C# `StorageDeviceBase.TruncateUntilAddress`：按逻辑地址截断删除该地址
/// 所在段之前的所有段文件，保留目标段及之后段的数据。
#[test]
fn truncate_until_address_deletes_all_prior_segments() -> Void {
  let rt = Runtime::new()?;
  rt.block_on(async {
    let dir = tempdir()?;
    let seg_size: u64 = 64 * 1024;
    let device = SegmentedDevice::segmented(dir.path().join("trunc_addr.log"), seg_size)?;

    // 向段 0..5 各写入一个扇区
    for seg_id in 0..5u32 {
      let buf = AlignedBuf::from_slice(&[(seg_id * 17 + 1) as u8; 4096], 4096)?;
      let (res, _) = device.write_aligned((seg_id as u64) * seg_size, buf).await;
      assert_eq!(res?, 4096);
    }

    // 地址 4 * 64KB + 1000 落在段 4：段 2、3 必须删除，段 4 保留
    device.truncate_until_address(4 * seg_size + 1000).await?;
    assert_eq!(device.start_segment(), 4);

    for seg_id in 2..4u32 {
      assert!(
        !device.segment_path(seg_id).exists(),
        "段 {seg_id} 必须已物理删除"
      );
      assert_eq!(device.get_file_size(seg_id)?, 0);
    }
    assert!(device.segment_path(4).exists(), "段 4 文件应保留");
    assert_eq!(device.get_file_size(4)?, 4096);

    let check = AlignedBuf::new(4096, 4096)?;
    let (res, check) = device.read_aligned(4 * seg_size, check).await;
    assert_eq!(res?, 4096);
    assert!(check.as_slice().iter().all(|&b| b == (4 * 17 + 1) as u8));

    info!("按地址截断删除历史段校验通过 (TruncateUntilAddress)");
    aok::Result::<()>::Ok(())
  })?;

  OK
}

/// 对标 libs/storage/Tsavorite/cs/test/test.hlog/DeviceTests.cs:Native_RemoveSegment_RemovesPersistedData：删除单段后磁盘文件移除，
/// 后续查询不崩溃且报告空尺寸。
#[test]
fn remove_segment_removes_persisted_data() -> Void {
  let rt = Runtime::new()?;
  rt.block_on(async {
    let dir = tempdir()?;
    let seg_size: u64 = 64 * 1024;
    let device = SegmentedDevice::segmented(dir.path().join("remove_seg.log"), seg_size)?;

    // 写入段 1，确认文件已生成
    let buf = AlignedBuf::from_slice(&[0x7Cu8; 4096], 4096)?;
    let (res, _) = device.write_aligned(seg_size, buf).await;
    assert_eq!(res?, 4096);
    assert!(device.get_file_size(1)? >= 4096, "段 1 写入后应存在");

    // 删除段 1：查询不崩溃且尺寸为 0
    device.remove_segment(1).await?;
    assert_eq!(device.get_file_size(1)?, 0, "段 1 删除后应报告空尺寸");
    assert!(!device.segment_path(1).exists(), "段 1 文件必须被物理删除");

    info!("单段删除与持久数据清理校验通过 (Native_RemoveSegment_RemovesPersistedData)");
    aok::Result::<()>::Ok(())
  })?;

  OK
}

/// 对标 libs/storage/Tsavorite/cs/test/test.hlog/DeviceTests.cs:Native_GetFileSize_ReflectsWrites：写入前段尺寸为 0，
/// 写入后 get_file_size 反映实际写入量。
#[test]
fn get_file_size_reflects_writes() -> Void {
  let rt = Runtime::new()?;
  rt.block_on(async {
    let dir = tempdir()?;
    let device = SegmentedDevice::segmented(dir.path().join("file_size.log"), 1 << 20)?;

    assert_eq!(device.get_file_size(0)?, 0, "未写入时段 0 尺寸应为 0");

    let size = 16 * 1024;
    let buf = AlignedBuf::from_slice(&[0xCDu8; 16 * 1024], 4096)?;
    let (res, _) = device.write_aligned(0, buf).await;
    assert_eq!(res?, size);
    assert!(
      device.get_file_size(0)? >= size as u64,
      "get_file_size 必须反映已写入的段"
    );

    info!("get_file_size 反映写入量校验通过 (Native_GetFileSize_ReflectsWrites)");
    aok::Result::<()>::Ok(())
  })?;

  OK
}

/// 对标 libs/storage/Tsavorite/cs/test/test.hlog/DeviceTests.cs:Native_Reset_ClosesSegments_DeviceRemainsUsable：
/// Reset 关闭全部段句柄后，设备必须按需惰性重开并保证数据完好。
#[test]
fn reset_closes_segments_and_device_remains_usable() -> Void {
  let rt = Runtime::new()?;
  rt.block_on(async {
    let dir = tempdir()?;
    let device = SegmentedDevice::segmented(dir.path().join("reset.log"), 1 << 20)?;

    let buf = AlignedBuf::from_slice(&[0xEEu8; 4096], 4096)?;
    let (res, _) = device.write_aligned(0, buf).await;
    assert_eq!(res?, 4096);

    // 重置关闭句柄，缓存清空
    device.reset();
    assert!(device.is_cached_empty(), "Reset 后句柄缓存必须已清空");

    // 重置后回读：句柄透明重开且数据精确
    let check = AlignedBuf::new(4096, 4096)?;
    let (res, check) = device.read_aligned(0, check).await;
    assert_eq!(res?, 4096);
    assert!(check.as_slice().iter().all(|&b| b == 0xEE));

    info!("Reset 关闭句柄后按需重开校验通过 (Native_Reset_ClosesSegments_DeviceRemainsUsable)");
    aok::Result::<()>::Ok(())
  })?;

  OK
}

/// Rust 扩展对抗语义（对标 C# 截断 + begin_segment_ 防御的组合深度边界）：
/// 快速连续多段截断（0 -> 3 -> 7 -> 按地址至 9），已截断段读写必须返回
/// SegmentNotFound 且严禁幽灵重建；Reset 后保留段按需重开且数据完全一致。
#[test]
fn successive_truncations_defend_against_ghost_segments() -> Void {
  let rt = Runtime::new()?;
  rt.block_on(async {
    let dir = tempdir()?;
    let seg_size: u64 = 64 * 1024;
    let device = SegmentedDevice::segmented(dir.path().join("rapid_trunc.log"), seg_size)?;

    // 初始化写入段 0..=10，各段独立模式
    for seg_id in 0..=10u32 {
      let pattern = vec![((seg_id * 23 + 7) & 0xFF) as u8; 4096];
      let buf = AlignedBuf::from_slice(&pattern, 4096)?;
      let (res, _) = device.write_aligned((seg_id as u64) * seg_size, buf).await;
      assert_eq!(res?, 4096);
      assert_eq!(device.get_file_size(seg_id)?, 4096);
    }

    // 第 1 阶段：截断至段 3（删除 0..3），已删段读写必须被拦截且无幽灵文件
    device.truncate_until_segment(3).await?;
    assert_eq!(device.start_segment(), 3);
    for seg_id in 0..3u32 {
      assert!(
        !device.segment_path(seg_id).exists(),
        "段 {seg_id} 必须已删除"
      );
      assert_eq!(device.get_file_size(seg_id)?, 0);

      let check = AlignedBuf::new(4096, 4096)?;
      let (res, _) = device.read_aligned((seg_id as u64) * seg_size, check).await;
      assert!(
        matches!(res, Err(Error::SegmentNotFound(id)) if id == seg_id),
        "对已截断段读取必须返回 SegmentNotFound，实际为 {res:?}"
      );

      let wbuf = AlignedBuf::from_slice(&[0x55u8; 4096], 4096)?;
      let (res, _) = device.write_aligned((seg_id as u64) * seg_size, wbuf).await;
      assert!(
        matches!(res, Err(Error::SegmentNotFound(id)) if id == seg_id),
        "对已截断段写入必须返回 SegmentNotFound，实际为 {res:?}"
      );
      assert!(
        !device.segment_path(seg_id).exists(),
        "严禁幽灵文件重现于磁盘"
      );
    }

    // 第 2 阶段：截断至段 7（删除 3..7）
    device.truncate_until_segment(7).await?;
    assert_eq!(device.start_segment(), 7);
    for seg_id in 3..7u32 {
      assert!(!device.segment_path(seg_id).exists());
      let check = AlignedBuf::new(4096, 4096)?;
      let (res, _) = device.read_aligned((seg_id as u64) * seg_size, check).await;
      assert!(matches!(res, Err(Error::SegmentNotFound(id)) if id == seg_id));
    }

    // 第 3 阶段：按地址截断至段 9 之前（删除 7..9）
    device.truncate_until_address(9 * seg_size + 2048).await?;
    assert_eq!(device.start_segment(), 9);
    for seg_id in 7..9u32 {
      assert!(!device.segment_path(seg_id).exists());
      assert_eq!(device.get_file_size(seg_id)?, 0);
    }

    // 段 9、10 依然有效且数据完好
    for seg_id in 9..=10u32 {
      assert!(device.segment_path(seg_id).exists());
      let expected = vec![((seg_id * 23 + 7) & 0xFF) as u8; 4096];
      let check = AlignedBuf::new(4096, 4096)?;
      let (res, check) = device.read_aligned((seg_id as u64) * seg_size, check).await;
      assert_eq!(res?, 4096);
      assert_eq!(check.as_slice(), &expected[..]);
    }

    // Reset 后句柄透明重开，数据保持一致
    device.reset();
    assert!(device.is_cached_empty());
    for seg_id in 9..=10u32 {
      let expected = vec![((seg_id * 23 + 7) & 0xFF) as u8; 4096];
      let check = AlignedBuf::new(4096, 4096)?;
      let (res, check) = device.read_aligned((seg_id as u64) * seg_size, check).await;
      assert_eq!(res?, 4096);
      assert_eq!(check.as_slice(), &expected[..]);
    }
    assert_eq!(device.cached_handle_count(), 2, "按需重开段 9 与 10");

    info!("快速连续截断与幽灵段防御校验通过");
    aok::Result::<()>::Ok(())
  })?;

  OK
}
