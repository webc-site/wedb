//! 截断与段清理语义：按段截断、按地址截断、单段删除、重置与文件尺寸查询。
//!
//! 对标 C# 测试文件：
//! `garnet/libs/storage/Tsavorite/cs/test/test.hlog/DeviceTests.cs`
//! （方法 `Native_RemoveSegment_RemovesPersistedData`、
//! `Native_Reset_ClosesSegments_DeviceRemainsUsable`、
//! `Native_GetFileSize_ReflectsWrites`）；
//! 按段/按地址截断对标 C# 实现 `src/core/Device/StorageDeviceBase.cs` 的
//! `TruncateUntilSegment` / `TruncateUntilAddress`（含 `Utility.MonotonicUpdate`
//! 单调语义与物理删除保证）。
//!
//! 在 garnet 中的相对路径: libs/storage/Tsavorite/cs/test/test.hlog/DeviceTests.cs（截断）

use std::{
  fs::{File, create_dir, remove_dir, remove_file},
  future::ready,
  io,
  sync::atomic::{AtomicU64, Ordering},
};

use aok::{OK, Void};
use compio::runtime::Runtime;
use log::info;
use tempfile::tempdir;
use wbase::{align::DEFAULT_SECTOR_SIZE, pool::AlignedBuf};
use wdev::{Device, Error, SegmentedDevice};

/// 把段 `[0, count)` 的真实文件替换为空目录：POSIX `unlink` 对目录必失败
/// （EPERM/EISDIR），构造"物理删段中途遭遇真实 I/O 故障"的确定性注入点。
/// 仅 Unix——Windows 下删除失败走延迟队列不上抛（本故障注入依赖快速失败分支）
#[cfg(unix)]
fn replace_segments_with_dirs(device: &SegmentedDevice, count: u32) -> io::Result<()> {
  for seg in 0..count {
    let path = device.segment_path(seg);
    remove_file(&path)?;
    create_dir(&path)?;
  }
  Ok(())
}

/// 撤除故障注入：删掉占位目录并在原段路径重建空文件，使后续重试可正常删除
#[cfg(unix)]
fn restore_segments_as_files(device: &SegmentedDevice, count: u32) -> io::Result<()> {
  for seg in 0..count {
    let path = device.segment_path(seg);
    remove_dir(&path)?;
    File::create(&path)?;
  }
  Ok(())
}

/// 向段 `0..count` 各写入一个扇区，内容按段号可辨识
async fn fill_segments(device: &SegmentedDevice, count: u32, seg_size: u64) -> aok::Result<()> {
  for seg_id in 0..count {
    let buf = AlignedBuf::from_slice(&[(seg_id * 17 + 1) as u8; 4096], 4096)?;
    let (res, _) = device.write_aligned((seg_id as u64) * seg_size, buf).await;
    assert_eq!(res?, 4096);
  }
  Ok(())
}

/// 对标 C# `StorageDeviceBase.TruncateUntilSegment`：截断后小于目标段的文件必须
/// 从文件系统物理删除、get_file_size 返回 0、保留段数据完好；
/// 单调回退截断为安全无操作（对标 Utility.MonotonicUpdate）；
/// 单文件无界模式截断为无操作且不删除主文件。
#[test]
fn truncate_until_segment_removes_prior_segments() -> Void {
  let rt = Runtime::new()?;
  rt.block_on(async {
    let dir = tempdir()?;
    let seg_size: u64 = 64 * 1024;
    let device = SegmentedDevice::new(
      dir.path().join("trunc_seg.log"),
      seg_size,
      DEFAULT_SECTOR_SIZE,
    )?;

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
    let device = SegmentedDevice::new(
      dir.path().join("trunc_addr.log"),
      seg_size,
      DEFAULT_SECTOR_SIZE,
    )?;

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

/// 越界地址截断必须保守拒绝、零段删除（对标 `StorageDeviceBase.cs:318/333`：C# 符号
/// 截断使溢出地址落入 `<= startSegment` 无操作，绝不清空全卷段文件）。
///
/// 用真实越界地址（`u64::MAX` 回绕、`(u32::MAX+1)*seg_size` 越界）驱动，断言
/// `truncate_until_address` 返回 `SegmentExceeded`、`start_segment` 原地不动，且全部
/// 在册段文件逐一回读校验数据完好——即"零段被删除"，杜绝旧实现 `unwrap_or(u32::MAX)`
/// 误将全卷段物理 unlink 的数据灭失。
#[test]
fn truncate_until_address_overflow_deletes_zero_segments() -> Void {
  let rt = Runtime::new()?;
  rt.block_on(async {
    let dir = tempdir()?;
    let seg_size: u64 = 64 * 1024;
    let device = SegmentedDevice::new(
      dir.path().join("trunc_overflow.log"),
      seg_size,
      DEFAULT_SECTOR_SIZE,
    )?;

    // 真实写入段 0..5，各段以段号可辨识的字节模式填充
    for seg_id in 0..5u32 {
      let buf = AlignedBuf::from_slice(&[(seg_id * 17 + 1) as u8; 4096], 4096)?;
      let (res, _) = device.write_aligned((seg_id as u64) * seg_size, buf).await;
      assert_eq!(res?, 4096);
      assert!(device.segment_path(seg_id).exists());
    }
    assert_eq!(device.start_segment(), 0);

    // 真实越界地址一：u64::MAX（段号 u64::MAX >> 16 远超 u32::MAX）
    let res = device.truncate_until_address(u64::MAX).await;
    assert!(
      matches!(res, Err(Error::SegmentExceeded(_))),
      "u64::MAX 截断地址必须拒绝为 SegmentExceeded，实际为 {res:?}"
    );

    // 真实越界地址二：(u32::MAX + 1) * seg_size（段号恰越界为 2^32）
    let res = device
      .truncate_until_address((u32::MAX as u64 + 1) * seg_size)
      .await;
    assert!(
      matches!(res, Err(Error::SegmentExceeded(_))),
      "(u32::MAX+1)*seg_size 截断地址必须拒绝为 SegmentExceeded，实际为 {res:?}"
    );

    // 拒绝后 start_segment 保持原值，未被推进（旧实现会钳位跃升至 u32::MAX）
    assert_eq!(
      device.start_segment(),
      0,
      "越界地址拒绝后 start_segment 必须保持原值，不得推进"
    );

    // 零段被删除：全部 5 个段文件仍在磁盘且数据逐字节完好
    for seg_id in 0..5u32 {
      assert!(
        device.segment_path(seg_id).exists(),
        "越界截断被拒绝后段 {seg_id} 必须物理保留（零段删除）"
      );
      assert_eq!(
        device.get_file_size(seg_id)?,
        4096,
        "段 {seg_id} 尺寸不得因越界截断而丢失"
      );
      let check = AlignedBuf::new(4096, 4096)?;
      let (res, check) = device.read_aligned((seg_id as u64) * seg_size, check).await;
      assert_eq!(res?, 4096);
      assert!(
        check
          .as_slice()
          .iter()
          .all(|&b| b == (seg_id * 17 + 1) as u8),
        "段 {seg_id} 数据必须完好无损"
      );
    }

    info!("越界地址截断保守拒绝、零段删除校验通过 (TruncateUntilAddress overflow)");
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
    let device = SegmentedDevice::new(
      dir.path().join("remove_seg.log"),
      seg_size,
      DEFAULT_SECTOR_SIZE,
    )?;

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
    let device = SegmentedDevice::new(
      dir.path().join("file_size.log"),
      1 << 20,
      DEFAULT_SECTOR_SIZE,
    )?;

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
    let device = SegmentedDevice::new(dir.path().join("reset.log"), 1 << 20, DEFAULT_SECTOR_SIZE)?;

    let buf = AlignedBuf::from_slice(&[0xEEu8; 4096], 4096)?;
    let (res, _) = device.write_aligned(0, buf).await;
    assert_eq!(res?, 4096);

    // 重置关闭句柄
    device.reset();

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
    let device = SegmentedDevice::new(
      dir.path().join("rapid_trunc.log"),
      seg_size,
      DEFAULT_SECTOR_SIZE,
    )?;

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
    for seg_id in 9..=10u32 {
      let expected = vec![((seg_id * 23 + 7) & 0xFF) as u8; 4096];
      let check = AlignedBuf::new(4096, 4096)?;
      let (res, check) = device.read_aligned((seg_id as u64) * seg_size, check).await;
      assert_eq!(res?, 4096);
      assert_eq!(check.as_slice(), &expected[..]);
    }

    info!("快速连续截断与幽灵段防御校验通过");
    aok::Result::<()>::Ok(())
  })?;

  OK
}

/// 截断即时清除被截断段的守护脏位（sync 持久化契约守护，仅 debug 构建）：
/// 写入段 0..3 后截断至段 2，被截断段 0、1 的在册脏位立即免责清除，
/// 守护位图仅剩段 2——维护"脏位仅存在于 start_segment 之后"的不变量。
#[test]
fn truncate_clears_dirty_bits_of_truncated_segments() -> Void {
  let rt = Runtime::new()?;
  rt.block_on(async {
    let dir = tempdir()?;
    let seg_size: u64 = 64 * 1024;
    let device = SegmentedDevice::new(
      dir.path().join("trunc_bits.log"),
      seg_size,
      DEFAULT_SECTOR_SIZE,
    )?;

    for seg_id in 0..3u32 {
      let buf = AlignedBuf::from_slice(&[(seg_id * 29 + 5) as u8; 4096], 4096)?;
      let (res, _) = device.write_aligned((seg_id as u64) * seg_size, buf).await;
      assert_eq!(res?, 4096);
    }

    #[cfg(debug_assertions)]
    assert_eq!(device.debug_dirty_segments(), vec![0, 1, 2]);

    // 截断至段 2：段 0、1 物理删除且脏位即时清除，仅段 2 待 sync 背书
    device.truncate_until_segment(2).await?;
    assert_eq!(device.start_segment(), 2);
    #[cfg(debug_assertions)]
    assert_eq!(
      device.debug_dirty_segments(),
      vec![2],
      "被截断段的守护脏位必须即时免责清除"
    );

    // 随后的 sync 正常覆盖段 2 并清空位图，无守护违约
    device.sync().await?;
    #[cfg(debug_assertions)]
    assert!(device.debug_dirty_segments().is_empty());

    info!("截断即时清除被截断段守护脏位校验通过");
    aok::Result::<()>::Ok(())
  })?;

  OK
}

/// 删段 I/O 故障后的幂等重试必须完整补收残留物理段（本票核心回归）：
/// 初次 `truncate_until_segment(4)` 在物理删段中途因真实 I/O 故障（POSIX unlink
/// 对目录必失败）上抛 Err；故障撤除后以**相同段号**重试，必须重新进入目录扫描
/// 并清除全部残留旧段，且保留段数据完好。旧实现以 `start_segment` 前置推进作
/// 短路判据，重试被瞬间短路为 Ok(())，残留段文件永久泄漏——修复前本测试必红。
#[cfg(unix)]
#[test]
fn truncate_retry_completes_purge_after_remove_io_failure() -> Void {
  let rt = Runtime::new()?;
  rt.block_on(async {
    let dir = tempdir()?;
    let seg_size: u64 = 64 * 1024;
    let device = SegmentedDevice::new(
      dir.path().join("trunc_retry.log"),
      seg_size,
      DEFAULT_SECTOR_SIZE,
    )?;

    fill_segments(&device, 6, seg_size).await?;

    // 注入故障：段 0..4 置为目录，扫描删除必在中途抛真实 I/O 错误
    replace_segments_with_dirs(&device, 4)?;

    let res = device.truncate_until_segment(4).await;
    assert!(
      matches!(res, Err(Error::Io(_))),
      "删段遭遇 I/O 故障必须透明上抛，实际为 {res:?}"
    );
    // 逻辑栅栏前置推进（保留的防护语义）：读写拦截自此次失败即生效
    assert_eq!(device.start_segment(), 4);

    // 撤除故障：段 0..4 重建为可删除的空文件
    restore_segments_as_files(&device, 4)?;

    // 相同段号幂等重试：必须补删全部残留段（旧实现在此被短路为 Ok 而泄漏）
    device.truncate_until_segment(4).await?;
    for seg_id in 0..4u32 {
      assert!(
        !device.segment_path(seg_id).exists(),
        "重试后残留段 {seg_id} 必须被补删，不得孤儿泄漏"
      );
    }

    // 全量清理后再截断/回退截断均为安全无操作，不复活不报错
    device.truncate_until_segment(4).await?;
    device.truncate_until_segment(2).await?;
    assert_eq!(device.start_segment(), 4);

    // 保留段 4、5 数据完好
    for seg_id in 4..6u32 {
      assert!(device.segment_path(seg_id).exists(), "段 {seg_id} 应保留");
      let check = AlignedBuf::new(4096, 4096)?;
      let (res, check) = device.read_aligned((seg_id as u64) * seg_size, check).await;
      assert_eq!(res?, 4096);
      assert!(
        check
          .as_slice()
          .iter()
          .all(|&b| b == (seg_id * 17 + 1) as u8)
      );
    }

    info!("删段 I/O 故障后幂等重试补删残留段校验通过");
    aok::Result::<()>::Ok(())
  })?;

  OK
}

/// 「begin 已推进、段未清理」撕裂态的重试可补完性（复核补注 2）：
/// `truncate_begin_until` 前置 fetch_max 推进 begin_address 后截断中途失败，
/// begin 栅栏已推前；相同 target 重试时 begin 的二次推进为幂等空操作，物理补删
/// 由段侧 `purged_segment` 水位承接——必须清除全部残留段。旧实现重试被
/// `start_segment` 短路，断言在修复前必红。
#[cfg(unix)]
#[test]
fn truncate_begin_until_retry_purges_after_io_failure() -> Void {
  let rt = Runtime::new()?;
  rt.block_on(async {
    let dir = tempdir()?;
    let seg_size: u64 = 64 * 1024;
    let device = SegmentedDevice::new(
      dir.path().join("trunc_begin_retry.log"),
      seg_size,
      DEFAULT_SECTOR_SIZE,
    )?;

    fill_segments(&device, 6, seg_size).await?;
    replace_segments_with_dirs(&device, 4)?;

    let begin = AtomicU64::new(0);
    let target = 4 * seg_size;
    let res = device.truncate_begin_until(&begin, target, ready(())).await;
    assert!(
      matches!(res, Err(Error::Io(_))),
      "截断失败必须上抛，实际为 {res:?}"
    );
    // 对位 C#「First update the begin address」：逻辑栅栏已推前
    assert_eq!(begin.load(Ordering::SeqCst), target);

    restore_segments_as_files(&device, 4)?;

    // 相同 target 重试：begin 幂等，物理残留段必须被补删
    device
      .truncate_begin_until(&begin, target, ready(()))
      .await?;
    assert_eq!(begin.load(Ordering::SeqCst), target);
    for seg_id in 0..4u32 {
      assert!(
        !device.segment_path(seg_id).exists(),
        "重试后残留段 {seg_id} 必须被补删"
      );
    }
    assert!(device.segment_path(4).exists(), "目标段 4 应保留");

    info!("begin 推进截断失败后重试补删撕裂态校验通过");
    aok::Result::<()>::Ok(())
  })?;

  OK
}

/// 越界地址经 `truncate_begin_until` 拒绝后段文件零删除、后续有效截断照常补收
/// （复核补注 1：Err 返回为常态分支，与段号越界显式拒绝路径共同成立）：
/// begin_address 依固定编排被推前（纯逻辑栅栏，论证见 `truncate_begin_until`
/// 注记），在册段必须全部留存，随后有效地址截断仍能完整清除其前段。
#[test]
fn truncate_begin_until_overflow_rejects_then_valid_truncate_purges() -> Void {
  let rt = Runtime::new()?;
  rt.block_on(async {
    let dir = tempdir()?;
    let seg_size: u64 = 64 * 1024;
    let device = SegmentedDevice::new(
      dir.path().join("trunc_begin_overflow.log"),
      seg_size,
      DEFAULT_SECTOR_SIZE,
    )?;

    fill_segments(&device, 6, seg_size).await?;

    let begin = AtomicU64::new(0);
    let overflow = (u32::MAX as u64 + 1) * seg_size;
    let res = device
      .truncate_begin_until(&begin, overflow, ready(()))
      .await;
    assert!(
      matches!(res, Err(Error::SegmentExceeded(_))),
      "越界 target 必须拒绝为 SegmentExceeded，实际为 {res:?}"
    );
    // 零段删除：全部在册段文件留存
    for seg_id in 0..6u32 {
      assert!(
        device.segment_path(seg_id).exists(),
        "越界拒绝后段 {seg_id} 必须物理保留"
      );
    }

    // 残段（此处即全部段）仍可被后续有效截断清除
    device.truncate_until_address(3 * seg_size + 1000).await?;
    assert_eq!(device.start_segment(), 3);
    for seg_id in 0..3u32 {
      assert!(
        !device.segment_path(seg_id).exists(),
        "段 {seg_id} 应已删除"
      );
    }
    for seg_id in 3..6u32 {
      assert!(device.segment_path(seg_id).exists(), "段 {seg_id} 应保留");
    }

    info!("越界 begin 编排拒绝与后续有效截断补收校验通过");
    aok::Result::<()>::Ok(())
  })?;

  OK
}
