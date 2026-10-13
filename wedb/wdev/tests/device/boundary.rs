//! 极限边界语义：超大跨段读写、u64 溢出与超大段号防御。
//!
//! 对标 C# 测试文件：
//! `garnet/libs/storage/Tsavorite/cs/test/test.hlog/DeviceTests.cs`
//! （跨段读写对标 libs/storage/Tsavorite/cs/test/test.hlog/DeviceTests.cs:IDevice_RoundTrip_AcrossSegmentBoundary 的多边界延展）；
//! 整数溢出与超大段号拦截为 Rust 补齐的 `checked_add` / `u32::try_from`
//! 防御语义（对标 C# unchecked 域外的上界防护）。
//!
//! 在 garnet 中的相对路径: libs/storage/Tsavorite/cs/test/test.hlog/DeviceTests.cs + DeviceLogTests.cs（边界容量）

use aok::{OK, Void};
use compio::runtime::Runtime;
use log::info;
use tempfile::tempdir;
use wbase::{align::DEFAULT_SECTOR_SIZE, pool::AlignedBuf};
use wdev::{Device, Error, SegmentedDevice};

use crate::support::make_pattern_data;

/// 超大跨段读写：一次性写入 320KB 跨越 5 个段并完整回读；
/// 从非对齐段内偏移（60KB）写入 260KB 跨越 5 个换段边界并回读；
/// read_range 大跨度非对齐读取跨越 3 个段。
#[test]
fn massive_cross_segment_round_trip() -> Void {
  let rt = Runtime::new()?;
  rt.block_on(async {
    let dir = tempdir()?;
    let seg_size: u64 = 64 * 1024;
    let device = SegmentedDevice::new(
      dir.path().join("massive.log"),
      seg_size,
      DEFAULT_SECTOR_SIZE,
    )?;

    // 1. 一次性写入 320KB（段 0..5），模式 (i * 41 + 19) & 0xFF
    let size_320k = 5 * seg_size as usize;
    let pattern = make_pattern_data(size_320k, 41, 19);
    let write_buf = AlignedBuf::from_slice(&pattern, 4096)?;
    assert!(write_buf.is_aligned_to(write_buf.align()));
    let (res, _) = device.write_aligned(0, write_buf).await;
    assert_eq!(res?, size_320k);

    // 5 个段文件全部物理存在且尺寸恰为段尺寸
    for seg_id in 0..5u32 {
      assert!(device.segment_path(seg_id).exists());
      assert_eq!(device.get_file_size(seg_id)?, seg_size);
    }

    // 一次性跨 5 段完整回读
    let read_buf = AlignedBuf::new(size_320k, 4096)?;
    let (res, read_buf) = device.read_aligned(0, read_buf).await;
    assert_eq!(res?, size_320k);
    assert_eq!(read_buf.as_slice(), &pattern[..]);

    // 2. 从 60KB 起写入 260KB（段 0 尾部 -> 段 4 头部，跨越 5 个换段边界）
    let offset_mid = 60 * 1024;
    let size_260k = 260 * 1024;
    let pattern_mid = make_pattern_data(size_260k, 47, 3);
    let write_buf2 = AlignedBuf::from_slice(&pattern_mid, 4096)?;
    let (res, _) = device.write_aligned(offset_mid, write_buf2).await;
    assert_eq!(res?, size_260k);

    let read_buf2 = AlignedBuf::new(size_260k, 4096)?;
    let (res, read_buf2) = device.read_aligned(offset_mid, read_buf2).await;
    assert_eq!(res?, size_260k);
    assert_eq!(read_buf2.as_slice(), &pattern_mid[..]);

    // 3. read_range 大跨度非对齐读取（65530 起读 70000 字节，跨 3 个段），
    //    与第二次写入数据的重叠部分必须一致
    let range = device.read_range(65530, 70000).await?;
    assert_eq!(range.len(), 70000);
    assert_eq!(range.as_slice()[..1000], pattern_mid[4090..5090]);

    info!("超大跨段（5 段）读写回环与大跨度切片校验通过");
    aok::Result::<()>::Ok(())
  })?;

  OK
}

/// u64 溢出与超大段号防御：`offset + len` 回绕必须拦截为 OutOfBounds，
/// 段编号超出 u32 的偏移必须拦截为 SegmentExceeded。
#[test]
fn integer_overflow_defense_on_offset_and_segment_number() -> Void {
  let rt = Runtime::new()?;
  rt.block_on(async {
    let dir = tempdir()?;
    let device = SegmentedDevice::new(
      dir.path().join("overflow.log"),
      64 * 1024,
      DEFAULT_SECTOR_SIZE,
    )?;

    // Case A: offset + len 导致 u64 溢出（u64::MAX - 4095 + 8192 回绕）
    let bad_offset = u64::MAX - 4095;
    let overflow_buf = AlignedBuf::zeroed(8192, 4096)?;
    let (res, _) = device.write_aligned(bad_offset, overflow_buf).await;
    assert!(
      matches!(res, Err(Error::OutOfBounds { offset, len }) if offset == bad_offset && len == 8192),
      "offset + len 溢出必须返回 OutOfBounds，实际为 {res:?}"
    );

    let read_buf = AlignedBuf::new(8192, 4096)?;
    let (res, _) = device.read_aligned(bad_offset, read_buf).await;
    assert!(
      matches!(res, Err(Error::OutOfBounds { offset, len }) if offset == bad_offset && len == 8192),
      "offset + len 溢出读取必须返回 OutOfBounds，实际为 {res:?}"
    );

    // Case B: 段编号超出 u32::MAX（1 << 50 偏移对应段号 1 << 34）
    let huge_offset = 1u64 << 50;
    let huge_buf = AlignedBuf::zeroed(4096, 4096)?;
    let (res, _) = device.write_aligned(huge_offset, huge_buf).await;
    assert!(
      matches!(res, Err(Error::SegmentExceeded(_))),
      "超大段号偏移必须返回 SegmentExceeded，实际为 {res:?}"
    );

    info!("u64 溢出与超大段号防御校验通过");
    aok::Result::<()>::Ok(())
  })?;

  OK
}

/// 按地址截断的段号溢出防御：超出 u32 段界限的越界/回绕地址必须显式拒绝为
/// `SegmentExceeded`，绝不钳位 u32::MAX 放行截断（对标 `StorageDeviceBase.cs:318/333`
/// C# 符号截断天然落入 `<= startSegment` 无操作、绝不误删有效段的契约）。
#[test]
fn truncate_until_address_overflow_is_rejected_not_clamped() -> Void {
  let rt = Runtime::new()?;
  rt.block_on(async {
    let dir = tempdir()?;
    let seg_size: u64 = 64 * 1024;
    let device = SegmentedDevice::new(dir.path().join("clamp.log"), seg_size, DEFAULT_SECTOR_SIZE)?;

    // Case A: u64::MAX 回绕地址，段号 u64::MAX >> 16 远超 u32::MAX
    let res = device.truncate_until_address(u64::MAX).await;
    assert!(
      matches!(res, Err(Error::SegmentExceeded(seg)) if seg == u64::MAX >> 16),
      "u64::MAX 截断地址必须拒绝为 SegmentExceeded，实际为 {res:?}"
    );

    // Case B: 段号恰越界 (u32::MAX + 1) * seg_size，段号 = 2^32 越出 u32
    let boundary_addr = (u32::MAX as u64 + 1) * seg_size;
    let res = device.truncate_until_address(boundary_addr).await;
    assert!(
      matches!(res, Err(Error::SegmentExceeded(seg)) if seg == u32::MAX as u64 + 1),
      "越界截断地址必须拒绝为 SegmentExceeded，实际为 {res:?}"
    );

    // 关键：拒绝即快速返回，start_segment 绝不因钳位 u32::MAX 而被推进
    assert_eq!(
      device.start_segment(),
      0,
      "越界地址拒绝后 start_segment 必须保持原值，不得钳位推进至 u32::MAX"
    );

    info!("按地址截断段号溢出拒绝防御校验通过");
    aok::Result::<()>::Ok(())
  })?;

  OK
}
