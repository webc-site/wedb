//! 读写回环语义：对齐回环、跨段回环、多段尺寸参数化、单文件无界模式与 read_range 切片。
//!
//! 对标 C# 测试文件：
//! `/Users/z/git/db/garnet/libs/storage/Tsavorite/cs/test/test.hlog/DeviceTests.cs`
//! （方法 `NativeDeviceTest1`、`NativeDeviceTest2`、`IDevice_RoundTrip_BasicReadWrite`、
//! `IDevice_RoundTrip_AcrossSegmentBoundary`、`IDevice_RoundTrip_VariousSegmentSizes`、
//! `IDevice_Initialize_SegmentSizeMinusOne_UnboundedSingleSegment`、
//! `IDevice_Initialize_OmitSegmentIdFromFilename_BareFileName`）。

use std::path::Path;

use aok::{OK, Void};
use compio::runtime::Runtime;
use log::info;
use tempfile::tempdir;
use wdev::{BufferPool, Device, Error, SegmentedDevice};
use wram::AlignedBuf;

use crate::support::make_pattern_data;

/// 对标 C# `NativeDeviceTest1`：512 字节最小扇区的对齐写入与回读
/// （C# entryLength = MinDeviceSectorSize * 2 = 1024，内容为 `(byte)i`）。
#[test]
fn native_device_test1() -> Void {
  let rt = Runtime::new()?;
  rt.block_on(async {
    let dir = tempdir()?;
    let device = SegmentedDevice::new(dir.path().join("native1.log"), None, 512)?;
    assert_eq!(device.sector_size(), 512);

    let entry_len = 1024;
    let entry = make_pattern_data(entry_len, 1, 0);
    let write_buf = AlignedBuf::from_slice(&entry, 512)?;
    assert!(write_buf.is_ptr_aligned());

    let (res, _) = device.write_aligned(0, write_buf).await;
    assert_eq!(res?, entry_len);

    let read_buf = AlignedBuf::new(entry_len, 512)?;
    let (res, read_buf) = device.read_aligned(0, read_buf).await;
    assert_eq!(res?, entry_len);
    assert_eq!(read_buf.as_slice(), &entry[..]);

    info!("512 扇区最小对齐读写回环通过 (NativeDeviceTest1)");
    aok::Result::<()>::Ok(())
  })?;

  OK
}

/// 对标 C# `NativeDeviceTest2`：4096 扇区下 64KB 数据写入回读，并在多个偏移上
/// 多轮迭代写入校验（C# 迭代 50 轮，此处按 CI 预算缩为 10 轮）。
#[test]
fn native_device_test2() -> Void {
  let rt = Runtime::new()?;
  rt.block_on(async {
    let dir = tempdir()?;
    let device = SegmentedDevice::new(dir.path().join("native2.log"), None, 4096)?;
    assert_eq!(device.sector_size(), 4096);

    // 首轮：64KB（16 个 4KB 扇区）整体回环，模式为 (i * 7 + 3) & 0xFF
    let size_64k = 64 * 1024;
    let pattern = make_pattern_data(size_64k, 7, 3);
    let write_buf = AlignedBuf::from_slice(&pattern, 4096)?;
    let (res, _) = device.write_aligned(0, write_buf).await;
    assert_eq!(res?, size_64k);

    let read_buf = AlignedBuf::new(size_64k, 4096)?;
    let (res, read_buf) = device.read_aligned(0, read_buf).await;
    assert_eq!(res?, size_64k);
    assert_eq!(read_buf.as_slice(), &pattern[..]);

    // 多轮迭代：不同偏移、不同模式写入并回读（对标 C# 50 次循环写入不同缓冲）
    for round in 1..10u64 {
      let block_offset = round * 2 * 4096;
      let block_pattern = make_pattern_data(8192, 11 + round as usize, 5);
      let block_buf = AlignedBuf::from_slice(&block_pattern, 4096)?;
      let (res, _) = device.write_aligned(block_offset, block_buf).await;
      assert_eq!(res?, 8192);

      let check_buf = AlignedBuf::new(8192, 4096)?;
      let (res, check_buf) = device.read_aligned(block_offset, check_buf).await;
      assert_eq!(res?, 8192);
      assert_eq!(check_buf.as_slice(), &block_pattern[..]);
    }

    info!("4096 扇区多轮对齐读写回环通过 (NativeDeviceTest2)");
    aok::Result::<()>::Ok(())
  })?;

  OK
}

/// 对标 C# `IDevice_RoundTrip_BasicReadWrite`：单次对齐写入后立即回读的闭环校验
/// （C# 为 64MiB 段 + 64KB 数据，模式 `(i * 7) & 0xFF`；此处等比缩小段尺寸）。
#[test]
fn idevice_round_trip_basic_read_write() -> Void {
  let rt = Runtime::new()?;
  rt.block_on(async {
    let dir = tempdir()?;
    let device = SegmentedDevice::segmented(dir.path().join("basic_rw.log"), 64 * 1024)?;

    let size = 64 * 1024;
    let pattern = make_pattern_data(size, 7, 0);
    let write_buf = AlignedBuf::from_slice(&pattern, 4096)?;
    let (res, _) = device.write_aligned(0, write_buf).await;
    assert_eq!(res?, size);

    let read_buf = AlignedBuf::new(size, 4096)?;
    let (res, read_buf) = device.read_aligned(0, read_buf).await;
    assert_eq!(res?, size);
    assert_eq!(read_buf.as_slice(), &pattern[..]);

    info!("基础对齐单次读写回环通过 (IDevice_RoundTrip_BasicReadWrite)");
    aok::Result::<()>::Ok(())
  })?;

  OK
}

/// 对标 C# `IDevice_RoundTrip_AcrossSegmentBoundary`：换段边界处的跨段连续读写
/// （C# 直接指定 segmentId=1；Rust 以逻辑偏移 60KB 起 16KB 等价覆盖段 0 尾部与段 1 头部）。
#[test]
fn idevice_round_trip_across_segment_boundary() -> Void {
  let rt = Runtime::new()?;
  rt.block_on(async {
    let dir = tempdir()?;
    let seg_size: u64 = 64 * 1024;
    let device = SegmentedDevice::segmented(dir.path().join("cross_seg.log"), seg_size)?;

    // 写入 16KB：段 0 尾部 4KB (61440..65536) + 段 1 头部 12KB (0..12288)，模式 (i * 11) & 0xFF
    let size = 16 * 1024;
    let pattern = make_pattern_data(size, 11, 0);
    let write_buf = AlignedBuf::from_slice(&pattern, 4096)?;
    let (res, _) = device.write_aligned(60 * 1024, write_buf).await;
    assert_eq!(res?, size);

    // 段 0 与段 1 文件均已在磁盘生成且物理尺寸精确
    assert!(device.segment_path(0).exists(), "段 0 必须存在");
    assert!(device.segment_path(1).exists(), "段 1 必须存在");
    assert_eq!(device.get_file_size(0)?, 65536);
    assert_eq!(device.get_file_size(1)?, 12288);

    // 跨段连续回读并逐字节校验
    let read_buf = AlignedBuf::new(size, 4096)?;
    let (res, read_buf) = device.read_aligned(60 * 1024, read_buf).await;
    assert_eq!(res?, size);
    assert_eq!(read_buf.as_slice(), &pattern[..]);

    // 分别单独读取各段内部数据，验证数据在段间的切分位置精准
    let seg0_tail = AlignedBuf::new(4096, 4096)?;
    let (res, seg0_tail) = device.read_aligned(60 * 1024, seg0_tail).await;
    assert_eq!(res?, 4096);
    assert_eq!(seg0_tail.as_slice(), &pattern[..4096]);

    let seg1_head = AlignedBuf::new(12288, 4096)?;
    let (res, seg1_head) = device.read_aligned(seg_size, seg1_head).await;
    assert_eq!(res?, 12288);
    assert_eq!(seg1_head.as_slice(), &pattern[4096..]);

    info!("换段边界跨段读写回环通过 (IDevice_RoundTrip_AcrossSegmentBoundary)");
    aok::Result::<()>::Ok(())
  })?;

  OK
}

/// 对标 C# `IDevice_RoundTrip_VariousSegmentSizes`：多种段尺寸参数化回读
/// （C# 参数为 64/256/1024 MiB；此处等比缩小为 1/4 MiB 并覆盖跨段边界与非对齐切片）。
#[test]
fn idevice_round_trip_various_segment_sizes() -> Void {
  let rt = Runtime::new()?;
  rt.block_on(async {
    let dir = tempdir()?;
    for seg_size in [1u64 << 20, 4u64 << 20] {
      let device =
        SegmentedDevice::segmented(dir.path().join(format!("seg_{seg_size}.log")), seg_size)?;

      // 从段尾 -8KB 起写入 16KB，跨越段 0 -> 段 1，模式 (i * 7) & 0xFF
      let size = 16 * 1024;
      let offset = seg_size - 8 * 1024;
      let pattern = make_pattern_data(size, 7, 0);
      let write_buf = AlignedBuf::from_slice(&pattern, 4096)?;
      let (res, _) = device.write_aligned(offset, write_buf).await;
      assert_eq!(res?, size);
      assert_eq!(
        device.end_segment(),
        Some(1),
        "跨段写入后 end_segment 应为 1"
      );

      let read_buf = AlignedBuf::new(size, 4096)?;
      let (res, read_buf) = device.read_aligned(offset, read_buf).await;
      assert_eq!(res?, size);
      assert_eq!(read_buf.as_slice(), &pattern[..]);

      // 段内非对齐任意范围回读
      let range = device.read_range(offset + 100, 5000).await?;
      assert_eq!(range.len(), 5000);
      assert_eq!(range.as_slice(), &pattern[100..5100]);

      info!("段尺寸 {seg_size} 跨段回环通过 (VariousSegmentSizes)");
    }
    aok::Result::<()>::Ok(())
  })?;

  OK
}

/// 对标 C# `IDevice_Initialize_SegmentSizeMinusOne_UnboundedSingleSegment` 与
/// `IDevice_Initialize_OmitSegmentIdFromFilename_BareFileName`：
/// Rust 单文件模式等价二者并集——所有 I/O 路由到段 0，物理文件为裸名（无 `.0` 段号后缀），
/// 任何正段尺寸下本应跨段的高偏移写入完整落入单一增长文件。
#[test]
fn idevice_initialize_segment_size_minus_one_unbounded_single_segment() -> Void {
  let rt = Runtime::new()?;
  rt.block_on(async {
    let dir = tempdir()?;
    let db_path = dir.path().join("unbounded_single.log");
    let device = SegmentedDevice::single_file(&db_path)?;

    // 1MiB 高偏移在任何正段尺寸下都会跨段，此处必须完整落入单一物理文件
    const HIGH_OFFSET: u64 = 1 << 20;
    let pattern = make_pattern_data(4096, 11, 3);
    let write_buf = AlignedBuf::from_slice(&pattern, 4096)?;
    let (res, _) = device.write_aligned(HIGH_OFFSET, write_buf).await;
    assert_eq!(res?, 4096);

    // 物理文件必须是裸名，且严禁生成段号后缀文件
    assert!(db_path.exists(), "单文件模式必须使用裸名文件");
    let mut seg0 = db_path.clone().into_os_string();
    seg0.push(".0");
    assert!(!Path::new(&seg0).exists(), "单文件模式严禁生成段号后缀文件");
    assert_eq!(device.get_file_size(0)?, HIGH_OFFSET + 4096);

    // 高偏移对齐回读 + 非对齐任意范围回读
    let read_buf = AlignedBuf::new(4096, 4096)?;
    let (res, read_buf) = device.read_aligned(HIGH_OFFSET, read_buf).await;
    assert_eq!(res?, 4096);
    assert_eq!(read_buf.as_slice(), &pattern[..]);

    let range = device.read_range(HIGH_OFFSET + 100, 500).await?;
    assert_eq!(range.as_slice(), &pattern[100..600]);

    info!("单文件无界模式高偏移回读与裸名文件校验通过 (UnboundedSingleSegment)");
    aok::Result::<()>::Ok(())
  })?;

  OK
}

/// Rust 扩展语义（C# 无对应 API）：`read_range` / `read_range_pooled` 任意逻辑范围读取，
/// 对标 C# 上层记录读取场景（ReadInto / 逻辑非对齐记录切片回读）。
/// 覆盖单扇区内、跨扇区、1 字节跨界、跨段、大跨度切片，以及 0 字节读写与 EOF 防御。
#[test]
fn read_range_cross_sector_unaligned_slices() -> Void {
  let rt = Runtime::new()?;
  rt.block_on(async {
    let dir = tempdir()?;
    let seg_size: u64 = 64 * 1024;
    let device = SegmentedDevice::segmented(dir.path().join("read_range.log"), seg_size)?;

    // 写入 128KB 跨段 0 与段 1 的基准数据
    let total = 128 * 1024;
    let data = make_pattern_data(total, 31, 11);
    let write_buf = AlignedBuf::from_slice(&data, 4096)?;
    let (res, _) = device.write_aligned(0, write_buf).await;
    assert_eq!(res?, total);

    // 单扇区内部非对齐读取
    let r1 = device.read_range(123, 456).await?;
    assert_eq!(r1.as_slice(), &data[123..579]);

    // 跨 4KB 扇区边界（4000..4500 跨越 4096）
    let r2 = device.read_range(4000, 500).await?;
    assert_eq!(r2.as_slice(), &data[4000..4500]);

    // 扇区边界恰好 1 字节跨界（4095..4097）
    let r3 = device.read_range(4095, 2).await?;
    assert_eq!(r3.as_slice(), &data[4095..4097]);

    // 跨 64KB 段边界且非对齐（65500..66500 跨越 65536）
    let r4 = device.read_range(65500, 1000).await?;
    assert_eq!(r4.as_slice(), &data[65500..66500]);

    // 跨多段多扇区的大跨度非对齐读取
    let r5 = device.read_range(1234, 80000).await?;
    assert_eq!(r5.len(), 80000);
    assert_eq!(r5.as_slice(), &data[1234..81234]);

    // 池化读取：对齐命中与非对齐子视图均正确（缓冲区 drop 时自动回池）
    let pool = BufferPool::new(4096)?;
    let pooled1 = device.read_range_pooled(100, 250, &pool).await?;
    assert_eq!(pooled1.as_slice(), &data[100..350]);
    drop(pooled1);

    let pooled2 = device.read_range_pooled(0, 4096, &pool).await?;
    assert_eq!(pooled2.as_slice(), &data[..4096]);
    drop(pooled2);

    // 0 字节极限读写
    let empty = SegmentedDevice::single_file(dir.path().join("zero_len.log"))?;
    let zero_buf = AlignedBuf::new(0, 4096)?;
    let (res, _) = empty.write_aligned(0, zero_buf).await;
    assert_eq!(res?, 0);

    let zero_read = AlignedBuf::new(0, 4096)?;
    let (res, _) = empty.read_aligned(0, zero_read).await;
    assert_eq!(res?, 0);
    assert_eq!(empty.read_range(0, 0).await?.len(), 0);

    // 超出文件范围的读取必须以 EOF 错误防御，而非静默截断
    let eof_res = empty.read_range(0, 100).await;
    assert!(
      matches!(eof_res, Err(Error::UnexpectedEof { .. })),
      "空设备非零长度读取必须返回 UnexpectedEof，实际为 {eof_res:?}"
    );

    info!("read_range 非对齐切片、池化读取与 0 字节/EOF 边界校验通过");
    aok::Result::<()>::Ok(())
  })?;

  OK
}

/// Rust 扩展语义：`read_aligned` 按 `required_len` 精确封顶，杜绝读放大；
/// 末段最后一扇区读取绝不越界跨段创建幽灵段文件。
#[test]
fn read_aligned_caps_at_required_len_without_ghost_segment() -> Void {
  let rt = Runtime::new()?;
  rt.block_on(async {
    let dir = tempdir()?;
    let seg_size: u64 = 64 * 1024;
    let pool = BufferPool::new(4096)?;
    let device = SegmentedDevice::with_pool(
      dir.path().join("ghost_defense.log"),
      Some(seg_size),
      4096,
      pool.clone(),
    )?;

    // 写满段 0（64KB），此时段 1 绝不应存在
    let full_seg = make_pattern_data(seg_size as usize, 13, 7);
    let write_buf = AlignedBuf::from_slice(&full_seg, 4096)?;
    let (res, _) = device.write_aligned(0, write_buf).await;
    assert_eq!(res?, seg_size as usize);
    assert!(device.segment_path(0).exists(), "段 0 文件应已存在");
    assert!(!device.segment_path(1).exists(), "段 1 文件此时绝不应存在");

    // 从缓冲池签发读缓冲，在段 0 最后一扇区（60KB..64KB）读取；
    // 若按池 class 容量而非 required_len 封顶，将越界跨入段 1 并凭空创建幽灵文件
    let read_buf = pool.get_with_policy(4096, false)?;
    assert_eq!(read_buf.required_len(), 4096);
    let last_sector = 60 * 1024;
    let (res, read_buf) = device.read_aligned(last_sector, read_buf).await;
    assert_eq!(res?, 4096);
    assert_eq!(read_buf.len(), 4096);
    assert_eq!(read_buf.as_slice(), &full_seg[last_sector as usize..]);

    // 核心断言：段 1 文件绝不能被幽灵创建
    assert!(
      !device.segment_path(1).exists(),
      "末尾段精确读取绝不能在磁盘创建幽灵段文件 1"
    );

    // read_range 读取段 0 尾部同样不得触发幽灵文件
    let range = device.read_range(last_sector, 4096).await?;
    assert_eq!(range.len(), 4096);
    assert!(
      !device.segment_path(1).exists(),
      "read_range 读取末尾段绝不能创建幽灵段文件 1"
    );

    info!("read_aligned 按 required_len 封顶与幽灵段防御通过");
    aok::Result::<()>::Ok(())
  })?;

  OK
}
