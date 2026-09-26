//! 读路径幽灵段防御：读缺失段绝不物理建文件、不预分配，且不击穿重启恢复。
//!
//! 对标 C# 设备族读/写打开模式严格解耦：
//! `garnet/libs/storage/Tsavorite/cs/src/core/Device/ManagedLocalStorageDevice.cs`
//! （`CreateReadHandle` 以 `FileAccess.Read` 打开且不执行 `SetFileSize`，预分配只发生在
//! `CreateWriteHandle`）；C++ `file_system_disk.h` 的 `FileSystemSegmentedFile.ReadAsync`
//! 仅打开已存在段、缺失即报错。Rust 侧据此把读路径收敛为 `get_or_open_file(.., false)`，
//! 并令预分配仅在物理新建段时生效。
//!
//! 自研回归锁: 幻影读防御（截断/复用段的越权读拦截）

use std::fs::{metadata, write as fs_write};

use aok::{OK, Void};
use log::info;
use tempfile::tempdir;
use wbase::{align::DEFAULT_SECTOR_SIZE, pool::AlignedBuf};
use wdev::{Device, DeviceParams, Error, SegmentedDevice};

use crate::support::make_pattern_data;

/// 单段快速路径：读未写入段与经 `remove_segment` 删除段均返回
/// [`Error::SegmentNotFound`]，磁盘上绝不因此新建任何段文件（`read_aligned` 与
/// `read_raw` 两条入口共用同一内核，一并覆盖）。
#[compio::test]
async fn read_unwritten_segment_returns_not_found_without_phantom() -> Void {
  let dir = tempdir()?;
  let seg_size: u64 = 64 * 1024;
  let db_path = dir.path().join("read_phantom.log");
  let device = SegmentedDevice::new(&db_path, seg_size, DEFAULT_SECTOR_SIZE)?;

  // 仅写段 0（4KB），使设备处于"段 0 在册、段 5/6 从未写入"的空洞态
  let pattern = make_pattern_data(4096, 3, 7);
  let buf = AlignedBuf::from_slice(&pattern, DEFAULT_SECTOR_SIZE)?;
  let (res, _) = device.write_aligned(0, buf).await;
  assert_eq!(res?, 4096);

  // read_aligned 命中单段快速路径读未写入段 5
  let check = AlignedBuf::new(4096, DEFAULT_SECTOR_SIZE)?;
  let (res, _) = device.read_aligned(5 * seg_size, check).await;
  assert!(
    matches!(res, Err(Error::SegmentNotFound(5))),
    "读未写入段 5 必须返回 SegmentNotFound(5)，实际为 {res:?}"
  );
  assert!(
    !device.segment_path(5).exists(),
    "读缺失段 5 后严禁在磁盘幽灵新建段文件"
  );

  // read_raw（缓冲 I/O 内核）同样不得建文件
  let check = AlignedBuf::new(4096, DEFAULT_SECTOR_SIZE)?;
  let (res, _) = device.read_raw(6 * seg_size, check).await;
  assert!(
    matches!(res, Err(Error::SegmentNotFound(6))),
    "read_raw 未写入段 6 必须返回 SegmentNotFound(6)，实际为 {res:?}"
  );
  assert!(
    !device.segment_path(6).exists(),
    "read_raw 缺失段 6 后严禁在磁盘幽灵新建段文件"
  );

  // 既有在册段 0 数据完好，未受影响
  let check = AlignedBuf::new(4096, DEFAULT_SECTOR_SIZE)?;
  let (res, check) = device.read_aligned(0, check).await;
  assert_eq!(res?, 4096);
  assert_eq!(
    check.as_slice(),
    &pattern[..],
    "段 0 既有数据不得被读操作破坏"
  );

  info!("读缺失段返回 SegmentNotFound 且不建幽灵段（CreateReadHandle 不建文件）");
  OK
}

/// 跨段分片慢路径：读区间落入未写入段时，首片缺失段即上抛
/// [`Error::SegmentNotFound`]，且不建任何幽灵分片段。
#[compio::test]
async fn read_cross_segment_unwritten_no_phantom() -> Void {
  let dir = tempdir()?;
  let seg_size: u64 = 64 * 1024;
  let db_path = dir.path().join("read_cross_phantom.log");
  let device = SegmentedDevice::new(&db_path, seg_size, DEFAULT_SECTOR_SIZE)?;

  // 写段 0、1（各 4KB），构造跨越未写入段 3/4 的读请求
  for seg in 0..2u32 {
    let buf = AlignedBuf::from_slice(&[0x9Au8; 4096], DEFAULT_SECTOR_SIZE)?;
    let (res, _) = device.write_aligned((seg as u64) * seg_size, buf).await;
    assert_eq!(res?, 4096);
  }

  // 起点落在段 3 中段、跨入段 4，两片段均未写入 → 触发慢路径
  let offset = 3 * seg_size + seg_size / 2;
  let check = AlignedBuf::new(seg_size as usize, DEFAULT_SECTOR_SIZE)?;
  let (res, _) = device.read_aligned(offset, check).await;
  assert!(
    matches!(res, Err(Error::SegmentNotFound(3))),
    "跨段读首片缺失段 3 必须返回 SegmentNotFound(3)，实际为 {res:?}"
  );
  assert!(
    !device.segment_path(3).exists(),
    "跨段读缺失段后严禁在磁盘幽灵新建段 3"
  );
  assert!(
    !device.segment_path(4).exists(),
    "跨段读缺失段后严禁在磁盘幽灵新建段 4"
  );

  info!("跨段慢路径读缺失段返回 SegmentNotFound 且不建幽灵段");
  OK
}

/// 幽灵段击穿重启恢复的回归：读未写入段若曾在磁盘留空洞文件，恢复扫描据空隙
/// 强推 `start_segment` 至空洞段，丢弃此前合法段（灾难性数据丢失）。修复后读
/// 不建文件，恢复的 `start_segment`/`end_segment` 保持既有连续区间、历史数据完好。
#[compio::test]
async fn read_unwritten_segment_does_not_break_recover_range() -> Void {
  let dir = tempdir()?;
  let seg_size: u64 = 64 * 1024;
  let db_path = dir.path().join("read_recover_gap.log");
  let pattern = make_pattern_data(4096, 11, 3);

  // 写入合法段 0、1 后，误读从未写入的段 5
  {
    let device = SegmentedDevice::new(&db_path, seg_size, DEFAULT_SECTOR_SIZE)?;
    for seg in 0..2u32 {
      let buf = AlignedBuf::from_slice(&pattern, DEFAULT_SECTOR_SIZE)?;
      let (res, _) = device.write_aligned((seg as u64) * seg_size, buf).await;
      assert_eq!(res?, 4096);
    }
    let check = AlignedBuf::new(4096, DEFAULT_SECTOR_SIZE)?;
    let (res, _) = device.read_aligned(5 * seg_size, check).await;
    assert!(
      matches!(res, Err(Error::SegmentNotFound(5))),
      "读未写入段 5 必须返回 SegmentNotFound(5)，实际为 {res:?}"
    );
  }

  // 重开恢复：无幽灵段则连续区间保持 start=0/end=1
  let device = SegmentedDevice::new(&db_path, seg_size, DEFAULT_SECTOR_SIZE)?;
  device.recover()?;
  assert_eq!(
    device.start_segment(),
    0,
    "读缺失段后恢复的 start_segment 必须仍为 0（幽灵段会把它推至 5 并吞掉段 0/1）"
  );
  assert_eq!(
    device.end_segment(),
    Some(1),
    "end_segment 应保持最大连续段号 1"
  );

  // 历史段 0、1 数据完好可读
  for seg in 0..2u32 {
    let check = AlignedBuf::new(4096, DEFAULT_SECTOR_SIZE)?;
    let (res, check) = device.read_aligned((seg as u64) * seg_size, check).await;
    assert_eq!(res?, 4096, "恢复后合法段 {seg} 必须可读");
    assert_eq!(check.as_slice(), &pattern[..], "段 {seg} 历史数据完好");
  }

  info!("读缺失段不生成幽灵段，重启恢复连续区间与历史数据完好");
  OK
}

/// 读操作零磁盘副作用（预分配收敛到"仅物理新建段"）：启用 `preallocate` 的设备
/// 读取一个已存在但小于整段的段文件时，绝不因 `set_len` 把它撑到整段尺寸；且
/// `create=false` 打开的已存在段句柄以 O_RDWR 保留写权限，供后续写入安全复用。
#[compio::test]
async fn read_existing_segment_does_not_preallocate_or_mutate() -> Void {
  let dir = tempdir()?;
  let seg_size: u64 = 64 * 1024;
  let db_path = dir.path().join("read_prealloc.log");
  let device = SegmentedDevice::with_params(
    &db_path,
    seg_size,
    DEFAULT_SECTOR_SIZE,
    DeviceParams {
      preallocate: true,
      ..DeviceParams::default()
    },
  )?;

  // 外部放置一个仅 4KB 的已存在段 0（小于整段 64KB），模拟半写段
  let pattern = make_pattern_data(4096, 5, 1);
  fs_write(device.segment_path(0), &pattern)?;
  assert_eq!(metadata(device.segment_path(0))?.len(), 4096);

  // 读段 0：必须原样读到 4KB，且绝不把文件撑到整段尺寸
  let check = AlignedBuf::new(4096, DEFAULT_SECTOR_SIZE)?;
  let (res, check) = device.read_aligned(0, check).await;
  assert_eq!(res?, 4096);
  assert_eq!(check.as_slice(), &pattern[..]);
  assert_eq!(
    metadata(device.segment_path(0))?.len(),
    4096,
    "读已存在半写段绝不得触发 set_len 预分配把文件撑到整段"
  );

  // 读后的 O_RDWR 句柄可被后续写入复用（同段紧邻追加 4KB）
  let more = AlignedBuf::from_slice(&[0x77u8; 4096], DEFAULT_SECTOR_SIZE)?;
  let (res, _) = device.write_aligned(4096, more).await;
  assert_eq!(
    res?, 4096,
    "复用读打开的段 0 句柄追加写入必须成功（无 EBADF）"
  );
  assert_eq!(metadata(device.segment_path(0))?.len(), 8192);

  let check = AlignedBuf::new(4096, DEFAULT_SECTOR_SIZE)?;
  let (res, check) = device.read_aligned(4096, check).await;
  assert_eq!(res?, 4096);
  assert!(check.as_slice().iter().all(|&b| b == 0x77));

  info!("读已存在段零磁盘副作用且句柄写复用成立");
  OK
}
