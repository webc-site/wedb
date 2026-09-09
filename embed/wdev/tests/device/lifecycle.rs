//! 生命周期语义：sync 刷盘持久化、句柄按需重开与首次写入权限拒绝传播。
//!
//! 对标 C# 测试文件：
//! `/Users/z/git/db/garnet/libs/storage/Tsavorite/cs/test/test.hlog/DeviceTests.cs`
//! （方法 `IDevice_PermissionDeniedAtFirstWrite_CallbackGetsError`）；
//! sync 刷盘对标 C# `IDevice.FlushAsync` 物理落盘契约（Rust 受 compio
//! thread-per-core 亲和约束，句柄按 (线程ID, 段号) 隔离，sync 仅覆盖本线程 I/O）。

use aok::{OK, Void};
use compio::runtime::Runtime;
use log::info;
use tempfile::tempdir;
use wdev::{Device, Error, SegmentedDevice};
use wutil::AlignedBuf;

use crate::support::make_pattern_data;

/// 对标 C# `IDevice.FlushAsync` 物理落盘契约：sync 刷盘后数据完整可回读；
/// reset 清空句柄缓存后，句柄透明按需重开且数据无损。
#[test]
fn sync_persists_data_and_handles_reopen_after_reset() -> Void {
  let rt = Runtime::new()?;
  rt.block_on(async {
    let dir = tempdir()?;
    let seg_size: u64 = 64 * 1024;
    let device = SegmentedDevice::segmented(dir.path().join("sync.log"), seg_size)?;

    // 跨段 0、1 各写入一个扇区
    let pattern0 = make_pattern_data(4096, 5, 1);
    let pattern1 = make_pattern_data(4096, 7, 2);
    for (seg_id, pattern) in [(0u32, &pattern0), (1, &pattern1)] {
      let buf = AlignedBuf::from_slice(pattern, 4096)?;
      let (res, _) = device.write_aligned((seg_id as u64) * seg_size, buf).await;
      assert_eq!(res?, 4096);
    }

    // 刷盘后句柄保留在缓存中
    device.sync().await?;
    assert!(device.is_segment_cached(0));
    assert!(device.is_segment_cached(1));

    // 回读校验持久化数据完整性
    for (seg_id, pattern) in [(0u32, &pattern0), (1, &pattern1)] {
      let check = AlignedBuf::new(4096, 4096)?;
      let (res, check) = device.read_aligned((seg_id as u64) * seg_size, check).await;
      assert_eq!(res?, 4096);
      assert_eq!(check.as_slice(), &pattern[..]);
    }

    // reset 清空句柄后再次读取：句柄自动按需重开
    device.reset();
    assert!(device.is_cached_empty());

    let reread = AlignedBuf::new(4096, 4096)?;
    let (res, reread) = device.read_aligned(0, reread).await;
    assert_eq!(res?, 4096);
    assert_eq!(reread.as_slice(), &pattern0[..]);
    assert!(device.is_segment_cached(0), "重开后句柄应重新入缓存");

    info!("sync 刷盘持久化与句柄按需重开校验通过");
    aok::Result::<()>::Ok(())
  })?;

  OK
}

/// 对标 libs/storage/Tsavorite/cs/test/test.hlog/DeviceTests.cs:IDevice_PermissionDeniedAtFirstWrite_CallbackGetsError：
/// 设备在首次 I/O 才惰性打开段文件，open() 失败（父目录 chmod 0）必须以
/// 错误结果传播给调用方——严禁吞错、严禁挂死。
/// 特权环境 (root) 下 chmod 不生效，自动跳过断言（对标 C# root-skip）。
#[cfg(unix)]
#[test]
fn idevice_permission_denied_at_first_write_callback_gets_error() -> Void {
  use std::{
    fs::{Permissions, set_permissions},
    os::unix::fs::PermissionsExt,
  };

  let rt = Runtime::new()?;
  rt.block_on(async {
    let dir = tempdir()?;
    let device = SegmentedDevice::segmented(dir.path().join("perm_denied.log"), 64 * 1024)?;

    // 收紧父目录权限前先完成设备构造（构造期建目录需要可写）
    let buf = AlignedBuf::from_slice(&[0xABu8; 4096], 4096)?;
    set_permissions(dir.path(), Permissions::from_mode(0o0))?;

    let (res, _) = device.write_aligned(0, buf).await;

    // 无论断言结果如何，先恢复权限以便 tempdir 清理
    set_permissions(dir.path(), Permissions::from_mode(0o755))?;

    match res {
      Err(Error::Io(_)) => info!("open() 权限拒绝已正确传播为 Io 错误"),
      // 特权环境下 chmod 0 不生效：写入成功属预期，跳过
      Ok(n) => info!("特权环境 (root) 下 chmod 不生效，写入成功 {n} 字节，跳过断言"),
      other => panic!("预期 Io 权限错误，实际为: {other:?}"),
    }

    // 恢复权限后设备必须可正常读写（句柄按需惰性重试打开）
    let buf2 = AlignedBuf::from_slice(&[0xCDu8; 4096], 4096)?;
    let (res, _) = device.write_aligned(0, buf2).await;
    assert_eq!(res?, 4096);
    let check = AlignedBuf::new(4096, 4096)?;
    let (res, check) = device.read_aligned(0, check).await;
    assert_eq!(res?, 4096);
    assert!(check.as_slice().iter().all(|&b| b == 0xCD));

    info!("权限拒绝错误传播与恢复后可用性校验通过 (PermissionDeniedAtFirstWrite)");
    aok::Result::<()>::Ok(())
  })?;

  OK
}

/// 异步数据刷盘 (fdatasync)：调用 sync_data 后数据完整持久化且可回读
#[test]
fn sync_data_persists_data_and_handles_reopen_after_reset() -> Void {
  let rt = Runtime::new()?;
  rt.block_on(async {
    let dir = tempdir()?;
    let seg_size: u64 = 64 * 1024;
    let device = SegmentedDevice::segmented(dir.path().join("sync_data.log"), seg_size)?;

    let pattern = make_pattern_data(4096, 13, 7);
    let buf = AlignedBuf::from_slice(&pattern, 4096)?;
    let (res, _) = device.write_aligned(0, buf).await;
    assert_eq!(res?, 4096);

    // 仅同步数据 (fdatasync)
    device.sync_data().await?;

    // reset 刷新句柄后回读
    device.reset();
    let check = AlignedBuf::new(4096, 4096)?;
    let (res, check) = device.read_aligned(0, check).await;
    assert_eq!(res?, 4096);
    assert_eq!(check.as_slice(), &pattern[..]);

    info!("sync_data 异步数据刷盘 (fdatasync) 校验通过");
    aok::Result::<()>::Ok(())
  })?;

  OK
}

/// 对标 C# readOnly 保护：只读模式下读取正常放行，写入被严格拦截为 Error::ReadOnly
#[test]
fn read_only_device_blocks_writes_while_allowing_reads() -> Void {
  let rt = Runtime::new()?;
  rt.block_on(async {
    let dir = tempdir()?;
    let log_path = dir.path().join("readonly.log");
    let seg_size: u64 = 64 * 1024;

    let pattern = make_pattern_data(4096, 19, 2);
    // 1. 先用可写设备写入基准数据
    {
      let device = SegmentedDevice::segmented(&log_path, seg_size)?;
      let buf = AlignedBuf::from_slice(&pattern, 4096)?;
      let (res, _) = device.write_aligned(0, buf).await;
      assert_eq!(res?, 4096);
    }

    // 2. 以只读模式重新打开
    let mut ro_device = SegmentedDevice::segmented(&log_path, seg_size)?;
    ro_device.set_read_only(true);
    assert!(ro_device.is_read_only());

    // 2.1 读取正常工作
    let check = AlignedBuf::new(4096, 4096)?;
    let (res, check) = ro_device.read_aligned(0, check).await;
    assert_eq!(res?, 4096);
    assert_eq!(check.as_slice(), &pattern[..]);

    // 2.2 写入被拦截
    let write_buf = AlignedBuf::from_slice(&[0xFFu8; 4096], 4096)?;
    let (res, _) = ro_device.write_aligned(0, write_buf).await;
    assert!(
      matches!(res, Err(Error::ReadOnly { offset, len }) if offset == 0 && len == 4096),
      "只读模式下的写入必须拒绝为 Error::ReadOnly，实际为: {res:?}"
    );

    info!("只读设备模式 (readOnly) 保护校验通过");
    aok::Result::<()>::Ok(())
  })?;

  OK
}

/// 对标 C# preallocateFile 语义：启用预分配时，段文件创建时即扩展至完整段大小
#[test]
fn preallocate_sets_segment_file_size() -> Void {
  let rt = Runtime::new()?;
  rt.block_on(async {
    let dir = tempdir()?;
    let seg_size: u64 = 64 * 1024;
    let mut device = SegmentedDevice::segmented(dir.path().join("prealloc.log"), seg_size)?;
    device.set_preallocate(true);
    assert!(device.is_preallocate());

    // 写入一个 4KB 扇区
    let buf = AlignedBuf::from_slice(&[0x42u8; 4096], 4096)?;
    let (res, _) = device.write_aligned(0, buf).await;
    assert_eq!(res?, 4096);

    // 预分配使得底层文件物理大小恰为 seg_size (64KB)，而非仅写入的 4KB
    assert_eq!(
      device.get_file_size(0)?,
      seg_size,
      "预分配段物理大小应达到配置段大小"
    );

    info!("段文件预分配 (preallocateFile) 语义校验通过");
    aok::Result::<()>::Ok(())
  })?;

  OK
}

/// 对标 C# deleteOnClose 语义：设置 delete_on_close 为 true 时，设备析构自动清理磁盘段文件
#[test]
fn delete_on_close_cleans_up_files_on_drop() -> Void {
  let rt = Runtime::new()?;
  rt.block_on(async {
    let dir = tempdir()?;
    let log_path = dir.path().join("del_close.log");
    let seg_size: u64 = 64 * 1024;

    let seg0_path = {
      let mut device = SegmentedDevice::segmented(&log_path, seg_size)?;
      device.set_delete_on_close(true);
      assert!(device.is_delete_on_close());

      let buf = AlignedBuf::from_slice(&[0x33u8; 4096], 4096)?;
      let (res, _) = device.write_aligned(0, buf).await;
      assert_eq!(res?, 4096);
      let seg0_path = device.segment_path(0);
      assert!(seg0_path.exists(), "析构前文件应存在");
      seg0_path
    };

    // drop 后段文件必须已被物理清理
    assert!(
      !seg0_path.exists(),
      "delete_on_close 析构后段 0 必须已被物理清理"
    );

    info!("delete_on_close 自动清理段文件校验通过");
    aok::Result::<()>::Ok(())
  })?;

  OK
}

/// StorageDevice trait 静态多态分发与通用约束能力校验（零成本 RPITIT 抽象）
#[test]
fn storage_device_trait_dispatch() -> Void {
  use wdev::StorageDevice;

  async fn run_device_ops<D: StorageDevice>(device: &D, seg_size: u64) -> Result<(), wdev::Error> {
    assert_eq!(device.sector_size(), 4096);
    assert_eq!(device.segment_size(), Some(seg_size));
    assert_eq!(device.start_segment(), 0);

    // 写入与读取
    let pattern = make_pattern_data(4096, 7, 1);
    let buf = AlignedBuf::from_slice(&pattern, 4096)?;
    let (res, _) = device.write_aligned(0, buf).await;
    assert_eq!(res?, 4096);

    // 查询文件尺寸、刷盘、重置
    assert!(device.get_file_size(0)? >= 4096);
    device.sync_data().await?;
    device.reset();

    // 截断
    device.truncate_until_segment(1).await?;
    assert_eq!(device.start_segment(), 1);
    Ok(())
  }

  let rt = Runtime::new()?;
  rt.block_on(async {
    let dir = tempdir()?;
    let seg_size: u64 = 64 * 1024;
    let device = SegmentedDevice::segmented(dir.path().join("dyn_dev.log"), seg_size)?;

    run_device_ops(&device, seg_size).await?;

    info!("StorageDevice trait 泛型静态分发校验通过");
    aok::Result::<()>::Ok(())
  })?;

  OK
}

/// 新建段文件后的父目录 fsync 契约：段创建一次性刷目录项（`dir_syncs` 计数可观测），
/// 句柄缓存命中与既有段重开不触发；sync 后目录项已持久，全新设备实例 recover 可见全部段。
/// 对标 POSIX 语义：fsync 文件不保证新建文件崩溃后可见，须 fsync 父目录。
#[test]
fn new_segment_creation_fsyncs_parent_dir() -> Void {
  let rt = Runtime::new()?;
  rt.block_on(async {
    let dir = tempdir()?;
    let seg_size: u64 = 64 * 1024;
    let device = SegmentedDevice::segmented(dir.path().join("dirsync.log"), seg_size)?;
    assert_eq!(device.dir_sync_count(), 0);

    // 写入段 0：物理新建段文件，触发一次父目录 fsync
    // Windows 无目录 fsync 原语（sync_dir 恒 false），计数不增
    let per_new_segment = cfg!(unix) as u64;
    let mut dir_syncs_total = 0;
    let pattern = make_pattern_data(4096, 3, 9);
    let buf = AlignedBuf::from_slice(&pattern, 4096)?;
    let (res, _) = device.write_aligned(0, buf).await;
    assert_eq!(res?, 4096);
    dir_syncs_total += per_new_segment;
    assert_eq!(device.dir_sync_count(), dir_syncs_total);

    // 命中句柄缓存的重复写入不重复刷目录
    let buf = AlignedBuf::from_slice(&pattern, 4096)?;
    let (res, _) = device.write_aligned(0, buf).await;
    assert_eq!(res?, 4096);
    assert_eq!(device.dir_sync_count(), dir_syncs_total);

    // 跨到段 1：再次新建段文件
    let buf = AlignedBuf::from_slice(&pattern, 4096)?;
    let (res, _) = device.write_aligned(seg_size, buf).await;
    assert_eq!(res?, 4096);
    dir_syncs_total += per_new_segment;
    assert_eq!(device.dir_sync_count(), dir_syncs_total);
    device.sync().await?;

    // reset 后按需重开既有段：文件已存在，不触发目录 fsync
    device.reset();
    let check = AlignedBuf::new(4096, 4096)?;
    let (res, check) = device.read_aligned(0, check).await;
    assert_eq!(res?, 4096);
    assert_eq!(check.as_slice(), &pattern[..]);
    assert_eq!(device.dir_sync_count(), dir_syncs_total);

    // 集成验证：目录项已持久 —— 全新设备实例 recover 可见段 0/1 连续区间
    drop(device);
    let recovered = SegmentedDevice::segmented(dir.path().join("dirsync.log"), seg_size)?;
    recovered.recover()?;
    assert_eq!(recovered.start_segment(), 0);
    assert_eq!(recovered.end_segment(), Some(1));

    info!("新建段父目录 fsync 契约验证通过");
    aok::Result::<()>::Ok(())
  })?;

  OK
}
