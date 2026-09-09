//! sync 持久化契约：全局 sync 覆盖他线程写入、跨线程 fsync 可行性实验、
//! 删段/截断免责与守护位图清空。
//!
//! 对标 C# `LocalStorageDevice`：句柄表进程级共享（`SafeConcurrentDictionary<int,
//! SafeFileHandle>` 按段键控）、任意线程可对全设备 sync。Rust 侧可行性依赖
//! `compio-driver/sync` feature（SharedFd 为 `Arc`，fd 无线程亲和，fsync 按 inode 全量生效）。
//!
//! 所有跨线程场景均挂看门狗（超时强制退出进程），防运行时互等挂死。

use std::{
  fs,
  sync::{Arc, mpsc},
  thread,
};

use aok::{OK, Void};
use compio::{buf::BufResult, fs::File, io::AsyncWriteAt, runtime::Runtime};
use log::info;
use tempfile::tempdir;
use wdev::{Device, SegmentedDevice};
use wram::AlignedBuf;

use crate::support::Watchdog;

const SECTOR: usize = 4096;
const SEG_SIZE: u64 = 64 * 1024;

/// 跨线程 fsync 可行性实验（转正为回归用例）：线程 A 打开 fd 并写入，线程 B 在
/// 自己的独立 Runtime 上对同一 fd 执行 sync_all / sync_data。fd 属进程级
/// files_struct，fsync 无线程亲和——验证 compio 管线（提交/收割）跨线程不报错。
#[test]
fn raw_cross_thread_fsync_feasibility() -> Void {
  let dir = tempdir()?;
  let path = dir.path().join("raw_ct_fsync.bin");
  let _wd = Watchdog::start(60);

  let path_a = path.clone();
  let (tx, rx) = mpsc::channel::<Arc<File>>();

  // 线程 A：打开文件并写入 4096 字节，句柄存活移交线程 B
  let ta = thread::spawn(move || -> Void {
    let rt = Runtime::new()?;
    rt.block_on(async {
      let file = Arc::new(File::create(&path_a).await?);
      let data: Vec<u8> = (0..SECTOR).map(|j| (j & 0xFF) as u8).collect();
      let buf = AlignedBuf::from_slice(&data, 4096)?;
      // compio 定位写实现于 &File，经共享引用提交
      let BufResult(res, _) = (&*file).write_at(buf, 0).await;
      assert_eq!(res?, SECTOR);
      tx.send(file).expect("接收端存活");
      aok::Result::<()>::Ok(())
    })?;
    OK
  });

  // 线程 B：独立 Runtime，对线程 A 打开的 fd 执行全量与数据级 fsync
  let tb = thread::spawn(move || -> Void {
    let file = rx.recv().expect("发送端存活");
    let rt = Runtime::new()?;
    rt.block_on(async {
      file.sync_all().await?;
      file.sync_data().await?;
      aok::Result::<()>::Ok(())
    })?;
    OK
  });

  ta.join().unwrap()?;
  tb.join().unwrap()?;

  // 常规 std 读路径复验数据完整
  let disk = fs::read(&path)?;
  assert_eq!(disk.len(), SECTOR);
  assert_eq!(disk[0], 0);
  assert_eq!(disk[SECTOR - 1], ((SECTOR - 1) & 0xFF) as u8);

  info!("跨线程 fsync 可行性实验通过：他线程 fd 的 sync_all/sync_data 全部成功");
  OK
}

/// 全局 sync 覆盖他线程写入：线程 A 写段 0 与段 1 后退出（不再 sync），线程 B
/// 调用 `sync()` 必须覆盖 A 的全部在表句柄——守护位图清空（debug 构建），且
/// 全新设备实例（全新句柄）能读回全部字节，证明数据已过 fsync 落盘。
#[test]
fn global_sync_covers_foreign_thread_writes() -> Void {
  let dir = tempdir()?;
  let path = dir.path().join("global_sync.log");
  let device = Arc::new(SegmentedDevice::segmented(&path, SEG_SIZE)?);
  let _wd = Watchdog::start(60);

  // 线程 A：写段 0 首块与段 1 首块（跨段覆盖守护窗口内两个段），写完即退出
  let dev_a = Arc::clone(&device);
  let ta = thread::spawn(move || -> Void {
    let rt = Runtime::new()?;
    rt.block_on(async move {
      for seg in 0u32..2 {
        let data: Vec<u8> = (0..SECTOR).map(|j| (j ^ seg as usize) as u8).collect();
        let wbuf = AlignedBuf::from_slice(&data, 4096)?;
        let offset = u64::from(seg) * SEG_SIZE;
        let (res, _) = dev_a.write_aligned(offset, wbuf).await;
        assert_eq!(res?, SECTOR);
      }
      aok::Result::<()>::Ok(())
    })?;
    OK
  });
  ta.join().unwrap()?;

  // 写入已记账、尚未 sync：守护位图应含段 0 与段 1
  #[cfg(debug_assertions)]
  assert_eq!(device.debug_dirty_segments(), vec![0, 1]);

  // 线程 B：独立 Runtime，全局 sync（覆盖线程 A 的句柄）+ 全新设备实例读回复验
  let dev_b = Arc::clone(&device);
  let tb = thread::spawn(move || -> Void {
    let rt = Runtime::new()?;
    rt.block_on(async move {
      dev_b.sync().await?;

      // 全新设备实例（全新句柄）读回：字节已在文件上
      let fresh = SegmentedDevice::segmented(&path, SEG_SIZE)?;
      for seg in 0u32..2 {
        let expected: Vec<u8> = (0..SECTOR).map(|j| (j ^ seg as usize) as u8).collect();
        let check = AlignedBuf::new(SECTOR, 4096)?;
        let (res, check) = fresh.read_aligned(u64::from(seg) * SEG_SIZE, check).await;
        assert_eq!(res?, SECTOR);
        assert_eq!(check.as_slice(), &expected[..], "段 {seg} 落盘字节不匹配");
      }
      aok::Result::<()>::Ok(())
    })?;
    OK
  });
  tb.join().unwrap()?;

  // sync 覆盖后守护位图清空（他线程写入被全局 sync 背书）
  #[cfg(debug_assertions)]
  assert!(
    device.debug_dirty_segments().is_empty(),
    "全局 sync 后仍有在册脏段"
  );

  info!("全局 sync 覆盖他线程写入并通过全新句柄读回复验");
  OK
}

/// 全局 sync_data 与 sync 同覆盖口径：线程 A 写入后退出，线程 B 仅 fdatasync，
/// 守护位图同样必须清空（数据级 fsync 即构成持久化背书）。
#[test]
fn global_sync_data_covers_foreign_thread_writes() -> Void {
  let dir = tempdir()?;
  let path = dir.path().join("global_sync_data.log");
  let device = Arc::new(SegmentedDevice::segmented(&path, SEG_SIZE)?);
  let _wd = Watchdog::start(60);

  let dev_a = Arc::clone(&device);
  let ta = thread::spawn(move || -> Void {
    let rt = Runtime::new()?;
    rt.block_on(async move {
      let data: Vec<u8> = (0..SECTOR).map(|j| (j | 0x80) as u8).collect();
      let wbuf = AlignedBuf::from_slice(&data, 4096)?;
      let (res, _) = dev_a.write_aligned(0, wbuf).await;
      assert_eq!(res?, SECTOR);
      aok::Result::<()>::Ok(())
    })?;
    OK
  });
  ta.join().unwrap()?;

  let dev_b = Arc::clone(&device);
  let tb = thread::spawn(move || -> Void {
    let rt = Runtime::new()?;
    rt.block_on(async move { dev_b.sync_data().await })?;
    OK
  });
  tb.join().unwrap()?;

  #[cfg(debug_assertions)]
  assert!(device.debug_dirty_segments().is_empty());

  info!("全局 sync_data 与 sync 同覆盖口径");
  OK
}

/// 删段与截断免责：写入后显式 `remove_segment` / `truncate_until_segment` 的段
/// 无须 fsync 背书，随后的全局 sync 不得触发守护违约断言，位图清空。
#[test]
fn sync_contract_remove_and_truncate_immunity() -> Void {
  let dir = tempdir()?;
  let path = dir.path().join("immunity.log");
  let device = Arc::new(SegmentedDevice::segmented(&path, SEG_SIZE)?);
  let _wd = Watchdog::start(60);

  let rt = Runtime::new()?;
  rt.block_on(async {
    // 写段 0 与段 1
    for seg in 0u32..2 {
      let wbuf = AlignedBuf::from_slice(&vec![0xA5; SECTOR], 4096)?;
      let (res, _) = device.write_aligned(u64::from(seg) * SEG_SIZE, wbuf).await;
      assert_eq!(res?, SECTOR);
    }
    #[cfg(debug_assertions)]
    assert_eq!(device.debug_dirty_segments(), vec![0, 1]);

    // 显式删段 0：免责清位
    device.remove_segment(0).await?;
    #[cfg(debug_assertions)]
    assert_eq!(device.debug_dirty_segments(), vec![1]);

    // 截断至段 1：仅删除段 0 之前的段，段 1 仍有效，脏位保持待 sync 背书
    device.truncate_until_segment(1).await?;
    #[cfg(debug_assertions)]
    assert_eq!(device.debug_dirty_segments(), vec![1]);

    // 免责后 sync：不得触发守护违约断言
    device.sync().await?;

    // 免责段物理消失，start_segment 单调推进
    assert_eq!(device.start_segment(), 1);
    assert_eq!(device.get_file_size(0)?, 0);

    aok::Result::<()>::Ok(())
  })?;

  info!("删段与截断免责路径通过，sync 无守护违约");
  OK
}

/// 多段并发 sync 与 sync_data 压榨：跨多个段并发写入后执行全局并发刷盘，验证快速路径与数据落盘
#[test]
fn multi_segment_concurrent_sync() -> Void {
  let dir = tempdir()?;
  let path = dir.path().join("multi_seg_concurrent_sync.log");
  let device = Arc::new(SegmentedDevice::segmented(&path, SEG_SIZE)?);
  let _wd = Watchdog::start(60);

  let rt = Runtime::new()?;
  rt.block_on(async {
    // 1. 快速路径 1：空段直接返回 Ok
    device.sync().await?;
    device.sync_data().await?;

    // 2. 快速路径 2：单段场景
    let data0 = vec![0x42u8; SECTOR];
    let wbuf0 = AlignedBuf::from_slice(&data0, 4096)?;
    let (res, _) = device.write_aligned(0, wbuf0).await;
    assert_eq!(res?, SECTOR);
    device.sync().await?;
    #[cfg(debug_assertions)]
    assert!(device.debug_dirty_segments().is_empty());

    // 3. 多段并发场景（8段并发下发）
    const SEGS: u32 = 8;
    for seg in 0..SEGS {
      let data = vec![(seg & 0xFF) as u8; SECTOR];
      let wbuf = AlignedBuf::from_slice(&data, 4096)?;
      let (res, _) = device.write_aligned(u64::from(seg) * SEG_SIZE, wbuf).await;
      assert_eq!(res?, SECTOR);
    }

    #[cfg(debug_assertions)]
    assert_eq!(device.debug_dirty_segments().len(), SEGS as usize);

    // 并发 sync_data 与 sync 全部段
    device.sync_data().await?;
    device.sync().await?;

    #[cfg(debug_assertions)]
    assert!(device.debug_dirty_segments().is_empty());

    // 零分配验证所有段数据落盘
    for seg in 0..SEGS {
      let check = AlignedBuf::new(SECTOR, 4096)?;
      let (res, check) = device.read_aligned(u64::from(seg) * SEG_SIZE, check).await;
      assert_eq!(res?, SECTOR);
      let expected_byte = (seg & 0xFF) as u8;
      assert!(
        check.as_slice().iter().all(|&b| b == expected_byte),
        "段 {seg} 落盘数据不匹配"
      );
    }

    aok::Result::<()>::Ok(())
  })?;

  info!("多段并发 sync / sync_data 压榨测试通过");
  OK
}
