//! sync 持久化契约：全局 sync 覆盖他线程写入、删段/截断免责与守护位图清空。
//!
//! 对标 libs/storage/Tsavorite/cs/test/test.hlog/DeviceTests.cs:LocalStorageDevice：句柄表进程级共享（`SafeConcurrentDictionary<int,
//! SafeFileHandle>` 按段键控）、任意线程可对全设备 sync。Rust 侧可行性依赖
//! `compio-driver/sync` feature（SharedFd 为 `Arc`，fd 无线程亲和，fsync 按 inode 全量生效）。
//!
//! 所有跨线程场景均挂看门狗（超时强制退出进程），防运行时互等挂死。
//!
//! 在 garnet 中的相对路径: libs/storage/Tsavorite/cs/test/test.hlog/DeviceTests.cs（fsync 契约）

use std::{sync::Arc, thread};

use aok::{OK, Void};
use compio::runtime::Runtime;
use log::info;
use tempfile::tempdir;
use wbase::pool::AlignedBuf;
use wdev::{Device, SegmentedDevice};

use crate::support::Watchdog;

const SECTOR: usize = 4096;
const SEG_SIZE: u64 = 64 * 1024;

/// 全局 sync 覆盖他线程写入：线程 A 写段 0 与段 1 后退出（不再 sync），线程 B
/// 调用 `sync()` 必须覆盖 A 的全部在表句柄——守护位图清空（debug 构建），且
/// 全新设备实例（全新句柄）能读回全部字节，证明数据已过 fsync 落盘。
#[test]
fn global_sync_covers_foreign_thread_writes() -> Void {
  let dir = tempdir()?;
  let path = dir.path().join("global_sync.log");
  let device = Arc::new(SegmentedDevice::new(&path, SEG_SIZE, SECTOR)?);
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
      let fresh = SegmentedDevice::new(&path, SEG_SIZE, SECTOR)?;
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
  let device = Arc::new(SegmentedDevice::new(&path, SEG_SIZE, SECTOR)?);
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
  let device = Arc::new(SegmentedDevice::new(&path, SEG_SIZE, SECTOR)?);
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
  let device = Arc::new(SegmentedDevice::new(&path, SEG_SIZE, SECTOR)?);
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

/// sync 补开路径的空洞段防御：写入段 0 与段 2（段 1 从未写入）后执行全局 sync，
/// 空洞段 1 绝不因 sync 被幽灵创建（Rust 补齐语义：C# IDevice 无设备级 sync，
/// 补开仅针对磁盘上已存在的段文件——他线程写入过的段必然已创建文件）；
/// 已写入段数据经 sync 落盘可回读。
#[test]
fn sync_does_not_ghost_create_hole_segments() -> Void {
  let dir = tempdir()?;
  let path = dir.path().join("hole_sync.log");
  let device = Arc::new(SegmentedDevice::new(&path, SEG_SIZE, SECTOR)?);
  let _wd = Watchdog::start(60);

  let rt = Runtime::new()?;
  rt.block_on(async {
    // 写段 0 与段 2，段 1 保持空洞
    for seg in [0u32, 2] {
      let data = vec![(seg * 37 + 3) as u8; SECTOR];
      let wbuf = AlignedBuf::from_slice(&data, 4096)?;
      let (res, _) = device.write_aligned(u64::from(seg) * SEG_SIZE, wbuf).await;
      assert_eq!(res?, SECTOR);
    }
    assert!(
      !device.segment_path(1).exists(),
      "写入阶段空洞段 1 不应存在"
    );

    // 全局 sync：覆盖段 0、2，且绝不为空洞段 1 创建幽灵文件
    device.sync().await?;
    device.sync_data().await?;
    assert!(
      !device.segment_path(1).exists(),
      "sync 绝不能为从未写入的空洞段创建幽灵文件"
    );

    // 已写入段数据落盘完好（全新设备实例复验，排除页缓存直读）
    let fresh = SegmentedDevice::new(&path, SEG_SIZE, SECTOR)?;
    for seg in [0u32, 2] {
      let expected = vec![(seg * 37 + 3) as u8; SECTOR];
      let check = AlignedBuf::new(SECTOR, 4096)?;
      let (res, check) = fresh.read_aligned(u64::from(seg) * SEG_SIZE, check).await;
      assert_eq!(res?, SECTOR);
      assert_eq!(check.as_slice(), &expected[..], "段 {seg} 落盘数据不匹配");
    }

    aok::Result::<()>::Ok(())
  })?;

  info!("sync 空洞段幽灵创建防御与在册段覆盖校验通过");
  OK
}

/// 新建段目录屏障失败时的回滚与报错：
/// 1. 父目录无读权限导致 sync_dir (File::open) 失败，write_aligned 上抛错误；
/// 2. 刚刚新建的段文件被尽力回滚（remove_file），磁盘上无残留段文件；
/// 3. 恢复目录权限后重试写入必须成功且目录屏障补齐，数据落盘且可完整读回；
/// 4. 打开已存在段（is_new_segment 为假）零行为变化，不触发目录屏障。
#[cfg(unix)]
#[test]
fn new_segment_dir_sync_failure_rolls_back_and_surfaces_error() -> Void {
  use std::{
    fs::{Permissions, create_dir, set_permissions},
    os::unix::fs::PermissionsExt,
    path::PathBuf,
  };

  struct PermGuard(PathBuf);
  impl Drop for PermGuard {
    fn drop(&mut self) {
      let _ = set_permissions(&self.0, Permissions::from_mode(0o700));
    }
  }

  let dir = tempdir()?;
  let sub = dir.path().join("sub");
  create_dir(&sub)?;
  let _guard = PermGuard(sub.clone());

  let path = sub.join("dir_sync_fail.log");
  let device = Arc::new(SegmentedDevice::new(&path, SEG_SIZE, SECTOR)?);
  let _wd = Watchdog::start(60);

  let rt = Runtime::new()?;
  rt.block_on(async {
    let seg0_path = device.segment_path(0);
    assert!(!seg0_path.exists(), "初始时段 0 文件不应存在");

    // 设置父目录为 0o300 (-wx------)：
    // 允许创建和删除文件，但 File::open(dir) (O_RDONLY) 报 PermissionDenied
    set_permissions(&sub, Permissions::from_mode(0o300))?;

    let wbuf = AlignedBuf::from_slice(&vec![0x42; SECTOR], 4096)?;
    let (res, _) = device.write_aligned(0, wbuf).await;

    // 1. 目录屏障失败必须响亮上抛
    assert!(res.is_err(), "新建段目录屏障失败必须报错上抛");

    // 2. 验证新建段已被回滚清除，磁盘无残留
    assert!(
      !seg0_path.exists(),
      "目录屏障失败后新建段文件必须被尽力回滚删除，不能留盘"
    );

    // 3. 恢复目录权限 (0o700)，重试写入必须成功且目录屏障补齐
    set_permissions(&sub, Permissions::from_mode(0o700))?;
    let wbuf = AlignedBuf::from_slice(&vec![0x42; SECTOR], 4096)?;
    let (res, _) = device.write_aligned(0, wbuf).await;
    assert_eq!(res?, SECTOR, "权限恢复后重试写入应成功");
    assert!(seg0_path.exists(), "写入成功后段 0 文件必须存在");

    // 验证数据已正确落盘
    device.sync().await?;
    let check = AlignedBuf::new(SECTOR, 4096)?;
    let (res, check) = device.read_aligned(0, check).await;
    assert_eq!(res?, SECTOR);
    assert!(check.as_slice().iter().all(|&b| b == 0x42));

    // 4. 打开已存在段（is_new_segment 为假）零行为变化：
    // reset 丢弃句柄缓存，随后再次只读打开既有段，即使父目录设为 0o300（无读权），
    // 读路径（create=false）也不会调用 sync_dir，正常命中磁盘文件
    device.reset();
    set_permissions(&sub, Permissions::from_mode(0o300))?;
    let check = AlignedBuf::new(SECTOR, 4096)?;
    let (res, check) = device.read_aligned(0, check).await;
    assert_eq!(res?, SECTOR, "已存在段读取不应调用 sync_dir，无行为变化");
    assert!(check.as_slice().iter().all(|&b| b == 0x42));

    // 恢复目录权限以便退出与清理
    set_permissions(&sub, Permissions::from_mode(0o700))?;

    aok::Result::<()>::Ok(())
  })?;

  info!("新建段目录屏障失败回滚与报错、重试及已存在段零影响校验通过");
  OK
}

/// 双任务并发开同段与单侧屏障失败回归用例：
/// 1. 任务 B 成功创建并写入段 0（完成排他创建与目录屏障），段 0 在册存活；
/// 2. 父目录权限翻转为 0o300（sync_dir File::open 必报 PermissionDenied）；
/// 3. 任务 A 并发写入段 0：因段 0 已存在，A create_new 报 AlreadyExists 并转既有段打开路径，
///    is_new_segment 置假，不刷目录屏障、不承担回滚；
/// 4. 核心断言：单侧失败或并发打开绝对不得卸载对侧 B 已登记的段 0 文件；
/// 5. 任务 C 尝试新建段 1：在 0o300 权限下因目录屏障失败触发回滚，段 1 被清除，
///    而段 0 及其数据依旧完好存活（单侧失败不跨段误伤）；
/// 6. 并发开同段压测：两线程同时向未创建的段 2 发起写入，验证排他创建与已存在回退的并发安全性；
/// 7. 恢复权限后验证段 0 与段 2 数据完整性。
#[cfg(unix)]
#[test]
fn concurrent_segment_open_barrier_failure_does_not_unlink_registered_segment() -> Void {
  use std::{
    fs::{Permissions, create_dir, set_permissions},
    os::unix::fs::PermissionsExt,
    path::PathBuf,
  };

  struct PermGuard(PathBuf);
  impl Drop for PermGuard {
    fn drop(&mut self) {
      let _ = set_permissions(&self.0, Permissions::from_mode(0o700));
    }
  }

  let dir = tempdir()?;
  let sub = dir.path().join("sub");
  create_dir(&sub)?;
  let _guard = PermGuard(sub.clone());

  let path = sub.join("concurrent_barrier_race.log");
  let device = Arc::new(SegmentedDevice::new(&path, SEG_SIZE, SECTOR)?);
  let _wd = Watchdog::start(60);

  // 1. 任务 B：正常权限 (0o700) 下创建并写入段 0
  let dev_b = Arc::clone(&device);
  let tb = thread::spawn(move || -> Void {
    let rt = Runtime::new()?;
    rt.block_on(async move {
      let data = vec![0x5Au8; SECTOR];
      let wbuf = AlignedBuf::from_slice(&data, 4096)?;
      let (res, _) = dev_b.write_aligned(0, wbuf).await;
      assert_eq!(res?, SECTOR, "任务 B 写入段 0 必须成功");
      dev_b.sync().await?;
      aok::Result::<()>::Ok(())
    })?;
    OK
  });
  tb.join().unwrap()?;

  let seg0_path = device.segment_path(0);
  assert!(seg0_path.exists(), "任务 B 写入后段 0 文件必须在盘");

  // 2. 注入故障：设置父目录为 0o300 (-wx------)，若触发 sync_dir 则必报 PermissionDenied
  set_permissions(&sub, Permissions::from_mode(0o300))?;

  // 3. 任务 A：在另一独立线程/Runtime 中写入段 0 第二块偏移（触发同段打开路径）
  //    因段 0 文件已由 B 创建，A 的 create_new 报 AlreadyExists，转既有段打开（is_new_segment 为假），
  //    不刷目录屏障、不触发 remove_file 回滚
  let dev_a = Arc::clone(&device);
  let ta = thread::spawn(move || -> Void {
    let rt = Runtime::new()?;
    rt.block_on(async move {
      let data = vec![0x3Cu8; SECTOR];
      let wbuf = AlignedBuf::from_slice(&data, 4096)?;
      let (res, _) = dev_a.write_aligned(SECTOR as u64, wbuf).await;
      assert_eq!(
        res?, SECTOR,
        "任务 A 打开已存在段写入第二块必须成功且不触发目录屏障"
      );
      aok::Result::<()>::Ok(())
    })?;
    OK
  });
  ta.join().unwrap()?;

  // 4. 关键断言：对侧 B 已登记的段 0 文件绝对不得被卸载
  assert!(
    seg0_path.exists(),
    "单侧屏障失败或并发打开绝不能卸载对侧已登记段文件"
  );

  // 5. 任务 C：在 0o300 权限下尝试新建段 1，目录屏障失败触发回滚删除段 1
  let dev_c = Arc::clone(&device);
  let tc = thread::spawn(move || -> Void {
    let rt = Runtime::new()?;
    rt.block_on(async move {
      let data = vec![0x7Eu8; SECTOR];
      let wbuf = AlignedBuf::from_slice(&data, 4096)?;
      let (res, _) = dev_c.write_aligned(SEG_SIZE, wbuf).await;
      assert!(res.is_err(), "新建段 1 目录屏障必须因 0o300 权限失败上抛");
      aok::Result::<()>::Ok(())
    })?;
    OK
  });
  tc.join().unwrap()?;

  let seg1_path = device.segment_path(1);
  assert!(!seg1_path.exists(), "新建段 1 屏障失败后必须被尽力回滚删除");
  // 再次断言段 0 依然存活，未被段 1 的回滚误伤
  assert!(seg0_path.exists(), "单侧失败回滚绝不能误删对侧已登记段 0");

  // 恢复目录权限以便退出与后续验证
  set_permissions(&sub, Permissions::from_mode(0o700))?;

  // 6. 并发开同段压测：两线程同时向未创建的段 2 发起写入，验证排他创建与已存在回退的并发安全性
  let dev_1 = Arc::clone(&device);
  let t1 = thread::spawn(move || -> Void {
    let rt = Runtime::new()?;
    rt.block_on(async move {
      let data = vec![0x11u8; SECTOR];
      let wbuf = AlignedBuf::from_slice(&data, 4096)?;
      let (res, _) = dev_1.write_aligned(2 * SEG_SIZE, wbuf).await;
      assert_eq!(res?, SECTOR);
      aok::Result::<()>::Ok(())
    })?;
    OK
  });
  let dev_2 = Arc::clone(&device);
  let t2 = thread::spawn(move || -> Void {
    let rt = Runtime::new()?;
    rt.block_on(async move {
      let data = vec![0x22u8; SECTOR];
      let wbuf = AlignedBuf::from_slice(&data, 4096)?;
      let (res, _) = dev_2
        .write_aligned(2 * SEG_SIZE + SECTOR as u64, wbuf)
        .await;
      assert_eq!(res?, SECTOR);
      aok::Result::<()>::Ok(())
    })?;
    OK
  });
  t1.join().unwrap()?;
  t2.join().unwrap()?;

  let seg2_path = device.segment_path(2);
  assert!(seg2_path.exists(), "并发开段后段 2 文件必须在盘");

  // 7. 数据完整性复验：通过全新设备实例验证段 0 与段 2 的数据均完好在盘
  let rt = Runtime::new()?;
  rt.block_on(async {
    let fresh = SegmentedDevice::new(&path, SEG_SIZE, SECTOR)?;
    let check_b = AlignedBuf::new(SECTOR, 4096)?;
    let (res_b, check_b) = fresh.read_aligned(0, check_b).await;
    assert_eq!(res_b?, SECTOR);
    assert!(
      check_b.as_slice().iter().all(|&b| b == 0x5A),
      "任务 B 写入数据完整"
    );

    let check_a = AlignedBuf::new(SECTOR, 4096)?;
    let (res_a, check_a) = fresh.read_aligned(SECTOR as u64, check_a).await;
    assert_eq!(res_a?, SECTOR);
    assert!(
      check_a.as_slice().iter().all(|&b| b == 0x3C),
      "任务 A 写入数据完整"
    );

    let check_1 = AlignedBuf::new(SECTOR, 4096)?;
    let (res_1, check_1) = fresh.read_aligned(2 * SEG_SIZE, check_1).await;
    assert_eq!(res_1?, SECTOR);
    assert!(
      check_1.as_slice().iter().all(|&b| b == 0x11),
      "并发任务 1 写入数据完整"
    );

    let check_2 = AlignedBuf::new(SECTOR, 4096)?;
    let (res_2, check_2) = fresh
      .read_aligned(2 * SEG_SIZE + SECTOR as u64, check_2)
      .await;
    assert_eq!(res_2?, SECTOR);
    assert!(
      check_2.as_slice().iter().all(|&b| b == 0x22),
      "并发任务 2 写入数据完整"
    );

    aok::Result::<()>::Ok(())
  })?;

  info!("双任务并发开同段与单侧失败不卸载在册段回归校验通过");
  OK
}
