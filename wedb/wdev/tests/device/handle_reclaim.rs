//! 跨线程句柄失效广播：他线程截断/整表失效对本线程 TLS 句柄表的即时驱逐。
//!
//! 对标 C# `garnet/libs/storage/Tsavorite/cs/src/core/Device/LocalStorageDevice.cs:37`
//! 的进程级共享句柄表（`SafeConcurrentDictionary<int, SafeFileHandle>`）与其全局失效
//! 口径：`RemoveSegment` :354-358 `TryRemove` + `Dispose`、`Reset` :168-176 整段
//! `TryRemove`——一处失效即全线程生效，fd 当场关闭、被删段空间当场回收。
//!
//! Rust 侧 `compio::fs::File` 实测 `!Send`（链路 `File → AsyncFd → Attacher →
//! SharedFd → Rc<Inner<File>>`），句柄不可跨线程迁移或由他线程代释放，等价语义由
//! 设备失效戳广播 + 各线程访问时就地对账承载。本域用例即钉住该对位语义：
//! 断言「A 线程截断后 B 线程句柄即失效」，并复验不再存在指向已解除链接 inode 的 fd。
//!
//! 观测口为 `SegmentedDevice::debug_local_segments`（仅 debug 构建，门禁
//! `./test.sh` 取 dev profile）；无该口的用例（`remove_segment` 一支）以纯行为口径
//! 断言，release 构建同样生效。
//!
//! 自研依据: 设备句柄回收（分段句柄生命周期，本仓组件）

use std::{
  sync::{Arc, mpsc},
  thread,
};

use aok::{OK, Void};
use compio::runtime::Runtime;
use log::info;
use tempfile::tempdir;
use wbase::pool::AlignedBuf;
use wdev::{Device, Error, SegmentedDevice};

use crate::support::Watchdog;

const SECTOR: usize = 4096;
const SEG_SIZE: u64 = 64 * 1024;

/// 段号 `seg` 首扇区的区分模式字节
fn pattern_of(seg: u32) -> u8 {
  ((seg * 17 + 1) & 0xFF) as u8
}

/// 进程内在册的、路径落在 `dir` 下的段文件 fd 计数 `(在册总数, 指向已解除链接 inode 数)`
///
/// fd 表属进程级（`files_struct`），任意线程读同一口径，故主线程可直接复核他线程
/// 句柄是否真的关闭。Linux 走 `/proc/self/fd`（已 unlink 而仍被持有的 fd 以
/// ` (deleted)` 后缀暴露，正是"fd 与磁盘空间延迟回收"的直接证据）；其余平台无该
/// 入口，由本线程句柄表断言承载同等证明
#[cfg(target_os = "linux")]
fn count_dir_fds(dir: &std::path::Path) -> (usize, usize) {
  let mut total = 0usize;
  let mut unlinked = 0usize;
  for entry in std::fs::read_dir("/proc/self/fd").expect("读取 /proc/self/fd 失败") {
    let link = match std::fs::read_link(entry.expect("/proc/self/fd 项读取失败").path()) {
      Ok(l) => l,
      // 采集瞬间已关闭的 fd，跳过（不影响在册集合判定）
      Err(_) => continue,
    };
    let raw = link.to_string_lossy();
    let (target, deleted) = match raw.strip_suffix(" (deleted)") {
      Some(t) => (t, true),
      None => (raw.as_ref(), false),
    };
    let path = std::path::Path::new(target);
    if path.parent() != Some(dir) {
      continue;
    }
    total += 1;
    if deleted {
      unlinked += 1;
    }
  }
  (total, unlinked)
}

/// 他线程截断即驱逐本线程陈旧句柄：A 线程写入段 0..5（5 个句柄在册），B 线程
/// `truncate_until_segment(3)` 物理删除段 0..2，A 线程随后仅访问一个**存活**段
/// （命中路径），即须关闭 0、1、2 的陈旧句柄——表内仅剩 3、4，且存活段句柄零误伤
/// （数据完好、fd 未增）、指向已删段 inode 的 fd 归零。
#[test]
#[cfg(debug_assertions)]
fn foreign_thread_truncate_evicts_stale_handles() -> Void {
  let dir = tempdir()?;
  let path = dir.path().join("xch_trunc.log");
  let device = Arc::new(SegmentedDevice::new(&path, SEG_SIZE, SECTOR)?);
  let _wd = Watchdog::start(60);

  let (ready_tx, ready_rx) = mpsc::channel::<Vec<u32>>();
  let (go_tx, go_rx) = mpsc::channel::<()>();
  let (done_tx, done_rx) = mpsc::channel::<Vec<u32>>();

  // 线程 A：持有段 0..5 的句柄，等 B 截断后再访问存活段
  let dev_a = Arc::clone(&device);
  let ta = thread::spawn(move || -> Void {
    let rt = Runtime::new()?;
    rt.block_on(async move {
      for seg in 0..5u32 {
        let buf = AlignedBuf::from_slice(&[pattern_of(seg); SECTOR], SECTOR)?;
        let (res, _) = dev_a.write_aligned(u64::from(seg) * SEG_SIZE, buf).await;
        assert_eq!(res?, SECTOR);
      }
      ready_tx
        .send(dev_a.debug_local_segments())
        .expect("A 上报初始句柄表失败");

      go_rx
        .recv()
        .expect("A 未收到截断完成的放行信号（发送端已提前释放）");

      // 仅访问存活段 4（命中路径）：他线程的截断广播须在此点生效
      let check = AlignedBuf::new(SECTOR, SECTOR)?;
      let (res, check) = dev_a.read_aligned(4 * SEG_SIZE, check).await;
      assert_eq!(res?, SECTOR, "存活段 4 的读取不应失败");
      assert!(
        check.as_slice().iter().all(|&b| b == pattern_of(4)),
        "存活段 4 数据必须完好（命中句柄未被误伤重开）"
      );
      done_tx
        .send(dev_a.debug_local_segments())
        .expect("A 上报对账后句柄表失败");
      aok::Result::<()>::Ok(())
    })?;
    OK
  });

  assert_eq!(
    ready_rx.recv()?,
    vec![0, 1, 2, 3, 4],
    "A 线程写入后 5 个段句柄应全部在册"
  );
  #[cfg(target_os = "linux")]
  {
    let (total, unlinked) = count_dir_fds(dir.path());
    assert_eq!(
      (total, unlinked),
      (5, 0),
      "A 线程初始应在册 5 个段文件 fd，且无已解除链接的 inode"
    );
  }

  // 线程 B：独立 Runtime 执行常规截断
  let dev_b = Arc::clone(&device);
  let tb = thread::spawn(move || -> Void {
    let rt = Runtime::new()?;
    rt.block_on(async move { dev_b.truncate_until_segment(3).await })?;
    OK
  });
  tb.join().unwrap()?;
  assert_eq!(device.start_segment(), 3);

  #[cfg(target_os = "linux")]
  {
    // B 已物理删除段 0..2，但 A 尚未访问句柄表——此刻仍持有 3 个指向已解除链接
    // 段 inode 的 fd（正是本票要消灭的泄漏形态），须等 A 侧对账关闭
    let (total, unlinked) = count_dir_fds(dir.path());
    assert_eq!(
      unlinked, 3,
      "截断后、A 对账前，A 线程的 3 个陈旧句柄应指向已解除链接的 inode"
    );
    assert_eq!(total, 5);
  }

  go_tx.send(()).expect("放行 A 失败");
  assert_eq!(
    done_rx.recv()?,
    vec![3, 4],
    "B 线程截断后 A 线程句柄即失效：表内只应剩存活段 3、4"
  );

  #[cfg(target_os = "linux")]
  {
    let (total, unlinked) = count_dir_fds(dir.path());
    assert_eq!(
      unlinked, 0,
      "A 线程不得残留任何指向已删段 inode 的 fd（磁盘空间延迟回收）"
    );
    assert_eq!(total, 2, "A 线程在册 fd 须随陈旧句柄驱逐降至存活段数 2");
  }

  // 被截断段在 A 线程同样被访问防御拦截（无幽灵重建）
  ta.join().unwrap()?;
  for seg in 0..3u32 {
    assert!(
      !device.segment_path(seg).exists(),
      "段 {seg} 必须已物理删除且未被复活"
    );
    assert_eq!(device.get_file_size(seg)?, 0);
  }

  info!("他线程截断即驱逐本线程陈旧句柄、fd 与已删段空间当场回收校验通过");
  OK
}

/// 他线程 `reset` 整表失效广播：A 线程持有段 0、1 句柄，B 线程 `reset`（对标 C#
/// 对进程级共享表的全量 `TryRemove`）后，A 线程下次访问（此处新开段 2）须弃用全部
/// 旧句柄——表内仅剩段 2，而非段 0、1、2 三句柄并存。
#[test]
#[cfg(debug_assertions)]
fn foreign_thread_reset_invalidates_all_local_handles() -> Void {
  let dir = tempdir()?;
  let path = dir.path().join("xch_reset.log");
  let device = Arc::new(SegmentedDevice::new(&path, SEG_SIZE, SECTOR)?);
  let _wd = Watchdog::start(60);

  let (ready_tx, ready_rx) = mpsc::channel::<Vec<u32>>();
  let (go_tx, go_rx) = mpsc::channel::<()>();
  let (done_tx, done_rx) = mpsc::channel::<Vec<u32>>();

  let dev_a = Arc::clone(&device);
  let ta = thread::spawn(move || -> Void {
    let rt = Runtime::new()?;
    rt.block_on(async move {
      for seg in 0..2u32 {
        let buf = AlignedBuf::from_slice(&[pattern_of(seg); SECTOR], SECTOR)?;
        let (res, _) = dev_a.write_aligned(u64::from(seg) * SEG_SIZE, buf).await;
        assert_eq!(res?, SECTOR);
      }
      ready_tx
        .send(dev_a.debug_local_segments())
        .expect("A 上报初始句柄表失败");
      go_rx
        .recv()
        .expect("A 未收到 reset 完成的放行信号（发送端已提前释放）");

      // 新开段 2：慢路径对账须同时关闭被 reset 弃用的段 0、1 句柄
      let buf = AlignedBuf::from_slice(&[pattern_of(2); SECTOR], SECTOR)?;
      let (res, _) = dev_a.write_aligned(2 * SEG_SIZE, buf).await;
      assert_eq!(res?, SECTOR);
      done_tx
        .send(dev_a.debug_local_segments())
        .expect("A 上报对账后句柄表失败");
      aok::Result::<()>::Ok(())
    })?;
    OK
  });

  assert_eq!(ready_rx.recv()?, vec![0, 1], "A 线程初始应在册段 0、1");
  device.reset();
  go_tx.send(()).expect("放行 A 失败");
  assert_eq!(
    done_rx.recv()?,
    vec![2],
    "他线程 reset 后本线程旧句柄须整表失效，只应剩新开段 2"
  );

  // reset 只弃句柄不丢数据：段 0、1 仍可按需重开读回原字节
  let rt = Runtime::new()?;
  let dev_r = Arc::clone(&device);
  rt.block_on(async move {
    for seg in 0..2u32 {
      let check = AlignedBuf::new(SECTOR, SECTOR)?;
      let (res, check) = dev_r.read_aligned(u64::from(seg) * SEG_SIZE, check).await;
      assert_eq!(res?, SECTOR, "段 {seg} 重开读取应成功");
      assert!(
        check.as_slice().iter().all(|&b| b == pattern_of(seg)),
        "段 {seg} 数据须完好（关闭 fd 不丢内核脏页）"
      );
    }
    aok::Result::<()>::Ok(())
  })?;

  ta.join().unwrap()?;
  info!("他线程 reset 整表失效广播与本线程按需重开校验通过");
  OK
}

/// 他线程 `remove_segment` 后，本线程陈旧句柄不得继续承接 I/O（纯行为口径，
/// release 构建同样有效）：A 线程写段 1 后句柄在册，B 线程显式删段 1（段 1 仍
/// 在 `start_segment` 之后，截断线无从表达该失效，只有整表世代广播能覆盖），
/// A 线程再读段 1 必须看到"已删段"的状态——读路径以 `create=false` 打开，缺失段
/// 直接返回 `SegmentNotFound`，绝不从已解除链接的旧 inode 里读回陈旧数据、也绝不
/// 重建幽灵空段（对齐 recover 的幽灵段防御）。
#[test]
fn foreign_thread_remove_segment_prevents_stale_io() -> Void {
  let dir = tempdir()?;
  let path = dir.path().join("xch_remove.log");
  let device = Arc::new(SegmentedDevice::new(&path, SEG_SIZE, SECTOR)?);
  let _wd = Watchdog::start(60);

  let (ready_tx, ready_rx) = mpsc::channel::<()>();
  let (go_tx, go_rx) = mpsc::channel::<()>();
  let (done_tx, done_rx) = mpsc::channel::<usize>();

  let dev_a = Arc::clone(&device);
  let ta = thread::spawn(move || -> Void {
    let rt = Runtime::new()?;
    rt.block_on(async move {
      let buf = AlignedBuf::from_slice(&[0x7Cu8; SECTOR], SECTOR)?;
      let (res, _) = dev_a.write_aligned(SEG_SIZE, buf).await;
      assert_eq!(res?, SECTOR);
      ready_tx.send(()).expect("A 上报写入完成失败");
      go_rx
        .recv()
        .expect("A 未收到删段完成的放行信号（发送端已提前释放）");

      let check = AlignedBuf::new(SECTOR, SECTOR)?;
      let (res, _check) = dev_a.read_aligned(SEG_SIZE, check).await;
      // 期望：缺失段被拦截返回 SegmentNotFound（usize::MAX 哨兵）；任何读到的字节数
      // ——含重建幽灵空段的 0 或读回旧 inode 的 4096——均属违约
      let outcome = match res {
        Err(Error::SegmentNotFound(_)) => usize::MAX,
        Ok(n) => n,
        Err(e) => panic!("删段后读段 1 必须返回 SegmentNotFound，实际 {e:?}"),
      };
      done_tx.send(outcome).expect("A 上报读取结果失败");
      aok::Result::<()>::Ok(())
    })?;
    OK
  });

  ready_rx.recv()?;
  let rt = Runtime::new()?;
  let dev_b = Arc::clone(&device);
  rt.block_on(async move { dev_b.remove_segment(1).await })?;
  assert!(!device.segment_path(1).exists(), "段 1 文件必须已物理删除");
  go_tx.send(()).expect("放行 A 失败");

  assert_eq!(
    done_rx.recv()?,
    usize::MAX,
    "他线程删段后本线程陈旧句柄即失效：读缺失段必须返回 SegmentNotFound，\
     绝不从已解除链接的 inode 读回 4096 字节旧数据、也绝不重建幽灵空段"
  );
  ta.join().unwrap()?;

  info!("他线程删段后本线程陈旧句柄不再承接 I/O 校验通过");
  OK
}
