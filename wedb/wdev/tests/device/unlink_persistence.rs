//! 删段 unlink 的父目录屏障与复活段幂等再收（删除形持久化收口）。
//!
//! C# 行为锚：删段链（LocalStorageDevice.RemoveSegment 的 TryRemove + Dispose +
//! File.Delete 尽力删除，StorageDeviceBase.TruncateUntilSegmentAsync 的编排）依赖
//! NTFS 卷元数据自动持久语义，全程无目录屏障；本组锁的是 rust 自研「持久化发布
//! 双屏障口径」的删除形收口（`wdev/src/lib.rs:sync_dir` 单点原语），非对 C# 契约
//! 分叉的对齐测试。掉电不可模拟按仓内惯例，判别用例以权限注入替代：父目录降为
//! wx（0o300）时 unlink 照常成功而屏障的 `open(dir, O_RDONLY)` 另需读权限即
//! EACCES，把「unlink 之后、返回之前」钉成确定性失败点，证明屏障真实位于删除
//! 调用链且失败透明上抛；复活面以重建已删段文件模拟掉电目录项回魂，锁 recover
//! 前缀空隙吸收臂的水位回退形态与下一轮截断的幂等再收，并对照真中部空洞
//! fail-fast 零变化。
//!
//! 在 garnet 中的相对路径: libs/storage/Tsavorite/cs/src/core/Device/LocalStorageDevice.cs（RemoveSegment）

use std::fs::{self, File};
#[cfg(unix)]
use std::{io, path::Path};

use aok::{OK, Void};
use compio::runtime::Runtime;
use log::info;
use tempfile::tempdir;
use wbase::{align::DEFAULT_SECTOR_SIZE, pool::AlignedBuf};
use wdev::{Device, Error, SegmentedDevice};

/// 调整目录权限位（Unix 判别注入专用）
#[cfg(unix)]
fn chmod(dir: &Path, mode: u32) -> io::Result<()> {
  use std::os::unix::fs::PermissionsExt;
  let mut perms = fs::metadata(dir)?.permissions();
  perms.set_mode(mode);
  fs::set_permissions(dir, perms)
}

/// 删段目录屏障调用链断言：`remove_segment` 的 unlink 成功后必须真实经过父目录
/// fsync 屏障（先删后刷），且屏障失败自本口透明上抛——权限注入把屏障钉成确定性
/// EACCES：unlink 只需父目录 w+x，屏障的 `open(dir, O_RDONLY)` 另需 r。修复前
/// unlink 成功即返回 Ok，本用例在权限语义生效的环境必红。
#[cfg(unix)]
#[test]
fn remove_segment_dir_barrier_failure_propagates_after_unlink() -> Void {
  let rt = Runtime::new()?;
  rt.block_on(async {
    let dir = tempdir()?;
    let seg_size: u64 = 64 * 1024;
    let device = SegmentedDevice::new(
      dir.path().join("rm_barrier.log"),
      seg_size,
      DEFAULT_SECTOR_SIZE,
    )?;

    // 段 0、1 各写一个扇区并 sync
    for seg_id in 0..2u32 {
      let buf = AlignedBuf::from_slice(&[(seg_id * 19 + 3) as u8; 4096], 4096)?;
      let (res, _) = device.write_aligned((seg_id as u64) * seg_size, buf).await;
      assert_eq!(res?, 4096);
    }
    device.sync().await?;

    // 注入：父目录降为 wx，段文件本身权限不动（独立 fd 打开/set_len 不受影响）
    chmod(dir.path(), 0o300)?;
    // root（CAP_DAC_OVERRIDE）下权限注入失效，按环境实测分流断言
    let barrier_denied = File::open(dir.path()).is_err();
    let res = device.remove_segment(0).await;
    if barrier_denied {
      assert!(
        matches!(res, Err(Error::Io(_))),
        "目录屏障失败必须自 remove_segment 透明上抛（位于 unlink 之后、返回之前），实际为 {res:?}"
      );
    } else {
      assert!(
        res.is_ok(),
        "权限注入失效环境（root），链路退化为常规成功: {res:?}"
      );
    }
    // unlink 已物理生效：失败确系其后的目录屏障，而非删除本身
    assert!(
      !device.segment_path(0).exists(),
      "段 0 必须已物理 unlink（错误来自其后的目录屏障）"
    );

    // 撤除注入后相同段号重试：NotFound 吞臂幂等收口，不再触碰目录项
    chmod(dir.path(), 0o700)?;
    device.remove_segment(0).await?;
    assert!(!device.segment_path(0).exists());

    // 正常路径完整过屏障：段 1 删除成功且目录项消失
    device.remove_segment(1).await?;
    assert!(
      !device.segment_path(1).exists(),
      "段 1 删除后目录项必须消失"
    );

    info!("删段目录屏障调用链与失败上抛校验通过 (remove_segment dir barrier)");
    aok::Result::<()>::Ok(())
  })?;

  OK
}

/// 复活段幂等再收回归：已截段文件目录项掉电复活（以重建文件模拟）后，recover
/// 前缀空隙吸收臂把复活段吸收重建 start_segment（恢复水位回退至复活段起点的
/// 危害链形态），下一轮 truncate_until_segment 必须把复活段重新物理回收（占盘
/// 可再收、幂等）；真中部空洞仍 fail-fast 报 SegmentGap 零变化。
#[test]
fn resurrected_truncated_segment_is_reabsorbed_and_recollected() -> Void {
  let rt = Runtime::new()?;
  rt.block_on(async {
    let dir = tempdir()?;
    let seg_size: u64 = 64 * 1024;
    let db_path = dir.path().join("resurrect.log");

    // 段 0..=5 各写一个扇区并 sync
    {
      let device = SegmentedDevice::new(&db_path, seg_size, DEFAULT_SECTOR_SIZE)?;
      for seg_id in 0..=5u32 {
        let buf = AlignedBuf::from_slice(&[(seg_id * 17 + 1) as u8; 4096], 4096)?;
        let (res, _) = device.write_aligned((seg_id as u64) * seg_size, buf).await;
        assert_eq!(res?, 4096);
      }
      device.sync().await?;
    }

    // 常规截断至段 2：段 0、1 unlink 并经目录屏障收口，父目录无残留目录项
    {
      let device = SegmentedDevice::new(&db_path, seg_size, DEFAULT_SECTOR_SIZE)?;
      device.truncate_until_segment(2).await?;
      for seg_id in 0..2u32 {
        assert!(
          !device.segment_path(seg_id).exists(),
          "截断后段 {seg_id} 目录项必须已消失"
        );
      }
    }

    // 模拟掉电复活：已 unlink 的段 1 目录项回魂（重建空文件形态）
    {
      let device = SegmentedDevice::new(&db_path, seg_size, DEFAULT_SECTOR_SIZE)?;
      File::create(device.segment_path(1))?;
    }

    // 重开恢复：前缀空隙吸收臂把复活段吸收，start_segment 回退至复活段起点
    let device = SegmentedDevice::new(&db_path, seg_size, DEFAULT_SECTOR_SIZE)?;
    device.recover()?;
    assert_eq!(
      device.start_segment(),
      1,
      "复活段经前缀空隙吸收臂重建水位：start_segment 回退至复活段起点"
    );
    assert_eq!(device.end_segment(), Some(5));

    // 幂等再收：下一轮截断把复活段重新物理回收，占盘面收敛
    device.truncate_until_segment(2).await?;
    assert!(
      !device.segment_path(1).exists(),
      "复活段必须被下一轮截断幂等再收"
    );
    assert_eq!(device.start_segment(), 2);
    for seg_id in 2..=5u32 {
      assert!(
        device.segment_path(seg_id).exists(),
        "存活段 {seg_id} 不得被误收"
      );
    }

    // 对照：真中部空洞（缺段 4）fail-fast 零变化，复活吸收仅限前缀空隙
    fs::remove_file(device.segment_path(4))?;
    let device2 = SegmentedDevice::new(&db_path, seg_size, DEFAULT_SECTOR_SIZE)?;
    let recovered = device2.recover();
    assert!(
      matches!(recovered, Err(Error::SegmentGap { gap: 4 })),
      "真中部空洞必须 fail-fast 报 SegmentGap {{ gap: 4 }}，实际为 {recovered:?}"
    );

    info!("复活段前缀吸收与下一轮截断幂等再收校验通过 (resurrection re-collection)");
    aok::Result::<()>::Ok(())
  })?;

  OK
}
