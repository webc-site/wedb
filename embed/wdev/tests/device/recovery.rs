//! 恢复语义：同尺寸重开、段文件超限检测、小文件放行与段号空隙恢复。
//!
//! 对标 C# 测试文件：
//! `/Users/z/git/db/garnet/libs/storage/Tsavorite/cs/test/test.hlog/DeviceTests.cs`
//! （方法 `NativeStorageDevice_Recovery_MatchingSegmentSize_Succeeds`、
//! `NativeStorageDevice_Recovery_LargerExistingSegment_DetectsMismatch`、
//! `NativeStorageDevice_Recovery_SmallerExistingSegment_Succeeds`）；
//! 段号空隙用例对标 C# 实现 `src/core/Device/LocalStorageDevice.cs` 的
//! `RecoverFiles` 空隙状态机（start_segment 恢复至首个空隙处）。

use std::{
  fs::{remove_file, write},
  path::Path,
};

use aok::{OK, Void};
use compio::runtime::Runtime;
use log::info;
use tempfile::tempdir;
use wbase::AlignedBuf;
use wdev::{Device, Error, SegmentedDevice};

use crate::support::make_pattern_data;

/// 对标 libs/storage/Tsavorite/cs/test/test.hlog/DeviceTests.cs:NativeStorageDevice_Recovery_MatchingSegmentSize_Succeeds：
/// 以相同段尺寸重开设备并恢复元数据后，原有数据完好可读。
#[test]
fn recovery_matching_segment_size_succeeds() -> Void {
  let rt = Runtime::new()?;
  rt.block_on(async {
    let dir = tempdir()?;
    let seg_size: u64 = 64 * 1024;
    let db_path = dir.path().join("recover_match.log");
    let pattern = make_pattern_data(4096, 29, 5);

    // 第一次打开：连续写入段 0..3
    {
      let device = SegmentedDevice::segmented(&db_path, seg_size)?;
      for seg_id in 0..3u32 {
        let buf = if seg_id == 2 {
          AlignedBuf::from_slice(&pattern[..], 4096)?
        } else {
          AlignedBuf::from_slice(&[(seg_id * 11 + 3) as u8; 4096], 4096)?
        };
        let (res, _) = device.write_aligned((seg_id as u64) * seg_size, buf).await;
        assert_eq!(res?, 4096);
      }
      assert_eq!(device.end_segment(), Some(2));
    }

    // 同段尺寸重开并恢复：start/end 段号还原，数据完好
    {
      let device = SegmentedDevice::segmented(&db_path, seg_size)?;
      device.recover()?;
      assert_eq!(
        device.start_segment(),
        0,
        "连续段恢复 start_segment 应保持 0"
      );
      assert_eq!(
        device.end_segment(),
        Some(2),
        "end_segment 应恢复为最大连续段号"
      );

      let check = AlignedBuf::new(4096, 4096)?;
      let (res, check) = device.read_aligned(2 * seg_size, check).await;
      assert_eq!(res?, 4096);
      assert_eq!(check.as_slice(), &pattern[..]);
    }

    info!("同段尺寸重开恢复成功 (Recovery_MatchingSegmentSize_Succeeds)");
    aok::Result::<()>::Ok(())
  })?;

  OK
}

/// 对标 libs/storage/Tsavorite/cs/test/test.hlog/DeviceTests.cs:NativeStorageDevice_Recovery_LargerExistingSegment_DetectsMismatch：
/// 已存在段文件超过新配置段尺寸时，恢复校验必须报 SegmentSizeMismatch，
/// 严禁静默放行（C# 在首次 I/O 报错，Rust 在 recover() 同步返回错误）。
#[test]
fn recovery_larger_existing_segment_detects_mismatch() -> Void {
  let rt = Runtime::new()?;
  rt.block_on(async {
    let dir = tempdir()?;
    let small_seg: u64 = 64 * 1024;
    let big_seg: u64 = 256 * 1024;
    let db_path = dir.path().join("recover_big.log");

    // 以 256KiB 段尺寸写满段 0，使段文件超过新配置的 64KiB
    {
      let device = SegmentedDevice::segmented(&db_path, big_seg)?;
      let buf = AlignedBuf::from_slice(&[0x5Au8; 256 * 1024], 4096)?;
      let (res, _) = device.write_aligned(0, buf).await;
      assert_eq!(res?, 256 * 1024);
    }

    // 以 64KiB 段尺寸重开：恢复校验必须拒绝
    let device = SegmentedDevice::segmented(&db_path, small_seg)?;
    let recovered = device.recover();
    assert!(
      matches!(
        recovered,
        Err(Error::SegmentSizeMismatch {
          segment: 0,
          file_size: 262144,
          segment_size: 65536
        })
      ),
      "已存在段文件超过配置段尺寸必须报 SegmentSizeMismatch，实际为 {recovered:?}"
    );

    info!("超大已存在段文件恢复检测通过 (Recovery_LargerExistingSegment_DetectsMismatch)");
    aok::Result::<()>::Ok(())
  })?;

  OK
}

/// 对标 libs/storage/Tsavorite/cs/test/test.hlog/DeviceTests.cs:NativeStorageDevice_Recovery_SmallerExistingSegment_Succeeds：
/// 已存在段文件不大于新配置段尺寸时恢复放行。
#[test]
fn recovery_smaller_existing_segment_succeeds() -> Void {
  let rt = Runtime::new()?;
  rt.block_on(async {
    let dir = tempdir()?;
    let small_seg: u64 = 64 * 1024;
    let big_seg: u64 = 256 * 1024;
    let db_path = dir.path().join("recover_small.log");

    // 以 64KiB 段尺寸写入 4KB
    {
      let device = SegmentedDevice::segmented(&db_path, small_seg)?;
      let buf = AlignedBuf::from_slice(&[0x3Cu8; 4096], 4096)?;
      let (res, _) = device.write_aligned(0, buf).await;
      assert_eq!(res?, 4096);
    }

    // 以 256KiB 段尺寸重开：小文件放行且 end_segment 恢复
    let device = SegmentedDevice::segmented(&db_path, big_seg)?;
    assert!(
      device.recover().is_ok(),
      "较小段文件应被放行 (SmallerExistingSegment)"
    );
    assert_eq!(device.end_segment(), Some(0));

    info!("较小已存在段文件恢复放行通过 (Recovery_SmallerExistingSegment_Succeeds)");
    aok::Result::<()>::Ok(())
  })?;

  OK
}

/// 对标 C# 实现 `LocalStorageDevice.RecoverFiles`：段号出现空隙处恢复 start_segment，
/// 连续区间恢复 end_segment；重启后访问已删段必须被拦截且绝不幽灵重建段文件。
#[test]
fn recover_files_restores_segment_range_after_gap() -> Void {
  let rt = Runtime::new()?;
  rt.block_on(async {
    let dir = tempdir()?;
    let seg_size: u64 = 64 * 1024;
    let db_path = dir.path().join("recover_gap.log");

    // 写入段 0..3 后，模拟历史截断：重启前物理删除段 0、1
    {
      let device = SegmentedDevice::segmented(&db_path, seg_size)?;
      for seg_id in 0..3u32 {
        let buf = AlignedBuf::from_slice(&[0xA5u8; 4096], 4096)?;
        let (res, _) = device.write_aligned((seg_id as u64) * seg_size, buf).await;
        assert_eq!(res?, 4096);
      }
    }
    let device = SegmentedDevice::segmented(&db_path, seg_size)?;
    remove_file(device.segment_path(0))?;
    remove_file(device.segment_path(1))?;

    // 无关文件不参与段扫描（对标 C# RecoverFiles 跳过 bareName 本名、
    // 拒绝无法解析为段号的后缀）：裸名文件与非数字后缀文件均不干扰恢复
    write(&db_path, b"bare")?;
    let mut txt_path = db_path.clone().into_os_string();
    txt_path.push(".txt");
    write(&txt_path, b"junk")?;

    // 重开恢复：空隙处 start_segment=2，无连续段时 end_segment 为 None
    device.recover()?;
    assert_eq!(device.start_segment(), 2, "空隙处应恢复 start_segment=2");
    assert_eq!(
      device.end_segment(),
      None,
      "无连续段时 end_segment 应为 None"
    );

    // 访问已删除的段 0 必须被拦截，且绝不幽灵重建
    let check = AlignedBuf::new(4096, 4096)?;
    let (res, _) = device.read_aligned(0, check).await;
    assert!(
      matches!(res, Err(Error::SegmentNotFound(0))),
      "重启后访问已删段必须返回 SegmentNotFound，实际为 {res:?}"
    );
    assert!(
      !device.segment_path(0).exists(),
      "严禁幽灵重建已删除的段 0 文件"
    );

    info!("段号空隙恢复状态机校验通过 (LocalStorageDevice.RecoverFiles)");
    aok::Result::<()>::Ok(())
  })?;

  OK
}

/// 段号数值序回归（文件名字典序陷阱）：段号超过 9 后文件名后缀字典序 (.10 < .2)
/// 与数值序不一致，恢复扫描与截断比较必须始终按解析后的数值段号进行；
/// 符号前缀等非纯数字后缀文件（如 `.+7`）严格忽略——绝不误认段号、绝不遭截断误删。
#[test]
fn recovery_segment_numbering_stays_numeric_beyond_two_digits() -> Void {
  let rt = Runtime::new()?;
  rt.block_on(async {
    let dir = tempdir()?;
    let seg_size: u64 = 64 * 1024;
    let db_path = dir.path().join("numeric_order.log");

    // 写入段 0..=12（各段独立模式），并放置符号前缀杂散文件 `.+7`
    {
      let device = SegmentedDevice::segmented(&db_path, seg_size)?;
      for seg_id in 0..=12u32 {
        let pattern = ((seg_id * 13 + 5) & 0xFF) as u8;
        let buf = AlignedBuf::from_slice(&vec![pattern; 4096], 4096)?;
        let (res, _) = device.write_aligned((seg_id as u64) * seg_size, buf).await;
        assert_eq!(res?, 4096);
      }
      let mut plus7 = db_path.clone().into_os_string();
      plus7.push(".+7");
      write(Path::new(&plus7), b"junk")?;
      device.sync().await?;
    }

    let device = SegmentedDevice::segmented(&db_path, seg_size)?;
    // 模拟历史截断：物理删除段 0..=9（保留 10、11、12——字典序下 ".10" 排最前）
    for seg_id in 0..=9u32 {
      remove_file(device.segment_path(seg_id))?;
    }

    // 重开恢复：空隙推导必须按数值序得出 start=10、end=12，
    // 杂散 `.+7` 不得被解析为段 7 而干扰恢复结果
    device.recover()?;
    assert_eq!(
      device.start_segment(),
      10,
      "空隙处应按数值序恢复 start_segment=10，字典序 (.10 < .2) 不得干扰"
    );
    assert_eq!(
      device.end_segment(),
      Some(12),
      "end_segment 应恢复为最大连续段号 12"
    );

    // 已删段 9 访问被拦截且无幽灵重建
    let check = AlignedBuf::new(4096, 4096)?;
    let (res, _) = device.read_aligned(9 * seg_size, check).await;
    assert!(
      matches!(res, Err(Error::SegmentNotFound(9))),
      "段 9 已删除，访问必须返回 SegmentNotFound，实际为 {res:?}"
    );
    assert!(
      !device.segment_path(9).exists(),
      "严禁幽灵重建已删除的段 9 文件"
    );

    // 段 10..=12 数据完好（两位数段号按数值正确寻址）
    for seg_id in 10..=12u32 {
      let expected = ((seg_id * 13 + 5) & 0xFF) as u8;
      let check = AlignedBuf::new(4096, 4096)?;
      let (res, check) = device.read_aligned((seg_id as u64) * seg_size, check).await;
      assert_eq!(res?, 4096);
      assert!(
        check.as_slice().iter().all(|&b| b == expected),
        "段 {seg_id} 数据不匹配"
      );
    }

    // 截断至段 12：两位数段 10、11 按数值比较删除，段 12 保留；
    // 杂散 `.+7` 绝不被误认为段号而遭物理误删
    device.truncate_until_segment(12).await?;
    assert_eq!(device.start_segment(), 12);
    assert!(!device.segment_path(10).exists(), "段 10 必须已物理删除");
    assert!(!device.segment_path(11).exists(), "段 11 必须已物理删除");
    assert!(device.segment_path(12).exists(), "段 12 应保留");
    let mut plus7 = db_path.clone().into_os_string();
    plus7.push(".+7");
    assert!(
      Path::new(&plus7).exists(),
      "符号前缀杂散文件不得被误认成段号而遭截断误删"
    );

    info!("段号数值序恢复与两位数段截断校验通过 (RecoverFiles numeric ordering)");
    aok::Result::<()>::Ok(())
  })?;

  OK
}
