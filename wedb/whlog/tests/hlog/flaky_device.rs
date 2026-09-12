//! 扫描页读故障注入回归（对标 libs/storage/Tsavorite/cs/test/test.hlog/FlakyDeviceTests.cs，
//! 上游 d20d63993 新增用例的语义移植）
//!
//! C# 侧 ScanIteratorBase.BufferAndLoad 的失败原子性缺陷（frame 预占后同步读失败
//! 导致 CAS 死循环 / 等待者永久挂起 / 异常逃逸进无关线程 drain pass）在 Rust compio
//! 调用方驱动模型中结构性不存在（论证见 whlog/src/scan.rs「页读失败原子性」一节），
//! 本组用例锁定其等价行为契约：
//! - 设备页读失败必须以 Err 传播给扫描调用方，绝不静默返回旧页数据；
//! - 失败后迭代器保持可复用：设备恢复后同一迭代器继续扫描且有前进、有穷终止；
//! - 交付的每条记录必须唯一、有序、自洽（页读失败不得以其他页的字节冒充）；
//! - 记录丢失绝不静默：任何丢失必须伴随已向调用方暴露的错误。
//!
//! 上游同批新增而 Rust 无对应物的用例（缺口说明）：
//! - ScanIteratorEpochFailureTests：C# 页读经 BumpCurrentEpoch(Action) 延迟执行才有
//!   「epoch 抛在动作注册前/后」的时序分叉与 frame 预占账目；Rust 读不依赖纪元延迟
//!   执行，无被测对象；
//! - LogFastCommitTests.FastCommitRecoverToMissingCommitNumThrows：whlog 未移植
//!   TsavoriteLog 的 fast-commit 提交协议（commit num / RecoverAsync），无被测对象；
//! - FlakyDeviceTests 的元数据三例（MetadataReadFailureIsReportedRatherThanReturningGarbage
//!   / MetadataReadToleratesEndOfFile / ZeroLengthCommitMetadataIsRejected）：Rust 无
//!   DeviceLogCommitCheckpointManager 对应物——wcpr 以整文件 JSON + 完整性封签承载
//!   检查点元数据，读取走 compio `fs::read`（错误传播 + 精确长度缓冲），无「池化
//!   缓冲脏数据 / 长度前缀截断被当元数据解析」的通道。

use std::{path::Path, sync::Arc};

use aok::{OK, Void};
use compio::runtime::Runtime;
use tempfile::tempdir;
use wdev::SegmentedDevice;
use wepoch::LightEpoch;
use whlog::{HybridLog, HybridLogConfig, SECTOR_ALIGNMENT};

use super::support::SyncThrowDevice;

type FlakyLog = Arc<HybridLog<SyncThrowDevice>>;

/// 搭建全冷扫描场景：追加 `entry_count` 条值为 `val_len` 字节的小记录（键 `k:{序号}` 自识别；
/// `unique_payload` 为真时值前 4 字节额外携带记录序号），全页落盘并驱逐出内存，
/// 返回 (hlog, device, tail)
async fn setup_cold_log(
  dir: &Path,
  entry_count: u32,
  val_len: usize,
  unique_payload: bool,
) -> aok::Result<(FlakyLog, Arc<SyncThrowDevice>, u64)> {
  let device = Arc::new(SyncThrowDevice::new(SegmentedDevice::single_file(dir)?));
  let epoch = Arc::new(LightEpoch::new(16));
  let config = HybridLogConfig::new(SECTOR_ALIGNMENT, 16, 0.5)?;
  let hlog = Arc::new(HybridLog::new(config, device.clone(), epoch)?);

  for i in 0..entry_count {
    let key = format!("k:{i:06}");
    let mut val = vec![0u8; val_len];
    if unique_payload {
      val[..4].copy_from_slice(&i.to_le_bytes());
    }
    hlog.append(key.as_bytes(), &val, 0, false)?;
  }

  let tail = hlog.tail_address();
  // 全部页落盘并驱逐，迫使扫描走设备冷读路径（同 disk_read_cache.rs 的驱逐序列）
  let last_page = hlog.config.page_id(tail - 1);
  for p in 0..=last_page {
    hlog.flush_page(p).await?;
  }
  hlog.shift_read_only_address(tail);
  hlog.shift_head_address(tail);
  Ok((hlog, device, tail))
}

/// 设备页读失败必须向扫描调用方显式报错；设备恢复后同一迭代器可复用、
/// 有前进且有穷终止，全新扫描逐条内容一致
///
/// libs/storage/Tsavorite/cs/test/test.hlog/FlakyDeviceTests.cs:
/// ScanTerminatesWhenPageReadThrowsSynchronously（C# 另断言失败不逃逸进无关线程的
/// epoch drain pass——Rust 读不经纪元延迟执行，无该通道，无需对应断言）
#[test]
fn scan_surfaces_error_and_resumes_when_device_read_fails() -> Void {
  let rt = Runtime::new()?;
  rt.block_on(async {
    let dir = tempdir()?;
    let (hlog, device, tail) =
      setup_cold_log(&dir.path().join("flaky_scan.db"), 1000, 4, false).await?;
    // 对标 C#：TailAddress > 1<<14，保证扫描覆盖内存窗口之外的磁盘区
    assert!(tail > 1 << 14, "扫描必须覆盖磁盘冷读区: tail={tail}");

    // 1. 武装读失败：扫描首读即失败，错误必须传播给调用方
    //    （对标 C# 首轮 GetNext 抛 OperationCanceledException 的失败可见性语义）
    device.set_arm_read_failure(true);
    let mut iter = hlog.scan_iter(0, tail);
    let err = iter.next_ref(|_| Ok(())).await.unwrap_err();
    assert!(
      matches!(err, whlog::Error::Device(wdev::Error::Io(_))),
      "页读失败必须显式传播: {err:?}"
    );

    // 2. 解除注入：同一迭代器继续扫描必须有前进且有穷终止
    //    （对标 C# entriesReadAfterRecovery > 0 与 scanDone 限时完成）
    device.set_arm_read_failure(false);
    let mut resumed = 0usize;
    while let Some(()) = iter.next_ref(|_| Ok(())).await? {
      resumed += 1;
    }
    assert_eq!(resumed, 1000, "设备恢复后同一迭代器必须完整交付全部记录");

    // 3. 全新健康扫描回验：逐条键一致、严格有序
    let mut seen = 0u32;
    let mut iter = hlog.scan_iter(0, tail);
    while let Some(key) = iter.next_ref(|item| Ok(item.rec.key().to_vec())).await? {
      assert_eq!(key, format!("k:{seen:06}").into_bytes(), "记录必须按序交付");
      seen += 1;
    }
    assert_eq!(seen, 1000, "健康扫描必须交付全部记录");

    OK
  })
}

/// 页读失败时扫描交付流必须严格有序、唯一、自洽；注入必须生效、失败恰好可见一次、
/// 记录零丢失
///
/// libs/storage/Tsavorite/cs/test/test.hlog/FlakyDeviceTests.cs:
/// ScanDoesNotReturnStaleDataWhenReadAheadPageFails（C# 依赖双页预取的 read-ahead
/// frame 失败后回填陈旧字节的窗口；Rust 单页预取无 read-ahead，序数注入改为命中
/// 第 k 次页读——失败页在下一次调用重试成功，语义收敛为交付流有序唯一 + 失败
/// 显式暴露 + 零静默丢失）
#[test]
fn scan_delivers_strictly_ordered_records_when_page_read_fails() -> Void {
  let rt = Runtime::new()?;
  rt.block_on(async {
    let entry_count = 500u32;
    for failing_ordinal in 0..4u32 {
      let dir = tempdir()?;
      let (hlog, device, tail) = setup_cold_log(
        &dir.path().join(format!("flaky_order_{failing_ordinal}.db")),
        entry_count,
        24,
        true,
      )
      .await?;

      device.set_throw_on_read_ordinal(failing_ordinal as i64);

      let mut last_index: Option<u32> = None;
      let mut entries_read = 0usize;
      let mut failures = 0usize;
      let mut iter = hlog.scan_iter(0, tail);
      loop {
        match iter
          .next_ref(|item| {
            let v = item.rec.value();
            let idx = u32::from_le_bytes([v[0], v[1], v[2], v[3]]);
            Ok((idx, item.rec.key().to_vec()))
          })
          .await
        {
          Ok(Some((idx, key))) => {
            // 每条交付的记录必须真实、自洽、唯一且有序——页读失败绝不能让
            // 陈旧/他页字节冒充有效记录（对标 C# 的 ordering 断言）
            assert_eq!(key, format!("k:{idx:06}").into_bytes(), "记录必须自洽");
            assert!(
              idx < entry_count && last_index.is_none_or(|prev| idx > prev),
              "交付流必须严格递增且唯一: idx={idx} after {last_index:?}"
            );
            last_index = Some(idx);
            entries_read += 1;
          }
          Ok(None) => break,
          Err(_) => {
            // 注入的失败重试即可恢复：标记可见性后重入当前页
            failures += 1;
          }
        }
      }

      assert!(
        device.read_failure_injected(),
        "第 {failing_ordinal} 次页读的故障注入未生效，本用例断言落空"
      );
      assert_eq!(failures, 1, "单次序数注入必须恰好暴露一次失败");
      assert_eq!(
        entries_read, entry_count as usize,
        "注入页读经重试成功后不得丢失任何记录（对标 C#：丢失必须伴随错误暴露）"
      );
    }
    OK
  })
}
