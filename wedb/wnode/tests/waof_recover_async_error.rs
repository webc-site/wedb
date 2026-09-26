//! 设备面恢复吞错回归（对标 C# TsavoriteLog.cs:623 RecoverAsync ValueTask
//! 透明上抛；C# 消费端 FailOnRecoveryError 门控生效默认关续行——rust 侧该
//! 旗标零代码消费，恢复失败全链 `?` 恒上抛、装配口恒拒启，系刻意收紧非缺省
//! 漏配，见 deviations.md §122）：
//! WaofSublog::recover_async 曾把 WalLog::recover 的 Err 记日志后 return ()，
//! 脏位点下毫无察觉续跑 replay；现全链 `?` 上抛，装配口快速失败拒启。

use std::sync::Arc;

use compio::runtime::Runtime;
use tempfile::tempdir;
use waof::{WalConfig, WalLog};
use wbase::align::DEFAULT_SECTOR_SIZE;
use wdev::SegmentedDevice;
use wnode::aof::waof_sublog::WaofSublog;

/// 段超限即恢复期硬错（对标 C# ValidateRecoveredSegments）：
///
/// SegmentedDevice::recover 在段文件大小超过配置段大小时返回
/// SegmentSizeMismatch（wdev::Error 经 waof::Error::Device 透明上浮）。
/// 该错不属 EOF/截断容错范畴，WalLog::recover 以 `?`/显式 Err 上抛——
///
/// WaofSublog::recover_async 必须原样返回 Err（修复前吞错返回 Ok，位点停留
/// 初值却谎称恢复成功）。
#[test]
fn recover_async_surfaces_device_recovery_error() {
  let rt = Runtime::new().expect("compio runtime");
  rt.block_on(async {
    let dir = tempdir().expect("tempdir");
    let base = dir.path().join("swallowed.wal");
    // 健康几何写一段提交数据（落盘，恢复有料可扫）
    let seg_size = 64 * 1024u64;
    let device =
      Arc::new(SegmentedDevice::new(&base, seg_size, DEFAULT_SECTOR_SIZE).expect("device"));
    let wal = Arc::new(WalLog::new(Arc::clone(&device), WalConfig::default()).expect("WalLog"));
    let sublog = WaofSublog::new(Arc::clone(&wal));
    sublog.enqueue(&vec![b'x'; 8192]).expect("enqueue");
    wal.commit().await.expect("commit");
    drop(wal);
    drop(device);

    // 窄几何重开同一目录：已落盘段文件 > 配置段大小 → device.recover
    // 报 SegmentSizeMismatch（确定性硬错，无需 mock 故障设备）
    let narrow =
      Arc::new(SegmentedDevice::new(&base, 4096, DEFAULT_SECTOR_SIZE).expect("narrow device"));
    let wal = Arc::new(WalLog::new(narrow, WalConfig::default()).expect("WalLog"));
    let sublog = WaofSublog::new(wal);
    let err = sublog
      .recover_async()
      .await
      .expect_err("设备面恢复硬错必须上抛，禁吞错返回 Ok");
    let msg = format!("{err:?}");
    assert!(
      msg.contains("SegmentSizeMismatch") || msg.contains("段"),
      "须携带段校验失败身份透传设备错，实得: {msg}"
    );
  });
}
