//! crate 内单测支撑：轻量真实段设备子日志装配（aof 域单测共享；
//! 集成测试对应物在 wnode_test——其依赖本 crate，无法反向引用）

use std::sync::Arc;

use waof::{WalConfig, WalLog};
use wdev::SegmentedDevice;

use super::waof_sublog::{AofSublog, WaofSublog};

/// 轻量真实段设备子日志：tempfile + `SegmentedDevice` 单文件 + `WalLog`
/// 默认配置（测试统一走真实设备，杜绝 mock 抽象）
pub fn test_sublog(tag: &str) -> (tempfile::TempDir, Arc<AofSublog>) {
  test_sublog_with_config(tag, WalConfig::default())
}

/// 指定 [`WalConfig`] 变体：分块多帧场景（小页大窗——值超页载荷即拆多帧，
/// 帧组总预留仍完整落入环形窗口）
pub fn test_sublog_with_config(
  tag: &str,
  config: WalConfig,
) -> (tempfile::TempDir, Arc<AofSublog>) {
  let dir = tempfile::tempdir().expect("tempdir");
  let device = Arc::new(
    SegmentedDevice::single_file(dir.path().join(format!("{tag}.wal"))).expect("SegmentedDevice"),
  );
  let wal = WalLog::new(device, config).expect("WalLog");
  (dir, Arc::new(WaofSublog::new(Arc::new(wal))))
}

/// 轻量真实段设备后端集（分片拓扑参数化；TempDir 即弃——设备句柄持
/// 已打开 fd，unix 语义下文件随句柄存活至测试结束）
pub fn test_backends(tag: &str, count: usize) -> Vec<Arc<AofSublog>> {
  (0..count)
    .map(|i| test_sublog(&format!("{tag}_{i}")).1)
    .collect()
}

/// 指定 [`WalConfig`] 的后端集变体（分块多帧场景装配）
pub fn test_backends_with_config(
  tag: &str,
  count: usize,
  config: WalConfig,
) -> Vec<Arc<AofSublog>> {
  (0..count)
    .map(|i| test_sublog_with_config(&format!("{tag}_{i}"), config).1)
    .collect()
}
