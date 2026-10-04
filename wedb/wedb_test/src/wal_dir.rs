//! 副本 wal 构造单源（临时目录内单文件 wal）
//!
//! 收口 replica_driver_store_generation / replica_recover_clamp_partial_resync /
//! replica_replay_truncate_clamp 三册逐字同形的 wal 落盘构造（tempdir +
//! SegmentedDevice 单文件 + WalLog 默认配置）。消费面经 `wedb_test::wal_dir`
//! 引用（原 common/ 直挂面已收口进本 crate）。
//!

use std::{path::Path, sync::Arc};

use waof::{WalConfig, WalLog};
use wdev::SegmentedDevice;

/// 在既存目录内开一套 wal（`name` 为日志文件名，各册原值保留）
pub fn wal_in_dir(dir: &Path, name: &str) -> Arc<WalLog<SegmentedDevice>> {
  let device = Arc::new(SegmentedDevice::single_file(dir.join(name)).expect("device"));
  Arc::new(WalLog::new(device, WalConfig::default()).expect("wal"))
}
