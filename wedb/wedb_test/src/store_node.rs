//! 独立 db 存储节点单源（无 wal 装配面）
//!
//! 收口 migrate / 集群槽位校验系测试逐字同形的 open_store 装配段：临时
//! 目录 + SegmentedDevice::single_file + 小预算测试配置，仅 db 文件无 wal
//! （open_node 面向 db + wal 双件节点，语义不动）。仅消费 db 存储的册
//! 直挂本文件，避免不消费册招 per-binary dead_code（replica_host /
//! ckpt_node 先例）：
//!

use std::sync::Arc;

use wdev::SegmentedDevice;
use wkv::WedbStore;
use wtest_base::test_store_config;

/// 独立 db 存储节点（db 文件 + 临时目录守卫）
pub struct StoreNode {
  /// 临时目录守卫（Drop 即清理落盘文件）
  pub _dir: tempfile::TempDir,
  /// 存储引擎实例
  pub store: Arc<WedbStore<SegmentedDevice>>,
}

/// 开一套临时目录内的独立 db 存储实例（测试配置；数据文件名由调用方给定，
/// 目录随 [`StoreNode`] 存活，Drop 自动清理）
pub fn open_store(file: &str) -> StoreNode {
  let dir = tempfile::tempdir().unwrap();
  let device = Arc::new(SegmentedDevice::single_file(dir.path().join(file)).unwrap());
  let store = Arc::new(WedbStore::open(test_store_config(), device).unwrap());
  StoreNode { _dir: dir, store }
}
