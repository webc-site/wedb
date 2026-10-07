//! 副本域 fixture 底座单源（默认裸壳拓扑 + 临时目录单文件 wal）
//!
//! 收口 replica_driver_store_generation / replica_recover_clamp_partial_resync /
//! replica_replay_truncate_clamp 三册逐字同形的 setup 头：裸壳 provider 拓扑
//! 预置（见 replica_topology 单源，仍留 common/）+ 临时目录内 "replica.wal"
//!（wal 单源已收口至 `wedb_test::wal_dir`）。依赖宿主册在场挂载
//! `replica_topology_core`（super:: 路径引用，直挂与聚合挂载两态同解）。
//! 宿主册直挂（沿用 primary_assets 先例）：
//!
//! ```text
//! #[path = "common/replica_wal_fixture.rs"]
//! mod replica_wal_fixture;
//! use replica_wal_fixture::replica_wal_fixture;
//! ```

use std::sync::Arc;

use waof::WalLog;
use wdev::SegmentedDevice;
use wedb::server::{
  cluster_provider::ClusterProvider, replication::replication_manager::ReplicationManager,
};
use wedb_test::wal_dir::wal_in_dir;

use super::replica_topology_core::replica_topology_with;

/// 装配副本域 fixture 底座：返回（provider, rm, 临时目录守卫, wal）——
/// 目录由调用方持有至用例末
pub fn replica_wal_fixture(
  replica_id: u128,
  primary_id: u128,
) -> (
  Arc<ClusterProvider>,
  Arc<ReplicationManager>,
  tempfile::TempDir,
  Arc<WalLog<SegmentedDevice>>,
) {
  let (provider, rm) = replica_topology_with(
    Arc::new(ClusterProvider::default()),
    replica_id,
    primary_id,
    None,
  );
  let dir = tempfile::tempdir().expect("tempdir");
  let wal = wal_in_dir(dir.path(), "replica.wal");
  (provider, rm, dir, wal)
}
