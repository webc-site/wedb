//! 无盘全量同步发起单源（推流资产装配 + meta 预置 + try_begin 收口）
//!
//! 收口 diskless 系逐字同形的同步发起段：`primary_assets` 推流资产装配、
//! 零位点 meta（见 sync_meta_seed 单源）、`try_begin_diskless_sync_async`
//! 发起。断言文案差异面（expect 文案 / 裸 unwrap）以 `expect_msg` 参数保留。
//! 宿主册直挂（沿用 primary_assets 先例）：
//!
//! ```text
//! #[path = "common/diskless_sync_kick.rs"]
//! mod diskless_sync_kick;
//! ```
//!
//! 推流资产与 meta 单源已收口至 `wedb_test::{primary_assets, sync_meta_seed}`；
//! 存储底座见 `wedb_test::node_storage`。

use std::sync::Arc;

use waof::AofAddress;
use wedb::server::{
  cluster_provider::{ClusterProvider, PrimaryReplicationAssets},
  replication::{
    replica_diskless_sync::try_begin_diskless_sync_async, replication_manager::ReplicationManager,
  },
};
use wedb_test::{
  node_storage::NodeStorage, primary_assets::primary_assets, sync_meta_seed::full_resync_meta,
};

/// 主端发起无盘全量同步（FullResync：推流资产 + 零位点 meta + 发起收口）；
/// 返回（授予位点, 推流资产）——资产供增量段复用同泵
///
/// `expect_msg`：Some = 带文案 expect（断言文案各册原样保留）；None = 裸
/// unwrap（原 unwrap 形册零漂移）
pub async fn try_full_sync(
  provider_p: &Arc<ClusterProvider>,
  source: &NodeStorage,
  replica_addr: &str,
  primary_id: u128,
  replica_id: u128,
  rm_r: &Arc<ReplicationManager>,
  expect_msg: Option<&str>,
) -> (AofAddress, PrimaryReplicationAssets) {
  let rm_p = provider_p.replication_manager().unwrap();
  let assets = primary_assets(source, &rm_p);
  let meta = full_resync_meta(replica_id, rm_r);
  let granted =
    try_begin_diskless_sync_async(provider_p, &assets, primary_id, replica_addr, &meta).await;
  let granted = match expect_msg {
    Some(msg) => granted.expect(msg),
    None => granted.unwrap(),
  };
  (granted, assets)
}
