//! 集群会话消费者单源（自建临时库播种 provider 形，r314 同形收口；
//! 装配形态同 cluster_resp_session 的 gate 库）
//!
//! 装配尾单源见 `wedb_test::cluster_consumer_with`。`options` 承接 max_databases 等
//! 各册差异面（默认形态传 `RespServerSessionOptions::default()`）。

use std::sync::Arc;

use wdev::SegmentedDevice;
use wedb::server::{cluster::IClusterProvider, cluster_provider::ClusterProvider};
use wkv::WedbStore;
use wnode::{RespSessionConsumer, resp::resp_server_session::RespServerSessionOptions};
use wtest_base::test_store_config;

use crate::cluster_consumer_with::cluster_consumer_with;

/// 构造挂接集群切面 + 存储执行域的会话消费者：新开临时库（db 文件名由
/// 调用方指定）并播种到 provider
pub fn cluster_consumer_fresh(
  cp: &ClusterProvider,
  db_name: &str,
  options: RespServerSessionOptions,
) -> RespSessionConsumer {
  let cluster_session = cp.create_cluster_session();
  let dir = tempfile::tempdir().unwrap().keep();
  let device = Arc::new(SegmentedDevice::single_file(dir.join(db_name)).unwrap());
  let store = Arc::new(WedbStore::open(test_store_config(), device).unwrap());
  cp.set_store(Arc::clone(&store));
  cluster_consumer_with(cluster_session, cp.provider_handle(), &store, options)
}
