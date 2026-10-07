//! 集群会话消费者单源（自建临时库播种 provider 形，随消费者返回存储句柄）
//!
//! 收口 cluster_migration / cluster_migration_domain /
//! cluster_slot_keys_alloc_fail_smooth / migrate_fail_inject 四册逐字同形的
//! 「临时库 + 播种 + 消费者 + 存储句柄」装配（灌键/读回与消费者同库）。
//! 装配尾单源见 `wedb_test::cluster_consumer_with`：

use std::sync::Arc;

use wdev::SegmentedDevice;
use wedb::server::{cluster::IClusterProvider, cluster_provider::ClusterProvider};
use wkv::WedbStore;
use wnode::{RespSessionConsumer, resp::resp_server_session::RespServerSessionOptions};
use wtest_base::test_store_config;

use crate::cluster_consumer_with::cluster_consumer_with;

/// 临时库播种 provider 并装配消费者，返回（消费者, 存储句柄）——句柄供
/// 关闭注入态灌键 / 读回断言与消费者同库
pub fn cluster_consumer_fresh_store(
  cp: &ClusterProvider,
  db_name: &str,
  options: RespServerSessionOptions,
) -> (RespSessionConsumer, Arc<WedbStore<SegmentedDevice>>) {
  let dir = tempfile::tempdir().unwrap().keep();
  let device = Arc::new(SegmentedDevice::single_file(dir.join(db_name)).unwrap());
  let store = Arc::new(WedbStore::open(test_store_config(), device).unwrap());
  cp.set_store(Arc::clone(&store));
  let consumer = cluster_consumer_with(
    cp.create_cluster_session(),
    cp.provider_handle(),
    &store,
    options,
  );
  (consumer, store)
}
