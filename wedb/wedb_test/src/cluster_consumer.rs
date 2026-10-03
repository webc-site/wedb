//! 集群会话消费者单源（provider 存储域与执行域同源形，r314 同形收口）
//!
//! 装配尾（with_cluster + 事务组件）单源见 `wedb_test::cluster_consumer_with`。

use wedb::server::{cluster::IClusterProvider, cluster_provider::ClusterProvider};
use wnode::{RespSessionConsumer, resp::resp_server_session::RespServerSessionOptions};

use crate::cluster_consumer_with::cluster_consumer_with;

/// 集群会话消费者（provider.set_store 与执行域同源）
pub fn cluster_consumer(provider: &ClusterProvider) -> RespSessionConsumer {
  let store = provider.try_store().unwrap();
  cluster_consumer_with(
    provider.create_cluster_session(),
    provider.provider_handle(),
    &store,
    RespServerSessionOptions::default(),
  )
}
