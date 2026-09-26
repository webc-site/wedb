//! 集群会话消费者单源（provider 存储域与执行域同源形，r314 同形收口）

use std::sync::Arc;

use wedb::server::{
  cluster::IClusterProvider, cluster_provider::ClusterProvider, cluster_session::ClusterSession,
};
use wnode::{
  RespSessionConsumer,
  resp::{garnet_api::StoreGarnetApi, resp_server_session::RespServerSessionOptions},
};
use wtxn::{TxnLockTable, WatchVersionMap};

/// 集群会话消费者（provider.set_store 与执行域同源）
pub fn cluster_consumer(provider: &ClusterProvider) -> RespSessionConsumer {
  let cluster_session: Arc<ClusterSession> = provider.create_cluster_session();
  let store = provider.try_store().unwrap();
  let mut consumer = RespSessionConsumer::with_cluster(
    1,
    RespServerSessionOptions::default(),
    cluster_session,
    provider.provider_handle(),
    Arc::new(StoreGarnetApi::new(store.new_session().unwrap())),
  );
  consumer.attach_transaction_components(Arc::new(WatchVersionMap::new(64)), TxnLockTable::new());
  consumer
}
