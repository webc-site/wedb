//! 集群会话消费者单源（存储域由调用方注入形，r314 同形收口）

use std::sync::Arc;

use wdev::SegmentedDevice;
use wedb::server::{cluster::IClusterProvider, cluster_provider::ClusterProvider};
use wkv::WedbStore;
use wnode::{
  RespSessionConsumer,
  resp::{garnet_api::StoreGarnetApi, resp_server_session::RespServerSessionOptions},
};
use wtxn::{TxnLockTable, WatchVersionMap};

/// 主端真 RESP 集群会话消费者（槽位门评 → 键门挂起 → 存储执行域；
/// 存储域取调用方注入的 store，写路径同生产）
pub fn cluster_consumer(
  provider: &ClusterProvider,
  store: &Arc<WedbStore<SegmentedDevice>>,
) -> RespSessionConsumer {
  let cluster_session = provider.create_cluster_session();
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
