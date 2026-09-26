//! 集群会话消费者单源（自建临时库播种 provider 形，r314 同形收口；
//! 装配形态同 cluster_resp_session 的 gate 库）

use std::sync::Arc;

use wdev::SegmentedDevice;
use wedb::server::{
  cluster::IClusterProvider, cluster_provider::ClusterProvider, cluster_session::ClusterSession,
};
use wkv::WedbStore;
use wnode::{
  RespSessionConsumer,
  resp::{garnet_api::StoreGarnetApi, resp_server_session::RespServerSessionOptions},
};
use wtest_base::test_store_config;
use wtxn::{TxnLockTable, WatchVersionMap};

/// 构造挂接集群切面 + 存储执行域的会话消费者：新开临时库（db 文件名由
/// 调用方指定）并播种到 provider
pub fn cluster_consumer(cp: &ClusterProvider, db_name: &str) -> RespSessionConsumer {
  let cluster_session: Arc<ClusterSession> = cp.create_cluster_session();
  let dir = tempfile::tempdir().unwrap().keep();
  let device = Arc::new(SegmentedDevice::single_file(dir.join(db_name)).unwrap());
  let config = test_store_config();
  let store = Arc::new(WedbStore::open(config, device).unwrap());
  cp.set_store(Arc::clone(&store));
  let mut consumer = RespSessionConsumer::with_cluster(
    1,
    RespServerSessionOptions::default(),
    cluster_session,
    cp.provider_handle(),
    Arc::new(StoreGarnetApi::new(store.new_session().unwrap())),
  );
  consumer.attach_transaction_components(Arc::new(WatchVersionMap::new(64)), TxnLockTable::new());
  consumer
}
