//! 副本宿主服务器装配单源（真 socket 单监听口 + 每连接集群会话 + 启动取址）
//!
//! 自 common/mod.rs 拆面：仅消费宿主服务器的册直挂本文件，避免不消费册招
//! per-binary dead_code（primary_assets 先例）。沿用挂载：
//!
//! ```text
//! #[path = "common/replica_host.rs"]
//! mod replica_host;
//! use replica_host::replica_host;
//! ```

use std::{num::NonZeroUsize, sync::Arc};

use wedb::server::{cluster::IClusterProvider, cluster_provider::ClusterProvider};
use wnode::{
  GarnetServer, RespSessionConsumer, SessionProviderFace,
  resp::{garnet_api::StoreGarnetApi, resp_server_session::RespServerSessionOptions},
};
use wtxn::{TxnLockTable, WatchVersionMap};

/// 副本宿主会话装配面（每连接新集群会话——对齐宿主 get_session）
pub struct ReplicaSessionProvider {
  pub provider: Arc<ClusterProvider>,
}

impl SessionProviderFace for ReplicaSessionProvider {
  type Consumer = RespSessionConsumer;

  fn get_session(
    &self,
    _wire_format: wnode::WireFormat,
    _network_sender_id: u64,
  ) -> Option<RespSessionConsumer> {
    let cluster_session = self.provider.create_cluster_session();
    let store = self.provider.try_store()?;
    let mut consumer = RespSessionConsumer::with_cluster(
      1,
      RespServerSessionOptions::default(),
      cluster_session,
      self.provider.provider_handle(),
      Arc::new(StoreGarnetApi::new(store.new_session().ok()?)),
    );
    consumer.attach_transaction_components(Arc::new(WatchVersionMap::new(64)), TxnLockTable::new());
    Some(consumer)
  }
}

/// 副本宿主服务器装配尾（真 socket 单监听口 + 每连接集群会话 + 启动取址）
///
/// `worker_threads` 由用例原样传入（各文件字面量不同值同：`new(1)` /
/// `Some(NonZeroUsize::MIN)`）；返回 `(服务器, 监听地址串)`，服务器须由
/// 调用方持有至用例末并 `dispose`。
pub fn replica_host(
  provider: &Arc<ClusterProvider>,
  worker_threads: Option<NonZeroUsize>,
) -> (GarnetServer<ReplicaSessionProvider>, String) {
  let server = GarnetServer::new(
    &["127.0.0.1:0".to_string()],
    65536,
    100,
    Arc::new(ReplicaSessionProvider {
      provider: Arc::clone(provider),
    }),
  )
  .unwrap();
  server.start(worker_threads).unwrap();
  let replica_addr = server.local_addr().unwrap().to_string();
  (server, replica_addr)
}
