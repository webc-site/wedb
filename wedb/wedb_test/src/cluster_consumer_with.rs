//! 集群会话消费者装配核心单源（会话 + 存储域 + 会话选项全参）
//!
//! 收口 cluster_failover、cluster_mgmt_epoch_drain_warn、cluster_migration、
//! cluster_migration_domain、cluster_resp_session、
//! cluster_slot_keys_alloc_fail_smooth、cluster_slot_verify_redirect_clusterdown、
//! cluster_swapdb_slot_state、failover_epoch_drain_failclose、
//! failover_timeout_bounds、migrate_fail_inject、replication_assembly_e2e
//! 十二册内联消费装配的公共尾：with_cluster（会话 id 1）+ 事务组件接线。
//! 差异面（存储来源、会话选项）以参数暴露，各形态薄壳见 cluster_consumer、
//! cluster_consumer_store、cluster_consumer_fresh。消费面经
//! `wedb_test::cluster_consumer_with` 引用（原 common/ 直挂面已收口进本 crate）。
//!

use std::sync::Arc;

use wdev::SegmentedDevice;
use wedb::server::cluster_session::ClusterSession;
use wkv::WedbStore;
use wnode::{
  ClusterProviderHandle, RespSessionConsumer,
  resp::{garnet_api::StoreGarnetApi, resp_server_session::RespServerSessionOptions},
};
use wtxn::{TxnLockTable, WatchVersionMap};

/// 集群切面 + 存储执行域消费者（`options` 承接 max_databases / enable_lua 等
/// 各册差异面；`provider_handle` 取调用方 provider 的 `provider_handle()`）
pub fn cluster_consumer_with(
  cluster_session: Arc<ClusterSession>,
  provider_handle: ClusterProviderHandle,
  store: &Arc<WedbStore<SegmentedDevice>>,
  options: RespServerSessionOptions,
) -> RespSessionConsumer {
  let mut consumer = RespSessionConsumer::with_cluster(
    1,
    options,
    cluster_session,
    provider_handle,
    Arc::new(StoreGarnetApi::new(store.new_session().unwrap())),
  );
  consumer.attach_transaction_components(Arc::new(WatchVersionMap::new(64)), TxnLockTable::new());
  consumer
}
