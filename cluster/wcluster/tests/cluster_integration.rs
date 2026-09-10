//! wcluster 集群配置与节点生命周期集成测试
use aok::{OK, Void};
use wcluster::server::{
  cluster_config::ClusterConfig,
  cluster_manager::ClusterManager,
  hash_slot::SlotState,
  worker::{LocalWorkerSpec, NodeRole, Worker},
};

#[test]
fn test_cluster_config_and_slot_mapping() -> Void {
  let mut config = ClusterConfig::new();

  // 1. 初始化本地 Worker
  config.initialize_local_worker(LocalWorkerSpec {
    address: "127.0.0.1",
    port: 7000,
    node_id: "local_node_1",
    config_epoch: 1,
    role: NodeRole::Primary,
    replica_of_node_id: None,
    hostname: Some("local-host"),
  });

  assert_eq!(config.local_node_config_epoch(), 1);
  assert!(config.is_primary());
  assert_eq!(config.local_node_id(), Some("local_node_1"));

  // 2. 接入远端节点
  let peer_worker = Worker {
    nodeid: Some("node_peer_1".to_string()),
    address: "127.0.0.1".to_string(),
    port: 7001,
    config_epoch: 5,
    role: NodeRole::Primary,
    replica_of_node_id: None,
    replication_offset: 0,
    hostname: None,
  };
  config.workers.push(peer_worker);

  // 3. 分配槽位并验证局部性
  config.assign_slots(&[0, 1, 2, 3], 1, SlotState::Stable);
  assert!(config.is_local(0, true));
  assert!(config.is_local(1, true));
  assert!(!config.is_local(100, true));

  // 4. 槽位区间格式化
  let range_str = ClusterManager::get_range(&[0, 1, 2, 8, 9, 10]);
  assert_eq!(range_str, "> 0-2 8-10 ");

  // 5. 角色流转与副本接管
  config.make_replica_of(Some("node_peer_1"));
  assert!(config.is_replica());
  assert_eq!(config.local_node_primary_id(), Some("node_peer_1"));

  config.take_over_from_primary();
  assert!(config.is_primary());
  assert_eq!(config.local_node_primary_id(), None);

  OK
}
