//! 集群配置集成测试，对标 Garnet ClusterConfigTests

use aok::Void;
use gxhash::HashMap;
use wedb::server::{
  cluster_config::{CLUSTER_CONFIG_VERSION, ClusterConfig},
  hash_slot::SlotState,
  worker::{LOCAL_WORKER_ID, LocalWorkerSpec, NodeRole},
};

/// 测试节点身份（内部 u128；协议面渲染 32 字符小写 hex）
const PRIMARY_ID: u128 = 0x0DE1_0000_0000_0000_0000_0000_0000_0001;
const REPLICA_ID: u128 = 0x0DE1_0000_0000_0000_0000_0000_0000_0002;

/// test/cluster/Garnet.test.cluster/ClusterConfigTests.cs:ClusterConfigInitializesUnassignedWorkerTest
#[test]
fn cluster_config_initializes_unassigned_worker_test() -> Void {
  let mut config = ClusterConfig::new();
  config.initialize_local_worker(LocalWorkerSpec {
    node_id: 0x0000_0000_0000_0000_0000_0000_0000_DE11,
    address: "127.0.0.1",
    port: 7001,
    config_epoch: 0,
    role: NodeRole::Primary,
    replica_of_node_id: None,
    hostname: Some(""),
  });

  let (address, port) = config.get_worker_address(0);
  assert_eq!(address, "unassigned");
  assert_eq!(port, 0);
  assert_eq!(
    config.get_node_role_from_node_id(0x9999),
    NodeRole::Unassigned
  );

  let config_bytes = config.to_byte_array();
  let restored_config = ClusterConfig::from_byte_array(&config_bytes)?;

  let (address, port) = restored_config.get_worker_address(0);
  assert_eq!(address, "unassigned");
  assert_eq!(port, 0);
  assert_eq!(
    restored_config.get_node_role_from_node_id(0x9999),
    NodeRole::Unassigned
  );

  aok::OK
}

/// test/cluster/Garnet.test.cluster/ClusterConfigTests.cs:ClusterConfigVersionRoundTripTest
#[test]
fn cluster_config_version_round_trip_test() -> Void {
  let mut config = ClusterConfig::new();
  config.initialize_local_worker(LocalWorkerSpec {
    node_id: 0x12_34,
    address: "127.0.0.1",
    port: 7001,
    config_epoch: 1,
    role: NodeRole::Primary,
    replica_of_node_id: None,
    hostname: Some(""),
  });

  let config_bytes = config.to_byte_array();
  assert_eq!(
    ClusterConfig::try_peek_version(&config_bytes),
    Some(CLUSTER_CONFIG_VERSION)
  );

  let restored = ClusterConfig::from_byte_array(&config_bytes)?;
  assert_eq!(restored.local_node_id(), config.local_node_id());

  aok::OK
}

/// test/cluster/Garnet.test.cluster/ClusterConfigTests.cs:ClusterConfigVersionMismatchThrowsTest
#[test]
fn cluster_config_version_mismatch_throws_test() {
  let mut config = ClusterConfig::new();
  config.initialize_local_worker(LocalWorkerSpec {
    node_id: 0x12_35,
    address: "127.0.0.1",
    port: 7001,
    config_epoch: 1,
    role: NodeRole::Primary,
    replica_of_node_id: None,
    hostname: Some(""),
  });

  let mut config_bytes = config.to_byte_array();
  config_bytes[0] = CLUSTER_CONFIG_VERSION.wrapping_add(1);

  assert!(ClusterConfig::from_byte_array(&config_bytes).is_err());
}

/// test/cluster/Garnet.test.cluster/ClusterConfigTests.cs:ClusterConfigTryPeekVersionEmptyDataTest
#[test]
fn cluster_config_try_peek_version_empty_data_test() {
  assert_eq!(ClusterConfig::try_peek_version(&[]), None);
}

/// test/cluster/Garnet.test.cluster/ClusterConfigTests.cs:ClusterConfigMergeSlotMapRetainsStaleOwnershipResetTest
#[test]
fn cluster_config_merge_slot_map_retains_stale_ownership_reset_test() {
  const STALE_SLOT: usize = 100;
  const SENDER_SLOT: usize = 200;

  let sender_id = 0x0DE1_0000_0000_0000_0000_0000_0000_0001;
  let third_party_id = 0x0DE1_0000_0000_0000_0000_0000_0000_0003;

  let mut third_party = ClusterConfig::new();
  third_party.initialize_local_worker(LocalWorkerSpec {
    node_id: third_party_id,
    address: "127.0.0.1",
    port: 7003,
    config_epoch: 5,
    role: NodeRole::Primary,
    replica_of_node_id: None,
    hostname: Some(""),
  });

  let mut sender = ClusterConfig::new();
  sender.initialize_local_worker(LocalWorkerSpec {
    node_id: sender_id,
    address: "127.0.0.1",
    port: 7001,
    config_epoch: 20,
    role: NodeRole::Primary,
    replica_of_node_id: None,
    hostname: Some(""),
  });
  sender = sender
    .merge(&third_party, &HashMap::default())
    .unwrap_or(sender);
  let third_party_worker_id = sender.get_worker_id_from_node_id(third_party_id);
  sender
    .update_slot_state(SENDER_SLOT, LOCAL_WORKER_ID as u16, SlotState::Stable)
    .update_slot_state(STALE_SLOT, third_party_worker_id, SlotState::Stable);

  let mut receiver = ClusterConfig::new();
  receiver.initialize_local_worker(LocalWorkerSpec {
    node_id: 0x0DE1_0000_0000_0000_0000_0000_0000_0002,
    address: "127.0.0.1",
    port: 7002,
    config_epoch: 1,
    role: NodeRole::Primary,
    replica_of_node_id: None,
    hostname: Some(""),
  });
  receiver = receiver
    .merge(&sender, &HashMap::default())
    .unwrap_or(receiver);
  let sender_worker_id = receiver.get_worker_id_from_node_id(sender_id);
  assert_ne!(sender_worker_id, 0);
  receiver
    .update_slot_state(STALE_SLOT, sender_worker_id, SlotState::Stable)
    .update_slot_state(SENDER_SLOT, sender_worker_id, SlotState::Stable);

  assert_eq!(
    receiver.get_node_id_from_slot(STALE_SLOT as u16),
    Some(sender_id)
  );

  let merged = receiver
    .merge(&sender, &HashMap::default())
    .expect("merge should apply stale ownership reset");

  assert_ne!(
    merged.get_node_id_from_slot(STALE_SLOT as u16),
    Some(sender_id)
  );
  assert_eq!(merged.get_state(STALE_SLOT as u16), SlotState::Offline);
  assert_eq!(
    merged.get_node_id_from_slot(SENDER_SLOT as u16),
    Some(sender_id)
  );
}

/// test/cluster/Garnet.test.cluster/ClusterConfigTests.cs:ClusterConfigMergeSlotMapAccumulatesUpdatedAcrossSlotsTest
#[test]
fn cluster_config_merge_slot_map_accumulates_updated_across_slots_test() {
  const STALE_SLOT: usize = 100;
  const SENDER_SLOT: usize = 200;

  let sender_id = 0x0DE1_0000_0000_0000_0000_0000_0000_0001;
  let third_party_id = 0x0DE1_0000_0000_0000_0000_0000_0000_0003;

  let mut third_party = ClusterConfig::new();
  third_party.initialize_local_worker(LocalWorkerSpec {
    node_id: third_party_id,
    address: "127.0.0.1",
    port: 7003,
    config_epoch: 5,
    role: NodeRole::Primary,
    replica_of_node_id: None,
    hostname: Some(""),
  });

  let mut sender = ClusterConfig::new();
  sender.initialize_local_worker(LocalWorkerSpec {
    node_id: sender_id,
    address: "127.0.0.1",
    port: 7001,
    config_epoch: 0,
    role: NodeRole::Primary,
    replica_of_node_id: None,
    hostname: Some(""),
  });
  sender = sender
    .merge(&third_party, &HashMap::default())
    .unwrap_or(sender);
  let third_party_worker_id = sender.get_worker_id_from_node_id(third_party_id);
  sender
    .update_slot_state(SENDER_SLOT, LOCAL_WORKER_ID as u16, SlotState::Stable)
    .update_slot_state(STALE_SLOT, third_party_worker_id, SlotState::Stable);

  let mut receiver = ClusterConfig::new();
  receiver.initialize_local_worker(LocalWorkerSpec {
    node_id: 0x0DE1_0000_0000_0000_0000_0000_0000_0002,
    address: "127.0.0.1",
    port: 7002,
    config_epoch: 1,
    role: NodeRole::Primary,
    replica_of_node_id: None,
    hostname: Some(""),
  });
  receiver = receiver
    .merge(&sender, &HashMap::default())
    .unwrap_or(receiver);
  let sender_worker_id = receiver.get_worker_id_from_node_id(sender_id);
  assert_ne!(sender_worker_id, 0);
  receiver
    .update_slot_state(STALE_SLOT, sender_worker_id, SlotState::Stable)
    .update_slot_state(SENDER_SLOT, sender_worker_id, SlotState::Stable);

  assert_eq!(
    receiver.get_node_id_from_slot(STALE_SLOT as u16),
    Some(sender_id)
  );
  assert_eq!(
    receiver.get_node_id_from_slot(SENDER_SLOT as u16),
    Some(sender_id)
  );

  let merged = receiver
    .merge(&sender, &HashMap::default())
    .expect("merge should accumulate updated across slots");

  assert_eq!(merged.get_state(STALE_SLOT as u16), SlotState::Offline);
  assert_ne!(
    merged.get_node_id_from_slot(STALE_SLOT as u16),
    Some(sender_id)
  );
}

#[test]
fn cluster_config_get_replicas_and_endpoints_test() {
  let mut primary = ClusterConfig::new();
  primary.initialize_local_worker(LocalWorkerSpec {
    node_id: PRIMARY_ID,
    address: "127.0.0.1",
    port: 7000,
    config_epoch: 1,
    role: NodeRole::Primary,
    replica_of_node_id: None,
    hostname: Some("host-master"),
  });

  let mut replica1 = ClusterConfig::new();
  replica1.initialize_local_worker(LocalWorkerSpec {
    node_id: REPLICA_ID,
    address: "127.0.0.1",
    port: 7001,
    config_epoch: 1,
    role: NodeRole::Replica,
    replica_of_node_id: Some(PRIMARY_ID),
    hostname: Some("host-rep1"),
  });

  let mut replica2 = ClusterConfig::new();
  replica2.initialize_local_worker(LocalWorkerSpec {
    node_id: 0x0DE1_0000_0000_0000_0000_0000_0000_0004,
    address: "127.0.0.2",
    port: 7002,
    config_epoch: 1,
    role: NodeRole::Replica,
    replica_of_node_id: Some(PRIMARY_ID),
    hostname: None,
  });

  let mut other = ClusterConfig::new();
  other.initialize_local_worker(LocalWorkerSpec {
    node_id: 0x0DE1_0000_0000_0000_0000_0000_0000_0003,
    address: "127.0.0.3",
    port: 7003,
    config_epoch: 1,
    role: NodeRole::Primary,
    replica_of_node_id: None,
    hostname: None,
  });

  let merged = primary
    .merge(&replica1, &HashMap::default())
    .unwrap()
    .merge(&replica2, &HashMap::default())
    .unwrap()
    .merge(&other, &HashMap::default())
    .unwrap();

  let replica_ids = merged.get_replica_ids(PRIMARY_ID);
  assert_eq!(replica_ids.len(), 2);
  assert!(replica_ids.contains(&0x0DE1_0000_0000_0000_0000_0000_0000_0002));
  assert!(replica_ids.contains(&0x0DE1_0000_0000_0000_0000_0000_0000_0004));

  let endpoints = merged.get_replica_endpoints(PRIMARY_ID);
  assert_eq!(endpoints.len(), 2);
  assert!(endpoints.contains(&("127.0.0.1".to_string(), 7001)));
  assert!(endpoints.contains(&("127.0.0.2".to_string(), 7002)));

  let lines = merged.get_replicas(PRIMARY_ID, None);
  assert_eq!(lines.len(), 2);
  // 渲染面：节点 id 与主 id 均为 32 字符小写 hex
  let master_hex = format!("{:032x}", PRIMARY_ID);
  let rep1_hex = format!("{:032x}", REPLICA_ID);
  let rep2_hex = format!("{:032x}", 0x0DE1_0000_0000_0000_0000_0000_0000_0004u128);
  assert!(
    lines
      .iter()
      .any(|l| l.contains(&rep1_hex) && l.contains(&format!("slave {master_hex}")))
  );
  assert!(
    lines
      .iter()
      .any(|l| l.contains(&rep2_hex) && l.contains(&format!("slave {master_hex}")))
  );

  // Non-existent master has 0 replicas
  assert!(merged.get_replicas(0x999, None).is_empty());
  assert!(merged.get_replica_endpoints(0x999).is_empty());
}

#[test]
fn cluster_config_get_worker_node_id_from_address_or_hostname_test() {
  let mut local = ClusterConfig::new();
  local.initialize_local_worker(LocalWorkerSpec {
    node_id: 0x10CA1,
    address: "127.0.0.1",
    port: 7000,
    config_epoch: 1,
    role: NodeRole::Primary,
    replica_of_node_id: None,
    hostname: Some("local.host"),
  });

  let mut remote1 = ClusterConfig::new();
  remote1.initialize_local_worker(LocalWorkerSpec {
    node_id: PRIMARY_ID,
    address: "192.168.1.10",
    port: 7001,
    config_epoch: 1,
    role: NodeRole::Primary,
    replica_of_node_id: None,
    hostname: Some("remote1.domain"),
  });

  let mut remote2 = ClusterConfig::new();
  remote2.initialize_local_worker(LocalWorkerSpec {
    node_id: 0x0DE1_0000_0000_0000_0000_0000_0000_0002,
    address: "192.168.1.20",
    port: 7002,
    config_epoch: 1,
    role: NodeRole::Replica,
    replica_of_node_id: Some(PRIMARY_ID),
    hostname: None,
  });

  let merged = local
    .merge(&remote1, &HashMap::default())
    .unwrap()
    .merge(&remote2, &HashMap::default())
    .unwrap();

  // Remote nodes lookup by IP + port
  assert_eq!(
    merged.get_worker_node_id_from_address_or_hostname("192.168.1.10", 7001),
    Some(0x0DE1_0000_0000_0000_0000_0000_0000_0001)
  );
  assert_eq!(
    merged.get_worker_node_id_from_address_or_hostname("192.168.1.20", 7002),
    Some(0x0DE1_0000_0000_0000_0000_0000_0000_0002)
  );

  // Remote nodes lookup by hostname + port (case-insensitive)
  assert_eq!(
    merged.get_worker_node_id_from_address_or_hostname("remote1.domain", 7001),
    Some(0x0DE1_0000_0000_0000_0000_0000_0000_0001)
  );
  assert_eq!(
    merged.get_worker_node_id_from_address_or_hostname("REMOTE1.DOMAIN", 7001),
    Some(0x0DE1_0000_0000_0000_0000_0000_0000_0001)
  );

  // Local worker (index 1) is skipped to prevent self-migration (matching C# Garnet)
  assert_eq!(
    merged.get_worker_node_id_from_address_or_hostname("127.0.0.1", 7000),
    None
  );
  assert_eq!(
    merged.get_worker_node_id_from_address_or_hostname("local.host", 7000),
    None
  );

  // Non-existent or port mismatch returns None
  assert_eq!(
    merged.get_worker_node_id_from_address_or_hostname("192.168.1.10", 9999),
    None
  );
  assert_eq!(
    merged.get_worker_node_id_from_address_or_hostname("unknown.host", 7001),
    None
  );
}
