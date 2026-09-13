//! 集群配置集成测试，对标 Garnet ClusterConfigTests
use aok::Void;
use wedb::server::{
  cluster_config::{CLUSTER_CONFIG_VERSION, ClusterConfig},
  hash_slot::SlotState,
  worker::{LOCAL_WORKER_ID, LocalWorkerSpec, NodeRole},
};

/// test/cluster/Garnet.test.cluster/ClusterConfigTests.cs:ClusterConfigInitializesUnassignedWorkerTest
#[test]
fn cluster_config_initializes_unassigned_worker_test() -> Void {
  let mut config = ClusterConfig::new();
  config.initialize_local_worker(LocalWorkerSpec {
    node_id: "node_1",
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
    config.get_node_role_from_node_id("asdasdqwe"),
    NodeRole::Unassigned
  );

  let config_bytes = config.to_byte_array();
  let restored_config = ClusterConfig::from_byte_array(&config_bytes)?;

  let (address, port) = restored_config.get_worker_address(0);
  assert_eq!(address, "unassigned");
  assert_eq!(port, 0);
  assert_eq!(
    restored_config.get_node_role_from_node_id("asdasdqwe"),
    NodeRole::Unassigned
  );

  aok::OK
}

/// test/cluster/Garnet.test.cluster/ClusterConfigTests.cs:ClusterConfigVersionRoundTripTest
#[test]
fn cluster_config_version_round_trip_test() -> Void {
  let mut config = ClusterConfig::new();
  config.initialize_local_worker(LocalWorkerSpec {
    node_id: "node_roundtrip",
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
    node_id: "node_mismatch",
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

  let sender_id = "sender_hex_id";
  let third_party_id = "third_party_hex_id";

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
    .merge(&third_party, &gxhash::HashMap::default())
    .unwrap_or(sender);
  let third_party_worker_id = sender.get_worker_id_from_node_id(third_party_id);
  sender
    .update_slot_state(SENDER_SLOT, LOCAL_WORKER_ID as u16, SlotState::Stable)
    .update_slot_state(STALE_SLOT, third_party_worker_id, SlotState::Stable);

  let mut receiver = ClusterConfig::new();
  receiver.initialize_local_worker(LocalWorkerSpec {
    node_id: "receiver_hex_id",
    address: "127.0.0.1",
    port: 7002,
    config_epoch: 1,
    role: NodeRole::Primary,
    replica_of_node_id: None,
    hostname: Some(""),
  });
  receiver = receiver
    .merge(&sender, &gxhash::HashMap::default())
    .unwrap_or(receiver);
  let sender_worker_id = receiver.get_worker_id_from_node_id(sender_id);
  assert_ne!(sender_worker_id, 0);
  receiver
    .update_slot_state(STALE_SLOT, sender_worker_id, SlotState::Stable)
    .update_slot_state(SENDER_SLOT, sender_worker_id, SlotState::Stable);

  assert_eq!(
    receiver.get_node_id_from_slot(STALE_SLOT as u16).as_deref(),
    Some(sender_id)
  );

  let merged = receiver
    .merge(&sender, &gxhash::HashMap::default())
    .expect("merge should apply stale ownership reset");

  assert_ne!(
    merged.get_node_id_from_slot(STALE_SLOT as u16).as_deref(),
    Some(sender_id)
  );
  assert_eq!(merged.get_state(STALE_SLOT as u16), SlotState::Offline);
  assert_eq!(
    merged.get_node_id_from_slot(SENDER_SLOT as u16).as_deref(),
    Some(sender_id)
  );
}

/// test/cluster/Garnet.test.cluster/ClusterConfigTests.cs:ClusterConfigMergeSlotMapAccumulatesUpdatedAcrossSlotsTest
#[test]
fn cluster_config_merge_slot_map_accumulates_updated_across_slots_test() {
  const STALE_SLOT: usize = 100;
  const SENDER_SLOT: usize = 200;

  let sender_id = "sender_hex_id_acc";
  let third_party_id = "third_party_hex_id_acc";

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
    .merge(&third_party, &gxhash::HashMap::default())
    .unwrap_or(sender);
  let third_party_worker_id = sender.get_worker_id_from_node_id(third_party_id);
  sender
    .update_slot_state(SENDER_SLOT, LOCAL_WORKER_ID as u16, SlotState::Stable)
    .update_slot_state(STALE_SLOT, third_party_worker_id, SlotState::Stable);

  let mut receiver = ClusterConfig::new();
  receiver.initialize_local_worker(LocalWorkerSpec {
    node_id: "receiver_hex_id_acc",
    address: "127.0.0.1",
    port: 7002,
    config_epoch: 1,
    role: NodeRole::Primary,
    replica_of_node_id: None,
    hostname: Some(""),
  });
  receiver = receiver
    .merge(&sender, &gxhash::HashMap::default())
    .unwrap_or(receiver);
  let sender_worker_id = receiver.get_worker_id_from_node_id(sender_id);
  assert_ne!(sender_worker_id, 0);
  receiver
    .update_slot_state(STALE_SLOT, sender_worker_id, SlotState::Stable)
    .update_slot_state(SENDER_SLOT, sender_worker_id, SlotState::Stable);

  assert_eq!(
    receiver.get_node_id_from_slot(STALE_SLOT as u16).as_deref(),
    Some(sender_id)
  );
  assert_eq!(
    receiver
      .get_node_id_from_slot(SENDER_SLOT as u16)
      .as_deref(),
    Some(sender_id)
  );

  let merged = receiver
    .merge(&sender, &gxhash::HashMap::default())
    .expect("merge should accumulate updated across slots");

  assert_eq!(merged.get_state(STALE_SLOT as u16), SlotState::Offline);
  assert_ne!(
    merged.get_node_id_from_slot(STALE_SLOT as u16).as_deref(),
    Some(sender_id)
  );
}
