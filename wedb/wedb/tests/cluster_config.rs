//! 集群配置集成测试，对标 Garnet ClusterConfigTests

use std::net::SocketAddr;

use aok::Void;
use bitcode::Encode;
use hipstr::HipStr;
use wbase::{hash_slot::CLUSTER_SLOT_COUNT, map::new_concurrent_map};
use wedb::{
  error::Error,
  server::{
    cluster_config::{CLUSTER_CONFIG_VERSION, ClusterConfig},
    connection_info::ConnectionInfo,
    hash_slot::SlotState,
    worker::{LOCAL_WORKER_ID, LocalWorkerSpec, NodeRole, RESERVED_WORKER_ID, Worker},
  },
};
use wedb_test::de11_node_id::DE11_NODE_ID;

/// 测试节点身份（内部 u128；协议面渲染 32 字符小写 hex）
const PRIMARY_ID: u128 = 0x0DE1_0000_0000_0000_0000_0000_0000_0001;
const REPLICA_ID: u128 = 0x0DE1_0000_0000_0000_0000_0000_0000_0002;

/// test/cluster/Garnet.test.cluster/ClusterConfigTests.cs:ClusterConfigInitializesUnassignedWorkerTest
#[test]
fn cluster_config_initializes_unassigned_worker_test() -> Void {
  let mut config = ClusterConfig::new();
  config.initialize_local_worker(LocalWorkerSpec {
    node_id: DE11_NODE_ID,
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
    .merge(&third_party, &new_concurrent_map())
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
    .merge(&sender, &new_concurrent_map())
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
    .merge(&sender, &new_concurrent_map())
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
    .merge(&third_party, &new_concurrent_map())
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
    .merge(&sender, &new_concurrent_map())
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
    .merge(&sender, &new_concurrent_map())
    .expect("merge should accumulate updated across slots");

  assert_eq!(merged.get_state(STALE_SLOT as u16), SlotState::Offline);
  assert_ne!(
    merged.get_node_id_from_slot(STALE_SLOT as u16),
    Some(sender_id)
  );
}

#[test]
fn cluster_config_get_replicas_test() {
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
    .merge(&replica1, &new_concurrent_map())
    .unwrap()
    .merge(&replica2, &new_concurrent_map())
    .unwrap()
    .merge(&other, &new_concurrent_map())
    .unwrap();

  let replica_ids = merged.get_replica_ids(PRIMARY_ID);
  assert_eq!(replica_ids.len(), 2);
  assert!(replica_ids.contains(&0x0DE1_0000_0000_0000_0000_0000_0000_0002));
  assert!(replica_ids.contains(&0x0DE1_0000_0000_0000_0000_0000_0000_0004));

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
    .merge(&remote1, &new_concurrent_map())
    .unwrap()
    .merge(&remote2, &new_concurrent_map())
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

fn create_replica_sender(
  primary_epoch: i64,
  replica_epoch: i64,
  slots: &[usize],
) -> (ClusterConfig, u128, u128) {
  let primary_id = 0x1000_0001;
  let replica_id = 0x2000_0002;

  let mut primary = ClusterConfig::new();
  primary.initialize_local_worker(LocalWorkerSpec {
    node_id: primary_id,
    address: "127.0.0.1",
    port: 7001,
    config_epoch: primary_epoch,
    role: NodeRole::Primary,
    replica_of_node_id: None,
    hostname: Some(""),
  });
  for &slot in slots {
    primary.update_slot_state(slot, LOCAL_WORKER_ID as u16, SlotState::Stable);
  }

  let mut replica = ClusterConfig::new();
  replica.initialize_local_worker(LocalWorkerSpec {
    node_id: replica_id,
    address: "127.0.0.1",
    port: 7002,
    config_epoch: replica_epoch,
    role: NodeRole::Replica,
    replica_of_node_id: Some(primary_id),
    hostname: Some(""),
  });
  let replica = replica.merge(&primary, &new_concurrent_map()).unwrap();
  assert_eq!(
    replica.get_node_id_from_slot(slots[0] as u16),
    Some(primary_id)
  );

  (replica, replica_id, primary_id)
}

fn create_receiver_aware_of(other: &ClusterConfig, unowned_slots: &[usize]) -> ClusterConfig {
  let mut receiver = ClusterConfig::new();
  receiver.initialize_local_worker(LocalWorkerSpec {
    node_id: 0x3000_0003,
    address: "127.0.0.1",
    port: 7003,
    config_epoch: 1,
    role: NodeRole::Primary,
    replica_of_node_id: None,
    hostname: Some(""),
  });
  let mut receiver = receiver.merge(other, &new_concurrent_map()).unwrap();
  for &slot in unowned_slots {
    receiver.update_slot_state(slot, RESERVED_WORKER_ID as u16, SlotState::Offline);
  }
  receiver
}

/// test/cluster/Garnet.test.cluster/ClusterConfigTests.cs:ClusterConfigMergeSlotMapReplicaSenderCannotClaimUnownedSlotTest
#[test]
fn cluster_config_merge_slot_map_replica_sender_cannot_claim_unowned_slot_test() {
  const UNOWNED_SLOT: usize = 300;
  let (replica, replica_id, primary_id) = create_replica_sender(10, 30, &[UNOWNED_SLOT]);
  let receiver = create_receiver_aware_of(&replica, &[UNOWNED_SLOT]);

  assert_eq!(
    receiver.get_worker_id_from_slot(UNOWNED_SLOT as u16),
    RESERVED_WORKER_ID
  );

  let merged = receiver
    .merge(&replica, &new_concurrent_map())
    .unwrap_or_else(|| receiver.clone());

  assert_ne!(
    merged.get_node_id_from_slot(UNOWNED_SLOT as u16),
    Some(replica_id)
  );
  assert_eq!(
    merged.get_worker_id_from_slot(UNOWNED_SLOT as u16),
    RESERVED_WORKER_ID
  );
  assert_ne!(
    merged.get_node_id_from_slot(UNOWNED_SLOT as u16),
    Some(primary_id)
  );
}

/// test/cluster/Garnet.test.cluster/ClusterConfigTests.cs:ClusterConfigMergeSlotMapReplicaSenderCannotStealOwnedSlotTest
#[test]
fn cluster_config_merge_slot_map_replica_sender_cannot_steal_owned_slot_test() {
  const OWNED_SLOT: usize = 400;
  let (replica, replica_id, primary_id) = create_replica_sender(10, 30, &[OWNED_SLOT]);
  let mut receiver = create_receiver_aware_of(&replica, &[OWNED_SLOT]);

  let wid = receiver.get_worker_id_from_node_id(primary_id);
  receiver.update_slot_state(OWNED_SLOT, wid, SlotState::Stable);

  let merged = receiver
    .merge(&replica, &new_concurrent_map())
    .unwrap_or_else(|| receiver.clone());

  assert_eq!(
    merged.get_node_id_from_slot(OWNED_SLOT as u16),
    Some(primary_id)
  );
  assert_ne!(
    merged.get_node_id_from_slot(OWNED_SLOT as u16),
    Some(replica_id)
  );
}

/// test/cluster/Garnet.test.cluster/ClusterConfigTests.cs:ClusterConfigMergeSlotMapReplicaSenderHandsOffOwnedSlotToPrimaryTest
#[test]
fn cluster_config_merge_slot_map_replica_sender_hands_off_owned_slot_to_primary_test() {
  const HANDOFF_SLOT: usize = 500;
  let (replica, replica_id, primary_id) = create_replica_sender(10, 30, &[HANDOFF_SLOT]);
  let mut receiver = create_receiver_aware_of(&replica, &[HANDOFF_SLOT]);

  let wid = receiver.get_worker_id_from_node_id(replica_id);
  receiver.update_slot_state(HANDOFF_SLOT, wid, SlotState::Stable);

  let merged = receiver.merge(&replica, &new_concurrent_map()).unwrap();

  assert_eq!(
    merged.get_node_id_from_slot(HANDOFF_SLOT as u16),
    Some(primary_id)
  );
  assert_eq!(merged.get_state(HANDOFF_SLOT as u16), SlotState::Stable);
}

/// test/cluster/Garnet.test.cluster/ClusterConfigTests.cs:ClusterConfigMergeSlotMapReplicaHandoffDoesNotLeakToLaterSlotsTest
#[test]
fn cluster_config_merge_slot_map_replica_handoff_does_not_leak_to_later_slots_test() {
  const HANDOFF_SLOT: usize = 600;
  const UNOWNED_SLOT: usize = 700;

  let (replica, replica_id, primary_id) =
    create_replica_sender(10, 30, &[HANDOFF_SLOT, UNOWNED_SLOT]);
  let mut receiver = create_receiver_aware_of(&replica, &[HANDOFF_SLOT, UNOWNED_SLOT]);

  let wid = receiver.get_worker_id_from_node_id(replica_id);
  receiver.update_slot_state(HANDOFF_SLOT, wid, SlotState::Stable);

  let merged = receiver.merge(&replica, &new_concurrent_map()).unwrap();

  assert_eq!(
    merged.get_node_id_from_slot(HANDOFF_SLOT as u16),
    Some(primary_id)
  );
  assert_eq!(
    merged.get_worker_id_from_slot(UNOWNED_SLOT as u16),
    RESERVED_WORKER_ID
  );
}

/// 未知主节点（未在 workers 登记）gossip 不得将槽位改写为 worker 0 且 Stable
#[test]
fn cluster_config_merge_slot_map_unknown_sender_cannot_corrupt_slots_test() {
  let mut receiver = ClusterConfig::new();
  receiver.initialize_local_worker(LocalWorkerSpec {
    node_id: 0x3001,
    address: "127.0.0.1",
    port: 7001,
    config_epoch: 1,
    role: NodeRole::Primary,
    replica_of_node_id: None,
    hostname: Some(""),
  });
  receiver.update_slot_state(100, LOCAL_WORKER_ID as u16, SlotState::Stable);

  let mut unknown_sender = ClusterConfig::new();
  unknown_sender.initialize_local_worker(LocalWorkerSpec {
    node_id: 0x9999,
    address: "127.0.0.1",
    port: 7099,
    config_epoch: 10,
    role: NodeRole::Primary,
    replica_of_node_id: None,
    hostname: Some(""),
  });
  unknown_sender.update_slot_state(100, LOCAL_WORKER_ID as u16, SlotState::Stable);

  let changed = receiver.merge_slot_map(&unknown_sender);
  assert!(!changed);
  assert_eq!(receiver.get_worker_id_from_slot(100), LOCAL_WORKER_ID);
  assert_eq!(receiver.get_state(100), SlotState::Stable);
}

/// 副本 sender 的目标主节点未知（handoff_worker_id == 0）时，不得将槽位交接给 0 号保留节点且置 Stable
#[test]
fn cluster_config_merge_slot_map_replica_with_unknown_primary_cannot_handoff_test() {
  let replica_id = 0x2001;
  let unknown_primary_id = 0x8888;
  const SLOT: usize = 500;

  let mut receiver = ClusterConfig::new();
  receiver.initialize_local_worker(LocalWorkerSpec {
    node_id: 0x3001,
    address: "127.0.0.1",
    port: 7001,
    config_epoch: 1,
    role: NodeRole::Primary,
    replica_of_node_id: None,
    hostname: Some(""),
  });

  let mut replica = ClusterConfig::new();
  replica.initialize_local_worker(LocalWorkerSpec {
    node_id: replica_id,
    address: "127.0.0.1",
    port: 7002,
    config_epoch: 1,
    role: NodeRole::Replica,
    replica_of_node_id: Some(unknown_primary_id),
    hostname: Some(""),
  });
  // 接收方仅认识 replica，不认识 unknown_primary_id
  let mut receiver = receiver.merge(&replica, &new_concurrent_map()).unwrap();
  let replica_wid = receiver.get_worker_id_from_node_id(replica_id);
  receiver.update_slot_state(SLOT, replica_wid, SlotState::Stable);

  let changed = receiver.merge_slot_map(&replica);
  assert!(!changed);
  assert_eq!(
    receiver.get_worker_id_from_slot(SLOT as u16),
    replica_wid as usize
  );
  assert_eq!(receiver.get_state(SLOT as u16), SlotState::Stable);
}

/// 测试节点 id（任意稳定的 u128 值，hex 渲染仅诊断用）
const NODE_A: u128 = 0xA000_0000_0000_0000_0000_0000_0000_0001;
const NODE_B: u128 = 0xB000_0000_0000_0000_0000_0000_0000_0002;
const NODE_Z: u128 = 0xF000_0000_0000_0000_0000_0000_0000_000A;

/// 本地主节点配置（epoch 可指定，便于碰撞仲裁测试）
fn config_with_local(node_id: u128, config_epoch: i64) -> ClusterConfig {
  let mut config = ClusterConfig::new();
  config.initialize_local_worker(LocalWorkerSpec {
    node_id,
    address: "127.0.0.1",
    port: 7000,
    config_epoch,
    role: NodeRole::Primary,
    replica_of_node_id: None,
    hostname: None,
  });
  config
}

/// C# SetLocalWorkerConfigEpoch：仅允许从 0 初始化且新值为正，
/// 二次设置（覆写/倒退/非正）一律拒绝，单调递增只能走 bump
#[test]
fn set_local_epoch_rejects_overwrite_and_regression() {
  let mut config = config_with_local(NODE_A, 0);
  assert!(config.set_local_worker_config_epoch(5));
  // 非 0 现值：更大值也不得覆写
  assert!(!config.set_local_worker_config_epoch(9));
  // 倒退拒绝
  assert!(!config.set_local_worker_config_epoch(1));
  // 非正值拒绝
  assert!(!config.set_local_worker_config_epoch(0));
  assert_eq!(config.local_node_config_epoch(), 5);
}

/// C# BumpLocalNodeConfigEpoch：取全体 worker 最大 epoch + 1
/// （全局最大纪元检查，本地纪元永不倒退到集群水位之下）
#[test]
fn bump_takes_global_max_epoch() {
  let mut config = config_with_local(NODE_A, 5);
  config.workers.push(Worker {
    nodeid: Some(NODE_B),
    config_epoch: 99,
    ..Worker::default()
  });
  config.bump_local_node_config_epoch();
  assert_eq!(config.local_node_config_epoch(), 100);
}

/// C# HandleConfigEpochCollision：等值碰撞且发送方 id 序更大才自愈
/// （提升为全局 max+1 并返回 true 供调用方置脏落盘），否则原样返回 false；
/// 内部身份收敛 u128 后按数值序仲裁（同为确定性全序）
#[test]
fn handle_config_epoch_collision() {
  // epoch 不等：不触发
  let mut local = config_with_local(NODE_A, 5);
  let sender = config_with_local(NODE_B, 6);
  assert!(!local.handle_config_epoch_collision(&sender));
  assert_eq!(local.local_node_config_epoch(), 5);

  // epoch 相等但发送方 id 更小：不触发（双方各退一步避免死循环）
  let mut local = config_with_local(NODE_Z, 5);
  let sender = config_with_local(NODE_B, 5);
  assert!(!local.handle_config_epoch_collision(&sender));
  assert_eq!(local.local_node_config_epoch(), 5);

  // 等值碰撞且发送方 id 更大：自增为全局 max+1
  let mut local = config_with_local(NODE_A, 5);
  let sender = config_with_local(NODE_B, 5);
  assert!(local.handle_config_epoch_collision(&sender));
  assert_eq!(local.local_node_config_epoch(), 6);
}

/// 本地主槽位枚举与故障接管（副本升级后本地槽位清空、主身份解绑）
#[test]
fn get_local_primary_slots_and_takeover() {
  let mut config = ClusterConfig::new();
  config.initialize_local_worker(LocalWorkerSpec {
    node_id: NODE_A,
    address: "127.0.0.1",
    port: 7001,
    config_epoch: 1,
    role: NodeRole::Replica,
    replica_of_node_id: Some(NODE_B),
    hostname: None,
  });
  // 未加 primary (NODE_B) 节点时为空
  assert!(config.get_local_primary_slots().is_empty());

  // 加入 NODE_B
  config.workers.push(Worker {
    nodeid: Some(NODE_B),
    address: HipStr::borrowed("127.0.0.1"),
    port: 7000,
    config_epoch: 1,
    role: NodeRole::Primary,
    ..Worker::default()
  });
  let wid = config.get_worker_id_from_node_id(NODE_B);
  assert_eq!(wid, 2);

  config.slot_map[0].worker_id = wid;
  config.slot_map[5].worker_id = wid;
  config.slot_map[10].worker_id = wid;

  let slots = config.get_local_primary_slots();
  assert_eq!(slots, vec![0, 5, 10]);

  // 故障接管
  config.take_over_from_primary();
  assert_eq!(config.local_node_role(), NodeRole::Primary);
  assert_eq!(config.local_node_primary_id(), None);
  assert_eq!(config.slot_map[0].worker_id, LOCAL_WORKER_ID as u16);
  assert_eq!(config.slot_map[5].worker_id, LOCAL_WORKER_ID as u16);
  assert_eq!(config.slot_map[10].worker_id, LOCAL_WORKER_ID as u16);
  assert!(config.get_local_primary_slots().is_empty());
}

/// merge 封禁跳过分支（对标 C# ClusterConfig.cs:Merge 的 workerBanList 逐
/// worker 点查跳过臂）：非空封禁表 + sender 拓扑含被封禁节点 → merge 后该
/// 节点不得并入；未封禁节点照常并入作对照，证明 merge 本体在跑
#[test]
fn cluster_config_merge_skips_banned_worker_test() {
  const LOCAL_ID: u128 = 0x0DE1_0000_0000_0000_0000_0000_0000_0002;
  const SENDER_ID: u128 = 0x0DE1_0000_0000_0000_0000_0000_0000_0001;
  const BANNED_ID: u128 = 0x0DE1_0000_0000_0000_0000_0000_0000_0004;
  const KEEPER_ID: u128 = 0x0DE1_0000_0000_0000_0000_0000_0000_0005;

  let mut receiver = ClusterConfig::new();
  receiver.initialize_local_worker(LocalWorkerSpec {
    node_id: LOCAL_ID,
    address: "127.0.0.1",
    port: 7002,
    config_epoch: 1,
    role: NodeRole::Primary,
    replica_of_node_id: None,
    hostname: Some(""),
  });

  // sender 拓扑：本地 + 被封禁节点 + 未封禁节点（三方配置逐轮 merge 并入）
  let mut sender = ClusterConfig::new();
  sender.initialize_local_worker(LocalWorkerSpec {
    node_id: SENDER_ID,
    address: "127.0.0.1",
    port: 7001,
    config_epoch: 20,
    role: NodeRole::Primary,
    replica_of_node_id: None,
    hostname: Some(""),
  });
  for (id, port) in [(BANNED_ID, 7004), (KEEPER_ID, 7005)] {
    let mut extra = ClusterConfig::new();
    extra.initialize_local_worker(LocalWorkerSpec {
      node_id: id,
      address: "127.0.0.1",
      port,
      config_epoch: 5,
      role: NodeRole::Primary,
      replica_of_node_id: None,
      hostname: Some(""),
    });
    sender = sender
      .merge(&extra, &new_concurrent_map())
      .unwrap_or(sender);
  }
  assert_ne!(sender.get_worker_id_from_node_id(BANNED_ID), 0);
  assert_ne!(sender.get_worker_id_from_node_id(KEEPER_ID), 0);

  // 非空封禁表：仅封 BANNED_ID（时间戳值不影响判定面）
  let ban_list = new_concurrent_map();
  ban_list.pin().insert(BANNED_ID, 1i64);

  let merged = receiver
    .merge(&sender, &ban_list)
    .expect("未封禁节点在拓扑内，merge 必产生变化");
  assert_eq!(
    merged.get_worker_id_from_node_id(BANNED_ID),
    0,
    "被封禁节点不得并入合并结果"
  );
  assert_ne!(
    merged.get_worker_id_from_node_id(KEEPER_ID),
    0,
    "未封禁节点应照常并入"
  );
}

#[test]
fn test_config_roundtrip() {
  let mut config = ClusterConfig::new();
  config.initialize_local_worker(LocalWorkerSpec {
    node_id: 0x1234_5678_9abc_def0_1234_5678_9abc_def0,
    address: "127.0.0.1",
    port: 7001,
    config_epoch: 42,
    role: NodeRole::Primary,
    replica_of_node_id: None,
    hostname: Some("node1.cluster"),
  });

  config.update_slot_state(100, LOCAL_WORKER_ID as u16, SlotState::Stable);
  config.update_slot_state(101, LOCAL_WORKER_ID as u16, SlotState::Migrating);
  config.update_slot_state(500, LOCAL_WORKER_ID as u16, SlotState::Importing);

  let bytes = config.to_byte_array();
  let decoded = ClusterConfig::from_byte_array(&bytes).expect("decode failed");

  assert_eq!(config.num_workers(), decoded.num_workers());
  assert_eq!(config.local_node_id(), decoded.local_node_id());
  assert_eq!(config.local_node_ip(), decoded.local_node_ip());
  assert_eq!(config.local_node_port(), decoded.local_node_port());
  assert_eq!(
    config.local_node_config_epoch(),
    decoded.local_node_config_epoch()
  );
  assert_eq!(config.local_node_role(), decoded.local_node_role());

  for i in 0..CLUSTER_SLOT_COUNT {
    assert_eq!(config.slot_map[i].worker_id, decoded.slot_map[i].worker_id);
    assert_eq!(config.slot_map[i].state, decoded.slot_map[i].state);
  }
}

/// 非 ASCII 宣告值经 ascii_sanitize 单机制逐字节折 `?`（"café"→"caf??"，
/// $len 随折叠后字节自洽；C# 逐 UTF-16 字符折 `caf?`/$4 系在册渲染分叉，
/// 登记锚见 serializer.rs 头注），杜绝整帧出口套折制造头体错位
#[test]
fn node_info_folds_non_ascii_announce_values() {
  let mut config = ClusterConfig::new();
  config.initialize_local_worker(LocalWorkerSpec {
    node_id: PRIMARY_ID,
    address: "127.0.0.1",
    port: 7001,
    config_epoch: 1,
    role: NodeRole::Primary,
    replica_of_node_id: None,
    hostname: Some("café"),
  });
  let nodes = config.get_node_info(LOCAL_WORKER_ID, &ConnectionInfo::default());
  assert!(
    nodes.contains(",caf??"),
    "非 ASCII 主机名须逐字节折 ?: {nodes}"
  );
}

/// 节点 id 线格式为 u128 纯二进制：roundtrip 后身份不变，
/// RESP 渲染点输出 32 字符小写 hex（仅协议面转字符串）
#[test]
fn test_node_id_binary_wire_and_hex_render() {
  let mut config = ClusterConfig::new();
  let id = 0x0123_4567_89ab_cdef_0fed_cba9_8765_4321u128;
  config.initialize_local_worker(LocalWorkerSpec {
    node_id: id,
    address: "127.0.0.1",
    port: 7001,
    config_epoch: 1,
    role: NodeRole::Primary,
    replica_of_node_id: None,
    hostname: None,
  });

  let decoded = ClusterConfig::from_byte_array(&config.to_byte_array()).expect("decode failed");
  assert_eq!(decoded.local_node_id(), Some(id));
  // CLUSTER NODES 渲染面：身份以 32 字符小写 hex 呈现
  let nodes = decoded.get_node_info(LOCAL_WORKER_ID, &ConnectionInfo::default());
  assert!(
    nodes.starts_with("0123456789abcdef0fedcba987654321 127.0.0.1:7001"),
    "hex 渲染不符: {nodes}"
  );
}

/// 接收方 + 已知远端节点（nodeid @ endpoint, epoch）标准形装配：
/// 接收方本地位 0x3000_0003 @ 7003，先 merge 一次把远端条目按更高 epoch 入库
fn receiver_with_known_node(node_id: u128, address: &str, port: i32, epoch: i64) -> ClusterConfig {
  let mut receiver = ClusterConfig::new();
  receiver.initialize_local_worker(LocalWorkerSpec {
    node_id: 0x3000_0003,
    address: "127.0.0.1",
    port: 7003,
    config_epoch: 1,
    role: NodeRole::Primary,
    replica_of_node_id: None,
    hostname: Some(""),
  });

  let mut known = ClusterConfig::new();
  known.initialize_local_worker(LocalWorkerSpec {
    node_id,
    address,
    port,
    config_epoch: epoch,
    role: NodeRole::Primary,
    replica_of_node_id: None,
    hostname: Some(""),
  });
  receiver
    .merge(&known, &new_concurrent_map())
    .expect("首次合并入库远端条目")
}

/// 单节点配置装配：该节点本地位（线格式 1 号位）即 node_id @ endpoint
fn single_node_config(
  node_id: u128,
  address: &str,
  port: i32,
  epoch: i64,
  role: NodeRole,
  hostname: &str,
) -> ClusterConfig {
  let mut config = ClusterConfig::new();
  config.initialize_local_worker(LocalWorkerSpec {
    node_id,
    address,
    port,
    config_epoch: epoch,
    role,
    replica_of_node_id: (role == NodeRole::Replica).then_some(0x3000_0003),
    hostname: Some(hostname),
  });
  config
}

/// 等值 epoch + owner（条目即发送方自身）端点漂移自愈：C# MergeWorkerInfo
/// （libs/cluster/Server/ClusterConfig.cs:1150-1166）等值 epoch 端点更新通道，
/// 节点以相同 epoch 重启且宣告端点漂移后，对端配置经 gossip merge 收敛到新端点
#[test]
fn merge_equal_epoch_owner_endpoint_drift_heals_test() {
  const DRIFTED_ID: u128 = 0x0DE1_0000_0000_0000_0000_0000_0000_000A;

  // 接收方视图：drifted 节点旧端点（epoch 7）
  let receiver = receiver_with_known_node(DRIFTED_ID, "192.168.1.10", 7001, 7);

  // drifted 节点 IP 漂移重启：恢复配置保 epoch 不变，宣告端点已换新
  let restarted = single_node_config(
    DRIFTED_ID,
    "192.168.1.20",
    7002,
    7,
    NodeRole::Primary,
    "drift.domain",
  );

  let merged = receiver
    .merge(&restarted, &new_concurrent_map())
    .expect("等值 epoch owner 端点漂移须产生更新");

  let (address, port) = merged.get_worker_address_from_node_id(DRIFTED_ID);
  assert_eq!(
    (address.as_deref(), port),
    (Some("192.168.1.20"), 7002),
    "端点须自愈到漂移后新值"
  );
  // 拨号视图同步收敛（对端重启后按新端点重连，杜绝人工重新 MEET）
  assert_eq!(
    merged.get_endpoint_from_node_id(DRIFTED_ID),
    Some("192.168.1.20:7002".parse::<SocketAddr>().unwrap())
  );
  // 等值路径只覆盖端点：epoch/role 不变（C# 1160-1166 仅写端点字段）
  let healed = merged.get_worker_from_node_id(DRIFTED_ID).expect("在册");
  assert_eq!(healed.config_epoch, 7);
  assert_eq!(healed.role, NodeRole::Primary);
  // 主机名随端点一并自愈
  assert_eq!(
    healed.hostname.as_deref(),
    Some("drift.domain"),
    "等值 epoch 自愈须覆盖 hostname"
  );
}

/// 等值 epoch + 非 owner（第三方转述条目）端点漂移拒绝：C# 1152-1154
/// 「仅 owner 可替换已知端点」，转述的漂移端点不入库，本地保持旧端点
#[test]
fn merge_equal_epoch_non_owner_endpoint_drift_rejected_test() {
  const DRIFTED_ID: u128 = 0x0DE1_0000_0000_0000_0000_0000_0000_000A;
  const RELAY_ID: u128 = 0x0DE1_0000_0000_0000_0000_0000_0000_000B;

  // 接收方视图：drifted 节点旧端点（epoch 7）
  let receiver = receiver_with_known_node(DRIFTED_ID, "192.168.1.10", 7001, 7);

  // 中继节点视图：自身条目 + drifted 节点的过期端点转述（同为 epoch 7）
  let mut relay = ClusterConfig::new();
  relay.initialize_local_worker(LocalWorkerSpec {
    node_id: RELAY_ID,
    address: "192.168.1.30",
    port: 7004,
    config_epoch: 1,
    role: NodeRole::Primary,
    replica_of_node_id: None,
    hostname: Some(""),
  });
  let stale_claim = single_node_config(DRIFTED_ID, "192.168.1.99", 9999, 7, NodeRole::Primary, "");
  let relay = relay.merge(&stale_claim, &new_concurrent_map()).unwrap();

  let merged = receiver.merge(&relay, &new_concurrent_map()).unwrap();

  // 中继自身条目照常入库，但 drifted 节点端点保持接收方原值
  let (address, port) = merged.get_worker_address_from_node_id(DRIFTED_ID);
  assert_eq!(
    (address.as_deref(), port),
    (Some("192.168.1.10"), 7001),
    "非 owner 等值 epoch 端点漂移须被拒"
  );
  assert_eq!(
    merged.get_worker_node_id_from_address_or_hostname("192.168.1.99", 9999),
    None,
    "转述的漂移端点不得入库"
  );
}

/// 等值 epoch + owner 端点全等零变化：merge 返回 None（无落盘），
/// 且仅 role 差异不触发等值更新（C# 1160-1166 等值路径只写端点字段）
#[test]
fn merge_equal_epoch_owner_identical_endpoints_no_op_test() {
  const KNOWN_ID: u128 = 0x0DE1_0000_0000_0000_0000_0000_0000_000C;

  let receiver = receiver_with_known_node(KNOWN_ID, "192.168.1.10", 7001, 7);

  // 端点全等：零变化
  let same = single_node_config(KNOWN_ID, "192.168.1.10", 7001, 7, NodeRole::Primary, "");
  assert!(receiver.merge(&same, &new_concurrent_map()).is_none());

  // 仅 role 降级（Primary→Replica）等值不传播：role 不在端点字段集内
  let demoted = single_node_config(KNOWN_ID, "192.168.1.10", 7001, 7, NodeRole::Replica, "");
  assert!(receiver.merge(&demoted, &new_concurrent_map()).is_none());
  let view = receiver.get_worker_from_node_id(KNOWN_ID).expect("在册");
  assert_eq!(view.role, NodeRole::Primary);
}

/// 线格式探针：与 serializer.rs 私有 ConfigWire 同构（字段序/型一致即字节同形），
/// 用于构造带越界端口的畸形载荷
#[derive(Encode)]
struct ProbeConfigWire<'a> {
  segments: Vec<ProbeSlotSegmentWire>,
  workers: Vec<ProbeWorkerWire<'a>>,
}

#[derive(Encode)]
struct ProbeSlotSegmentWire {
  count: u16,
  worker_id: u16,
  state: u8,
}

#[derive(Encode)]
struct ProbeWorkerWire<'a> {
  nodeid: Option<u128>,
  address: &'a str,
  port: i32,
  config_epoch: i64,
  role: NodeRole,
  replica_of_node_id: Option<u128>,
  replication_offset: i64,
  hostname: Option<&'a str>,
}

/// 畸形端口拒载（解码门 fail-loud）：负数与 >65535 的端口放行后会在端点换算
/// `as u16` 静默回绕（-1→65535、70000→4464），C# 同场景 IPEndPoint 构造抛
/// ArgumentOutOfRange；边界值 0/65535 合法放行
#[test]
fn from_byte_array_rejects_out_of_range_worker_port_test() {
  let wire = |port: i32| ProbeConfigWire {
    segments: Vec::new(),
    workers: vec![ProbeWorkerWire {
      nodeid: Some(0x12_34),
      address: "127.0.0.1",
      port,
      config_epoch: 1,
      role: NodeRole::Primary,
      replica_of_node_id: None,
      replication_offset: 0,
      hostname: Some(""),
    }],
  };
  let payload = |port: i32| {
    let mut bytes = vec![CLUSTER_CONFIG_VERSION];
    bytes.extend_from_slice(&bitcode::encode(&wire(port)));
    bytes
  };

  assert!(
    matches!(
      ClusterConfig::from_byte_array(&payload(-1)),
      Err(Error::WorkerPort(-1))
    ),
    "负端口须被解码门拒载"
  );
  assert!(
    matches!(
      ClusterConfig::from_byte_array(&payload(70000)),
      Err(Error::WorkerPort(70000))
    ),
    "超界端口 70000 须被解码门拒载"
  );

  // 边界值 0 与 65535 合法放行
  for port in [0, 65535] {
    let decoded = ClusterConfig::from_byte_array(&payload(port)).expect("边界端口须放行");
    assert_eq!(decoded.local_node_port(), port);
  }
}
