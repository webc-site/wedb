//! 集群管理集成测试，对标 Garnet ClusterManagementTests
use std::sync::Arc;

use aok::Void;
use wbase::hash_slot::hash_slot as cluster_slot;
use wedb::{
  error::Error,
  server::{
    cluster_config::ClusterPreferredEndpointType,
    cluster_manager::ClusterManager,
    cluster_provider::ClusterProvider,
    hash_slot::SlotState,
    worker::{LocalWorkerSpec, NodeRole, Worker},
  },
};

/// test/cluster/Garnet.test.cluster/ClusterManagementTests.cs:ClusterKeySlotTest
#[test]
fn cluster_key_slot_test() {
  let test_cases: &[(&str, u16)] = &[
    ("6e6bzswz8}", 7038),
    ("8}jb94e7tf", 4828),
    ("{}2xc5pbb7", 11672),
    ("vr{a07}pdt", 12154),
    ("cx{ldv}wdl", 14261),
    ("erv805by}u", 15389),
    ("{ey1pqbij}", 8341),
    ("2tbjjyn}n8", 5152),
    ("t}jehlyo06", 1232),
    ("{u08t}xjal", 2490),
    ("5g{mkb95a}", 3345),
    ("x{v}x70nka", 7761),
    ("g67ikt}q8q", 7694),
    ("ovi8}mn7t7", 14473),
    ("p5ljmg{}8s", 11196),
    ("3wov{fd}8m", 3502),
    ("bxmcjzi3{}", 10246),
    ("{b1rrm7rn}", 14105),
    ("e0{4ylm}78", 5069),
    ("rkptge5}sx", 3468),
    ("o6{uyxsy}j", 3278),
    ("ykd6q{ma8}", 5754),
    ("w{j5pz3iy}", 6520),
    ("mhsr{dm}x0", 15077),
    ("0}dtokfryr", 5134),
    ("h7}0cj9mwm", 8187),
    ("w{jhqd}frk", 5369),
    ("5yzd{6}hzw", 5781),
    ("w6b4vgtzr}", 6045),
    ("4{b17h85}l", 5923),
    ("Hm{W\x13\x1c", 7517),
    ("zyy8yt1chw", 3081),
    ("7858tqv03y", 773),
    ("fdhhuk8yqv", 5763),
    ("8bfgeino4s", 6257),
  ];

  for &(key, expected_slot) in test_cases {
    assert_eq!(
      cluster_slot(key.as_bytes()),
      expected_slot,
      "Key slot mismatch for {key}"
    );
  }
}

/// test/cluster/Garnet.test.cluster/ClusterManagementTests.cs:ClusterForgetTest
#[test]
fn cluster_forget_test() -> Void {
  let cp = Arc::new(ClusterProvider::default());
  let m = ClusterManager::new(cp);
  {
    let mut config = m.current_config.write();
    config.initialize_local_worker(LocalWorkerSpec {
      node_id: "node_1",
      address: "127.0.0.1",
      port: 7000,
      config_epoch: 1,
      role: NodeRole::Primary,
      replica_of_node_id: None,
      hostname: None,
    });
    config.workers.push(Worker {
      nodeid: Some("node_2".to_string()),
      address: "127.0.0.1".to_string(),
      port: 7001,
      config_epoch: 1,
      role: NodeRole::Primary,
      replica_of_node_id: None,
      replication_offset: 0,
      hostname: None,
    });
  }

  // Cannot forget self
  assert!(matches!(
    m.try_remove_worker("node_1", 60),
    Err(Error::CannotForgetMyself)
  ));

  // Forget node_2
  m.try_remove_worker("node_2", 60)?;
  assert!(m.is_banned("node_2"));
  assert!(!m.current_config.read().is_known("node_2"));

  aok::OK
}

/// test/cluster/Garnet.test.cluster/ClusterManagementTests.cs:ClusterResetTest
#[test]
fn cluster_reset_test() -> Void {
  let cp = Arc::new(ClusterProvider::default());
  let m = ClusterManager::new(cp);
  {
    let mut config = m.current_config.write();
    config.initialize_local_worker(LocalWorkerSpec {
      node_id: "node_1",
      address: "127.0.0.1",
      port: 7000,
      config_epoch: 1,
      role: NodeRole::Primary,
      replica_of_node_id: None,
      hostname: None,
    });
  }
  let old_id = m.current_config.read().local_node_id().unwrap().to_string();

  // Soft reset keeps node_id
  m.try_reset(true, 60)?;
  assert_eq!(m.current_config.read().local_node_id().unwrap(), old_id);

  // Hard reset generates fresh node_id and resets config epoch
  m.try_reset(false, 60)?;
  let new_id = m.current_config.read().local_node_id().unwrap().to_string();
  assert_ne!(new_id, old_id);
  assert_eq!(m.current_config.read().local_node_config_epoch(), 0);

  aok::OK
}

/// test/cluster/Garnet.test.cluster/ClusterManagementTests.cs:ClusterSlotsTest
#[test]
fn cluster_slots_test() {
  let cp = Arc::new(ClusterProvider::default());
  let m = ClusterManager::new(cp);
  {
    let mut config = m.current_config.write();
    config.initialize_local_worker(LocalWorkerSpec {
      node_id: "node_1",
      address: "127.0.0.1",
      port: 7000,
      config_epoch: 1,
      role: NodeRole::Primary,
      replica_of_node_id: None,
      hostname: Some("localhost"),
    });
    config.assign_slots(&[0, 1, 2, 8, 9, 10], 1, SlotState::Stable);
  }

  let slots_info = m
    .current_config
    .read()
    .get_slots_info(ClusterPreferredEndpointType::Ip);
  assert!(slots_info.contains(":0\r\n:2\r\n"));
  assert!(slots_info.contains(":8\r\n:10\r\n"));

  assert_eq!(
    ClusterManager::get_range(&[0, 1, 2, 8, 9, 10]),
    "> 0-2 8-10 "
  );
}

/// test/cluster/Garnet.test.cluster/ClusterManagementTests.cs:ClusterSlotRangesTest
#[test]
fn cluster_slot_ranges_test() {
  assert_eq!(
    ClusterManager::get_range(&[0, 1, 2, 5, 9, 10]),
    "> 0-2 5-5 9-10 "
  );
  assert_eq!(ClusterManager::get_range(&[7]), "> 7-7 ");
  assert_eq!(ClusterManager::get_range(&[]), "> ");
}

/// test/cluster/Garnet.test.cluster/ClusterManagementTests.cs:ClusterShardsTest
#[test]
fn cluster_shards_test() {
  let cp = Arc::new(ClusterProvider::default());
  let m = ClusterManager::new(cp);
  {
    let mut config = m.current_config.write();
    config.initialize_local_worker(LocalWorkerSpec {
      node_id: "node_1",
      address: "127.0.0.1",
      port: 7000,
      config_epoch: 1,
      role: NodeRole::Primary,
      replica_of_node_id: None,
      hostname: None,
    });
    config.assign_slots(&[0, 1, 2, 3], 1, SlotState::Stable);
  }

  let shards = m
    .current_config
    .read()
    .get_shards_info(None, ClusterPreferredEndpointType::Ip);
  assert!(shards.contains(":0\r\n:3"));
}

/// test/cluster/Garnet.test.cluster/ClusterManagementTests.cs:ClusterSetSlotBadOptions
#[test]
fn cluster_set_slot_bad_options() {
  let cp = Arc::new(ClusterProvider::default());
  let m = ClusterManager::new(cp);
  {
    let mut config = m.current_config.write();
    config.initialize_local_worker(LocalWorkerSpec {
      node_id: "local_node",
      address: "127.0.0.1",
      port: 7000,
      config_epoch: 1,
      role: NodeRole::Primary,
      replica_of_node_id: None,
      hostname: None,
    });
  }

  // Cannot migrate to myself
  assert!(matches!(
    m.try_prepare_slot_for_migration(100, "local_node"),
    Err(Error::MigrateToMyself)
  ));

  // Unknown node
  assert!(matches!(
    m.try_prepare_slot_for_migration(100, "unknown_node"),
    Err(Error::NodeNotFound(_))
  ));
}
