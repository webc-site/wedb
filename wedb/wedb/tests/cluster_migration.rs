use std::{io, sync::Arc};

use aok::Void;
use compio::runtime::Runtime;
use gxhash::HashSet;
use parking_lot::Mutex;
use wbase::hash_slot::hash_slot as cluster_slot;
use wedb::{
  error::Error,
  server::{
    cluster_manager::ClusterManager,
    cluster_manager_slot_state::SlotStorageFace,
    cluster_provider::ClusterProvider,
    hash_slot::SlotState,
    migration::{
      migrate_session::{MigrateSession, MigrateTaskSpec},
      migration_manager::MigrationManager,
      sketch::Sketch,
      sketch_status::SketchStatus,
    },
    worker::{LocalWorkerSpec, NodeRole, Worker},
  },
};

struct MockSlotStorage {
  deleted_slots: Mutex<Vec<u16>>,
}

impl SlotStorageFace for MockSlotStorage {
  async fn delete_slot_keys(&self, slots: &[u16]) -> io::Result<u64> {
    self.deleted_slots.lock().extend_from_slice(slots);
    Ok(slots.len() as u64)
  }
}

/// test/cluster/Garnet.test.cluster.migrate/ClusterMigrateTests.cs:ClusterDelKeysInSlotRemovesStringAndObjectKeys
#[test]
fn cluster_del_keys_in_slot_removes_string_and_object_keys() -> Void {
  Runtime::new()?.block_on(async {
    let mock = MockSlotStorage {
      deleted_slots: Mutex::new(Vec::new()),
    };

    let key1 = b"del_slot_user_1";
    let key2 = b"del_slot_user_2";
    let slot1 = cluster_slot(key1);
    let slot2 = cluster_slot(key2);

    let deleted = ClusterManager::delete_keys_in_slots(&mock, &[slot1]).await?;
    assert_eq!(deleted, 1);
    assert_eq!(*mock.deleted_slots.lock(), vec![slot1]);

    if slot1 != slot2 {
      let deleted2 = ClusterManager::delete_keys_in_slots(&mock, &[slot2]).await?;
      assert_eq!(deleted2, 1);
      assert_eq!(*mock.deleted_slots.lock(), vec![slot1, slot2]);
    }

    aok::OK
  })
}

/// test/cluster/Garnet.test.cluster.migrate/ClusterMigrateTests.cs:ClusterSlotChangeStatus
#[test]
fn cluster_slot_change_status() -> Void {
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
    config.workers.push(Worker {
      nodeid: Some("remote_node".to_string()),
      address: "127.0.0.1".to_string(),
      port: 7001,
      config_epoch: 1,
      role: NodeRole::Primary,
      replica_of_node_id: None,
      replication_offset: 0,
      hostname: None,
    });
  }

  let mut slots = HashSet::default();
  slots.insert(200);
  m.try_add_slots(&slots)?;

  // 1. Prepare migration to self rejected
  assert!(matches!(
    m.try_prepare_slot_for_migration(200, "local_node"),
    Err(Error::MigrateToMyself)
  ));

  // 2. Prepare migration to unknown node rejected
  assert!(matches!(
    m.try_prepare_slot_for_migration(200, "unknown_node"),
    Err(Error::NodeNotFound(_))
  ));

  // 3. Prepare migration to remote node
  m.try_prepare_slot_for_migration(200, "remote_node")?;
  assert_eq!(m.current_config.read().get_state(200), SlotState::Migrating);

  // 4. Reset slot state
  m.try_reset_slot_state(200);
  assert_eq!(m.current_config.read().get_state(200), SlotState::Stable);

  // 5. Prepare slot for ownership change
  m.try_prepare_slot_for_migration(200, "remote_node")?;
  m.try_prepare_slot_for_ownership_change(200, "remote_node")?;
  assert_eq!(m.current_config.read().get_state(200), SlotState::Stable);
  let remote_wid = m
    .current_config
    .read()
    .get_worker_id_from_node_id("remote_node");
  assert_eq!(
    m.current_config.read().get_worker_id_from_slot(200),
    remote_wid as usize
  );

  aok::OK
}

/// test/cluster/Garnet.test.cluster.migrate/ClusterMigrateTests.cs:ClusterSimpleMigrateSlots
#[test]
fn cluster_simple_migrate_slots() {
  let mgr = MigrationManager::new(Arc::new(ClusterProvider::default()));
  assert_eq!(mgr.get_migration_task_count(), 0);

  let spec = MigrateTaskSpec {
    source_node_id: "src",
    target_address: "10.0.0.2",
    target_port: 7002,
    target_node_id: "dst",
    username: "",
    passwd: "",
    copy_option: false,
    replace_option: false,
    timeout: 0,
  };

  let slots: HashSet<i32> = [1, 2].into_iter().collect();
  let sketch = Sketch::new();
  let session = mgr
    .try_add_migration_task(spec, slots, sketch)
    .expect("add migration task");
  assert_eq!(mgr.get_migration_task_count(), 1);

  session.sketch.hash_and_store(b"foo");

  // Key accessibility in sketch states
  assert!(session.can_access_key(b"foo", 1, false));
  assert!(session.can_access_key(b"foo", 1, true));

  session.sketch.set_status(SketchStatus::Transmitting);
  assert!(!session.can_access_key(b"foo", 1, false));
  assert!(session.can_access_key(b"foo", 1, true));
  assert!(session.can_access_key(b"bar", 1, false));

  session.sketch.set_status(SketchStatus::Deleting);
  assert!(!session.can_access_key(b"foo", 1, false));
  assert!(!session.can_access_key(b"foo", 1, true));
  assert!(session.can_access_key(b"bar", 1, false));

  // Remove node
  assert!(mgr.try_remove_migration_task_node("dst"));
  assert_eq!(mgr.get_migration_task_count(), 0);

  mgr.dispose();
}

/// test/cluster/Garnet.test.cluster.migrate/ClusterMigrateTests.cs:ClusterSketchBloomFilterTest
#[test]
fn cluster_sketch_bloom_filter_test() {
  let sketch = Sketch::with_key_count(1024);

  // Probe before insertion
  let (exists, status) = sketch.probe(b"user:1001");
  assert!(!exists);
  assert_eq!(status, SketchStatus::Initializing);

  // TryHashAndStore
  assert!(sketch.try_hash_and_store(b"user:1001"));
  let (exists, status) = sketch.probe(b"user:1001");
  assert!(exists);
  assert_eq!(status, SketchStatus::Initializing);

  // Update status reflects in probe
  sketch.set_status(SketchStatus::Transmitting);
  let (exists, status) = sketch.probe(b"user:1001");
  assert!(exists);
  assert_eq!(status, SketchStatus::Transmitting);

  // HashAndStore another key
  sketch.hash_and_store(b"user:1002");
  let (exists, status) = sketch.probe(b"user:1002");
  assert!(exists);
  assert_eq!(status, SketchStatus::Transmitting);

  // Clear resets bitmap and status
  sketch.clear();
  let (exists, status) = sketch.probe(b"user:1001");
  assert!(!exists);
  assert_eq!(status, SketchStatus::Initializing);
  let (exists, _) = sketch.probe(b"user:1002");
  assert!(!exists);
}

/// test/cluster/Garnet.test.cluster.migrate/ClusterMigrateTests.cs:ClusterMigrateSessionMethodsTest
#[test]
fn cluster_migrate_session_methods_test() -> Void {
  let cp = ClusterProvider::new();
  let cm = cp.cluster_manager().unwrap();
  {
    let mut config = cm.current_config.write();
    config.initialize_local_worker(LocalWorkerSpec {
      node_id: "local_node",
      address: "127.0.0.1",
      port: 7000,
      config_epoch: 1,
      role: NodeRole::Primary,
      replica_of_node_id: None,
      hostname: None,
    });
    config.workers.push(Worker {
      nodeid: Some("target_node".to_string()),
      address: "127.0.0.1".to_string(),
      port: 7001,
      config_epoch: 1,
      role: NodeRole::Primary,
      replica_of_node_id: None,
      replication_offset: 0,
      hostname: None,
    });
  }

  let slots_to_add: HashSet<usize> = [10, 11, 12, 20].into_iter().collect();
  cm.try_add_slots(&slots_to_add)?;

  let spec = MigrateTaskSpec {
    source_node_id: "local_node",
    target_address: "127.0.0.1",
    target_port: 7001,
    target_node_id: "target_node",
    username: "",
    passwd: "",
    copy_option: false,
    replace_option: false,
    timeout: 0,
  };

  let mut session = MigrateSession::new(
    Arc::clone(&cp),
    spec,
    [10, 11, 12, 20].into_iter().collect(),
    Sketch::new(),
  );

  // 1. get_ranges: [(10, 12), (20, 20)]
  let ranges = session.get_ranges();
  assert_eq!(ranges, vec![(10, 12), (20, 20)]);

  // 2. overlap check
  let other_spec = MigrateTaskSpec {
    source_node_id: "local_node",
    target_address: "127.0.0.1",
    target_port: 7001,
    target_node_id: "target_node",
    username: "",
    passwd: "",
    copy_option: false,
    replace_option: false,
    timeout: 0,
  };
  let session_overlap = MigrateSession::new(
    Arc::clone(&cp),
    other_spec,
    [12, 30].into_iter().collect(),
    Sketch::new(),
  );
  assert!(session.overlap(&session_overlap));

  // 3. try_prepare_local_for_migration transitions slots to Migrating
  assert!(session.try_prepare_local_for_migration());
  assert_eq!(cm.current_config.read().get_state(10), SlotState::Migrating);
  assert_eq!(cm.current_config.read().get_state(11), SlotState::Migrating);

  // 4. reset_local_slot returns slots to Stable
  session.reset_local_slot();
  assert_eq!(cm.current_config.read().get_state(10), SlotState::Stable);

  // 5. relinquish_ownership moves ownership to target_node
  assert!(session.try_prepare_local_for_migration());
  assert!(session.relinquish_ownership());
  let target_wid = cm
    .current_config
    .read()
    .get_worker_id_from_node_id("target_node");
  assert_eq!(
    cm.current_config.read().get_worker_id_from_slot(10),
    target_wid as usize
  );

  // 6. Sketch keys tracking
  let sketch = Sketch::new();
  sketch.hash_and_store(b"key_a");
  sketch.hash_and_store(b"key_b");
  assert_eq!(sketch.keys().len(), 2);
  assert_eq!(sketch.keys()[0].0, b"key_a");
  assert_eq!(sketch.keys()[1].0, b"key_b");
  sketch.clear();
  assert!(sketch.keys().is_empty());

  aok::OK
}
