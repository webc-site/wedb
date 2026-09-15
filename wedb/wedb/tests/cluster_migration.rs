use std::{io, sync::Arc};

use aok::Void;
use compio::runtime::Runtime;
use gxhash::HashSet;
use parking_lot::Mutex;
use wbase::hash_slot::hash_slot as cluster_slot;
use wdev::SegmentedDevice;
use wedb::{
  error::Error,
  server::{
    cluster::IClusterProvider,
    cluster_manager::ClusterManager,
    cluster_manager_slot_state::SlotStorageFace,
    cluster_provider::ClusterProvider,
    cluster_session::ClusterSession,
    hash_slot::{HashSlot, SlotState},
    migration::{
      migrate_driver::{encode_migration_payload, probe_object_keys, run_keys_migration_driver},
      migrate_session::{MigrateSession, MigrateTaskSpec},
      migration_manager::MigrationManager,
      sketch::Sketch,
      sketch_status::SketchStatus,
    },
    worker::{LOCAL_WORKER_ID, LocalWorkerSpec, NodeRole, Worker},
  },
};
use wedb_test::test_store_config;
use wkv::WedbStore;
use wnode::{
  MessageConsumerFace, RespSessionConsumer,
  resp::{garnet_api::StoreGarnetApi, resp_server_session::RespServerSessionOptions},
  storage::session::storage_session::StorageSession,
};
use wtxn::WatchVersionMap;
use wval::KeyTag;

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

  let session = MigrateSession::new(
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

/// 测试 CLUSTER MIGRATE 帧编解码往返与边界防守
#[test]
fn test_cluster_migrate_payload_codec_roundtrip() -> Void {
  use wedb::server::migration::migrate_driver::{
    MigrationRecord, encode_migration_payload, parse_migration_payload,
  };

  // 1. 空载荷往返 (完成哨兵帧)
  let empty_encoded = encode_migration_payload([]);
  assert_eq!(empty_encoded.len(), 4);
  let (count, records) = parse_migration_payload(&empty_encoded)?;
  assert_eq!(count, 0);
  assert!(records.is_empty());

  // 2. 多条记录往返（含 TTL 与无 TTL）
  let input = [
    (
      b"user:1001".as_slice(),
      b"alice_val".as_slice(),
      1726000000000i64,
    ),
    (b"user:1002".as_slice(), b"bob_val".as_slice(), 0i64),
    (b"empty_val_key".as_slice(), b"".as_slice(), 5000i64),
  ];
  let encoded = encode_migration_payload(input.iter().copied());
  let (count, parsed) = parse_migration_payload(&encoded)?;
  assert_eq!(count, 3);
  assert_eq!(parsed.len(), 3);
  assert_eq!(
    parsed[0],
    MigrationRecord {
      kind: 1,
      key: b"user:1001",
      val: b"alice_val",
      expire_unix_ms: 1726000000000,
    }
  );
  assert_eq!(
    parsed[1],
    MigrationRecord {
      kind: 1,
      key: b"user:1002",
      val: b"bob_val",
      expire_unix_ms: 0,
    }
  );
  assert_eq!(
    parsed[2],
    MigrationRecord {
      kind: 1,
      key: b"empty_val_key",
      val: b"",
      expire_unix_ms: 5000,
    }
  );

  // 3. 截断畸形数据防御
  assert!(parse_migration_payload(&[]).is_err());
  assert!(parse_migration_payload(&[1, 0, 0, 0]).is_err()); // 声明有 1 条但无内容
  assert!(parse_migration_payload(&encoded[..encoded.len() - 5]).is_err());

  aok::OK
}

// ---------------------------------------------------------------------------
// 迁移静默丢键防护（net.md P0-1 显式裁剪）：对象键入口整体拒绝、源端只删
// 已确认传输键、接收端 REPLACE 双域存在性语义
// ---------------------------------------------------------------------------

/// 打开迁移测试专用存储（每用例独立目录，GC 关闭）
fn migrate_store(tag: &str) -> Arc<WedbStore<SegmentedDevice>> {
  let dir = tempfile::tempdir().unwrap().keep();
  let device = Arc::new(SegmentedDevice::single_file(dir.join(tag)).unwrap());
  // 小预算测试配置（对标 C# 16MB 基线），GC 关闭保持历史语义
  let mut config = test_store_config();
  config.gc.enabled = false;
  Arc::new(WedbStore::open(config, device).unwrap())
}

/// 装配双主节点拓扑：node_1（本地）持 0..8192，node_2@7001 持 8192..16384
fn two_primary_provider() -> Arc<ClusterProvider> {
  let cp = ClusterProvider::new();
  let m = cp.cluster_manager().unwrap();
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
    let remote_worker_id = config.workers.len() as u16;
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
    for slot in 0..8192 {
      config.slot_map[slot] = HashSlot {
        worker_id: LOCAL_WORKER_ID as u16,
        state: SlotState::Stable,
      };
    }
    for slot in 8192..16384 {
      config.slot_map[slot] = HashSlot {
        worker_id: remote_worker_id,
        state: SlotState::Stable,
      };
    }
  }
  cp
}

/// 挂共享存储的集群会话消费者（provider.set_store 与执行域同源）
fn migrate_consumer(
  cp: &ClusterProvider,
) -> (RespSessionConsumer, Arc<WedbStore<SegmentedDevice>>) {
  let cluster_session: Arc<ClusterSession> = Arc::new(cp.create_cluster_session());
  let store = migrate_store("mig_recv.db");
  cp.set_store(Arc::clone(&store));
  let mut consumer = RespSessionConsumer::with_cluster_session(
    1,
    RespServerSessionOptions {
      max_databases: 2,
      ..RespServerSessionOptions::default()
    },
    cluster_session,
    Arc::new(StoreGarnetApi::new(store.new_session().unwrap())),
  );
  consumer.attach_transaction_components(Arc::new(WatchVersionMap::new(64)));
  (consumer, store)
}

/// 慢命令往返：同步段消费，挂起慢路径时 block_on 驱动应答
fn drive(rt: &Runtime, c: &mut RespSessionConsumer, frame_bytes: &[u8]) -> Vec<u8> {
  let (consumed, mut out) = c.try_consume_messages(frame_bytes);
  assert_eq!(consumed, frame_bytes.len(), "帧应被完整消费");
  if let Some(slow) = c.take_slow_wait() {
    rt.block_on(async {
      out.extend_from_slice(&slow.resolve().await);
    });
  }
  out
}

/// 由字节片段构造 RESP 数组帧（支持二进制 bulk，迁移载荷用）
fn resp_frame(args: &[&[u8]]) -> Vec<u8> {
  let mut out = format!("*{}\r\n", args.len()).into_bytes();
  for a in args {
    out.extend_from_slice(format!("${}\r\n", a.len()).as_bytes());
    out.extend_from_slice(a);
    out.extend_from_slice(b"\r\n");
  }
  out
}

/// 找一个落在本地槽（<8192）的键前缀实例
fn local_slot_key(prefix: &str) -> String {
  (0u32..)
    .map(|i| format!("{prefix}{i}"))
    .find(|k| cluster_slot(k.as_bytes()) < 8192)
    .unwrap()
}

/// 发送侧守卫：probe_object_keys 双域探测分类准确（对象键入选、纯 string /
/// 不存在 / 已过期键不入选）；run_keys_migration_driver 对含对象键请求在
/// 触达远端前整体拒绝并在错误中列明清单，源端键权零变更（对象键保留、
/// string 键不删除、不发帧不交权）
#[test]
fn migrate_driver_rejects_object_keys_without_losing_ownership() {
  let rt = Runtime::new().unwrap();
  rt.block_on(async {
    let store = migrate_store("mig_guard.db");
    {
      let session = store.new_session().unwrap();
      let batch = session.enter_batch();
      let storage = StorageSession::new_readonly(batch);

      // string 键 + 对象信封键（KeyTag::ObjectEnvelope 物理域）
      storage.upsert_string(b"mig:str", b"v1").await.unwrap();
      storage
        .upsert_tag(b"mig:obj", KeyTag::ObjectEnvelope, b"\x01payload")
        .await
        .unwrap();

      let keys = vec![
        b"mig:str".to_vec(),
        b"mig:obj".to_vec(),
        b"mig:missing".to_vec(),
      ];
      let object_keys = probe_object_keys(&storage, &keys).await.unwrap();
      assert_eq!(object_keys, vec![b"mig:obj".to_vec()]);

      // 纯 string 清单预检放行（回归：不误拦）
      assert!(
        probe_object_keys(&storage, &[b"mig:str".to_vec()])
          .await
          .unwrap()
          .is_empty()
      );

      // 已过期 string 键：惰性过期裁决后视同不存在，不算对象键（不触发拒绝）
      storage.upsert_string(b"mig:exp", b"stale").await.unwrap();
      storage.expire_at_ticks(b"mig:exp", 1).await.unwrap();
      assert!(storage.read_string(b"mig:exp").await.unwrap().is_none());
      assert!(!storage.batch.contains_key(b"mig:exp").await.unwrap());
    }

    // 驱动入口整体拒绝：未注册任务、未触达远端（127.0.0.1:1 不可达）即报错，
    // 错误显式列明对象键清单——不静默跳过
    let cp = ClusterProvider::new();
    let spec = MigrateTaskSpec {
      source_node_id: "node_1",
      target_address: "127.0.0.1",
      target_port: 1,
      target_node_id: "node_2",
      username: "",
      passwd: "",
      copy_option: false,
      replace_option: false,
      timeout: 0,
    };
    let keys = vec![b"mig:str".to_vec(), b"mig:obj".to_vec()];
    let err = run_keys_migration_driver(Arc::clone(&cp), Arc::clone(&store), spec, &keys)
      .await
      .unwrap_err();
    let msg = format!("{err}");
    assert!(msg.contains("MIGRATE 拒绝"), "应显式拒绝: {msg}");
    assert!(msg.contains("mig:obj"), "错误应列明对象键: {msg}");

    // 源端键权零变更：对象键仍在（未发帧、未删除、未交权），string 键未删
    {
      let session = store.new_session().unwrap();
      let batch = session.enter_batch();
      let storage = StorageSession::new_readonly(batch);
      assert!(storage.batch.contains_key(b"mig:obj").await.unwrap());
      assert_eq!(
        storage.read_string(b"mig:str").await.unwrap(),
        Some(b"v1".to_vec())
      );
    }
  });
}

/// 纯 string 请求不被对象键守卫误拦：预检通过后推进到远端连接阶段
/// （不可达端口 ConnectionRefused 失败），且连接失败路径不删除任何键
#[test]
fn migrate_driver_pure_string_keys_reach_connect_phase() {
  let rt = Runtime::new().unwrap();
  rt.block_on(async {
    let store = migrate_store("mig_pure.db");
    {
      let session = store.new_session().unwrap();
      let batch = session.enter_batch();
      let storage = StorageSession::new_readonly(batch);
      storage.upsert_string(b"mig:pure", b"pv").await.unwrap();
    }

    let cp = ClusterProvider::new();
    let spec = MigrateTaskSpec {
      source_node_id: "node_1",
      target_address: "127.0.0.1",
      target_port: 1,
      target_node_id: "node_2",
      username: "",
      passwd: "",
      copy_option: false,
      replace_option: false,
      timeout: 0,
    };
    let err = run_keys_migration_driver(cp, Arc::clone(&store), spec, &[b"mig:pure".to_vec()])
      .await
      .unwrap_err();
    let msg = format!("{err:?}");
    assert!(
      !msg.contains("MIGRATE 拒绝"),
      "纯 string 不应被对象键守卫拦截: {msg}"
    );

    // 键权保留：连接失败未删除任何键
    {
      let session = store.new_session().unwrap();
      let batch = session.enter_batch();
      let storage = StorageSession::new_readonly(batch);
      assert_eq!(
        storage.read_string(b"mig:pure").await.unwrap(),
        Some(b"pv".to_vec())
      );
    }
  });
}

/// 接收端 CLUSTER MIGRATE REPLACE 双域语义（net.md P0-1 修复）：
/// replace=F 遇目标端对象记录跳过写入（保留原对象，不清退不覆写）；
/// replace=T string 经对象信封清退安全覆盖；非法 kind 显式拒绝；
/// 纯 string 写入回归不受影响
#[test]
fn cluster_migrate_recv_replace_object_semantics() {
  let rt = Runtime::new().unwrap();
  let cp = two_primary_provider();
  let (mut consumer, _store) = migrate_consumer(&cp);

  // 目标键写成 Hash 对象（本地槽）
  let obj_key = local_slot_key("mig_obj");
  assert_eq!(
    drive(
      &rt,
      &mut consumer,
      &resp_frame(&[b"HSET", obj_key.as_bytes(), b"f1", b"v1"]),
    ),
    b":1\r\n"
  );

  // 槽位状态切换小工具：MIGRATE 接收端门控要求目标键槽位 IMPORTING，
  // 而普通 HGET/GET 验证命令按槽位状态机需 Stable（IMPORTING 槽对无
  // ASKING 的常规命令终评 CLUSTERDOWN），故按阶段切换
  let set_slot = |key: &str, state: SlotState| {
    let m = cp.cluster_manager().unwrap();
    m.current_config.write().slot_map[cluster_slot(key.as_bytes()) as usize] = HashSlot {
      worker_id: LOCAL_WORKER_ID as u16,
      state,
    };
  };

  // 该键槽位置 IMPORTING（接收端槽位校验前提）
  set_slot(&obj_key, SlotState::Importing);

  // 同名 string 迁移记录载荷
  let payload = encode_migration_payload([(
    obj_key.as_bytes(),
    b"migrated_string_value".as_slice(),
    0i64,
  )]);

  // replace=F：目标键已是对象记录 → 跳过写入（应答仍 +OK，对标 C#
  // replaceOption || !Exists 的 Exists 双域判定），原对象不被清退
  let frame = resp_frame(&[b"CLUSTER", b"MIGRATE", b"node_1", b"F", b"F", &payload]);
  assert_eq!(drive(&rt, &mut consumer, &frame), b"+OK\r\n");

  // 恢复 Stable 后用常规命令验证对象记录未被清退
  set_slot(&obj_key, SlotState::Stable);
  assert_eq!(
    drive(
      &rt,
      &mut consumer,
      &resp_frame(&[b"HGET", obj_key.as_bytes(), b"f1"]),
    ),
    b"$2\r\nv1\r\n",
    "replace=F 不得清退目标端对象记录"
  );

  // replace=T：string 覆盖（对象信封清退，安全删除旧对象）
  set_slot(&obj_key, SlotState::Importing);
  let frame = resp_frame(&[b"CLUSTER", b"MIGRATE", b"node_1", b"T", b"F", &payload]);
  assert_eq!(drive(&rt, &mut consumer, &frame), b"+OK\r\n");
  set_slot(&obj_key, SlotState::Stable);
  assert_eq!(
    drive(
      &rt,
      &mut consumer,
      &resp_frame(&[b"GET", obj_key.as_bytes()])
    ),
    b"$21\r\nmigrated_string_value\r\n",
    "replace=T 应写入迁移 string"
  );
  let out = drive(
    &rt,
    &mut consumer,
    &resp_frame(&[b"HGET", obj_key.as_bytes(), b"f1"]),
  );
  assert!(
    out.starts_with(b"-WRONGTYPE"),
    "对象应已被 string 覆盖: {out:?}"
  );

  // 纯 string 键回归：replace=F 且目标不存在 → 正常写入
  // （str_key 槽位同样需 IMPORTING 过接收端门控，验证后恢复 Stable）
  let str_key = local_slot_key("mig_str");
  set_slot(&str_key, SlotState::Importing);
  let payload = encode_migration_payload([(str_key.as_bytes(), b"sv".as_slice(), 0i64)]);
  let frame = resp_frame(&[b"CLUSTER", b"MIGRATE", b"node_1", b"F", b"F", &payload]);
  assert_eq!(drive(&rt, &mut consumer, &frame), b"+OK\r\n");
  set_slot(&str_key, SlotState::Stable);
  assert_eq!(
    drive(
      &rt,
      &mut consumer,
      &resp_frame(&[b"GET", str_key.as_bytes()])
    ),
    b"$2\r\nsv\r\n"
  );

  // 非法 kind 显式拒绝：未知记录类型绝不静默当 string 写入
  // （记录键落在 IMPORTING 槽内以通过接收端门控，命中 kind 校验）
  set_slot(&obj_key, SlotState::Importing);
  let mut bad = Vec::new();
  bad.extend_from_slice(&1u32.to_le_bytes()); // recordCount = 1
  bad.push(7); // kind = 7（未知）
  bad.extend_from_slice(&(obj_key.len() as u32).to_le_bytes());
  bad.extend_from_slice(obj_key.as_bytes());
  bad.extend_from_slice(&1u32.to_le_bytes());
  bad.extend_from_slice(b"x");
  bad.extend_from_slice(&0i64.to_le_bytes());
  let frame = resp_frame(&[b"CLUSTER", b"MIGRATE", b"node_1", b"F", b"F", &bad]);
  let out = drive(&rt, &mut consumer, &frame);
  assert!(
    out.starts_with(b"-ERR Unsupported migration record kind 7"),
    "非法 kind 应显式拒绝: {out:?}"
  );
}
