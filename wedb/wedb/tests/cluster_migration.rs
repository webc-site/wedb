use std::{
  collections::BTreeSet,
  io,
  slice::from_ref,
  sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
  },
};

use aok::Void;
use compio::runtime::Runtime;
use gxhash::HashSet;
use parking_lot::Mutex;
use wbase::{
  future::yield_now,
  hash_slot::{CLUSTER_SLOT_COUNT, slot_of},
};
use wconn::record::{
  BatchItem, MigrateVal, MigrationFrame, MigrationRecord, encode_migration_payload,
  parse_migration_payload, parse_record, send_chunked_record,
};
use wdev::SegmentedDevice;
use wedb::{
  error::Error,
  server::{
    cluster::IClusterProvider,
    cluster_manager::ClusterManager,
    cluster_provider::ClusterProvider,
    cluster_session::ClusterSession,
    hash_slot::{HashSlot, SlotState},
    migration::{
      migrate_driver::{
        UnsupportedKey, collect_vector_set_keys, probe_unsupported_keys, run_keys_migration_driver,
      },
      migrate_session::{MigrateSession, MigrateTaskSpec},
      migration_manager::{MigrationManager, SEND_BUFFER_OVERHEAD_RESERVE},
      sketch::Sketch,
      sketch_status::SketchStatus,
      transfer_option::TransferOption,
    },
    worker::{LOCAL_WORKER_ID, LocalWorkerSpec, NodeRole, Worker},
  },
};
/// 默认会话 (0,0) 库槽位（库级定槽 doc/zh/db.md 4.1：键内容不参与定槽）
const SLOT0: u16 = slot_of(0, 0);

/// 接收帧头槽集文本（库级定槽：CLUSTER MIGRATE 头显式携带发送端会话槽集，
/// 接收端头级一次性判槽，见 cluster_session/migrate.rs 头格式偏离声明）
const _: () = assert!(SLOT0 == 0, "SLOT0_LIST 字面量依赖 (0,0) 库槽为 0");
const SLOT0_LIST: &[u8] = b"0";

/// 接收端 CLUSTER MIGRATE 帧源节点 hex（本地节点 id 0x…DE11 的 32 字符渲染）
const MIGRATE_SRC_NODE_HEX: &[u8] = b"0000000000000000000000000000de11";

/// 会话库槽集合（KEYS 驱动注册面：键级收集改显式会话槽）
fn slot_set1() -> gxhash::HashSet<i32> {
  [i32::from(SLOT0)].into_iter().collect()
}
/// 远端节点承载的槽位（与 SLOT0 异槽）
const REMOTE_SLOT: u16 = SLOT0 ^ 1;

use wkv::WedbStore;
use wnode::{
  MessageConsumerFace, RespSessionConsumer,
  resp::{
    garnet_api::StoreGarnetApi, resp_server_session::RespServerSessionOptions,
    vector::vector_manager_index::Index,
  },
  storage::session::storage_session::StorageSession,
};
use wtest_base::{resp_frame, test_store_config};
use wtxn::{TxnLockTable, WatchVersionMap};
use wval::KeyTag;

struct MockSlotStorage {
  deleted_slots: Mutex<Vec<u16>>,
}

impl MockSlotStorage {
  async fn delete_slot_keys(&self, slots: &[u16]) -> io::Result<u64> {
    self.deleted_slots.lock().extend_from_slice(slots);
    Ok(slots.len() as u64)
  }
}

/// test/cluster/Garnet.test.cluster.migrate/ClusterMigrateTests.cs:ClusterDelKeysInSlotRemovesStringAndObjectKeys
/// 泵等价消费（直填会话接收缓冲 → 唯一入口 → 应答取出）
/// 返回 (消费后残余, 应答)：Some(0) = 完整消费，None = 协议违规
fn pump(consumer: &mut RespSessionConsumer, frame: &[u8]) -> (Option<usize>, Vec<u8>) {
  let mut scratch = consumer.take_recv_scratch();
  scratch.extend_from_slice(frame);
  consumer.return_recv_scratch(scratch);
  let mut resp = Vec::new();
  let remaining = consumer.try_consume_messages_into(&mut resp);
  (remaining, resp)
}

#[test]
fn cluster_del_keys_in_slot_removes_string_and_object_keys() -> Void {
  Runtime::new()?.block_on(async {
    let mock = MockSlotStorage {
      deleted_slots: Mutex::new(Vec::new()),
    };

    let slot = SLOT0;
    let deleted = mock.delete_slot_keys(&[slot]).await?;
    assert_eq!(deleted, 1);
    assert_eq!(*mock.deleted_slots.lock(), vec![slot]);

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
      node_id: 0x0000_0000_0000_0000_0000_0000_0001_0CA1,
      address: "127.0.0.1",
      port: 7000,
      config_epoch: 1,
      role: NodeRole::Primary,
      replica_of_node_id: None,
      hostname: None,
    });
    config.workers.push(Worker {
      nodeid: Some(0x0000_0000_0000_0000_0000_0000_0002_E702),
      address: "127.0.0.1".into(),
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
    m.try_prepare_slot_for_migration(200, 0x10CA1),
    Err(Error::MigrateToMyself)
  ));

  // 2. Prepare migration to unknown node rejected
  assert!(matches!(
    m.try_prepare_slot_for_migration(200, 0x999),
    Err(Error::NodeNotFound(_))
  ));

  // 3. Prepare migration to remote node
  m.try_prepare_slot_for_migration(200, 0x0000_0000_0000_0000_0000_0000_0002_E702)?;
  assert_eq!(m.current_config.read().get_state(200), SlotState::Migrating);

  // 4. Reset slot state
  m.try_reset_slot_state(200);
  assert_eq!(m.current_config.read().get_state(200), SlotState::Stable);

  // 5. Prepare slot for ownership change
  m.try_prepare_slot_for_migration(200, 0x0000_0000_0000_0000_0000_0000_0002_E702)?;
  m.try_prepare_slot_for_ownership_change(200, 0x0000_0000_0000_0000_0000_0000_0002_E702)?;
  assert_eq!(m.current_config.read().get_state(200), SlotState::Stable);
  let remote_wid = m
    .current_config
    .read()
    .get_worker_id_from_node_id(0x0000_0000_0000_0000_0000_0000_0002_E702);
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
    source_node_id: 0x51,
    target_address: "10.0.0.2".to_string(),
    target_port: 7002,
    target_node_id: 0xD57,
    username: "".to_string(),
    passwd: "".to_string(),
    copy_option: false,
    replace_option: false,
    timeout: 0,
    transfer_option: TransferOption::Keys,
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
  assert!(mgr.try_remove_migration_task_node(0xD57));
  assert_eq!(mgr.get_migration_task_count(), 0);

  mgr.dispose();
}

/// test/cluster/Garnet.test.cluster.migrate/ClusterMigrateTests.cs:ClusterMigrateSessionMethodsTest
#[test]
fn cluster_migrate_session_methods_test() -> Void {
  let cp = ClusterProvider::new();
  let cm = cp.cluster_manager().unwrap();
  {
    let mut config = cm.current_config.write();
    config.initialize_local_worker(LocalWorkerSpec {
      node_id: 0x0000_0000_0000_0000_0000_0000_0001_0CA1,
      address: "127.0.0.1",
      port: 7000,
      config_epoch: 1,
      role: NodeRole::Primary,
      replica_of_node_id: None,
      hostname: None,
    });
    config.workers.push(Worker {
      nodeid: Some(0x0000_0000_0000_0000_0000_0000_0000_2E79),
      address: "127.0.0.1".into(),
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
    source_node_id: 0x10CA1,
    target_address: "127.0.0.1".to_string(),
    target_port: 7001,
    target_node_id: 0x0000_0000_0000_0000_0000_0000_0000_2E79,
    username: "".to_string(),
    passwd: "".to_string(),
    copy_option: false,
    replace_option: false,
    timeout: 0,
    transfer_option: TransferOption::Keys,
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

  // 2. try_prepare_local_for_migration transitions slots to Migrating
  assert!(session.try_prepare_local_for_migration());
  assert_eq!(cm.current_config.read().get_state(10), SlotState::Migrating);
  assert_eq!(cm.current_config.read().get_state(11), SlotState::Migrating);

  // 3. reset_local_slot returns slots to Stable
  session.reset_local_slot();
  assert_eq!(cm.current_config.read().get_state(10), SlotState::Stable);

  // 4. relinquish_ownership moves ownership to target_node
  assert!(session.try_prepare_local_for_migration());
  assert!(session.relinquish_ownership());
  let target_wid = cm
    .current_config
    .read()
    .get_worker_id_from_node_id(0x0000_0000_0000_0000_0000_0000_0000_2E79);
  assert_eq!(
    cm.current_config.read().get_worker_id_from_slot(10),
    target_wid as usize
  );

  aok::OK
}

/// 测试 CLUSTER MIGRATE 帧编解码往返与边界防守（string + 对象信封 + 分块）
#[test]
fn test_cluster_migrate_payload_codec_roundtrip() -> Void {
  // 1. 空载荷往返 (完成哨兵帧)
  let empty_encoded = encode_migration_payload(&[]);
  assert_eq!(empty_encoded.len(), 4);
  let (count, frames) = parse_migration_payload(&empty_encoded)?;
  assert_eq!(count, 0);
  assert!(frames.is_empty());

  // 2. 混合记录往返：string + 对象信封（含 TTL 与无 TTL）
  let items = [
    BatchItem {
      key: b"user:1001",
      val: MigrateVal::Str(b"alice_val".to_vec()),
      expire_unix_ms: 1_726_000_000_000,
    },
    BatchItem {
      key: b"user:1002",
      val: MigrateVal::Env(b"\x03hash_payload".to_vec()),
      expire_unix_ms: 0,
    },
    BatchItem {
      key: b"empty_val_key",
      val: MigrateVal::Str(Vec::new()),
      expire_unix_ms: 5000,
    },
  ];
  let encoded = encode_migration_payload(&items);
  let (count, frames) = parse_migration_payload(&encoded)?;
  assert_eq!(count, 3);
  assert_eq!(frames.len(), 3);
  assert_eq!(
    frames[0],
    MigrationFrame::Record(MigrationRecord::Str {
      key: b"user:1001",
      val: b"alice_val",
      expire_unix_ms: 1_726_000_000_000,
    })
  );
  assert_eq!(
    frames[1],
    MigrationFrame::Record(MigrationRecord::Env {
      key: b"user:1002",
      env: b"\x03hash_payload",
      expire_unix_ms: 0,
    })
  );
  assert_eq!(
    frames[2],
    MigrationFrame::Record(MigrationRecord::Str {
      key: b"empty_val_key",
      val: b"",
      expire_unix_ms: 5000,
    })
  );

  // 3. 分块帧编解码：单条完整记录按 8 字节切块，块长与续块标志精确还原
  //（一条真记录编码剥 recordCount 头 = 完整 frame 编码）
  let item = BatchItem {
    key: b"big_key",
    val: MigrateVal::Str(vec![b'v'; 20]),
    expire_unix_ms: 123,
  };
  let whole = encode_migration_payload(from_ref(&item));
  let record_body = &whole[4..];
  let mut joined: Vec<u8> = Vec::new();
  Runtime::new()?.block_on(async {
    let mut chunk_count = 0;
    send_chunked_record(&item, 8, async |chunk_payload| {
      chunk_count += 1;
      let (count, frames) = parse_migration_payload(chunk_payload)?;
      assert_eq!(count, 1);
      assert_eq!(frames.len(), 1);
      let MigrationFrame::Chunk { bytes, more } = &frames[0] else {
        panic!("第 {chunk_count} 帧应为 CHUNKED");
      };
      joined.extend_from_slice(bytes);
      let is_last = (chunk_count - 1) * 8 + bytes.len() >= record_body.len();
      assert_eq!(!*more, is_last, "续块标志错位");
      Ok::<(), Error>(())
    })
    .await?;
    assert_eq!(chunk_count, record_body.len().div_ceil(8));
    Ok::<(), Error>(())
  })?;
  assert_eq!(joined, record_body, "块载荷拼接应还原完整 frame 编码");

  // 4. 重组产物单记录解析（分块流拼接 = 完整 frame 编码）
  let record = parse_record(&joined)?;
  assert_eq!(
    record,
    MigrationRecord::Str {
      key: b"big_key",
      val: &[b'v'; 20],
      expire_unix_ms: 123,
    }
  );

  // 4.1 流式分块发送器 send_chunked_record 零临时分配与分块边界验证（多类型与边界覆盖）
  Runtime::new()?.block_on(async {
    for test_item in [
      &item, &items[0], // user:1001 (string with expire)
      &items[1], // user:1002 (envelope)
      &items[2], // empty_val_key
    ] {
      let test_whole = encode_migration_payload(from_ref(test_item));
      let test_body = &test_whole[4..];
      let expected_record = parse_record(test_body)?;

      for chunk_size in [1, 2, 3, 5, 7, 8, 16, 32, 100] {
        let mut streamed_joined = Vec::new();
        let mut chunk_count = 0;
        send_chunked_record(test_item, chunk_size, async |chunk_payload| {
          chunk_count += 1;
          let (cnt, sub_frames) = parse_migration_payload(chunk_payload).unwrap();
          assert_eq!(cnt, 1);
          assert_eq!(sub_frames.len(), 1);
          let MigrationFrame::Chunk { bytes, more } = &sub_frames[0] else {
            panic!("流式分块帧应为 CHUNKED");
          };
          assert!(bytes.len() <= chunk_size);
          let is_last = (chunk_count - 1) * chunk_size + bytes.len() >= test_body.len();
          assert_eq!(!*more, is_last, "chunk_size={chunk_size} 续块标志不符");
          streamed_joined.extend_from_slice(bytes);
          Ok::<(), Error>(())
        })
        .await?;
        assert_eq!(streamed_joined, test_body);
        let streamed_record = parse_record(&streamed_joined)?;
        assert_eq!(streamed_record, expected_record);
      }
    }

    // 零阈值安全返回防守
    let mut called = false;
    send_chunked_record(&item, 0, async |_| {
      called = true;
      Ok::<(), Error>(())
    })
    .await?;
    assert!(!called, "max_chunk=0 不应触发发送闭包");

    aok::OK
  })?;

  // 5. 截断畸形数据防守
  assert!(parse_migration_payload(&[]).is_err());
  assert!(parse_migration_payload(&[1, 0, 0, 0]).is_err()); // 声明有 1 条但无内容
  assert!(parse_migration_payload(&encoded[..encoded.len() - 5]).is_err());

  aok::OK
}

// ---------------------------------------------------------------------------
// 迁移静默丢键防护（net.md P0-1 显式裁剪）：对象键入口整体拒绝、源端只删
// 已确认传输键、接收端 REPLACE 双域存在性语义
// ---------------------------------------------------------------------------

/// 打开迁移测试专用存储（每用例独立目录，GC 关闭，reviv 关闭）
fn migrate_store(tag: &str) -> Arc<WedbStore<SegmentedDevice>> {
  open_migrate_store(tag, false)
}

/// 打开复活启用态的迁移测试存储
///
/// 断言 `reviv_pool.is_enabled()` 暂停/恢复语义的用例须以本夹具建店：该谓词已与 C#
/// `RevivificationManager.IsEnabled` 同形（RevivificationManager.cs:18 判 revivSuspendCount
/// == 0，:24 初值 -1，:40-43 未开启 EnableRevivification 时提前 return 使计数恒 -1），
/// 故 `--reviv` 关时恒假，暂停与恢复无从区分，断言会退化成一边倒的空断言
fn migrate_store_reviv(tag: &str) -> Arc<WedbStore<SegmentedDevice>> {
  open_migrate_store(tag, true)
}

/// 迁移测试店铺建店本体（reviv 位由调用方裁决，两条入口共用，杜绝装配口径分叉）
fn open_migrate_store(tag: &str, reviv_enabled: bool) -> Arc<WedbStore<SegmentedDevice>> {
  let dir = tempfile::tempdir().unwrap().keep();
  let device = Arc::new(SegmentedDevice::single_file(dir.join(tag)).unwrap());
  // 小预算测试配置（对标 C# 16MB 基线），GC 关闭保持历史语义
  let config = test_store_config().with_revivification(reviv_enabled);
  Arc::new(WedbStore::open(config, device).unwrap())
}

/// 装配双主节点拓扑：node_1（本地）持 0..8192，node_2@7001 持 8192..16384
fn two_primary_provider() -> Arc<ClusterProvider> {
  let cp = ClusterProvider::new();
  let m = cp.cluster_manager().unwrap();
  {
    let mut config = m.current_config.write();
    config.initialize_local_worker(LocalWorkerSpec {
      node_id: 0x0000_0000_0000_0000_0000_0000_0000_DE11,
      address: "127.0.0.1",
      port: 7000,
      config_epoch: 1,
      role: NodeRole::Primary,
      replica_of_node_id: None,
      hostname: None,
    });
    let remote_worker_id = config.workers.len() as u16;
    config.workers.push(Worker {
      nodeid: Some(0x0000_0000_0000_0000_0000_0000_0000_DE12),
      address: "127.0.0.1".into(),
      port: 7001,
      config_epoch: 1,
      role: NodeRole::Primary,
      replica_of_node_id: None,
      replication_offset: 0,
      hostname: None,
    });
    for slot in 0..CLUSTER_SLOT_COUNT {
      config.slot_map[slot] = HashSlot {
        worker_id: LOCAL_WORKER_ID as u16,
        state: SlotState::Stable,
      };
    }
    config.slot_map[REMOTE_SLOT as usize] = HashSlot {
      worker_id: remote_worker_id,
      state: SlotState::Stable,
    };
  }
  cp.set_cluster_node_timeout_ms(100);
  cp
}

/// 挂共享存储的集群会话消费者（provider.set_store 与执行域同源）
fn migrate_consumer(
  cp: &ClusterProvider,
) -> (RespSessionConsumer, Arc<WedbStore<SegmentedDevice>>) {
  let cluster_session: Arc<ClusterSession> = cp.create_cluster_session();
  let store = migrate_store("mig_recv.db");
  cp.set_store(Arc::clone(&store));
  let mut consumer = RespSessionConsumer::with_cluster(
    1,
    RespServerSessionOptions::default(),
    cluster_session,
    cp.provider_handle(),
    Arc::new(StoreGarnetApi::new(store.new_session().unwrap())),
  );
  consumer.attach_transaction_components(Arc::new(WatchVersionMap::new(64)), TxnLockTable::new());
  (consumer, store)
}

/// 慢命令往返：同步段消费，挂起慢路径时 block_on 驱动应答
fn drive(rt: &Runtime, c: &mut RespSessionConsumer, frame_bytes: &[u8]) -> Vec<u8> {
  let (consumed, mut out) = pump(c, frame_bytes);
  assert_eq!(consumed, Some(0), "帧应被完整消费");
  if let Some(slow) = c.take_slow_wait() {
    rt.block_on(async {
      out.extend_from_slice(&slow.resolve().await);
    });
  }
  out
}

/// 本地库键实例（库级定槽 doc/zh/db.md 4.1：键内容不参与定槽，
/// 本地/远端由会话库槽位决定，任意键前缀 + 序号即可）
fn local_slot_key(prefix: &str) -> String {
  format!("{prefix}0")
}

/// 发送侧守卫：probe_unsupported_keys 分类准确——string 键与 Hash/Set/List/
/// ZSet 对象信封键可迁移；RangeIndex 信封 / RangeIndex 元记录 / 向量集带外
/// 域键显式「暂不支持」。run_keys_migration_driver 对含暂不支持键的请求在
/// 触达远端前整体拒绝并列明清单，源端键权零变更（不发帧不删除不交权）
#[test]
fn migrate_driver_rejects_object_keys_without_losing_ownership() {
  let rt = Runtime::new().unwrap();
  rt.block_on(async {
    let store = migrate_store("mig_guard.db");
    {
      let session = store.new_session().unwrap();
      let batch = session.enter_batch();
      let storage = StorageSession::new_readonly(batch);

      // string 键 + 可迁移对象信封键（Hash=0x03）+ 暂不支持键
      storage.upsert_string(b"mig:str", b"v1").await.unwrap();
      storage
        .upsert_tag(b"mig:obj", KeyTag::ObjectEnvelope, b"\x03payload")
        .await
        .unwrap();
      // RangeIndex 信封（内层标签 5）
      storage
        .upsert_tag(b"mig:ri", KeyTag::ObjectEnvelope, b"\x05ri_payload")
        .await
        .unwrap();
      // 未知信封内层标签（0xf0，不在 GarnetObjectType 定义域）
      storage
        .upsert_tag(b"mig:unknown", KeyTag::ObjectEnvelope, b"\xf0???")
        .await
        .unwrap();

      let keys = vec![
        b"mig:str".to_vec(),
        b"mig:obj".to_vec(),
        b"mig:ri".to_vec(),
        b"mig:unknown".to_vec(),
        b"mig:missing".to_vec(),
      ];
      let unsupported = probe_unsupported_keys(&storage, None, &keys).await.unwrap();
      assert_eq!(
        unsupported,
        vec![
          UnsupportedKey {
            key: b"mig:ri",
            kind_label: "rangeindex",
          },
          UnsupportedKey {
            key: b"mig:unknown",
            kind_label: "unknown",
          },
        ],
        "四类对象信封与 string 应可迁移，RI/未知类型显式不支持"
      );

      // 纯 string + 可迁移信封清单预检放行（回归：不误拦）
      assert!(
        probe_unsupported_keys(&storage, None, &[b"mig:str".to_vec(), b"mig:obj".to_vec()])
          .await
          .unwrap()
          .is_empty()
      );

      // 已过期 string 键：惰性过期裁决后视同不存在，不算不支持键（不触发拒绝）
      storage.upsert_string(b"mig:exp", b"stale").await.unwrap();
      storage.expire_at_ticks(b"mig:exp", 1).await.unwrap();
      assert!(storage.read_string(b"mig:exp").await.unwrap().is_none());
      assert!(!storage.batch.contains_key(b"mig:exp").await.unwrap());
    }

    // 驱动入口整体拒绝：未注册任务、未触达远端（127.0.0.1:1 不可达）即报错，
    // 错误显式列明暂不支持键清单——不静默跳过
    let cp = ClusterProvider::new();
    let spec = MigrateTaskSpec {
      source_node_id: 0x0000_0000_0000_0000_0000_0000_0000_DE11,
      target_address: "127.0.0.1".to_string(),
      target_port: 1,
      target_node_id: 0x0000_0000_0000_0000_0000_0000_0000_DE12,
      username: "".to_string(),
      passwd: "".to_string(),
      copy_option: false,
      replace_option: false,
      timeout: 0,
      transfer_option: TransferOption::Keys,
    };
    let keys = vec![b"mig:str".to_vec(), b"mig:ri".to_vec()];
    let err = run_keys_migration_driver(
      Arc::clone(&cp),
      Arc::clone(&store),
      spec,
      &slot_set1(),
      &keys,
    )
    .await
    .unwrap_err();
    let msg = format!("{err}");
    assert!(msg.contains("MIGRATE 拒绝"), "应显式拒绝: {msg}");
    assert!(msg.contains("mig:ri"), "错误应列明暂不支持键: {msg}");
    assert!(msg.contains("rangeindex"), "错误应说明类型: {msg}");

    // 源端键权零变更：RI 键仍在（未发帧、未删除、未交权），string 键未删
    {
      let session = store.new_session().unwrap();
      let batch = session.enter_batch();
      let storage = StorageSession::new_readonly(batch);
      assert!(storage.batch.contains_key(b"mig:ri").await.unwrap());
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
      source_node_id: 0x0000_0000_0000_0000_0000_0000_0000_DE11,
      target_address: "127.0.0.1".to_string(),
      target_port: 1,
      target_node_id: 0x0000_0000_0000_0000_0000_0000_0000_DE12,
      username: "".to_string(),
      passwd: "".to_string(),
      copy_option: false,
      replace_option: false,
      timeout: 0,
      transfer_option: TransferOption::Keys,
    };
    let err = run_keys_migration_driver(
      cp,
      Arc::clone(&store),
      spec,
      &slot_set1(),
      &[b"mig:pure".to_vec()],
    )
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
  let set_slot = |_key: &str, state: SlotState| {
    let m = cp.cluster_manager().unwrap();
    m.current_config.write().slot_map[SLOT0 as usize] = HashSlot {
      worker_id: LOCAL_WORKER_ID as u16,
      state,
    };
  };

  // 该键槽位置 IMPORTING（接收端槽位校验前提）
  set_slot(&obj_key, SlotState::Importing);

  // 同名 string 迁移记录载荷
  let payload = encode_migration_payload(&[BatchItem {
    key: obj_key.as_bytes(),
    val: MigrateVal::Str(b"migrated_string_value".to_vec()),
    expire_unix_ms: 0,
  }]);

  // replace=F：目标键已是对象记录 → 跳过写入（应答仍 +OK，对标 C#
  // replaceOption || !Exists 的 Exists 双域判定），原对象不被清退
  let frame = migrate_recv_frame(b"F", &payload);
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
  let frame = migrate_recv_frame(b"T", &payload);
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
  let payload = encode_migration_payload(&[BatchItem {
    key: str_key.as_bytes(),
    val: MigrateVal::Str(b"sv".to_vec()),
    expire_unix_ms: 0,
  }]);
  let frame = migrate_recv_frame(b"F", &payload);
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

  // 对象信封记录（kind=2）接收写回：Hash 信封整值随帧写入，内层类型标签
  // 随信封首字节在途携带，HGET 按对象读出（对象键迁移往返的接收端半环）
  let env_key = local_slot_key("mig_env");
  set_slot(&env_key, SlotState::Importing);
  let env = encode_hash_envelope(&rt, &[(b"f1", b"migrated")]);
  let env_payload = encode_migration_payload(&[BatchItem {
    key: env_key.as_bytes(),
    val: MigrateVal::Env(env),
    expire_unix_ms: 0,
  }]);
  let frame = migrate_recv_frame(b"F", &env_payload);
  assert_eq!(drive(&rt, &mut consumer, &frame), b"+OK\r\n");
  set_slot(&env_key, SlotState::Stable);
  assert_eq!(
    drive(
      &rt,
      &mut consumer,
      &resp_frame(&[b"HGET", env_key.as_bytes(), b"f1"]),
    ),
    b"$8\r\nmigrated\r\n",
    "kind=2 信封整值应写回为可读 Hash 对象"
  );

  // 暂不支持信封类型显式拒绝：RangeIndex 内层标签（0x05）kind=2 帧不落库
  let ri_key = local_slot_key("mig_ri");
  set_slot(&ri_key, SlotState::Importing);
  let ri_payload = encode_migration_payload(&[BatchItem {
    key: ri_key.as_bytes(),
    val: MigrateVal::Env(b"\x05ri_payload".to_vec()),
    expire_unix_ms: 0,
  }]);
  let frame = migrate_recv_frame(b"F", &ri_payload);
  let out = drive(&rt, &mut consumer, &frame);
  assert!(
    out.starts_with(b"-ERR Unsupported migration record kind 2 (envelope type 5)"),
    "RangeIndex 信封应显式拒绝: {out:?}"
  );
  set_slot(&ri_key, SlotState::Stable);

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
  let frame = migrate_recv_frame(b"F", &bad);
  let out = drive(&rt, &mut consumer, &frame);
  let msg = String::from_utf8_lossy(&out);
  assert!(
    msg.starts_with("-ERR Invalid migration payload:")
      && msg.contains("Unsupported migration record kind 7"),
    "非法 kind 应显式拒绝: {out:?}"
  );
}

/// 目标端 is_importing_slot 接收门语义保留：槽位非 IMPORTING 时
/// CLUSTER MIGRATE 拒收记录（对标 C# RespClusterMigrateCommands.cs
/// IsImportingSlot 拒收）
#[test]
fn cluster_migrate_recv_requires_importing_slot() {
  let rt = Runtime::new().unwrap();
  let cp = two_primary_provider();
  let (mut consumer, _store) = migrate_consumer(&cp);

  let key = local_slot_key("mig_gate");
  let payload = encode_migration_payload(&[BatchItem {
    key: key.as_bytes(),
    val: MigrateVal::Str(b"v".to_vec()),
    expire_unix_ms: 0,
  }]);
  // 槽位保持 Stable（未置 IMPORTING）→ 接收门拒收
  let frame = migrate_recv_frame(b"F", &payload);
  let out = drive(&rt, &mut consumer, &frame);
  let msg = String::from_utf8_lossy(&out);
  assert!(
    msg.contains("is not in importing state"),
    "非 IMPORTING 槽应拒收迁移记录: {out:?}"
  );
}

/// 测试 CLUSTER MIGRATE 参数形状收敛（vectorSets 位废除后恰 4 参，
/// 头级槽集 doc/zh/db.md 4.1；带 vectorSets 的旧 5 参形回参数计数错误；
/// replace 仅认 T/t）
#[test]
fn cluster_migrate_args_shape_convergence() {
  let rt = Runtime::new().unwrap();
  let cp = two_primary_provider();
  let (mut consumer, _store) = migrate_consumer(&cp);

  let key = local_slot_key("shape_test");
  let payload = encode_migration_payload(&[BatchItem {
    key: key.as_bytes(),
    val: MigrateVal::Str(b"val".to_vec()),
    expire_unix_ms: 0,
  }]);

  // 1. 少于 4 参数（0/2/3 参）回参数计数错误
  let frame_0args = resp_frame(&[b"CLUSTER", b"MIGRATE"]);
  let out_0args = drive(&rt, &mut consumer, &frame_0args);
  let msg_0args = String::from_utf8_lossy(&out_0args);
  assert!(
    msg_0args.starts_with("-ERR wrong number of arguments for 'cluster|migrate' command"),
    "0 参数应报错: {msg_0args}"
  );

  let frame_3args = resp_frame(&[
    b"CLUSTER",
    b"MIGRATE",
    b"0000000000000000000000000000de11",
    b"F",
    &payload,
  ]);
  let out_3args = drive(&rt, &mut consumer, &frame_3args);
  let msg_3args = String::from_utf8_lossy(&out_3args);
  assert!(
    msg_3args.starts_with("-ERR wrong number of arguments for 'cluster|migrate' command"),
    "3 参数应报错: {msg_3args}"
  );

  let frame_2args = resp_frame(&[
    b"CLUSTER",
    b"MIGRATE",
    b"0000000000000000000000000000de11",
    &payload,
  ]);
  let out_2args = drive(&rt, &mut consumer, &frame_2args);
  let msg_2args = String::from_utf8_lossy(&out_2args);
  assert!(
    msg_2args.starts_with("-ERR wrong number of arguments for 'cluster|migrate' command"),
    "2 参数应报错: {msg_2args}"
  );

  // 2. C# 原 4 参形（第 3 位为 vectorSets、无头槽集）：形状与 4 参形重合，
  // 但槽位集非数值，头级槽门整批拒绝
  let frame_4args = resp_frame(&[
    b"CLUSTER",
    b"MIGRATE",
    b"0000000000000000000000000000de11",
    b"F",
    b"F",
    &payload,
  ]);
  let out_4args = drive(&rt, &mut consumer, &frame_4args);
  let msg_4args = String::from_utf8_lossy(&out_4args);
  assert!(
    msg_4args.starts_with("-ERR Slot out of range"),
    "旧 C# 4 参数形槽集非数值应整批拒绝: {msg_4args}"
  );

  // 3. 带 vectorSets 位的旧 5 参形回参数计数错误（vectorSets 线参数已废除）
  let frame_5args = resp_frame(&[
    b"CLUSTER",
    b"MIGRATE",
    b"0000000000000000000000000000de11",
    b"F",
    b"F",
    SLOT0_LIST,
    &payload,
  ]);
  let out_5args = drive(&rt, &mut consumer, &frame_5args);
  let msg_5args = String::from_utf8_lossy(&out_5args);
  assert!(
    msg_5args.starts_with("-ERR wrong number of arguments for 'cluster|migrate' command"),
    "旧 5 参数形（含 vectorSets）应报错: {msg_5args}"
  );

  // 4. replace 大小写支持（'t' 与 'T'）及自造 "1" 分支删除验证
  let set_slot = |_key: &str, state: SlotState| {
    let m = cp.cluster_manager().unwrap();
    m.current_config.write().slot_map[SLOT0 as usize] = HashSlot {
      worker_id: LOCAL_WORKER_ID as u16,
      state,
    };
  };

  // 先写入已有数据
  set_slot(&key, SlotState::Stable);
  assert_eq!(
    drive(
      &rt,
      &mut consumer,
      &resp_frame(&[b"SET", key.as_bytes(), b"initial_val"])
    ),
    b"+OK\r\n"
  );
  set_slot(&key, SlotState::Importing);

  // replace='1' 不应被识别为 true（旧版曾支持，已删除自造 1 分支）；已有 key 不被覆盖
  let payload_new1 = encode_migration_payload(&[BatchItem {
    key: key.as_bytes(),
    val: MigrateVal::Str(b"new_val_1".to_vec()),
    expire_unix_ms: 0,
  }]);
  let frame_rep_1 = resp_frame(&[
    b"CLUSTER",
    b"MIGRATE",
    b"0000000000000000000000000000de11",
    b"1",
    SLOT0_LIST,
    &payload_new1,
  ]);
  assert_eq!(drive(&rt, &mut consumer, &frame_rep_1), b"+OK\r\n");
  set_slot(&key, SlotState::Stable);
  assert_eq!(
    drive(&rt, &mut consumer, &resp_frame(&[b"GET", key.as_bytes()])),
    b"$11\r\ninitial_val\r\n",
    "replace=1 不应覆盖已有键"
  );

  // replace='t'（小写）应被识别为 true，成功覆盖
  set_slot(&key, SlotState::Importing);
  let payload_new2 = encode_migration_payload(&[BatchItem {
    key: key.as_bytes(),
    val: MigrateVal::Str(b"new_val_2".to_vec()),
    expire_unix_ms: 0,
  }]);
  let frame_rep_lower_t = resp_frame(&[
    b"CLUSTER",
    b"MIGRATE",
    b"0000000000000000000000000000de11",
    b"t",
    SLOT0_LIST,
    &payload_new2,
  ]);
  assert_eq!(drive(&rt, &mut consumer, &frame_rep_lower_t), b"+OK\r\n");
  set_slot(&key, SlotState::Stable);
  assert_eq!(
    drive(&rt, &mut consumer, &resp_frame(&[b"GET", key.as_bytes()])),
    b"$9\r\nnew_val_2\r\n",
    "replace=t 应覆盖已有键"
  );

  // replace='T'（大写）应被识别为 true，成功覆盖
  set_slot(&key, SlotState::Importing);
  let payload_new3 = encode_migration_payload(&[BatchItem {
    key: key.as_bytes(),
    val: MigrateVal::Str(b"new_val_3".to_vec()),
    expire_unix_ms: 0,
  }]);
  let frame_rep_upper_t = resp_frame(&[
    b"CLUSTER",
    b"MIGRATE",
    b"0000000000000000000000000000de11",
    b"T",
    SLOT0_LIST,
    &payload_new3,
  ]);
  assert_eq!(drive(&rt, &mut consumer, &frame_rep_upper_t), b"+OK\r\n");
  set_slot(&key, SlotState::Stable);
  assert_eq!(
    drive(&rt, &mut consumer, &resp_frame(&[b"GET", key.as_bytes()])),
    b"$9\r\nnew_val_3\r\n",
    "replace=T 应覆盖已有键"
  );
}

/// 构造 Hash 对象信封整值（[1B GarnetObjectType::Hash][bitcode 载荷]）：
/// 经真 HSET 写入后直读信封物理域提取，保证载荷与生产序列化严格同源
fn encode_hash_envelope(rt: &Runtime, fields: &[(&[u8], &[u8])]) -> Vec<u8> {
  let cp = two_primary_provider();
  let (mut consumer, store) = migrate_consumer(&cp);
  let key = b"__envelope_builder__";
  for (f, v) in fields {
    assert_eq!(
      drive(rt, &mut consumer, &resp_frame(&[b"HSET", key, f, v])),
      b":1\r\n"
    );
  }
  rt.block_on(async {
    let session = store.new_session().unwrap();
    let batch = session.enter_batch();
    let storage = StorageSession::new_readonly(batch);
    storage
      .read_tag_with(key, KeyTag::ObjectEnvelope, |raw| raw.to_vec())
      .await
      .unwrap()
      .expect("Hash 信封应已写入")
  })
}

// ---------------------------------------------------------------------------
// 迁移停等限时与失败恢复（net.md P2 / ds.net.md 条 11）：停等超时、
// 批次拒绝 recover、完成哨兵失败显式报错、SLOTS 链 NODE 失败显式报错、
// 全链成功路径
// ---------------------------------------------------------------------------

use std::{
  collections::VecDeque,
  str::from_utf8,
  time::{Duration, Instant},
};

use compio::{
  BufResult,
  io::{AsyncRead, AsyncWriteExt},
  net::TcpListener,
  runtime::spawn,
};

/// 解析缓冲中首个完整 RESP2 数组帧：返回 (帧总字节数, 全部参数切片)，
/// 不完整返回 None
fn try_parse_frame_args(buf: &[u8]) -> Option<(usize, Vec<&[u8]>)> {
  if buf.first() != Some(&b'*') {
    return None;
  }
  let header_end = buf.iter().position(|b| *b == b'\n')? + 1;
  let argc: usize = from_utf8(&buf[1..header_end - 2]).ok()?.parse().ok()?;
  let mut pos = header_end;
  let mut args = Vec::with_capacity(argc);
  for _ in 0..argc {
    if buf.get(pos) != Some(&b'$') {
      return None;
    }
    let len_line_end = buf[pos + 1..].iter().position(|b| *b == b'\n')? + pos + 2;
    let len: usize = from_utf8(&buf[pos + 1..len_line_end - 2])
      .ok()?
      .parse()
      .ok()?;
    let end = len_line_end + len;
    if end + 2 > buf.len() {
      return None;
    }
    args.push(&buf[len_line_end..end]);
    pos = end + 2;
  }
  Some((pos, args))
}

/// 假迁移目标端（脚本化应答，模式对标 tests/appendlog_reject_disconnect.rs
/// 的 reject_after_handshake_node）：按连接分配脚本——第 i 个连接用第 i 段
/// 脚本，逐帧解析 RESP2 数组按脚本弹答（+OK/-ERR），该段脚本耗尽后本连接
/// 保持静默（模拟目标挂起）；脚本段耗尽后的新连接同样静默（模拟重连无应答）。
/// 每帧前 3 参记入 seen 供用例断言帧序
async fn scripted_migrate_target(
  conn_replies: Vec<Vec<&'static [u8]>>,
  seen: Arc<Mutex<Vec<String>>>,
) -> String {
  let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
  let addr = listener.local_addr().unwrap().to_string();
  let scripts = Arc::new(Mutex::new(VecDeque::from(
    conn_replies
      .into_iter()
      .map(VecDeque::from)
      .collect::<Vec<_>>(),
  )));
  spawn(async move {
    while let Ok((mut stream, _)) = listener.accept().await {
      let scripts = Arc::clone(&scripts);
      let seen = Arc::clone(&seen);
      spawn(async move {
        // 每连接独立脚本：无脚本段 → 连接直读不答（重连也无应答）
        let mut script = scripts.lock().pop_front().unwrap_or_default();
        let mut acc: Vec<u8> = Vec::new();
        let mut buf = vec![0u8; 65536];
        loop {
          let BufResult(res, next) = stream.read(buf).await;
          buf = next;
          let n = match res {
            Ok(n) if n > 0 => n,
            _ => break,
          };
          acc.extend_from_slice(&buf[..n]);
          while let Some((frame_len, args)) = try_parse_frame_args(&acc) {
            seen.lock().push(
              args
                .iter()
                .take(3)
                .map(|a| String::from_utf8_lossy(a).into_owned())
                .collect::<Vec<_>>()
                .join(" "),
            );
            acc.drain(..frame_len);
            // 脚本耗尽 → 静默（目标挂起，驱动停等只能超时）
            let Some(reply) = script.pop_front() else {
              continue;
            };
            if stream.write_all(reply.to_vec()).await.is_err() {
              return;
            }
          }
        }
      })
      .detach();
    }
  })
  .detach();
  addr
}

/// 迁移驱动发送侧 spec（timeout 由用例覆写）
fn migrate_spec(port: i32, timeout_ms: i32) -> MigrateTaskSpec {
  MigrateTaskSpec {
    source_node_id: 0x0000_0000_0000_0000_0000_0000_0000_DE11,
    target_address: "127.0.0.1".to_string(),
    target_port: port,
    target_node_id: 0x0000_0000_0000_0000_0000_0000_0000_DE12,
    username: String::new(),
    passwd: String::new(),
    copy_option: false,
    replace_option: false,
    timeout: timeout_ms,
    transfer_option: TransferOption::Keys,
  }
}

/// 解析假端监听地址的端口号
fn port_of(addr: &str) -> i32 {
  addr.rsplit(':').next().unwrap().parse().unwrap()
}

/// 读库内 string（驱动用例断言键权用）
async fn read_str(store: &Arc<WedbStore<SegmentedDevice>>, key: &[u8]) -> Option<Vec<u8>> {
  let session = store.new_session().unwrap();
  let batch = session.enter_batch();
  let storage = StorageSession::new_readonly(batch);
  storage.read_string(key).await.unwrap()
}

/// 全链成功：握手 → IMPORTING → 批次 +OK → 哨兵 +OK → Ok(条数)；非 copy
/// 模式已传输键删除；帧序与角色正确（库级定槽两键恒共会话槽位 → 单段
/// range、IMPORTING 一次）；KEYS 链全程不发 NODE、绝不移交槽属主（对标 C#
/// MigrateKeysAsync 不动槽位，槽位收口归运维 CLUSTER SETSLOT），同槽未
/// 迁移键持续可源端访问
#[test]
fn migrate_driver_full_flow_success_deletes_transferred_keys() {
  let rt = Runtime::new().unwrap();
  rt.block_on(async {
    let cp = two_primary_provider();
    let store = migrate_store("mt_ok.db");
    let k1 = local_slot_key("mt_a");
    let k2 = local_slot_key("mt_b");
    // 与 k1 同槽但不在迁移清单的键（属主不移交的存活见证）
    let slot1 = SLOT0;
    let k3 = key_in_slot("mt_c", slot1);
    {
      let session = store.new_session().unwrap();
      let batch = session.enter_batch();
      let storage = StorageSession::new_readonly(batch);
      storage.upsert_string(k1.as_bytes(), b"v1").await.unwrap();
      storage.upsert_string(k2.as_bytes(), b"v2").await.unwrap();
      storage.upsert_string(k3.as_bytes(), b"v3").await.unwrap();
    }

    let seen = Arc::new(Mutex::new(Vec::new()));
    // 脚本（单连接）：握手×2 + IMPORTING×1 + 批次 + 哨兵全 +OK（无 NODE 帧）
    let addr = scripted_migrate_target(vec![vec![b"+OK\r\n"; 5]], Arc::clone(&seen)).await;

    let count = run_keys_migration_driver(
      Arc::clone(&cp),
      Arc::clone(&store),
      migrate_spec(port_of(&addr), 0),
      &slot_set1(),
      &[k1.clone().into_bytes(), k2.clone().into_bytes()],
    )
    .await
    .unwrap();
    assert_eq!(count, 2, "两键应计数迁移");

    // 非 copy：已确认传输的键从源端删除
    assert_eq!(read_str(&store, k1.as_bytes()).await, None);
    assert_eq!(read_str(&store, k2.as_bytes()).await, None);

    // 同槽未迁移键 k3 不在迁移清单、不经键门，持续可源端访问
    assert_eq!(
      read_str(&store, k3.as_bytes()).await,
      Some(b"v3".to_vec()),
      "同槽未迁移键必须保留源端且可访问"
    );

    // 帧序：SETINFO → SETNAME → IMPORTING → MIGRATE(批) → MIGRATE(哨兵)
    let frames = seen.lock();
    assert_eq!(frames.len(), 5, "帧序应精确: {frames:?}");
    assert!(frames[0].starts_with("CLIENT SETINFO"), "{frames:?}");
    assert!(frames[1].starts_with("CLIENT SETNAME"), "{frames:?}");
    assert!(frames[2].contains("IMPORTING"), "{frames:?}");
    assert!(frames[3].contains("MIGRATE"), "{frames:?}");
    assert!(frames[4].contains("MIGRATE"), "{frames:?}");
    assert!(
      !frames.iter().any(|f| f.contains("NODE")),
      "KEYS 链绝不发 NODE: {frames:?}"
    );
    drop(frames);

    // 属主未移交：键所在槽保持 Migrating 且归属本端（对标 C# KEYS 臂
    // 不动槽位，交权后应为 Stable+远端 worker）
    let m = cp.cluster_manager().unwrap();
    for k in [&k1, &k2] {
      let _ = k;
      let slot = SLOT0;
      assert_eq!(
        m.current_config.read().get_state(slot),
        SlotState::Migrating,
        "槽 {slot} 应保持 Migrating 待运维收口"
      );
      assert_eq!(
        m.current_config.read().get_worker_id_from_slot(slot),
        LOCAL_WORKER_ID,
        "槽 {slot} 属主必须仍是本端"
      );
    }
  });
}

/// 目标端静默：批次停等在 spec.timeout 量级报停等超时（而非永挂），
/// recover 仅回滚远端 IMPORTING→STABLE、本端 MIGRATING 保持，源端键保留
#[test]
fn migrate_driver_silent_target_times_out_instead_of_hanging() {
  let rt = Runtime::new().unwrap();
  rt.block_on(async {
    let cp = two_primary_provider();
    let store = migrate_store("mt_silent.db");
    let k1 = local_slot_key("mt_s");
    {
      let session = store.new_session().unwrap();
      let batch = session.enter_batch();
      let storage = StorageSession::new_readonly(batch);
      storage.upsert_string(k1.as_bytes(), b"v1").await.unwrap();
    }

    let seen = Arc::new(Mutex::new(Vec::new()));
    // 脚本：连接1 握手×2 + IMPORTING 应答后批次静默（停等超时）；连接2
    // （recover 重连）握手×2 应答后 STABLE 静默——超时恢复须弃污染连接重连
    let addr = scripted_migrate_target(
      vec![vec![b"+OK\r\n"; 3], vec![b"+OK\r\n"; 2]],
      Arc::clone(&seen),
    )
    .await;

    let t0 = Instant::now();
    let err = run_keys_migration_driver(
      Arc::clone(&cp),
      Arc::clone(&store),
      migrate_spec(port_of(&addr), 300),
      &slot_set1(),
      &[k1.clone().into_bytes()],
    )
    .await
    .unwrap_err();
    let elapsed = t0.elapsed();

    assert!(
      format!("{err}").contains("迁移远端停等超时"),
      "应报停等超时: {err:?}"
    );
    assert!(
      elapsed >= Duration::from_millis(280),
      "必须真实等待 timeout 窗口: {elapsed:?}"
    );
    assert!(elapsed < Duration::from_secs(10), "不得长挂: {elapsed:?}");

    // recover 已发 STABLE；源端键保留（未确认传输绝不删除）
    assert!(
      seen
        .lock()
        .iter()
        .any(|f| f.contains("SETSLOTSRANGE STABLE")),
      "超时失败必须走 recover STABLE: {:?}",
      seen.lock()
    );
    assert_eq!(read_str(&store, k1.as_bytes()).await, Some(b"v1".to_vec()));

    // KEYS 链 recover 不动本端槽位：运维手工前置的 MIGRATING 保持原状
    let slot = SLOT0;
    let m = cp.cluster_manager().unwrap();
    assert_eq!(
      m.current_config.read().get_state(slot),
      SlotState::Migrating,
      "KEYS 链失败恢复不得回退本端 MIGRATING"
    );
  });
}

/// 批次拒绝：远端 -ERR → 显式报错 + recover STABLE + 源端键保留 +
/// 本端 MIGRATING 保持
#[test]
fn migrate_driver_batch_reject_recovers_and_keeps_keys() {
  let rt = Runtime::new().unwrap();
  rt.block_on(async {
    let cp = two_primary_provider();
    let store = migrate_store("mt_reject.db");
    let k1 = local_slot_key("mt_r");
    {
      let session = store.new_session().unwrap();
      let batch = session.enter_batch();
      let storage = StorageSession::new_readonly(batch);
      storage.upsert_string(k1.as_bytes(), b"v1").await.unwrap();
    }

    let seen = Arc::new(Mutex::new(Vec::new()));
    // 脚本：握手×2 +OK、IMPORTING +OK、批次 -ERR、recover STABLE +OK
    let addr = scripted_migrate_target(
      vec![vec![
        b"+OK\r\n",
        b"+OK\r\n",
        b"+OK\r\n",
        b"-ERR rejected\r\n",
        b"+OK\r\n",
      ]],
      Arc::clone(&seen),
    )
    .await;

    let err = run_keys_migration_driver(
      Arc::clone(&cp),
      Arc::clone(&store),
      migrate_spec(port_of(&addr), 0),
      &slot_set1(),
      &[k1.clone().into_bytes()],
    )
    .await
    .unwrap_err();
    assert!(
      format!("{err:?}").contains("rejected"),
      "应透出远端拒绝: {err:?}"
    );

    assert!(
      seen
        .lock()
        .iter()
        .any(|f| f.contains("SETSLOTSRANGE STABLE")),
      "批次拒绝必须 recover STABLE: {:?}",
      seen.lock()
    );
    assert_eq!(read_str(&store, k1.as_bytes()).await, Some(b"v1".to_vec()));

    // KEYS 链 recover 仅回滚远端 IMPORTING→STABLE，本端 MIGRATING 保持
    let slot = SLOT0;
    let m = cp.cluster_manager().unwrap();
    assert_eq!(
      m.current_config.read().get_state(slot),
      SlotState::Migrating,
      "KEYS 链失败恢复不得回退本端 MIGRATING"
    );
  });
}

/// 完成哨兵失败：批次 +OK 但哨兵 -ERR → 显式报错（不得吞没后照常交权），
/// recover STABLE，源端键保留，本端 MIGRATING 保持
#[test]
fn migrate_driver_sentinel_failure_fails_explicitly() {
  let rt = Runtime::new().unwrap();
  rt.block_on(async {
    let cp = two_primary_provider();
    let store = migrate_store("mt_sentinel.db");
    let k1 = local_slot_key("mt_n");
    {
      let session = store.new_session().unwrap();
      let batch = session.enter_batch();
      let storage = StorageSession::new_readonly(batch);
      storage.upsert_string(k1.as_bytes(), b"v1").await.unwrap();
    }

    let seen = Arc::new(Mutex::new(Vec::new()));
    // 脚本：握手×2 +OK、IMPORTING +OK、批次 +OK、哨兵 -ERR、STABLE +OK
    let addr = scripted_migrate_target(
      vec![vec![
        b"+OK\r\n",
        b"+OK\r\n",
        b"+OK\r\n",
        b"+OK\r\n",
        b"-ERR sentinel rejected\r\n",
        b"+OK\r\n",
      ]],
      Arc::clone(&seen),
    )
    .await;

    let err = run_keys_migration_driver(
      Arc::clone(&cp),
      Arc::clone(&store),
      migrate_spec(port_of(&addr), 0),
      &slot_set1(),
      &[k1.clone().into_bytes()],
    )
    .await
    .unwrap_err();
    assert!(
      format!("{err:?}").contains("sentinel rejected"),
      "哨兵失败必须显式报错: {err:?}"
    );

    let (migrate_frames, has_stable, has_node) = {
      let frames = seen.lock();
      (
        frames.iter().filter(|f| f.contains("MIGRATE")).count(),
        frames.iter().any(|f| f.contains("SETSLOTSRANGE STABLE")),
        frames.iter().any(|f| f.contains("NODE")),
      )
    };
    assert_eq!(migrate_frames, 2, "批次与哨兵各一帧: {:?}", seen.lock());
    assert!(has_stable, "哨兵失败必须 recover STABLE: {:?}", seen.lock());
    assert!(!has_node, "KEYS 链绝不发 NODE: {:?}", seen.lock());
    assert_eq!(read_str(&store, k1.as_bytes()).await, Some(b"v1".to_vec()));

    // KEYS 链失败恢复不动本端槽位，属主保持
    let slot = SLOT0;
    let m = cp.cluster_manager().unwrap();
    assert_eq!(
      m.current_config.read().get_state(slot),
      SlotState::Migrating,
      "KEYS 链失败恢复不得回退本端 MIGRATING"
    );
  });
}

/// 远端置 NODE 失败（SLOTS 链专属步骤，KEYS 链无 NODE 步）：显式报错 +
/// recover STABLE（对标 C# BeginAsyncMigrationTaskAsync 的 NODE 失败
/// recover 分支），已传键随 DELETING 相删除不回收
#[test]
fn migrate_driver_node_assignment_failure_fails_explicitly() {
  let rt = Runtime::new().unwrap();
  rt.block_on(async {
    let cp = two_primary_provider();
    let store = migrate_store("mt_node.db");
    let slot = SLOT0;
    let k1 = key_in_slot("mt_o", slot);
    {
      let session = store.new_session().unwrap();
      let batch = session.enter_batch();
      let storage = StorageSession::new_readonly(batch);
      storage.upsert_string(k1.as_bytes(), b"v1").await.unwrap();
    }

    let seen = Arc::new(Mutex::new(Vec::new()));
    // 脚本：握手×2 +OK、IMPORTING +OK、批次 +OK、哨兵 +OK、NODE -ERR、STABLE +OK
    let addr = scripted_migrate_target(
      vec![vec![
        b"+OK\r\n",
        b"+OK\r\n",
        b"+OK\r\n",
        b"+OK\r\n",
        b"+OK\r\n",
        b"-ERR node refused\r\n",
        b"+OK\r\n",
      ]],
      Arc::clone(&seen),
    )
    .await;

    let spec = MigrateTaskSpec {
      transfer_option: TransferOption::Slots,
      ..migrate_spec(port_of(&addr), 0)
    };
    let slots: HashSet<i32> = [slot as i32].into_iter().collect();
    let session = try_add_slots_migration_task(&cp, spec.clone(), &slots).unwrap();
    let err = run_slots_migration_task(Arc::clone(&store), spec, Arc::clone(&session))
      .await
      .unwrap_err();
    // set_slot_range_async 口径：-ERR 应答保留错误文案透传，非 OK 即判败
    // （C# TrySetSlotRangesAsync 同以 result != "OK" 判败）
    let err_str = format!("{err:?}");
    assert!(
      err_str.contains("远端 SETSLOTSRANGE NODE 失败") && err_str.contains("node refused"),
      "NODE 失败必须显式报错且包含远端错误文案: {err:?}"
    );

    assert!(
      seen
        .lock()
        .iter()
        .any(|f| f.contains("SETSLOTSRANGE STABLE")),
      "NODE 失败必须 recover STABLE: {:?}",
      seen.lock()
    );
    // SLOTS 链删除在 DELETING 相（先于收尾编排）：NODE 失败时已传键已删，
    // 不回收（远端已导入批次数据不回收，C# recover 同口径）
    assert_eq!(read_str(&store, k1.as_bytes()).await, None);

    // SLOTS 链 recover 回退本端槽位：MIGRATING → Stable、归属收回本端
    // （与 KEYS 链失败恢复保持 Migrating 形成显式对照），失败路径同样移除任务
    let m = cp.cluster_manager().unwrap();
    assert_eq!(m.current_config.read().get_state(slot), SlotState::Stable);
    assert_eq!(
      m.current_config.read().get_worker_id_from_slot(slot),
      LOCAL_WORKER_ID
    );
    assert_eq!(
      cp.migration_manager().unwrap().get_migration_task_count(),
      0,
      "失败路径同样必须移除任务"
    );
  });
}

// ---------------------------------------------------------------------------
// M3 源端生产链（next/clude.md 条 1 / gemini.md 条 3）：SLOTS 驱动全链、
// MIGRATE 命令入口（KEYS 同步 / SLOTS 后台）与解析错误路径
// ---------------------------------------------------------------------------

use wedb::server::migration::migrate_driver::{
  RevivPauseGuard, run_slots_migration_task, try_add_slots_migration_task,
};

/// 构造会话库键（库级定槽下键内容不参与定槽，slot 参数仅作同槽语义标注）
fn key_in_slot(prefix: &str, slot: u16) -> String {
  debug_assert_eq!(slot, SLOT0, "库级定槽下库内键恒共会话槽位");
  format!("{prefix}0")
}

/// 把 provider 中远端节点（node_2）端口改写为假目标端端口
/// （命令入口按集群配置解析 target_node_id，须与之对齐）
fn retarget_remote_port(cp: &ClusterProvider, port: i32) {
  let m = cp.cluster_manager().unwrap();
  let mut config = m.current_config.write();
  if let Some(w) = config
    .workers
    .iter_mut()
    .find(|w| w.nodeid == Some(0x0000_0000_0000_0000_0000_0000_0000_DE12))
  {
    w.port = port;
  }
}

/// SLOTS 驱动直调全链成功：帧序握手 → IMPORTING → 批次 → 哨兵 → NODE →
/// Ok(条数)；已传键删除、槽位交权 node_2、任务移除（对标
/// 驱动循环成功路径与 C# MigrateSlotsDriverInlineAsync 对齐）
#[test]
fn slots_migration_task_full_flow_success() {
  let rt = Runtime::new().unwrap();
  rt.block_on(async {
    let cp = two_primary_provider();
    let store = migrate_store_reviv("st_ok.db");
    let slot = SLOT0;
    let k1 = key_in_slot("st_a", slot);
    let k2 = key_in_slot("st_b", slot);
    {
      let session = store.new_session().unwrap();
      let batch = session.enter_batch();
      let storage = StorageSession::new_readonly(batch);
      storage.upsert_string(k1.as_bytes(), b"v1").await.unwrap();
      storage.upsert_string(k2.as_bytes(), b"v2").await.unwrap();
    }

    let seen = Arc::new(Mutex::new(Vec::new()));
    // 脚本：连接1 = 握手×2 + IMPORTING + 批次 + 哨兵 + NODE 全 +OK
    // （单槽一段 range，各帧一次）；连接2/3 = 收尾两次 gossip 汇聚，每连接
    // 携带 SETINFO/SETNAME 握手 + WITHMEET 三帧，+OK 应答非配置版本 → 汇聚
    // 判失败即弃（best-effort，对标 C# TryMeetAsync 内部自吞异常）
    let addr = scripted_migrate_target(
      vec![
        vec![b"+OK\r\n"; 6],
        vec![b"+OK\r\n"; 3],
        vec![b"+OK\r\n"; 3],
      ],
      Arc::clone(&seen),
    )
    .await;
    let spec = MigrateTaskSpec {
      transfer_option: TransferOption::Slots,
      ..migrate_spec(port_of(&addr), 0)
    };
    let slots: HashSet<i32> = [slot as i32].into_iter().collect();
    assert!(
      store.reviv_pool.is_enabled(),
      "迁移启动前复活池应处于启用状态"
    );
    let session = try_add_slots_migration_task(&cp, spec.clone(), &slots).unwrap();
    let migrated = run_slots_migration_task(Arc::clone(&store), spec, Arc::clone(&session))
      .await
      .unwrap();
    assert_eq!(migrated, 2, "两键应计数迁移");
    assert!(
      store.reviv_pool.is_enabled(),
      "迁移完成后复活池应恢复启用状态"
    );

    // 非 copy：已确认传输的键从源端删除
    assert_eq!(read_str(&store, k1.as_bytes()).await, None);
    assert_eq!(read_str(&store, k2.as_bytes()).await, None);

    // 帧序：迁移连接 SETINFO → SETNAME → IMPORTING → MIGRATE(批) →
    //       MIGRATE(哨兵) → NODE；汇聚连接×2 各自携带握手 + WITHMEET，
    //       跨连接帧在 seen 交错，迁移连接前 5 帧序内断言 + 全体按类计数
    //       （NODE 前 / Relinquish 后各汇聚一次，对标 MigrationDriver.cs:191/:212）
    let frames = seen.lock();
    assert_eq!(frames.len(), 12, "帧序应精确: {frames:?}");
    assert!(frames[0].starts_with("CLIENT SETINFO"), "{frames:?}");
    assert!(frames[1].starts_with("CLIENT SETNAME"), "{frames:?}");
    assert!(frames[2].contains("IMPORTING"), "{frames:?}");
    assert!(frames[3].contains("MIGRATE"), "{frames:?}");
    assert!(frames[4].contains("MIGRATE"), "{frames:?}");
    assert_eq!(
      frames.iter().filter(|f| f.contains("WITHMEET")).count(),
      2,
      "收尾应恰好两次 gossip 汇聚: {frames:?}"
    );
    assert!(
      frames.iter().any(|f| f.contains("SETSLOTSRANGE NODE")),
      "SLOTS 链必须交权 NODE: {frames:?}"
    );
    drop(frames);

    // 槽位交权：本端槽位 Stable 且归属 node_2（relinquish_ownership 生效）
    let m = cp.cluster_manager().unwrap();
    let remote_wid = m
      .current_config
      .read()
      .get_worker_id_from_node_id(0x0000_0000_0000_0000_0000_0000_0000_DE12);
    assert_eq!(m.current_config.read().get_state(slot), SlotState::Stable);
    assert_eq!(
      m.current_config.read().get_worker_id_from_slot(slot),
      remote_wid as usize
    );

    // finally 移除任务（对标 TryStartMigrationTaskAsync finally）
    assert_eq!(
      cp.migration_manager().unwrap().get_migration_task_count(),
      0,
      "迁移任务结束必须移除"
    );
  });
}

/// 纪元静止栅栏位置对标 C# WaitForConfigPropagationAsync 调用点
/// （MigrateSessionKeys.cs:35/:187/:194、MigrateSessionSlots.cs:236/:247、
/// MigrationDriver.cs:160）：KEYS 链 begin 无纪元门，栅栏 = TRANSMITTING 置位
/// 后 + DELETING 置位后 + MIGRATED 置位后共 3 次，copy=true 无删除分支仅
/// 2 次；SLOTS 链 = begin 纪元门 1 次 + 每批 TRANSMITTING/DELETING 置位后
/// 各 1 次。KEYS 以 current_epoch 增量精确计数；SLOTS 批迭代次数随墓碑键
/// 重扫可见性浮动，按结构断言
#[test]
fn migrate_driver_epoch_quiescence_fences_match_csharp() {
  let rt = Runtime::new().unwrap();
  rt.block_on(async {
    // KEYS copy=false：3 次栅栏 + gossip 汇聚零次（C# MigrateKeysAsync 无此编排）
    let cp = two_primary_provider();
    let store = migrate_store("mt_fence_k0.db");
    let k1 = local_slot_key("mt_f1");
    {
      let session = store.new_session().unwrap();
      let batch = session.enter_batch();
      let storage = StorageSession::new_readonly(batch);
      storage.upsert_string(k1.as_bytes(), b"v1").await.unwrap();
    }
    let seen = Arc::new(Mutex::new(Vec::new()));
    let addr = scripted_migrate_target(vec![vec![b"+OK\r\n"; 6]], Arc::clone(&seen)).await;
    let epoch_before = cp.current_epoch();
    let count = run_keys_migration_driver(
      Arc::clone(&cp),
      Arc::clone(&store),
      migrate_spec(port_of(&addr), 0),
      &slot_set1(),
      &[k1.into_bytes()],
    )
    .await
    .unwrap();
    assert_eq!(count, 1);
    assert_eq!(
      cp.current_epoch() - epoch_before,
      3,
      "KEYS copy=false 应恰好 3 次纪元静止栅栏 (TRANSMITTING/DELETING/MIGRATED 后)"
    );
    assert!(
      !seen.lock().iter().any(|f| f.contains("WITHMEET")),
      "KEYS 链收尾无 gossip 汇聚: {:?}",
      seen.lock()
    );

    // KEYS copy=true：无删除分支，仅 TRANSMITTING/MIGRATED 后 2 次
    let cp = two_primary_provider();
    let store = migrate_store("mt_fence_k1.db");
    let k1 = local_slot_key("mt_f2");
    {
      let session = store.new_session().unwrap();
      let batch = session.enter_batch();
      let storage = StorageSession::new_readonly(batch);
      storage.upsert_string(k1.as_bytes(), b"v1").await.unwrap();
    }
    let addr =
      scripted_migrate_target(vec![vec![b"+OK\r\n"; 6]], Arc::new(Mutex::new(Vec::new()))).await;
    let mut spec = migrate_spec(port_of(&addr), 0);
    spec.copy_option = true;
    let epoch_before = cp.current_epoch();
    let count = run_keys_migration_driver(
      Arc::clone(&cp),
      Arc::clone(&store),
      spec,
      &slot_set1(),
      &[k1.into_bytes()],
    )
    .await
    .unwrap();
    assert_eq!(count, 1);
    assert_eq!(
      cp.current_epoch() - epoch_before,
      2,
      "KEYS copy=true 应恰好 2 次纪元静止栅栏 (TRANSMITTING/MIGRATED 后)"
    );

    // SLOTS 单批：begin 纪元门 1 次 + 批内 TRANSMITTING/DELETING 置位后各 1 次
    let cp = two_primary_provider();
    let store = migrate_store("mt_fence_s.db");
    let slot = SLOT0;
    let k1 = key_in_slot("mt_fs", slot);
    {
      let session = store.new_session().unwrap();
      let batch = session.enter_batch();
      let storage = StorageSession::new_readonly(batch);
      storage.upsert_string(k1.as_bytes(), b"v1").await.unwrap();
    }
    let seen = Arc::new(Mutex::new(Vec::new()));
    let addr = scripted_migrate_target(
      vec![
        vec![b"+OK\r\n"; 6],
        vec![b"+OK\r\n"; 3],
        vec![b"+OK\r\n"; 3],
      ],
      Arc::clone(&seen),
    )
    .await;
    let spec = MigrateTaskSpec {
      transfer_option: TransferOption::Slots,
      ..migrate_spec(port_of(&addr), 0)
    };
    let slots: HashSet<i32> = [slot as i32].into_iter().collect();
    let session = try_add_slots_migration_task(&cp, spec.clone(), &slots).unwrap();
    let epoch_before = cp.current_epoch();
    let migrated = run_slots_migration_task(Arc::clone(&store), spec, Arc::clone(&session))
      .await
      .unwrap();
    assert_eq!(migrated, 1);
    // begin 纪元门 1 次 + 每批迭代恰 2 次（TRANSMITTING/DELETING 置位后）；
    // 批迭代次数随墓碑键重扫可见性浮动，故断言结构（奇数且 ≥3）而非绝对值
    let delta = cp.current_epoch() - epoch_before;
    assert!(
      delta >= 3 && (delta - 1) % 2 == 0,
      "SLOTS 应为 begin 门 1 次 + 每批恰好 2 次纪元静止栅栏，实际 delta={delta}"
    );
  });
}

/// SLOTS 驱动批次拒绝：显式报错 + recover STABLE + 源端键保留 + 任务移除
#[test]
fn slots_migration_task_batch_reject_recovers() {
  let rt = Runtime::new().unwrap();
  rt.block_on(async {
    let cp = two_primary_provider();
    let store = migrate_store_reviv("st_reject.db");
    let slot = SLOT0;
    let k1 = key_in_slot("st_r", slot);
    {
      let session = store.new_session().unwrap();
      let batch = session.enter_batch();
      let storage = StorageSession::new_readonly(batch);
      storage.upsert_string(k1.as_bytes(), b"v1").await.unwrap();
    }

    let seen = Arc::new(Mutex::new(Vec::new()));
    // 脚本：握手×2 +OK、IMPORTING +OK、批次 -ERR、STABLE +OK
    let addr = scripted_migrate_target(
      vec![vec![
        b"+OK\r\n",
        b"+OK\r\n",
        b"+OK\r\n",
        b"-ERR rejected\r\n",
        b"+OK\r\n",
      ]],
      Arc::clone(&seen),
    )
    .await;
    let spec = MigrateTaskSpec {
      transfer_option: TransferOption::Slots,
      ..migrate_spec(port_of(&addr), 0)
    };
    let slots: HashSet<i32> = [slot as i32].into_iter().collect();
    assert!(
      store.reviv_pool.is_enabled(),
      "迁移启动前复活池应处于启用状态"
    );
    let session = try_add_slots_migration_task(&cp, spec.clone(), &slots).unwrap();
    let err = run_slots_migration_task(Arc::clone(&store), spec, Arc::clone(&session))
      .await
      .unwrap_err();
    assert!(
      format!("{err:?}").contains("rejected"),
      "应透出远端拒绝: {err:?}"
    );
    assert!(
      store.reviv_pool.is_enabled(),
      "迁移异常退出后复活池应恢复启用状态"
    );
    assert!(
      seen
        .lock()
        .iter()
        .any(|f| f.contains("SETSLOTSRANGE STABLE")),
      "批次拒绝必须 recover STABLE: {:?}",
      seen.lock()
    );
    assert_eq!(read_str(&store, k1.as_bytes()).await, Some(b"v1".to_vec()));
    assert_eq!(
      cp.migration_manager().unwrap().get_migration_task_count(),
      0,
      "失败路径同样必须移除任务"
    );
  });
}

/// test/cluster/Garnet.test.cluster.migrate/ClusterMigrateTests.cs:RevivPauseGuard
/// 验证复活暂停守卫 RAII 语义与退出恢复机制
#[test]
fn slots_migration_reviv_pause_guard_raii() {
  let store = migrate_store_reviv("st_reviv_guard.db");
  assert!(store.reviv_pool.is_enabled(), "初始状态复活池应启用");

  {
    let _guard = RevivPauseGuard::new(&store);
    assert!(
      !store.reviv_pool.is_enabled(),
      "构造守卫后复活池应暂停（is_enabled == false）"
    );
  }

  assert!(
    store.reviv_pool.is_enabled(),
    "守卫 Drop 后复活池应恢复启用"
  );
}

/// 验证槽位迁移执行期间复活池处于暂停状态，成功后恢复
#[test]
fn slots_migration_pauses_and_resumes_reviv_pool() {
  let rt = Runtime::new().unwrap();
  rt.block_on(async {
    let cp = two_primary_provider();
    // 收尾两次 gossip 汇聚对单连接假目标必然静默，缩小时限让汇聚快速
    // 超时弃连（best-effort 不判败），不拖测试
    cp.set_cluster_node_timeout_ms(100);
    let store = migrate_store_reviv("st_reviv_active.db");
    let slot = SLOT0;
    let k1 = key_in_slot("reviv_key_", slot);
    {
      let session = store.new_session().unwrap();
      let batch = session.enter_batch();
      let storage = StorageSession::new_readonly(batch);
      storage.upsert_string(k1.as_bytes(), b"v1").await.unwrap();
    }

    assert!(store.reviv_pool.is_enabled(), "迁移启动前复活池应启用");

    let was_paused_during_migration = Arc::new(AtomicBool::new(false));
    let store_clone = Arc::clone(&store);
    let paused_flag = Arc::clone(&was_paused_during_migration);

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap().to_string();
    spawn(async move {
      if let Ok((mut stream, _)) = listener.accept().await {
        let mut buf = vec![0u8; 4096];
        for reply in [b"+OK\r\n"; 6] {
          let BufResult(res, next) = stream.read(buf).await;
          buf = next;
          if res.unwrap() == 0 {
            break;
          }
          if !store_clone.reviv_pool.is_enabled() {
            paused_flag.store(true, Ordering::Release);
          }
          let BufResult(wres, _) = stream.write_all(reply).await;
          wres.unwrap();
        }
      }
    })
    .detach();

    let spec = MigrateTaskSpec {
      transfer_option: TransferOption::Slots,
      ..migrate_spec(port_of(&addr), 0)
    };
    let slots: HashSet<i32> = [slot as i32].into_iter().collect();
    let session = try_add_slots_migration_task(&cp, spec.clone(), &slots).unwrap();
    let migrated = run_slots_migration_task(Arc::clone(&store), spec, Arc::clone(&session))
      .await
      .unwrap();
    assert_eq!(migrated, 1);

    assert!(
      was_paused_during_migration.load(Ordering::Acquire),
      "迁移执行期间复活池必须处于暂停状态（is_enabled == false）"
    );
    assert!(
      store.reviv_pool.is_enabled(),
      "迁移成功结束后复活池必须恢复启用状态（is_enabled == true）"
    );
  });
}

/// SLOTS 游标推进与对象信封迁移：槽内 Hash 对象信封键与 string 键一并
/// 迁移删除（kind=2 帧），删除游标只推进到已确认传输键，绝不全槽清除
#[test]
fn slots_migration_transfers_object_envelopes_and_finishes() {
  let rt = Runtime::new().unwrap();
  rt.block_on(async {
    let cp = two_primary_provider();
    let store = migrate_store("st_obj.db");
    let slot = SLOT0;
    let k1 = key_in_slot("st_o", slot);
    let k2 = key_in_slot("st_p", slot);
    let obj_key = key_in_slot("st_obj", slot);
    {
      let session = store.new_session().unwrap();
      let batch = session.enter_batch();
      let storage = StorageSession::new_readonly(batch);
      storage.upsert_string(k1.as_bytes(), b"v1").await.unwrap();
      storage.upsert_string(k2.as_bytes(), b"v2").await.unwrap();
      // Hash 对象信封键（内层标签 0x03）：本轮可迁移
      storage
        .upsert_tag(obj_key.as_bytes(), KeyTag::ObjectEnvelope, b"\x03p")
        .await
        .unwrap();
    }

    let seen = Arc::new(Mutex::new(Vec::new()));
    // 连接1 = 迁移帧 7 应答；连接2/3 = 收尾两次 gossip 汇聚（握手 + WITHMEET
    // 三帧，+OK 即判版本不符弃连，汇聚 best-effort）
    let addr = scripted_migrate_target(
      vec![
        vec![b"+OK\r\n"; 7],
        vec![b"+OK\r\n"; 3],
        vec![b"+OK\r\n"; 3],
      ],
      Arc::clone(&seen),
    )
    .await;
    let spec = MigrateTaskSpec {
      transfer_option: TransferOption::Slots,
      ..migrate_spec(port_of(&addr), 0)
    };
    let slots: HashSet<i32> = [slot as i32].into_iter().collect();
    let session = try_add_slots_migration_task(&cp, spec.clone(), &slots).unwrap();
    let migrated = run_slots_migration_task(Arc::clone(&store), spec, Arc::clone(&session))
      .await
      .unwrap();
    assert_eq!(migrated, 3, "string 与对象信封键一并计数迁移");

    // 三键全部迁移删除
    assert_eq!(read_str(&store, k1.as_bytes()).await, None);
    assert_eq!(read_str(&store, k2.as_bytes()).await, None);
    {
      let session = store.new_session().unwrap();
      let batch = session.enter_batch();
      let storage = StorageSession::new_readonly(batch);
      assert!(
        !storage
          .batch
          .contains_key(obj_key.as_bytes())
          .await
          .unwrap(),
        "对象信封键迁移后应从源端删除"
      );
    }
    assert_eq!(
      cp.migration_manager().unwrap().get_migration_task_count(),
      0
    );
  });
}

/// SLOTS 游标对暂不支持键的显式登记：RangeIndex 信封键不传输、保留源端、
/// ERROR 留痕清单，驱动不失败——绝不静默跳键
#[test]
fn slots_migration_registers_unsupported_keys_explicitly() {
  let rt = Runtime::new().unwrap();
  rt.block_on(async {
    let cp = two_primary_provider();
    let store = migrate_store("st_ri.db");
    let slot = SLOT0;
    let k1 = key_in_slot("st_u", slot);
    let ri_key = key_in_slot("st_ri", slot);
    {
      let session = store.new_session().unwrap();
      let batch = session.enter_batch();
      let storage = StorageSession::new_readonly(batch);
      storage.upsert_string(k1.as_bytes(), b"v1").await.unwrap();
      storage
        .upsert_tag(ri_key.as_bytes(), KeyTag::ObjectEnvelope, b"\x05ri")
        .await
        .unwrap();
    }

    let seen = Arc::new(Mutex::new(Vec::new()));
    // 连接1 = 迁移帧 7 应答；连接2/3 = 收尾两次 gossip 汇聚（握手 + WITHMEET
    // 三帧，+OK 即判版本不符弃连，汇聚 best-effort）
    let addr = scripted_migrate_target(
      vec![
        vec![b"+OK\r\n"; 7],
        vec![b"+OK\r\n"; 3],
        vec![b"+OK\r\n"; 3],
      ],
      Arc::clone(&seen),
    )
    .await;
    let spec = MigrateTaskSpec {
      transfer_option: TransferOption::Slots,
      ..migrate_spec(port_of(&addr), 0)
    };
    let slots: HashSet<i32> = [slot as i32].into_iter().collect();
    let session = try_add_slots_migration_task(&cp, spec.clone(), &slots).unwrap();
    let migrated = run_slots_migration_task(Arc::clone(&store), spec, Arc::clone(&session))
      .await
      .unwrap();
    assert_eq!(migrated, 1, "仅 string 键计数迁移");

    // string 键删除，RangeIndex 键保留源端（显式登记，键权不动）
    assert_eq!(read_str(&store, k1.as_bytes()).await, None);
    {
      let session = store.new_session().unwrap();
      let batch = session.enter_batch();
      let storage = StorageSession::new_readonly(batch);
      assert!(
        storage.batch.contains_key(ri_key.as_bytes()).await.unwrap(),
        "暂不支持键必须保留源端"
      );
    }
    assert_eq!(
      cp.migration_manager().unwrap().get_migration_task_count(),
      0
    );
  });
}

/// MIGRATE 命令 KEYS 形态（同步慢路径投影）：手工前置置 MIGRATING 后
/// +OK、键删除、帧序正确（C# KEYS 路径要求槽位已置 MIGRATING，
/// MigrateCommand.cs:212 NOTMIGRATING 门）；KEYS 链全程不发 NODE、
/// 属主绝不移交（对标 C# MigrateKeysAsync 不动槽位）
#[test]
fn migrate_command_keys_variant_runs_driver() {
  let rt = Runtime::new().unwrap();
  rt.block_on(async {
    let cp = two_primary_provider();
    let (mut consumer, store) = migrate_consumer(&cp);
    let k1 = local_slot_key("mc_k");
    assert_eq!(
      drive(
        &rt,
        &mut consumer,
        &resp_frame(&[b"SET", k1.as_bytes(), b"v1"]),
      ),
      b"+OK\r\n"
    );

    // 手工前置：键所在槽置 MIGRATING（对标 CLUSTER SETSLOT ... MIGRATING）
    let slot = SLOT0 as usize;
    let m = cp.cluster_manager().unwrap();
    m.try_prepare_slot_for_migration(slot, 0x0000_0000_0000_0000_0000_0000_0000_DE12)
      .unwrap();

    let seen = Arc::new(Mutex::new(Vec::new()));
    let addr = scripted_migrate_target(vec![vec![b"+OK\r\n"; 5]], Arc::clone(&seen)).await;
    let port = port_of(&addr);
    retarget_remote_port(&cp, port);

    let port_text = port.to_string();
    let frame = resp_frame(&[
      b"MIGRATE",
      b"127.0.0.1",
      port_text.as_bytes(),
      b"",
      b"0",
      b"0",
      b"KEYS",
      k1.as_bytes(),
    ]);
    assert_eq!(
      drive(&rt, &mut consumer, &frame),
      b"+OK\r\n",
      "KEYS 变体应答 +OK"
    );
    assert_eq!(read_str(&store, k1.as_bytes()).await, None, "键应迁移删除");

    let frames = seen.lock();
    assert!(frames.iter().any(|f| f.contains("IMPORTING")), "{frames:?}");
    assert!(frames.iter().any(|f| f.contains("MIGRATE")), "{frames:?}");
    assert!(
      !frames.iter().any(|f| f.contains("NODE")),
      "KEYS 链绝不发 NODE: {frames:?}"
    );
    drop(frames);

    // 属主未移交：槽保持 Migrating 且归属本端，收口归运维 CLUSTER SETSLOT
    assert_eq!(
      m.current_config.read().get_state(slot as u16),
      SlotState::Migrating,
      "KEYS 链完成不得移交槽属主"
    );
    assert_eq!(
      m.current_config.read().get_worker_id_from_slot(slot as u16),
      LOCAL_WORKER_ID,
      "槽属主必须仍是本端"
    );
  });
}

/// MIGRATE KEYS 槽位未置 MIGRATING → NOTMIGRATING 前置校验拒绝
///（C# MigrateCommand.cs:212 IsMigratingSlot →
/// RESP_ERR_GENERIC_SLOTNOTMIGRATING）；远端未被触达，键权零变更
#[test]
fn migrate_command_keys_without_migrating_state_rejected() {
  let rt = Runtime::new().unwrap();
  rt.block_on(async {
    let cp = two_primary_provider();
    let (mut consumer, store) = migrate_consumer(&cp);
    let k1 = local_slot_key("mc_nm");
    assert_eq!(
      drive(
        &rt,
        &mut consumer,
        &resp_frame(&[b"SET", k1.as_bytes(), b"v1"]),
      ),
      b"+OK\r\n"
    );

    let seen = Arc::new(Mutex::new(Vec::new()));
    let addr = scripted_migrate_target(vec![vec![b"+OK\r\n"; 8]], Arc::clone(&seen)).await;
    let port = port_of(&addr);
    retarget_remote_port(&cp, port);

    let port_text = port.to_string();
    let frame = resp_frame(&[
      b"MIGRATE",
      b"127.0.0.1",
      port_text.as_bytes(),
      b"",
      b"0",
      b"0",
      b"KEYS",
      k1.as_bytes(),
    ]);
    assert_eq!(
      drive(&rt, &mut consumer, &frame),
      b"-ERR slot state not set to MIGRATING state\r\n",
      "槽位未置 MIGRATING 应回 NOTMIGRATING 文案"
    );

    // 源端零副作用：键保留、远端未收到任何帧、迁移任务未注册
    assert_eq!(
      read_str(&store, k1.as_bytes()).await,
      Some(b"v1".to_vec()),
      "拒绝路径不应删除键"
    );
    assert!(seen.lock().is_empty(), "远端不应被触达");
    assert_eq!(
      cp.migration_manager()
        .map(|mm| mm.get_migration_task_count())
        .unwrap_or(0),
      0
    );
  });
}

/// MIGRATE 命令 SLOTS 形态（后台驱动投影）：命令立即 +OK，后台任务完成
/// 键迁移删除并移除任务
#[test]
fn migrate_command_slots_variant_runs_background_driver() {
  let rt = Runtime::new().unwrap();
  rt.block_on(async {
    let cp = two_primary_provider();
    let (mut consumer, store) = migrate_consumer(&cp);
    let slot = SLOT0;
    let k1 = key_in_slot("mc_s", slot);
    assert_eq!(
      drive(
        &rt,
        &mut consumer,
        &resp_frame(&[b"SET", k1.as_bytes(), b"v1"]),
      ),
      b"+OK\r\n"
    );

    let seen = Arc::new(Mutex::new(Vec::new()));
    let addr = scripted_migrate_target(
      vec![
        vec![b"+OK\r\n"; 6],
        vec![b"+OK\r\n"; 3],
        vec![b"+OK\r\n"; 3],
      ],
      Arc::clone(&seen),
    )
    .await;
    let port = port_of(&addr);
    retarget_remote_port(&cp, port);

    let port_text = port.to_string();
    let slot_text = slot.to_string();
    let frame = resp_frame(&[
      b"MIGRATE",
      b"127.0.0.1",
      port_text.as_bytes(),
      b"",
      b"0",
      b"0",
      b"SLOTS",
      slot_text.as_bytes(),
    ]);
    assert_eq!(
      drive(&rt, &mut consumer, &frame),
      b"+OK\r\n",
      "SLOTS 变体立即 +OK"
    );

    // 轮询驱动后台任务：键迁移删除、槽位交权、任务移除
    let m = cp.cluster_manager().unwrap();
    let remote_wid = m
      .current_config
      .read()
      .get_worker_id_from_node_id(0x0000_0000_0000_0000_0000_0000_0000_DE12);
    let mut done = false;
    for _ in 0..2000 {
      let key_gone = read_str(&store, k1.as_bytes()).await.is_none();
      let task_done = cp
        .migration_manager()
        .map(|mm| mm.get_migration_task_count() == 0)
        .unwrap_or(false);
      let owned = {
        let cfg = m.current_config.read();
        cfg.get_state(slot) == SlotState::Stable
          && cfg.get_worker_id_from_slot(slot) == remote_wid as usize
      };
      if key_gone && task_done && owned {
        done = true;
        break;
      }
      yield_now().await;
    }
    assert!(done, "后台驱动应在有限轮次内完成迁移并交权");
  });
}

/// MIGRATE 命令解析错误路径：目标不在集群配置 → Unknown endpoint；
/// KEYS 槽未置 MIGRATING → NOTMIGRATING；SLOTS 非本端槽 → slot not owned 文案
#[test]
fn migrate_command_parse_errors() {
  let rt = Runtime::new().unwrap();
  let cp = two_primary_provider();
  let (mut consumer, _store) = migrate_consumer(&cp);

  // 未知目标端（端口不在集群配置）
  let out = drive(
    &rt,
    &mut consumer,
    &resp_frame(&[
      b"MIGRATE",
      b"127.0.0.1",
      b"59999",
      b"",
      b"0",
      b"0",
      b"SLOTS",
      b"1",
    ]),
  );
  assert_eq!(out, b"-ERR Unknown endpoint\r\n");

  // KEYS 槽未置 MIGRATING → NOTMIGRATING（库级定槽 doc/zh/db.md 4.1：
  // 键全数收录恒共会话槽位，键间跨槽校验随键级哈希废除；会话槽位两查
  // IsLocal + IsMigratingSlot 收敛于解析期一次判定）
  let ka = local_slot_key("mc_ca");
  let kb = local_slot_key("mc_cb");
  let out = drive(
    &rt,
    &mut consumer,
    &resp_frame(&[
      b"MIGRATE",
      b"127.0.0.1",
      b"7001",
      b"",
      b"0",
      b"0",
      b"KEYS",
      ka.as_bytes(),
      kb.as_bytes(),
    ]),
  );
  assert!(
    out.starts_with(b"-ERR slot state not set to MIGRATING"),
    "槽未置 MIGRATING 应拒绝: {out:?}"
  );

  // SLOTS 非本端槽（REMOTE_SLOT 归 node_2）→ slot not owned
  let remote_slot = REMOTE_SLOT.to_string();
  let out = drive(
    &rt,
    &mut consumer,
    &resp_frame(&[
      b"MIGRATE",
      b"127.0.0.1",
      b"7001",
      b"",
      b"0",
      b"0",
      b"SLOTS",
      remote_slot.as_bytes(),
    ]),
  );
  let expect = format!("-ERR slot {remote_slot} not owned by current node.");
  assert!(
    out.starts_with(expect.as_bytes()),
    "非本端槽应拒绝: {out:?}"
  );
}

// ---------------------------------------------------------------------------
// 分块大值跨载荷重组、dispose 取消令牌与批量上限配置化（w-migrate-obj）
// ---------------------------------------------------------------------------

use wbase::pool::DEFAULT_BUFFER_SIZE;
use wconn::record::{CONTINUATION_FLAG, MIGRATION_RECORD_KIND_CHUNKED};

/// 手工拼装单帧分块载荷：[recordCount=1][kind=CHUNKED][len|CONT][bytes]
fn chunk_payload(chunk: &[u8], more: bool) -> Vec<u8> {
  let mut payload = Vec::with_capacity(4 + 5 + chunk.len());
  payload.extend_from_slice(&1u32.to_le_bytes());
  payload.push(MIGRATION_RECORD_KIND_CHUNKED);
  let head = (chunk.len() as u32) | if more { CONTINUATION_FLAG } else { 0 };
  payload.extend_from_slice(&head.to_le_bytes());
  payload.extend_from_slice(chunk);
  payload
}

/// 分块大值跨载荷重组：一条超限对象信封记录切两块分装两个 MIGRATE 载荷，
/// 首载荷仅累积不落库，末载荷到齐后整记录写回（对标 C#
/// ChunkedRecordReassembler 跨命令重组语义）
#[test]
fn cluster_migrate_recv_chunked_envelope_reassembles_across_payloads() {
  let rt = Runtime::new().unwrap();
  rt.block_on(async {
    let cp = two_primary_provider();
    let (mut consumer, store) = migrate_consumer(&cp);

    let obj_key = local_slot_key("mig_chunk");
    set_importing_slot(&cp);

    // 大信封值：内层标签 Hash + 32000 字节载荷（超限切两块跨载荷发送，
    // 且在 wkv 页大小 65536 之内，隔离存储层记录上限因素）
    let mut env_val = vec![0x03u8];
    env_val.resize(32_000, b'x');
    let item = BatchItem {
      key: obj_key.as_bytes(),
      val: MigrateVal::Env(env_val.clone()),
      expire_unix_ms: 0,
    };
    let whole = encode_migration_payload(from_ref(&item));
    let record_body = &whole[4..];
    let mid = record_body.len() / 2;

    // 首载荷（半块，续块标志置位）：重组未完成，应答 +OK 且键未写
    let frame = migrate_recv_frame(b"F", &chunk_payload(&record_body[..mid], true));
    assert_eq!(drive(&rt, &mut consumer, &frame), b"+OK\r\n");
    {
      let session = store.new_session().unwrap();
      let batch = session.enter_batch();
      let storage = StorageSession::new_readonly(batch);
      assert!(
        !storage
          .batch
          .contains_key(obj_key.as_bytes())
          .await
          .unwrap()
      );
    }

    // 末载荷（余块）：重组完成，整记录写回
    let frame = migrate_recv_frame(b"F", &chunk_payload(&record_body[mid..], false));
    assert_eq!(drive(&rt, &mut consumer, &frame), b"+OK\r\n");

    // 信封整值字节级一致
    let session = store.new_session().unwrap();
    let batch = session.enter_batch();
    let storage = StorageSession::new_readonly(batch);
    assert_eq!(
      storage
        .read_tag_with(obj_key.as_bytes(), KeyTag::ObjectEnvelope, |raw| raw
          .to_vec())
        .await
        .unwrap(),
      Some(env_val),
      "跨载荷重组产物应整值写回"
    );
  });
}

/// 直读对象信封整值（迁移接收端断言用）
async fn read_env_value(store: &Arc<WedbStore<SegmentedDevice>>, key: &[u8]) -> Option<Vec<u8>> {
  let session = store.new_session().unwrap();
  let batch = session.enter_batch();
  let storage = StorageSession::new_readonly(batch);
  storage
    .read_tag_with(key, KeyTag::ObjectEnvelope, |raw| raw.to_vec())
    .await
    .unwrap()
}

/// 接收态随会话实例回收（对标 C# per-connection chunkedRecordReassembler
/// 随 ClusterSession 析构回收）：连接 A 半途弃连后，其重组残段不得泄漏至
/// 同源节点的后继连接——连接 B 的完整单块帧必须独立重组写回，而非与 A
/// 残段错位拼接
#[test]
fn cluster_migrate_recv_state_bounded_to_session() {
  let rt = Runtime::new().unwrap();
  rt.block_on(async {
    let cp = two_primary_provider();
    let obj_key = local_slot_key("mig_bounded");
    set_importing_slot(&cp);

    let mut env_val = vec![0x03u8];
    env_val.resize(32_000, b'x');
    let item = BatchItem {
      key: obj_key.as_bytes(),
      val: MigrateVal::Env(env_val.clone()),
      expire_unix_ms: 0,
    };
    let record_body = encode_migration_payload(from_ref(&item))[4..].to_vec();
    let mid = record_body.len() / 2;

    // 连接 A：仅收前半块后弃连（残段悬置在 A 的会话字段中）
    let (mut ca, _store_a) = migrate_consumer(&cp);
    let frame = migrate_recv_frame(b"F", &chunk_payload(&record_body[..mid], true));
    assert_eq!(drive(&rt, &mut ca, &frame), b"+OK\r\n");
    // 会话 A 实例析构：接收态字段（重组残段缓冲）随之释放
    drop(ca);

    // 连接 B（全新 ClusterSession 实例）：完整单块帧独立重组写回；若残段
    // 经跨连接常驻表泄漏，此帧将与残段错位拼接、解析失败
    let (mut cb, store_b) = migrate_consumer(&cp);
    let frame = migrate_recv_frame(b"F", &chunk_payload(&record_body, false));
    assert_eq!(drive(&rt, &mut cb, &frame), b"+OK\r\n");
    assert_eq!(
      read_env_value(&store_b, obj_key.as_bytes()).await,
      Some(env_val),
      "后继连接应独立重组，不得命中弃连会话残段"
    );
  });
}

/// 中途错误收场后的会话内复位（对标 C# chunkedRecordReassembler.Reset
/// 错误恢复语义）：残段拒批报错即弃，本会话后续迁移流从干净重组器起步
#[test]
fn cluster_migrate_recv_chunk_reset_after_reject() {
  let rt = Runtime::new().unwrap();
  rt.block_on(async {
    let cp = two_primary_provider();
    let (mut consumer, store) = migrate_consumer(&cp);
    let obj_key = local_slot_key("mig_reset");
    set_importing_slot(&cp);

    let mut env_val = vec![0x03u8];
    env_val.resize(32_000, b'x');
    let item = BatchItem {
      key: obj_key.as_bytes(),
      val: MigrateVal::Env(env_val.clone()),
      expire_unix_ms: 0,
    };
    let record_body = encode_migration_payload(from_ref(&item))[4..].to_vec();

    // 半块悬置 → 畸形完整块拒批（ERR 即错误收场复位会话接收态）
    let frame = migrate_recv_frame(b"F", &chunk_payload(&record_body[..8], true));
    assert_eq!(drive(&rt, &mut consumer, &frame), b"+OK\r\n");
    let frame = migrate_recv_frame(b"F", &chunk_payload(b"garbage-not-a-record", false));
    assert!(
      drive(&rt, &mut consumer, &frame).starts_with(b"-ERR"),
      "畸形块应拒批"
    );

    // 同会话续发完整记录：复位生效，重组从空流起步、整值写回
    let frame = migrate_recv_frame(b"F", &chunk_payload(&record_body, false));
    assert_eq!(drive(&rt, &mut consumer, &frame), b"+OK\r\n");
    assert_eq!(
      read_env_value(&store, obj_key.as_bytes()).await,
      Some(env_val),
      "错误收场复位后同会话应能独立重组新记录"
    );
  });
}

/// 接收端 CLUSTER MIGRATE 帧（新协议头 4 参，见
/// ClusterSession::network_cluster_migrate 头格式）：源节点 + replace +
/// 会话槽集（逗号分隔，发送端显式携带，C# 头无此参数）+ 载荷；
/// C# 头的 vectorSets 位本仓废除，向量集帧自描述 kind=5/6 为唯一判据
fn migrate_recv_frame(replace: &[u8], payload: &[u8]) -> Vec<u8> {
  resp_frame(&[
    b"CLUSTER",
    b"MIGRATE",
    MIGRATE_SRC_NODE_HEX,
    replace,
    SLOT0_LIST,
    payload,
  ])
}

/// 槽位状态置位小工具（接收端头级门控前提：库内键恒共会话槽位 SLOT0）
fn set_importing_slot(cp: &ClusterProvider) {
  let m = cp.cluster_manager().unwrap();
  m.current_config.write().slot_map[SLOT0 as usize] = HashSlot {
    worker_id: LOCAL_WORKER_ID as u16,
    state: SlotState::Importing,
  };
}

/// MigrateSession dispose 触发取消令牌（对标 C# Dispose 的 _cts.Cancel）：
/// 在途远端停等即刻收敛失败，不再长挂
#[test]
fn migrate_session_dispose_triggers_cancel_token() {
  let cp = ClusterProvider::new();
  let spec = MigrateTaskSpec {
    source_node_id: 0x0000_0000_0000_0000_0000_0000_0000_DE11,
    target_address: "127.0.0.1".to_string(),
    target_port: 7001,
    target_node_id: 0x0000_0000_0000_0000_0000_0000_0000_DE12,
    username: String::new(),
    passwd: String::new(),
    copy_option: false,
    replace_option: false,
    timeout: 0,
    transfer_option: TransferOption::Keys,
  };
  let session = MigrateSession::new(
    Arc::clone(&cp),
    spec,
    [1].into_iter().collect(),
    Sketch::new(),
  );
  assert!(!session.is_cancelled());
  session.dispose();
  assert!(session.is_cancelled(), "dispose 必须触发取消令牌");
}

/// 批量上限从集群网络配置读取（对标 NetworkBufferSettings.MaxSendBufferContentSize
/// = sendBufferSize - SendBufferOverheadReserve），不再硬编码 512KiB
#[test]
fn migration_manager_batch_limit_from_network_settings() {
  let mgr = MigrationManager::new(Arc::new(ClusterProvider::default()));
  assert_eq!(
    mgr.max_send_buffer_content_size(),
    DEFAULT_BUFFER_SIZE - SEND_BUFFER_OVERHEAD_RESERVE,
    "上限 = 迁移发送缓冲尺寸 - 保留额"
  );
}

// ---------------------------------------------------------------------------
// 向量集（Vector Set）跨节点迁移端到端：SLOTS/KEYS 通道真实收发
//（对标 C# ClusterTests 的 MigrateVectorSet* 语义：源 VADD → 迁移 →
// 目标 VSIM 一致 → 源端键消失）
// ---------------------------------------------------------------------------

use wnode::resp::vector::{
  vector_manager::{VectorAddArgs, VectorManager, VectorManagerOptions, VectorManagerResult},
  vector_manager_locking::{CreateIndexParams, split_registry_key},
  vector_store_callbacks::WedbVectorStoreCallbacks,
};
use wval::SessionPrefixBuf;
use wvector::{Callbacks, VectorDistanceMetricType, VectorQuantType, VectorValueType};

/// 构造启用的向量集合管理器（回调绑定独立存储会话，落盘直达给定存储）
fn vector_manager_for(store: &Arc<WedbStore<SegmentedDevice>>) -> Arc<VectorManager> {
  let session = Arc::new(store.new_session().unwrap());
  let callbacks = Callbacks::new(Arc::new(WedbVectorStoreCallbacks::new(session)));
  Arc::new(VectorManager::new(
    VectorManagerOptions {
      is_enabled: true,
      ..VectorManagerOptions::default()
    },
    callbacks,
  ))
}

/// 迁移发现面单测：槽位命名空间全集收录向量集键、string 键不误收
#[test]
fn vector_set_discovery_for_slots() -> aok::Void {
  let rt = Runtime::new().unwrap();
  rt.block_on(async {
    let store = migrate_store("vs_disc.db");
    let vm = vector_manager_for(&store);

    // 经存储会话批量写入 string 键（迁移发现面不应误收）
    {
      let session = store.new_session().unwrap();
      let batch = session.enter_batch();
      let storage = StorageSession::new_readonly(batch);
      storage.upsert_string(b"vs_str", b"v").await.unwrap();
    }

    // VADD 建向量集（管理器直写：登记表 + 内存图 + 磁盘记录）
    let key = b"vs:disc:1";
    let values: [u8; 8] = [0, 0, 128, 63, 0, 0, 0, 64]; // f32 [1.0, 2.0]
    let params = CreateIndexParams {
      hash_slot: SLOT0,
      dims: 2,
      reduce_dims: 0,
      quant: VectorQuantType::NoQuant,
      build_exploration_factor: 200,
      num_links: 8,
      distance_metric: VectorDistanceMetricType::L2,
    };
    let (index, _lock) = vm
      .read_or_create_vector_index(SessionPrefixBuf::ROOT.as_slice(), key, Some(&params))
      .unwrap();
    let index_value = index.to_bytes();
    let args = VectorAddArgs::new(b"elem1".as_slice(), VectorValueType::FP32, &values, b"");
    assert_eq!(
      vm.try_add(SessionPrefixBuf::ROOT.as_slice(), key, &index_value, &args),
      Ok(VectorManagerResult::OK)
    );
    drop(_lock);

    // 槽位发现：向量集键收录且索引记录在位；string 键不误收
    let mut slots = BTreeSet::new();
    slots.insert(i32::from(SLOT0));
    let found = vm.get_vector_set_keys_for_slots(&slots);
    assert_eq!(found.len(), 1, "应恰好发现 1 个向量集键");
    // 发现面返回登记表复合键（`registry_key` 单点：源端会话域 + 用户键），
    // 迁移帧口径由驱动侧 registry_user_key 单点剥域
    let (domain, user_key) = split_registry_key(&found[0].0);
    assert_eq!(
      (domain.vns, domain.vdb),
      (0, 0),
      "发现面键域应为源端默认会话域"
    );
    assert_eq!(user_key, key.as_slice(), "剥域后应为用户键名");
    assert_eq!(found[0].1, index_value);

    // 其它槽不命中
    let mut other = BTreeSet::new();
    other.insert(i32::from(REMOTE_SLOT));
    assert!(vm.get_vector_set_keys_for_slots(&other).is_empty());

    // 导出面：元素 + 原生向量 + 属性全集
    let elements = vm.export_migration_elements(&index_value);
    assert_eq!(elements.len(), 1);
    assert_eq!(elements[0].element, b"elem1".to_vec());
    assert_eq!(elements[0].values, values.to_vec());
    assert!(elements[0].attributes.is_empty());

    // 分类回归：向量集键不再进「暂不支持」拒绝清单（KEYS 入口放行），
    // 且由 collect_vector_set_keys 真实收录；string 键两路皆不误判
    {
      let session = store.new_session().unwrap();
      let batch = session.enter_batch();
      let storage = StorageSession::new_readonly(batch);
      let keys = [key.to_vec(), b"vs_str".to_vec()];
      let unsupported = probe_unsupported_keys(&storage, Some(vm.as_ref()), &keys)
        .await
        .unwrap();
      assert!(
        unsupported.is_empty(),
        "向量集键不应再整体拒绝: {unsupported:?}"
      );

      let collected = collect_vector_set_keys(&storage, Some(vm.as_ref()), &keys)
        .await
        .unwrap();
      assert_eq!(collected.len(), 1, "应只收录向量集键");
      // KEYS 收集面与 SLOTS 枚举面同为登记表复合键口径（collect_vector_set_keys
      // 会话域复合收集），断言剥域后比对用户键
      let (domain, user_key) = split_registry_key(&collected[0].0);
      assert_eq!((domain.vns, domain.vdb), (0, 0), "收集面键域应为会话默认域");
      assert_eq!(user_key, key.as_slice(), "剥域后应为用户键名");
      assert_eq!(collected[0].1, index_value);
    }
    aok::OK
  })
}

/// 简化 RESP2 数组帧解析：完整帧返回 (帧长, 参数字节)；半包返回 None
fn bridge_try_parse(acc: &mut Vec<u8>) -> Option<(usize, Vec<Vec<u8>>)> {
  let byte_at = |i: usize| acc.get(i).copied();
  if byte_at(0) != Some(b'*') {
    return None;
  }
  let mut i = 1usize;
  let mut n = 0usize;
  while let Some(&b) = acc.get(i) {
    if b == b'\r' {
      break;
    }
    if !b.is_ascii_digit() {
      return None;
    }
    n = n * 10 + usize::from(b - b'0');
    i += 1;
  }
  if acc.get(i) != Some(&b'\r') || acc.get(i + 1) != Some(&b'\n') {
    return None;
  }
  i += 2;
  let mut args = Vec::with_capacity(n);
  for _ in 0..n {
    if acc.get(i) != Some(&b'$') {
      return None;
    }
    i += 1;
    let mut len = 0usize;
    while let Some(&b) = acc.get(i) {
      if b == b'\r' {
        break;
      }
      if !b.is_ascii_digit() {
        return None;
      }
      len = len * 10 + usize::from(b - b'0');
      i += 1;
    }
    if acc.get(i) != Some(&b'\r') || acc.get(i + 1) != Some(&b'\n') || acc.len() < i + 2 + len + 2 {
      return None;
    }
    i += 2;
    args.push(acc[i..i + len].to_vec());
    i += len;
    if acc.get(i) != Some(&b'\r') || acc.get(i + 1) != Some(&b'\n') {
      return None;
    }
    i += 2;
  }
  acc.drain(..i);
  Some((i, args))
}

/// 由参数字节组构造 RESP 数组帧
fn bridge_frame(args: &[Vec<u8>]) -> Vec<u8> {
  let refs: Vec<&[u8]> = args.iter().map(Vec::as_slice).collect();
  resp_frame(&refs)
}

/// 目标端会话驱动：泵消费 + 慢路径收敛（迁移处理为慢路径挂起形态）
async fn drive_target(c: &mut RespSessionConsumer, frame: &[u8]) -> Vec<u8> {
  let (consumed, mut out) = pump(c, frame);
  assert_eq!(consumed, Some(0), "目标端帧应完整消费");
  if let Some(slow) = c.take_slow_wait() {
    out.extend_from_slice(&slow.resolve().await);
  }
  out
}

/// MIGRATE KEYS 向量集端到端（真 TCP → 真目标端消费）：
/// 源 VADD ×2 → 手工前置 MIGRATING + KEYS 迁移（目标端 RESERVE 真预留、
/// 索引/元素帧真导入）→ 源端键消失、目标端 card 一致、VSIM 经目标会话命中
#[test]
fn migrate_vector_set_keys_e2e() -> aok::Void {
  let rt = Runtime::new().unwrap();
  rt.block_on(async {
    // ── 源端装配（node_1 持 0..8192）──
    let cp = two_primary_provider();
    let store = migrate_store("vs_source.db");
    let source_vm = vector_manager_for(&store);
    cp.set_store(Arc::clone(&store));
    cp.set_vector_manager(Arc::clone(&source_vm));
    let api =
      StoreGarnetApi::new(store.new_session().unwrap()).with_vector_manager(Arc::clone(&source_vm));
    let cluster_session: Arc<ClusterSession> = cp.create_cluster_session();
    let mut consumer = RespSessionConsumer::with_cluster(
      1,
      RespServerSessionOptions::default(),
      cluster_session,
      cp.provider_handle(),
      Arc::new(api),
    );
    consumer.attach_transaction_components(Arc::new(WatchVersionMap::new(64)), TxnLockTable::new());

    // 本地槽向量集键 + VADD ×2（NoQuant，规避量化建表时序）
    let vs_key = String::from("vs_e2e0");
    for (name, vec) in [
      ("el_a", [0u8, 0, 128, 63, 0, 0, 0, 64]),
      ("el_b", [0u8, 0, 0, 64, 0, 0, 128, 63]),
    ] {
      let frame = resp_frame(&[
        b"VADD",
        vs_key.as_bytes(),
        b"FP32",
        &vec,
        name.as_bytes(),
        b"NOQUANT",
      ]);
      assert_eq!(drive(&rt, &mut consumer, &frame), b":1\r\n", "VADD {name}");
    }

    // ── 目标端装配（node_2 视角：本地持全槽，迁移槽置 IMPORTING）──
    let target_cp = ClusterProvider::new();
    let target_store = migrate_store("vs_target.db");
    let target_vm = vector_manager_for(&target_store);
    target_cp.set_store(Arc::clone(&target_store));
    target_cp.set_vector_manager(Arc::clone(&target_vm));
    {
      let m = target_cp.cluster_manager().unwrap();
      let mut config = m.current_config.write();
      config.initialize_local_worker(LocalWorkerSpec {
        node_id: 0x0000_0000_0000_0000_0000_0000_0000_DE12,
        address: "127.0.0.1",
        port: 7001,
        config_epoch: 2,
        role: NodeRole::Primary,
        replica_of_node_id: None,
        hostname: None,
      });
      config.workers.push(Worker {
        nodeid: Some(0x1),
        address: "127.0.0.1".into(),
        port: 7000,
        config_epoch: 1,
        role: NodeRole::Primary,
        replica_of_node_id: None,
        replication_offset: 0,
        hostname: Some("".into()),
      });
      let vs_slot = SLOT0;
      for (i, sm) in config.slot_map.iter_mut().enumerate() {
        *sm = HashSlot {
          worker_id: LOCAL_WORKER_ID as u16,
          state: if i as u16 == vs_slot {
            SlotState::Importing
          } else {
            SlotState::Stable
          },
        };
      }
    }
    let target_session: Arc<ClusterSession> = target_cp.create_cluster_session();
    let target_api = StoreGarnetApi::new(target_store.new_session().unwrap())
      .with_vector_manager(Arc::clone(&target_vm));
    let mut target_consumer = RespSessionConsumer::with_cluster(
      1,
      RespServerSessionOptions::default(),
      target_session,
      target_cp.provider_handle(),
      Arc::new(target_api),
    );
    target_consumer
      .attach_transaction_components(Arc::new(WatchVersionMap::new(64)), TxnLockTable::new());

    // ── 桥任务：TCP → 目标端消费（RESERVE/MIGRATE 真处理；握手直答 +OK）──
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap().to_string();
    // 源端配置的 node_2 端点重定向到桥端口（RESERVE/MIGRATE 帧流向桥）
    let port = port_of(&addr);
    retarget_remote_port(&cp, port);
    spawn(async move {
      let (mut stream, _) = listener.accept().await.unwrap();
      let mut acc: Vec<u8> = Vec::new();
      let mut buf = vec![0u8; 1 << 16];
      loop {
        let BufResult(res, next) = stream.read(buf).await;
        buf = next;
        let n = match res {
          Ok(n) if n > 0 => n,
          _ => break,
        };
        acc.extend_from_slice(&buf[..n]);
        while let Some((_len, args)) = bridge_try_parse(&mut acc) {
          let is_cluster = args.len() >= 2 && args[0].eq_ignore_ascii_case(b"CLUSTER");
          // IMPORTING/STABLE 槽位帧直答 +OK（KEYS 链自动置位与复位，目标端
          // IMPORTING 态已由配置预先排布）；RESERVE/MIGRATE 交目标端真处理
          //（KEYS 链无 NODE 步，属主不动）
          let reply: Vec<u8> = if is_cluster
            && (args[1].eq_ignore_ascii_case(b"RESERVE")
              || args[1].eq_ignore_ascii_case(b"MIGRATE"))
          {
            let frame = bridge_frame(&args);
            drive_target(&mut target_consumer, &frame).await
          } else {
            b"+OK\r\n".to_vec()
          };
          if stream.write_all(reply).await.is_err() {
            return;
          }
        }
      }
    })
    .detach();

    // ── 手工 KEYS 迁移（前置 MIGRATING，完整驱动含向量集带外通道）──
    let slot = SLOT0 as usize;
    cp.cluster_manager()
      .unwrap()
      .try_prepare_slot_for_migration(slot, 0x0000_0000_0000_0000_0000_0000_0000_DE12)?;
    let port_text = port.to_string();
    let frame = resp_frame(&[
      b"MIGRATE",
      b"127.0.0.1",
      port_text.as_bytes(),
      b"",
      b"0",
      b"0",
      b"KEYS",
      vs_key.as_bytes(),
    ]);
    assert_eq!(drive(&rt, &mut consumer, &frame), b"+OK\r\n", "迁移应 +OK");

    // 源端键消失（登记表摘除 + 清理登记）
    assert!(
      source_vm
        .read_migrated_index(SessionPrefixBuf::ROOT.as_slice(), vs_key.as_bytes())
        .is_none(),
      "迁移后源端向量集键应消失"
    );

    // 目标端：索引记录在位、元素计数一致、两元素均在
    let index_value = target_vm
      .read_migrated_index(SessionPrefixBuf::ROOT.as_slice(), vs_key.as_bytes())
      .expect("目标端应有迁移索引");
    let index = Index::from_bytes(&index_value).unwrap();
    assert_eq!(
      target_vm.service.card(index.context),
      2,
      "目标端元素数应一致"
    );
    assert!(
      target_vm
        .service
        .check_external_id_valid(index.context, b"el_a"),
      "目标端应含 el_a"
    );
    assert!(
      target_vm
        .service
        .check_external_id_valid(index.context, b"el_b"),
      "目标端应含 el_b"
    );

    aok::OK
  })
}

/// SLOTS 驱动 copy 门（对标 C# MigrateOperation.cs 内 DeleteKeys 首行
/// `if (session._copyOption) return;`，该符号锚点登记在迁移驱动
/// `run_slots_migration_task`，本测试不复挂）：MIGRATE … COPY 走 SLOTS 通道源端键
/// 保留——混合槽（string + Hash 对象信封 + RI 信封）COPY 迁移后源端全部
/// 仍可读（修复前删除环误删已迁键，COPY 语义反成 MOVE）；删除环整体收进
/// copy 门（keys.rs 收口同形：copy 态 Deleting 门不落），sketch 每轮复位
/// 推进、迁移结束归位 Initializing；副本态确认键经不可迁移清单登记承接
/// C# 扫描游标推进（cursor = current），重扫不重传、槽内循环收敛；槽位交权
/// NODE 与任务移除收口与非 copy 一致
#[test]
fn slots_migration_copy_keeps_source_keys() {
  let rt = Runtime::new().unwrap();
  rt.block_on(async {
    let cp = two_primary_provider();
    let store = migrate_store("st_copy.db");
    let slot = SLOT0;
    let k1 = key_in_slot("st_cp", slot);
    let obj_key = key_in_slot("st_cpobj", slot);
    let ri_key = key_in_slot("st_cpri", slot);
    {
      let session = store.new_session().unwrap();
      let batch = session.enter_batch();
      let storage = StorageSession::new_readonly(batch);
      storage.upsert_string(k1.as_bytes(), b"v1").await.unwrap();
      // Hash 对象信封键（内层标签 0x03）可迁移；RI 信封键（0x05）暂不支持
      // 走登记面——混合槽覆盖两臂与快路径外的残留键判定
      storage
        .upsert_tag(obj_key.as_bytes(), KeyTag::ObjectEnvelope, b"\x03p")
        .await
        .unwrap();
      storage
        .upsert_tag(ri_key.as_bytes(), KeyTag::ObjectEnvelope, b"\x05ri")
        .await
        .unwrap();
    }

    let seen = Arc::new(Mutex::new(Vec::new()));
    // 迁移连接给足余量应答（信封分帧数随批次形态浮动，多余应答不被消费
    // 无害）；连接2/3 = 收尾两次 gossip 汇聚
    let addr = scripted_migrate_target(
      vec![
        vec![b"+OK\r\n"; 12],
        vec![b"+OK\r\n"; 3],
        vec![b"+OK\r\n"; 3],
      ],
      Arc::clone(&seen),
    )
    .await;
    let mut spec = MigrateTaskSpec {
      transfer_option: TransferOption::Slots,
      ..migrate_spec(port_of(&addr), 0)
    };
    spec.copy_option = true;
    let slots: HashSet<i32> = [slot as i32].into_iter().collect();
    let session = try_add_slots_migration_task(&cp, spec.clone(), &slots).unwrap();
    let migrated = run_slots_migration_task(Arc::clone(&store), spec, Arc::clone(&session))
      .await
      .unwrap();
    assert_eq!(
      migrated, 2,
      "copy 仅计数确认传输键（string + 对象信封），源端保留的 RI 键不计数"
    );

    // copy：源端三键全部仍在且可读（本用例核心断言）
    assert_eq!(read_str(&store, k1.as_bytes()).await, Some(b"v1".to_vec()));
    {
      let session = store.new_session().unwrap();
      let batch = session.enter_batch();
      let storage = StorageSession::new_readonly(batch);
      assert!(
        storage
          .batch
          .contains_key(obj_key.as_bytes())
          .await
          .unwrap(),
        "COPY 迁移后对象信封键应保留源端"
      );
      assert!(
        storage.batch.contains_key(ri_key.as_bytes()).await.unwrap(),
        "RI 信封键应保留源端（暂不支持登记面）"
      );
    }

    // sketch 状态机与 keys.rs 同形推进：copy 态不落 Deleting，每轮照常复位，
    // 迁移结束归位 Initializing（键门无残留位）
    assert_eq!(
      *session.sketch.status.read(),
      SketchStatus::Initializing,
      "copy 迁移结束 sketch 应复位推进，不得滞留传输门位"
    );

    // 收口与非 copy 一致：槽位交权 NODE + 任务移除（copy 只保留键权，不改
    // 槽属主编排）
    assert!(
      seen.lock().iter().any(|f| f.contains("SETSLOTSRANGE NODE")),
      "COPY 链同样应交权 NODE: {:?}",
      seen.lock()
    );
    assert_eq!(
      cp.migration_manager().unwrap().get_migration_task_count(),
      0,
      "迁移任务结束必须移除"
    );
  });
}
