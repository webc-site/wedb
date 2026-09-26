//! SWAPDB 换库翻转槽位后向量上下文联动与槽迁移发现面回归测试（zcode-r54-slotfn）
//!
//! 核心验证点：
//! 1. VADD 落 db A（槽 S1=slot_of(ns, A)）→ SWAPDB A B → 槽翻转至 S2=slot_of(ns, B)；
//! 2. 槽迁移发现面改现算（单一真源）及在线改章联动：
//!    - get_namespaces_for_hash_slots 经在线改章命中新槽 S2，旧槽 S1 不再命中；
//!    - get_vector_set_keys_for_slots 及 get_vector_set_keys_for_slots_with 命中新槽 S2，旧槽 S1 为空；
//! 3. 在线改章与 AOF 回放重盖章逐值一致断言；
//! 4. 重启/恢复前后元数据发现面逐值一致断言。

use std::{
  collections::BTreeSet,
  fs::create_dir_all,
  path::{Path, PathBuf},
  sync::Arc,
};

use aok::{Error, Void};
use tempfile::TempDir;
use waof::{WalConfig, WalLog};
use wbase::{align::DEFAULT_SECTOR_SIZE, hash_slot::slot_of};
use wconf::RuntimeServerOptions;
use wdev::SegmentedDevice;
use wkv::WedbStore;
use wnode::{
  GarnetAppendOnlyFile, MessageConsumerFace, RespSessionConsumer,
  aof::{
    aof_processor::{AofProcessor, ReplayTarget},
    recover::aof_recover::AofRecover,
    waof_sublog::single_log_aof,
  },
  database::{GarnetDatabase, SingleDatabaseManager},
  resp::{
    garnet_api::StoreGarnetApi,
    resp_server_session::RespServerSessionOptions,
    vector::{
      vector_manager::{VectorManager, VectorManagerOptions},
      vector_manager_index::Index,
      vector_manager_locking::split_registry_key,
      vector_manager_replication::VectorAofSink,
      vector_store_callbacks::{
        ActiveDedicatedVectorSession, OwnedActiveVectorSession, WedbVectorStoreCallbacks,
      },
    },
  },
  service::NodeService,
  storage::session::storage_session::StorageSession,
};
use wtest_base::{resp_frame as frame, test_store_config};
use wvector::Callbacks;

struct Node {
  _dir: TempDir,
  device: Arc<SegmentedDevice>,
  store: Arc<WedbStore<SegmentedDevice>>,
  aof: Arc<GarnetAppendOnlyFile>,
  cp_dir: PathBuf,
  _service: NodeService<SegmentedDevice>,
}

fn open_node(tag: &str) -> aok::Result<Node> {
  let dir = TempDir::new()?;
  let device = Arc::new(SegmentedDevice::single_file(
    dir.path().join(format!("{tag}.db")),
  )?);
  let wal_device = Arc::new(SegmentedDevice::new(
    dir.path().join(format!("{tag}.wal")),
    64 * 1024,
    DEFAULT_SECTOR_SIZE,
  )?);
  let cp_dir = dir.path().join("ckpt");
  create_dir_all(&cp_dir)?;
  let store = Arc::new(WedbStore::open(test_store_config(), Arc::clone(&device))?);
  let wal = Arc::new(WalLog::new(wal_device, WalConfig::new(1 << 20))?);
  let aof = single_log_aof(Arc::clone(&wal), &RuntimeServerOptions::default())
    .expect("装配 single_log_aof");
  let service = NodeService::new(Arc::clone(&store), Arc::clone(&aof))?;
  Ok(Node {
    _dir: dir,
    device,
    store,
    aof,
    cp_dir,
    _service: service,
  })
}

fn vector_manager_of(store: &Arc<WedbStore<SegmentedDevice>>) -> Arc<VectorManager> {
  let vm = Arc::new(VectorManager::new(
    VectorManagerOptions {
      is_enabled: true,
      ..Default::default()
    },
    Callbacks::new(Arc::new(WedbVectorStoreCallbacks::new())),
  ));
  let s = Arc::clone(store);
  vm.attach_dedicated_session_factory(Arc::new(move || {
    s.new_session()
      .ok()
      .map(OwnedActiveVectorSession::new)
      .map(ActiveDedicatedVectorSession::from_bound)
  }));
  vm
}

fn single_database_manager(
  store: &Arc<WedbStore<SegmentedDevice>>,
  device: &Arc<SegmentedDevice>,
  aof: &Arc<GarnetAppendOnlyFile>,
  cp_dir: &Path,
) -> Arc<SingleDatabaseManager<SegmentedDevice>> {
  let db = Arc::new(GarnetDatabase::new(
    0,
    Arc::clone(store),
    Arc::clone(device),
    cp_dir.to_path_buf(),
    Some(Arc::clone(aof)),
  ));
  Arc::new(SingleDatabaseManager::new(cp_dir.to_path_buf(), db))
}

fn consumer_of(
  store: &Arc<WedbStore<SegmentedDevice>>,
  mgr: &Arc<SingleDatabaseManager<SegmentedDevice>>,
  vm: &Arc<VectorManager>,
) -> RespSessionConsumer {
  let api = StoreGarnetApi::new(store.new_session().unwrap())
    .with_database_manager(Arc::clone(mgr))
    .with_vector_manager(Arc::clone(vm));
  RespSessionConsumer::new(1, RespServerSessionOptions::default(), Arc::new(api))
}

async fn pump(consumer: &mut RespSessionConsumer, frame: &[u8]) -> Vec<u8> {
  let mut scratch = consumer.take_recv_scratch();
  scratch.extend_from_slice(frame);
  consumer.return_recv_scratch(scratch);
  let mut resp = Vec::new();
  let remaining = consumer.try_consume_messages_into(&mut resp);
  assert_eq!(remaining, Some(0), "帧应完整消费");
  if let Some(slow) = consumer.take_slow_wait() {
    let reply = slow.resolve().await;
    consumer.resolve_slow_wait_into(&reply, &mut resp);
  }
  resp
}

async fn replay_to(
  rstore: &Arc<WedbStore<SegmentedDevice>>,
  aof: &Arc<GarnetAppendOnlyFile>,
  replayed_vm: &Arc<VectorManager>,
) -> aok::Result<u64> {
  let _pause = rstore.pause_aof_listeners();
  let session = rstore.new_session()?;
  let batch = session.enter_batch();
  let storage = StorageSession::new(batch);
  let target = ReplayTarget {
    session: &storage,
    store: Arc::clone(rstore),
    aof_floor: rstore
      .recovered_aof_floor()
      .iter()
      .map(|&a| a as i64)
      .collect(),
  };
  aof.set_vector_manager(Arc::clone(replayed_vm));
  let processor = AofProcessor::new(Arc::clone(aof));
  let replayed = AofRecover::single_log_recover(&processor, aof, 0, 0, -1, &target)
    .await
    .map_err(|e| Error::msg(e.to_string()))?;
  Ok(replayed)
}

/// 验证：VADD 落 db 0 → SWAPDB 0 1 → 发现面改现算与在线改章联动 → AOF 回放与重启逐值一致
#[compio::test]
async fn swapdb_flips_vector_slot_discovery_and_aof_parity() -> Void {
  let Node {
    _dir,
    device,
    store,
    aof,
    cp_dir,
    _service,
  } = open_node("swapdb_slot_parity")?;

  let mgr = single_database_manager(&store, &device, &aof, &cp_dir);
  let vm = vector_manager_of(&store);
  let _vector_domain = vm
    .bind_dedicated_session()
    .expect("专用向量会话工厂应已注入");
  vm.set_aof_sink(Arc::new(VectorAofSink::new(
    &aof,
    Arc::clone(store.current_version_atomic()),
  )));
  aof.set_vector_manager(Arc::clone(&vm));
  mgr.attach_vector_manager(Arc::clone(&vm));

  let mut consumer = consumer_of(&store, &mgr, &vm);

  let ns = 0u64;
  let db0 = 0u64;
  let db1 = 1u64;
  let slot0 = slot_of(ns, db0);
  let slot1 = slot_of(ns, db1);
  assert_ne!(slot0, slot1, "db 0 与 db 1 槽位须互异");

  // 1. 在 db 0 上写入向量集 vs_swap
  let vec_data = [0u8, 0, 128, 63, 0, 0, 0, 64]; // [1.0, 2.0]
  let resp = pump(
    &mut consumer,
    &frame(&[b"VADD", b"vs_swap", b"FP32", &vec_data, b"e1", b"NOQUANT"]),
  )
  .await;
  assert_eq!(resp, b":1\r\n", "VADD 须成功返回 :1");

  // 换库前：db 0 槽位 slot0 须发现该向量集；db 1 槽位 slot1 发现面为空
  let ns_slot0_before = vm.get_namespaces_for_hash_slots(&BTreeSet::from([i32::from(slot0)]));
  let ns_slot1_before = vm.get_namespaces_for_hash_slots(&BTreeSet::from([i32::from(slot1)]));
  assert!(!ns_slot0_before.is_empty(), "换库前 slot0 须命中在用上下文");
  assert!(ns_slot1_before.is_empty(), "换库前 slot1 须为空");

  let keys_slot0_before = vm.get_vector_set_keys_for_slots(&BTreeSet::from([i32::from(slot0)]));
  assert_eq!(keys_slot0_before.len(), 1);
  let (_domain, user_key) = split_registry_key(&keys_slot0_before[0].0);
  assert_eq!(user_key, b"vs_swap");
  assert_eq!(
    keys_slot0_before[0].1,
    Index::from_bytes(&keys_slot0_before[0].1)
      .unwrap()
      .to_bytes()
  );

  // 2. 执行 SWAPDB 0 1
  let swap_resp = pump(&mut consumer, &frame(&[b"SWAPDB", b"0", b"1"])).await;
  assert_eq!(swap_resp, b"+OK\r\n", "SWAPDB 0 1 须成功返回 +OK");

  // 3. 换库后在线面改章与发现面断言
  let ns_slot0_after = vm.get_namespaces_for_hash_slots(&BTreeSet::from([i32::from(slot0)]));
  let ns_slot1_after = vm.get_namespaces_for_hash_slots(&BTreeSet::from([i32::from(slot1)]));
  assert!(
    ns_slot0_after.is_empty(),
    "SWAPDB 换库后旧槽 slot0 须不再命中任何在用上下文"
  );
  assert!(
    !ns_slot1_after.is_empty(),
    "SWAPDB 换库后新槽 slot1 须命中已翻转上下文"
  );
  assert_eq!(
    ns_slot1_after, ns_slot0_before,
    "翻转后新槽上下文集合须与翻转前旧槽上下文集合逐值一致"
  );

  // 发现面（常规按 slots[] 过滤）
  let keys_slot0_after = vm.get_vector_set_keys_for_slots(&BTreeSet::from([i32::from(slot0)]));
  let keys_slot1_after = vm.get_vector_set_keys_for_slots(&BTreeSet::from([i32::from(slot1)]));
  assert!(keys_slot0_after.is_empty(), "换库后按旧槽发现须为空");
  assert_eq!(keys_slot1_after.len(), 1, "换库后按新槽发现须恰好 1 键");
  let (_, user_key_after) = split_registry_key(&keys_slot1_after[0].0);
  assert_eq!(user_key_after, b"vs_swap");

  // 发现面改现算（方案 1 单一真源经 logic_domain_of 现算槽位）
  let dynamic_found_s1 =
    vm.get_vector_set_keys_for_slots_with(&BTreeSet::from([i32::from(slot1)]), |vns, vdb| {
      store
        .vdb
        .logic_domain_of(vns, vdb)
        .map(|(lns, ldb)| slot_of(lns, ldb))
    });
  let dynamic_found_s0 =
    vm.get_vector_set_keys_for_slots_with(&BTreeSet::from([i32::from(slot0)]), |vns, vdb| {
      store
        .vdb
        .logic_domain_of(vns, vdb)
        .map(|(lns, ldb)| slot_of(lns, ldb))
    });
  assert_eq!(
    dynamic_found_s1.len(),
    1,
    "现算发现面按新槽 slot1 须精准命中"
  );
  assert!(dynamic_found_s0.is_empty(), "现算发现面按旧槽 slot0 须为空");

  // 4. AOF 回放面逐值一致断言（AOF 回放重盖章与在线改章逐值一致）
  let dir_replay = TempDir::new()?;
  let rdevice = Arc::new(SegmentedDevice::single_file(
    dir_replay.path().join("replay.db"),
  )?);
  let rstore = Arc::new(WedbStore::open(test_store_config(), Arc::clone(&rdevice))?);
  let replayed_vm = vector_manager_of(&rstore);
  let _r_domain = replayed_vm
    .bind_dedicated_session()
    .expect("回放端专用向量会话工厂应已注入");

  aof.log().commit();
  let replayed_count = replay_to(&rstore, &aof, &replayed_vm).await?;
  assert!(replayed_count > 0, "AOF 须回放成功");

  let r_slot0 = replayed_vm.get_namespaces_for_hash_slots(&BTreeSet::from([i32::from(slot0)]));
  let r_slot1 = replayed_vm.get_namespaces_for_hash_slots(&BTreeSet::from([i32::from(slot1)]));
  assert!(r_slot0.is_empty(), "回放面旧槽 slot0 须为空");
  assert_eq!(
    r_slot1, ns_slot1_after,
    "在线改章与 AOF 回放重盖章须逐值一致！"
  );

  let r_keys_s1 = replayed_vm.get_vector_set_keys_for_slots(&BTreeSet::from([i32::from(slot1)]));
  assert_eq!(r_keys_s1.len(), 1, "回放面按新槽 slot1 须恰好发现该键");
  let (_, r_user_key) = split_registry_key(&r_keys_s1[0].0);
  assert_eq!(r_user_key, b"vs_swap");
  Ok(())
}
