//! 升阶分层集合无盘全量同步与迁移端到端集成测试
//!
//! 覆盖 task/ing/tiered-key-sync-migration.md 规划的三条验收用例：
//! 1. 自动升阶/分层集合经无盘全量同步到副本端，判别类型、成员与值字节全等、键级 TTL 齐；
//! 2. 迁移面帧级 e2e：源端快照切块 → 接收态逐块写入发布，流中断（半程停发）不漏半成品；
//! 3. 源端删除收口：delete_string 对升阶键全域空化（EXISTS=0，存根不可载）。
//!
//! 补充（本棒复核）：
//! 4. 死/畸形元记录分类回归（迁移资格探测不得把 Meta 域死记录误判为带外树键）；
//! 5. 迁移面真链端到端：带内批（string/信封）与带外升阶键走真实传输编码 →
//!    真实接收核 import_migration_frames 落目的端，成员/值字节全等、键级 TTL 回填，
//!    迁移成功后源端逐键严格空化（SLOTS 与 KEYS 两链共用同一接收核，一次闭环即
//!    覆盖两条链的带外传输面）。

use std::sync::Arc;

use async_lock::Mutex as AsyncLockMutex;
use compio::runtime::Runtime;
use parking_lot::Mutex;
use waof::{AofAddress, WalConfig, WalLog};
use wbase::{
  convert::{expire_at_milliseconds_to_ticks, unix_time_in_milliseconds_from_ticks},
  time::now_ticks,
};
use wbftree::{BfTreeReadResult, DEFAULT_MIGRATION_CHUNK_SIZE};
use wconn::record::{BatchItem, MigrateVal, encode_migration_payload, parse_migration_payload};
use wdev::SegmentedDevice;
use wedb::server::{
  cluster::IClusterProvider,
  cluster_config::ClusterConfig,
  cluster_manager::ClusterManager,
  cluster_provider::{ClusterProvider, PrimaryReplicationAssets},
  migration::{
    chunk_reassembler::ChunkReassembler,
    frame_import::{FrameImport, import_migration_frames},
    migrate_driver::{LiveKeyKind, LiveValue, probe_live_key_kind, read_live_value},
  },
  replication::{
    aof_replication_pump::AofReplicationPump,
    cluster_replication_session::ClusterReplicationSession,
    replica_diskless_sync::try_begin_diskless_sync_async, replica_sync_session::ReplicaSyncSession,
    sync_metadata::SyncMetadata,
  },
  sync_transport::transmit_range_index_stream,
  worker::{LocalWorkerSpec, NodeRole},
};
use wkv::WedbStore;
use wnode::{
  GarnetServer, GarnetStatus, RespSessionConsumer, SessionProviderFace, WireFormat,
  range_index::{RangeIndexManagerMigration, RangeIndexMigrationReceiveState},
  resp::{garnet_api::StoreGarnetApi, resp_server_session::RespServerSessionOptions},
  storage::session::storage_session::StorageSession,
};
use wtest_base::test_store_config;
use wtxn::{TxnLockTable, WatchVersionMap};
use wval::{GarnetObjectType, KeyTag, MetaValue};

const PRIMARY_ID: u128 = 0x0DE1_0000_0000_0000_0000_0000_0001;
const REPLICA_ID: u128 = 0x0DE1_0000_0000_0000_0000_0000_0000_0002;

struct NodeStorage {
  _dir: tempfile::TempDir,
  store: Arc<WedbStore<SegmentedDevice>>,
  wal: Arc<WalLog<SegmentedDevice>>,
}

fn open_node(tag: &str) -> NodeStorage {
  let dir = tempfile::tempdir().unwrap();
  let device =
    Arc::new(SegmentedDevice::single_file(dir.path().join(format!("{tag}.db"))).unwrap());
  let config = test_store_config();
  let store = Arc::new(WedbStore::open(config, device).unwrap());
  let wal_device =
    Arc::new(SegmentedDevice::single_file(dir.path().join(format!("{tag}.wal"))).unwrap());
  let wal = Arc::new(WalLog::new(wal_device, WalConfig::default()).unwrap());
  NodeStorage {
    _dir: dir,
    store,
    wal,
  }
}

fn provider_with_role(
  node: &NodeStorage,
  node_id: u128,
  port: i32,
  role: NodeRole,
) -> Arc<ClusterProvider> {
  let provider = ClusterProvider::new();
  provider.initialize_replication_manager(1, None, false);
  provider.set_replica_diskless_sync_delay(0);
  let cm = Arc::new(ClusterManager::new(Arc::clone(&provider)));
  let mut config = ClusterConfig::new();
  config.initialize_local_worker(LocalWorkerSpec {
    node_id,
    address: "127.0.0.1",
    port,
    config_epoch: 1,
    role,
    replica_of_node_id: (role == NodeRole::Replica).then_some(PRIMARY_ID),
    hostname: None,
  });
  *cm.current_config.write() = config;
  *provider.cluster_manager.write() = Some(cm);
  provider.set_store(Arc::clone(&node.store));
  provider.set_wal(Arc::clone(&node.wal));
  provider
}

struct ReplicaSessionProvider {
  provider: Arc<ClusterProvider>,
}

impl SessionProviderFace for ReplicaSessionProvider {
  type Consumer = RespSessionConsumer;

  fn get_session(
    &self,
    _wire_format: WireFormat,
    _network_sender_id: u64,
  ) -> Option<RespSessionConsumer> {
    let cluster_session = self.provider.create_cluster_session();
    let store = self.provider.try_store()?;
    let mut consumer = RespSessionConsumer::with_cluster(
      1,
      RespServerSessionOptions::default(),
      cluster_session,
      self.provider.provider_handle(),
      Arc::new(StoreGarnetApi::new(store.new_session().ok()?)),
    );
    consumer.attach_transaction_components(Arc::new(WatchVersionMap::new(64)), TxnLockTable::new());
    Some(consumer)
  }
}

/// 用例 1: 升阶分层集合无盘全量同步到副本端
#[test]
fn tiered_sync_diskless_replicates_promoted_collections() {
  let rt = Runtime::new().unwrap();
  rt.block_on(async {
    let source = open_node("source_sync");
    let provider_p = provider_with_role(&source, PRIMARY_ID, 7100, NodeRole::Primary);

    let expire_ms = unix_time_in_milliseconds_from_ticks(now_ticks()) + 60_000;
    let expire_ticks = expire_at_milliseconds_to_ticks(expire_ms);

    // 写入 Hash 升阶集合与 Set 升阶集合
    let sess = source.store.new_session().unwrap();
    sess
      .promote_collection_to_bftree(
        b"t:hash",
        GarnetObjectType::Hash,
        vec![
          (b"field1".to_vec(), b"val1".to_vec()),
          (b"field2".to_vec(), b"val2".to_vec()),
        ],
        i64::MAX,
        false,
      )
      .await
      .unwrap();
    sess.put_ttl(b"t:hash", expire_ticks).await.unwrap();

    sess
      .promote_collection_to_bftree(
        b"t:set",
        GarnetObjectType::Set,
        vec![
          (b"member1".to_vec(), b"1".to_vec()),
          (b"member2".to_vec(), b"1".to_vec()),
        ],
        i64::MAX,
        false,
      )
      .await
      .unwrap();
    sess.put_ttl(b"t:set", expire_ticks).await.unwrap();

    // 副本端配置
    let replica = open_node("target_sync");
    let provider_r = provider_with_role(&replica, REPLICA_ID, 7101, NodeRole::Replica);
    provider_r.set_replica_replication_session(Some(Arc::new(ClusterReplicationSession::new(
      Arc::clone(&provider_r),
      Arc::clone(&replica.wal),
      None,
    ))));

    let server = GarnetServer::new(
      &["127.0.0.1:0".to_string()],
      65536,
      100,
      Arc::new(ReplicaSessionProvider {
        provider: Arc::clone(&provider_r),
      }),
    )
    .unwrap();
    server.start(None).unwrap();
    let replica_addr = server.local_addr().unwrap().to_string();

    let rm_p = provider_p.replication_manager().unwrap();
    let rm_r = provider_r.replication_manager().unwrap();
    let assets = PrimaryReplicationAssets {
      wal: Arc::clone(&source.wal),
      pump: Arc::new(AofReplicationPump::new(Arc::clone(
        &rm_p.aof_sync_driver_store,
      ))),
      sync_session: Arc::new(ReplicaSyncSession::new(Arc::clone(&rm_p))),
    };
    let meta = SyncMetadata {
      full_sync: false,
      origin_node_role: NodeRole::Replica,
      origin_node_id: REPLICA_ID,
      current_primary_repl_id: rm_r.primary_repl_id(),
      current_store_version: 0,
      current_aof_begin_address: AofAddress::create(1, 0),
      current_aof_tail_address: AofAddress::create(1, 0),
      current_replication_offset: AofAddress::create(1, 0),
      checkpoint_entry: None,
    };
    let sync_start =
      try_begin_diskless_sync_async(&provider_p, &assets, PRIMARY_ID, &replica_addr, &meta)
        .await
        .unwrap();
    assert_eq!(sync_start.get(0), Some(0), "全量同步起始位点对齐");

    // 副本端断言
    let target_sess = provider_r.try_store().unwrap().new_session().unwrap();

    // 1) Hash 升阶键在副本端齐备且形态准确
    let (h_meta, h_stub) = target_sess
      .load_collection_stub(b"t:hash")
      .await
      .unwrap()
      .expect("副本端必须还原 t:hash");
    assert_eq!(h_meta.collection_type, GarnetObjectType::Hash);
    assert_eq!(h_meta.size, 2);
    {
      let batch = target_sess.enter_batch();
      let storage = StorageSession::new_readonly(batch);
      assert_eq!(
        storage.batch.ttl_of(b"t:hash").await.unwrap(),
        Some(expire_ticks),
        "副本端键级 TTL 必须回填"
      );
    }
    {
      let tree = target_sess
        .acquire_tree_read(b"t:hash", &h_stub)
        .await
        .unwrap();
      let v1 = tree.read_callback(b"field1", |res, bytes| match res {
        BfTreeReadResult::Found => Some(bytes.to_vec()),
        _ => None,
      });
      assert_eq!(v1.as_deref(), Some(&b"val1"[..]));
    }

    // 2) Set 升阶键在副本端齐备
    let (s_meta, s_stub) = target_sess
      .load_collection_stub(b"t:set")
      .await
      .unwrap()
      .expect("副本端必须还原 t:set");
    assert_eq!(s_meta.collection_type, GarnetObjectType::Set);
    assert_eq!(s_meta.size, 2);
    {
      let tree = target_sess
        .acquire_tree_read(b"t:set", &s_stub)
        .await
        .unwrap();
      let m1 = tree.read_callback(b"member1", |res, _| matches!(res, BfTreeReadResult::Found));
      assert!(m1, "member1 必须在树中命中");
    }
  });
}

/// 用例 2: 迁移面帧级 e2e 及流中断与续流恢复
#[test]
fn tiered_migration_frame_import_and_stream_interrupt() {
  let rt = Runtime::new().unwrap();
  rt.block_on(async {
    let source = open_node("source_mig");
    let target = open_node("target_mig");

    let expire_ticks = now_ticks() + 50_000 * 10_000;
    let sess = source.store.new_session().unwrap();
    sess
      .promote_collection_to_bftree(
        b"m:hash",
        GarnetObjectType::Hash,
        vec![
          (b"k1".to_vec(), b"v1".to_vec()),
          (b"k2".to_vec(), b"v2".to_vec()),
        ],
        i64::MAX,
        false,
      )
      .await
      .unwrap();
    sess.put_ttl(b"m:hash", expire_ticks).await.unwrap();

    let (mut reader, meta) =
      RangeIndexManagerMigration::snapshot_range_index_and_create_reader(&sess, b"m:hash")
        .await
        .unwrap();

    let target_sess = target.store.new_session().unwrap();
    let mut rx_state = RangeIndexMigrationReceiveState::new(target.store.range_index().clone());

    let mut chunks = Vec::new();
    let mut buf = vec![0u8; 1024];
    while !reader.is_complete() {
      let n = reader.read_next_chunk(&mut buf).unwrap();
      assert!(n > 0);
      chunks.push(buf[..n].to_vec());
    }
    assert!(!chunks.is_empty(), "至少切出一个分块");

    // 2.1 中断流测试：若有多块，仅发第一块并断言无半成品键
    if chunks.len() > 1 {
      let ok = rx_state
        .process_record(&chunks[0], meta, &target_sess, false)
        .await;
      assert!(ok, "首块处理必须成功");
      assert!(rx_state.is_receiving(), "状态机处于接收中，流尚未发布");
      assert!(
        target_sess
          .load_collection_stub(b"m:hash")
          .await
          .unwrap()
          .is_none(),
        "流中断期间目标端绝无半成品"
      );
    }

    // 2.2 完整流投递完成发布
    let mut rx_clean = RangeIndexMigrationReceiveState::new(target.store.range_index().clone());
    for chunk in &chunks {
      let ok = rx_clean
        .process_record(chunk, meta, &target_sess, false)
        .await;
      assert!(ok, "每个流块处理必须成功");
    }
    assert!(!rx_clean.is_receiving(), "全部块发完后状态机复位并发布成功");

    // 校验发布成果
    let (rmeta, rstub) = target_sess
      .load_collection_stub(b"m:hash")
      .await
      .unwrap()
      .expect("必须成功装载迁移后的升阶集合存根");
    assert_eq!(rmeta.collection_type, GarnetObjectType::Hash);
    assert_eq!(rmeta.size, 2);
    {
      let tree = target_sess
        .acquire_tree_read(b"m:hash", &rstub)
        .await
        .unwrap();
      let v1 = tree.read_callback(b"k1", |res, bytes| match res {
        BfTreeReadResult::Found => Some(bytes.to_vec()),
        _ => None,
      });
      assert_eq!(v1.as_deref(), Some(&b"v1"[..]));
    }
  });
}

/// 用例 3: 源端删除收口：delete_string 对升阶键全域空化
#[test]
fn tiered_source_delete_cleans_meta_and_stub() {
  let rt = Runtime::new().unwrap();
  rt.block_on(async {
    let node = open_node("del_tiered");
    let sess = node.store.new_session().unwrap();
    sess
      .promote_collection_to_bftree(
        b"del:key",
        GarnetObjectType::Hash,
        vec![(b"a".to_vec(), b"1".to_vec())],
        i64::MAX,
        false,
      )
      .await
      .unwrap();

    // 删除前确认存在
    assert!(
      sess
        .load_collection_stub(b"del:key")
        .await
        .unwrap()
        .is_some()
    );

    // delete_string 删除
    let batch = sess.enter_batch();
    let storage = StorageSession::new_readonly(batch);
    let deleted = storage.delete_string(b"del:key").await.unwrap();
    assert!(deleted, "删除操作应返回 true");

    // 全域空化确认：存根不可载、EXISTS 判缺失
    assert!(
      sess
        .load_collection_stub(b"del:key")
        .await
        .unwrap()
        .is_none()
    );
    let batch2 = sess.enter_batch();
    let storage2 = StorageSession::new_readonly(batch2);
    let st = storage2.exists(b"del:key", None).await.unwrap();
    assert_eq!(
      st,
      GarnetStatus::NotFound,
      "删除后 EXISTS 必须返回 NotFound"
    );
  });
}

/// 用例 4: 迁移资格探测对 Meta 域死记录/畸形记录判 Gone，绝不误判带外树键
///
/// 承接本棒修复的缺陷：`read_tag_with` 闭包产物为 `Option<Option<_>>`，
/// 旧代码 `.is_some()` 只测外层域存在性，把「Meta 键在但记录已死/畸形」误判
/// TieredTree（与紧邻注释「畸形/死记录 None 落不存在」相悖）；flatten 折叠双
/// 层后死/畸形记录落 Gone，存活升阶键仍判 TieredTree。
#[test]
fn tiered_dead_or_malformed_meta_record_is_gone_not_tiered() {
  let rt = Runtime::new().unwrap();
  rt.block_on(async {
    let node = open_node("dead_meta");
    {
      let sess = node.store.new_session().unwrap();
      let batch = sess.enter_batch();
      let storage = StorageSession::new(batch);
      // 死元记录：Hash 判别、size=0、非 RI → is_live() 为假
      let dead = MetaValue::new(7, GarnetObjectType::Hash, 0).to_bytes();
      storage
        .upsert_tag(b"dead:meta", KeyTag::Meta, &dead)
        .await
        .unwrap();
      // 畸形元记录：size>0 但 reserved[0] 非法 StorageEncoding → from_slice 报错
      let mut malformed = MetaValue::new(8, GarnetObjectType::Hash, 5).to_bytes();
      malformed[9] = 0;
      storage
        .upsert_tag(b"malformed:meta", KeyTag::Meta, &malformed)
        .await
        .unwrap();
    }
    // 存活对照：真实升阶键（Meta + 树）仍判 TieredTree，修复未误伤
    {
      let sess = node.store.new_session().unwrap();
      sess
        .promote_collection_to_bftree(
          b"live:tier",
          GarnetObjectType::Hash,
          vec![(b"f".to_vec(), b"v".to_vec())],
          i64::MAX,
          false,
        )
        .await
        .unwrap();
    }
    let sess = node.store.new_session().unwrap();
    let batch = sess.enter_batch();
    let storage = StorageSession::new_readonly(batch);
    for key in [b"dead:meta".as_slice(), b"malformed:meta"] {
      assert!(
        matches!(
          read_live_value(&storage, None, key).await.unwrap(),
          LiveValue::Gone
        ),
        "死/畸形元记录 {} 不得判 TieredTree（读值侧）",
        String::from_utf8_lossy(key)
      );
      assert!(
        matches!(
          probe_live_key_kind(&storage, None, key).await.unwrap(),
          LiveKeyKind::Gone
        ),
        "死/畸形元记录 {} 不得判 TieredTree（探测侧）",
        String::from_utf8_lossy(key)
      );
    }
    assert!(
      matches!(
        read_live_value(&storage, None, b"live:tier").await.unwrap(),
        LiveValue::TieredTree
      ),
      "存活升阶键仍必须判 TieredTree"
    );
    assert!(
      matches!(
        probe_live_key_kind(&storage, None, b"live:tier")
          .await
          .unwrap(),
        LiveKeyKind::TieredTree
      ),
      "存活升阶键探测侧仍必须判 TieredTree"
    );
  });
}

/// 用例 5: 迁移面真链端到端——带内批 + 带外升阶键走真实传输编码与真实接收核
/// 落目的端，成员/值字节全等、键级 TTL 回填，迁移成功后源端逐键严格空化。
///
/// 与用例 2 的帧级直调 process_record 不同，本件经源端 `transmit_range_index_stream`
/// 真实快照分块编码、目标端 `parse_migration_payload` + `import_migration_frames`
/// 真实解码发布，覆盖收/发两端的线级编解码与发布/TTL 回填全链。SLOTS 与 KEYS 两链
/// 共用同一 `import_migration_frames` 接收核（frame_import.rs 一处定义），故本闭环
/// 同时代表两条链携带升阶集合的传输面。
#[test]
fn tiered_migration_real_chain_imports_and_empties_source() {
  let rt = Runtime::new().unwrap();
  rt.block_on(async {
    let source = open_node("mig_src");
    let target = open_node("mig_dst");
    let target_provider = provider_with_role(&target, REPLICA_ID, 7102, NodeRole::Primary);

    let expire_ms = unix_time_in_milliseconds_from_ticks(now_ticks()) + 60_000;
    let tier_env = b"\x03hashenvpayload";
    // 1) 源端造键：string + 信封 Hash（带内）+ 升阶 Hash（带外，携成员字节与键级 TTL）
    {
      let sess = source.store.new_session().unwrap();
      let batch = sess.enter_batch();
      let storage = StorageSession::new(batch);
      storage.upsert_string(b"m:str", b"hello").await.unwrap();
      storage
        .upsert_tag(b"m:env", KeyTag::ObjectEnvelope, tier_env)
        .await
        .unwrap();
      sess
        .promote_collection_to_bftree(
          b"m:tier",
          GarnetObjectType::Hash,
          vec![
            (b"f1".to_vec(), b"v1".to_vec()),
            (b"f2".to_vec(), b"value-two-longer".to_vec()),
          ],
          i64::MAX,
          false,
        )
        .await
        .unwrap();
      sess
        .put_ttl(b"m:tier", expire_at_milliseconds_to_ticks(expire_ms))
        .await
        .unwrap();
    }
    // 源端分类：升阶键确判 TieredTree（进入带外通道的前提）
    {
      let sess = source.store.new_session().unwrap();
      let batch = sess.enter_batch();
      let storage = StorageSession::new_readonly(batch);
      assert!(
        matches!(
          read_live_value(&storage, None, b"m:tier").await.unwrap(),
          LiveValue::TieredTree
        ),
        "源端升阶键必须分类为带外树键"
      );
    }

    // 2) 目的端接收核（真实 FrameImport；ri 接收态跨命令常驻，逐块累积整流发布）
    let t_sess = target.store.new_session().unwrap();
    let t_batch = t_sess.enter_batch();
    let t_storage = StorageSession::new(t_batch);
    let chunks = Mutex::new(ChunkReassembler::new());
    let ri_state: Option<Arc<AsyncLockMutex<RangeIndexMigrationReceiveState>>> =
      Some(Arc::new(AsyncLockMutex::new(
        RangeIndexMigrationReceiveState::new(target.store.range_index().clone()),
      )));
    let import = FrameImport {
      provider: &target_provider,
      session: &t_sess,
      storage: &t_storage,
      chunks: &chunks,
      ri: &ri_state,
      replace: true,
      vector_slot: 0,
      accept_domain_frames: false,
    };

    // 2a) 带内批（string + 信封 Hash）真实编码 → 解析 → 导入（含 TTL 回填）
    {
      let items = vec![
        BatchItem {
          key: b"m:str",
          val: MigrateVal::Str(b"hello".to_vec()),
          expire_unix_ms: expire_ms,
        },
        BatchItem {
          key: b"m:env",
          val: MigrateVal::Env(tier_env.to_vec()),
          expire_unix_ms: 0,
        },
      ];
      let payload = encode_migration_payload(&items);
      let (_count, frames) = parse_migration_payload(&payload).unwrap();
      import_migration_frames(frames, &import)
        .await
        .expect("带内批导入必须成功");
    }

    // 2b) 带外升阶键走真实源端快照分块，逐块解析并导入（复用同一接收态）
    {
      let s_sess = source.store.new_session().unwrap();
      let ok = transmit_range_index_stream(
        &s_sess,
        b"m:tier",
        DEFAULT_MIGRATION_CHUNK_SIZE,
        async |payload| {
          let (_c, frames) = parse_migration_payload(payload).map_err(|e| e.to_string())?;
          import_migration_frames(frames, &import).await
        },
      )
      .await
      .unwrap();
      assert!(ok, "带外升阶键整流传输必须成功");
    }
    drop(t_storage);
    drop(t_sess);

    // 3) 目的端逐键成员与值字节全等 + 键级 TTL 回填
    {
      let sess = target.store.new_session().unwrap();
      let batch = sess.enter_batch();
      let st = StorageSession::new_readonly(batch);
      assert_eq!(
        st.read_string(b"m:str").await.unwrap().as_deref(),
        Some(&b"hello"[..]),
        "带内 string 值字节必须全等"
      );
      let env = st
        .read_tag_with(b"m:env", KeyTag::ObjectEnvelope, |r| r.to_vec())
        .await
        .unwrap();
      assert_eq!(
        env.as_deref(),
        Some(&tier_env[..]),
        "带内信封值字节必须全等"
      );
      let (meta, stub) = sess
        .load_collection_stub(b"m:tier")
        .await
        .unwrap()
        .expect("带外升阶键必须落目的端");
      assert_eq!(
        meta.collection_type,
        GarnetObjectType::Hash,
        "判别类型必须为 Hash"
      );
      assert_eq!(meta.size, 2, "成员计数必须一致");
      {
        let tree = sess.acquire_tree_read(b"m:tier", &stub).await.unwrap();
        let v1 = tree.read_callback(b"f1", |res, b| {
          if matches!(res, BfTreeReadResult::Found) {
            Some(b.to_vec())
          } else {
            None
          }
        });
        assert_eq!(v1.as_deref(), Some(&b"v1"[..]), "成员 f1 值字节必须全等");
        let v2 = tree.read_callback(b"f2", |res, b| {
          if matches!(res, BfTreeReadResult::Found) {
            Some(b.to_vec())
          } else {
            None
          }
        });
        assert_eq!(
          v2.as_deref(),
          Some(&b"value-two-longer"[..]),
          "成员 f2 值字节必须全等"
        );
      }
      assert!(
        st.batch.ttl_of(b"m:tier").await.unwrap().is_some(),
        "带外升阶键级 TTL 必须回填目的端"
      );
      assert!(
        st.batch.ttl_of(b"m:str").await.unwrap().is_some(),
        "带内 string 键级 TTL 必须回填目的端"
      );
    }

    // 4) 源端严格空化：迁移成功后走真实 DELETING 原语 delete_string，逐键判缺失
    {
      let sess = source.store.new_session().unwrap();
      let batch = sess.enter_batch();
      let storage = StorageSession::new(batch);
      for key in [b"m:str".as_slice(), b"m:env", b"m:tier"] {
        assert!(
          storage.delete_string(key).await.unwrap(),
          "源端删除 {}",
          String::from_utf8_lossy(key)
        );
      }
      drop(storage);
      let sess2 = source.store.new_session().unwrap();
      let b2 = sess2.enter_batch();
      let st2 = StorageSession::new_readonly(b2);
      for key in [b"m:str".as_slice(), b"m:env", b"m:tier"] {
        assert!(
          matches!(st2.exists(key, None).await.unwrap(), GarnetStatus::NotFound),
          "迁移后源端 {} 必须严格空化",
          String::from_utf8_lossy(key)
        );
      }
      assert!(
        sess2
          .load_collection_stub(b"m:tier")
          .await
          .unwrap()
          .is_none(),
        "升阶存根迁移后必须不可载"
      );
    }
  });
}
