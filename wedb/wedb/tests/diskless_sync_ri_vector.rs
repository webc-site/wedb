//! 无盘全量同步 RangeIndex / wbftree 升阶分层集合 / 向量集记录面集成测试
//!
//! 对标 C# 快照迭代全记录类型分流（RangeIndexRecordType=2 / VectorSet
//! RecordType=1 整记录直拷 + 副本端接收会话承接）：
//! - 发送端分流：libs/cluster/Server/Replication/PrimaryOps/DisklessReplication/ReplicationSnapshotIterator.cs:WriteRecord
//! - 向量集传输面分发：libs/cluster/Server/Migration/MigrateOperation.cs:EncounteredVectorSet
//! - 副本端承接：libs/cluster/Session/RangeIndexMigrationReceiveSession.cs、
//!   libs/cluster/Session/RespClusterReplicationCommands.cs:NetworkClusterSync
//!
//! 链路：主端 try_begin_diskless_sync_async（FullResync 协商 → 快照流：字符串
//! 装普通帧 + RI 树与升阶分层集合走 RangeIndexStream 带外分块帧（rust 分层
//! 扩展：判别类型与 TTL 随流元携载，副本端接收态按真实形态原子发布）+
//! 向量集经 CLUSTER RESERVE 预留后装索引/元素帧）→ 真 socket GarnetServer
//! 副本集群会话导入 → 副本端 RI.EXISTS/RI.GET 数据在、升阶 Hash 键存根
//! 装载真实判别 + 成员字节逐字全等、字符串值与 TTL 保留、RI 与升阶键的键级
//! TTL 保留（本票闭合 kind=4 帧流元携载后新契约）、向量集索引与元素齐。

use std::sync::Arc;

use compio::runtime::Runtime;
use waof::{AofAddress, WalConfig, WalLog};
use wbase::{
  convert::{expire_at_milliseconds_to_ticks, unix_time_in_milliseconds_from_ticks},
  hash_slot::slot_of,
  time::now_ticks,
};
use wbftree::{BfTreeReadResult, StorageBackendType, TreeTuning};
use wdev::SegmentedDevice;
use wedb::server::{
  cluster::IClusterProvider,
  cluster_config::ClusterConfig,
  cluster_manager::ClusterManager,
  cluster_provider::{ClusterProvider, PrimaryReplicationAssets},
  replication::{
    aof_replication_pump::AofReplicationPump,
    cluster_replication_session::ClusterReplicationSession,
    replica_diskless_sync::try_begin_diskless_sync_async, replica_sync_session::ReplicaSyncSession,
    sync_metadata::SyncMetadata,
  },
  worker::{LocalWorkerSpec, NodeRole},
};
use wkv::WedbStore;
use wnode::{
  GarnetServer, RespSessionConsumer, SessionProviderFace, WireFormat,
  resp::{
    garnet_api::StoreGarnetApi,
    resp_server_session::RespServerSessionOptions,
    vector::{
      resp_server_session_vectors::{RespServerSessionVectors, VectorReply},
      vector_manager::{VectorManager, VectorManagerOptions},
      vector_store_callbacks::WedbVectorStoreCallbacks,
    },
  },
  storage::session::storage_session::StorageSession,
};
use wtest_base::test_store_config;
use wtxn::{TxnLockTable, WatchVersionMap};
use wval::{GarnetObjectType, SessionPrefixBuf};
use wvector::Callbacks;

/// 测试节点身份（内部 u128；协议面渲染 32 字符小写 hex）
const PRIMARY_ID: u128 = 0x0DE1_0000_0000_0000_0000_0000_0000_0001;
const REPLICA_ID: u128 = 0x0DE1_0000_0000_0000_0000_0000_0000_0002;

/// 独立存储节点（db 文件 + wal）
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

/// 绑 wkv 存储会话的向量管理器（生产装配同形态）
fn vector_manager(store: &Arc<WedbStore<SegmentedDevice>>) -> Arc<VectorManager> {
  let session = Arc::new(store.new_session().unwrap());
  Arc::new(VectorManager::new(
    VectorManagerOptions {
      is_enabled: true,
      ..Default::default()
    },
    Callbacks::new(Arc::new(WedbVectorStoreCallbacks::new(session))),
  ))
}

/// 带角色 provider（对齐宿主装配：rm/store/wal 全接线，集群配置自持；
/// 副本角色固定挂 primary_1——APPENDLOG init 的主 ID 校验依赖该字段）
fn provider_with_role(
  node: &NodeStorage,
  node_id: u128,
  port: i32,
  role: NodeRole,
) -> Arc<ClusterProvider> {
  let provider = ClusterProvider::new();
  provider.initialize_replication_manager(1, None, false);
  // 关闭 leader 攒批窗口（C# ReplicaDisklessSyncDelay 默认 5 秒；测试直驱
  // 会话入口，无需等同批副本）
  provider.set_replica_diskless_sync_delay(0);
  // 关闭 leader 攒批窗口（C# ReplicaDisklessSyncDelay 默认 5 秒；测试直驱
  // 会话入口，无需等同批副本）
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

/// 副本宿主会话装配面（每连接新集群会话——对齐宿主 get_session）
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

/// 全量同步快照流三记录面齐备：RI 树经 RangeIndexStream 帧在副本重建、
/// 字符串值与 TTL 保留、向量集经 CLUSTER RESERVE 预留 + 索引/元素帧导入
/// 默认会话库槽（测试键不参与定槽）
const SLOT0: u16 = slot_of(0, 0);

#[test]
fn diskless_sync_streams_range_index_and_vector_sets() {
  let rt = Runtime::new().unwrap();
  rt.block_on(async {
    // ===== 主端：RI 树（Disk 后端可快照）+ 带 TTL 字符串 + 向量集（两元素）
    let source = open_node("diskless_ri_vec_source");
    let provider_p = provider_with_role(&source, PRIMARY_ID, 7000, NodeRole::Primary);

    let field_a = [b'f'; 32];
    let field_b = [b'g'; 32];
    let value_a = [b'a'; 40];
    let value_b = [b'b'; 40];
    let expire_ms = unix_time_in_milliseconds_from_ticks(now_ticks()) + 60_000;
    let expire_ticks = expire_at_milliseconds_to_ticks(expire_ms);
    {
      let session = source.store.new_session().unwrap();
      session
        .range_index_create(b"ri:key", StorageBackendType::Disk, TreeTuning::default())
        .await
        .unwrap();
      session
        .range_index_set(b"ri:key", &field_a, &value_a)
        .await
        .unwrap();
      session
        .range_index_set(b"ri:key", &field_b, &value_b)
        .await
        .unwrap();
      let batch = session.enter_batch();
      let storage = StorageSession::new_readonly(batch);
      storage.upsert_string(b"plain:str", b"v1").await.unwrap();
      storage
        .expire_at_ticks(b"plain:str", expire_ticks)
        .await
        .unwrap();
      // 升阶分层集合键（Hash 入驻 Meta 域元记录，携原集合类型）与 RI 键级
      // TTL：经 kind=4 带外分块流随流元携载判别类型与 expire_unix_ms，
      // 副本端接收态按真实形态原子发布并回填键级 TTL（本票新契约）
      let tier_sess = source.store.new_session().unwrap();
      tier_sess
        .promote_collection_to_bftree(
          b"tiered:hash",
          GarnetObjectType::Hash,
          vec![
            (b"f1".to_vec(), b"v1".to_vec()),
            (b"f2".to_vec(), b"v2".to_vec()),
          ],
          i64::MAX,
          false,
        )
        .await
        .unwrap();
      tier_sess
        .put_ttl(b"tiered:hash", expire_ticks)
        .await
        .unwrap();
      // RI 键级 TTL：随同一 kind=4 帧流元携载，副本端发布后回填
      {
        let ri_sess = source.store.new_session().unwrap();
        ri_sess.put_ttl(b"ri:key", expire_ticks).await.unwrap();
      }
    }
    let source_vm = vector_manager(&source.store);
    let vsess = RespServerSessionVectors::new(Arc::clone(&source_vm));
    for element in ["elem1", "elem2"] {
      let reply = vsess.network_vadd(
        SessionPrefixBuf::ROOT.as_slice(),
        &[
          b"vs:set",
          b"VALUES",
          b"4",
          b"1.5",
          b"-2.5",
          b"0.25",
          b"4.0",
          element.as_bytes(),
        ],
        SLOT0,
      );
      assert!(!matches!(reply, VectorReply::Error(_)), "源端 VADD 失败");
    }
    provider_p.set_vector_manager(Arc::clone(&source_vm));

    // ===== 副本：空库全接线（store + wal + 复制会话 + 向量管理器）+ 宿主服务器
    let replica = open_node("diskless_ri_vec_replica");
    let provider_r = provider_with_role(&replica, REPLICA_ID, 7001, NodeRole::Replica);
    let replica_vm = vector_manager(&replica.store);
    provider_r.set_vector_manager(Arc::clone(&replica_vm));
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

    // ===== 主端发起无盘全量同步（副本上报自有主复制 id —— 未 attach 过的新
    //    副本，对标 replica_diskless_sync.rs 的 current_primary_repl_id 取本端
    //    ReplicationManager::primary_repl_id —— NeedToFullSync 第 1 条件
    //    「主从历史不一致」成立 → FullResync 扇出快照）
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
    assert_eq!(sync_start.get(0), Some(0), "空日志主端授予位点为日志起点");

    // ===== 副本断言：RI 树 + RI 键级 TTL、升阶 Hash 键（真实判别 + 成员字节
    //    逐字一致 + 键级 TTL）、字符串值与 TTL、向量集索引与元素齐备
    let session = provider_r.try_store().unwrap().new_session().unwrap();
    assert!(
      session.range_index_exists(b"ri:key").await.unwrap(),
      "RI 树必须经快照流在副本重建"
    );
    assert_eq!(
      session.range_index_get(b"ri:key", &field_a).await.unwrap(),
      Some(value_a.to_vec())
    );
    assert_eq!(
      session.range_index_get(b"ri:key", &field_b).await.unwrap(),
      Some(value_b.to_vec())
    );
    {
      let batch = session.enter_batch();
      let storage = StorageSession::new_readonly(batch);
      assert_eq!(
        storage.read_string(b"plain:str").await.unwrap(),
        Some(b"v1".to_vec())
      );
      assert_eq!(
        storage.batch.ttl_of(b"plain:str").await.unwrap(),
        Some(expire_ticks),
        "带 TTL 字符串键经全量同步后 TTL 保留"
      );
      // RI 键级 TTL 新契约：随 kind=4 帧流元携载，副本端发布后回填
      assert_eq!(
        storage.batch.ttl_of(b"ri:key").await.unwrap(),
        Some(expire_ticks),
        "RI 键级 TTL 必须经带外流元携载在副本回填（本票新契约）"
      );
    }
    // 升阶 Hash 键经带外流落副本：判别类型 + size + 键级 TTL 齐
    let (rmeta, rstub) = session
      .load_collection_stub(b"tiered:hash")
      .await
      .unwrap()
      .expect("升阶键必须经带外分块流在副本重建（新契约：不再有留痕跳键路径）");
    assert_eq!(rmeta.collection_type, GarnetObjectType::Hash);
    assert_eq!(rmeta.size, 2);
    {
      let batch = session.enter_batch();
      let storage = StorageSession::new_readonly(batch);
      assert_eq!(
        storage.batch.ttl_of(b"tiered:hash").await.unwrap(),
        Some(expire_ticks),
        "升阶 Hash 键级 TTL 必须经带外流元携载在副本回填"
      );
    }
    // 成员字节逐字全等：经 wbftree 树读守卫取字段（升阶键走 RI 点操作会被
    // WrongType 拒绝，此处以 load_collection_stub + acquire_tree_read 直读）
    {
      let tree = session
        .acquire_tree_read(b"tiered:hash", &rstub)
        .await
        .unwrap();
      let v1 = tree.read_callback(b"f1", |res, bytes| match res {
        BfTreeReadResult::Found => Some(bytes.to_vec()),
        _ => None,
      });
      let v2 = tree.read_callback(b"f2", |res, bytes| match res {
        BfTreeReadResult::Found => Some(bytes.to_vec()),
        _ => None,
      });
      assert_eq!(v1.as_deref(), Some(&b"v1"[..]), "f1 成员字节必须与源端一致");
      assert_eq!(v2.as_deref(), Some(&b"v2"[..]), "f2 成员字节必须与源端一致");
    }
    assert!(
      replica_vm
        .read_migrated_index(SessionPrefixBuf::ROOT.as_slice(), b"vs:set")
        .is_some(),
      "向量集索引记录必须经快照流在副本登记"
    );
    let rsess = RespServerSessionVectors::new(replica_vm);
    assert!(
      matches!(
        rsess.network_vcard(SessionPrefixBuf::ROOT.as_slice(), &[b"vs:set"]),
        VectorReply::Integer(2)
      ),
      "向量集两元素必须经元素帧在副本导入"
    );

    server.dispose();
  });
}
