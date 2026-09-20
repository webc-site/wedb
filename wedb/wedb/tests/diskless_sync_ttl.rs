//! 无盘全量同步快照流 TTL 保留集成测试：发送端 read_live_value 提取真实
//! 过期时间戳装帧，接收端 cluster_sync_slow 按迁移链同款回填——带 TTL 键
//! 经 CLUSTER SYNC 全量导入副本后 TTL 保留，无 TTL 键不携带过期
//!
//! 对标 C# 快照迭代整记录搬运含过期字段：
//! - libs/cluster/Server/Replication/PrimaryOps/DisklessReplication/ReplicationSnapshotIterator.cs:WriteRecord
//! - libs/cluster/Session/RespClusterReplicationCommands.cs:NetworkClusterSync

use std::sync::Arc;

use compio::runtime::Runtime;
use wbase::{
  convert::{expire_at_milliseconds_to_ticks, unix_time_in_milliseconds_from_ticks},
  hash_slot::CLUSTER_SLOT_COUNT,
  time::now_ticks,
};
use wconn::record::{BatchItem, MigrateVal, encode_migration_payload};
use wdev::SegmentedDevice;
use wedb::server::{
  cluster::IClusterProvider,
  cluster_manager::ClusterManager,
  cluster_provider::ClusterProvider,
  cluster_session::ClusterSession,
  hash_slot::{HashSlot, SlotState},
  migration::migrate_driver::{LiveValue, read_live_value},
  worker::{LOCAL_WORKER_ID, LocalWorkerSpec, NodeRole, Worker},
};
use wkv::WedbStore;
use wnode::{
  MessageConsumerFace, RespSessionConsumer,
  resp::{garnet_api::StoreGarnetApi, resp_server_session::RespServerSessionOptions},
  storage::session::storage_session::StorageSession,
};
use wtest_base::test_store_config;
use wtxn::{TxnLockTable, WatchVersionMap};
use wval::KeyTag;

/// 测试节点身份（内部 u128；协议面渲染 32 字符小写 hex）
const NODE1_HEX: &str = "0000000000000000000000000000de11";

/// 装配双主节点拓扑：node_1（本地）持 0..8192，node_2@7001 持 8192..16384
fn two_primary_provider() -> Arc<ClusterProvider> {
  let cp = ClusterProvider::new();
  let m = cp.cluster_manager().unwrap_or_else(|| {
    let m = Arc::new(ClusterManager::new(Arc::clone(&cp)));
    *cp.cluster_manager.write() = Some(Arc::clone(&m));
    m
  });
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
    for slot in 0..8192 {
      config.slot_map[slot] = HashSlot {
        worker_id: LOCAL_WORKER_ID as u16,
        state: SlotState::Stable,
      };
    }
    for slot in 8192..CLUSTER_SLOT_COUNT {
      config.slot_map[slot] = HashSlot {
        worker_id: remote_worker_id,
        state: SlotState::Stable,
      };
    }
  }
  cp
}

/// 打开小预算测试存储（GC 关闭保持历史语义，与集群会话测试同款配置）
fn open_store(tag: &str) -> Arc<WedbStore<SegmentedDevice>> {
  let dir = tempfile::tempdir().unwrap().keep();
  let device = Arc::new(SegmentedDevice::single_file(dir.join(tag)).unwrap());
  let config = test_store_config();
  Arc::new(WedbStore::open(config, device).unwrap())
}

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

/// CLUSTER SYNC 二进制安全帧（payload 含内嵌 NUL 与任意字节）
fn cluster_sync_frame(source_node_id: &str, payload: &[u8]) -> Vec<u8> {
  let mut frame = format!(
    "*4\r\n$7\r\nCLUSTER\r\n$4\r\nSYNC\r\n${}\r\n{source_node_id}\r\n${}\r\n",
    source_node_id.len(),
    payload.len()
  )
  .into_bytes();
  frame.extend_from_slice(payload);
  frame.extend_from_slice(b"\r\n");
  frame
}

/// 带 TTL 键（string 与 Hash 对象信封）经快照装帧 + CLUSTER SYNC 导入副本后
/// TTL 保留且值一致；无 TTL 键不携带过期；发送端提取的过期时间戳为源键真值
#[test]
fn diskless_sync_preserves_ttl() {
  let rt = Runtime::new().unwrap();
  rt.block_on(async {
    // ===== 副本端：provider + store + 集群会话消费者
    let cp = two_primary_provider();
    let replica = open_store("diskless_replica.db");
    cp.set_store(Arc::clone(&replica));
    let cluster_session: Arc<ClusterSession> = cp.create_cluster_session();
    let mut consumer = RespSessionConsumer::with_cluster(
      1,
      RespServerSessionOptions::default(),
      cluster_session,
      cp.provider_handle(),
      Arc::new(StoreGarnetApi::new(replica.new_session().unwrap())),
    );
    consumer.attach_transaction_components(Arc::new(WatchVersionMap::new(64)), TxnLockTable::new());

    // ===== 源端：带 TTL string + 带 TTL Hash 信封 + 无 TTL string
    let source = open_store("diskless_source.db");
    let expire_ms = unix_time_in_milliseconds_from_ticks(now_ticks()) + 60_000;
    let expire_ticks = expire_at_milliseconds_to_ticks(expire_ms);
    {
      let session = source.new_session().unwrap();
      let batch = session.enter_batch();
      let storage = StorageSession::new_readonly(batch);
      storage.upsert_string(b"diskless:str", b"v1").await.unwrap();
      storage
        .expire_at_ticks(b"diskless:str", expire_ticks)
        .await
        .unwrap();
      storage
        .upsert_tag(b"diskless:hash", KeyTag::ObjectEnvelope, b"\x03payload")
        .await
        .unwrap();
      storage
        .expire_at_ticks(b"diskless:hash", expire_ticks)
        .await
        .unwrap();
      storage
        .upsert_string(b"diskless:plain", b"p1")
        .await
        .unwrap();
    }

    // ===== 发送端形态：read_live_value 提取活值与真实过期时间戳装帧
    //（与 diskless_replication::replication_snapshot_iterator 快照扫描循环同构）
    let (str_val, str_expire) = {
      let session = source.new_session().unwrap();
      let batch = session.enter_batch();
      let storage = StorageSession::new_readonly(batch);
      let (val, expire) = match read_live_value(&storage, None, b"diskless:str")
        .await
        .unwrap()
      {
        LiveValue::Migratable(val, expire) => (val, expire),
        _ => panic!("string 键应为可迁移活值"),
      };
      assert!(matches!(val, MigrateVal::Str(_)), "string 域读出 string 值");
      assert_eq!(expire, expire_ms, "发送端提取源键真实过期时间戳");
      (val, expire)
    };
    let (hash_val, hash_expire) = {
      let session = source.new_session().unwrap();
      let batch = session.enter_batch();
      let storage = StorageSession::new_readonly(batch);
      let (val, expire) = match read_live_value(&storage, None, b"diskless:hash")
        .await
        .unwrap()
      {
        LiveValue::Migratable(val, expire) => (val, expire),
        _ => panic!("合规信封键应为可迁移活值"),
      };
      assert!(matches!(val, MigrateVal::Env(_)), "信封域读出整值");
      assert_eq!(expire, expire_ms, "信封键同样携带真实过期时间戳");
      (val, expire)
    };
    let plain_expire = {
      let session = source.new_session().unwrap();
      let batch = session.enter_batch();
      let storage = StorageSession::new_readonly(batch);
      let (_, expire) = match read_live_value(&storage, None, b"diskless:plain")
        .await
        .unwrap()
      {
        LiveValue::Migratable(val, expire) => (val, expire),
        _ => panic!("无 TTL string 键应为可迁移活值"),
      };
      assert_eq!(expire, 0, "无 TTL 键过期时间戳为 0");
      expire
    };
    // 不存在键分类为 Gone（快照循环跳过，不装帧）
    {
      let session = source.new_session().unwrap();
      let batch = session.enter_batch();
      let storage = StorageSession::new_readonly(batch);
      assert!(matches!(
        read_live_value(&storage, None, b"diskless:missing")
          .await
          .unwrap(),
        LiveValue::Gone
      ));
    }

    let items = [
      BatchItem {
        key: b"diskless:str",
        val: str_val,
        expire_unix_ms: str_expire,
      },
      BatchItem {
        key: b"diskless:hash",
        val: hash_val,
        expire_unix_ms: hash_expire,
      },
      BatchItem {
        key: b"diskless:plain",
        val: MigrateVal::Str(b"p1".to_vec()),
        expire_unix_ms: plain_expire,
      },
    ];
    let payload = encode_migration_payload(&items);

    // ===== CLUSTER SYNC 导入副本（内联零拷贝驱动：借用载荷直挂接收缓冲，
    // blocking_wait 收割，应答直写不挂起慢路径）
    let (consumed, out) = pump(&mut consumer, &cluster_sync_frame(NODE1_HEX, &payload));
    assert_eq!(consumed, Some(0), "帧应被完整消费");
    assert!(consumer.take_slow_wait().is_none(), "内联驱动不挂起慢路径");
    assert_eq!(out, b"+OK\r\n");

    // ===== 副本断言：TTL 保留 + 值一致 + 无 TTL 键不携带过期
    let session = replica.new_session().unwrap();
    let batch = session.enter_batch();
    let storage = StorageSession::new_readonly(batch);
    assert_eq!(
      storage.read_string(b"diskless:str").await.unwrap(),
      Some(b"v1".to_vec())
    );
    assert_eq!(
      storage.batch.ttl_of(b"diskless:str").await.unwrap(),
      Some(expire_ticks),
      "带 TTL string 键经全量同步后 TTL 保留"
    );
    let env = storage
      .read_tag_with(b"diskless:hash", KeyTag::ObjectEnvelope, |raw| raw.to_vec())
      .await
      .unwrap()
      .unwrap();
    assert_eq!(env, b"\x03payload", "信封整值一致");
    assert_eq!(
      storage.batch.ttl_of(b"diskless:hash").await.unwrap(),
      Some(expire_ticks),
      "带 TTL 信封键经全量同步后 TTL 保留"
    );
    assert_eq!(
      storage.read_string(b"diskless:plain").await.unwrap(),
      Some(b"p1".to_vec())
    );
    assert_eq!(
      storage.batch.ttl_of(b"diskless:plain").await.unwrap(),
      None,
      "无 TTL 键不携带过期"
    );
  });
}

/// SYNC 承接面信封类型门（帧导入核心统一后 MIGRATE / SYNC 共用）：内层
/// RangeIndex 标签 (0x05) 的 kind=2 帧显式拒绝且键绝不落库——对标 C#
/// RespClusterReplicationCommands.cs NetworkClusterSync 对意外 kind 直接抛
/// 的诚实面（本仓 rust 发送端经 read_live_value 门控不产出该帧，本测试钉
/// 死防御拒绝面而非旧静默写）
#[test]
fn cluster_sync_rejects_unsupported_envelope() {
  let rt = Runtime::new().unwrap();
  rt.block_on(async {
    let cp = two_primary_provider();
    let replica = open_store("diskless_reject.db");
    cp.set_store(Arc::clone(&replica));
    let cluster_session: Arc<ClusterSession> = cp.create_cluster_session();
    let mut consumer = RespSessionConsumer::with_cluster(
      1,
      RespServerSessionOptions::default(),
      cluster_session,
      cp.provider_handle(),
      Arc::new(StoreGarnetApi::new(replica.new_session().unwrap())),
    );
    consumer.attach_transaction_components(Arc::new(WatchVersionMap::new(64)), TxnLockTable::new());

    let payload = encode_migration_payload(&[BatchItem {
      key: b"diskless:ri_env",
      val: MigrateVal::Env(b"\x05payload".to_vec()),
      expire_unix_ms: 0,
    }]);
    let (consumed, out) = pump(&mut consumer, &cluster_sync_frame(NODE1_HEX, &payload));
    assert_eq!(consumed, Some(0), "帧应被完整消费");
    assert!(consumer.take_slow_wait().is_none(), "内联驱动不挂起慢路径");
    assert!(
      String::from_utf8_lossy(&out)
        .starts_with("-ERR Unsupported migration record kind 2 (envelope type 5)"),
      "非迁移类型信封经 SYNC 应显式拒绝: {out:?}"
    );

    // 绝不静默写：键不落库
    let session = replica.new_session().unwrap();
    let batch = session.enter_batch();
    let storage = StorageSession::new_readonly(batch);
    assert!(
      storage
        .read_tag_with(b"diskless:ri_env", KeyTag::ObjectEnvelope, |raw| raw
          .to_vec())
        .await
        .unwrap()
        .is_none(),
      "被拒信封不得落库"
    );
  });
}
