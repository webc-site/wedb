//! SWAPDB 集群槽态门禁集成测试（对标 C# ClusterManager.cs 槽态管理）：
//! 迁移窗口（Migrating/Importing）内对涉及库执行 SWAPDB 必须拦截，
//! 与 doc/zh/db.md SWAPDB 条款的「两库槽位均由本地节点掌管」语义补齐
//! 状态半边——属主判定（is_local）对 Migrating 槽 eff 恒本地、对
//! Importing 槽（目标端）亦本地，单靠属主门放行会打破换号与在途搬迁的互斥。
//! 装配形态复用 cluster_resp_session.rs 的会话泵（不编辑该文件，避免与
//! 迁移面作业冲突）
use std::sync::Arc;

use compio::runtime::Runtime;
use wbase::hash_slot::slot_of;
use wdev::SegmentedDevice;
use wedb::server::{
  cluster::IClusterProvider,
  cluster_manager::ClusterManager,
  cluster_provider::ClusterProvider,
  cluster_session::ClusterSession,
  hash_slot::{HashSlot, SlotState},
  worker::{LOCAL_WORKER_ID, LocalWorkerSpec, NodeRole, Worker},
};
use wkv::WedbStore;
use wnode::{
  MessageConsumerFace, RespSessionConsumer,
  resp::{garnet_api::StoreGarnetApi, resp_server_session::RespServerSessionOptions},
};
use wnode_test::pump;
use wtest_base::test_store_config;
use wtxn::{TxnLockTable, WatchVersionMap};

/// 本地节点持全部槽位（Stable）的单主拓扑
fn local_primary_provider() -> Arc<ClusterProvider> {
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
    for slot in config.slot_map.iter_mut() {
      *slot = HashSlot {
        worker_id: LOCAL_WORKER_ID as u16,
        state: SlotState::Stable,
      };
    }
  }
  Arc::clone(&cp)
}

/// 双库上界消费者（集群切面 + 存储执行域）
fn cluster_consumer(cp: &ClusterProvider) -> RespSessionConsumer {
  let cluster_session: Arc<ClusterSession> = cp.create_cluster_session();
  let dir = tempfile::tempdir().unwrap().keep();
  let device = Arc::new(SegmentedDevice::single_file(dir.join("gate.db")).unwrap());
  let config = test_store_config();
  let store = Arc::new(WedbStore::open(config, device).unwrap());
  cp.set_store(Arc::clone(&store));
  let mut consumer = RespSessionConsumer::with_cluster(
    1,
    RespServerSessionOptions {
      max_databases: 2,
      ..RespServerSessionOptions::default()
    },
    cluster_session,
    cp.provider_handle(),
    Arc::new(StoreGarnetApi::new(store.new_session().unwrap())),
  );
  consumer.attach_transaction_components(Arc::new(WatchVersionMap::new(64)), TxnLockTable::new());
  consumer
}

/// 单命令往返（单帧完整到达）
fn roundtrip(consumer: &mut RespSessionConsumer, frame: &[u8]) -> Vec<u8> {
  let (consumed, out) = pump(consumer, frame);
  assert_eq!(consumed, Some(0), "帧应被完整消费: {frame:?}");
  out
}

/// 迁移窗口拦截回既有单源模板文案（wresp::cmd_strings
/// RESP_ERR_GENERIC_SWAPDB_CLUSTER_MODE，不手拼第二处文本源）
const REJECT: &[u8] = b"-ERR SWAPDB databases are not served by this node\r\n";

fn swapdb_frame(a: u64, b: u64) -> Vec<u8> {
  let (sa, sb) = (a.to_string(), b.to_string());
  format!(
    "*3\r\n$6\r\nSWAPDB\r\n${}\r\n{sa}\r\n${}\r\n{sb}\r\n",
    sa.len(),
    sb.len()
  )
  .into_bytes()
}

/// 源端视角：db1 掌管槽位置为 Migrating（eff 属主仍恒本地），
/// SWAPDB 0 1 与同库 1 1 都必须被槽态门拦截，且数据不动
#[test]
fn swapdb_rejected_while_slot_migrating() {
  let cp = local_primary_provider();
  {
    let cm = cp.cluster_manager().unwrap();
    let mut config = cm.current_config.write();
    config.slot_map[slot_of(0, 1) as usize] = HashSlot {
      worker_id: (config.workers.len() - 1) as u16,
      state: SlotState::Migrating,
    };
  }
  let mut consumer = cluster_consumer(&cp);

  // 预置两库标记值，验证拒绝后归属未被动过
  assert_eq!(
    roundtrip(&mut consumer, b"*3\r\n$3\r\nSET\r\n$1\r\nk\r\n$1\r\na\r\n"),
    b"+OK\r\n"
  );
  assert_eq!(
    roundtrip(&mut consumer, &swapdb_frame(0, 1)),
    REJECT,
    "Migrating 窗口内跨库 SWAPDB 必须拦截"
  );
  assert_eq!(
    roundtrip(&mut consumer, &swapdb_frame(1, 1)),
    REJECT,
    "Migrating 窗口内同库 SWAPDB 同样拦截（门禁先于同库短路）"
  );
  assert_eq!(
    roundtrip(&mut consumer, b"*2\r\n$3\r\nGET\r\n$1\r\nk\r\n"),
    b"$1\r\na\r\n"
  );
}

/// 目标端视角：db1 掌管槽位置为 Importing（worker_id 已是本地），
/// 属主门单判会放行，槽态门必须拦截
#[test]
fn swapdb_rejected_while_slot_importing() {
  let cp = local_primary_provider();
  {
    let cm = cp.cluster_manager().unwrap();
    let mut config = cm.current_config.write();
    config.slot_map[slot_of(0, 1) as usize] = HashSlot {
      worker_id: LOCAL_WORKER_ID as u16,
      state: SlotState::Importing,
    };
  }
  let mut consumer = cluster_consumer(&cp);

  assert_eq!(
    roundtrip(&mut consumer, &swapdb_frame(0, 1)),
    REJECT,
    "Importing 窗口内 SWAPDB 必须拦截"
  );
}

/// 回归锁：两库槽位均 Stable 且属本地时照常放行，慢路径换号生效
#[test]
fn swapdb_allowed_when_both_slots_stable() {
  let rt = Runtime::new().unwrap();
  let cp = local_primary_provider();
  let mut consumer = cluster_consumer(&cp);

  assert_eq!(
    roundtrip(&mut consumer, b"*3\r\n$3\r\nSET\r\n$1\r\nk\r\n$1\r\na\r\n"),
    b"+OK\r\n"
  );
  let (consumed, mut out) = pump(&mut consumer, &swapdb_frame(0, 1));
  assert_eq!(consumed, Some(0));
  let slow = consumer.take_slow_wait().expect("SWAPDB 应挂起慢路径");
  rt.block_on(async {
    out.extend_from_slice(&slow.resolve().await);
  });
  assert_eq!(out, b"+OK\r\n");
  // 换号生效：db1 见原 db0 的 a
  assert_eq!(
    roundtrip(&mut consumer, b"*2\r\n$6\r\nSELECT\r\n$1\r\n1\r\n"),
    b"+OK\r\n"
  );
  assert_eq!(
    roundtrip(&mut consumer, b"*2\r\n$3\r\nGET\r\n$1\r\nk\r\n"),
    b"$1\r\na\r\n"
  );
}
