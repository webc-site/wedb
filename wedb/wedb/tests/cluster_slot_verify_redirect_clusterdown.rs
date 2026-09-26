//! 集群槽位重定向未指派槽兜底 CLUSTERDOWN 测试
//!
//! 对标 garnet/libs/cluster/Session/SlotVerification/RespClusterSlotVerify.cs:Redirect:
//! port != 0 输出 MOVED，port == 0 兜底 RESP_ERR_CLUSTERDOWN。
//! 验证 COUNTKEYSINSLOT / GETKEYSINSLOT 对未指派槽与已指派远端槽的应答帧。

use std::sync::Arc;

use aok::Void;
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
  ClusterSessionFace, RespSessionConsumer,
  resp::{garnet_api::StoreGarnetApi, resp_server_session::RespServerSessionOptions},
};
use wnode_test::pump;
use wresp::command::RespCommand;
use wtest_base::test_store_config;
use wtxn::{TxnLockTable, WatchVersionMap};

const LOCAL_NODE_ID: u128 = 0x0000_0000_0000_0000_0000_0000_0000_DE11;
const REMOTE_NODE_ID: u128 = 0x0000_0000_0000_0000_0000_0000_0000_DE12;
const UNASSIGNED_SLOT_BYTES: &[u8] = b"5000";
const REMOTE_SLOT_BYTES: &[u8] = b"8000";

/// 装配测试拓扑：
/// - 本地节点：port=7000，分配槽位 0..4000
/// - 远端节点：port=7001，分配槽位 8000..9000
/// - 其余槽位未指派（含 5000，默认 wid=0, port=0, state=Offline）
fn setup_test_cluster() -> (Arc<ClusterProvider>, Arc<ClusterSession>) {
  let cp = ClusterProvider::new();
  let m = cp.cluster_manager().unwrap_or_else(|| {
    let m = Arc::new(ClusterManager::new(Arc::clone(&cp)));
    *cp.cluster_manager.write() = Some(Arc::clone(&m));
    m
  });
  {
    let mut config = m.current_config.write();
    config.initialize_local_worker(LocalWorkerSpec {
      node_id: LOCAL_NODE_ID,
      address: "127.0.0.1",
      port: 7000,
      config_epoch: 1,
      role: NodeRole::Primary,
      replica_of_node_id: None,
      hostname: None,
    });
    let remote_worker_id = config.workers.len() as u16;
    config.workers.push(Worker {
      nodeid: Some(REMOTE_NODE_ID),
      address: "127.0.0.1".into(),
      port: 7001,
      config_epoch: 1,
      role: NodeRole::Primary,
      replica_of_node_id: None,
      replication_offset: 0,
      hostname: None,
    });
    for slot in 0..4000 {
      config.slot_map[slot] = HashSlot {
        worker_id: LOCAL_WORKER_ID as u16,
        state: SlotState::Stable,
      };
    }
    for slot in 8000..9000 {
      config.slot_map[slot] = HashSlot {
        worker_id: remote_worker_id,
        state: SlotState::Stable,
      };
    }
  }
  let session = cp.create_cluster_session();
  (cp, session)
}

/// 构造 RESP 消费者，用于全协议栈端到端测试
fn test_consumer(cp: &ClusterProvider, session: Arc<ClusterSession>) -> RespSessionConsumer {
  let dir = tempfile::tempdir().unwrap().keep();
  let device = Arc::new(SegmentedDevice::single_file(dir.join("cluster_redirect.db")).unwrap());
  let config = test_store_config();
  let store = Arc::new(WedbStore::open(config, device).unwrap());
  cp.set_store(Arc::clone(&store));
  let mut consumer = RespSessionConsumer::with_cluster(
    1,
    RespServerSessionOptions::default(),
    session,
    cp.provider_handle(),
    Arc::new(StoreGarnetApi::new(store.new_session().unwrap())),
  );
  consumer.attach_transaction_components(Arc::new(WatchVersionMap::new(64)), TxnLockTable::new());
  consumer
}

fn roundtrip(consumer: &mut RespSessionConsumer, frame: &[u8]) -> Vec<u8> {
  let (consumed, out) = pump(consumer, frame);
  assert_eq!(consumed, Some(0), "帧应被完整消费: {frame:?}");
  out
}

/// 验证未指派槽（port==0）执行 COUNTKEYSINSLOT / GETKEYSINSLOT 返回 CLUSTERDOWN
#[test]
fn test_unassigned_slot_redirect_clusterdown() -> Void {
  let (cp, session) = setup_test_cluster();

  // 1. 集群会话层直接派发验证：COUNTKEYSINSLOT 5000
  let mut output = Vec::new();
  let handled = session.process_cluster_commands(
    RespCommand::ClusterCountkeysinslot,
    &[UNASSIGNED_SLOT_BYTES],
    &mut output,
    0,
  );
  assert!(handled);
  assert_eq!(output, b"-CLUSTERDOWN Hash slot not served\r\n");

  // 2. 集群会话层直接派发验证：GETKEYSINSLOT 5000 10
  output.clear();
  let handled = session.process_cluster_commands(
    RespCommand::ClusterGetkeysinslot,
    &[UNASSIGNED_SLOT_BYTES, b"10"],
    &mut output,
    0,
  );
  assert!(handled);
  assert_eq!(output, b"-CLUSTERDOWN Hash slot not served\r\n");

  // 3. 全协议栈端到端往返验证
  let mut consumer = test_consumer(&cp, session);

  let resp_count = roundtrip(
    &mut consumer,
    b"*3\r\n$7\r\nCLUSTER\r\n$15\r\nCOUNTKEYSINSLOT\r\n$4\r\n5000\r\n",
  );
  assert_eq!(resp_count, b"-CLUSTERDOWN Hash slot not served\r\n");

  let resp_get = roundtrip(
    &mut consumer,
    b"*4\r\n$7\r\nCLUSTER\r\n$13\r\nGETKEYSINSLOT\r\n$4\r\n5000\r\n$2\r\n10\r\n",
  );
  assert_eq!(resp_get, b"-CLUSTERDOWN Hash slot not served\r\n");

  aok::OK
}

/// 验证已指派至远端节点的正常槽（port!=0）执行 COUNTKEYSINSLOT / GETKEYSINSLOT 仍正确返回 MOVED
#[test]
fn test_remote_slot_redirect_moved() -> Void {
  let (cp, session) = setup_test_cluster();

  // 1. 集群会话层直接派发验证：COUNTKEYSINSLOT 8000
  let mut output = Vec::new();
  let handled = session.process_cluster_commands(
    RespCommand::ClusterCountkeysinslot,
    &[REMOTE_SLOT_BYTES],
    &mut output,
    0,
  );
  assert!(handled);
  assert_eq!(output, b"-MOVED 8000 127.0.0.1:7001\r\n");

  // 2. 集群会话层直接派发验证：GETKEYSINSLOT 8000 10
  output.clear();
  let handled = session.process_cluster_commands(
    RespCommand::ClusterGetkeysinslot,
    &[REMOTE_SLOT_BYTES, b"10"],
    &mut output,
    0,
  );
  assert!(handled);
  assert_eq!(output, b"-MOVED 8000 127.0.0.1:7001\r\n");

  // 3. 全协议栈端到端往返验证
  let mut consumer = test_consumer(&cp, session);

  let resp_count = roundtrip(
    &mut consumer,
    b"*3\r\n$7\r\nCLUSTER\r\n$15\r\nCOUNTKEYSINSLOT\r\n$4\r\n8000\r\n",
  );
  assert_eq!(resp_count, b"-MOVED 8000 127.0.0.1:7001\r\n");

  let resp_get = roundtrip(
    &mut consumer,
    b"*4\r\n$7\r\nCLUSTER\r\n$13\r\nGETKEYSINSLOT\r\n$4\r\n8000\r\n$2\r\n10\r\n",
  );
  assert_eq!(resp_get, b"-MOVED 8000 127.0.0.1:7001\r\n");

  aok::OK
}
