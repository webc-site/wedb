//! 迭代式槽位校验与端点偏好集成测试
//!
//! 对标 garnet/libs/cluster/Session/SlotVerification/RespClusterIterativeSlotVerify.cs
//! （事务 Prepare 段逐键校验 + 会话级缓存：跨槽 CROSSSLOT、状态漂移
//! TRYAGAIN、首错短路）与 RespClusterSlotVerify.cs:Redirect 的
//! ClusterPreferredEndpointType 端点偏好（hostname 重定向形态）。

use std::sync::Arc;

use parking_lot::RwLock;
use wbase::hash_slot::hash_slot as cluster_slot;
use wcustom::{CustomCommandManager, CustomTxnProc, SetTxnProc};
use wdev::SegmentedDevice;
use wedb::server::{
  cluster::IClusterProvider,
  cluster_config::ClusterPreferredEndpointType,
  cluster_provider::ClusterProvider,
  cluster_session::ClusterSession,
  hash_slot::{HashSlot, SlotState},
  slot_verify::{
    ClusterSlotVerificationState, IterativeSlotVerifyCache, SlotVerifyKind,
    iterative_slot_verify_step,
  },
  worker::{LOCAL_WORKER_ID, LocalWorkerSpec, NodeRole, Worker},
};
use wedb_test::test_store_config;
use wkv::WedbStore;
use wnode::{
  MessageConsumerFace, RespSessionConsumer,
  resp::{garnet_api::StoreGarnetApi, resp_server_session::RespServerSessionOptions},
};

/// 装配双主节点拓扑：node_1（本地）持 0..8192，node_2@7001 持 8192..16384；
/// node_2 带 hostname 供端点偏好用例消费
fn two_primary_provider_with_hostname() -> Arc<ClusterProvider> {
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
      hostname: Some("node1.example.com"),
    });
    config.workers.push(Worker {
      nodeid: Some("node_2".to_string()),
      address: "127.0.0.1".to_string(),
      port: 7001,
      config_epoch: 1,
      role: NodeRole::Primary,
      replica_of_node_id: None,
      replication_offset: 0,
      hostname: Some("node2.example.com".to_string()),
    });
    for slot in 0..8192 {
      config.slot_map[slot] = HashSlot {
        worker_id: LOCAL_WORKER_ID as u16,
        state: SlotState::Stable,
      };
    }
    for slot in 8192..16384 {
      config.slot_map[slot] = HashSlot {
        worker_id: 2,
        state: SlotState::Stable,
      };
    }
  }
  cp
}

/// 构造挂接集群切面 + 存储执行域的会话消费者（与 cluster_resp_session 同款装配）
fn cluster_consumer(cp: &ClusterProvider) -> RespSessionConsumer {
  let cluster_session: Arc<ClusterSession> = cp.create_cluster_session();
  let dir = tempfile::tempdir().unwrap().keep();
  let device = Arc::new(SegmentedDevice::single_file(dir.join("iter.db")).unwrap());
  let mut config = test_store_config();
  config.gc.enabled = false;
  let store = Arc::new(WedbStore::open(config, device).unwrap());
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
  consumer.attach_transaction_components(Arc::new(wtxn::WatchVersionMap::new(64)));
  consumer
}

/// 单命令往返（单帧完整到达；scratch 持久游标消费面）
fn roundtrip(consumer: &mut RespSessionConsumer, frame: &[u8]) -> Vec<u8> {
  let (consumed, out) = pump(consumer, frame);
  assert_eq!(consumed, Some(0), "帧应被完整消费: {frame:?}");
  out
}

/// scratch 直读消费泵（与 cluster_resp_session 同款：帧入接收缓冲 → 消费至收尾 0）
fn pump(consumer: &mut RespSessionConsumer, frame: &[u8]) -> (Option<usize>, Vec<u8>) {
  let mut scratch = consumer.take_recv_scratch();
  scratch.extend_from_slice(frame);
  consumer.return_recv_scratch(scratch);
  let mut resp = Vec::new();
  let remaining = consumer.try_consume_messages_into(&mut resp);
  (remaining, resp)
}

/// 迭代步进状态机：首键初始化、跨槽 CROSSSLOT、状态漂移 TRYAGAIN、首错短路
#[test]
fn iterative_step_state_machine() {
  let mut cache = IterativeSlotVerifyCache::default();

  // 首键本地 OK → 透传 true，缓存 (slot, Ok)
  assert!(iterative_slot_verify_step(
    &mut cache,
    ClusterSlotVerificationState::Ok,
    100,
  ));
  assert_eq!(cache.slot(), 100);
  assert_eq!(cache.state(), SlotVerifyKind::Ok);

  // 同槽同状态 → 透传
  assert!(iterative_slot_verify_step(
    &mut cache,
    ClusterSlotVerificationState::Ok,
    100,
  ));

  // 状态漂移（同槽 MIGRATING 产出 ASK）→ TRYAGAIN 且缓存捕获
  assert!(!iterative_slot_verify_step(
    &mut cache,
    ClusterSlotVerificationState::Ask {
      slot: 100,
      endpoint: "127.0.0.1".into(),
      port: 7001,
    },
    100,
  ));
  assert_eq!(cache.state(), SlotVerifyKind::TryAgain);

  // 首错短路：缓存非 OK 后任何键直接 false（C# 校验失败提前返回）
  assert!(!iterative_slot_verify_step(
    &mut cache,
    ClusterSlotVerificationState::Ok,
    100,
  ));

  // 跨槽：fresh 缓存下第二键异槽 → CROSSSLOT
  let mut cache = IterativeSlotVerifyCache::default();
  assert!(iterative_slot_verify_step(
    &mut cache,
    ClusterSlotVerificationState::Ok,
    100,
  ));
  assert!(!iterative_slot_verify_step(
    &mut cache,
    ClusterSlotVerificationState::Ok,
    200,
  ));
  assert_eq!(cache.state(), SlotVerifyKind::CrossSlot);
  // 缓存槽位保持首键（C# new(CROSSSLOT, cachedVerificationResult.slot)）
  assert_eq!(cache.slot(), 100);

  // 首键失败：缓存记失败类别，后续短路
  let mut cache = IterativeSlotVerifyCache::default();
  assert!(!iterative_slot_verify_step(
    &mut cache,
    ClusterSlotVerificationState::Moved {
      slot: 12182,
      endpoint: "127.0.0.1".into(),
      port: 7001,
    },
    12182,
  ));
  assert_eq!(cache.state(), SlotVerifyKind::Moved);
  assert!(!iterative_slot_verify_step(
    &mut cache,
    ClusterSlotVerificationState::Ok,
    12182,
  ));

  // reset 回到未初始化
  cache.reset();
  assert!(!cache.initialized());
}

/// 会话级迭代校验三方法族：本地/远端逐键 + 缓存错误写出 + 重置
#[test]
fn cluster_session_iterative_verify_and_cached_message() {
  let cp = two_primary_provider_with_hostname();
  let cs: Arc<ClusterSession> = cp.create_cluster_session();

  // 本地键 bar(5061) 首键 → true
  assert_eq!(cluster_slot(b"bar"), 5061);
  assert!(cs.network_iterative_slot_verify(b"bar", false, false));

  // 远端键 foo(12182) → slot 变化 → false，缓存 CROSSSLOT
  assert_eq!(cluster_slot(b"foo"), 12182);
  assert!(!cs.network_iterative_slot_verify(b"foo", false, false));

  // 缓存错误写出：CROSSSLOT 文案（C# WriteCachedSlotVerificationMessage）
  let mut out = Vec::new();
  cs.write_cached_slot_verification_message(&mut out);
  assert_eq!(
    out,
    b"-CROSSSLOT Keys in request don't hash to the same slot\r\n"
  );

  // 首错短路：缓存非 OK 后同槽键也 false
  assert!(!cs.network_iterative_slot_verify(b"bar", false, false));

  // 重置后单键远端 MOVED：缓存记 Moved，写出 hostname 形态
  //（provider 默认 Ip → 地址形态）
  cs.reset_cached_slot_verification_result();
  assert!(!cs.network_iterative_slot_verify(b"foo", false, false));
  let mut out = Vec::new();
  cs.write_cached_slot_verification_message(&mut out);
  assert_eq!(out, b"-MOVED 12182 127.0.0.1:7001\r\n");

  // 端点偏好 hostname：MOVED 输出通告 hostname
  cp.set_preferred_endpoint_type(ClusterPreferredEndpointType::Hostname);
  cs.reset_cached_slot_verification_result();
  assert!(!cs.network_iterative_slot_verify(b"foo", false, false));
  let mut out = Vec::new();
  cs.write_cached_slot_verification_message(&mut out);
  assert_eq!(out, b"-MOVED 12182 node2.example.com:7001\r\n");
}

/// 端点偏好 hostname：会话数据命令 MOVED 重定向通告 hostname
///（对标 C# Redirect 经 serverOptions.ClusterPreferredEndpointType 取端点）
#[test]
fn moved_redirect_prefers_hostname() {
  let cp = two_primary_provider_with_hostname();
  cp.set_preferred_endpoint_type(ClusterPreferredEndpointType::Hostname);
  let mut consumer = cluster_consumer(&cp);

  // foo(12182) ∈ node_2 → MOVED 通告 hostname（C# ip 形态默认由配置切换）
  let out = roundtrip(&mut consumer, b"*2\r\n$3\r\nGET\r\n$3\r\nfoo\r\n");
  assert_eq!(out, b"-MOVED 12182 node2.example.com:7001\r\n");

  // hostname 缺失回退 "?"（C# GetEndpointByPreferredType IsNullOrEmpty 臂）
  let m = cp.cluster_manager().unwrap();
  // LOCAL_WORKER_ID = 1 占 workers[1]，node_2 在 workers[2]
  m.current_config.write().workers[2].hostname = None;
  let out = roundtrip(&mut consumer, b"*2\r\n$3\r\nGET\r\n$3\r\nfoo\r\n");
  assert_eq!(out, b"-MOVED 12182 ?:7001\r\n");
}

/// RUNTXP 跨槽事务：Prepare 段逐键校验（C# CustomTransactionProcedure.AddKey
/// → TxnKeyManager.VerifyKeyOwnership → NetworkIterativeSlotVerify），
/// 异槽键对第二键触发 CROSSSLOT，事务 Aborted 且缓存错误落线
#[test]
fn runtxp_cross_slot_transaction_aborts_with_crossslot() {
  let cp = two_primary_provider_with_hostname();
  let mut consumer = cluster_consumer(&cp);

  // 注册 SET 形态事务过程（键值对逐键登记锁 + 所有权校验）
  let mut registry = CustomCommandManager::new();
  let txn_id = registry
    .register_transaction(
      "setkv",
      Some(|| CustomTxnProc::Set(SetTxnProc::default())),
      None,
      None,
    )
    .unwrap();
  consumer.set_custom_command_manager(Arc::new(RwLock::new(registry)));

  // 异槽键对 bar(5061 本地) + foo(12182 远端)：第二键 slot 漂移 →
  // CROSSSLOT 写出（C# 缓存消息经 WriteCachedSlotVerificationMessage 落线）
  let frame = format!(
    "*6\r\n$6\r\nRUNTXP\r\n${}\r\n{}\r\n$3\r\nbar\r\n$1\r\nv\r\n$3\r\nfoo\r\n$1\r\nw\r\n",
    txn_id.to_string().len(),
    txn_id
  );
  let out = roundtrip(&mut consumer, frame.as_bytes());
  assert_eq!(
    out,
    b"-CROSSSLOT Keys in request don't hash to the same slot\r\n"
  );

  // 同槽键对（hash tag {bar} 同槽 5061）：本地放行真执行（+OK），无重定向
  let frame = format!(
    "*6\r\n$6\r\nRUNTXP\r\n${}\r\n{}\r\n$6\r\n{{bar}}a\r\n$1\r\n1\r\n$6\r\n{{bar}}b\r\n$1\r\n2\r\n",
    txn_id.to_string().len(),
    txn_id
  );
  let out = roundtrip(&mut consumer, frame.as_bytes());
  assert_eq!(out, b"+OK\r\n");

  // 跨槽中止后事务不残留（C# Reset(running)）：同会话继续可用（演示过程
  // main 不落存储，键返回 nil 即会话消费链完好）
  let out = roundtrip(&mut consumer, b"*2\r\n$3\r\nGET\r\n$6\r\n{bar}a\r\n");
  assert_eq!(out, b"$-1\r\n");
}
