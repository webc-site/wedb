//! 迭代式槽位校验与端点偏好集成测试
//!
//! 对标 garnet/libs/cluster/Session/SlotVerification/RespClusterIterativeSlotVerify.cs
//! （事务 Prepare 段逐键校验 + 会话级缓存：状态漂移 TRYAGAIN、首错短路）
//! 与 RespClusterSlotVerify.cs:Redirect 的 ClusterPreferredEndpointType
//! 端点偏好（hostname 重定向形态）。库级定槽（doc/zh/db.md 4.1）下键内容
//! 不参与定槽，裁决由会话库槽位驱动；C# 键间 CROSSSLOT 裁决随键级哈希废除。

use std::sync::Arc;

use wbase::hash_slot::{CLUSTER_SLOT_COUNT, slot_of};

/// 默认会话 (0, 0) 库槽位（键内容不参与定槽）
const SLOT0: u16 = slot_of(0, 0);
/// 远端库号：会话 max_databases 内可 SELECT 的最小异槽库
const REMOTE_DB: u64 = 1;
/// 远端节点承载的槽位：库 (0, 1) 的库级定槽
const REMOTE_SLOT: u16 = slot_of(0, REMOTE_DB);
const _: () = assert!(REMOTE_SLOT != SLOT0, "远端库必须与默认库异槽");

/// SELECT 帧构造
fn select_frame(db: u64) -> Vec<u8> {
  let db_str = db.to_string();
  format!("*2\r\n$6\r\nSELECT\r\n${}\r\n{}\r\n", db_str.len(), db_str).into_bytes()
}
use wdev::SegmentedDevice;
use wedb::server::{
  cluster::IClusterProvider,
  cluster_config::ClusterPreferredEndpointType,
  cluster_provider::ClusterProvider,
  cluster_session::ClusterSession,
  hash_slot::{HashSlot, SlotState},
  slot_verify::{
    ClusterSlotVerificationState, IterativeSlotVerifyCache, SlotVerifiedState,
    iterative_slot_verify_step,
  },
  worker::{LOCAL_WORKER_ID, LocalWorkerSpec, NodeRole, Worker},
};
use wkv::WedbStore;
use wnode::{
  MessageConsumerFace, RespSessionConsumer,
  resp::{garnet_api::StoreGarnetApi, resp_server_session::RespServerSessionOptions},
};
use wtest_base::test_store_config;
use wtxn::{TxnLockTable, WatchVersionMap};

/// 装配双主节点拓扑：node_1（本地）持全部库槽位，唯 REMOTE_SLOT（库 (0,1)
/// 定槽）归 node_2@7001；node_2 带 hostname 供端点偏好用例消费
fn two_primary_provider_with_hostname() -> Arc<ClusterProvider> {
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
      hostname: Some("node1.example.com"),
    });
    config.workers.push(Worker {
      nodeid: Some(0x0000_0000_0000_0000_0000_0000_0000_DE12),
      address: "127.0.0.1".into(),
      port: 7001,
      config_epoch: 1,
      role: NodeRole::Primary,
      replica_of_node_id: None,
      replication_offset: 0,
      hostname: Some("node2.example.com".into()),
    });
    for slot in 0..CLUSTER_SLOT_COUNT {
      config.slot_map[slot] = HashSlot {
        worker_id: LOCAL_WORKER_ID as u16,
        state: SlotState::Stable,
      };
    }
    config.slot_map[REMOTE_SLOT as usize] = HashSlot {
      worker_id: 2,
      state: SlotState::Stable,
    };
  }
  cp
}

/// 构造挂接集群切面 + 存储执行域的会话消费者（与 cluster_resp_session 同款装配）
fn cluster_consumer(cp: &ClusterProvider) -> RespSessionConsumer {
  let cluster_session: Arc<ClusterSession> = cp.create_cluster_session();
  let dir = tempfile::tempdir().unwrap().keep();
  let device = Arc::new(SegmentedDevice::single_file(dir.join("iter.db")).unwrap());
  let config = test_store_config();
  let store = Arc::new(WedbStore::open(config, device).unwrap());
  cp.set_store(Arc::clone(&store));
  let mut consumer = RespSessionConsumer::with_cluster(
    1,
    RespServerSessionOptions::default(),
    cluster_session,
    cp.provider_handle(),
    Arc::new(StoreGarnetApi::new(store.new_session().unwrap())),
  );
  consumer.attach_transaction_components(Arc::new(WatchVersionMap::new(64)), TxnLockTable::new());
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

/// 迭代步进状态机：首键初始化、裁决漂移（含槽位改写）TRYAGAIN、首错短路
#[test]
fn iterative_step_state_machine() {
  let mut cache = IterativeSlotVerifyCache::default();

  // 首键本地 OK → 透传 true，缓存 (slot, Ok)
  assert!(iterative_slot_verify_step(
    &mut cache,
    ClusterSlotVerificationState::new(SlotVerifiedState::Ok, 100),
  ));
  assert_eq!(cache.slot(), 100);
  assert_eq!(cache.state(), SlotVerifiedState::Ok);

  // 同槽同状态 → 透传
  assert!(iterative_slot_verify_step(
    &mut cache,
    ClusterSlotVerificationState::new(SlotVerifiedState::Ok, 100),
  ));

  // 状态漂移（同槽 MIGRATING 产出 ASK）→ TRYAGAIN 且缓存捕获
  assert!(!iterative_slot_verify_step(
    &mut cache,
    ClusterSlotVerificationState::new(SlotVerifiedState::Ask, 100),
  ));
  assert_eq!(cache.state(), SlotVerifiedState::TryAgain);

  // 首错短路：缓存非 OK 后任何键直接 false（C# 校验失败提前返回）
  assert!(!iterative_slot_verify_step(
    &mut cache,
    ClusterSlotVerificationState::new(SlotVerifiedState::Ok, 100),
  ));

  // 槽位改写：fresh 缓存下第二键异槽（库级定槽不可能，上下文被改写降级）→ TRYAGAIN
  let mut cache = IterativeSlotVerifyCache::default();
  assert!(iterative_slot_verify_step(
    &mut cache,
    ClusterSlotVerificationState::new(SlotVerifiedState::Ok, 100),
  ));
  assert!(!iterative_slot_verify_step(
    &mut cache,
    ClusterSlotVerificationState::new(SlotVerifiedState::Ok, 200),
  ));
  assert_eq!(cache.state(), SlotVerifiedState::TryAgain);
  assert_eq!(cache.slot(), 100);

  // 首键失败：缓存记失败类别，后续短路
  let mut cache = IterativeSlotVerifyCache::default();
  assert!(!iterative_slot_verify_step(
    &mut cache,
    ClusterSlotVerificationState::new(SlotVerifiedState::Moved, REMOTE_SLOT),
  ));
  assert_eq!(cache.state(), SlotVerifiedState::Moved);
  assert!(!iterative_slot_verify_step(
    &mut cache,
    ClusterSlotVerificationState::new(SlotVerifiedState::Ok, REMOTE_SLOT),
  ));

  // reset 回到未初始化
  cache.reset();
  assert!(!cache.initialized());
}

/// 会话级迭代校验三方法族：本地/远端槽位 + 缓存错误写出 + 重置
///（库级定槽：裁决由传入的会话库槽位驱动，键内容不参与）
#[test]
fn cluster_session_iterative_verify_and_cached_message() {
  let cp = two_primary_provider_with_hostname();
  let cs: Arc<ClusterSession> = cp.create_cluster_session();

  // 本地库槽位首键 → true
  assert!(cs.network_iterative_slot_verify(b"bar", false, false, SLOT0));

  // 第二键异槽：库级定槽下同批键恒共会话槽位，批次中途槽位漂移即上下文
  // 被改写，与状态漂移同降级 TRYAGAIN（C# CROSSSLOT 随键级哈希废除，
  // 口径同 iterative_step_state_machine）
  assert!(!cs.network_iterative_slot_verify(b"foo", false, false, REMOTE_SLOT));

  // 缓存错误写出：TRYAGAIN 文案（C# WriteCachedSlotVerificationMessage）
  let mut out = Vec::new();
  cs.write_cached_slot_verification_message(&mut out);
  assert_eq!(
    out,
    b"-TRYAGAIN Multiple keys request during rehashing of slot\r\n"
  );

  // 首错短路：缓存非 OK 后同槽键也 false
  assert!(!cs.network_iterative_slot_verify(b"bar", false, false, SLOT0));

  // 重置后首键即远端槽位 → false，缓存记 Moved，写出地址形态
  //（provider 默认 Ip 偏好）
  cs.reset_cached_slot_verification_result();
  assert!(!cs.network_iterative_slot_verify(b"foo", false, false, REMOTE_SLOT));
  let mut out = Vec::new();
  cs.write_cached_slot_verification_message(&mut out);
  assert_eq!(
    out,
    format!("-MOVED {REMOTE_SLOT} 127.0.0.1:7001\r\n").into_bytes()
  );

  // 端点偏好 hostname：MOVED 输出通告 hostname
  cp.set_preferred_endpoint_type(ClusterPreferredEndpointType::Hostname);
  cs.reset_cached_slot_verification_result();
  assert!(!cs.network_iterative_slot_verify(b"foo", false, false, REMOTE_SLOT));
  let mut out = Vec::new();
  cs.write_cached_slot_verification_message(&mut out);
  assert_eq!(
    out,
    format!("-MOVED {REMOTE_SLOT} node2.example.com:7001\r\n").into_bytes()
  );
}

/// 端点偏好 hostname：会话数据命令 MOVED 重定向通告 hostname
///（对标 C# Redirect 经 serverOptions.ClusterPreferredEndpointType 取端点）
#[test]
fn moved_redirect_prefers_hostname() {
  let cp = two_primary_provider_with_hostname();
  cp.set_preferred_endpoint_type(ClusterPreferredEndpointType::Hostname);
  let mut consumer = cluster_consumer(&cp);

  // 会话切至 node_2 承载的库（库级定槽：db=1 恒落 REMOTE_SLOT）→ GET MOVED 通告 hostname
  let out = roundtrip(&mut consumer, &select_frame(REMOTE_DB));
  assert_eq!(out, b"+OK\r\n");
  let out = roundtrip(&mut consumer, b"*2\r\n$3\r\nGET\r\n$3\r\nfoo\r\n");
  assert_eq!(
    out,
    format!("-MOVED {REMOTE_SLOT} node2.example.com:7001\r\n").into_bytes()
  );

  // hostname 缺失回退 "?"（C# GetEndpointByPreferredType IsNullOrEmpty 臂）
  let m = cp.cluster_manager().unwrap();
  // LOCAL_WORKER_ID = 1 占 workers[1]，node_2 在 workers[2]
  m.current_config.write().workers[2].hostname = None;
  let out = roundtrip(&mut consumer, b"*2\r\n$3\r\nGET\r\n$3\r\nfoo\r\n");
  assert_eq!(out, format!("-MOVED {REMOTE_SLOT} ?:7001\r\n").into_bytes());
}

/// 验证连续多命令在发生 MOVED 重定向后，下一合法命令不会携带上一命令的缓存
#[test]
fn sequential_commands_moved_then_ok_not_polluted() {
  let cp = two_primary_provider_with_hostname();
  let mut consumer = cluster_consumer(&cp);

  // 1. 会话切远端库 db=1（恒落 REMOTE_SLOT，归 node_2 服务）→ 触发 MOVED 重定向
  let out = roundtrip(&mut consumer, &select_frame(REMOTE_DB));
  assert_eq!(out, b"+OK\r\n");
  let out1 = roundtrip(&mut consumer, b"*2\r\n$3\r\nGET\r\n$3\r\nfoo\r\n");
  assert_eq!(
    out1,
    format!("-MOVED {REMOTE_SLOT} 127.0.0.1:7001\r\n").into_bytes()
  );

  // 2. 切回本地库 -> 成功返回 nil（不被上一命令残留缓存污染）
  let out = roundtrip(&mut consumer, &select_frame(0));
  assert_eq!(out, b"+OK\r\n");
  let out2 = roundtrip(&mut consumer, b"*2\r\n$3\r\nGET\r\n$3\r\nbar\r\n");
  assert_eq!(out2, b"$-1\r\n");

  // 3. 同会话再次访问本地写入 SET bar val -> 成功返回 +OK
  let out3 = roundtrip(
    &mut consumer,
    b"*3\r\n$3\r\nSET\r\n$3\r\nbar\r\n$3\r\nval\r\n",
  );
  assert_eq!(out3, b"+OK\r\n");

  // 4. 同会话再次读取本地 bar -> 成功返回 val
  let out4 = roundtrip(&mut consumer, b"*2\r\n$3\r\nGET\r\n$3\r\nbar\r\n");
  assert_eq!(out4, b"$3\r\nval\r\n");
}

/// 验证在事务中触发重定向中止后，后续合法命令或新事务不会受到旧缓存污染
///（库级定槽下整批事务键恒共会话槽位，远端库事务 → MOVED 取代旧跨槽路径）
#[test]
fn transaction_boundary_resets_slot_cache() {
  let cp = two_primary_provider_with_hostname();
  let mut consumer = cluster_consumer(&cp);

  // 1. 会话切远端库 db=1，MULTI 事务整批远端 → EXEC MOVED
  let out = roundtrip(&mut consumer, &select_frame(REMOTE_DB));
  assert_eq!(out, b"+OK\r\n");
  let out = roundtrip(&mut consumer, b"*1\r\n$5\r\nMULTI\r\n");
  assert_eq!(out, b"+OK\r\n");

  let out = roundtrip(
    &mut consumer,
    b"*3\r\n$3\r\nSET\r\n$3\r\nfoo\r\n$1\r\n2\r\n",
  );
  assert_eq!(out, b"+QUEUED\r\n");

  let out = roundtrip(&mut consumer, b"*1\r\n$4\r\nEXEC\r\n");
  assert_eq!(
    out,
    format!("-MOVED {REMOTE_SLOT} 127.0.0.1:7001\r\n").into_bytes()
  );

  // 2. 事务边界结束切回本地库，新命令合法，验证不受污染
  let out = roundtrip(&mut consumer, &select_frame(0));
  assert_eq!(out, b"+OK\r\n");
  let out = roundtrip(
    &mut consumer,
    b"*3\r\n$3\r\nSET\r\n$3\r\nbar\r\n$5\r\nhello\r\n",
  );
  assert_eq!(out, b"+OK\r\n");

  let out = roundtrip(&mut consumer, b"*2\r\n$3\r\nGET\r\n$3\r\nbar\r\n");
  assert_eq!(out, b"$5\r\nhello\r\n");

  // 3. DISCARD 边界重置验证
  let out = roundtrip(&mut consumer, b"*1\r\n$5\r\nMULTI\r\n");
  assert_eq!(out, b"+OK\r\n");
  let out = roundtrip(
    &mut consumer,
    b"*3\r\n$3\r\nSET\r\n$3\r\nfoo\r\n$1\r\n1\r\n",
  );
  assert_eq!(out, b"+QUEUED\r\n");
  let out = roundtrip(&mut consumer, b"*1\r\n$7\r\nDISCARD\r\n");
  assert_eq!(out, b"+OK\r\n");

  let out = roundtrip(&mut consumer, b"*2\r\n$3\r\nGET\r\n$3\r\nbar\r\n");
  assert_eq!(out, b"$5\r\nhello\r\n");
}
