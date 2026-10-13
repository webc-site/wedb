#![recursion_limit = "256"]
//! 集群管理臂有界纪元排空未静止留痕与照常生效回归测试（工单 wedb-cluster-mgmt-epoch-drain-warn-only-trace）
//!
//! 全族统一判据（deviations §182）：
//! 有界纪元排空（bump_and_wait_for_epoch_transition）返 false 的处理按「本臂有无精确逆件」分界：
//! - 无精确逆件的管理臂（SETSLOT、SETSLOTSRANGE、REPLICAOF NO ONE）：
//!   若排空超时未在 cluster-node-timeout 内达成，则承判留痕（warn 记录未静止事实与槽号/区间/臂位），
//!   应答按变更已生效照实回 +OK，绝不假报失败回滚，亦不给收口钩加门。
//!
//! 三面锁：
//! 1. CLUSTER SETSLOT <slot> NODE <node2>：排空超时时记 warn 恰一条，仍回 +OK，槽权转移生效，
//!    config_version 推进，revoke_shard_subscriptions_for_slots 照常触发；
//! 2. CLUSTER SETSLOTSRANGE NODE <node2> <start> <end>：排空超时时记 warn 恰一条（含槽区间），
//!    仍回 +OK，槽区间转移生效，config_version 推进，分片订阅收口照常触发；
//! 3. REPLICAOF NO ONE：排空超时时记 warn 恰一条，仍回 +OK，升主生效，恢复锁正常释放，
//!    后续可再次执行不被 ERR_RECOVERY_LOCK 拒。

use std::sync::Arc;

use aok::Void;
use log::Level;
use wbase::hash_slot::CLUSTER_SLOT_COUNT;
use wedb::server::{
  cluster::IClusterProvider,
  cluster_provider::ClusterProvider,
  cluster_session::ClusterSession,
  hash_slot::{HashSlot, SlotState},
  worker::{LOCAL_WORKER_ID, LocalWorkerSpec, NodeRole, Worker},
};
use wedb_test::{
  cluster_consumer_fresh::cluster_consumer_fresh, de11_node_id::DE11_NODE_ID,
  de12_node_id::DE12_NODE_ID,
};
use wnode::{
  ClusterSessionFace, RespSessionConsumer, resp::resp_server_session::RespServerSessionOptions,
};
use wnode_test::pump_frame;
use wpubsub::{
  subscribe_broker::SubscribeBroker,
  subscriber::{PubSubMailbox, PubSubMessageKind},
};
use wtest_base::{log_capture_mark, log_capture_records_since, resp_frame_str};

const LOCAL_ID: u128 = DE11_NODE_ID;
const REMOTE_ID: u128 = DE12_NODE_ID;
const REMOTE_HEX: &str = "0000000000000000000000000000de12";
/// 排空未达成注入超时：50ms 既令超时即刻达，又避免拖慢整套测试
const DRAIN_TIMEOUT_MS: u64 = 50;

/// 装配包含本地节点与远端节点的集群提供者
fn test_cluster_provider(is_replica: bool) -> Arc<ClusterProvider> {
  let cp = ClusterProvider::new();
  let m = cp.cluster_manager().unwrap();
  {
    let mut config = m.current_config.write();
    config.initialize_local_worker(LocalWorkerSpec {
      node_id: LOCAL_ID,
      address: "127.0.0.1",
      port: 7000,
      config_epoch: 1,
      role: if is_replica {
        NodeRole::Replica
      } else {
        NodeRole::Primary
      },
      replica_of_node_id: if is_replica { Some(REMOTE_ID) } else { None },
      hostname: None,
    });
    config.workers.push(Worker {
      nodeid: Some(REMOTE_ID),
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
  }
  cp
}

/// 装配 RESP 会话消费者（装配单源见 `wedb_test::cluster_consumer_fresh`）
fn test_cluster_consumer(cp: &Arc<ClusterProvider>) -> RespSessionConsumer {
  cluster_consumer_fresh(cp, "test_drain.db", RespServerSessionOptions::default())
}

/// 单命令往返
/// 恒不追平夹具（复用 failover_epoch_drain_failclose.rs 形态）：
/// 注册会话先行批首纪元快照，原语 bump 后该会话恒落后 → 排空等待超时返 false
fn park_lagging_session(cp: &Arc<ClusterProvider>) -> Arc<ClusterSession> {
  cp.bump_current_epoch();
  let lag = cp.create_cluster_session();
  lag.acquire_current_epoch();
  assert_eq!(lag.local_current_epoch(), cp.current_epoch());
  cp.set_cluster_node_timeout_ms(DRAIN_TIMEOUT_MS);
  lag
}

/// 1. CLUSTER SETSLOT NODE 臂：排空超时时承判留痕（warn 恰一条），仍响应 +OK，
///    槽态与版本单调推进，分片订阅收口钩照常触发
#[test]
fn test_setslot_node_epoch_drain_timeout_warns_and_retains_ok() -> Void {
  let cp = test_cluster_provider(false);
  let broker = Arc::new(SubscribeBroker::new());
  cp.set_pubsub(Some(broker.clone()));

  const TARGET_SLOT: usize = 100;
  let mb = Arc::new(PubSubMailbox::new(16));
  assert!(broker.shard_subscribe(TARGET_SLOT as u16, b"channel_100", 1, mb.clone()));
  assert_eq!(
    broker.list_all_shard_subscriptions(),
    vec![b"channel_100".to_vec()]
  );

  let _lag = park_lagging_session(&cp);
  let mut consumer = test_cluster_consumer(&cp);
  let m = cp.cluster_manager().unwrap();
  let v0 = m.config_version();

  let mark = log_capture_mark();
  let frame = resp_frame_str(&[
    "CLUSTER",
    "SETSLOT",
    &TARGET_SLOT.to_string(),
    "NODE",
    REMOTE_HEX,
  ]);
  let out = pump_frame(&mut consumer, &frame);

  // 应答依然为 +OK（不假报失败）
  assert_eq!(out, b"+OK\r\n");

  // warn 留痕恰一条且内容对位
  let warns: Vec<_> = log_capture_records_since(mark)
    .into_iter()
    .filter(|(lvl, msg)| *lvl == Level::Warn && msg.contains("SETSLOT 100"))
    .collect();
  assert_eq!(warns.len(), 1, "SETSLOT 留痕应恰一条 warn: {warns:?}");
  assert!(
    warns[0]
      .1
      .contains("槽权变更已生效，纪元排空未在 cluster-node-timeout 内达成"),
    "warn 内容应明确槽权已生效与排空超时: {}",
    warns[0].1
  );

  // 槽态已转移至远程属主，config_version 单调递增
  let config = m.current_config();
  assert_eq!(
    config.get_node_id_from_slot(TARGET_SLOT as u16),
    Some(REMOTE_ID)
  );
  assert!(m.config_version() > v0, "config_version 应单调推进");

  // 分片订阅收口钩已照常触发并清空锚定订阅
  assert!(broker.list_all_shard_subscriptions().is_empty());
  assert_eq!(mb.len(), 1);
  let mut msgs = Vec::new();
  mb.drain_into(&mut msgs);
  assert_eq!(msgs[0].kind, PubSubMessageKind::ShardUnsubscribe);
  assert_eq!(msgs[0].channel.as_ref(), b"channel_100");

  aok::OK
}

/// 2. CLUSTER SETSLOTSRANGE NODE 臂：排空超时时承判留痕（warn 恰一条且携带区间），
///    仍响应 +OK，槽区间转移生效，config_version 推进，分片订阅收口钩照常触发
#[test]
fn test_setslots_range_node_epoch_drain_timeout_warns_and_retains_ok() -> Void {
  let cp = test_cluster_provider(false);
  let broker = Arc::new(SubscribeBroker::new());
  cp.set_pubsub(Some(broker.clone()));

  const START_SLOT: usize = 200;
  const END_SLOT: usize = 205;
  let mb_start = Arc::new(PubSubMailbox::new(16));
  let mb_end = Arc::new(PubSubMailbox::new(16));
  assert!(broker.shard_subscribe(START_SLOT as u16, b"ch200", 1, mb_start.clone()));
  assert!(broker.shard_subscribe(END_SLOT as u16, b"ch205", 2, mb_end.clone()));

  let _lag = park_lagging_session(&cp);
  let mut consumer = test_cluster_consumer(&cp);
  let m = cp.cluster_manager().unwrap();
  let v0 = m.config_version();

  let mark = log_capture_mark();
  let frame = resp_frame_str(&[
    "CLUSTER",
    "SETSLOTSRANGE",
    "NODE",
    REMOTE_HEX,
    &START_SLOT.to_string(),
    &END_SLOT.to_string(),
  ]);
  let out = pump_frame(&mut consumer, &frame);

  assert_eq!(out, b"+OK\r\n");

  let warns: Vec<_> = log_capture_records_since(mark)
    .into_iter()
    .filter(|(lvl, msg)| *lvl == Level::Warn && msg.contains("SETSLOTSRANGE"))
    .collect();
  assert_eq!(warns.len(), 1, "SETSLOTSRANGE 留痕应恰一条 warn: {warns:?}");
  assert!(
    warns[0]
      .1
      .contains("槽区间变更已生效，纪元排空未在 cluster-node-timeout 内达成")
      && warns[0].1.contains(&format!("[{START_SLOT}, {END_SLOT}]")),
    "warn 内容应包含槽区间与排空超时: {}",
    warns[0].1
  );

  let config = m.current_config();
  for s in START_SLOT..=END_SLOT {
    assert_eq!(config.get_node_id_from_slot(s as u16), Some(REMOTE_ID));
  }
  assert!(m.config_version() > v0, "config_version 应单调推进");

  assert!(broker.list_all_shard_subscriptions().is_empty());
  assert_eq!(mb_start.len(), 1);
  assert_eq!(mb_end.len(), 1);

  aok::OK
}

/// 3. REPLICAOF NO ONE 臂：排空超时时承判留痕（warn 恰一条），仍响应 +OK，
///    角色转为主节点，恢复锁正常释放，后续可再次发起不被 ERR_RECOVERY_LOCK 拒
#[test]
fn test_replicaof_noone_epoch_drain_timeout_warns_and_retains_ok() -> Void {
  let cp = test_cluster_provider(true);
  let rm = cp.replication_manager().unwrap();
  let m = cp.cluster_manager().unwrap();

  let _lag = park_lagging_session(&cp);
  let mut consumer = test_cluster_consumer(&cp);

  let mark = log_capture_mark();
  let frame = resp_frame_str(&["REPLICAOF", "NO", "ONE"]);
  let out = pump_frame(&mut consumer, &frame);

  assert_eq!(out, b"+OK\r\n");

  let warns: Vec<_> = log_capture_records_since(mark)
    .into_iter()
    .filter(|(lvl, msg)| *lvl == Level::Warn && msg.contains("REPLICAOF NO ONE"))
    .collect();
  assert_eq!(
    warns.len(),
    1,
    "REPLICAOF NO ONE 留痕应恰一条 warn: {warns:?}"
  );
  assert!(
    warns[0]
      .1
      .contains("角色变更已生效但纪元排空未在超时内达成"),
    "warn 内容应说明角色变更已生效且排空超时: {}",
    warns[0].1
  );

  // 角色已翻转为主，且恢复锁已释放
  assert!(m.current_config().is_primary(), "配置应已为主角色");
  assert!(!rm.is_recovering(), "恢复状态应已收口为 NoRecovery");

  // 再次发起 REPLICAOF NO ONE，不被 ERR_RECOVERY_LOCK 拒绝（证明锁未泄露）
  let out2 = pump_frame(&mut consumer, &frame);
  assert_eq!(out2, b"+OK\r\n");

  aok::OK
}
