//! 集群端点偏好与迭代缓存重置时序集成测试
//!
//! 迭代式逐键校验族（C# RespClusterIterativeSlotVerify.cs /
//! TxnKeyManager.VerifyKeyOwnership）随存储过程域清退删除（本仓 RUNTXP
//! 无条件回 NO_TRANSACTION_PROCEDURE），其活口由多键门与缓存重置时序承接：
//! - 端点偏好（hostname 重定向形态）：对标 RespClusterSlotVerify.cs:Redirect 经
//!   ClusterPreferredEndpointType 取端点，会话数据命令 / EXEC 事务多键门直写 MOVED；
//! - 缓存重置时序：多键门入口 reset_cached_slot_verification_result 于新命令 / 事务
//!   边界清态，杜绝跨命令、跨事务的裁决污染（C# NetworkMultiKeySlotVerify 起点同 reset）。
//!
//! 库级定槽（doc/zh/db.md 4.1）下键内容不参与定槽，裁决由会话库槽位驱动；
//! C# 键间 CROSSSLOT 裁决随键级哈希废除。多键门 TRYAGAIN 传输窗混合态帧见
//! wnode txn_exec_cluster_readonly_verify.rs，槽位状态机渲染见 cluster_slot_verify.rs。

use std::sync::Arc;

#[path = "common/cluster_consumer_fresh.rs"]
mod cluster_cc_fresh;

use cluster_cc_fresh::cluster_consumer;
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
use wedb::server::{
  cluster_config::ClusterPreferredEndpointType,
  cluster_provider::ClusterProvider,
  hash_slot::{HashSlot, SlotState},
  worker::{LOCAL_WORKER_ID, LocalWorkerSpec, NodeRole, Worker},
};
use wnode::RespSessionConsumer;
use wnode_test::pump;

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

/// 单命令往返（单帧完整到达；scratch 持久游标消费面）
fn roundtrip(consumer: &mut RespSessionConsumer, frame: &[u8]) -> Vec<u8> {
  let (consumed, out) = pump(consumer, frame);
  assert_eq!(consumed, Some(0), "帧应被完整消费: {frame:?}");
  out
}

/// 端点偏好 hostname：会话数据命令 MOVED 重定向通告 hostname
///（对标 C# Redirect 经 serverOptions.ClusterPreferredEndpointType 取端点）
#[test]
fn moved_redirect_prefers_hostname() {
  let cp = two_primary_provider_with_hostname();
  cp.set_preferred_endpoint_type(ClusterPreferredEndpointType::Hostname);
  let mut consumer = cluster_consumer(&cp, "iter.db");

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
  let mut consumer = cluster_consumer(&cp, "iter.db");

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
  let mut consumer = cluster_consumer(&cp, "iter.db");

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
