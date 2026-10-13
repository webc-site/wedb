#![recursion_limit = "256"]
//! 分片订阅对槽权移交的生命周期收口端到端测试
//!
//! 锚定工单 task/ing/wpubsub-shard-subscription-no-slot-anchor-hang-on-migration：
//! SSUBSCRIBE 订阅表原无槽锚、槽迁移/SETSLOT 全路径零订阅清理钩，槽迁走后
//! 原节点订阅者悬挂饿死无 sunsubscribe 推送（Redis 标准在 SETSLOT 丢槽路径有
//! pubsub.c:pubsubShardUnsubscribeAllChannelsInSlot 收口，§6 已声明向 Redis
//! 标准语义对齐）。本套用例双宿主真 socket 装配锁死收口行为：
//! 1. 主臂：A 节点 SSUBSCRIBE（锚槽 S0）→ CLUSTER SETSLOT S0 NODE B →
//!    A 侧订阅被清、客户端收 sunsubscribe 帧（帧形与 SUNSUBSCRIBE 应答同构），
//!    其后 SPUBLISH 落 A 入口本地零投递；
//! 2. 部分清理正确性：锚他槽（db 1 会话域 S1）的订阅者不受 S0 移交影响，
//!    SPUBLISH 仍投递；
//! 3. 回滚臂零副作用：SETSLOT MIGRATING → SETSLOT STABLE 回滚不触发清理
//!    （两订阅者均零帧）；
//! 4. STABLE 臂零副作用：对未迁移槽 SETSLOT STABLE 零清理。

use std::{sync::Arc, time::Duration};

use aok::Void;
use compio::{net::TcpStream, time::timeout};
use wbase::hash_slot::slot_of;
use wedb::server::{
  cluster_provider::ClusterProvider,
  hash_slot::{HashSlot, SlotState},
  worker::{LOCAL_WORKER_ID, LocalWorkerSpec, NodeRole, Worker},
};
use wedb_test::{cluster_decorate, start_node};
use wnode_test::{cmd, complete_len, fill_until_frame};
use wresp::ext::RespVecExt;

/// 主臂槽（默认会话域 (0,0) 的库级定槽，亦是迁移域门禁唯一可承接槽）
const SLOT0: u16 = slot_of(0, 0);
/// 他槽（db 1 会话域）：部分清理正确性的对照锚
const SLOT1: u16 = slot_of(0, 1);

/// 发布节点 A（失槽源侧）
const NODE_A: u128 = 0xC0FF_EE00_0000_0000_0000_0000_0000_0A01;
/// 槽权移交目标 B
const NODE_B: u128 = 0xC0FF_EE00_0000_0000_0000_0000_0000_0B02;

/// 订阅态读帧的等待上界（订阅推送经 sink 邮箱由连接任务 drain，毫秒级）
const READ_BUDGET: Duration = Duration::from_millis(500);
/// sunsubscribe 帧到达的有界轮询上界
const FRAME_ROUNDS: usize = 40;
/// 无帧断言的静置窗（回滚/他槽臂零副作用的观测窗）：推送经 sink 邮箱毫秒级
/// 到达（READ_BUDGET 注释口径），2 轮 = 1s 观测窗仍有千倍余量；6 轮时本测试
/// 静置断言累计 ~15s 成全量 Top 慢面
const QUIET_ROUNDS: usize = 2;

/// 本地 worker + 对端互指（全槽指派本地 Stable，与 cluster_pubsub_peer_shutdown
/// 同款装配）
fn wire_pair(cp: &ClusterProvider, node_id: u128, own_port: u16, peer_id: u128, peer_port: u16) {
  let cm = cp.cluster_manager().expect("cluster manager 在场");
  let mut config = cm.current_config.write();
  config.initialize_local_worker(LocalWorkerSpec {
    node_id,
    address: "127.0.0.1",
    port: own_port as i32,
    config_epoch: 1,
    role: NodeRole::Primary,
    replica_of_node_id: None,
    hostname: None,
  });
  for slot in config.slot_map.iter_mut() {
    *slot = HashSlot {
      worker_id: LOCAL_WORKER_ID as u16,
      state: SlotState::Stable,
    };
  }
  config.workers.push(Worker {
    nodeid: Some(peer_id),
    address: "127.0.0.1".into(),
    port: peer_port as i32,
    config_epoch: 1,
    role: NodeRole::Primary,
    replica_of_node_id: None,
    replication_offset: 0,
    hostname: None,
  });
}

async fn connect(port: u16) -> TcpStream {
  TcpStream::connect(format!("127.0.0.1:{port}"))
    .await
    .unwrap()
}

/// 有界轮询等待一条完整帧（返回帧字节并从缓冲消费）
async fn next_frame(stream: &mut TcpStream, acc: &mut Vec<u8>) -> Option<Vec<u8>> {
  for _ in 0..FRAME_ROUNDS {
    if timeout(READ_BUDGET, fill_until_frame(stream, acc))
      .await
      .unwrap_or(false)
    {
      let at = complete_len(acc).expect("complete_len 已就绪");
      let frame = acc[..at].to_vec();
      acc.drain(..at);
      return Some(frame);
    }
  }
  None
}

/// 静置窗内必须零帧（回滚/他槽臂零副作用判据）
async fn assert_no_frame(stream: &mut TcpStream, acc: &mut Vec<u8>, ctx: &str) {
  for _ in 0..QUIET_ROUNDS {
    assert!(
      !timeout(READ_BUDGET, fill_until_frame(stream, acc))
        .await
        .unwrap_or(false),
      "{ctx}：静置窗内不得有任何推送帧，实收 {:?}",
      String::from_utf8_lossy(acc)
    );
  }
}

/// 构造 `*3\r\n$<n>\r\n<head>\r\n$<m>\r\n<name>\r\n:<count>\r\n` 订阅族帧
fn tri_frame(head: &[u8], name: &[u8], count: i64) -> Vec<u8> {
  let mut out = Vec::new();
  out.write_resp_array_len(3);
  out.write_resp_bulk_string(head);
  out.write_resp_bulk_string(name);
  out.write_resp_int(count);
  out
}

/// CLUSTER SETSLOT 慢命令（经管理连接）
async fn set_slot(c: &mut TcpStream, slot: u16, state: &[u8], node: Option<u128>) {
  let slot_str = slot.to_string();
  let mut args: Vec<&[u8]> = vec![b"CLUSTER", b"SETSLOT", slot_str.as_bytes(), state];
  let node_hex = node.map(|id| format!("{id:032x}"));
  if let Some(hex) = &node_hex {
    args.push(hex.as_bytes());
  }
  let out = cmd(c, &args).await;
  assert_eq!(out, b"+OK\r\n", "SETSLOT {slot} {state:?} 应答须为 OK");
}

#[compio::test]
async fn shard_subscriptions_revoked_and_notified_on_slot_handover() -> Void {
  // ===== 双宿主真节点：A 失槽源侧，B 槽权目标
  let acp = ClusterProvider::new();
  let (_adir, aserver, _aacp, _awal, aport) =
    start_node(Arc::clone(&acp), cluster_decorate(Arc::clone(&acp)))?;
  let bcp = ClusterProvider::new();
  let (_bdir, bserver, _bacp, _bwal, bport) =
    start_node(Arc::clone(&bcp), cluster_decorate(Arc::clone(&bcp)))?;
  // 收端投递源与会话注册面同一 broker（宿主 boot.rs 同款注入）
  acp.set_pubsub(aserver.session_provider().pubsub.clone());
  bcp.set_pubsub(bserver.session_provider().pubsub.clone());
  wire_pair(&acp, NODE_A, aport, NODE_B, bport);
  wire_pair(&bcp, NODE_B, bport, NODE_A, aport);

  // ===== 订阅者就位：c1 锚 SLOT0（默认域），c2 切 db 1 锚 SLOT1（他槽对照）
  // c3 为 db 1 域发布连接：库级定槽 Slot=slot_of(发布者会话域)（doc/zh/db.md
  // 4.1），SPUBLISH 的槽门随发布者现役库而非订阅者域——admin 恒在 db 0（即
  // SLOT0=槽 0），ch1 发布须经 db 1 连接方能落 SLOT1 域
  let mut c1 = connect(aport).await;
  let mut c2 = connect(aport).await;
  let mut c3 = connect(aport).await;
  let mut admin = connect(aport).await;
  assert_eq!(
    cmd(&mut c1, &[b"SSUBSCRIBE", b"ch0"]).await,
    tri_frame(b"ssubscribe", b"ch0", 1),
    "c1 分片订阅确认帧不符"
  );
  assert_eq!(cmd(&mut c2, &[b"SELECT", b"1"]).await, b"+OK\r\n");
  assert_eq!(
    cmd(&mut c2, &[b"SSUBSCRIBE", b"ch1"]).await,
    tri_frame(b"ssubscribe", b"ch1", 1),
    "c2 分片订阅确认帧不符"
  );
  assert_eq!(cmd(&mut c3, &[b"SELECT", b"1"]).await, b"+OK\r\n");

  // ===== 回滚臂零副作用：MIGRATING → STABLE 回滚不得触发订阅清理
  set_slot(&mut admin, SLOT0, b"MIGRATING", Some(NODE_B)).await;
  set_slot(&mut admin, SLOT0, b"STABLE", None).await;
  let mut acc1 = Vec::new();
  let mut acc2 = Vec::new();
  assert_no_frame(&mut c1, &mut acc1, "MIGRATING→STABLE 回滚臂").await;
  assert_no_frame(&mut c2, &mut acc2, "MIGRATING→STABLE 回滚臂（他槽）").await;

  // ===== STABLE 臂零副作用：对未迁移他槽置 STABLE 零清理
  set_slot(&mut admin, SLOT1, b"STABLE", None).await;
  assert_no_frame(&mut c1, &mut acc1, "SETSLOT STABLE 臂").await;
  assert_no_frame(&mut c2, &mut acc2, "SETSLOT STABLE 臂（他槽）").await;

  // ===== 主臂：SLOT0 权移 B → c1 收 sunsubscribe 帧（帧形与 SUNSUBSCRIBE
  // 应答同构：头 + 裸名 + 剩余计数 0），c2 零帧（锚未动不受牵连）
  set_slot(&mut admin, SLOT0, b"NODE", Some(NODE_B)).await;
  let frame = next_frame(&mut c1, &mut acc1)
    .await
    .expect("槽权移交后锚定订阅者必须收到 sunsubscribe 推送帧");
  assert_eq!(
    frame,
    tri_frame(b"sunsubscribe", b"ch0", 0),
    "强制退订帧须与 SUNSUBSCRIBE 应答同构"
  );
  assert_no_frame(&mut c2, &mut acc2, "槽权移交主臂（他槽订阅者）").await;

  // ===== 订阅确已清：SPUBLISH ch0 撞槽门回 MOVED（槽权已移 B，命令不达本地
  // 投递层；订阅清理已由前述 sunsubscribe 推送帧断言闭环），ch1 经 db 1 域
  // 连接发布（落 SLOT1 未动）仍投递 1
  let moved = cmd(&mut admin, &[b"SPUBLISH", b"ch0", b"v0"]).await;
  assert!(
    moved.starts_with(b"-MOVED "),
    "槽权已移 B，SPUBLISH ch0 须被槽门 MOVED 重定向，实测 {moved:?}"
  );
  assert_eq!(
    cmd(&mut c3, &[b"SPUBLISH", b"ch1", b"v1"]).await,
    b":1\r\n",
    "他槽订阅者不受槽权移交影响"
  );

  // c1 不再收到 ch0 的任何消息帧；c2 正常收到 ch1 投递
  assert_no_frame(&mut c1, &mut acc1, "清理后 ch0 再投递").await;
  let msg = next_frame(&mut c2, &mut acc2)
    .await
    .expect("他槽订阅者必须正常收到投递");
  let mut want = Vec::new();
  want.write_resp_array_len(3);
  want.write_resp_bulk_string(b"smessage");
  want.write_resp_bulk_string(b"ch1");
  want.write_resp_bulk_string(b"v1");
  assert_eq!(msg, want, "ch1 投递帧不符");

  // ===== 自移不移：槽权移交回本节点不得清 B 侧（或本地）锚订阅——对 c2
  // 锚槽做本地节点自移（SLOT1 属 A，NODE A 即自移），订阅不清理
  set_slot(&mut admin, SLOT1, b"NODE", Some(NODE_A)).await;
  assert_eq!(
    cmd(&mut c3, &[b"SPUBLISH", b"ch1", b"v2"]).await,
    b":1\r\n",
    "自移不移：目标即本地节点不得清订阅"
  );
  let msg2 = next_frame(&mut c2, &mut acc2)
    .await
    .expect("自移后订阅仍存活并正常投递");
  let mut want2 = Vec::new();
  want2.write_resp_array_len(3);
  want2.write_resp_bulk_string(b"smessage");
  want2.write_resp_bulk_string(b"ch1");
  want2.write_resp_bulk_string(b"v2");
  assert_eq!(msg2, want2, "自移后投递帧不符");

  drop((c1, c2, c3, admin));
  // 收尾：先停投递面的连接仓库再拆宿主（cluster_pubsub_peer_shutdown 同款收口序）
  acp.gossip_manager().expect("gossip manager 在场").dispose();
  bcp.gossip_manager().expect("gossip manager 在场").dispose();
  aserver.dispose();
  bserver.dispose();
  aok::OK
}
