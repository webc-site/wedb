//! REPLICAOF 互指环角色门集成测试（票 wedb-repl-mutual-replicaof-ring-no-receive-role-gate）
//!
//! C# 同源缺陷（ClusterManagerWorkerState.cs:TryAddReplicaAsync 三门只裁直接
//! 自指与本地已知套娃，PrimarySync.cs 承接链零反向校验）：A↔B 互指
//! REPLICAOF 经 gossip 陈旧窗成交、flush_config 持久化固化后，分片双主互推
//! 位点乒乓、无可写主。本面四点收口回归：
//! - 承接面角色门：副本同步承接两入口（CLUSTER INITIATE_REPLICA_SYNC /
//!   CLUSTER ATTACH_SYNC 主端支）本端非 Primary 即拒（ReplicateTargetNotPrimary
//!   族文案），主角色不误伤；
//! - 翻转臂一跳环判：try_add_replica_async 沿目标 primary 链回溯命中己身即拒
//!   （同错误族），合法单套娃与升主后重挂不回退；
//! - 持久互指配置重启臂：双方 start_replication_attach 显式失败，零复制流
//!   建立，不再乒乓；
//! - gossip 停摆窗竞态环：次路 REPLICAOF 前台发起被对端承接门 -ERR 拒，
//!   发起侧经 AllowReplicaResetOnFailure 回滚臂复位 Primary，环拆单向。

use std::{num::NonZeroUsize, sync::Arc, time::Duration};

use compio::{runtime::Runtime, time::sleep};
use waof::AofAddress;
use wbase::hex::hex_str_u128;
use wdev::SegmentedDevice;
use wedb::{
  error::Error,
  server::{
    cluster_provider::ClusterProvider,
    replication::{
      assembly::{try_replicate_sync_async, wire_replication_data_plane},
      checkpoint_entry::CheckpointEntry,
      replicate_sync_options::ReplicateSyncOptions,
      sync_metadata::SyncMetadata,
    },
    worker::{LocalWorkerSpec, NodeRole, Worker},
  },
};
use wedb_test::{
  cluster_consumer::cluster_consumer,
  node_storage::{open_node, provider_with_role},
  replica_host::replica_host,
};
use wnode::{MessageConsumerFace, RespSessionConsumer};
use wtest_base::{resp_frame, wait_for};

/// 测试节点身份（内部 u128；协议面渲染 32 字符小写 hex）
const NODE_A: u128 = 0x0DE1_0000_0000_0000_0000_0000_0000_00A1;
const NODE_B: u128 = 0x0DE1_0000_0000_0000_0000_0000_0000_00A2;
const NODE_C: u128 = 0x0DE1_0000_0000_0000_0000_0000_0000_00A3;
const NODE_D: u128 = 0x0DE1_0000_0000_0000_0000_0000_0000_00A4;

/// 承接门 -ERR 应答帧（ReplicateTargetNotPrimary 族文案，副本域单源
/// cluster_err_text；括号内为承接方即本端节点 id——来方视角「试图复制
/// 该节点而其非主」）
fn accept_gate_err(local_id: u128) -> Vec<u8> {
  format!(
    "-ERR Trying to replicate node ({}) that is not a primary.\r\n",
    hex_str_u128(local_id)
  )
  .into_bytes()
}

/// 同步段消费应答（两入口角色门为同步拒臂，无慢路径挂起）
fn pump(consumer: &mut RespSessionConsumer, frame: &[u8]) -> Vec<u8> {
  let mut scratch = consumer.take_recv_scratch();
  scratch.extend_from_slice(frame);
  consumer.return_recv_scratch(scratch);
  let mut out = Vec::new();
  let remaining = consumer.try_consume_messages_into(&mut out);
  assert_eq!(remaining, Some(0), "帧应被完整消费");
  out
}

/// 视图内追加 peer worker 条目（互指夹具的对端位）
fn push_peer(
  provider: &Arc<ClusterProvider>,
  peer: u128,
  port: i32,
  role: NodeRole,
  replica_of: Option<u128>,
) {
  provider
    .cluster_manager()
    .unwrap()
    .current_config
    .write()
    .workers
    .push(Worker {
      nodeid: Some(peer),
      address: "127.0.0.1".into(),
      port,
      config_epoch: 1,
      role,
      replica_of_node_id: replica_of,
      replication_offset: 0,
      hostname: None,
    });
}

/// 修订 peer 端口（真服务器起监听后回填，出站 attach 取配置端点）
fn patch_peer_port(provider: &Arc<ClusterProvider>, peer: u128, port: i32) {
  let cm = provider.cluster_manager().unwrap();
  let mut config = cm.current_config.write();
  if let Some(idx) = config.workers.iter().position(|w| w.nodeid == Some(peer)) {
    config.workers[idx].port = port;
  }
}

/// 装配复制数据面（对标宿主 wire_replication_data_plane 调用点：主端推流
/// 资产在位，承接门若被移除则承接链深入资产面——保证用例对门删除断感）
fn wire_data_plane(provider: &Arc<ClusterProvider>, wal: &Arc<waof::WalLog<SegmentedDevice>>) {
  wire_replication_data_plane(provider, Arc::clone(wal));
}

/// 本端为副本态（互指固化配置形态）时，副本同步承接两入口都必须被角色门
/// 以 ReplicateTargetNotPrimary 族文案拒绝
#[compio::test]
async fn accept_faces_reject_when_local_not_primary() {
  let node = open_node("ring_accept_replica");
  // 本端 A：角色 Replica、复制源 B（互指环任一方的持久化形态）
  let provider = provider_with_role(&node, NODE_A, 7161, NodeRole::Replica, NODE_B, false, None);
  let mut consumer = cluster_consumer(&provider);

  // INITIATE_REPLICA_SYNC（磁盘基承接入口）：角色门先于载荷解析与资产取用
  let frame = resp_frame(&[
    b"CLUSTER",
    b"INITIATE_REPLICA_SYNC",
    hex_str_u128(NODE_B).as_bytes(),
    b"assigned-primary-repl-id",
    &CheckpointEntry::with_sublogs(1).to_byte_array(),
    &0i64.to_le_bytes(),
    &0i64.to_le_bytes(),
  ]);
  let resp = pump(&mut consumer, &frame);
  assert_eq!(
    resp,
    accept_gate_err(NODE_A),
    "副本态承接 INITIATE 必须被角色门拒绝"
  );

  // ATTACH_SYNC 主端支（origin_node_role=Replica，无盘承接入口）：同门同文案
  let meta = SyncMetadata {
    full_sync: true,
    origin_node_role: NodeRole::Replica,
    origin_node_id: NODE_B,
    current_primary_repl_id: "assigned-primary-repl-id".into(),
    current_store_version: 0,
    current_aof_begin_address: AofAddress::create(1, 0),
    current_aof_tail_address: AofAddress::create(1, 0),
    checkpoint_entry: None,
  };
  let frame = resp_frame(&[b"CLUSTER", b"ATTACH_SYNC", &meta.to_byte_array()]);
  let resp = pump(&mut consumer, &frame);
  assert_eq!(
    resp,
    accept_gate_err(NODE_A),
    "副本态承接 ATTACH_SYNC 主端支必须被角色门拒绝"
  );
}

/// 主角色不误伤：合法主承接同样两入口，角色门放行（后续因夹具未注入主端
/// 推流资产而报 Cluster not initialized——与角色门文案判然有别）
#[compio::test]
async fn accept_faces_allow_primary_role() {
  let node = open_node("ring_accept_primary");
  let provider = provider_with_role(&node, NODE_A, 7162, NodeRole::Primary, NODE_A, false, None);
  let mut consumer = cluster_consumer(&provider);

  let frame = resp_frame(&[
    b"CLUSTER",
    b"INITIATE_REPLICA_SYNC",
    hex_str_u128(NODE_B).as_bytes(),
    b"assigned-primary-repl-id",
    &CheckpointEntry::with_sublogs(1).to_byte_array(),
    &0i64.to_le_bytes(),
    &0i64.to_le_bytes(),
  ]);
  let resp = pump(&mut consumer, &frame);
  assert_eq!(
    resp, b"-ERR Cluster not initialized\r\n",
    "主角色承接不得被角色门拦截（缺资产臂接管）"
  );

  let meta = SyncMetadata {
    full_sync: true,
    origin_node_role: NodeRole::Replica,
    origin_node_id: NODE_B,
    current_primary_repl_id: "assigned-primary-repl-id".into(),
    current_store_version: 0,
    current_aof_begin_address: AofAddress::create(1, 0),
    current_aof_tail_address: AofAddress::create(1, 0),
    checkpoint_entry: None,
  };
  let frame = resp_frame(&[b"CLUSTER", b"ATTACH_SYNC", &meta.to_byte_array()]);
  let resp = pump(&mut consumer, &frame);
  assert_eq!(
    resp, b"-ERR Cluster not initialized\r\n",
    "主角色 ATTACH_SYNC 主端支不得被角色门拦截"
  );
}

/// 翻转臂一跳环判：目标 primary 链回溯命中己身即拒（直接互指与跨节点环），
/// 链不归己的陈旧视图放行；拒绝路径零配置翻转
#[test]
fn flip_arm_rejects_cycle_in_local_view() {
  Runtime::new().unwrap().block_on(async {
    // 直接环：B 已翻为 A 的副本（A 视图已收敛），A 再 REPLICAOF B 即成环
    let provider = ClusterProvider::new();
    let cm = provider.cluster_manager().unwrap();
    {
      let mut config = cm.current_config.write();
      config.initialize_local_worker(LocalWorkerSpec {
        node_id: NODE_A,
        address: "127.0.0.1",
        port: 7163,
        config_epoch: 1,
        role: NodeRole::Primary,
        replica_of_node_id: None,
        hostname: None,
      });
      config.workers.push(Worker {
        nodeid: Some(NODE_B),
        address: "127.0.0.1".into(),
        port: 7164,
        config_epoch: 1,
        role: NodeRole::Replica,
        replica_of_node_id: Some(NODE_A),
        replication_offset: 0,
        hostname: None,
      });
    }
    let err = cm
      .try_add_replica_async(NODE_B, true, false)
      .await
      .expect_err("互指环翻转必须被拒绝");
    assert!(
      matches!(err, Error::ReplicateTargetNotPrimary(ref id) if *id == hex_str_u128(NODE_B)),
      "环判必须落在 ReplicateTargetNotPrimary 族: {err}"
    );
    {
      let config = cm.current_config.read();
      assert_eq!(
        config.local_node_role(),
        NodeRole::Primary,
        "拒绝路径不得翻转角色"
      );
      assert_eq!(config.local_node_primary_id(), None, "拒绝路径不得落复制源");
    }

    // 跨节点环（gossip 陈旧窗视图：role 仍 Primary 而 replica_of 链已指回本端）
    // A 视图：B(Primary)→C(Replica of A)，REPLICAOF B 回溯 B→C→A 命中己身
    let provider = ClusterProvider::new();
    let cm = provider.cluster_manager().unwrap();
    {
      let mut config = cm.current_config.write();
      config.initialize_local_worker(LocalWorkerSpec {
        node_id: NODE_A,
        address: "127.0.0.1",
        port: 7163,
        config_epoch: 1,
        role: NodeRole::Primary,
        replica_of_node_id: None,
        hostname: None,
      });
      config.workers.push(Worker {
        nodeid: Some(NODE_B),
        address: "127.0.0.1".into(),
        port: 7164,
        config_epoch: 1,
        role: NodeRole::Primary,
        replica_of_node_id: Some(NODE_C),
        replication_offset: 0,
        hostname: None,
      });
      config.workers.push(Worker {
        nodeid: Some(NODE_C),
        address: "127.0.0.1".into(),
        port: 7165,
        config_epoch: 1,
        role: NodeRole::Replica,
        replica_of_node_id: Some(NODE_A),
        replication_offset: 0,
        hostname: None,
      });
    }
    let err = cm
      .try_add_replica_async(NODE_B, true, false)
      .await
      .expect_err("跨节点环翻转必须被拒绝");
    assert!(
      matches!(err, Error::ReplicateTargetNotPrimary(ref id) if *id == hex_str_u128(NODE_B)),
      "跨节点环同落一族文案: {err}"
    );

    // 链不归己：B→C→D（D 无复制源），回溯不命中本端即放行（合法链式视图）
    let provider = ClusterProvider::new();
    let cm = provider.cluster_manager().unwrap();
    {
      let mut config = cm.current_config.write();
      config.initialize_local_worker(LocalWorkerSpec {
        node_id: NODE_A,
        address: "127.0.0.1",
        port: 7163,
        config_epoch: 1,
        role: NodeRole::Primary,
        replica_of_node_id: None,
        hostname: None,
      });
      config.workers.push(Worker {
        nodeid: Some(NODE_B),
        address: "127.0.0.1".into(),
        port: 7164,
        config_epoch: 1,
        role: NodeRole::Primary,
        replica_of_node_id: Some(NODE_C),
        replication_offset: 0,
        hostname: None,
      });
      config.workers.push(Worker {
        nodeid: Some(NODE_C),
        address: "127.0.0.1".into(),
        port: 7165,
        config_epoch: 1,
        role: NodeRole::Primary,
        replica_of_node_id: Some(NODE_D),
        replication_offset: 0,
        hostname: None,
      });
      config.workers.push(Worker {
        nodeid: Some(NODE_D),
        address: "127.0.0.1".into(),
        port: 7166,
        config_epoch: 1,
        role: NodeRole::Primary,
        replica_of_node_id: None,
        replication_offset: 0,
        hostname: None,
      });
    }
    cm.try_add_replica_async(NODE_B, true, false)
      .await
      .expect("链不归己的陈旧视图不得误拒");
    assert_eq!(
      cm.current_config.read().local_node_primary_id(),
      Some(NODE_B),
      "放行路径必须完成翻转"
    );
  });
}

/// 合法形态不回退：单套娃 A→B（B 为无复制源的独立主）翻转成功；接管升主后
/// 的节点（视图内 role=Primary 且复制源已清）可再被挂靠——承接门与环判对
/// 「升主重挂」零误伤
#[test]
fn legal_single_nesting_and_promoted_reattach_pass() {
  Runtime::new().unwrap().block_on(async {
    // 单套娃：A 复制自独立主 B
    let provider = ClusterProvider::new();
    let cm = provider.cluster_manager().unwrap();
    {
      let mut config = cm.current_config.write();
      config.initialize_local_worker(LocalWorkerSpec {
        node_id: NODE_A,
        address: "127.0.0.1",
        port: 7167,
        config_epoch: 1,
        role: NodeRole::Primary,
        replica_of_node_id: None,
        hostname: None,
      });
      config.workers.push(Worker {
        nodeid: Some(NODE_B),
        address: "127.0.0.1".into(),
        port: 7168,
        config_epoch: 1,
        role: NodeRole::Primary,
        replica_of_node_id: None,
        replication_offset: 0,
        hostname: None,
      });
    }
    cm.try_add_replica_async(NODE_B, true, false)
      .await
      .expect("单套娃翻转必须放行");
    assert_eq!(
      cm.current_config.read().local_node_primary_id(),
      Some(NODE_B)
    );

    // 接管升主后重挂：B 曾为副本、升主后复制源已清（视图形态与独立主同），
    // 环判谓词不得因历史形态误判（新夹具即升主后的收敛视图）
    let provider = ClusterProvider::new();
    let cm = provider.cluster_manager().unwrap();
    {
      let mut config = cm.current_config.write();
      config.initialize_local_worker(LocalWorkerSpec {
        node_id: NODE_A,
        address: "127.0.0.1",
        port: 7167,
        config_epoch: 1,
        role: NodeRole::Primary,
        replica_of_node_id: None,
        hostname: None,
      });
      config.workers.push(Worker {
        nodeid: Some(NODE_B),
        address: "127.0.0.1".into(),
        port: 7168,
        config_epoch: 1,
        role: NodeRole::Primary,
        replica_of_node_id: None,
        replication_offset: 0,
        hostname: None,
      });
      config.workers.push(Worker {
        nodeid: Some(NODE_C),
        address: "127.0.0.1".into(),
        port: 7169,
        config_epoch: 1,
        role: NodeRole::Primary,
        replica_of_node_id: None,
        replication_offset: 0,
        hostname: None,
      });
    }
    cm.try_add_replica_async(NODE_B, true, false)
      .await
      .expect("升主后重挂必须放行");
  });
}

/// 持久互指配置重启臂：双方 role=Replica 且互指（flush_config 固化形态），
/// 双双 start_replication_attach 各被对端承接门显式拒绝——零复制流建立、
/// 角色不再翻转，杜绝重启后互推乒乓
#[compio::test]
async fn restart_mutual_config_attach_fails_without_pingpong() {
  // ===== A：副本（复制源 B），真服务器承接
  let node_a = open_node("ring_boot_a");
  let provider_a = provider_with_role(&node_a, NODE_A, 0, NodeRole::Replica, NODE_B, false, None);
  push_peer(&provider_a, NODE_B, 0, NodeRole::Replica, Some(NODE_A));
  wire_data_plane(&provider_a, &node_a.wal);
  let (server_a, addr_a) = replica_host(&provider_a, NonZeroUsize::new(1));
  let port_a: i32 = addr_a.rsplit(':').next().unwrap().parse().unwrap();

  // ===== B：副本（复制源 A），配置端点指向 A 真监听口
  let node_b = open_node("ring_boot_b");
  let provider_b = provider_with_role(&node_b, NODE_B, 0, NodeRole::Replica, NODE_A, false, None);
  push_peer(&provider_b, NODE_A, port_a, NodeRole::Replica, Some(NODE_B));
  wire_data_plane(&provider_b, &node_b.wal);
  let (server_b, addr_b) = replica_host(&provider_b, NonZeroUsize::new(1));
  let port_b: i32 = addr_b.rsplit(':').next().unwrap().parse().unwrap();
  patch_peer_port(&provider_a, NODE_B, port_b);

  // ===== 重启臂：双方各按持久化互指配置发起 attach（对标 Start 臂）
  provider_a.set_recover(true);
  provider_b.set_recover(true);
  provider_a.start_replication_attach();
  provider_b.start_replication_attach();

  // ===== 稳定窗：双方显式失败（零流、角色保持副本），双采样证不再乒乓
  let rm_a = provider_a.replication_manager().unwrap();
  let rm_b = provider_b.replication_manager().unwrap();
  let role_a = || {
    provider_a
      .cluster_manager()
      .unwrap()
      .current_config()
      .local_node_role()
  };
  let role_b = || {
    provider_b
      .cluster_manager()
      .unwrap()
      .current_config()
      .local_node_role()
  };
  assert!(
    wait_for(
      || !rm_a.has_active_replication_stream() && !rm_b.has_active_replication_stream(),
      Duration::from_secs(5),
    )
    .await,
    "互指重启臂双侧承接门必须拒绝建流"
  );
  sleep(Duration::from_millis(400)).await;
  assert!(
    !rm_a.has_active_replication_stream(),
    "A 侧不得乒乓建立反向流"
  );
  assert!(
    !rm_b.has_active_replication_stream(),
    "B 侧不得乒乓建立反向流"
  );
  assert_eq!(
    role_a(),
    NodeRole::Replica,
    "失败臂（AllowReplicaResetOnFailure:false）不得翻转角色"
  );
  assert_eq!(role_b(), NodeRole::Replica, "失败臂不得翻转角色");

  server_a.dispose();
  server_b.dispose();
}

/// gossip 停摆窗竞态环：A 已翻为 B 的副本，B 视图内 A 仍为 Primary（陈旧），
/// B 执行 REPLICAOF A（次路翻转）后向 A 发起 attach——A 承接门 -ERR 拒，
/// 发起侧经 AllowReplicaResetOnFailure 回滚臂复位 Primary，环拆单向不再互推
#[compio::test]
async fn race_ring_second_initiate_rejected_and_rolls_back() {
  // ===== A：已翻副本（复制源 B，持久化形态），真服务器承接
  let node_a = open_node("ring_race_a");
  let provider_a = provider_with_role(&node_a, NODE_A, 0, NodeRole::Replica, NODE_B, false, None);
  wire_data_plane(&provider_a, &node_a.wal);
  let (server_a, addr_a) = replica_host(&provider_a, NonZeroUsize::new(1));
  let port_a: i32 = addr_a.rsplit(':').next().unwrap().parse().unwrap();

  // ===== B：主（视图内 A 仍 Primary、无复制源——gossip 停摆窗陈旧态）
  let node_b = open_node("ring_race_b");
  let provider_b = provider_with_role(&node_b, NODE_B, 0, NodeRole::Primary, NODE_B, false, None);
  push_peer(&provider_b, NODE_A, port_a, NodeRole::Primary, None);

  // ===== B 次路 REPLICAOF A（命令面同参：前台 Force/TryAddReplica/
  // AllowReplicaResetOnFailure:true，UpgradeLock:false）：翻转过三门
  //（陈旧视图），attach 承接被 A 角色门拒绝，回滚臂复位主
  let opts = ReplicateSyncOptions::new(NODE_A, false, true, true, true, false);
  let result = try_replicate_sync_async(&provider_b, opts).await;
  let err = result.expect_err("竞态环次路 attach 必须被承接门拒绝");
  let gate_text = accept_gate_err(NODE_A);
  let gate_text = String::from_utf8(gate_text[1..gate_text.len() - 2].to_vec()).unwrap();
  assert!(
    err.to_string().contains(&gate_text),
    "发起侧必须收到承接门 -ERR 文案: {err}"
  );

  // ===== 发起侧回滚为 Primary（环拆单向），A 保持 B 的副本
  let cm_b = provider_b.cluster_manager().unwrap();
  let config_b = cm_b.current_config();
  assert_eq!(
    config_b.local_node_role(),
    NodeRole::Primary,
    "AllowReplicaResetOnFailure 回滚臂必须复位主角色"
  );
  assert_eq!(config_b.local_node_primary_id(), None, "回滚必须清复制源");
  let cm_a = provider_a.cluster_manager().unwrap();
  let config_a = cm_a.current_config();
  assert_eq!(config_a.local_node_role(), NodeRole::Replica);
  assert_eq!(config_a.local_node_primary_id(), Some(NODE_B));

  server_a.dispose();
}
