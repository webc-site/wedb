//! gossip 主循环 panic 终局闭环集成测试（案一：事件重拉接线 + 终局臂拆池
//! 收口）：测面直调 Err 臂本体（wbase supervise_task panic 后 Err 臂执行的
//! 即 GossipManager::dispose 单点——复位 is_running 幂等启动位 + 拆连接池，
//! 对标 C# GossipMainAsync finally 无条件拆池 Gossip.cs:369 →
//! GarnetClusterConnectionStore.Dispose），再经真实 RESP 会话帧驱动 CLUSTER
//! MEET 发起 / gossip 入站两类监控事件到达，断言主循环在下一安全点重拉、
//! 连接池起池重建、广播再服务（全程真实 TCP 假端点，无 mock）
#[path = "common/cluster_consumer_fresh.rs"]
mod cluster_cc_fresh;

use std::{
  sync::{Arc, atomic::Ordering},
  time::Duration,
};

use aok::Void;
use cluster_cc_fresh::cluster_consumer;
use compio::runtime::Runtime;
use wedb::server::{
  cluster_provider::ClusterProvider,
  gossip::{gossip_manager::GossipManager, node_connection::NodeConnection},
  worker::{LocalWorkerSpec, NodeRole, Worker},
};
use wnode::{MessageConsumerFace, RespSessionConsumer};
use wtest_base::{GossipNode, resp_frame_str, wait_for};

/// 配置在册远端 worker 的节点 id（地址指向 gossip 假端点）
const REMOTE_ID: u128 = 0x2E71;
/// 入站 gossip 帧携带的陌生对端节点 id（WITHMEET 显式信任合并）
const INBOUND_ID: u128 = 0x3E71;
/// 扇出入池探针节点 id（不在配置内，验证池闸门开合，仅假端点接受连接）
const FANOUT_ID: u128 = 0x9999;

/// 初始化本地 worker（worker 1）的提供者，对标 C# 测试基建的最小装配
fn provider_with_local(node_id: u128, port: i32) -> Arc<ClusterProvider> {
  let cp = ClusterProvider::new();
  let cm = cp.cluster_manager().unwrap();
  cm.init_local("127.0.0.1", port, false, "");
  let mut config = cm.current_config.write();
  config.initialize_local_worker(LocalWorkerSpec {
    node_id,
    address: "127.0.0.1",
    port,
    config_epoch: 1,
    role: NodeRole::Primary,
    replica_of_node_id: None,
    hostname: None,
  });
  drop(config);
  cp
}

/// 单命令往返（直填会话接收缓冲 → 唯一入口 → 应答取出）
fn pump(consumer: &mut RespSessionConsumer, frame: &[u8]) -> Vec<u8> {
  let mut scratch = consumer.take_recv_scratch();
  scratch.extend_from_slice(frame);
  consumer.return_recv_scratch(scratch);
  let mut resp = Vec::new();
  let remaining = consumer.try_consume_messages_into(&mut resp);
  assert_eq!(remaining, Some(0), "帧应被完整消费: {frame:?}");
  resp
}

/// 逐字节构造带二进制载荷的 RESP 帧（配置载荷禁经 char 有损转换）
fn binary_frame(parts: &[&[u8]]) -> Vec<u8> {
  let mut frame = format!("*{}\r\n", parts.len()).into_bytes();
  for part in parts {
    frame.extend_from_slice(format!("${}\r\n", part.len()).as_bytes());
    frame.extend_from_slice(part);
    frame.extend_from_slice(b"\r\n");
  }
  frame
}

/// 装配：快周期 gossip 提供者 + 已登记远端 worker（指向假端点）+ 起环并等
/// 待主循环真实建连与广播服务
async fn start_serving(cp: &Arc<ClusterProvider>, fake_port: u16) {
  cp.set_gossip_delay_ms(200);
  let cm = cp.cluster_manager().unwrap();
  {
    let mut config = cm.current_config.write();
    config.workers.push(Worker {
      nodeid: Some(REMOTE_ID),
      address: "127.0.0.1".into(),
      port: fake_port as i32,
      config_epoch: 1,
      role: NodeRole::Primary,
      replica_of_node_id: None,
      replication_offset: 0,
      hostname: None,
    });
  }
  let gm = cp.gossip_manager().unwrap();
  assert!(!gm.is_running(), "装配期环未自启");
  cm.start();
  assert!(gm.is_running(), "start 应点亮 gossip 主循环");
  assert!(
    wait_for(
      || gm
        .connection_store
        .get_connection(REMOTE_ID)
        .is_some_and(|c| c.is_connected()),
      Duration::from_secs(5)
    )
    .await,
    "主循环建连补员应对假端点建立活跃连接"
  );
  let before = gm.stats.gossip_success_count.load(Ordering::Acquire);
  assert!(
    wait_for(
      || gm.stats.gossip_success_count.load(Ordering::Acquire) > before,
      Duration::from_secs(5)
    )
    .await,
    "主循环应持续广播派发（服务态）"
  );
}

/// Err 臂本体直调（票面测面直调臂形态：supervise_task panic 展开后 Err 臂
/// 执行的即本 dispose 单点——复位幂等启动位 + 拆池）后核验终局契约
fn terminate_and_assert_takedown(
  gm: &Arc<GossipManager>,
  cp: &Arc<ClusterProvider>,
  served: &Arc<NodeConnection>,
  fake_port: u16,
) {
  gm.dispose();
  assert!(!gm.is_running(), "环死：幂等启动位复位");
  assert!(
    served.disposed.load(Ordering::Acquire),
    "拆池收口：环内旧连接实例必须随环断开（对标 C# finally 逐连接 Dispose）"
  );
  assert!(
    gm.connection_store.get_connection(REMOTE_ID).is_none(),
    "拆池收口：池内不得残留环死连接"
  );
  // 共有断言：环死窗口扇出（publish/flushall get_or_add 同源入口）不再向
  // 死池塞入连接、MEET 交接 add_connection 恒拒
  gm.connection_store
    .get_or_add(FANOUT_ID, "127.0.0.1", fake_port as i32, cp);
  assert_eq!(gm.connection_store.count(), 0, "环死窗口扇出不得使池增长");
  assert!(
    !gm
      .connection_store
      .add_connection(Arc::new(NodeConnection::new(
        INBOUND_ID,
        "127.0.0.1".into(),
        fake_port as i32,
        cp,
      ))),
    "已拆池应拒绝 MEET 交接"
  );
}

/// CLUSTER MEET 事件重拉闭环：真实广播服务 → Err 臂本体收口（复位 + 拆池，
/// 扇出恒拒）→ 常驻 runtime 内真实会话发起 CLUSTER MEET → 处理器 spawn 前
/// try_start_gossip_tasks 重拉 → 起池、建连补员重建、广播再服务、扇出恢复
/// 入池
#[test]
fn cluster_meet_event_repulls_panic_terminated_loop() -> Void {
  Runtime::new().unwrap().block_on(async {
    // 空应答靶端：接受 TCP 并对 gossip 帧回空 bulk（不扰动配置合并判定）
    let fake = GossipNode::bind(Arc::new(Vec::new())).await;
    let cp = provider_with_local(0x10CA1, 7001);
    let cm = cp.cluster_manager().unwrap();
    let gm = cp.gossip_manager().unwrap();
    let mut consumer = cluster_consumer(&cp, "gossip_repull.db");

    start_serving(&cp, fake.port()).await;
    let served = gm.connection_store.get_connection(REMOTE_ID).unwrap();

    terminate_and_assert_takedown(&gm, &cp, &served, fake.port());

    // 真实事件：会话帧发起 CLUSTER MEET（block_on 内 Runtime::try_current
    // 命中 runtime 臂 → 重拉行同步执行于 spawn 前）
    let out = pump(
      &mut consumer,
      &resp_frame_str(&["CLUSTER", "MEET", "127.0.0.1", &fake.port().to_string()]),
    );
    assert_eq!(out, b"+OK\r\n");
    assert!(
      gm.is_running(),
      "MEET 事件应在下一安全点重拉主循环（is_running 回真）"
    );

    // 再服务：起池后建连补员重建活跃连接、广播恢复派发
    assert!(
      wait_for(
        || gm
          .connection_store
          .get_connection(REMOTE_ID)
          .is_some_and(|c| c.is_connected()),
        Duration::from_secs(5)
      )
      .await,
      "重拉后应恢复对假端点的活跃连接"
    );
    let before = gm.stats.gossip_success_count.load(Ordering::Acquire);
    assert!(
      wait_for(
        || gm.stats.gossip_success_count.load(Ordering::Acquire) > before,
        Duration::from_secs(5)
      )
      .await,
      "重拉后广播派发应恢复（再服务）"
    );
    // 闸门已开：扇出入池恢复受理（revive 生效的可观测面）
    gm.connection_store
      .get_or_add(FANOUT_ID, "127.0.0.1", fake.port() as i32, &cp);
    assert!(
      gm.connection_store.contains(FANOUT_ID),
      "重拉起池后扇出应重新入池"
    );
    assert!(cm.current_config().is_known(REMOTE_ID), "在册节点保持已知");
    gm.dispose();
    aok::OK
  })
}

/// 入站 gossip 事件重拉闭环：CLUSTER GOSSIP WITHMEET 帧（真实二进制配置
/// 载荷）由网络泵驱动慢路径——合并段落地陌生节点后 try_start_gossip_tasks
/// 重拉主循环，断言 is_running 回真、合并生效、广播再服务
#[test]
fn inbound_gossip_event_repulls_panic_terminated_loop() -> Void {
  Runtime::new().unwrap().block_on(async {
    let fake = GossipNode::bind(Arc::new(Vec::new())).await;
    let cp = provider_with_local(0x10CA1, 7001);
    let gm = cp.gossip_manager().unwrap();
    let mut consumer = cluster_consumer(&cp, "gossip_repull.db");

    start_serving(&cp, fake.port()).await;
    let served = gm.connection_store.get_connection(REMOTE_ID).unwrap();

    // 对端提供者：其自配置声明陌生节点 INBOUND_ID
    let peer = provider_with_local(INBOUND_ID, 7003);
    let peer_bytes = peer
      .cluster_manager()
      .unwrap()
      .current_config()
      .to_byte_array();

    terminate_and_assert_takedown(&gm, &cp, &served, fake.port());

    // 真实事件：入站 CLUSTER GOSSIP WITHMEET 帧——同步段登记慢路径续段，
    // resolve 由常驻 runtime 驱动（cluster_gossip_slow 合并段后重拉）
    let frame = binary_frame(&[b"CLUSTER", b"GOSSIP", b"WITHMEET", &peer_bytes]);
    let mut out = pump(&mut consumer, &frame);
    let slow = consumer.take_slow_wait().expect("GOSSIP 应登记慢路径");
    out.extend_from_slice(&slow.resolve().await);
    assert!(
      out.starts_with(b"$"),
      "WITHMEET 应回配置 bulk 帧，实得 {out:?}"
    );

    assert!(
      gm.is_running(),
      "入站 gossip 事件应在下一安全点重拉主循环（is_running 回真）"
    );
    assert!(
      cp.cluster_manager()
        .unwrap()
        .current_config()
        .is_known(INBOUND_ID),
      "合并段应先于重拉落地陌生节点"
    );

    // 再服务：建连补员重建 + 广播恢复派发
    assert!(
      wait_for(
        || gm
          .connection_store
          .get_connection(REMOTE_ID)
          .is_some_and(|c| c.is_connected()),
        Duration::from_secs(5)
      )
      .await,
      "重拉后应恢复对假端点的活跃连接"
    );
    let before = gm.stats.gossip_success_count.load(Ordering::Acquire);
    assert!(
      wait_for(
        || gm.stats.gossip_success_count.load(Ordering::Acquire) > before,
        Duration::from_secs(5)
      )
      .await,
      "重拉后广播派发应恢复（再服务）"
    );
    gm.dispose();
    aok::OK
  })
}
