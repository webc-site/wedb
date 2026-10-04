//! 集群跨节点 PUBLISH 在 gossip 对端关停后的存活/恢复端到端测试
//!
//! 对标 test/cluster/Garnet.test.cluster/ClusterPubSubForwardTests.cs:50-158
//! `ClusterPublishSurvivesPeerNodeShutdown`（issues #1928 TryClusterPublish 空引用
//! 与 #1929 gossip client 释放级联的回归面）。装配为真双宿主节点
//!（wedb_test::start_node 生产形态 + 互指真实监听端点）：B 侧真 socket 订阅、
//! A 侧真 socket 发布——
//! ① 双侧在线时 A 的 PUBLISH 须经 gossip 连接转发并在 B 的订阅端逐字节投递
//!    （C# :85-104 基线投递臂）；
//! ② A 侧对 B 的转发连接点亮为在册实连（投递确经真 socket，非桩应答）；
//! ③ 关停 B 后（C# :110 ShutdownNode）在 A 持续 PING/PUBLISH：幸存端始终应答、
//!    PUBLISH 恒回本地计数帧，且对 B 的转发连接收敛为断开，断开之后再计连续
//!    成功次数（C# :112-157 恢复臂与 :157 末态 sanity）；
//! ④ 失效转发的行为凭据取「跳过转发」留痕（log_capture 单源）——rust 侧对端
//!    已断开时的转发既不抛出也不挂起，仅应答面无法区分，故以留痕钉死该臂。
//!
//! 与 C# 的判据差异申报：C# 以「4 秒静置墙钟窗 + 15 秒内累计 8 次成功」为恢复
//! 信号，且依赖转发抛出异常这一可见错误面；rust 侧转发是发布端 detached 任务、
//! 无抛出面（未连接即跳过转发，见 node_connection::try_cluster_publish_async），
//! 故静置信号改取「A 观测到对 B 的 gossip 连接转为断开」这一状态谓词，其后
//! 再断言连续成功。全臂零墙钟敏感断言，仅以有界重试与末态谓词收口。

use std::{sync::Arc, time::Duration};

use aok::Void;
use compio::{
  buf::BufResult,
  io::AsyncRead,
  net::TcpStream,
  time::{sleep, timeout},
};
use wedb::server::{
  cluster_provider::ClusterProvider,
  hash_slot::{HashSlot, SlotState},
  worker::{LOCAL_WORKER_ID, LocalWorkerSpec, NodeRole, Worker},
};
use wedb_test::{cluster_decorate, start_node};
use wnode_test::{cmd, complete_len};
use wresp::ext::RespVecExt;
use wtest_base::{log_capture_mark, log_capture_records_since, wait_for};

/// C# :67-68 频道名
const CHANNEL: &[u8] = b"test-forward-channel";
/// C# :69 消息体
const MESSAGE: &[u8] = b"forwarded-message";

/// 发布端（幸存节点，C# publisherIndex 0）节点身份
const SURVIVOR_ID: u128 = 0xC0FF_EE00_0000_0000_0000_0000_0000_0001;
/// 被关停的对端（C# peerIndex 1）节点身份
const PEER_ID: u128 = 0xC0FF_EE00_0000_0000_0000_0000_0000_0002;

/// C# :92 基线 15 秒窗在 rust 侧收敛为轮次上界：每轮一次真发布 + 一次限时读，
/// 健康路径首轮即命中（订阅已生效、转发连接已点亮）
const DELIVERY_ATTEMPTS: usize = 30;
/// 单轮投递读等待上界（C# :97 delivered.Wait(500ms) 同款）
const READ_BUDGET: Duration = Duration::from_millis(500);
/// C# :123 requiredLateSuccesses——静置信号之后的连续成功次数下界
const REQUIRED_LATE_SUCCESSES: usize = 8;
/// 关停后等待「对端转发连接转为断开」的轮次上界（每轮驱动一次真发布与一次探测）
const SETTLE_ROUNDS: usize = 100;
/// 静置轮次间的让出步长
const SETTLE_STEP: Duration = Duration::from_millis(20);
/// 转发连接点亮等待上界（仅用于红态收口，健康路径毫秒级命中）
const CONNECT_WAIT: Duration = Duration::from_secs(10);
/// 「未连接即跳过转发」臂的留痕锚（node_connection::try_cluster_publish_async）
const SKIP_FORWARD_LOG: &str = "client not connected; skipping publish forwarding";

/// 订阅确认帧（*3：类型 bulk + 通道 bulk + 计数整数）
fn subscribe_ack_frame() -> Vec<u8> {
  let mut out = Vec::new();
  out.write_resp_array_len(3);
  out.write_resp_bulk_string(b"subscribe");
  out.write_resp_bulk_string(CHANNEL);
  out.write_resp_int(1);
  out
}

/// 跨节点投递的消息推送帧（收端剥 ns 隔离前缀还原裸通道名）
fn message_push_frame() -> Vec<u8> {
  let mut out = Vec::new();
  out.write_resp_array_len(3);
  out.write_resp_bulk_string(b"message");
  out.write_resp_bulk_string(CHANNEL);
  out.write_resp_bulk_string(MESSAGE);
  out
}

/// 本地 worker 身份 + 对端 worker 互指（转发枚举源
/// ClusterConfig::get_all_node_ids 自 2 号位起，故本地位先落 1 号 worker）；
/// 全槽指派点亮本地 Stable 面，供末态 SET/GET 走真写命令门
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

/// 本节点对 `peer_id` 的 gossip 转发连接是否处于实连态（跨节点投递的真凭据）
fn peer_conn_live(cp: &ClusterProvider, peer_id: u128) -> bool {
  cp.gossip_manager()
    .expect("gossip manager 在场")
    .connection_store
    .get_connection(peer_id)
    .is_some_and(|conn| conn.is_connected())
}

/// 接入宿主监听口
async fn connect(port: u16) -> TcpStream {
  TcpStream::connect(format!("127.0.0.1:{port}"))
    .await
    .unwrap()
}

/// 向调用方持有的累积缓冲续读，直至出现一条完整 RESP 帧（EOF/出错返回 false）；
/// 半帧留在缓冲下一轮续读，故外层限时取消不丢帧
async fn fill_until_frame(stream: &mut TcpStream, acc: &mut Vec<u8>) -> bool {
  loop {
    if complete_len(acc).is_some() {
      return true;
    }
    let BufResult(res, buf) = stream.read(vec![0u8; 1024]).await;
    let Ok(n) = res else { return false };
    if n == 0 {
      return false;
    }
    acc.extend_from_slice(&buf[..n]);
  }
}

#[compio::test]
async fn cluster_publish_survives_peer_node_shutdown() -> Void {
  // ===== C# :56 双实例 + :58 两主集群：A = 发布/幸存端，B = 订阅/被关停端
  let acp = ClusterProvider::new();
  let (_adir, aserver, _aacp, _awal, aport) =
    start_node(Arc::clone(&acp), cluster_decorate(Arc::clone(&acp)))?;
  let bcp = ClusterProvider::new();
  let (_bdir, bserver, _bacp, _bwal, bport) =
    start_node(Arc::clone(&bcp), cluster_decorate(Arc::clone(&bcp)))?;

  // 收端 CLUSTER PUBLISH 的投递源与会话注册面须同一 broker 实例
  //（宿主 boot.rs 同款注入：cluster.set_pubsub(provider.pubsub)）
  acp.set_pubsub(aserver.session_provider().pubsub.clone());
  bcp.set_pubsub(bserver.session_provider().pubsub.clone());
  wire_pair(&acp, SURVIVOR_ID, aport, PEER_ID, bport);
  wire_pair(&bcp, PEER_ID, bport, SURVIVOR_ID, aport);

  // ===== C# :60-62 双侧互知后才开始发布（此处拓扑直写，互指即成）
  // ===== C# :74-83 专用连接：订阅端连 B、发布端连 A，投递即跨节点转发凭据
  let mut sub = connect(bport).await;
  let mut pubc = connect(aport).await;
  let ack = cmd(&mut sub, &[b"SUBSCRIBE", CHANNEL]).await;
  assert_eq!(ack, subscribe_ack_frame(), "订阅确认帧不符");

  // ===== C# :85-104 基线投递臂：转发是发布端 detached 任务且首连惰性，
  // 故按有界重试直至订阅端收帧（C# 同一形态，仅把 15 秒窗换成轮次上界）
  let mut acc = Vec::new();
  let mut delivered = false;
  for _ in 0..DELIVERY_ATTEMPTS {
    let out = cmd(&mut pubc, &[b"PUBLISH", CHANNEL, MESSAGE]).await;
    assert_eq!(
      out, b":0\r\n",
      "C# :94 DoesNotThrow：双侧在线时发布不得回错"
    );
    if timeout(READ_BUDGET, fill_until_frame(&mut sub, &mut acc))
      .await
      .unwrap_or(false)
    {
      delivered = true;
      break;
    }
  }
  assert!(delivered, "C# :103：双侧在线时的发布未被转发/投递");
  let at = complete_len(&acc).expect("订阅端首帧须是完整 RESP 推送帧");
  assert_eq!(
    &acc[..at],
    message_push_frame(),
    "跨节点投递帧须逐字节自洽，实收 {:?}",
    String::from_utf8_lossy(&acc[..at])
  );

  // 基线的机器侧凭据：A 确已点亮对 B 的 gossip 实连（转发走过真 socket）
  assert!(
    wait_for(|| peer_conn_live(&acp, PEER_ID), CONNECT_WAIT).await,
    "A 侧对 B 的转发连接应点亮为在册实连"
  );

  // ===== C# :106-110 取消订阅并关停对端节点（A 仍把 B 列为在册主节点）
  let mark = log_capture_mark();
  cmd(&mut sub, &[b"UNSUBSCRIBE", CHANNEL]).await;
  drop(sub);
  bserver.dispose();

  // ===== C# :112-153 恢复臂：持续在幸存端发布直至 A 观测到对 B 的连接断开
  //（rust 侧静置信号＝「未连接即跳过转发」这一修复行为的可见状态）
  let mut settled = false;
  for _ in 0..SETTLE_ROUNDS {
    // C# :130 幸存端全程必须应答（主线程被钉死即失联）
    let pong = cmd(&mut pubc, &[b"PING"]).await;
    assert_eq!(pong, b"+PONG\r\n", "C# :130：幸存端在对端关停后不得失联");
    let out = cmd(&mut pubc, &[b"PUBLISH", CHANNEL, MESSAGE]).await;
    assert_eq!(out, b":0\r\n", "C# :136：幸存端发布不得回错或挂起");
    if !peer_conn_live(&acp, PEER_ID) {
      settled = true;
      break;
    }
    sleep(SETTLE_STEP).await;
  }
  assert!(
    settled,
    "关停对端后 A 侧的转发连接未收敛为断开（跳过失效转发的修复行为未生效）"
  );

  // C# :137-153 静置信号之后的连续成功计数：恢复的真凭据
  for round in 0..REQUIRED_LATE_SUCCESSES {
    let out = cmd(&mut pubc, &[b"PUBLISH", CHANNEL, MESSAGE]).await;
    assert_eq!(out, b":0\r\n", "C# :153：静置后第 {round} 次发布未恢复");
  }

  // 关键臂的行为凭据（log_capture 单源）：对端失效后转发须「静默跳过」而非
  // 摸向已断开的 client——C# 修复前正是在这一步抛异常打挂集群 pub/sub 面
  assert!(
    wait_for(
      || {
        log_capture_records_since(mark)
          .iter()
          .any(|(_, msg)| msg.contains(SKIP_FORWARD_LOG))
      },
      CONNECT_WAIT
    )
    .await,
    "关停对端后幸存端须留下跳过失效转发的留痕，实收 {:?}",
    log_capture_records_since(mark)
  );

  // ===== C# :157 末态 sanity：幸存端仍完整服务命令面
  let set = cmd(&mut pubc, &[b"SET", b"pubpeer-key", b"v"]).await;
  assert_eq!(set, b"+OK\r\n", "幸存端末态须仍能执行写命令");
  let get = cmd(&mut pubc, &[b"GET", b"pubpeer-key"]).await;
  assert_eq!(get, b"$1\r\nv\r\n", "幸存端末态须能读回写入值");

  drop(pubc);
  // 收尾：先停投递面的连接仓库再拆宿主（gossip_manager.rs 同款收口序）
  acp.gossip_manager().expect("gossip manager 在场").dispose();
  aserver.dispose();
  aok::OK
}
