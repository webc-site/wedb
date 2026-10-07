#![recursion_limit = "256"]
//! CLUSTER PUBLISH / SPUBLISH 收令帧身份门禁集成判据（doc/zh/db.md 3.5）
//!
//! 攻击链：发送侧 wpubsub network_publish 先把频道折叠成 "<ns>:" 隔离键再
//! 经 CLUSTER PUBLISH 帧转发，收端 network_cluster_publish 原样直投本地
//! broker（租户分区随键贯通）——该帧实质是多租户集群总线收令帧，仅节点间
//! 连接或 ns0 超管可发起（C# 无 ns 维度，NetworkClusterPublish 等价普通
//! PUBLISH 无越权面）。本帧与 CLUSTER FLUSHALL_NS 共用同一调用方身份门
//! （wnode admin_commands network_process_cluster_command，姊妹票
//! cluster_flushall_ns_caller_gate 同门）。本文件用真实 TCP 集群装配验证三面：
//! 1. 裸发拒绝：ns5 租户会话（仅 +@pubsub，pubsub 客户端常规授权即持
//!    CLUSTER|PUBLISH 执行位）对 ns7 隔离键发收令帧必须回单源 NOPERM，
//!    ns7 的通道 / 模式 / 分片订阅者在静默窗内零投递；
//! 2. ns0 超管放行：同帧直达订阅者（message / pmessage / smessage
//!    剥前缀帧形逐字节自洽），合法管理面不误伤；
//! 3. 节点间转发链路回归：双节点集群 ns7 会话普通 PUBLISH 经 gossip
//!    NodeConnection 转发（收令帧走节点间连接放行臂），对端 ns7 订阅者
//!    正常收到——门禁不得误伤节点间转发。
//!
//! 装配复刻 cluster_flushall_ns_caller_gate.rs 的 start_flush_node ACL 档
//! 起服（nopass default 免认证连接形态不变）与
//! cluster_shard_sub_unsubscribe_on_slot_migration 的全槽 Stable 指派形态
//!（SSUBSCRIBE 的 channel 键位参与槽校验）。

use std::{num::NonZeroUsize, sync::Arc, time::Duration};

use aok::{Result, Void};
use compio::{buf::BufResult, io::AsyncRead, net::TcpStream, time::timeout};
use tempfile::{TempDir, tempdir};
use wacl::AccessControlList;
use wconf::RuntimeServerOptions;
use wdev::SegmentedDevice;
use wedb::server::{
  cluster_config::ClusterConfig,
  cluster_provider::ClusterProvider,
  hash_slot::{HashSlot, SlotState},
  worker::{LOCAL_WORKER_ID, LocalWorkerSpec, NodeRole, Worker},
};
use wedb_test::cluster_decorate;
use wnode::{
  GarnetServer, RespSessionConsumer, resp::garnet_api::StoreGarnetApi,
  service::StorageSessionProvider,
};
use wnode_test::{complete_len, send_cmd};
use wresp::{cmd_strings as cs, ext::RespVecExt};
use wtest_base::test_store_config;

/// 受害租户通道裸名（ns7 订阅面）
const CHANNEL: &[u8] = b"ch";
/// ns7 模式订阅（覆盖 CHANNEL）
const PATTERN: &[u8] = b"c*";
/// ns7 的通道隔离键（ChannelNsPrefix::isolate 折叠形态，攻击帧第 0 参）
const ISOLATED: &[u8] = b"7:ch";
/// 攻击消息体
const ATTACK: &[u8] = b"evil";
/// 合法投递消息体
const PAYLOAD: &[u8] = b"x";
/// 静默窗与单轮投递读预算（C# ClusterPubSubForwardTests :97 的 500ms 同款）
const READ_BUDGET: Duration = Duration::from_millis(500);
/// 命令应答等待上界（同步应答毫秒级，宽预算防装配抖动）
const CALL_BUDGET: Duration = Duration::from_secs(10);
/// 跨节点投递有界重试轮次（转发 detached + 惰性建连，健康路径毫秒级命中）
const DELIVERY_ATTEMPTS: usize = 30;
/// 双节点身份（wire_pair 互指）
const NODE_A: u128 = 0x0FB2_0000_0000_0000_0000_0000_0000_0001;
const NODE_B: u128 = 0x0FB2_0000_0000_0000_0000_0000_0000_0002;

/// 裸 socket RESP 流：累积缓冲续读半帧、逐帧消费（订阅态推送帧与同步
/// 应答帧同读法，参照 cluster_pubsub_peer_shutdown 的 fill_until_frame）
struct RawSession {
  stream: TcpStream,
  acc: Vec<u8>,
}

impl RawSession {
  async fn connect(port: u16) -> Self {
    Self {
      stream: TcpStream::connect(format!("127.0.0.1:{port}"))
        .await
        .unwrap(),
      acc: Vec::new(),
    }
  }

  async fn send(&mut self, args: &[&[u8]]) {
    send_cmd(&mut self.stream, args).await.expect("发送命令");
  }

  /// 发命令并读一条完整应答帧
  async fn call(&mut self, args: &[&[u8]]) -> Vec<u8> {
    self.send(args).await;
    self.frame(CALL_BUDGET).await.expect("应答帧超时")
  }

  /// 认证为 <ns>#user 的长连接
  async fn auth(port: u16, user: &str, password: &str) -> Self {
    let mut s = Self::connect(port).await;
    assert_eq!(
      s.call(&[b"AUTH", user.as_bytes(), password.as_bytes()])
        .await,
      b"+OK\r\n",
      "用户 {user} 应认证成功"
    );
    s
  }

  /// 读下一条完整帧：budget 为单次读等待上界，静默（超时 / EOF / 读错）
  /// 返回 None
  async fn frame(&mut self, budget: Duration) -> Option<Vec<u8>> {
    loop {
      if let Some(at) = complete_len(&self.acc) {
        let head = self.acc[..at].to_vec();
        self.acc.drain(..at);
        return Some(head);
      }
      let BufResult(res, buf) = timeout(budget, self.stream.read(vec![0u8; 1024]))
        .await
        .ok()?;
      let n = res.ok()?;
      if n == 0 {
        return None;
      }
      self.acc.extend_from_slice(&buf[..n]);
    }
  }

  /// 静默窗断言：READ_BUDGET 内不得出现任何推送帧
  async fn assert_silent(&mut self) {
    assert!(
      self.frame(READ_BUDGET).await.is_none(),
      "静默窗内不得出现任何推送帧，实收缓冲 {:?}",
      String::from_utf8_lossy(&self.acc)
    );
  }
}

/// 经 ns0 免认证 default 会话建租户用户（doc/zh/db.md 三、认证格式）
async fn seed_user(port: u16, user: &str, password: &str, rules: &[&str]) {
  let pwd = format!(">{password}");
  let mut args: Vec<&[u8]> = vec![b"ACL", b"SETUSER", user.as_bytes(), b"on", pwd.as_bytes()];
  args.extend(rules.iter().map(|r| r.as_bytes()));
  let mut admin = RawSession::connect(port).await;
  assert_eq!(
    admin.call(&args).await,
    b"+OK\r\n",
    "ACL SETUSER {user} 应成功"
  );
}

/// 订阅确认帧（前缀常量已含 *3 与类型名，补通道 / 模式名与订阅计数）
fn ack_frame(prefix: &[u8], name: &[u8]) -> Vec<u8> {
  let mut out = prefix.to_vec();
  out.write_resp_bulk_string(name);
  out.write_resp_int(1);
  out
}

/// 通道投递帧（message / smessage 同构：裸通道名 + 消息体）
fn push_frame(prefix: &[u8], channel: &[u8], value: &[u8]) -> Vec<u8> {
  let mut out = prefix.to_vec();
  out.write_resp_bulk_string(channel);
  out.write_resp_bulk_string(value);
  out
}

/// 模式投递帧（pmessage：裸模式名 + 裸通道名 + 消息体）
fn pmessage_frame(pattern: &[u8], channel: &[u8], value: &[u8]) -> Vec<u8> {
  let mut out = cs::PUBSUB_PUSH_PMSG_PREFIX_RESP2.to_vec();
  out.write_resp_bulk_string(pattern);
  out.write_resp_bulk_string(channel);
  out.write_resp_bulk_string(value);
  out
}

/// ACL 档节点装配产物（临时目录 / 服务器 / 监听端口）
type AclNode<D> = (TempDir, GarnetServer<StorageSessionProvider<D>>, u16);

/// 起一个带 ACL 认证面的生产宿主形态节点（随机端口，AOF 门控点亮）
///
/// 装配同 wedb_test::start_node，链尾点亮 nopass ACL 档（对齐
/// cluster_flushall_ns_caller_gate 的 start_flush_node 先例）：nopass
/// default 会话构造尾自动认证，免认证连接形态不变，同时 ACL SETUSER /
/// AUTH 租户认证面在场可用
fn start_acl_node<D>(decorate: D) -> Result<AclNode<D>>
where
  D:
    Fn(u64, StoreGarnetApi<SegmentedDevice>) -> Option<RespSessionConsumer> + Send + Sync + 'static,
{
  let dir = tempdir()?;
  let session_provider = Arc::new(
    StorageSessionProvider::open_with_config_and_aof(
      test_store_config(),
      dir.path().join("node.db"),
      None,
      RuntimeServerOptions::default(),
      decorate,
    )?
    .with_acl(Arc::new(AccessControlList::new("")?)),
  );
  let server = GarnetServer::new(&["127.0.0.1:0".to_string()], 65536, session_provider)?;
  server.start(NonZeroUsize::new(1))?;
  let port = server.local_addr()?.port();
  Ok((dir, server, port))
}

/// 本地 worker 登记 + 全槽 Stable 指派（SSUBSCRIBE 的 channel 经命令目录
/// key spec 提键参与槽校验，槽未指派即 CLUSTERDOWN——与 garnet 同口径；
/// 指派形态与 cluster_shard_sub_unsubscribe_on_slot_migration 的 wire_pair
/// 同款）
fn assign_local_slots(config: &mut ClusterConfig, node_id: u128, own_port: u16) {
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
}

/// 本地全槽指派 + 对端 worker 互指（转发枚举源 get_all_node_ids 自
/// 2 号位起，故本地位先落 1 号 worker）
fn wire_pair(cp: &ClusterProvider, node_id: u128, own_port: u16, peer: u128, peer_port: u16) {
  let cm = cp.cluster_manager().expect("cluster manager 在场");
  let mut config = cm.current_config.write();
  assign_local_slots(&mut config, node_id, own_port);
  config.workers.push(Worker {
    nodeid: Some(peer),
    address: "127.0.0.1".into(),
    port: peer_port as i32,
    config_epoch: 1,
    role: NodeRole::Primary,
    replica_of_node_id: None,
    replication_offset: 0,
    hostname: None,
  });
}

/// ns5 租户会话（仅 +@pubsub）对 ns7 隔离键发 CLUSTER PUBLISH / SPUBLISH
/// 收令帧：必须回单源 NOPERM，且 ns7 的通道 / 模式 / 分片订阅者零投递
#[compio::test]
async fn cluster_publish_denies_foreign_tenant_session() -> Void {
  let cp = ClusterProvider::new();
  let (_dir, server, port) = start_acl_node(cluster_decorate(Arc::clone(&cp)))?;
  cp.set_pubsub(server.session_provider().pubsub.clone());
  let cm = cp.cluster_manager().expect("cluster manager 在场");
  assign_local_slots(&mut cm.current_config.write(), NODE_A, port);

  seed_user(port, "5#mallory", "malpw", &["+@pubsub"]).await;
  seed_user(port, "7#carol", "capw", &["+@all"]).await;

  // ns7 受害订阅面：通道 / 模式 / 分片三连接
  let mut sub = RawSession::auth(port, "7#carol", "capw").await;
  assert_eq!(
    sub.call(&[b"SUBSCRIBE", CHANNEL]).await,
    ack_frame(cs::PUBSUB_SUBSCRIBE_FRAME_PREFIX, CHANNEL)
  );
  let mut pat = RawSession::auth(port, "7#carol", "capw").await;
  assert_eq!(
    pat.call(&[b"PSUBSCRIBE", PATTERN]).await,
    ack_frame(cs::PUBSUB_PSUBSCRIBE_FRAME_PREFIX, PATTERN)
  );
  let mut shard = RawSession::auth(port, "7#carol", "capw").await;
  assert_eq!(
    shard.call(&[b"SSUBSCRIBE", CHANNEL]).await,
    ack_frame(cs::PUBSUB_SSUBSCRIBE_FRAME_PREFIX, CHANNEL)
  );

  // ns5 攻击者：pubsub 客户端常规授权即持 CLUSTER|PUBLISH 执行位
  //（目录全量收录含 IsInternal 条目），直发他租户隔离键收令帧
  let mut mallory = RawSession::auth(port, "5#mallory", "malpw").await;
  for subcmd in [b"PUBLISH" as &[u8], b"SPUBLISH"] {
    let err = mallory.call(&[b"CLUSTER", subcmd, ISOLATED, ATTACK]).await;
    assert!(
      String::from_utf8_lossy(&err).contains("NOPERM"),
      "CLUSTER {subcmd:?} 收令帧应回单源 NOPERM 权限错误，实收 {err:?}"
    );
  }

  // 攻击帧整帧拒绝：三类订阅者静默窗内零投递
  sub.assert_silent().await;
  pat.assert_silent().await;
  shard.assert_silent().await;
  aok::OK
}

/// ns0 超管（免认证 default 会话）同帧直达订阅者：合法管理面不误伤，
/// 剥前缀帧形逐字节自洽（message / pmessage / smessage）
#[compio::test]
async fn cluster_publish_ns0_frame_reaches_tenant_subscribers() -> Void {
  let cp = ClusterProvider::new();
  let (_dir, server, port) = start_acl_node(cluster_decorate(Arc::clone(&cp)))?;
  cp.set_pubsub(server.session_provider().pubsub.clone());
  let cm = cp.cluster_manager().expect("cluster manager 在场");
  assign_local_slots(&mut cm.current_config.write(), NODE_A, port);

  seed_user(port, "7#carol", "capw", &["+@all"]).await;

  let mut sub = RawSession::auth(port, "7#carol", "capw").await;
  assert_eq!(
    sub.call(&[b"SUBSCRIBE", CHANNEL]).await,
    ack_frame(cs::PUBSUB_SUBSCRIBE_FRAME_PREFIX, CHANNEL)
  );
  let mut pat = RawSession::auth(port, "7#carol", "capw").await;
  assert_eq!(
    pat.call(&[b"PSUBSCRIBE", PATTERN]).await,
    ack_frame(cs::PUBSUB_PSUBSCRIBE_FRAME_PREFIX, PATTERN)
  );
  let mut shard = RawSession::auth(port, "7#carol", "capw").await;
  assert_eq!(
    shard.call(&[b"SSUBSCRIBE", CHANNEL]).await,
    ack_frame(cs::PUBSUB_SSUBSCRIBE_FRAME_PREFIX, CHANNEL)
  );

  // CLUSTER PUBLISH / SPUBLISH 收令帧无应答写出（C# NetworkClusterPublish
  // 同形），发后不等应答；PUBLISH 直达通道与模式订阅面
  let mut admin = RawSession::connect(port).await;
  admin
    .send(&[b"CLUSTER", b"PUBLISH", ISOLATED, PAYLOAD])
    .await;
  assert_eq!(
    sub.frame(READ_BUDGET).await.expect("通道订阅者应收到投递"),
    push_frame(cs::PUBSUB_PUSH_MSG_PREFIX_RESP2, CHANNEL, PAYLOAD)
  );
  assert_eq!(
    pat.frame(READ_BUDGET).await.expect("模式订阅者应收到投递"),
    pmessage_frame(PATTERN, CHANNEL, PAYLOAD)
  );
  // SPUBLISH 仅投分片域（路由隔离）
  admin
    .send(&[b"CLUSTER", b"SPUBLISH", ISOLATED, PAYLOAD])
    .await;
  assert_eq!(
    shard
      .frame(READ_BUDGET)
      .await
      .expect("分片订阅者应收到投递"),
    push_frame(cs::PUBSUB_PUSH_SMSG_PREFIX_RESP2, CHANNEL, PAYLOAD)
  );
  aok::OK
}

/// 双节点集群：ns7 会话在节点 A 普通 PUBLISH，经 gossip NodeConnection
/// 转发（收端会话为节点间连接形态，走 remote_node_id / ns0 放行臂），
/// 节点 B 的 ns7 订阅者正常收到——门禁不得误伤节点间转发链路
#[compio::test]
async fn cluster_publish_gate_keeps_peer_forwarding() -> Void {
  let acp = ClusterProvider::new();
  let (_adir, aserver, aport) = start_acl_node(cluster_decorate(Arc::clone(&acp)))?;
  let bcp = ClusterProvider::new();
  let (_bdir, bserver, bport) = start_acl_node(cluster_decorate(Arc::clone(&bcp)))?;
  // 收端 CLUSTER PUBLISH 的投递源与会话注册面须同一 broker 实例
  acp.set_pubsub(aserver.session_provider().pubsub.clone());
  bcp.set_pubsub(bserver.session_provider().pubsub.clone());
  wire_pair(&acp, NODE_A, aport, NODE_B, bport);
  wire_pair(&bcp, NODE_B, bport, NODE_A, aport);

  seed_user(aport, "7#carol", "capw", &["+@all"]).await;
  seed_user(bport, "7#carol", "capw", &["+@all"]).await;

  let mut sub = RawSession::auth(bport, "7#carol", "capw").await;
  assert_eq!(
    sub.call(&[b"SUBSCRIBE", CHANNEL]).await,
    ack_frame(cs::PUBSUB_SUBSCRIBE_FRAME_PREFIX, CHANNEL)
  );

  let expect = push_frame(cs::PUBSUB_PUSH_MSG_PREFIX_RESP2, CHANNEL, PAYLOAD);
  let mut publisher = RawSession::auth(aport, "7#carol", "capw").await;
  let mut delivered = false;
  for _ in 0..DELIVERY_ATTEMPTS {
    let out = publisher.call(&[b"PUBLISH", CHANNEL, PAYLOAD]).await;
    assert_eq!(
      out, b":0\r\n",
      "A 本地无订阅者，发布应回本地计数 0 且不得回错"
    );
    if let Some(frame) = sub.frame(READ_BUDGET).await {
      assert_eq!(frame, expect, "跨节点投递帧须逐字节自洽");
      delivered = true;
      break;
    }
  }
  assert!(delivered, "普通 PUBLISH 的节点间转发链路被门禁误拦");

  // 收尾：先停投递面的连接仓库再拆宿主（gossip_manager 同款收口序）
  acp.gossip_manager().expect("gossip manager 在场").dispose();
  aserver.dispose();
  bcp.gossip_manager().expect("gossip manager 在场").dispose();
  bserver.dispose();
  aok::OK
}
