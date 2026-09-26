//! PUBSUB 会话命令内联单测册（r327 自 wpubsub/src/session_commands.rs 部分迁移）
//!
//! 来源：r317 迁移票弃迁后的「可迁子集」——本册 8 例对 crate 私有面零触达，
//! 逐字搬运零语义（断言、夹具、时序不动）；MockSession 夹具随块迁。
//! 仅 `test_drain_mailbox_into_reuses_capacity` 触私有字段 `wire`/`msg_buf`
//! （容量复用断言），按「不为迁就搬运扩 pub」红线留地 src 原位，见该文件注记。

use std::sync::Arc;

use wpubsub::{
  session_commands::{PubSubSession, PubSubSessionCommands},
  subscribe_broker::SubscribeBroker,
};

struct MockSession {
  id: i64,
  ns: u64,
  output: Vec<u8>,
  protocol_version: u8,
  is_subscription: bool,
  clustered: bool,
  forwarded: Vec<(bool, Vec<u8>)>,
}

impl MockSession {
  fn new(id: i64) -> Self {
    Self {
      id,
      ns: 0,
      output: Vec::new(),
      protocol_version: 2,
      is_subscription: false,
      clustered: false,
      forwarded: Vec::new(),
    }
  }
}

impl PubSubSessionCommands for MockSession {
  fn session_id(&self) -> i64 {
    self.id
  }

  fn namespace(&self) -> u64 {
    self.ns
  }

  fn output_mut(&mut self) -> &mut Vec<u8> {
    &mut self.output
  }

  fn resp_protocol_version(&self) -> u8 {
    self.protocol_version
  }

  fn set_subscription_session(&mut self, is_subscription: bool) {
    self.is_subscription = is_subscription;
  }

  fn has_cluster_session(&self) -> bool {
    self.clustered
  }

  fn cluster_publish(&mut self, is_spublish: bool, channel: &[u8], _message: &[u8]) {
    self.forwarded.push((is_spublish, channel.to_vec()));
  }
}

#[test]
fn subscribe_unsubscribe_lifecycle() {
  let broker = Arc::new(SubscribeBroker::new());
  let mut wire = PubSubSession::new(broker);
  let mut session = MockSession::new(100);

  assert!(session.network_subscribe(&mut wire, false, &[b"chan1", b"chan2"]));
  assert!(session.is_subscription);
  assert_eq!(wire.num_active_channels(), 2);

  // 退订一个通道
  assert!(session.network_unsubscribe(&mut wire, &[b"chan1"]));
  assert!(session.is_subscription);
  assert_eq!(wire.num_active_channels(), 1);

  // 退订最后一个通道
  assert!(session.network_unsubscribe(&mut wire, &[b"chan2"]));
  assert!(!session.is_subscription);
  assert_eq!(wire.num_active_channels(), 0);
}

#[test]
fn shard_subscribe_unsubscribe_lifecycle() {
  let broker = Arc::new(SubscribeBroker::new());
  let mut wire = PubSubSession::new(broker);
  let mut session = MockSession::new(100);
  // shard 命令须集群装配才达订阅路径（否则回 CLUSTER_DISABLED）
  session.clustered = true;

  assert!(session.network_subscribe(&mut wire, true, &[b"shard1", b"shard2"]));
  assert!(session.is_subscription);
  assert_eq!(wire.num_active_channels(), 2);
  let out_str = String::from_utf8_lossy(&session.output);
  assert!(out_str.contains("*3\r\n$10\r\nssubscribe\r\n$6\r\nshard1\r\n:1\r\n"));
  assert!(out_str.contains("*3\r\n$10\r\nssubscribe\r\n$6\r\nshard2\r\n:2\r\n"));

  session.output.clear();
  assert!(session.network_sunsubscribe(&mut wire, &[b"shard1"]));
  assert_eq!(wire.num_active_channels(), 1);
  let out_str2 = String::from_utf8_lossy(&session.output);
  assert!(out_str2.contains("*3\r\n$12\r\nsunsubscribe\r\n$6\r\nshard1\r\n:1\r\n"));

  session.output.clear();
  assert!(session.network_sunsubscribe(&mut wire, &[]));
  assert_eq!(wire.num_active_channels(), 0);
  assert!(!session.is_subscription);
}

#[test]
fn shard_publish_and_drain_push_frames() {
  let broker = Arc::new(SubscribeBroker::new());
  let mut wire = PubSubSession::new(broker);
  let mut session = MockSession::new(101);
  session.clustered = true;

  session.network_subscribe(&mut wire, true, &[b"slot1"]);
  session.output.clear();

  assert!(session.network_publish(&mut wire, true, &[b"slot1", b"hello"]));
  let out_str = String::from_utf8_lossy(&session.output);
  assert!(out_str.contains("*3\r\n$8\r\nsmessage\r\n$5\r\nslot1\r\n$5\r\nhello\r\n:1\r\n"));
}

#[test]
fn publish_and_drain_push_frames() {
  let broker = Arc::new(SubscribeBroker::new());
  let mut wire = PubSubSession::new(broker.clone());
  let mut session = MockSession::new(101);

  session.network_subscribe(&mut wire, false, &[b"news"]);
  session.output.clear();

  // 自发布自接收
  assert!(session.network_publish(&mut wire, false, &[b"news", b"hello"]));
  assert_eq!(session.drain_pubsub_frames(&mut wire), 0); // 已在 publish 内 drain 过
  let out_str = String::from_utf8_lossy(&session.output);
  assert!(out_str.contains("*3\r\n$7\r\nmessage\r\n$4\r\nnews\r\n$5\r\nhello\r\n:1\r\n"));
}

#[test]
fn disabled_pubsub_handling() {
  let mut wire = PubSubSession::with_mailbox_capacity(None, 16);
  let mut session = MockSession::new(102);

  assert!(session.network_subscribe(&mut wire, false, &[b"test"]));
  assert_eq!(
    session.output,
    b"-ERR SUBSCRIBE is disabled, enable it with --pubsub option.\r\n"
  );

  // SPUBLISH 需过集群门才达 broker 判空禁用臂（否则命令层先回 CLUSTER_DISABLED）
  session.clustered = true;
  session.output.clear();
  assert!(session.network_publish(&mut wire, true, &[b"test", b"v"]));
  assert_eq!(
    session.output,
    b"-ERR PUBLISH is disabled, enable it with --pubsub option.\r\n"
  );
}

#[test]
fn publish_cluster_hook_forwarding() {
  let broker = Arc::new(SubscribeBroker::new());
  let mut wire = PubSubSession::new(broker);
  let mut session = MockSession::new(103);

  // 单机形态 PUBLISH：非 shard 无集群门，本地广播（0 订阅者）、无转发
  assert!(session.network_publish(&mut wire, false, &[b"ch", b"msg"]));
  assert_eq!(String::from_utf8_lossy(&session.output), ":0\r\n");
  assert!(session.forwarded.is_empty());

  // 集群形态：SPUBLISH 本地广播 + 转发（ns 隔离键随广播透传）+ 应答通知数（:0 无订阅者）
  session.clustered = true;
  session.output.clear();
  assert!(session.network_publish(&mut wire, true, &[b"ch", b"msg"]));
  assert_eq!(session.forwarded, vec![(true, b"0:ch".to_vec())]);
  assert_eq!(String::from_utf8_lossy(&session.output), ":0\r\n");

  // 集群形态 PUBLISH 同样转发（is_spublish = false）
  session.output.clear();
  assert!(session.network_publish(&mut wire, false, &[b"ch", b"msg"]));
  assert_eq!(session.forwarded.last(), Some(&(false, b"0:ch".to_vec())));
  assert_eq!(String::from_utf8_lossy(&session.output), ":0\r\n");
}

// C# PubSubCommands.cs:108-112/168-172：SSUBSCRIBE/SPUBLISH 在
// clusterSession == null 时命令层拒绝门——回 CLUSTER_DISABLED 且不入 broker、
// 不改计数、不转发；SUNSUBSCRIBE（rust 补全命令）对位同门
#[test]
fn shard_commands_rejected_without_cluster() {
  let broker = Arc::new(SubscribeBroker::new());
  let mut wire = PubSubSession::new(broker);
  let mut session = MockSession::new(104); // clustered 默认 false（trait 无集群形态）

  // SSUBSCRIBE 无集群门 → CLUSTER_DISABLED，不订阅、不改活跃计数
  assert!(session.network_subscribe(&mut wire, true, &[b"chan"]));
  assert_eq!(
    session.output,
    b"-ERR This instance has cluster support disabled\r\n"
  );
  assert_eq!(wire.num_active_channels(), 0);
  assert!(!session.is_subscription);
  assert_eq!(wire.broker().unwrap().num_subscriptions(b"chan"), 0);

  // SPUBLISH 无集群门 → CLUSTER_DISABLED，不入 broker、不转发
  session.output.clear();
  assert!(session.network_publish(&mut wire, true, &[b"chan", b"msg"]));
  assert_eq!(
    session.output,
    b"-ERR This instance has cluster support disabled\r\n"
  );
  assert!(session.forwarded.is_empty());

  // 带参 SUNSUBSCRIBE 无集群门 → CLUSTER_DISABLED，不退订、不改计数与订阅态
  session.output.clear();
  assert!(session.network_sunsubscribe(&mut wire, &[b"chan"]));
  assert_eq!(
    session.output,
    b"-ERR This instance has cluster support disabled\r\n"
  );
  assert_eq!(wire.num_active_channels(), 0);
  assert!(!session.is_subscription);

  // 无参 SUNSUBSCRIBE 同样被门拦截，严禁落入 broker 全表遍历分片通道
  session.output.clear();
  assert!(session.network_sunsubscribe(&mut wire, &[]));
  assert_eq!(
    session.output,
    b"-ERR This instance has cluster support disabled\r\n"
  );
  assert_eq!(wire.num_active_channels(), 0);
  assert!(!session.is_subscription);
}

// 空参退订收口（对标 Redis 规范；C# NetworkUNSUBSCRIBE/ListAllSubscriptions
// 缺陷修复）：零订阅仅回单条 null 尾帧计数 0；同租户他人订阅零泄露；
// 持有普通频道时空参 PUNSUBSCRIBE 尾帧计数 = 真实剩余订阅数（非硬编码 :0）
#[test]
fn empty_unsubscribe_only_frames_owned_and_counts_remaining() {
  let broker = Arc::new(SubscribeBroker::new());
  let mut wire_a = PubSubSession::new(broker.clone());
  let mut wire_b = PubSubSession::new(broker);
  let mut a = MockSession::new(1);
  let mut b = MockSession::new(2);
  a.clustered = true;
  b.clustered = true;

  // 零订阅空参退订：三类命令各回单条 null 名尾帧（RESP2 `$-1`），计数 0
  assert!(a.network_unsubscribe(&mut wire_a, &[]));
  assert_eq!(a.output, &b"*3\r\n$11\r\nunsubscribe\r\n$-1\r\n:0\r\n"[..]);
  a.output.clear();
  assert!(a.network_sunsubscribe(&mut wire_a, &[]));
  assert_eq!(a.output, &b"*3\r\n$12\r\nsunsubscribe\r\n$-1\r\n:0\r\n"[..]);
  a.output.clear();
  assert!(a.network_punsubscribe(&mut wire_a, &[]));
  assert_eq!(a.output, &b"*3\r\n$12\r\npunsubscribe\r\n$-1\r\n:0\r\n"[..]);
  a.output.clear();
  assert!(!a.is_subscription);

  // A 订阅两个频道后，同租户旁观者 B 空参退订：仅 null 帧，A 的频道名零泄露
  assert!(a.network_subscribe(&mut wire_a, false, &[b"secret1", b"secret2"]));
  a.output.clear();
  assert!(b.network_unsubscribe(&mut wire_b, &[]));
  assert_eq!(b.output, &b"*3\r\n$11\r\nunsubscribe\r\n$-1\r\n:0\r\n"[..]);
  assert_eq!(wire_a.num_active_channels(), 2, "B 的刺探不得动 A 的订阅");

  // A 持有 2 个普通频道（无模式订阅）时空参 PUNSUBSCRIBE：
  // 仅 null 尾帧，计数 = 真实剩余 2（C# 硬编码 :0 缺陷修复锚点）
  assert!(a.network_punsubscribe(&mut wire_a, &[]));
  assert_eq!(a.output, &b"*3\r\n$12\r\npunsubscribe\r\n$-1\r\n:2\r\n"[..]);
  assert_eq!(wire_a.num_active_channels(), 2);
  assert!(a.is_subscription);

  // A 自身空参 UNSUBSCRIBE：仅回自有频道帧且计数严格递减
  a.output.clear();
  assert!(a.network_unsubscribe(&mut wire_a, &[]));
  let out = String::from_utf8_lossy(&a.output);
  assert_eq!(out.matches("$11\r\nunsubscribe\r\n").count(), 2);
  assert!(out.contains("secret1") && out.contains("secret2"));
  assert!(out.contains("\r\n:1\r\n"), "首帧计数应递减: {out}");
  assert!(out.ends_with(":0\r\n"));
  assert!(!a.is_subscription);
  assert_eq!(wire_a.num_active_channels(), 0);
}
