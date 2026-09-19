//! pub/sub 通道 namespace 隔离集成测试（wedb 自有架构验收，C# 无对位）
//!
//! 验收 task/ing/pubsub-namespace-isolation.md：
//! - 不同 ns 两会话 SUBSCRIBE 同名通道，一侧 PUBLISH 对侧不可见，推送帧与
//!   退订帧通道名均为剥离后的裸通道（用户视角无感）；
//! - PUBSUB CHANNELS / NUMSUB / NUMPAT 按 ns 独立过滤计数；
//! - 同 ns 同名通道对正常互通；
//! - 模式订阅经隔离键后 glob 语义不被前缀污染（含数字前缀歧义对抗用例）；
//! - 分片（SSUBSCRIBE/SPUBLISH）行为保留且同域隔离；
//! - 集群转发面携带隔离键（传输面不改，租户分区随键跨节点贯通）。

use std::{mem::take, sync::Arc};

use wpubsub::{
  channel_ns::ChannelNsPrefix,
  session_commands::{PubSubSession, PubSubSessionCommands},
  subscribe_broker::SubscribeBroker,
};

/// 最小接线会话宿主：仅提供隔离验收所需的 ID/ns/输出/集群门面对
struct Host {
  id: i64,
  ns: u64,
  output: Vec<u8>,
  subscription_mode: bool,
  clustered: bool,
  forwarded: Vec<Vec<u8>>,
}

impl Host {
  fn new(id: i64, ns: u64) -> Self {
    Self {
      id,
      ns,
      output: Vec::new(),
      subscription_mode: false,
      clustered: false,
      forwarded: Vec::new(),
    }
  }

  /// 取走并转译当前输出缓冲（断言后即清空视角）
  fn take(&mut self) -> String {
    String::from_utf8_lossy(&take(&mut self.output)).into_owned()
  }

  /// 本域（隔离前缀命中）通道订阅计数：PUBSUB NUMSUB 会话面口径
  fn domain_channel_subscribers(&self, broker: &SubscribeBroker) -> usize {
    let prefix = ChannelNsPrefix::new(self.ns);
    let mut count = 0;
    broker.for_each_channel(|channel| {
      if prefix.strip(channel).is_some() {
        count += broker.num_subscriptions(channel);
      }
    });
    count
  }
}

impl PubSubSessionCommands for Host {
  fn session_id(&self) -> i64 {
    self.id
  }

  fn namespace(&self) -> u64 {
    self.ns
  }

  fn output_mut(&mut self) -> &mut Vec<u8> {
    &mut self.output
  }

  fn set_subscription_session(&mut self, is_subscription: bool) {
    self.subscription_mode = is_subscription;
  }

  fn has_cluster_session(&self) -> bool {
    self.clustered
  }

  fn cluster_publish(&mut self, _is_spublish: bool, channel: &[u8], _message: &[u8]) {
    self.forwarded.push(channel.to_vec());
  }
}

/// 同 broker 双 ns 会话对接（单节点多租户形态）
fn pair(
  ns_a: u64,
  ns_b: u64,
) -> (
  Arc<SubscribeBroker>,
  PubSubSession,
  Host,
  PubSubSession,
  Host,
) {
  let broker = Arc::new(SubscribeBroker::new());
  let wire_a = PubSubSession::new(broker.clone());
  let wire_b = PubSubSession::new(broker.clone());
  (
    broker,
    wire_a,
    Host::new(1, ns_a),
    wire_b,
    Host::new(2, ns_b),
  )
}

#[test]
fn cross_namespace_publish_is_invisible() {
  let (_broker, mut wa, mut a, mut wb, mut b) = pair(1, 2);

  assert!(a.network_subscribe(&mut wa, false, &[b"news"]));
  assert!(b.network_subscribe(&mut wb, false, &[b"news"]));
  // 订阅应答帧回写裸通道名（前缀对用户不可见）
  assert_eq!(a.take(), "*3\r\n$9\r\nsubscribe\r\n$4\r\nnews\r\n:1\r\n");
  assert_eq!(b.take(), "*3\r\n$9\r\nsubscribe\r\n$4\r\nnews\r\n:1\r\n");

  // A(ns1) 发布：通知数仅本域订阅者 1，推送帧通道名为裸通道（publish 内已 drain）
  assert!(a.network_publish(&mut wa, false, &[b"news", b"ping-a"]));
  assert_eq!(
    a.take(),
    "*3\r\n$7\r\nmessage\r\n$4\r\nnews\r\n$6\r\nping-a\r\n:1\r\n"
  );
  // B(ns2) 零接收
  assert_eq!(b.drain_pubsub_frames(&mut wb), 0);
  assert!(b.take().is_empty());

  // B(ns2) 发布方向对称：A 同样不可见
  assert!(b.network_publish(&mut wb, false, &[b"news", b"ping-b"]));
  assert_eq!(
    b.take(),
    "*3\r\n$7\r\nmessage\r\n$4\r\nnews\r\n$6\r\nping-b\r\n:1\r\n"
  );
  assert_eq!(a.drain_pubsub_frames(&mut wa), 0);
  assert!(a.take().is_empty());
}

#[test]
fn same_namespace_pair_communicates() {
  let (_broker, mut wa, mut a, mut wb, mut b) = pair(7, 7);

  a.network_subscribe(&mut wa, false, &[b"news"]);
  b.network_subscribe(&mut wb, false, &[b"news"]);
  a.take();
  b.take();

  // 同域双订阅者：通知数 2，双方各收一份裸通道名推送帧
  assert!(a.network_publish(&mut wa, false, &[b"news", b"v"]));
  assert_eq!(
    a.take(),
    "*3\r\n$7\r\nmessage\r\n$4\r\nnews\r\n$1\r\nv\r\n:2\r\n"
  );
  assert_eq!(b.drain_pubsub_frames(&mut wb), 1);
  assert_eq!(b.take(), "*3\r\n$7\r\nmessage\r\n$4\r\nnews\r\n$1\r\nv\r\n");
}

#[test]
fn pubsub_queries_are_namespace_scoped() {
  let (_broker, mut wa, mut a, mut wb, mut b) = pair(11, 22);

  a.network_subscribe(&mut wa, false, &[b"news", b"sport"]); // ns11 独有 sport
  b.network_subscribe(&mut wb, false, &[b"news"]); // ns22 仅 news
  b.network_psubscribe(&mut wb, &[b"sp*"]); // ns22 模式订阅
  a.take();
  b.take();

  // CHANNELS：各只见本域通道，剥离前缀还原裸名
  b.network_pubsub_channels(&mut wb, &[]);
  assert_eq!(b.take(), "*1\r\n$4\r\nnews\r\n");
  a.network_pubsub_channels(&mut wa, &[]);
  let out = a.take();
  assert!(out.starts_with("*2\r\n"), "ns11 应见 2 通道: {out}");
  assert!(out.contains("$4\r\nnews\r\n") && out.contains("$5\r\nsport\r\n"));

  // CHANNELS 用户 glob 作用于裸通道名（不带前缀视角）
  a.network_pubsub_channels(&mut wa, &[b"spo*"]);
  assert_eq!(a.take(), "*1\r\n$5\r\nsport\r\n");

  // NUMSUB：同名通道计数按 ns 独立（本域计数不含他域订阅者）
  a.network_pubsub_numsub(&mut wa, &[b"news", b"sport"]);
  assert_eq!(a.take(), "*4\r\n$4\r\nnews\r\n:1\r\n$5\r\nsport\r\n:1\r\n");
  b.network_pubsub_numsub(&mut wb, &[b"news", b"sport"]);
  assert_eq!(b.take(), "*4\r\n$4\r\nnews\r\n:1\r\n$5\r\nsport\r\n:0\r\n");

  // NUMPAT：模式计数按 ns 独立
  b.network_pubsub_numpat(&mut wb, &[]);
  assert_eq!(b.take(), ":1\r\n");
  a.network_pubsub_numpat(&mut wa, &[]);
  assert_eq!(a.take(), ":0\r\n");
}

#[test]
fn pattern_isolation_is_glob_safe() {
  // 对抗用例：ns4 裸模式 "2*"（隔离键 "4:2*"）与 ns42 裸通道/模式 "x"
  //（隔离键 "42:x"）在无定界符编码下存在数字前导歧义，隔离键必不互撞
  let (_broker, mut wa, mut a, mut wb, mut b) = pair(4, 42);

  a.network_psubscribe(&mut wa, &[b"2*"]); // ns4 模式
  b.network_psubscribe(&mut wb, &[b"x"]); // ns42 模式
  a.network_subscribe(&mut wa, false, &[b"2x"]); // ns4 通道
  b.network_subscribe(&mut wb, false, &[b"x"]); // ns42 通道
  a.take();
  b.take();

  // ns42 发布 "x"：仅本域通道订阅者 + 模式订阅者（b 自身 2 份，publish 内 drain），
  // ns4 的 "4:2*" 模式与 "42:x" 键零命中
  assert!(b.network_publish(&mut wb, false, &[b"x", b"v42"]));
  let out = b.take();
  assert!(out.ends_with(":2\r\n"));
  assert!(out.contains("*3\r\n$7\r\nmessage\r\n$1\r\nx\r\n$3\r\nv42\r\n"));
  assert!(
    out.contains("*4\r\n$8\r\npmessage\r\n$1\r\nx\r\n$1\r\nx\r\n$3\r\nv42\r\n"),
    "pmessage 应回显裸模式与裸通道: {out}"
  );
  assert_eq!(a.drain_pubsub_frames(&mut wa), 0);
  assert!(a.take().is_empty());

  // ns4 发布 "2x"：仅 ns4 通道订阅 + 模式 "2*" 命中（pmessage 回显裸名），
  // ns42 模式 "x" 键 "42:x" 必不匹配 "4:2x"
  assert!(a.network_publish(&mut wa, false, &[b"2x", b"v4"]));
  let out = a.take();
  assert!(out.ends_with(":2\r\n"));
  assert!(
    out.contains("*4\r\n$8\r\npmessage\r\n$2\r\n2*\r\n$2\r\n2x\r\n$2\r\nv4\r\n"),
    "本域模式应命中且回显裸名: {out}"
  );
  assert_eq!(b.drain_pubsub_frames(&mut wb), 0);
  assert!(b.take().is_empty());
}

#[test]
fn unsubscribe_is_namespace_scoped() {
  let (broker, mut wa, mut a, mut wb, mut b) = pair(1, 2);

  a.network_subscribe(&mut wa, false, &[b"news"]);
  b.network_subscribe(&mut wb, false, &[b"news"]);
  assert!(a.subscription_mode && b.subscription_mode);
  a.take();
  b.take();

  // 定向退订仅作用于本 ns 键，应答回写裸通道名
  assert!(a.network_unsubscribe(&mut wa, &[b"news"]));
  assert_eq!(a.take(), "*3\r\n$11\r\nunsubscribe\r\n$4\r\nnews\r\n:0\r\n");
  assert_eq!(b.domain_channel_subscribers(&broker), 1);
  assert_eq!(a.domain_channel_subscribers(&broker), 0);

  // 同域内他人通道不受损：B(ns2) 发布仍自收
  assert!(b.network_publish(&mut wb, false, &[b"news", b"still"]));
  assert_eq!(
    b.take(),
    "*3\r\n$7\r\nmessage\r\n$4\r\nnews\r\n$5\r\nstill\r\n:1\r\n"
  );

  // 空参全退只清本域：他 ns 订阅保留
  a.network_subscribe(&mut wa, false, &[b"x"]);
  a.take();
  assert!(a.network_unsubscribe(&mut wa, &[]));
  assert_eq!(a.take(), "*3\r\n$11\r\nunsubscribe\r\n$1\r\nx\r\n:0\r\n");
  assert!(!a.subscription_mode);
  assert_eq!(b.domain_channel_subscribers(&broker), 1);
}

#[test]
fn shard_channel_behavior_preserved_and_isolated() {
  let (_broker, mut wa, mut a, mut wb, mut b) = pair(5, 6);
  // 分片命令须集群装配才达订阅/广播路径（C# 门语义，不随本改动变化）
  a.clustered = true;
  b.clustered = true;

  assert!(a.network_subscribe(&mut wa, true, &[b"slot1"]));
  assert!(b.network_subscribe(&mut wb, true, &[b"slot1"]));
  assert_eq!(a.take(), "*3\r\n$10\r\nssubscribe\r\n$5\r\nslot1\r\n:1\r\n");
  assert_eq!(b.take(), "*3\r\n$10\r\nssubscribe\r\n$5\r\nslot1\r\n:1\r\n");

  // SPUBLISH 仅本域投递（smessage 帧 + 计数 1），他域零接收
  assert!(a.network_publish(&mut wa, true, &[b"slot1", b"s"]));
  assert_eq!(
    a.take(),
    "*3\r\n$8\r\nsmessage\r\n$5\r\nslot1\r\n$1\r\ns\r\n:1\r\n"
  );
  assert_eq!(b.drain_pubsub_frames(&mut wb), 0);
  assert!(b.take().is_empty());

  // 路由隔离保留：A 仅持有 slot1 分片订阅，普通 PUBLISH slot1 通知数 0
  assert!(a.network_publish(&mut wa, false, &[b"slot1", b"c"]));
  assert_eq!(a.take(), ":0\r\n");

  // SUNSUBSCRIBE 定向退订走本域键
  assert!(a.network_sunsubscribe(&mut wa, &[b"slot1"]));
  assert_eq!(
    a.take(),
    "*3\r\n$12\r\nsunsubscribe\r\n$5\r\nslot1\r\n:0\r\n"
  );
}

#[test]
fn cluster_broadcast_carries_isolated_key() {
  let broker = Arc::new(SubscribeBroker::new());
  let mut wire = PubSubSession::new(broker);
  let mut h = Host::new(9, 5);
  // 广播前折叠：转发面的通道即隔离键（对端原样以该键入其本地 broker）
  h.clustered = true;

  assert!(h.network_publish(&mut wire, false, &[b"news", b"v"]));
  assert_eq!(h.forwarded, vec![ChannelNsPrefix::new(5).isolate(b"news")]);
  assert_eq!(h.forwarded[0], b"5:news");
  h.take();

  // SPUBLISH 同样携带隔离键
  assert!(h.network_publish(&mut wire, true, &[b"slot", b"v"]));
  assert_eq!(h.forwarded.last(), Some(&b"5:slot".to_vec()));
}
