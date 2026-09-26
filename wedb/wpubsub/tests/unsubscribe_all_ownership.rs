//! 空参 UNSUBSCRIBE / SUNSUBSCRIBE / PUNSUBSCRIBE 收口语义集成测试
//! （对标 Redis 规范退订契约；C# 缺陷锚点：
//! garnet/libs/server/Resp/PubSubCommands.cs:NetworkUNSUBSCRIBE /
//! NetworkPUNSUBSCRIBE 与 garnet/libs/server/PubSub/SubscribeBroker.cs:
//! ListAllSubscriptions —— 列举不校验会话归属、未命中仍盲发退订帧、
//! PUNSUBSCRIBE 零命中尾帧硬编码 :0）
//!
//! 验收 task/ing/wpubsub-unsubscribe-all-channels-leak-and-spurious-frames.md：
//! - 零订阅会话空参退订仅回单条 null 名尾帧（RESP2 `$-1` / RESP3 `_`），计数 0；
//! - 同租户他人频道/模式名零泄露，刺探不动他人订阅状态；
//! - 仅实际退订成功项回帧，活跃计数严格递减，他域/他人订阅不受损；
//! - PUNSUBSCRIBE 零命中尾帧计数 = 真实剩余活跃订阅数（普通/分片频道计入）。

use std::{mem::take, sync::Arc};

use wpubsub::{
  session_commands::{PubSubSession, PubSubSessionCommands},
  subscribe_broker::SubscribeBroker,
};

/// 最小接线会话宿主：仅提供退订收口验收所需的 ID/ns/协议版本/输出/集群门
struct Host {
  id: i64,
  ns: u64,
  resp3: bool,
  output: Vec<u8>,
  subscription_mode: bool,
  clustered: bool,
}

impl Host {
  fn new(id: i64, ns: u64) -> Self {
    Self {
      id,
      ns,
      resp3: false,
      output: Vec::new(),
      subscription_mode: false,
      clustered: false,
    }
  }

  /// 取走并转译当前输出缓冲（断言后即清空视角）
  fn take(&mut self) -> String {
    String::from_utf8_lossy(&take(&mut self.output)).into_owned()
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

  fn resp_protocol_version(&self) -> u8 {
    if self.resp3 { 3 } else { 2 }
  }

  fn set_subscription_session(&mut self, is_subscription: bool) {
    self.subscription_mode = is_subscription;
  }

  fn has_cluster_session(&self) -> bool {
    self.clustered
  }
}

/// 按 `*3\r\n` 切分多帧应答并排序（broker 表迭代序不定，帧集合须按序无关断言）
fn sorted_frames(out: &str) -> Vec<&str> {
  let mut frames: Vec<&str> = out.split("*3\r\n").filter(|f| !f.is_empty()).collect();
  frames.sort_unstable();
  frames
}

/// 零订阅会话空参退订：三类命令各回单条 null 名尾帧且计数 0；RESP3 null 为 `_` 形态
#[test]
fn zero_subscriber_empty_unsubscribe_writes_single_null_frame() {
  let broker = Arc::new(SubscribeBroker::new());
  let mut wire = PubSubSession::new(broker);
  let mut h = Host::new(1, 0);
  // 分片命令面须集群装配（SUNSUBSCRIBE 集群门在途收口，预置集群门免受牵连）
  h.clustered = true;

  assert!(h.network_unsubscribe(&mut wire, &[]));
  assert_eq!(h.take(), "*3\r\n$11\r\nunsubscribe\r\n$-1\r\n:0\r\n");
  assert!(h.network_sunsubscribe(&mut wire, &[]));
  assert_eq!(h.take(), "*3\r\n$12\r\nsunsubscribe\r\n$-1\r\n:0\r\n");
  assert!(h.network_punsubscribe(&mut wire, &[]));
  assert_eq!(h.take(), "*3\r\n$12\r\npunsubscribe\r\n$-1\r\n:0\r\n");
  assert!(!h.subscription_mode);

  // RESP3 同契约：null 名以 `_` 回写
  h.resp3 = true;
  assert!(h.network_unsubscribe(&mut wire, &[]));
  assert_eq!(h.take(), "*3\r\n$11\r\nunsubscribe\r\n_\r\n:0\r\n");
}

/// 同租户旁观者空参退订：仅各回单条 null 帧，他人频道/模式名零外泄且订阅状态不动
#[test]
fn cross_session_names_never_leak_via_empty_unsubscribe() {
  let broker = Arc::new(SubscribeBroker::new());
  let mut wa = PubSubSession::new(broker.clone());
  let mut wb = PubSubSession::new(broker);
  let mut a = Host::new(1, 3);
  let mut b = Host::new(2, 3); // 同租户 ns3 旁观者，零订阅
  b.clustered = true;

  assert!(a.network_subscribe(&mut wa, false, &[b"secret-chan", b"secret-chan2"]));
  assert!(a.network_psubscribe(&mut wa, &[b"secret-pat*"]));
  a.take();

  // B 空参退订三类命令：仅 null 帧，A 的频道与模式名零泄露
  assert!(b.network_unsubscribe(&mut wb, &[]));
  assert_eq!(b.take(), "*3\r\n$11\r\nunsubscribe\r\n$-1\r\n:0\r\n");
  assert!(b.network_punsubscribe(&mut wb, &[]));
  assert_eq!(b.take(), "*3\r\n$12\r\npunsubscribe\r\n$-1\r\n:0\r\n");
  assert!(b.network_sunsubscribe(&mut wb, &[]));
  assert_eq!(b.take(), "*3\r\n$12\r\nsunsubscribe\r\n$-1\r\n:0\r\n");

  // A 的订阅状态完好：发布仍通知本域 1 订阅者（自身），计数未被刺探冲掉
  assert_eq!(wa.num_active_channels(), 3);
  assert!(a.subscription_mode);
  assert!(a.network_publish(&mut wa, false, &[b"secret-chan", b"v"]));
  assert_eq!(
    a.take(),
    "*3\r\n$7\r\nmessage\r\n$11\r\nsecret-chan\r\n$1\r\nv\r\n:1\r\n"
  );
}

/// 空参 UNSUBSCRIBE 仅回自有频道帧且计数严格递减；他域订阅不受损
#[test]
fn empty_unsubscribe_frames_only_owned_and_preserves_others() {
  let broker = Arc::new(SubscribeBroker::new());
  let mut wa = PubSubSession::new(broker.clone());
  let mut wb = PubSubSession::new(broker);
  let mut a = Host::new(1, 1);
  let mut b = Host::new(2, 2); // 他域

  assert!(a.network_subscribe(&mut wa, false, &[b"a1", b"a2"]));
  assert!(b.network_subscribe(&mut wb, false, &[b"b1"]));
  a.take();
  b.take();

  // A 空参全退：仅自有 a1/a2 回帧，计数 1→0 递减，b1 零回显
  assert!(a.network_unsubscribe(&mut wa, &[]));
  let out = a.take();
  let sorted = sorted_frames(&out);
  assert!(
    sorted
      == vec![
        "$11\r\nunsubscribe\r\n$2\r\na1\r\n:1\r\n",
        "$11\r\nunsubscribe\r\n$2\r\na2\r\n:0\r\n",
      ]
      || sorted
        == vec![
          "$11\r\nunsubscribe\r\n$2\r\na1\r\n:0\r\n",
          "$11\r\nunsubscribe\r\n$2\r\na2\r\n:1\r\n",
        ],
    "a1/a2 必须全部退订且计数严格从 1 递减到 0: got {sorted:?}"
  );
  assert!(!out.contains("b1"));
  assert!(!a.subscription_mode);

  // 他域 B 订阅不受损：发布仍自收
  assert!(b.network_publish(&mut wb, false, &[b"b1", b"v"]));
  assert_eq!(
    b.take(),
    "*3\r\n$7\r\nmessage\r\n$2\r\nb1\r\n$1\r\nv\r\n:1\r\n"
  );
}

/// 空参 SUNSUBSCRIBE 仅回自有分片频道帧且计数严格递减；同域他人分片订阅不受损
#[test]
fn empty_sunsubscribe_frames_only_owned_shards() {
  let broker = Arc::new(SubscribeBroker::new());
  let mut wa = PubSubSession::new(broker.clone());
  let mut wb = PubSubSession::new(broker);
  let mut a = Host::new(1, 4);
  let mut b = Host::new(2, 4);
  a.clustered = true;
  b.clustered = true;

  assert!(a.network_subscribe(&mut wa, true, &[b"s1", b"s2"]));
  assert!(b.network_subscribe(&mut wb, true, &[b"s3"]));
  a.take();
  b.take();

  assert!(a.network_sunsubscribe(&mut wa, &[]));
  let out = a.take();
  let sorted = sorted_frames(&out);
  assert!(
    sorted
      == vec![
        "$12\r\nsunsubscribe\r\n$2\r\ns1\r\n:1\r\n",
        "$12\r\nsunsubscribe\r\n$2\r\ns2\r\n:0\r\n",
      ]
      || sorted
        == vec![
          "$12\r\nsunsubscribe\r\n$2\r\ns1\r\n:0\r\n",
          "$12\r\nsunsubscribe\r\n$2\r\ns2\r\n:1\r\n",
        ],
    "s1/s2 必须全部退订且计数严格从 1 递减到 0: got {sorted:?}"
  );
  assert!(!out.contains("s3"));
  assert!(!a.subscription_mode);

  // 同域 B 的分片订阅不受损
  assert!(b.network_publish(&mut wb, true, &[b"s3", b"v"]));
  assert_eq!(
    b.take(),
    "*3\r\n$8\r\nsmessage\r\n$2\r\ns3\r\n$1\r\nv\r\n:1\r\n"
  );
}

/// PUNSUBSCRIBE 零命中尾帧计数 = 真实剩余活跃订阅数（普通/分片计入，非硬编码 :0）
#[test]
fn punsubscribe_zero_hit_frame_counts_remaining_subscriptions() {
  let broker = Arc::new(SubscribeBroker::new());
  let mut wire = PubSubSession::new(broker);
  let mut h = Host::new(7, 0);
  h.clustered = true;

  // 持 2 个普通频道（无模式订阅）时空参 PUNSUBSCRIBE：null 尾帧计数 = 2
  assert!(h.network_subscribe(&mut wire, false, &[b"c1", b"c2"]));
  h.take();
  assert!(h.network_punsubscribe(&mut wire, &[]));
  assert_eq!(h.take(), "*3\r\n$12\r\npunsubscribe\r\n$-1\r\n:2\r\n");
  assert!(h.subscription_mode);
  assert_eq!(wire.num_active_channels(), 2);

  // 再持 1 个分片频道（合计 3）时空参 PUNSUBSCRIBE：计数 = 3（分片计入）
  assert!(h.network_subscribe(&mut wire, true, &[b"s1"]));
  h.take();
  assert!(h.network_punsubscribe(&mut wire, &[]));
  assert_eq!(h.take(), "*3\r\n$12\r\npunsubscribe\r\n$-1\r\n:3\r\n");
  assert_eq!(wire.num_active_channels(), 3);

  // 持 1 个自有模式时空参 PUNSUBSCRIBE：仅回该模式帧，计数含普通/分片订阅 4→3
  assert!(h.network_psubscribe(&mut wire, &[b"p1*"]));
  h.take();
  assert!(h.network_punsubscribe(&mut wire, &[]));
  assert_eq!(h.take(), "*3\r\n$12\r\npunsubscribe\r\n$3\r\np1*\r\n:3\r\n");
  assert_eq!(wire.num_active_channels(), 3);
  assert!(h.subscription_mode);

  // 多自有模式全退：两帧计数连续递减，帧集合序无关成立
  assert!(h.network_psubscribe(&mut wire, &[b"q1*", b"q2*"]));
  h.take();
  assert!(h.network_punsubscribe(&mut wire, &[]));
  let out = h.take();
  assert_eq!(sorted_frames(&out).len(), 2);
  assert!(out.contains("q1*") && out.contains("q2*"));
  assert!(out.ends_with(":3\r\n"), "尾帧计数应落回剩余 3: {out}");
  assert_eq!(wire.num_active_channels(), 3);
}
