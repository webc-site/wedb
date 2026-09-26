//! 退订族「邮箱已落地帧先于 ack 写回」集成测试
//! （对标 garnet/libs/server/PubSub/SubscribeBroker.cs:87/:108 Broadcast 对订阅
//! 会话的同步内联直写 + garnet/libs/server/Resp/RespServerSession.cs:492/:575
//! 整批持锁窗 —— 退订命令处理窗内已开始的 Broadcast 对已落地帧恒即时写出，
//! 绝无「已入列帧延至后续批次」）
//!
//! 验收 task/todo/wnode-unsubscribe-mailed-frame-writeback-gap.md 缺陷甲：
//! rust 推送经邮箱中转，三退订命令体若不先 drain 邮箱，已入列的 message /
//! pmessage / smessage 帧会延到下一输入批的轮头 drain 才出（ack 后插帧，RESP2
//! 裸数组与响应流错位；全退后静默客户端则滞留至连接关闭）。
//!
//! 判据全为确定性结构判据：输出缓冲的字节级顺序（推送帧严格先于退订 ack）、
//! 邮箱在命令返回后恒空（帧确已出列而非延后）。

use std::{mem::take, sync::Arc};

use wpubsub::{
  session_commands::{PubSubSession, PubSubSessionCommands},
  subscribe_broker::SubscribeBroker,
};

/// 最小接线会话宿主：退订族写回点验收只需 ID/ns/协议版本/输出/集群门
struct Host {
  id: i64,
  output: Vec<u8>,
  resp3: bool,
  subscription_mode: bool,
  clustered: bool,
}

impl Host {
  fn new(id: i64) -> Self {
    Self {
      id,
      output: Vec::new(),
      resp3: false,
      subscription_mode: false,
      clustered: false,
    }
  }

  /// 取走当前输出缓冲原文（断言后即清空视角）
  fn take(&mut self) -> Vec<u8> {
    take(&mut self.output)
  }
}

impl PubSubSessionCommands for Host {
  fn session_id(&self) -> i64 {
    self.id
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

/// 定位子序列起点（断帧序时用）
fn find(hay: &[u8], needle: &[u8]) -> usize {
  hay
    .windows(needle.len())
    .position(|w| w == needle)
    .unwrap_or_else(|| {
      panic!(
        "输出缺帧 {needle:?}，实得 {:?}",
        String::from_utf8_lossy(hay)
      )
    })
}

/// 定参 UNSUBSCRIBE：退订末通道前邮箱已落地的 message 帧必须先于 ack 出流，
/// 且命令返回后邮箱确已排空（RESP3 下推送为 `>` push 头、ack 仍 `*3`）
#[test]
fn unsubscribe_flushes_mailed_message_before_ack() {
  for resp3 in [false, true] {
    let broker = Arc::new(SubscribeBroker::new());
    let mut wire = PubSubSession::new(broker.clone());
    let mut h = Host::new(1);
    h.resp3 = resp3;

    assert!(h.network_subscribe(&mut wire, false, &[b"ch"]));
    h.take();
    // 广播线程在本会话邮箱已落地的帧（ns 0 隔离键，等价 Publish 直投）
    assert_eq!(broker.publish_now(b"0:ch", b"m1"), 1);
    let mailbox = wire.mailbox().expect("接线邮箱在场");
    assert!(mailbox.has_messages(), "前置：帧确已入邮箱");

    assert!(h.network_unsubscribe(&mut wire, &[b"ch"]));
    let out = h.take();
    let msg_head: &[u8] = if resp3 {
      b">3\r\n$7\r\nmessage\r\n$2\r\nch\r\n$2\r\nm1\r\n"
    } else {
      b"*3\r\n$7\r\nmessage\r\n$2\r\nch\r\n$2\r\nm1\r\n"
    };
    // 字节级严格顺序：推送帧整体先于 unsubscribe ack
    assert_eq!(
      &out[..msg_head.len()],
      msg_head,
      "RESP3={resp3} 已落地帧未先于 ack 写出"
    );
    assert!(
      out[msg_head.len()..].starts_with(b"*3\r\n$11\r\nunsubscribe\r\n$2\r\nch\r\n:0\r\n"),
      "RESP3={resp3} ack 未紧随推送帧，实得 {:?}",
      String::from_utf8_lossy(&out)
    );
    assert!(
      !mailbox.has_messages(),
      "RESP3={resp3} 退订返回后邮箱仍有残帧（帧被延后而非写出）"
    );
  }
}

/// 空参 UNSUBSCRIBE（全退形态）同点：自有频道退订帧前先冲邮箱残帧
#[test]
fn empty_unsubscribe_flushes_mailed_message_before_frames() {
  let broker = Arc::new(SubscribeBroker::new());
  let mut wire = PubSubSession::new(broker.clone());
  let mut h = Host::new(2);
  assert!(h.network_subscribe(&mut wire, false, &[b"ch"]));
  h.take();
  assert_eq!(broker.publish_now(b"0:ch", b"m1"), 1);

  assert!(h.network_unsubscribe(&mut wire, &[]));
  let out = h.take();
  let msg = find(&out, b"*3\r\n$7\r\nmessage\r\n");
  let unsub = find(&out, b"*3\r\n$11\r\nunsubscribe\r\n");
  assert!(
    msg < unsub,
    "空参退订未先冲邮箱残帧，实得 {:?}",
    String::from_utf8_lossy(&out)
  );
}

/// PUNSUBSCRIBE：模式帧（pmessage）先于 punsubscribe ack 出流
#[test]
fn punsubscribe_flushes_mailed_pmessage_before_ack() {
  let broker = Arc::new(SubscribeBroker::new());
  let mut wire = PubSubSession::new(broker.clone());
  let mut h = Host::new(3);
  assert!(h.network_psubscribe(&mut wire, &[b"ch*"]));
  h.take();
  assert_eq!(broker.publish_now(b"0:ch1", b"m1"), 1);

  assert!(h.network_punsubscribe(&mut wire, &[b"ch*"]));
  let out = h.take();
  assert!(
    out.starts_with(
      b"*4\r\n$8\r\npmessage\r\n$3\r\nch*\r\n$3\r\nch1\r\n$2\r\nm1\r\n*3\r\n$12\r\npunsubscribe\r\n$3\r\nch*\r\n:0\r\n"
    ),
    "模式残帧未先于 punsubscribe ack，实得 {:?}",
    String::from_utf8_lossy(&out)
  );
  assert!(
    !wire.mailbox().expect("邮箱在场").has_messages(),
    "退订返回后邮箱仍有残帧"
  );
}

/// SUNSUBSCRIBE：分片帧（smessage）先于 sunsubscribe ack 出流
#[test]
fn sunsubscribe_flushes_mailed_smessage_before_ack() {
  let broker = Arc::new(SubscribeBroker::new());
  let mut wire = PubSubSession::new(broker.clone());
  let mut h = Host::new(4);
  // 分片命令面须集群装配（过 SUNSUBSCRIBE/SSUBSCRIBE 集群门）
  h.clustered = true;
  assert!(h.network_subscribe(&mut wire, true, &[b"sh"]));
  h.take();
  assert_eq!(broker.publish_shard_now(b"0:sh", b"m1"), 1);

  assert!(h.network_sunsubscribe(&mut wire, &[b"sh"]));
  let out = h.take();
  assert!(
    out.starts_with(
      b"*3\r\n$8\r\nsmessage\r\n$2\r\nsh\r\n$2\r\nm1\r\n*3\r\n$12\r\nsunsubscribe\r\n$2\r\nsh\r\n:0\r\n"
    ),
    "分片残帧未先于 sunsubscribe ack，实得 {:?}",
    String::from_utf8_lossy(&out)
  );
}

/// 多帧积压同样一次冲净：两帧严格先于 ack、ack 尾计数不受推送帧影响
#[test]
fn unsubscribe_flushes_whole_backlog_before_ack() {
  let broker = Arc::new(SubscribeBroker::new());
  let mut wire = PubSubSession::new(broker.clone());
  let mut h = Host::new(5);
  assert!(h.network_subscribe(&mut wire, false, &[b"ch"]));
  h.take();
  assert_eq!(broker.publish_now(b"0:ch", b"m1"), 1);
  assert_eq!(broker.publish_now(b"0:ch", b"m2"), 1);

  assert!(h.network_unsubscribe(&mut wire, &[b"ch"]));
  let out = h.take();
  let ack = find(&out, b"*3\r\n$11\r\nunsubscribe\r\n");
  assert_eq!(
    &out[..ack],
    b"*3\r\n$7\r\nmessage\r\n$2\r\nch\r\n$2\r\nm1\r\n*3\r\n$7\r\nmessage\r\n$2\r\nch\r\n$2\r\nm2\r\n",
    "积压帧未整批先于 ack 冲出"
  );
  assert!(out[ack..].starts_with(b"*3\r\n$11\r\nunsubscribe\r\n$2\r\nch\r\n:0\r\n"));
}
