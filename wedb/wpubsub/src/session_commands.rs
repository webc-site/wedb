//! pub/sub 会话侧命令面与推送编码（对标 libs/server/Resp/PubSubCommands.cs）
//!
//! 集群与单机共用统一的发布订阅底座：
//! - 会话侧状态（broker 引用 + 邮箱 + numActiveChannels）落 [`PubSubSession`]，由宿主按会话持有；
//! - 会话命令协议抽象为 [`PubSubSessionCommands`] trait，解耦底层会话实现（单机/集群）；
//! - 推送编码收敛于 [`PubSubSessionCommands::drain_pubsub_frames`]：邮箱取出 → 按会话协议版本编码推送帧。

use std::sync::Arc;

use wresp::{RespVecExt, cmd_strings as cs};

use crate::{PubSubMailbox, PubSubMessage, PubSubMessageKind, SubscribeBroker};

/// 会话邮箱默认容量（宿主可经 [`PubSubSession::with_mailbox_capacity`] 调整）
pub const DEFAULT_MAILBOX_CAPACITY: usize = 1024;

/// libs/server/Resp/CmdStrings.cs:GenericPubSubCommandDisabled 的四处实例化
/// （`ERR {0} is disabled, enable it with --pubsub option.`）
pub const ERR_PUBLISH_DISABLED: &str = "ERR PUBLISH is disabled, enable it with --pubsub option.";
pub const ERR_SUBSCRIBE_DISABLED: &str =
  "ERR SUBSCRIBE is disabled, enable it with --pubsub option.";
pub const ERR_UNSUBSCRIBE_DISABLED: &str =
  "ERR UNSUBSCRIBE is disabled, enable it with --pubsub option.";
pub const ERR_PUNSUBSCRIBE_DISABLED: &str =
  "ERR PUNSUBSCRIBE is disabled, enable it with --pubsub option.";
pub const ERR_PUBSUB_CHANNELS_DISABLED: &str =
  "ERR PUBSUB CHANNELS is disabled, enable it with --pubsub option.";
pub const ERR_PUBSUB_NUMPAT_DISABLED: &str =
  "ERR PUBSUB NUMPAT is disabled, enable it with --pubsub option.";
pub const ERR_PUBSUB_NUMSUB_DISABLED: &str =
  "ERR PUBSUB NUMSUB is disabled, enable it with --pubsub option.";

/// 会话侧 pub/sub 接线态（C# `subscribeBroker` 字段 + `numActiveChannels`）
///
/// 中枢与邮箱随构造成对建立（邮箱为广播线程 → 会话线程的投递中介）；
/// `None` 承接 C# `subscribeBroker == null`（--pubsub 关闭）形态，命令面
/// 按同款禁用文案回错。
pub struct PubSubSession {
  /// 订阅中枢 + 会话邮箱接线
  wire: Option<(Arc<SubscribeBroker>, Arc<PubSubMailbox>)>,
  /// 活跃订阅数（C# numActiveChannels；SUBSCRIBE 应答尾帧的序号源）
  pub num_active_channels: i32,
}

impl PubSubSession {
  /// 接线到共享中枢（邮箱容量取默认）
  pub fn new(broker: Arc<SubscribeBroker>) -> Self {
    Self::with_mailbox_capacity(Some(broker), DEFAULT_MAILBOX_CAPACITY)
  }

  /// 指定邮箱容量接线；`broker = None` 承接 --pubsub 关闭形态
  pub fn with_mailbox_capacity(broker: Option<Arc<SubscribeBroker>>, capacity: usize) -> Self {
    Self {
      wire: broker.map(|broker| {
        let mailbox = Arc::new(PubSubMailbox::new(capacity));
        (broker, mailbox)
      }),
      num_active_channels: 0,
    }
  }

  /// 订阅中枢视图（None = --pubsub 关闭）
  pub fn broker(&self) -> Option<&SubscribeBroker> {
    self.wire.as_ref().map(|(broker, _)| broker.as_ref())
  }

  /// 中枢视图 + 本会话投递面（订阅/退订路径的共同体；None = --pubsub 关闭）
  pub fn broker_sink(&self) -> Option<(&SubscribeBroker, Arc<PubSubMailbox>)> {
    self
      .wire
      .as_ref()
      .map(|(broker, mailbox)| (broker.as_ref(), mailbox.clone()))
  }

  /// 活跃订阅数（C# numActiveChannels）
  #[inline]
  pub fn num_active_channels(&self) -> i32 {
    self.num_active_channels
  }

  /// 设置活跃订阅数
  #[inline]
  pub fn set_num_active_channels(&mut self, count: i32) {
    self.num_active_channels = count;
  }

  /// 取走全部待投递消息（drain 前置；无接线时为空）
  pub fn drain_mailbox(&self) -> Vec<PubSubMessage> {
    self
      .wire
      .as_ref()
      .map_or_else(Vec::new, |(_, mailbox)| mailbox.drain())
  }
}

/// 退订应答帧共同体：`*3: 头（unsubscribe/punsubscribe）、名、计数`
#[inline]
fn write_unsubscribe_frame(
  output: &mut Vec<u8>,
  active: &mut i32,
  header: &[u8],
  name: &[u8],
  unsubscribed: bool,
) {
  output.write_resp_array_len(3);
  output.write_resp_bulk_string(header);
  output.write_resp_bulk_string(name);
  if unsubscribed {
    *active -= 1;
  }
  output.write_resp_int(i64::from(*active));
}

/// 发布订阅会话命令面抽象接口
///
/// 集群与单机会话通过实现本 trait，共享统一的发布订阅协议应答与状态机行为。
pub trait PubSubSessionCommands {
  /// 会话唯一标识符（SUBSCRIBE 订阅者 ID）
  fn session_id(&self) -> i64;

  /// 可变响应缓冲
  fn output_mut(&mut self) -> &mut Vec<u8>;

  /// 当前会话 RESP 协议版本（2 或 3，默认为 2）
  #[inline]
  fn resp_protocol_version(&self) -> u8 {
    2
  }

  /// 设置当前会话是否处于订阅模式
  fn set_subscription_session(&mut self, is_subscription: bool);

  /// 终止并返回参数数量错误应答（对标 C# AbortWithWrongNumberOfArguments）
  #[inline]
  fn abort_wrong_num_args(&mut self, cmd_name: &str) {
    cs::abort_with_wrong_number_of_arguments(self.output_mut(), cmd_name);
  }

  /// 终止并返回指定错误文案（对标 C# AbortWithErrorMessage）
  #[inline]
  fn abort_error_message(&mut self, message: &str) {
    cs::abort_with_error_message(self.output_mut(), message);
  }

  /// 写入 RESP 空值（RESP3: `_\r\n`，RESP2: `$-1\r\n`）
  #[inline]
  fn write_null(&mut self) {
    if self.resp_protocol_version() >= 3 {
      self.output_mut().extend_from_slice(b"_\r\n");
    } else {
      self.output_mut().write_resp_null();
    }
  }

  /// 写入推送帧长度（RESP3: `>count\r\n`，RESP2: `*count\r\n`）
  #[inline]
  fn write_push_length(&mut self, count: usize) {
    if self.resp_protocol_version() >= 3 {
      let out = self.output_mut();
      out.push(b'>');
      let mut buf = itoa::Buffer::new();
      out.extend_from_slice(buf.format(count).as_bytes());
      out.extend_from_slice(b"\r\n");
    } else {
      self.output_mut().write_resp_array_len(count);
    }
  }

  /// 冲洗/发送并重置输出缓冲（对标 C# SendAndReset）
  #[inline]
  fn send_and_reset(&mut self) {}

  /// libs/server/Resp/PubSubCommands.cs:NetworkSUBSCRIBE
  ///
  /// `shard` 承接同方法体的 SSUBSCRIBE 分支（header 换 `ssubscribe` +
  /// 集群门槛）；shard 请求在单机无集群槽位时恒回集群未启用错误。
  /// 应答帧每通道一条 `*3: "subscribe"、通道、活跃订阅数`（重复订阅不增计数）。
  fn network_subscribe(&mut self, wire: &mut PubSubSession, shard: bool, args: &[&[u8]]) -> bool {
    if args.is_empty() {
      self.abort_wrong_num_args(if shard { "SSUBSCRIBE" } else { "SUBSCRIBE" });
      return true;
    }
    if shard {
      self.abort_error_message(cs::RESP_ERR_GENERIC_CLUSTER_DISABLED);
      return true;
    }
    let Some((broker, sink)) = wire.broker_sink() else {
      cs::write_error_raw(self.output_mut(), ERR_SUBSCRIBE_DISABLED);
      return true;
    };
    let subscriber = self.session_id() as u64;
    let mut active = wire.num_active_channels;
    let out = self.output_mut();
    for channel in args {
      out.write_resp_array_len(3);
      out.write_resp_bulk_string(b"subscribe");
      out.write_resp_bulk_string(channel);

      if broker.subscribe(channel, subscriber, sink.clone()) {
        active += 1;
      }
      out.write_resp_int(i64::from(active));
    }
    wire.num_active_channels = active;
    self.set_subscription_session(true);
    true
  }

  /// libs/server/Resp/PubSubCommands.cs:NetworkPSUBSCRIBE
  ///
  /// 应答帧每模式一条 `*3: "psubscribe"、模式、活跃订阅数`；broker 缺席的
  /// 禁用文案沿用 C# 的 SUBSCRIBE 字面。
  fn network_psubscribe(&mut self, wire: &mut PubSubSession, args: &[&[u8]]) -> bool {
    if args.is_empty() {
      self.abort_wrong_num_args("PSUBSCRIBE");
      return true;
    }
    let Some((broker, sink)) = wire.broker_sink() else {
      cs::write_error_raw(self.output_mut(), ERR_SUBSCRIBE_DISABLED);
      return true;
    };
    let subscriber = self.session_id() as u64;
    let mut active = wire.num_active_channels;
    let out = self.output_mut();
    for pattern in args {
      out.write_resp_array_len(3);
      out.write_resp_bulk_string(b"psubscribe");
      out.write_resp_bulk_string(pattern);

      if broker.pattern_subscribe(pattern, subscriber, sink.clone()) {
        active += 1;
      }
      out.write_resp_int(i64::from(active));
    }
    wire.num_active_channels = active;
    self.set_subscription_session(true);
    true
  }

  /// libs/server/Resp/PubSubCommands.cs:NetworkUNSUBSCRIBE
  ///
  /// 无参形态：C# `ListAllSubscriptions` 遍历全表逐通道回 `*3: "unsubscribe"、通道、计数`；
  /// 全无订阅时回 `*3: "unsubscribe"、null、计数`。
  fn network_unsubscribe(&mut self, wire: &mut PubSubSession, args: &[&[u8]]) -> bool {
    let subscriber = self.session_id() as u64;
    let mut active = wire.num_active_channels;
    match wire.broker() {
      None if args.is_empty() => {
        self.abort_error_message(ERR_UNSUBSCRIBE_DISABLED);
        return true;
      }
      None => {
        cs::write_error_raw(self.output_mut(), ERR_UNSUBSCRIBE_DISABLED);
      }
      Some(broker) if args.is_empty() => {
        let channels = broker.list_all_subscriptions();
        for channel in &channels {
          let removed = broker.unsubscribe(channel, subscriber);
          write_unsubscribe_frame(
            self.output_mut(),
            &mut active,
            b"unsubscribe",
            channel,
            removed,
          );
        }
        if channels.is_empty() {
          self.output_mut().write_resp_array_len(3);
          self.output_mut().write_resp_bulk_string(b"unsubscribe");
          self.write_null();
          self.output_mut().write_resp_int(i64::from(active));
        }
      }
      Some(broker) => {
        for channel in args {
          let removed = broker.unsubscribe(channel, subscriber);
          write_unsubscribe_frame(
            self.output_mut(),
            &mut active,
            b"unsubscribe",
            channel,
            removed,
          );
        }
      }
    }
    wire.num_active_channels = active;
    if wire.num_active_channels == 0 {
      self.set_subscription_session(false);
    }
    true
  }

  /// libs/server/Resp/PubSubCommands.cs:NetworkPUNSUBSCRIBE
  ///
  /// 与 UNSUBSCRIBE 同构（模式表遍历）；全无模式订阅分支的尾帧 C# 恒写 0。
  fn network_punsubscribe(&mut self, wire: &mut PubSubSession, args: &[&[u8]]) -> bool {
    let subscriber = self.session_id() as u64;
    let mut active = wire.num_active_channels;
    match wire.broker() {
      None if args.is_empty() => {
        self.abort_error_message(ERR_PUNSUBSCRIBE_DISABLED);
        return true;
      }
      None => {
        cs::write_error_raw(self.output_mut(), ERR_PUNSUBSCRIBE_DISABLED);
      }
      Some(broker) if args.is_empty() => {
        let patterns = broker.list_all_pattern_subscriptions();
        for pattern in &patterns {
          let removed = broker.pattern_unsubscribe(pattern, subscriber);
          write_unsubscribe_frame(
            self.output_mut(),
            &mut active,
            b"punsubscribe",
            pattern,
            removed,
          );
        }
        if patterns.is_empty() {
          self.output_mut().write_resp_array_len(3);
          self.output_mut().write_resp_bulk_string(b"punsubscribe");
          self.write_null();
          self.output_mut().write_resp_int(0);
        }
      }
      Some(broker) => {
        for pattern in args {
          let removed = broker.pattern_unsubscribe(pattern, subscriber);
          write_unsubscribe_frame(
            self.output_mut(),
            &mut active,
            b"punsubscribe",
            pattern,
            removed,
          );
        }
      }
    }
    wire.num_active_channels = active;
    if wire.num_active_channels == 0 {
      self.set_subscription_session(false);
    }
    true
  }

  /// libs/server/Resp/PubSubCommands.cs:NetworkPUBLISH
  ///
  /// 同步广播并优先写出重入落入本会话邮箱的消息帧，再写出通知订阅者数应答。
  fn network_publish(&mut self, wire: &mut PubSubSession, shard: bool, args: &[&[u8]]) -> bool {
    if args.len() != 2 {
      self.abort_wrong_num_args(if shard { "SPUBLISH" } else { "PUBLISH" });
      return true;
    }
    if shard {
      self.abort_error_message(cs::RESP_ERR_GENERIC_CLUSTER_DISABLED);
      return true;
    }
    let Some(broker) = wire.broker() else {
      cs::write_error_raw(self.output_mut(), ERR_PUBLISH_DISABLED);
      return true;
    };
    let notified = broker.publish_now(args[0], args[1]);
    self.drain_pubsub_frames(wire);
    self.output_mut().write_resp_int(notified as i64);
    true
  }

  /// libs/server/Resp/PubSubCommands.cs:NetworkPUBSUB_CHANNELS
  ///
  /// 应答为通道名数组；带模式参数走 glob 过滤。
  fn network_pubsub_channels(&mut self, wire: &mut PubSubSession, args: &[&[u8]]) -> bool {
    if args.len() > 1 {
      self.abort_wrong_num_args("PUBSUB_CHANNELS");
      return true;
    }
    let Some(broker) = wire.broker() else {
      self.abort_error_message(ERR_PUBSUB_CHANNELS_DISABLED);
      return true;
    };
    let channels = match args.first() {
      Some(&pattern) => broker.get_channels_matching(pattern),
      None => broker.get_channels(),
    };
    let out = self.output_mut();
    out.write_resp_array_len(channels.len());
    for channel in &channels {
      out.write_resp_bulk_string(channel);
    }
    true
  }

  /// libs/server/Resp/PubSubCommands.cs:NetworkPUBSUB_NUMPAT
  fn network_pubsub_numpat(&mut self, wire: &mut PubSubSession, args: &[&[u8]]) -> bool {
    if !args.is_empty() {
      self.abort_wrong_num_args("PUBSUB_NUMPAT");
      return true;
    }
    let Some(broker) = wire.broker() else {
      self.abort_error_message(ERR_PUBSUB_NUMPAT_DISABLED);
      return true;
    };
    self
      .output_mut()
      .write_resp_int(broker.num_pattern_subscriptions() as i64);
    true
  }

  /// libs/server/Resp/PubSubCommands.cs:NetworkPUBSUB_NUMSUB
  fn network_pubsub_numsub(&mut self, wire: &mut PubSubSession, args: &[&[u8]]) -> bool {
    let Some(broker) = wire.broker() else {
      self.abort_error_message(ERR_PUBSUB_NUMSUB_DISABLED);
      return true;
    };
    let out = self.output_mut();
    out.write_resp_array_len(args.len() * 2);
    for channel in args {
      out.write_resp_bulk_string(channel);
      out.write_resp_int(broker.num_subscriptions(channel) as i64);
    }
    true
  }

  /// libs/server/Resp/PubSubCommands.cs:Publish / libs/server/Resp/PubSubCommands.cs:PatternPublish
  ///
  /// 会话推送编码收敛点：邮箱消息按会话协议版本写帧 —— 通道消息 `*3/>3: "message"、通道、负载`，
  /// 模式消息 `*4/>4: "pmessage"、模式、通道、负载`；尾部一次 [`Self::send_and_reset`]。
  fn drain_pubsub_frames(&mut self, wire: &PubSubSession) -> usize {
    let messages = wire.drain_mailbox();
    if messages.is_empty() {
      return 0;
    }
    for message in &messages {
      match message.kind {
        PubSubMessageKind::Channel => {
          self.write_push_length(3);
          let out = self.output_mut();
          out.write_resp_bulk_string(b"message");
          out.write_resp_bulk_string(&message.channel);
          out.write_resp_bulk_string(&message.value);
        }
        PubSubMessageKind::Pattern => {
          self.write_push_length(4);
          let out = self.output_mut();
          out.write_resp_bulk_string(b"pmessage");
          out.write_resp_bulk_string(message.pattern.as_deref().unwrap_or(&[]));
          out.write_resp_bulk_string(&message.channel);
          out.write_resp_bulk_string(&message.value);
        }
      }
    }
    self.send_and_reset();
    messages.len()
  }
}

#[cfg(test)]
mod tests {
  use super::*;

  struct MockSession {
    id: i64,
    output: Vec<u8>,
    protocol_version: u8,
    is_subscription: bool,
  }

  impl MockSession {
    fn new(id: i64) -> Self {
      Self {
        id,
        output: Vec::new(),
        protocol_version: 2,
        is_subscription: false,
      }
    }
  }

  impl PubSubSessionCommands for MockSession {
    fn session_id(&self) -> i64 {
      self.id
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
  }

  #[test]
  fn subscribe_unsubscribe_lifecycle() {
    let broker = Arc::new(SubscribeBroker::new(4096));
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
  fn publish_and_drain_push_frames() {
    let broker = Arc::new(SubscribeBroker::new(4096));
    let mut wire = PubSubSession::new(broker.clone());
    let mut session = MockSession::new(101);

    session.network_subscribe(&mut wire, false, &[b"news"]);
    session.output.clear();

    // 自发布自接收
    assert!(session.network_publish(&mut wire, false, &[b"news", b"hello"]));
    assert_eq!(session.drain_pubsub_frames(&wire), 0); // 已在 publish 内 drain 过
    let out_str = String::from_utf8_lossy(&session.output);
    assert!(out_str.contains("*3\r\n$7\r\nmessage\r\n$4\r\nnews\r\n$5\r\nhello\r\n:1\r\n"));
  }

  #[test]
  fn disabled_pubsub_handling() {
    let mut wire = PubSubSession::with_mailbox_capacity(None, 16);
    let mut session = MockSession::new(102);

    assert!(session.network_subscribe(&mut wire, false, &[b"test"]));
    let out_str = String::from_utf8_lossy(&session.output);
    assert!(out_str.contains(ERR_SUBSCRIBE_DISABLED));
  }
}
