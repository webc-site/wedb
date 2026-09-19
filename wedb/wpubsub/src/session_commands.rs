//! pub/sub 会话侧命令面与推送编码（对标 libs/server/Resp/PubSubCommands.cs）
//!
//! 集群与单机共用统一的发布订阅底座：
//! - 会话侧状态（broker 引用 + 邮箱 + numActiveChannels）落 [`PubSubSession`]，由宿主按会话持有；
//! - 会话命令协议抽象为 [`PubSubSessionCommands`] trait，解耦底层会话实现（单机/集群）；
//! - 推送编码收敛于 [`PubSubSessionCommands::drain_pubsub_frames`]：邮箱取出 → 按会话协议版本编码推送帧。

use std::sync::Arc;

use wresp::{cmd_strings as cs, ext::RespVecExt};

use crate::{
  channel_ns::ChannelNsPrefix,
  subscribe_broker::SubscribeBroker,
  subscriber::{PubSubMailbox, PubSubMessage, PubSubMessageKind},
};

/// 会话邮箱默认容量（宿主可经 [`PubSubSession::with_mailbox_capacity`] 调整）
pub const DEFAULT_MAILBOX_CAPACITY: usize = 1024;

/// 会话侧 pub/sub 接线态（C# `subscribeBroker` 字段 + `numActiveChannels`）
pub struct PubSubSession {
  /// 订阅中枢 + 会话邮箱接线
  wire: Option<(Arc<SubscribeBroker>, Arc<PubSubMailbox>)>,
  /// 活跃订阅数（C# numActiveChannels；SUBSCRIBE 应答尾帧的序号源）
  pub num_active_channels: i32,
  /// 复用的消息缓冲（消除高频推送消费时的临时堆分配）
  msg_buf: Vec<PubSubMessage>,
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
      msg_buf: Vec::new(),
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

  /// 本会话邮箱句柄（None = --pubsub 关闭）
  pub fn mailbox(&self) -> Option<Arc<PubSubMailbox>> {
    self.wire.as_ref().map(|(_, mailbox)| mailbox.clone())
  }

  /// 本会话邮箱累计溢出丢弃数
  pub fn dropped_count(&self) -> Option<u64> {
    self
      .wire
      .as_ref()
      .map(|(_, mailbox)| mailbox.dropped_count())
  }

  /// 活跃订阅数（C# numActiveChannels）
  #[inline]
  pub fn num_active_channels(&self) -> i32 {
    self.num_active_channels
  }

  /// 取走全部待投递消息排入自身缓冲并返回切片（零堆分配复用内部 msg_buf）
  #[inline]
  pub fn drain_mailbox_into(&mut self) -> &[PubSubMessage] {
    self.msg_buf.clear();
    if let Some((_, mailbox)) = &self.wire {
      mailbox.drain_into(&mut self.msg_buf);
    }
    &self.msg_buf
  }
}

/// 退订应答帧共同体：`*3: 头（unsubscribe/punsubscribe/sunsubscribe）、名、计数`
#[inline]
fn write_unsubscribe_frame(
  output: &mut Vec<u8>,
  active: &mut i32,
  prefix: &[u8],
  name: &[u8],
  unsubscribed: bool,
) {
  output.extend_from_slice(prefix);
  output.write_resp_bulk_string(name);
  if unsubscribed {
    *active -= 1;
  }
  output.write_resp_int(i64::from(*active));
}

/// 发布订阅会话命令面抽象接口
pub trait PubSubSessionCommands {
  /// 会话唯一标识符（SUBSCRIBE 订阅者 ID）
  fn session_id(&self) -> i64;

  /// 当前会话命名空间（消息域隔离键前缀唯一来源，wedb 自有面，C# 无对位）
  ///
  /// 认证 `<ns>#用户名` 绑定会话 ns 后，全部通道/模式以
  /// [`ChannelNsPrefix`] 隔离键入 broker，实现与存储域 `[NsVarint]`
  /// 同口径的跨租户订阅隔离；默认 0 承接非多租户宿主与 mock 形态。
  #[inline]
  fn namespace(&self) -> u64 {
    0
  }

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

  /// 终止并返回 `--pubsub` 关闭态错误应答（对标 C#
  /// AbortWithErrorMessage(string.Format(CmdStrings 的禁用模板, 命令名))）
  #[inline]
  fn abort_pubsub_command_disabled(&mut self, cmd_name: &str) {
    cs::abort_with_pubsub_command_disabled(self.output_mut(), cmd_name);
  }

  /// 写入 RESP 空值（RESP3: `_\r\n`，RESP2: `$-1\r\n`）
  /// （版本分派单源在 wresp::ext::RespVecExt::write_resp_null_ver）
  #[inline]
  fn write_null(&mut self) {
    let resp_version = self.resp_protocol_version();
    self.output_mut().write_resp_null_ver(resp_version);
  }

  /// 写入推送帧长度（RESP3: `>count\r\n`，RESP2 降级为 `*count\r\n`）
  #[inline]
  fn write_push_length(&mut self, count: usize) {
    if self.resp_protocol_version() >= 3 {
      self.output_mut().resp_writer3().write_push_length(count);
    } else {
      self.output_mut().resp_writer2().write_push_length(count);
    }
  }

  /// 冲洗/发送并重置输出缓冲（对标 C# SendAndReset）
  #[inline]
  fn send_and_reset(&mut self) {}

  /// 当前会话是否挂接集群会话切面
  #[inline]
  fn has_cluster_session(&self) -> bool {
    false
  }

  /// 集群态 PUBLISH/SPUBLISH 跨节点广播
  ///
  /// `channel` 为 [`network_publish`](Self::network_publish) 折叠的 ns 隔离键：
  /// 集群传输面原样透传，收端以该键直入本地 broker，租户分区随键跨节点贯通
  #[inline]
  fn cluster_publish(&mut self, _is_spublish: bool, _channel: &[u8], _message: &[u8]) {}

  /// libs/server/Resp/PubSubCommands.cs:NetworkSUBSCRIBE / NetworkSSUBSCRIBE
  ///
  /// `shard` 承接同方法体的 SSUBSCRIBE 分支（header 换 `ssubscribe`，底层接入 `shard_subscribe`）。
  fn network_subscribe(&mut self, wire: &mut PubSubSession, shard: bool, args: &[&[u8]]) -> bool {
    if args.is_empty() {
      self.abort_wrong_num_args(if shard { "SSUBSCRIBE" } else { "SUBSCRIBE" });
      return true;
    }
    // C# :168-172 SSUBSCRIBE && clusterSession == null → CLUSTER_DISABLED；
    // 判据复用会话切面单点 has_cluster_session，非 shard 路径零改动
    if shard && !self.has_cluster_session() {
      self.abort_error_message(cs::RESP_ERR_GENERIC_CLUSTER_DISABLED);
      return true;
    }
    let Some((broker, sink)) = wire.broker_sink() else {
      self.abort_pubsub_command_disabled(if shard { "SSUBSCRIBE" } else { "SUBSCRIBE" });
      return true;
    };
    let subscriber = self.session_id() as u64;
    let ns_prefix = ChannelNsPrefix::new(self.namespace());
    let mut active = wire.num_active_channels;
    let out = self.output_mut();
    let prefix = if shard {
      cs::PUBSUB_SSUBSCRIBE_FRAME_PREFIX
    } else {
      cs::PUBSUB_SUBSCRIBE_FRAME_PREFIX
    };
    // 入 broker 前折叠 ns 隔离键（应答帧回写裸通道名，用户视角无感）
    for channel in args {
      out.extend_from_slice(prefix);
      out.write_resp_bulk_string(channel);

      let isolated = ns_prefix.isolate(channel);
      let is_new = if shard {
        broker.shard_subscribe(&isolated, subscriber, sink.clone())
      } else {
        broker.subscribe(&isolated, subscriber, sink.clone())
      };
      if is_new {
        active += 1;
      }
      out.write_resp_int(i64::from(active));
    }
    wire.num_active_channels = active;
    self.set_subscription_session(true);
    true
  }

  /// libs/server/Resp/PubSubCommands.cs:NetworkPSUBSCRIBE
  fn network_psubscribe(&mut self, wire: &mut PubSubSession, args: &[&[u8]]) -> bool {
    if args.is_empty() {
      self.abort_wrong_num_args("PSUBSCRIBE");
      return true;
    }
    let Some((broker, sink)) = wire.broker_sink() else {
      self.abort_pubsub_command_disabled("SUBSCRIBE");
      return true;
    };
    let subscriber = self.session_id() as u64;
    let ns_prefix = ChannelNsPrefix::new(self.namespace());
    let mut active = wire.num_active_channels;
    let out = self.output_mut();
    // 入 broker 前折叠 ns 隔离键（应答帧回写裸模式串，用户视角无感）
    for pattern in args {
      out.extend_from_slice(cs::PUBSUB_PSUBSCRIBE_FRAME_PREFIX);
      out.write_resp_bulk_string(pattern);

      let isolated = ns_prefix.isolate(pattern);
      if broker.pattern_subscribe(&isolated, subscriber, sink.clone()) {
        active += 1;
      }
      out.write_resp_int(i64::from(active));
    }
    wire.num_active_channels = active;
    self.set_subscription_session(true);
    true
  }

  /// libs/server/Resp/PubSubCommands.cs:NetworkUNSUBSCRIBE
  fn network_unsubscribe(&mut self, wire: &mut PubSubSession, args: &[&[u8]]) -> bool {
    let subscriber = self.session_id() as u64;
    let ns_prefix = ChannelNsPrefix::new(self.namespace());
    let mut active = wire.num_active_channels;
    match wire.broker() {
      None if args.is_empty() => {
        self.abort_pubsub_command_disabled("UNSUBSCRIBE");
        return true;
      }
      None => {
        self.abort_pubsub_command_disabled("UNSUBSCRIBE");
      }
      Some(broker) if args.is_empty() => {
        // broker 全量键为隔离键：仅触及本 ns 通道，应答回写剥离后的裸通道名
        let channels = broker.list_all_subscriptions();
        let mut owned = false;
        for channel in &channels {
          if let Some(raw) = ns_prefix.strip(channel) {
            owned = true;
            let removed = broker.unsubscribe(channel, subscriber);
            write_unsubscribe_frame(
              self.output_mut(),
              &mut active,
              cs::PUBSUB_UNSUBSCRIBE_FRAME_PREFIX,
              raw,
              removed,
            );
          }
        }
        if !owned {
          let out = self.output_mut();
          out.extend_from_slice(cs::PUBSUB_UNSUBSCRIBE_FRAME_PREFIX);
          self.write_null();
          self.output_mut().write_resp_int(i64::from(active));
        }
      }
      Some(broker) => {
        for channel in args {
          let isolated = ns_prefix.isolate(channel);
          let removed = broker.unsubscribe(&isolated, subscriber);
          write_unsubscribe_frame(
            self.output_mut(),
            &mut active,
            cs::PUBSUB_UNSUBSCRIBE_FRAME_PREFIX,
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

  /// 退订分片通道（rust 补全命令，非 C# 转写）：C# 上游无 NetworkSUNSUBSCRIBE，
  /// RespCommand.cs 只有 SSUBSCRIBE（:217），分片订阅后无配套退订，属上游缺口；
  /// rust 补全 SSUBSCRIBE 的配套退订（含无参全退形态），对齐 Redis 标准命令面。
  fn network_sunsubscribe(&mut self, wire: &mut PubSubSession, args: &[&[u8]]) -> bool {
    let subscriber = self.session_id() as u64;
    let ns_prefix = ChannelNsPrefix::new(self.namespace());
    let mut active = wire.num_active_channels;
    match wire.broker() {
      None if args.is_empty() => {
        self.abort_pubsub_command_disabled("SUNSUBSCRIBE");
        return true;
      }
      None => {
        self.abort_pubsub_command_disabled("SUNSUBSCRIBE");
      }
      Some(broker) if args.is_empty() => {
        // broker 全量键为隔离键：仅触及本 ns 分片通道，应答回写剥离后的裸通道名
        let channels = broker.list_all_shard_subscriptions();
        let mut owned = false;
        for channel in &channels {
          if let Some(raw) = ns_prefix.strip(channel) {
            owned = true;
            let removed = broker.shard_unsubscribe(channel, subscriber);
            write_unsubscribe_frame(
              self.output_mut(),
              &mut active,
              cs::PUBSUB_SUNSUBSCRIBE_FRAME_PREFIX,
              raw,
              removed,
            );
          }
        }
        if !owned {
          let out = self.output_mut();
          out.extend_from_slice(cs::PUBSUB_SUNSUBSCRIBE_FRAME_PREFIX);
          self.write_null();
          self.output_mut().write_resp_int(i64::from(active));
        }
      }
      Some(broker) => {
        for channel in args {
          let isolated = ns_prefix.isolate(channel);
          let removed = broker.shard_unsubscribe(&isolated, subscriber);
          write_unsubscribe_frame(
            self.output_mut(),
            &mut active,
            cs::PUBSUB_SUNSUBSCRIBE_FRAME_PREFIX,
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
  fn network_punsubscribe(&mut self, wire: &mut PubSubSession, args: &[&[u8]]) -> bool {
    let subscriber = self.session_id() as u64;
    let ns_prefix = ChannelNsPrefix::new(self.namespace());
    let mut active = wire.num_active_channels;
    match wire.broker() {
      None if args.is_empty() => {
        self.abort_pubsub_command_disabled("PUNSUBSCRIBE");
        return true;
      }
      None => {
        self.abort_pubsub_command_disabled("PUNSUBSCRIBE");
      }
      Some(broker) if args.is_empty() => {
        // broker 全量键为隔离键：仅触及本 ns 模式，应答回写剥离后的裸模式串
        let patterns = broker.list_all_pattern_subscriptions();
        let mut owned = false;
        for pattern in &patterns {
          if let Some(raw) = ns_prefix.strip(pattern) {
            owned = true;
            let removed = broker.pattern_unsubscribe(pattern, subscriber);
            write_unsubscribe_frame(
              self.output_mut(),
              &mut active,
              cs::PUBSUB_PUNSUBSCRIBE_FRAME_PREFIX,
              raw,
              removed,
            );
          }
        }
        if !owned {
          let out = self.output_mut();
          out.extend_from_slice(cs::PUBSUB_PUNSUBSCRIBE_FRAME_PREFIX);
          self.write_null();
          self.output_mut().extend_from_slice(cs::RESP_RETURN_VAL_0);
        }
      }
      Some(broker) => {
        for pattern in args {
          let isolated = ns_prefix.isolate(pattern);
          let removed = broker.pattern_unsubscribe(&isolated, subscriber);
          write_unsubscribe_frame(
            self.output_mut(),
            &mut active,
            cs::PUBSUB_PUNSUBSCRIBE_FRAME_PREFIX,
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

  /// libs/server/Resp/PubSubCommands.cs:NetworkPUBLISH / NetworkSPUBLISH
  fn network_publish(&mut self, wire: &mut PubSubSession, shard: bool, args: &[&[u8]]) -> bool {
    if args.len() != 2 {
      self.abort_wrong_num_args(if shard { "SPUBLISH" } else { "PUBLISH" });
      return true;
    }
    let clustered = self.has_cluster_session();
    // C# :108-112 SPUBLISH && clusterSession == null → CLUSTER_DISABLED；
    // 判据复用现取 clustered，非 shard 路径零改动
    if shard && !clustered {
      self.abort_error_message(cs::RESP_ERR_GENERIC_CLUSTER_DISABLED);
      return true;
    }
    let Some(broker) = wire.broker() else {
      self.abort_pubsub_command_disabled(if shard { "SPUBLISH" } else { "PUBLISH" });
      return true;
    };
    // 广播前折叠 ns 隔离键：本地直投与集群跨节点转发共用同一隔离键，
    // 收端 network_cluster_publish 原样以该键入本地 broker，租户分区随键贯通
    let ns_prefix = ChannelNsPrefix::new(self.namespace());
    let isolated = ns_prefix.isolate(args[0]);
    let notified = if shard {
      broker.publish_shard_now(&isolated, args[1])
    } else {
      broker.publish_now(&isolated, args[1])
    };
    self.drain_pubsub_frames(wire);
    if clustered {
      self.cluster_publish(shard, &isolated, args[1]);
    }
    let out = self.output_mut();
    match notified {
      0 => out.extend_from_slice(cs::RESP_RETURN_VAL_0),
      1 => out.extend_from_slice(cs::RESP_RETURN_VAL_1),
      n => out.write_resp_int(n as i64),
    }
    true
  }

  /// libs/server/Resp/PubSubCommands.cs:NetworkPUBSUB_CHANNELS
  fn network_pubsub_channels(&mut self, wire: &mut PubSubSession, args: &[&[u8]]) -> bool {
    if args.len() > 1 {
      self.abort_wrong_num_args("PUBSUB_CHANNELS");
      return true;
    }
    let Some(broker) = wire.broker() else {
      self.abort_pubsub_command_disabled("PUBSUB CHANNELS");
      return true;
    };
    // 隔离前缀过滤至本 ns 并剥回裸通道名（见 broker::write_channels）
    let ns_prefix = ChannelNsPrefix::new(self.namespace());
    broker.write_channels(
      self.output_mut(),
      ns_prefix.as_slice(),
      args.first().copied(),
    );
    true
  }

  /// libs/server/Resp/PubSubCommands.cs:NetworkPUBSUB_NUMPAT
  fn network_pubsub_numpat(&mut self, wire: &mut PubSubSession, args: &[&[u8]]) -> bool {
    if !args.is_empty() {
      self.abort_wrong_num_args("PUBSUB_NUMPAT");
      return true;
    }
    let Some(broker) = wire.broker() else {
      self.abort_pubsub_command_disabled("PUBSUB NUMPAT");
      return true;
    };
    // 模式计数按本 ns 隔离前缀过滤
    let ns_prefix = ChannelNsPrefix::new(self.namespace());
    self
      .output_mut()
      .write_resp_int(broker.num_pattern_subscriptions(ns_prefix.as_slice()) as i64);
    true
  }

  /// libs/server/Resp/PubSubCommands.cs:NetworkPUBSUB_NUMSUB
  fn network_pubsub_numsub(&mut self, wire: &mut PubSubSession, args: &[&[u8]]) -> bool {
    let Some(broker) = wire.broker() else {
      self.abort_pubsub_command_disabled("PUBSUB NUMSUB");
      return true;
    };
    let ns_prefix = ChannelNsPrefix::new(self.namespace());
    let out = self.output_mut();
    out.write_resp_array_len(args.len() * 2);
    for channel in args {
      out.write_resp_bulk_string(channel);
      // 计数以本 ns 隔离键查询，计数天然按租户独立
      let isolated = ns_prefix.isolate(channel);
      out.write_resp_int(broker.num_subscriptions(&isolated) as i64);
    }
    true
  }

  /// libs/server/Resp/PubSubCommands.cs:Publish / libs/server/Resp/PubSubCommands.cs:PatternPublish
  fn drain_pubsub_frames(&mut self, wire: &mut PubSubSession) -> usize {
    let messages = wire.drain_mailbox_into();
    if messages.is_empty() {
      return 0;
    }
    // 邮箱内 channel/pattern 均为 broker 隔离键（订阅即按本会话 ns 前缀入表，
    // 投递必命中同前缀键），推送编码前按本会话前缀剥离，还原用户视角通道名
    let ns_prefix = ChannelNsPrefix::new(self.namespace());
    let (ch_prefix, pat_prefix, sh_prefix) = if self.resp_protocol_version() >= 3 {
      (
        cs::PUBSUB_PUSH_MSG_PREFIX_RESP3,
        cs::PUBSUB_PUSH_PMSG_PREFIX_RESP3,
        cs::PUBSUB_PUSH_SMSG_PREFIX_RESP3,
      )
    } else {
      (
        cs::PUBSUB_PUSH_MSG_PREFIX_RESP2,
        cs::PUBSUB_PUSH_PMSG_PREFIX_RESP2,
        cs::PUBSUB_PUSH_SMSG_PREFIX_RESP2,
      )
    };
    let out = self.output_mut();
    for message in messages {
      let channel = ns_prefix
        .strip(&message.channel)
        .unwrap_or(&message.channel);
      match message.kind {
        PubSubMessageKind::Channel => {
          out.extend_from_slice(ch_prefix);
          out.write_resp_bulk_string(channel);
          out.write_resp_bulk_string(&message.value);
        }
        PubSubMessageKind::Pattern => {
          let pattern = message
            .pattern
            .as_deref()
            .and_then(|p| ns_prefix.strip(p))
            .unwrap_or(&[]);
          out.extend_from_slice(pat_prefix);
          out.write_resp_bulk_string(pattern);
          out.write_resp_bulk_string(channel);
          out.write_resp_bulk_string(&message.value);
        }
        PubSubMessageKind::Shard => {
          out.extend_from_slice(sh_prefix);
          out.write_resp_bulk_string(channel);
          out.write_resp_bulk_string(&message.value);
        }
      }
    }
    let count = messages.len();
    self.send_and_reset();
    count
  }
}

#[cfg(test)]
mod tests {
  use super::*;
  use crate::subscriber::PubSubSink;

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
  fn test_drain_mailbox_into_reuses_capacity() {
    let broker = Arc::new(SubscribeBroker::new());
    let mut wire = PubSubSession::new(broker.clone());
    let mailbox = wire.wire.as_ref().unwrap().1.clone();

    mailbox.publish(b"chan", b"val1");
    mailbox.publish(b"chan", b"val2");

    let msgs = wire.drain_mailbox_into();
    assert_eq!(msgs.len(), 2);
    assert_eq!(msgs[0].value.as_ref(), b"val1");

    let cap_before = wire.msg_buf.capacity();
    assert!(cap_before >= 2);

    mailbox.publish(b"chan", b"val3");
    let msgs2 = wire.drain_mailbox_into();
    assert_eq!(msgs2.len(), 1);
    assert_eq!(msgs2[0].value.as_ref(), b"val3");
    assert_eq!(wire.msg_buf.capacity(), cap_before); // 无重新分配
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
      b"-ERR SPUBLISH is disabled, enable it with --pubsub option.\r\n"
    );
  }

  #[test]
  fn dropped_count_projects_mailbox_overflow() {
    let wire = PubSubSession::with_mailbox_capacity(None, 4);
    assert_eq!(wire.dropped_count(), None);

    let broker = Arc::new(SubscribeBroker::new());
    let wire = PubSubSession::with_mailbox_capacity(Some(broker), 1);
    assert_eq!(wire.dropped_count(), Some(0));
    let mailbox = wire.wire.as_ref().unwrap().1.clone();
    mailbox.publish(b"c1", b"v1");
    mailbox.publish(b"c2", b"v2"); // 满容量丢尾帧
    assert_eq!(wire.dropped_count(), Some(1));
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
  // 不改计数、不转发
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
  }
}
