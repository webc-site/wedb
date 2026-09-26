//! pub/sub 会话侧命令面与推送编码（对标 libs/server/Resp/PubSubCommands.cs）
//!
//! 集群与单机共用统一的发布订阅底座：
//! - 会话侧状态（broker 引用 + 邮箱 + numActiveChannels）落 [`PubSubSession`]，由宿主按会话持有；
//! - 会话命令协议抽象为 [`PubSubSessionCommands`] trait，解耦底层会话实现（单机/集群）；
//! - 推送编码收敛于 [`PubSubSessionCommands::drain_pubsub_frames`]：邮箱取出 → 按会话协议版本编码推送帧。

use std::sync::Arc;

use wresp::{
  cmd_strings as cs,
  ext::{RespVecExt, is_resp3},
};

use crate::{
  channel_ns::ChannelNsPrefix,
  subscribe_broker::SubscribeBroker,
  subscriber::{PubSubMailbox, PubSubMessage, PubSubMessageKind},
};

/// 会话邮箱默认水位（慢订阅者待投积压上限，满即拒收丢尾帧；
/// 宿主可经 [`PubSubSession::with_mailbox_capacity`] 调整）
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
  /// 接线到共享中枢（邮箱水位取默认）
  pub fn new(broker: Arc<SubscribeBroker>) -> Self {
    Self::with_mailbox_capacity(Some(broker), DEFAULT_MAILBOX_CAPACITY)
  }

  /// 指定邮箱水位接线；`broker = None` 承接 --pubsub 关闭形态
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

  /// 满水位拒收累计帧数（发布端在邮箱内计数，会话侧直读零分配；
  /// --pubsub 关闭为 0。观测出口：会话 `current_client_view` 并入
  /// ClientView 发布轨，rn14 丢弃计数盲区收口）
  #[inline]
  pub fn dropped(&self) -> u64 {
    self
      .wire
      .as_ref()
      .map_or(0, |(_, mailbox)| mailbox.dropped())
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

/// 订阅族共用骨架（SUBSCRIBE/SSUBSCRIBE/PSUBSCRIBE 三向同构）：
/// 差异参数化——arity 错误词 `$cmd`、分片门 `$shard_gate`、应答帧头 `$prefix`
/// 与 broker 订阅动作 `$sub`（闭包 `(broker, 隔离键, 订阅者, 邮箱) -> 是否新增`）。
/// 禁用臂三命令恒报 "SUBSCRIBE"：C# disabledBroker 硬编码该词
/// （libs/server/Resp/PubSubCommands.cs:208），SSUBSCRIBE/PSUBSCRIBE 同报属忠实对齐，
/// 勿改为精确命令名分叉。`$sess` 为会话自身（trait 默认方法内展用）。
macro_rules! subscribe_family {
  ($sess:expr, $wire:expr, $args:expr, $cmd:expr, $shard_gate:expr, $prefix:expr, $sub:expr) => {{
    if $args.is_empty() {
      $sess.abort_wrong_num_args($cmd);
      return true;
    }
    // C# :168-172 SSUBSCRIBE && clusterSession == null → CLUSTER_DISABLED；
    // 判据复用会话切面单点 has_cluster_session，非分片路径（门恒 false）零改动
    if $shard_gate && !$sess.has_cluster_session() {
      $sess.abort_error_message(cs::RESP_ERR_GENERIC_CLUSTER_DISABLED);
      return true;
    }
    let Some((broker, sink)) = $wire.broker_sink() else {
      $sess.abort_pubsub_command_disabled("SUBSCRIBE");
      return true;
    };
    let subscriber = $sess.session_id() as u64;
    let ns_prefix = ChannelNsPrefix::new($sess.namespace());
    let mut active = $wire.num_active_channels;
    let out = $sess.output_mut();
    // 入 broker 前折叠 ns 隔离键（应答帧回写裸通道/模式名，用户视角无感）
    for channel in $args {
      out.extend_from_slice($prefix);
      out.write_resp_bulk_string(channel);
      let isolated = ns_prefix.isolate(channel);
      if $sub(broker, &isolated, subscriber, &sink) {
        active += 1;
      }
      out.write_resp_int(i64::from(active));
    }
    $wire.num_active_channels = active;
    $sess.set_subscription_session(true);
    true
  }};
}

/// 退订族共用骨架（UNSUBSCRIBE/SUNSUBSCRIBE/PUNSUBSCRIBE 三向同构）：
/// 差异参数化——命令名 `$cmd`、应答帧头 `$prefix`、分片集群门 `$shard_gate`、
/// 全量列举 `$list_all` 与退订 `$remove`；`$sess` 为会话自身（trait 默认方法内展用）。
macro_rules! unsubscribe_family {
  ($sess:expr, $wire:expr, $args:expr, $cmd:expr, $prefix:expr, $shard_gate:expr, $list_all:expr, $remove:expr) => {{
    // 摘订阅前先冲邮箱已落地帧（C# 广播线程经批锁内联直写订阅会话发送器，
    // 退订处理窗内已落地的推送恒先于 unsubscribe ack 出流；rust 推送经邮箱
    // 中转，缺此冲程即令已入列帧延至下一输入批才出——ack 后插帧在 RESP2 裸
    // 数组下与响应流错位。清旗后残帧另由双路等待臂兜底）
    $sess.drain_pubsub_frames($wire);
    // 分片退订恒为分片命令，对齐 C# SSUBSCRIBE/SPUBLISH 集群门
    // （PubSubCommands.cs:168-172/108-112 clusterSession == null → CLUSTER_DISABLED）：
    // 单机未挂集群会话即短路，禁入 broker、不改订阅态，杜绝与 SSUBSCRIBE 语义割裂
    if $shard_gate && !$sess.has_cluster_session() {
      $sess.abort_error_message(cs::RESP_ERR_GENERIC_CLUSTER_DISABLED);
      return true;
    }
    let subscriber = $sess.session_id() as u64;
    let ns_prefix = ChannelNsPrefix::new($sess.namespace());
    let mut active = $wire.num_active_channels;
    match $wire.broker() {
      None if $args.is_empty() => {
        $sess.abort_pubsub_command_disabled($cmd);
        return true;
      }
      None => {
        $sess.abort_pubsub_command_disabled($cmd);
      }
      Some(broker) if $args.is_empty() => {
        // broker 全量键为隔离键：仅触及本 ns 通道/模式，应答回写剥离后的裸名；
        // 仅本会话确曾订阅且退订成功才回帧置位（Redis 规范：他人订阅严禁回帧，
        // 防同租户全域频道名泄露与未订阅频道的虚假退订帧）
        let channels = $list_all(broker);
        let mut owned = false;
        for channel in &channels {
          let Some(raw) = ns_prefix.strip(channel) else {
            continue;
          };
          if $remove(broker, channel, subscriber) {
            owned = true;
            write_unsubscribe_frame($sess.output_mut(), &mut active, $prefix, raw, true);
          }
        }
        if !owned {
          // C# NetworkPUNSUBSCRIBE 同位分支硬编码 TryWriteInt32(0) 冲掉真实活跃数，
          // 此处按 Redis 规范统一回写当前剩余活跃订阅总数（与定参退订尾帧严格同构）
          $sess.write_unsubscribe_null_frame($prefix, active);
        }
      }
      Some(broker) => {
        for channel in $args {
          let isolated = ns_prefix.isolate(channel);
          let removed = $remove(broker, &isolated, subscriber);
          write_unsubscribe_frame($sess.output_mut(), &mut active, $prefix, channel, removed);
        }
      }
    }
    $wire.num_active_channels = active;
    if $wire.num_active_channels == 0 {
      $sess.set_subscription_session(false);
    }
    true
  }};
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

  /// 空参退订零命中的规范尾帧：`*3 + 命令头 + null 名 + 活跃订阅计数`
  ///
  /// libs/server/Resp/PubSubCommands.cs 的 NetworkUNSUBSCRIBE（channels.Count == 0 分支）。
  /// C# NetworkPUNSUBSCRIBE 同位分支硬编码 TryWriteInt32(0) 冲掉真实活跃数，
  /// 此处按 Redis 规范统一回写当前剩余活跃订阅总数（与定参退订尾帧严格同构）
  #[inline]
  fn write_unsubscribe_null_frame(&mut self, prefix: &[u8], active: i32) {
    let resp_version = self.resp_protocol_version();
    let out = self.output_mut();
    out.extend_from_slice(prefix);
    out.write_resp_null_ver(resp_version);
    out.write_resp_int(i64::from(active));
  }

  /// 写入推送帧长度（RESP3: `>count\r\n`，RESP2 降级为 `*count\r\n`）
  #[inline]
  fn write_push_length(&mut self, count: usize) {
    if is_resp3(self.resp_protocol_version()) {
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
    subscribe_family!(
      self,
      wire,
      args,
      if shard { "SSUBSCRIBE" } else { "SUBSCRIBE" },
      shard,
      if shard {
        cs::PUBSUB_SSUBSCRIBE_FRAME_PREFIX
      } else {
        cs::PUBSUB_SUBSCRIBE_FRAME_PREFIX
      },
      |broker: &SubscribeBroker, isolated: &[u8], subscriber: u64, sink: &Arc<PubSubMailbox>| {
        if shard {
          broker.shard_subscribe(isolated, subscriber, sink.clone())
        } else {
          broker.subscribe(isolated, subscriber, sink.clone())
        }
      }
    )
  }

  /// libs/server/Resp/PubSubCommands.cs:NetworkPSUBSCRIBE
  fn network_psubscribe(&mut self, wire: &mut PubSubSession, args: &[&[u8]]) -> bool {
    subscribe_family!(
      self,
      wire,
      args,
      "PSUBSCRIBE",
      false,
      cs::PUBSUB_PSUBSCRIBE_FRAME_PREFIX,
      |broker: &SubscribeBroker, isolated: &[u8], subscriber: u64, sink: &Arc<PubSubMailbox>| {
        broker.pattern_subscribe(isolated, subscriber, sink.clone())
      }
    )
  }

  /// libs/server/Resp/PubSubCommands.cs:NetworkUNSUBSCRIBE
  fn network_unsubscribe(&mut self, wire: &mut PubSubSession, args: &[&[u8]]) -> bool {
    unsubscribe_family!(
      self,
      wire,
      args,
      "UNSUBSCRIBE",
      cs::PUBSUB_UNSUBSCRIBE_FRAME_PREFIX,
      false,
      SubscribeBroker::list_all_subscriptions,
      SubscribeBroker::unsubscribe
    )
  }

  /// 退订分片通道（rust 补全命令，非 C# 转写）：C# 上游无 NetworkSUNSUBSCRIBE，
  /// RespCommand.cs 只有 SSUBSCRIBE（:217），分片订阅后无配套退订，属上游缺口；
  /// rust 补全 SSUBSCRIBE 的配套退订（含无参全退形态），对齐 Redis 标准命令面。
  fn network_sunsubscribe(&mut self, wire: &mut PubSubSession, args: &[&[u8]]) -> bool {
    unsubscribe_family!(
      self,
      wire,
      args,
      "SUNSUBSCRIBE",
      cs::PUBSUB_SUNSUBSCRIBE_FRAME_PREFIX,
      true,
      SubscribeBroker::list_all_shard_subscriptions,
      SubscribeBroker::shard_unsubscribe
    )
  }

  /// libs/server/Resp/PubSubCommands.cs:NetworkPUNSUBSCRIBE
  fn network_punsubscribe(&mut self, wire: &mut PubSubSession, args: &[&[u8]]) -> bool {
    unsubscribe_family!(
      self,
      wire,
      args,
      "PUNSUBSCRIBE",
      cs::PUBSUB_PUNSUBSCRIBE_FRAME_PREFIX,
      false,
      SubscribeBroker::list_all_pattern_subscriptions,
      SubscribeBroker::pattern_unsubscribe
    )
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
      // C# NetworkPUBLISH 不区分 PUBLISH/SPUBLISH，恒写 "PUBLISH is disabled..."
      self.abort_pubsub_command_disabled("PUBLISH");
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
    // 正常投递必命中同前缀键）；推送编码前按本会话前缀剥离还原用户视角通道
    // 名，剥离失败（如 AUTH 换 ns 前的旧租户残留投递）即整帧丢弃，裸隔离键
    // 与跨租户消息体严禁外发（安全丢弃语义，C# 无 namespace 无对位）
    let ns_prefix = ChannelNsPrefix::new(self.namespace());
    let (ch_prefix, pat_prefix, sh_prefix) = if is_resp3(self.resp_protocol_version()) {
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
      // 隔离键剥离失败（他 ns 残留消息/前缀失配）→ 安全丢弃该帧：
      // 严禁把未剥离内部隔离前缀的通道名与跨租户消息体外发给客户端
      let Some(channel) = ns_prefix.strip(&message.channel) else {
        continue;
      };
      match message.kind {
        PubSubMessageKind::Channel => {
          out.extend_from_slice(ch_prefix);
          out.write_resp_bulk_string(channel);
          out.write_resp_bulk_string(&message.value);
        }
        PubSubMessageKind::Pattern => {
          // 模式帧同样安全校验：模式串剥离失败即整帧丢弃（裸隔离键禁外发）
          let Some(pattern) = message.pattern.as_deref().and_then(|p| ns_prefix.strip(p)) else {
            continue;
          };
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

  // r327 部分迁移留地注报：本块原 9 测中 8 测与 MockSession 夹具对 crate
  // 私有面零触达，已逐字迁往 wpubsub/tests/session_commands_unit.rs（零语义）。
  // 仅下测触私有字段 `wire`（取 mailbox）与 `msg_buf`（容量复用断言），
  // 按「不为迁就搬运扩 pub」红线留地原位、一字不动。
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
}
