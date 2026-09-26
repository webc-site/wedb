//! pub/sub 会话面（对标 libs/server/Resp/PubSubCommands.cs：订阅 / 退订 /
//! 发布 / PUBSUB 查询的会话侧路由，wire 为会话自持接线）。

use std::mem;

use wpubsub::session_commands::{PubSubSession, PubSubSessionCommands};
use wresp::command::RespCommand;

use super::core::RespServerSession;

impl RespServerSession {
  /// pub/sub 命令会话侧统一入参（wire 为会话自持接线，参数取解析态）
  pub(super) fn process_pubsub_command(&mut self, cmd: RespCommand, shard: bool) -> bool {
    let store = self.collect_args_store();
    let args = store.views();
    match cmd {
      RespCommand::Subscribe => self.network_subscribe(shard, &args),
      RespCommand::Ssubscribe => self.network_subscribe(true, &args),
      RespCommand::Psubscribe => self.network_psubscribe(&args),
      RespCommand::Unsubscribe => self.network_unsubscribe(&args),
      RespCommand::Sunsubscribe => self.network_sunsubscribe(&args),
      RespCommand::Punsubscribe => self.network_punsubscribe(&args),
      RespCommand::Publish | RespCommand::Spublish => self.network_publish(shard, &args),
      RespCommand::PubsubChannels => self.network_pubsub_channels(&args),
      RespCommand::PubsubNumsub => self.network_pubsub_numsub(&args),
      RespCommand::PubsubNumpat => self.network_pubsub_numpat(&args),
      _ => true,
    }
  }

  /// pub/sub 命令共同骨架：trait 默认实现同时借用会话输出面与自持 wire，
  /// take→调用→归还规避字段级双重借用；占位 wire 零堆分配（Vec::new 不分配）
  fn with_pubsub(&mut self, f: impl FnOnce(&mut Self, &mut PubSubSession) -> bool) -> bool {
    let mut wire = mem::replace(
      &mut self.pubsub,
      PubSubSession::with_mailbox_capacity(None, 0),
    );
    let ok = f(self, &mut wire);
    self.pubsub = wire;
    ok
  }

  /// 订阅通道（C# NetworkSUBSCRIBE / NetworkSSUBSCRIBE；wire 为会话自持接线）
  #[inline]
  pub fn network_subscribe(&mut self, shard: bool, args: &[&[u8]]) -> bool {
    self.with_pubsub(|s, wire| PubSubSessionCommands::network_subscribe(s, wire, shard, args))
  }

  /// 模式订阅（C# NetworkPSUBSCRIBE）
  #[inline]
  pub fn network_psubscribe(&mut self, args: &[&[u8]]) -> bool {
    self.with_pubsub(|s, wire| PubSubSessionCommands::network_psubscribe(s, wire, args))
  }

  /// 退订通道（C# NetworkUNSUBSCRIBE）
  #[inline]
  pub fn network_unsubscribe(&mut self, args: &[&[u8]]) -> bool {
    self.with_pubsub(|s, wire| PubSubSessionCommands::network_unsubscribe(s, wire, args))
  }

  /// 退订分片通道（rust 补全命令，C# 无对位，声明见
  /// wpubsub::session_commands::PubSubSessionCommands::network_sunsubscribe）
  #[inline]
  pub fn network_sunsubscribe(&mut self, args: &[&[u8]]) -> bool {
    self.with_pubsub(|s, wire| PubSubSessionCommands::network_sunsubscribe(s, wire, args))
  }

  /// 退订模式（C# NetworkPUNSUBSCRIBE）
  #[inline]
  pub fn network_punsubscribe(&mut self, args: &[&[u8]]) -> bool {
    self.with_pubsub(|s, wire| PubSubSessionCommands::network_punsubscribe(s, wire, args))
  }

  /// 发布消息（C# NetworkPUBLISH / NetworkSPUBLISH）
  #[inline]
  pub fn network_publish(&mut self, shard: bool, args: &[&[u8]]) -> bool {
    self.with_pubsub(|s, wire| PubSubSessionCommands::network_publish(s, wire, shard, args))
  }

  /// 列出活跃通道（C# NetworkPUBSUB_CHANNELS）
  #[inline]
  pub fn network_pubsub_channels(&mut self, args: &[&[u8]]) -> bool {
    self.with_pubsub(|s, wire| PubSubSessionCommands::network_pubsub_channels(s, wire, args))
  }

  /// 活跃模式订阅数（C# NetworkPUBSUB_NUMPAT）
  #[inline]
  pub fn network_pubsub_numpat(&mut self, args: &[&[u8]]) -> bool {
    self.with_pubsub(|s, wire| PubSubSessionCommands::network_pubsub_numpat(s, wire, args))
  }

  /// 指定通道订阅数（C# NetworkPUBSUB_NUMSUB）
  #[inline]
  pub fn network_pubsub_numsub(&mut self, args: &[&[u8]]) -> bool {
    self.with_pubsub(|s, wire| PubSubSessionCommands::network_pubsub_numsub(s, wire, args))
  }

  /// 会话推送编码收敛点（C# Publish / PatternPublish）
  #[inline]
  pub fn drain_pubsub_frames(&mut self) -> usize {
    let mut wire = mem::replace(
      &mut self.pubsub,
      PubSubSession::with_mailbox_capacity(None, 0),
    );
    let n = PubSubSessionCommands::drain_pubsub_frames(self, &mut wire);
    self.pubsub = wire;
    n
  }
}

impl PubSubSessionCommands for RespServerSession {
  #[inline]
  fn session_id(&self) -> i64 {
    self.id
  }

  /// 消息域隔离键前缀唯一来源：认证绑定的会话 ns（与存储域物理键同口径）
  #[inline]
  fn namespace(&self) -> u64 {
    self.namespace
  }

  #[inline]
  fn output_mut(&mut self) -> &mut Vec<u8> {
    &mut self.output
  }

  #[inline]
  fn resp_protocol_version(&self) -> u8 {
    self.resp_protocol_version
  }

  #[inline]
  fn set_subscription_session(&mut self, is_subscription: bool) {
    self.is_subscription_session = is_subscription;
  }

  #[inline]
  fn abort_wrong_num_args(&mut self, cmd_name: &str) {
    self.abort_wrong_num_args(cmd_name);
  }

  #[inline]
  fn abort_error_message(&mut self, message: &str) {
    self.abort_error_message(message);
  }

  #[inline]
  fn write_null(&mut self) {
    self.write_null();
  }

  #[inline]
  fn write_push_length(&mut self, count: usize) {
    self.write_push_length(count);
  }

  #[inline]
  fn send_and_reset(&mut self) {
    // 托管缓冲模式下由网络消费端统一提取写出，此处保留在 output 中
  }

  #[inline]
  fn has_cluster_session(&self) -> bool {
    self.cluster_session.is_some()
  }

  #[inline]
  fn cluster_publish(&mut self, is_spublish: bool, channel: &[u8], message: &[u8]) {
    let Some(cluster) = &self.cluster_session else {
      return;
    };
    let cmd = if is_spublish {
      RespCommand::Spublish
    } else {
      RespCommand::Publish
    };
    cluster.cluster_publish(cmd, channel, message);
  }
}
