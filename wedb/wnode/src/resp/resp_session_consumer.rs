//! RESP 会话消费者（单机与集群统一装配形态）
//!
//! 对标 C# `GarnetServer` 会话工厂：`IMessageConsumer` 包装同一套
//! `RespServerSession`，集群与单机的差异仅在构造期是否挂接
//! `ClusterSession` 切面（C# 构造函数 clusterProvider == null 与否），
//! 命令消费主循环完全一致。

use std::{mem, sync::Arc};

use wcol::itembroker::collection_item_observer::CollectionItemResult;
use wconf::RuntimeServerConfig;
use wresp::RespCommand;

use super::{
  BlockedWait, ItemBroker,
  garnet_api::GarnetApi,
  resp_server_session::{RespServerSession, RespServerSessionOptions},
  slow_path::SlowWait,
};
use crate::{
  MessageConsumerFace,
  cluster_session::ClusterSession,
  servers::consumer_registry::ConsumerEntry,
};

/// 对应 libs/server/Resp/RespServerSession.cs:RespServerSession 会话网络消费驱动
pub struct RespSessionConsumer {
  /// 会话本体（独占式串行访问，零锁零原子操作）
  session: RespServerSession,
}

impl RespSessionConsumer {
  /// 构造单机形态会话消费者（挂接存储执行域）
  pub fn new(
    network_sender_id: u64,
    options: RespServerSessionOptions,
    garnet_api: impl Into<GarnetApi>,
  ) -> Self {
    let mut session = RespServerSession::new(network_sender_id as i64, options);
    session.set_garnet_api(garnet_api);
    Self { session }
  }

  /// 构造集群形态会话消费者（挂接集群会话切面 + 存储执行域；单机与集群
  /// 同一命令执行路径，差异仅切面驱动槽位门）
  pub fn with_cluster_session(
    network_sender_id: u64,
    options: RespServerSessionOptions,
    cluster_session: impl Into<ClusterSession>,
    garnet_api: impl Into<GarnetApi>,
  ) -> Self {
    let mut session = RespServerSession::new(network_sender_id as i64, options);
    session.attach_cluster_session(cluster_session);
    session.set_garnet_api(garnet_api);
    Self { session }
  }

  /// 注入集合项经纪（服务器级共享；对标 C# storeWrapper.itemBroker，
  /// 阻塞命令经其挂起/唤醒）
  pub fn set_item_broker(&mut self, broker: Arc<ItemBroker>) {
    self.session.set_item_broker(broker);
  }

  /// 注入服务器级运行时配置（对标 C# storeWrapper.runtimeConfig：会话
  /// 共享同一实例，CONFIG SET 即时全服务器生效）
  pub fn set_runtime_config(&mut self, config: Arc<RuntimeServerConfig>) {
    self.session.set_runtime_config(config);
  }

  /// 注入自定义命令注册表（对标 C# storeWrapper.customCommandManager：
  /// RUNTXP 过程体解析与自定义命令族共用）
  pub fn set_custom_command_manager(
    &mut self,
    manager: Arc<parking_lot::Mutex<wcustom::CustomCommandManager>>,
  ) {
    self.session.attach_custom_command_manager(manager);
  }
}

impl MessageConsumerFace for RespSessionConsumer {
  fn try_consume_messages_into(&mut self, req_buffer: &[u8], resp_buf: &mut Vec<u8>) -> usize {
    if req_buffer.is_empty() {
      return 0;
    }
    match self.session.try_consume_messages(req_buffer) {
      Some(consumed) => {
        self.session.take_output_into(resp_buf);
        consumed
      }
      None => 0,
    }
  }

  fn take_recv_scratch(&mut self) -> Option<Vec<u8>> {
    // 会话自有接收缓冲整体移交泵直填（mem::take 占位，归还前缓冲为空壳）
    Some(mem::take(&mut self.session.recv_buffer))
  }

  fn return_recv_scratch(&mut self, buf: Vec<u8>) {
    self.session.recv_buffer = buf;
  }

  fn try_consume_scratch_into(&mut self, resp_buf: &mut Vec<u8>) -> Option<usize> {
    match self.session.try_consume_pending() {
      Some(remaining) => {
        self.session.take_output_into(resp_buf);
        Some(remaining)
      }
      // 协议违规（C# RespParsingException）：应答面丢弃，由泵断连
      None => None,
    }
  }

  fn dispose(&mut self) {
    self.session.dispose();
  }

  fn mirror_session_counters(&mut self, entry: &ConsumerEntry) {
    if let Some(metrics) = &self.session.session_metrics {
      entry.set_commands_processed(metrics.get_total_commands_processed());
    }
  }

  fn take_blocked_wait(&mut self) -> Option<BlockedWait> {
    self.session.take_blocked_wait()
  }

  fn take_slow_wait(&mut self) -> Option<SlowWait> {
    self.session.take_slow_wait()
  }

  fn resolve_blocked_wait(&mut self, cmd: RespCommand, result: CollectionItemResult) -> Vec<u8> {
    self.session.resolve_blocked_wait(cmd, result)
  }

  fn resolve_blocked_wait_into(
    &mut self,
    cmd: RespCommand,
    result: CollectionItemResult,
    resp_buf: &mut Vec<u8>,
  ) {
    self
      .session
      .resolve_blocked_wait_into(cmd, result, resp_buf);
  }
}
