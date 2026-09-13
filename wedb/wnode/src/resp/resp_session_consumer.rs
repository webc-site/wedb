//! RESP 会话消费者（单机与集群统一装配形态）
//!
//! 对标 C# `GarnetServer` 会话工厂：`IMessageConsumer` 包装同一套
//! `RespServerSession`，集群与单机的差异仅在构造期是否挂接
//! `ClusterSession` 切面（C# 构造函数 clusterProvider == null 与否），
//! 命令消费主循环完全一致。

use std::sync::Arc;

use wconf::RuntimeServerConfig;
use wobject::itembroker::collection_item_observer::CollectionItemResult;
use wresp::RespCommand;

use super::{
  garnet_api::GarnetApi,
  resp_server_session::{RespServerSession, RespServerSessionOptions},
  slow_path::SlowWait,
};
use crate::{BlockedWait, ItemBroker, MessageConsumerFace, cluster_session::ClusterSession};

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
}

impl MessageConsumerFace for RespSessionConsumer {
  fn try_consume_messages(&mut self, req_buffer: &[u8]) -> (usize, Vec<u8>) {
    if req_buffer.is_empty() {
      return (0, Vec::new());
    }
    match self.session.try_consume_messages(req_buffer) {
      // ProcessMessages 尾部 output 经 take_output 提取（C# 写网络的托管等价）
      Some(consumed) => (consumed, self.session.take_output()),
      // 协议违规（C# RespParsingException）：不消费字节，由网络层断连
      None => (0, Vec::new()),
    }
  }

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

  fn dispose(&mut self) {
    self.session.dispose();
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
