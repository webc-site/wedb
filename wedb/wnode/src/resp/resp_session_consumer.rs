//! RESP 会话消费者（单机与集群统一装配形态）
//!
//! 对标 C# `GarnetServer` 会话工厂：`IMessageConsumer` 包装同一套
//! `RespServerSession`，集群与单机的差异仅在构造期是否挂接
//! `ClusterSession` 切面（C# 构造函数 clusterProvider == null 与否），
//! 命令消费主循环完全一致。

use std::{mem, sync::Arc};

use parking_lot::Mutex;
use wacl::{
  GarnetAclAuthenticator, auth::settings::acl_authentication_settings::AclAuthenticationSettings,
};
use wcol::itembroker::collection_item_observer::CollectionItemResult;
use wconf::RuntimeServerConfig;
use wcustom::SharedCustomCommandManager;
use wmetric::SlowLogContainer;
use wpubsub::{PubSubMailbox, SubscribeBroker};
use wresp::RespCommand;
use wtxn::WatchVersionMap;

use super::{
  BlockedWait, ItemBroker,
  garnet_api::GarnetApi,
  resp_server_session::{RespServerSession, RespServerSessionOptions},
  slow_path::SlowWait,
};
use crate::{
  MessageConsumerFace, cluster_session::ClusterSession, servers::consumer_registry::ConsumerEntry,
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

  /// 注入发布订阅中枢（对标 C# 构造函数 `subscribeBroker` 装配；
  /// 服务器级共享单例，PUBLISH/SUBSCRIBE 族命令与推送投递的接线源）
  pub fn attach_pubsub(&mut self, broker: Arc<SubscribeBroker>) {
    self.session.attach_pubsub(broker);
  }

  /// 注入事务组件（对标 C# `new TransactionManager(storeWrapper.watchversionMap, ...)`）
  pub fn attach_transaction_components(&mut self, watch_version_map: Arc<WatchVersionMap>) {
    self
      .session
      .attach_transaction_components(watch_version_map);
  }

  /// 注入服务器级运行时配置（对标 C# storeWrapper.runtimeConfig：会话
  /// 共享同一实例，CONFIG SET 即时全服务器生效）
  pub fn set_runtime_config(&mut self, config: Arc<RuntimeServerConfig>) {
    self.session.set_runtime_config(config);
  }

  /// 注入慢日志容器（对标 C# StoreWrapper.cs:243 slowLogContainer 装配：
  /// 服务器级共享，容量启动期定死）
  pub fn set_slow_log_container(&mut self, container: Arc<SlowLogContainer>) {
    self.session.set_slow_log_container(container);
  }

  /// 注入自定义命令注册表（对标 C# storeWrapper.customCommandManager：
  /// RUNTXP 过程体解析与自定义命令族共用）
  pub fn set_custom_command_manager(&mut self, manager: SharedCustomCommandManager) {
    self.session.attach_custom_command_manager(manager);
  }

  /// 注入 ACL 认证器与设置（直接委托内部 self.session.attach_acl）
  pub fn attach_acl(
    &mut self,
    authenticator: Option<Arc<Mutex<GarnetAclAuthenticator>>>,
    settings: Option<Arc<AclAuthenticationSettings>>,
  ) {
    self.session.attach_acl(authenticator, settings);
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
      // 协议违规（C# RespParsingException → catch 块）：先放行累积应答
      //（含同批此前命令应答 + ERR Protocol Error，C# Send 顺序），
      // 由调用方发出后断连
      None => {
        self.session.take_output_into(resp_buf);
        0
      }
    }
  }

  /// 致命断流信号转发（切面 GarnetException clientResponse:false 投影；
  /// 主路径为 scratch 形态 None 通道，此处为回退形态兜底）
  fn take_fatal_disconnect(&mut self) -> bool {
    self.session.fatal_disconnect
  }

  /// 会话待释放哨兵转发（QUIT → toDispose；泵发尽应答后断连）
  fn take_dispose_request(&mut self) -> bool {
    self.session.take_dispose_request()
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
      // 协议违规（C# RespParsingException → catch 块）：先放行累积应答
      //（含同批此前命令应答 + ERR Protocol Error，对标 C# Send 后
      // DisposeNetworkSender 的顺序），由泵发出后断连
      None => {
        self.session.take_output_into(resp_buf);
        None
      }
    }
  }

  fn dispose(&mut self) {
    self.session.dispose();
  }

  fn mirror_session_counters(&mut self, entry: &ConsumerEntry) {
    if let Some(metrics) = &self.session.session_metrics {
      entry.set_commands_processed(metrics.get_total_commands_processed());
    }
    // 逐命令统计共享句柄挂接（幂等；C# 监视器经 ActiveConsumers 直查
    // GetCommandStats 的镜像承接，CommandStatsMonitor 关闭为空操作）
    entry.attach_command_stats(self.session.command_stats.clone());
  }

  fn take_blocked_wait(&mut self) -> Option<BlockedWait> {
    self.session.take_blocked_wait()
  }

  fn take_slow_wait(&mut self) -> Option<SlowWait> {
    self.session.take_slow_wait()
  }

  fn pubsub_mailbox(&self) -> Option<Arc<PubSubMailbox>> {
    // 仅订阅态接线（空闲推送只对活跃订阅者有意义；非订阅会话读等待
    // 保持纯读阻塞，零额外唤醒开销）
    if self.session.is_subscription_session {
      self.session.pubsub.mailbox()
    } else {
      None
    }
  }

  fn drain_pubsub_into(&mut self, resp_buf: &mut Vec<u8>) {
    self.session.drain_pubsub_frames();
    self.session.take_output_into(resp_buf);
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
