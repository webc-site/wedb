//! RESP 会话消费者（单机与集群统一装配形态）
//!
//! 对标 C# `GarnetServer` 会话工厂：`IMessageConsumer` 包装同一套
//! `RespServerSession`，集群与单机的差异仅在构造期是否挂接
//! `ClusterSession` 切面（C# 构造函数 clusterProvider == null 与否），
//! 命令消费主循环完全一致。

use std::{mem, sync::Arc};

use parking_lot::Mutex;
use wacl::GarnetAclAuthenticator;
use wcol::itembroker::collection_item_observer::CollectionItemResult;
use wconf::RuntimeServerConfig;
use wmetric::{SessionMetricsHandle, SlowLogContainer};
use wpubsub::{subscribe_broker::SubscribeBroker, subscriber::PubSubMailbox};
use wresp::command::RespCommand;
use wtxn::{TxnLockTable, WatchVersionMap};

use super::{
  BlockedWait, ItemBroker,
  garnet_api::GarnetApi,
  resp_server_session::{RespServerSession, RespServerSessionOptions},
  session_dependencies::SessionDependencies,
  slow_path::SlowWait,
};
use crate::{
  MessageConsumerFace, cluster_provider::ClusterProviderHandle, cluster_session::ClusterSession,
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
    garnet_api: GarnetApi,
  ) -> Self {
    let mut session = RespServerSession::new(network_sender_id as i64, options);
    session.set_garnet_api(garnet_api);
    Self { session }
  }

  /// 获取内部会话引用
  #[inline]
  pub fn session(&self) -> &RespServerSession {
    &self.session
  }

  /// 构造集群形态会话消费者（挂接集群会话切面 + 集群提供者切面 + 存储执行域）
  pub fn with_cluster(
    network_sender_id: u64,
    options: RespServerSessionOptions,
    cluster_session: ClusterSession,
    cluster_provider: ClusterProviderHandle,
    garnet_api: GarnetApi,
  ) -> Self {
    let mut session = RespServerSession::new(network_sender_id as i64, options);
    session.attach_cluster_session(cluster_session);
    session.attach_cluster_provider(cluster_provider);
    session.set_garnet_api(garnet_api);
    Self { session }
  }

  /// 注入集群提供者切面（只读查询与缓冲池管理）
  pub fn attach_cluster_provider(&mut self, cluster_provider: ClusterProviderHandle) {
    self.session.attach_cluster_provider(cluster_provider);
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

  /// 注入事务组件（对标 C# `new TransactionManager(storeWrapper.watchversionMap, ...)`；
  /// `lock_table` 为该会话所属引擎实例的锁表句柄，经
  /// `SessionFunctionsWrapper.cs:30` `_clientSession.store.LockTable` 同位下发）
  pub fn attach_transaction_components(
    &mut self,
    watch_version_map: Arc<WatchVersionMap>,
    lock_table: TxnLockTable,
  ) {
    self
      .session
      .attach_transaction_components(watch_version_map, lock_table);
  }

  /// 装配期注入会话指标共享句柄（对标 C# RespServerSession 把自己的
  /// sessionMetrics 传入 StorageSession 构造的共享关系：生产装配以存储执行域
  /// 侧为单一构造点，会话据此共持同一对象；直接转发会话同名注入口，
  /// 写入单点在 [`RespServerSession::attach_session_metrics`]）
  pub fn attach_session_metrics(&mut self, metrics: Option<Arc<SessionMetricsHandle>>) {
    self.session.attach_session_metrics(metrics);
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

  /// 注入 ACL 认证器（直接委托内部 self.session.attach_acl）
  pub fn attach_acl(&mut self, authenticator: Option<Arc<Mutex<GarnetAclAuthenticator>>>) {
    self.session.attach_acl(authenticator);
  }

  /// 统一单次注入会话共享依赖集合（对标 C# StoreWrapper 共享依赖组会话装配）
  pub fn inject_dependencies(&mut self, deps: SessionDependencies) -> &mut Self {
    self.session.inject_dependencies(deps);
    self
  }

  /// 远端端点描述（接口属性实现，接口映射在 traits.rs）
  ///
  /// 关联远端端点（客户端 IP:Port，对标 C# NetworkSender.RemoteEndpointName）
  pub fn set_remote_endpoint(&mut self, endpoint: &str) {
    self.session.set_remote_endpoint(endpoint);
  }
}

impl MessageConsumerFace for RespSessionConsumer {
  /// libs/common/Networking/IMessageConsumer.cs:TryConsumeMessages
  ///
  /// 消费会话自有接收缓冲中自游标起的完整帧（会话唯一入口
  /// [`RespServerSession::try_consume_messages`]），应答直写 resp_buf
  fn try_consume_messages_into(&mut self, resp_buf: &mut Vec<u8>) -> Option<usize> {
    match self.session.try_consume_messages() {
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

  /// 致命断流信号转发（切面 GarnetException clientResponse:false 投影；
  /// 消费 None 通道同批承运，此处为泵逐批复查兜底）
  fn take_fatal_disconnect(&mut self) -> bool {
    self.session.fatal_disconnect
  }

  /// 会话待释放哨兵转发（QUIT → toDispose；泵发尽应答后断连）
  fn take_dispose_request(&mut self) -> bool {
    self.session.take_dispose_request()
  }

  /// 批内输出水位让渡哨兵转发（会话累计应答达 OUTPUT_WATERMARK_BYTES
  /// 在命令边界停住；泵实写本轮应答后立即重入消费）
  fn take_output_watermark_yield(&mut self) -> bool {
    self.session.take_output_watermark_yield()
  }

  fn take_recv_scratch(&mut self) -> Vec<u8> {
    // 会话自有接收缓冲整体移交泵直填（mem::take 占位，归还前缓冲为空壳）
    mem::take(&mut self.session.recv_buffer)
  }

  fn return_recv_scratch(&mut self, buf: Vec<u8>) {
    self.session.recv_buffer = buf;
  }

  fn set_remote_endpoint(&mut self, endpoint: &str) {
    self.set_remote_endpoint(endpoint);
  }

  fn dispose(&mut self) {
    self.session.dispose();
  }

  fn mirror_session_counters(&mut self, entry: &ConsumerEntry) {
    // INFO RESET STATS 复位位消费（C# 监视器对活跃会话原地
    // GetSessionMetrics.Reset() 的独占会话体承接：同步前清零属主指标，
    // 复位后自零位重新累计）
    if entry.take_session_stats_reset()
      && let Some(metrics) = &mut self.session.session_metrics
    {
      metrics.reset();
    }
    if let Some(metrics) = &self.session.session_metrics {
      entry.set_commands_processed(metrics.snapshot().total_commands_processed);
    }
    // 订阅邮箱溢出丢弃数镜像（C# 直写模型无丢帧语义，rust 有界邮箱背压的
    // 可观测投影；None = --pubsub 关闭，跳过保持 0）
    if let Some(dropped) = self.session.pubsub.dropped_count() {
      entry.set_pubsub_dropped(dropped);
    }
    // 逐命令统计共享句柄挂接（幂等；C# 监视器经 ActiveConsumers 直查
    // GetCommandStats 的镜像承接，CommandStatsMonitor 关闭为空操作）
    entry.attach_command_stats(self.session.command_stats.clone());
    // 延迟指标共享句柄挂接（幂等；C# 监视器经 ActiveConsumers 直查
    // LatencyMetrics 的镜像承接，LatencyMonitor 关闭为空操作）
    entry.attach_latency_metrics(self.session.latency_metrics.clone());
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

  /// 本批 AOF 提交等待标记（C# `RespServerSession.cs` 内 Send 直读的
  /// `waitForAofBlocking` 字段；解析期 `HandleAofCommitMode` 按命令依赖性维护。
  /// 该符号锚点 1:1 挂在 trait 契约面
  /// [`traits::MessageConsumerFace::wait_for_aof_blocking`]，本转调位不复挂）
  fn wait_for_aof_blocking(&self) -> bool {
    self.session.wait_for_aof_blocking
  }
}
