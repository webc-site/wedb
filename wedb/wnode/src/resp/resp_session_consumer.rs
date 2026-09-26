//! RESP 会话消费者（单机与集群统一装配形态）
//!
//! 对标 C# `GarnetServer` 会话工厂：`IMessageConsumer` 包装同一套
//! `RespServerSession`，集群与单机的差异仅在构造期是否挂接
//! `ClusterSession` 切面（C# 构造函数 clusterProvider == null 与否），
//! 命令消费主循环完全一致。

use std::{future::Future, mem, pin::Pin, sync::Arc};

use wacl::GarnetAclAuthenticator;
use wbase::pool::LimitedFixedBufferPool;
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
  MessageConsumerFace, PeerSource, cluster_provider::ClusterProviderHandle,
  cluster_session::ClusterSession, servers::consumer_registry::ConsumerEntry,
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

  /// 获取内部会话可变引用
  #[inline]
  pub fn session_mut(&mut self) -> &mut RespServerSession {
    &mut self.session
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
  pub fn attach_acl(&mut self, authenticator: Option<Arc<GarnetAclAuthenticator>>) {
    self.session.attach_acl(authenticator);
  }

  /// 统一单次注入会话共享依赖集合（对标 C# StoreWrapper 共享依赖组会话装配）
  pub fn inject_dependencies(&mut self, deps: SessionDependencies) -> &mut Self {
    self.session.inject_dependencies(deps);
    self
  }

  /// 远端端点描述（接口属性实现，接口映射在 traits.rs）
  ///
  /// 关联远端端点（客户端 IP:Port，对标 C# NetworkSender.RemoteEndpointName；
  /// 来源类型判据成对装配，映射见 traits.rs）
  pub fn set_remote_endpoint(&mut self, endpoint: &str, source: PeerSource) {
    self.session.set_remote_endpoint(endpoint, source);
  }

  /// 本地端点描述（接口属性实现，接口映射在 traits.rs）
  ///
  /// 关联本地端点（监听端点文本，对标 C# networkSender.LocalEndpointName）
  pub fn set_local_endpoint(&mut self, endpoint: &str) {
    self.session.set_local_endpoint(endpoint);
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
    mem::take(&mut self.session.fatal_disconnect)
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

  fn recv_prime_key(&self) -> Option<(usize, usize)> {
    self.session.recv_prime_key
  }

  fn set_recv_prime_key(&mut self, key: Option<(usize, usize)>) {
    self.session.recv_prime_key = key;
  }

  fn set_remote_endpoint(&mut self, endpoint: &str, source: PeerSource) {
    self.set_remote_endpoint(endpoint, source);
  }

  fn set_local_endpoint(&mut self, endpoint: &str) {
    self.set_local_endpoint(endpoint);
  }

  /// 网络监听层缓冲池句柄注入转发（DEBUG PURGEBP ServerListener 清理源，
  /// 泵 `NetworkHandler::set_session` 装配期单点调用）
  fn attach_buffer_pool(&mut self, pool: Arc<LimitedFixedBufferPool>) {
    self.session.attach_buffer_pool(pool);
  }

  fn dispose(&mut self) {
    self.session.dispose();
  }

  fn mirror_session_counters(&mut self, entry: &ConsumerEntry) {
    if let Some(metrics) = &self.session.session_metrics {
      entry.set_commands_processed(metrics.snapshot().total_commands_processed);
    }
    // 逐命令统计共享句柄挂接（幂等；C# 监视器经 ActiveConsumers 直查
    // GetCommandStats 的镜像承接，CommandStatsMonitor 关闭为空操作）
    entry.attach_command_stats(self.session.command_stats.clone());
    // 会话指标共享句柄挂接（幂等；C# 监视器经 ActiveConsumers 直查
    // GetSessionMetrics 的镜像承接，采样关闭为空操作——监视器据此读
    // found/notfound 等全部会话计数）
    entry.attach_session_metrics(self.session.session_metrics.clone());
    // 会话延迟表不镜像给注册表：其为 `self.session` 独占的拥有型实例，
    // 样本由属主连接任务在版本翻转点按引用直并全局延迟表，镜像只会把
    // 零锁直写降级成写锁（C# ActiveConsumers 直查 LatencyMetrics 的臂在
    // rust 归属转移，见 consumer_registry 文件头）
    // CLIENT LIST/KILL 的会话动态字段发布（对标 C# WriteClientInfo/IsMatch 跨
    // 线程直读目标会话活字段）：rust 会话体由连接任务 &mut 独占，跨线程无锁裸读
    // 即数据竞争，注册表条目遂持一份投影。整份重导收敛于此每批汇聚单点，与命令
    // 计数、订阅丢弃数同一发布轨——任何动态字段变更都在所属批收场被完整捕获，
    // 杜绝逐字段补调的漏点；他者会话据此读到目标最近完成批的真值。真值单源仍在
    // 会话字段，本投影仅为其跨线程可读快照，不构成第二处写入源。
    entry.update_view(self.session.current_client_view());
  }

  fn take_blocked_wait(&mut self) -> Option<BlockedWait> {
    self.session.take_blocked_wait()
  }

  fn take_slow_wait(&mut self) -> Option<SlowWait> {
    self.session.take_slow_wait()
  }

  fn take_pending_acl_refresh(&mut self) -> bool {
    self.session.take_pending_acl_refresh()
  }

  fn take_pending_auth_acl(&mut self) -> Option<(RespCommand, Vec<Vec<u8>>, usize)> {
    self.session.take_pending_auth_acl()
  }

  fn account_parked_auth_acl_failure(&mut self, cmd: RespCommand, start_len: usize) {
    self.session.account_parked_auth_acl_failure(cmd, start_len);
  }

  fn pending_acl_refresh_fut(&mut self) -> Pin<Box<dyn Future<Output = ()> + '_>> {
    // Arc 移入 future 保执行域存活（返回值只借 self 与参数）
    let api = self.session.garnet_api.clone();
    let session = &mut self.session;
    Box::pin(async move {
      if let Some(api) = api {
        api.exec_acl_refresh(session).await
      }
    })
  }

  fn pending_auth_acl_fut<'a>(
    &'a mut self,
    cmd: RespCommand,
    args: &'a [&'a [u8]],
  ) -> Pin<Box<dyn Future<Output = bool> + 'a>> {
    let api = self.session.garnet_api.clone();
    let session = &mut self.session;
    Box::pin(async move {
      match api {
        Some(api) => api.exec_auth_acl(session, cmd, args).await,
        None => false,
      }
    })
  }

  fn flush_output_into(&mut self, resp_buf: &mut Vec<u8>) {
    self.session.take_output_into(resp_buf);
  }

  /// 慢路径应答并入（锚点 1:1 挂在 trait 契约面
  /// [`traits::MessageConsumerFace::resolve_slow_wait_into`]，本转调位不复挂）
  fn resolve_slow_wait_into(&mut self, reply: &[u8], resp_buf: &mut Vec<u8>) {
    self.session.resolve_slow_wait_into(reply, resp_buf);
  }

  /// 脚本内挂起探测（协程化承接，锚点 1:1 挂在 trait 契约面
  /// [`traits::MessageConsumerFace::has_script_suspend`]）
  fn has_script_suspend(&self) -> bool {
    self.session.has_script_suspend()
  }

  /// 挂起脚本续跑执行体（协程化承接，锚点 1:1 挂在 trait 契约面
  /// [`traits::MessageConsumerFace::resume_suspended_script_fut`]）
  fn resume_suspended_script_fut<'a>(
    &'a mut self,
    resp_buf: &'a mut Vec<u8>,
  ) -> Pin<Box<dyn Future<Output = ()> + 'a>> {
    Box::pin(self.session.resume_suspended_script(resp_buf))
  }

  fn pubsub_mailbox(&self) -> Option<Arc<PubSubMailbox>> {
    // 订阅态恒接线（空闲推送只对活跃订阅者有意义）；非订阅态补一条邮箱非空
    // 支路——退订命令把最后一通道摘掉即清旗，而同窗在途广播（发布线程摘除前
    // 已 pin 订阅者快照）仍可落入本会话邮箱，纯读臂下该残帧要滞留到下一输入
    // 批的轮头 drain 才出（C# 广播线程批锁内联直写，绝无滞留）。非订阅且空
    // 邮箱恒 None，空闲非订阅连接零额外唤醒成本不变
    let mailbox = self.session.pubsub.mailbox()?;
    if self.session.is_subscription_session || mailbox.has_messages() {
      Some(mailbox)
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
