//! 网络服务与会话抽象面
//!
//! 1:1 对标微软 Garnet Garnet.networking 的 WireFormat / ISessionProvider / IMessageConsumer

use std::{future::Future, pin::Pin, sync::Arc};

use wbase::pool::LimitedFixedBufferPool;
use wcol::itembroker::collection_item_observer::CollectionItemResult;
use wpubsub::subscriber::PubSubMailbox;
use wresp::command::RespCommand;
#[cfg(feature = "tls")]
use wtls::ServerTlsConfig;

use crate::{
  aof::GarnetAppendOnlyFile,
  resp::{BlockedWait, slow_path::SlowWait},
  servers::consumer_registry::{ConsumerEntry, ConsumerRegistry},
};

/// 会话线格式（对标 WireFormat）
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(u8)]
pub enum WireFormat {
  /// ASCII 线格式（RESP 协议）
  Ascii = 255,
}

/// 对端连接来源类型（本地连接判定的唯一真源）
///
/// 对位 C# `remoteEndpoint` 端点对象类型判据
///（`libs/common/Networking/TcpNetworkHandlerBase.cs:42-44`：
/// UnixDomainSocketEndPoint 恒本地、IPEndPoint 臂走 IPAddress.IsLoopback）
/// ——accept 侧自 typed socket 地址一次性折叠，会话判定直读本型，
/// 不再解析 remote_endpoint 展示字符串
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PeerSource {
  /// Unix 域套接字对端（C# UnixDomainSocketEndPoint 恒真臂：含未命名对端
  /// ——getpeername 无名时展示串为空串，来源类型不受影响，恒本地）
  Unix,
  /// IP 端点（C# IPEndPoint 臂）：loopback 为 accept 侧按
  /// [`wbase::endpoint::ip_is_loopback`]（C# IPAddress.IsLoopback 口径，
  /// 含 v4-mapped IPv6 解映射）折出的布尔
  Ip {
    /// typed 回环判定结果（127.0.0.0/8、::1 及解映射后的 v4-mapped 回环）
    loopback: bool,
  },
}

/// 未接网形态缺省即非本地（对位 C# networkSender 缺失时 IsLocalConnection
/// 恒假臂），杜绝缺省放行
impl Default for PeerSource {
  #[inline]
  fn default() -> Self {
    Self::Ip { loopback: false }
  }
}

impl PeerSource {
  /// C# TcpNetworkHandlerBase.cs:IsLocalConnection 判据（Unix 恒真、IP 臂读折叠布尔）
  #[inline]
  pub fn is_local(&self) -> bool {
    matches!(self, Self::Unix | Self::Ip { loopback: true })
  }
}

/// 消息消费者面（底层网络读写切片泵消费端）
///
/// 消费形态唯一（C# IMessageConsumer 单方法）：缓冲与游标驻留消费者
///（[`Self::take_recv_scratch`] 泵直填 + [`Self::try_consume_messages_into`]
/// 持久游标消费），应答零拷贝直写泵写缓冲
pub trait MessageConsumerFace: Send + 'static {
  /// libs/common/Networking/IMessageConsumer.cs:TryConsumeMessages
  ///
  /// 消费会话自有接收缓冲中自上次游标起的完整帧，应答直写 resp_buf
  ///（零拷贝，避免中间堆分配）
  ///
  /// 返回消费后残余字节数（半包长度；`0` = 整段消费完毕，会话已复位缓冲）；
  /// `None` = 协议违规（C# RespParsingException 语义，泵发尽应答后断连）
  fn try_consume_messages_into(&mut self, resp_buf: &mut Vec<u8>) -> Option<usize>;

  /// 取走致命断流信号（C# `GarnetException` 且 `DisposeSession = true` 的
  /// `DisposeNetworkSender` 等价：不写错误应答行，发尽本轮累积应答后断连）
  ///
  /// 消费面处理失败（APPENDLOG 拒收 / 畸形帧等不可恢复错误）时登记，
  /// 泵在消费段逐次检查，据此断连（对标 C# `clientResponse: false` 异常
  /// 上抛 → RespServerSession catch → 断流的信号通道）。默认 false
  fn take_fatal_disconnect(&mut self) -> bool {
    false
  }

  /// 取走会话待释放哨兵（C# RespServerSession.Process 尾部 `if (toDispose)
  /// DisposeNetworkSender(true)` 的信号通道：QUIT 置位，泵发尽本轮累积
  /// 应答后据此主动断连）。默认 false
  fn take_dispose_request(&mut self) -> bool {
    false
  }

  /// 取走批内输出水位让渡哨兵（C# RespServerSession.cs:SendAndReset 满刷
  /// 循环的命令边界投影：置位表示本批因累计应答达水位在命令边界停住、
  /// 接收缓冲尚有完整帧待续消费；网络泵实写本轮应答后立即重入消费，
  /// 不等下一批网络字节——对标 C# Send 后重取缓冲续写）。默认 false
  fn take_output_watermark_yield(&mut self) -> bool {
    false
  }

  /// 取走会话自有接收缓冲供泵直填（对齐 C# RespServerSession 的
  /// bytesRead/readHead 私有缓冲模型：网络字节零拷贝直入会话缓冲，游标
  /// 跨批次持久）
  ///
  /// 取走至 [`Self::return_recv_scratch`] 归还期间，泵不得触碰会话其余状态
  fn take_recv_scratch(&mut self) -> Vec<u8>;

  /// 归还接收缓冲（泵完成网络写入后调用；半包残余字节必须驻留会话）
  fn return_recv_scratch(&mut self, buf: Vec<u8>);

  /// 接收缓冲空闲段初始化代际（`(首指针, 容量)` 键；`None` = 无记忆）
  ///
  /// TLS 读的空闲段整段清零随缓冲记忆付一次（`wbase::primed` 契约），
  /// 记忆须与借出的接收缓冲同生命周期存储于会话（缓冲的常驻所有者）：
  /// 泵经 [`Self::take_recv_scratch`]/[`Self::return_recv_scratch`] 进出
  /// 缓冲时用本对方法转存代际。默认无记忆（每次 TLS 读重清零，与无记忆
  /// 形态同价）；换缓冲实例的实现必须清键
  fn recv_prime_key(&self) -> Option<(usize, usize)> {
    None
  }

  /// 回写接收缓冲初始化代际（读毕由泵调用）
  fn set_recv_prime_key(&mut self, _key: Option<(usize, usize)>) {}

  /// 会话累计命令数镜像进注册表条目（网络泵逐批调用；监视器瞬时 ops/s
  /// 数据源——C# MainMonitorTaskAsync 跨线程直读 sessionMetrics，rust
  /// 会话体连接任务独占，承接为泵侧同任务拷贝）。默认空操作
  fn mirror_session_counters(&mut self, _entry: &ConsumerEntry) {}

  /// libs/common/Networking/INetworkSender.cs:RemoteEndpointName +
  /// TcpNetworkHandlerBase.cs:42-44（端点展示文本与来源类型判据成对装配）
  ///
  /// 关联远端端点：文本（客户端 IP:Port 或套接字路径，仅 CLIENT INFO/日志
  /// 展示）与 [`PeerSource`] 来源类型（本地判定唯一真源）同点落字段
  fn set_remote_endpoint(&mut self, _endpoint: &str, _source: PeerSource) {}

  /// 关联本地端点（监听端点文本，对标 C# 会话侧读
  /// networkSender.LocalEndpointName 的 CLIENT INFO 承接；取值收敛于 stream
  /// 抽象层单源。默认空操作）
  fn set_local_endpoint(&mut self, _endpoint: &str) {}

  /// 装配期注入网络监听层缓冲池句柄（DEBUG PURGEBP ServerListener 的清理
  /// 源：C# `NetworkPurgeBP` 遍历 `storeWrapper.Servers` 直调
  /// `GarnetServerTcp.Purge()`，rust 侧泵 `NetworkHandler::set_session`
  /// 单点注入。默认空操作 = 无监听池语义的消费者）
  fn attach_buffer_pool(&mut self, _pool: Arc<LimitedFixedBufferPool>) {}

  /// libs/common/Networking/IServerHook.cs:DisposeMessageConsumer
  /// 释放会话资源
  fn dispose(&mut self);

  /// 取走挂起中的阻塞命令等待体（无阻塞命令的消费者恒 None）
  ///
  /// 网络泵 await [`BlockedWait::resolve`] 驱动（compio 挂起不占线程，
  /// C# 由专线网络线程 BlockingWait 承担），完成后将
  /// (命令, 结果) 经 [`Self::resolve_blocked_wait_into`] 写回应答
  fn take_blocked_wait(&mut self) -> Option<BlockedWait> {
    None
  }

  /// 阻塞等待完成后的应答写出；直接追加到调用方传入的写缓冲区中（零拷贝避免
  /// 堆分配）。无阻塞命令的消费者恒空操作
  fn resolve_blocked_wait_into(
    &mut self,
    _cmd: RespCommand,
    _result: CollectionItemResult,
    _resp_buf: &mut Vec<u8>,
  ) {
  }

  /// 取走挂起中的慢路径执行体（无慢路径命令的消费者恒 None）
  ///
  /// 网络泵 await [`SlowWait::resolve`] 驱动（compio 挂起不占线程，
  /// 对照 C# 网络线程同步执行慢命令），完成后把应答字节按流水线
  /// 顺序追加写出
  fn take_slow_wait(&mut self) -> Option<SlowWait> {
    None
  }

  /// 慢路径完成后的应答写出：先冲出会话已累积应答，再把应答字节按流水线顺序
  /// 追加到调用方传入的写缓冲区。无慢路径命令的消费者恒空操作
  fn resolve_slow_wait_into(&mut self, _reply: &[u8], _resp_buf: &mut Vec<u8>) {}

  /// 脚本内挂起探测（协程化承接：EVAL 内 redis.call 命中阻塞/慢路径，挂起
  /// 体让渡会话挂起槽时置位；无脚本挂起面的消费者恒 false）
  fn has_script_suspend(&self) -> bool {
    false
  }

  /// 挂起脚本续跑执行体（仅在 [`Self::has_script_suspend`] 为 true 后调用；
  /// 无脚本挂起面的消费者为立即完成空转）：await 驱动挂起体并以应答转换值
  /// 续跑脚本协程至完成，应答随续跑窗口并入会话输出缓冲（泵重入消费时经
  /// [`Self::try_consume_messages_into`] 按流水线顺序冲出）
  fn resume_suspended_script_fut<'a>(
    &'a mut self,
    resp_buf: &'a mut Vec<u8>,
  ) -> Pin<Box<dyn Future<Output = ()> + 'a>> {
    let _ = resp_buf;
    Box::pin(async {})
  }

  /// 取走 ACL 挂载刷新停车标志（重驱型：鉴权预门判挂载陈旧时置位，无 ACL
  /// 挂载面的消费者恒 false）
  ///
  /// 网络泵经执行域刷新臂 await 点查（不产应答）后重入消费，游标已回退
  /// 即重解析重评原命令
  fn take_pending_acl_refresh(&mut self) -> bool {
    false
  }

  /// 取走 AUTH / HELLO / ACL 族停车快照（产应答型；无存储执行域面的消费者
  /// 恒 None）。伴随输出水位 = 本命令应答段起点，闭环后供
  /// [`Self::account_parked_auth_acl_failure`] 补计失败
  ///
  /// 网络泵经 [`Self::pending_auth_acl_fut`] 异步臂 await 闭环（认证/规则
  /// 读写回写会话本地态，须借用会话不可静态装箱），应答直写会话输出缓冲，
  /// 经 [`Self::flush_output_into`] 按流水线顺序冲出
  fn take_pending_auth_acl(&mut self) -> Option<(RespCommand, Vec<Vec<u8>>, usize)> {
    None
  }

  /// 停车臂失败应答补计（CommandStats 门对位）：泵在
  /// [`Self::pending_auth_acl_fut`] await 闭环完成后、[`Self::flush_output_into`]
  /// 冲出前，按同步段同一判据补计该停车命令的 failed_calls（calls 已在同步
  /// 段计，此处只补 failed）；无会话统计面的消费者恒空操作
  fn account_parked_auth_acl_failure(&mut self, _cmd: RespCommand, _start_len: usize) {}

  /// ACL 挂载刷新执行体（重驱型停车臂：借用会话构建执行域刷新 future，
  /// 泵内联 await；无 ACL 挂载面的消费者为立即完成空转）
  fn pending_acl_refresh_fut(&mut self) -> Pin<Box<dyn Future<Output = ()> + '_>> {
    Box::pin(async {})
  }

  /// AUTH / HELLO / ACL 族执行体（产应答型停车臂：命令与参数快照已脱离
  /// 接收缓冲生命周期，借用会话构建执行域异步臂 future，泵内联 await；
  /// 无存储执行域面的消费者恒 false——应答面由调用方兜底）
  fn pending_auth_acl_fut<'a>(
    &'a mut self,
    cmd: RespCommand,
    args: &'a [&'a [u8]],
  ) -> Pin<Box<dyn Future<Output = bool> + 'a>> {
    let _ = (cmd, args);
    Box::pin(async { false })
  }

  /// AUTH / HELLO / ACL 族闭环后的应答冲出（会话输出缓冲直写形态：
  /// 异步臂应答累积于会话缓冲，泵按流水线顺序冲入本轮写缓冲；无会话
  /// 输出面的消费者恒空操作）
  fn flush_output_into(&mut self, _resp_buf: &mut Vec<u8>) {}

  /// 订阅推送邮箱句柄（订阅态会话返回 Some；网络泵读等待段的双路
  /// 等待源——读挂起期间邮箱到达事件唤醒连接任务直写推送帧，
  /// C# 由广播线程直写订阅会话网络发送器的等价承接）。默认 None
  fn pubsub_mailbox(&self) -> Option<Arc<PubSubMailbox>> {
    None
  }

  /// 排空订阅邮箱并把推送帧追加到写缓冲（消费段补充面：有输入的
  /// 会话在应答写出前顺带收推送；空闲唤醒主路径见
  /// [`Self::pubsub_mailbox`]）。默认空操作
  fn drain_pubsub_into(&mut self, _resp_buf: &mut Vec<u8>) {}

  /// 本批是否需先等 AOF 提交落盘再出网（C# RespServerSession.cs:Send 内
  /// `if (waitForAofBlocking)` 的消费面投影：标记由解析期
  /// `HandleAofCommitMode` 按命令依赖性维护，网络泵写出段读取并前置
  /// [`SessionProviderFace::wait_for_commit_async`]）。
  /// 默认 false = 无 RESP 会话语义的消费者（副本回放/裸行协议）
  fn wait_for_aof_blocking(&self) -> bool {
    false
  }
}

/// 会话提供者面（按线格式创建对应的会话消费者）
pub trait SessionProviderFace: Send + Sync {
  /// 消费者类型
  type Consumer: MessageConsumerFace;

  /// libs/common/Networking/IServerHook.cs:TryCreateMessageConsumer
  /// libs/server/Sessions/ISessionProvider.cs:GetSession
  /// 创建会话消费者
  fn get_session(&self, wire_format: WireFormat, network_sender: u64) -> Option<Self::Consumer>;

  /// 活跃消费者注册表（libs/server/Servers/GarnetServerBase.cs 的 activeHandlers
  /// 域；网络泵建连/释放经此注册/注销；None = 未装配，泵侧跳过注册）
  fn consumer_registry(&self) -> Option<Arc<ConsumerRegistry>> {
    None
  }

  /// 复位存储复活化统计（INFO RESETSTAT 的 reviv 臂宿主入口，对标 C#
  /// libs/server/StoreWrapper.cs:ResetRevivificationStats 的
  /// `databaseManager.ResetRevivificationStats()` 直下；
  /// 默认空操作 = 该宿主无复活账目可复位（裸会话提供者/测试宿主）
  fn reset_revivification_stats(&self) {}

  /// AOF 门面（C# storeWrapper.appendOnlyFile；None = AOF 未点亮）。
  ///
  /// 宿主关停链唯一取用面：stop() 在 join 前经
  /// [`GarnetAppendOnlyFile::backpressure`] 放行滞留追加方（rust worker 线程
  /// join 仅有防御档有界上界，滞留追加方不放行将吃满该窗口；C# 由
  /// DrainActiveHandlers 超时兜底而无此前置），wait_for_shutdown
  /// 在排空后调 [`GarnetAppendOnlyFile::dispose_async`] 收口未提交帧
  ///（C# InternalDispose Phase 3 Provider.Dispose → AppendOnlyFile.Dispose）
  fn aof(&self) -> Option<&Arc<GarnetAppendOnlyFile>> {
    None
  }

  /// 等待 AOF 提交落盘（在 garnet 中的相对路径:libs/server/StoreWrapper.cs:
  /// WaitForCommitAsync：`!EnableAOF` 直返 Ok(false)，否则下达
  /// `databaseManager.WaitForCommitToAofAsync`）。网络泵写出段在
  /// [`MessageConsumerFace::wait_for_aof_blocking`] 置位时前置调用，
  /// 即 C# `Send` 内 `AsyncUtils.BlockingWait` 的 compio 挂起等价
  ///（挂起不占线程）。提交失败 Err 上浮——C# BlockingWait 抛
  /// CommitFailureException 后应答不发出、连接 dispose，等待结果本身
  /// 的返回值（false = 跳过）才弃用。默认 Ok(false) = 无 AOF 形态宿主
  ///
  /// 不设 `Send` 上界：AOF 刷盘链（`waof::WalLog` 提交步进）在 compio
  /// thread-per-core 下刻意不可跨线程迁移，连接任务同核原地驱动
  fn wait_for_commit_async(&self) -> impl Future<Output = waof::Result<bool>> {
    async { Ok(false) }
  }

  /// 向量清理协程停机收敛（对标 C# `VectorManager.Dispose` 逐通道
  /// `CompleteAndWaitForConsumerTask`：requestDrop → requestCleanup → cleanup
  /// 顺序关闭并等待消费退出）。宿主 `stop()` 在 `shutdown_coordinator.stop()`
  /// 之后（Phase 1 已阻断新连接，收敛期无新入写任务可丢）、worker join 之前
  /// 于主线程调用——消费协程栖 worker 运行时，硬约束仅先于 join（join 即
  /// 运行时析构屏障）。默认 true = 该宿主无向量清理协程需收敛（测试/裸会话
  /// 提供者）。
  fn dispose_vector_cleanup(&self) -> bool {
    true
  }

  /// pubsub 中枢停机收口（对标 C# InternalDispose 尾部
  /// `subscribeBroker?.Dispose()`，该步排在 Provider.Dispose 即活跃连接排空
  /// 之后）。rust 无 C# 的 TsavoriteLog 介质与后台消费循环（SubscribeBroker
  /// 无常驻消费任务），收口为同步单步：置 disposed 并清三订阅表，无等待面、
  /// 无返回值。宿主 `stop()` 在 worker join 之后调用——排空前清表会破坏在途
  /// 订阅连接注销与在途 PUBLISH 投递窗口。默认空操作 = 该宿主未装配 pubsub。
  fn dispose_pubsub(&self) {}

  /// 集合项经纪停机收口（对标 C# StoreWrapper.Dispose 的
  /// `itemBroker?.Dispose()`：置取消位、解除全部等待观察者送最终空应答、
  /// 投事件唤醒经纪主循环退出）。宿主 `stop()` 在向量清理收敛后、join
  /// worker 之前调用——主循环跑在 worker 运行时，紧随其后的 join 即
  /// 天然排空屏障，无需另设等待。默认空操作 = 该宿主未装配经纪
  /// （测试/裸会话提供者）。
  fn dispose_item_broker(&self) {}

  /// Lua 超时看门狗停机收口（对标 C# `StoreWrapper.Dispose` 的
  /// `luaTimeoutManager?.Dispose()`：置停机位唤醒专属定时线程并 Join）。
  /// 宿主 `stop()` 调用；看门狗线程独立于 worker 运行时，收口次序不受
  /// join 屏障约束。默认空操作 = 该宿主未装配超时管理器。
  fn dispose_lua_timeout(&self) {}

  /// 范围索引停机收口（对应 StoreWrapper.Dispose
  /// 中的 `rangeIndexManager?.Dispose()`，时序在 Provider.Dispose 段——Phase 2
  /// 连接排空之后、`databaseManager.Dispose()` 引擎兜底析构之前）。实现需
  /// 清理复制面未完成流重组并释放在线引擎全部在线树。默认空操作 = 该宿主
  /// 无范围索引引擎（测试/裸会话提供者）。
  fn dispose_range_index(&self) {}

  /// TLS 证书热加载共享句柄（C# `storeWrapper.serverOptions.TlsOptions`
  /// 单实例共享的会话侧可达面对位：CONFIG SET cert-file-name 经会话触达
  /// 同一 TlsOptions 实例在线重载；None = 该宿主未装配 TLS，证书对回
  /// "ERR TLS is disabled."）。宿主启动链经此取同一句柄注入网络端点，
  /// 全端点与 CONFIG SET 共享同一活跃证书
  #[cfg(feature = "tls")]
  fn tls_config(&self) -> Option<Arc<ServerTlsConfig>> {
    None
  }
}
