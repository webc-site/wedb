//! 网络服务与会话抽象面
//!
//! 1:1 对标微软 Garnet Garnet.networking 的 WireFormat / ISessionProvider / IMessageConsumer

use std::{future::Future, sync::Arc};

use wcol::itembroker::collection_item_observer::CollectionItemResult;
use wpubsub::subscriber::PubSubMailbox;
use wresp::command::RespCommand;

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

  /// 会话累计命令数镜像进注册表条目（网络泵逐批调用；监视器瞬时 ops/s
  /// 数据源——C# MainMonitorTaskAsync 跨线程直读 sessionMetrics，rust
  /// 会话体连接任务独占，承接为泵侧同任务拷贝）。默认空操作
  fn mirror_session_counters(&mut self, _entry: &ConsumerEntry) {}

  /// libs/common/Networking/INetworkSender.cs:RemoteEndpointName
  /// 关联远端端点（客户端 IP:Port，对标 C# NetworkSender.RemoteEndpointName）
  fn set_remote_endpoint(&mut self, _endpoint: &str) {}

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
  /// [`GarnetAppendOnlyFile::backpressure`] 放行滞留追加方（rust join 无超时
  /// 上界，C# 由 DrainActiveHandlers 超时兜底而无此前置），wait_for_shutdown
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
  /// 之前于主线程调用，此时 worker 运行时仍在驱动排空。默认 true = 该宿主
  /// 无向量清理协程需收敛（测试/裸会话提供者）。
  fn dispose_vector_cleanup(&self) -> bool {
    true
  }

  /// pubsub 中枢停机收口（对标 C# InternalDispose 尾部
  /// `subscribeBroker?.Dispose()`：唤醒后台消费循环、等其跑完在途批次退出、
  /// 清空订阅与待发队列）。宿主 `stop()` 在 join worker 之前调用——消费任务
  /// 跑在 worker 运行时，等待需要运行时驱动。返回 false = 消费循环未在超时
  /// 内收敛（订阅表仍清理，停机继续）。默认 true = 该宿主未装配 pubsub。
  fn dispose_pubsub(&self) -> bool {
    true
  }

  /// 范围索引停机收口（对应 StoreWrapper.Dispose
  /// 中的 `rangeIndexManager?.Dispose()`，时序在 Provider.Dispose 段——Phase 2
  /// 连接排空之后、`databaseManager.Dispose()` 引擎兜底析构之前）。实现需
  /// 清理复制面未完成流重组并释放在线引擎全部在线树。默认空操作 = 该宿主
  /// 无范围索引引擎（测试/裸会话提供者）。
  fn dispose_range_index(&self) {}
}
