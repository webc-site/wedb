//! 网络连接处理器与协议帧切片泵
//!
//! 1:1 对标微软 Garnet NetworkHandler 与 ServerTcpNetworkHandler
//!
//! 统一承接 TCP 与 Unix 域套接字的单连接生命周期：
//! 1. 握手检测：读取首包判定 WireFormat（默认 WireFormat::Ascii）；
//! 2. 关联 SessionProvider 创建会话消费者；
//! 3. 驱动协议帧切片循环（半包 shift、超大包 reserve）；
//! 4. 慢客户端控制（Throttle 背压写回）；
//! 5. 优雅关停与资源回收（Dispose）。
//!
//! 子模块按域拆分：
//! - [`buffer`]：追加式接收读缓冲与探测阈值（探测域）；
//! - [`drive`]：连接泵主循环（读泵/写回域，ServerTcpNetworkHandler.cs:Start）；
//! - [`kill`]：CLIENT KILL/注销终止哨兵（令牌域）；
//! - [`push`]：订阅推送双路等待。

mod buffer;
pub(crate) mod drive;
pub(crate) mod kill;
mod push;

use std::sync::Arc;

use compio::runtime::CancelToken;
use wbase::{pool::LimitedFixedBufferPool, throttle::NetworkSenderThrottle};

use crate::{
  servers::consumer_registry::{ConsumerEntry, ConsumerRegistry},
  traits::{MessageConsumerFace, PeerSource},
};

/// 连接网络处理器与协议帧切片泵
///
/// 在 garnet 中的相对路径:
/// - `libs/common/Networking/NetworkHandler.cs`
/// - `libs/server/Servers/ServerTcpNetworkHandler.cs`
/// - `libs/server/Servers/GarnetServerTcp.cs`
///
/// 统一承接 TCP、Unix 域套接字及 TLS 单连接生命周期：
/// 1. 握手检测：读取首包判定 WireFormat（默认 WireFormat::Ascii）；
/// 2. 关联 SessionProvider 创建会话消费者；
/// 3. 驱动协议帧切片循环（半包 shift、超大包 reserve）；
/// 4. 慢客户端控制（Throttle 背压写回）；
/// 5. 优雅关停与资源回收（Dispose）。
pub struct NetworkHandler<C: MessageConsumerFace> {
  /// 处理器唯一标识符（rust 自有自增序号；C# NetworkHandler 无对应 id 字段）
  pub handler_id: u64,
  /// 远程客户端地址（在 garnet 中的相对路径: libs/common/Networking/INetworkSender.cs:RemoteEndpointName）
  pub remote_endpoint: String,
  /// 对端来源类型（accept 侧一次性折叠的本地判定真源，C# remoteEndpoint
  /// 端点对象对位；展示文本之外的判定面不再解析字符串）
  peer_source: PeerSource,
  /// 本地端点（accept 侧构造期捕获，C# TcpNetworkHandlerBase 构造期捕获
  /// socket.LocalEndPoint 的 localEndpointName 对标；TLS 臂流型擦除不透出
  /// 内层 TCP，取值必须前移到握手前）
  local_endpoint: String,
  buffer_pool: Arc<LimitedFixedBufferPool>,
  throttle: NetworkSenderThrottle,
  session: Option<C>,
  /// 活跃消费者注册表（C# GarnetServerBase 域；释放时注销）
  consumers: Option<Arc<ConsumerRegistry>>,
  /// 本连接的注册条目（accept 侧预注册装配；字节镜像 / KILL 触发位载体）
  consumer_entry: Option<Arc<ConsumerEntry>>,
  /// KILL/注销终止令牌（accept 侧预注册装配，与 KILL 哨兵同运行时；None =
  /// 无注册表哑桩形态，泵侧读不挂取消）
  kill_token: Option<CancelToken>,
}

impl<C: MessageConsumerFace> NetworkHandler<C> {
  /// 构造连接网络处理器
  ///
  /// 在 garnet 中的相对路径: libs/common/Networking/NetworkHandler.cs:NetworkHandler
  /// （对端来源 `peer_source` 为 C# TcpNetworkHandlerBase 构造期捕获
  /// remoteEndpoint 端点对象的 rust 承接：accept 侧自 typed 地址折叠置位）
  pub fn new(
    handler_id: u64,
    remote_endpoint: String,
    peer_source: PeerSource,
    local_endpoint: String,
    buffer_pool: Arc<LimitedFixedBufferPool>,
    throttle_max: usize,
  ) -> Self {
    Self {
      handler_id,
      remote_endpoint,
      peer_source,
      local_endpoint,
      buffer_pool,
      throttle: NetworkSenderThrottle::new(throttle_max),
      session: None,
      consumers: None,
      consumer_entry: None,
      kill_token: None,
    }
  }

  /// 预注册装配（accept 循环 register 后、连接任务拉起前调用；C#
  /// HandleNewConnection 的 activeHandlers.TryAdd，GarnetServerTcp.cs:256——
  /// handler 注册后才 Start）：条目、注册表与终止令牌一次挂接，此后
  /// TLS 握手与泵读写均挂令牌、收场统一经 [`Self::dispose`] 注销
  pub fn attach_consumer(
    &mut self,
    registry: Arc<ConsumerRegistry>,
    entry: Arc<ConsumerEntry>,
    kill_token: CancelToken,
  ) {
    self.consumers = Some(registry);
    self.consumer_entry = Some(entry);
    self.kill_token = Some(kill_token);
  }

  /// 关联会话实例（端点对在此一并装配——remote/local 均构造期捕获单源）
  ///
  /// 在 garnet 中的相对路径: libs/common/Networking/NetworkHandler.cs:session / libs/server/Servers/GarnetServerTcp.cs:TryCreateMessageConsumer
  pub fn set_session(&mut self, mut session: C) {
    session.set_remote_endpoint(&self.remote_endpoint, self.peer_source);
    session.set_local_endpoint(&self.local_endpoint);
    // 注入监听层缓冲池句柄（DEBUG PURGEBP ServerListener 清理源：C# 经
    // storeWrapper.Servers 直调 GarnetServerTcp.Purge()，本处理器构造期
    // 即持宿主 GarnetServer 的共享池，全连接同池等价覆盖全部监听器）
    session.attach_buffer_pool(Arc::clone(&self.buffer_pool));
    self.session = Some(session);
  }

  /// 释放网络处理器与底层会话资源
  ///
  /// 只做资源面，不碰 socket：套接字的关闭序（FIN / close_notify）是异步操作，
  /// Drop 不能 await，故只落在 drive.rs 的 process_stream 收场段（先 shutdown
  /// 后 dispose），本函数与下方的 Drop 实现皆不含 socket 面。
  ///
  /// 在 garnet 中的相对路径: libs/common/Networking/NetworkHandler.cs:Dispose / libs/server/Servers/GarnetServerTcp.cs:DisposeMessageConsumer
  pub fn dispose(&mut self) {
    self.throttle.close();
    // 注销先于会话释放（C# GarnetServerTcp.DisposeMessageConsumer：TryRemove
    // → Session.Dispose 顺序），监视器不会同轮双计条目字节镜像与 dispose 归并
    if let (Some(registry), Some(entry)) = (self.consumers.take(), self.consumer_entry.take()) {
      registry.unregister(entry.id);
    }
    if let Some(mut session) = self.session.take() {
      session.dispose();
    }
  }
}

impl<C: MessageConsumerFace> Drop for NetworkHandler<C> {
  fn drop(&mut self) {
    self.dispose();
  }
}
