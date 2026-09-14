//! 网络服务与会话抽象面
//!
//! 1:1 对标微软 Garnet Garnet.networking 的 WireFormat / ISessionProvider / IMessageConsumer

use std::sync::Arc;

use wcol::itembroker::collection_item_observer::CollectionItemResult;
use wresp::RespCommand;

use crate::{
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

/// 活跃会话枚举面（C# GarnetServerBase 的 ActiveConsumers；锚点实现见
/// [`crate::servers::consumer_registry::ConsumerRegistry`]）
///
/// rust 会话体为连接任务独占（C# 为跨线程裸读会话字段），枚举面承接为
/// 注册表条目：注册快照 + 动态字段镜像 + kill 触发位，注销时机 = 会话
/// dispose（网络泵释放）
pub trait ServerEnumerate: Send + Sync {
  /// 全部活跃消息消费者（注册表条目句柄）
  fn active_consumers(&self) -> Vec<Arc<ConsumerEntry>>;
}

/// 消息消费者面（底层网络读写切片泵消费端）
///
/// 应答主形态为 [`Self::try_consume_messages_into`]（零拷贝直写泵写缓冲，
/// 必选）；分配形态 [`Self::try_consume_messages`] 为存量兼容面（测试与
/// 外部调用方仍依赖），默认桥接到主形态（临时 Vec 承接后转写）
pub trait MessageConsumerFace: Send + 'static {
  /// 零拷贝消费消息（主形态）：将应答直接追加到调用方传入的写缓冲区，
  /// 避免中间 Vec 堆分配与二次拷贝
  ///
  /// 返回已消费字节数：`consumed == 0` 表示尚未凑齐完整帧
  fn try_consume_messages_into(&mut self, req_buffer: &[u8], resp_buf: &mut Vec<u8>) -> usize;

  /// 兼容形态（默认桥接）：消费接收缓冲区中的消息，应答整体落在独立 Vec 中
  ///
  /// 返回 (已消费字节数, 待写回应答载荷)：`consumed == 0` 表示尚未凑齐完整帧
  fn try_consume_messages(&mut self, req_buffer: &[u8]) -> (usize, Vec<u8>) {
    let mut resp = Vec::new();
    let consumed = self.try_consume_messages_into(req_buffer, &mut resp);
    (consumed, resp)
  }

  /// 取走会话自有接收缓冲供泵直读（scratch 直读形态，对齐 C# RespServerSession
  /// 的 bytesRead/readHead 私有缓冲模型：网络字节零拷贝直入会话缓冲，游标
  /// 跨批次持久）
  ///
  /// 返回 None = 会话无直读面（泵回退拷贝路径，非 TCP 消费方与测试零感知）；
  /// 取走至 [`Self::return_recv_scratch`] 归还期间，泵不得触碰会话其余状态
  fn take_recv_scratch(&mut self) -> Option<Vec<u8>> {
    None
  }

  /// 归还接收缓冲（泵完成网络写入后调用；半包残余字节必须驻留会话）
  fn return_recv_scratch(&mut self, _buf: Vec<u8>) {}

  /// scratch 直读消费：消费会话自有接收缓冲中自上次游标起的完整帧，应答
  /// 直写 resp_buf（C# TryConsumeMessages 的持久游标等价）
  ///
  /// 返回消费后残余字节数（半包长度；`0` = 整段消费完毕，会话已复位缓冲）；
  /// `None` = 协议违规（C# RespParsingException 语义，泵断连）
  fn try_consume_scratch_into(&mut self, _resp_buf: &mut Vec<u8>) -> Option<usize> {
    None
  }

  /// 会话累计命令数镜像进注册表条目（网络泵逐批调用；监视器瞬时 ops/s
  /// 数据源——C# MainMonitorTaskAsync 跨线程直读 sessionMetrics，rust
  /// 会话体连接任务独占，承接为泵侧同任务拷贝）。默认空操作
  fn mirror_session_counters(&mut self, _entry: &ConsumerEntry) {}

  /// libs/common/Networking/IServerHook.cs:DisposeMessageConsumer
  /// 释放会话资源
  fn dispose(&mut self);

  /// 挂载底层服务器引用（会话用于枚举同伴/反查状态）
  /// 默认实现为空操作，保留形参名以满足接口契约
  fn attach_server<S: ServerEnumerate>(&mut self, _server: Arc<S>) {}

  /// 取走挂起中的阻塞命令等待体（无阻塞命令的消费者恒 None）
  ///
  /// 网络泵 await [`BlockedWait::resolve`] 驱动（compio 挂起不占线程，
  /// C# 由专线网络线程 BlockingWait 承担），完成后将
  /// (命令, 结果) 经 [`Self::resolve_blocked_wait`] 写回应答
  fn take_blocked_wait(&mut self) -> Option<BlockedWait> {
    None
  }

  /// 阻塞等待完成后的应答写出；返回应答字节（无阻塞命令的消费者恒空）
  fn resolve_blocked_wait(&mut self, _cmd: RespCommand, _result: CollectionItemResult) -> Vec<u8> {
    Vec::new()
  }

  /// 阻塞等待完成后的应答写出；直接追加到调用方传入的写缓冲区中（零拷贝避免堆分配）
  fn resolve_blocked_wait_into(
    &mut self,
    cmd: RespCommand,
    result: CollectionItemResult,
    resp_buf: &mut Vec<u8>,
  ) {
    let reply = self.resolve_blocked_wait(cmd, result);
    if !reply.is_empty() {
      resp_buf.extend_from_slice(&reply);
    }
  }

  /// 取走挂起中的慢路径执行体（无慢路径命令的消费者恒 None）
  ///
  /// 网络泵 await [`SlowWait::resolve`] 驱动（compio 挂起不占线程，
  /// 对照 C# 网络线程同步执行慢命令），完成后把应答字节按流水线
  /// 顺序追加写出
  fn take_slow_wait(&mut self) -> Option<SlowWait> {
    None
  }
}

/// 会话提供者面（按线格式创建对应的会话消费者）
pub trait SessionProviderFace: Send + Sync {
  /// 消费者类型
  type Consumer: MessageConsumerFace;

  /// libs/common/Networking/IServerHook.cs:TryCreateMessageConsumer
  /// 创建会话消费者
  fn get_session(&self, wire_format: WireFormat, network_sender: u64) -> Option<Self::Consumer>;

  /// 活跃消费者注册表（libs/server/Servers/GarnetServerBase.cs 的 activeHandlers
  /// 域；网络泵建连/释放经此注册/注销；None = 未装配，泵侧跳过注册）
  fn consumer_registry(&self) -> Option<Arc<ConsumerRegistry>> {
    None
  }
}
