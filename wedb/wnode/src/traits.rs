//! 网络服务与会话抽象面
//!
//! 1:1 对标微软 Garnet Garnet.networking 的 WireFormat / ISessionProvider / IMessageConsumer

use std::sync::Arc;

use wcol::itembroker::collection_item_observer::CollectionItemResult;
use wresp::RespCommand;

use crate::resp::{BlockedWait, slow_path::SlowWait};

/// 会话线格式（对标 WireFormat）
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(u8)]
pub enum WireFormat {
  /// ASCII 线格式（RESP 协议）
  Ascii = 255,
}

/// 活跃会话枚举面（会话经此枚举其他会话）
pub trait ServerEnumerate: Send + Sync {
  /// 消费者类型
  type Consumer: MessageConsumerFace;

  /// 全部活跃消息消费者
  fn active_consumers(&self) -> Vec<Arc<Self::Consumer>>;
}

/// 消息消费者面（底层网络读写切片泵消费端）
///
/// 应答主形态为 [`Self::try_consume_messages_into`]（零拷贝直写泵写缓冲）；
/// 分配形态 [`Self::try_consume_messages`] 为存量兼容面（集群复制会话与
/// 副本流水线 FrameSink 等外部调用方仍依赖，默认桥接实现，勿在新路径使用）
pub trait MessageConsumerFace: Send + 'static {
  /// 兼容形态：消费接收缓冲区中的消息，应答整体落在独立 Vec 中
  ///
  /// 返回 (已消费字节数, 待写回应答载荷)：`consumed == 0` 表示尚未凑齐完整帧
  fn try_consume_messages(&mut self, req_buffer: &[u8]) -> (usize, Vec<u8>);

  /// 零拷贝消费消息（主形态）：将应答直接追加到调用方传入的写缓冲区，
  /// 避免中间 Vec 堆分配与二次拷贝
  ///
  /// 默认实现向下兼容存量仅实现分配形态的消费者：
  fn try_consume_messages_into(&mut self, req_buffer: &[u8], resp_buf: &mut Vec<u8>) -> usize {
    let (consumed, resp) = self.try_consume_messages(req_buffer);
    if !resp.is_empty() {
      resp_buf.extend_from_slice(&resp);
    }
    consumed
  }

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
}
