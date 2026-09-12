//! 网络服务与会话抽象面
//!
//! 1:1 对标微软 Garnet Garnet.networking 的 WireFormat / ISessionProvider / IMessageConsumer

use std::sync::Arc;

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
pub trait MessageConsumerFace: Send + Sync + 'static {
  /// 消费接收缓冲区中的消息
  ///
  /// 返回 (已消费字节数, 待写回应答载荷)：`consumed == 0` 表示尚未凑齐完整帧
  fn try_consume_messages(&self, req_buffer: &[u8]) -> (usize, Vec<u8>);

  /// libs/common/Networking/IServerHook.cs:DisposeMessageConsumer
  /// 释放会话资源
  fn dispose(&self);

  /// 挂载底层服务器引用（会话用于枚举同伴/反查状态）
  /// 默认实现为空操作，保留形参名以满足接口契约
  fn attach_server<S: ServerEnumerate>(&self, _server: Arc<S>) {}
}

/// 会话提供者面（按线格式创建对应的会话消费者）
pub trait SessionProviderFace: Send + Sync {
  /// 消费者类型
  type Consumer: MessageConsumerFace;

  /// libs/common/Networking/IServerHook.cs:TryCreateMessageConsumer
  /// 创建会话消费者
  fn get_session(
    &self,
    wire_format: WireFormat,
    network_sender_id: u64,
  ) -> Option<Arc<Self::Consumer>>;
}
