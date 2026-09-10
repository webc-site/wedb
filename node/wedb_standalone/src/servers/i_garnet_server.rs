//! 服务器接口面（对标 libs/server/Servers/IGarnetServer.cs 与
//! Garnet.networking 的 WireFormat / ISessionProvider / IMessageConsumer）
//!
//! C# 以接口组合承载"会话提供者注册 → 连接到来 → 会话创建"链路；Rust 侧
//! 以对象安全 trait 组承接（会话与提供者均以 `Arc` 句柄共享）。

use std::{io, sync::Arc};

/// 会话线格式（libs/common/Networking/WireFormat.cs:WireFormat）
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(u8)]
pub enum WireFormat {
  /// ASCII 线格式（RESP）
  Ascii = 255,
}

/// 集群会话面（C# Garnet.server/IClusterSession 的标记投影；集群域类型
/// 接入时实现）
pub trait ClusterSessionFace: Send + Sync {
  /// 会话标识（CLIENT KILL / 槽校验日志用）
  fn session_id(&self) -> i64 {
    0
  }
}

/// 消息消费者面（C# Garnet.networking/IMessageConsumer 的服务器侧投影）
///
/// 实现方为会话容器句柄；`dispose` 由服务器在连接处置时调用。
pub trait MessageConsumerFace: Send + Sync {
  /// 释放会话（连接关闭 / 服务器 Dispose 时调用）
  fn dispose(&self);
  /// 关联的集群会话（无集群会话为 None；
  /// C# ((RespServerSession)consumer).clusterSession）
  fn cluster_session(&self) -> Option<Arc<dyn ClusterSessionFace>> {
    None
  }
  /// 服务器回填（C# respSession.Server = this 的投影；实现方保存弱引用
  /// 用于枚举其他会话）
  fn attach_server(&self, server: Arc<dyn ServerEnumerate>);
}

/// 会话提供者面（libs/server/Sessions/ISessionProvider.cs:ISessionProvider
/// 的 GetSession 投影）
pub trait SessionProviderFace: Send + Sync {
  /// 按线格式创建会话（`network_sender_id` 为网络发送器标识）
  fn get_session(
    &self,
    wire_format: WireFormat,
    network_sender_id: u64,
  ) -> Option<Arc<dyn MessageConsumerFace>>;
}

/// 活跃会话枚举面（C# GarnetServerBase.ActiveConsumers / ActiveClusterSessions
/// 的回填投影；RESP 会话经此枚举其他会话）
pub trait ServerEnumerate: Send + Sync {
  /// 全部活跃消息消费者
  fn active_consumers(&self) -> Vec<Arc<dyn MessageConsumerFace>>;
  /// 全部活跃集群会话
  fn active_cluster_sessions(&self) -> Vec<Arc<dyn ClusterSessionFace>>;
}

/// 服务器接口（libs/server/Servers/IGarnetServer.cs:IGarnetServer）
pub trait GarnetServer: Send + Sync {
  /// 注册指定线格式的会话提供者（libs/server/Servers/IGarnetServer.cs:Register）
  fn register(
    &self,
    wire_format: WireFormat,
    backend_provider: Arc<dyn SessionProviderFace>,
  ) -> Result<(), ServerError>;
  /// 注销线格式对应的提供者（libs/server/Servers/IGarnetServer.cs:Unregister）
  fn unregister(&self, wire_format: WireFormat) -> Option<Arc<dyn SessionProviderFace>>;
  /// 提供者表快照（libs/server/Servers/IGarnetServer.cs:GetSessionProviders）
  fn get_session_providers(&self) -> Vec<(WireFormat, Arc<dyn SessionProviderFace>)>;
  /// 创建会话（libs/server/Servers/IGarnetServer.cs:AddSession）
  fn add_session(
    &self,
    wire_format: WireFormat,
    backend_provider: &dyn SessionProviderFace,
    network_sender_id: u64,
  ) -> Option<Arc<dyn MessageConsumerFace>>;
  /// 启动监听（libs/server/Servers/IGarnetServer.cs:Start）
  fn start(&self) -> io::Result<()>;
  /// 停止接受新连接并释放监听端口（libs/server/Servers/IGarnetServer.cs:Close）
  fn close(&self);
}

/// 服务器域错误（C# GarnetException 抛出点的本域投影）
#[derive(Debug, thiserror::Error)]
pub enum ServerError {
  /// 线格式重复注册（C# "Wire format {wireFormat} already registered"）
  #[error("Wire format {0:?} already registered")]
  WireFormatAlreadyRegistered(WireFormat),
}
