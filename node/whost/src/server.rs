//! 宿主装配层（对标 libs/host/GarnetServer.cs:GarnetServer）
//!
//! C# 宿主按 `opts.EndPoints` 逐端点构造 GarnetServerTcp、统一注册 RESP
//! 会话提供者，启动/关停走全量两阶段序：Start 先恢复后逐服务器开监听；
//! InternalDispose 先全量 Close 释放端口，再全量 Dispose 排空处理器
//! （Phase 3 的 Provider/存储引擎关停归存储域）。本模块承接同一装配与
//! 关停序；命令行解析（ServerSettingsManager）与日志面另行接线。

use std::{io, sync::Arc};

use wedb_standalone::servers::{
  garnet_server_tcp::GarnetServerTcp,
  i_garnet_server::{GarnetServer as GarnetServerFace, SessionProviderFace, WireFormat},
};

/// Garnet 宿主服务器
pub struct GarnetServer {
  /// TCP 服务器实例（C# `IGarnetServer[] servers`，每端点一个）
  servers: Vec<Arc<GarnetServerTcp>>,
}

impl GarnetServer {
  /// 装配宿主服务器：每端点一个 TCP 服务器并注册 RESP 会话提供者
  ///
  /// libs/host/GarnetServer.cs:InitializeServer（`servers == null` 分支：
  /// `new GarnetServerTcp(opts.EndPoints[i], ...)` + `Register(WireFormat.ASCII,
  /// Provider)`）
  pub fn new(
    endpoints: &[String],
    network_buffer_size: usize,
    network_send_throttle_max: usize,
    network_connection_limit: i32,
    session_provider: Arc<dyn SessionProviderFace>,
  ) -> Self {
    let servers = endpoints
      .iter()
      .map(|endpoint| {
        let server = Arc::new(GarnetServerTcp::new(
          endpoint,
          network_buffer_size,
          network_send_throttle_max,
          network_connection_limit,
        ));
        // 全新服务器首个注册绝不冲突（C# Register 同位仅此一处调用）
        server
          .register(WireFormat::Ascii, Arc::clone(&session_provider))
          .expect("全新服务器的首次注册不可冲突");
        server
      })
      .collect();
    Self { servers }
  }

  /// TCP 服务器实例表（提供者/存储域装配用）
  pub fn servers(&self) -> &[Arc<GarnetServerTcp>] {
    &self.servers
  }

  /// 启动服务器：逐服务器绑定并进入监听态
  ///
  /// libs/host/GarnetServer.cs:Start
  ///
  /// C# Start 先 `Provider.RecoverAsync` 恢复再开监听；恢复属存储域，
  /// 由调用方在本次调用之前完成。任一监听失败即上抛（C# 异常语义）。
  pub fn start(&self) -> io::Result<()> {
    for server in &self.servers {
      server.start()?;
    }
    Ok(())
  }

  /// 两阶段关停
  ///
  /// libs/host/GarnetServer.cs:InternalDispose
  ///
  /// Phase 1：全量 Close——先停止全部监听，立即释放端口并阻断新连接；
  /// Phase 2：全量 Dispose——排空活跃处理器与提供者表。
  /// Phase 3（Provider/存储引擎关停，耗时面）归存储域，由调用方接续。
  pub fn dispose(&self) {
    for server in &self.servers {
      server.close();
    }
    for server in &self.servers {
      server.dispose();
    }
  }
}

/// libs/host/ServerSettingsManager.cs:ServerSettingsManager
pub struct ServerSettingsManager;

/// libs/host/MemoryLogger.cs:MemoryLogger
pub struct MemoryLogger;

/// libs/host/MemoryLogger.cs:MemoryLoggerProvider
pub struct MemoryLoggerProvider;

/// libs/host/MemoryLogger.cs:LoggingBuilderExtensions
pub struct LoggingBuilderExtensions;

#[cfg(test)]
mod tests {
  use std::{net::TcpListener, sync::Arc};

  use wedb_standalone::servers::i_garnet_server::{
    MessageConsumerFace, ServerEnumerate, SessionProviderFace, WireFormat,
  };

  use super::*;

  struct TestConsumer;
  impl MessageConsumerFace for TestConsumer {
    fn dispose(&self) {}
    fn attach_server(&self, _server: Arc<dyn ServerEnumerate>) {}
  }

  struct TestProvider;
  impl SessionProviderFace for TestProvider {
    fn get_session(
      &self,
      _wire_format: WireFormat,
      _network_sender_id: u64,
    ) -> Option<Arc<dyn MessageConsumerFace>> {
      Some(Arc::new(TestConsumer))
    }
  }

  fn assembly(endpoints: &[String]) -> GarnetServer {
    GarnetServer::new(endpoints, 0, 8, -1, Arc::new(TestProvider))
  }

  #[test]
  fn assembles_one_server_per_endpoint() {
    let endpoints = ["127.0.0.1:0".to_string(), "127.0.0.1:0".to_string()];
    let host = assembly(&endpoints);
    assert_eq!(host.servers().len(), 2);
    // RESP 提供者已登记到每个实例
    for server in host.servers() {
      assert!(
        server
          .base()
          .find_session_provider(WireFormat::Ascii)
          .is_some()
      );
    }
  }

  #[test]
  fn start_and_two_phase_dispose() {
    let host = assembly(&["127.0.0.1:0".to_string()]);
    host.start().expect("监听启动成功");

    // 关停序：Phase 1 全量释放端口，Phase 2 排空处理器
    host.dispose();
    for server in host.servers() {
      assert!(server.base().disposed());
      assert_eq!(server.base().get_conn_active(), 0);
    }
    // 幂等
    host.dispose();
  }

  #[test]
  fn start_failure_reports_error() {
    // 预先占用端口，装配后的监听必失败
    let occupied = TcpListener::bind("127.0.0.1:0").expect("占用端口成功");
    let addr = occupied.local_addr().expect("本地地址可得").to_string();
    let host = assembly(&[addr]);
    assert!(host.start().is_err());
  }
}
