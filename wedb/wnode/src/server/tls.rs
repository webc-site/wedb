//! TLS 装配域：GarnetServer 的 TLS 证书与超时注入位
//!
//! 对标 C# GarnetTlsOptions 的服务端装配面（证书对投影入口
//! `tls_config_from_node` 留 mod.rs 公共路径锚）

use super::*;

impl<P: SessionProviderFace + 'static> GarnetServer<P> {
  /// 设置 TLS 证书配置
  #[cfg(feature = "tls")]
  pub fn with_tls_config(mut self, tls_config: impl Into<Option<ServerTlsConfig>>) -> Self {
    self.tls_config = tls_config.into();
    self
  }

  /// 设置 TLS 握手超时（Slowloris 慢速握手防护铁边；生产缺省
  /// [`TLS_HANDSHAKE_TIMEOUT`] 10s，本装配位仅供集成测试注入短超时
  /// 有界收敛验证，ServerBootstrap 标准启动路径不覆盖、恒取缺省值）
  #[cfg(feature = "tls")]
  pub fn with_tls_handshake_timeout(mut self, timeout: Duration) -> Self {
    self.tls_handshake_timeout = timeout;
    self
  }

  /// 设置 TLS 收场尾帧超时界（生产缺省 [`TLS_SHUTDOWN_TIMEOUT`] 1s，本
  /// 装配位仅供集成测试注入短超时有界收敛验证，ServerBootstrap 标准启动
  /// 路径不覆盖、恒取缺省值）
  #[cfg(feature = "tls")]
  pub fn with_tls_shutdown_timeout(mut self, timeout: Duration) -> Self {
    self.tls_shutdown_timeout = timeout;
    self
  }
}
