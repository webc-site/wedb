//! TLS 选项抽象（对标 libs/server/TLS/IGarnetTlsOptions.cs）
//!
//! C# 接口承接证书文件热更新（UpdateCertFile 触发 SslStream 凭据换新）；
//! rust TLS 面由 tls 域（garnet_tls_options 等）并行承接，本接口壳保留
//! 承接说明。

/// TLS 选项抽象
pub struct IGarnetTlsOptions;

impl IGarnetTlsOptions {
  /// libs/server/TLS/IGarnetTlsOptions.cs:UpdateCertFile
  ///
  /// 缺口说明：证书热更新入口；rust TLS 域并行推进面（豁免登记见
  /// js/check/ignore/libs/server/TLS/IGarnetTlsOptions.yml）
  pub fn update_cert_file() {}
}
