//! 集群出站 TLS 客户端配置（纯 Rust 实现，基于 rustls 与 compio-tls）
//!
//! 与 [`crate::server`]（入站 ServerTlsConfig）同一套 rustls 栈的
//! 客户端方向；pem 解析同样经 rustls-pemfile，不引入第二套解析器。

use std::{io, path::Path, sync::Arc};

use arc_swap::ArcSwap;
use compio::net::TcpStream;
use compio_tls::{
  TlsConnector, TlsStream,
  rustls::{
    self as rustls_ns, ClientConfig, RootCertStore,
    client::{
      ResolvesClientCert,
      danger::{HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier},
    },
    pki_types::{CertificateDer, ServerName, UnixTime},
    sign::CertifiedKey,
  },
};

use crate::cert::{
  add_certs, load_certs, supported_verify_schemes, verify_tls12_signature, verify_tls13_signature,
};

/// 集群出站 TLS 配置装配器
///
/// 对位 C# GarnetTlsOptions 的 GetSslClientAuthenticationOptions 件（装配真身锚留
/// [`ClientTlsConfig::from_shared_source`] 一处）
///
/// 对标 SslClientAuthenticationOptions 的四要素：
/// - TargetHost（SNI 与远端证书名校验目标，空则建连时回落 endpoint host 段）
/// - ServerCertificateRequired=false → 远端证书校验恒真（C# 校验回调的
///   always-succeed 臂）；true → webpki 校验（roots 为 IssuerCertificatePath
///   指定的签发者 CA，未指定用 webpki-roots 内置根，等价 C# 系统信任链臂）
/// - LocalCertificateSelectionCallback → 共享证书源动态解析器
///   （[`SharedClientCertResolver`]，握手期现取入站同一 ArcSwap 单源的当前
///   活跃证书，对位 C# 闭包动态读 serverCertificateSelector，
///   GarnetTlsOptions.cs:185-189；热换装与周期刷新即时传播出站）
/// - CertificateRevocationCheckMode 不实现（rustls 仅 unstable CRL 面未用；
///   旋钮删员缺席已在 deviations.md §124d) 实条在册）
#[derive(Clone)]
pub struct ClientTlsConfig {
  connector: TlsConnector,
  /// SNI / 远端证书名校验目标（空串 = 建连回落 endpoint host 段）
  target_host: String,
}

impl ClientTlsConfig {
  /// 从入站共享证书源装配出站 TLS 客户端
  ///
  /// 在 garnet 中的相对路径: libs/server/TLS/GarnetTlsOptions.cs:GetSslClientAuthenticationOptions
  ///
  /// `certs` 为 [`crate::server::ServerTlsConfig::cert_source`] 的共享句柄：
  /// 握手期经 [`SharedClientCertResolver`] load_full 现取当前活跃证书，
  /// CONFIG SET cert-file-name 热换装与周期刷新对出站即时生效（C# 出站闭包
  /// 动态读 selector 的对位）；句柄缺席（仅 issuer/target-host 而无证书对的
  /// 纯校验向配置）回落不带客户端证书，禁拒启。
  /// `server_cert_required` 对标 ServerCertificateRequired（defaults.conf:253
  /// 默认 true）；`issuer_path` 对标 IssuerCertificatePath（空 = 内置根）
  ///
  /// fail-fast 门（对标 C# GetSslClientAuthenticationOptions 开头 throw 臂，
  /// GarnetTlsOptions.cs:172-176）：`server_cert_required && target_host` 为空
  /// 即构造期报错拒绝——校验目标缺失时回落 endpoint host 段会把故障面从
  /// 启动瞬间后移到运行期逐连接握手失败（IP 端点常无对应 IP SAN），且校验
  /// 目标漂移为对端地址，不再是操作者声明的名字
  pub fn from_shared_source(
    certs: Option<Arc<ArcSwap<CertifiedKey>>>,
    target_host: &str,
    server_cert_required: bool,
    issuer_path: Option<&Path>,
  ) -> io::Result<Self> {
    if server_cert_required && target_host.is_empty() {
      return Err(io::Error::new(
        io::ErrorKind::InvalidInput,
        "tls-client-target-host should be provided when tls-server-cert-required is enabled",
      ));
    }
    let builder = ClientConfig::builder();
    let builder = if server_cert_required {
      builder.with_root_certificates(root_store(issuer_path)?)
    } else {
      // 远端证书校验恒真：对齐 C# ServerCertificateRequired=false 时
      // RemoteCertificateValidationCallback 恒返 true 的不安全模式
      builder
        .dangerous()
        .with_custom_certificate_verifier(Arc::new(NoVerify))
    };
    let config = match certs {
      Some(certs) => builder.with_client_cert_resolver(Arc::new(SharedClientCertResolver(certs))),
      None => builder.with_no_client_auth(),
    };
    Ok(Self {
      connector: TlsConnector::from(Arc::new(config)),
      target_host: target_host.to_string(),
    })
  }

  /// 在既有 TCP 流上完成 TLS 握手
  ///
  /// 对应 C# GarnetClient.ConnectAsync 中的 SslStream AuthenticateAsClientAsync 的 rustls 等价物
  ///
  /// SNI 目标：构造期 target_host 优先，空则取 endpoint 的 host 段
  ///（剥 IPv6 方括号；IP 字面量经 ServerName::IpAddress 承载）
  pub async fn connect(
    &self,
    stream: TcpStream,
    endpoint: &str,
  ) -> io::Result<TlsStream<TcpStream>> {
    let domain = server_name(&self.target_host, endpoint)?;
    self.connector.connect(&domain, stream).await
  }
}

/// 出站客户端证书动态解析器：握手期现取共享证书源当前活跃证书
///
/// 对位 C# GarnetTlsOptions 的 GetSslClientAuthenticationOptions 件之选择器闭包段
///（:185-189 LocalCertificateSelectionCallback 闭包忽略 acceptableIssuers
/// 恒返 selector 当前证书的对位——同不筛 root hint，呈出即当前活跃证书）
#[derive(Debug)]
struct SharedClientCertResolver(Arc<ArcSwap<CertifiedKey>>);

impl ResolvesClientCert for SharedClientCertResolver {
  fn resolve(
    &self,
    _root_hint_subjects: &[&[u8]],
    _sigschemes: &[rustls_ns::SignatureScheme],
  ) -> Option<Arc<CertifiedKey>> {
    Some(self.0.load_full())
  }

  /// 句柄在位即有证可呈（对标 C# selector 恒备证书；缺证构造走
  /// with_no_client_auth 臂，不入此解析器）
  fn has_certs(&self) -> bool {
    true
  }
}

/// 信任根装配：issuer 在位则以其 PEM 为根（C# ValidateCertificateIssuer 臂），
/// 否则用 webpki-roots 内置根（C# 系统信任链臂）
fn root_store(issuer_path: Option<&Path>) -> io::Result<RootCertStore> {
  let mut roots = RootCertStore::empty();
  if let Some(path) = issuer_path {
    // 入站 ca_roots 同形骨架：逐张入根库单点在 cert::add_certs
    add_certs(&mut roots, load_certs(path)?)?;
  } else {
    roots.extend(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());
  }
  Ok(roots)
}

/// SNI 名解析：target_host 优先，空则 endpoint host 段（剥 IPv6 方括号）
///
/// pub 直曝：connect 生产面 + tests/client.rs 解析断言共用（免包装转发层）
pub fn server_name(target_host: &str, endpoint: &str) -> io::Result<String> {
  let host = if target_host.is_empty() {
    endpoint.rsplit_once(':').map_or(endpoint, |(host, _)| host)
  } else {
    target_host
  };
  let host = host.strip_prefix('[').unwrap_or(host);
  let host = host.strip_suffix(']').unwrap_or(host);
  // 借用形态预校验（IP 字面量 / DNS 名），合法才落 String 交 connector
  ServerName::try_from(host)
    .map_err(|e| io::Error::new(io::ErrorKind::InvalidInput, e.to_string()))
    .map(|_| host.to_string())
}

/// 不安全校验器：远端证书恒通过
///
/// 对标 GarnetTlsOptions.cs:ValidateServerCertificateCallback 的
/// ServerCertificateRequired=false 臂（验证回调恒返 true）；TLS 握手签名
/// 仍经 ring provider 实校验（.NET SslStream 同样只豁免证书校验面）
#[derive(Debug)]
struct NoVerify;

impl ServerCertVerifier for NoVerify {
  fn verify_server_cert(
    &self,
    _end_entity: &CertificateDer<'_>,
    _intermediates: &[CertificateDer<'_>],
    _server_name: &ServerName<'_>,
    _ocsp_response: &[u8],
    _now: UnixTime,
  ) -> Result<ServerCertVerified, rustls_ns::Error> {
    Ok(ServerCertVerified::assertion())
  }

  fn verify_tls12_signature(
    &self,
    message: &[u8],
    cert: &CertificateDer<'_>,
    dss: &rustls_ns::DigitallySignedStruct,
  ) -> Result<HandshakeSignatureValid, rustls_ns::Error> {
    verify_tls12_signature(message, cert, dss)
  }

  fn verify_tls13_signature(
    &self,
    message: &[u8],
    cert: &CertificateDer<'_>,
    dss: &rustls_ns::DigitallySignedStruct,
  ) -> Result<HandshakeSignatureValid, rustls_ns::Error> {
    verify_tls13_signature(message, cert, dss)
  }

  fn supported_verify_schemes(&self) -> Vec<rustls_ns::SignatureScheme> {
    supported_verify_schemes()
  }
}
