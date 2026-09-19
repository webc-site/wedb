//! 集群出站 TLS 客户端配置（纯 Rust 实现，基于 rustls 与 compio-tls）
//!
//! 与 wnode/src/tls/config.rs（入站 ServerTlsConfig）同一套 rustls 栈的
//! 客户端方向；pem 解析同样经 rustls-pemfile，不引入第二套解析器。

use std::{
  fs::File,
  io,
  path::Path,
  sync::{Arc, LazyLock},
};

use compio::net::TcpStream;
use compio_tls::{
  TlsConnector, TlsStream,
  rustls::{
    self as rustls_ns, ClientConfig, RootCertStore,
    client::danger::{HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier},
    crypto::{self, WebPkiSupportedAlgorithms, ring},
    pki_types::{CertificateDer, PrivateKeyDer, ServerName, UnixTime},
  },
};

/// 集群出站 TLS 配置装配器
///
/// 在 garnet 中的相对路径: libs/server/TLS/GarnetTlsOptions.cs:GetSslClientAuthenticationOptions
///
/// 对标 SslClientAuthenticationOptions 的四要素：
/// - TargetHost（SNI 与远端证书名校验目标，空则建连时回落 endpoint host 段）
/// - ServerCertificateRequired=false → 远端证书校验恒真（C# 校验回调的
///   always-succeed 臂）；true → webpki 校验（roots 为 IssuerCertificatePath
///   指定的签发者 CA，未指定用 webpki-roots 内置根，等价 C# 系统信任链臂）
/// - LocalCertificateSelectionCallback 复用服务端证书对（同一 cert/key 做
///   mTLS 对等证书）
/// - CertificateRevocationCheckMode 不实现（rustls 无吊销检查面）
#[derive(Clone)]
pub struct ClientTlsConfig {
  connector: TlsConnector,
  /// SNI / 远端证书名校验目标（空串 = 建连回落 endpoint host 段）
  target_host: String,
}

impl ClientTlsConfig {
  /// 从 PEM 配置装配出站 TLS 客户端
  ///
  /// 在 garnet 中的相对路径: libs/server/TLS/GarnetTlsOptions.cs:GetSslClientAuthenticationOptions
  ///
  /// `cert_path`/`key_path` 成对提供即携带 mTLS 客户端证书（C# 复用服务端
  /// 证书选择器的对等形态），成对缺席则不带客户端证书；半配对报错。
  /// `server_cert_required` 对标 ServerCertificateRequired（defaults.conf:253
  /// 默认 true）；`issuer_path` 对标 IssuerCertificatePath（空 = 内置根）
  pub fn new(
    cert_path: Option<&Path>,
    key_path: Option<&Path>,
    target_host: &str,
    server_cert_required: bool,
    issuer_path: Option<&Path>,
  ) -> io::Result<Self> {
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
    let config = match (cert_path, key_path) {
      (Some(cert), Some(key)) => {
        let certs = load_certs(cert)?;
        let key = load_private_key(key)?;
        builder
          .with_client_auth_cert(certs, key)
          .map_err(|e| io::Error::new(io::ErrorKind::InvalidInput, e.to_string()))?
      }
      (None, None) => builder.with_no_client_auth(),
      (Some(_), None) | (None, Some(_)) => {
        return Err(io::Error::new(
          io::ErrorKind::InvalidInput,
          "tls 客户端证书与私钥必须成对提供",
        ));
      }
    };
    Ok(Self {
      connector: TlsConnector::from(Arc::new(config)),
      target_host: target_host.to_string(),
    })
  }

  /// 在既有 TCP 流上完成 TLS 握手
  ///
  /// 在 garnet 中的相对路径: libs/client/GarnetClient.cs:ConnectAsync
  ///（SslStream AuthenticateAsClientAsync 的 rustls 等价物）
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

/// 信任根装配：issuer 在位则以其 PEM 为根（C# ValidateCertificateIssuer 臂），
/// 否则用 webpki-roots 内置根（C# 系统信任链臂）
fn root_store(issuer_path: Option<&Path>) -> io::Result<RootCertStore> {
  let mut roots = RootCertStore::empty();
  match issuer_path {
    Some(path) => {
      for cert in load_certs(path)? {
        roots
          .add(cert)
          .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e.to_string()))?;
      }
    }
    None => roots.extend(webpki_roots::TLS_SERVER_ROOTS.iter().cloned()),
  }
  Ok(roots)
}

/// SNI 名解析：target_host 优先，空则 endpoint host 段（剥 IPv6 方括号）
fn server_name(target_host: &str, endpoint: &str) -> io::Result<String> {
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

/// 签名校验算法表（ring provider 单例，避免每次握手重建）
static SIGNATURE_ALGS: LazyLock<WebPkiSupportedAlgorithms> =
  LazyLock::new(|| ring::default_provider().signature_verification_algorithms);

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
    crypto::verify_tls12_signature(message, cert, dss, &SIGNATURE_ALGS)
  }

  fn verify_tls13_signature(
    &self,
    message: &[u8],
    cert: &CertificateDer<'_>,
    dss: &rustls_ns::DigitallySignedStruct,
  ) -> Result<HandshakeSignatureValid, rustls_ns::Error> {
    crypto::verify_tls13_signature(message, cert, dss, &SIGNATURE_ALGS)
  }

  fn supported_verify_schemes(&self) -> Vec<rustls_ns::SignatureScheme> {
    SIGNATURE_ALGS.supported_schemes()
  }
}

/// 加载 PEM 证书链（rustls-pemfile 单一解析栈）
///
/// 与 wnode/src/tls/config.rs:load_certs 同源实现（crate 平级不互依）
fn load_certs(path: &Path) -> io::Result<Vec<CertificateDer<'static>>> {
  let file = File::open(path)?;
  let mut reader = io::BufReader::new(file);
  let certs = rustls_pemfile::certs(&mut reader)
    .collect::<Result<Vec<_>, _>>()
    .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e.to_string()))?;
  if certs.is_empty() {
    return Err(io::Error::new(
      io::ErrorKind::NotFound,
      format!("未在证书文件 {} 中找到有效证书", path.display()),
    ));
  }
  Ok(certs)
}

/// 加载 PEM 私钥（rustls-pemfile 单一解析栈）
///
/// 与 wnode/src/tls/config.rs:load_private_key 同源实现（crate 平级不互依）
fn load_private_key(path: &Path) -> io::Result<PrivateKeyDer<'static>> {
  let file = File::open(path)?;
  let mut reader = io::BufReader::new(file);
  rustls_pemfile::private_key(&mut reader)
    .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e.to_string()))?
    .ok_or_else(|| {
      io::Error::new(
        io::ErrorKind::NotFound,
        format!("未在私钥文件 {} 中找到有效私钥", path.display()),
      )
    })
}

#[cfg(test)]
mod tests {
  use super::*;

  /// SNI 名解析：target_host 优先、空回落 endpoint host 段（含 IPv6 方括号剥除）
  #[test]
  fn server_name_fallback() {
    assert_eq!(server_name("", "10.0.0.1:6379").unwrap(), "10.0.0.1");
    assert_eq!(server_name("", "[::1]:6379").unwrap(), "::1");
    assert_eq!(
      server_name("node.a.io", "10.0.0.1:6379").unwrap(),
      "node.a.io"
    );
    assert_eq!(server_name("", "barehost").unwrap(), "barehost");
  }
}
