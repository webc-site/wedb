//! TLS 证书与鉴权配置（纯 Rust 实现，基于 rustls 与 compio-tls）
//!
//! 1:1 对标微软 Garnet libs/server/TLS/GarnetTlsOptions.cs 与 IGarnetTlsOptions.cs

#[cfg(feature = "tls")]
use std::{
  io,
  path::Path,
  sync::{Arc, LazyLock},
};

#[cfg(feature = "tls")]
use compio_tls::rustls::{
  DigitallySignedStruct, RootCertStore, ServerConfig, SignatureScheme,
  client::danger::HandshakeSignatureValid,
  crypto::{self, WebPkiSupportedAlgorithms, ring},
  pki_types::{CertificateDer, PrivateKeyDer, UnixTime},
  server::{
    WebPkiClientVerifier,
    danger::{ClientCertVerified, ClientCertVerifier},
  },
};
#[cfg(feature = "tls")]
use compio_tls::{TlsAcceptor, rustls};
#[cfg(feature = "tls")]
use wbase::tls::{load_certs, load_private_key};

/// TLS 服务端配置装配器
#[derive(Clone)]
pub struct ServerTlsConfig {
  #[cfg(feature = "tls")]
  acceptor: TlsAcceptor,
}

impl ServerTlsConfig {
  /// 从 PEM 格式的证书与私钥文件加载配置（纯 Rust，零 C/OpenSSL 依赖）
  ///
  /// `client_cert_required` 对标 ClientCertificateRequired；`issuer_path` 对标
  /// IssuerCertificatePath（客户端证书校验根，None = 宽松模式见 [`client_verifier`]）
  #[cfg(feature = "tls")]
  pub fn from_pem_files(
    cert_path: impl AsRef<Path>,
    key_path: impl AsRef<Path>,
    client_cert_required: bool,
    issuer_path: Option<&Path>,
  ) -> io::Result<Self> {
    let certs = load_certs(cert_path.as_ref())?;
    let key = load_private_key(key_path.as_ref())?;
    let issuer = issuer_path.map(CaSource::Pem);

    server_config(certs, key, client_cert_required, issuer).map(|config| Self {
      acceptor: TlsAcceptor::from(Arc::new(config)),
    })
  }

  /// 从 DER 格式证书链与私钥构造（供测试或自签名证书直接内存装配）
  ///
  /// 在 garnet 中的相对路径: libs/server/TLS/GarnetTlsOptions.cs:GetSslServerAuthenticationOptions
  ///
  /// 参数语义同 [`Self::from_pem_files`]，`issuer_ca` 为内存 DER 形态的签发者 CA
  #[cfg(feature = "tls")]
  pub fn from_der(
    certs: Vec<CertificateDer<'static>>,
    key: PrivateKeyDer<'static>,
    client_cert_required: bool,
    issuer_ca: Option<Vec<CertificateDer<'static>>>,
  ) -> io::Result<Self> {
    let issuer = issuer_ca.map(CaSource::Der);

    server_config(certs, key, client_cert_required, issuer).map(|config| Self {
      acceptor: TlsAcceptor::from(Arc::new(config)),
    })
  }

  /// 获取 compio-tls Acceptor
  #[cfg(feature = "tls")]
  #[inline]
  pub fn acceptor(&self) -> &TlsAcceptor {
    &self.acceptor
  }
}

/// 签发者 CA 来源（PEM 文件或内存 DER，两构造入口收敛同一装配链）
#[cfg(feature = "tls")]
enum CaSource<'a> {
  Pem(&'a Path),
  Der(Vec<CertificateDer<'static>>),
}

/// 服务端 ServerConfig 单点装配：客户端认证面三态收敛于此
///
/// 在 garnet 中的相对路径: libs/server/TLS/GarnetTlsOptions.cs:GetSslServerAuthenticationOptions
///
/// C# `SslServerAuthenticationOptions { ClientCertificateRequired,
/// RemoteCertificateValidationCallback }` 的 rustls 对位：required=false →
/// 不请求客户端证书（单向 TLS，行为零变化）；required=true →
/// with_client_cert_verifier（校验器形态见 [`client_verifier`]）
#[cfg(feature = "tls")]
fn server_config(
  certs: Vec<CertificateDer<'static>>,
  key: PrivateKeyDer<'static>,
  client_cert_required: bool,
  issuer: Option<CaSource<'_>>,
) -> io::Result<ServerConfig> {
  let builder = ServerConfig::builder();
  let builder = if client_cert_required {
    builder.with_client_cert_verifier(client_verifier(issuer)?)
  } else {
    builder.with_no_client_auth()
  };
  builder
    .with_single_cert(certs, key)
    .map_err(|e| io::Error::new(io::ErrorKind::InvalidInput, e.to_string()))
}

/// 客户端证书校验器装配
///
/// 在 garnet 中的相对路径: libs/server/TLS/GarnetTlsOptions.cs:ValidateClientCertificateCallback
///
/// - issuer 在位 → rustls 原生 [`WebPkiClientVerifier`]（根为 issuer PEM/DER
///   载入的 CA，对标 C# ValidateCertificateIssuer 的颁发者钉根臂）
/// - issuer 缺席 → [`AnyClientCert`] 宽松校验器（要求证书但不校验颁发者链，
///   对标 C# GetCertificateIssuer :273 告警语义，构造期 warn 一次）
#[cfg(feature = "tls")]
fn client_verifier(issuer: Option<CaSource<'_>>) -> io::Result<Arc<dyn ClientCertVerifier>> {
  match issuer {
    Some(src) => WebPkiClientVerifier::builder(Arc::new(ca_roots(src)?))
      .build()
      .map_err(|e| io::Error::new(io::ErrorKind::InvalidInput, e.to_string())),
    None => {
      log::warn!(
        "tls_client_cert_required=true 且未提供 tls_issuer_cert：要求客户端证书但不校验颁发者链（GarnetTlsOptions.cs:273）"
      );
      Ok(Arc::new(AnyClientCert))
    }
  }
}

/// CA 信任根装载（空证书集快速失败，禁静默空根拒绝一切）
///
/// 在 garnet 中的相对路径: libs/server/TLS/GarnetTlsOptions.cs:GetCertificateIssuer
#[cfg(feature = "tls")]
fn ca_roots(src: CaSource<'_>) -> io::Result<RootCertStore> {
  let mut roots = RootCertStore::empty();
  let certs = match src {
    CaSource::Pem(path) => load_certs(path)?,
    CaSource::Der(certs) => certs,
  };
  for cert in certs {
    roots
      .add(cert)
      .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e.to_string()))?;
  }
  if roots.is_empty() {
    return Err(io::Error::new(
      io::ErrorKind::NotFound,
      "tls_issuer_cert 未提供任何可用的 CA 证书",
    ));
  }
  Ok(roots)
}

/// 宽松客户端证书校验器：证书在位即过，不校验颁发者链
///
/// 在 garnet 中的相对路径: libs/server/TLS/GarnetTlsOptions.cs:ValidateCertificateIssuer
///（authority == null 臂：链策略 AllowUnknownCertificateAuthority，任意自签/未知
/// CA 证书放行）
///
/// 与 wconn/tls.rs 的 NoVerify（出站远端恒真）对等形态：握手指名校验仍全量
/// 委托 rustls provider 实校验（.NET SslStream 同样只豁免证书链校验面），
/// 证书缺失仍按 client_auth_mandatory 拒绝——禁静默降级为不请求证书
#[cfg(feature = "tls")]
#[derive(Debug)]
struct AnyClientCert;

/// 签名校验算法表（ring provider 单例，避免每次握手重建；与 wconn/tls.rs 同源形态）
#[cfg(feature = "tls")]
static SIGNATURE_ALGS: LazyLock<WebPkiSupportedAlgorithms> =
  LazyLock::new(|| ring::default_provider().signature_verification_algorithms);

#[cfg(feature = "tls")]
impl ClientCertVerifier for AnyClientCert {
  /// 证书必选（对标 ClientCertificateRequired=true：缺失即握手失败）
  fn client_auth_mandatory(&self) -> bool {
    true
  }

  /// 无钉根 CA 即不发送 certificate_authorities 提示
  fn root_hint_subjects(&self) -> &[rustls::DistinguishedName] {
    &[]
  }

  /// 链校验豁免面：证书在位即过（DER 合法性由后续指名校验兜底）
  fn verify_client_cert(
    &self,
    _end_entity: &CertificateDer<'_>,
    _intermediates: &[CertificateDer<'_>],
    _now: UnixTime,
  ) -> Result<ClientCertVerified, rustls::Error> {
    Ok(ClientCertVerified::assertion())
  }

  fn verify_tls12_signature(
    &self,
    message: &[u8],
    cert: &CertificateDer<'_>,
    dss: &DigitallySignedStruct,
  ) -> Result<HandshakeSignatureValid, rustls::Error> {
    crypto::verify_tls12_signature(message, cert, dss, &SIGNATURE_ALGS)
  }

  fn verify_tls13_signature(
    &self,
    message: &[u8],
    cert: &CertificateDer<'_>,
    dss: &DigitallySignedStruct,
  ) -> Result<HandshakeSignatureValid, rustls::Error> {
    crypto::verify_tls13_signature(message, cert, dss, &SIGNATURE_ALGS)
  }

  fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
    SIGNATURE_ALGS.supported_schemes()
  }
}
