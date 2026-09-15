//! TLS 证书与鉴权配置（纯 Rust 实现，基于 rustls 与 compio-tls）
//!
//! 1:1 对标微软 Garnet libs/server/TLS/GarnetTlsOptions.cs 与 IGarnetTlsOptions.cs

#[cfg(feature = "tls")]
use std::{
  fs::File,
  io::{self, BufReader},
  path::Path,
  sync::Arc,
};

#[cfg(feature = "tls")]
use compio_tls::{
  TlsAcceptor,
  rustls::{
    ServerConfig,
    pki_types::{CertificateDer, PrivateKeyDer},
  },
};

/// TLS 服务端配置装配器
#[derive(Clone)]
pub struct ServerTlsConfig {
  #[cfg(feature = "tls")]
  acceptor: TlsAcceptor,
}

impl ServerTlsConfig {
  /// 从 PEM 格式的证书与私钥文件加载配置（纯 Rust，零 C/OpenSSL 依赖）
  #[cfg(feature = "tls")]
  pub fn from_pem_files(
    cert_path: impl AsRef<Path>,
    key_path: impl AsRef<Path>,
  ) -> io::Result<Self> {
    let certs = load_certs(cert_path.as_ref())?;
    let key = load_private_key(key_path.as_ref())?;

    let server_config = ServerConfig::builder()
      .with_no_client_auth()
      .with_single_cert(certs, key)
      .map_err(|e| io::Error::new(io::ErrorKind::InvalidInput, e.to_string()))?;

    Ok(Self {
      acceptor: TlsAcceptor::from(Arc::new(server_config)),
    })
  }

  /// 从 DER 格式证书链与私钥构造（供测试或自签名证书直接内存装配）
  #[cfg(feature = "tls")]
  pub fn from_der(
    certs: Vec<CertificateDer<'static>>,
    key: PrivateKeyDer<'static>,
  ) -> io::Result<Self> {
    let server_config = ServerConfig::builder()
      .with_no_client_auth()
      .with_single_cert(certs, key)
      .map_err(|e| io::Error::new(io::ErrorKind::InvalidInput, e.to_string()))?;

    Ok(Self {
      acceptor: TlsAcceptor::from(Arc::new(server_config)),
    })
  }

  /// 获取 compio-tls Acceptor
  #[cfg(feature = "tls")]
  #[inline]
  pub fn acceptor(&self) -> &TlsAcceptor {
    &self.acceptor
  }
}

#[cfg(feature = "tls")]
fn load_certs(path: &Path) -> io::Result<Vec<CertificateDer<'static>>> {
  let file = File::open(path)?;
  let mut reader = BufReader::new(file);
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

#[cfg(feature = "tls")]
fn load_private_key(path: &Path) -> io::Result<PrivateKeyDer<'static>> {
  let file = File::open(path)?;
  let mut reader = BufReader::new(file);
  rustls_pemfile::private_key(&mut reader)
    .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e.to_string()))?
    .ok_or_else(|| {
      io::Error::new(
        io::ErrorKind::NotFound,
        format!("未在私钥文件 {} 中找到有效私钥", path.display()),
      )
    })
}
