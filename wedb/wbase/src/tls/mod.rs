//! TLS 证书与私钥 PEM 加载工具（单点收敛）
//!
//! 在 garnet 中的相对路径: libs/server/TLS/CertificateUtils.cs:GetMachineCertificateByFile

use std::{
  fs::File,
  io::{self, BufReader},
  path::Path,
};

use rustls_pki_types::{CertificateDer, PrivateKeyDer};

/// 从 PEM 格式文件加载证书链
///
/// 在 garnet 中的相对路径: libs/server/TLS/CertificateUtils.cs:GetCertificateFromPemFile
pub fn load_certs(path: &Path) -> io::Result<Vec<CertificateDer<'static>>> {
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

/// 从 PEM 格式文件加载私钥
///
/// 在 garnet 中的相对路径: libs/server/TLS/CertificateUtils.cs:GetMachineCertificateByFile
pub fn load_private_key(path: &Path) -> io::Result<PrivateKeyDer<'static>> {
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
pub mod stream;
