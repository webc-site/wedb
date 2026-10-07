//! 证书与私钥装载（PEM 文件 → rustls 内存形态）
//!
//! 在 garnet 中的相对路径: libs/server/TLS/CertificateUtils.cs

use std::{
  fs::File,
  io::{self, BufReader},
  path::Path,
  sync::LazyLock,
};

use compio_tls::rustls::{
  DigitallySignedStruct, Error, RootCertStore, SignatureScheme,
  client::danger::HandshakeSignatureValid,
  crypto::{self, WebPkiSupportedAlgorithms, ring},
  pki_types::{CertificateDer, PrivateKeyDer},
  sign::CertifiedKey,
};

/// 签名校验算法表（ring provider 单例，进程内唯一定义点）：入站
/// `AnyClientCert` 与出站 `NoVerify` 两校验器
/// 握手签名核验同源共用，避免多份 LazyLock 各自重建 provider
pub static SIGNATURE_ALGS: LazyLock<WebPkiSupportedAlgorithms> =
  LazyLock::new(|| ring::default_provider().signature_verification_algorithms);

/// 从 PEM 格式文件加载证书链
///
/// 在 garnet 中的相对路径: libs/server/TLS/CertificateUtils.cs:GetCertificateFromPemFile
pub fn load_certs(path: &Path) -> io::Result<Vec<CertificateDer<'static>>> {
  let file = File::open(path)
    .map_err(|e| io::Error::other(format!("打开证书文件 {} 失败: {e}", path.display())))?;
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

/// 从 PEM 格式文件加载私钥（C# CertificateUtils.GetMachineCertificateByFile 的
/// 私钥解析子件；该合成体同名锚单点留 [`certified_key`] 一处）
pub fn load_private_key(path: &Path) -> io::Result<PrivateKeyDer<'static>> {
  let file = File::open(path)
    .map_err(|e| io::Error::other(format!("打开私钥文件 {} 失败: {e}", path.display())))?;
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

/// 装载证书链与私钥为 rustls CertifiedKey（签名钥经 ring provider 单点支撑）
///
/// 在 garnet 中的相对路径: libs/server/TLS/CertificateUtils.cs:GetMachineCertificateByFile
pub fn certified_key(
  certs: Vec<CertificateDer<'static>>,
  key: PrivateKeyDer<'static>,
) -> io::Result<CertifiedKey> {
  let signing_key = crypto::ring::sign::any_supported_type(&key)
    .map_err(|e| io::Error::new(io::ErrorKind::InvalidInput, e.to_string()))?;
  Ok(CertifiedKey::new(certs, signing_key))
}

/// 证书逐张入根库（load→add→map_err 循环单点）：入站 CA 装载
/// `ca_roots` 与出站信任根 `root_store`
/// 同形骨架收口，单证非法即整装失败（InvalidData）
pub fn add_certs(
  roots: &mut RootCertStore,
  certs: impl IntoIterator<Item = CertificateDer<'static>>,
) -> io::Result<()> {
  for cert in certs {
    roots
      .add(cert)
      .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e.to_string()))?;
  }
  Ok(())
}

/// TLS1.2 握手签名核验（入站 `AnyClientCert` 与出站
/// `NoVerify` 两豁免链校验器逐字同构三件套之一，算法表
/// 同源 [`SIGNATURE_ALGS`]）
#[inline]
pub fn verify_tls12_signature(
  message: &[u8],
  cert: &CertificateDer<'_>,
  dss: &DigitallySignedStruct,
) -> Result<HandshakeSignatureValid, Error> {
  crypto::verify_tls12_signature(message, cert, dss, &SIGNATURE_ALGS)
}

/// TLS1.3 握手签名核验（三件套之二，同 [`verify_tls12_signature`] 单点）
#[inline]
pub fn verify_tls13_signature(
  message: &[u8],
  cert: &CertificateDer<'_>,
  dss: &DigitallySignedStruct,
) -> Result<HandshakeSignatureValid, Error> {
  crypto::verify_tls13_signature(message, cert, dss, &SIGNATURE_ALGS)
}

/// 校验器支持签名算法表（三件套之三：`supported_verify_schemes` 出形单点）
#[inline]
pub fn supported_verify_schemes() -> Vec<SignatureScheme> {
  SIGNATURE_ALGS.supported_schemes()
}
