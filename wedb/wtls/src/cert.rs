//! 证书与私钥装载（PEM 文件 → rustls 内存形态）
//!
//! 在 garnet 中的相对路径: libs/server/TLS/CertificateUtils.cs

use std::{
  fs::File,
  io::{self, BufReader},
  path::Path,
};

use compio_tls::rustls::{
  crypto,
  pki_types::{CertificateDer, PrivateKeyDer},
  sign::CertifiedKey,
};

/// 从 PEM 格式文件加载证书链
///
/// 在 garnet 中的相对路径: libs/server/TLS/CertificateUtils.cs:GetCertificateFromPemFile
pub(crate) fn load_certs(path: &Path) -> io::Result<Vec<CertificateDer<'static>>> {
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

/// 从 PEM 格式文件加载私钥
///
/// 在 garnet 中的相对路径: libs/server/TLS/CertificateUtils.cs:GetMachineCertificateByFile
pub(crate) fn load_private_key(path: &Path) -> io::Result<PrivateKeyDer<'static>> {
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
pub(crate) fn certified_key(
  certs: Vec<CertificateDer<'static>>,
  key: PrivateKeyDer<'static>,
) -> io::Result<CertifiedKey> {
  let signing_key = crypto::ring::sign::any_supported_type(&key)
    .map_err(|e| io::Error::new(io::ErrorKind::InvalidInput, e.to_string()))?;
  Ok(CertifiedKey::new(certs, signing_key))
}

#[cfg(test)]
mod tests {
  use std::{
    io::Write,
    path::{Path, PathBuf},
  };

  use super::*;

  /// 运行期自签证书 + PKCS8 私钥 PEM（C# CertificateUtilsTests 同样运行期
  /// 构造证书资产，不依赖仓库内证书文件）
  fn issue_pem() -> (String, String) {
    let ck = rcgen::generate_simple_self_signed(vec!["localhost".into()]).expect("自签证书生成");
    (ck.cert.pem(), ck.signing_key.serialize_pem())
  }

  /// 写测试文件并返回路径
  fn write_file(dir: &Path, name: &str, contents: &[u8]) -> PathBuf {
    let path = dir.join(name);
    File::create(&path)
      .and_then(|mut f| f.write_all(contents))
      .expect("写测试证书文件");
    path
  }

  /// test/standalone/Garnet.test/CertificateUtilsTests.cs:GetMachineCertificateByFileDetectsPemContentsRegardlessOfExtension
  ///
  /// PEM 内容识别不依赖扩展名：无扩展名文件按内容正常装载（rust 侧
  /// load_certs 直接按路径读字节，扩展名从不参与判定）
  #[test]
  fn loads_pem_regardless_of_extension() {
    let (cert_pem, _) = issue_pem();
    let dir = tempfile::tempdir().expect("临时目录");
    let path = write_file(dir.path(), "server.node-cert", cert_pem.as_bytes());
    let certs = load_certs(&path).expect("无扩展名 PEM 应可装载");
    assert_eq!(certs.len(), 1, "自签证书链应为单张");
  }

  /// test/standalone/Garnet.test/CertificateUtilsTests.cs:GetMachineCertificateByFileDetectsPemWithLeadingBomAndBlankLines
  ///
  /// 前导 BOM 与空行不阻塞 PEM 段识别（解析器跳过非 PEM 垃圾段直达
  /// BEGIN 行）
  #[test]
  fn leading_bom_and_blank_lines_tolerated() {
    let (cert_pem, _) = issue_pem();
    let dir = tempfile::tempdir().expect("临时目录");
    let mut bytes = vec![0xEF, 0xBB, 0xBF];
    bytes.extend_from_slice(b"\r\n\r\n");
    bytes.extend_from_slice(cert_pem.as_bytes());
    let path = write_file(dir.path(), "bom.pem", &bytes);
    let certs = load_certs(&path).expect("BOM + 空行前导应被容忍");
    assert_eq!(certs.len(), 1);
  }

  /// test/standalone/Garnet.test/CertificateUtilsTests.cs:GetMachineCertificateByFileLoadsPemCertificateWithEmbeddedKey
  ///
  /// 证书与私钥同文件（PEM 嵌入形态）：私钥装载从混合文件中定位 KEY 段
  #[test]
  fn embedded_key_pem_loads() {
    let (cert_pem, key_pem) = issue_pem();
    let dir = tempfile::tempdir().expect("临时目录");
    let combined = format!("{cert_pem}\n{key_pem}");
    let path = write_file(dir.path(), "combined.pem", combined.as_bytes());
    load_certs(&path).expect("混合文件应装载出证书链");
    load_private_key(&path).expect("混合文件应装载出私钥");
  }

  /// test/standalone/Garnet.test/CertificateUtilsTests.cs:GetMachineCertificateByFileLoadsPemCertificateWithSeparateKeyFile
  ///
  /// 证书与私钥分离文件：各按路径独立装载
  #[test]
  fn separate_key_file_loads() {
    let (cert_pem, key_pem) = issue_pem();
    let dir = tempfile::tempdir().expect("临时目录");
    let cert_path = write_file(dir.path(), "cert.pem", cert_pem.as_bytes());
    let key_path = write_file(dir.path(), "key.pem", key_pem.as_bytes());
    assert_eq!(load_certs(&cert_path).expect("证书装载").len(), 1);
    load_private_key(&key_path).expect("私钥装载");
  }

  /// 无有效 PEM 段的文件报 NotFound（GetMachineCertificate 类错误臂的
  /// rust 形态：空证书链显式失败，绝不静默产出空链）
  #[test]
  fn file_without_pem_section_is_not_found() {
    let dir = tempfile::tempdir().expect("临时目录");
    let path = write_file(dir.path(), "junk.pem", b"not a pem file\n");
    let err = load_certs(&path).expect_err("无 PEM 段必须报错");
    assert_eq!(err.kind(), io::ErrorKind::NotFound);
  }

  /// 缺失证书文件打开报错包含路径与角色（证书）
  #[test]
  fn missing_cert_file_error_contains_path_and_role() {
    let missing_path = Path::new("/tmp/nonexistent_cert_file_12345.pem");
    let err = load_certs(missing_path).expect_err("缺失证书文件必须报错");
    let msg = err.to_string();
    assert!(
      msg.contains("/tmp/nonexistent_cert_file_12345.pem"),
      "报错应包含路径: {msg}"
    );
    assert!(msg.contains("证书"), "报错应包含证书角色: {msg}");
  }

  /// 缺失私钥文件打开报错包含路径与角色（私钥）
  #[test]
  fn missing_key_file_error_contains_path_and_role() {
    let missing_path = Path::new("/tmp/nonexistent_key_file_12345.pem");
    let err = load_private_key(missing_path).expect_err("缺失私钥文件必须报错");
    let msg = err.to_string();
    assert!(
      msg.contains("/tmp/nonexistent_key_file_12345.pem"),
      "报错应包含路径: {msg}"
    );
    assert!(msg.contains("私钥"), "报错应包含私钥角色: {msg}");
  }

  // PFX/PKCS#12 容器臂不在 rust 转写面（.NET 专有装载路径，转写纪律
  // 删除，rust 唯一接受 PEM 形态，CertificateUtilsTests.cs 对应 5 例中
  // 的 GetMachineCertificateByFileLoadsPfxCertificate 不移植）
}
