#![recursion_limit = "512"] // 泛型设备实例化下 async 状态机嵌套深（R27 CI 实证）
//! 出站 TLS 构造期 fail-fast 门：required + 空目标 host 拒绝装配，
//! 其余三形（不校验 / 有目标 / 共享证书源在位）正常出配置
//!
//! 在 garnet 中的相对路径: libs/server/TLS/GarnetTlsOptions.cs:172-176
//!（GetSslClientAuthenticationOptions 开头 throw 臂）

use std::{io, sync::Arc};

use arc_swap::ArcSwap;
use compio_tls::rustls::{
  crypto,
  pki_types::{CertificateDer, PrivateKeyDer, PrivatePkcs8KeyDer},
  sign::CertifiedKey,
};
use rcgen::generate_simple_self_signed;
use wtls::ClientTlsConfig;

/// 内存自签 CertifiedKey（rcgen 产 DER，供共享源形装配）
fn test_certified_key() -> CertifiedKey {
  let ck = generate_simple_self_signed(vec!["localhost".into()]).expect("自签证书生成");
  let key = crypto::ring::sign::any_supported_type(&PrivateKeyDer::Pkcs8(
    PrivatePkcs8KeyDer::from(ck.signing_key.serialized_der().to_vec()),
  ))
  .expect("ring 装载自签私钥");
  CertifiedKey::new(vec![CertificateDer::from(ck.cert.der().to_vec())], key)
}

/// fail-fast 门（GarnetTlsOptions.cs:172-176 对位）：required + 空目标
/// 构造期报错；required=false 空目标回落可用；required + 有目标正常装配
///（证书源句柄在位/缺席两形同受此门）
#[test]
fn empty_target_host_gate() {
  let err = ClientTlsConfig::from_shared_source(None, "", true, None)
    .map(|_| ())
    .expect_err("required 且空目标必须构造期拒绝");
  assert_eq!(err.kind(), io::ErrorKind::InvalidInput);
  assert!(err.to_string().contains("tls-client-target-host"));
  // required=false：出站校验恒真臂，空目标回落 endpoint host 段可用
  ClientTlsConfig::from_shared_source(None, "", false, None).expect("不校验远端时空目标可用");
  ClientTlsConfig::from_shared_source(None, "node1.cluster", true, None).expect("有目标即正常装配");
  // 共享证书源在位形同过门（句柄仅透传，装载语义不入构造期）
  let source = Arc::new(ArcSwap::from_pointee(test_certified_key()));
  ClientTlsConfig::from_shared_source(Some(source), "node1.cluster", true, None)
    .expect("共享源在位即正常装配");
}
