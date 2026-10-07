#![recursion_limit = "512"] // 泛型设备实例化下 async 状态机嵌套深（R27 CI 实证）
//! X.509 证书有效期解析与校验集成测试

use compio_tls::rustls::{
  CertificateError, Error,
  pki_types::{CertificateDer, UnixTime},
};
use time::OffsetDateTime;
use wtls::validity::{Tlv, Validity, asn1_time_unix, days_from_civil};

/// 纪元换算锁定已知时点（含闰年边界）
#[test]
fn civil_days_known_epochs() {
  assert_eq!(days_from_civil(1970, 1, 1), 0);
  assert_eq!(days_from_civil(2024, 1, 1) * 86_400, 1_704_067_200);
  assert_eq!(days_from_civil(2025, 1, 1) * 86_400, 1_735_689_600);
  assert_eq!(days_from_civil(2026, 1, 1) * 86_400, 1_767_225_600);
  assert_eq!(days_from_civil(2024, 2, 29) * 86_400, 1_709_164_800);
}

/// UTCTime / GeneralizedTime 两形态解析（rcgen 产 GeneralizedTime 长年，
/// UTCTime 按两位纪年折叠）
#[test]
fn asn1_time_forms() {
  let utc = asn1_time_unix(Tlv {
    tag: 0x17,
    content: b"260101000000Z",
  })
  .expect("UTCTime");
  assert_eq!(utc, 1_767_225_600);
  let generalized = asn1_time_unix(Tlv {
    tag: 0x18,
    content: b"20260101000000Z",
  })
  .expect("GeneralizedTime");
  assert_eq!(generalized, utc, "两形态同刻等值");
  // 两位纪年折叠：50-99 → 19xx
  let fold = asn1_time_unix(Tlv {
    tag: 0x17,
    content: b"991231235959Z",
  })
  .expect("UTCTime 折叠");
  assert_eq!(fold, 946_684_799); // 1999-12-31T23:59:59Z
}

/// rcgen 真证书：窗口可解析且覆盖当前时点
#[test]
fn rcgen_cert_validity_parses() {
  let ck = rcgen::generate_simple_self_signed(vec!["localhost".into()]).expect("自签证书");
  let validity = Validity::from_der(ck.cert.der()).expect("解析");
  let now = UnixTime::now().as_secs();
  assert!(validity.not_before <= now && now <= validity.not_after);
  validity.check(UnixTime::now()).expect("当前时点在窗");
}

/// 过期证书（rcgen 指定过去窗口）：check 报 Expired，当前实现不再无条件放行
#[test]
fn expired_cert_rejected() {
  use rcgen::{CertificateParams, KeyPair};
  let mut params = CertificateParams::new(vec!["expired.test".into()]).expect("参数");
  let (past_start, past_end) = (
    OffsetDateTime::from_unix_timestamp(1_000_000_000).expect("2001-09-09"),
    OffsetDateTime::from_unix_timestamp(1_100_000_000).expect("2004-11-24"),
  );
  params.not_before = past_start;
  params.not_after = past_end;
  let key = KeyPair::generate().expect("密钥");
  let cert = params.self_signed(&key).expect("签发");
  let validity = Validity::from_der(cert.der()).expect("解析");
  let err = validity.check(UnixTime::now()).expect_err("过期必拒");
  assert!(matches!(
    err,
    Error::InvalidCertificate(CertificateError::ExpiredContext { .. })
  ));
}

/// 未生效证书：check 报 NotValidYet
#[test]
fn not_yet_valid_cert_rejected() {
  use rcgen::{CertificateParams, KeyPair};
  let mut params = CertificateParams::new(vec!["future.test".into()]).expect("参数");
  params.not_before = OffsetDateTime::from_unix_timestamp(4_000_000_000).expect("2096-10-13");
  params.not_after = OffsetDateTime::from_unix_timestamp(4_100_000_000).expect("2099-11-25");
  let key = KeyPair::generate().expect("密钥");
  let cert = params.self_signed(&key).expect("签发");
  let validity = Validity::from_der(cert.der()).expect("解析");
  let err = validity.check(UnixTime::now()).expect_err("未生效必拒");
  assert!(matches!(
    err,
    Error::InvalidCertificate(CertificateError::NotValidYetContext { .. })
  ));
}

/// 畸形 DER（截断/错位/非法长度）一律拒绝
#[test]
fn malformed_der_rejected() {
  assert!(Validity::from_der(&CertificateDer::from(vec![])).is_err());
  assert!(Validity::from_der(&CertificateDer::from(vec![0x02, 0x01, 0x00])).is_err());
  // 截断的 SEQUENCE
  assert!(Validity::from_der(&CertificateDer::from(vec![0x30, 0x10, 0x30, 0x08])).is_err());
}
