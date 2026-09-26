//! X.509 证书有效期窗口抽取（DER 严格 TLV 定位，畸形即拒绝）
//!
//! 在 garnet 中的相对路径: libs/server/TLS/GarnetTlsOptions.cs
//! ValidateCertificateIssuer（C# X509Chain.Build 的 NotTimeValid 时间面：
//! 宽松 mTLS 臂豁免链信任构建，但不豁免有效期窗口——过期/未生效客户端
//! 证书放行即身份门失效）
//!
//! 失败方向恒为拒绝（fail-closed）：解析失败、结构错位、时间非法一律报错，
//! 由校验臂映射为握手失败——解析器缺陷只可能误拒（可用性），不可能误放
//! （安全性）。时间换算为纯函数（civil_from_days 公历纪元算法），单测锁定
//! 已知纪元值

use std::time::Duration;

use compio_tls::rustls::{
  CertificateError, Error,
  pki_types::{CertificateDer, UnixTime},
};

/// 证书有效期窗口（Unix 秒，闭区间 `[not_before, not_after]`）
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct Validity {
  not_before: u64,
  not_after: u64,
}

impl Validity {
  /// 从 DER 证书抽取有效期窗口（结构错位/时间非法即 Err）
  pub(crate) fn from_der(cert: &CertificateDer<'_>) -> Result<Self, Error> {
    parse_cert_validity(cert.as_ref())
      .ok_or_else(|| Error::InvalidCertificate(CertificateError::BadEncoding))
  }

  /// 校验时点落窗（`now` 窗口外即过期/未生效，映射 rustls 对应变体，
  /// 带上下文数值供握手报错可观测）
  pub(crate) fn check(&self, now: UnixTime) -> Result<(), Error> {
    let now = now.as_secs();
    if now < self.not_before {
      return Err(Error::InvalidCertificate(
        CertificateError::NotValidYetContext {
          time: unix_time(now),
          not_before: unix_time(self.not_before),
        },
      ));
    }
    if now > self.not_after {
      return Err(Error::InvalidCertificate(
        CertificateError::ExpiredContext {
          time: unix_time(now),
          not_after: unix_time(self.not_after),
        },
      ));
    }
    Ok(())
  }
}

/// Unix 秒 → [`UnixTime`]（窗口上下文数值的构造原语）
fn unix_time(secs: u64) -> UnixTime {
  UnixTime::since_unix_epoch(Duration::from_secs(secs))
}

/// 单个 DER TLV（tag + 定长内容切片；不定长 0x80 形态直接拒绝）
struct Tlv<'a> {
  tag: u8,
  content: &'a [u8],
}

/// 读一个 TLV：短形与长形长度均支持，剩余不足即 None（fail-closed）
fn next_tlv<'a>(input: &mut &'a [u8]) -> Option<Tlv<'a>> {
  let (&tag, rest) = input.split_first()?;
  let (&first, mut rest) = rest.split_first()?;
  let len = match first {
    // 高位全 1 且低 6 位为 0 = 不定长（DER 禁用）；拒绝
    0x80 => return None,
    len if len < 0x80 => len as usize,
    n @ 0x81..=0x84 => {
      let num_bytes = (n & 0x7f) as usize;
      let (len_bytes, content_rest) = rest.split_at_checked(num_bytes)?;
      rest = content_rest;
      len_bytes
        .iter()
        .fold(0usize, |acc, &b| (acc << 8) | b as usize)
    }
    // 长度字节数 > 4（本面证书不适用）拒绝
    _ => return None,
  };
  let (content, rest2) = rest.split_at_checked(len)?;
  *input = rest2;
  Some(Tlv { tag, content })
}

/// 定位 tbsCertificate 内的 validity SEQUENCE 并抽取两个 Time
fn parse_cert_validity(der: &[u8]) -> Option<Validity> {
  let mut top = der;
  // Certificate ::= SEQUENCE { tbsCertificate, signatureAlgorithm, signatureValue }
  let cert = next_tlv(&mut top)?;
  if cert.tag != 0x30 {
    return None;
  }
  let mut tbs = cert.content;
  // tbsCertificate ::= SEQUENCE { version [0] EXPLICIT(可选), serialNumber,
  //   signature, issuer, validity, ... }
  let tbs_seq = next_tlv(&mut tbs)?;
  if tbs_seq.tag != 0x30 {
    return None;
  }
  let mut fields = tbs_seq.content;
  // version [0] EXPLICIT（v1 证书缺席该字段，两形态都接受）
  if let Some(head) = fields.first()
    && *head == 0xa0
  {
    next_tlv(&mut fields)?;
  }
  // serialNumber INTEGER
  let serial = next_tlv(&mut fields)?;
  if serial.tag != 0x02 {
    return None;
  }
  // signature AlgorithmIdentifier
  let sig = next_tlv(&mut fields)?;
  if sig.tag != 0x30 {
    return None;
  }
  // issuer Name
  let issuer = next_tlv(&mut fields)?;
  if issuer.tag != 0x30 {
    return None;
  }
  // validity SEQUENCE { notBefore Time, notAfter Time }
  let validity = next_tlv(&mut fields)?;
  if validity.tag != 0x30 {
    return None;
  }
  let mut times = validity.content;
  let not_before = asn1_time_unix(next_tlv(&mut times)?)?;
  let not_after = asn1_time_unix(next_tlv(&mut times)?)?;
  // 非法窗口（签发晚于过期）按畸形拒绝
  (not_before <= not_after).then_some(Validity {
    not_before,
    not_after,
  })
}

/// ASN.1 Time（UTCTime 0x17 / GeneralizedTime 0x18，RFC 5280 Z 形态）转 Unix 秒
fn asn1_time_unix(tlv: Tlv<'_>) -> Option<u64> {
  let bytes = tlv.content;
  // 全数字 + 尾部 'Z'（秒必在场的紧缩形态；偏移时区形态 RFC 5280 禁用）
  let (&last, digits) = bytes.split_last()?;
  if last != b'Z' {
    return None;
  }
  let (year_len, year_base) = match tlv.tag {
    0x17 => (2usize, 1900i64), // UTCTime
    0x18 => (4, 0),            // GeneralizedTime
    _ => return None,
  };
  if digits.len() < year_len + 10 || !digits.iter().all(u8::is_ascii_digit) {
    return None;
  }
  let num = |from: usize, len: usize| -> Option<i64> {
    digits
      .get(from..from + len)?
      .iter()
      .fold(0i64, |acc, &b| acc * 10 + i64::from(b - b'0'))
      .into()
  };
  let mut year = num(0, year_len)? + year_base;
  // UTCTime 两位纪年：RFC 5280 截断规则（00-49 → 20xx，50-99 → 19xx）
  if year_len == 2 {
    year = if year >= 1950 { year } else { year + 100 };
  }
  let month = num(year_len, 2)?;
  let day = num(year_len + 2, 2)?;
  let hour = num(year_len + 4, 2)?;
  let minute = num(year_len + 6, 2)?;
  let second = num(year_len + 8, 2)?;
  if !(1..=12).contains(&month)
    || !(1..=31).contains(&day)
    || hour > 23
    || minute > 59
    || second > 60
  {
    return None;
  }
  // 纪元前时点（负值）超出 u64 域，fail-closed 拒绝
  let secs = days_from_civil(year, month, day) * 86_400 + hour * 3_600 + minute * 60 + second;
  u64::try_from(secs).ok()
}

/// 公历日期 → 纪元天数（Howard Hinnant civil_from_days 算法，纯整数运算）
fn days_from_civil(year: i64, month: i64, day: i64) -> i64 {
  let year = if month <= 2 { year - 1 } else { year };
  let era = if year >= 0 { year } else { year - 399 } / 400;
  let year_of_era = year - era * 400;
  let month_shift = if month > 2 { month - 3 } else { month + 9 };
  let day_of_year = (153 * month_shift + 2) / 5 + day - 1;
  let day_of_era = year_of_era * 365 + year_of_era / 4 - year_of_era / 100 + day_of_year;
  era * 146_097 + day_of_era - 719_468
}

#[cfg(test)]
mod tests {
  use super::*;

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
      time::OffsetDateTime::from_unix_timestamp(1_000_000_000).expect("2001-09-09"),
      time::OffsetDateTime::from_unix_timestamp(1_100_000_000).expect("2004-11-24"),
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
    params.not_before =
      time::OffsetDateTime::from_unix_timestamp(4_000_000_000).expect("2096-10-13");
    params.not_after =
      time::OffsetDateTime::from_unix_timestamp(4_100_000_000).expect("2099-11-25");
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
}
