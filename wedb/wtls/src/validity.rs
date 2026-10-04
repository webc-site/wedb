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

/// DER TLV tag：SEQUENCE（parse_cert_validity 各层结构壳统一判别）
const TAG_SEQUENCE: u8 = 0x30;
/// DER TLV tag：INTEGER（serialNumber）
const TAG_INTEGER: u8 = 0x02;
/// DER TLV tag：[0] EXPLICIT 上下文构造（version 可选壳）
const TAG_CTX_0: u8 = 0xa0;

/// 证书有效期窗口（Unix 秒，闭区间 `[not_before, not_after]`）
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Validity {
  pub not_before: u64,
  pub not_after: u64,
}

impl Validity {
  /// 从 DER 证书抽取有效期窗口（结构错位/时间非法即 Err）
  pub fn from_der(cert: &CertificateDer<'_>) -> Result<Self, Error> {
    parse_cert_validity(cert.as_ref())
      .ok_or_else(|| Error::InvalidCertificate(CertificateError::BadEncoding))
  }

  /// 校验时点落窗（`now` 窗口外即过期/未生效，映射 rustls 对应变体，
  /// 带上下文数值供握手报错可观测）
  pub fn check(&self, now: UnixTime) -> Result<(), Error> {
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
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Tlv<'a> {
  pub tag: u8,
  pub content: &'a [u8],
}

/// 读一个 TLV：短形与长形长度均支持，剩余不足即 None（fail-closed）
pub fn next_tlv<'a>(input: &mut &'a [u8]) -> Option<Tlv<'a>> {
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
pub fn parse_cert_validity(der: &[u8]) -> Option<Validity> {
  let mut top = der;
  // Certificate ::= SEQUENCE { tbsCertificate, signatureAlgorithm, signatureValue }
  let cert = next_tlv(&mut top)?;
  if cert.tag != TAG_SEQUENCE {
    return None;
  }
  let mut tbs = cert.content;
  // tbsCertificate ::= SEQUENCE { version [0] EXPLICIT(可选), serialNumber,
  //   signature, issuer, validity, ... }
  let tbs_seq = next_tlv(&mut tbs)?;
  if tbs_seq.tag != TAG_SEQUENCE {
    return None;
  }
  let mut fields = tbs_seq.content;
  // version [0] EXPLICIT（v1 证书缺席该字段，两形态都接受）
  if let Some(head) = fields.first()
    && *head == TAG_CTX_0
  {
    next_tlv(&mut fields)?;
  }
  // serialNumber INTEGER
  let serial = next_tlv(&mut fields)?;
  if serial.tag != TAG_INTEGER {
    return None;
  }
  // signature AlgorithmIdentifier
  let sig = next_tlv(&mut fields)?;
  if sig.tag != TAG_SEQUENCE {
    return None;
  }
  // issuer Name
  let issuer = next_tlv(&mut fields)?;
  if issuer.tag != TAG_SEQUENCE {
    return None;
  }
  // validity SEQUENCE { notBefore Time, notAfter Time }
  let validity = next_tlv(&mut fields)?;
  if validity.tag != TAG_SEQUENCE {
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
pub fn asn1_time_unix(tlv: Tlv<'_>) -> Option<u64> {
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
pub const fn days_from_civil(year: i64, month: i64, day: i64) -> i64 {
  let year = if month <= 2 { year - 1 } else { year };
  let era = if year >= 0 { year } else { year - 399 } / 400;
  let year_of_era = year - era * 400;
  let month_shift = if month > 2 { month - 3 } else { month + 9 };
  let day_of_year = (153 * month_shift + 2) / 5 + day - 1;
  let day_of_era = year_of_era * 365 + year_of_era / 4 - year_of_era / 100 + day_of_year;
  era * 146_097 + day_of_era - 719_468
}
