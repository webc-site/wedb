#![recursion_limit = "256"]
//! ClusterPreferredEndpointType 单元与集成测试
//!
//! 验证点：
//! 1. try_parse 大小写不敏感解析与数字 0/1/2 映射
//! 2. 非法值（非法字符串、越界数字）返回 None / Err
//! 3. FromStr 行为与明确错误信息
//! 4. Display 与 TOML 往返

use std::str::FromStr;

use toml_spanner::Arena;
use wedb::server::cluster::ClusterPreferredEndpointType;

#[derive(Debug, PartialEq, toml_spanner::Toml)]
#[toml(FromToml, ToToml)]
struct TestConfig {
  endpoint_type: ClusterPreferredEndpointType,
}

#[test]
fn try_parse_case_insensitive_and_numeric() {
  // 大小写混形解析
  for (text, want) in [
    ("ip", ClusterPreferredEndpointType::Ip),
    ("IP", ClusterPreferredEndpointType::Ip),
    ("Ip", ClusterPreferredEndpointType::Ip),
    ("hostname", ClusterPreferredEndpointType::Hostname),
    ("Hostname", ClusterPreferredEndpointType::Hostname),
    ("HOSTNAME", ClusterPreferredEndpointType::Hostname),
    ("HostName", ClusterPreferredEndpointType::Hostname),
    ("unknown", ClusterPreferredEndpointType::Unknown),
    ("Unknown", ClusterPreferredEndpointType::Unknown),
    ("UNKNOWN", ClusterPreferredEndpointType::Unknown),
  ] {
    assert_eq!(
      ClusterPreferredEndpointType::try_parse(text),
      Some(want),
      "解析 {text} 失败"
    );
  }

  // 数字 0/1/2 映射至对应变体（对位 C# Enum.Parse + IsDefinedEx）
  assert_eq!(
    ClusterPreferredEndpointType::try_parse("0"),
    Some(ClusterPreferredEndpointType::Ip)
  );
  assert_eq!(
    ClusterPreferredEndpointType::try_parse("1"),
    Some(ClusterPreferredEndpointType::Hostname)
  );
  assert_eq!(
    ClusterPreferredEndpointType::try_parse("2"),
    Some(ClusterPreferredEndpointType::Unknown)
  );

  // 非法值与越界数字返回 None
  for invalid in ["3", "255", "-1", "dnswithcare", "unknown2", ""] {
    assert_eq!(
      ClusterPreferredEndpointType::try_parse(invalid),
      None,
      "{invalid} 应解析失败"
    );
  }
}

#[test]
fn from_str_valid_and_error_message() {
  // 正常解析
  assert_eq!(
    ClusterPreferredEndpointType::from_str("Hostname").unwrap(),
    ClusterPreferredEndpointType::Hostname
  );
  assert_eq!(
    ClusterPreferredEndpointType::from_str("1").unwrap(),
    ClusterPreferredEndpointType::Hostname
  );

  // 非法值返回明确错误
  let err = ClusterPreferredEndpointType::from_str("invalid_type").unwrap_err();
  assert!(
    err.contains("取值须为 ip/hostname/unknown 之一"),
    "错误信息不匹配: {err}"
  );
  assert!(
    err.contains("invalid_type"),
    "错误信息应包含输入文本: {err}"
  );
}

#[test]
fn display_and_toml_roundtrip() {
  for variant in [
    ClusterPreferredEndpointType::Ip,
    ClusterPreferredEndpointType::Hostname,
    ClusterPreferredEndpointType::Unknown,
  ] {
    // Display 恒输出小写
    assert_eq!(
      variant.to_string(),
      variant.to_string().to_ascii_lowercase()
    );

    // TOML 往返
    let cfg = TestConfig {
      endpoint_type: variant,
    };
    let text = toml_spanner::to_string(&cfg).expect("serialize toml");
    let arena = Arena::new();
    let mut doc = toml_spanner::parse(&text, &arena).expect("parse toml");
    let back: TestConfig = doc.to().expect("deserialize toml");
    assert_eq!(cfg, back);
  }

  // TOML 能够解析大小写混形及数字字符串
  let arena = Arena::new();
  let mut doc = toml_spanner::parse("endpoint_type = 'Hostname'", &arena).expect("parse");
  let cfg: TestConfig = doc.to().expect("deserialize");
  assert_eq!(cfg.endpoint_type, ClusterPreferredEndpointType::Hostname);

  let mut doc = toml_spanner::parse("endpoint_type = '1'", &arena).expect("parse");
  let cfg: TestConfig = doc.to().expect("deserialize");
  assert_eq!(cfg.endpoint_type, ClusterPreferredEndpointType::Hostname);

  // TOML 非法值报错
  let mut doc = toml_spanner::parse("endpoint_type = 'invalid'", &arena).expect("parse");
  assert!(doc.to::<TestConfig>().is_err());
}
