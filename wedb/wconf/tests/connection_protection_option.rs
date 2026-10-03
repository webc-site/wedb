use toml_spanner::Arena;
use wconf::ConnectionProtectionOption;

#[derive(Debug, PartialEq, toml_spanner::Toml)]
#[toml(FromToml, ToToml)]
struct Case {
  v: ConnectionProtectionOption,
}

/// 三档成员名忽略大小写解析 + 判别值数字解析（Enum.Parse 语义；
/// all 为 C# redis.conf 专属别名，本仓无该层不收）
#[test]
fn try_parse_accepts_members_and_raw_values() {
  assert_eq!(
    Some(ConnectionProtectionOption::No),
    ConnectionProtectionOption::try_parse("no")
  );
  assert_eq!(
    Some(ConnectionProtectionOption::No),
    ConnectionProtectionOption::try_parse("NO")
  );
  assert_eq!(
    Some(ConnectionProtectionOption::Local),
    ConnectionProtectionOption::try_parse("Local")
  );
  assert_eq!(
    Some(ConnectionProtectionOption::Yes),
    ConnectionProtectionOption::try_parse("yes")
  );
  assert_eq!(
    Some(ConnectionProtectionOption::Local),
    ConnectionProtectionOption::try_parse("1")
  );
  assert_eq!(None, ConnectionProtectionOption::try_parse("all"));
  assert_eq!(None, ConnectionProtectionOption::try_parse("3"));
  assert_eq!(None, ConnectionProtectionOption::try_parse(""));
}

/// 展示名小写 + TOML 配置面往返（导出面 ToLowerInvariant 同源）
#[test]
fn display_and_toml_roundtrip() {
  for m in [
    ConnectionProtectionOption::No,
    ConnectionProtectionOption::Local,
    ConnectionProtectionOption::Yes,
  ] {
    assert_eq!(m.to_string(), m.to_string().to_ascii_lowercase());
    let case = Case { v: m };
    let text = toml_spanner::to_string(&case).expect("serialize");
    let arena = Arena::new();
    let mut doc = toml_spanner::parse(&text, &arena).expect("parse");
    let back: Case = doc.to().expect("deserialize");
    assert_eq!(m, back.v);
  }
  let arena = Arena::new();
  let mut doc = toml_spanner::parse("v = 'all'", &arena).expect("parse");
  assert!(doc.to::<Case>().is_err());
}
