#![recursion_limit = "512"] // 泛型设备实例化下 async 状态机嵌套深（R27 CI 实证）
use toml_spanner::Arena;
use wconf::{LuaLoggingMode, LuaMemoryManagementMode};

#[derive(Debug, PartialEq, toml_spanner::Toml)]
#[toml(FromToml, ToToml)]
struct Case {
  mem: LuaMemoryManagementMode,
  log: LuaLoggingMode,
}

/// 成员名忽略大小写 + 判别值数字解析（Enum.Parse 语义）；非法名/越界数值拒
#[test]
fn try_parse_accepts_members_and_raw_values() {
  for (text, want) in [
    ("native", LuaMemoryManagementMode::Native),
    ("Tracked", LuaMemoryManagementMode::Tracked),
    ("MANAGED", LuaMemoryManagementMode::Managed),
    ("1", LuaMemoryManagementMode::Tracked),
    ("2", LuaMemoryManagementMode::Managed),
  ] {
    assert_eq!(
      Some(want),
      LuaMemoryManagementMode::try_parse(text),
      "{text}"
    );
  }
  assert_eq!(None, LuaMemoryManagementMode::try_parse("all"));
  assert_eq!(None, LuaMemoryManagementMode::try_parse("3"));
  for (text, want) in [
    ("Enable", LuaLoggingMode::Enable),
    ("silent", LuaLoggingMode::Silent),
    ("DISABLE", LuaLoggingMode::Disable),
    ("2", LuaLoggingMode::Disable),
  ] {
    assert_eq!(Some(want), LuaLoggingMode::try_parse(text), "{text}");
  }
  assert_eq!(None, LuaLoggingMode::try_parse("verbose"));
}

/// 默认档对齐 C# 生效默认（Native / Enable）+ CLI 名小写 + TOML 配置面往返
#[test]
fn defaults_display_and_toml_roundtrip() {
  assert_eq!(
    LuaMemoryManagementMode::default(),
    LuaMemoryManagementMode::Native
  );
  assert_eq!(LuaLoggingMode::default(), LuaLoggingMode::Enable);
  for (m, l) in [
    (LuaMemoryManagementMode::Native, LuaLoggingMode::Enable),
    (LuaMemoryManagementMode::Tracked, LuaLoggingMode::Silent),
    (LuaMemoryManagementMode::Managed, LuaLoggingMode::Disable),
  ] {
    assert_eq!(m.to_string(), m.to_string().to_ascii_lowercase());
    let case = Case { mem: m, log: l };
    let text = toml_spanner::to_string(&case).expect("serialize");
    let arena = Arena::new();
    let mut doc = toml_spanner::parse(&text, &arena).expect("parse");
    let back: Case = doc.to().expect("deserialize");
    assert_eq!(case, back);
  }
  let arena = Arena::new();
  let mut doc = toml_spanner::parse("mem = 'bogus'\nlog = 'enable'", &arena).expect("parse");
  assert!(doc.to::<Case>().is_err());
}
