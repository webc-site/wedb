//! 自研依据: 连接保护选项（C# 对应 ConnectionProtectionOption）
use std::str::FromStr;

use num_enum::{IntoPrimitive, TryFromPrimitive};
use strum::Display;

/// 连接保护选项（对标 libs/server/Auth/Settings/ConnectionProtectionOption.cs:ConnectionProtectionOption）。
///
/// 命令级连接保护门（如 DEBUG）的三档值域，判别值与 C# 声明一致。
/// C# redis.conf 专属别名 all（RedisTypes.cs:19 All=2 与 Yes 同值）不设：
/// 本仓配置文件唯一格式为 TOML，无 redis.conf 兼容层。
#[derive(
  Debug, Clone, Copy, PartialEq, Eq, Hash, Default, Display, TryFromPrimitive, IntoPrimitive,
)]
#[strum(serialize_all = "lowercase")]
#[repr(u8)]
pub enum ConnectionProtectionOption {
  /// 全部连接拒绝（C# 枚举零值，缺省档）
  #[default]
  No = 0,
  /// 仅本地连接放行（回环 / Unix 套接字）
  Local = 1,
  /// 全部连接放行
  Yes = 2,
}

impl ConnectionProtectionOption {
  /// 判别值反查已声明成员（对标 Enum.IsDefined + 强转语义）
  #[inline]
  pub fn from_raw(raw: u8) -> Option<Self> {
    Self::try_from(raw).ok()
  }

  /// 按成员名或十进制数值解析（忽略大小写，仅接受已声明成员；对标
  /// TypeConverters.cs:225 `Enum.Parse<RedisConnectionProtectionOption>(strVal, true)`
  /// 与 CommandLineParser 枚举解析的共通语义）
  pub fn try_parse(value: &str) -> Option<Self> {
    if let Ok(raw) = value.parse::<u8>() {
      return Self::from_raw(raw);
    }
    if value.eq_ignore_ascii_case("no") {
      Some(Self::No)
    } else if value.eq_ignore_ascii_case("local") {
      Some(Self::Local)
    } else if value.eq_ignore_ascii_case("yes") {
      Some(Self::Yes)
    } else {
      None
    }
  }
}

/// 命令行值解析入口（clap value_parser；错误文案即 CLI 提示面）
impl FromStr for ConnectionProtectionOption {
  type Err = String;

  fn from_str(s: &str) -> Result<Self, Self::Err> {
    Self::try_parse(s).ok_or_else(|| format!("取值须为 no/local/yes 之一，当前为 {s}"))
  }
}

impl<'de> toml_spanner::FromToml<'de> for ConnectionProtectionOption {
  fn from_toml(
    ctx: &mut toml_spanner::Context<'de>,
    item: &toml_spanner::Item<'de>,
  ) -> Result<Self, toml_spanner::Failed> {
    let Some(s) = item.as_str() else {
      return Err(ctx.report_expected_but_found(&"a string", item));
    };
    Self::try_parse(s).ok_or_else(|| {
      ctx.report_custom_error(format!("取值须为 no/local/yes 之一，当前为 {s}"), item)
    })
  }
}

impl toml_spanner::ToToml for ConnectionProtectionOption {
  fn to_toml<'a>(
    &'a self,
    arena: &'a toml_spanner::Arena,
  ) -> Result<toml_spanner::Item<'a>, toml_spanner::ToTomlError> {
    Ok(toml_spanner::Item::string(
      arena.alloc_str(&self.to_string()),
    ))
  }
}

#[cfg(test)]
mod tests {
  use super::*;

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
      let arena = toml_spanner::Arena::new();
      let mut doc = toml_spanner::parse(&text, &arena).expect("parse");
      let back: Case = doc.to().expect("deserialize");
      assert_eq!(m, back.v);
    }
    let arena = toml_spanner::Arena::new();
    let mut doc = toml_spanner::parse("v = 'all'", &arena).expect("parse");
    assert!(doc.to::<Case>().is_err());
  }
}
