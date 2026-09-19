use std::{fmt, str::FromStr};

use serde::{Deserialize, Deserializer, Serialize, Serializer, de::Error};

/// 连接保护选项（对标 libs/server/Auth/Settings/ConnectionProtectionOption.cs:ConnectionProtectionOption）。
///
/// 命令级连接保护门（如 DEBUG）的三档值域，判别值与 C# 声明一致。
/// C# redis.conf 专属别名 all（RedisTypes.cs:19 All=2 与 Yes 同值）不设：
/// 本仓配置文件唯一格式为 nested_text，无 redis.conf 兼容层。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
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
  const fn from_raw(raw: u8) -> Option<Self> {
    match raw {
      0 => Some(Self::No),
      1 => Some(Self::Local),
      2 => Some(Self::Yes),
      _ => None,
    }
  }

  /// 按成员名或十进制数值解析（忽略大小写，仅接受已声明成员；对标
  /// TypeConverters.cs:225 `Enum.Parse<RedisConnectionProtectionOption>(strVal, true)`
  /// 与 CommandLineParser 枚举解析的共通语义）
  pub fn try_parse(value: &str) -> Option<Self> {
    if let Ok(raw) = value.parse::<u8>() {
      return Self::from_raw(raw);
    }
    if value.eq_ignore_ascii_case("No") {
      Some(Self::No)
    } else if value.eq_ignore_ascii_case("Local") {
      Some(Self::Local)
    } else if value.eq_ignore_ascii_case("Yes") {
      Some(Self::Yes)
    } else {
      None
    }
  }
}

/// 配置展示名（小写，对标 TypeConverters.cs:198
/// RedisConnectionProtectionOptionConverter.ConvertTo 的 ToLowerInvariant）
impl fmt::Display for ConnectionProtectionOption {
  fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    f.write_str(match self {
      Self::No => "no",
      Self::Local => "local",
      Self::Yes => "yes",
    })
  }
}

/// 命令行值解析入口（clap value_parser；错误文案即 CLI 提示面）
impl FromStr for ConnectionProtectionOption {
  type Err = String;

  fn from_str(s: &str) -> Result<Self, Self::Err> {
    Self::try_parse(s).ok_or_else(|| format!("取值须为 no/local/yes 之一，当前为 {s}"))
  }
}

/// nested_text 配置面序列化：小写名（与 Display 同源，对标 C# 导出面
/// ToLowerInvariant）
impl Serialize for ConnectionProtectionOption {
  fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
    serializer.collect_str(self)
  }
}

/// nested_text 配置面反序列化：走 try_parse 忽略大小写（对标
/// Enum.Parse ignoreCase），非法值拒启。nested_text 反序列化器不供借用
/// &str，以 String 承接（配置装载为一次性启动路径）
impl<'de> Deserialize<'de> for ConnectionProtectionOption {
  fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
    let s = String::deserialize(deserializer)?;
    ConnectionProtectionOption::try_parse(&s)
      .ok_or_else(|| Error::custom(format!("取值须为 no/local/yes 之一，当前为 {s}")))
  }
}

#[cfg(test)]
mod tests {
  use serde::{Deserialize, Serialize};

  use super::*;

  #[derive(Serialize, Deserialize)]
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

  /// 展示名小写 + nested_text 配置面往返（导出面 ToLowerInvariant 同源）
  #[test]
  fn display_and_nested_text_roundtrip() {
    for m in [
      ConnectionProtectionOption::No,
      ConnectionProtectionOption::Local,
      ConnectionProtectionOption::Yes,
    ] {
      assert_eq!(m.to_string(), m.to_string().to_ascii_lowercase());
      let text = nested_text::to_string(&Case { v: m }).expect("serialize");
      let back: Case = nested_text::from_str(&text).expect("deserialize");
      assert_eq!(m, back.v);
    }
    assert!(nested_text::from_str::<Case>("v: all").is_err());
  }
}
