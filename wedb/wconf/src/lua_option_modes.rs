//! Lua 脚本内存/日志模式枚举（对标 libs/server/Lua/LuaOptions.cs 的
//! LuaMemoryManagementMode / LuaLoggingMode 两枚宿主配置侧枚举）。
//!
//! 自研依据: 配置端值域镜像（C# host 层 Options 以 server 层枚举作 CLI 值域，
//! rust 分层下 wconf 不得反向依赖 wlua，故在本层落同形镜像枚举，attach 单点投影
//! 进 wlua 消费端枚举）。判别值与 C# 声明一致。
use std::str::FromStr;

use num_enum::{IntoPrimitive, TryFromPrimitive};
use strum::Display;

/// 内存管理模式（对标 LuaOptions.cs:LuaMemoryManagementMode）。
#[derive(
  Debug, Clone, Copy, PartialEq, Eq, Hash, Default, Display, TryFromPrimitive, IntoPrimitive,
)]
#[strum(serialize_all = "lowercase")]
#[repr(u8)]
pub enum LuaMemoryManagementMode {
  /// 默认分配器，宿主不感知分配（C# 枚举零值，缺省档，无法施加脚本限额）
  #[default]
  Native = 0,
  /// 宿主感知的原生分配（限额不精确）
  Tracked = 1,
  /// 预分配自由表分配器
  Managed = 2,
}

impl LuaMemoryManagementMode {
  /// 判别值反查已声明成员（对标 Enum.IsDefined + 强转语义）
  #[inline]
  pub fn from_raw(raw: u8) -> Option<Self> {
    Self::try_from(raw).ok()
  }

  /// 按成员名或十进制数值解析（忽略大小写；对标 CommandLineParser 的
  /// Enum.TryParse(_, ignoreCase) 与 TypeConverter 的数字反查共通语义）
  pub fn try_parse(value: &str) -> Option<Self> {
    if let Ok(raw) = value.parse::<u8>() {
      return Self::from_raw(raw);
    }
    if value.eq_ignore_ascii_case("native") {
      Some(Self::Native)
    } else if value.eq_ignore_ascii_case("tracked") {
      Some(Self::Tracked)
    } else if value.eq_ignore_ascii_case("managed") {
      Some(Self::Managed)
    } else {
      None
    }
  }
}

/// 命令行值解析入口（clap value_parser；错误文案即 CLI 提示面）
impl FromStr for LuaMemoryManagementMode {
  type Err = String;

  fn from_str(s: &str) -> Result<Self, Self::Err> {
    Self::try_parse(s).ok_or_else(|| format!("取值须为 native/tracked/managed 之一，当前为 {s}"))
  }
}

impl<'de> toml_spanner::FromToml<'de> for LuaMemoryManagementMode {
  fn from_toml(
    ctx: &mut toml_spanner::Context<'de>,
    item: &toml_spanner::Item<'de>,
  ) -> Result<Self, toml_spanner::Failed> {
    let Some(s) = item.as_str() else {
      return Err(ctx.report_expected_but_found(&"a string", item));
    };
    Self::try_parse(s).ok_or_else(|| {
      ctx.report_custom_error(
        format!("取值须为 native/tracked/managed 之一，当前为 {s}"),
        item,
      )
    })
  }
}

impl toml_spanner::ToToml for LuaMemoryManagementMode {
  fn to_toml<'a>(
    &'a self,
    arena: &'a toml_spanner::Arena,
  ) -> Result<toml_spanner::Item<'a>, toml_spanner::ToTomlError> {
    Ok(toml_spanner::Item::string(
      arena.alloc_str(&self.to_string()),
    ))
  }
}

/// redis.log 行为模式（对标 LuaOptions.cs:LuaLoggingMode）。
#[derive(
  Debug, Clone, Copy, PartialEq, Eq, Hash, Default, Display, TryFromPrimitive, IntoPrimitive,
)]
#[strum(serialize_all = "lowercase")]
#[repr(u8)]
pub enum LuaLoggingMode {
  /// redis.log 透传记录（C# 生效默认，defaults.conf:509 在册）
  #[default]
  Enable = 0,
  /// redis.log 成功但无操作
  Silent = 1,
  /// redis.log 报错（日志被禁用）
  Disable = 2,
}

impl LuaLoggingMode {
  /// 判别值反查已声明成员（对标 Enum.IsDefined + 强转语义）
  #[inline]
  pub fn from_raw(raw: u8) -> Option<Self> {
    Self::try_from(raw).ok()
  }

  /// 按成员名或十进制数值解析（忽略大小写）
  pub fn try_parse(value: &str) -> Option<Self> {
    if let Ok(raw) = value.parse::<u8>() {
      return Self::from_raw(raw);
    }
    if value.eq_ignore_ascii_case("enable") {
      Some(Self::Enable)
    } else if value.eq_ignore_ascii_case("silent") {
      Some(Self::Silent)
    } else if value.eq_ignore_ascii_case("disable") {
      Some(Self::Disable)
    } else {
      None
    }
  }
}

/// 命令行值解析入口（clap value_parser；错误文案即 CLI 提示面）
impl FromStr for LuaLoggingMode {
  type Err = String;

  fn from_str(s: &str) -> Result<Self, Self::Err> {
    Self::try_parse(s).ok_or_else(|| format!("取值须为 enable/silent/disable 之一，当前为 {s}"))
  }
}

impl<'de> toml_spanner::FromToml<'de> for LuaLoggingMode {
  fn from_toml(
    ctx: &mut toml_spanner::Context<'de>,
    item: &toml_spanner::Item<'de>,
  ) -> Result<Self, toml_spanner::Failed> {
    let Some(s) = item.as_str() else {
      return Err(ctx.report_expected_but_found(&"a string", item));
    };
    Self::try_parse(s).ok_or_else(|| {
      ctx.report_custom_error(
        format!("取值须为 enable/silent/disable 之一，当前为 {s}"),
        item,
      )
    })
  }
}

impl toml_spanner::ToToml for LuaLoggingMode {
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
  use super::{LuaLoggingMode, LuaMemoryManagementMode};

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
      let arena = toml_spanner::Arena::new();
      let mut doc = toml_spanner::parse(&text, &arena).expect("parse");
      let back: Case = doc.to().expect("deserialize");
      assert_eq!(case, back);
    }
    let arena = toml_spanner::Arena::new();
    let mut doc = toml_spanner::parse("mem = 'bogus'\nlog = 'enable'", &arena).expect("parse");
    assert!(doc.to::<Case>().is_err());
  }
}
