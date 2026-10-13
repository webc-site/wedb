//! 集群优选端点类型（对标 libs/server/Cluster/ClusterPreferredEndpointType.cs）

use std::{fmt, str::FromStr};

use toml_spanner::Item;

/// libs/server/Cluster/ClusterPreferredEndpointType.cs:ClusterPreferredEndpointType
///
/// 集群优选端点类型（判别值对齐 C# 声明序：MOVED/ASK 重定向与 CLUSTER
/// SLOTS/SHARDS 输出的地址形态偏好；命令行取值 ip/hostname/unknown，数字仅收已声明的 0-2，
/// 对位 C# Enum.Parse+IsDefinedEx 语义，越界数字与非法词同错误面）。
#[derive(Debug, Copy, PartialEq, Default, Clone)]
#[repr(u8)]
pub enum ClusterPreferredEndpointType {
  /// IP 地址（ex -MOVED 12182 127.0.0.1:7000）
  #[default]
  Ip = 0,
  /// 主机名（ex -MOVED 12182 localhost:7000；无 hostname 时为 ?:7000）
  Hostname = 1,
  /// 客户端自身已知形式（ex -MOVED 12182 ?:7000）
  Unknown = 2,
}

impl ClusterPreferredEndpointType {
  /// 判别值反查已声明成员（对标 C# Enum.IsDefined + 强转语义，仅收已定义 0-2）
  #[inline]
  pub const fn from_raw(raw: u8) -> Option<Self> {
    match raw {
      0 => Some(Self::Ip),
      1 => Some(Self::Hostname),
      2 => Some(Self::Unknown),
      _ => None,
    }
  }

  /// 按成员名或十进制数值解析（忽略大小写，仅接受已声明成员；对标
  /// libs/server/Cluster/ClusterPreferredEndpointType.cs 与 CommandLineParser 的
  /// Enum.Parse(conversionType, value, ignoreValueCase) + IsDefinedEx 共通语义）
  pub fn try_parse(value: &str) -> Option<Self> {
    if let Ok(raw) = value.parse::<u8>() {
      return Self::from_raw(raw);
    }
    if value.eq_ignore_ascii_case("ip") {
      Some(Self::Ip)
    } else if value.eq_ignore_ascii_case("hostname") {
      Some(Self::Hostname)
    } else if value.eq_ignore_ascii_case("unknown") {
      Some(Self::Unknown)
    } else {
      None
    }
  }

  /// 对应的配置与输出字符串切片
  #[inline]
  pub const fn as_str(&self) -> &'static str {
    match self {
      Self::Ip => "ip",
      Self::Hostname => "hostname",
      Self::Unknown => "unknown",
    }
  }
}

impl fmt::Display for ClusterPreferredEndpointType {
  fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    f.write_str(self.as_str())
  }
}

/// 命令行值解析入口（clap value_parser；错误文案即 CLI 提示面）
impl FromStr for ClusterPreferredEndpointType {
  type Err = String;

  fn from_str(s: &str) -> Result<Self, Self::Err> {
    Self::try_parse(s).ok_or_else(|| format!("取值须为 ip/hostname/unknown 之一，当前为 {s}"))
  }
}

impl<'de> toml_spanner::FromToml<'de> for ClusterPreferredEndpointType {
  fn from_toml(
    ctx: &mut toml_spanner::Context<'de>,
    item: &toml_spanner::Item<'de>,
  ) -> Result<Self, toml_spanner::Failed> {
    let Some(s) = item.as_str() else {
      return Err(ctx.report_expected_but_found(&"a string", item));
    };
    Self::try_parse(s).ok_or_else(|| {
      ctx.report_custom_error(
        format!("取值须为 ip/hostname/unknown 之一，当前为 {s}"),
        item,
      )
    })
  }
}

impl toml_spanner::ToToml for ClusterPreferredEndpointType {
  fn to_toml<'a>(
    &'a self,
    arena: &'a toml_spanner::Arena,
  ) -> Result<toml_spanner::Item<'a>, toml_spanner::ToTomlError> {
    Ok(Item::string(arena.alloc_str(&self.to_string())))
  }
}
