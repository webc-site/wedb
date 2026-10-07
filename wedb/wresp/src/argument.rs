//! 命令参数描述（对标 libs/server/Resp/RespCommandArgument.cs）
//!
//! C# 以抽象基类 + 三个密封实现（Key / Basic / Container）+ JSON
//! TypeDiscriminator 多态；Rust 侧以枚举承接，RESP 序列化面一致。
//!
//! 在 garnet 中的相对路径: libs/server/Resp/RespCommandArgument.cs(对标 C# RespCommand 参数面)

use crate::{
  catalog::parse_member_names,
  resp_memory_writer::{RespBuffer, RespProtocol, RespWriter},
};

/// libs/server/Resp/RespCommandArgument.cs:RespCommandArgumentType
///
/// 命令参数类型（C# RespCommandArgumentType）
#[derive(
  Debug, Clone, PartialEq, Default, strum::Display, strum::EnumString, strum::IntoStaticStr,
)]
#[strum(ascii_case_insensitive)]
pub enum RespCommandArgumentType {
  /// 无
  #[default]
  #[strum(to_string = "None", serialize = "None")]
  None,
  /// 字符串
  #[strum(to_string = "string", serialize = "String")]
  String,
  /// 整数
  #[strum(to_string = "integer", serialize = "Integer")]
  Integer,
  /// 双精度
  #[strum(to_string = "double", serialize = "Double")]
  Double,
  /// 键名
  #[strum(to_string = "key", serialize = "Key")]
  Key,
  /// 通配模式
  #[strum(to_string = "pattern", serialize = "Pattern")]
  Pattern,
  /// Unix 时间戳
  #[strum(to_string = "unix-time", serialize = "UnixTime")]
  UnixTime,
  /// 保留关键字
  #[strum(to_string = "pure-token", serialize = "PureToken")]
  PureToken,
  /// 多选一容器
  #[strum(to_string = "oneof", serialize = "OneOf")]
  OneOf,
  /// 分组容器
  #[strum(to_string = "block", serialize = "Block")]
  Block,
}

impl RespCommandArgumentType {
  /// wire 描述（C# Description 特性；C# EnumUtils.GetEnumDescriptions）
  #[inline]
  pub const fn description(&self) -> &'static str {
    match self {
      Self::None => "None",
      Self::String => "string",
      Self::Integer => "integer",
      Self::Double => "double",
      Self::Key => "key",
      Self::Pattern => "pattern",
      Self::UnixTime => "unix-time",
      Self::PureToken => "pure-token",
      Self::OneOf => "oneof",
      Self::Block => "block",
    }
  }

  /// 按成员名解析（C# Enum.Parse(ignoreCase)；JSON 侧存成员名）
  #[inline]
  pub fn from_member_name(name: &str) -> Option<Self> {
    name.parse().ok()
  }
}

/// libs/server/Resp/RespCommandArgument.cs:RespCommandArgumentFlags
///
/// 参数标记（C# RespCommandArgumentFlags）
#[derive(Debug, Clone, PartialEq, Default)]
pub struct RespCommandArgumentFlags(u8);

impl RespCommandArgumentFlags {
  /// 无
  pub const NONE: Self = Self(0);
  /// optional
  pub const OPTIONAL: Self = Self(1);
  /// multiple
  pub const MULTIPLE: Self = Self(1 << 1);
  /// multiple-token
  pub const MULTIPLE_TOKEN: Self = Self(1 << 2);

  /// 是否无标记
  #[inline]
  pub const fn is_none(&self) -> bool {
    self.0 == 0
  }

  /// 置位标记
  #[inline]
  pub const fn union(self, other: Self) -> Self {
    Self(self.0 | other.0)
  }

  /// 置位数量
  #[inline]
  #[must_use]
  pub const fn count(&self) -> usize {
    self.0.count_ones() as usize
  }

  /// wire 描述迭代器（C# EnumUtils.GetEnumDescriptions），零堆分配
  #[inline]
  pub fn iter_descriptions(&self) -> impl Iterator<Item = &'static str> {
    let val = self.0;
    [
      (val & Self::OPTIONAL.0 != 0, "optional"),
      (val & Self::MULTIPLE.0 != 0, "multiple"),
      (val & Self::MULTIPLE_TOKEN.0 != 0, "multiple-token"),
    ]
    .into_iter()
    .filter_map(|(has, name)| if has { Some(name) } else { None })
  }

  /// wire 描述（C# EnumUtils.GetEnumDescriptions）
  pub fn descriptions(&self) -> Vec<&'static str> {
    self.iter_descriptions().collect()
  }

  /// 按成员名解析（C# Enum.Parse(ignoreCase)；JSON 侧存成员名）
  pub fn from_member_name(name: &str) -> Option<Self> {
    parse_member_names(name, |part| {
      let trimmed = part.trim();
      if trimmed.eq_ignore_ascii_case("NONE") {
        Some(Self::NONE.0 as u32)
      } else if trimmed.eq_ignore_ascii_case("OPTIONAL") {
        Some(Self::OPTIONAL.0 as u32)
      } else if trimmed.eq_ignore_ascii_case("MULTIPLE") {
        Some(Self::MULTIPLE.0 as u32)
      } else if trimmed.eq_ignore_ascii_case("MULTIPLE_TOKEN")
        || trimmed.eq_ignore_ascii_case("MULTIPLETOKEN")
      {
        Some(Self::MULTIPLE_TOKEN.0 as u32)
      } else {
        None
      }
    })
    .map(|bits| Self(bits as u8))
  }
}

/// libs/server/Resp/RespCommandArgument.cs:RespCommandArgumentBase
///
/// 参数公共字段（C# RespCommandArgumentBase）
#[derive(Default, Clone)]
pub struct ArgumentBase {
  /// 参数名（C# Name）
  pub name: String,
  /// 展示串（C# DisplayText）
  pub display_text: Option<String>,
  /// 参数类型（C# Type）
  pub argument_type: RespCommandArgumentType,
  /// 前置常量令牌（C# Token）
  pub token: Option<String>,
  /// 简述（C# Summary）
  pub summary: Option<String>,
  /// 参数标记（C# ArgumentFlags）
  pub argument_flags: RespCommandArgumentFlags,
}

/// libs/server/Resp/RespCommandArgument.cs:RespCommandArgument
///
/// 命令参数（C# RespCommandArgumentBase 三实现的多态承接）
#[derive(Default, Clone)]
pub enum RespCommandArgument {
  /// 键参数（C# RespCommandKeyArgument：额外带 key_spec_index）
  Key {
    /// 公共字段
    base: ArgumentBase,
    /// 参数值描述串（C# Value）
    value: Option<String>,
    /// 对应键规格下标（C# KeySpecIndex）
    key_spec_index: i32,
  },
  /// 基础参数（C# RespCommandBasicArgument：额外带 value）
  Basic {
    /// 公共字段
    base: ArgumentBase,
    /// 参数值描述串（C# Value）
    value: Option<String>,
  },
  /// 容器参数（C# RespCommandContainerArgument：嵌套参数）
  Container {
    /// 公共字段
    base: ArgumentBase,
    /// 嵌套参数（C# Arguments；None 对应 JSON null）
    arguments: Option<Vec<RespCommandArgument>>,
  },
  /// JSON 反序列化空壳（C# 无参构造）
  #[default]
  Empty,
}

impl RespCommandArgument {
  /// C# RespCommandArgumentConverter.CanConvert：给定判别名是否可转换
  pub fn can_convert(type_discriminator: &str) -> bool {
    matches!(
      type_discriminator,
      "RespCommandKeyArgument" | "RespCommandBasicArgument" | "RespCommandContainerArgument"
    )
  }

  /// 序列化为 RESP 格式
  ///
  /// libs/server/Resp/RespCommandArgument.cs:ToRespFormat
  pub fn to_resp_format<B: RespBuffer, P: RespProtocol>(&self, writer: &mut RespWriter<B, P>) {
    match self {
      Self::Key {
        base,
        key_spec_index,
        ..
      } => {
        // 键参数恒带 increment 位，写入 key_spec_index（C# 不输出 value 键）
        to_byte_resp_format(base, true, writer);
        writer.write_bulk_string(b"key_spec_index");
        writer.write_int32(*key_spec_index);
      }
      Self::Basic { base, value } => {
        to_byte_resp_format(base, value.is_some(), writer);
        if let Some(value) = value {
          writer.write_bulk_string(b"value");
          writer.write_ascii_bulk_string(value);
        }
      }
      Self::Container { base, arguments } => {
        if let Some(arguments) = arguments {
          to_byte_resp_format(base, true, writer);
          writer.write_bulk_string(b"arguments");
          writer.write_array_length(arguments.len());
          for argument in arguments {
            argument.to_resp_format(writer);
          }
        } else {
          to_byte_resp_format(base, false, writer);
        }
      }
      Self::Empty => {}
    }
  }
}

/// 公共字段序列化（C# ToByteRespFormat；increment 为是否预留 value 位）
///
/// libs/server/Resp/RespCommandArgument.cs:ToByteRespFormat
fn to_byte_resp_format<B: RespBuffer, P: RespProtocol>(
  base: &ArgumentBase,
  increment: bool,
  writer: &mut RespWriter<B, P>,
) {
  let arg_count = 2
    + usize::from(base.display_text.is_some())
    + usize::from(base.token.is_some())
    + usize::from(base.summary.is_some())
    + usize::from(!base.argument_flags.is_none())
    + usize::from(increment);

  writer.write_map_length(arg_count);

  writer.write_bulk_string(b"name");
  writer.write_ascii_bulk_string(&base.name);

  writer.write_bulk_string(b"type");
  writer.write_ascii_bulk_string(base.argument_type.description());

  if let Some(display_text) = &base.display_text {
    writer.write_bulk_string(b"display_text");
    writer.write_ascii_bulk_string(display_text);
  }

  if let Some(token) = &base.token {
    writer.write_bulk_string(b"token");
    writer.write_ascii_bulk_string(token);
  }

  if let Some(summary) = &base.summary {
    writer.write_bulk_string(b"summary");
    writer.write_ascii_bulk_string(summary);
  }

  if !base.argument_flags.is_none() {
    writer.write_bulk_string(b"flags");
    writer.write_set_length(base.argument_flags.count());
    for resp_arg_flag in base.argument_flags.iter_descriptions() {
      writer.write_simple_string(resp_arg_flag);
    }
  }
}
