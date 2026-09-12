//! 命令参数描述（对标 libs/server/Resp/RespCommandArgument.cs）
//!
//! C# 以抽象基类 + 三个密封实现（Key / Basic / Container）+ JSON
//! TypeDiscriminator 多态；Rust 侧以枚举承接，RESP 序列化面一致。

use super::{i_resp_serializable::IRespSerializable, resp_memory_writer::RespMemoryWriter};

/// 命令参数类型（C# RespCommandArgumentType）
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum RespCommandArgumentType {
  /// 无
  #[default]
  None,
  /// 字符串
  String,
  /// 整数
  Integer,
  /// 双精度
  Double,
  /// 键名
  Key,
  /// 通配模式
  Pattern,
  /// Unix 时间戳
  UnixTime,
  /// 保留关键字
  PureToken,
  /// 多选一容器
  OneOf,
  /// 分组容器
  Block,
}

impl RespCommandArgumentType {
  /// wire 描述（C# Description 特性；C# EnumUtils.GetEnumDescriptions）
  pub fn description(&self) -> &'static str {
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
  pub fn from_member_name(name: &str) -> Option<Self> {
    Some(match name.to_ascii_uppercase().as_str() {
      "NONE" => Self::None,
      "STRING" => Self::String,
      "INTEGER" => Self::Integer,
      "DOUBLE" => Self::Double,
      "KEY" => Self::Key,
      "PATTERN" => Self::Pattern,
      "UNIXTIME" => Self::UnixTime,
      "PURETOKEN" => Self::PureToken,
      "ONEOF" => Self::OneOf,
      "BLOCK" => Self::Block,
      _ => return None,
    })
  }
}

/// 参数标记（C# RespCommandArgumentFlags）
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
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
  pub fn is_none(&self) -> bool {
    self.0 == 0
  }

  /// 置位标记
  #[inline]
  pub fn union(self, other: Self) -> Self {
    Self(self.0 | other.0)
  }

  /// wire 描述（C# EnumUtils.GetEnumDescriptions）
  pub fn descriptions(&self) -> Vec<&'static str> {
    [
      (Self::OPTIONAL.0, "optional"),
      (Self::MULTIPLE.0, "multiple"),
      (Self::MULTIPLE_TOKEN.0, "multiple-token"),
    ]
    .iter()
    .filter(|(bit, _)| self.0 & bit != 0)
    .map(|(_, name)| *name)
    .collect()
  }

  /// 按成员名解析（C# Enum.Parse(ignoreCase)；JSON 侧存成员名）
  pub fn from_member_name(name: &str) -> Option<Self> {
    let mut out = Self::NONE;
    for part in name.split(',') {
      let bit = match part.trim().to_ascii_uppercase().as_str() {
        "NONE" => Self::NONE,
        "OPTIONAL" => Self::OPTIONAL,
        "MULTIPLE" => Self::MULTIPLE,
        "MULTIPLE_TOKEN" | "MULTIPLETOKEN" => Self::MULTIPLE_TOKEN,
        _ => return None,
      };
      out = out.union(bit);
    }
    Some(out)
  }
}

/// 参数公共字段（C# RespCommandArgumentBase）
#[derive(Debug, Clone, Default)]
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

/// 命令参数（C# RespCommandArgumentBase 三实现的多态承接）
#[derive(Debug, Clone, Default)]
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
  pub fn to_resp_format(&self, writer: &mut RespMemoryWriter) {
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

impl IRespSerializable for RespCommandArgument {
  fn to_resp_format(&self, writer: &mut RespMemoryWriter) {
    self.to_resp_format(writer);
  }
}

/// 公共字段序列化（C# ToByteRespFormat；increment 为是否预留 value 位）
///
/// libs/server/Resp/RespCommandArgument.cs:ToByteRespFormat
fn to_byte_resp_format(base: &ArgumentBase, increment: bool, writer: &mut RespMemoryWriter) {
  let mut arg_count = 2; // name, type

  if base.display_text.is_some() {
    arg_count += 1;
  }

  if base.token.is_some() {
    arg_count += 1;
  }

  if base.summary.is_some() {
    arg_count += 1;
  }

  if !base.argument_flags.is_none() {
    arg_count += 1;
  }

  if increment {
    arg_count += 1;
  }

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
    let resp_format_arg_flags = base.argument_flags.descriptions();
    writer.write_bulk_string(b"flags");
    writer.write_set_length(resp_format_arg_flags.len());
    for resp_arg_flag in resp_format_arg_flags {
      writer.write_simple_string(resp_arg_flag);
    }
  }
}

#[cfg(test)]
mod tests {
  use super::{
    ArgumentBase, RespCommandArgument, RespCommandArgumentFlags, RespCommandArgumentType,
  };
  use crate::resp::resp_memory_writer::RespMemoryWriter;

  #[test]
  fn type_descriptions_and_parse() {
    assert_eq!(RespCommandArgumentType::Key.description(), "key");
    assert_eq!(RespCommandArgumentType::UnixTime.description(), "unix-time");
    assert_eq!(
      RespCommandArgumentType::from_member_name("oneof"),
      Some(RespCommandArgumentType::OneOf)
    );
    assert_eq!(RespCommandArgumentType::from_member_name("bad"), None);
  }

  #[test]
  fn flags_descriptions_and_parse() {
    let flags = RespCommandArgumentFlags::OPTIONAL.union(RespCommandArgumentFlags::MULTIPLE_TOKEN);
    assert_eq!(flags.descriptions(), vec!["optional", "multiple-token"]);
    assert_eq!(
      RespCommandArgumentFlags::from_member_name("MultipleToken"),
      Some(RespCommandArgumentFlags::MULTIPLE_TOKEN)
    );
  }

  #[test]
  fn key_argument_resp_format() {
    let arg = RespCommandArgument::Key {
      base: ArgumentBase {
        name: "key".to_string(),
        display_text: None,
        argument_type: RespCommandArgumentType::Key,
        token: None,
        summary: None,
        argument_flags: RespCommandArgumentFlags::NONE,
      },
      value: Some("key".to_string()),
      key_spec_index: 0,
    };
    let mut w = RespMemoryWriter::new(true);
    arg.to_resp_format(&mut w);
    let text = String::from_utf8(w.out).unwrap();
    // C# Key 参数 RESP 面只出 name/type/key_spec_index 三键（RESP3 %3）
    assert!(text.starts_with("%3\r\n"), "{text}");
    assert!(text.contains("$14\r\nkey_spec_index\r\n:0\r\n"), "{text}");
  }
}
