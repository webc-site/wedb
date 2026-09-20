//! 命令键规格（对标 libs/server/Resp/RespCommandKeySpecification.cs 与 RespCommandInfoSimplifiedStructs.cs）
//!
//! C# 以类层次（BeginSearchIndex/Keyword/Unknown、FindKeysRange/KeyNum/
//! Unknown）+ JSON TypeDiscriminator 多态；Rust 侧以枚举承接，本文件只承载
//! 命令信息导出/导入面（RESP 序列化与解析）。
//!
//! 键提取不在本面：C# `ExtractKeys`/`TryGetStartIndex` 在 C# 侧亦无生产调用点
//! （活消费面只有命令信息导出），rust 生产键提取唯一口径为
//! [`wresp::catalog::SimpleRespKeySpec`] + `wnode::key_spec`（对标
//! libs/server/SessionParseStateExtensions.cs），此处不设第二套提取实现。

use core::str::FromStr;

use bitflags::bitflags;

use crate::resp_memory_writer::{RespBuffer, RespProtocol, RespWriter};

bitflags! {
  /// RESP 键规格标记位图（对标 C# KeySpecificationFlags [Flags] enum）
  #[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Hash)]
  pub struct KeySpecificationFlags: u16 {
    /// RW: 读写
    const RW = 1;
    /// RO: 只读
    const RO = 1 << 1;
    /// OW: 覆写
    const OW = 1 << 2;
    /// RM: 读后写
    const RM = 1 << 3;
    /// ACCESS: 访问
    const ACCESS = 1 << 4;
    /// UPDATE: 更新
    const UPDATE = 1 << 5;
    /// INSERT: 插入
    const INSERT = 1 << 6;
    /// DELETE: 删除
    const DELETE = 1 << 7;
    /// NOT_KEY: 非键参数
    const NOT_KEY = 1 << 8;
    /// INCOMPLETE: 不完整规格
    const INCOMPLETE = 1 << 9;
    /// VARIABLE_FLAGS: 动态标记
    const VARIABLE_FLAGS = 1 << 10;
  }
}

/// 所有置位标记及其对应的 wire 描述（编译期常量表，按声明序排列）
pub const ALL_FLAGS: [(KeySpecificationFlags, &str); 11] = [
  (KeySpecificationFlags::RW, "RW"),
  (KeySpecificationFlags::RO, "RO"),
  (KeySpecificationFlags::OW, "OW"),
  (KeySpecificationFlags::RM, "RM"),
  (KeySpecificationFlags::ACCESS, "access"),
  (KeySpecificationFlags::UPDATE, "update"),
  (KeySpecificationFlags::INSERT, "insert"),
  (KeySpecificationFlags::DELETE, "delete"),
  (KeySpecificationFlags::NOT_KEY, "not_key"),
  (KeySpecificationFlags::INCOMPLETE, "incomplete"),
  (KeySpecificationFlags::VARIABLE_FLAGS, "variable_flags"),
];

impl KeySpecificationFlags {
  /// 按声明序产出各置位标记的 wire 描述迭代器（零分配）
  #[inline]
  pub fn iter_descriptions(&self) -> impl Iterator<Item = &'static str> + Clone {
    let mask = *self;
    ALL_FLAGS
      .iter()
      .filter(move |(flag, _)| mask.contains(*flag))
      .map(|(_, name)| *name)
  }

  /// 按声明序产出各置位标记的 wire 描述（对标 C# EnumUtils.GetEnumDescriptions）
  #[inline]
  pub fn descriptions(&self) -> Vec<&'static str> {
    let mut out = Vec::with_capacity(self.bits().count_ones() as usize);
    out.extend(self.iter_descriptions());
    out
  }

  /// 解析单个 wire 描述（先根据字符串长度分流，消除无效比较）
  pub fn from_wire_name_single(name: &str) -> Option<Self> {
    let s = name.trim();
    match s.len() {
      2 => {
        if s.eq_ignore_ascii_case("RW") {
          Some(Self::RW)
        } else if s.eq_ignore_ascii_case("RO") {
          Some(Self::RO)
        } else if s.eq_ignore_ascii_case("OW") {
          Some(Self::OW)
        } else if s.eq_ignore_ascii_case("RM") {
          Some(Self::RM)
        } else {
          None
        }
      }
      4 => {
        if s.eq_ignore_ascii_case("NONE") {
          Some(Self::empty())
        } else {
          None
        }
      }
      6 => {
        if s.eq_ignore_ascii_case("ACCESS") {
          Some(Self::ACCESS)
        } else if s.eq_ignore_ascii_case("UPDATE") {
          Some(Self::UPDATE)
        } else if s.eq_ignore_ascii_case("INSERT") {
          Some(Self::INSERT)
        } else if s.eq_ignore_ascii_case("DELETE") {
          Some(Self::DELETE)
        } else if s.eq_ignore_ascii_case("NOTKEY") {
          Some(Self::NOT_KEY)
        } else {
          None
        }
      }
      7 => {
        if s.eq_ignore_ascii_case("NOT_KEY") {
          Some(Self::NOT_KEY)
        } else {
          None
        }
      }
      10 => {
        if s.eq_ignore_ascii_case("INCOMPLETE") {
          Some(Self::INCOMPLETE)
        } else {
          None
        }
      }
      13 | 14 => {
        if s.eq_ignore_ascii_case("VARIABLEFLAGS") || s.eq_ignore_ascii_case("VARIABLE_FLAGS") {
          Some(Self::VARIABLE_FLAGS)
        } else {
          None
        }
      }
      _ => None,
    }
  }

  /// 按 wire 描述解析（大小写不敏感；逗号分隔）
  pub fn from_wire_names(names: &str) -> Option<Self> {
    let mut out = Self::empty();
    for part in names.split(',') {
      let bit = Self::from_wire_name_single(part)?;
      out |= bit;
    }
    Some(out)
  }
}

impl FromStr for KeySpecificationFlags {
  type Err = ();
  #[inline]
  fn from_str(s: &str) -> Result<Self, Self::Err> {
    Self::from_wire_names(s).ok_or(())
  }
}

/// begin_search / find_keys 方法名（C# KeySpecMethodBase.MethodName）
const METHOD_NAME_BEGIN_SEARCH: &str = "begin_search";
const METHOD_NAME_FIND_KEYS: &str = "find_keys";

/// libs/server/Resp/RespCommandKeySpecification.cs:BeginSearchKeySpecMethodBase
///
/// begin_search 方法（C# BeginSearchKeySpecMethodBase 层次）
#[derive(Debug, Clone, PartialEq)]
pub enum BeginSearchMethod {
  /// 固定下标（C# BeginSearchIndex）
  Index(i32),
  /// 关键字定位（C# BeginSearchKeyword）
  Keyword { keyword: String, start_from: i32 },
  /// 未知（C# BeginSearchUnknown）
  Unknown,
}

/// libs/server/Resp/RespCommandKeySpecification.cs:FindKeysKeySpecMethodBase
///
/// find_keys 方法（C# FindKeysKeySpecMethodBase 层次）
#[derive(Debug, Clone, PartialEq)]
pub enum FindKeysMethod {
  /// 区间（C# FindKeysRange）
  Range {
    last_key: i32,
    key_step: i32,
    limit: i32,
  },
  /// 按数量（C# FindKeysKeyNum）
  KeyNum {
    key_num_idx: i32,
    first_key: i32,
    key_step: i32,
  },
  /// 未知（C# FindKeysUnknown）
  Unknown,
}

/// libs/server/Resp/RespCommandKeySpecification.cs:RespCommandKeySpecification
///
/// 单条键规格（C# RespCommandKeySpecification）
#[derive(Debug, Clone, Default)]
pub struct RespCommandKeySpecification {
  /// 提取起点（C# BeginSearch）
  pub begin_search: Option<BeginSearchMethod>,
  /// 键定位规则（C# FindKeys）
  pub find_keys: Option<FindKeysMethod>,
  /// 非显而易见的说明（C# Notes）
  pub notes: Option<String>,
  /// 键标记（C# Flags）
  pub flags: KeySpecificationFlags,
}

impl BeginSearchMethod {
  /// C# nameof(BeginSearchIndex/Keyword/Unknown)
  #[must_use]
  pub fn discriminator(&self) -> &'static str {
    match self {
      Self::Index(_) => "BeginSearchIndex",
      Self::Keyword { .. } => "BeginSearchKeyword",
      Self::Unknown => "BeginSearchUnknown",
    }
  }

  /// C# KeySpecConverter.CanConvert：给定判别名是否可转换
  #[must_use]
  pub fn can_convert(type_discriminator: &str) -> bool {
    matches!(
      type_discriminator,
      "BeginSearchIndex" | "BeginSearchKeyword" | "BeginSearchUnknown"
    )
  }
}

impl FindKeysMethod {
  /// C# nameof(FindKeysRange/KeyNum/Unknown)
  #[must_use]
  pub fn discriminator(&self) -> &'static str {
    match self {
      Self::Range { .. } => "FindKeysRange",
      Self::KeyNum { .. } => "FindKeysKeyNum",
      Self::Unknown => "FindKeysUnknown",
    }
  }

  /// C# KeySpecConverter.CanConvert
  #[must_use]
  pub fn can_convert(type_discriminator: &str) -> bool {
    matches!(
      type_discriminator,
      "FindKeysRange" | "FindKeysKeyNum" | "FindKeysUnknown"
    )
  }
}

impl RespCommandKeySpecification {
  /// 序列化为 RESP 格式
  ///
  /// libs/server/Resp/RespCommandKeySpecification.cs:ToRespFormat
  pub fn to_resp_format<B: RespBuffer, P: RespProtocol>(&self, writer: &mut RespWriter<B, P>) {
    let elem_count = usize::from(self.notes.is_some())
      + usize::from(!self.flags.is_empty())
      + usize::from(self.begin_search.is_some())
      + usize::from(self.find_keys.is_some());

    writer.write_map_length(elem_count);

    if let Some(notes) = &self.notes {
      writer.write_bulk_string(b"notes");
      writer.write_ascii_bulk_string(notes);
    }

    if !self.flags.is_empty() {
      writer.write_bulk_string(b"flags");
      writer.write_set_length(self.flags.iter().count());
      for flag in self.flags.iter_descriptions() {
        writer.write_simple_string(flag);
      }
    }

    if let Some(begin_search) = &self.begin_search {
      write_begin_search(begin_search, writer);
    }

    if let Some(find_keys) = &self.find_keys {
      write_find_keys(find_keys, writer);
    }
  }
}

/// begin_search 序列化
fn write_begin_search<B: RespBuffer, P: RespProtocol>(
  method: &BeginSearchMethod,
  writer: &mut RespWriter<B, P>,
) {
  writer.write_ascii_bulk_string(METHOD_NAME_BEGIN_SEARCH);
  writer.write_map_length(2);
  writer.write_bulk_string(b"type");
  match method {
    BeginSearchMethod::Index(index) => {
      writer.write_bulk_string(b"index");
      writer.write_bulk_string(b"spec");
      writer.write_map_length(1);
      writer.write_bulk_string(b"index");
      writer.write_int32(*index);
    }
    BeginSearchMethod::Keyword {
      keyword,
      start_from,
    } => {
      writer.write_bulk_string(b"keyword");
      writer.write_bulk_string(b"spec");
      writer.write_map_length(2);
      writer.write_bulk_string(b"keyword");
      writer.write_ascii_bulk_string(keyword);
      writer.write_bulk_string(b"startfrom");
      writer.write_int32(*start_from);
    }
    BeginSearchMethod::Unknown => {
      writer.write_bulk_string(b"unknown");
      writer.write_bulk_string(b"spec");
      writer.write_array_length(0);
    }
  }
}

/// find_keys 序列化
fn write_find_keys<B: RespBuffer, P: RespProtocol>(
  method: &FindKeysMethod,
  writer: &mut RespWriter<B, P>,
) {
  writer.write_ascii_bulk_string(METHOD_NAME_FIND_KEYS);
  writer.write_map_length(2);
  writer.write_bulk_string(b"type");
  match method {
    FindKeysMethod::Range {
      last_key,
      key_step,
      limit,
    } => {
      writer.write_bulk_string(b"range");
      writer.write_bulk_string(b"spec");
      writer.write_map_length(3);
      writer.write_bulk_string(b"lastkey");
      writer.write_int32(*last_key);
      writer.write_bulk_string(b"keystep");
      writer.write_int32(*key_step);
      writer.write_bulk_string(b"limit");
      writer.write_int32(*limit);
    }
    FindKeysMethod::KeyNum {
      key_num_idx,
      first_key,
      key_step,
    } => {
      writer.write_bulk_string(b"keynum");
      writer.write_bulk_string(b"spec");
      writer.write_map_length(3);
      writer.write_bulk_string(b"keynumidx");
      writer.write_int32(*key_num_idx);
      writer.write_bulk_string(b"firstkey");
      writer.write_int32(*first_key);
      writer.write_bulk_string(b"keystep");
      writer.write_int32(*key_step);
    }
    FindKeysMethod::Unknown => {
      writer.write_bulk_string(b"unknown");
      writer.write_bulk_string(b"spec");
      writer.write_array_length(0);
    }
  }
}

#[cfg(test)]
mod tests {
  use super::*;
  use crate::resp_memory_writer::Resp2;

  #[test]
  fn flags_descriptions_roundtrip() {
    let flags = KeySpecificationFlags::from_wire_names("RW, Insert").unwrap();
    assert_eq!(flags.descriptions(), vec!["RW", "insert"]);
    assert!(KeySpecificationFlags::from_wire_names("BOGUS").is_none());
    assert!(
      KeySpecificationFlags::from_wire_names("None")
        .unwrap()
        .is_empty()
    );
  }

  #[test]
  fn to_resp_format_index_range() {
    let ks = RespCommandKeySpecification {
      begin_search: Some(BeginSearchMethod::Index(1)),
      find_keys: Some(FindKeysMethod::Range {
        last_key: 0,
        key_step: 1,
        limit: 0,
      }),
      notes: None,
      flags: KeySpecificationFlags::from_wire_names("RW, Insert").unwrap(),
    };
    let mut w = RespWriter::<Vec<u8>, Resp2>::new();
    ks.to_resp_format(&mut w);
    // RESP2：map 降级倍长数组（flags + begin_search + find_keys = 3 键 → *6）
    let text = String::from_utf8(w.into_inner()).unwrap();
    assert!(text.starts_with("*6\r\n"), "map 头：{text}");
    assert!(
      text.contains("$5\r\nflags\r\n*2\r\n+RW\r\n+insert\r\n"),
      "{text}"
    );
    assert!(text.contains("$12\r\nbegin_search\r\n"), "{text}");
    assert!(text.contains("$9\r\nfind_keys\r\n"), "{text}");
  }
}
