//! 命令键规格（对标 libs/server/Resp/RespCommandKeySpecification.cs 与 RespCommandInfoSimplifiedStructs.cs）
//!
//! C# 以类层次（BeginSearchIndex/Keyword/Unknown、FindKeysRange/KeyNum/
//! Unknown）+ JSON TypeDiscriminator 多态；Rust 侧以枚举承接，本文件只承载
//! 命令信息导出/导入面（RESP 序列化与解析）。
//!
//! 键提取不在本面：C# `ExtractKeys`/`TryGetStartIndex` 在 C# 侧亦无生产调用点
//! （活消费面只有命令信息导出），rust 生产键提取唯一口径为
//! [`crate::catalog::SimpleRespKeySpec`]（对标
//! libs/server/SessionParseStateExtensions.cs），此处不设第二套提取实现。
//!
//! 在 garnet 中的相对路径: libs/server/Resp/RespCommandKeySpecification.cs(对标 C# RespCommand KeyArgumentsSpec)

use core::str::FromStr;

use bitflags::bitflags;

use crate::{
  catalog::parse_member_names,
  resp_memory_writer::{RespBuffer, RespProtocol, RespWriter},
};

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

/// 归一化线名定长槽：剥下划线 + ASCII 大写折叠后零填充（表内最长
/// VARIABLEFLAGS=13，取 16 上界；更长的归一形必不命中任何表项）
const NORM_NAME_MAX: usize = 16;
type NormWireName = [u8; NORM_NAME_MAX];

/// wire 描述 → 归一化定长键（编译期由 ALL_FLAGS 单源派生，无第二份手写表）
const fn normalize_wire_name(wire: &str) -> NormWireName {
  let bytes = wire.as_bytes();
  let mut out = [0_u8; NORM_NAME_MAX];
  let mut i = 0;
  let mut o = 0;
  while i < bytes.len() {
    let c = bytes[i];
    if c != b'_' {
      out[o] = c.to_ascii_uppercase();
      o += 1;
    }
    i += 1;
  }
  out
}

/// 归一化查表（编译期构建）：from_wire_name_single 的每查询双迭代器
/// 逐字节滤下划线 + 大小写折叠，收敛为单次归一 + 定长数组比较
const WIRE_NAME_LOOKUP: [(NormWireName, KeySpecificationFlags); ALL_FLAGS.len()] = {
  let mut out = [([0_u8; NORM_NAME_MAX], KeySpecificationFlags::empty()); ALL_FLAGS.len()];
  let mut i = 0;
  while i < ALL_FLAGS.len() {
    out[i] = (normalize_wire_name(ALL_FLAGS[i].1), ALL_FLAGS[i].0);
    i += 1;
  }
  out
};

impl KeySpecificationFlags {
  /// 置位数量（单指令 popcnt）
  #[inline]
  #[must_use]
  pub const fn count(&self) -> usize {
    self.bits().count_ones() as usize
  }

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
    let mut out = Vec::with_capacity(self.count());
    out.extend(self.iter_descriptions());
    out
  }

  /// 解析单个 wire 描述（忽略大小写与下划线，归一后查编译期派生表）
  pub fn from_wire_name_single(name: &str) -> Option<Self> {
    let s = name.trim();
    if s.is_empty() {
      return None;
    }
    if s.eq_ignore_ascii_case("NONE") {
      return Some(Self::empty());
    }
    let mut norm = [0_u8; NORM_NAME_MAX];
    let mut o = 0;
    for &c in s.as_bytes() {
      if c == b'_' {
        continue;
      }
      // 归一形超表内最长名 → 必不命中，早退避免截断误配
      if o == NORM_NAME_MAX {
        return None;
      }
      norm[o] = c.to_ascii_uppercase();
      o += 1;
    }
    WIRE_NAME_LOOKUP
      .iter()
      .find_map(|(table_norm, flag)| (*table_norm == norm).then_some(*flag))
  }

  /// 按 wire 描述解析（大小写不敏感；逗号分隔；crate 内经 [`FromStr`] 承接外部面）
  pub fn from_wire_names(names: &str) -> Option<Self> {
    parse_member_names(names, |part| {
      Self::from_wire_name_single(part).map(|flag| flag.bits() as u32)
    })
    .map(|bits| Self::from_bits_retain(bits as u16))
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
#[derive(Clone, PartialEq)]
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
#[derive(Clone, PartialEq)]
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
#[derive(Clone)]
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
  pub const fn discriminator(&self) -> &'static str {
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
      writer.write_set_length(self.flags.count());
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
