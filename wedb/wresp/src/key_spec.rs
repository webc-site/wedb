//! 命令键规格（对标 libs/server/Resp/RespCommandKeySpecification.cs 与 RespCommandInfoSimplifiedStructs.cs）
//!
//! C# 以类层次（BeginSearchIndex/Keyword/Unknown、FindKeysRange/KeyNum/
//! Unknown）+ JSON TypeDiscriminator 多态；Rust 侧以枚举承接，导出/导入
//! 面保持一致。

use core::{
  ops::{BitAnd, BitAndAssign, BitOr, BitOrAssign, Not},
  str::FromStr,
};

use crate::{IRespSerializable, RespBuffer, RespProtocol, RespSliceExt, RespWriter};

/// RESP 键规格标记位图（对标 C# KeySpecificationFlags）
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Hash)]
pub struct KeySpecificationFlags(pub u16);

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
  /// 无标记
  pub const NONE: Self = Self(0);
  /// RW: 读写
  pub const RW: Self = Self(1);
  /// RO: 只读
  pub const RO: Self = Self(1 << 1);
  /// OW: 覆写
  pub const OW: Self = Self(1 << 2);
  /// RM: 读后写
  pub const RM: Self = Self(1 << 3);
  /// ACCESS: 访问
  pub const ACCESS: Self = Self(1 << 4);
  /// UPDATE: 更新
  pub const UPDATE: Self = Self(1 << 5);
  /// INSERT: 插入
  pub const INSERT: Self = Self(1 << 6);
  /// DELETE: 删除
  pub const DELETE: Self = Self(1 << 7);
  /// NOT_KEY: 非键参数
  pub const NOT_KEY: Self = Self(1 << 8);
  /// INCOMPLETE: 不完整规格
  pub const INCOMPLETE: Self = Self(1 << 9);
  /// VARIABLE_FLAGS: 动态标记
  pub const VARIABLE_FLAGS: Self = Self(1 << 10);

  /// 空标记集
  #[inline]
  pub const fn empty() -> Self {
    Self(0)
  }

  /// 置位数量
  #[inline]
  pub const fn count(&self) -> usize {
    self.0.count_ones() as usize
  }

  /// 判断是否包含目标标记位
  #[inline]
  pub const fn contains(&self, other: Self) -> bool {
    (self.0 & other.0) == other.0
  }

  /// 判断是否与目标标记位有交集
  #[inline]
  pub const fn intersects(&self, other: Self) -> bool {
    (self.0 & other.0) != 0
  }

  /// 是否无标记
  #[inline]
  pub const fn is_none(&self) -> bool {
    self.0 == 0
  }

  /// 是否为空
  #[inline]
  pub const fn is_empty(&self) -> bool {
    self.0 == 0
  }

  /// 按声明序产出各置位标记的 wire 描述迭代器（零分配）
  #[inline]
  pub fn iter_descriptions(&self) -> impl Iterator<Item = &'static str> + Clone {
    let mask = self.0;
    ALL_FLAGS
      .iter()
      .filter(move |(flag, _)| mask & flag.0 != 0)
      .map(|(_, name)| *name)
  }

  /// 按声明序产出各置位标记的 wire 描述（对标 C# EnumUtils.GetEnumDescriptions）
  #[inline]
  pub fn descriptions(&self) -> Vec<&'static str> {
    let mut out = Vec::with_capacity(self.count());
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
          Some(Self::NONE)
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
    let mut out = Self::NONE;
    for part in names.split(',') {
      let bit = Self::from_wire_name_single(part)?;
      out |= bit;
    }
    Some(out)
  }
}

impl BitOr for KeySpecificationFlags {
  type Output = Self;
  #[inline]
  fn bitor(self, rhs: Self) -> Self {
    Self(self.0 | rhs.0)
  }
}

impl BitOrAssign for KeySpecificationFlags {
  #[inline]
  fn bitor_assign(&mut self, rhs: Self) {
    self.0 |= rhs.0;
  }
}

impl BitAnd for KeySpecificationFlags {
  type Output = Self;
  #[inline]
  fn bitand(self, rhs: Self) -> Self {
    Self(self.0 & rhs.0)
  }
}

impl BitAndAssign for KeySpecificationFlags {
  #[inline]
  fn bitand_assign(&mut self, rhs: Self) {
    self.0 &= rhs.0;
  }
}

impl Not for KeySpecificationFlags {
  type Output = Self;
  #[inline]
  fn not(self) -> Self {
    Self(!self.0)
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

/// libs/server/Resp/RespCommandKeySpecification.cs:BeginSearchMethod
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

/// libs/server/Resp/RespCommandKeySpecification.cs:FindKeysMethod
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
      + usize::from(!self.flags.is_none())
      + usize::from(self.begin_search.is_some())
      + usize::from(self.find_keys.is_some());

    writer.write_map_length(elem_count);

    if let Some(notes) = &self.notes {
      writer.write_bulk_string(b"notes");
      writer.write_ascii_bulk_string(notes);
    }

    if !self.flags.is_none() {
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

impl IRespSerializable for RespCommandKeySpecification {
  fn to_resp_format<B: RespBuffer, P: RespProtocol>(&self, writer: &mut RespWriter<B, P>) {
    self.to_resp_format(writer);
  }
}

impl RespCommandKeySpecification {
  /// 依据键规格定位键提取起点（参数表不含命令名，起始于首参数）
  ///
  /// libs/server/Resp/RespCommandKeySpecification.cs:TryGetStartIndex
  pub fn try_get_start_index(&self, parse_state: &[&[u8]]) -> Option<usize> {
    match self.begin_search.as_ref()? {
      BeginSearchMethod::Index(index) => {
        let idx = if *index < 0 {
          parse_state.len() as i64 + *index as i64
        } else {
          *index as i64
        };
        usize::try_from(idx).ok()
      }
      BeginSearchMethod::Keyword {
        keyword,
        start_from,
      } => {
        let kw = keyword.as_bytes();
        let start = usize::try_from((*start_from).max(0)).ok()?;
        parse_state
          .iter()
          .enumerate()
          .skip(start)
          .find_map(|(idx, arg)| {
            if arg.eq_ignore_ascii_case(kw) {
              Some(idx + 1)
            } else {
              None
            }
          })
      }
      BeginSearchMethod::Unknown => None,
    }
  }

  /// 依据键规格提取全部键切片
  ///
  /// libs/server/Resp/RespCommandKeySpecification.cs:ExtractKeys
  pub fn extract_keys<'a>(
    &self,
    parse_state: &[&'a [u8]],
    start_index: usize,
    keys: &mut Vec<&'a [u8]>,
  ) {
    match self.find_keys.as_ref() {
      Some(FindKeysMethod::Range {
        last_key,
        key_step,
        limit,
      }) => {
        let total = parse_state.len();
        if start_index >= total {
          return;
        }
        let step = (*key_step).max(1) as usize;
        let last_key_idx = if *last_key < 0 {
          let available = total.saturating_sub(start_index);
          let limit_factor = if *limit <= 1 {
            available
          } else {
            available / (*limit as usize)
          };
          let slots = limit_factor.div_ceil(step);
          let calculated = start_index + slots * step - step;
          calculated.min(total.saturating_sub(1))
        } else {
          (start_index + *last_key as usize).min(total.saturating_sub(1))
        };
        let mut curr = start_index;
        while curr <= last_key_idx && curr < total {
          if !parse_state[curr].is_empty() {
            keys.push(parse_state[curr]);
          }
          curr = curr.saturating_add(step);
        }
      }
      Some(FindKeysMethod::KeyNum {
        key_num_idx,
        first_key,
        key_step,
      }) => {
        let total = parse_state.len();
        let num_pos = if *key_num_idx < 0 {
          (total as i64 + *key_num_idx as i64) as usize
        } else {
          start_index + *key_num_idx as usize
        };
        if num_pos >= total {
          return;
        }
        let Some(num_keys) = parse_state[num_pos].try_parse_i64() else {
          return;
        };
        if num_keys <= 0 {
          return;
        }
        let step = (*key_step).max(1) as usize;
        let first = if *first_key < 0 {
          (total as i64 + *first_key as i64) as usize
        } else {
          start_index + *first_key as usize
        };
        let mut curr = first;
        for _ in 0..num_keys {
          if curr >= total {
            break;
          }
          if !parse_state[curr].is_empty() {
            keys.push(parse_state[curr]);
          }
          curr = curr.saturating_add(step);
        }
      }
      _ => {}
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
  use crate::Resp2;

  #[test]
  fn flags_descriptions_roundtrip() {
    let flags = KeySpecificationFlags::from_wire_names("RW, Insert").unwrap();
    assert_eq!(flags.descriptions(), vec!["RW", "insert"]);
    assert!(KeySpecificationFlags::from_wire_names("BOGUS").is_none());
    assert!(
      KeySpecificationFlags::from_wire_names("None")
        .unwrap()
        .is_none()
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

  #[test]
  fn try_get_start_index_forms() {
    let args = [b"GET".as_slice(), b"key", b"extra"];
    let by_index = RespCommandKeySpecification {
      begin_search: Some(BeginSearchMethod::Index(1)),
      find_keys: Some(FindKeysMethod::Unknown),
      notes: None,
      flags: KeySpecificationFlags::NONE,
    };
    assert_eq!(by_index.try_get_start_index(&args), Some(1));
    // 负下标自尾部折算
    let neg = RespCommandKeySpecification {
      begin_search: Some(BeginSearchMethod::Index(-1)),
      find_keys: Some(FindKeysMethod::Unknown),
      notes: None,
      flags: KeySpecificationFlags::NONE,
    };
    assert_eq!(neg.try_get_start_index(&args), Some(2));
    // 关键字定位
    let kw = RespCommandKeySpecification {
      begin_search: Some(BeginSearchMethod::Keyword {
        keyword: "FROM".to_string(),
        start_from: 0,
      }),
      find_keys: Some(FindKeysMethod::Unknown),
      notes: None,
      flags: KeySpecificationFlags::NONE,
    };
    let with_kw = [b"X".as_slice(), b"FROM", b"key"];
    assert_eq!(kw.try_get_start_index(&with_kw), Some(2));
    assert_eq!(kw.try_get_start_index(&args), None);
  }

  #[test]
  fn extract_keys_range_and_keynum() {
    let args = [b"MSET".as_slice(), b"k1", b"v1", b"k2", b"v2"];
    let range = RespCommandKeySpecification {
      begin_search: Some(BeginSearchMethod::Index(1)),
      find_keys: Some(FindKeysMethod::Range {
        last_key: 2,
        key_step: 2,
        limit: 0,
      }),
      notes: None,
      flags: KeySpecificationFlags::NONE,
    };
    let mut keys = Vec::new();
    range.extract_keys(&args, 1, &mut keys);
    assert_eq!(keys, vec![b"k1".as_slice(), b"k2".as_slice()]);

    // keynum：XADD 式 [keynumidx=1, firstkey=1, step=1]
    let xadd = [b"3".as_slice(), b"k1", b"k2", b"k3"];
    let keynum = RespCommandKeySpecification {
      begin_search: Some(BeginSearchMethod::Index(0)),
      find_keys: Some(FindKeysMethod::KeyNum {
        key_num_idx: 0,
        first_key: 1,
        key_step: 1,
      }),
      notes: None,
      flags: KeySpecificationFlags::NONE,
    };
    let mut keys = Vec::new();
    keynum.extract_keys(&xadd, 0, &mut keys);
    assert_eq!(
      keys,
      vec![b"k1".as_slice(), b"k2".as_slice(), b"k3".as_slice()]
    );
  }
}
