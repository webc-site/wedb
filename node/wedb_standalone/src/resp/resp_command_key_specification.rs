//! 命令键规格（对标 libs/server/Resp/RespCommandKeySpecification.cs）
//!
//! C# 以类层次（BeginSearchIndex/Keyword/Unknown、FindKeysRange/KeyNum/
//! Unknown）+ JSON TypeDiscriminator 多态；Rust 侧以枚举承接，导出/导入
//! 面保持一致。

use super::resp_memory_writer::RespMemoryWriter;
use crate::resp::parser::resp_ext::RespSliceExt;

/// begin_search / find_keys 方法名（C# KeySpecMethodBase.MethodName）
const METHOD_NAME_BEGIN_SEARCH: &str = "begin_search";
const METHOD_NAME_FIND_KEYS: &str = "find_keys";

/// RESP 键规格标记（C# KeySpecificationFlags）
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct KeySpecificationFlags(u16);

impl KeySpecificationFlags {
  /// 无
  pub const NONE: Self = Self(0);
  /// RW
  pub const RW: Self = Self(1);
  /// RO
  pub const RO: Self = Self(1 << 1);
  /// OW
  pub const OW: Self = Self(1 << 2);
  /// RM
  pub const RM: Self = Self(1 << 3);
  /// access
  pub const ACCESS: Self = Self(1 << 4);
  /// update
  pub const UPDATE: Self = Self(1 << 5);
  /// insert
  pub const INSERT: Self = Self(1 << 6);
  /// delete
  pub const DELETE: Self = Self(1 << 7);
  /// not_key
  pub const NOT_KEY: Self = Self(1 << 8);
  /// incomplete
  pub const INCOMPLETE: Self = Self(1 << 9);
  /// variable_flags
  pub const VARIABLE_FLAGS: Self = Self(1 << 10);

  /// 是否无标记
  #[inline]
  pub fn is_none(&self) -> bool {
    self.0 == 0
  }

  /// 按声明序产出各置位标记的 wire 描述（C# EnumUtils.GetEnumDescriptions）
  pub fn descriptions(&self) -> Vec<&'static str> {
    const ALL: [(u16, &str); 11] = [
      (KeySpecificationFlags::RW.0, "RW"),
      (KeySpecificationFlags::RO.0, "RO"),
      (KeySpecificationFlags::OW.0, "OW"),
      (KeySpecificationFlags::RM.0, "RM"),
      (KeySpecificationFlags::ACCESS.0, "access"),
      (KeySpecificationFlags::UPDATE.0, "update"),
      (KeySpecificationFlags::INSERT.0, "insert"),
      (KeySpecificationFlags::DELETE.0, "delete"),
      (KeySpecificationFlags::NOT_KEY.0, "not_key"),
      (KeySpecificationFlags::INCOMPLETE.0, "incomplete"),
      (KeySpecificationFlags::VARIABLE_FLAGS.0, "variable_flags"),
    ];
    ALL
      .iter()
      .filter(|(bit, _)| self.0 & bit != 0)
      .map(|(_, name)| *name)
      .collect()
  }

  /// 按 wire 描述解析（大小写不敏感；C# JsonStringEnumConverter 语义）
  pub fn from_wire_names(names: &str) -> Option<Self> {
    let mut out = Self(0);
    for name in names.split(',') {
      let name = name.trim().to_ascii_uppercase();
      let bit = match name.as_str() {
        "NONE" => Self::NONE,
        "RW" => Self::RW,
        "RO" => Self::RO,
        "OW" => Self::OW,
        "RM" => Self::RM,
        "ACCESS" => Self::ACCESS,
        "UPDATE" => Self::UPDATE,
        "INSERT" => Self::INSERT,
        "DELETE" => Self::DELETE,
        "NOT_KEY" | "NOTKEY" => Self::NOT_KEY,
        "INCOMPLETE" => Self::INCOMPLETE,
        "VARIABLE_FLAGS" | "VARIABLEFLAGS" => Self::VARIABLE_FLAGS,
        _ => return None,
      };
      out.0 |= bit.0;
    }
    Some(out)
  }
}

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

/// TypeDiscriminator ↔ 方法互转（C# KeySpecConverter 的读写判别面）
impl BeginSearchMethod {
  /// C# nameof(BeginSearchIndex/Keyword/Unknown)
  #[cfg_attr(not(test), allow(dead_code))] // 判别名仅导出面触达
  pub(crate) fn discriminator(&self) -> &'static str {
    match self {
      Self::Index(_) => "BeginSearchIndex",
      Self::Keyword { .. } => "BeginSearchKeyword",
      Self::Unknown => "BeginSearchUnknown",
    }
  }

  /// C# KeySpecConverter.CanConvert：给定判别名是否可转换
  pub(crate) fn can_convert(type_discriminator: &str) -> bool {
    matches!(
      type_discriminator,
      "BeginSearchIndex" | "BeginSearchKeyword" | "BeginSearchUnknown"
    )
  }
}

impl FindKeysMethod {
  /// C# nameof(FindKeysRange/KeyNum/Unknown)
  #[cfg_attr(not(test), allow(dead_code))] // 判别名仅导出面触达
  pub(crate) fn discriminator(&self) -> &'static str {
    match self {
      Self::Range { .. } => "FindKeysRange",
      Self::KeyNum { .. } => "FindKeysKeyNum",
      Self::Unknown => "FindKeysUnknown",
    }
  }

  /// C# KeySpecConverter.CanConvert
  pub(crate) fn can_convert(type_discriminator: &str) -> bool {
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
  pub fn to_resp_format(&self, writer: &mut RespMemoryWriter) {
    let mut elem_count = 0;

    if self.notes.is_some() {
      elem_count += 1;
    }

    if !self.flags.is_none() {
      elem_count += 1;
    }

    if self.begin_search.is_some() {
      elem_count += 1;
    }

    if self.find_keys.is_some() {
      elem_count += 1;
    }

    writer.write_map_length(elem_count);

    if let Some(notes) = &self.notes {
      writer.write_bulk_string(b"notes");
      writer.write_ascii_bulk_string(notes);
    }

    if !self.flags.is_none() {
      let resp_format_flags = self.flags.descriptions();
      writer.write_bulk_string(b"flags");
      writer.write_set_length(resp_format_flags.len());
      for flag in resp_format_flags {
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

  /// 依据键规格定位键提取起点（参数表不含命令名，起始于首参数）
  ///
  /// libs/server/Resp/RespCommandKeySpecification.cs:TryGetStartIndex
  pub fn try_get_start_index(&self, parse_state: &[&[u8]]) -> Option<usize> {
    match self.begin_search.as_ref()? {
      BeginSearchMethod::Index(index) => Some(if *index < 0 {
        (parse_state.len() as i64 + *index as i64) as usize
      } else {
        *index as usize
      }),
      BeginSearchMethod::Keyword {
        keyword,
        start_from,
      } => {
        let keyword = keyword.as_bytes();
        // 负 start_from 自尾部反查（C# 同语义）
        let count = parse_state.len() as i64;
        let mut i = if *start_from < 0 {
          count + *start_from as i64
        } else {
          *start_from as i64
        };
        let step: i64 = if *start_from < 0 { -1 } else { 1 };
        let end: i64 = if *start_from < 0 { -1 } else { count };
        while i != end {
          let idx = i as usize;
          if idx < parse_state.len() && parse_state[idx].eq_ignore_ascii_case(keyword) {
            return Some(i as usize + 1);
          }
          i += step;
        }
        None
      }
      // 未知规格取不到起点
      BeginSearchMethod::Unknown => None,
    }
  }

  /// 自 start_index 起提取键名（parse_state 含命令名时由调用方传入完整表）
  ///
  /// libs/server/Resp/RespCommandKeySpecification.cs:ExtractKeys
  pub fn extract_keys<'a>(
    &self,
    parse_state: &[&'a [u8]],
    start_index: usize,
    keys: &mut Vec<&'a [u8]>,
  ) {
    let Some(find_keys) = self.find_keys.as_ref() else {
      return;
    };
    match find_keys {
      FindKeysMethod::Range {
        last_key,
        key_step,
        limit,
      } => {
        let count = parse_state.len() as i64;
        let last_key = if *last_key < 0 {
          // 负 last_key：按 limit 因子折算槽位数
          let available_args = count - start_index as i64;
          let limit_factor = if *limit <= 1 {
            available_args
          } else {
            available_args / *limit as i64
          };
          let slots_available = (limit_factor + *key_step as i64 - 1) / *key_step as i64;
          let last = start_index as i64 + slots_available * *key_step as i64 - *key_step as i64;
          last.min(count - 1)
        } else {
          (start_index as i64 + *last_key as i64).min(count - 1)
        };

        let mut i = start_index as i64;
        while i <= last_key {
          let arg = parse_state[i as usize];
          if !arg.is_empty() {
            keys.push(arg);
          }
          i += *key_step as i64;
        }
      }
      FindKeysMethod::KeyNum {
        key_num_idx,
        first_key,
        key_step,
      } => {
        let count = parse_state.len() as i64;
        let mut num_keys = 0i64;
        let mut first = start_index as i64 + *first_key as i64;
        if *first_key < 0 {
          first = count + *first_key as i64;
        }

        let key_num_pos = if *key_num_idx >= 0 {
          let pos = start_index as i64 + *key_num_idx as i64;
          (pos < count).then_some(pos)
        } else {
          let pos = count + *key_num_idx as i64;
          (pos >= 0).then_some(pos)
        };
        if let Some(n) = key_num_pos.and_then(|pos| parse_state[pos as usize].try_parse_i64()) {
          num_keys = n;
        }

        if num_keys > 0 && first >= 0 {
          let step = *key_step as i64;
          let mut i = 0i64;
          while i < num_keys && first + i * step < count {
            keys.push(parse_state[(first + i * step) as usize]);
            i += 1;
          }
        }
      }
      // 未知规格不提取（C# 空实现）
      FindKeysMethod::Unknown => {}
    }
  }
}

/// begin_search 序列化
///
/// libs/server/Resp/RespCommandKeySpecification.cs:BeginSearch*.ToRespFormat
fn write_begin_search(method: &BeginSearchMethod, writer: &mut RespMemoryWriter) {
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
///
/// libs/server/Resp/RespCommandKeySpecification.cs:FindKeys*.ToRespFormat
fn write_find_keys(method: &FindKeysMethod, writer: &mut RespMemoryWriter) {
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
  use super::{
    BeginSearchMethod, FindKeysMethod, KeySpecificationFlags, RespCommandKeySpecification,
  };
  use crate::resp::resp_memory_writer::RespMemoryWriter;

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
    let mut w = RespMemoryWriter::new(false);
    ks.to_resp_format(&mut w);
    // RESP2：map 降级倍长数组（flags + begin_search + find_keys = 3 键 → *6）
    let text = String::from_utf8(w.out).unwrap();
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
