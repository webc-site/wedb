//! 简化命令信息结构（对标 libs/server/Resp/RespCommandInfoSimplifiedStructs.cs）
//!
//! C# 以 fixed 布局 struct 承接会话快速路径（SimpleRespCommandInfo /
//! SimpleRespKeySpec*）；Rust 侧为纯数据结构，键规格以下标寻址。

use smallvec::SmallVec;
use wbase::{num::strict_i32, store_type::StoreType};

use super::commands_info::{RespCommandFlags, RespCommandsInfo};
use crate::key_spec::{
  BeginSearchMethod, FindKeysMethod, KeySpecificationFlags, RespCommandKeySpecification,
};

/// 键收集内联容量（高频命令 1-4 键零堆分配）
pub const INLINE_KEYS: usize = 4;

/// 简化版命令信息（C# SimpleRespCommandInfo）
#[derive(Debug, Clone)]
pub struct SimpleRespCommandInfo {
  /// 命令 arity（C# sbyte Arity）
  pub arity: i8,
  /// 事务上下文是否可用（C# AllowedInTxn）
  pub allowed_in_txn: bool,
  /// 是否含子命令（C# IsParent）
  pub is_parent: bool,
  /// 是否为子命令（C# IsSubCommand）
  pub is_sub_command: bool,
  /// 简化键规格（C# KeySpecs）
  pub key_specs: Vec<SimpleRespKeySpec>,
  /// 作用存储类型（C# StoreType）
  pub store_type: StoreType,
}

impl Default for SimpleRespCommandInfo {
  /// C# SimpleRespCommandInfo.Default
  fn default() -> Self {
    Self {
      arity: 0,
      allowed_in_txn: false,
      is_parent: false,
      is_sub_command: false,
      key_specs: Vec::new(),
      store_type: StoreType::None,
    }
  }
}

/// 简化版 begin_search 规格（对标 C# SimpleRespKeySpecBeginSearch）
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct SimpleRespKeySpecBeginSearch {
  /// 键前置关键字（index 型为空）
  pub keyword: Vec<u8>,
  /// 键下标或关键字检索起点
  pub index: i32,
  /// true = index 型，否则 keyword 型
  pub is_index_type: bool,
}

/// 简化版 find_keys 规格（对标 C# SimpleRespKeySpecFindKeys）
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct SimpleRespKeySpecFindKeys {
  /// 键数量参数下标（keynum 型）
  pub key_num_index: i32,
  /// 首键下标（keynum 型）
  pub first_key: i32,
  /// 末键下标或 limit（range 型）
  pub last_key_or_limit: i32,
  /// 找到一键后跳过的参数个数
  pub key_step: i32,
  /// true = range 型，否则 keynum 型
  pub is_range_type: bool,
  /// true = range 且按 limit 截断
  pub is_range_limit_type: bool,
}

/// 简化版单条键规格（对标 C# SimpleRespKeySpec）
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct SimpleRespKeySpec {
  /// begin_search 规格
  pub begin_search: SimpleRespKeySpecBeginSearch,
  /// find_keys 规格
  pub find_keys: SimpleRespKeySpecFindKeys,
  /// 键规格标记位图
  pub flags: KeySpecificationFlags,
}

impl SimpleRespKeySpec {
  /// 依键规格从参数流中计算 (first_idx, last_idx, step)；返回 None 表示越界或关键字未命中
  ///
  /// 对标 libs/server/SessionParseStateExtensions.cs:TryGetKeySearchArgsFromSimpleKeySpec
  pub fn try_get_key_search_args<B, F>(
    &self,
    arg_count: usize,
    mut get_arg: F,
    is_sub_command: bool,
  ) -> Option<(usize, usize, usize)>
  where
    B: AsRef<[u8]>,
    F: FnMut(usize) -> Option<B>,
  {
    let count = arg_count as isize;
    if count <= 0 {
      return None;
    }

    let begin_search_idx = if self.begin_search.index < 0 {
      count + self.begin_search.index as isize
    } else {
      self.begin_search.index as isize - if is_sub_command { 2 } else { 1 }
    };
    if begin_search_idx < 0 || begin_search_idx >= count {
      return None;
    }

    let mut first_key_idx: isize = -1;
    if self.begin_search.is_index_type {
      first_key_idx = begin_search_idx;
    } else {
      let step: isize = if self.begin_search.index < 0 { -1 } else { 1 };
      let mut i = begin_search_idx;
      while i >= 0 && i < count {
        if let Some(bytes) = get_arg(i as usize)
          && bytes
            .as_ref()
            .eq_ignore_ascii_case(&self.begin_search.keyword)
        {
          first_key_idx = i + 1;
          break;
        }
        i += step;
      }
    }
    if first_key_idx < 0 {
      return None;
    }

    let key_step = self.find_keys.key_step as isize;
    if key_step <= 0 {
      return None;
    }

    let mut last_key_idx: isize;
    if self.find_keys.is_range_type {
      if self.find_keys.is_range_limit_type {
        let limit = self.find_keys.last_key_or_limit as isize;
        let key_num = 1 + (count - 1 - first_key_idx) / key_step;
        last_key_idx = if limit <= 1 {
          first_key_idx + (key_num - 1) * key_step
        } else {
          first_key_idx + ((key_num / limit) - 1) * key_step
        };
      } else {
        let raw = self.find_keys.last_key_or_limit as isize;
        last_key_idx = if raw < 0 {
          raw + count
        } else {
          first_key_idx + raw
        };
      }
    } else {
      let key_num_idx = begin_search_idx + self.find_keys.key_num_index as isize;
      if key_num_idx < 0 || key_num_idx >= count {
        return None;
      }
      let key_num_bytes = get_arg(key_num_idx as usize)?;
      let key_num = strict_i32(key_num_bytes.as_ref())?;
      if key_num <= 0 {
        return None;
      }
      first_key_idx += self.find_keys.first_key as isize;
      last_key_idx = first_key_idx.saturating_add((key_num as isize - 1).saturating_mul(key_step));
    }

    if last_key_idx >= count {
      last_key_idx = count - 1;
    }

    if first_key_idx < 0 || first_key_idx > last_key_idx {
      return None;
    }

    Some((
      first_key_idx as usize,
      last_key_idx as usize,
      key_step as usize,
    ))
  }

  /// 针对 &[&[u8]] 切片的快捷计算（集群槽位提取高频路径，零分配）
  #[inline]
  pub fn get_key_search_args_slice(
    &self,
    args: &[&[u8]],
    is_sub_command: bool,
  ) -> Option<(usize, usize, usize)> {
    self.try_get_key_search_args(args.len(), |i| args.get(i).copied(), is_sub_command)
  }

  /// 扫描单条键规格的键参数区，按键下标升序回调 sink
  ///
  /// 对标 libs/server/SessionParseStateExtensions.cs:TryAppendKeysFromSpec 与
  /// TryAppendKeysAndFlagsFromSpec 的逐规格扫描循环；参数区 (first, last, step)
  /// 已由 wresp 侧钳制在参数表界内（numkeys 溢出安全，PR #2112 语义）
  #[inline]
  pub fn scan_keys<'a>(
    &self,
    args: &[&'a [u8]],
    is_sub_command: bool,
    mut sink: impl FnMut(&'a [u8], usize),
  ) {
    let Some((first_idx, last_idx, step)) = self.get_key_search_args_slice(args, is_sub_command)
    else {
      return;
    };
    for i in (first_idx..=last_idx).step_by(step) {
      if let Some(&bytes) = args.get(i)
        && !bytes.is_empty()
      {
        sink(bytes, i);
      }
    }
  }

  /// 从参数切片中提取本规格键切片（按下标升序，零拷贝）
  #[inline]
  pub fn extract_keys<'a>(
    &self,
    args: &[&'a [u8]],
    is_sub_command: bool,
  ) -> SmallVec<[&'a [u8]; INLINE_KEYS]> {
    let mut keys = SmallVec::new();
    self.scan_keys(args, is_sub_command, |key, _| keys.push(key));
    keys
  }

  /// 从参数切片中提取本规格键切片与标记（按下标升序，零拷贝）
  #[inline]
  pub fn extract_keys_and_flags<'a>(
    &self,
    args: &[&'a [u8]],
    is_sub_command: bool,
  ) -> SmallVec<[(&'a [u8], u8); INLINE_KEYS]> {
    let flags = self.flags.bits() as u8;
    let mut keys = SmallVec::new();
    self.scan_keys(args, is_sub_command, |key, _| keys.push((key, flags)));
    keys
  }

  /// 从参数切片中提取多规格键切片（关联方法，按下标升序，零拷贝）
  #[inline]
  pub fn extract_keys_from_slice<'a>(
    args: &[&'a [u8]],
    key_specs: &[Self],
    is_sub_command: bool,
  ) -> SmallVec<[&'a [u8]; INLINE_KEYS]> {
    extract_keys_from_slice(args, key_specs, is_sub_command)
  }

  /// 从参数切片中提取多规格键切片与标记（关联方法，按下标升序，零拷贝）
  #[inline]
  pub fn extract_keys_and_flags_from_slice<'a>(
    args: &[&'a [u8]],
    key_specs: &[Self],
    is_sub_command: bool,
  ) -> SmallVec<[(&'a [u8], u8); INLINE_KEYS]> {
    extract_keys_and_flags_from_slice(args, key_specs, is_sub_command)
  }
}

/// 从参数切片中提取键切片（按下标升序，零拷贝）
///
/// 单规格下标天然升序直推结果；多规格按下标交织，需带下标收集排序后折叠；
/// 内联容量内零堆分配（消除临时向量 + 二次 collect 的双重分配）
pub fn extract_keys_from_slice<'a>(
  args: &[&'a [u8]],
  key_specs: &[SimpleRespKeySpec],
  is_sub_command: bool,
) -> SmallVec<[&'a [u8]; INLINE_KEYS]> {
  match key_specs {
    [] => SmallVec::new(),
    [spec] => spec.extract_keys(args, is_sub_command),
    specs => {
      let mut keyed = SmallVec::<[(&'a [u8], usize); INLINE_KEYS]>::new();
      for spec in specs {
        spec.scan_keys(args, is_sub_command, |key, idx| {
          keyed.push((key, idx));
        });
      }
      keyed.sort_unstable_by_key(|&(_, idx)| idx);
      keyed.into_iter().map(|(key, _)| key).collect()
    }
  }
}

/// 从参数切片中提取键切片与标记（按下标升序，零拷贝）
///
/// 单规格标记恒定直推结果；多规格带下标收集排序后折叠；内联容量内零堆分配
pub fn extract_keys_and_flags_from_slice<'a>(
  args: &[&'a [u8]],
  key_specs: &[SimpleRespKeySpec],
  is_sub_command: bool,
) -> SmallVec<[(&'a [u8], u8); INLINE_KEYS]> {
  match key_specs {
    [] => SmallVec::new(),
    [spec] => spec.extract_keys_and_flags(args, is_sub_command),
    specs => {
      let mut keyed = SmallVec::<[(&'a [u8], u8, usize); INLINE_KEYS]>::new();
      for spec in specs {
        let flags = spec.flags.bits() as u8;
        spec.scan_keys(args, is_sub_command, |key, idx| {
          keyed.push((key, flags, idx));
        });
      }
      keyed.sort_unstable_by_key(|&(_, _, idx)| idx);
      keyed
        .into_iter()
        .map(|(key, flags, _)| (key, flags))
        .collect()
    }
  }
}

/// 把完整命令信息折叠为简化结构
///
/// libs/server/Resp/RespCommandInfoSimplifiedStructs.cs:PopulateSimpleCommandInfo
pub fn populate_simple_command_info(
  cmd_info: &RespCommandsInfo,
  simple_cmd_info: &mut SimpleRespCommandInfo,
) {
  simple_cmd_info.arity = cmd_info.arity as i8;
  simple_cmd_info.allowed_in_txn = !cmd_info.flags.intersects(RespCommandFlags::NO_MULTI);
  simple_cmd_info.is_parent = !cmd_info.sub_commands.is_empty();
  simple_cmd_info.is_sub_command = cmd_info.is_sub_command;
  simple_cmd_info.store_type = cmd_info.store_type;

  simple_cmd_info.key_specs.clear();
  simple_cmd_info.key_specs.extend(
    cmd_info
      .key_specifications
      .iter()
      .filter_map(try_get_simple_key_spec),
  );
}

/// 把键规格折叠为简化结构
///
/// libs/server/Resp/RespCommandInfoSimplifiedStructs.cs:TryGetSimpleKeySpec
pub fn try_get_simple_key_spec(
  key_spec: &RespCommandKeySpecification,
) -> Option<SimpleRespKeySpec> {
  let begin_search = match key_spec.begin_search.as_ref()? {
    BeginSearchMethod::Index(index) => SimpleRespKeySpecBeginSearch {
      keyword: Vec::new(),
      index: *index,
      is_index_type: true,
    },
    BeginSearchMethod::Keyword {
      keyword,
      start_from,
    } => SimpleRespKeySpecBeginSearch {
      keyword: keyword.as_bytes().to_vec(),
      index: *start_from,
      is_index_type: false,
    },
    BeginSearchMethod::Unknown => return None,
  };

  let find_keys = match key_spec.find_keys.as_ref()? {
    FindKeysMethod::Range {
      last_key,
      key_step,
      limit,
    } => {
      let is_limit = *last_key == -1;
      SimpleRespKeySpecFindKeys {
        key_num_index: 0,
        first_key: 0,
        last_key_or_limit: if is_limit { *limit } else { *last_key },
        key_step: *key_step,
        is_range_type: true,
        is_range_limit_type: is_limit,
      }
    }
    FindKeysMethod::KeyNum {
      key_num_idx,
      first_key,
      key_step,
    } => SimpleRespKeySpecFindKeys {
      key_num_index: *key_num_idx,
      first_key: *first_key,
      last_key_or_limit: 0,
      key_step: *key_step,
      is_range_type: false,
      is_range_limit_type: false,
    },
    FindKeysMethod::Unknown => return None,
  };

  Some(SimpleRespKeySpec {
    begin_search,
    find_keys,
    flags: key_spec.flags,
  })
}

#[cfg(test)]
mod tests {
  use std::slice::from_ref;

  use super::*;
  use crate::{catalog::RespAclCategories, command::RespCommand};

  /// 完整信息折叠（GET 形态：fast + readonly，index 键规格）
  #[test]
  fn populate_get_shape() {
    let info = RespCommandsInfo {
      command: RespCommand::Get,
      name: "GET",
      is_internal: false,
      arity: 2,
      flags: RespCommandFlags::from_member_names("Fast, ReadOnly").unwrap(),
      first_key: 1,
      last_key: 1,
      step: 1,
      acl_categories: RespAclCategories::READ | RespAclCategories::FAST,
      tips: Vec::new(),
      key_specifications: vec![RespCommandKeySpecification {
        begin_search: Some(BeginSearchMethod::Index(1)),
        find_keys: Some(FindKeysMethod::Range {
          last_key: 0,
          key_step: 1,
          limit: 0,
        }),
        notes: None,
        flags: KeySpecificationFlags::from_wire_names("RO,access").unwrap(),
      }],
      store_type: StoreType::Main,
      sub_commands: Vec::new(),
      is_sub_command: false,
      parent_is_internal: false,
    };

    let mut simple = SimpleRespCommandInfo::default();
    populate_simple_command_info(&info, &mut simple);
    assert_eq!(simple.arity, 2);
    assert!(simple.allowed_in_txn);
    assert!(!simple.is_parent);
    assert!(!simple.is_sub_command);
    assert_eq!(simple.store_type, StoreType::Main);

    let ks = &simple.key_specs[0];
    assert!(ks.begin_search.is_index_type);
    assert_eq!(ks.begin_search.index, 1);
    assert!(ks.find_keys.is_range_type);
    assert_eq!(ks.find_keys.last_key_or_limit, 0);

    // 集群槽位提取路径（参数表不含命令名）
    let args = [b"key".as_slice(), b"v1"];
    assert_eq!(ks.get_key_search_args_slice(&args, false), Some((0, 0, 1)));
  }

  /// keyword 型 begin_search 折叠（XAUTOCLAIM 式）
  #[test]
  fn simple_key_spec_keyword_form() {
    let spec = RespCommandKeySpecification {
      begin_search: Some(BeginSearchMethod::Keyword {
        keyword: "FROM".to_string(),
        start_from: 2,
      }),
      find_keys: Some(FindKeysMethod::KeyNum {
        key_num_idx: 1,
        first_key: 0,
        key_step: 1,
      }),
      notes: None,
      flags: KeySpecificationFlags::empty(),
    };
    let ks = try_get_simple_key_spec(&spec).unwrap();
    assert!(!ks.begin_search.is_index_type);
    assert_eq!(ks.begin_search.keyword, b"FROM".to_vec());
    assert_eq!(ks.begin_search.index, 2);
    assert!(!ks.find_keys.is_range_type);
    assert_eq!(ks.find_keys.key_num_index, 1);

    // Unknown 方法不可折叠
    let unknown = RespCommandKeySpecification {
      begin_search: Some(BeginSearchMethod::Unknown),
      find_keys: None,
      notes: None,
      flags: KeySpecificationFlags::empty(),
    };
    assert!(try_get_simple_key_spec(&unknown).is_none());
  }

  /// COMMAND GETKEYS ZUNION 2147483647 zset1 模式：大 keynum 截断至 count - 1
  #[test]
  fn simple_key_spec_numkeys_clamped() {
    let spec = RespCommandKeySpecification {
      begin_search: Some(BeginSearchMethod::Index(1)),
      find_keys: Some(FindKeysMethod::KeyNum {
        key_num_idx: 0,
        first_key: 1,
        key_step: 1,
      }),
      notes: None,
      flags: KeySpecificationFlags::empty(),
    };
    let ks = try_get_simple_key_spec(&spec).unwrap();
    // 模拟参数列表：["2147483647", "zset1"]（不含命令名时，begin_search_idx 0 指向 "2147483647"）
    let args = [b"2147483647".as_slice(), b"zset1"];
    let (first, last, step) = ks.get_key_search_args_slice(&args, false).unwrap();
    assert_eq!(first, 1);
    assert_eq!(last, 1);
    assert_eq!(step, 1);
  }

  /// keynum 型规格（numkeys 位于下标 0，首键下标 1）
  fn keynum_spec(key_step: i32) -> SimpleRespKeySpec {
    SimpleRespKeySpec {
      begin_search: SimpleRespKeySpecBeginSearch {
        index: 1,
        is_index_type: true,
        keyword: Vec::new(),
      },
      find_keys: SimpleRespKeySpecFindKeys {
        key_num_index: 0,
        first_key: 1,
        key_step,
        is_range_type: false,
        ..Default::default()
      },
      flags: KeySpecificationFlags::RO,
    }
  }

  #[test]
  fn extract_keys_range_slice() {
    let args: &[&[u8]] = &[b"k1", b"v1", b"k2", b"v2"];
    let spec = SimpleRespKeySpec {
      begin_search: SimpleRespKeySpecBeginSearch {
        index: 1,
        is_index_type: true,
        keyword: Vec::new(),
      },
      find_keys: SimpleRespKeySpecFindKeys {
        key_step: 2,
        is_range_type: true,
        last_key_or_limit: -1,
        ..Default::default()
      },
      flags: KeySpecificationFlags::RW,
    };
    let keys = extract_keys_from_slice(args, from_ref(&spec), false);
    assert_eq!(keys.as_slice(), &[&b"k1"[..], &b"k2"[..]]);

    let pairs = extract_keys_and_flags_from_slice(args, &[spec], false);
    let rw = KeySpecificationFlags::RW.bits() as u8;
    assert_eq!(pairs.as_slice(), &[(&b"k1"[..], rw), (&b"k2"[..], rw)]);
  }

  #[test]
  fn extract_keys_keynum_slice() {
    let args: &[&[u8]] = &[b"3", b"k1", b"k2", b"k3"];
    let keys = extract_keys_from_slice(args, from_ref(&keynum_spec(1)), false);
    assert_eq!(keys.as_slice(), &[&b"k1"[..], &b"k2"[..], &b"k3"[..]]);
  }

  #[test]
  fn extract_keys_keyword_slice() {
    let args: &[&[u8]] = &[b"KEY", b"k1", b"k2"];
    let spec = SimpleRespKeySpec {
      begin_search: SimpleRespKeySpecBeginSearch {
        index: 1,
        is_index_type: false,
        keyword: b"KEY".to_vec(),
      },
      find_keys: SimpleRespKeySpecFindKeys {
        key_step: 1,
        is_range_type: true,
        last_key_or_limit: 1,
        ..Default::default()
      },
      flags: KeySpecificationFlags::RW,
    };
    let pairs = extract_keys_and_flags_from_slice(args, from_ref(&spec), false);
    let rw = KeySpecificationFlags::RW.bits() as u8;
    assert_eq!(pairs.as_slice(), &[(&b"k1"[..], rw), (&b"k2"[..], rw)]);
  }

  #[test]
  fn extract_keys_reverse_keyword_scan_terminates() {
    let args: &[&[u8]] = &[b"KEY", b"k1", b"k2"];
    let miss = SimpleRespKeySpec {
      begin_search: SimpleRespKeySpecBeginSearch {
        index: -1,
        is_index_type: false,
        keyword: b"MISSING".to_vec(),
      },
      find_keys: SimpleRespKeySpecFindKeys {
        key_step: 1,
        is_range_type: true,
        last_key_or_limit: 1,
        ..Default::default()
      },
      flags: KeySpecificationFlags::empty(),
    };
    assert!(extract_keys_from_slice(args, &[miss], false).is_empty());

    let hit = SimpleRespKeySpec {
      begin_search: SimpleRespKeySpecBeginSearch {
        index: -2,
        is_index_type: false,
        keyword: b"KEY".to_vec(),
      },
      find_keys: SimpleRespKeySpecFindKeys {
        key_step: 1,
        is_range_type: true,
        last_key_or_limit: 1,
        ..Default::default()
      },
      flags: KeySpecificationFlags::empty(),
    };
    let keys = extract_keys_from_slice(args, &[hit], false);
    assert_eq!(keys.as_slice(), &[&b"k1"[..], &b"k2"[..]]);
  }

  #[test]
  fn extract_keys_malicious_numkeys_clamped() {
    let args: &[&[u8]] = &[b"2147483647", b"k1", b"k2"];
    let keys = extract_keys_from_slice(args, from_ref(&keynum_spec(1)), false);
    assert_eq!(keys.as_slice(), &[&b"k1"[..], &b"k2"[..]]);

    let keys = extract_keys_from_slice(args, from_ref(&keynum_spec(2)), false);
    assert_eq!(keys.as_slice(), &[&b"k1"[..]]);

    let ro = KeySpecificationFlags::RO.bits() as u8;
    let pairs = extract_keys_and_flags_from_slice(args, from_ref(&keynum_spec(1)), false);
    assert_eq!(pairs.as_slice(), &[(&b"k1"[..], ro), (&b"k2"[..], ro)]);
  }

  #[test]
  fn extract_keys_malicious_numkeys_rejected() {
    for raw in ["2147483648", "-2147483648", "0", "abc", "3.5", ""] {
      let args: &[&[u8]] = &[raw.as_bytes(), b"k1"];
      assert!(
        extract_keys_from_slice(args, from_ref(&keynum_spec(1)), false).is_empty(),
        "numkeys={raw}",
      );
    }
  }

  #[test]
  fn extract_keys_empty_specs() {
    let args: &[&[u8]] = &[b"k1", b"k2"];
    assert!(extract_keys_from_slice(args, &[], false).is_empty());
    assert!(extract_keys_and_flags_from_slice(args, &[], false).is_empty());
  }

  #[test]
  fn extract_keys_multi_specs_sorted() {
    let args: &[&[u8]] = &[b"k1", b"v1", b"k2", b"v2"];
    let spec1 = SimpleRespKeySpec {
      begin_search: SimpleRespKeySpecBeginSearch {
        index: 3,
        is_index_type: true,
        keyword: Vec::new(),
      },
      find_keys: SimpleRespKeySpecFindKeys {
        key_step: 1,
        is_range_type: true,
        last_key_or_limit: 0,
        ..Default::default()
      },
      flags: KeySpecificationFlags::RO,
    };
    let spec2 = SimpleRespKeySpec {
      begin_search: SimpleRespKeySpecBeginSearch {
        index: 1,
        is_index_type: true,
        keyword: Vec::new(),
      },
      find_keys: SimpleRespKeySpecFindKeys {
        key_step: 1,
        is_range_type: true,
        last_key_or_limit: 0,
        ..Default::default()
      },
      flags: KeySpecificationFlags::RW,
    };
    let keys = extract_keys_from_slice(args, &[spec1.clone(), spec2.clone()], false);
    assert_eq!(keys.as_slice(), &[&b"k1"[..], &b"k2"[..]]);

    let pairs = extract_keys_and_flags_from_slice(args, &[spec1, spec2], false);
    let rw = KeySpecificationFlags::RW.bits() as u8;
    let ro = KeySpecificationFlags::RO.bits() as u8;
    assert_eq!(pairs.as_slice(), &[(&b"k1"[..], rw), (&b"k2"[..], ro)]);
  }
}
