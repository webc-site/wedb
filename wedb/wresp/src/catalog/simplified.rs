//! 简化命令信息结构（对标 libs/server/Resp/RespCommandInfoSimplifiedStructs.cs）
//!
//! C# 以 fixed 布局 struct 承接会话快速路径（SimpleRespCommandInfo /
//! SimpleRespKeySpec*）；Rust 侧为纯数据结构，键规格以下标寻址。
//!
//! 自研依据: 命令目录简表（编译期 phf 面）

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
      // 双闸之一（严禁回改）：numkeys<=0 空键区显式早拒。C# keynum 臂对负偏无
      // 护栏，直回 (firstIdx, firstIdx-1, step) 后被集群槽校验核无条件当必存在
      // 键首取，读同会话参数数组残留槽定槽（live parseState 不含命令名，归残槽
      // 族）；rust 保留无键放行形，裁决登记 doc/zh/deviations.md §149，锁测本文
      // 件 tests::numkeys_zero_keynum_spec_yields_no_keys
      if key_num <= 0 {
        return None;
      }
      first_key_idx += self.find_keys.first_key as isize;
      last_key_idx = first_key_idx.saturating_add((key_num as isize - 1).saturating_mul(key_step));
    }

    if last_key_idx >= count {
      last_key_idx = count - 1;
    }

    // 双闸之二（严禁回改）：空区末端兜底，first > last 即本规格不命中。与上闸
    // 同一机制双保险（§149）；按 C# 形摘除本闸即把假键定槽接进本仓
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
  /// 已由 wresp 侧钳制在参数表界内（numkeys 溢出安全，PR #2112 语义；C# 对位
  /// unchecked int 加法回绕空回 *0 的分叉裁决见 doc/zh/deviations.md §106 宗 b，
  /// bs=2 溢出形锁测 wresp/tests/getkeys_keynum_bs2_saturating_clamp.rs，严禁回改；
  /// numkeys=0 空键区经双闸短路本口直出空键区，C# 槽校验核首键直取残槽定槽之
  /// 分叉裁决见 §149，严禁按该形回改上两闸）
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
  ///
  /// 标记直传 16 位位图（对标 C# `(PinnedSpanByte, KeySpecificationFlags)[]`），
  /// 高位标记 NOT_KEY / INCOMPLETE / VARIABLE_FLAGS 不得降级截断
  #[inline]
  pub fn extract_keys_and_flags<'a>(
    &self,
    args: &[&'a [u8]],
    is_sub_command: bool,
  ) -> SmallVec<[(&'a [u8], KeySpecificationFlags); INLINE_KEYS]> {
    let flags = self.flags;
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
  ) -> SmallVec<[(&'a [u8], KeySpecificationFlags); INLINE_KEYS]> {
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
/// 单规格标记恒定直推结果；多规格带下标收集排序后折叠；内联容量内零堆分配；
/// 标记全程保持 16 位位图精度（对标 C# TryAppendKeysAndFlagsFromSpec）
pub fn extract_keys_and_flags_from_slice<'a>(
  args: &[&'a [u8]],
  key_specs: &[SimpleRespKeySpec],
  is_sub_command: bool,
) -> SmallVec<[(&'a [u8], KeySpecificationFlags); INLINE_KEYS]> {
  match key_specs {
    [] => SmallVec::new(),
    [spec] => spec.extract_keys_and_flags(args, is_sub_command),
    specs => {
      let mut keyed = SmallVec::<[(&'a [u8], KeySpecificationFlags, usize); INLINE_KEYS]>::new();
      for spec in specs {
        let flags = spec.flags;
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
