//! 简化命令信息结构（对标 libs/server/Resp/RespCommandInfoSimplifiedStructs.cs）
//!
//! C# 以 fixed 布局 struct 承接会话快速路径（SimpleRespCommandInfo /
//! SimpleRespKeySpec*）；Rust 侧为纯数据结构，键规格以下标寻址。

use super::{
  resp_command_key_specification::{
    BeginSearchMethod, FindKeysMethod, KeySpecificationFlags, RespCommandKeySpecification,
  },
  resp_commands_info::{RespCommandFlags, RespCommandsInfo, StoreType},
};

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

/// 简化版 begin_search 规格（C# SimpleRespKeySpecBeginSearch）
#[derive(Debug, Clone, Default)]
pub struct SimpleRespKeySpecBeginSearch {
  /// 键前置关键字（C# Keyword；index 型为空）
  pub keyword: Vec<u8>,
  /// 键下标或关键字检索起点（C# Index）
  pub index: i32,
  /// true = index 型，否则 keyword 型（C# IsIndexType）
  pub is_index_type: bool,
}

/// 简化版 find_keys 规格（C# SimpleRespKeySpecFindKeys 的 union 面平铺）
#[derive(Debug, Clone, Default)]
pub struct SimpleRespKeySpecFindKeys {
  /// 键数量参数下标（keynum 型；C# KeyNumIndex）
  pub key_num_index: i32,
  /// 首键下标（keynum 型；C# FirstKey）
  pub first_key: i32,
  /// 末键下标或 limit（range 型；C# LastKeyOrLimit）
  pub last_key_or_limit: i32,
  /// 找到一键后跳过的参数个数（C# KeyStep）
  pub key_step: i32,
  /// true = range 型，否则 keynum 型（C# IsRangeType）
  pub is_range_type: bool,
  /// true = range 且按 limit 截断（C# IsRangeLimitType）
  pub is_range_limit_type: bool,
}

/// 简化版单条键规格（C# SimpleRespKeySpec）
#[derive(Debug, Clone, Default)]
pub struct SimpleRespKeySpec {
  /// begin_search 规格（C# BeginSearch）
  pub begin_search: SimpleRespKeySpecBeginSearch,
  /// find_keys 规格（C# FindKeys）
  pub find_keys: SimpleRespKeySpecFindKeys,
  /// 键规格标记（C# Flags）
  pub flags: KeySpecificationFlags,
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

  let mut tmp_simple_key_specs = Vec::new();
  for key_spec in &cmd_info.key_specifications {
    if let Some(simple_key_spec) = try_get_simple_key_spec(key_spec) {
      tmp_simple_key_specs.push(simple_key_spec);
    }
  }
  simple_cmd_info.key_specs = tmp_simple_key_specs;
}

/// 把键规格折叠为简化结构
///
/// libs/server/Resp/RespCommandInfoSimplifiedStructs.cs:TryGetSimpleKeySpec
pub fn try_get_simple_key_spec(
  key_spec: &RespCommandKeySpecification,
) -> Option<SimpleRespKeySpec> {
  // 任一侧未知即不可折叠
  if matches!(key_spec.begin_search, Some(BeginSearchMethod::Unknown))
    || matches!(key_spec.find_keys, Some(FindKeysMethod::Unknown))
  {
    return None;
  }

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
      if *last_key == -1 {
        SimpleRespKeySpecFindKeys {
          key_num_index: 0,
          first_key: 0,
          last_key_or_limit: *limit,
          key_step: *key_step,
          is_range_type: true,
          is_range_limit_type: true,
        }
      } else {
        SimpleRespKeySpecFindKeys {
          key_num_index: 0,
          first_key: 0,
          last_key_or_limit: *last_key,
          key_step: *key_step,
          is_range_type: true,
          is_range_limit_type: false,
        }
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
