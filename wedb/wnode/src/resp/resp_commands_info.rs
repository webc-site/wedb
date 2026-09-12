//! 命令元数据表（对标 libs/server/Resp/RespCommandsInfo.cs）
//!
//! C# 于 Garnet.resources 程序集内嵌 `RespCommandsInfo.json`，首次访问时
//! 反序列化并构建多张静态索引（全量 / 外部 / 按枚举扁平 / ACL 分类 / 简化
//! 结构 / 快速数组）；Rust 侧以 `include_str!` 内嵌同一份 JSON，sonic-rs
//! 反序列化后构建 `OnceLock` 静态表，语义一一对应。

use std::sync::OnceLock;

use gxhash::{GxBuildHasher, HashMap, HashSet};
use serde::Serialize;
use sonic_rs::Deserialize;
use wacl::RespAclCategories;

use super::{
  i_resp_serializable::IRespSerializable,
  resp_command_data_common::try_import_resp_commands_data,
  resp_command_data_provider::IRespCommandData,
  resp_command_info_simplified_structs::{SimpleRespCommandInfo, populate_simple_command_info},
  resp_command_key_specification::{
    BeginSearchMethod, FindKeysMethod, KeySpecificationFlags, RespCommandKeySpecification,
  },
  resp_commands_info_data::{
    FIRST_DATA_COMMAND, LAST_DATA_COMMAND, LAST_VALID_COMMAND, resp_command_from_cs_name,
  },
  resp_memory_writer::RespMemoryWriter,
};
use crate::types::RespCommand;

/// 内嵌命令元数据（C# Garnet.resources:RespCommandsInfo.json）
const RESP_COMMANDS_INFO_JSON: &str = include_str!("RespCommandsInfo.json");

/// 未知命令名（C# UnknownCommandName）
const UNKNOWN_COMMAND_NAME: &str = "UNKNOWN";

/// RESP 命令标记（C# RespCommandFlags；声明序即 wire 顺序）
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RespCommandFlags(pub u32);

impl RespCommandFlags {
  /// admin
  pub const ADMIN: Self = Self(1);
  /// asking
  pub const ASKING: Self = Self(1 << 1);
  /// blocking
  pub const BLOCKING: Self = Self(1 << 2);
  /// denyoom
  pub const DENY_OOM: Self = Self(1 << 3);
  /// fast
  pub const FAST: Self = Self(1 << 4);
  /// loading
  pub const LOADING: Self = Self(1 << 5);
  /// movablekeys
  pub const MOVABLE_KEYS: Self = Self(1 << 6);
  /// no_auth
  pub const NO_AUTH: Self = Self(1 << 7);
  /// no_async_loading
  pub const NO_ASYNC_LOADING: Self = Self(1 << 8);
  /// no_mandatory_keys
  pub const NO_MANDATORY_KEYS: Self = Self(1 << 9);
  /// no_multi
  pub const NO_MULTI: Self = Self(1 << 10);
  /// noscript
  pub const NO_SCRIPT: Self = Self(1 << 11);
  /// pubsub
  pub const PUB_SUB: Self = Self(1 << 12);
  /// random
  pub const RANDOM: Self = Self(1 << 13);
  /// readonly
  pub const READ_ONLY: Self = Self(1 << 14);
  /// sort_for_script
  pub const SORT_FOR_SCRIPT: Self = Self(1 << 15);
  /// skip_monitor
  pub const SKIP_MONITOR: Self = Self(1 << 16);
  /// skip_slowlog
  pub const SKIP_SLOW_LOG: Self = Self(1 << 17);
  /// stale
  pub const STALE: Self = Self(1 << 18);
  /// write
  pub const WRITE: Self = Self(1 << 19);
  /// allow_busy
  pub const ALLOW_BUSY: Self = Self(1 << 20);
  /// module
  pub const MODULE: Self = Self(1 << 21);

  /// 空集
  #[inline]
  pub const fn empty() -> Self {
    Self(0)
  }

  /// 是否无标记
  #[inline]
  pub fn is_empty(&self) -> bool {
    self.0 == 0
  }

  /// 与给定标记有交集
  #[inline]
  pub fn intersects(&self, other: Self) -> bool {
    self.0 & other.0 != 0
  }

  /// 标记位 / 成员名 / wire 描述对照（C# 枚举成员与 Description 特性）
  const TABLE: [(u32, &'static str, &'static str); 22] = [
    (Self::ADMIN.0, "Admin", "admin"),
    (Self::ASKING.0, "Asking", "asking"),
    (Self::BLOCKING.0, "Blocking", "blocking"),
    (Self::DENY_OOM.0, "DenyOom", "denyoom"),
    (Self::FAST.0, "Fast", "fast"),
    (Self::LOADING.0, "Loading", "loading"),
    (Self::MOVABLE_KEYS.0, "MovableKeys", "movablekeys"),
    (Self::NO_AUTH.0, "NoAuth", "no_auth"),
    (
      Self::NO_ASYNC_LOADING.0,
      "NoAsyncLoading",
      "no_async_loading",
    ),
    (
      Self::NO_MANDATORY_KEYS.0,
      "NoMandatoryKeys",
      "no_mandatory_keys",
    ),
    (Self::NO_MULTI.0, "NoMulti", "no_multi"),
    (Self::NO_SCRIPT.0, "NoScript", "noscript"),
    (Self::PUB_SUB.0, "PubSub", "pubsub"),
    (Self::RANDOM.0, "Random", "random"),
    (Self::READ_ONLY.0, "ReadOnly", "readonly"),
    (Self::SORT_FOR_SCRIPT.0, "SortForScript", "sort_for_script"),
    (Self::SKIP_MONITOR.0, "SkipMonitor", "skip_monitor"),
    (Self::SKIP_SLOW_LOG.0, "SkipSlowLog", "skip_slowlog"),
    (Self::STALE.0, "Stale", "stale"),
    (Self::WRITE.0, "Write", "write"),
    (Self::ALLOW_BUSY.0, "AllowBusy", "allow_busy"),
    (Self::MODULE.0, "Module", "module"),
  ];

  /// wire 描述（C# EnumUtils.GetEnumDescriptions）
  pub fn descriptions(&self) -> Vec<&'static str> {
    Self::TABLE
      .iter()
      .filter(|(bit, ..)| self.0 & bit != 0)
      .map(|(_, _, desc)| *desc)
      .collect()
  }

  /// C# 枚举成员名（导出 JSON 用）
  pub fn member_names(&self) -> Vec<&'static str> {
    Self::TABLE
      .iter()
      .filter(|(bit, ..)| self.0 & bit != 0)
      .map(|(_, name, _)| *name)
      .collect()
  }

  /// 按成员名串解析（大小写不敏感；C# JsonStringEnumConverter 语义）
  pub fn from_member_names(names: &str) -> Option<Self> {
    let mut out = Self::empty();
    for name in names.split(',') {
      let trimmed = name.trim().to_ascii_uppercase();
      let bit = Self::TABLE
        .iter()
        .find(|(_, member, _)| member.to_ascii_uppercase() == trimmed)
        .map(|(bit, ..)| *bit)?;
      out.0 |= bit;
    }
    Some(out)
  }
}

pub use crate::types::StoreType;

/// ACL 分类的 wire 描述（C# RespAclCategories Description；声明序）
///
/// libs/server/Resp/RespCommandInfoFlags.cs:RespAclCategories
pub(crate) fn acl_category_descriptions(cats: RespAclCategories) -> Vec<&'static str> {
  const ALL: [(u32, &str); 24] = [
    (RespAclCategories::ADMIN.bits(), "admin"),
    (RespAclCategories::BITMAP.bits(), "bitmap"),
    (RespAclCategories::BLOCKING.bits(), "blocking"),
    (RespAclCategories::CONNECTION.bits(), "connection"),
    (RespAclCategories::DANGEROUS.bits(), "dangerous"),
    (RespAclCategories::GEO.bits(), "geo"),
    (RespAclCategories::HASH.bits(), "hash"),
    (RespAclCategories::HYPERLOGLOG.bits(), "hyperloglog"),
    (RespAclCategories::FAST.bits(), "fast"),
    (RespAclCategories::KEYSPACE.bits(), "keyspace"),
    (RespAclCategories::LIST.bits(), "list"),
    (RespAclCategories::PUBSUB.bits(), "pubsub"),
    (RespAclCategories::READ.bits(), "read"),
    (RespAclCategories::SCRIPTING.bits(), "scripting"),
    (RespAclCategories::SET.bits(), "set"),
    (RespAclCategories::SORTEDSET.bits(), "sortedset"),
    (RespAclCategories::SLOW.bits(), "slow"),
    (RespAclCategories::STREAM.bits(), "stream"),
    (RespAclCategories::STRING.bits(), "string"),
    (RespAclCategories::TRANSACTION.bits(), "transaction"),
    (RespAclCategories::WRITE.bits(), "write"),
    (RespAclCategories::GARNET.bits(), "garnet"),
    (RespAclCategories::CUSTOM.bits(), "custom"),
    (RespAclCategories::VECTOR.bits(), "vector"),
  ];
  ALL
    .iter()
    .filter(|(bit, _)| cats.bits() & bit != 0)
    .map(|(_, desc)| *desc)
    .collect()
}

/// ACL 分类的 C# 枚举成员名（导出 JSON 面；声明序）
#[cfg(test)]
pub(crate) fn acl_category_member_names(cats: RespAclCategories) -> Vec<&'static str> {
  const ALL: [(u32, &str); 24] = [
    (1, "Admin"),
    (1 << 1, "Bitmap"),
    (1 << 2, "Blocking"),
    (1 << 3, "Connection"),
    (1 << 4, "Dangerous"),
    (1 << 5, "Geo"),
    (1 << 6, "Hash"),
    (1 << 7, "HyperLogLog"),
    (1 << 8, "Fast"),
    (1 << 9, "KeySpace"),
    (1 << 10, "List"),
    (1 << 11, "PubSub"),
    (1 << 12, "Read"),
    (1 << 13, "Scripting"),
    (1 << 14, "Set"),
    (1 << 15, "SortedSet"),
    (1 << 16, "Slow"),
    (1 << 17, "Stream"),
    (1 << 18, "String"),
    (1 << 19, "Transaction"),
    (1 << 20, "Write"),
    (1 << 21, "Garnet"),
    (1 << 22, "Custom"),
    (1 << 23, "Vector"),
  ];
  ALL
    .iter()
    .filter(|(bit, _)| cats.bits() & bit != 0)
    .map(|(_, name)| *name)
    .collect()
}

/// ACL 分类成员名串解析（"Fast, String, Write"；大小写不敏感）
pub(crate) fn acl_categories_from_member_names(names: &str) -> Option<RespAclCategories> {
  const ALL: [(&str, u32); 24] = [
    ("ADMIN", 1),
    ("BITMAP", 1 << 1),
    ("BLOCKING", 1 << 2),
    ("CONNECTION", 1 << 3),
    ("DANGEROUS", 1 << 4),
    ("GEO", 1 << 5),
    ("HASH", 1 << 6),
    ("HYPERLOGLOG", 1 << 7),
    ("FAST", 1 << 8),
    ("KEYSPACE", 1 << 9),
    ("LIST", 1 << 10),
    ("PUBSUB", 1 << 11),
    ("READ", 1 << 12),
    ("SCRIPTING", 1 << 13),
    ("SET", 1 << 14),
    ("SORTEDSET", 1 << 15),
    ("SLOW", 1 << 16),
    ("STREAM", 1 << 17),
    ("STRING", 1 << 18),
    ("TRANSACTION", 1 << 19),
    ("WRITE", 1 << 20),
    ("GARNET", 1 << 21),
    ("CUSTOM", 1 << 22),
    ("VECTOR", 1 << 23),
  ];
  let mut bits = 0u32;
  for name in names.split(',') {
    let trimmed = name.trim().to_ascii_uppercase();
    let bit = ALL
      .iter()
      .find(|(member, _)| *member == trimmed)
      .map(|(_, bit)| *bit)?;
    bits |= bit;
  }
  Some(RespAclCategories::from_bits_retain(bits))
}

/// 一条 RESP 命令的元数据（C# RespCommandsInfo）
#[derive(Debug, Clone)]
pub struct RespCommandsInfo {
  /// 命令枚举（C# Command）
  pub command: RespCommand,
  /// 命令名（子命令为 `ACL|CAT` 形式）
  pub name: String,
  /// 是否内部命令（不对客户端暴露）
  pub is_internal: bool,
  /// arity：正数 = 固定参数个数；负数 = 最少参数个数
  pub arity: i32,
  /// 命令标记（C# Flags）
  pub flags: RespCommandFlags,
  /// 首个键名参数位置（C# FirstKey）
  pub first_key: i32,
  /// 末个键名参数位置（C# LastKey）
  pub last_key: i32,
  /// 键步进（C# Step）
  pub step: i32,
  /// ACL 分类位集（C# AclCategories）
  pub acl_categories: RespAclCategories,
  /// 提示信息（C# Tips）
  pub tips: Vec<String>,
  /// 键定位规则（C# KeySpecifications）
  pub key_specifications: Vec<RespCommandKeySpecification>,
  /// 作用存储类型（C# StoreType）
  pub store_type: StoreType,
  /// 子命令（C# SubCommands）
  pub sub_commands: Vec<RespCommandsInfo>,
  /// 是否为子命令（C# Parent != null 的投影）
  pub is_sub_command: bool,
  /// 父命令是否内部命令（C# Parent.IsInternal 的导入期投影）
  pub parent_is_internal: bool,
}

impl RespCommandsInfo {
  /// 按命令名查找命令元数据（对标 C# RespCommandsInfo.TryGetRespCommandInfo）
  pub fn try_get_resp_command_info(name: &str) -> Option<&'static Self> {
    try_get_resp_command_info_by_name(name, false, true)
  }
}

impl RespCommandsInfo {
  /// 序列化为 RESP 格式
  ///
  /// libs/server/Resp/RespCommandsInfo.cs:ToRespFormat
  pub fn to_resp_format(&self, writer: &mut RespMemoryWriter) {
    if self.name.trim().is_empty() {
      writer.write_null();
      return;
    }

    writer.write_array_length(10);
    // 1) Name
    writer.write_ascii_bulk_string(&self.name);
    // 2) Arity
    writer.write_int32(self.arity);
    // 3) Flags
    let resp_format_flags = self.flags.descriptions();
    writer.write_set_length(resp_format_flags.len());
    for flag in resp_format_flags {
      writer.write_simple_string(flag);
    }
    // 4) First key
    writer.write_int32(self.first_key);
    // 5) Last key
    writer.write_int32(self.last_key);
    // 6) Step
    writer.write_int32(self.step);
    // 7) ACL categories
    let resp_format_acl_categories = acl_category_descriptions(self.acl_categories);
    writer.write_set_length(resp_format_acl_categories.len());
    for acl_cat in resp_format_acl_categories {
      writer.write_simple_string(&format!("@{acl_cat}"));
    }
    // 8) Tips
    writer.write_set_length(self.tips.len());
    for tip in &self.tips {
      writer.write_ascii_bulk_string(tip);
    }
    // 9) Key specifications
    writer.write_set_length(self.key_specifications.len());
    for ks in &self.key_specifications {
      ks.to_resp_format(writer);
    }
    // 10) SubCommands
    writer.write_array_length(self.sub_commands.len());
    for sub_command in &self.sub_commands {
      sub_command.to_resp_format(writer);
    }
  }
}

impl IRespSerializable for RespCommandsInfo {
  fn to_resp_format(&self, writer: &mut RespMemoryWriter) {
    self.to_resp_format(writer);
  }
}

impl RespCommandsInfo {
  /// 逆向为 JSON 导入结构（C# 导出面：类自身即 JSON 契约，经
  /// `DefaultRespCommandsDataProvider.TryExportRespCommandsData` 序列化；
  /// 空/默认字段的省略差异仅为字节面，语义一致）
  #[cfg(test)]
  pub(crate) fn to_import(&self) -> RespCommandsInfoImport {
    let flags = {
      let names = self.flags.member_names();
      (!names.is_empty()).then(|| names.join(", "))
    };
    let acl_categories = {
      let names = acl_category_member_names(self.acl_categories);
      (!names.is_empty()).then(|| names.join(", "))
    };
    let store_type = (!matches!(self.store_type, StoreType::None))
      .then_some(match self.store_type {
        StoreType::Main => "Main",
        StoreType::Object => "Object",
        StoreType::All => "All",
        StoreType::None => "None",
      })
      .map(str::to_string);
    let key_specifications = (!self.key_specifications.is_empty())
      .then(|| self.key_specifications.iter().map(ks_to_import).collect());
    let sub_commands = (!self.sub_commands.is_empty())
      .then(|| self.sub_commands.iter().map(|sc| sc.to_import()).collect());

    RespCommandsInfoImport {
      command: cs_name_of(self.command).to_string(),
      name: self.name.clone(),
      is_internal: self.is_internal,
      arity: self.arity,
      flags,
      first_key: self.first_key,
      last_key: self.last_key,
      step: self.step,
      acl_categories,
      tips: (!self.tips.is_empty()).then(|| self.tips.clone()),
      key_specifications,
      store_type,
      sub_commands,
    }
  }
}

/// C# 枚举成员名（`Command` 字段导出用）
#[cfg(test)]
fn cs_name_of(cmd: RespCommand) -> &'static str {
  super::resp_commands_info_data::resp_command_to_cs_name(cmd)
}

/// 键规格逆向导入结构（C# 导出经 KeySpecConverter 写 TypeDiscriminator）
#[cfg(test)]
fn ks_to_import(ks: &RespCommandKeySpecification) -> KeySpecificationImport {
  let begin_search = ks.begin_search.as_ref().map(|m| match m {
    BeginSearchMethod::Index(index) => KeySpecMethodImport {
      discriminator: m.discriminator().to_string(),
      index: Some(*index),
      ..Default::default()
    },
    BeginSearchMethod::Keyword {
      keyword,
      start_from,
    } => KeySpecMethodImport {
      discriminator: m.discriminator().to_string(),
      keyword: Some(keyword.clone()),
      start_from: Some(*start_from),
      ..Default::default()
    },
    BeginSearchMethod::Unknown => KeySpecMethodImport {
      discriminator: m.discriminator().to_string(),
      ..Default::default()
    },
  });
  let find_keys = ks.find_keys.as_ref().map(|m| match m {
    FindKeysMethod::Range {
      last_key,
      key_step,
      limit,
    } => KeySpecMethodImport {
      discriminator: m.discriminator().to_string(),
      last_key: Some(*last_key),
      key_step: Some(*key_step),
      limit: Some(*limit),
      ..Default::default()
    },
    FindKeysMethod::KeyNum {
      key_num_idx,
      first_key,
      key_step,
    } => KeySpecMethodImport {
      discriminator: m.discriminator().to_string(),
      key_num_idx: Some(*key_num_idx),
      first_key: Some(*first_key),
      key_step: Some(*key_step),
      ..Default::default()
    },
    FindKeysMethod::Unknown => KeySpecMethodImport {
      discriminator: m.discriminator().to_string(),
      ..Default::default()
    },
  });
  KeySpecificationImport {
    begin_search,
    find_keys,
    notes: ks.notes.clone(),
    flags: {
      let names = ks.flags.descriptions();
      (!names.is_empty()).then(|| names.join(", "))
    },
  }
}

// —— JSON 导入结构（C# JsonSerializer 的类型面） ——

/// 键规格方法导入（C# KeySpecConverter 读出的多态字段平铺）
#[derive(Deserialize, Serialize, Clone, Default)]
struct KeySpecMethodImport {
  #[serde(rename = "TypeDiscriminator")]
  discriminator: String,
  #[serde(rename = "Index")]
  index: Option<i32>,
  #[serde(rename = "Keyword")]
  keyword: Option<String>,
  #[serde(rename = "StartFrom")]
  start_from: Option<i32>,
  #[serde(rename = "LastKey")]
  last_key: Option<i32>,
  #[serde(rename = "KeyStep")]
  key_step: Option<i32>,
  #[serde(rename = "Limit")]
  limit: Option<i32>,
  #[serde(rename = "KeyNumIdx")]
  key_num_idx: Option<i32>,
  #[serde(rename = "FirstKey")]
  first_key: Option<i32>,
}

/// 键规格导入（C# RespCommandKeySpecification JSON 面）
#[derive(Deserialize, Serialize, Clone, Default)]
struct KeySpecificationImport {
  #[serde(rename = "BeginSearch")]
  begin_search: Option<KeySpecMethodImport>,
  #[serde(rename = "FindKeys")]
  find_keys: Option<KeySpecMethodImport>,
  #[serde(rename = "Notes")]
  notes: Option<String>,
  #[serde(rename = "Flags")]
  flags: Option<String>,
}

impl KeySpecificationImport {
  fn convert(self) -> Option<RespCommandKeySpecification> {
    let flags = match self.flags {
      Some(f) => KeySpecificationFlags::from_wire_names(&f)?,
      None => super::resp_command_key_specification::KeySpecificationFlags::NONE,
    };
    Some(RespCommandKeySpecification {
      begin_search: self.begin_search.and_then(|m| m.into_begin_search()),
      find_keys: self.find_keys.and_then(|m| m.into_find_keys()),
      notes: self.notes,
      flags,
    })
  }
}

impl KeySpecMethodImport {
  fn into_begin_search(self) -> Option<BeginSearchMethod> {
    // C# KeySpecConverter.CanConvert + 读出
    if !BeginSearchMethod::can_convert(&self.discriminator) {
      return None;
    }
    Some(match self.discriminator.as_str() {
      "BeginSearchIndex" => BeginSearchMethod::Index(self.index.unwrap_or(0)),
      "BeginSearchKeyword" => BeginSearchMethod::Keyword {
        keyword: self.keyword?,
        start_from: self.start_from.unwrap_or(0),
      },
      _ => BeginSearchMethod::Unknown,
    })
  }

  fn into_find_keys(self) -> Option<FindKeysMethod> {
    if !FindKeysMethod::can_convert(&self.discriminator) {
      return None;
    }
    Some(match self.discriminator.as_str() {
      "FindKeysRange" => FindKeysMethod::Range {
        last_key: self.last_key.unwrap_or(0),
        key_step: self.key_step.unwrap_or(0),
        limit: self.limit.unwrap_or(0),
      },
      "FindKeysKeyNum" => FindKeysMethod::KeyNum {
        key_num_idx: self.key_num_idx.unwrap_or(0),
        first_key: self.first_key.unwrap_or(0),
        key_step: self.key_step.unwrap_or(0),
      },
      _ => FindKeysMethod::Unknown,
    })
  }
}

/// 命令元数据导入（C# RespCommandsInfo 的 JSON 面）
#[derive(Deserialize, Serialize, Clone, Default)]
pub(crate) struct RespCommandsInfoImport {
  #[serde(rename = "Command")]
  command: String,
  #[serde(rename = "Name")]
  name: String,
  #[serde(rename = "IsInternal", default)]
  is_internal: bool,
  #[serde(rename = "Arity", default)]
  arity: i32,
  #[serde(rename = "Flags")]
  flags: Option<String>,
  #[serde(rename = "FirstKey", default)]
  first_key: i32,
  #[serde(rename = "LastKey", default)]
  last_key: i32,
  #[serde(rename = "Step", default)]
  step: i32,
  #[serde(rename = "AclCategories")]
  acl_categories: Option<String>,
  #[serde(rename = "Tips")]
  tips: Option<Vec<String>>,
  #[serde(rename = "KeySpecifications")]
  key_specifications: Option<Vec<KeySpecificationImport>>,
  #[serde(rename = "StoreType")]
  store_type: Option<String>,
  #[serde(rename = "SubCommands")]
  sub_commands: Option<Vec<RespCommandsInfoImport>>,
}

impl IRespCommandData for RespCommandsInfoImport {
  fn name(&self) -> &str {
    &self.name
  }
}

impl RespCommandsInfoImport {
  /// 转换为域类型；枚举名/标记无法解析即失败（C# JsonException 语义）
  fn convert(self, parent_is_internal: bool, depth: usize) -> Option<RespCommandsInfo> {
    if self.name.is_empty() {
      return None;
    }
    let command = resp_command_from_cs_name(&self.command)?;
    let flags = match &self.flags {
      Some(f) => RespCommandFlags::from_member_names(f)?,
      None => RespCommandFlags::empty(),
    };
    let acl_categories = match &self.acl_categories {
      Some(a) => acl_categories_from_member_names(a)?,
      None => RespAclCategories::from_bits_retain(0),
    };
    let store_type = match &self.store_type {
      Some(s) => StoreType::from_member_name(s)?,
      None => StoreType::None,
    };
    let mut key_specifications = Vec::new();
    for ks in self.key_specifications.unwrap_or_default() {
      key_specifications.push(ks.convert()?);
    }

    let mut sub_commands = Vec::new();
    // C# JSON 面无嵌套深度限制，防御性上限防环
    if depth < 4 {
      for sc in self.sub_commands.unwrap_or_default() {
        sub_commands.push(sc.convert(self.is_internal, depth + 1)?);
      }
    }

    Some(RespCommandsInfo {
      command,
      name: self.name,
      is_internal: self.is_internal,
      arity: self.arity,
      flags,
      first_key: self.first_key,
      last_key: self.last_key,
      step: self.step,
      acl_categories,
      tips: self.tips.unwrap_or_default(),
      key_specifications,
      store_type,
      sub_commands,
      is_sub_command: depth > 0,
      parent_is_internal,
    })
  }
}

// —— 静态表 ——

/// 全量索引（C# 静态字段族）
pub struct RespCommandsTables {
  /// 全部命令（键 = 小写名；C# AllRespCommandsInfo）
  pub all: HashMap<String, RespCommandsInfo>,
  /// 全部子命令（键 = 小写名；C# AllRespSubCommandsInfo）
  pub all_sub: HashMap<String, RespCommandsInfo>,
  /// 外部命令（C# ExternalRespCommandsInfo）
  pub external: HashMap<String, RespCommandsInfo>,
  /// 外部子命令（C# ExternalRespSubCommandsInfo）
  pub external_sub: HashMap<String, RespCommandsInfo>,
  /// 全部命令名（C# AllRespCommandNames）
  pub all_names: HashSet<String>,
  /// 外部命令名（C# ExternalRespCommandNames）
  pub external_names: HashSet<String>,
  /// 按命令枚举扁平索引（C# FlattenedRespCommandsInfo；键 = 判别值）
  pub flattened: HashMap<u16, RespCommandsInfo>,
  /// 简化信息数组（下标 = 命令枚举值；C# SimpleRespCommandsInfo）
  pub simple: Vec<SimpleRespCommandInfo>,
  /// ACL 分类 → 命令列表（键 = 单类别位；C# AclCommandInfo）
  pub acl_command_info: HashMap<u32, Vec<RespCommandsInfo>>,
  /// 数据命令快速数组（下标 = 枚举值 - FirstDataCommand；C# FastBasicRespCommandsInfo）
  pub fast_basic: Vec<Option<RespCommandsInfo>>,
}

static TABLES: OnceLock<Option<RespCommandsTables>> = OnceLock::new();

/// libs/server/Resp/RespCommandsInfo.cs:TryInitialize
fn try_initialize() -> bool {
  TABLES
    .get_or_init(|| {
      if try_initialize_resp_commands_info() {
        Some(build_tables())
      } else {
        None
      }
    })
    .is_some()
}

/// 导入产物（构建期中间结构）
static IMPORTED: OnceLock<Vec<RespCommandsInfo>> = OnceLock::new();

/// libs/server/Resp/RespCommandsInfo.cs:TryInitializeRespCommandsInfo
///
/// 导入 + 校验；成功后把导入结果暂存 [`IMPORTED`]，表构建见
/// [`build_tables`]（两段式对应 C# 构造函数内先反序列化再建索引）。
fn try_initialize_resp_commands_info() -> bool {
  let imported = try_import_resp_commands_data::<RespCommandsInfoImport>(RESP_COMMANDS_INFO_JSON);
  let Some(imported) = imported else {
    return false;
  };

  let mut converted = Vec::with_capacity(imported.len());
  for entry in imported {
    let Some(info) = entry.convert(false, 0) else {
      return false;
    };
    converted.push(info);
  }
  IMPORTED.set(converted).is_ok()
}

/// 依据导入结果构建全部索引
fn build_tables() -> RespCommandsTables {
  let imported = IMPORTED.get().expect("导入已完成");

  let mut all: HashMap<String, RespCommandsInfo> = HashMap::with_hasher(GxBuildHasher::default());
  let mut all_sub: HashMap<String, RespCommandsInfo> =
    HashMap::with_hasher(GxBuildHasher::default());
  let mut external: HashMap<String, RespCommandsInfo> =
    HashMap::with_hasher(GxBuildHasher::default());
  let mut external_sub: HashMap<String, RespCommandsInfo> =
    HashMap::with_hasher(GxBuildHasher::default());
  let mut flattened: HashMap<u16, RespCommandsInfo> =
    HashMap::with_hasher(GxBuildHasher::default());
  let mut acl_command_info: HashMap<u32, Vec<RespCommandsInfo>> =
    HashMap::with_hasher(GxBuildHasher::default());

  for entry in imported {
    // C# 枚举判别值 NONE 的条目不入扁平表
    if entry.command == RespCommand::None {
      continue;
    }
    // 历史原因：SLAVEOF 可接受但非真实命令，让位 SECONDARYOF/REPLICAOF
    if entry.name == "SLAVEOF" {
      continue;
    }

    flattened.insert(entry.command as u16, entry.clone());

    for sc in &entry.sub_commands {
      flattened.insert(sc.command as u16, sc.clone());
    }
  }

  for entry in imported {
    all.insert(entry.name.to_lowercase(), entry.clone());
    if !entry.is_internal {
      external.insert(entry.name.to_lowercase(), entry.clone());
    }
    for sc in &entry.sub_commands {
      all_sub.insert(sc.name.to_lowercase(), sc.clone());
      if !entry.is_internal && !sc.is_internal {
        external_sub.insert(sc.name.to_lowercase(), sc.clone());
      }
    }
    // ACL 分类索引：根 + 子命令，各单类别位一组（C# IndividualAcls 展开）
    for cmd in [entry].into_iter().chain(entry.sub_commands.iter()) {
      for single in individual_acls(cmd.acl_categories) {
        acl_command_info
          .entry(single)
          .or_default()
          .push(cmd.clone());
      }
    }
  }

  let mut all_names: HashSet<String> = HashSet::with_hasher(GxBuildHasher::default());
  for k in all.keys() {
    all_names.insert(k.clone());
  }
  let mut external_names: HashSet<String> = HashSet::with_hasher(GxBuildHasher::default());
  for k in external.keys() {
    external_names.insert(k.clone());
  }

  // 简化信息数组：[FirstDataCommand, LastValidCommand] 区间填充
  let table_len = LAST_VALID_COMMAND as usize + 1;
  let mut simple = vec![SimpleRespCommandInfo::default(); table_len];
  for (cmd_id, slot) in simple
    .iter_mut()
    .enumerate()
    .take(table_len)
    .skip(FIRST_DATA_COMMAND as usize)
  {
    let Some(cmd_info) = flattened.get(&(cmd_id as u16)) else {
      continue;
    };
    populate_simple_command_info(cmd_info, slot);
  }

  // 数据命令快速数组
  let fast_len = LAST_DATA_COMMAND as usize - FIRST_DATA_COMMAND as usize + 1;
  let mut fast_basic: Vec<Option<RespCommandsInfo>> = (0..fast_len).map(|_| None).collect();
  for (i, slot) in fast_basic.iter_mut().enumerate() {
    if let Some(info) = flattened.get(&((i + FIRST_DATA_COMMAND as usize) as u16)) {
      *slot = Some(info.clone());
    }
  }

  RespCommandsTables {
    all,
    all_sub,
    external,
    external_sub,
    all_names,
    external_names,
    flattened,
    simple,
    acl_command_info,
    fast_basic,
  }
}

/// 已初始化表引用
fn tables() -> Option<&'static RespCommandsTables> {
  if !try_initialize() {
    return None;
  }
  TABLES.get().and_then(|t| t.as_ref())
}

/// 产出位集中的每个单类别位（C# IndividualAcls）
pub(crate) fn individual_acls(acl_categories: RespAclCategories) -> Vec<u32> {
  let mut out = Vec::new();
  let mut remaining = acl_categories.bits();
  while remaining != 0 {
    let single = remaining.isolate_lowest_one();
    remaining &= !single;
    out.push(single);
  }
  out
}

/// 取某 ACL 分类覆盖的命令
///
/// libs/server/Resp/RespCommandsInfo.cs:TryGetCommandsforAclCategory
pub fn try_get_commandsfor_acl_category(
  acl: RespAclCategories,
) -> Option<Vec<&'static RespCommandsInfo>> {
  let tables = tables()?;
  if acl.bits().count_ones() != 1 {
    return None;
  }
  tables
    .acl_command_info
    .get(&acl.bits())
    .map(|v| v.iter().collect())
}

/// 取 Garnet 支持的命令数
///
/// libs/server/Resp/RespCommandsInfo.cs:TryGetRespCommandsInfoCount
pub fn try_get_resp_commands_info_count(external_only: bool) -> Option<usize> {
  let tables = tables()?;
  Some(if external_only {
    tables.external.len()
  } else {
    tables.all.len()
  })
}

/// 取全部命令元数据（键为小写命令名）
///
/// libs/server/Resp/RespCommandsInfo.cs:TryGetRespCommandsInfo
pub fn try_get_resp_commands_info(
  external_only: bool,
) -> Option<&'static HashMap<String, RespCommandsInfo>> {
  let tables = tables()?;
  Some(if external_only {
    &tables.external
  } else {
    &tables.all
  })
}

/// 取全部命令名集合
///
/// libs/server/Resp/RespCommandsInfo.cs:TryGetRespCommandNames
pub fn try_get_resp_command_names(external_only: bool) -> Option<&'static HashSet<String>> {
  let tables = tables()?;
  Some(if external_only {
    &tables.external_names
  } else {
    &tables.all_names
  })
}

/// 按命令名取元数据（大小写不敏感；`include_sub_commands` 同时查子命令表）
///
/// libs/server/Resp/RespCommandsInfo.cs:TryGetRespCommandInfo(string,...)
pub fn try_get_resp_command_info_by_name(
  cmd_name: &str,
  external_only: bool,
  include_sub_commands: bool,
) -> Option<&'static RespCommandsInfo> {
  let tables = tables()?;
  let key = cmd_name.to_lowercase();
  let primary = if external_only {
    &tables.external
  } else {
    &tables.all
  };
  primary.get(&key).or_else(|| {
    if include_sub_commands {
      let sub = if external_only {
        &tables.external_sub
      } else {
        &tables.all_sub
      };
      sub.get(&key)
    } else {
      None
    }
  })
}

/// 按命令枚举取元数据（`txn_only` 时剔除 NoMulti 命令）
///
/// 对应 C# TryGetRespCommandInfo(RespCommand, ...) 枚举重载
pub fn try_get_resp_command_info_by_cmd(
  cmd: RespCommand,
  txn_only: bool,
) -> Option<&'static RespCommandsInfo> {
  let tables = tables()?;
  let info = tables.flattened.get(&(cmd as u16))?;
  if txn_only && info.flags.intersects(RespCommandFlags::NO_MULTI) {
    return None;
  }
  Some(info)
}

/// 自数据命令快速数组按枚举取元数据
///
/// libs/server/Resp/RespCommandsInfo.cs:TryFastGetRespCommandInfo
pub fn try_fast_get_resp_command_info(cmd: RespCommand) -> Option<&'static RespCommandsInfo> {
  let tables = tables()?;
  let offset = cmd as usize - FIRST_DATA_COMMAND as usize;
  if offset >= tables.fast_basic.len() {
    return None;
  }
  tables.fast_basic[offset].as_ref()
}

/// 取全部子命令元数据（键为小写名）
///
/// libs/server/Resp/RespCommandsInfo.cs:TryGetRespSubCommandsInfo
pub fn try_get_resp_sub_commands_info(
  external_only: bool,
) -> Option<&'static HashMap<String, RespCommandsInfo>> {
  let tables = tables()?;
  Some(if external_only {
    &tables.external_sub
  } else {
    &tables.all_sub
  })
}

/// 取命令简化信息
///
/// libs/server/Resp/RespCommandsInfo.cs:TryGetSimpleRespCommandInfo
pub fn try_get_simple_resp_command_info(
  cmd: RespCommand,
) -> Option<&'static SimpleRespCommandInfo> {
  let tables = tables()?;
  let cmd_id = cmd as usize;
  if cmd_id >= tables.simple.len() {
    return None;
  }
  Some(&tables.simple[cmd_id])
}

/// 取命令名（未知命令回 UNKNOWN）
///
/// libs/server/Resp/RespCommandsInfo.cs:GetRespCommandName
pub fn get_resp_command_name(cmd: RespCommand) -> String {
  match try_get_resp_command_info_by_cmd(cmd, false) {
    Some(info) => info.name.clone(),
    None => UNKNOWN_COMMAND_NAME.to_string(),
  }
}

#[cfg(test)]
mod tests {
  use gxhash::{GxBuildHasher, HashMap};
  use wacl::RespAclCategories;

  use super::{
    super::resp_memory_writer::RespMemoryWriter, RespCommandFlags, StoreType,
    acl_categories_from_member_names, get_resp_command_name, individual_acls,
    try_fast_get_resp_command_info, try_get_commandsfor_acl_category,
    try_get_resp_command_info_by_cmd, try_get_resp_command_info_by_name,
    try_get_resp_commands_info, try_get_resp_commands_info_count,
  };
  use crate::types::RespCommand;

  /// 表初始化 + 基本检索（COMMAND 表快照的入口断言）
  #[test]
  fn tables_initialize_and_lookup() {
    // 初始化成功
    assert!(super::try_initialize());

    // 计数快照：C# 表为 262 根命令（其中 4 个内部命令）
    let all = super::try_get_resp_commands_info(false).unwrap();
    let external = super::try_get_resp_commands_info(true).unwrap();
    assert_eq!(all.len(), 262, "根命令数快照");
    assert_eq!(external.len(), 258, "外部根命令数快照");
    assert_eq!(try_get_resp_commands_info_count(false), Some(all.len()));
    assert_eq!(try_get_resp_commands_info_count(true), Some(external.len()));

    // 命令名检索（大小写不敏感）
    let get = try_get_resp_command_info_by_name("GET", false, false).unwrap();
    assert_eq!(get.arity, 2);
    assert_eq!(get.first_key, 1);
    assert_eq!(get.last_key, 1);
    assert_eq!(get.step, 1);
    assert_eq!(get.store_type, StoreType::Main);
    assert_eq!(
      get.flags,
      RespCommandFlags::from_member_names("Fast, ReadOnly").unwrap()
    );

    // 子命令检索
    let acl_cat = try_get_resp_command_info_by_name("ACL|CAT", false, true).unwrap();
    assert_eq!(acl_cat.command, RespCommand::AclCat);
    assert!(
      try_get_resp_command_info_by_name("ACL|CAT", false, false).is_none(),
      "不带子命令检索时不可达"
    );

    // 按枚举扁平检索
    let set = try_get_resp_command_info_by_cmd(RespCommand::Set, false).unwrap();
    assert_eq!(set.name, "SET");
    // 事务过滤：NoMulti 命令被剔除（ASYNC 带 NoMulti）
    assert!(try_get_resp_command_info_by_cmd(RespCommand::Async, true).is_none());
    assert!(try_get_resp_command_info_by_cmd(RespCommand::Async, false).is_some());
    // SLAVEOF 被跳过，SECONDARYOF 保留
    assert!(try_get_resp_command_info_by_cmd(RespCommand::Secondaryof, false).is_some());
    assert!(try_get_resp_command_info_by_cmd(RespCommand::Replicaof, false).is_some());

    // 快速数组
    let fast = try_fast_get_resp_command_info(RespCommand::Append).unwrap();
    assert_eq!(fast.name, "APPEND");
    // 非数据命令越界
    assert!(try_fast_get_resp_command_info(RespCommand::Quit).is_none());

    // 命令名解析
    assert_eq!(get_resp_command_name(RespCommand::Bitcount), "BITCOUNT");
    assert_eq!(get_resp_command_name(RespCommand::Invalid), "UNKNOWN");
  }

  #[test]
  fn acl_category_index() {
    let bitmap = try_get_commandsfor_acl_category(RespAclCategories::BITMAP).unwrap();
    let names: Vec<&str> = bitmap.iter().map(|c| c.name.as_str()).collect();
    assert!(names.contains(&"SETBIT"));
    assert!(names.contains(&"GETBIT"));
    assert!(names.contains(&"BITCOUNT"));
    assert!(names.contains(&"BITPOS"));
    assert!(names.contains(&"BITFIELD"));
    assert!(names.contains(&"BITFIELD_RO"));
    assert!(names.contains(&"BITOP"));
    // 复合分类无直接组（C# TryGetValue 单类别语义）
    assert!(
      try_get_commandsfor_acl_category(RespAclCategories::BITMAP | RespAclCategories::STRING)
        .is_none()
    );
  }

  /// COMMAND INFO 快照：GET（RESP3）逐字节对标 C# ToRespFormat 输出
  #[test]
  fn command_info_resp3_snapshot_get() {
    let get = try_get_resp_command_info_by_cmd(RespCommand::Get, false).unwrap();
    let mut w = RespMemoryWriter::new(true);
    get.to_resp_format(&mut w);
    let expected = concat!(
      "*10\r\n$3\r\nGET\r\n:2\r\n~2\r\n+fast\r\n+readonly\r\n:1\r\n:1\r\n:1\r\n",
      "~3\r\n+@fast\r\n+@read\r\n+@string\r\n~0\r\n~1\r\n%3\r\n$5\r\nflags\r\n~2\r\n+RO\r\n+access\r\n",
      "$12\r\nbegin_search\r\n%2\r\n$4\r\ntype\r\n$5\r\nindex\r\n$4\r\nspec\r\n%1\r\n$5\r\nindex\r\n:1\r\n",
      "$9\r\nfind_keys\r\n%2\r\n$4\r\ntype\r\n$5\r\nrange\r\n$4\r\nspec\r\n%3\r\n$7\r\nlastkey\r\n:0\r\n$7\r\nkeystep\r\n:1\r\n$5\r\nlimit\r\n:0\r\n*0\r\n",
    );
    assert_eq!(String::from_utf8(w.out).unwrap(), expected);
  }

  /// COMMAND INFO 快照：BITFIELD（RESP3，含 notes 与四重键标记）
  #[test]
  fn command_info_resp3_snapshot_bitfield() {
    let bf = try_get_resp_command_info_by_cmd(RespCommand::Bitfield, false).unwrap();
    let mut w = RespMemoryWriter::new(true);
    bf.to_resp_format(&mut w);
    let expected = concat!(
      "*10\r\n$8\r\nBITFIELD\r\n:-2\r\n~2\r\n+denyoom\r\n+write\r\n:1\r\n:1\r\n:1\r\n",
      "~3\r\n+@bitmap\r\n+@slow\r\n+@write\r\n~0\r\n~1\r\n%4\r\n$5\r\nnotes\r\n$59\r\nThis command allows both access and modification of the key\r\n",
      "$5\r\nflags\r\n~4\r\n+RW\r\n+access\r\n+update\r\n+variable_flags\r\n",
      "$12\r\nbegin_search\r\n%2\r\n$4\r\ntype\r\n$5\r\nindex\r\n$4\r\nspec\r\n%1\r\n$5\r\nindex\r\n:1\r\n",
      "$9\r\nfind_keys\r\n%2\r\n$4\r\ntype\r\n$5\r\nrange\r\n$4\r\nspec\r\n%3\r\n$7\r\nlastkey\r\n:0\r\n$7\r\nkeystep\r\n:1\r\n$5\r\nlimit\r\n:0\r\n*0\r\n",
    );
    assert_eq!(String::from_utf8(w.out).unwrap(), expected);
  }

  /// COMMAND INFO 快照：SETBIT（RESP2 降级面）
  #[test]
  fn command_info_resp2_snapshot_setbit() {
    let setbit = try_get_resp_command_info_by_cmd(RespCommand::Setbit, false).unwrap();
    let mut w = RespMemoryWriter::new(false);
    setbit.to_resp_format(&mut w);
    let expected = concat!(
      "*10\r\n$6\r\nSETBIT\r\n:4\r\n*2\r\n+denyoom\r\n+write\r\n:1\r\n:1\r\n:1\r\n",
      "*3\r\n+@bitmap\r\n+@slow\r\n+@write\r\n*0\r\n*1\r\n*6\r\n$5\r\nflags\r\n*3\r\n+RW\r\n+access\r\n+update\r\n",
      "$12\r\nbegin_search\r\n*4\r\n$4\r\ntype\r\n$5\r\nindex\r\n$4\r\nspec\r\n*2\r\n$5\r\nindex\r\n:1\r\n",
      "$9\r\nfind_keys\r\n*4\r\n$4\r\ntype\r\n$5\r\nrange\r\n$4\r\nspec\r\n*6\r\n$7\r\nlastkey\r\n:0\r\n$7\r\nkeystep\r\n:1\r\n$5\r\nlimit\r\n:0\r\n*0\r\n",
    );
    assert_eq!(String::from_utf8(w.out).unwrap(), expected);
  }

  /// 导出 → 再导入闭环（C# TryExportRespCommandsData 面的等价校验）
  #[test]
  fn export_import_roundtrip() {
    use super::super::resp_command_data_provider::get_resp_commands_data_provider;

    let all = try_get_resp_commands_info(false).unwrap();
    let mut exports: Vec<super::RespCommandsInfoImport> = Vec::with_capacity(all.len());
    let mut expected_arity: HashMap<String, i32> = HashMap::with_hasher(GxBuildHasher::default());
    for info in all.values() {
      exports.push(info.to_import());
      expected_arity.insert(info.name.to_lowercase(), info.arity);
    }

    let provider = get_resp_commands_data_provider();
    let json = provider.try_export_resp_commands_data(&exports).unwrap();
    let reimported = provider
      .try_import_resp_commands_data::<super::RespCommandsInfoImport>(&json)
      .unwrap();
    assert_eq!(reimported.len(), all.len(), "重导入条目数一致");
    for entry in reimported {
      let info = entry.convert(false, 0).expect("重导入可转换");
      assert_eq!(
        expected_arity.get(&info.name.to_lowercase()),
        Some(&info.arity),
        "{} arity 保持",
        info.name
      );
    }
  }

  #[test]
  fn individual_acls_yields_single_bits() {
    let cats = acl_categories_from_member_names("Fast, String, Write").unwrap();
    let bits = individual_acls(cats);
    assert_eq!(
      bits,
      vec![
        RespAclCategories::FAST.bits(),
        RespAclCategories::STRING.bits(),
        RespAclCategories::WRITE.bits()
      ]
    );
  }
}
