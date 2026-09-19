//! 命令元数据域类型与静态索引（对标 libs/server/Resp/RespCommandsInfo.cs）
//!
//! C# 于 Garnet.resources 程序集内嵌 `RespCommandsInfo.json`，首次访问时
//! 反序列化一次并构建多张静态索引（全量 / 外部 / 按枚举扁平 / ACL 分类 /
//! 简化结构 / 快速数组），全进程一份；Rust 侧 JSON 的读取与反序列化收敛于
//! [`super`] 的单点编排（[`super::try_initialize`]），本模块承接域类型
//! （字符串字段 `&'static str`，解析期一次定型）、导入面与索引构建。

use core::str::FromStr;

use gxhash::{GxBuildHasher, HashMap, HashSet};
use sonic_rs::Deserialize;
use wbase::store_type::StoreType;

use super::{
  RespAclCategories,
  data_provider::IRespCommandData,
  simplified::{SimpleRespCommandInfo, populate_simple_command_info},
};
use crate::{
  command::{FIRST_DATA_COMMAND, LAST_DATA_COMMAND, LAST_VALID_COMMAND, RespCommand},
  i_resp_serializable::IRespSerializable,
  key_spec::{
    BeginSearchMethod, FindKeysMethod, KeySpecificationFlags, RespCommandKeySpecification,
  },
  resp_memory_writer::{RespBuffer, RespProtocol, RespWriter},
};

/// 未知命令名（C# UnknownCommandName）
const UNKNOWN_COMMAND_NAME: &str = "UNKNOWN";

/// 字符串定型为 'static（解析一次，静态表永久持有）
#[inline]
pub(super) fn static_str(s: String) -> &'static str {
  Box::leak(s.into_boxed_str())
}

/// libs/server/Resp/RespCommandsInfo.cs:RespCommandFlags
///
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

/// libs/server/Resp/RespCommandsInfo.cs:RespCommandsInfo
///
/// 一条 RESP 命令的元数据（C# RespCommandsInfo）
#[derive(Debug, Clone)]
pub struct RespCommandsInfo {
  /// 命令枚举（C# Command）
  pub command: RespCommand,
  /// 命令名（子命令为 `ACL|CAT` 形式；解析期定型）
  pub name: &'static str,
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
  pub tips: Vec<&'static str>,
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
  pub fn to_resp_format<B: RespBuffer, P: RespProtocol>(&self, writer: &mut RespWriter<B, P>) {
    if self.name.trim().is_empty() {
      writer.write_null();
      return;
    }

    writer.write_array_length(10);
    // 1) Name
    writer.write_ascii_bulk_string(self.name);
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
      let out = writer.buf_mut();
      out.reserve(2 + acl_cat.len() + 2);
      out.extend_from_slice(b"+@");
      out.extend_from_slice(acl_cat.as_bytes());
      out.extend_from_slice(b"\r\n");
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
  fn to_resp_format<B: RespBuffer, P: RespProtocol>(&self, writer: &mut RespWriter<B, P>) {
    self.to_resp_format(writer);
  }
}

// —— JSON 导入结构（C# JsonSerializer 的类型面） ——

/// 键规格方法导入（C# KeySpecConverter 读出的多态字段平铺）
#[derive(Deserialize, Clone, Default)]
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
#[derive(Deserialize, Clone, Default)]
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
      None => KeySpecificationFlags::empty(),
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
#[derive(Deserialize, Clone, Default)]
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
  /// 全量导入 + 转换（C# 构造函数内先反序列化再逐条转换的合并面）；
  /// 枚举名 / 标记 / 分类 / 存储类型无法解析即整体失败（C# JsonException
  /// → TryInitialize false 语义）
  pub(super) fn convert_all(roots: Vec<Self>) -> Option<Vec<RespCommandsInfo>> {
    let mut out = Vec::with_capacity(roots.len());
    for entry in roots {
      out.push(entry.convert(false, 0)?);
    }
    Some(out)
  }

  /// 转换为域类型；字符串字段就地定型 'static
  fn convert(self, parent_is_internal: bool, depth: usize) -> Option<RespCommandsInfo> {
    if self.name.is_empty() {
      return None;
    }
    let command = RespCommand::from_str(&self.command).ok()?;
    let flags = match &self.flags {
      Some(f) => RespCommandFlags::from_member_names(f)?,
      None => RespCommandFlags::empty(),
    };
    let acl_categories = match &self.acl_categories {
      Some(a) => RespAclCategories::from_member_names(a)?,
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
      name: static_str(self.name),
      is_internal: self.is_internal,
      arity: self.arity,
      flags,
      first_key: self.first_key,
      last_key: self.last_key,
      step: self.step,
      acl_categories,
      tips: self
        .tips
        .unwrap_or_default()
        .into_iter()
        .map(static_str)
        .collect(),
      key_specifications,
      store_type,
      sub_commands,
      is_sub_command: depth > 0,
      parent_is_internal,
    })
  }
}

// —— 静态索引构建（C# 静态字段族） ——

/// 全量索引（C# 静态字段族）
pub struct RespCommandsTables {
  /// 全部命令（键 = 小写名；C# AllRespCommandsInfo）
  pub all: HashMap<&'static str, RespCommandsInfo>,
  /// 全部子命令（键 = 小写名；C# AllRespSubCommandsInfo）
  pub all_sub: HashMap<&'static str, RespCommandsInfo>,
  /// 外部命令（C# ExternalRespCommandsInfo）
  pub external: HashMap<&'static str, RespCommandsInfo>,
  /// 外部子命令（C# ExternalRespSubCommandsInfo）
  pub external_sub: HashMap<&'static str, RespCommandsInfo>,
  /// 全部命令名（C# AllRespCommandNames）
  pub all_names: HashSet<&'static str>,
  /// 外部命令名（C# ExternalRespCommandNames）
  pub external_names: HashSet<&'static str>,
  /// 按命令枚举扁平索引（C# FlattenedRespCommandsInfo；键 = 判别值）
  pub flattened: HashMap<u16, RespCommandsInfo>,
  /// 简化信息数组（下标 = 命令枚举值；C# SimpleRespCommandsInfo）
  pub simple: Vec<SimpleRespCommandInfo>,
  /// ACL 分类 → 命令列表（键 = 单类别位；C# AclCommandInfo）
  pub acl_command_info: HashMap<u32, Vec<RespCommandsInfo>>,
  /// 数据命令快速数组（下标 = 枚举值 - FirstDataCommand；C# FastBasicRespCommandsInfo）
  pub fast_basic: Vec<Option<RespCommandsInfo>>,
}

/// 依据导入产物（先序树）构建全部索引
///
/// C# 侧此段位于 RespCommandsInfo 静态构造函数内（反序列化之后的索引
/// 构建部分，无独立 C# 函数）
pub(super) fn build_tables(imported: &[RespCommandsInfo]) -> RespCommandsTables {
  let mut all: HashMap<&'static str, RespCommandsInfo> =
    HashMap::with_hasher(GxBuildHasher::default());
  let mut all_sub: HashMap<&'static str, RespCommandsInfo> =
    HashMap::with_hasher(GxBuildHasher::default());
  let mut external: HashMap<&'static str, RespCommandsInfo> =
    HashMap::with_hasher(GxBuildHasher::default());
  let mut external_sub: HashMap<&'static str, RespCommandsInfo> =
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
    all.insert(static_str(entry.name.to_lowercase()), entry.clone());
    if !entry.is_internal {
      external.insert(static_str(entry.name.to_lowercase()), entry.clone());
    }
    for sc in &entry.sub_commands {
      all_sub.insert(static_str(sc.name.to_lowercase()), sc.clone());
      if !entry.is_internal && !sc.is_internal {
        external_sub.insert(static_str(sc.name.to_lowercase()), sc.clone());
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

  let mut all_names: HashSet<&'static str> = HashSet::with_hasher(GxBuildHasher::default());
  for k in all.keys() {
    all_names.insert(k);
  }
  let mut external_names: HashSet<&'static str> = HashSet::with_hasher(GxBuildHasher::default());
  for k in external.keys() {
    external_names.insert(k);
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

// —— 查询面（经 [`super::tables`] 取单点索引） ——

/// 取某 ACL 分类覆盖的命令
///
/// libs/server/Resp/RespCommandsInfo.cs:TryGetCommandsforAclCategory
pub fn try_get_commandsfor_acl_category(
  acl: RespAclCategories,
) -> Option<Vec<&'static RespCommandsInfo>> {
  let tables = super::tables()?;
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
  let tables = super::tables()?;
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
) -> Option<&'static HashMap<&'static str, RespCommandsInfo>> {
  let tables = super::tables()?;
  Some(if external_only {
    &tables.external
  } else {
    &tables.all
  })
}

/// 取全部命令名集合
///
/// libs/server/Resp/RespCommandsInfo.cs:TryGetRespCommandNames
pub fn try_get_resp_command_names(external_only: bool) -> Option<&'static HashSet<&'static str>> {
  let tables = super::tables()?;
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
  let tables = super::tables()?;
  let key = cmd_name.to_lowercase();
  let primary = if external_only {
    &tables.external
  } else {
    &tables.all
  };
  primary.get(key.as_str()).or_else(|| {
    if include_sub_commands {
      let sub = if external_only {
        &tables.external_sub
      } else {
        &tables.all_sub
      };
      sub.get(key.as_str())
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
  let tables = super::tables()?;
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
  let tables = super::tables()?;
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
) -> Option<&'static HashMap<&'static str, RespCommandsInfo>> {
  let tables = super::tables()?;
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
  let tables = super::tables()?;
  let cmd_id = cmd as usize;
  if cmd_id >= tables.simple.len() {
    return None;
  }
  Some(&tables.simple[cmd_id])
}

/// 取命令名（未知命令回 UNKNOWN）
///
/// libs/server/Resp/RespCommandsInfo.cs:GetRespCommandName
pub fn get_resp_command_name(cmd: RespCommand) -> &'static str {
  match try_get_resp_command_info_by_cmd(cmd, false) {
    Some(info) => info.name,
    None => UNKNOWN_COMMAND_NAME,
  }
}

#[cfg(test)]
mod tests {
  use wbase::store_type::StoreType;

  use super::{
    RespAclCategories, RespCommandFlags, acl_category_descriptions, get_resp_command_name,
    individual_acls, static_str, try_get_simple_resp_command_info,
  };
  use crate::{
    catalog::{
      try_fast_get_resp_command_info, try_get_commandsfor_acl_category,
      try_get_resp_command_info_by_cmd, try_get_resp_command_info_by_name,
      try_get_resp_commands_info, try_get_resp_commands_info_count, try_initialize,
    },
    command::{FIRST_DATA_COMMAND, LAST_VALID_COMMAND, RespCommand},
    resp_memory_writer::{Resp2, Resp3, RespWriter},
  };

  /// 表初始化 + 基本检索（COMMAND 表快照的入口断言）
  #[test]
  fn tables_initialize_and_lookup() {
    // 初始化成功
    assert!(try_initialize());

    // 计数快照：清理 MODULE 和 REGISTERCS 后为 262 根命令（其中 4 个内部命令，
    // 258 个外部命令；含本仓自定义扩展 RI.COUNT 与 SUNSUBSCRIBE）
    let all = try_get_resp_commands_info(false).unwrap();
    let external = try_get_resp_commands_info(true).unwrap();
    assert_eq!(all.len(), 262, "根命令数快照");
    assert_eq!(external.len(), 258, "外部根命令数快照");
    assert_eq!(try_get_resp_commands_info_count(false), Some(all.len()));
    assert_eq!(try_get_resp_commands_info_count(true), Some(external.len()));

    // rust 自增命令 SUNSUBSCRIBE 的 COMMAND 面：arity -1 承接无参全退形态，
    // PubSub 类别使 +@pubsub 可展开到位 370
    let sunsub = try_get_resp_command_info_by_name("SUNSUBSCRIBE", false, false).unwrap();
    assert_eq!(sunsub.command, RespCommand::Sunsubscribe);
    assert_eq!(sunsub.arity, -1);
    assert!(sunsub.acl_categories.contains(RespAclCategories::PUBSUB));

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

    // 简化信息面
    let simple = try_get_simple_resp_command_info(RespCommand::Get).unwrap();
    assert!(simple.allowed_in_txn);
    assert_eq!(simple.arity, 2);
  }

  #[test]
  fn acl_category_index() {
    let bitmap = try_get_commandsfor_acl_category(RespAclCategories::BITMAP).unwrap();
    let names: Vec<&str> = bitmap.iter().map(|c| c.name).collect();
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
    let mut w = RespWriter::<Vec<u8>, Resp3>::new();
    get.to_resp_format(&mut w);
    let expected = concat!(
      "*10\r\n$3\r\nGET\r\n:2\r\n~2\r\n+fast\r\n+readonly\r\n:1\r\n:1\r\n:1\r\n",
      "~3\r\n+@fast\r\n+@read\r\n+@string\r\n~0\r\n~1\r\n%3\r\n$5\r\nflags\r\n~2\r\n+RO\r\n+access\r\n",
      "$12\r\nbegin_search\r\n%2\r\n$4\r\ntype\r\n$5\r\nindex\r\n$4\r\nspec\r\n%1\r\n$5\r\nindex\r\n:1\r\n",
      "$9\r\nfind_keys\r\n%2\r\n$4\r\ntype\r\n$5\r\nrange\r\n$4\r\nspec\r\n%3\r\n$7\r\nlastkey\r\n:0\r\n$7\r\nkeystep\r\n:1\r\n$5\r\nlimit\r\n:0\r\n*0\r\n",
    );
    assert_eq!(String::from_utf8(w.into_inner()).unwrap(), expected);
  }

  /// COMMAND INFO 快照：BITFIELD（RESP3，含 notes 与四重键标记）
  #[test]
  fn command_info_resp3_snapshot_bitfield() {
    let bf = try_get_resp_command_info_by_cmd(RespCommand::Bitfield, false).unwrap();
    let mut w = RespWriter::<Vec<u8>, Resp3>::new();
    bf.to_resp_format(&mut w);
    let expected = concat!(
      "*10\r\n$8\r\nBITFIELD\r\n:-2\r\n~2\r\n+denyoom\r\n+write\r\n:1\r\n:1\r\n:1\r\n",
      "~3\r\n+@bitmap\r\n+@slow\r\n+@write\r\n~0\r\n~1\r\n%4\r\n$5\r\nnotes\r\n$59\r\nThis command allows both access and modification of the key\r\n",
      "$5\r\nflags\r\n~4\r\n+RW\r\n+access\r\n+update\r\n+variable_flags\r\n",
      "$12\r\nbegin_search\r\n%2\r\n$4\r\ntype\r\n$5\r\nindex\r\n$4\r\nspec\r\n%1\r\n$5\r\nindex\r\n:1\r\n",
      "$9\r\nfind_keys\r\n%2\r\n$4\r\ntype\r\n$5\r\nrange\r\n$4\r\nspec\r\n%3\r\n$7\r\nlastkey\r\n:0\r\n$7\r\nkeystep\r\n:1\r\n$5\r\nlimit\r\n:0\r\n*0\r\n",
    );
    assert_eq!(String::from_utf8(w.into_inner()).unwrap(), expected);
  }

  /// COMMAND INFO 快照：SETBIT（RESP2 降级面）
  #[test]
  fn command_info_resp2_snapshot_setbit() {
    let setbit = try_get_resp_command_info_by_cmd(RespCommand::Setbit, false).unwrap();
    let mut w = RespWriter::<Vec<u8>, Resp2>::new();
    setbit.to_resp_format(&mut w);
    let expected = concat!(
      "*10\r\n$6\r\nSETBIT\r\n:4\r\n*2\r\n+denyoom\r\n+write\r\n:1\r\n:1\r\n:1\r\n",
      "*3\r\n+@bitmap\r\n+@slow\r\n+@write\r\n*0\r\n*1\r\n*6\r\n$5\r\nflags\r\n*3\r\n+RW\r\n+access\r\n+update\r\n",
      "$12\r\nbegin_search\r\n*4\r\n$4\r\ntype\r\n$5\r\nindex\r\n$4\r\nspec\r\n*2\r\n$5\r\nindex\r\n:1\r\n",
      "$9\r\nfind_keys\r\n*4\r\n$4\r\ntype\r\n$5\r\nrange\r\n$4\r\nspec\r\n*6\r\n$7\r\nlastkey\r\n:0\r\n$7\r\nkeystep\r\n:1\r\n$5\r\nlimit\r\n:0\r\n*0\r\n",
    );
    assert_eq!(String::from_utf8(w.into_inner()).unwrap(), expected);
  }

  #[test]
  fn individual_acls_yields_single_bits() {
    let cats = RespAclCategories::from_member_names("Fast, String, Write").unwrap();
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

  /// 常量与辅助面锚定
  #[test]
  fn constants_and_helpers() {
    assert_eq!(FIRST_DATA_COMMAND, RespCommand::Append);
    // 快速数组覆盖 [APPEND, EVALSHA]
    assert!(try_fast_get_resp_command_info(RespCommand::Evalsha).is_some());
    let cats = RespAclCategories::from_member_names("Fast, Read").unwrap();
    assert_eq!(acl_category_descriptions(cats), vec!["fast", "read"]);
    assert_eq!(static_str("x".to_string()), "x");
    // 表区间：LAST_VALID_COMMAND 内简化槽均存在
    let tables_ok = try_get_simple_resp_command_info(LAST_VALID_COMMAND).is_some();
    assert!(tables_ok);
  }
}
