//! 命令元数据域类型与静态索引（对标 libs/server/Resp/RespCommandsInfo.cs）
//!
//! C# 于 Garnet.resources 程序集内嵌 `RespCommandsInfo.json`，首次访问时
//! 反序列化一次并构建多张静态索引（全量 / 外部 / 按枚举扁平 / ACL 分类 /
//! 简化结构 / 快速数组），全进程一份；Rust 侧 JSON 的读取与反序列化收敛于
//! [`super`] 的单点编排（[`super::try_initialize`]），本模块承接域类型
//! （字符串字段 `&'static str`，解析期一次定型）、导入面与索引构建。
//!
//! 在 garnet 中的相对路径: libs/server/Resp/RespCommandsInfo.cs（COMMAND INFO）

use core::str::FromStr;

use sonic_rs::Deserialize;
use wbase::{
  map::{HashMap, HashSet},
  store_type::StoreType,
};

use super::{
  RespAclCategories,
  data_provider::IRespCommandData,
  simplified::{SimpleRespCommandInfo, populate_simple_command_info},
};
use crate::{
  command::{FIRST_DATA_COMMAND, LAST_VALID_COMMAND, RespCommand},
  key_spec::{
    BEGIN_SEARCH_INDEX, BEGIN_SEARCH_KEYWORD, BeginSearchMethod, FIND_KEYS_KEY_NUM,
    FIND_KEYS_RANGE, FindKeysMethod, KeySpecificationFlags, RespCommandKeySpecification,
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
#[derive(Debug, Clone, PartialEq)]
pub struct RespCommandFlags(pub u32);

impl RespCommandFlags {
  /// admin
  pub const ADMIN: Self = Self(1);
  /// asking
  pub const ASKING: Self = Self(1 << 1);
  /// blocking
  pub const BLOCKING: Self = Self(1 << 2);
  /// denyoom
  const DENY_OOM: Self = Self(1 << 3);
  /// fast
  pub const FAST: Self = Self(1 << 4);
  /// loading
  const LOADING: Self = Self(1 << 5);
  /// movablekeys
  const MOVABLE_KEYS: Self = Self(1 << 6);
  /// no_auth
  const NO_AUTH: Self = Self(1 << 7);
  /// no_async_loading
  const NO_ASYNC_LOADING: Self = Self(1 << 8);
  /// no_mandatory_keys
  const NO_MANDATORY_KEYS: Self = Self(1 << 9);
  /// no_multi
  pub const NO_MULTI: Self = Self(1 << 10);
  /// noscript
  pub const NO_SCRIPT: Self = Self(1 << 11);
  /// pubsub
  const PUB_SUB: Self = Self(1 << 12);
  /// random
  const RANDOM: Self = Self(1 << 13);
  /// readonly
  const READ_ONLY: Self = Self(1 << 14);
  /// sort_for_script
  const SORT_FOR_SCRIPT: Self = Self(1 << 15);
  /// skip_monitor
  const SKIP_MONITOR: Self = Self(1 << 16);
  /// skip_slowlog
  const SKIP_SLOW_LOG: Self = Self(1 << 17);
  /// stale
  const STALE: Self = Self(1 << 18);
  /// write
  pub const WRITE: Self = Self(1 << 19);
  /// allow_busy
  const ALLOW_BUSY: Self = Self(1 << 20);
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

  /// 标记计数（popcnt）
  #[inline]
  pub const fn count(&self) -> usize {
    self.0.count_ones() as usize
  }

  /// 迭代 wire 描述（零堆分配）
  pub fn iter_descriptions(&self) -> impl Iterator<Item = &'static str> + '_ {
    Self::TABLE
      .iter()
      .filter(|(bit, ..)| self.0 & bit != 0)
      .map(|(_, _, desc)| *desc)
  }

  /// wire 描述（C# EnumUtils.GetEnumDescriptions）
  pub fn descriptions(&self) -> Vec<&'static str> {
    self.iter_descriptions().collect()
  }

  /// 按成员名串解析（大小写不敏感；C# JsonStringEnumConverter 语义）
  pub fn from_member_names(names: &str) -> Option<Self> {
    super::parse_member_names(names, |part| {
      let trimmed = part.trim();
      Self::TABLE
        .iter()
        .find(|(_, member, _)| member.eq_ignore_ascii_case(trimmed))
        .map(|(bit, ..)| *bit)
    })
    .map(Self)
  }
}

/// ACL 分类的 wire 描述（C# RespAclCategories Description；声明序）
///
/// libs/server/Resp/RespCommandInfoFlags.cs:RespAclCategories
pub fn acl_category_descriptions(cats: RespAclCategories) -> Vec<&'static str> {
  cats.iter_descriptions().collect()
}

/// libs/server/Resp/RespCommandsInfo.cs:RespCommandsInfo
///
/// 一条 RESP 命令的元数据（C# RespCommandsInfo）
#[derive(Clone)]
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
    writer.write_set_length(self.flags.count());
    for flag in self.flags.iter_descriptions() {
      writer.write_simple_string(flag);
    }
    // 4) First key
    writer.write_int32(self.first_key);
    // 5) Last key
    writer.write_int32(self.last_key);
    // 6) Step
    writer.write_int32(self.step);
    // 7) ACL categories
    writer.write_set_length(self.acl_categories.count());
    for acl_cat in self.acl_categories.iter_descriptions() {
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

// —— JSON 导入结构（C# JsonSerializer 的类型面） ——

/// 键规格方法导入（C# KeySpecConverter 读出的多态字段平铺：
/// CanConvert 判别多态族、Read 按 TypeDiscriminator 分派字段）
///
/// libs/server/Resp/RespCommandKeySpecification.cs:CanConvert
/// libs/server/Resp/RespCommandKeySpecification.cs:Read
#[derive(Deserialize, Default)]
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
#[derive(Deserialize, Default)]
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
      BEGIN_SEARCH_INDEX => BeginSearchMethod::Index(self.index.unwrap_or(0)),
      BEGIN_SEARCH_KEYWORD => BeginSearchMethod::Keyword {
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
      FIND_KEYS_RANGE => FindKeysMethod::Range {
        last_key: self.last_key.unwrap_or(0),
        key_step: self.key_step.unwrap_or(0),
        limit: self.limit.unwrap_or(0),
      },
      FIND_KEYS_KEY_NUM => FindKeysMethod::KeyNum {
        key_num_idx: self.key_num_idx.unwrap_or(0),
        first_key: self.first_key.unwrap_or(0),
        key_step: self.key_step.unwrap_or(0),
      },
      _ => FindKeysMethod::Unknown,
    })
  }
}

/// 命令元数据导入（C# RespCommandsInfo 的 JSON 面）
#[derive(Deserialize, Default)]
pub(crate) struct RespCommandsInfoImport {
  /// 命令枚举名；缺键落空串默认值（C# STJ 缺成员 = 枚举默认值 NONE，
  /// RespCommandDataProvider.cs:136-164 仅未知名的值抛 JsonException 整表败）
  #[serde(rename = "Command", default)]
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
    roots
      .into_iter()
      .map(|entry| entry.convert(false, 0))
      .collect()
  }

  /// 转换为域类型；字符串字段就地定型 'static
  fn convert(self, parent_is_internal: bool, depth: usize) -> Option<RespCommandsInfo> {
    if self.name.is_empty() {
      return None;
    }
    // Command 缺键（serde default 空串）落 RespCommand::None、条目保留
    // （C# STJ 缺成员 = 默认值 NONE）；未知枚举名仍整表失败（JsonException 同臂）
    let command = if self.command.is_empty() {
      RespCommand::None
    } else {
      RespCommand::from_str(&self.command).ok()?
    };
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

/// 全量索引（C# 静态字段族；字段域 crate 内封闭，查询面经 `catalog::tables` 单点）
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
  /// 按导入文档顺序排列的全部命令
  pub all_ordered: Vec<(&'static str, RespCommandsInfo)>,
  /// 按导入文档顺序排列的外部命令
  pub external_ordered: Vec<(&'static str, RespCommandsInfo)>,
}

type CommandsScope<'a> = (
  &'a HashMap<&'static str, RespCommandsInfo>,
  &'a HashMap<&'static str, RespCommandsInfo>,
  &'a HashSet<&'static str>,
  &'a [(&'static str, RespCommandsInfo)],
);

impl RespCommandsTables {
  /// 按 external-only 口径一次取齐 (主表, 子命令表, 名集合, 导入序列表) 四索引
  /// （收口查询面五处重复的 `if external_only` 双借分派）
  #[inline]
  fn scope(&'static self, external_only: bool) -> CommandsScope<'static> {
    if external_only {
      (
        &self.external,
        &self.external_sub,
        &self.external_names,
        &self.external_ordered,
      )
    } else {
      (&self.all, &self.all_sub, &self.all_names, &self.all_ordered)
    }
  }
}

/// 依据导入产物（先序树）构建全部索引
///
/// C# 侧此段位于 RespCommandsInfo 静态构造函数内（反序列化之后的索引
/// 构建部分，无独立 C# 函数）。跨父子命令重名即整表熔断（None → 目录
/// 永久空表）：C# AllRespSubCommandsInfo.Add 抛 ArgumentException 且不被
/// TryImportRespCommandsData 的 JsonException catch 覆盖，向上炸初始化
/// （RespCommandsInfo.cs:184/:186）。两口径措辞分写：rust 为永空表，C# 为
/// 命令层异常上抛，非等形（登记级裁决）。根表重名拒于 data_provider（C#
/// TryAdd 同臂），此处不重复。
pub(super) fn build_tables(imported: &[RespCommandsInfo]) -> Option<RespCommandsTables> {
  let mut all: HashMap<&'static str, RespCommandsInfo> = HashMap::default();
  let mut all_sub: HashMap<&'static str, RespCommandsInfo> = HashMap::default();
  let mut external: HashMap<&'static str, RespCommandsInfo> = HashMap::default();
  let mut external_sub: HashMap<&'static str, RespCommandsInfo> = HashMap::default();
  let mut flattened: HashMap<u16, RespCommandsInfo> = HashMap::default();
  let mut all_ordered: Vec<(&'static str, RespCommandsInfo)> = Vec::with_capacity(imported.len());
  let mut external_ordered: Vec<(&'static str, RespCommandsInfo)> = Vec::new();

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
    let name = static_str(entry.name.to_lowercase());
    all.insert(name, entry.clone());
    all_ordered.push((name, entry.clone()));
    if !entry.is_internal {
      external.insert(name, entry.clone());
      external_ordered.push((name, entry.clone()));
    }
    for sc in &entry.sub_commands {
      // 定型一次小写键，双表共用（同值名不再双份泄漏）
      let sname = static_str(sc.name.to_lowercase());
      // C# Add 重名抛异常同臂判重：insert 返旧值即整表熔断
      if all_sub.insert(sname, sc.clone()).is_some() {
        return None;
      }
      if !entry.is_internal && !sc.is_internal && external_sub.insert(sname, sc.clone()).is_some() {
        return None;
      }
    }
  }

  let all_names: HashSet<&'static str> = all.keys().copied().collect();
  let external_names: HashSet<&'static str> = external.keys().copied().collect();

  // 简化信息数组：[FirstDataCommand, LastValidCommand] 区间填充
  let table_len = LAST_VALID_COMMAND as usize + 1;
  let mut simple = vec![SimpleRespCommandInfo::default(); table_len];
  for (cmd_id, slot) in simple
    .iter_mut()
    .enumerate()
    .skip(FIRST_DATA_COMMAND as usize)
  {
    let Some(cmd_info) = flattened.get(&(cmd_id as u16)) else {
      continue;
    };
    populate_simple_command_info(cmd_info, slot);
  }

  Some(RespCommandsTables {
    all,
    all_sub,
    external,
    external_sub,
    all_names,
    external_names,
    flattened,
    simple,
    all_ordered,
    external_ordered,
  })
}

// —— 查询面（经 [`super::tables`] 取单点索引） ——

/// 取 Garnet 支持的命令数
///
/// libs/server/Resp/RespCommandsInfo.cs:TryGetRespCommandsInfoCount
pub fn try_get_resp_commands_info_count(external_only: bool) -> Option<usize> {
  let (map, ..) = super::tables()?.scope(external_only);
  Some(map.len())
}

/// 取全部命令元数据（键为小写命令名）
///
/// libs/server/Resp/RespCommandsInfo.cs:TryGetRespCommandsInfo
pub fn try_get_resp_commands_info(
  external_only: bool,
) -> Option<&'static HashMap<&'static str, RespCommandsInfo>> {
  Some(super::tables()?.scope(external_only).0)
}

/// 取全部命令元数据列表（按文档导入顺序；每项为 (小写名, 命令元数据)）
///
/// 对标 C# ExternalRespCommandsInfo 的插入序 Values 遍历（消除 HashMap 种子随机漂移）
pub fn try_get_resp_commands_info_ordered(
  external_only: bool,
) -> Option<&'static [(&'static str, RespCommandsInfo)]> {
  Some(super::tables()?.scope(external_only).3)
}

/// 取全部命令名集合
///
/// libs/server/Resp/RespCommandsInfo.cs:TryGetRespCommandNames
pub fn try_get_resp_command_names(external_only: bool) -> Option<&'static HashSet<&'static str>> {
  Some(super::tables()?.scope(external_only).2)
}

/// 按命令名取元数据（大小写不敏感；`include_sub_commands` 同时查子命令表）
///
/// libs/server/Resp/RespCommandsInfo.cs:TryGetRespCommandInfo(string,...)
///
/// libs/server/Resp/RespCommandsInfo.cs:TryGetRespSubCommandsInfo（C# 独立子命令
/// 取表臂；rust 折叠为本函数 `include_sub_commands` 形参单点）
pub fn try_get_resp_command_info_by_name(
  cmd_name: &str,
  external_only: bool,
  include_sub_commands: bool,
) -> Option<&'static RespCommandsInfo> {
  let (map, sub, ..) = super::tables()?.scope(external_only);
  let key = cmd_name.to_lowercase();
  map.get(key.as_str()).or_else(|| {
    include_sub_commands
      .then(|| sub.get(key.as_str()))
      .flatten()
  })
}

/// 按命令枚举取元数据（`txn_only` 时剔除 NoMulti 命令）
///
/// 对应 C# TryGetRespCommandInfo(RespCommand, ...) 枚举重载；
/// 在 garnet 中的相对路径:libs/server/Resp/RespCommandsInfo.cs:TryFastGetRespCommandInfo
///（FastBasicRespCommandsInfo 数组直取臂，同点承接）
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

/// 取命令简化信息
///
/// libs/server/Resp/RespCommandsInfo.cs:TryGetSimpleRespCommandInfo
pub fn try_get_simple_resp_command_info(
  cmd: RespCommand,
) -> Option<&'static SimpleRespCommandInfo> {
  super::tables()?.simple.get(cmd as usize)
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
