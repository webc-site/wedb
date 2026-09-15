//! 自定义命令管理器（对标 libs/server/Custom/CustomCommandManager.cs）
//!
//! 承接四类注册空间（原始字符串命令 / 自定义对象命令 / 事务过程 / 自定义过程）
//! 与模块注册、按名索引/文档索引。C# 侧经 ExpandableMap 分配 id；
//! 本文件以域内 `IdSpace` 承接同语义
//! （min..=max 顺序分配 + 按值去重查找 + 按名匹配）。

use std::{fmt, sync::Arc};

use gxhash::{HashMap, HashSet};
use parking_lot::RwLock;

use crate::{
  custom_transaction_procedure::CustomTxnProc,
  error::{Error, Result},
};

/// 自定义原始字符串命令的注册上限（对齐 C# MaxCustomRawStringCommands）。
const MAX_CUSTOM_RAW_STRING_COMMANDS: usize = 256;

/// 自定义原始字符串命令 id 区间（[INVALID - 256, INVALID - 1]）。
const CUSTOM_RAW_STRING_COMMAND_MIN_ID: u16 =
  (u16::MAX - 1) - MAX_CUSTOM_RAW_STRING_COMMANDS as u16 + 1;
/// 对齐 C# CustomRawStringCommandMaxId = INVALID - 1。
const CUSTOM_RAW_STRING_COMMAND_MAX_ID: u16 = u16::MAX - 1;

/// 自定义对象类型 id 区间：固定基址（对齐 C# 注释，避免内建类型增长挪动持久化 id）。
const CUSTOM_OBJECT_TYPE_MIN_ID: u8 = 0x40;
const CUSTOM_OBJECT_TYPE_MAX_ID: u8 = 0xFE;

/// 命令类型（对齐 libs/server/Custom/CommandType.cs:CommandType）。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum CommandType {
  /// 只读。
  Read = 0,
  /// 读-改-写。
  ReadModifyWrite = 1,
}

/// 自定义原始字符串命令的处理函数形态（输入参数 → 应答字节）。
pub type RawStringFn = fn(&[&[u8]]) -> Vec<u8>;

/// 已注册的自定义原始字符串命令。
#[derive(Clone)]
pub struct CustomRawStringCommand {
  /// 命令名（小写规范化）。
  pub name: String,
  /// 扩展 id（0 基，对齐 C# extId）。
  pub ext_id: u16,
  /// 命令类型。
  pub command_type: CommandType,
  /// 元数（命令信息缺省时为 0）。
  pub arity: i32,
  /// 过期时长（ticks；0 表示不处理过期）。
  pub expiration_ticks: i64,
  /// 处理函数。
  pub functions: RawStringFn,
}

/// 自定义原始字符串命令注册参数（参数对象：对齐 C# Register 的多字段形参）。
///
/// 将原本的 6 个形参收敛为一个描述体，调用方以字面量组装，
/// 语义与 C# Register(name, commandType, functions, ...) 保持 1:1。
pub struct RawStringCommandSpec<'a> {
  /// 命令名（注册时规范化小写）。
  pub name: &'a str,
  /// 命令类型。
  pub command_type: CommandType,
  /// 处理函数。
  pub functions: RawStringFn,
  /// 命令元数信息（可选）。
  pub command_info: Option<CustomCommandInfo>,
  /// 命令文档（可选）。
  pub command_docs: Option<CustomCommandDocs>,
  /// 过期时长（ticks；0 表示不处理过期）。
  pub expiration_ticks: i64,
}

impl fmt::Debug for CustomRawStringCommand {
  fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    f.debug_struct("CustomRawStringCommand")
      .field("name", &self.name)
      .field("ext_id", &self.ext_id)
      .field("command_type", &self.command_type)
      .field("arity", &self.arity)
      .finish_non_exhaustive()
  }
}

/// NeedInitialUpdate 执行体签名：参数域 → RESP 输出（false = 已写错误应答）
pub type CustomNeedInitialFn = fn(&[&[u8]], &mut Vec<u8>) -> bool;
/// Updater 执行体签名：载荷就地改写 + 参数域 → RESP 输出
pub type CustomUpdaterFn = fn(&mut Vec<u8>, &[&[u8]], &mut Vec<u8>) -> bool;
/// Reader 执行体签名：载荷 + 参数域 → RESP 输出
pub type CustomReaderFn = fn(&[u8], &[&[u8]], &mut Vec<u8>) -> bool;
/// NotFound 执行体签名：参数域 → RESP 输出
pub type CustomNotFoundFn = fn(&[&[u8]], &mut Vec<u8>);
/// 空对象判定签名
pub type CustomIsEmptyFn = fn(&[u8]) -> bool;

/// 自定义对象命令执行体（CustomObjectFunctions 四接口的函数指针承接）
///
/// libs/server/Custom/CustomObjectFunctions.cs:NeedInitialUpdate / Updater /
/// Reader / NotFound
///
/// 载荷域为对象信封载荷字节（信封首字节类型标签已剥除，由注册层
/// [`CustomCommandManager::object_type_tag`] 单独承载）。空载荷 = 键缺失时
/// 新建对象的零载荷初值（C# 工厂 Create 的承接形态，Updater 自行解出）。
///
/// 对照 C# 分层：C# 框架（Tsavorite）持工厂并管理对象生命周期，命令类
/// 只收 `IGarnetObject`；rust 侧对象以信封载荷字节落库，序列化/反序列化
/// 内聚到执行体（等价 C# 工厂 Create/SerializeObject/Deserialize 分层）。
#[derive(Clone, Copy)]
pub struct CustomObjectFns {
  /// NeedInitialUpdate：键缺失时建对象前先行校验（防空墓碑）；
  /// false = output 已写错误应答，放弃建对象
  pub need_initial_update: CustomNeedInitialFn,
  /// Updater：读改写执行体（载荷就地改写后回写）；false = output 已写
  /// 错误应答，放弃落库（C# AbortWithErrorMessage）
  pub updater: CustomUpdaterFn,
  /// Reader：命中键只读执行体；false = output 已写错误应答
  pub reader: CustomReaderFn,
  /// NotFound：缺键只读应答（读不建键；C# 缺省实现 WriteNull）
  pub not_found: CustomNotFoundFn,
  /// 载荷是否空对象（空 → 整键回收，wedb 严格删空公理；
  /// 对照 C# 空对象常驻的刻意差异，见 wnode 执行层注释）
  pub is_empty: CustomIsEmptyFn,
}

/// 已注册的自定义对象子命令。
#[derive(Clone)]
pub struct CustomObjectCommand {
  /// 命令名（小写规范化）。
  pub name: String,
  /// 对象类型扩展 id。
  pub ext_id: u8,
  /// 子命令 id。
  pub sub_id: u8,
  /// 命令类型。
  pub command_type: CommandType,
  /// 元数。
  pub arity: i32,
  /// 执行体（None = 仅元数据登记，执行域未接线）。
  pub functions: Option<CustomObjectFns>,
}

/// 自定义对象类型包装（类型工厂 + 子命令表）。
#[derive(Default)]
pub struct CustomObjectCommandWrapper {
  /// 类型扩展 id。
  pub ext_id: u8,
  /// 子命令表（子命令 id → 命令）。
  pub command_map: Vec<Option<CustomObjectCommand>>,
  /// 已占用子命令数（id 分配游标）。
  pub next_sub_id: usize,
}

/// 自定义事务过程工厂（对齐 C# `Func<CustomTransactionProcedure>`：
/// 零动态分派函数指针，产出静态分派的 [`CustomTxnProc`]）。
pub type TxnProcFactory = fn() -> CustomTxnProc;

/// 已注册的自定义事务过程。
#[derive(Clone)]
pub struct CustomTransaction {
  /// 事务名（小写规范化）。
  pub name: String,
  /// 事务 id。
  pub id: u8,
  /// 元数。
  pub arity: i32,
  /// 过程实例工厂（C# procCreator；None = 仅元数据登记）。
  pub factory: Option<TxnProcFactory>,
}

/// 已注册的自定义过程包装。
#[derive(Clone)]
pub struct CustomProcedureWrapper {
  /// 过程名（小写规范化）。
  pub name: String,
  /// 过程 id。
  pub id: u8,
  /// 元数。
  pub arity: i32,
}

/// 自定义命令信息（RespCommandsInfo 的最小承接）。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CustomCommandInfo {
  /// 命令名。
  pub name: String,
  /// 元数。
  pub arity: i32,
  /// ACL 类别（如 "read", "custom"）。
  pub acl_categories: Vec<String>,
}

/// 自定义命令文档（RespCommandDocs 的最小承接）。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CustomCommandDocs {
  /// 命令名。
  pub name: String,
  /// 摘要。
  pub summary: String,
}

/// 顺序 id 分配空间（ExpandableMap 的域内承接：min..=max + 按值查找）。
#[derive(Debug)]
struct IdSpace {
  /// 下一个候选 id。
  next: u64,
  /// 空间上界。
  max: u64,
}

impl IdSpace {
  fn new(min: u64, max: u64) -> Self {
    Self { next: min, max }
  }

  /// 顺序取下一可用 id（跳过已占用槽位）。
  fn try_get_next_id(&mut self, occupied: impl Fn(u64) -> bool) -> Option<u64> {
    while self.next <= self.max {
      let candidate = self.next;
      self.next += 1;
      if !occupied(candidate) {
        return Some(candidate);
      }
    }
    None
  }
}

/// 自定义命令管理器。
pub struct CustomCommandManager {
  /// 原始字符串命令表（id → 命令）。
  raw_string_commands: Vec<Option<CustomRawStringCommand>>,
  /// 原始字符串 id 游标。
  raw_string_ids: IdSpace,
  /// 自定义对象类型表（类型 id → 包装）。
  object_commands: Vec<Option<CustomObjectCommandWrapper>>,
  /// 对象类型 id 游标。
  object_type_ids: IdSpace,
  /// 事务过程表（id → 事务）。
  transaction_procs: Vec<Option<CustomTransaction>>,
  /// 事务 id 游标。
  transaction_ids: IdSpace,
  /// 自定义过程表（id → 过程）。
  custom_procedures: Vec<Option<CustomProcedureWrapper>>,
  /// 过程 id 游标。
  procedure_ids: IdSpace,
  /// 对象类型名 → 类型扩展 id（C# 工厂引用去重的按名承接形态）。
  type_names: HashMap<String, u8>,
  /// 按名命令信息索引（大小写不敏感：存小写键）。
  custom_commands_info: HashMap<String, CustomCommandInfo>,
  /// 按名命令文档索引（小写键）。
  custom_commands_docs: HashMap<String, CustomCommandDocs>,
  /// 全部已注册命令名集合（小写键；含未附信息的注册）。
  custom_command_names: HashSet<String>,
}

impl Default for CustomCommandManager {
  fn default() -> Self {
    Self::new()
  }
}

impl CustomCommandManager {
  /// 创建管理器（对齐 C# 构造器的空间装配）。
  pub fn new() -> Self {
    Self {
      raw_string_commands: Vec::new(),
      raw_string_ids: IdSpace::new(
        u64::from(CUSTOM_RAW_STRING_COMMAND_MIN_ID),
        u64::from(CUSTOM_RAW_STRING_COMMAND_MAX_ID),
      ),
      object_commands: Vec::new(),
      object_type_ids: IdSpace::new(
        u64::from(CUSTOM_OBJECT_TYPE_MIN_ID),
        u64::from(CUSTOM_OBJECT_TYPE_MAX_ID),
      ),
      transaction_procs: Vec::new(),
      transaction_ids: IdSpace::new(0, u8::MAX as u64),
      custom_procedures: Vec::new(),
      procedure_ids: IdSpace::new(0, u8::MAX as u64),
      type_names: HashMap::default(),
      custom_commands_info: HashMap::default(),
      custom_commands_docs: HashMap::default(),
      custom_command_names: HashSet::default(),
    }
  }

  /// libs/server/Custom/CustomCommandManager.cs:Register（原始字符串命令）
  ///
  /// 注册自定义原始字符串命令；返回扩展 id。空间耗尽报错。
  pub fn register_raw_string_command(&mut self, spec: RawStringCommandSpec) -> Result<u16> {
    let cmd_id = self
      .raw_string_ids
      .try_get_next_id(|id| {
        let ext = (id - u64::from(CUSTOM_RAW_STRING_COMMAND_MIN_ID)) as usize;
        self
          .raw_string_commands
          .get(ext)
          .and_then(Option::as_ref)
          .is_some()
      })
      .ok_or(Error::OutOfSpace)?;

    let ext_id = (cmd_id - u64::from(CUSTOM_RAW_STRING_COMMAND_MIN_ID)) as u16;
    let arity = spec.command_info.as_ref().map_or(0, |info| info.arity);
    let new_cmd = CustomRawStringCommand {
      name: spec.name.to_lowercase(),
      ext_id,
      command_type: spec.command_type,
      arity,
      expiration_ticks: spec.expiration_ticks,
      functions: spec.functions,
    };

    // 精确槽位写入（按 ext_id，对齐 C# ExpandableMap 相对索引）
    let slot = ext_id as usize;
    if self.raw_string_commands.len() <= slot {
      self.raw_string_commands.resize_with(slot + 1, || None);
    }
    self.raw_string_commands[slot] = Some(new_cmd);

    self.track_registration(spec.name, spec.command_info, spec.command_docs);
    Ok(ext_id)
  }

  /// 注册自定义事务（对应 C# CustomCommandManager.Register(CustomTransactionProcedure) 重载）；返回事务 id。
  ///
  /// `factory` 为过程实例工厂（C# procCreator；None = 仅元数据登记）。
  pub fn register_transaction(
    &mut self,
    name: &str,
    factory: Option<TxnProcFactory>,
    command_info: Option<CustomCommandInfo>,
    command_docs: Option<CustomCommandDocs>,
  ) -> Result<u8> {
    let cmd_id = self
      .transaction_ids
      .try_get_next_id(|id| {
        self
          .transaction_procs
          .get(id as usize)
          .and_then(Option::as_ref)
          .is_some()
      })
      .ok_or(Error::OutOfSpace)?;

    let arity = command_info.as_ref().map_or(0, |info| info.arity);
    let new_cmd = CustomTransaction {
      name: name.to_lowercase(),
      id: cmd_id as u8,
      arity,
      factory,
    };

    let slot = cmd_id as usize;
    if self.transaction_procs.len() <= slot {
      self.transaction_procs.resize_with(slot + 1, || None);
    }
    self.transaction_procs[slot] = Some(new_cmd);

    self.track_registration(name, command_info, command_docs);
    Ok(cmd_id as u8)
  }

  /// libs/server/Custom/CustomCommandManager.cs:RegisterType
  ///
  /// 注册自定义对象类型（同名类型重复注册报错；对标 C# 工厂引用去重的
  /// 按名承接形态）；返回类型扩展 id。
  pub fn register_type(&mut self, type_name: &str) -> Result<u8> {
    let type_key = type_name.to_lowercase();
    if self.type_names.contains_key(&type_key) {
      return Err(Error::TypeAlreadyRegistered);
    }
    Ok(self.register_new_type(&type_key)? - CUSTOM_OBJECT_TYPE_MIN_ID)
  }

  /// 注册自定义对象命令（对应 C# CustomCommandManager.Register(CustomObjectCommand) 重载）；返回 (类型扩展 id, 子命令 id)。
  pub fn register_object_command(
    &mut self,
    type_name: &str,
    name: &str,
    command_type: CommandType,
    functions: Option<CustomObjectFns>,
    command_info: Option<CustomCommandInfo>,
    command_docs: Option<CustomCommandDocs>,
  ) -> Result<(u8, u8)> {
    // 按类型名定位既有包装（C# TryGetFirstId(c => c.factory == factory) 的
    // 按名承接形态）；缺失即注册新类型。
    let type_key = type_name.to_lowercase();
    let type_slot = if let Some(&ext_id) = self.type_names.get(&type_key) {
      ext_id as usize
    } else {
      (self.register_new_type(&type_key)? - CUSTOM_OBJECT_TYPE_MIN_ID) as usize
    };

    let wrapper = self.object_commands[type_slot]
      .as_mut()
      .ok_or(Error::OutOfSpace)?;

    // 子命令 id 顺序分配
    let sc_id = wrapper.next_sub_id;
    if sc_id > u8::MAX as usize {
      return Err(Error::OutOfSpace);
    }
    wrapper.next_sub_id += 1;

    let ext_id = wrapper.ext_id;
    let arity = command_info.as_ref().map_or(0, |info| info.arity);
    let new_sub_cmd = CustomObjectCommand {
      name: name.to_lowercase(),
      ext_id,
      sub_id: sc_id as u8,
      command_type,
      arity,
      functions,
    };
    let slot = sc_id;
    if wrapper.command_map.len() <= slot {
      wrapper.command_map.resize_with(slot + 1, || None);
    }
    wrapper.command_map[slot] = Some(new_sub_cmd);

    self.track_registration(name, command_info, command_docs);
    Ok((ext_id, sc_id as u8))
  }

  /// 注册自定义过程（对应 C# CustomCommandManager.Register(CustomProcedure) 重载）；返回过程 id。
  pub fn register_procedure(
    &mut self,
    name: &str,
    command_info: Option<CustomCommandInfo>,
    command_docs: Option<CustomCommandDocs>,
  ) -> Result<u8> {
    let cmd_id = self
      .procedure_ids
      .try_get_next_id(|id| {
        self
          .custom_procedures
          .get(id as usize)
          .and_then(Option::as_ref)
          .is_some()
      })
      .ok_or(Error::OutOfSpace)?;

    let arity = command_info.as_ref().map_or(0, |info| info.arity);
    let new_cmd = CustomProcedureWrapper {
      name: name.to_lowercase(),
      id: cmd_id as u8,
      arity,
    };

    let slot = cmd_id as usize;
    if self.custom_procedures.len() <= slot {
      self.custom_procedures.resize_with(slot + 1, || None);
    }
    self.custom_procedures[slot] = Some(new_cmd);

    self.track_registration(name, command_info, command_docs);
    Ok(cmd_id as u8)
  }

  /// libs/server/Custom/CustomCommandManager.cs:TryGetCustomProcedure
  pub fn try_get_custom_procedure(&self, id: u8) -> Option<&CustomProcedureWrapper> {
    self
      .custom_procedures
      .get(id as usize)
      .and_then(Option::as_ref)
  }

  /// libs/server/Custom/CustomCommandManager.cs:TryGetCustomTransactionProcedure
  pub fn try_get_custom_transaction_procedure(&self, id: u8) -> Option<&CustomTransaction> {
    self
      .transaction_procs
      .get(id as usize)
      .and_then(Option::as_ref)
  }

  /// libs/server/Custom/CustomCommandManager.cs:TryGetCustomCommand
  pub fn try_get_custom_command(&self, id: u16) -> Option<&CustomRawStringCommand> {
    self
      .raw_string_commands
      .get(id as usize)
      .and_then(Option::as_ref)
  }

  /// libs/server/Custom/CustomCommandManager.cs:TryGetCustomObjectCommand
  ///
  /// `id` 为类型扩展 id（与 [`Self::register_type`] 的返回一致）。
  pub fn try_get_custom_object_command(&self, id: u8) -> Option<&CustomObjectCommandWrapper> {
    self
      .object_commands
      .get(id as usize)
      .and_then(Option::as_ref)
  }

  /// libs/server/Custom/CustomCommandManager.cs:TryGetCustomObjectSubCommand
  pub fn try_get_custom_object_sub_command(
    &self,
    id: u8,
    sub_id: u8,
  ) -> Option<&CustomObjectCommand> {
    self
      .try_get_custom_object_command(id)?
      .command_map
      .get(sub_id as usize)
      .and_then(Option::as_ref)
  }

  /// libs/server/Custom/CustomCommandManagerSession.cs:Match(CustomObjectCommand)
  ///
  /// 按名匹配自定义对象子命令（大小写不敏感，跨全部对象类型遍历）。
  pub fn match_object_command(&self, command: &[u8]) -> Option<&CustomObjectCommand> {
    self
      .object_commands
      .iter()
      .flatten()
      .flat_map(|w| w.command_map.iter().flatten())
      .find(|cmd| cmd.name.as_bytes().eq_ignore_ascii_case(command))
  }

  /// libs/server/Custom/CustomCommandManager.cs:GetCustomGarnetObjectType
  ///
  /// 类型扩展 id（0 基）→ 对象信封类型标签（C# `CustomObjectTypeMinId + id`
  /// 的数值强转承接；Rust 枚举未开放动态位，以原始 u8 承载同一持久化语义）。
  pub const fn object_type_tag(&self, ext_id: u8) -> u8 {
    CUSTOM_OBJECT_TYPE_MIN_ID + ext_id
  }

  /// 按名匹配自定义原始字符串命令（Match 语义；大小写不敏感，零堆分配比对）。
  pub fn match_raw_string_command(&self, command: &[u8]) -> Option<&CustomRawStringCommand> {
    self
      .raw_string_commands
      .iter()
      .flatten()
      .find(|cmd| cmd.name.as_bytes().eq_ignore_ascii_case(command))
  }

  /// libs/server/Custom/CustomCommandManager.cs:TryGetCustomCommandInfo
  pub fn try_get_custom_command_info(&self, cmd_name: &str) -> Option<&CustomCommandInfo> {
    self.custom_commands_info.get(&cmd_name.to_lowercase())
  }

  /// libs/server/Custom/CustomCommandManager.cs:IsCustomCommandRegistered
  ///
  /// 大小写不敏感的注册名查询（info-independent 名集合承接）。
  pub fn is_custom_command_registered(&self, cmd_name: &str) -> bool {
    if cmd_name.is_empty() {
      return false;
    }
    if cmd_name.bytes().all(|b| !b.is_ascii_uppercase()) {
      self.custom_command_names.contains(cmd_name)
    } else {
      self.custom_command_names.contains(&cmd_name.to_lowercase())
    }
  }

  /// libs/server/Custom/CustomCommandManager.cs:TryGetCustomCommandDocs
  pub fn try_get_custom_command_docs(&self, cmd_name: &str) -> Option<&CustomCommandDocs> {
    self.custom_commands_docs.get(&cmd_name.to_lowercase())
  }

  /// libs/server/Custom/CustomCommandManager.cs:GetAllCustomCommandsInfos
  pub fn get_all_custom_commands_infos(&self) -> Vec<(String, CustomCommandInfo)> {
    self
      .custom_commands_info
      .iter()
      .map(|(k, v)| (k.clone(), v.clone()))
      .collect()
  }

  /// libs/server/Custom/CustomCommandManager.cs:GetAllCustomCommandsDocs
  pub fn get_all_custom_commands_docs(&self) -> Vec<(String, CustomCommandDocs)> {
    self
      .custom_commands_docs
      .iter()
      .map(|(k, v)| (k.clone(), v.clone()))
      .collect()
  }

  /// libs/server/Custom/CustomCommandManager.cs:GetCustomCommandInfoCount
  pub fn get_custom_command_info_count(&self) -> usize {
    self.custom_commands_info.len()
  }

  /// libs/server/Custom/CustomCommandManager.cs:GetCustomRespCommand
  ///
  /// 扩展 id → RespCommand 的 repr 值（位图外的动态 id 区；
  /// Rust enum 未开放动态位，故以原始 repr 承载 C# 的数值强转语义）。
  pub fn get_custom_resp_command(&self, id: u16) -> u16 {
    CUSTOM_RAW_STRING_COMMAND_MIN_ID + id
  }

  /// libs/server/Custom/CustomCommandManager.cs:RegisterNewType
  ///
  /// 分配新对象类型并登记包装；返回类型 id。
  fn register_new_type(&mut self, type_name: &str) -> Result<u8> {
    let type_id = self
      .object_type_ids
      .try_get_next_id(|id| {
        let ext = (id - u64::from(CUSTOM_OBJECT_TYPE_MIN_ID)) as usize;
        self
          .object_commands
          .get(ext)
          .and_then(Option::as_ref)
          .is_some()
      })
      .ok_or(Error::OutOfSpace)?;

    let ext_id = (type_id - u64::from(CUSTOM_OBJECT_TYPE_MIN_ID)) as u8;
    let wrapper = CustomObjectCommandWrapper {
      ext_id,
      command_map: Vec::new(),
      next_sub_id: 0,
    };

    let slot = ext_id as usize;
    if self.object_commands.len() <= slot {
      self.object_commands.resize_with(slot + 1, || None);
    }
    self.object_commands[slot] = Some(wrapper);

    // 类型名登记（同名去重与按名定位依据；不入命令信息表，对齐 C#）。
    self.type_names.insert(type_name.to_lowercase(), ext_id);
    Ok(type_id as u8)
  }

  /// 登记名集合 / 信息 / 文档（对齐 C# Register 尾部的三表维护）。
  fn track_registration(
    &mut self,
    name: &str,
    command_info: Option<CustomCommandInfo>,
    command_docs: Option<CustomCommandDocs>,
  ) {
    let key = name.to_lowercase();
    self.custom_command_names.insert(key.clone());
    if let Some(info) = command_info {
      self.custom_commands_info.insert(key.clone(), info);
    }
    if let Some(docs) = command_docs {
      self.custom_commands_docs.insert(key, docs);
    }
  }
}

/// 共享句柄（会话域引用形态）。
pub type SharedCustomCommandManager = Arc<RwLock<CustomCommandManager>>;

#[cfg(test)]
mod tests {
  use wtxn::TxnProcedure;

  use super::*;

  fn info(name: &str, arity: i32) -> CustomCommandInfo {
    CustomCommandInfo {
      name: name.to_string(),
      arity,
      acl_categories: vec!["custom".to_string()],
    }
  }

  fn docs(name: &str) -> CustomCommandDocs {
    CustomCommandDocs {
      name: name.to_string(),
      summary: format!("{} docs", name),
    }
  }

  /// 执行体四接口测试桩（恒成功空操作）
  fn fns() -> CustomObjectFns {
    fn ok_initial(_args: &[&[u8]], _output: &mut Vec<u8>) -> bool {
      true
    }
    fn ok_update(_payload: &mut Vec<u8>, _args: &[&[u8]], _output: &mut Vec<u8>) -> bool {
      true
    }
    fn ok_read(_payload: &[u8], _args: &[&[u8]], _output: &mut Vec<u8>) -> bool {
      true
    }
    fn ok_missing(_args: &[&[u8]], _output: &mut Vec<u8>) {}
    fn never_empty(_payload: &[u8]) -> bool {
      false
    }
    CustomObjectFns {
      need_initial_update: ok_initial,
      updater: ok_update,
      reader: ok_read,
      not_found: ok_missing,
      is_empty: never_empty,
    }
  }

  fn echo_impl(args: &[&[u8]]) -> Vec<u8> {
    args.first().copied().unwrap_or(b"").to_vec()
  }

  fn echo_fn() -> RawStringFn {
    echo_impl
  }

  #[test]
  fn raw_string_command_registration_and_lookup() {
    let mut manager = CustomCommandManager::new();

    let id = manager
      .register_raw_string_command(RawStringCommandSpec {
        name: "MYCMD",
        command_type: CommandType::Read,
        functions: echo_fn(),
        command_info: Some(info("MYCMD", 2)),
        command_docs: Some(docs("MYCMD")),
        expiration_ticks: 0,
      })
      .unwrap();
    assert_eq!(id, 0);

    // 按 id 取回（含处理函数执行）
    let cmd = manager.try_get_custom_command(id).unwrap();
    assert_eq!(cmd.name, "mycmd");
    assert_eq!(cmd.arity, 2);
    assert_eq!(cmd.command_type, CommandType::Read);
    let output = (cmd.functions)(&[b"payload".as_slice()]);
    assert_eq!(output, b"payload".to_vec());

    // 按名匹配（大小写不敏感）
    assert!(manager.match_raw_string_command(b"myCmd").is_some());
    assert!(manager.match_raw_string_command(b"nope").is_none());

    // 注册名集合（不含信息也记录）
    assert!(manager.is_custom_command_registered("MyCmd"));
    assert!(!manager.is_custom_command_registered("Other"));

    // 信息/文档索引
    assert_eq!(manager.get_custom_command_info_count(), 1);
    assert_eq!(
      manager.try_get_custom_command_info("mycmd").unwrap().arity,
      2
    );
    assert!(manager.try_get_custom_command_docs("MYCMD").is_some());

    // RespCommand 映射：进入位图外动态区（repr 值承接 C# 数值强转语义）
    let resp_cmd_repr = manager.get_custom_resp_command(id);
    assert_eq!(resp_cmd_repr, CUSTOM_RAW_STRING_COMMAND_MIN_ID);
  }

  #[test]
  fn registration_space_exhaustion() {
    let mut manager = CustomCommandManager::new();
    // 注满 256 个原始字符串命令空间
    for i in 0..MAX_CUSTOM_RAW_STRING_COMMANDS {
      manager
        .register_raw_string_command(RawStringCommandSpec {
          name: &format!("cmd{i}"),
          command_type: CommandType::ReadModifyWrite,
          functions: echo_fn(),
          command_info: None,
          command_docs: None,
          expiration_ticks: 0,
        })
        .unwrap();
    }
    assert!(matches!(
      manager
        .register_raw_string_command(RawStringCommandSpec {
          name: "overflow",
          command_type: CommandType::ReadModifyWrite,
          functions: echo_fn(),
          command_info: None,
          command_docs: None,
          expiration_ticks: 0,
        })
        .unwrap_err(),
      crate::Error::OutOfSpace
    ));
  }

  #[test]
  fn object_type_and_sub_commands() {
    let mut manager = CustomCommandManager::new();

    // 注册类型
    let type_id = manager.register_type("MYOBJ").unwrap();
    assert_eq!(type_id, 0);
    // 同名类型重复注册报错
    assert!(manager.register_type("myobj").is_err());

    // 注册子命令
    let (ext_id, sub_id) = manager
      .register_object_command(
        "MYOBJ",
        "GETPROP",
        CommandType::Read,
        Some(fns()),
        Some(info("GETPROP", 1)),
        None,
      )
      .unwrap();
    assert_eq!((ext_id, sub_id), (0, 0));
    let (_, sub_id2) = manager
      .register_object_command(
        "MYOBJ",
        "SETPROP",
        CommandType::ReadModifyWrite,
        None,
        None,
        None,
      )
      .unwrap();
    assert_eq!(sub_id2, 1);

    // 子命令查询
    let sub = manager
      .try_get_custom_object_sub_command(ext_id, sub_id)
      .unwrap();
    assert_eq!(sub.name, "getprop");
    // 执行体随注册携带（None = 仅元数据登记）
    assert!(sub.functions.is_some());
    assert!(
      manager
        .try_get_custom_object_sub_command(ext_id, sub_id2)
        .unwrap()
        .functions
        .is_none()
    );
    assert!(
      manager
        .try_get_custom_object_sub_command(ext_id, 9)
        .is_none()
    );
    assert!(manager.try_get_custom_object_command(5).is_none());

    // 按名匹配（大小写不敏感，跨类型遍历）
    let matched = manager.match_object_command(b"GetProp").unwrap();
    assert_eq!(matched.sub_id, sub_id);
    assert!(matched.functions.is_some());
    assert!(manager.match_object_command(b"setprop2").is_none());

    // 对象信封类型标签：0x40 固定基址 + 类型扩展 id
    assert_eq!(manager.object_type_tag(ext_id), 0x40);

    // 命令信息仅含显式登记项（类型名不入信息表，对齐 C#）
    assert_eq!(manager.get_custom_command_info_count(), 1);
  }

  #[test]
  fn multiple_object_types_resolve_by_name() {
    let mut manager = CustomCommandManager::new();

    manager.register_type("TYPE_A").unwrap();
    let type_b = manager.register_type("TYPE_B").unwrap();
    assert_eq!(type_b, 1);

    // 向第二个类型注册子命令：必须挂到 TYPE_B，而非首个类型。
    let (ext_id, _) = manager
      .register_object_command("TYPE_B", "CMD_B", CommandType::Read, None, None, None)
      .unwrap();
    assert_eq!(ext_id, type_b);
    assert!(
      manager
        .try_get_custom_object_sub_command(type_b, 0)
        .is_some()
    );
    assert!(manager.try_get_custom_object_sub_command(0, 0).is_none());

    // 未注册类型自动补注册。
    let (ext_c, _) = manager
      .register_object_command("TYPE_C", "CMD_C", CommandType::Read, None, None, None)
      .unwrap();
    assert_eq!(ext_c, 2);
  }

  #[test]
  fn transactions_and_procedures() {
    let mut manager = CustomCommandManager::new();

    let txn_id = manager
      .register_transaction("MYTXN", None, Some(info("MYTXN", -3)), None)
      .unwrap();
    assert_eq!(txn_id, 0);
    let txn = manager
      .try_get_custom_transaction_procedure(txn_id)
      .unwrap();
    assert_eq!(txn.arity, -3);
    // 过程体工厂可实例化（C# entry.proc() 同径）
    assert_eq!(txn.id, 0);
    assert!(txn.factory.is_none());
    assert!(manager.try_get_custom_transaction_procedure(9).is_none());

    // 带工厂注册（独立管理器：工厂槽位 id = 0），实例可重建
    fn demo_factory() -> CustomTxnProc {
      CustomTxnProc::Default(crate::DefaultTxnProc { id: 0 })
    }

    let mut factory_manager = CustomCommandManager::new();
    let factory_id = factory_manager
      .register_transaction("MYTXN2", Some(demo_factory), None, None)
      .unwrap();
    assert_eq!(factory_id, 0);
    let factory_txn = factory_manager
      .try_get_custom_transaction_procedure(factory_id)
      .unwrap();
    let built = (factory_txn.factory.expect("factory"))();
    assert_eq!(built.id(), factory_id);

    let proc_id = manager
      .register_procedure("MYPROC", None, Some(docs("MYPROC")))
      .unwrap();
    assert_eq!(proc_id, 0);
    let proc = manager.try_get_custom_procedure(proc_id).unwrap();
    assert_eq!(proc.name, "myproc");

    // docs 可查
    assert!(manager.try_get_custom_command_docs("myproc").is_some());
    assert!(!manager.get_all_custom_commands_docs().is_empty());
    assert!(!manager.get_all_custom_commands_infos().is_empty());
  }

  #[test]
  fn id_ranges_match_csharp() {
    // 自定义原始命令区位于 INVALID 之前 256 个 id
    assert_eq!(CUSTOM_RAW_STRING_COMMAND_MAX_ID, u16::MAX - 1);
    assert_eq!(
      CUSTOM_RAW_STRING_COMMAND_MAX_ID - CUSTOM_RAW_STRING_COMMAND_MIN_ID + 1,
      MAX_CUSTOM_RAW_STRING_COMMANDS as u16
    );
    // 对象类型固定基址
    assert_eq!(CUSTOM_OBJECT_TYPE_MIN_ID, 0x40);
  }
}
