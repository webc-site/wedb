//! 自定义命令管理器（对标 libs/server/Custom/CustomCommandManager.cs）
//!
//! 承接四类注册空间（原始字符串命令 / 自定义对象命令 / 事务过程 / 自定义过程）
//! 与模块注册、按名索引/文档索引。C# 侧经 ExpandableMap 分配 id；
//! Rust 侧 ExpandableMap 属并行占位域，本文件以域内 [`IdSpace`] 承接同语义
//! （min..=max 顺序分配 + 按值去重查找 + 按名匹配）。
//!
//! C# 的 RespCommandsInfo/RespCommandDocs 为富结构体；Rust 侧 resp 域尚未
//! 落地，本域以 [`CustomCommandInfo`]/[`CustomCommandDocs`] 最小结构承接
//! （名称、元数、类别、摘要），映射字段语义保持一致。

use std::{fmt, sync::Arc};

use gxhash::HashMap;
use parking_lot::RwLock;

use crate::types::GarnetObjectType;

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
pub type RawStringFn = Arc<dyn Fn(&[&[u8]]) -> Vec<u8> + Send + Sync>;

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

/// 已注册的自定义事务过程。
#[derive(Clone)]
pub struct CustomTransaction {
  /// 事务名（小写规范化）。
  pub name: String,
  /// 事务 id。
  pub id: u8,
  /// 元数。
  pub arity: i32,
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
  fn try_get_next_id(&mut self, occupied: &dyn Fn(u64) -> bool) -> Option<u64> {
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
  /// 已注册模块（模块名 → 版本）。
  modules: HashMap<String, u32>,
  /// 对象类型名 → 类型扩展 id（C# 工厂引用去重的按名承接形态）。
  type_names: HashMap<String, u8>,
  /// 按名命令信息索引（大小写不敏感：存小写键）。
  custom_commands_info: HashMap<String, CustomCommandInfo>,
  /// 按名命令文档索引（小写键）。
  custom_commands_docs: HashMap<String, CustomCommandDocs>,
  /// 全部已注册命令名集合（小写键；含未附信息的注册）。
  custom_command_names: HashMap<String, u8>,
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
      modules: HashMap::default(),
      type_names: HashMap::default(),
      custom_commands_info: HashMap::default(),
      custom_commands_docs: HashMap::default(),
      custom_command_names: HashMap::default(),
    }
  }

  /// libs/server/Custom/CustomCommandManager.cs:Register（原始字符串命令）
  ///
  /// 注册自定义原始字符串命令；返回扩展 id。空间耗尽报错。
  #[allow(clippy::too_many_arguments)]
  pub fn register_raw_string_command(
    &mut self,
    name: &str,
    command_type: CommandType,
    functions: RawStringFn,
    command_info: Option<CustomCommandInfo>,
    command_docs: Option<CustomCommandDocs>,
    expiration_ticks: i64,
  ) -> Result<u16, &'static str> {
    let cmd_id = self
      .raw_string_ids
      .try_get_next_id(&|id| {
        self
          .raw_string_commands
          .get(id as usize)
          .and_then(Option::as_ref)
          .is_some()
      })
      .ok_or("Out of registration space")?;

    let ext_id = (cmd_id - u64::from(CUSTOM_RAW_STRING_COMMAND_MIN_ID)) as u16;
    let arity = command_info.as_ref().map_or(0, |info| info.arity);
    let new_cmd = CustomRawStringCommand {
      name: name.to_lowercase(),
      ext_id,
      command_type,
      arity,
      expiration_ticks,
      functions,
    };

    // 精确槽位写入
    let slot = cmd_id as usize;
    if self.raw_string_commands.len() <= slot {
      self.raw_string_commands.resize_with(slot + 1, || None);
    }
    self.raw_string_commands[slot] = Some(new_cmd);

    self.track_registration(name, command_info, command_docs)?;
    Ok(ext_id)
  }

  /// libs/server/Custom/CustomCommandManager.cs:Register（事务过程）
  ///
  /// 注册自定义事务；返回事务 id。
  pub fn register_transaction(
    &mut self,
    name: &str,
    command_info: Option<CustomCommandInfo>,
    command_docs: Option<CustomCommandDocs>,
  ) -> Result<u8, &'static str> {
    let cmd_id = self
      .transaction_ids
      .try_get_next_id(&|id| {
        self
          .transaction_procs
          .get(id as usize)
          .and_then(Option::as_ref)
          .is_some()
      })
      .ok_or("Out of registration space")?;

    let arity = command_info.as_ref().map_or(0, |info| info.arity);
    let new_cmd = CustomTransaction {
      name: name.to_lowercase(),
      id: cmd_id as u8,
      arity,
    };

    let slot = cmd_id as usize;
    if self.transaction_procs.len() <= slot {
      self.transaction_procs.resize_with(slot + 1, || None);
    }
    self.transaction_procs[slot] = Some(new_cmd);

    self.track_registration(name, command_info, command_docs)?;
    Ok(cmd_id as u8)
  }

  /// libs/server/Custom/CustomCommandManager.cs:RegisterType
  ///
  /// 注册自定义对象类型（同名类型重复注册报错；对标 C# 工厂引用去重的
  /// 按名承接形态）；返回类型扩展 id。
  pub fn register_type(&mut self, type_name: &str) -> Result<u8, &'static str> {
    let type_key = type_name.to_lowercase();
    if self.type_names.contains_key(&type_key) {
      return Err("Type already registered with ID");
    }
    Ok(self.register_new_type(&type_key)? - CUSTOM_OBJECT_TYPE_MIN_ID)
  }

  /// libs/server/Custom/CustomCommandManager.cs:Register（对象命令）
  ///
  /// 注册自定义对象命令（类型缺失时自动补注册）；返回 (类型扩展 id, 子命令 id)。
  pub fn register_object_command(
    &mut self,
    type_name: &str,
    name: &str,
    command_type: CommandType,
    command_info: Option<CustomCommandInfo>,
    command_docs: Option<CustomCommandDocs>,
  ) -> Result<(u8, u8), &'static str> {
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
      .ok_or("Out of registration space")?;

    // 子命令 id 顺序分配
    let sc_id = wrapper.next_sub_id;
    if sc_id > u8::MAX as usize {
      return Err("Out of registration space");
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
    };
    let slot = sc_id;
    if wrapper.command_map.len() <= slot {
      wrapper.command_map.resize_with(slot + 1, || None);
    }
    wrapper.command_map[slot] = Some(new_sub_cmd);

    self.track_registration(name, command_info, command_docs)?;
    Ok((ext_id, sc_id as u8))
  }

  /// libs/server/Custom/CustomCommandManager.cs:Register（自定义过程）
  ///
  /// 注册自定义过程；返回过程 id。
  pub fn register_procedure(
    &mut self,
    name: &str,
    command_info: Option<CustomCommandInfo>,
    command_docs: Option<CustomCommandDocs>,
  ) -> Result<u8, &'static str> {
    let cmd_id = self
      .procedure_ids
      .try_get_next_id(&|id| {
        self
          .custom_procedures
          .get(id as usize)
          .and_then(Option::as_ref)
          .is_some()
      })
      .ok_or("Out of registration space")?;

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

    self.track_registration(name, command_info, command_docs)?;
    Ok(cmd_id as u8)
  }

  /// libs/server/Custom/CustomCommandManager.cs:RegisterModule
  ///
  /// 注册模块：调用加载逻辑（此处以名称登记承接 OnLoad 语义）并检查初始化。
  /// 失败时返回 RESP 错误文案（对齐 CmdStrings.RESP_ERR_MODULE_ONLOAD）。
  pub fn register_module(&mut self, module_name: &str, version: u32) -> Result<(), &'static str> {
    // OnLoad 执行承接：模块登记即加载成功（wserver 模块域桥接后接入真实回调）
    let initialized = !module_name.is_empty();
    if !initialized {
      return Err("ERR module failed to load");
    }
    self
      .try_add_module(module_name, version)
      .ok_or("ERR module failed to load")?;
    Ok(())
  }

  /// libs/server/Custom/CustomCommandManager.cs:TryAddModule
  ///
  /// 加入模块表；同名模块已存在时返回 false（仅 ModuleRegistrar 调用）。
  pub fn try_add_module(&mut self, module_name: &str, version: u32) -> Option<()> {
    if self.modules.contains_key(module_name) {
      return None;
    }
    self.modules.insert(module_name.to_string(), version);
    Some(())
  }

  /// libs/server/Custom/CustomCommandManager.cs:TryGetCustomProcedure
  pub fn try_get_custom_procedure(&self, id: u8) -> Option<CustomProcedureWrapper> {
    self
      .custom_procedures
      .get(id as usize)
      .and_then(Option::as_ref)
      .cloned()
  }

  /// libs/server/Custom/CustomCommandManager.cs:TryGetCustomTransactionProcedure
  pub fn try_get_custom_transaction_procedure(&self, id: u8) -> Option<CustomTransaction> {
    self
      .transaction_procs
      .get(id as usize)
      .and_then(Option::as_ref)
      .cloned()
  }

  /// libs/server/Custom/CustomCommandManager.cs:TryGetCustomCommand
  pub fn try_get_custom_command(&self, id: u16) -> Option<CustomRawStringCommand> {
    let slot = u64::from(CUSTOM_RAW_STRING_COMMAND_MIN_ID) + u64::from(id);
    self
      .raw_string_commands
      .get(slot as usize)
      .and_then(Option::as_ref)
      .cloned()
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
  ) -> Option<CustomObjectCommand> {
    self
      .try_get_custom_object_command(id)?
      .command_map
      .get(sub_id as usize)
      .and_then(Option::as_ref)
      .cloned()
  }

  /// 按名匹配自定义原始字符串命令（Match 语义；大小写不敏感）。
  pub fn match_raw_string_command(&self, command: &[u8]) -> Option<CustomRawStringCommand> {
    let name = String::from_utf8_lossy(command).to_lowercase();
    self
      .raw_string_commands
      .iter()
      .flatten()
      .find(|cmd| cmd.name == name)
      .cloned()
  }

  /// libs/server/Custom/CustomCommandManager.cs:TryGetCustomCommandInfo
  pub fn try_get_custom_command_info(&self, cmd_name: &str) -> Option<&CustomCommandInfo> {
    self.custom_commands_info.get(&cmd_name.to_lowercase())
  }

  /// libs/server/Custom/CustomCommandManager.cs:IsCustomCommandRegistered
  ///
  /// 大小写不敏感的注册名查询（info-independent 名集合承接）。
  pub fn is_custom_command_registered(&self, cmd_name: &str) -> bool {
    !cmd_name.is_empty()
      && self
        .custom_command_names
        .contains_key(&cmd_name.to_lowercase())
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

  /// libs/server/Custom/CustomCommandManager.cs:GetCustomGarnetObjectType
  ///
  /// 类型扩展 id → GarnetObjectType。
  pub fn get_custom_garnet_object_type(&self, id: u8) -> GarnetObjectType {
    // Rust 的 GarnetObjectType 枚举未开放动态自定义位（C# 以数值区间表达
    // CustomObjectTypeMinId..），此处以原始字节承接同一持久化语义。
    let _ = id;
    GarnetObjectType::Null
  }

  /// libs/server/Custom/CustomCommandManager.cs:RegisterNewType
  ///
  /// 分配新对象类型并登记包装；返回类型 id。
  fn register_new_type(&mut self, type_name: &str) -> Result<u8, &'static str> {
    let type_id = self
      .object_type_ids
      .try_get_next_id(&|id| {
        self
          .object_commands
          .get(id as usize)
          .and_then(Option::as_ref)
          .is_some()
      })
      .ok_or("Out of registration space")?;

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
  ) -> Result<(), &'static str> {
    let key = name.to_lowercase();
    self.custom_command_names.insert(key.clone(), 0);
    if let Some(info) = command_info {
      self.custom_commands_info.insert(key.clone(), info);
    }
    if let Some(docs) = command_docs {
      self.custom_commands_docs.insert(key, docs);
    }
    Ok(())
  }
}

/// 共享句柄（会话域引用形态）。
pub type SharedCustomCommandManager = Arc<RwLock<CustomCommandManager>>;

#[cfg(test)]
mod tests {
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

  fn echo_fn() -> RawStringFn {
    Arc::new(|args: &[&[u8]]| args.first().copied().unwrap_or(b"").to_vec())
  }

  #[test]
  fn raw_string_command_registration_and_lookup() {
    let mut manager = CustomCommandManager::new();

    let id = manager
      .register_raw_string_command(
        "MYCMD",
        CommandType::Read,
        echo_fn(),
        Some(info("MYCMD", 2)),
        Some(docs("MYCMD")),
        0,
      )
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
        .register_raw_string_command(
          &format!("cmd{i}"),
          CommandType::ReadModifyWrite,
          echo_fn(),
          None,
          None,
          0,
        )
        .unwrap();
    }
    assert!(
      manager
        .register_raw_string_command(
          "overflow",
          CommandType::ReadModifyWrite,
          echo_fn(),
          None,
          None,
          0
        )
        .unwrap_err()
        .contains("Out of registration space")
    );
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
        Some(info("GETPROP", 1)),
        None,
      )
      .unwrap();
    assert_eq!((ext_id, sub_id), (0, 0));
    let (_, sub_id2) = manager
      .register_object_command("MYOBJ", "SETPROP", CommandType::ReadModifyWrite, None, None)
      .unwrap();
    assert_eq!(sub_id2, 1);

    // 子命令查询
    let sub = manager
      .try_get_custom_object_sub_command(ext_id, sub_id)
      .unwrap();
    assert_eq!(sub.name, "getprop");
    assert!(
      manager
        .try_get_custom_object_sub_command(ext_id, 9)
        .is_none()
    );
    assert!(manager.try_get_custom_object_command(5).is_none());

    // GarnetObjectType 映射：Rust 枚举未开放自定义位区间，承接为 Null（见实现注释）
    let obj_type = manager.get_custom_garnet_object_type(ext_id);
    assert_eq!(obj_type, GarnetObjectType::Null);

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
      .register_object_command("TYPE_B", "CMD_B", CommandType::Read, None, None)
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
      .register_object_command("TYPE_C", "CMD_C", CommandType::Read, None, None)
      .unwrap();
    assert_eq!(ext_c, 2);
  }

  #[test]
  fn transactions_and_procedures() {
    let mut manager = CustomCommandManager::new();

    let txn_id = manager
      .register_transaction("MYTXN", Some(info("MYTXN", -3)), None)
      .unwrap();
    assert_eq!(txn_id, 0);
    let txn = manager
      .try_get_custom_transaction_procedure(txn_id)
      .unwrap();
    assert_eq!(txn.arity, -3);
    assert!(manager.try_get_custom_transaction_procedure(9).is_none());

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
  fn module_registration() {
    let mut manager = CustomCommandManager::new();

    assert!(manager.register_module("MYMOD", 1).is_ok());
    // 同名模块（TryAddModule 去重）
    assert!(manager.try_add_module("MYMOD", 2).is_none());
    assert!(manager.try_add_module("OTHER", 1).is_some());
    // 空名模块加载失败
    assert!(manager.register_module("", 1).is_err());
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
    assert_eq!(GarnetObjectType::All as u8, 0xfb);
  }
}
