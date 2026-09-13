//! 模块装载 SPI（对标 libs/server/Module/ModuleRegistrar.cs 与 ModuleBase）
//!
//! C# 模块经 `ModuleRegistrar.RegisterModule(module, moduleArgs)` 驱动
//! `ModuleBase.OnLoad(IModuleRegistry)`：模块在装载回调内经注册面向
//! CustomCommandManager 登记命令/类型/过程。Rust 侧以 [`GarnetModule`] +
//! [`ModuleLoadContext`] 同构承接：上下文持有注册表可变引用，转发
//! RegisterApi 同名注册语义（方法均为 [`crate::CustomCommandManager`]
//! 既有原语，零新注册路径）。

use crate::{
  CommandType, CustomCommandDocs, CustomCommandInfo, CustomCommandManager, RawStringCommandSpec,
  RawStringFn,
};

/// 模块装载上下文（C# IModuleRegistry 转发面）
///
/// 持注册表可变引用，方法族与 RegisterApi 一一同名同义
///（libs/server/Servers/RegisterApi.cs）
pub struct ModuleLoadContext<'a> {
  /// 自定义命令注册表（C# storeWrapper.customCommandManager）
  pub manager: &'a mut CustomCommandManager,
}

impl ModuleLoadContext<'_> {
  /// 注册自定义原始字符串命令，返回命令 id（RegisterApi:NewCommand）
  pub fn new_command(
    &mut self,
    name: &str,
    command_type: CommandType,
    custom_functions: RawStringFn,
    command_info: Option<CustomCommandInfo>,
    command_docs: Option<CustomCommandDocs>,
    expiration_ticks: i64,
  ) -> Result<u16, &'static str> {
    self
      .manager
      .register_raw_string_command(RawStringCommandSpec {
        name,
        command_type,
        functions: custom_functions,
        command_info,
        command_docs,
        expiration_ticks,
      })
  }

  /// 注册自定义事务过程，返回事务 id（RegisterApi:NewTransactionProc）
  pub fn new_transaction_proc(
    &mut self,
    name: &str,
    proc: crate::CustomTransactionProcFactory,
    command_info: Option<CustomCommandInfo>,
    command_docs: Option<CustomCommandDocs>,
  ) -> Result<u8, &'static str> {
    self
      .manager
      .register_transaction(name, proc, command_info, command_docs)
  }

  /// 注册自定义对象类型，返回类型扩展 id（RegisterApi:NewType）
  pub fn new_type(&mut self, type_name: &str) -> Result<u8, &'static str> {
    self.manager.register_type(type_name)
  }

  /// 注册自定义对象命令，返回（类型 id, 子命令 id）（RegisterApi:NewCommand 对象重载）
  pub fn new_command_object(
    &mut self,
    type_name: &str,
    name: &str,
    command_type: CommandType,
    command_info: Option<CustomCommandInfo>,
    command_docs: Option<CustomCommandDocs>,
  ) -> Result<(u8, u8), &'static str> {
    self
      .manager
      .register_object_command(type_name, name, command_type, command_info, command_docs)
  }

  /// 注册自定义过程，返回过程 id（RegisterApi:NewProcedure）
  pub fn new_procedure(
    &mut self,
    name: &str,
    command_info: Option<CustomCommandInfo>,
    command_docs: Option<CustomCommandDocs>,
  ) -> Result<u8, &'static str> {
    self
      .manager
      .register_procedure(name, command_info, command_docs)
  }

  /// 登记模块元数据（RegisterApi:NewModule）
  pub fn register_module(&mut self, module_name: &str, version: u32) -> Result<(), &'static str> {
    self.manager.register_module(module_name, version)
  }
}

/// Garnet 模块装载 SPI（C# ModuleBase:OnLoad 同位）
///
/// 宿主在装配期（服务器启动、命令面就绪前）逐模块驱动 [`Self::on_load`]；
/// 模块名与版本经 [`ModuleLoadContext::register_module`] 自登记。
pub trait GarnetModule: Send + Sync {
  /// 模块名（登记与诊断用）
  fn name(&self) -> &'static str;

  /// 模块版本号（C# ModuleBase.Version）
  fn version(&self) -> u32;

  /// 装载回调：经上下文注册本模块命令（C# ModuleBase.OnLoad）
  fn on_load(&self, ctx: &mut ModuleLoadContext<'_>);
}
