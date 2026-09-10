//! 命令注册 API（对标 libs/server/Servers/RegisterApi.cs:RegisterApi）
//!
//! C# 经 provider.StoreWrapper.customCommandManager 落注册表；托管面持
//! 自定义命令管理器句柄（Arc + Mutex，注册面为可变操作）。

use std::sync::Arc;

use parking_lot::Mutex as ParkingMutex;

use crate::custom::custom_command_manager::{
  CommandType, CustomCommandDocs, CustomCommandInfo, CustomCommandManager, CustomTransaction,
  RawStringFn,
};

/// 命令注册 API
pub struct RegisterApi {
  /// 自定义命令管理器（C# provider.StoreWrapper.customCommandManager）
  command_manager: Arc<ParkingMutex<CustomCommandManager>>,
}

impl RegisterApi {
  /// 构造注册 API
  ///
  /// libs/server/Servers/RegisterApi.cs:RegisterApi
  pub fn new(command_manager: Arc<ParkingMutex<CustomCommandManager>>) -> Self {
    Self { command_manager }
  }

  /// 注册自定义原始字符串命令，返回命令 id
  ///
  /// libs/server/Servers/RegisterApi.cs:NewCommand（RawString 重载）
  ///
  /// `expiration_ticks`：-1 清除既有过期；0 保持；>0 设定过期。
  pub fn new_command(
    &self,
    name: &str,
    command_type: CommandType,
    custom_functions: RawStringFn,
    command_info: Option<CustomCommandInfo>,
    command_docs: Option<CustomCommandDocs>,
    expiration_ticks: i64,
  ) -> Result<u16, &'static str> {
    self
      .command_manager
      .lock()
      .register_raw_string_command(RawStringCommandSpec {
        name,
        command_type,
        functions: custom_functions,
        command_info,
        command_docs,
        expiration_ticks,
      })
  }

  /// 注册自定义事务过程，返回事务 id
  ///
  /// libs/server/Servers/RegisterApi.cs:NewTransactionProc
  pub fn new_transaction_proc(
    &self,
    name: &str,
    command_info: Option<CustomCommandInfo>,
    command_docs: Option<CustomCommandDocs>,
  ) -> Result<u8, &'static str> {
    // C# 收 Func<CustomTransactionProcedure> 工厂；custom 域注册表以元数据
    // 承接（过程体随 custom 会话面接线）
    self
      .command_manager
      .lock()
      .register_transaction(name, command_info, command_docs)
  }

  /// 注册自定义对象类型，返回类型扩展 id
  ///
  /// libs/server/Servers/RegisterApi.cs:NewType
  ///
  /// C# 收工厂对象；custom 域以类型名登记承接（对象工厂随对象域接线）。
  pub fn new_type(&self, type_name: &str) -> Result<u8, &'static str> {
    self.command_manager.lock().register_type(type_name)
  }

  /// 注册自定义对象命令，返回（类型 id, 子命令 id）
  ///
  /// libs/server/Servers/RegisterApi.cs:NewCommand（对象重载）
  pub fn new_command_object(
    &self,
    type_name: &str,
    name: &str,
    command_type: CommandType,
    command_info: Option<CustomCommandInfo>,
    command_docs: Option<CustomCommandDocs>,
  ) -> Result<(u8, u8), &'static str> {
    self.command_manager.lock().register_object_command(
      type_name,
      name,
      command_type,
      command_info,
      command_docs,
    )
  }

  /// 注册自定义过程，返回过程 id
  ///
  /// libs/server/Servers/RegisterApi.cs:NewProcedure
  pub fn new_procedure(
    &self,
    name: &str,
    command_info: Option<CustomCommandInfo>,
    command_docs: Option<CustomCommandDocs>,
  ) -> Result<u8, &'static str> {
    self
      .command_manager
      .lock()
      .register_procedure(name, command_info, command_docs)
  }

  /// 注册自定义模块
  ///
  /// libs/server/Servers/RegisterApi.cs:NewModule
  ///
  /// C# 传入 ModuleBase 实例 + 模块参数；托管面以名称登记承接 OnLoad 语义
  /// （模块域桥接后接入真实回调）。
  pub fn new_module(&self, module_name: &str, version: u32) -> Result<(), &'static str> {
    self
      .command_manager
      .lock()
      .register_module(module_name, version)
  }

  /// 查注册的自定义事务过程（RUNTXP 路由用）
  pub fn get_custom_transaction_procedure(&self, txn_id: u8) -> Option<CustomTransaction> {
    self
      .command_manager
      .lock()
      .try_get_custom_transaction_procedure(txn_id)
  }
}

#[cfg(test)]
mod tests {
  use std::sync::Arc;

  use super::*;
  use crate::custom::custom_command_manager::CommandType;

  #[test]
  fn registers_commands_transactions_and_modules() {
    let manager = Arc::new(ParkingMutex::new(CustomCommandManager::new()));
    let api = RegisterApi::new(manager.clone());

    let functions: RawStringFn = Arc::new(|_args: &[&[u8]]| b"ok".to_vec());
    let cmd_id = api
      .new_command("MYCMD", CommandType::Read, functions, None, None, 0)
      .expect("原始命令注册成功");
    assert!(manager.lock().try_get_custom_command(cmd_id).is_some());

    let txn_id = api
      .new_transaction_proc("MYTXN", None, None)
      .expect("事务过程注册成功");
    assert!(api.get_custom_transaction_procedure(txn_id).is_some());

    let type_id = api.new_type("MyType").expect("类型注册成功");
    let (obj_type, sub_id) = api
      .new_command_object("MyType", "MYOBJCMD", CommandType::Read, None, None)
      .expect("对象命令注册成功");
    assert_eq!(obj_type, type_id);
    assert_eq!(sub_id, 0);

    let proc_id = api
      .new_procedure("MYPROC", None, None)
      .expect("过程注册成功");
    assert!(manager.lock().try_get_custom_procedure(proc_id).is_some());

    api.new_module("MyModule", 1).expect("模块注册成功");
    assert!(api.new_module("MyModule", 1).is_err()); // 同名重复
  }
}
