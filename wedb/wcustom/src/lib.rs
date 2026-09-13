//! WeDB 自定义扩展系统基座 (`wcustom`)
//!
//! 1:1 对标微软 Garnet 自定义扩展系统架构（`libs/server/Custom/`）：
//! - 自定义命令管理器 (`CustomCommandManager`)
//! - 自定义会话管理器 (`CustomCommandManagerSession`)
//! - 自定义命令注册 (`CustomCommandRegistration`)
//! - 自定义命令工具集 (`CustomCommandUtils`)
//! - 自定义对象抽象基底 (`CustomObjectBase`, `CustomObjectFunctions`)
//! - 自定义存储过程与事务基底 (`CustomProcedureBase`, `CustomTransactionProcedure`)
//! - 自定义原始字符串函数 (`CustomRawStringFunctions`)
//! - 自定义 RESP 命令调度与扩展表 (`CustomRespCommands`, `ExpandableMap`)

pub mod custom_command_manager;
pub mod custom_command_manager_session;
pub mod custom_command_registration;
pub mod custom_command_utils;
pub mod custom_object_base;
pub mod custom_object_functions;
pub mod custom_procedure_base;
pub mod custom_raw_string_functions;
pub mod custom_resp_commands;
pub mod custom_transaction_procedure;
pub mod expandable_map;
pub mod object_input_extensions;

pub use custom_command_manager::{
  CommandType, CustomCommandDocs, CustomCommandInfo, CustomCommandManager, CustomObjectCommand,
  CustomObjectCommandWrapper, CustomProcedureWrapper, CustomRawStringCommand, CustomTransaction,
  RawStringCommandSpec, RawStringFn, SharedCustomCommandManager,
};
pub use custom_command_manager_session::CustomCommandManagerSession;
pub use custom_command_registration::CustomCommandRegistration;
pub use custom_command_utils::CustomCommandUtils;
pub use custom_object_base::CustomObjectBase;
pub use custom_object_functions::CustomObjectFunctions;
pub use custom_procedure_base::CustomProcedureBase;
pub use custom_raw_string_functions::CustomRawStringFunctions;
pub use custom_resp_commands::CustomRespCommands;
pub use custom_transaction_procedure::CustomTransactionProcedure;
pub use expandable_map::ExpandableMap;
pub use object_input_extensions::ObjectInputExtensions;
