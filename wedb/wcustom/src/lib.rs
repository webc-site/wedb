//! WeDB 自定义扩展系统基座 (`wcustom`)
//!
//! 对标微软 Garnet 自定义扩展系统架构（`libs/server/Custom/`）：
//! - 自定义命令管理器 (`CustomCommandManager`)
//! - 自定义存储过程与事务基底 (`CustomTransactionProcedure`, `CustomTxnProc`)

mod custom_command_manager;
mod custom_transaction_procedure;
mod error;

pub use custom_command_manager::{
  CommandType, CustomCommandDocs, CustomCommandInfo, CustomCommandManager, CustomObjectCommand,
  CustomObjectCommandWrapper, CustomObjectFns, CustomProcedureWrapper, CustomRawStringCommand,
  CustomTransaction, RawStringCommandSpec, RawStringFn, SharedCustomCommandManager, TxnProcFactory,
};
pub use custom_transaction_procedure::{
  CustomTransactionProcedure, CustomTxnProc, DefaultTxnProc, LAST_SET_KV, SetTxnProc,
};
pub use error::{Error, Result};
