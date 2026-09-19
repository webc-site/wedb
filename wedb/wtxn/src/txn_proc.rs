//! 事务过程与排队命令元数据（C# SimpleRespCommandInfo / CustomTransactionProcedure
//! 的协议无关投影；RESP 命令面在宿主侧）

use crate::{transaction_manager::TransactionManager, txn_key_manager::TxnCommandKeys};

/// 排队命令元数据（C# SimpleRespCommandInfo 中 NetworkSKIP 所需子集的本域
/// 投影；宿主从 resp 命令信息域构建）
#[derive(Debug, Clone)]
pub struct TxnQueuedCommandInfo {
  /// 命令名（错误回显用）
  pub name: String,
  /// 元数（C# Arity；0 不校验 / 正值精确 / 负值至少）
  pub arity: i32,
  /// 是否允许出现在事务内（C# AllowedInTxn）
  pub allowed_in_txn: bool,
  /// 是否子命令（键参数窗口额外偏移；C# IsSubCommand，BITOP 同此论）
  pub is_sub_command: bool,
  /// 键登记元数据（C# KeySpecs 的检索窗口投影；None = 无键面）
  pub keys: Option<TxnCommandKeys>,
}

/// 自定义事务过程句柄（C# CustomTransactionProcedure 的元数据投影；
/// 执行体经 [`TxnProcResolver::try_transaction_proc`] 回调承接）
pub struct TxnProcHandle {
  /// 过程名
  pub name: String,
  /// 元数（C# arity；0 不校验 / 正值精确 / 负值至少）
  pub arity: i32,
}

/// 自定义事务过程解析面（C# customCommandManagerSession 的 RUNTXP 相关投影；
/// 宿主注册表接入时实现）
pub trait TxnProcResolver<S: ?Sized> {
  /// 取注册的自定义事务过程（C# GetCustomTransactionProcedure；未注册为
  /// None，对应 C# 抛异常路径）
  fn get_custom_transaction_procedure(&self, txn_id: u8) -> Option<TxnProcHandle>;
  /// 执行过程三段式（C# TryTransactionProc → RunTransactionProc）；输出
  /// 写入会话输出缓冲
  fn try_transaction_proc(
    &mut self,
    txn_id: u8,
    txn_manager: &mut TransactionManager,
    session: &mut S,
  ) -> bool;
}
