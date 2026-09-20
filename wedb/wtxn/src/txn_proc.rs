//! 排队命令元数据（C# SimpleRespCommandInfo 的协议无关投影；RESP 命令面在宿主侧）

use crate::txn_key_manager::TxnCommandKeys;

/// 排队命令元数据（C# SimpleRespCommandInfo 中 NetworkSKIP 所需子集的本域
/// 投影；宿主从 resp 命令信息域构建）
#[derive(Debug, Clone)]
pub struct TxnQueuedCommandInfo {
  /// 命令名（错误回显用）：命令名源面本就是编译期静态串（C# 侧同名串只存在于
  /// 进程启动期预构建的 SimpleRespCommandsInfo 静态表内，排队结构体本身不持名），
  /// 故此处直借而不逐命令复制
  pub name: &'static str,
  /// 元数（C# Arity；0 不校验 / 正值精确 / 负值至少）
  pub arity: i32,
  /// 是否允许出现在事务内（C# AllowedInTxn）
  pub allowed_in_txn: bool,
  /// 是否子命令（键参数窗口额外偏移；C# IsSubCommand，BITOP 同此论）
  pub is_sub_command: bool,
  /// 键登记元数据（C# KeySpecs 的检索窗口投影；None = 无键面）
  pub keys: Option<TxnCommandKeys>,
}
