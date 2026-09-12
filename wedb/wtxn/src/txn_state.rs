//! 事务状态定义（对标 libs/server/Transaction/TxnState.cs:TxnState）

/// libs/server/Transaction/TxnState.cs:TxnState
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum TxnState {
  /// 非事务模式
  #[default]
  None,
  /// MULTI 已入队、命令进入跳过（排队）模式
  Started,
  /// EXEC 后的执行中（IsSkippingOperations 为 false 的窗口）
  Running,
  /// 槽校验失败等导致的中止（EXEC 时报 EXECABORT）
  Aborted,
}
