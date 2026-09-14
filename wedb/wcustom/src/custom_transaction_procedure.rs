//! 自定义事务过程（对标 libs/server/Custom/CustomTransactionProcedure.cs:
//! abstract CustomTransactionProcedure 与 libs/common/CustomProcedureInput.cs
//! 的 rust 投影：过程实例经注册表工厂产出，输入以参数序列绑定）。

use wtxn::{TransactionManager, TxnProcedure};

/// 自定义事务过程（C# CustomTransactionProcedure 抽象基类的 rust 承接：
/// [`TxnProcedure`] 三段式 + 输入绑定面）。实现方经 [`Self::bind_args`]
/// 接收 AOF / 会话两侧的过程输入（C# procInput 引用参数的值投影），
/// 经 [`TxnProcFactory`](crate::TxnProcFactory) 闭包工厂产出（工厂自持
/// 存储句柄，C# proc.Main(api) 的等价承载）。
pub trait CustomTransactionProcedure: TxnProcedure {
  /// 绑定过程输入参数（C# `Prepare/Main(api, ref procInput)` 的 procInput
  /// 投影；无参过程默认忽略）。
  fn bind_args(&mut self, _args: &[Vec<u8>]) {}

  /// 锁键登记（C# AddKey：SaveKeyEntryToLock + VerifyKeyOwnership +
  /// ComputeCustomProcShardedLogAccess；集群槽校验在单机域解耦）。
  ///
  /// 排他锁型自动标记事务含写操作（决定 AOF 事务条目是否记录）。
  fn add_key(
    &self,
    txn_manager: &mut TransactionManager,
    key: &[u8],
    lock_type: wtxn::LockType,
  ) {
    txn_manager.save_key_entry_to_lock(key, lock_type);
  }
}

/// 自定义事务过程默认承接位（C# 抽象基类的最小托管投影：默认空事务
/// 三段式；SPI 模块与无存储访问场景的最小过程体，真实过程体自实现
/// [`CustomTransactionProcedure`] 经工厂接入）。
#[derive(Debug, Clone)]
pub struct DefaultTxnProc {
  /// 过程注册 id（C# entry.proc() 实例化后的归属 id）
  pub id: u8,
}

impl TxnProcedure for DefaultTxnProc {
  fn id(&self) -> u8 {
    self.id
  }

  fn prepare(&mut self, _txn_manager: &mut TransactionManager) -> bool {
    // 默认空集 = 无键事务（C# Prepare 默认）
    true
  }

  fn main(&mut self, _txn_manager: &mut TransactionManager, _output: &mut Vec<u8>) {
    // 默认无输出 → 空事务提交回 +OK
  }

  fn finalize(&mut self, _txn_manager: &mut TransactionManager, _output: &mut Vec<u8>) {
    // 默认收尾为空
  }
}

impl CustomTransactionProcedure for DefaultTxnProc {}
