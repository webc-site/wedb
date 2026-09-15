//! 自定义事务过程（对标 libs/server/Custom/CustomTransactionProcedure.cs:
//! abstract CustomTransactionProcedure 与 libs/common/CustomProcedureInput.cs
//! 的 rust 投影：过程实例经注册表工厂产出，输入以参数序列绑定）。

use std::time::Duration;

use enum_dispatch::enum_dispatch;
use parking_lot::Mutex;
use wtxn::{LockType, TransactionManager, TxnProcedure};

/// 自定义事务过程（C# CustomTransactionProcedure 抽象基类的 rust 承接：
/// [`TxnProcedure`] 三段式 + 输入绑定面）。
#[enum_dispatch]
pub trait CustomTransactionProcedure: TxnProcedure {
  /// 绑定过程输入参数（C# `Prepare/Main(api, ref procInput)` 的 procInput 投影）
  fn bind_args(&mut self, _args: &[Vec<u8>]) {}

  /// 锁键登记（C# AddKey：SaveKeyEntryToLock + VerifyKeyOwnership 调用序）
  fn add_key(&self, txn_manager: &mut TransactionManager, key: &[u8], lock_type: LockType) {
    txn_manager.save_key_entry_to_lock(key, lock_type);
    txn_manager.verify_key_ownership(key, lock_type);
  }
}

/// 自定义事务过程默认承接位（C# 抽象基类的最小托管投影：默认空事务三段式）
#[derive(Debug, Clone, Default)]
pub struct DefaultTxnProc {
  /// 过程注册 id
  pub id: u8,
}

impl TxnProcedure for DefaultTxnProc {
  fn id(&self) -> u8 {
    self.id
  }

  fn prepare(&mut self, _txn_manager: &mut TransactionManager) -> bool {
    true
  }

  fn main(&mut self, _txn_manager: &mut TransactionManager, _output: &mut Vec<u8>) {}

  fn finalize(&mut self, _txn_manager: &mut TransactionManager, _output: &mut Vec<u8>) {}
}

impl CustomTransactionProcedure for DefaultTxnProc {}

/// 全局测试记录器：用于单元测试断言事务过程执行效果
pub static LAST_SET_KV: Mutex<Option<(Vec<u8>, Vec<u8>)>> = Mutex::new(None);

/// 测试与演示通用的键值事务过程（支持参数绑定与内存落盘验证）
#[derive(Debug, Clone, Default)]
pub struct SetTxnProc {
  /// 过程注册 id
  pub id: u8,
  /// 输入参数序列
  pub args: Vec<Vec<u8>>,
}

impl TxnProcedure for SetTxnProc {
  fn id(&self) -> u8 {
    self.id
  }

  fn prepare(&mut self, txn_manager: &mut TransactionManager) -> bool {
    for key in self.args.iter().step_by(2) {
      self.add_key(txn_manager, key, LockType::Exclusive);
    }
    !self.args.is_empty()
  }

  fn main(&mut self, _txn_manager: &mut TransactionManager, _output: &mut Vec<u8>) {
    if self.args.len() >= 2 {
      let mut guard = LAST_SET_KV.lock();
      *guard = Some((self.args[0].clone(), self.args[1].clone()));
    }
  }

  fn finalize(&mut self, _txn_manager: &mut TransactionManager, _output: &mut Vec<u8>) {}
}

impl CustomTransactionProcedure for SetTxnProc {
  fn bind_args(&mut self, args: &[Vec<u8>]) {
    self.args = args.to_vec();
  }
}

/// 静态分派的自定义事务过程枚举（消除虚表跳转）
#[enum_dispatch(CustomTransactionProcedure)]
#[derive(Debug, Clone)]
pub enum CustomTxnProc {
  Default(DefaultTxnProc),
  Set(SetTxnProc),
}

impl TxnProcedure for CustomTxnProc {
  fn id(&self) -> u8 {
    match self {
      Self::Default(p) => p.id(),
      Self::Set(p) => p.id(),
    }
  }

  fn fail_fast_on_key_lock_failure(&self) -> bool {
    match self {
      Self::Default(p) => p.fail_fast_on_key_lock_failure(),
      Self::Set(p) => p.fail_fast_on_key_lock_failure(),
    }
  }

  fn key_lock_timeout(&self) -> Duration {
    match self {
      Self::Default(p) => p.key_lock_timeout(),
      Self::Set(p) => p.key_lock_timeout(),
    }
  }

  fn prepare(&mut self, txn_manager: &mut TransactionManager) -> bool {
    match self {
      Self::Default(p) => p.prepare(txn_manager),
      Self::Set(p) => p.prepare(txn_manager),
    }
  }

  fn main(&mut self, txn_manager: &mut TransactionManager, output: &mut Vec<u8>) {
    match self {
      Self::Default(p) => p.main(txn_manager, output),
      Self::Set(p) => p.main(txn_manager, output),
    }
  }

  fn finalize(&mut self, txn_manager: &mut TransactionManager, output: &mut Vec<u8>) {
    match self {
      Self::Default(p) => p.finalize(txn_manager, output),
      Self::Set(p) => p.finalize(txn_manager, output),
    }
  }
}
