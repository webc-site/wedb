//! 自定义事务过程（对标 libs/server/Custom/CustomTransactionProcedure.cs:
//! abstract CustomTransactionProcedure 与 libs/server/InputHeader.cs:556
//! CustomProcedureInput 的 rust 投影：过程实例经注册表工厂产出，输入以参数序列绑定）。

use std::time::Duration;

use enum_dispatch::enum_dispatch;
use wbase::store_type::StoreType;
use wtxn::{
  LockType, SlotVerifyHandle, TransactionManager, TxnProcApi, TxnProcReadApi, TxnProcedure,
};

/// 自定义事务过程（C# CustomTransactionProcedure 抽象基类的 rust 承接：
/// [`TxnProcedure`] 三段式 + 输入绑定面）
#[enum_dispatch]
pub trait CustomTransactionProcedure: TxnProcedure {
  /// 绑定过程输入参数（C# `Prepare/Main(api, ref procInput)` 的 procInput 投影）
  fn bind_args(&mut self, _args: &[Vec<u8>]) {}

  /// 锁键登记与槽位校验（C# AddKey 四步的 rust 承接：AddTransactionStoreType →
  /// SaveKeyEntryToLock → VerifyKeyOwnership；第四段
  /// ComputeCustomProcShardedLogAccess 不移植——rust 分片子日志访问向量在
  /// 落盘时按锁集统一算（wtxn compute_sublog_access_vector），逐键累加属
  /// 重复机制）
  ///
  /// libs/server/Custom/CustomTransactionProcedure.cs:AddKey
  /// libs/server/Transaction/TxnKeyManager.cs:VerifyKeyOwnership
  fn add_key(
    &self,
    txn_manager: &mut TransactionManager,
    verifier: Option<&SlotVerifyHandle<'_>>,
    key: &[u8],
    lock_type: LockType,
    store_type: StoreType,
  ) {
    // C# AddKey:45 首步：并入事务触达存储面
    txn_manager.add_transaction_store_type(store_type);
    txn_manager.save_key_entry_to_lock(key, lock_type);
    if !txn_manager.is_replaying
      && let Some(v) = verifier
      && !v.network_iterative_slot_verify(key, lock_type == LockType::Shared)
    {
      txn_manager.abort();
    }
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

  fn prepare(
    &mut self,
    _txn_manager: &mut TransactionManager,
    _api: &mut dyn TxnProcReadApi,
    _verifier: Option<&SlotVerifyHandle<'_>>,
  ) -> bool {
    true
  }

  fn main(
    &mut self,
    _txn_manager: &mut TransactionManager,
    _api: &mut dyn TxnProcApi,
    _output: &mut Vec<u8>,
  ) {
  }

  fn finalize(
    &mut self,
    _txn_manager: &mut TransactionManager,
    _api: &mut dyn TxnProcApi,
    _output: &mut Vec<u8>,
  ) {
  }
}

impl CustomTransactionProcedure for DefaultTxnProc {}

/// 空操作事务过程（C# modules/NoOpModule/NoOpTxn.cs:NoOpTxn 的逐字承接：
/// 覆写基类两个抽象段为「准备恒真 + 主段空体」，不读参数、不登记键、
/// 不触存储、不写输出（输出空 → 宿主按 C# CustomRespCommands.cs:TryTransactionProc
/// 的空输出分支回 +OK））。
///
/// 与 [`DefaultTxnProc`] 的分别不在体段而在注册面：C# 抽象基类的 Prepare/Main
/// 为 abstract（无默认体），每个注册过程各自覆写，故每一号位各自成类型，
/// 空体亦然（本类型即 NoOpModule 注册名与元数所绑定的过程体）。
#[derive(Debug, Clone, Default)]
pub struct NoOpTxnProc {
  /// 过程注册 id
  pub id: u8,
}

impl TxnProcedure for NoOpTxnProc {
  fn id(&self) -> u8 {
    self.id
  }

  fn prepare(
    &mut self,
    _txn_manager: &mut TransactionManager,
    _api: &mut dyn TxnProcReadApi,
    _verifier: Option<&SlotVerifyHandle<'_>>,
  ) -> bool {
    true
  }

  fn main(
    &mut self,
    _txn_manager: &mut TransactionManager,
    _api: &mut dyn TxnProcApi,
    _output: &mut Vec<u8>,
  ) {
  }

  fn finalize(
    &mut self,
    _txn_manager: &mut TransactionManager,
    _api: &mut dyn TxnProcApi,
    _output: &mut Vec<u8>,
  ) {
  }
}

impl CustomTransactionProcedure for NoOpTxnProc {}

/// 静态分派的自定义事务过程枚举（消除虚表跳转）
#[enum_dispatch(CustomTransactionProcedure)]
#[derive(Debug, Clone)]
pub enum CustomTxnProc {
  Default(DefaultTxnProc),
  NoOp(NoOpTxnProc),
}

impl TxnProcedure for CustomTxnProc {
  fn id(&self) -> u8 {
    match self {
      Self::Default(p) => p.id(),
      Self::NoOp(p) => p.id(),
    }
  }

  fn fail_fast_on_key_lock_failure(&self) -> bool {
    match self {
      Self::Default(p) => p.fail_fast_on_key_lock_failure(),
      Self::NoOp(p) => p.fail_fast_on_key_lock_failure(),
    }
  }

  fn key_lock_timeout(&self) -> Duration {
    match self {
      Self::Default(p) => p.key_lock_timeout(),
      Self::NoOp(p) => p.key_lock_timeout(),
    }
  }

  fn prepare(
    &mut self,
    txn_manager: &mut TransactionManager,
    api: &mut dyn TxnProcReadApi,
    verifier: Option<&SlotVerifyHandle<'_>>,
  ) -> bool {
    match self {
      Self::Default(p) => p.prepare(txn_manager, api, verifier),
      Self::NoOp(p) => p.prepare(txn_manager, api, verifier),
    }
  }

  fn main(
    &mut self,
    txn_manager: &mut TransactionManager,
    api: &mut dyn TxnProcApi,
    output: &mut Vec<u8>,
  ) {
    match self {
      Self::Default(p) => p.main(txn_manager, api, output),
      Self::NoOp(p) => p.main(txn_manager, api, output),
    }
  }

  fn finalize(
    &mut self,
    txn_manager: &mut TransactionManager,
    api: &mut dyn TxnProcApi,
    output: &mut Vec<u8>,
  ) {
    match self {
      Self::Default(p) => p.finalize(txn_manager, api, output),
      Self::NoOp(p) => p.finalize(txn_manager, api, output),
    }
  }
}
