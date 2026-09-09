/// libs/server/Transaction/TxnState.cs:TxnState
#[derive(Debug, PartialEq)]
pub enum TxnState {
  None,
  Started,
  /// EXEC/事务过程执行中（IsSkippingOperations 为 false 的窗口）
  Running,
  Aborted,
}

pub struct TransactionManager {
  pub state: TxnState,
}

impl TransactionManager {
  pub fn new() -> Self {
    Self {
      state: TxnState::None,
    }
  }

  /// libs/server/Transaction/TransactionManager.cs:BeginTransaction
  pub fn begin_transaction(&mut self) {
    self.state = TxnState::Started;
  }

  /// libs/server/Transaction/TransactionManager.cs:Reset（收尾置 None；
  /// C# 的 TxnCommit AOF 入队与锁释放随执行器接线一并补齐）
  pub fn commit(&mut self, _internal_txn: bool) {
    if self.state == TxnState::Started || self.state == TxnState::Running {
      self.state = TxnState::None;
    }
  }

  /// libs/server/Transaction/TransactionManager.cs:Abort
  pub fn abort(&mut self) {
    self.state = TxnState::Aborted;
  }

  /// libs/server/Transaction/TransactionManager.cs:IsSkippingOperations
  pub fn is_skipping_operations(&self) -> bool {
    self.state == TxnState::Started || self.state == TxnState::Aborted
  }

  // Keeping stubs for compilation
  pub fn run_transaction_proc(&self) {
    Default::default()
  }
  pub fn run_transaction_proc_internal(&self) {
    Default::default()
  }
  pub fn watch(&self) {
    Default::default()
  }
  pub fn add_transaction_store_types(&self) {
    Default::default()
  }
  pub fn add_transaction_store_type(&self) {
    Default::default()
  }
  pub fn get_lockset(&self) {
    Default::default()
  }
  pub fn get_slot_verification_input(&self) {
    Default::default()
  }
  pub fn locks_acquired(&self) {
    Default::default()
  }
  pub fn run(&self) {
    Default::default()
  }
  pub fn compute_custom_proc_sharded_log_access(&self) {
    Default::default()
  }
  pub fn compute_sublog_access_vector(&self) {
    Default::default()
  }
  pub fn promote_to_transaction(&self) {
    Default::default()
  }
}

impl Default for TransactionManager {
  fn default() -> Self {
    Self::new()
  }
}
