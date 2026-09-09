#[derive(Debug, PartialEq)]
pub enum TxnState {
  None,
  Started,
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

  pub fn commit(&mut self, _internal_txn: bool) {
    if self.state == TxnState::Started {
      self.state = TxnState::None;
    }
  }

  pub fn abort(&mut self) {
    self.state = TxnState::Aborted;
  }

  // Keeping stubs for compilation
  pub fn run_transaction_proc(&self) {
    Default::default()
  }
  pub fn run_transaction_proc_internal(&self) {
    Default::default()
  }
  pub fn is_skipping_operations(&self) {
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
