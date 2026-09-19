//! RUNTXP 迭代槽校验直穿集成测试（对标 C#
//! libs/server/Custom/CustomTransactionProcedure.cs:AddKey 内联
//! VerifyKeyOwnership 与 TransactionManager.cs:RunTransactionProcInternal
//! 的直穿校验链：reset 缓存 → prepare 逐键 add_key 直验 →
//! 首败置 Aborted → 缓存错误落线 → Reset）

use parking_lot::Mutex;
use wtxn::{
  LockType, SlotVerifyHandle, TransactionManager, TxnProcApi, TxnProcReadApi, TxnProcedure,
  TxnSlotVerifyFace,
};

mod common;

use common::manager;

/// 记录型校验切面：按登记序捕获逐次校验调用，可注入失败键集
struct RecordingVerifier {
  /// reset 缓存触发次数
  resets: Mutex<usize>,
  /// 逐键校验调用序列（键、只读位）
  calls: Mutex<Vec<(Vec<u8>, bool)>>,
  /// 恒判失败的键
  denied: Vec<&'static [u8]>,
}

impl RecordingVerifier {
  fn new(denied: Vec<&'static [u8]>) -> Self {
    Self {
      resets: Mutex::new(0),
      calls: Mutex::new(Vec::new()),
      denied,
    }
  }
}

impl TxnSlotVerifyFace for RecordingVerifier {
  fn reset_cached_slot_verification_result(&self) {
    *self.resets.lock() += 1;
  }

  fn network_iterative_slot_verify(&self, key: &[u8], read_only: bool) -> bool {
    self.calls.lock().push((key.to_vec(), read_only));
    !self.denied.contains(&key)
  }

  fn write_cached_slot_verification_message(&self, output: &mut Vec<u8>) {
    output.extend_from_slice(b"-MOVED 42 127.0.0.1:7379\r\n");
  }
}

/// 登记指定键序列的最小过程（prepare 逐键直验 +
/// save_key_entry_to_lock，同 C# AddKey 调用序）
struct KeyRegisteringProc {
  /// (键, 锁型) 登记序列
  keys: Vec<(&'static [u8], LockType)>,
}

impl TxnProcedure for KeyRegisteringProc {
  fn id(&self) -> u8 {
    7
  }

  fn prepare(
    &mut self,
    txn_manager: &mut TransactionManager,
    _api: &mut dyn TxnProcReadApi,
    verifier: Option<&SlotVerifyHandle<'_>>,
  ) -> bool {
    for (key, lock_type) in &self.keys {
      txn_manager.save_key_entry_to_lock(key, *lock_type);
      if !txn_manager.is_replaying
        && let Some(v) = verifier
        && !v.network_iterative_slot_verify(key, *lock_type == LockType::Shared)
      {
        txn_manager.abort();
      }
    }
    true
  }

  fn main(
    &mut self,
    _txn_manager: &mut TransactionManager,
    _api: &mut dyn TxnProcApi,
    output: &mut Vec<u8>,
  ) {
    output.extend_from_slice(b"+PONG\r\n");
  }

  fn finalize(
    &mut self,
    _txn_manager: &mut TransactionManager,
    _api: &mut dyn TxnProcApi,
    _output: &mut Vec<u8>,
  ) {
  }
}

/// 零行为视图（槽校验链测试的过程体不触存储，视图仅签名在场）
struct NoopView;

impl TxnProcApi for NoopView {
  fn get(&mut self, _key: &[u8]) -> Option<Vec<u8>> {
    None
  }

  fn set(&mut self, _key: &[u8], _val: &[u8]) -> bool {
    false
  }

  fn setex(&mut self, _key: &[u8], _val: &[u8], _expiry_ticks: i64) -> bool {
    false
  }

  fn delete(&mut self, _key: &[u8]) -> bool {
    false
  }

  fn increment(&mut self, _key: &[u8], _delta: i64) -> Option<i64> {
    None
  }

  fn sorted_set_add(&mut self, _key: &[u8], _score: f64, _member: &[u8]) -> bool {
    false
  }

  fn sorted_set_remove(&mut self, _key: &[u8], _member: &[u8]) -> bool {
    false
  }
}

#[test]
fn pass_path_verifies_registered_keys_in_order_with_lock_types() {
  let mut txn = manager();
  let verifier = RecordingVerifier::new(Vec::new());
  let handle: SlotVerifyHandle<'_> = &verifier;
  let mut proc = KeyRegisteringProc {
    keys: vec![
      (b"a", LockType::Exclusive),
      (b"b", LockType::Shared),
      (b"c", LockType::Exclusive),
    ],
  };
  let mut output = Vec::new();
  // 槽校验链测试不触存储：视图缺席（传 NoOp 形态）不影响断言面
  let mut view = NoopView;
  assert!(txn.run_transaction_proc(&mut proc, b"", &mut output, false, Some(&handle), &mut view));
  // 切面缺席短路不成立时 reset 恰好一次，逐键调用按登记序、锁型折算只读位
  assert_eq!(*verifier.resets.lock(), 1);
  assert_eq!(
    verifier.calls.lock().as_slice(),
    &[
      (b"a".to_vec(), false),
      (b"b".to_vec(), true),
      (b"c".to_vec(), false),
    ]
  );
  assert!(output.starts_with(b"+PONG"));
}

#[test]
fn denied_key_aborts_and_writes_cached_message() {
  let mut txn = manager();
  let verifier = RecordingVerifier::new(vec![b"b"]);
  let handle: SlotVerifyHandle<'_> = &verifier;
  let mut proc = KeyRegisteringProc {
    keys: vec![
      (b"a", LockType::Exclusive),
      (b"b", LockType::Shared),
      (b"c", LockType::Exclusive),
    ],
  };
  let mut output = Vec::new();
  let mut view = NoopView;
  let ok = txn.run_transaction_proc(&mut proc, b"", &mut output, false, Some(&handle), &mut view);
  // 逐键直验对标 C# AddKey：全部键均经历直验（b 失败置 Aborted，c 依然被调用），
  // 事务中止、缓存错误落线、主段未执行（无 PONG）
  assert!(!ok);
  assert_eq!(output, b"-MOVED 42 127.0.0.1:7379\r\n");
  assert_eq!(
    verifier.calls.lock().as_slice(),
    &[
      (b"a".to_vec(), false),
      (b"b".to_vec(), true),
      (b"c".to_vec(), false),
    ]
  );
  // Reset 闭环：状态回 None、锁集清空
  assert!(txn.txn_keys.is_empty());
}

#[test]
fn verify_interleaves_with_registration() {
  let mut txn = manager();
  let verifier = RecordingVerifier::new(vec![b"mid"]);
  let handle: SlotVerifyHandle<'_> = &verifier;
  let mut proc = KeyRegisteringProc {
    keys: vec![
      (b"first", LockType::Exclusive),
      (b"mid", LockType::Shared),
      (b"last", LockType::Exclusive),
    ],
  };
  let mut output = Vec::new();
  let mut view = NoopView;
  let ok = txn.run_transaction_proc(&mut proc, b"", &mut output, false, Some(&handle), &mut view);
  // mid 失败后，后续 last 仍执行 save_key_entry_to_lock 与校验，事务最终 Aborted，main 未执行
  assert!(!ok);
  assert_eq!(output, b"-MOVED 42 127.0.0.1:7379\r\n");
  assert_eq!(
    verifier.calls.lock().as_slice(),
    &[
      (b"first".to_vec(), false),
      (b"mid".to_vec(), true),
      (b"last".to_vec(), false),
    ]
  );
}

#[test]
fn stale_watch_key_not_iteratively_verified() {
  let mut txn = manager();
  // WATCH 键入 EXEC 多键面（txn_keys），不落入过程 prepare 的 add_key
  txn.watch(b"stale");
  let verifier = RecordingVerifier::new(vec![b"stale"]);
  let handle: SlotVerifyHandle<'_> = &verifier;
  let mut proc = KeyRegisteringProc {
    keys: vec![(b"fresh", LockType::Exclusive)],
  };
  let mut output = Vec::new();
  // 槽校验链测试不触存储：视图缺席（传 NoOp 形态）不影响断言面
  let mut view = NoopView;
  assert!(txn.run_transaction_proc(&mut proc, b"", &mut output, false, Some(&handle), &mut view));
  assert_eq!(
    verifier.calls.lock().as_slice(),
    &[(b"fresh".to_vec(), false)]
  );
}

#[test]
fn same_key_two_lock_types_verified_twice() {
  let mut txn = manager();
  let verifier = RecordingVerifier::new(Vec::new());
  let handle: SlotVerifyHandle<'_> = &verifier;
  // 同键先 Shared 后 Exclusive：C# AddKey 逐次校验各取当次锁型，逐次直验
  let mut proc = KeyRegisteringProc {
    keys: vec![(b"k", LockType::Shared), (b"k", LockType::Exclusive)],
  };
  let mut output = Vec::new();
  // 槽校验链测试不触存储：视图缺席（传 NoOp 形态）不影响断言面
  let mut view = NoopView;
  assert!(txn.run_transaction_proc(&mut proc, b"", &mut output, false, Some(&handle), &mut view));
  assert_eq!(
    verifier.calls.lock().as_slice(),
    &[(b"k".to_vec(), true), (b"k".to_vec(), false)]
  );
}

#[test]
fn replay_path_skips_verification() {
  let mut txn = manager();
  let verifier = RecordingVerifier::new(vec![b"a"]);
  let handle: SlotVerifyHandle<'_> = &verifier;
  let mut proc = KeyRegisteringProc {
    keys: vec![(b"a", LockType::Exclusive)],
  };
  let mut output = Vec::new();
  // 回放双闸钉死：调用侧 None 闸（切面缺席）+ IsReplaying 闸。带切面回放
  // （Some + is_replaying）时 reset 缓存照常一次（C# ResetCache 不看
  // IsReplaying），add_key 内 IsReplaying 短路——denied 键不触校验仍放行
  let mut view = NoopView;
  assert!(txn.run_transaction_proc(&mut proc, b"", &mut output, true, Some(&handle), &mut view));
  assert_eq!(*verifier.resets.lock(), 1);
  assert!(verifier.calls.lock().is_empty());
  let mut view = NoopView;
  assert!(txn.run_transaction_proc(&mut proc, b"", &mut output, true, None, &mut view));
  assert_eq!(*verifier.resets.lock(), 1);
}
