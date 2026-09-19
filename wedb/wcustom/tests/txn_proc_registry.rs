//! RUNTXP 事务过程注册表的运行时验证：槽位派发 → 三段式过程 → wtxn 事务管理器。
//!
//! 单元面（`src/txn_proc.rs`）只核编译期表自洽；本面把注册表里的过程实例真的
//! 跑进 `libs/server/Transaction/TransactionManager.cs:RunTransactionProc` 的
//! 对位实现，验证两件事：
//! - 已注册号位的过程在真事务管理器上三段跑得通，且过程体不触存储
//! - 过程体可用的键登记面（`libs/server/Custom/CustomTransactionProcedure.cs:AddKey`
//!   的「并入存储面 + 登记键 + 逐键属主校验」顺序、校验失败的由管理器裁决中止）
//!   语义正确，后续过程族移植据此接线
//! - 过程体可用的存储视图（`libs/server/API/IGarnetApi.cs:IGarnetReadApi`
//!   原语面：prepare 读即 WATCH 的只读界、main/finalize 读写界）三段契约跑通

use std::{
  cell::{Cell, RefCell},
  sync::Arc,
};

use wbase::store_type::StoreType;
use wcustom::{
  CustomTransactionProcedure, CustomTxnProc, REGISTERED_SLOTS, txn_proc, txn_proc_slot,
};
use wtxn::{
  LockType, SlotVerifyHandle, TransactionManager, TransactionStoreTypes, TxnLockTable, TxnProcApi,
  TxnProcReadApi, TxnProcedure, TxnSlotVerifyFace, TxnState, WatchVersionMap,
};

/// 单个测试独占一套事务管理器与锁表（锁面随管理器实例构造，对标 C# 锁表随
/// store 实例创建；跨测试互不干扰）
fn manager() -> TransactionManager {
  TransactionManager::new(
    TxnLockTable::new(),
    Arc::new(WatchVersionMap::new(64)),
    None,
  )
}

/// 槽位过程体对事务状态的期望：移植新过程时必须同步登记此处
///
/// 期望值取自 C# 过程体本身（`modules/NoOpModule/NoOpTxn.cs:NoOpTxn` 为空事务体
/// → 零键零写零输出；号位 0 是抽象基类的最小投影，同为空）。新增号位若漏登，
/// 本表直接 panic 报缺，防止「加了空壳常量当已移植」蒙过测试。
fn expected_touch(id: u8) -> (usize, bool, bool) {
  match id {
    txn_proc_slot::DEFAULT | txn_proc_slot::NOOP => (0, false, false),
    other => panic!(
      "号位 {other} 未登记触达期望：移植 C# 过程体后须在此按其 Prepare/Main 体段\
       补（键登记数 / 是否置写标志 / 是否自写输出）"
    ),
  }
}

/// 一段跑完后的事务状态快照
#[derive(Debug, Default, PartialEq, Eq)]
struct TxnSnapshot {
  /// 本事务登记的待锁键数
  keys: usize,
  /// 事务是否已标为含写
  perform_writes: bool,
  /// 事务状态
  state: TxnState,
}

fn snapshot(txn_manager: &TransactionManager) -> TxnSnapshot {
  TxnSnapshot {
    keys: txn_manager.key_entries.count(),
    perform_writes: txn_manager.perform_writes,
    state: txn_manager.state,
  }
}

/// 转调注册表过程并抄下段间状态的过程壳
struct Probe {
  inner: CustomTxnProc,
  after_prepare: TxnSnapshot,
  after_main: TxnSnapshot,
  finalize_ran: bool,
}

impl TxnProcedure for Probe {
  fn id(&self) -> u8 {
    self.inner.id()
  }

  fn prepare(
    &mut self,
    txn_manager: &mut TransactionManager,
    api: &mut dyn TxnProcReadApi,
    verifier: Option<&SlotVerifyHandle<'_>>,
  ) -> bool {
    let ran = self.inner.prepare(txn_manager, api, verifier);
    self.after_prepare = snapshot(txn_manager);
    ran
  }

  fn main(
    &mut self,
    txn_manager: &mut TransactionManager,
    api: &mut dyn TxnProcApi,
    output: &mut Vec<u8>,
  ) {
    self.inner.main(txn_manager, api, output);
    self.after_main = snapshot(txn_manager);
  }

  fn finalize(
    &mut self,
    txn_manager: &mut TransactionManager,
    api: &mut dyn TxnProcApi,
    output: &mut Vec<u8>,
  ) {
    self.inner.finalize(txn_manager, api, output);
    self.finalize_ran = true;
  }
}

/// 夹具过程：准备段按 C# 过程体的写法逐键 add_key，主段把已登记的键数回写输出
///
/// 用于取证注册表面（add_key / 中止裁决 / 回放短路）而非某个具体 C# 过程。
struct LockingProc {
  /// 过程槽位号（AOF StoredProcedure 条目的归属）
  id: u8,
  keys: Vec<(&'static [u8], LockType)>,
  /// C# AddKey 首参 storeType：并入事务触达存储面
  store_type: StoreType,
  /// 准备段收尾时刻的事务存储面快照（置位取证）
  store_types_after_prepare: Cell<TransactionStoreTypes>,
  finalize_ran: Cell<bool>,
}

impl LockingProc {
  fn new(id: u8, keys: Vec<(&'static [u8], LockType)>, store_type: StoreType) -> Self {
    Self {
      id,
      keys,
      store_type,
      store_types_after_prepare: Cell::new(TransactionStoreTypes::None),
      finalize_ran: Cell::new(false),
    }
  }
}

impl CustomTransactionProcedure for LockingProc {}

impl TxnProcedure for LockingProc {
  fn id(&self) -> u8 {
    self.id
  }

  fn prepare(
    &mut self,
    txn_manager: &mut TransactionManager,
    _api: &mut dyn TxnProcReadApi,
    verifier: Option<&SlotVerifyHandle<'_>>,
  ) -> bool {
    // C# AddKey 为 void：过程体不判属主结果，逐键登记后交由管理器裁决
    for index in 0..self.keys.len() {
      let (key, lock_type) = self.keys[index];
      self.add_key(txn_manager, verifier, key, lock_type, self.store_type);
    }
    self.store_types_after_prepare.set(txn_manager.store_types);
    true
  }

  fn main(
    &mut self,
    txn_manager: &mut TransactionManager,
    _api: &mut dyn TxnProcApi,
    output: &mut Vec<u8>,
  ) {
    // 跑进本段即键锁已备，把锁集规模回写为 RESP 字面值
    output.extend_from_slice(format!("+{}\r\n", txn_manager.key_entries.count()).as_bytes());
  }

  fn finalize(
    &mut self,
    _txn_manager: &mut TransactionManager,
    _api: &mut dyn TxnProcApi,
    _output: &mut Vec<u8>,
  ) {
    self.finalize_ran.set(true);
  }
}

/// 夹具过程：准备段即拒绝、收尾段自写 +OK（C# MSetPx.cs 形态——真效果全在
/// Finalize，Prepare 返回 false 后靠 finally 里的收尾段落线）
#[derive(Default)]
struct PrepareRejectProc {
  finalize_ran: Cell<bool>,
}

impl TxnProcedure for PrepareRejectProc {
  fn id(&self) -> u8 {
    txn_proc_slot::NOOP
  }

  fn prepare(
    &mut self,
    _txn_manager: &mut TransactionManager,
    _api: &mut dyn TxnProcReadApi,
    _verifier: Option<&SlotVerifyHandle<'_>>,
  ) -> bool {
    false
  }

  fn main(
    &mut self,
    _txn_manager: &mut TransactionManager,
    _api: &mut dyn TxnProcApi,
    output: &mut Vec<u8>,
  ) {
    output.extend_from_slice(b"+MAIN\r\n");
  }

  fn finalize(
    &mut self,
    _txn_manager: &mut TransactionManager,
    _api: &mut dyn TxnProcApi,
    output: &mut Vec<u8>,
  ) {
    self.finalize_ran.set(true);
    output.extend_from_slice(b"+OK\r\n");
  }
}

/// 记录逐键属主校验请求的校验切面
#[derive(Default)]
struct RecordingVerifier {
  /// 每次校验的 (键, 只读锁位)
  calls: RefCell<Vec<(Vec<u8>, bool)>>,
  /// 批次起点重置次数
  resets: Cell<usize>,
  /// 令第 n 次（1 起）校验失败，None = 全放行
  deny_at: Option<usize>,
}

impl TxnSlotVerifyFace for RecordingVerifier {
  fn reset_cached_slot_verification_result(&self) {
    self.resets.set(self.resets.get() + 1);
  }

  fn network_iterative_slot_verify(&self, key: &[u8], read_only: bool) -> bool {
    let mut calls = self.calls.borrow_mut();
    calls.push((key.to_vec(), read_only));
    self.deny_at != Some(calls.len())
  }

  fn write_cached_slot_verification_message(&self, output: &mut Vec<u8>) {
    output.extend_from_slice(b"-MOVED slot verification failed\r\n");
  }
}

#[test]
fn registered_slot_ids_are_the_only_dispatchable() {
  // 全号位扫一遍：派发命中集必须恰等于登记表，杜绝「加常量不加体」的半注册
  for id in 0..=u8::MAX {
    let dispatched = txn_proc(id).is_some();
    assert_eq!(
      dispatched,
      REGISTERED_SLOTS.contains(&id),
      "号位 {id} 的派发结果与登记表不一致"
    );
    assert_eq!(
      wcustom::txn_proc_meta(id).is_some(),
      dispatched,
      "号位 {id} 的过程元数据与派发结果不一致"
    );
  }
}

#[test]
fn registered_procs_run_in_real_transaction() {
  for &id in REGISTERED_SLOTS {
    let (want_keys, want_writes, want_output) = expected_touch(id);
    let mut txn_manager = manager();
    let verifier = RecordingVerifier::default();
    let handle: SlotVerifyHandle<'_> = &verifier;

    let mut probe = Probe {
      inner: txn_proc(id).expect("登记表号位必可派发"),
      after_prepare: TxnSnapshot::default(),
      after_main: TxnSnapshot::default(),
      finalize_ran: false,
    };
    assert_eq!(probe.id(), id, "过程实例自报号位与注册号位不一致");

    let mut view = MockView::default();
    let mut output = Vec::new();
    let ran = txn_manager.run_transaction_proc(
      &mut probe,
      b"",
      &mut output,
      false,
      Some(&handle),
      &mut view,
    );
    assert!(ran, "号位 {id} 的过程在真事务管理器上跑不通");

    // 准备段：体段登记的键数与写标志须与 C# 过程体一致，此刻尚未取锁
    assert_eq!(
      probe.after_prepare.keys, want_keys,
      "号位 {id} 准备段登记键数与 C# 过程体不符"
    );
    assert_eq!(
      probe.after_prepare.perform_writes, want_writes,
      "号位 {id} 准备段写标志与 C# 过程体不符"
    );
    assert_eq!(
      probe.after_prepare.state,
      TxnState::None,
      "号位 {id} 主段前事务状态不应已置起"
    );

    // 主段：键锁齐备后才执行，此刻事务为 Running
    assert_eq!(
      probe.after_main,
      TxnSnapshot {
        keys: want_keys,
        perform_writes: want_writes,
        state: TxnState::Running,
      },
      "号位 {id} 主段的事务状态不符"
    );
    assert_eq!(
      output.is_empty(),
      !want_output,
      "号位 {id} 过程体自写输出的期望与 C# 不符（输出空则宿主回 +OK）"
    );

    // 收尾段跑过；提交即复位：锁集清空、写标志落下、事务回 None
    assert!(probe.finalize_ran, "号位 {id} 的收尾段被跳过");
    assert_eq!(
      snapshot(&txn_manager),
      TxnSnapshot::default(),
      "号位 {id} 跑完后事务未复位"
    );
    assert_eq!(
      verifier.resets.get(),
      1,
      "号位 {id} 未在批次起点重置迭代槽校验缓存"
    );
    assert!(
      verifier.calls.borrow().is_empty(),
      "号位 {id} 未登记键却做了属主校验"
    );
  }
}

#[test]
fn add_key_locks_each_key_and_verifies_in_order() {
  let mut txn_manager = manager();
  let verifier = RecordingVerifier::default();
  let handle: SlotVerifyHandle<'_> = &verifier;
  let mut proc = LockingProc::new(
    txn_proc_slot::NOOP,
    vec![
      (b"txnproc/k1", LockType::Exclusive),
      (b"txnproc/k2", LockType::Shared),
    ],
    StoreType::Object,
  );

  let mut view = MockView::default();
  let mut output = Vec::new();
  let ran =
    txn_manager.run_transaction_proc(&mut proc, b"", &mut output, false, Some(&handle), &mut view);
  assert!(ran, "两键登记后主段须跑通");
  // C# AddKey:45 首步：AddTransactionStoreType 在登记键前置位（准备段收尾时刻已并入）
  assert_eq!(
    proc.store_types_after_prepare.get(),
    TransactionStoreTypes::Object,
    "add_key 须先把 storeType 并入事务存储面"
  );
  assert_eq!(
    txn_manager.store_types,
    TransactionStoreTypes::None,
    "提交复位后事务存储面须清零"
  );

  assert_eq!(
    verifier.calls.borrow().as_slice(),
    &[
      (b"txnproc/k1".to_vec(), false),
      (b"txnproc/k2".to_vec(), true)
    ],
    "C# AddKey 对每个键校验一次，只读锁位须随锁类型传下去"
  );
  assert_eq!(output, b"+2\r\n", "主段须看到两键全部入锁集");
  assert!(proc.finalize_ran.get(), "正常路径须跑收尾段");
  assert_eq!(txn_manager.key_entries.count(), 0, "提交后锁集须已释放清空");
}

#[test]
fn denied_slot_ownership_aborts_transaction_before_main() {
  let mut txn_manager = manager();
  let verifier = RecordingVerifier {
    deny_at: Some(2),
    ..Default::default()
  };
  let handle: SlotVerifyHandle<'_> = &verifier;
  let mut proc = LockingProc::new(
    txn_proc_slot::NOOP,
    vec![
      (b"txnproc/granted", LockType::Exclusive),
      (b"txnproc/denied", LockType::Exclusive),
      (b"txnproc/unreached", LockType::Exclusive),
    ],
    StoreType::Main,
  );

  let mut view = MockView::default();
  let mut output = Vec::new();
  let ran =
    txn_manager.run_transaction_proc(&mut proc, b"", &mut output, false, Some(&handle), &mut view);
  assert!(!ran, "属主校验失败须中止事务");

  assert_eq!(
    verifier.calls.borrow().len(),
    3,
    "C# AddKey 不判属主结果，三键全登记后由管理器裁决中止"
  );
  assert_eq!(
    output, b"-MOVED slot verification failed\r\n",
    "中止时须写出缓存的槽位校验错误，且主段不得再跑"
  );
  assert_eq!(
    snapshot(&txn_manager),
    TxnSnapshot::default(),
    "中止后事务须已复位，不留残锁"
  );
  assert!(
    proc.finalize_ran.get(),
    "收尾段对标 C# finally：中止早退路径同样跑过"
  );
}

#[test]
fn prepare_rejection_still_runs_finalize_and_its_output_lands() {
  let mut txn_manager = manager();
  let verifier = RecordingVerifier::default();
  let handle: SlotVerifyHandle<'_> = &verifier;
  let mut proc = PrepareRejectProc::default();

  let mut view = MockView::default();
  let mut output = Vec::new();
  let ran =
    txn_manager.run_transaction_proc(&mut proc, b"", &mut output, false, Some(&handle), &mut view);
  assert!(!ran, "准备段拒绝须返回 false");
  assert!(
    proc.finalize_ran.get(),
    "收尾段对标 C# finally：准备段早退路径同样跑过"
  );
  assert_eq!(
    output, b"+OK\r\n",
    "主段不得跑（无 +MAIN），收尾段自写的 +OK 即全部输出——宿主侧非空即写回线上"
  );
  assert_eq!(
    snapshot(&txn_manager),
    TxnSnapshot::default(),
    "早退后事务须已复位，收尾段跑在无锁、TxnState::None 态上"
  );
}

#[test]
fn replay_path_skips_slot_verification_and_finalize() {
  let mut txn_manager = manager();
  let verifier = RecordingVerifier::default();
  let handle: SlotVerifyHandle<'_> = &verifier;
  let mut proc = LockingProc::new(
    txn_proc_slot::NOOP,
    vec![(b"txnproc/replayed", LockType::Exclusive)],
    StoreType::Main,
  );

  let mut view = MockView::default();
  let mut output = Vec::new();
  let ran =
    txn_manager.run_transaction_proc(&mut proc, b"", &mut output, true, Some(&handle), &mut view);
  assert!(ran, "回放段须照常跑完过程体");

  assert!(
    verifier.calls.borrow().is_empty(),
    "回放段不重复属主校验（C# VerifyKeyOwnership 只在正常路径生效）"
  );
  assert_eq!(output, b"+1\r\n", "回放段仍登记键并跑主段");
  assert!(
    !proc.finalize_ran.get(),
    "回放期跳过收尾段（C# Finalize 不参与 AOF 恢复）"
  );
}

/// 过程存储视图夹具（内存键值投影；三段式对视图的契约验证不需要真存储，
/// wnode 宿主 [`wnode::storage::session::txn_proc_view::TxnProcView`] 在
/// 存储执行域用同一 [`TxnProcApi`] 契约跑真存储）
#[derive(Default)]
struct MockView {
  rows: Vec<(Vec<u8>, Vec<u8>)>,
}

impl MockView {
  fn preset(key: &[u8], val: &[u8]) -> Self {
    Self {
      rows: vec![(key.to_vec(), val.to_vec())],
    }
  }
}

impl TxnProcApi for MockView {
  fn get(&mut self, key: &[u8]) -> Option<Vec<u8>> {
    self
      .rows
      .iter()
      .find(|(k, _)| k == key)
      .map(|(_, v)| v.clone())
  }

  fn set(&mut self, key: &[u8], val: &[u8]) -> bool {
    match self.rows.iter_mut().find(|(k, _)| k == key) {
      Some(slot) => slot.1 = val.to_vec(),
      None => self.rows.push((key.to_vec(), val.to_vec())),
    }
    true
  }

  fn setex(&mut self, key: &[u8], val: &[u8], _expiry_ticks: i64) -> bool {
    // 夹具无 TTL 域：写值即 SET 语义
    self.set(key, val)
  }

  fn delete(&mut self, key: &[u8]) -> bool {
    let before = self.rows.len();
    self.rows.retain(|(k, _)| k != key);
    self.rows.len() != before
  }

  fn increment(&mut self, key: &[u8], delta: i64) -> Option<i64> {
    let cur = self
      .get(key)
      .and_then(|v| String::from_utf8(v).ok()?.parse::<i64>().ok())
      .unwrap_or(0);
    let next = cur.checked_add(delta)?;
    self.set(key, next.to_string().as_bytes());
    Some(next)
  }

  fn sorted_set_add(&mut self, _key: &[u8], _score: f64, _member: &[u8]) -> bool {
    unreachable!("存储契约夹具不触有序集原语")
  }

  fn sorted_set_remove(&mut self, _key: &[u8], _member: &[u8]) -> bool {
    unreachable!("存储契约夹具不触有序集原语")
  }
}

/// 夹具过程：三段各用一面（C# Prepare 只读界 → Main 读写界 → Finalize
/// 非事务界）。准备段读预置门键按值裁决；主段写入后从视图真读回（断言
/// 落值而非「调用发生过」）；收尾段读主段提交值自写输出（C# Finalize
/// 可读写 + 产输出的投影）。
struct StoreRoundTripProc {
  /// 准备段读的门键：值 == "deny" 即拒绝（C# Prepare 返回 false）
  gate_key: &'static [u8],
  /// 主段写读回的键
  main_key: &'static [u8],
  finalize_ran: Cell<bool>,
}

impl CustomTransactionProcedure for StoreRoundTripProc {}

impl TxnProcedure for StoreRoundTripProc {
  fn id(&self) -> u8 {
    txn_proc_slot::NOOP
  }

  fn prepare(
    &mut self,
    txn_manager: &mut TransactionManager,
    api: &mut dyn TxnProcReadApi,
    _verifier: Option<&SlotVerifyHandle<'_>>,
  ) -> bool {
    // 读即 WATCH（C# GarnetWatchApi）：只读界把读键并入监视集
    let gate = api.get(txn_manager, self.gate_key);
    txn_manager.save_key_entry_to_lock(self.main_key, LockType::Exclusive);
    gate.as_deref() != Some(b"deny".as_slice())
  }

  fn main(
    &mut self,
    _txn_manager: &mut TransactionManager,
    api: &mut dyn TxnProcApi,
    _output: &mut Vec<u8>,
  ) {
    assert!(api.set(self.main_key, b"v2"), "主段写入须成功");
    assert_eq!(
      api.get(self.main_key).as_deref(),
      Some(b"v2".as_slice()),
      "主段须从视图真读回刚写入的值"
    );
  }

  fn finalize(
    &mut self,
    _txn_manager: &mut TransactionManager,
    api: &mut dyn TxnProcApi,
    output: &mut Vec<u8>,
  ) {
    self.finalize_ran.set(true);
    // 收尾段自写输出：主段提交值随提交对视图可见（C# Finalize 直连视图）
    match api.get(self.main_key) {
      Some(v) => {
        output.extend_from_slice(format!("+{}\r\n", String::from_utf8_lossy(&v)).as_bytes())
      }
      None => output.extend_from_slice(b"+nil\r\n"),
    }
  }
}

#[test]
fn store_view_round_trips_across_three_phases() {
  let mut txn_manager = manager();
  let mut view = MockView::preset(b"txnproc/gate", b"go");
  let mut proc = StoreRoundTripProc {
    gate_key: b"txnproc/gate",
    main_key: b"txnproc/main",
    finalize_ran: Cell::new(false),
  };

  let mut output = Vec::new();
  let ran = txn_manager.run_transaction_proc(&mut proc, b"", &mut output, false, None, &mut view);
  assert!(ran, "三段存储契约在真事务管理器上须跑通");
  assert!(proc.finalize_ran.get(), "正常路径须跑收尾段");
  assert_eq!(
    output, b"+v2\r\n",
    "收尾段读主段提交值并自写输出——宿主侧非空即写回线上"
  );
  assert_eq!(
    view.get(b"txnproc/main").as_deref(),
    Some(b"v2".as_slice()),
    "提交后写入值对存储视图可见"
  );
}

#[test]
fn prepare_read_gate_denies_by_view_value() {
  let mut txn_manager = manager();
  // 预置门键 = deny：prepare 经只读视图读到即拒绝（对标 C# Prepare false 路径）
  let mut view = MockView::preset(b"txnproc/gate", b"deny");
  let mut proc = StoreRoundTripProc {
    gate_key: b"txnproc/gate",
    main_key: b"txnproc/main",
    finalize_ran: Cell::new(false),
  };

  let mut output = Vec::new();
  let ran = txn_manager.run_transaction_proc(&mut proc, b"", &mut output, false, None, &mut view);
  assert!(!ran, "只读视图读得 deny 值须拒绝事务");
  assert!(
    proc.finalize_ran.get(),
    "收尾段对标 C# finally：准备段早退路径同样跑过"
  );
  assert!(view.get(b"txnproc/main").is_none(), "中止路径主段不得落写");
}
