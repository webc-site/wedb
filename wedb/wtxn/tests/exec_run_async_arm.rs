//! 外部 EXEC compio 安全事务起点 `run_exec` 的争用重试闭环（对标
//! task/ing/wtxn-exec-lock-all-keys-sync-spin-starves-runtime.md 的 wtxn 域判据）
//!
//! 缺陷原貌：外部 EXEC 经 `run(false,false,ZERO)` → `lock_all_keys` 的
//! `thread::yield_now` 无界同步自旋取桶闩。compio 单核 worker 上该自旋霸占
//! 协程调度器，饿死同 worker 挂起持闩的阻塞命令（BLPOP-in-MULTI）→ 死锁。
//! `run_exec` 改为「单次尝试取闩 + 争用即返回 Contended（不 reset、不丢键集）」，
//! 由宿主会话登记既有慢臂让步重驱。本用例在无 compio 的纯线程环境下直证
//! `run_exec` 内核契约：
//!   1. 桶闩被外部持有时，`run_exec` 立即返回 `Contended`（绝不内部自旋挂死），
//!      事务态保持 `Started`、不 reset；
//!   2. 前置登记（WATCH 键并入 + 取全局版本）仅在首轮做一次——多轮 `Contended`
//!      复入键集计数与事务版本恒定，杜绝重复登记/重复消耗版本；
//!   3. 外部放闩后复入 `run_exec` 单次尝试成功 → `Started`（置 Running、WATCH
//!      校验通过），随后 `commit` 收尾复位。
//!
//! 自研依据: 事务异步执行臂（compio 运行时形态，C# TransactionalContext 同步面对应）

use std::{sync::Arc, time::Duration};

use wtxn::{
  ExecRun, LockType, TransactionManager, TxnKeyEntries, TxnKeyEntryComparison, TxnLockTable,
  TxnState, WatchVersionMap,
};
use wval::SessionPrefixBuf;

/// 根域会话前缀（WATCH 版本表与锁登记共用同一归属域，与生产恒有前缀形态一致）
fn root() -> SessionPrefixBuf {
  SessionPrefixBuf::ROOT
}

fn manager(table: &TxnLockTable) -> TransactionManager {
  TransactionManager::new(table.clone(), Arc::new(WatchVersionMap::new(64)), None)
}

#[test]
fn run_exec_contended_preserves_keyset_and_retries_to_started() {
  let table = TxnLockTable::new();
  let mut mgr = manager(&table);
  let key = b"run-exec-contention-key";
  let hash = TxnKeyEntryComparison::scoped_key_hash(root().as_slice(), key);

  // 外部占用者先以排他型锁住同桶（模拟 BLPOP-in-MULTI 挂起持排他闩的形态）
  let mut holder = TxnKeyEntries::new(1, table.clone());
  holder.add_key(hash, LockType::Exclusive);
  holder.lock_all_keys();

  // 事务侧登记：WATCH 单键 + 同键命令排他锁，二者同桶
  mgr.watch(root().as_slice(), key);
  mgr.save_key_entry_to_lock(root().as_slice(), key, LockType::Exclusive);
  mgr.state = TxnState::Started;
  // WATCH 键的锁集并入发生在 run_exec 前置，命令键登记即刻可见（仅 1）
  assert_eq!(
    mgr.key_entries.count(),
    1,
    "命令键即刻登记，WATCH 键并入延后"
  );

  // 首轮取闩：同桶被外部排他持有 → Contended，内核不持任何闩、不 reset
  assert_eq!(mgr.run_exec(root().as_slice()), ExecRun::Contended);
  assert_eq!(mgr.state, TxnState::Started, "争用不得改写出 Started 态");
  // 前置已做：命令键 + WATCH 键并入锁集（2）
  let stable_count = mgr.key_entries.count();
  assert_eq!(stable_count, 2, "首轮前置并入 WATCH 键后锁集定型");
  let version = mgr.txn_version;
  assert_ne!(version, 0, "首轮前置已取全局事务版本");

  // 复入争用：门控令前置登记只做一次——键集计数与版本跨轮恒定
  assert_eq!(mgr.run_exec(root().as_slice()), ExecRun::Contended);
  assert_eq!(mgr.run_exec(root().as_slice()), ExecRun::Contended);
  assert_eq!(
    mgr.key_entries.count(),
    stable_count,
    "争用重试轮不得重复登记键集"
  );
  assert_eq!(mgr.txn_version, version, "争用重试轮不得重复消耗事务版本");

  // 外部放闩 → 下一轮单次尝试成功 → Started（锁后收尾通过 WATCH 校验，置 Running）
  holder.unlock_all_keys();
  assert_eq!(mgr.run_exec(root().as_slice()), ExecRun::Started);
  assert_eq!(mgr.state, TxnState::Running);

  // 提交收尾复位：状态归零并清除门控位（下一笔事务须重新完整前置）
  mgr.commit(false).unwrap();
  assert_eq!(mgr.state, TxnState::None);
  assert_eq!(mgr.txn_version, 0, "复位清事务版本");
  assert_eq!(mgr.key_entries.count(), 0, "复位清键集");
}

#[test]
fn run_exec_uncontended_fast_path_starts_in_one_attempt() {
  let table = TxnLockTable::new();
  let mut mgr = manager(&table);
  let key = b"run-exec-fast-key";

  // 无外部争用：WATCH + 命令键，单次 run_exec 即 Started（快路径零让步轮）
  mgr.watch(root().as_slice(), key);
  mgr.save_key_entry_to_lock(root().as_slice(), key, LockType::Exclusive);
  mgr.state = TxnState::Started;
  assert_eq!(mgr.run_exec(root().as_slice()), ExecRun::Started);
  assert_eq!(mgr.state, TxnState::Running);
  assert_ne!(mgr.txn_version, 0);
  mgr.commit(false).unwrap();
}

#[test]
fn run_exec_lock_timeout_fail_fast_resets_via_run() {
  // 线程臂 `run` 快速失败（fail_fast + 零超时）保持既有语义：锁失败 → reset，
  // 与 `run_exec` 争用不 reset 形成对偶，确认两条臂互不污染。
  let table = TxnLockTable::new();
  let mut mgr = manager(&table);
  let key = b"run-failfast-key";
  let hash = TxnKeyEntryComparison::scoped_key_hash(root().as_slice(), key);

  let mut holder = TxnKeyEntries::new(1, table.clone());
  holder.add_key(hash, LockType::Exclusive);
  holder.lock_all_keys();

  mgr.save_key_entry_to_lock(root().as_slice(), key, LockType::Exclusive);
  mgr.state = TxnState::Started;
  assert!(
    !mgr.run(root().as_slice(), true, true, Duration::ZERO),
    "同桶被占、零超时快速失败应返回 false"
  );
  assert_eq!(mgr.state, TxnState::None, "线程臂锁失败路径复位事务");
  assert_eq!(mgr.key_entries.count(), 0, "线程臂锁失败路径清空键集");

  holder.unlock_all_keys();
}
