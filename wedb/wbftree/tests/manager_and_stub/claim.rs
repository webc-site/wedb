//! 快照防重入认领 (per-tree claim) 的串行等待与生命周期回归测试
//!
//! 对标 libs/server/Resp/RangeIndex/RangeIndexManager.cs:TreeEntry.SnapshotUnderClaim
//! 的 C# 契约：`while (!TryClaimSnapshot()) Thread.Yield();` + `finally ReleaseSnapshot()`
//! 为**无超时**串行等待，认领释放由 RAII 守卫兜底。本组测试锁定修复后的真实行为：
//! 同树并发认领只多等、永不失败，且不引入无界挂死（守卫必然归还）。
//!
//! 确定性约定（严禁 sleep / 挂钟断言）：持有窗的释放由「等待线程置位的推进信号」
//! 触发，完成与否由 channel 阻塞收取与快照文件存在性、原子量状态判定，机器负载
//! 抖动只改变自旋轮数、绝不改变断言结果。
//!
//! 自研依据: doc/zh/collection.md 存根句柄认领（RIPROMOTE/RIRESTORE 保序）

use std::{
  fs, result,
  sync::{Arc, atomic::Ordering, mpsc},
  thread,
};

use aok::{OK, Result};
use wbftree::{
  BfTreeInsertResult, BfTreeReadResult, BfTreeService, Error, RangeIndexManager,
  StorageBackendType, TreeEntry,
};

use super::common::{ManagerEnvGuard, TUNE};

/// 认领被他人持有时，snapshot_under_claim 必须串行等待至持有者释放后成功完成，
/// 绝不因等待时长判失败（本票核心回归：大树慢盘认领耗时正比于树规模，超时即饥饿）
#[test]
fn claim_waits_for_holder_then_succeeds() -> Result<()> {
  let env = ManagerEnvGuard::new("claim_wait");
  let manager = RangeIndexManager::new(&env.ri_root.path, &env.cpr_root.path)?;
  let key = b"claim_wait_key";
  let tree = manager.create_bftree(key, StorageBackendType::Disk, TUNE)?;
  assert_eq!(BfTreeInsertResult::Success, tree.insert(b"k1", b"v1"));

  let entry = Arc::new(TreeEntry::new(
    Some(Arc::clone(&tree)),
    0,
    RangeIndexManager::key_id_of(key),
  ));

  // 主线程充当快照持有者：占住 claim，模拟大树慢盘单次 CPR 落盘进行中（不 sleep）
  assert!(entry.try_claim_snapshot(), "首次认领应成功");
  assert!(
    entry.snapshot_in_progress.load(Ordering::Acquire),
    "持有前置条件：claim 应处于占用态"
  );

  // 等待方线程：进入 snapshot_under_claim 前置位推进信号；因持有者仍占用 claim，
  // 其首次 try_claim 必然失败并进入退避等待循环，直到持有者释放后才完成快照
  let dest = env.ri_root.join("claim_wait_dest.bftree");
  let (ready_tx, ready_rx) = mpsc::channel::<()>();
  let (done_tx, done_rx) = mpsc::channel::<result::Result<(), Error>>();
  let waiter_entry = Arc::clone(&entry);
  let waiter_tree = Arc::clone(&tree);
  let waiter_dest = dest.clone();
  let waiter = thread::spawn(move || {
    let _ = ready_tx.send(());
    let _ = done_tx.send(waiter_entry.snapshot_under_claim(&waiter_tree, &waiter_dest));
  });

  // 推进信号：状态等待（无挂钟），确认等待方已就位；随后由持有者显式释放
  ready_rx.recv().expect("等待线程应送达就位信号");
  entry.release_snapshot();

  // 无超时收取：修复后认领成功必然发生，等待方只多等、绝不失败
  done_rx.recv().expect("等待线程应回报认领结果")?;
  waiter.join().expect("等待线程不应 panic");

  assert!(
    dest.exists(),
    "等待方应在 claim 释放后生成真实 CPR 快照文件"
  );
  assert!(
    !entry.snapshot_in_progress.load(Ordering::Acquire),
    "持有者释放与等待方守卫归还需成对：claim 最终必须回到空闲，无占用泄漏"
  );

  OK
}

/// 无竞争路径：认领-快照-守卫释放成对闭环，claim 用毕复位可再次认领
#[test]
fn claim_releases_and_is_reusable() -> Result<()> {
  let env = ManagerEnvGuard::new("claim_reuse");
  let manager = RangeIndexManager::new(&env.ri_root.path, &env.cpr_root.path)?;
  let key = b"claim_reuse_key";
  let tree = manager.create_bftree(key, StorageBackendType::Disk, TUNE)?;
  tree.insert(b"k1", b"v1");

  let entry = TreeEntry::new(
    Some(Arc::clone(&tree)),
    0,
    RangeIndexManager::key_id_of(key),
  );
  let dest = env.ri_root.join("claim_reuse_dest.bftree");

  entry.snapshot_under_claim(&tree, &dest)?;
  assert!(dest.exists(), "快照文件应真实落盘");
  assert!(
    !entry.snapshot_in_progress.load(Ordering::Acquire),
    "RAII 守卫应在成功返回后归还 claim"
  );

  // 复位后可再次认领（同一原子量循环复用，无残留占用）
  assert!(entry.try_claim_snapshot(), "复位后应可再次认领");
  entry.release_snapshot();

  // 快照内容可独立恢复，佐证落盘为有效 CPR 件而非空文件
  let recovered = BfTreeService::recover_from_cpr_snapshot(&dest, false, StorageBackendType::Disk)?;
  let (res, val) = recovered.read(b"k1");
  assert_eq!(res, BfTreeReadResult::Found);
  assert_eq!(val, Some(b"v1".to_vec()));

  fs::remove_file(&dest).ok();
  OK
}
