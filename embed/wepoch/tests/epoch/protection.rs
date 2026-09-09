//! 纪元保护生命周期测试（对标 Garnet test.epoch/ProtectionTests）

use std::sync::Arc;

use aok::{OK, Void};
use log::info;
use wepoch::{LightEpoch, ProtectedScope};

use super::support::{assert_not_send, assert_not_sync, assert_protected_at};

/// 对标 Garnet ProtectionTests: UnprotectedThreadHoldsNoSlot
///
/// 未受保护的线程不占用任何 entry 槽位，try_suspend 返回 false
#[test]
fn unprotected_thread_holds_no_slot() -> Void {
  info!("验证未受保护的线程不持有任何槽位");

  let epoch = LightEpoch::new(16);
  assert!(!epoch.this_instance_protected());
  assert_eq!(epoch.test_hook_this_thread_entry(), 0);
  assert_eq!(epoch.test_hook_this_thread_announced_epoch(), 0);
  assert!(!epoch.try_suspend());

  OK
}

/// 对标 Garnet ProtectionTests: AProtectedThreadOwnsAValidSlot
///
/// 进入 ProtectedScope 后当前线程拥有合法槽位，且公布的纪元与全局 current_epoch 一致
#[test]
fn protected_thread_owns_valid_slot() -> Void {
  info!("验证受保护线程拥有合法槽位");

  let epoch = LightEpoch::new(16);
  {
    let _scope = epoch.protected_scope();
    assert_protected_at(&epoch, epoch.current_epoch(), "受保护作用域应持有合法槽位");
  }

  OK
}

/// 对标 Garnet ProtectionTests: SuspendLeavesTheSlotCompletelyFree
///
/// 挂起后原槽位的 announced_epoch、thread_id 以及当前线程的 entry 索引完全清零
#[test]
fn suspend_leaves_slot_completely_free() -> Void {
  info!("验证挂起完全释放槽位");

  let epoch = LightEpoch::new(16);
  epoch.resume();
  let entry = epoch.test_hook_this_thread_entry();
  assert!(entry > 0);
  epoch.suspend();

  assert_eq!(
    epoch.test_hook_announced_epoch_at(entry),
    0,
    "公布的纪元残留"
  );
  assert_eq!(epoch.test_hook_thread_id_at(entry), 0, "线程 ID 残留");
  assert_eq!(
    epoch.test_hook_this_thread_entry(),
    0,
    "当前线程 entry 未清零"
  );

  OK
}

/// 对标 Garnet ProtectionTests: SuspendResumeKeepsTheThreadProtected
///
/// 在保护作用域内调用 suspend_resume，当前线程仍维持合法保护槽位
#[test]
fn suspend_resume_keeps_thread_protected() -> Void {
  info!("验证 SuspendResume 后线程仍受保护");

  let epoch = LightEpoch::new(16);
  {
    let _scope = epoch.protected_scope();
    epoch.suspend_resume();
    assert_protected_at(
      &epoch,
      epoch.current_epoch(),
      "suspend_resume 必须维持受保护状态",
    );
  }

  OK
}

/// 对标 Garnet ProtectionTests: RefreshRepublishesTheLatestEpochEveryTime
///
/// 循环推进全局纪元，调用 protect_and_drain 每次均正确公布最新全局纪元
#[test]
fn refresh_republishes_latest_epoch_every_time() -> Void {
  info!("验证 Refresh 每次都公布最新全局纪元");

  const ITERATIONS: usize = 16;
  let epoch = LightEpoch::new(16);
  {
    let _scope = epoch.protected_scope();
    for _ in 0..ITERATIONS {
      let announced = epoch.test_hook_this_thread_announced_epoch();
      let bumped = epoch.bump_current_epoch();
      assert_protected_at(
        &epoch,
        announced,
        "bump_current_epoch 绝不能隐式更新当前线程已公布的纪元",
      );

      epoch.protect_and_drain();
      assert_protected_at(
        &epoch,
        bumped,
        "protect_and_drain 必须公布推进后的最新全局纪元",
      );
    }
  }

  OK
}

/// 对标 Garnet ProtectionTests: ProtectionSurvivesRepeatedResumeSuspendCycles
///
/// 连续经历 128 次 resume/suspend 循环，保护状态准确切换
#[test]
fn protection_survives_repeated_resume_suspend_cycles() -> Void {
  info!("验证经历多次连续 resume/suspend 循环保护依然完好");

  const CYCLES: usize = 128;
  let epoch = LightEpoch::new(16);
  for _ in 0..CYCLES {
    epoch.resume();
    assert!(epoch.this_instance_protected());
    epoch.suspend();
    assert!(!epoch.this_instance_protected());
  }

  OK
}

/// 对标 Garnet ProtectionTests: ResumeAndSuspendTrackProtectionState
///
/// 显式调用 resume 和 suspend，this_instance_protected 正确反映保护状态
#[test]
fn resume_and_suspend_track_protection_state() -> Void {
  info!("验证 Resume 与 Suspend 状态跟踪");

  let epoch = LightEpoch::new(16);
  assert!(!epoch.this_instance_protected());

  epoch.resume();
  assert!(epoch.this_instance_protected());

  epoch.suspend();
  assert!(!epoch.this_instance_protected());

  OK
}

/// 对标 Garnet ProtectionTests: ResumeIfNotProtectedIsIdempotent
///
/// resume_if_not_protected 与 try_suspend 具备幂等性与正确的布尔返回值
#[test]
fn resume_if_not_protected_is_idempotent() -> Void {
  info!("验证 ResumeIfNotProtected 幂等性");

  let epoch = LightEpoch::new(16);
  assert!(epoch.resume_if_not_protected());
  assert!(!epoch.resume_if_not_protected());
  assert!(epoch.try_suspend());
  assert!(!epoch.try_suspend());

  OK
}

/// 验证多层嵌套 ProtectedScope 保护作用域重入安全与生命周期收敛
#[test]
fn nested_protected_scope_and_reentrancy() -> Void {
  info!("验证嵌套 ProtectedScope 保护作用域重入安全与生命周期收敛");

  let epoch = LightEpoch::new(16);
  assert!(!epoch.this_instance_protected());
  assert_eq!(epoch.test_hook_this_thread_entry(), 0);

  {
    let _scope1 = epoch.protected_scope();
    assert!(epoch.this_instance_protected());
    let entry1 = epoch.test_hook_this_thread_entry();
    assert!(entry1 > 0);

    {
      let _scope2 = epoch.protected_scope();
      assert!(epoch.this_instance_protected());
      assert_eq!(epoch.test_hook_this_thread_entry(), entry1);

      {
        let _scope3 = epoch.protected_scope();
        assert!(epoch.this_instance_protected());
        assert_eq!(epoch.test_hook_this_thread_entry(), entry1);
      } // scope3 drop

      assert!(epoch.this_instance_protected());
      assert_eq!(epoch.test_hook_this_thread_entry(), entry1);
    } // scope2 drop

    assert!(epoch.this_instance_protected());
    assert_eq!(epoch.test_hook_this_thread_entry(), entry1);
  } // scope1 drop

  assert!(!epoch.this_instance_protected());
  assert_eq!(epoch.test_hook_this_thread_entry(), 0);

  OK
}

/// 验证手动嵌套 resume 与 suspend 配对计数与下溢保护
#[test]
fn nested_resume_suspend_pairs() -> Void {
  info!("验证手动嵌套 resume 与 suspend 配对计数与下溢保护");

  let epoch = LightEpoch::new(16);
  assert!(!epoch.this_instance_protected());

  epoch.resume();
  assert!(epoch.this_instance_protected());
  let entry = epoch.test_hook_this_thread_entry();
  assert!(entry > 0);

  // 第 2 次 resume 重入
  epoch.resume();
  assert!(epoch.this_instance_protected());
  assert_eq!(epoch.test_hook_this_thread_entry(), entry);

  // 第 3 次 resume 重入
  epoch.resume();
  assert!(epoch.this_instance_protected());
  assert_eq!(epoch.test_hook_this_thread_entry(), entry);

  // 逐层退出
  epoch.suspend();
  assert!(epoch.this_instance_protected());
  assert_eq!(epoch.test_hook_this_thread_entry(), entry);

  epoch.suspend();
  assert!(epoch.this_instance_protected());
  assert_eq!(epoch.test_hook_this_thread_entry(), entry);

  epoch.suspend();
  assert!(!epoch.this_instance_protected());
  assert_eq!(epoch.test_hook_this_thread_entry(), 0);

  // 额外调用 suspend 不会发生借位下溢
  epoch.suspend();
  assert!(!epoch.this_instance_protected());
  assert_eq!(epoch.test_hook_this_thread_entry(), 0);

  OK
}

/// 验证 ProtectedScope 线程亲和性 (!Send/!Sync) 与 Debug 格式化
#[test]
fn protected_scope_thread_affinity_and_debug() -> Void {
  info!("验证 ProtectedScope 线程亲和性 (!Send/!Sync) 与 Debug 格式化");

  assert_not_send::<ProtectedScope<'_>>();
  assert_not_sync::<ProtectedScope<'_>>();

  let epoch = LightEpoch::new(16);
  {
    let scope = epoch.protected_scope();
    let debug_str = format!("{scope:?}");
    assert!(debug_str.contains("ProtectedScope"));
    assert!(debug_str.contains("epoch_id"));
    assert!(debug_str.contains("current_epoch"));
  }

  OK
}

/// 验证安全回收纪元缓存单调不回退
#[test]
fn safe_epoch_monotonic() -> Void {
  info!("验证安全回收纪元缓存单调不回退");

  const ADVANCE_STEPS: usize = 8;
  let epoch = Arc::new(LightEpoch::new(16));
  let p = epoch.register()?;

  let guard = p.enter();
  let e1 = guard.protected_epoch();

  let mut prev = epoch.safe_to_reclaim_epoch();
  for _ in 0..ADVANCE_STEPS {
    epoch.bump_epoch();
    let safe = epoch.compute_safe_to_reclaim_epoch();
    assert!(safe >= prev, "安全纪元缓存回退: {prev} -> {safe}");
    prev = safe;
  }
  assert!(!epoch.is_safe_to_reclaim(e1), "被保护纪元绝不安全");

  drop(guard);

  epoch.bump_and_wait(epoch.current_epoch());
  assert!(epoch.is_safe_to_reclaim(epoch.current_epoch() - 1));

  OK
}

/// 验证边界纪元 (0, 1, 当前纪元) 安全回收判定精确性
#[test]
fn safe_epoch_boundary_conditions() -> Void {
  info!("验证边界纪元 (0, 1, 当前纪元) 安全回收判定精确性");

  let epoch = Arc::new(LightEpoch::new(8));

  // 纪元 0 永远安全
  assert!(epoch.is_safe_to_reclaim(0));

  // 初始时 current_epoch 为 1，任何正纪元均不可回收
  assert!(!epoch.is_safe_to_reclaim(1));
  assert_eq!(epoch.safe_to_reclaim_epoch(), 0);

  // 推进至 2，此时无任何读者，纪元 1 变安全
  epoch.bump_epoch();
  assert_eq!(epoch.current_epoch(), 2);
  assert!(epoch.is_safe_to_reclaim(1));
  assert!(!epoch.is_safe_to_reclaim(2));

  // 注册读者在纪元 2 进入保护
  let p = epoch.register()?;
  let guard = p.enter();
  assert_eq!(guard.protected_epoch(), 2);

  // 持续推进纪元至 5
  epoch.bump_epoch();
  epoch.bump_epoch();
  epoch.bump_epoch();
  assert_eq!(epoch.current_epoch(), 5);

  // 读者仍保护纪元 2，故纪元 1 安全，纪元 2 及以上绝不安全
  assert!(epoch.is_safe_to_reclaim(1));
  assert!(!epoch.is_safe_to_reclaim(2));
  assert!(!epoch.is_safe_to_reclaim(3));
  assert!(!epoch.is_safe_to_reclaim(4));

  drop(guard);

  epoch.compute_safe_to_reclaim_epoch();
  assert!(epoch.is_safe_to_reclaim(4));
  assert!(!epoch.is_safe_to_reclaim(5));

  OK
}

/// 验证 ProtectedScope 与 EpochGuard 的 Deref 智能指针人体工程学
#[test]
fn deref_ergonomics() -> Void {
  info!("验证 ProtectedScope 与 EpochGuard 的 Deref 智能指针人体工程学");

  let epoch = Arc::new(LightEpoch::new(8));

  // 验证 ProtectedScope 的 Deref<Target = LightEpoch>
  {
    let scope = epoch.protected_scope();
    assert_eq!(scope.current_epoch(), 1);
    assert_eq!(scope.entry_count(), 8);
    assert!(scope.this_instance_protected());
  }
  assert!(!epoch.this_instance_protected());

  // 验证 EpochGuard 的 Deref<Target = Participant>
  let p = epoch.register()?;
  let word_idx = epoch.allocate_user_word(42)?;
  {
    let guard = p.enter();
    assert_eq!(guard.entry_idx(), p.entry_idx());
    assert!(guard.is_protected());
    assert_eq!(guard.user_word(word_idx)?, 42);
    guard.set_user_word(word_idx, 99)?;
    assert_eq!(guard.user_word(word_idx)?, 99);
  }

  epoch.release_user_word(word_idx)?;
  OK
}
