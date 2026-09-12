//! 高并发竞争、槽位耗尽与多实例压力测试

use std::{
  hint::spin_loop,
  sync::{
    Arc,
    atomic::{AtomicBool, AtomicUsize, Ordering},
  },
  thread::{spawn, yield_now},
};

use aok::{OK, Void};
use log::info;
use wepoch::LightEpoch;

use super::support::join_all;

/// 极小容量槽位 (4 slots) 多线程 (16 threads) 极限并发竞争与安全回收
#[test]
fn adversarial_slot_exhaustion_and_recycling() -> Void {
  info!("验证极小容量槽位 (4 slots) 多线程 (16 threads) 极限并发竞争与安全回收");

  const MAX_SLOTS: usize = 4;
  const THREAD_COUNT: usize = 16;
  const ITERATIONS: usize = 50;
  let epoch = Arc::new(LightEpoch::new(MAX_SLOTS));
  let running = Arc::new(AtomicBool::new(true));

  let mut handles = Vec::with_capacity(THREAD_COUNT);
  for _ in 0..THREAD_COUNT {
    let ep = Arc::clone(&epoch);
    let r = Arc::clone(&running);
    handles.push(spawn(move || {
      for _ in 0..ITERATIONS {
        if !r.load(Ordering::Relaxed) {
          break;
        }
        // 模式 1: 显式 resume / suspend
        ep.resume();
        assert!(ep.this_instance_protected());
        let cur = ep.current_epoch();
        assert!(cur > 0);
        ep.suspend();
        assert!(!ep.this_instance_protected());

        // 模式 2: protected_scope
        {
          let _scope = ep.protected_scope();
          assert!(ep.this_instance_protected());
        }
        assert!(!ep.this_instance_protected());
      }
    }));
  }

  // 主线程并发推进纪元
  for _ in 0..ITERATIONS {
    epoch.bump_epoch();
    yield_now();
  }

  join_all(handles);

  // 所有线程已挂起，推进并验证 safe_to_reclaim_epoch 追平
  let cur = epoch.current_epoch();
  epoch.bump_and_wait(cur);
  assert!(epoch.is_safe_to_reclaim(cur));

  // 验证所有槽位均完全处于空闲状态
  for i in 1..=MAX_SLOTS {
    assert_eq!(epoch.test_hook_announced_epoch_at(i), 0);
    assert_eq!(epoch.test_hook_thread_id_at(i), 0);
  }

  OK
}

/// Participant::try_reserve 与 resume::try_claim 极限并发交叉抢占验证
#[test]
fn adversarial_concurrent_claim_and_reserve_race() -> Void {
  info!("验证 Participant::try_reserve 与 resume::try_claim 极限并发交叉抢占");

  const SLOTS: usize = 4;
  const WORKERS: usize = 4;
  const ITERATIONS: usize = 100;
  let epoch = Arc::new(LightEpoch::new(SLOTS));
  let start = Arc::new(AtomicBool::new(false));

  let mut handles = Vec::with_capacity(WORKERS * 2);

  // 4 个线程频繁 register & drop
  for _ in 0..WORKERS {
    let ep = Arc::clone(&epoch);
    let s = Arc::clone(&start);
    handles.push(spawn(move || {
      while !s.load(Ordering::Acquire) {
        yield_now();
      }
      for _ in 0..ITERATIONS {
        if let Ok(p) = ep.register() {
          let g = p.enter();
          assert!(g.protected_epoch() > 0);
          drop(g);
          drop(p);
        }
        yield_now();
      }
    }));
  }

  // 4 个线程频繁 resume & suspend
  for _ in 0..WORKERS {
    let ep = Arc::clone(&epoch);
    let s = Arc::clone(&start);
    handles.push(spawn(move || {
      while !s.load(Ordering::Acquire) {
        yield_now();
      }
      for _ in 0..ITERATIONS {
        ep.resume();
        assert!(ep.this_instance_protected());
        ep.suspend();
        assert!(!ep.this_instance_protected());
        yield_now();
      }
    }));
  }

  start.store(true, Ordering::Release);
  join_all(handles);

  // 验证结束后所有条目恢复未被占用
  for i in 1..=SLOTS {
    assert_eq!(epoch.test_hook_announced_epoch_at(i), 0);
    assert_eq!(epoch.test_hook_thread_id_at(i), 0);
  }

  OK
}

/// 多实例交替进出（超过 4 个实例触发 overflow 向量）隔离验证
#[test]
fn rapid_multi_instance_interleaving_with_overflow() -> Void {
  info!("验证多实例交替进出（超过 4 个实例触发 overflow 向量）隔离");

  const NUM_INSTANCES: usize = 8;
  let instances: Vec<LightEpoch> = (0..NUM_INSTANCES).map(|_| LightEpoch::new(4)).collect();

  // 确保所有实例 ID 互不相同
  for (i, ep1) in instances.iter().enumerate() {
    for ep2 in &instances[i + 1..] {
      assert_ne!(ep1.id, ep2.id, "实例 ID 必须全局唯一");
    }
  }

  // 同一线程按序进入全部 8 个实例保护区（跨越 MAX_LOCAL_ENTRIES = 4 门限触发 overflow）
  for ep in &instances {
    ep.resume();
    assert!(ep.this_instance_protected());
  }

  // 验证每个实例均正确记录当前线程保护状态
  for ep in &instances {
    assert!(ep.this_instance_protected());
  }

  // 逆序退出全部 8 个实例保护区
  for ep in instances.iter().rev() {
    ep.suspend();
    assert!(!ep.this_instance_protected());
  }

  // 最终全部处于未受保护状态
  for ep in &instances {
    assert!(!ep.this_instance_protected());
    assert_eq!(ep.test_hook_this_thread_entry(), 0);
  }

  OK
}

/// 同一线程跨多实例槽位映射相互隔离
#[test]
fn multi_instance_thread_local_isolation() -> Void {
  info!("验证同一线程跨多实例槽位映射相互隔离");

  let e1 = LightEpoch::new(8);
  let e2 = LightEpoch::new(8);
  assert_ne!(e1.id, e2.id, "实例 ID 必须全局唯一");

  {
    let _s1 = e1.protected_scope();
    assert!(e1.this_instance_protected());
    assert!(!e2.this_instance_protected());
    assert!(e1.test_hook_this_thread_entry() > 0);
    assert_eq!(e2.test_hook_this_thread_entry(), 0);

    {
      let _s2 = e2.protected_scope();
      assert!(e1.this_instance_protected());
      assert!(e2.this_instance_protected());

      // 两实例的全局纪元互不影响
      assert_eq!(e2.bump_current_epoch(), 2);
      assert_eq!(e1.current_epoch(), 1);
      assert_eq!(e1.test_hook_this_thread_announced_epoch(), 1);
    }

    // e2 挂起后，e1 的保护不受影响
    assert!(e1.this_instance_protected());
    assert!(!e2.this_instance_protected());
  }
  assert!(!e1.this_instance_protected());

  OK
}

/// 16 线程高并发进退、重入、纪元推进与延迟动作极限压力验证
#[test]
fn high_concurrency_heavy_stress() -> Void {
  info!("验证 16 线程高并发进退、重入、纪元推进与延迟动作极限压力");

  const THREAD_COUNT: usize = 16;
  const ACTION_COUNT: usize = 200;
  const SPIN_CYCLES: usize = 5;

  let epoch = Arc::new(LightEpoch::new(THREAD_COUNT * 2));
  let running = Arc::new(AtomicBool::new(true));
  let drained_actions = Arc::new(AtomicUsize::new(0));

  let mut handles = Vec::with_capacity(THREAD_COUNT);

  for _ in 0..THREAD_COUNT {
    let ep = Arc::clone(&epoch);
    let r = Arc::clone(&running);
    handles.push(spawn(move || {
      while r.load(Ordering::Relaxed) {
        ep.resume();
        assert!(ep.this_instance_protected());

        // 嵌套重入
        ep.resume();
        for _ in 0..SPIN_CYCLES {
          spin_loop();
        }
        ep.suspend();

        ep.suspend();
        assert!(!ep.this_instance_protected());
      }
    }));
  }

  // 主写者线程：高频推进纪元并挂接延迟动作
  for _ in 0..ACTION_COUNT {
    let da = Arc::clone(&drained_actions);
    epoch.bump_current_epoch_action(move || {
      da.fetch_add(1, Ordering::Relaxed);
    });
  }

  running.store(false, Ordering::Release);
  join_all(handles);

  // 最终清空所有挂起的延迟动作
  let target = epoch.current_epoch();
  epoch.bump_and_wait(target);

  assert_eq!(
    drained_actions.load(Ordering::Acquire),
    ACTION_COUNT,
    "所有注册的延迟动作必须全量且恰好执行一次"
  );
  assert!(epoch.is_safe_to_reclaim(target));

  OK
}

/// 多读者高频进出保护区与主线程并发推进纪元压力测试
#[test]
fn concurrent_readers_and_epoch_advance_stress() -> Void {
  info!("验证多读者高频进出保护区与主线程并发推进纪元压力测试");

  const THREAD_COUNT: usize = 8;
  const ITERATIONS: usize = 500;
  const SPIN_CYCLES: usize = 10;

  let epoch = Arc::new(LightEpoch::new(THREAD_COUNT + 2));
  let running = Arc::new(AtomicBool::new(true));
  let mut handles = Vec::with_capacity(THREAD_COUNT);

  // 启动并发读者线程
  for _ in 0..THREAD_COUNT {
    let ep = Arc::clone(&epoch);
    let r = Arc::clone(&running);
    handles.push(spawn(move || {
      let p = ep.register().expect("参与者注册成功");
      while r.load(Ordering::Relaxed) {
        let g1 = p.enter();
        let e1 = g1.protected_epoch();
        assert!(e1 > 0);

        // 嵌套重入
        {
          let g2 = p.enter();
          assert_eq!(g2.protected_epoch(), e1);
        }

        for _ in 0..SPIN_CYCLES {
          spin_loop();
        }
      }
    }));
  }

  // 主线程推进纪元并检测安全回收
  for _ in 0..ITERATIONS {
    let curr = epoch.bump_epoch();
    assert!(curr >= 2);
  }

  running.store(false, Ordering::Release);
  join_all(handles);

  // 所有读者线程退出后，推进并等待所有读者完全退出
  let target = epoch.current_epoch();
  epoch.bump_and_wait(target);
  assert!(epoch.is_safe_to_reclaim(target));

  OK
}
