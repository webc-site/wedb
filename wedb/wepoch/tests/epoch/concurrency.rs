//! 高并发竞争、槽位耗尽与多实例压力测试

use std::{
  hint::spin_loop,
  sync::{
    Arc,
    atomic::{AtomicBool, AtomicUsize, Ordering},
  },
  thread::{self, spawn, yield_now},
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
  let instances: Vec<Arc<LightEpoch>> = (0..NUM_INSTANCES)
    .map(|_| Arc::new(LightEpoch::new(4)))
    .collect();

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

  let e1 = Arc::new(LightEpoch::new(8));
  let e2 = Arc::new(LightEpoch::new(8));
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
      da.fetch_add(1, Ordering::SeqCst);
    });
  }

  running.store(false, Ordering::Release);
  join_all(handles);

  // 最终清空所有挂起的延迟动作
  let target = epoch.current_epoch();
  epoch.bump_and_wait(target);

  assert_eq!(
    drained_actions.load(Ordering::SeqCst),
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

/// 验证线程退出未释放槽位时的 TLS 自动兜底回收 (Drop / TLS cleanup)
#[test]
fn thread_exit_tls_cleanup() -> Void {
  info!("验证线程退出未释放槽位时的 TLS 自动兜底回收");

  let epoch = Arc::new(LightEpoch::new(8));
  let slot_holder = Arc::new(AtomicUsize::new(0));

  let ep = Arc::clone(&epoch);
  let sh = Arc::clone(&slot_holder);

  let handle = thread::spawn(move || {
    ep.resume();
    let slot = ep.test_hook_this_thread_entry();
    assert_ne!(slot, 0);
    sh.store(slot, Ordering::SeqCst);
    // 故意不调用 ep.suspend()，模拟线程异常退出或遗漏 suspend
  });

  handle.join().unwrap();

  let slot = slot_holder.load(Ordering::SeqCst);
  assert_ne!(slot, 0);

  // 线程退出后，TLS 的 LocalEpochEntries::drop 应已兜底释放该槽位
  assert_eq!(
    epoch.test_hook_announced_epoch_at(slot),
    0,
    "线程析构后，遗留槽位的 announced_epoch 必须已被 TLS drop 清零"
  );
  assert_eq!(
    epoch.test_hook_thread_id_at(slot),
    0,
    "线程析构后，遗留槽位的 thread_id 必须已被 TLS drop 清零"
  );

  // 全局推进纪元并验证 safe_to_reclaim 可以顺利推进至最新纪元，绝不因死线程卡死
  epoch.bump_epoch();
  assert_eq!(epoch.current_epoch(), 2);
  assert!(
    epoch.is_safe_to_reclaim(1),
    "遗留死线程已被清理，纪元 1 必须判定为可安全回收"
  );

  OK
}

/// 验证长寿线程在大量瞬态 LightEpoch 下 TLS Weak 清理与 Arc 零泄漏
#[test]
fn transient_epoch_instance_lifecycle_and_tls_weak_cleanup() -> Void {
  info!("验证长寿线程在大量瞬态 LightEpoch 下 TLS Weak 清理与 Arc 零泄漏");

  const ITERATIONS: usize = 32;
  const CAPACITY: usize = 4;

  // 在同一线程上连续创建、使用并销毁 32 个不同的 LightEpoch 实例
  for _ in 0..ITERATIONS {
    let epoch = Arc::new(LightEpoch::new(CAPACITY));
    let weak = Arc::downgrade(&epoch);
    assert_eq!(Arc::strong_count(&epoch), 1);

    // 进入保护区并退出
    epoch.resume();
    assert!(epoch.this_instance_protected());
    epoch.suspend();
    assert!(!epoch.this_instance_protected());

    // 丢弃 epoch 实例
    drop(epoch);

    // 验证 Arc 彻底释放，强引用计数归零，TLS 仅持有 Weak，绝对无循环引用或内存泄露
    assert_eq!(
      weak.strong_count(),
      0,
      "LightEpoch 销毁后 epoch 强引用必须为 0"
    );
  }

  OK
}

/// 回归测试：reset_all_instances 先行清零后再 Drop 实例，活动计数饱和递减绝不回绕
///
/// 修复前 Drop 用 fetch_sub，reset 后计数回绕至 usize::MAX 附近，
/// 后续任何计数观测全部失真；修复后饱和停留在 0
#[test]
fn active_instance_count_saturates_on_drop_after_reset() -> Void {
  info!("验证 reset 后 Drop 不使活动实例计数无符号回绕");

  {
    let _epoch = LightEpoch::new(4);
    LightEpoch::reset_all_instances();
  } // _epoch drop：计数已为 0，饱和递减须停留 0 而非回绕

  // 并行测试可能同时持有少量实例，合法计数为小数值；
  // 回绕值为 usize::MAX 附近，二者数量级截然可分
  assert!(
    LightEpoch::active_instance_count() < 1024,
    "活动实例计数回绕：{}",
    LightEpoch::active_instance_count()
  );

  OK
}

/// 验证 current_thread_id 多线程唯一且非零（原语本体在 wbase）
#[test]
fn unified_current_thread_id_consistency() -> Void {
  info!("验证线程标识唯一原语");

  let main_tid = wbase::thread::current_thread_id();
  assert!(main_tid > 0, "thread_id 必须大于 0");

  let mut handles = Vec::new();
  for _ in 0..8 {
    handles.push(thread::spawn(|| {
      let tid = wbase::thread::current_thread_id();
      assert!(tid > 0);
      tid
    }));
  }

  let mut ids = vec![main_tid];
  for h in handles {
    ids.push(h.join().unwrap());
  }

  let total = ids.len();
  ids.sort_unstable();
  ids.dedup();
  assert_eq!(
    ids.len(),
    total,
    "所有并发线程获取的 thread_id 必须全局唯一"
  );

  OK
}
