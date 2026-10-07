//! 纪元延迟清理队列与 Drain 机制测试（对标 Garnet test.epoch/DrainTests）
//!
//! 自研依据: 纪元排空动作表（LightEpoch DrainList 语义的确定性装配验证）

// 钩子消费面（容量钩 / DRAIN_LIST_SIZE / ParkedReaderThread）随 debug 门剔除
#[cfg(debug_assertions)]
use std::{array::from_fn, thread::sleep, time::Duration};
use std::{
  sync::{
    Arc,
    atomic::{AtomicBool, AtomicU32, AtomicUsize, Ordering},
  },
  thread::{spawn, yield_now},
};

use aok::{OK, Void};
use log::info;
#[cfg(debug_assertions)]
use parking_lot::Mutex;
use wepoch::LightEpoch;

#[cfg(debug_assertions)]
use super::support::ParkedReaderThread;
use super::support::join_all;

/// 对标 libs/storage/Tsavorite/cs/test/test.epoch/DrainTests.cs:ActionRunsImmediatelyWhenNobodyElseIsProtected
///
/// 无其他线程受保护时，注册的延迟动作对应的纪元立即处于安全状态并同步执行
#[test]
fn action_runs_immediately_when_nobody_else_is_protected() -> Void {
  info!("验证无其他线程受保护时延迟动作立即执行");

  let epoch = Arc::new(LightEpoch::new(16));
  let ran = Arc::new(AtomicU32::new(0));

  {
    let _scope = epoch.protected_scope();
    let ran_clone = Arc::clone(&ran);
    epoch.bump_current_epoch_action(move || {
      ran_clone.fetch_add(1, Ordering::SeqCst);
    });
    assert_eq!(
      ran.load(Ordering::SeqCst),
      1,
      "无其他线程受保护时，该动作对应的纪元应立即安全并执行"
    );
  }

  OK
}

/// 对标 libs/storage/Tsavorite/cs/test/test.epoch/DrainTests.cs:TheLastThreadToSuspendRunsPendingActions
///
/// 有常驻读者阻止回收时动作暂不执行；最后一个挂起的读者在退出时代为清空已就绪动作
// ParkedReaderThread 经 test_hook 记录公布纪元，release 随辅助面剔除
#[cfg(debug_assertions)]
#[test]
fn the_last_thread_to_suspend_runs_pending_actions() -> Void {
  info!("验证最后一个挂起的线程代为执行 pending actions");

  let epoch = Arc::new(LightEpoch::new(16));
  let drained = Arc::new(AtomicU32::new(0));
  let mut reader = ParkedReaderThread::new(Arc::clone(&epoch));
  assert_eq!(reader.announced_epoch, 1);

  {
    let _scope = epoch.protected_scope();
    let drained_clone = Arc::clone(&drained);
    epoch.bump_current_epoch_action(move || {
      drained_clone.fetch_add(1, Ordering::SeqCst);
    });
  }

  assert_eq!(
    drained.load(Ordering::SeqCst),
    0,
    "当后台 reader 线程仍在保护区时，动作绝不得提前触发"
  );

  reader.leave_and_join();

  assert_eq!(
    drained.load(Ordering::SeqCst),
    1,
    "最后一个挂起的线程必须在退出时负责执行挂起的动作"
  );

  OK
}

/// 对标 libs/storage/Tsavorite/cs/test/test.epoch/DrainTests.cs:EveryActionRunsExactlyOnceWhenTheDrainListFills
///
/// 填满整个延迟动作队列（容量上限），常驻读者退出后所有延迟动作恰好各自执行一次
#[cfg(debug_assertions)]
#[test]
fn every_action_runs_exactly_once_when_drain_list_fills() -> Void {
  info!("验证满容量延迟队列中每个动作恰好执行一次");

  let epoch = Arc::new(LightEpoch::new(16));
  let capacity = LightEpoch::test_hook_drain_list_capacity();
  let counts: Vec<Arc<AtomicU32>> = (0..capacity).map(|_| Arc::new(AtomicU32::new(0))).collect();
  let mut reader = ParkedReaderThread::new(Arc::clone(&epoch));

  {
    let _scope = epoch.protected_scope();
    for count in &counts {
      let count = Arc::clone(count);
      epoch.bump_current_epoch_action(move || {
        count.fetch_add(1, Ordering::SeqCst);
      });
    }
  }

  for (i, count) in counts.iter().enumerate() {
    assert_eq!(
      count.load(Ordering::SeqCst),
      0,
      "槽位 {i} 的动作在 reader 未退出前绝不得执行"
    );
  }

  reader.leave_and_join();

  for (i, count) in counts.iter().enumerate() {
    assert_eq!(
      count.load(Ordering::SeqCst),
      1,
      "槽位 {i} 的动作必须恰好执行一次"
    );
  }

  OK
}

/// 对标 libs/storage/Tsavorite/cs/test/test.epoch/DrainTests.cs:RegisteringBlocksWhileTheDrainListIsFullAndCompletesAfterwards
///
/// 延迟队列占满且无槽位可回收时，新注册线程阻塞，待阻碍读者退出后解除阻塞并完成注册
///
/// 装配前置（列表满且全部滞留）以有界轮询等齐后再断负向：固定 100ms 单发窗
/// 在前置未齐时即判「注册被阻塞」，结论失真
#[cfg(debug_assertions)]
fn wait_until_drain_list_full(epoch: &LightEpoch, capacity: usize) -> bool {
  for _ in 0..500 {
    if epoch.drain_count.load(Ordering::Acquire) == capacity as u32 {
      return true;
    }
    sleep(Duration::from_millis(10));
  }
  epoch.drain_count.load(Ordering::Acquire) == capacity as u32
}

#[cfg(debug_assertions)]
#[test]
fn registering_blocks_while_drain_list_is_full_and_completes_afterwards() -> Void {
  info!("验证队列满且未安全时注册动作阻塞，待阻碍者退出后继续完成");

  let epoch = Arc::new(LightEpoch::new(16));
  let capacity = LightEpoch::test_hook_drain_list_capacity();
  let counts: Vec<Arc<AtomicU32>> = (0..capacity).map(|_| Arc::new(AtomicU32::new(0))).collect();
  let extra_ran = Arc::new(AtomicU32::new(0));

  let mut reader = ParkedReaderThread::new(Arc::clone(&epoch));

  {
    let _scope = epoch.protected_scope();
    for count in &counts {
      let count = Arc::clone(count);
      epoch.bump_current_epoch_action(move || {
        count.fetch_add(1, Ordering::SeqCst);
      });
    }

    let registered = Arc::new(AtomicBool::new(false));
    let registered_clone = Arc::clone(&registered);
    let extra_clone = Arc::clone(&extra_ran);
    let epoch_clone = Arc::clone(&epoch);

    let latecomer = spawn(move || {
      let _scope = epoch_clone.protected_scope();
      epoch_clone.bump_current_epoch_action(move || {
        extra_clone.fetch_add(1, Ordering::SeqCst);
      });
      registered_clone.store(true, Ordering::Release);
    });

    // 先等装配前置就位：容量个槽位全部滞留（列表满且被 reader 钉死），再断负向
    assert!(
      wait_until_drain_list_full(&epoch, capacity),
      "装配前置未就位：延迟清理列表必须满载且全部滞留（drain_count == capacity）"
    );
    assert!(
      !registered.load(Ordering::Acquire),
      "延迟清理列表已满且没有任何槽位可回收时，新动作注册必须阻塞"
    );

    // 唤醒并退出阻碍者 reader
    reader.leave_and_join();

    // reader 退出后，晚到线程解除阻塞并成功注册
    const MAX_SPINS: usize = 5000;
    let mut spins = 0;
    while !registered.load(Ordering::Acquire) {
      spins += 1;
      if spins < 32 {
        yield_now();
      } else {
        sleep(Duration::from_millis(1));
      }
      assert!(spins <= MAX_SPINS, "超时未解除注册阻塞");
    }
    latecomer.join().unwrap();

    for (i, count) in counts.iter().enumerate() {
      assert_eq!(
        count.load(Ordering::SeqCst),
        1,
        "积压槽位 {i} 的动作必须恰好执行一次"
      );
    }
  }

  assert_eq!(
    extra_ran.load(Ordering::SeqCst),
    1,
    "晚到注册的动作必须恰好执行一次"
  );

  OK
}

/// 对标 libs/storage/Tsavorite/cs/test/test.epoch/DrainTests.cs:ActionsRunInEpochOrder
///
/// 跨递增纪元注册的多个动作，在回收时严格按照纪元单调推进顺序排队触发
// ParkedReaderThread 连带剔除（release 下辅助面不可用）
#[cfg(debug_assertions)]
#[test]
fn actions_run_in_epoch_order() -> Void {
  info!("验证延迟清理动作按纪元单调顺序执行");

  const ACTION_COUNT: usize = 8;
  let order = Arc::new(Mutex::new(Vec::with_capacity(ACTION_COUNT)));

  let epoch = Arc::new(LightEpoch::new(16));
  let mut reader = ParkedReaderThread::new(Arc::clone(&epoch));

  {
    let _scope = epoch.protected_scope();
    for i in 0..ACTION_COUNT {
      let order_clone = Arc::clone(&order);
      epoch.bump_current_epoch_action(move || {
        order_clone.lock().push(i);
      });
    }
  }

  assert!(order.lock().is_empty(), "reader 退出前动作列表必须为空");

  reader.leave_and_join();

  let recorded = order.lock();
  let expected: [usize; ACTION_COUNT] = from_fn(|i| i);
  assert_eq!(
    &**recorded, &expected,
    "延迟动作必须严格按纪元推进顺序排队执行"
  );

  OK
}

/// 对标 libs/storage/Tsavorite/cs/test/test.epoch/DrainTests.cs:ManyThreadsRegisteringActionsAllRunExactlyOnce
///
/// 多线程高并发注册延迟清理动作，所有动作全量且仅执行一次
#[test]
fn many_threads_registering_actions_all_run_exactly_once() -> Void {
  info!("验证多线程高并发注册延迟动作且全部恰好执行一次");

  const THREAD_COUNT: usize = 8;
  const PER_THREAD: usize = 200;
  const TOTAL_ACTIONS: usize = THREAD_COUNT * PER_THREAD;

  let epoch = Arc::new(LightEpoch::new(16));
  let counts = Arc::new(
    (0..TOTAL_ACTIONS)
      .map(|_| AtomicU32::new(0))
      .collect::<Vec<_>>(),
  );
  let start_barrier = Arc::new(AtomicBool::new(false));

  let handles: Vec<_> = (0..THREAD_COUNT)
    .map(|t| {
      let epoch_clone = Arc::clone(&epoch);
      let counts_clone = Arc::clone(&counts);
      let barrier = Arc::clone(&start_barrier);

      spawn(move || {
        while !barrier.load(Ordering::Acquire) {
          yield_now();
        }
        for i in 0..PER_THREAD {
          let index = t * PER_THREAD + i;
          let c = Arc::clone(&counts_clone);
          let _scope = epoch_clone.protected_scope();
          epoch_clone.bump_current_epoch_action(move || {
            c[index].fetch_add(1, Ordering::SeqCst);
          });
        }
      })
    })
    .collect();

  start_barrier.store(true, Ordering::Release);
  join_all(handles);

  // 最终清空挂起的全部 action
  epoch.bump_and_wait(epoch.current_epoch());

  for (i, c) in counts.iter().enumerate() {
    assert_eq!(c.load(Ordering::SeqCst), 1, "动作 {i} 必须恰好执行一次");
  }

  OK
}

/// 对标 libs/storage/Tsavorite/cs/test/test.epoch/DrainTests.cs:ActionDoesNotRunWhileAnotherThreadIsProtected
///
/// 只要其他线程仍处于受保护纪元，延迟动作绝不提前触发；挂起后正常执行
#[test]
fn action_does_not_run_while_another_thread_is_protected() -> Void {
  info!("验证只要有其他线程仍处于受保护纪元，延迟动作绝不触发");

  let epoch = Arc::new(LightEpoch::new(16));
  let protected_thread_entered = Arc::new(AtomicBool::new(false));
  let entered_clone = Arc::clone(&protected_thread_entered);
  let release_protected_thread = Arc::new(AtomicBool::new(false));
  let release_clone = Arc::clone(&release_protected_thread);

  let drained = Arc::new(AtomicU32::new(0));

  let epoch_reader = Arc::clone(&epoch);
  let reader = spawn(move || {
    epoch_reader.resume();
    entered_clone.store(true, Ordering::Release);
    while !release_clone.load(Ordering::Acquire) {
      yield_now();
    }
    epoch_reader.suspend();
  });

  while !protected_thread_entered.load(Ordering::Acquire) {
    yield_now();
  }

  epoch.resume();
  let drained_clone = Arc::clone(&drained);
  epoch.bump_current_epoch_action(move || {
    drained_clone.fetch_add(1, Ordering::SeqCst);
  });
  epoch.suspend();

  assert_eq!(
    drained.load(Ordering::SeqCst),
    0,
    "读者线程仍在保护区时，延迟动作绝不可提前触发"
  );

  release_protected_thread.store(true, Ordering::Release);
  reader.join().unwrap();

  assert_eq!(
    drained.load(Ordering::SeqCst),
    1,
    "最后一个退出的读者线程挂起时必须代为触发所有待处理动作"
  );

  OK
}

/// 验证 Refresh (protect_and_drain) 路径就绪延迟动作的主动收割
// protect_and_drain 与 ParkedReaderThread 均收口于 debug 门控面，release 随剔
#[cfg(debug_assertions)]
#[test]
fn refresh_path_executes_pending_action() -> Void {
  info!("验证 Refresh 路径收割就绪延迟动作");

  let epoch = Arc::new(LightEpoch::new(16));
  let ran = Arc::new(AtomicU32::new(0));
  let mut reader = ParkedReaderThread::new(Arc::clone(&epoch));

  {
    let _scope = epoch.protected_scope();
    let ran_clone = Arc::clone(&ran);
    epoch.bump_current_epoch_action(move || {
      ran_clone.fetch_add(1, Ordering::SeqCst);
    });

    // 本线程仍受保护时，最后的 reader 挂起必须移交收割权
    reader.leave_and_join();
    assert_eq!(
      ran.load(Ordering::SeqCst),
      0,
      "本线程仍受保护时，最后的 reader 挂起不得触发动作"
    );

    // Refresh 路径：protect_and_drain 收割就绪动作
    epoch.protect_and_drain();
    assert_eq!(
      ran.load(Ordering::SeqCst),
      1,
      "protect_and_drain 必须收割就绪的延迟动作"
    );
    assert_eq!(
      epoch.drain_count.load(Ordering::Acquire),
      0,
      "收割完成后计数必须归零"
    );
  }

  OK
}

/// 验证延迟动作内部级联注册与嵌套推进防死锁
#[test]
fn action_trigger_cascading_and_reentrant_bump_current_epoch() -> Void {
  info!("验证延迟动作内部级联注册与嵌套推进防死锁");

  let epoch = Arc::new(LightEpoch::new(16));
  let step1_ran = Arc::new(AtomicBool::new(false));
  let step2_ran = Arc::new(AtomicBool::new(false));

  let step1_clone = Arc::clone(&step1_ran);
  let step2_clone = Arc::clone(&step2_ran);
  let epoch_clone = Arc::clone(&epoch);

  {
    let _scope = epoch.protected_scope();
    epoch.bump_current_epoch_action(move || {
      step1_clone.store(true, Ordering::SeqCst);
      // 在回调内部嵌套触发二级延迟清理
      let s2 = Arc::clone(&step2_clone);
      let _nested_scope = epoch_clone.protected_scope();
      epoch_clone.bump_current_epoch_action(move || {
        s2.store(true, Ordering::SeqCst);
      });
    });
  }

  // 读者退出后级联动作完全执行
  epoch.bump_and_wait(epoch.current_epoch());
  assert!(step1_ran.load(Ordering::SeqCst));
  assert!(step2_ran.load(Ordering::SeqCst));

  OK
}

/// 验证 bump_and_wait 自动完成所有延迟动作收割，无需外部手动介入
#[test]
fn bump_and_wait_drains_queued_actions_without_manual_drain() -> Void {
  info!("验证 bump_and_wait 自动完成所有延迟动作收割，无需外部手动介入");

  const ACTION_COUNT: usize = 5;
  let epoch = Arc::new(LightEpoch::new(16));
  let count = Arc::new(AtomicU32::new(0));

  {
    let _scope = epoch.protected_scope();
    for _ in 0..ACTION_COUNT {
      let c = Arc::clone(&count);
      epoch.bump_current_epoch_action(move || {
        c.fetch_add(1, Ordering::SeqCst);
      });
    }
  }

  // 此时无任何活跃读者，直接调用 bump_and_wait
  let target = epoch.current_epoch();
  epoch.bump_and_wait(target);

  assert_eq!(
    count.load(Ordering::SeqCst),
    ACTION_COUNT as u32,
    "bump_and_wait 返回时所有关联延迟动作必须完全执行"
  );
  assert_eq!(epoch.drain_count.load(Ordering::Acquire), 0);

  OK
}

/// 验证注册与消费并发交错时 drain_count 守恒：计数与 drain_list 槽位状态严格配对，
/// 高频交错下每个延迟动作全量恰好执行一次、终态计数精确归零
///
/// 锁定 `LightEpoch::drain` 与 `bump_current_epoch_action` 的镜像计数协议
/// （注册：先加计数后公布纪元；消费：先减计数后发布 FREE）——对照 LightEpoch.cs Drain
/// 的非镜像次序（可交换 RMW 下总量仍守恒，仅存在瞬时观测偏差），本不变式压力测试
/// 守护任意交错下「动作恰好一次 + 终态计数归零」的核心契约
#[test]
fn concurrent_register_drain_interleave_conserves_drain_count() -> Void {
  info!("验证注册与消费并发交错时 drain_count 守恒且动作全量恰好执行一次");

  const REGISTRARS: usize = 4;
  const DRAINS: usize = 4;
  const PER_THREAD: usize = 500;
  const TOTAL: usize = REGISTRARS * PER_THREAD;

  let epoch = Arc::new(LightEpoch::new(32));
  let counts = Arc::new((0..TOTAL).map(|_| AtomicU32::new(0)).collect::<Vec<_>>());
  let start = Arc::new(AtomicBool::new(false));
  let stop = Arc::new(AtomicBool::new(false));
  let mut handles: Vec<Option<_>> = Vec::new();

  // 注册者：保护区内高频注册延迟动作（动作执行时机完全交给消费方）
  for t in 0..REGISTRARS {
    let ep = Arc::clone(&epoch);
    let counts_clone = Arc::clone(&counts);
    let s = Arc::clone(&start);
    handles.push(Some(spawn(move || {
      while !s.load(Ordering::Acquire) {
        yield_now();
      }
      for i in 0..PER_THREAD {
        let index = t * PER_THREAD + i;
        let c = Arc::clone(&counts_clone);
        let _scope = ep.protected_scope();
        ep.bump_current_epoch_action(move || {
          c[index].fetch_add(1, Ordering::SeqCst);
        });
      }
    })));
  }

  // 消费者：不注册任何动作，高频 bump + drain，与注册路径极限交错
  for _ in 0..DRAINS {
    let ep = Arc::clone(&epoch);
    let s = Arc::clone(&start);
    let stop = Arc::clone(&stop);
    handles.push(Some(spawn(move || {
      while !s.load(Ordering::Acquire) {
        yield_now();
      }
      while !stop.load(Ordering::Acquire) {
        ep.bump_current_epoch();
        ep.drain();
        yield_now();
      }
    })));
  }

  start.store(true, Ordering::Release);
  for h in handles.iter_mut().take(REGISTRARS) {
    if let Some(h) = h.take() {
      h.join().unwrap();
    }
  }

  // 注册全部完成后停止消费者，收尾强制排空
  stop.store(true, Ordering::Release);
  for h in handles.iter_mut().skip(REGISTRARS) {
    if let Some(h) = h.take() {
      h.join().unwrap();
    }
  }
  epoch.bump_and_wait(epoch.current_epoch());

  for (i, c) in counts.iter().enumerate() {
    assert_eq!(
      c.load(Ordering::SeqCst),
      1,
      "动作 {i} 必须恰好执行一次（重复或丢失将偏离 1）"
    );
  }
  assert_eq!(
    epoch.drain_count.load(Ordering::Acquire),
    0,
    "全部动作执行完毕后 drain_count 必须精确归零（计数错乱将表现为正残留）"
  );
  assert!(!epoch.has_pending_drain());

  OK
}

/// 验证 Participant 会话句柄的 refresh 刷新机制
#[test]
fn participant_refresh() -> Void {
  info!("验证 Participant 会话句柄的 refresh 刷新机制");

  let epoch = Arc::new(LightEpoch::new(8));
  let p = epoch.register()?;

  let drained = Arc::new(AtomicBool::new(false));
  let drained_clone = Arc::clone(&drained);

  let guard = p.enter();
  assert_eq!(guard.protected_epoch(), 1);

  // 推进纪元并挂接延迟动作。对照 C# BumpCurrentEpoch(Action) 尾部 ProtectAndDrain
  // （"may execute the action we just added"）：注册路径统一刷新本线程公布纪元并
  // 收割就绪动作，Participant 持有的旧纪元 1 在挂接时即被刷新至 2，动作随之触发。
  // 修复前 help_drain 仅识别 TLS 机制，Participant 长期守卫自钉回收进度，
  // 耗尽 drain_list 后 append 页翻转注册路径永久自旋（活锁）
  epoch.bump_current_epoch_action(move || {
    drained_clone.store(true, Ordering::SeqCst);
  });

  // 公布纪元已刷新至最新，旧纪元 1 安全，延迟动作被成功触发
  assert_eq!(p.protected_epoch(), 2);
  assert!(drained.load(Ordering::Acquire));

  drop(guard);
  OK
}

/// 回归测试：Participant 长期守卫下连续注册超过 drain_list 容量（16 槽）的延迟动作
///
/// 修复前：help_drain 看不见 Participant 保护 → 公布纪元自钉 → safe 永不推进 →
/// 动作注册路径在列表耗尽后永久自旋；修复后每次注册即刷新即收割，全程无阻塞
// DRAIN_LIST_SIZE 仅 debug 导出，release 随剔
#[cfg(debug_assertions)]
#[test]
fn participant_long_guard_drain_list_exhaustion() -> Void {
  info!("验证 Participant 长期守卫下 drain_list 满载注册不活锁");

  let epoch = Arc::new(LightEpoch::new(8));
  let p = epoch.register()?;

  let fired = Arc::new(AtomicUsize::new(0));
  // 2 倍容量：修复前必活锁
  const ACTIONS: usize = 2 * wepoch::DRAIN_LIST_SIZE;

  let _guard = p.enter();
  for _ in 0..ACTIONS {
    let f = Arc::clone(&fired);
    epoch.bump_current_epoch_action(move || {
      f.fetch_add(1, Ordering::SeqCst);
    });
  }

  assert_eq!(fired.load(Ordering::Acquire), ACTIONS);
  OK
}

/// 回归测试：同一线程 TLS 与 Participant 双机制并存时注册延迟动作不活锁
///
/// 修复前 help_drain 仅刷新 TLS 优先命中的单条保护条目，Participant 槽公布的旧纪元
/// 自钉 safe_to_reclaim_epoch，drain_list 耗尽后 bump_current_epoch_action 注册路径
/// 永久自旋（活锁）；修复后 help_drain 全量刷新本线程以任一机制持有的保护条目
#[cfg(debug_assertions)]
#[test]
fn mixed_tls_participant_drain_no_livelock() -> Void {
  info!("验证同线程 TLS+Participant 双机制并存时注册延迟动作不活锁");

  let epoch = Arc::new(LightEpoch::new(8));
  let p = epoch.register()?;
  // Participant 槽公布纪元 1（长期会话守卫）
  let _pg = p.enter();

  // 2 倍 drain_list 容量：修复前第二轮注册必活锁
  const ACTIONS: usize = 2 * wepoch::DRAIN_LIST_SIZE;
  let fired = Arc::new(AtomicUsize::new(0));

  {
    // TLS 槽与 Participant 槽并存，两条公布纪元均为 1
    let _tls = epoch.protected_scope();
    for _ in 0..ACTIONS {
      let f = Arc::clone(&fired);
      epoch.bump_current_epoch_action(move || {
        f.fetch_add(1, Ordering::SeqCst);
      });
    }
  }

  assert_eq!(fired.load(Ordering::Acquire), ACTIONS);
  OK
}

/// 回归测试：同线程先建 Participant 守卫，再经 TLS 轨 claim_entry，守卫公布纪元不得被抬起
///
/// 受害实点 wkv/src/store/resize.rs rebuild_index_from_hlog：barrier_enter 守卫横跨
/// async run_recovery_kernel，内核内 scan.rs protected_scope 同线程触发 claim_entry。
/// 修复前 claim_entry 尾部 help_drain 全量刷新本线程全部保护条目，把重建线程自己的
/// 屏障守卫一并抬起，并发 grow 双驱动下重建中途切表静默丢键；修复后只 drain 不刷新
/// （同 resume 快路径判据）。
///
/// 红判据（修复前必红）：help_drain 将 Participant 守卫公布纪元抬至最新全局纪元，
/// `p.protected_epoch()` 断言失败
#[test]
fn claim_entry_does_not_lift_participant_guard() -> Void {
  info!("验证 TLS 轨 claim_entry 只 drain 不刷新，同线程 Participant 守卫不被抬起");

  let epoch = Arc::new(LightEpoch::new(8));
  let p = epoch.register()?;
  // 长期会话守卫钉住纪元 1（模拟 resize barrier_enter 守卫横跨 async 内核）
  let guard = p.enter();
  assert_eq!(guard.protected_epoch(), 1);

  // 他线程注册滞留延迟动作并保持 pending（模拟活负载下组提交 flush 的 seal 注册）：
  // 触发纪元 1 被本线程守卫钉住，safe_to_reclaim 恒不追平，动作持续滞留
  {
    let ep = Arc::clone(&epoch);
    spawn(move || {
      let _scope = ep.protected_scope();
      ep.bump_current_epoch_action(|| {});
    })
    .join()
    .unwrap();
  }
  assert!(epoch.has_pending_drain());

  {
    // 同线程内核内 protected_scope → TLS 轨 claim_entry（scan.rs 同款路径）
    let _tls = epoch.protected_scope();
    // 修复前：help_drain 全量刷新把守卫抬至最新纪元，本断言即红
    assert_eq!(
      p.protected_epoch(),
      1,
      "claim_entry 不得抬起同线程 Participant 守卫的公布纪元"
    );
    assert!(
      epoch.has_pending_drain(),
      "旧纪元守卫钉住期间 pending 延迟动作不得被收割"
    );
  }

  // 守卫全部解除后收口排空：滞留动作完成收割（旧纪元排水活性回归）
  drop(guard);
  assert!(!epoch.has_pending_drain(), "守卫解除后滞留动作必须完成收割");
  OK
}

/// 回归测试：三获取路径（claim_entry 慢路径 / resume 重入快路径 / Participant::enter
/// 重入）在 pending 存在时统一只 drain 不刷新，守卫公布纪元全程不被抬起
///
/// 对齐 participant.rs enter/resume 既有口径（均仅 drain_if_pending），锁定
/// claim_entry 改造后全获取路径口径自洽
#[test]
fn acquire_paths_stay_drain_only_with_participant_guard() -> Void {
  info!("验证 claim/resume/enter 三获取路径统一只 drain 不刷新");

  let epoch = Arc::new(LightEpoch::new(8));
  let p = epoch.register()?;
  let guard = p.enter();
  let guard_epoch = guard.protected_epoch();

  // 他线程注册滞留延迟动作，保持 pending（构造同上）
  {
    let ep = Arc::clone(&epoch);
    spawn(move || {
      let _scope = ep.protected_scope();
      ep.bump_current_epoch_action(|| {});
    })
    .join()
    .unwrap();
  }
  assert!(epoch.has_pending_drain());

  {
    // 路径一：TLS 慢路径 claim_entry
    let _tls = epoch.protected_scope();
    assert_eq!(
      p.protected_epoch(),
      guard_epoch,
      "claim_entry 不得抬起同线程 Participant 守卫的公布纪元"
    );
    // 路径二：resume 重入快路径（本线程已持 TLS 槽），配对 suspend 收敛重入层
    epoch.resume();
    assert_eq!(
      p.protected_epoch(),
      guard_epoch,
      "resume 快路径不得抬起同线程 Participant 守卫的公布纪元"
    );
    epoch.suspend();
  }
  // 路径三：Participant::enter 重入
  {
    let reentered = p.enter();
    assert_eq!(
      p.protected_epoch(),
      guard_epoch,
      "enter 重入不得抬起 Participant 守卫的公布纪元"
    );
    drop(reentered);
  }

  assert!(
    epoch.has_pending_drain(),
    "守卫钉住期间 pending 延迟动作不得被收割"
  );
  drop(guard);
  assert!(!epoch.has_pending_drain(), "守卫解除后滞留动作必须完成收割");
  OK
}

/// 回归测试：同线程 Participant 守卫钉旧纪元（公布纪元落后全局前沿）时触达
/// bump_current_epoch_action，收尾 help_drain 不得抬起守卫公布纪元
///
/// 受害机理同 claim_entry 票（c01d 同源）：whlog shift 链 / wbftree 管理面的
/// 调用线程若持跨 await Participant 守卫触达 bump 注册，旧形收尾无条件全量刷新
/// 本线程条目，守卫被抬 → 旧纪元保护静默失效 → 内存安全事故。修复后收尾与
/// 列表满自旋同罩定向判据：仅刷新 TLS 轨条目与公布纪元恰为 prior_epoch 的新鲜
/// 钉，钉旧纪元的 Participant 守卫不在刷新面内（对齐 C# Acquire 尾部仅 Drain
/// 的获取侧口径；C# 无条件 ProtectAndDrain 形在双轨下即守卫抬升事故）。
///
/// 红判据（修复前必红）：help_drain 将守卫公布纪元抬至最新全局纪元，
/// `p.protected_epoch()` 断言失败
#[test]
fn bump_action_tail_keeps_stale_participant_guard() -> Void {
  info!("验证 bump 注册收尾不抬起钉旧纪元的同线程 Participant 守卫");

  let epoch = Arc::new(LightEpoch::new(8));
  let p = epoch.register()?;
  // 长期会话守卫钉住纪元 1（模拟横跨 await 的屏障守卫）
  let guard = p.enter();
  assert_eq!(guard.protected_epoch(), 1);

  // 他线程推进全局纪元（守卫无人刷新 → 公布纪元 1 成为陈旧钉）
  {
    let ep = Arc::clone(&epoch);
    spawn(move || {
      let _scope = ep.protected_scope();
      for _ in 0..3 {
        ep.bump_current_epoch();
      }
    })
    .join()
    .unwrap();
  }
  assert_eq!(p.protected_epoch(), 1);
  assert!(epoch.current_epoch() > 1);

  // 同线程触达 bump_current_epoch_action（模拟 shift 链 / wbftree 管理面注册收尾）
  let drained = Arc::new(AtomicBool::new(false));
  let drained_clone = Arc::clone(&drained);
  epoch.bump_current_epoch_action(move || {
    drained_clone.store(true, Ordering::SeqCst);
  });

  // 修复后：守卫公布纪元不被抬（旧形即红：被抬至最新全局纪元）
  assert_eq!(
    p.protected_epoch(),
    1,
    "bump 注册收尾不得抬起钉旧纪元的同线程 Participant 守卫"
  );
  // 动作触发纪元被守卫钉住 → 保持 pending（排水不得靠抬守卫静默提前）
  assert!(!drained.load(Ordering::Acquire));
  assert!(epoch.has_pending_drain());

  // 守卫解除后收口排空：滞留动作完成收割（bump 排水活性回归）
  drop(guard);
  assert!(
    drained.load(Ordering::Acquire),
    "守卫解除后滞留动作必须完成收割"
  );
  assert!(!epoch.has_pending_drain());
  OK
}

/// 回归测试：双轨并存时 bump 注册收尾的定向刷新分界——TLS 轨条目（C# 单轨
/// 同构，同步临界区）照常刷新推进回收，钉旧纪元的同线程 Participant 守卫不被抬
///
/// 锁定 help_drain 单点判据的两半：TLS 轨无条件（陈旧 TLS 钉不刷新则满列表
/// 自旋臂自钉活锁），Participant 轨仅新鲜钉（== prior_epoch）参与刷新
#[cfg(debug_assertions)]
#[test]
fn bump_action_refreshes_tls_but_not_stale_participant_guard() -> Void {
  info!("验证 bump 注册收尾刷新 TLS 轨但保留钉旧纪元的 Participant 守卫");

  let epoch = Arc::new(LightEpoch::new(8));
  let p = epoch.register()?;
  let guard = p.enter();
  assert_eq!(guard.protected_epoch(), 1);

  // 他线程推进全局纪元 → 守卫公布纪元 1 陈旧
  {
    let ep = Arc::clone(&epoch);
    spawn(move || {
      let _scope = ep.protected_scope();
      for _ in 0..3 {
        ep.bump_current_epoch();
      }
    })
    .join()
    .unwrap();
  }
  assert_eq!(p.protected_epoch(), 1);
  let tls_epoch = epoch.current_epoch();

  let drained = Arc::new(AtomicBool::new(false));
  let drained_clone = Arc::clone(&drained);
  {
    // TLS 轨与 Participant 轨并存：TLS 槽按当前前沿取得（新鲜），
    // Participant 槽公布纪元 1（陈旧）
    let _tls = epoch.protected_scope();
    epoch.bump_current_epoch_action(move || {
      drained_clone.store(true, Ordering::SeqCst);
    });

    // TLS 轨照常刷新至最新前沿（protect_and_drain 同构合约面）
    assert_eq!(
      epoch.test_hook_this_thread_announced_epoch(),
      tls_epoch + 1,
      "bump 注册收尾必须刷新本线程 TLS 轨公布纪元"
    );
    // 钉旧纪元的 Participant 守卫不被抬（旧形即红：守卫与 TLS 轨一并被全量刷新）
    assert_eq!(
      p.protected_epoch(),
      1,
      "bump 注册收尾不得抬起钉旧纪元的同线程 Participant 守卫"
    );
  }
  // TLS 作用域退出后，守卫仍钉住触发纪元 → 动作保持 pending
  assert!(!drained.load(Ordering::Acquire));
  assert!(epoch.has_pending_drain());

  // 守卫解除后收口排空（排水活性回归）
  drop(guard);
  assert!(
    drained.load(Ordering::Acquire),
    "守卫解除后滞留动作必须完成收割"
  );
  assert!(!epoch.has_pending_drain());
  OK
}

/// 定向 relaxed 注册破「多注册方快照互卡」锁死（rc_grow 迁移窗挂死形态的双线程
/// 重演，对标 C# BumpCurrentEpoch(Action) 自旋臂 ProtectAndDrain 的循环自抬语义）：
///
/// 读者 A/B 先后钉快照纪元；drain_list 被 SIZE 个动作填满（safe 停滞，槽全不可
/// 回收）；B 进保护区后全局纪元再被裸推高——B 成非新鲜钉（help_drain 判据不可
/// 达）。双方在**各自属主线程**并发 relaxed 注册：环内逐轮定向自抬自己（他方
/// 条目绝不触碰——refresh_entry_relaxed 的 thread_id 校验拒绝非属主刷新），safe
/// 随双方抬升逐级解锁，双注册闭环、全部堆积动作收割。若定向刷新误抬他方、
/// 判据回归或属主契约破裂，本测试以挂死（slow-timeout 杀）或收割计数不闭环
/// 的形式红。防护力边界：本装配下 help_drain 的 TLS 命中面（note_participant_slot
/// 登记）已在承担部分刷新，本测试锁「双注册闭环+收割闭环+误抬探针」的行为面；
/// refresh_entry_relaxed 判据本身的强隔离回归防线需 help_drain 判据隔离测试
/// （交 fix 席统筹）。
// DRAIN_LIST_SIZE 仅 debug 导出，release 随剔
#[cfg(debug_assertions)]
#[test]
fn relaxed_registration_lifts_declared_participant_through_full_drain_list() -> Void {
  use std::sync::atomic::AtomicU64;

  let epoch = Arc::new(LightEpoch::new(8));
  let pa = epoch.register()?;
  let pb = epoch.register()?;
  let drained = Arc::new(AtomicU64::new(0));

  // 读者 A：属主线程内先钉快照纪元（safe 停滞的锚），等 go 后自抬注册
  let go = Arc::new(AtomicBool::new(false));
  let a_snapshot = Arc::new(AtomicU64::new(0));
  let reader_a = {
    let epoch = Arc::clone(&epoch);
    let go = Arc::clone(&go);
    let a_snapshot = Arc::clone(&a_snapshot);
    let done = Arc::clone(&drained);
    spawn(move || {
      let guard = pa.enter();
      a_snapshot.store(guard.protected_epoch(), Ordering::Release);
      gate_ready(&go);
      epoch.bump_current_epoch_action_relaxed(&pa, move || {
        done.fetch_add(1, Ordering::AcqRel);
      });
      guard.protected_epoch()
    })
  };
  while a_snapshot.load(Ordering::Acquire) == 0 {
    yield_now();
  }

  // drain_list 填满：恰好 SIZE 次（多一次即非 relaxed 注册会进 help_drain 自旋臂
  // 被 A 钉卡死——这正是被测死锁形态，测试本体不得触发）
  for _ in 0..wepoch::DRAIN_LIST_SIZE {
    let d = Arc::clone(&drained);
    epoch.bump_current_epoch_action(move || {
      d.fetch_add(1, Ordering::AcqRel);
    });
  }
  assert_eq!(
    drained.load(Ordering::Acquire),
    0,
    "滞留守卫钉住时堆积动作不得收割"
  );

  // 读者 B：主线程属主，进保护区（钉当时 current）后全局纪元被裸推高——B 成
  // 非新鲜钉（help_drain 判据不可达，即 rc_grow 形），此刻任何非 relaxed 注册必卡
  let gb = pb.enter();
  let b_before = gb.protected_epoch();
  assert!(b_before > a_snapshot.load(Ordering::Acquire), "装配序错乱");
  epoch.bump_current_epoch();
  assert!(
    gb.protected_epoch() < epoch.current_epoch(),
    "装配须使 B 为非新鲜钉"
  );

  // 发 go：双注册方在各自属主线程并发 relaxed（B 属主即本线程），环内定向
  // 自抬自己 → safe 逐级解锁 → 双闭环
  go.store(true, Ordering::Release);
  let done = Arc::clone(&drained);
  epoch.bump_current_epoch_action_relaxed(&pb, move || {
    done.fetch_add(1, Ordering::AcqRel);
  });
  let _a_final = reader_a.join().expect("A 注册线程零 panic");

  // 注册闭环断言：双注册完成（判据回归时以挂死或计数不闭环形式红）；双方
  // 守卫均被各自环自抬至不低于 go 前的 current。≥SIZE+1：双守卫存活期 safe=
  // 两者较小值，prior 较大的动作延迟收割属纪元语义（非缺陷）
  assert!(
    drained.load(Ordering::Acquire) > wepoch::DRAIN_LIST_SIZE as u64,
    "双注册闭环后堆积动作与至少一个 relaxed 动作必须收割"
  );

  // 双守卫退场收口：exit 的 drain_if_pending 协助排空（A 为最后保护者时
  // SuspendDrain 代收割）→ 残留延迟动作全部收割闭环
  drop(gb);
  assert_eq!(
    drained.load(Ordering::Acquire),
    (wepoch::DRAIN_LIST_SIZE + 2) as u64,
    "双守卫退场后全部动作必须收割闭环"
  );
  OK
}

/// 一次性就绪门（测试辅助：置位前自旋让核，置位后放行）
#[cfg(debug_assertions)]
fn gate_ready(go: &AtomicBool) {
  // A 线程在进入本函数前已置 a_snapshot；此处仅等待主线程发 go
  while !go.load(Ordering::Acquire) {
    yield_now();
  }
}

/// 强环孤岛自愈验收（宿主消亡终态收割，注册面所有权契约的析构半）：
/// 注册多枚动作后负载静止不收割，释尽全部外部 Arc 后 `LightEpoch::drop`
/// 体内无条件收割全部滞留动作（含 Weak 宿主升级失败的退化臂动作）。
///
/// 装配关键：读者线程经 TLS 轨 `resume` 进入保护后持 Arc 停等，主线程注册
/// 完毕释尽自己的 Arc，随后放行读者——读者闭包结束时其 Arc 是末位强引用，
/// 保护态仍存续（TLS 条目仅持弱引用，线程退出兜底复位在 Arc 释放之后才跑、
/// 升级失败跳过），`LightEpoch::drop` 恰在保护态存续时触发，与「宿主消亡
/// 时 drain_list 尚有滞留动作」的生产形态（从库换库等实例消亡时点恰落在
/// 注册与排空之间）同构。
///
/// 红判据（负控 b，还原强捕形态 a 即转红）：动作闭包强持有回指本纪元的宿主
/// （epoch→动作→宿主→epoch 强环）时 Drop 永不到来——本测试在读者线程退出处
/// 挂死、动作计数恒零，即环在则析构收割永不可达。
#[test]
fn drop_harvests_pending_actions_when_last_arc_released() -> Void {
  info!("验证宿主消亡终态 Drop 无条件收割全部滞留动作（含 Weak 退化臂）");

  const ACTIONS: usize = 8;
  let epoch = Arc::new(LightEpoch::new(8));
  let ran = Arc::new(AtomicUsize::new(0));
  let weak_ran = Arc::new(AtomicUsize::new(0));
  let entered = Arc::new(AtomicBool::new(false));
  let go = Arc::new(AtomicBool::new(false));

  // 保护态读者：resume 后持 Arc 停等；放行后闭包结束，末位 Arc 在保护态
  // 存续下释放（TLS 仅弱引用，退出兜底复位升级失败即跳过）。
  // 句柄留待 go 放行后 join——读者自旋等待期间主流程须先完成注册与释 Arc
  let reader = {
    let ep = Arc::clone(&epoch);
    let entered = Arc::clone(&entered);
    let go = Arc::clone(&go);
    spawn(move || {
      ep.resume();
      entered.store(true, Ordering::Release);
      while !go.load(Ordering::Acquire) {
        yield_now();
      }
      // 故意不 suspend：闭包结束即末位 Arc 释放，Drop 于保护态存续中触发
    })
  };
  while !entered.load(Ordering::Acquire) {
    yield_now();
  }

  for _ in 0..ACTIONS {
    let r = Arc::clone(&ran);
    epoch.bump_current_epoch_action(move || {
      r.fetch_add(1, Ordering::SeqCst);
    });
  }
  // Weak 宿主退化臂：宿主先行消亡，升级失败的退化动作同样必须被收割
  {
    let host = Arc::new(AtomicUsize::new(0));
    let host_weak = Arc::downgrade(&host);
    let wr = Arc::clone(&weak_ran);
    epoch.bump_current_epoch_action(move || {
      assert!(
        host_weak.upgrade().is_none(),
        "宿主已消亡，Weak 升级必须失败（退化臂前提）"
      );
      wr.fetch_add(1, Ordering::SeqCst);
    });
  }

  assert!(
    epoch.has_pending_drain(),
    "滞留保护钉住时注册动作必须 pending（无人收割）"
  );
  assert_eq!(
    ran.load(Ordering::SeqCst) + weak_ran.load(Ordering::SeqCst),
    0,
    "收割前任何动作不得执行"
  );

  // 主线程先释自己的 Arc（读者仍持一枚，实例存活），再放行读者：末位 Arc
  // 随读者闭包消亡，Drop 收割全部滞留动作
  let e_weak = Arc::downgrade(&epoch);
  drop(epoch);
  assert!(
    e_weak.upgrade().is_some(),
    "读者停等期间实例必须仍存活（装配序错乱）"
  );
  go.store(true, Ordering::Release);
  // join 须在放行之后：读者自旋等待期间先 join 即主流程自锁（上一版挂死根因）
  reader.join().expect("读者线程零 panic");

  // 读者已退场：实例已析构、滞留动作已全部收割
  assert!(
    e_weak.upgrade().is_none(),
    "末位 Arc 释放后纪元实例必须析构（滞留动作闭包强持本纪元即在此红）"
  );
  assert_eq!(
    ran.load(Ordering::SeqCst),
    ACTIONS,
    "宿主消亡终态：全部滞留动作必须被 Drop 无条件收割"
  );
  assert_eq!(
    weak_ran.load(Ordering::SeqCst),
    1,
    "Weak 升级失败退化臂动作同样必须被收割"
  );

  OK
}
