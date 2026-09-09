//! 纪元延迟清理队列与 Drain 机制测试（对标 Garnet test.epoch/DrainTests）

use std::{
  array::from_fn,
  sync::{
    Arc,
    atomic::{AtomicBool, AtomicU32, Ordering},
  },
  thread::{sleep, spawn, yield_now},
  time::Duration,
};

use aok::{OK, Void};
use log::info;
use parking_lot::Mutex;
use wepoch::LightEpoch;

use super::support::{ParkedReaderThread, join_all};

/// 对标 Garnet DrainTests: ActionRunsImmediatelyWhenNobodyElseIsProtected
///
/// 无其他线程受保护时，注册的延迟动作对应的纪元立即处于安全状态并同步执行
#[test]
fn action_runs_immediately_when_nobody_else_is_protected() -> Void {
  info!("验证无其他线程受保护时延迟动作立即执行");

  let epoch = LightEpoch::new(16);
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

/// 对标 Garnet DrainTests: TheLastThreadToSuspendRunsPendingActions
///
/// 有常驻读者阻止回收时动作暂不执行；最后一个挂起的读者在退出时代为清空已就绪动作
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

/// 对标 Garnet DrainTests: EveryActionRunsExactlyOnceWhenTheDrainListFills
///
/// 填满整个延迟动作队列（容量上限），常驻读者退出后所有延迟动作恰好各自执行一次
#[test]
fn every_action_runs_exactly_once_when_drain_list_fills() -> Void {
  info!("验证满容量延迟队列中每个动作恰好执行一次");

  let epoch = Arc::new(LightEpoch::new(16));
  let capacity = epoch.test_hook_drain_list_capacity();
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

/// 对标 Garnet DrainTests: RegisteringBlocksWhileTheDrainListIsFullAndCompletesAfterwards
///
/// 延迟队列占满且无槽位可回收时，新注册线程阻塞，待阻碍读者退出后解除阻塞并完成注册
#[test]
fn registering_blocks_while_drain_list_is_full_and_completes_afterwards() -> Void {
  info!("验证队列满且未安全时注册动作阻塞，待阻碍者退出后继续完成");

  let epoch = Arc::new(LightEpoch::new(16));
  let capacity = epoch.test_hook_drain_list_capacity();
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

    // 等待 100ms，验证在 reader 仍保护旧纪元且队列占满时，晚到线程被阻塞
    sleep(Duration::from_millis(100));
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

/// 对标 Garnet DrainTests: ActionsRunInEpochOrder
///
/// 跨递增纪元注册的多个动作，在回收时严格按照纪元单调推进顺序排队触发
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

/// 对标 Garnet DrainTests: ManyThreadsRegisteringActionsAllRunExactlyOnce
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

/// 对标 Garnet DrainTests: ActionDoesNotRunWhileAnotherThreadIsProtected
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
fn action_trigger_cascading_and_reentrant_bump_epoch() -> Void {
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
/// （注册：先加计数后公布纪元；消费：先减计数后发布 FREE）——对照 C# LightEpoch.Drain
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
  let counts: Vec<Arc<AtomicU32>> = (0..TOTAL).map(|_| Arc::new(AtomicU32::new(0))).collect();
  let start = Arc::new(AtomicBool::new(false));
  let stop = Arc::new(AtomicBool::new(false));
  let mut handles: Vec<Option<_>> = Vec::new();

  // 注册者：保护区内高频注册延迟动作（动作执行时机完全交给消费方）
  for t in 0..REGISTRARS {
    let ep = Arc::clone(&epoch);
    let all_counts = counts.clone();
    let s = Arc::clone(&start);
    handles.push(Some(spawn(move || {
      while !s.load(Ordering::Acquire) {
        yield_now();
      }
      for i in 0..PER_THREAD {
        let c = Arc::clone(&all_counts[t * PER_THREAD + i]);
        let _scope = ep.protected_scope();
        ep.bump_current_epoch_action(move || {
          c.fetch_add(1, Ordering::SeqCst);
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
        ep.bump_epoch();
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
