#![recursion_limit = "512"] // 泛型设备实例化下 async 状态机嵌套深（R27 CI 实证）
//! 换号元数据串行锁（SerialLock）异步等待与争用挂起测试（自 src/store/mod.rs 外迁）
//!
//! 换号元数据串行锁的等待形态自证：快路径一次 CAS、争用真挂起（零唤醒零
//! CPU）、释放精准移交一位（task/ing/my-dbmeta-lock-yield-spin 的「争用下不
//! 烧 CPU」行为面验收）。
//!
//! 覆盖：
//! 1. uncontended_acquire_needs_one_poll_and_no_wake: 无争用快路径一次 poll 即取到锁，零唤醒；
//! 2. contended_waiter_sleeps_until_handoff: 争用下等待者真挂起（持锁期内等待者零唤醒），释放恰醒一位；
//! 3. contended_tasks_serialize_across_await: 真运行时下的跨 await 串行性，临界区不得重叠，无死锁。

use std::{
  pin::pin,
  sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
  },
  task::{Context, Poll, Wake, Waker},
  time::Duration,
};

use compio::{
  runtime::{Runtime, spawn},
  time::sleep,
};
use wbase::future::yield_now;
use wkv::SerialLock;

/// 唤醒计数 waker：运行时只在任务被唤醒后才 poll 它，故等待期的 wake 次数
/// 就是该任务占用的调度次数（不引 libc 取线程 CPU 时间，wake 数是更直接的
/// 观测量——烧 CPU 的形态正是「自唤醒把自己反复灌进就绪队列」）
#[derive(Default)]
struct WakeCounter(AtomicUsize);

impl WakeCounter {
  fn get(&self) -> usize {
    self.0.load(Ordering::Relaxed)
  }
}

impl Wake for WakeCounter {
  fn wake(self: Arc<Self>) {
    self.0.fetch_add(1, Ordering::Relaxed);
  }
  fn wake_by_ref(self: &Arc<Self>) {
    self.0.fetch_add(1, Ordering::Relaxed);
  }
}

#[test]
fn uncontended_acquire_needs_one_poll_and_no_wake() {
  let lock = SerialLock::default();
  let wakes = Arc::new(WakeCounter::default());
  let waker = Waker::from(Arc::clone(&wakes));
  let mut cx = Context::from_waker(&waker);

  let mut acquire = pin!(lock.acquire());
  let Poll::Ready(guard) = acquire.as_mut().poll(&mut cx) else {
    panic!("无争用快路径应一次 poll 即取到锁");
  };
  assert!(lock.is_busy(), "取锁后认领位应置位");
  assert_eq!(0, lock.total_listeners(), "快路径不入等待队列");
  assert_eq!(0, wakes.get(), "快路径不应产生任何唤醒");

  drop(guard);
  assert!(!lock.is_busy(), "守卫释放应清认领位");
  assert_eq!(0, wakes.get(), "无等待者时释放不多发通知");
}

/// 争用下等待者真挂起的行为面自证（本票核心诉求）
///
/// 改造前形态：`while compare_exchange 失败 { yield_now().await }`——
/// `wbase::future::YieldNow` 的首次 poll 必 `cx.waker().wake_by_ref()` 再返回
/// Pending，故持锁临界区（DbMeta 原子批落盘，含 IO）每推进一刻，等待任务就
/// 被自唤醒并重新 poll 一次，wake 计数随时长线性增长（钉核下还在持续占用
/// 该核调度槽）。改造后：等待者挂进事件队列，临界区整段零唤醒，释放时恰醒一次。
#[test]
fn contended_waiter_sleeps_until_handoff() {
  let lock = SerialLock::default();
  let wakes = Arc::new(WakeCounter::default());
  let waker = Waker::from(Arc::clone(&wakes));
  let mut cx = Context::from_waker(&waker);

  // 持有者：快路径取锁后进入临界区（生产形态为 await persist_dbmeta_batch）
  let mut acquire = pin!(lock.acquire());
  let Poll::Ready(holder) = acquire.as_mut().poll(&mut cx) else {
    panic!("无争用快路径应一次 poll 即取到锁");
  };

  // 等待者：首次 poll 走完「CAS 失败 → 注册监听 → 复核 CAS 失败 → 挂起」
  let mut waiter = pin!(lock.acquire());
  assert!(
    matches!(waiter.as_mut().poll(&mut cx), Poll::Pending),
    "锁被占用时等待者应挂起而非就绪"
  );
  assert_eq!(1, lock.total_listeners(), "等待者应挂在事件队列上");
  assert_eq!(0, wakes.get(), "挂起不得自唤醒（自旋形态此处即开始烧 CPU）");

  // 临界区期间的重复观测：未被唤醒即不会被调度，等待任务零 CPU
  assert_eq!(0, wakes.get(), "持锁期内等待者零唤醒");
  assert_eq!(1, lock.total_listeners(), "等待者应稳定挂在队列上");

  // 释放：清认领位 + 精准移交一位
  drop(holder);
  assert!(!lock.is_busy());
  assert_eq!(1, wakes.get(), "释放应恰好唤醒一位等待者");

  let mut waiter2 = pin!(lock.acquire());
  let Poll::Ready(second) = waiter.as_mut().poll(&mut cx) else {
    panic!("移交后等待者应取到锁");
  };
  assert!(lock.is_busy(), "交接后认领位仍置位");
  // 队列已把移交出的名额用掉：新来者在锁未释放前挂进队列
  assert!(matches!(waiter2.as_mut().poll(&mut cx), Poll::Pending));
  assert_eq!(1, lock.total_listeners());
  assert_eq!(1, wakes.get(), "第三任务挂起不产生额外唤醒");
  drop(second);
  assert_eq!(2, wakes.get(), "再一次移交恰醒一位");
  assert!(matches!(waiter2.as_mut().poll(&mut cx), Poll::Ready(_)));
}

/// 真运行时下的跨 await 串行性自证：八任务各四轮，临界区内必 await 一次
/// （对位落盘让渡），断言任一时刻至多一位持有者且全部轮次推进完成（不丢
/// 唤醒、不死锁）
#[test]
fn contended_tasks_serialize_across_await() {
  const TASKS: usize = 8;
  const ROUNDS: usize = 4;

  let rt = Runtime::new().expect("compio 运行时构造失败");
  rt.block_on(async {
    let lock = Arc::new(SerialLock::default());
    let held = Arc::new(AtomicUsize::new(0));
    let overlap = Arc::new(AtomicUsize::new(0));
    let done = Arc::new(AtomicUsize::new(0));

    let mut handles = Vec::with_capacity(TASKS);
    for _ in 0..TASKS {
      let lock = Arc::clone(&lock);
      let held = Arc::clone(&held);
      let overlap = Arc::clone(&overlap);
      let done = Arc::clone(&done);
      handles.push(spawn(async move {
        for _ in 0..ROUNDS {
          let _guard = lock.acquire().await;
          if held.fetch_add(1, Ordering::AcqRel) != 0 {
            overlap.fetch_add(1, Ordering::Relaxed);
          }
          // 临界区内跨 await：同步锁在此形态下会把整核 park 死
          sleep(Duration::from_millis(1)).await;
          yield_now().await;
          held.fetch_sub(1, Ordering::AcqRel);
          done.fetch_add(1, Ordering::Relaxed);
        }
      }));
    }
    for h in handles {
      h.await.expect("换号任务不应被取消");
    }

    assert_eq!(0, overlap.load(Ordering::Relaxed), "临界区不得重叠");
    assert_eq!(
      TASKS * ROUNDS,
      done.load(Ordering::Relaxed),
      "全部轮次应推进完成"
    );
    assert_eq!(0, held.load(Ordering::Relaxed));
    assert!(!lock.is_busy(), "末轮释放后应无残留认领");
  });
}
