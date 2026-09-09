use std::sync::Arc;
use std::thread;

use aok::{OK, Void};
use log::info;
use wsync::{DoubleTurnstileBarrier, LeaderBarrier, LockType, ReadOptimizedLock};

#[ctor::ctor(unsafe)]
fn _log_init() {
  log_init::init();
}

#[test]
fn test() -> Void {
  info!("> test {}", 123456);
  OK
}

/// LeaderBarrier 三方汇合：恰好一名首参与者返回 true，其余阻塞至 Release；计数回拨报类型化错误
#[test]
fn leader_barrier_rendezvous_and_underflow() -> Void {
  let barrier = Arc::new(LeaderBarrier::new(3));
  // 独立释放线程：三方（主线程 + 2 工作线程）无论谁先抵达均能完成汇合
  let releaser = {
    let b = Arc::clone(&barrier);
    thread::spawn(move || {
      thread::sleep(std::time::Duration::from_millis(50));
      b.release();
    })
  };
  let workers: Vec<_> = (0..2)
    .map(|_| {
      let b = Arc::clone(&barrier);
      thread::spawn(move || b.try_signal_or_wait(None).unwrap())
    })
    .collect();
  let main_is_leader = barrier.try_signal_or_wait(None).unwrap();

  let mut leader_count = u32::from(main_is_leader);
  for w in workers {
    leader_count += u32::from(w.join().unwrap());
  }
  assert_eq!(leader_count, 1, "三方汇合中恰好一人成为 leader");
  releaser.join().unwrap();

  // 全员重复 Signal：arrived_count 回拨为负 → CountUnderflow
  let err = barrier.try_signal_or_wait(None).unwrap_err();
  assert_eq!(err, wsync::Error::CountUnderflow(-1));
  OK
}

/// LeaderBarrier 超时：单人汇合等待其余参与者，超时返回 Timeout
#[test]
fn leader_barrier_timeout() -> Void {
  let barrier = LeaderBarrier::new(2);
  let res = barrier.try_signal_or_wait(Some(std::time::Duration::from_millis(20)));
  assert_eq!(res.unwrap_err(), wsync::Error::Timeout);
  OK
}

/// DoubleTurnstileBarrier：三方两阶段循环汇合后计数归零，可复用；非法参数类型化报错
#[test]
fn double_turnstile_cycles() -> Void {
  assert_eq!(
    DoubleTurnstileBarrier::new(0).err(),
    Some(wsync::Error::InvalidParticipantCount(0))
  );

  let barrier = Arc::new(DoubleTurnstileBarrier::new(3).unwrap());
  let workers: Vec<_> = (0..2)
    .map(|_| {
      let b = Arc::clone(&barrier);
      thread::spawn(move || {
        for _ in 0..8 {
          b.signal_work_ready_wait();
          b.signal_work_completed_wait();
        }
      })
    })
    .collect();
  for _ in 0..8 {
    barrier.signal_work_ready_wait();
    barrier.signal_work_completed_wait();
  }
  for w in workers {
    w.join().unwrap();
  }
  OK
}

/// ReadOptimizedLock：共享锁互斥写锁、写锁全量排他、释放后可复取
#[test]
fn read_optimized_lock_shared_exclusive() -> Void {
  let locks = ReadOptimizedLock::new(1024, 4);
  let shared = locks.acquire_shared_lock(0x1234_5678);
  assert_eq!(shared.typ, LockType::Shared);
  // 持共享锁时独占锁不可获取
  assert!(locks.try_acquire_exclusive_lock(0x1234_5678).is_none());
  locks.release_lock(&shared);
  assert!(locks.try_acquire_exclusive_lock(0x1234_5678).is_some());
  // 写锁持有期间共享锁不可获取，释放后恢复
  locks.release_lock(&wsync::LockToken {
    token: 0x1234_5678_u32 as i32,
    typ: LockType::Exclusive,
  });
  let again = locks.acquire_shared_lock(0x1234_5678);
  locks.release_lock(&again);
  OK
}
