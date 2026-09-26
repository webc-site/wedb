//! 自研依据: 同步上下文 block_on 执行器（compio 形态，C# 无对应）
use core::task::Waker;
use std::{
  future::Future,
  pin::Pin,
  sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
  },
  task::{Context, Poll},
  thread,
  time::Duration,
};

use parking_lot::Mutex;
use wbase::future::{YieldNow, block_on, yield_now};

#[test]
fn test_yield_now() {
  let mut y = yield_now();
  let waker = Waker::noop();
  let mut cx = Context::from_waker(waker);

  assert_eq!(Pin::new(&mut y).poll(&mut cx), Poll::Pending);
  assert_eq!(Pin::new(&mut y).poll(&mut cx), Poll::Ready(()));
  // 验证熔断幂等性
  assert_eq!(Pin::new(&mut y).poll(&mut cx), Poll::Ready(()));
  // 验证 Default 等价性
  assert_eq!(YieldNow::default(), yield_now());
}

#[test]
fn test_block_on() {
  // 就绪 Future 直接产出值
  assert_eq!(block_on(async { 3 }), 3);
  // 让步 Future：首次 poll 注册唤醒并 Pending，park 后二次 poll Ready
  block_on(yield_now());
}

#[test]
fn test_block_on_cross_thread_wake() {
  // 真实跨线程唤醒：Future 进入 Pending park 挂起，子线程就绪后显式通过 waker.wake() 唤醒
  let flag = Arc::new(AtomicBool::new(false));
  let waker_slot: Arc<parking_lot::Mutex<Option<Waker>>> = Arc::new(Mutex::new(None));
  let f_flag = Arc::clone(&flag);
  let f_waker = Arc::clone(&waker_slot);

  let handle = thread::spawn(move || {
    thread::sleep(Duration::from_millis(5));
    f_flag.store(true, Ordering::Release);
    // 轮询直至等待者存入 waker 并予以唤醒
    loop {
      if let Some(w) = f_waker.lock().take() {
        w.wake();
        break;
      }
      thread::yield_now();
    }
  });

  struct WaitFlag {
    flag: Arc<AtomicBool>,
    waker: Arc<parking_lot::Mutex<Option<Waker>>>,
  }

  impl Future for WaitFlag {
    type Output = ();

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
      if self.flag.load(Ordering::Acquire) {
        return Poll::Ready(());
      }
      *self.waker.lock() = Some(cx.waker().clone());
      if self.flag.load(Ordering::Acquire) {
        Poll::Ready(())
      } else {
        Poll::Pending
      }
    }
  }

  let val = block_on(async move {
    WaitFlag {
      flag,
      waker: waker_slot,
    }
    .await;
    handle.join().expect("子线程不 panic");
    42
  });
  assert_eq!(val, 42);
}
