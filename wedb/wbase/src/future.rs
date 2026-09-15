//! 异步协程协作原语与 Future 辅助工具

use core::{
  future::Future,
  pin::Pin,
  task::{Context, Poll},
};
use std::{
  sync::Arc,
  task::{Wake, Waker},
  thread,
};

/// Future 就绪唤醒器（以线程 unpark 承接跨线程唤醒）。
struct ThreadWaker(thread::Thread);

impl Wake for ThreadWaker {
  #[inline]
  fn wake(self: Arc<Self>) {
    self.0.unpark();
  }

  #[inline]
  fn wake_by_ref(self: &Arc<Self>) {
    self.0.unpark();
  }
}

/// 轻量 Future 阻塞驱动器（不依赖特定运行时；Pending 时 park 线程让出 CPU）。
pub fn block_on<F: Future>(f: F) -> F::Output {
  let mut f = Box::pin(f);
  let waker = Waker::from(Arc::new(ThreadWaker(thread::current())));
  let mut cx = Context::from_waker(&waker);
  loop {
    match f.as_mut().poll(&mut cx) {
      Poll::Ready(val) => return val,
      Poll::Pending => thread::park(),
    }
  }
}

/// 协程协作让步 Future（1:1 对标 C# Task.Yield）。
///
/// 首次 poll 时向上下文注册 waker 并返回 [`Poll::Pending`]，将当前任务重新挂入
/// 执行器就绪队列；第二次 poll 时返回 [`Poll::Ready(())`] 恢复执行。
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
#[must_use = "futures do nothing unless you `.await` or poll them"]
pub struct YieldNow(pub bool);

impl Future for YieldNow {
  type Output = ();

  #[inline]
  fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
    if self.0 {
      Poll::Ready(())
    } else {
      self.0 = true;
      cx.waker().wake_by_ref();
      Poll::Pending
    }
  }
}

/// 协程协作让步（将当前任务重新挂入就绪队列并让出执行权）。
#[inline(always)]
pub const fn yield_now() -> YieldNow {
  YieldNow(false)
}

#[cfg(test)]
mod tests {
  use core::task::Waker;
  use std::{
    sync::atomic::{AtomicBool, Ordering},
    time::Duration,
  };

  use super::*;

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
    // 跨线程唤醒：Future 阻塞等待子线程 unpark
    let flag = Arc::new(AtomicBool::new(false));
    let f_flag = Arc::clone(&flag);
    let val = block_on(async move {
      let handle = thread::spawn(move || {
        thread::sleep(Duration::from_millis(1));
        f_flag.store(true, Ordering::Release);
      });
      // 自定义 Future：flag 置位前 Pending（waker 已注册，子线程置位后让步重查）
      struct WaitFlag(Arc<AtomicBool>);
      impl Future for WaitFlag {
        type Output = ();

        fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
          if self.0.load(Ordering::Acquire) {
            return Poll::Ready(());
          }
          cx.waker().wake_by_ref();
          Poll::Pending
        }
      }
      WaitFlag(flag).await;
      handle.join().expect("子线程不 panic");
      42
    });
    assert_eq!(val, 42);
  }
}
