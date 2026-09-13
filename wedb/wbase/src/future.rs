//! 异步协程协作原语与 Future 辅助工具

use core::{
  future::Future,
  pin::Pin,
  task::{Context, Poll},
};

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
}
