//! 异步协程协作原语与 Future 辅助工具
//!
//! 全仓生产侧「同步上下文内联驱动 future」收敛为本模块 [`blocking_wait`] 一处，
//! 纯计算驱动另立 [`block_on`] 一口，两口径分工刚性、调用方不得再复刻第三形态
//! （各 crate 内自建同名驱动器即多套架构复发）：
//!
//! - [`blocking_wait`]：生产同步收割口（对标 C# `AsyncUtils.BlockingWait`），
//!   compio 运行时上下文内收割本线程任务队列与 I/O driver，冷读/落盘回读、
//!   挂起锁等待全靠它闭环；
//! - [`block_on`]：纯 park 驱动器，线程不依赖任何运行时，只能推进「由其他线程
//!   unpark 唤醒」的纯计算 future；它没有 I/O driver 收割面，驱动任何触盘
//!   future 都会永久挂起，故仅供本 crate 的纯计算用例与单测消费，生产链路
//!   一律走 [`blocking_wait`]。

use core::{
  future::Future,
  pin::{Pin, pin},
  task::{Context, Poll},
};
use std::{
  sync::Arc,
  task::{Wake, Waker},
  thread,
};

use compio::runtime::Runtime;

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

/// 同步收割 future 至产出值（本仓唯一生产同步收割口，全仓一处定义）。
///
/// 在 garnet 中的相对路径: libs/common/AsyncUtils.cs:BlockingWait
///
/// C# 侧 `Task.GetAwaiter().GetResult()` 靠线程池 + 完成回调兜底，任何线程都能
/// 收割；compio 一线程一运行时，挂起 future 的完成事件只由本线程运行时驱动收割，
/// 故语义按是否有运行时上下文分两态：
///
/// - compio 运行时线程上（生产全部调用点：RESP 命令分派段、gossip 收帧、DiskANN
///   同步回调）走 [`Runtime::block_on`]，挂起窗口内让位给本运行时的其他任务与
///   I/O driver，转发/落盘/锁等待得以在调入线程内闭环；
/// - 无运行时上下文（纯计算单测的已就绪快路径）只能收割一次即就绪的 future：
///   此时没有任何 driver 可收割完成事件，一旦 future 需要挂起即 panic 暴露装配
///   错误。原「park 式手工轮询」回退已删除——它在 io_uring 平台上必然永久挂起，
///   在 poll 平台上也只是借 asyncify 线程池侥幸推进，属调用方自造的第二套驱动；
///   需要挂起的用例按本仓既有形态以 `Runtime::new()` 包裹（真运行时上下文，与
///   生产同路）。
///
/// 与 [`block_on`] 的区别：本函数是「收割已在跑的运行时」，[`block_on`] 是
/// 「无运行时空转驱动」，二者不可互换。
#[inline]
pub fn blocking_wait<F: Future>(f: F) -> F::Output {
  if let Some(rt) = Runtime::try_current() {
    return rt.block_on(f);
  }

  let mut f = pin!(f);
  let mut cx = Context::from_waker(Waker::noop());
  match f.as_mut().poll(&mut cx) {
    Poll::Ready(val) => val,
    Poll::Pending => panic!(
      "blocking_wait: 无 compio 运行时上下文且 future 未就绪，无线程可收割其完成事件（按 \
       Runtime::new 包裹或改为 await）"
    ),
  }
}

/// 轻量 Future 阻塞驱动器（不依赖特定运行时；Pending 时 park 线程让出 CPU）。
///
/// 仅服务纯计算/跨线程 unpark 形态：waker 是本线程的 park 句柄，无人向本线程
/// 投递 unpark 即永不复 poll，且完全没有 I/O driver 收割面。生产同步收割一律
/// 走 [`blocking_wait`]，本函数不得作为其回退分支（详见模块头分工）。
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
  fn test_blocking_wait() {
    // 无运行时上下文：一次即就绪的 future 直接产出（纯计算快路径）
    assert_eq!(blocking_wait(async { 3 }), 3);
    // 运行时上下文：内联收割本运行时任务队列（yield_now 需复 poll 方就绪）
    Runtime::new().unwrap().block_on(async {
      assert_eq!(blocking_wait(async { 7 }), 7);
      assert_eq!(blocking_wait(yield_now()), ());
    });
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
