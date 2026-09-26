//! 异步协程协作原语与 Future 辅助工具
//!
//! 全仓仅存 [`block_on`] 一口：纯 park 驱动器，线程不依赖任何运行时，只能推进
//! 「由其他线程 unpark 唤醒」的纯计算 future；没有 I/O driver 收割面，驱动任何
//! 触盘 future 都会永久挂起，故仅供纯计算用例与单测消费。
//!
//! 历史注记：旧「同步上下文内联驱动」两口（`blocking_wait`/`inline_wait`，对标
//! C# `AsyncUtils.BlockingWait`）已随任务栈内同步收割的退役全链删除——compio
//! thread-per-core 下任务 poll 栈内重入驱动调度器会击穿 executor 迭代器
//! （compio-executor `next_hot` 的 `item.is_hot` 断言），同步契约边界的异步
//! 需求一律以协程让渡（Luau lua_yield/lua_resume）或挂起臂登记 + 泵 await
//! 驱动承接，严禁复刻内联驱动第三形态。

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

thread_local! {
  static CURRENT_THREAD_WAKER: Waker = Waker::from(Arc::new(ThreadWaker(thread::current())));
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
/// 轻量 Future 阻塞驱动器（不依赖特定运行时；Pending 时 park 线程让出 CPU）。
///
/// 仅服务纯计算/跨线程 unpark 形态：waker 是本线程的 park 句柄，无人向本线程
/// 投递 unpark 即永不复 poll，且完全没有 I/O driver 收割面。生产链路的同步收割已全链退役（见模块头历史注记）。
pub fn block_on<F: Future>(f: F) -> F::Output {
  let mut f = pin!(f);
  CURRENT_THREAD_WAKER.with(|waker| {
    let mut cx = Context::from_waker(waker);
    loop {
      match f.as_mut().poll(&mut cx) {
        Poll::Ready(val) => return val,
        Poll::Pending => thread::park(),
      }
    }
  })
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
