//! 无界单消费者清理工作通道（对标 libs/server/Resp/Vector/Cleanup/VectorSetCleanupWorkChannel.cs）
//!
//! 基于轻量有锁队列（`parking_lot::Mutex<VecDeque<T>>`）与 `event_listener` 异步事件通知构建。
//! 遵循 crossfire 官方实践指导：在锁竞争不激烈的单消费者后台任务场景，
//! 有锁队列相比无锁队列不仅内存紧凑零额外分配，且彻底消除锁套娃、原子自旋与缓存颠簸。

use std::{
  collections::VecDeque,
  sync::atomic::{AtomicBool, Ordering},
  time::{Duration, Instant},
};

use event_listener::{Event, Listener};
use parking_lot::Mutex;

/// 单消费者清理工作通道（有锁连续缓冲队列 + 事件驱动挂起与唤醒）。
pub struct VectorSetCleanupWorkChannel<T> {
  queue: Mutex<VecDeque<T>>,
  event: Event,
  is_completed: AtomicBool,
}

impl<T> Default for VectorSetCleanupWorkChannel<T> {
  fn default() -> Self {
    Self::new()
  }
}

impl<T> VectorSetCleanupWorkChannel<T> {
  /// 创建工作通道。
  pub fn new() -> Self {
    Self {
      queue: Mutex::new(VecDeque::new()),
      event: Event::new(),
      is_completed: AtomicBool::new(false),
    }
  }

  /// libs/server/Resp/Vector/Cleanup/VectorSetCleanupWorkChannel.cs:TryPublish
  ///
  /// 发布一项；通道完成后返回 false（shutdown 期间）。
  pub fn try_publish(&self, item: T) -> bool {
    if self.is_completed.load(Ordering::Acquire) {
      return false;
    }
    {
      let mut q = self.queue.lock();
      if self.is_completed.load(Ordering::Acquire) {
        return false;
      }
      q.push_back(item);
    }
    self.event.notify(1);
    true
  }

  /// libs/server/Resp/Vector/Cleanup/VectorSetCleanupWorkChannel.cs:TryRead
  ///
  /// 取出下一项（若队列尚有元素）。
  pub fn try_read(&self) -> Option<T> {
    self.queue.lock().pop_front()
  }

  /// libs/server/Resp/Vector/Cleanup/VectorSetCleanupWorkChannel.cs:HasPending
  ///
  /// 队列中是否尚有元素。
  pub fn has_pending(&self) -> bool {
    !self.queue.lock().is_empty()
  }

  /// libs/server/Resp/Vector/Cleanup/VectorSetCleanupWorkChannel.cs:WaitToReadAsync
  ///
  /// 异步等待直至可能有元素可读；通道完成且已排空时返回 false。
  pub async fn wait_to_read(&self) -> bool {
    loop {
      let listener = {
        let q = self.queue.lock();
        if !q.is_empty() {
          return true;
        }
        if self.is_completed.load(Ordering::Acquire) {
          return false;
        }
        self.event.listen()
      };
      listener.await;
    }
  }

  /// 同步带超时等待直至可能有元素可读（供非异步后台线程兼容使用）。
  pub fn wait_to_read_timeout(&self, timeout_ms: u64) -> bool {
    let deadline = Instant::now() + Duration::from_millis(timeout_ms);
    loop {
      let listener = {
        let q = self.queue.lock();
        if !q.is_empty() {
          return true;
        }
        if self.is_completed.load(Ordering::Acquire) {
          return false;
        }
        self.event.listen()
      };
      let now = Instant::now();
      if now >= deadline {
        return self.has_pending();
      }
      let remaining = deadline - now;
      if listener.wait_timeout(remaining).is_none() {
        return self.has_pending();
      }
    }
  }

  /// libs/server/Resp/Vector/Cleanup/VectorSetCleanupWorkChannel.cs:DrainPending
  ///
  /// 丢弃全部排队元素（仅适用于无载荷通道；有载荷的丢弃即丢失工作）。
  pub fn drain_pending(&self) -> usize {
    let mut q = self.queue.lock();
    let len = q.len();
    q.clear();
    len
  }

  /// libs/server/Resp/Vector/Cleanup/VectorSetCleanupWorkChannel.cs:CompleteAndWaitForConsumerTask
  ///
  /// 停止接收并关闭发送端，唤醒所有等待读取的协程。
  ///
  /// 持锁 store + notify：与 [`Self::wait_to_read`] 系列的空查→注册监听
  /// 同锁互斥，消除「notify 先于监听注册即丢失」的挂死窗口。
  pub fn complete(&self) {
    let q = self.queue.lock();
    self.is_completed.store(true, Ordering::Release);
    self.event.notify(usize::MAX);
    drop(q);
  }

  /// 通道是否已完成（不再接收，无锁原子读取）。
  #[inline(always)]
  pub fn is_completed(&self) -> bool {
    self.is_completed.load(Ordering::Acquire)
  }
}
