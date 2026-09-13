use std::collections::VecDeque;

use event_listener::Event;
use parking_lot::Mutex;

/// 标准通用的低频异步工作队列
pub struct EventWorkQueue<T> {
  queue: Mutex<VecDeque<T>>,
  event: Event,
  closed: std::sync::atomic::AtomicBool,
}

impl<T> Default for EventWorkQueue<T> {
  fn default() -> Self {
    Self::new()
  }
}

impl<T> EventWorkQueue<T> {
  pub fn new() -> Self {
    Self {
      queue: Mutex::new(VecDeque::new()),
      event: Event::new(),
      closed: std::sync::atomic::AtomicBool::new(false),
    }
  }

  /// 推入一项；若队列已关闭，返回 false
  pub fn push(&self, item: T) -> bool {
    if self.closed.load(std::sync::atomic::Ordering::Acquire) {
      return false;
    }
    {
      let mut q = self.queue.lock();
      if self.closed.load(std::sync::atomic::Ordering::Acquire) {
        return false;
      }
      q.push_back(item);
    }
    self.event.notify(1);
    true
  }

  /// 尝试取出一项
  pub fn try_pop(&self) -> Option<T> {
    self.queue.lock().pop_front()
  }

  /// 异步等待直至可能有元素可读；通道关闭且排空时返回 false
  pub async fn wait_to_read(&self) -> bool {
    loop {
      let listener = {
        let q = self.queue.lock();
        if !q.is_empty() {
          return true;
        }
        if self.closed.load(std::sync::atomic::Ordering::Acquire) {
          return false;
        }
        self.event.listen()
      };
      listener.await;
    }
  }

  /// 丢弃并返回所有排队元素
  pub fn drain(&self) -> Vec<T> {
    let mut q = self.queue.lock();
    std::mem::take(&mut *q).into_iter().collect()
  }

  /// 关闭队列并唤醒所有等待者
  pub fn close(&self) {
    let q = self.queue.lock();
    self
      .closed
      .store(true, std::sync::atomic::Ordering::Release);
    self.event.notify(usize::MAX);
    drop(q);
  }

  /// 队列是否已关闭
  pub fn is_closed(&self) -> bool {
    self.closed.load(std::sync::atomic::Ordering::Acquire)
  }

  /// 队列是否为空
  pub fn is_empty(&self) -> bool {
    self.queue.lock().is_empty()
  }

  /// 队列长度
  pub fn len(&self) -> usize {
    self.queue.lock().len()
  }
}
