//! 无界单消费者清理工作通道（对标 libs/server/Resp/Vector/Cleanup/VectorSetCleanupWorkChannel.cs）
//!
//! 每个元素是一项清理工作单元；关闭后拒绝再发布（shutdown 语义）。

use std::{
  sync::{
    Mutex,
    mpsc::{self, Receiver, Sender},
  },
  time::Duration,
};

/// 内部共享状态：发送端 + 接收端（单消费者）。
/// `front` 为 wait_to_read 预取后暂存的队首元素。
struct Inner<T> {
  sender: Mutex<Option<Sender<T>>>,
  receiver: Mutex<Receiver<T>>,
  front: Mutex<Option<T>>,
}

/// 无界单消费者队列。
pub struct VectorSetCleanupWorkChannel<T> {
  inner: Inner<T>,
}

impl<T> Default for VectorSetCleanupWorkChannel<T> {
  fn default() -> Self {
    Self::new()
  }
}

impl<T> VectorSetCleanupWorkChannel<T> {
  /// 创建无界通道。
  pub fn new() -> Self {
    let (sender, receiver) = mpsc::channel();
    Self {
      inner: Inner {
        sender: Mutex::new(Some(sender)),
        receiver: Mutex::new(receiver),
        front: Mutex::new(None),
      },
    }
  }

  /// libs/server/Resp/Vector/Cleanup/VectorSetCleanupWorkChannel.cs:TryPublish
  ///
  /// 发布一项；通道完成后返回 false（shutdown 期间）。
  pub fn try_publish(&self, item: T) -> bool {
    self
      .inner
      .sender
      .lock()
      .unwrap_or_else(|e| e.into_inner())
      .as_ref()
      .is_some_and(|s| s.send(item).is_ok())
  }

  /// libs/server/Resp/Vector/Cleanup/VectorSetCleanupWorkChannel.cs:TryRead
  ///
  /// 取出下一项（若队列尚有元素）。
  pub fn try_read(&self) -> Option<T> {
    // 优先交还 wait_to_read 预取的队首元素
    if let Some(item) = self
      .inner
      .front
      .lock()
      .unwrap_or_else(|e| e.into_inner())
      .take()
    {
      return Some(item);
    }
    self
      .inner
      .receiver
      .lock()
      .unwrap_or_else(|e| e.into_inner())
      .try_recv()
      .ok()
  }

  /// libs/server/Resp/Vector/Cleanup/VectorSetCleanupWorkChannel.cs:WaitToReadAsync
  ///
  /// 阻塞等待直至可能有元素可读（预取暂存，交由 [`Self::try_read`] 领取）；
  /// 通道完成且已排空时返回 false。
  pub fn wait_to_read(&self, timeout_ms: u64) -> bool {
    if self.has_pending() {
      return true;
    }
    let rx = self
      .inner
      .receiver
      .lock()
      .unwrap_or_else(|e| e.into_inner());
    match rx.recv_timeout(Duration::from_millis(timeout_ms)) {
      Ok(item) => {
        *self.inner.front.lock().unwrap_or_else(|e| e.into_inner()) = Some(item);
        true
      }
      Err(_) => false,
    }
  }

  /// 队列中是否尚有元素（C# HasPending）。
  pub fn has_pending(&self) -> bool {
    if self
      .inner
      .front
      .lock()
      .unwrap_or_else(|e| e.into_inner())
      .is_some()
    {
      return true;
    }
    // 预取探测（mpsc 无 peek；元素暂存 front 供 try_read 领取）
    match self
      .inner
      .receiver
      .lock()
      .unwrap_or_else(|e| e.into_inner())
      .try_recv()
    {
      Ok(item) => {
        *self.inner.front.lock().unwrap_or_else(|e| e.into_inner()) = Some(item);
        true
      }
      Err(_) => false,
    }
  }

  /// libs/server/Resp/Vector/Cleanup/VectorSetCleanupWorkChannel.cs:DrainPending
  ///
  /// 丢弃全部排队元素（仅适用于无载荷通道；有载荷的丢弃即丢失工作）。
  pub fn drain_pending(&self) -> usize {
    let mut n = 0;
    while self.try_read().is_some() {
      n += 1;
    }
    n
  }

  /// libs/server/Resp/Vector/Cleanup/VectorSetCleanupWorkChannel.cs:CompleteAndWaitForConsumerTask
  ///
  /// 停止接收并排空；消费者线程由调用方 join。
  pub fn complete(&self) {
    *self.inner.sender.lock().unwrap_or_else(|e| e.into_inner()) = None;
  }

  /// 通道是否已完成（不再接收）。
  pub fn is_completed(&self) -> bool {
    self
      .inner
      .sender
      .lock()
      .unwrap_or_else(|e| e.into_inner())
      .is_none()
  }
}

#[cfg(test)]
mod tests {
  use super::*;

  #[test]
  fn publish_read_and_complete() {
    let ch: VectorSetCleanupWorkChannel<u64> = VectorSetCleanupWorkChannel::new();
    assert!(ch.try_publish(1));
    assert!(ch.try_publish(2));
    assert!(ch.has_pending());
    assert_eq!(ch.try_read(), Some(1));
    assert_eq!(ch.try_read(), Some(2));
    assert_eq!(ch.try_read(), None);

    assert!(ch.try_publish(3));
    assert_eq!(ch.drain_pending(), 1);
    assert!(!ch.has_pending());

    ch.complete();
    assert!(ch.is_completed());
    // 完成后拒绝发布
    assert!(!ch.try_publish(4));
    assert_eq!(ch.try_read(), None);
  }
}
