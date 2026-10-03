//! 优雅停机协调器
//!
//! 统一管理工作线程的停机通知，基于 AtomicBool + event_listener::Event 实现 O(1) 广播，
//! 消除为每个任务创建 bounded_async(1) 与 Mutex<Vec<AsyncTx>> 遍历发送的反模式。

use std::sync::{
  Arc,
  atomic::{AtomicBool, Ordering},
};

use event_listener::{Event, EventListener};

struct ShutdownCoordinatorInner {
  /// 停机状态标志
  is_stopped: AtomicBool,
  /// 停机广播事件
  event: Event,
}

/// 优雅停机协调器
#[derive(Clone)]
pub struct ShutdownCoordinator(Arc<ShutdownCoordinatorInner>);

impl Default for ShutdownCoordinator {
  fn default() -> Self {
    Self::new()
  }
}

impl ShutdownCoordinator {
  /// 创建新的优雅停机协调器
  pub fn new() -> Self {
    Self(Arc::new(ShutdownCoordinatorInner {
      is_stopped: AtomicBool::new(false),
      event: Event::new(),
    }))
  }

  /// 当前是否已处于停机态
  #[inline]
  pub fn is_stopped(&self) -> bool {
    self.0.is_stopped.load(Ordering::Acquire)
  }

  /// 触发优雅停机：翻转停机标志并 O(1) 广播唤醒所有监听者
  pub fn stop(&self) {
    if self
      .0
      .is_stopped
      .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
      .is_ok()
    {
      self.0.event.notify(usize::MAX);
    }
  }

  /// 订阅停机通知监听器
  #[inline]
  pub fn listen(&self) -> EventListener {
    self.0.event.listen()
  }

  /// 异步等待停机信号（防竞态：先注册监听器后复查状态，纯事件驱动）
  ///
  /// 先注册 `listen()`（`event_listener` 创建即插入等待链），
  /// 再复查 `is_stopped` 状态：若观测到未停机，后续 `stop()` 的 CAS 置位与
  /// `notify` 必晚于插入，通知必达本 listener，无丢唤醒窗口；
  /// 若已停机则直接返回，避免无谓挂起。
  pub async fn wait(&self) {
    let listener = self.listen();
    if self.is_stopped() {
      return;
    }
    listener.await;
  }
}
