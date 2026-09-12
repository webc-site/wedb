//! 优雅停机协调器
//!
//! 统一管理工作线程的取消通知通道，实现即时唤醒、有序排空与安全停机。

use std::{
  sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
  },
  time::Duration,
};

use compio::time::{sleep, timeout};
use crossfire::{
  AsyncRx, AsyncTx, MAsyncTx,
  mpsc::{Array, bounded_async},
};
use parking_lot::Mutex;

/// 优雅停机协调器
#[derive(Clone)]
pub struct ShutdownCoordinator {
  /// 停机状态标志
  is_stopped: Arc<AtomicBool>,
  /// 主循环唤醒发送端（多生产者模式，支持并发触发）
  wake_tx: MAsyncTx<Array<()>>,
  /// 主循环唤醒接收端（首次获取后取走）
  wake_rx: Arc<Mutex<Option<AsyncRx<Array<()>>>>>,
  /// 各工作线程或后台任务的取消通知通道列表
  cancel_senders: Arc<Mutex<Vec<AsyncTx<Array<()>>>>>,
}

impl Default for ShutdownCoordinator {
  fn default() -> Self {
    Self::new()
  }
}

impl ShutdownCoordinator {
  /// 创建新的优雅停机协调器
  pub fn new() -> Self {
    let (wake_tx, wake_rx) = bounded_async::<()>(1);
    Self {
      is_stopped: Arc::new(AtomicBool::new(false)),
      wake_tx,
      wake_rx: Arc::new(Mutex::new(Some(wake_rx))),
      cancel_senders: Arc::new(Mutex::new(Vec::new())),
    }
  }

  /// 当前是否已处于停机态
  #[inline]
  pub fn is_stopped(&self) -> bool {
    self.is_stopped.load(Ordering::Acquire)
  }

  /// 获取停机状态原子句柄
  #[inline]
  pub fn stopped_handle(&self) -> Arc<AtomicBool> {
    Arc::clone(&self.is_stopped)
  }

  /// 注册一个工作线程或后台任务的取消通知端
  pub fn register_cancel_sender(&self, tx: AsyncTx<Array<()>>) {
    self.cancel_senders.lock().push(tx);
  }

  /// 创建并注册一对用于后台任务的取消通知通道
  pub fn new_cancel_channel(&self) -> AsyncRx<Array<()>> {
    let (tx, rx) = bounded_async::<()>(1);
    self.cancel_senders.lock().push(tx.into());
    rx
  }

  /// 触发优雅停机：翻转停机标志，广播取消通知，唤醒主等待循环
  pub fn stop(&self) {
    if self
      .is_stopped
      .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
      .is_ok()
    {
      // 唤醒主循环
      let _ = self.wake_tx.try_send(());

      // 广播打断所有工作线程
      let senders = self.cancel_senders.lock();
      for tx in senders.iter() {
        let _ = tx.try_send(());
      }
    }
  }

  /// 取走主循环唤醒接收端（仅限单次获取）
  pub fn take_wake_rx(&self) -> Option<AsyncRx<Array<()>>> {
    self.wake_rx.lock().take()
  }

  /// 阻塞或超时等待停机唤醒
  pub async fn wait_stopped(&self, fallback_timeout: Duration) {
    if self.is_stopped() {
      return;
    }
    let rx_opt = self.take_wake_rx();
    if let Some(rx) = rx_opt {
      match timeout(fallback_timeout, rx.recv()).await {
        Ok(_) => {}
        Err(_) => {
          // 超时继续轮询标志位
        }
      }
    } else {
      sleep(fallback_timeout).await;
    }
  }
}
