//! 慢客户端背压控制（Slow Client Throttle）
//!
//! 1:1 对标微软 Garnet GarnetTcpNetworkSender.Throttle
//!
//! 限制每个网络会话并发在途发送请求数（默认 ThrottleMax = 8）；
//! 当在途数超过上限时产生背压等待；连接关闭时单次屏障 (i32::MIN) 快速唤醒。

use std::sync::atomic::{
  AtomicI32,
  Ordering::{AcqRel, Acquire, Release},
};

use event_listener::Event;
use thiserror::Error;

/// 节流器已关闭错误
#[derive(Debug, Clone, Copy, PartialEq, Eq, Error)]
#[error("网络发送节流器已关闭")]
pub struct ThrottleClosed;

/// 慢客户端网络发送节流控制器
pub struct NetworkSenderThrottle {
  /// 最大并发在途发送数
  throttle_max: i32,
  /// 当前在途发送计数（负值表示已进入关闭屏障）
  throttle_count: AtomicI32,
  /// 唤醒等待事件
  event: Event,
}

impl NetworkSenderThrottle {
  /// 创建节流控制器
  pub fn new(throttle_max: usize) -> Self {
    Self {
      throttle_max: (throttle_max.max(1)) as i32,
      throttle_count: AtomicI32::new(0),
      event: Event::new(),
    }
  }

  /// 进入发送：若在途计数达到上限则异步等待（背压）
  ///
  /// 返回 Ok(()) 表示可执行发送；Err(ThrottleClosed) 表示连接已关闭，应丢弃发送
  pub async fn enter_send(&self) -> Result<(), ThrottleClosed> {
    loop {
      let current = self.throttle_count.load(Acquire);
      if current < 0 {
        return Err(ThrottleClosed);
      }
      if current < self.throttle_max {
        if self
          .throttle_count
          .compare_exchange_weak(current, current + 1, AcqRel, Acquire)
          .is_ok()
        {
          return Ok(());
        }
        continue;
      }

      // 超过阈值：挂号监听并等待
      let listener = self.event.listen();
      let recheck = self.throttle_count.load(Acquire);
      if recheck < 0 {
        return Err(ThrottleClosed);
      }
      if recheck < self.throttle_max {
        // 快速重试
        continue;
      }
      listener.await;
    }
  }

  /// 完成发送：释放槽位并唤醒等待者
  pub fn exit_send(&self) {
    let prev = self.throttle_count.fetch_sub(1, AcqRel);
    if prev >= self.throttle_max {
      self.event.notify(1);
    }
  }

  /// 关闭节流器：置入关闭哨兵并全量唤醒，阻断后续发送
  pub fn close(&self) {
    self.throttle_count.store(i32::MIN / 2, Release);
    self.event.notify(usize::MAX);
  }

  /// 是否已关闭
  #[inline]
  pub fn is_closed(&self) -> bool {
    self.throttle_count.load(Acquire) < 0
  }

  /// 当前在途发送数
  #[inline]
  pub fn in_flight(&self) -> i32 {
    self.throttle_count.load(Acquire).max(0)
  }
}

#[cfg(test)]
mod tests {
  use std::sync::Arc;

  use compio::runtime::Runtime;

  use super::*;

  #[test]
  fn test_throttle_lifecycle() {
    let throttle = Arc::new(NetworkSenderThrottle::new(2));
    assert_eq!(throttle.in_flight(), 0);

    Runtime::new().unwrap().block_on(async {
      assert!(throttle.enter_send().await.is_ok());
      assert_eq!(throttle.in_flight(), 1);

      assert!(throttle.enter_send().await.is_ok());
      assert_eq!(throttle.in_flight(), 2);

      throttle.exit_send();
      assert_eq!(throttle.in_flight(), 1);

      throttle.close();
      assert!(throttle.is_closed());
      assert_eq!(throttle.enter_send().await, Err(ThrottleClosed));
    });
  }
}
