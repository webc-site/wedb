use std::sync::atomic::{AtomicI32, Ordering};

use crate::{
  error::{Error, Result},
  semaphore::Semaphore,
};

/// 在 garnet 中的相对路径:libs/common/Synchronization/DoubleTurnstileBarrier.cs:DoubleTurnstileBarrier
pub struct DoubleTurnstileBarrier {
  participant_count: i32,
  worker_count: AtomicI32,
  work_ready: Semaphore,
  work_complete: Semaphore,
}

impl DoubleTurnstileBarrier {
  /// 在 garnet 中的相对路径:libs/common/Synchronization/DoubleTurnstileBarrier.cs:DoubleTurnstileBarrier
  ///
  /// 刻意差异（对照 C#）：C# 以 `ArgumentOutOfRangeException` 校验参数，此处以
  /// 类型化 [`Error::InvalidParticipantCount`] 上抛，杜绝 panic。
  pub fn new(participant_count: i32) -> Result<Self> {
    if participant_count < 1 {
      return Err(Error::InvalidParticipantCount(participant_count));
    }
    Ok(Self {
      participant_count,
      worker_count: AtomicI32::new(0),
      work_ready: Semaphore::new(0),
      work_complete: Semaphore::new(0),
    })
  }

  fn signal_work_ready_internal(&self) -> bool {
    let new_value = self.worker_count.fetch_add(1, Ordering::AcqRel) + 1;
    if new_value == self.participant_count {
      if self.participant_count > 1 {
        self
          .work_ready
          .release((self.participant_count - 1) as usize);
      }
      return false;
    }
    true
  }

  /// 在 garnet 中的相对路径:libs/common/Synchronization/DoubleTurnstileBarrier.cs:SignalWorkReadyWait
  ///
  /// 刻意差异（对照 C#）：C# 支持 timeout/cancellationToken 超时失败，Rust 侧
  /// [`Semaphore`] 暂无超时能力，等待恒为无限期汇合，故不含超时参数。
  pub fn signal_work_ready_wait(&self) {
    if self.signal_work_ready_internal() {
      self.work_ready.wait();
    }
  }

  /// 在 garnet 中的相对路径:libs/common/Synchronization/DoubleTurnstileBarrier.cs:SignalWorkReadyWaitAsync
  pub async fn signal_work_ready_wait_async(&self) {
    if self.signal_work_ready_internal() {
      self.work_ready.wait_async().await;
    }
  }

  fn signal_work_completed_internal(&self) -> bool {
    let new_value = self.worker_count.fetch_sub(1, Ordering::AcqRel) - 1;
    if new_value == 0 {
      if self.participant_count > 1 {
        self
          .work_complete
          .release((self.participant_count - 1) as usize);
      }
      return false;
    }
    true
  }

  /// 在 garnet 中的相对路径:libs/common/Synchronization/DoubleTurnstileBarrier.cs:SignalWorkCompletedWait
  pub fn signal_work_completed_wait(&self) {
    if self.signal_work_completed_internal() {
      self.work_complete.wait();
    }
  }

  /// 在 garnet 中的相对路径:libs/common/Synchronization/DoubleTurnstileBarrier.cs:SignalWorkCompletedWaitAsync
  pub async fn signal_work_completed_wait_async(&self) {
    if self.signal_work_completed_internal() {
      self.work_complete.wait_async().await;
    }
  }
}
