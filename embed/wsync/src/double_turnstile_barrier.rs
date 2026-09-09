use std::{
  sync::atomic::{AtomicI32, Ordering},
  time::Duration,
};

use crate::semaphore::Semaphore;

/// garnet相对路径:garnet/libs/common/Synchronization/DoubleTurnstileBarrier.cs:DoubleTurnstileBarrier
pub struct DoubleTurnstileBarrier {
  participant_count: i32,
  worker_count: AtomicI32,
  work_ready: Semaphore,
  work_complete: Semaphore,
}

impl DoubleTurnstileBarrier {
  /// garnet相对路径:garnet/libs/common/Synchronization/DoubleTurnstileBarrier.cs:DoubleTurnstileBarrier
  pub fn new(participant_count: i32) -> Self {
    assert!(participant_count >= 1);
    Self {
      participant_count,
      worker_count: AtomicI32::new(0),
      work_ready: Semaphore::new(0),
      work_complete: Semaphore::new(0),
    }
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

  /// garnet相对路径:garnet/libs/common/Synchronization/DoubleTurnstileBarrier.cs:SignalWorkReadyWait
  pub fn signal_work_ready_wait(&self, _timeout: Option<Duration>) -> Result<(), String> {
    if self.signal_work_ready_internal() {
      // Note: Currently ignores timeout. Add timeout logic to semaphore if needed.
      self.work_ready.wait();
    }
    Ok(())
  }

  /// garnet相对路径:garnet/libs/common/Synchronization/DoubleTurnstileBarrier.cs:SignalWorkReadyWaitAsync
  pub async fn signal_work_ready_wait_async(
    &self,
    _timeout: Option<Duration>,
  ) -> Result<(), String> {
    if self.signal_work_ready_internal() {
      self.work_ready.wait_async().await;
    }
    Ok(())
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

  /// garnet相对路径:garnet/libs/common/Synchronization/DoubleTurnstileBarrier.cs:SignalWorkCompletedWait
  pub fn signal_work_completed_wait(&self, _timeout: Option<Duration>) -> Result<(), String> {
    if self.signal_work_completed_internal() {
      self.work_complete.wait();
    }
    Ok(())
  }

  /// garnet相对路径:garnet/libs/common/Synchronization/DoubleTurnstileBarrier.cs:SignalWorkCompletedWaitAsync
  pub async fn signal_work_completed_wait_async(
    &self,
    _timeout: Option<Duration>,
  ) -> Result<(), String> {
    if self.signal_work_completed_internal() {
      self.work_complete.wait_async().await;
    }
    Ok(())
  }
}
