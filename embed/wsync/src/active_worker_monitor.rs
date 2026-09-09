use std::{
  sync::atomic::{AtomicI32, Ordering},
  time::Duration,
};

use parking_lot::{Condvar, Mutex};

/// 在 garnet 中的相对路径:libs/common/Synchronization/ActiveWorkerMonitor.cs:ActiveWorkerMonitor
pub struct ActiveWorkerMonitor {
  disposed: AtomicI32,
  worker_count: AtomicI32,
  drain_mutex: Mutex<bool>,
  drain_cond: Condvar,
}

impl Default for ActiveWorkerMonitor {
  fn default() -> Self {
    Self::new()
  }
}

impl ActiveWorkerMonitor {
  pub fn new() -> Self {
    Self {
      disposed: AtomicI32::new(0),
      worker_count: AtomicI32::new(0),
      drain_mutex: Mutex::new(false),
      drain_cond: Condvar::new(),
    }
  }

  /// 在 garnet 中的相对路径:libs/common/Synchronization/ActiveWorkerMonitor.cs:CurrentCount
  pub fn current_count(&self) -> i32 {
    let observed_count = self.worker_count.load(Ordering::Relaxed);
    if observed_count < 0 {
      0
    } else {
      observed_count
    }
  }

  /// 在 garnet 中的相对路径:libs/common/Synchronization/ActiveWorkerMonitor.cs:Dispose
  pub fn dispose(&self) {
    if self.disposed.swap(1, Ordering::SeqCst) != 0 {
      return;
    }
    self.try_close(None);
  }

  /// 在 garnet 中的相对路径:libs/common/Synchronization/ActiveWorkerMonitor.cs:TryOpen
  pub fn try_open(&self) -> bool {
    debug_assert_eq!(self.worker_count.load(Ordering::Relaxed), i32::MIN);
    {
      let mut signalled = self.drain_mutex.lock();
      *signalled = false;
    }
    self
      .worker_count
      .compare_exchange(i32::MIN, 0, Ordering::SeqCst, Ordering::Relaxed)
      .is_ok()
  }

  /// 在 garnet 中的相对路径:libs/common/Synchronization/ActiveWorkerMonitor.cs:TryClose
  pub fn try_close(&self, timeout: Option<Duration>) {
    if self.worker_count.fetch_add(i32::MIN, Ordering::SeqCst) != 0 {
      let mut signalled = self.drain_mutex.lock();
      while !*signalled {
        if let Some(t) = timeout {
          let res = self.drain_cond.wait_for(&mut signalled, t);
          if res.timed_out() && !*signalled {
            break;
          }
        } else {
          self.drain_cond.wait(&mut signalled);
        }
      }
    }
  }

  /// 在 garnet 中的相对路径:libs/common/Synchronization/ActiveWorkerMonitor.cs:TryEnter
  #[inline]
  pub fn try_enter(&self) -> bool {
    self.try_enter_with_count(1).is_some()
  }

  /// 在 garnet 中的相对路径:libs/common/Synchronization/ActiveWorkerMonitor.cs:TryEnter
  #[inline]
  pub fn try_enter_with_count(&self, add: i32) -> Option<i32> {
    let cnt = self.worker_count.fetch_add(add, Ordering::SeqCst) + add;
    if cnt < 0 {
      if self.worker_count.fetch_sub(add, Ordering::SeqCst) - add == i32::MIN {
        let mut signalled = self.drain_mutex.lock();
        *signalled = true;
        self.drain_cond.notify_all();
      }
      return None;
    }
    Some(cnt)
  }

  /// 在 garnet 中的相对路径:libs/common/Synchronization/ActiveWorkerMonitor.cs:Exit
  #[inline]
  pub fn exit(&self) -> i32 {
    let cnt = self.worker_count.fetch_sub(1, Ordering::SeqCst) - 1;
    if cnt == i32::MIN {
      let mut signalled = self.drain_mutex.lock();
      *signalled = true;
      self.drain_cond.notify_all();
    }
    cnt
  }
}
