use std::time::Duration;

use parking_lot::{Condvar, Mutex};

/// garnet相对路径:garnet/libs/common/Synchronization/CountingEventSlim.cs:CountingEventSlim
pub struct CountingEventSlim {
  count: Mutex<i32>,
  cond: Condvar,
}

impl Default for CountingEventSlim {
  fn default() -> Self {
    Self::new()
  }
}

impl CountingEventSlim {
  /// garnet相对路径:garnet/libs/common/Synchronization/CountingEventSlim.cs:Create
  pub fn new() -> Self {
    Self {
      count: Mutex::new(0),
      cond: Condvar::new(),
    }
  }

  /// garnet相对路径:garnet/libs/common/Synchronization/CountingEventSlim.cs:Increment
  pub fn increment(&self) {
    let mut count = self.count.lock();
    *count += 1;
  }

  /// garnet相对路径:garnet/libs/common/Synchronization/CountingEventSlim.cs:Decrement
  pub fn decrement(&self) {
    let mut count = self.count.lock();
    *count -= 1;
    debug_assert!(
      *count >= 0,
      "Decrement fell below 0, implies unbalanced calls to Increment and Decrement"
    );
    if *count == 0 {
      self.cond.notify_all();
    }
  }

  /// garnet相对路径:garnet/libs/common/Synchronization/CountingEventSlim.cs:Wait
  pub fn wait(&self, milliseconds_timeout: i32) -> bool {
    let mut count = self.count.lock();
    while *count > 0 {
      if milliseconds_timeout < 0 {
        self.cond.wait(&mut count);
      } else if milliseconds_timeout == 0 {
        return false;
      } else {
        let res = self.cond.wait_for(
          &mut count,
          Duration::from_millis(milliseconds_timeout as u64),
        );
        if res.timed_out() {
          return *count == 0;
        }
      }
    }
    true
  }
}
