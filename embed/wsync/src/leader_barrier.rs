use std::time::Duration;

use parking_lot::{Condvar, Mutex};

struct State {
  arrived_count: i32,
  first_released: bool,
  all_released: bool,
}

/// garnet相对路径:garnet/libs/common/Synchronization/LeaderBarrier.cs:LeaderBarrier
pub struct LeaderBarrier {
  participant_count: i32,
  state: Mutex<State>,
  cond_first: Condvar,
  cond_all: Condvar,
}

impl LeaderBarrier {
  /// garnet相对路径:garnet/libs/common/Synchronization/LeaderBarrier.cs:LeaderBarrier
  pub fn new(participant_count: i32) -> Self {
    Self {
      participant_count,
      state: Mutex::new(State {
        arrived_count: participant_count,
        first_released: false,
        all_released: false,
      }),
      cond_first: Condvar::new(),
      cond_all: Condvar::new(),
    }
  }

  /// garnet相对路径:garnet/libs/common/Synchronization/LeaderBarrier.cs:TrySignalOrWait
  pub fn try_signal_or_wait(&self, timeout: Option<Duration>) -> Result<bool, String> {
    let mut state = self.state.lock();
    let new_value = state.arrived_count - 1;
    state.arrived_count = new_value;

    if new_value < 0 {
      return Err("Invalid count value < 0".to_string());
    }

    if new_value == self.participant_count - 1 {
      // First participant
      if new_value > 0 {
        while !state.first_released {
          if let Some(t) = timeout {
            let res = self.cond_first.wait_for(&mut state, t);
            if res.timed_out() && !state.first_released {
              return Err("Timeout".to_string());
            }
          } else {
            self.cond_first.wait(&mut state);
          }
        }
      }
      return Ok(true);
    }

    if new_value == 0 {
      // Last participant
      state.first_released = true;
      self.cond_first.notify_all();
    }

    // All non-first wait for release
    while !state.all_released {
      if let Some(t) = timeout {
        let res = self.cond_all.wait_for(&mut state, t);
        if res.timed_out() && !state.all_released {
          return Err("Timeout".to_string());
        }
      } else {
        self.cond_all.wait(&mut state);
      }
    }

    Ok(false)
  }

  /// garnet相对路径:garnet/libs/common/Synchronization/LeaderBarrier.cs:Release
  pub fn release(&self) {
    let mut state = self.state.lock();
    state.all_released = true;
    self.cond_all.notify_all();
  }
}
