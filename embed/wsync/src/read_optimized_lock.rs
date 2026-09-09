use std::{
  sync::atomic::{AtomicI32, Ordering},
  thread,
};

/// garnet相对路径:garnet/libs/common/Synchronization/ReadOptimizedLock.cs:LockType
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LockType {
  Shared = 0,
  Exclusive = 1,
  AllExclusive = 2,
  Invalid = 3,
}

/// garnet相对路径:garnet/libs/common/Synchronization/ReadOptimizedLock.cs:LockToken
#[derive(Debug, Clone, Copy)]
pub struct LockToken {
  pub token: i32,
  pub typ: LockType,
}

impl LockToken {
  #[inline]
  pub fn create_shared(token: i32) -> Self {
    Self {
      token,
      typ: LockType::Shared,
    }
  }

  #[inline]
  pub fn create_exclusive(token: i32) -> Self {
    Self {
      token,
      typ: LockType::Exclusive,
    }
  }

  #[inline]
  pub fn create_all_exclusive() -> Self {
    Self {
      token: 0,
      typ: LockType::AllExclusive,
    }
  }

  #[inline]
  pub fn create_invalid() -> Self {
    Self {
      token: 0,
      typ: LockType::Invalid,
    }
  }
}

/// garnet相对路径:garnet/libs/common/Synchronization/ReadOptimizedLock.cs:ReadOptimizedLock
pub struct ReadOptimizedLock {
  hash_mask: usize,
  core_selection_mask: usize,
  lock_counts: Vec<AtomicI32>,
}

thread_local! {
    static PROCESSOR_HINT: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
}

#[inline]
fn get_processor_hint() -> usize {
  PROCESSOR_HINT.with(|hint| {
    let mut val = hint.get();
    if val == 0 {
      // we use the thread id as a simple hint, hash it a bit
      let tid = std::thread::current().id();
      // A simple hash of thread id string representation to get a number
      // since ThreadId doesn't expose integer natively in stable yet
      let tid_str = format!("{:?}", tid);
      let mut h: usize = 0;
      for b in tid_str.bytes() {
        h = h.wrapping_mul(31).wrapping_add(b as usize);
      }
      if h == 0 {
        h = 1;
      }
      hint.set(h);
      val = h;
    }
    val
  })
}

impl ReadOptimizedLock {
  /// garnet相对路径:garnet/libs/common/Synchronization/ReadOptimizedLock.cs:ReadOptimizedLock
  pub fn new(total_size: usize, core_count: usize) -> Self {
    let size = total_size.next_power_of_two();
    let cores = core_count.next_power_of_two();

    let hash_mask = (size / cores) - 1;
    let core_selection_mask = cores - 1;

    let mut lock_counts = Vec::with_capacity(size);
    for _ in 0..size {
      lock_counts.push(AtomicI32::new(0));
    }

    Self {
      hash_mask,
      core_selection_mask,
      lock_counts,
    }
  }

  #[inline]
  fn calculate_index(&self, hash: i64, core_selection: usize) -> usize {
    let folded_hash = (hash ^ (hash >> 32)) as usize;
    let mut acquire_ix = folded_hash & self.hash_mask;
    acquire_ix |= core_selection << self.hash_mask.count_ones();
    acquire_ix
  }

  /// garnet相对路径:garnet/libs/common/Synchronization/ReadOptimizedLock.cs:ReleaseLock
  pub fn release_lock(&self, lock_token: &LockToken) {
    match lock_token.typ {
      LockType::Shared => {
        let ix = lock_token.token as usize;
        debug_assert!(self.lock_counts[ix].load(Ordering::Relaxed) > 0);
        self.lock_counts[ix].fetch_sub(1, Ordering::AcqRel);
      }
      LockType::Exclusive => {
        let hash = lock_token.token as i64;
        let core_count = self.core_selection_mask + 1;
        for i in 0..core_count {
          let ix = self.calculate_index(hash, i);
          let res =
            self.lock_counts[ix].compare_exchange(i32::MIN, 0, Ordering::AcqRel, Ordering::Relaxed);
          debug_assert!(res.is_ok());
        }
      }
      LockType::AllExclusive => {
        for count in &self.lock_counts {
          let res = count.compare_exchange(i32::MIN, 0, Ordering::AcqRel, Ordering::Relaxed);
          debug_assert!(res.is_ok());
        }
      }
      LockType::Invalid => {}
    }
  }

  /// garnet相对路径:garnet/libs/common/Synchronization/ReadOptimizedLock.cs:TryAcquireSharedLock
  pub fn try_acquire_shared_lock(&self, hash: i64) -> Option<LockToken> {
    let core_selection = get_processor_hint() & self.core_selection_mask;
    let ix = self.calculate_index(hash, core_selection);

    let counter = &self.lock_counts[ix];
    if counter.load(Ordering::Relaxed) >= 0 {
      if counter.fetch_add(1, Ordering::AcqRel) >= 0 {
        return Some(LockToken::create_shared(ix as i32));
      }
      counter.fetch_sub(1, Ordering::AcqRel);
    }
    None
  }

  /// garnet相对路径:garnet/libs/common/Synchronization/ReadOptimizedLock.cs:AcquireSharedLock
  pub fn acquire_shared_lock(&self, hash: i64) -> LockToken {
    let core_selection = get_processor_hint() & self.core_selection_mask;
    let ix = self.calculate_index(hash, core_selection);

    let counter = &self.lock_counts[ix];
    loop {
      if counter.load(Ordering::Relaxed) >= 0 {
        if counter.fetch_add(1, Ordering::AcqRel) >= 0 {
          return LockToken::create_shared(ix as i32);
        }
        counter.fetch_sub(1, Ordering::AcqRel);
      }
      thread::yield_now();
    }
  }

  /// garnet相对路径:garnet/libs/common/Synchronization/ReadOptimizedLock.cs:TryAcquireExclusiveLock
  pub fn try_acquire_exclusive_lock(&self, hash: i64) -> Option<LockToken> {
    let core_count = self.core_selection_mask + 1;
    for i in 0..core_count {
      let acquire_ix = self.calculate_index(hash, i);
      if self.lock_counts[acquire_ix]
        .compare_exchange(0, i32::MIN, Ordering::AcqRel, Ordering::Relaxed)
        .is_err()
      {
        for j in 0..i {
          let release_ix = self.calculate_index(hash, j);
          while self.lock_counts[release_ix]
            .compare_exchange(i32::MIN, 0, Ordering::AcqRel, Ordering::Relaxed)
            .is_err()
          {
            thread::yield_now();
          }
        }
        return None;
      }
    }
    Some(LockToken::create_exclusive(hash as i32))
  }

  /// garnet相对路径:garnet/libs/common/Synchronization/ReadOptimizedLock.cs:AcquireExclusiveLock
  pub fn acquire_exclusive_lock(&self, hash: i64) -> LockToken {
    let core_count = self.core_selection_mask + 1;
    for i in 0..core_count {
      let acquire_ix = self.calculate_index(hash, i);
      while self.lock_counts[acquire_ix]
        .compare_exchange(0, i32::MIN, Ordering::AcqRel, Ordering::Relaxed)
        .is_err()
      {
        thread::yield_now();
      }
    }
    LockToken::create_exclusive(hash as i32)
  }

  /// garnet相对路径:garnet/libs/common/Synchronization/ReadOptimizedLock.cs:AcquireAllExclusiveLock
  pub fn acquire_all_exclusive_lock(&self) -> LockToken {
    for count in &self.lock_counts {
      while count
        .compare_exchange(0, i32::MIN, Ordering::AcqRel, Ordering::Relaxed)
        .is_err()
      {
        thread::yield_now();
      }
    }
    LockToken::create_all_exclusive()
  }

  /// garnet相对路径:garnet/libs/common/Synchronization/ReadOptimizedLock.cs:TryPromoteSharedLock
  pub fn try_promote_shared_lock(&self, hash: i64, lock_token: &mut LockToken) -> bool {
    debug_assert_eq!(lock_token.typ, LockType::Shared);

    let core_count = self.core_selection_mask + 1;
    for i in 0..core_count {
      let acquire_ix = self.calculate_index(hash, i);

      if acquire_ix == lock_token.token as usize {
        if self.lock_counts[acquire_ix]
          .compare_exchange(1, i32::MIN, Ordering::AcqRel, Ordering::Relaxed)
          .is_err()
        {
          for j in 0..i {
            let release_ix = self.calculate_index(hash, j);
            while self.lock_counts[release_ix]
              .compare_exchange(i32::MIN, 0, Ordering::AcqRel, Ordering::Relaxed)
              .is_err()
            {
              thread::yield_now();
            }
          }
          return false;
        }
      } else {
        if self.lock_counts[acquire_ix]
          .compare_exchange(0, i32::MIN, Ordering::AcqRel, Ordering::Relaxed)
          .is_err()
        {
          for j in 0..i {
            let release_ix = self.calculate_index(hash, j);
            let release_target_value = if release_ix == lock_token.token as usize {
              1
            } else {
              0
            };
            while self.lock_counts[release_ix]
              .compare_exchange(
                i32::MIN,
                release_target_value,
                Ordering::AcqRel,
                Ordering::Relaxed,
              )
              .is_err()
            {
              thread::yield_now();
            }
          }
          return false;
        }
      }
    }
    *lock_token = LockToken::create_exclusive(hash as i32);
    true
  }
}
