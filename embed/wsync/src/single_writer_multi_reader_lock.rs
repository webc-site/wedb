use std::{
  sync::atomic::{AtomicI32, Ordering},
  thread,
};

/// garnet相对路径:garnet/libs/common/Synchronization/SingleWriterMultiReaderLock.cs:SingleWriterMultiReaderLock
pub struct SingleWriterMultiReaderLock {
  lock: AtomicI32,
}

impl Default for SingleWriterMultiReaderLock {
  fn default() -> Self {
    Self::new()
  }
}

impl SingleWriterMultiReaderLock {
  /// garnet相对路径:garnet/libs/common/Synchronization/SingleWriterMultiReaderLock.cs:SingleWriterMultiReaderLock()
  pub fn new() -> Self {
    Self {
      lock: AtomicI32::new(0),
    }
  }

  /// garnet相对路径:garnet/libs/common/Synchronization/SingleWriterMultiReaderLock.cs:IsWriteLocked
  #[inline]
  pub fn is_write_locked(&self) -> bool {
    self.lock.load(Ordering::Acquire) < 0
  }

  /// garnet相对路径:garnet/libs/common/Synchronization/SingleWriterMultiReaderLock.cs:TryWriteLock
  #[inline]
  pub fn try_write_lock(&self) -> bool {
    self
      .lock
      .compare_exchange(0, i32::MIN, Ordering::AcqRel, Ordering::Relaxed)
      .is_ok()
  }

  /// garnet相对路径:garnet/libs/common/Synchronization/SingleWriterMultiReaderLock.cs:WriteLock
  #[inline]
  pub fn write_lock(&self) {
    while !self.try_write_lock() {
      thread::yield_now();
    }
  }

  /// garnet相对路径:garnet/libs/common/Synchronization/SingleWriterMultiReaderLock.cs:WriteUnlock
  #[inline]
  pub fn write_unlock(&self) {
    debug_assert!(self.is_write_locked());
    while self
      .lock
      .compare_exchange(i32::MIN, 0, Ordering::AcqRel, Ordering::Relaxed)
      .is_err()
    {
      thread::yield_now();
    }
  }

  /// garnet相对路径:garnet/libs/common/Synchronization/SingleWriterMultiReaderLock.cs:TryReadLock
  #[inline]
  pub fn try_read_lock(&self) -> bool {
    if self.lock.load(Ordering::Relaxed) >= 0 {
      if self.lock.fetch_add(1, Ordering::AcqRel) >= 0 {
        return true;
      }
      self.lock.fetch_sub(1, Ordering::AcqRel);
    }
    false
  }

  /// garnet相对路径:garnet/libs/common/Synchronization/SingleWriterMultiReaderLock.cs:ReadLock
  #[inline]
  pub fn read_lock(&self) {
    while !self.try_read_lock() {
      thread::yield_now();
    }
  }

  /// garnet相对路径:garnet/libs/common/Synchronization/SingleWriterMultiReaderLock.cs:ReadUnlock
  #[inline]
  pub fn read_unlock(&self) {
    debug_assert!(self.lock.load(Ordering::Relaxed) > 0);
    self.lock.fetch_sub(1, Ordering::AcqRel);
  }

  /// garnet相对路径:garnet/libs/common/Synchronization/SingleWriterMultiReaderLock.cs:TryUpgradeReadLock
  #[inline]
  pub fn try_upgrade_read_lock(&self) -> bool {
    debug_assert!(self.lock.load(Ordering::Relaxed) > 0);
    self
      .lock
      .compare_exchange(1, i32::MIN, Ordering::AcqRel, Ordering::Relaxed)
      .is_ok()
  }

  /// garnet相对路径:garnet/libs/common/Synchronization/SingleWriterMultiReaderLock.cs:DowngradeWriteLock
  #[inline]
  pub fn downgrade_write_lock(&self) {
    debug_assert!(self.lock.load(Ordering::Relaxed) < 0);
    while self
      .lock
      .compare_exchange(i32::MIN, 1, Ordering::AcqRel, Ordering::Relaxed)
      .is_err()
    {
      thread::yield_now();
    }
  }

  /// garnet相对路径:garnet/libs/common/Synchronization/SingleWriterMultiReaderLock.cs:TryCloseLock
  #[inline]
  pub fn try_close_lock(&self) -> bool {
    loop {
      let is_write_locked = self.is_write_locked();
      let acquired_write_lock = self.try_write_lock();
      if is_write_locked || acquired_write_lock {
        return acquired_write_lock;
      }
      thread::yield_now();
    }
  }
}
