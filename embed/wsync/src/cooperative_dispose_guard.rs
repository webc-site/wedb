use std::sync::atomic::{AtomicI32, Ordering};

const FLAG_ACTIVE: i32 = 1 << 0;
const FLAG_DISPOSED: i32 = 1 << 1;

/// 在 garnet 中的相对路径:libs/common/Synchronization/CooperativeDisposeGuard.cs:DisposeResult
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DisposeResult {
  AlreadyDisposed,
  DeferredToWorker,
  CleanupNow,
}

/// 在 garnet 中的相对路径:libs/common/Synchronization/CooperativeDisposeGuard.cs:CooperativeDisposeGuard
pub struct CooperativeDisposeGuard {
  state: AtomicI32,
}

impl Default for CooperativeDisposeGuard {
  fn default() -> Self {
    Self::new()
  }
}

impl CooperativeDisposeGuard {
  pub fn new() -> Self {
    Self {
      state: AtomicI32::new(0),
    }
  }

  /// 在 garnet 中的相对路径:libs/common/Synchronization/CooperativeDisposeGuard.cs:IsDisposed
  #[inline]
  pub fn is_disposed(&self) -> bool {
    (self.state.load(Ordering::Acquire) & FLAG_DISPOSED) != 0
  }

  /// 在 garnet 中的相对路径:libs/common/Synchronization/CooperativeDisposeGuard.cs:TryEnter
  #[inline]
  pub fn try_enter(&self) -> bool {
    let prev = self.state.fetch_or(FLAG_ACTIVE, Ordering::SeqCst);
    (prev & FLAG_DISPOSED) == 0
  }

  /// 在 garnet 中的相对路径:libs/common/Synchronization/CooperativeDisposeGuard.cs:ExitAndCheckShouldCleanup
  #[inline]
  pub fn exit_and_check_should_cleanup(&self) -> bool {
    let prev = self.state.fetch_and(!FLAG_ACTIVE, Ordering::SeqCst);
    (prev & FLAG_DISPOSED) != 0
  }

  /// 在 garnet 中的相对路径:libs/common/Synchronization/CooperativeDisposeGuard.cs:TryDispose
  #[inline]
  pub fn try_dispose(&self) -> DisposeResult {
    let prev = self.state.fetch_or(FLAG_DISPOSED, Ordering::SeqCst);
    if (prev & FLAG_DISPOSED) != 0 {
      DisposeResult::AlreadyDisposed
    } else if (prev & FLAG_ACTIVE) != 0 {
      DisposeResult::DeferredToWorker
    } else {
      DisposeResult::CleanupNow
    }
  }
}
