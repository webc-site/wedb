use core::future::Future;
use std::{
  sync::Arc,
  time::{Duration, Instant},
};

use wbase::{backoff::Backoff, thread::current_thread_id};

use crate::LightEpoch;

macro_rules! define_epoch_wait {
  (
    $name:ident
    $(, async: $async_kw:ident)?
    , wait_step: |$backoff:ident $(, $sleeper:ident)?| $wait_body:expr
    $(, sleeper_type: $S:ident, $SOut:ident)?
  ) => {
    /// 统一等待原语（支持取消安全、自适应退避与调用方保护区临时让渡）
    pub $($async_kw)? fn $name<F, O $(, $S, $SOut)?>(
      epoch: Option<&Arc<LightEpoch>>,
      allow_protected: bool,
      mut condition: F,
      mut on_step: O,
      timeout: Option<Duration>
      $(, mut $sleeper: $S)?
    ) -> bool
    where
      F: FnMut() -> bool,
      O: FnMut(&mut Backoff),
      $(
        $S: FnMut(Duration) -> $SOut,
        $SOut: Future<Output = ()>,
      )?
    {
      if condition() {
        return true;
      }

      let mut exited = 0usize;
      if let Some(ep) = epoch {
        if allow_protected {
          while ep.this_instance_protected() {
            ep.suspend();
            exited += 1;
          }
          let tid = current_thread_id();
          let current = ep.current_epoch();
          ep.refresh_thread_protected_entries(tid, current);
        } else {
          debug_assert!(
            !ep.this_instance_protected(),
            "wait 活锁防御: 调用方以受保护态进入不容忍重入的等待档位"
          );
        }
      } else {
        debug_assert!(!allow_protected, "无 LightEpoch 关联时不能指定 allow_protected");
      }

      struct ResumeGuard<'a> {
        epoch: Option<&'a Arc<LightEpoch>>,
        count: usize,
      }
      impl Drop for ResumeGuard<'_> {
        fn drop(&mut self) {
          if let Some(ep) = self.epoch {
            for _ in 0..self.count {
              ep.resume();
            }
          }
        }
      }
      let _guard = ResumeGuard {
        epoch,
        count: exited,
      };

      let start = timeout.map(|_| Instant::now());
      let mut $backoff = Backoff::new();

      while !condition() {
        if let (Some(limit), Some(st)) = (timeout, start) {
          if st.elapsed() >= limit {
            return false;
          }
        }
        on_step(&mut $backoff);
        $wait_body;
        $backoff.advance();
      }
      true
    }
  };
}

define_epoch_wait!(
  wait_condition_sync,
  wait_step: |backoff| backoff.stage().wait()
);

define_epoch_wait!(
  wait_condition_async,
  async: async,
  wait_step: |backoff, sleeper| backoff.stage().wait_async(&mut sleeper).await,
  sleeper_type: S, SOut
);
