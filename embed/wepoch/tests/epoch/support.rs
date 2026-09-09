//! 测试公共辅助结构与断言函数（对标 Garnet test.epoch 辅助工具）

use std::{
  sync::{
    Arc,
    atomic::{AtomicBool, AtomicU64, Ordering},
  },
  thread::{JoinHandle, spawn, yield_now},
};

use wepoch::{LightEpoch, current_thread_id};

/// 后台常驻读事务线程（对标 Garnet ParkedReaderThread）
///
/// 进入受保护纪元区后保持等待，直到外部通知释放并退出挂起。
/// 该线程用于钉住某个纪元，阻止 safe_to_reclaim_epoch 前进以及延迟动作执行。
pub struct ParkedReaderThread {
  thread: Option<JoinHandle<()>>,
  release: Arc<AtomicBool>,
  pub announced_epoch: u64,
}

impl ParkedReaderThread {
  pub fn new(epoch: Arc<LightEpoch>) -> Self {
    let release = Arc::new(AtomicBool::new(false));
    let release_clone = Arc::clone(&release);
    let announced = Arc::new(AtomicU64::new(0));
    let announced_clone = Arc::clone(&announced);

    let thread = spawn(move || {
      epoch.resume();
      announced_clone.store(
        epoch.test_hook_this_thread_announced_epoch(),
        Ordering::Release,
      );

      while !release_clone.load(Ordering::Acquire) {
        yield_now();
      }
      epoch.suspend();
    });

    let announced_epoch = loop {
      let val = announced.load(Ordering::Acquire);
      if val != 0 {
        break val;
      }
      yield_now();
    };

    Self {
      thread: Some(thread),
      release,
      announced_epoch,
    }
  }

  pub fn leave_and_join(&mut self) {
    if let Some(t) = self.thread.take() {
      self.release.store(true, Ordering::Release);
      t.join().unwrap();
    }
  }
}

impl Drop for ParkedReaderThread {
  fn drop(&mut self) {
    self.leave_and_join();
  }
}

/// 等待全部线程执行完毕（对标 Garnet EpochTestBase JoinAll）
pub fn join_all<T>(handles: impl IntoIterator<Item = JoinHandle<T>>) {
  for h in handles {
    h.join().unwrap();
  }
}

/// 辅助断言函数：验证当前线程处于纪元保护区时的所有状态（对标 Garnet ProtectionTests AssertProtectedAt）
pub fn assert_protected_at(epoch: &LightEpoch, announced_epoch: u64, because: &str) {
  let entry = epoch.test_hook_this_thread_entry();

  assert!(epoch.this_instance_protected(), "{because}");
  assert!(entry > 0, "{because}");
  assert!(entry <= epoch.entry_count(), "{because}");
  assert_eq!(
    epoch.test_hook_thread_id_at(entry),
    current_thread_id(),
    "{because}"
  );
  assert_eq!(
    epoch.test_hook_this_thread_announced_epoch(),
    announced_epoch,
    "{because}"
  );
  assert_eq!(
    epoch.test_hook_announced_epoch_at(entry),
    announced_epoch,
    "{because}"
  );
}

/// 编译期静态断言：类型 T 未实现 Send
pub fn assert_not_send<T>() {
  trait AmbiguousIfSend<A> {
    fn check(&self) {}
  }
  impl<T: Send> AmbiguousIfSend<[(); 0]> for T {}
  impl<T> AmbiguousIfSend<[(); 1]> for T {}

  let placeholder: Option<T> = None;
  placeholder.check();
}

/// 编译期静态断言：类型 T 未实现 Sync
pub fn assert_not_sync<T>() {
  trait AmbiguousIfSync<A> {
    fn check(&self) {}
  }
  impl<T: Sync> AmbiguousIfSync<[(); 0]> for T {}
  impl<T> AmbiguousIfSync<[(); 1]> for T {}

  let placeholder: Option<T> = None;
  placeholder.check();
}
