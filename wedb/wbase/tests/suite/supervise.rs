use std::{
  future::Future,
  sync::{
    Arc,
    atomic::{AtomicU64, Ordering::Relaxed},
  },
  task::{Context, Poll, Waker},
  thread::yield_now,
};

use wbase::supervise::{
  PanicPayload, counter_snapshots, register_counter, snapshots, supervise_item, supervise_task,
};

/// 纯 park 驱动器（监督未来体不挂起：单 poll 即终局，noop waker 足够）
fn drive<F: Future>(fut: F) -> F::Output {
  let mut fut = Box::pin(fut);
  let waker = Waker::noop();
  let mut cx = Context::from_waker(waker);
  loop {
    if let Poll::Ready(v) = fut.as_mut().poll(&mut cx) {
      return v;
    }
    yield_now();
  }
}

#[test]
fn supervise_task_ok_passes_through() {
  let out = drive(supervise_task("t_ok", async { 7 }));
  assert_eq!(out.unwrap(), 7);
  let e = snapshots().into_iter().find(|s| s.name == "t_ok").unwrap();
  assert!(!e.alive, "终局后存活位复位为假");
  assert_eq!(e.panics, 0);
}

#[test]
fn supervise_task_panic_counts_and_reports() {
  let out: Result<(), _> = drive(supervise_task("t_panic", async {
    panic!("boom");
  }));
  assert_eq!(out.unwrap_err().text(), "boom");
  let e = snapshots()
    .into_iter()
    .find(|s| s.name == "t_panic")
    .unwrap();
  assert!(!e.alive);
  assert_eq!(e.panics, 1, "panic 次数累计一次");
  // 同名归组：逐项监督复用同一计数，且不触碰任务存活位
  let item: Result<(), _> = drive(supervise_item("t_panic", async {}));
  assert!(item.is_ok());
  let e = snapshots()
    .into_iter()
    .find(|s| s.name == "t_panic")
    .unwrap();
  assert_eq!(e.panics, 1);
  assert!(!e.alive, "逐项监督不得触碰任务存活位");
}

#[test]
fn supervise_item_panics_do_not_touch_alive() {
  let out: Result<(), _> = drive(supervise_item("t_item", async {
    panic!("item boom");
  }));
  assert_eq!(out.unwrap_err().text(), "item boom");
  let e = snapshots()
    .into_iter()
    .find(|s| s.name == "t_item")
    .unwrap();
  assert_eq!(e.panics, 1);
  assert!(!e.alive, "逐项监督无任务级注册，存活位保持假");
}

#[test]
fn counter_register_idempotent_and_snapshot() {
  let c = Arc::new(AtomicU64::new(3));
  register_counter("c_x", Arc::clone(&c));
  register_counter("c_x", Arc::clone(&c));
  c.fetch_add(2, Relaxed);
  let e = counter_snapshots()
    .into_iter()
    .find(|s| s.name == "c_x")
    .unwrap();
  assert_eq!(e.value, 5);
}

#[test]
fn payload_text_covers_str_string_and_other() {
  assert_eq!(PanicPayload::new(Box::new("静态串")).text(), "静态串");
  assert_eq!(
    PanicPayload::new(Box::new("堆串".to_owned())).text(),
    "堆串"
  );
  assert_eq!(
    PanicPayload::new(Box::new(42_u32)).text(),
    "非文本 panic 载荷"
  );
}
