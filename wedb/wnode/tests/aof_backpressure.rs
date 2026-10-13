#![recursion_limit = "512"] // 泛型设备实例化下 async 状态机嵌套深（R27 CI 实证）
//! 主侧复制背压闸门集成测试与并发状态机测试
//! （对应 libs/server/AOF/AofBackpressure.cs:AofBackpressure）
//!
//! 尾地址推进全部走真实日志替身（段设备 WaofSublog + GarnetLog，经生产
//! 唯一装配口 `set_weak_log` 绑定弱引用）；行为断言面为 `any_stalled` /
//! `is_released` / `wait` 阻塞-唤醒，不再依赖计数器替身与水位直读口。

use std::{
  future::Future,
  sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
  },
  thread,
  time::Duration,
};

use compio::runtime::Runtime;
use wconf::RuntimeServerOptions;
use wnode::aof::{
  aof_backpressure::AofBackpressure, garnet_log::GarnetLog, waof_sublog::AofSublog,
};
use wnode_test::test_sublogs;

const MAX_ATTEMPTS: usize = 10;
const RETRY_INTERVAL: Duration = Duration::from_millis(50);

/// 弹性 block_on：在 Linux 环境下 io_uring memlock 瞬态受限时微睡重试，彻底杜绝 ENOMEM
fn block_on<F: Future>(fut: F) -> F::Output {
  let mut attempts = 0usize;
  let rt = loop {
    match Runtime::new() {
      Ok(r) => break r,
      Err(_e) if attempts < MAX_ATTEMPTS => {
        attempts += 1;
        thread::sleep(RETRY_INTERVAL);
      }
      Err(e) => panic!("Runtime::new 耗尽重试仍然失败: {e}"),
    }
  };
  rt.block_on(fut)
}

/// 真实日志替身：段设备子日志装配分片 GarnetLog（对标 aof_sharded_commit.rs
/// make_sharded_log 形态），返回临时目录守卫与日志句柄
fn make_log(sublogs: usize) -> (Vec<tempfile::TempDir>, Arc<GarnetLog>) {
  let (dirs, backends): (Vec<_>, Vec<Arc<AofSublog>>) = test_sublogs("bp_gate", sublogs);
  let options = RuntimeServerOptions {
    aof_physical_sublog_count: sublogs as i32,
    aof_replay_task_count: 1,
    ..RuntimeServerOptions::default()
  };
  let log = Arc::new(GarnetLog::new(&options, backends, None).expect("构造 GarnetLog"));
  (dirs, log)
}

/// enqueue 推进子日志尾越过目标档位（记录头开销自适应，不预设帧格式；
/// 返回推进后的实时尾地址）
fn advance_tail(log: &GarnetLog, sublog: usize, target: i64) -> i64 {
  let sub = log.get_sub_log(sublog);
  while sub.tail_address() <= target {
    sub.enqueue(b"x").expect("记录入环形缓冲");
  }
  sub.tail_address()
}

#[test]
fn budget_enables_and_disables_gate() {
  let gate = AofBackpressure::new(2, 1024);
  assert!(gate.enabled());
  assert_eq!(gate.publish_delta_bytes(), 1024 / 2 / 8);
  assert_eq!(gate.per_sublog_budget(), 512);

  // 预算 <= 0 禁用。
  let off = AofBackpressure::new(2, -1);
  assert!(!off.enabled());
  assert!(!off.any_stalled());

  let off_zero = AofBackpressure::new(2, 0);
  assert!(!off_zero.enabled());
  assert!(!off_zero.any_stalled());
}

#[test]
fn wait_passes_within_budget_and_stalls_beyond() {
  let (_dirs, log) = make_log(1);
  let gate = AofBackpressure::new(1, 100);
  assert_eq!(gate.publish_delta_bytes(), 100 / 8);
  gate.set_weak_log(Arc::downgrade(&log));

  // 水位 i64::MAX（无复制端）直接放行。
  gate.wait(0, 10_000);
  assert!(!gate.any_stalled());

  // 复制端附着：水位 0，空日志尾 0 在预算 100 内。
  gate.publish_shipped_address(0, 0);
  assert!(!gate.any_stalled());

  // 尾越过预算 100：出现滞后。
  let tail = advance_tail(&log, 0, 100);
  assert!(gate.any_stalled());

  // 水位推进到预算内即解除。
  gate.publish_shipped_address(0, tail - 50);
  assert!(!gate.any_stalled());
}

#[test]
fn dispose_releases_all() {
  let gate = AofBackpressure::new(1, 100);
  gate.publish_shipped_address(0, 0);
  // 尾地址超预算且无复制端推进：处于滞留态。
  assert!(!gate.is_released(0, 1_000_000));

  gate.dispose();
  // 关停置位后立即放行，同步慢路径不阻塞。
  assert!(gate.is_released(0, 1_000_000));
  gate.wait_slow(0, 1_000_000);
}

#[test]
fn sync_cross_thread_event_wake() {
  let (_dirs, log) = make_log(1);
  let gate = Arc::new(AofBackpressure::new(1, 100));
  gate.set_weak_log(Arc::downgrade(&log));
  gate.publish_shipped_address(0, 0);

  // 尾越过预算 100：wait 进入同步慢路径；发布方推水位至预算内解除
  let tail = advance_tail(&log, 0, 100);
  let gate_clone = Arc::clone(&gate);
  let thread_handle = thread::spawn(move || {
    while gate_clone.total_listeners() == 0 {
      thread::yield_now();
    }
    gate_clone.publish_shipped_address(0, tail - 50);
  });

  gate.wait(0, tail);
  thread_handle.join().unwrap();
}

#[test]
fn async_wait_event_driven_wake() {
  let (_dirs, log) = make_log(1);
  let gate = Arc::new(AofBackpressure::new(1, 100));
  gate.set_weak_log(Arc::downgrade(&log));
  gate.publish_shipped_address(0, 0);

  let tail = advance_tail(&log, 0, 100);
  let gate_clone = Arc::clone(&gate);
  let thread_handle = thread::spawn(move || {
    while gate_clone.total_listeners() == 0 {
      thread::yield_now();
    }
    gate_clone.publish_shipped_address(0, tail - 50);
  });

  block_on(async {
    gate.wait_async(0, tail).await;
  });

  thread_handle.join().unwrap();
}

#[test]
fn async_wait_dispose_wake() {
  let (_dirs, log) = make_log(1);
  let gate = Arc::new(AofBackpressure::new(1, 100));
  gate.set_weak_log(Arc::downgrade(&log));
  gate.publish_shipped_address(0, 0);

  let tail = advance_tail(&log, 0, 100);
  let done = Arc::new(AtomicBool::new(false));
  let done_clone = Arc::clone(&done);
  let gate_clone = Arc::clone(&gate);

  let thread_handle = thread::spawn(move || {
    while gate_clone.total_listeners() == 0 {
      thread::yield_now();
    }
    gate_clone.dispose();
    done_clone.store(true, Ordering::Release);
  });

  block_on(async {
    gate.wait_async(0, tail).await;
  });

  assert!(done.load(Ordering::Acquire));
  thread_handle.join().unwrap();
}

#[test]
fn multi_sublog_async_wait() {
  let (_dirs, log) = make_log(2);
  let gate = Arc::new(AofBackpressure::new(2, 200));
  gate.set_weak_log(Arc::downgrade(&log));
  gate.publish_shipped_address(0, 0);
  gate.publish_shipped_address(1, 0);

  // sublog 1 尾越过每子日志预算 100：异步等待挂起；水位推进到预算内
  //（尾差 50 <= 100）唤醒
  let tail1 = advance_tail(&log, 1, 100);
  let gate_clone = Arc::clone(&gate);
  let thread_handle = thread::spawn(move || {
    while gate_clone.total_listeners() == 0 {
      thread::yield_now();
    }
    gate_clone.publish_shipped_address(1, tail1 - 50);
  });

  block_on(async {
    gate.wait_async(1, tail1).await;
  });

  thread_handle.join().unwrap();
}

#[test]
fn dynamic_budget_expansion_unblocks() {
  let (_dirs, log) = make_log(1);
  let gate = Arc::new(AofBackpressure::new(1, 100));
  gate.set_weak_log(Arc::downgrade(&log));
  gate.publish_shipped_address(0, 0);

  let tail = advance_tail(&log, 0, 100);
  let gate_clone = Arc::clone(&gate);
  let thread_handle = thread::spawn(move || {
    while gate_clone.total_listeners() == 0 {
      thread::yield_now();
    }
    // 动态扩充预算到 500，立即解除背压
    gate_clone.set_budget(500);
  });

  gate.wait(0, tail);
  thread_handle.join().unwrap();
  assert!(!gate.any_stalled());
}
