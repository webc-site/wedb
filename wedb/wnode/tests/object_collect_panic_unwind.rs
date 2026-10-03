//! 对象收集单写位 panic unwind 释放与再入测试
//!
//! 验证契约（对标 C# ObjectCollect collectLock.TryWriteLock 成功后的 try/finally WriteUnlock）：
//! 1. 单写位 RAII 守卫 [`CollectLockGuard`] 在栈展开（panic unwind）时，Drop 臂
//!    必然执行 `store(false, Ordering::Release)`，单写位复位为 false；
//! 2. 手动侧（`HCOLLECT *` / `ZCOLLECT *`）：慢路径收集体注入 panic 后，单写位自动释放，
//!    下轮可正常再入（不会永久被拒 `already-in-progress`）；
//! 3. 周期侧（`collect_family` / `PrimaryTasks`）：周期收集轮次注入 panic 后，单写位
//!    自动释放，下轮可正常再入；任务经 supervise_resumable 重新拉起后，收集面持续可用。

// 全文件围绕 debug-only 注入钩构造，release 整文件剔除
#![cfg(debug_assertions)]

use std::{
  panic::{AssertUnwindSafe, catch_unwind},
  sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
  },
  time::Duration,
};

use compio::{runtime::Runtime, time::sleep};
use parking_lot::Mutex;
use tempfile::tempdir;
use wconf::{RuntimeServerConfig, RuntimeServerOptions};
use wdev::SegmentedDevice;
use wkv::{StoreConfig, WedbStore};
use wnode::{
  PrimaryTasks,
  primary_tasks::collect_family,
  resp::garnet_api::{
    CollectLockGuard, GarnetApiFace, OBJECT_COLLECT_PANIC_INJECT, StoreGarnetApi,
  },
};
use wresp::command::RespCommand;

/// 全局测试串行门：`OBJECT_COLLECT_PANIC_INJECT` 为进程级静态注入桩，
/// 多用例必须串行执行，避免并行抢跑污染
static TEST_SERIAL: Mutex<()> = Mutex::new(());

/// 创建测试用轻量级存储
fn open_test_store(path: &str) -> Arc<WedbStore<SegmentedDevice>> {
  let dir = tempdir().unwrap().keep();
  let config = StoreConfig::new(1024, 64 * 1024, 16, 0.5).unwrap();
  let device = Arc::new(SegmentedDevice::single_file(dir.join(path)).unwrap());
  Arc::new(WedbStore::open(config, device).unwrap())
}

#[test]
fn collect_lock_guard_unwind_resets_flag_and_allows_reentry() {
  let _guard = TEST_SERIAL.lock();
  let flag = AtomicBool::new(false);

  // 1. 成功抢占单写位，持有期间互斥排他
  {
    let guard = CollectLockGuard::try_acquire(&flag);
    assert!(guard.is_some(), "初次抢占必须成功");
    assert!(flag.load(Ordering::SeqCst), "抢占后标志位必须为 true");

    let second = CollectLockGuard::try_acquire(&flag);
    assert!(second.is_none(), "在途抢占必须被拒绝（排他互斥）");
  }
  // 正常离开作用域：Drop 自动释放
  assert!(!flag.load(Ordering::SeqCst), "正常 Drop 后标志位必须复位");

  // 2. panic unwind 异常展开：Drop 臂兜底复位
  let panic_result = catch_unwind(AssertUnwindSafe(|| {
    let _g = CollectLockGuard::try_acquire(&flag).expect("抢占必须成功");
    assert!(flag.load(Ordering::SeqCst), "作用域内标志位为 true");
    panic!("测试定向 panic 展开");
  }));
  assert!(panic_result.is_err(), "闭包必须抛出 panic");
  assert!(
    !flag.load(Ordering::SeqCst),
    "panic unwind 后 CollectLockGuard 的 Drop 臂必须将标志位复位为 false"
  );

  // 3. 下轮可再入
  let reenter = CollectLockGuard::try_acquire(&flag);
  assert!(reenter.is_some(), "标志位复位后下轮必须可成功再入抢占");
  assert!(flag.load(Ordering::SeqCst));
}

#[test]
fn manual_hcollect_panic_unwind_resets_bit_and_reenters() {
  let _lock = TEST_SERIAL.lock();
  let store = open_test_store("manual_hcollect_panic.db");
  let api = Arc::new(StoreGarnetApi::new(store.new_session().unwrap()));
  let rt = Runtime::new().unwrap();

  // 初始状态：未在途
  assert!(!api.hcollect_in_progress.load(Ordering::SeqCst));

  // 注入 panic
  OBJECT_COLLECT_PANIC_INJECT.store(true, Ordering::SeqCst);

  let panic_res = catch_unwind(AssertUnwindSafe(|| {
    rt.block_on(Arc::clone(&api).exec_slow(
      RespCommand::Hcollect,
      vec![b"*".to_vec()],
      wconf::DEFAULT_RESP_VERSION,
    ))
  }));
  assert!(panic_res.is_err(), "收集体注入 panic 必须触发异常展开");

  // 断言：守卫 Drop 后单写位复位为 false（非永久封死）
  assert!(
    !api.hcollect_in_progress.load(Ordering::SeqCst),
    "HCOLLECT panic unwind 后单写位必须自动复位为 false"
  );

  // 下轮可再入：不带注入正常执行全库收集，返回 +OK
  let out = rt.block_on(Arc::clone(&api).exec_slow(
    RespCommand::Hcollect,
    vec![b"*".to_vec()],
    wconf::DEFAULT_RESP_VERSION,
  ));
  assert_eq!(out, b"+OK\r\n", "下轮 HCOLLECT * 必须成功再入并返回 +OK");
  assert!(
    !api.hcollect_in_progress.load(Ordering::SeqCst),
    "收集完成后单写位必须保持释放状态"
  );
}

#[test]
fn manual_zcollect_panic_unwind_resets_bit_and_reenters() {
  let _lock = TEST_SERIAL.lock();
  let store = open_test_store("manual_zcollect_panic.db");
  let api = Arc::new(StoreGarnetApi::new(store.new_session().unwrap()));
  let rt = Runtime::new().unwrap();

  // 初始状态：未在途
  assert!(!api.zcollect_in_progress.load(Ordering::SeqCst));

  // 注入 panic
  OBJECT_COLLECT_PANIC_INJECT.store(true, Ordering::SeqCst);

  let panic_res = catch_unwind(AssertUnwindSafe(|| {
    rt.block_on(Arc::clone(&api).exec_slow(
      RespCommand::Zcollect,
      vec![b"*".to_vec()],
      wconf::DEFAULT_RESP_VERSION,
    ))
  }));
  assert!(panic_res.is_err(), "收集体注入 panic 必须触发异常展开");

  // 断言：守卫 Drop 后单写位复位为 false
  assert!(
    !api.zcollect_in_progress.load(Ordering::SeqCst),
    "ZCOLLECT panic unwind 后单写位必须自动复位为 false"
  );

  // 下轮可再入：正常执行全库收集，返回 +OK
  let out = rt.block_on(Arc::clone(&api).exec_slow(
    RespCommand::Zcollect,
    vec![b"*".to_vec()],
    wconf::DEFAULT_RESP_VERSION,
  ));
  assert_eq!(out, b"+OK\r\n", "下轮 ZCOLLECT * 必须成功再入并返回 +OK");
  assert!(
    !api.zcollect_in_progress.load(Ordering::SeqCst),
    "收集完成后单写位必须保持释放状态"
  );
}

#[test]
fn periodic_collect_family_panic_unwind_resets_bit_and_reenters() {
  let _lock = TEST_SERIAL.lock();
  let store = open_test_store("periodic_panic.db");
  // 激活会话与域 (0, 0)
  let session = store.new_session().unwrap();
  session.set_context(0, 0);

  let tasks = PrimaryTasks::default();
  let rt = Runtime::new().unwrap();

  // ---- Hash 族周期收集 panic unwind ----
  assert!(!tasks.hcollect_in_progress.load(Ordering::SeqCst));
  OBJECT_COLLECT_PANIC_INJECT.store(true, Ordering::SeqCst);

  let panic_res = catch_unwind(AssertUnwindSafe(|| {
    rt.block_on(collect_family(&tasks, &store, true));
  }));
  assert!(
    panic_res.is_err(),
    "周期 Hash 收集体注入 panic 必须触发异常展开"
  );

  assert!(
    !tasks.hcollect_in_progress.load(Ordering::SeqCst),
    "周期 collect_family panic 后 hcollect_in_progress 必须复位为 false"
  );

  // 下轮可再入
  rt.block_on(collect_family(&tasks, &store, true));
  assert!(
    !tasks.hcollect_in_progress.load(Ordering::SeqCst),
    "下轮周期 Hash 收集必须正常再入并完成"
  );

  // ---- SortedSet 族周期收集 panic unwind ----
  assert!(!tasks.zcollect_in_progress.load(Ordering::SeqCst));
  OBJECT_COLLECT_PANIC_INJECT.store(true, Ordering::SeqCst);

  let panic_res = catch_unwind(AssertUnwindSafe(|| {
    rt.block_on(collect_family(&tasks, &store, false));
  }));
  assert!(
    panic_res.is_err(),
    "周期 ZSet 收集体注入 panic 必须触发异常展开"
  );

  assert!(
    !tasks.zcollect_in_progress.load(Ordering::SeqCst),
    "周期 collect_family panic 后 zcollect_in_progress 必须复位为 false"
  );

  // 下轮可再入
  rt.block_on(collect_family(&tasks, &store, false));
  assert!(
    !tasks.zcollect_in_progress.load(Ordering::SeqCst),
    "下轮周期 ZSet 收集必须正常再入并完成"
  );
}

#[test]
fn periodic_task_supervised_loop_recovers_after_panic() {
  let _lock = TEST_SERIAL.lock();
  let store = open_test_store("periodic_supervise.db");
  let session = store.new_session().unwrap();
  session.set_context(0, 0);

  let tasks = Arc::new(PrimaryTasks::default());
  let conf = Arc::new(RuntimeServerConfig::new(RuntimeServerOptions {
    expired_object_collection_frequency_secs: 1,
    ..Default::default()
  }));
  tasks.bind_object_collect_env(&store, Some(&conf));

  let rt = Runtime::new().unwrap();
  rt.block_on(async {
    // 注入 panic：周期任务拉起首轮运行即 panic
    OBJECT_COLLECT_PANIC_INJECT.store(true, Ordering::SeqCst);

    assert!(
      tasks.try_start_object_collect_task(),
      "首次拉起周期收集任务必须成功"
    );

    // 等待 supervise_resumable 捕获 panic 并复位 object_collect_started
    for _ in 0..50 {
      sleep(Duration::from_millis(50)).await;
      if !tasks.object_collect_running() {
        break;
      }
    }
    assert!(
      !tasks.object_collect_running(),
      "任务体 panic 后 supervise_resumable 必须将 started 标志复位"
    );

    // 关键断言：单写位未被泄漏卡死，两族单写位均为 false
    assert!(
      !tasks.hcollect_in_progress.load(Ordering::SeqCst),
      "panic 展开后周期任务 hcollect 单写位必须已复位"
    );
    assert!(
      !tasks.zcollect_in_progress.load(Ordering::SeqCst),
      "panic 展开后周期任务 zcollect 单写位必须已复位"
    );

    // 重新拉起任务（对标 CONFIG SET 调停或 resume 机制）：能够重新启动且正常收集
    assert!(
      tasks.try_start_object_collect_task(),
      "panic 复位后周期任务必须可重新拉起"
    );
    assert!(tasks.object_collect_running(), "重新拉起后任务必须在跑");

    // 单写位未卡死，任务循环正常推进
    sleep(Duration::from_millis(150)).await;
    assert!(
      !tasks.hcollect_in_progress.load(Ordering::SeqCst),
      "重拉后轮次单写位正常释放"
    );
    assert!(
      !tasks.zcollect_in_progress.load(Ordering::SeqCst),
      "重拉后轮次单写位正常释放"
    );
  });
}
