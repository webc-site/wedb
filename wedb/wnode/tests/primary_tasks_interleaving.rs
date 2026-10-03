//! Primary 类周期任务禁用/重拉交错锁测（自 src/primary_tasks.rs 内联测试
//! 迁入：集成测试形态，断言与覆盖原样保留）
//!
//! 覆盖：退出判定、标志翻转、重拉判定在 commit_env / object_collect_env
//! 锁内严格串行——禁用与重启用交错下终态恰有一个任务在跑，无任务丢失。
//! 退出臂挂起点经 debug 形态注入槽 `wnode::primary_tasks::TaskExitGate`
//! 承接（原 Arc<dyn Fn> 全局静态钩子的非 dyn 化收编），release 形态整体
//! 编译消除。

#![cfg(debug_assertions)]

use std::{
  sync::{Arc, mpsc},
  thread,
  time::Duration,
};

use compio::{runtime::Runtime, time};
use tempfile::tempdir;
use waof::{WalConfig, WalLog};
use wconf::{RuntimeServerConfig, RuntimeServerOptions, ServerConfigType};
use wdev::SegmentedDevice;
use wkv::{StoreConfig, WedbStore};
use wnode::{
  PrimaryTasks,
  aof::{GarnetAppendOnlyFile, GarnetLog, waof_sublog::WaofSublog},
  primary_tasks::TaskExitGate,
};

fn test_aof_facade() -> (tempfile::TempDir, Arc<GarnetAppendOnlyFile>) {
  let options = RuntimeServerOptions {
    aof_physical_sublog_count: 1,
    aof_replay_task_count: 1,
    ..RuntimeServerOptions::default()
  };
  let dir = tempdir().expect("tempdir");
  let device =
    Arc::new(SegmentedDevice::single_file(dir.path().join("pt_aof.wal")).expect("SegmentedDevice"));
  let wal = WalLog::new(device, WalConfig::default()).expect("WalLog");
  let backend = Arc::new(WaofSublog::new(Arc::new(wal)));
  let aof = Arc::new(GarnetAppendOnlyFile::new(
    Arc::new(GarnetLog::new(&options, vec![backend], None).expect("构造 GarnetLog")),
    &options,
    None,
  ));
  (dir, aof)
}

fn test_open_store(tag: &str) -> (tempfile::TempDir, Arc<WedbStore<SegmentedDevice>>) {
  let dir = tempdir().expect("tempdir");
  let cfg = StoreConfig::new(1024, 64 * 1024, 16, 0.5).expect("StoreConfig");
  let device = Arc::new(
    SegmentedDevice::single_file(dir.path().join(format!("{tag}.db"))).expect("SegmentedDevice"),
  );
  let store = Arc::new(WedbStore::open(cfg, device).expect("open store"));
  (dir, store)
}

/// 锁测：验证 AOF 提交任务在禁用与重新启用交错时，退出判定、标志翻转、重拉判定
/// 在 commit_env 锁内严格串行，保证终态恰有一个任务在运行，无任务丢失。
#[test]
fn test_commit_task_exit_restart_interleaving_serialized() {
  let tasks = Arc::new(PrimaryTasks::default());
  let (_dir, aof) = test_aof_facade();
  let rc = Arc::new(RuntimeServerConfig::new(RuntimeServerOptions {
    commit_frequency_ms: 10,
    ..RuntimeServerOptions::default()
  }));
  tasks.bind_commit_env(&aof, &rc);

  // 注入闸门语义：命中退出臂即挂起，测试放行前 started 不翻转（原
  // COMMIT_EXIT_BARRIER Arc<dyn Fn> 钩子的非 dyn 等价形态）
  tasks.commit_exit_gate.reset();
  struct GateDrop<'a>(&'a TaskExitGate);
  impl Drop for GateDrop<'_> {
    fn drop(&mut self) {
      self.0.release();
    }
  }
  let _guard = GateDrop(&tasks.commit_exit_gate);

  // 运行线程 1：宿主并驱动旧提交任务
  let (stop_runner_tx, stop_runner_rx) = mpsc::channel();
  let (ready_tx, ready_rx) = mpsc::channel();
  let tasks_runner = Arc::clone(&tasks);
  let runner = thread::spawn(move || {
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
      assert!(tasks_runner.try_start_commit_task(), "初始拉起提交任务成功");
      ready_tx.send(()).unwrap();
      while stop_runner_rx.try_recv().is_err() {
        time::sleep(Duration::from_millis(10)).await;
      }
    });
  });

  ready_rx.recv().unwrap();
  assert!(tasks.commit_running(), "任务处于运行态");

  // 禁用任务：触发 aof_commit_loop 下一轮进退出臂
  rc.try_set(ServerConfigType::AofCommitFreq, "-1").unwrap();

  // 等待旧任务在 commit_env 锁内读禁用值并命中测试闸门挂起
  assert!(
    tasks.commit_exit_gate.wait_entered(Duration::from_secs(5)),
    "旧任务必须进入退出臂并在锁内挂起"
  );

  // 此时旧任务正在持锁，尚未执行 started.store(false)
  assert!(
    tasks.commit_running(),
    "旧任务持锁挂起期间 started 尚未落 false"
  );

  // 重新启用：CONFIG SET 调停写回正值
  rc.try_set(ServerConfigType::AofCommitFreq, "20").unwrap();

  // 在独立线程尝试拉起任务（模拟并发 CONFIG SET 调停）
  let (start_done_tx, start_done_rx) = mpsc::channel();
  let (stop_starter_tx, stop_starter_rx) = mpsc::channel();
  let tasks_clone = Arc::clone(&tasks);
  let starter = thread::spawn(move || {
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
      let res = tasks_clone.try_start_commit_task();
      let _ = start_done_tx.send(res);
      while stop_starter_rx.try_recv().is_err() {
        time::sleep(Duration::from_millis(10)).await;
      }
    });
  });

  // 验证：由于旧任务持有 commit_env 锁，try_start_commit_task 必须阻塞在锁外，无法抢跑
  thread::sleep(Duration::from_millis(50));
  assert!(
    start_done_rx.try_recv().is_err(),
    "try_start 必须在 commit_env 锁外排队，不得抢跑"
  );

  // 放行旧任务：旧任务在锁内落 started.store(false)，释放环境锁并退出循环
  tasks.commit_exit_gate.release();

  // starter 获取锁并成功拉起新任务
  let started = start_done_rx
    .recv_timeout(Duration::from_secs(5))
    .expect("starter 必须成功完成");
  assert!(started, "终态必须成功拉起新任务");

  // 终态验证：恰有一个任务在跑，无任务丢失
  assert!(tasks.commit_running(), "终态任务必须在运行，零任务丢失");

  let _ = stop_runner_tx.send(());
  let _ = stop_starter_tx.send(());
  runner.join().unwrap();
  starter.join().unwrap();
}

/// 锁测：验证对象收集任务在禁用与重新启用交错时，退出判定、标志翻转、重拉判定
/// 在 object_collect_env 锁内严格串行，保证终态恰有一个任务在运行，无任务丢失。
#[test]
fn test_object_collect_task_exit_restart_interleaving_serialized() {
  let tasks = Arc::new(PrimaryTasks::default());
  let (_dir, store) = test_open_store("pt_obj_test");
  let rc = Arc::new(RuntimeServerConfig::new(RuntimeServerOptions {
    expired_object_collection_frequency_secs: 1,
    ..RuntimeServerOptions::default()
  }));
  tasks.bind_object_collect_env(&store, Some(&rc));

  tasks.object_collect_exit_gate.reset(); // 注入：置 Armed 待任务命中
  struct GateDrop<'a>(&'a TaskExitGate);
  impl Drop for GateDrop<'_> {
    fn drop(&mut self) {
      self.0.release();
    }
  }
  let _guard = GateDrop(&tasks.object_collect_exit_gate);

  // 运行线程 1：宿主并驱动旧对象收集任务
  let (stop_runner_tx, stop_runner_rx) = mpsc::channel();
  let (ready_tx, ready_rx) = mpsc::channel();
  let tasks_runner = Arc::clone(&tasks);
  let runner = thread::spawn(move || {
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
      assert!(
        tasks_runner.try_start_object_collect_task(),
        "初始拉起对象收集成功"
      );
      ready_tx.send(()).unwrap();
      while stop_runner_rx.try_recv().is_err() {
        time::sleep(Duration::from_millis(50)).await;
      }
    });
  });

  ready_rx.recv().unwrap();
  assert!(tasks.object_collect_running(), "任务处于运行态");

  // 禁用任务：触发 object_collect_loop 下一轮进退出臂
  rc.try_set(ServerConfigType::ExpiredObjectCollectionFreq, "0")
    .unwrap();

  // 等待旧任务在 object_collect_env 锁内读禁用值并命中测试闸门挂起
  assert!(
    tasks
      .object_collect_exit_gate
      .wait_entered(Duration::from_secs(5)),
    "旧任务必须进入退出臂并在锁内挂起"
  );

  assert!(
    tasks.object_collect_running(),
    "旧任务持锁挂起期间 started 尚未落 false"
  );

  // 重新启用：CONFIG SET 调停写回正值
  rc.try_set(ServerConfigType::ExpiredObjectCollectionFreq, "1")
    .unwrap();

  // 在独立线程尝试拉起任务
  let (start_done_tx, start_done_rx) = mpsc::channel();
  let (stop_starter_tx, stop_starter_rx) = mpsc::channel();
  let tasks_clone = Arc::clone(&tasks);
  let starter = thread::spawn(move || {
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
      let res = tasks_clone.try_start_object_collect_task();
      let _ = start_done_tx.send(res);
      while stop_starter_rx.try_recv().is_err() {
        time::sleep(Duration::from_millis(50)).await;
      }
    });
  });

  // 验证：旧任务持锁期间，try_start 必须阻塞在锁外
  thread::sleep(Duration::from_millis(50));
  assert!(
    start_done_rx.try_recv().is_err(),
    "try_start 必须在 object_collect_env 锁外排队，不得抢跑"
  );

  // 放行旧任务：旧任务在锁内落 started.store(false)，释放环境锁并退出循环
  tasks.object_collect_exit_gate.release();

  // starter 获取锁并成功拉起新任务
  let started = start_done_rx
    .recv_timeout(Duration::from_secs(5))
    .expect("starter 必须成功完成");
  assert!(started, "终态必须成功拉起新任务");

  // 终态验证：恰有一个任务在跑，无任务丢失
  assert!(
    tasks.object_collect_running(),
    "终态任务必须在运行，零任务丢失"
  );

  let _ = stop_runner_tx.send(());
  let _ = stop_starter_tx.send(());
  runner.join().unwrap();
  starter.join().unwrap();
}
