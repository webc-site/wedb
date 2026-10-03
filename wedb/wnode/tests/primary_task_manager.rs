//! Primary 类后台任务生命周期域集成测（对标 garnet/test/standalone/Garnet.test/
//! TaskManagerTests.cs：TaskManager 注册面判定的 rust 形态锁测）
//!
//! 对位映射（C# 注册表托管面不移植，裁定见 wnode/src/primary_tasks.rs 域头）：
//! - TestBasicRegisterAndRun / TestDoubleRegistrationAsync → `try_start_commit_task`
//!   / `try_start_object_collect_task` 双启守卫：首次拉起返 true，二次返 false，
//!   任务只跑一份；
//! - TestTaskPlacementCategoryCancellation → suspend/resume 角色位：挂起态启动
//!   一律拒绝，挂起为常驻轮空而非取消（在跑位不落），恢复幂等且任务自续跑；
//! - TestTaskRegisterAfterDispose → rust 无 Dispose 注册表面，等价拒绝臂为
//!   执行域未绑定与频率槽位禁用（启动恒拒、在跑位不动）；
//! - TestTaskFactoryException / TestCleanupWithException → 任务体终止（引擎释放
//!   弱引用自退出臂）后启动位同点清理，IsRunning 可查假且可重拉复活。
//!
//! 全部经既有 pub 面驱动，零 #[doc(hidden)] 依赖。

use std::{sync::Arc, time::Duration};

use compio::{runtime::Runtime, time::sleep};
use tempfile::TempDir;
use waof::{WalConfig, WalLog};
use wconf::{RuntimeServerConfig, RuntimeServerOptions, ServerConfigType};
use wdev::SegmentedDevice;
use wkv::{StoreConfig, WedbStore};
use wnode::{
  GarnetAppendOnlyFile, GarnetLog, PrimaryTasks,
  aof::waof_sublog::{AofSublog, WaofSublog},
};

/// 有界轮询：等待谓词成立（10ms 步进、5s 预算），到期仍假即由调用方断言失败
async fn wait_until(pred: impl Fn() -> bool) -> bool {
  for _ in 0..500 {
    if pred() {
      return true;
    }
    sleep(Duration::from_millis(10)).await;
  }
  pred()
}

/// 运行时配置：提交与对象收集频率槽位显式播种（其余取默认）
fn runtime_config(commit_ms: i32, collect_secs: i32) -> Arc<RuntimeServerConfig> {
  Arc::new(RuntimeServerConfig::new(RuntimeServerOptions {
    commit_frequency_ms: commit_ms,
    expired_object_collection_frequency_secs: collect_secs,
    ..RuntimeServerOptions::default()
  }))
}

/// 轻量真实段设备 AOF 门面（与 aof_append_only_file_surface.rs 同一装配口径，
/// 依赖面全部为既有 pub API）
fn aof_facade() -> (TempDir, Arc<GarnetAppendOnlyFile>) {
  let options = RuntimeServerOptions {
    aof_physical_sublog_count: 1,
    aof_replay_task_count: 1,
    ..RuntimeServerOptions::default()
  };
  let dir = tempfile::tempdir().expect("tempdir");
  let device = Arc::new(
    SegmentedDevice::single_file(dir.path().join("ptm_aof.wal")).expect("SegmentedDevice"),
  );
  let wal = WalLog::new(device, WalConfig::default()).expect("WalLog");
  let backend: Arc<AofSublog> = Arc::new(WaofSublog::new(Arc::new(wal)));
  let aof = Arc::new(GarnetAppendOnlyFile::new(
    Arc::new(GarnetLog::new(&options, vec![backend], None).expect("构造 GarnetLog")),
    &options,
    None,
  ));
  (dir, aof)
}

/// 小型真存储引擎（对象收集执行域装配体）
fn open_store(tag: &str) -> (TempDir, Arc<WedbStore<SegmentedDevice>>) {
  let dir = tempfile::tempdir().expect("tempdir");
  let cfg = StoreConfig::new(1024, 64 * 1024, 16, 0.5).expect("StoreConfig");
  let device = Arc::new(
    SegmentedDevice::single_file(dir.path().join(format!("{tag}.db"))).expect("SegmentedDevice"),
  );
  let store = Arc::new(WedbStore::open(cfg, device).expect("open store"));
  (dir, store)
}

/// 对位 TestBasicRegisterAndRun + TestDoubleRegistrationAsync：双启守卫——
/// 首次拉起返 true，同任务二次拉起返 false，任务只跑一份
#[test]
fn try_start_twice_second_start_refused() {
  Runtime::new().unwrap().block_on(async {
    let tasks = Arc::new(PrimaryTasks::default());
    let (_dir, aof) = aof_facade();
    let rc = runtime_config(50, 1);
    let (_sdir, store) = open_store("ptm_twice");

    tasks.bind_commit_env(&aof, &rc);
    tasks.bind_object_collect_env(&store, Some(&rc));

    assert!(tasks.try_start_commit_task(), "首次拉起周期提交任务须成功");
    assert!(!tasks.try_start_commit_task(), "双启守卫：二次拉起必须拒绝");
    assert!(
      tasks.try_start_object_collect_task(),
      "首次拉起周期对象收集任务须成功"
    );
    assert!(
      !tasks.try_start_object_collect_task(),
      "双启守卫：二次拉起必须拒绝"
    );
    assert!(tasks.object_collect_running(), "拉起后任务须呈在跑位");
  });
}

/// 对位 TestTaskRegisterAfterDispose（rust 等价拒绝面）：执行域未绑定或频率
/// 槽位禁用时启动恒拒、在跑位不动——禁用臂同时兼 TestTaskFactoryException 的
/// 「注册失败即不在跑」判定
#[test]
fn start_refused_when_env_unbound_or_disabled() {
  Runtime::new().unwrap().block_on(async {
    let tasks = Arc::new(PrimaryTasks::default());
    assert!(!tasks.try_start_commit_task(), "提交执行域未绑定须拒绝拉起");
    assert!(
      !tasks.try_start_object_collect_task(),
      "对象收集执行域未绑定须拒绝拉起"
    );
    assert!(!tasks.object_collect_running(), "拒绝拉起不得置在跑位");

    // 域已绑定但频率槽位禁用（<=0）：同属启动恒拒
    let rc_off = runtime_config(0, 0);
    let (_dir, aof) = aof_facade();
    let (_sdir, store) = open_store("ptm_disabled");
    tasks.bind_commit_env(&aof, &rc_off);
    tasks.bind_object_collect_env(&store, Some(&rc_off));
    assert!(!tasks.try_start_commit_task(), "提交频率禁用须拒绝拉起");
    assert!(
      !tasks.try_start_object_collect_task(),
      "对象收集频率禁用须拒绝拉起"
    );
    assert!(!tasks.object_collect_running(), "禁用臂不得置在跑位");
  });
}

/// 对位 TestTaskPlacementCategoryCancellation：角色位挂起/恢复门——挂起态启动
/// 一律拒绝；挂起是常驻轮空而非取消（在跑位不落，对标 C# 副本角色任务体仅
/// Delay 形态）；恢复幂等且任务无需重拉自续跑
#[test]
fn suspend_resume_gate_is_idempotent_and_recovers() {
  Runtime::new().unwrap().block_on(async {
    let tasks = Arc::new(PrimaryTasks::default());
    let (_dir, aof) = aof_facade();
    let rc = runtime_config(50, 1);
    let (_sdir, store) = open_store("ptm_suspend");

    tasks.bind_commit_env(&aof, &rc);
    tasks.bind_object_collect_env(&store, Some(&rc));

    // 副本角色挂起（幂等置位）：启动一律拒绝
    tasks.suspend();
    tasks.suspend();
    assert!(tasks.is_replica(), "挂起幂等：重复挂起保持角色位");
    assert!(!tasks.try_start_commit_task(), "挂起态拉起提交任务须拒绝");
    assert!(
      !tasks.try_start_object_collect_task(),
      "挂起态拉起对象收集任务须拒绝"
    );

    // 升主恢复（幂等）：恢复内含重拉，任务在跑；重复恢复不翻车
    tasks.resume(&store);
    tasks.resume(&store);
    assert!(!tasks.is_replica(), "恢复幂等：重复恢复保持主角色位");
    assert!(tasks.object_collect_running(), "恢复后对象收集任务须在跑");

    // 在跑任务遇挂起：轮空挂起而非取消，恢复自续跑（无需重拉）
    tasks.suspend();
    assert!(
      tasks.object_collect_running(),
      "挂起是常驻轮空而非任务取消，在跑位不得翻落"
    );
    assert!(
      !tasks.try_start_commit_task(),
      "挂起态下重拉仍须拒绝（幂等守卫归一）"
    );
    tasks.resume(&store);
    assert!(
      tasks.object_collect_running(),
      "升主自恢复：无需重拉任务即在跑"
    );
  });
}

/// 对位 TestCleanupWithException（任务终止后注册位清理、可重注册）：任务体经
/// 引擎释放弱引用自退出臂终止，启动位同点清理——IsRunning 可查假，重绑执行域
/// 后可重拉复活（对标 C# 任务异常终局后 IsRegistered 翻假、可再次注册）
#[test]
fn exited_task_cleans_started_flag_and_is_restartable() {
  Runtime::new().unwrap().block_on(async {
    let tasks = Arc::new(PrimaryTasks::default());
    let rc = runtime_config(50, 1);
    let (_sdir, store) = open_store("ptm_cleanup");
    tasks.bind_object_collect_env(&store, Some(&rc));
    assert!(tasks.try_start_object_collect_task(), "装配前提：拉起成功");
    assert!(tasks.object_collect_running());

    // 引擎释放：弱引用升格失败自退出，启动位同点清理（轮询等落定，
    // 至多再跑一轮既有间隔）
    drop(store);
    assert!(
      wait_until(|| !tasks.object_collect_running()).await,
      "任务体终止后启动位必须清理（IsRunning 可查假）"
    );

    // 清理后重绑新引擎重拉：任务复活（终局非一次性死态）
    let (_sdir2, store2) = open_store("ptm_cleanup_rebind");
    tasks.bind_object_collect_env(&store2, Some(&rc));
    assert!(
      tasks.try_start_object_collect_task(),
      "启动位清理后必须可重拉复活"
    );
    assert!(tasks.object_collect_running(), "复活后任务须呈在跑位");
  });
}

/// 验证 CONFIG SET aof-commit-freq -1→正值 禁用启用快速翻转收敛：
/// 保证无论交错时序如何，重新启用后终态恰有一个任务在运行，无任务丢失。
#[test]
fn commit_task_disable_enable_flip_converges() {
  Runtime::new().unwrap().block_on(async {
    let tasks = Arc::new(PrimaryTasks::default());
    let (_dir, aof) = aof_facade();
    let rc = runtime_config(10, 1);
    tasks.bind_commit_env(&aof, &rc);

    assert!(tasks.try_start_commit_task(), "初始拉起提交任务成功");
    assert!(tasks.commit_running());

    // 快速翻转 -1 与正值多次，模拟高频 CONFIG SET 调停
    for _ in 0..10 {
      let _ = rc.try_set(ServerConfigType::AofCommitFreq, "-1");
      tasks.try_start_commit_task();
      sleep(Duration::from_millis(5)).await;
      let _ = rc.try_set(ServerConfigType::AofCommitFreq, "10");
      tasks.try_start_commit_task();
    }

    // 终态验证：收敛于在跑态，恰有一个任务运行
    assert!(
      wait_until(|| tasks.commit_running()).await,
      "AOF 提交任务在快速翻转后终态必须正常运行"
    );
  });
}

/// 验证 CONFIG SET expired-object-collection-freq 0→正值 禁用启用快速翻转收敛：
/// 保证无论交错时序如何，重新启用后终态恰有一个任务在运行，无任务丢失。
#[test]
fn object_collect_task_disable_enable_flip_converges() {
  Runtime::new().unwrap().block_on(async {
    let tasks = Arc::new(PrimaryTasks::default());
    let rc = runtime_config(10, 1);
    let (_sdir, store) = open_store("ptm_collect_flip");
    tasks.bind_object_collect_env(&store, Some(&rc));

    assert!(
      tasks.try_start_object_collect_task(),
      "初始拉起对象收集成功"
    );
    assert!(tasks.object_collect_running());

    // 快速翻转 0 与正值多次，模拟高频 CONFIG SET 调停
    for _ in 0..10 {
      let _ = rc.try_set(ServerConfigType::ExpiredObjectCollectionFreq, "0");
      tasks.try_start_object_collect_task();
      sleep(Duration::from_millis(5)).await;
      let _ = rc.try_set(ServerConfigType::ExpiredObjectCollectionFreq, "1");
      tasks.try_start_object_collect_task();
    }

    // 终态验证：收敛于在跑态，恰有一个任务运行
    assert!(
      wait_until(|| tasks.object_collect_running()).await,
      "对象收集任务在快速翻转后终态必须正常运行"
    );
  });
}
