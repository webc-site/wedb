//! 单键读改写窗口取闩单次尝试时延回归（票 wkv-rmw-window-single-key-async-latch-budget-latency-bomb）
//!
//! 锁定契约：单键两臂取闩均为「扩容协同定位 + `try_lock_exclusive` 单次尝试」，
//! 失闩让核重试全部由轮间 `yield_now` 预算环承接——与多键臂 `try_acquire_rmw_plan`
//! 先例（票 pair_bucket_order_latch 定因）同口径，对标 C#
//! `BasicSessionLocker.TryLockEphemeralExclusive` 单次取闩 + `RETRY_LATER`
//! 调度级重试契约。轮内嵌自旋取闩 × 每次内嵌 10 次线程让渡，永久持闩下退化为
//! 小时级时延炸弹（单轮实测 ~3.5s），即本回归钉死的缺陷形制。
//!
//! 验证：
//! 1. 钉闩下异步单键臂 `rmw_window` 烧满让核预算回 `LockTimeout`，耗时与失闩
//!    次数双上界
//! 2. 钉闩下同步单键臂 `try_rmw_window` 单次尝试即回 `None`（失闩计数上界）
//! 3. `rmw_window_sorted` 单键入参退化路径同受两界约束
//! 4. 无争用快路径单次即成、零失闩计数，持窗写回点查全绿
//!
//! 判据形制：失闩尝试计数（`RMW_KEY_LATCH_ATTEMPTS` 观测口，仿
//! `RMW_PLAN_ACQUIRE_MISS` 在库先例）为确定性主判据——嵌自旋取闩核即放大三个
//! 数量级，必红；异步臂另断言墙钟耗时上界（票面「秒级预算内回 LockTimeout」），
//! 并以满核忙自旋竞争者放大让渡成本，使嵌自旋形态在该界下真实炸出（本机空闲时
//! 线程让渡纳秒级即返回，且抖动跨三个数量级，纯墙钟界无法确定性判定，故同步臂
//! 以计数为主界、墙钟只防秒级忙等形态，防假绿）。
//!
//! 夹具：仿 wnode/tests/pair_bucket_order_latch.rs——外部件钉闩（scoped_hash
//! 寻桶）+ 真实 wkv 会话窗口，无 mock 无 sleep。

use std::{
  hint::spin_loop,
  sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
  },
  thread,
  thread::available_parallelism,
  time::{Duration, Instant},
};

use aok::{OK, Void};
use compio::runtime::Runtime;
use parking_lot::Mutex;
use wbase::align::DEFAULT_SECTOR_SIZE;
use wdev::SegmentedDevice;
use whasher::scoped_hash;
use windex::Error as WindexError;
use wkv::{Error, RMW_KEY_LATCH_ATTEMPTS, WedbStore};
use wval::SessionPrefixBuf;

use crate::support::{config, open_store};

/// 全局失闩计数观测口为进程级，本文件四案与仓内其余争用案互斥串行
/// （仿 `rmw_window_sorted.rs` 的 TEST_LOCK 先例）
static TEST_LOCK: Mutex<()> = Mutex::new(());

/// 让核预算环轮数（须与 `rmw_window.rs` 私有常量 `RMW_LATCH_YIELD_BUDGET` 同值，
/// 预算耗尽终态契约的上界判据）
const YIELD_BUDGET: usize = 1024;

/// 异步臂预算环耗时上界：单次尝试形态 1024 轮「单次尝试 + 让核」实测秒级以内；
/// 嵌自旋取闩形态每轮 1024×10 线程让渡（竞争者放大下实测百毫秒至秒级/轮），
/// 全环小时级，界必炸
const ASYNC_BUDGET_DEADLINE: Duration = Duration::from_secs(10);

/// 钉闩下同步臂单次尝试契约的失闩计数上界（恰 1 次，余量吸收他案偶发失闩；
/// 嵌自旋取闩核即 10250 次，三个数量级越界）
const SYNC_ATTEMPT_MISS_MAX: usize = 4;

/// 钉闩下异步臂轮内单次尝试契约的失闩计数上下界（每轮恰 1 次 + 预算外尾试一次；
/// 下界确证争用真的烧满预算而非首轮即成的空转通过，上界锁死轮内不嵌自旋）
const ASYNC_ATTEMPT_MISS_MIN: usize = YIELD_BUDGET / 2;
const ASYNC_ATTEMPT_MISS_MAX: usize = YIELD_BUDGET * 2;

/// 键的本键桶下标（窗口寻桶 scoped 口径：会话物理前缀种子，测试会话为缺省
/// 根域，与 `rmw_window_sorted.rs` 夹具同构）
fn bucket_of(store: &Arc<WedbStore<SegmentedDevice>>, user_key: &[u8]) -> usize {
  store
    .active_index()
    .bucket_index_for_hash(scoped_hash(SessionPrefixBuf::ROOT.as_slice(), user_key))
}

/// 外部件钉住指定桶排他闩（模拟事务持锁 / EXPIRE 协同窗 / 卡死持有者）
fn pin_bucket(store: &Arc<WedbStore<SegmentedDevice>>, bucket: usize) {
  assert!(
    store.active_index().get_bucket(bucket).try_lock_exclusive(),
    "夹具钉闩前提：目标桶须空闲"
  );
}

fn unpin_bucket(store: &Arc<WedbStore<SegmentedDevice>>, bucket: usize) {
  store.active_index().get_bucket(bucket).unlock_exclusive();
}

/// 失闩计数快照差值
fn miss_delta(before: usize) -> usize {
  RMW_KEY_LATCH_ATTEMPTS.load(Ordering::Relaxed) - before
}

/// 满核忙自旋竞争者（让核成本放大器）：使嵌自旋取闩形态下每轮 1024×10 次线程
/// 让渡成本确定性放大（macOS swtch_pri / Linux sched_yield 均有同优先 runnable
/// 对手让位），异步臂墙钟耗时上界判据得以真实炸出；单次尝试形态每轮仅 10 次
/// 桶闩内嵌让渡 + 1 次 compio 让核，界内富余
struct LoadSpreader {
  stop: Arc<AtomicBool>,
  handles: Vec<thread::JoinHandle<()>>,
}

impl LoadSpreader {
  fn new() -> Self {
    let stop = Arc::new(AtomicBool::new(false));
    let handles = (0..available_parallelism().map_or(4, |n| n.get()))
      .map(|_| {
        let stop = Arc::clone(&stop);
        thread::spawn(move || {
          while !stop.load(Ordering::Relaxed) {
            spin_loop();
          }
        })
      })
      .collect();
    Self { stop, handles }
  }
}

impl Drop for LoadSpreader {
  fn drop(&mut self) {
    self.stop.store(true, Ordering::Relaxed);
    for handle in self.handles.drain(..) {
      let _ = handle.join();
    }
  }
}

/// 案 a：钉闩下异步单键臂 rmw_window 须在让核预算内烧满轮数后回 LockTimeout，
/// 并断言耗时上界（非仅终态）与轮内单次尝试计数上界——永久持闩不再放大为
/// 小时级
#[test]
fn pinned_latch_async_single_key_window_times_out_within_deadline() -> Void {
  let _test_lock = TEST_LOCK.lock();
  Runtime::new()?.block_on(async {
    let env = open_store(
      "rmw_single_async_latch_bomb",
      config(64, DEFAULT_SECTOR_SIZE, 16)?,
    )?;
    let store = env.store;
    let session = store.new_session()?;
    let batch = session.enter_batch();

    let key = b"rmw_single_pin_async";
    let bucket = bucket_of(&store, key);
    pin_bucket(&store, bucket);
    let _load = LoadSpreader::new();

    let before = RMW_KEY_LATCH_ATTEMPTS.load(Ordering::Relaxed);
    let start = Instant::now();
    let err = match batch.rmw_window(key).await {
      Err(err) => err,
      Ok(window) => {
        drop(window);
        panic!("外部件永久钉闩下异步单键臂必回 LockTimeout");
      }
    };
    let elapsed = start.elapsed();
    let misses = miss_delta(before);
    assert!(
      matches!(err, Error::Index(WindexError::LockTimeout)),
      "预算耗尽终态必须为 LockTimeout，实得 {err:?}"
    );
    assert!(
      elapsed <= ASYNC_BUDGET_DEADLINE,
      "钉闩下预算环耗时须落在 {ASYNC_BUDGET_DEADLINE:?} 上界内（轮内嵌 1024 自旋\
       × 每次 10 让渡的时延炸弹即此炸出），实测 {elapsed:?}"
    );
    assert!(
      (ASYNC_ATTEMPT_MISS_MIN..=ASYNC_ATTEMPT_MISS_MAX).contains(&misses),
      "钉闩下异步臂须烧满让核预算且轮内至多单次尝试（{ASYNC_ATTEMPT_MISS_MIN}..=\
       {ASYNC_ATTEMPT_MISS_MAX} 次失闩），实得 {misses} 次——嵌自旋核回归即此炸出"
    );

    unpin_bucket(&store, bucket);
    OK
  })
}

/// 案 b：钉闩下同步单键臂 try_rmw_window 须即刻单次尝试回 None（嵌自旋取闩核
/// 单次调用即 10250 次让渡忙等，本机墙钟抖动跨三个数量级不可立确定性界，以失闩
/// 尝试计数锁死单次尝试契约），降级通道保持调用点协议不变
#[test]
fn pinned_latch_sync_single_key_window_returns_none_on_single_attempt() -> Void {
  let _test_lock = TEST_LOCK.lock();
  Runtime::new()?.block_on(async {
    let env = open_store(
      "rmw_single_sync_latch_bomb",
      config(64, DEFAULT_SECTOR_SIZE, 16)?,
    )?;
    let store = env.store;
    let session = store.new_session()?;
    let batch = session.enter_batch();

    let key = b"rmw_single_pin_sync";
    let bucket = bucket_of(&store, key);
    pin_bucket(&store, bucket);

    let before = RMW_KEY_LATCH_ATTEMPTS.load(Ordering::Relaxed);
    let start = Instant::now();
    assert!(
      batch.try_rmw_window(key).is_none(),
      "钉闩下同步单键臂必回 None 降级"
    );
    let elapsed = start.elapsed();
    let misses = miss_delta(before);
    assert!(
      misses <= SYNC_ATTEMPT_MISS_MAX,
      "同步臂失闩必须是单次尝试契约（≤{SYNC_ATTEMPT_MISS_MAX} 次，嵌自旋取闩核\
       为 10250 次同核忙等），实得 {misses} 次"
    );
    // 竞争确证与宽松兜底界：钉闩下失闩必发生（≥1），且单次尝试的让渡成本绝不
    // 应进入秒级忙等形态（嵌自旋取闩形态在本机竞争形态下可越数十毫秒至秒级，
    // 界防最恶情形的忙等回归而非精细判定）
    assert!(misses >= 1, "钉闩下同步臂必须至少失闩一次，争用未真实发生");
    assert!(
      elapsed <= Duration::from_secs(2),
      "同步臂失闩墙钟耗时不应进入秒级同核忙等形态（thread-per-core 下阻塞本核\
       全部任务 poll），实测 {elapsed:?}"
    );

    unpin_bucket(&store, bucket);
    OK
  })
}

/// 案 c：rmw_window_sorted 单键入参退化路径（委托 rmw_window / try_rmw_window）
/// 同受钉闩时延界与单次尝试契约约束
#[test]
fn pinned_latch_sorted_arm_single_key_delegation_bounded() -> Void {
  let _test_lock = TEST_LOCK.lock();
  Runtime::new()?.block_on(async {
    let env = open_store(
      "rmw_single_sorted_delegation",
      config(64, DEFAULT_SECTOR_SIZE, 16)?,
    )?;
    let store = env.store;
    let session = store.new_session()?;
    let batch = session.enter_batch();

    let key = b"rmw_single_pin_sorted";
    let bucket = bucket_of(&store, key);
    pin_bucket(&store, bucket);

    let before = RMW_KEY_LATCH_ATTEMPTS.load(Ordering::Relaxed);
    assert!(
      batch.try_rmw_window_sorted([key as &[u8]]).is_none(),
      "钉闩下 sorted 臂单键退化同步路径必回 None"
    );
    let sync_misses = miss_delta(before);
    assert!(
      (1..=SYNC_ATTEMPT_MISS_MAX).contains(&sync_misses),
      "sorted 退化同步路径必须恰按单次尝试契约失闩（1..={SYNC_ATTEMPT_MISS_MAX} \
       次），实得 {sync_misses} 次"
    );

    let _load = LoadSpreader::new();
    let before = RMW_KEY_LATCH_ATTEMPTS.load(Ordering::Relaxed);
    let start = Instant::now();
    let err = match batch.rmw_window_sorted([key as &[u8]]).await {
      Err(err) => err,
      Ok(windows) => {
        drop(windows);
        panic!("钉闩下 sorted 臂单键退化异步路径必回 LockTimeout");
      }
    };
    let elapsed = start.elapsed();
    let misses = miss_delta(before);
    assert!(
      matches!(err, Error::Index(WindexError::LockTimeout)),
      "退化路径预算耗尽终态必须为 LockTimeout，实得 {err:?}"
    );
    assert!(
      elapsed <= ASYNC_BUDGET_DEADLINE,
      "退化路径预算环耗时须落在 {ASYNC_BUDGET_DEADLINE:?} 上界内，实测 {elapsed:?}",
    );
    assert!(
      (ASYNC_ATTEMPT_MISS_MIN..=ASYNC_ATTEMPT_MISS_MAX).contains(&misses),
      "退化路径同样轮内至多单次尝试烧满预算，实得 {misses} 次失闩"
    );

    unpin_bucket(&store, bucket);
    OK
  })
}

/// 案 d：无争用回归——同步/异步臂单次尝试即得闩（成功臂零失闩计数），窗口持闩
/// 期桶忙、退窗即放，持窗写回点查全绿（单次尝试收口不得损害无争用快路径）
#[test]
fn uncontended_single_key_window_read_write_regression() -> Void {
  let _test_lock = TEST_LOCK.lock();
  Runtime::new()?.block_on(async {
    let env = open_store(
      "rmw_single_uncontended",
      config(64, DEFAULT_SECTOR_SIZE, 16)?,
    )?;
    let store = env.store;
    let session = store.new_session()?;
    let batch = session.enter_batch();

    let key = b"rmw_single_uncontended";
    let bucket = bucket_of(&store, key);

    // 同步臂：无争用单次尝试即持闩，持窗期桶排他闩不可复取，退窗即放
    let before = RMW_KEY_LATCH_ATTEMPTS.load(Ordering::Relaxed);
    let window = batch.try_rmw_window(key).expect("无争用同步臂必取窗");
    assert!(
      !store.active_index().get_bucket(bucket).try_lock_exclusive(),
      "窗口持有期间该桶排他闩不可复取"
    );
    window.try_rmw_sync(b"v1")?.expect("无争用同步写回必闭环");
    drop(window);
    assert!(
      store.active_index().get_bucket(bucket).try_lock_exclusive(),
      "退窗后该桶排他闩必须已释放"
    );
    store.active_index().get_bucket(bucket).unlock_exclusive();
    assert_eq!(
      miss_delta(before),
      0,
      "无争用取窗成功臂必须零失闩计数（快路径零额外原子）"
    );
    assert_eq!(session.read(key).await?, Some(b"v1".to_vec()));

    // 异步臂：无争用首轮即持闩（让核预算零触达、零失闩计数），持窗 upsert_rmw
    // 写回点查
    let before = RMW_KEY_LATCH_ATTEMPTS.load(Ordering::Relaxed);
    let start = Instant::now();
    let window = batch.rmw_window(key).await.expect("无争用异步臂必取窗");
    let elapsed = start.elapsed();
    assert!(
      elapsed <= ASYNC_BUDGET_DEADLINE,
      "无争用异步臂应首轮即成，实测 {elapsed:?}"
    );
    assert!(
      window.held.is_some(),
      "非事务会话异步臂取窗必持本键桶排他闩"
    );
    window.upsert_rmw(b"v2").await?;
    drop(window);
    assert_eq!(
      miss_delta(before),
      0,
      "无争用异步臂首轮单次尝试即成，预算环与失闩计数零触达"
    );
    assert_eq!(session.read(key).await?, Some(b"v2".to_vec()));

    // sorted 臂单键退化：无争用同步/异步均即成窗且零失闩计数
    let before = RMW_KEY_LATCH_ATTEMPTS.load(Ordering::Relaxed);
    let windows = batch
      .try_rmw_window_sorted([key as &[u8]])
      .expect("sorted 单键退化同步臂无争用必取窗");
    assert_eq!(windows.len(), 1);
    drop(windows);
    let windows = batch
      .rmw_window_sorted([key as &[u8]])
      .await
      .expect("sorted 单键退化异步臂无争用必取窗");
    assert_eq!(windows.len(), 1);
    drop(windows);
    assert_eq!(miss_delta(before), 0, "sorted 退化路径无争用零失闩计数");

    OK
  })
}
