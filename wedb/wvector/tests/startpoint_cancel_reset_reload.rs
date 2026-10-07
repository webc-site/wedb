#![recursion_limit = "512"] // 泛型设备实例化下 async 状态机嵌套深（R27 CI 实证）
//! 起点装载取消窗状态机复位回归（票：wvector-ensure-index-ready-startpoint-cancel-stuck）
//!
//! 缺陷：`ensure_index_ready_or_init` CAS 置 `SettingStartPoints(1)` 后，
//! init 体（`maybe_set_start_point`）任一 await 点被 KILL/注销取消丢弃
//! （`wnode::resp::slow_path` 头注取消面自证）时状态永久滞 1——后续一切
//! VADD 在等待臂自旋让位永不返回，且 VADD 持共享条带锁使并发 DEL/VDROP
//! 永阻（键级冻结，重启前不可自愈）。
//!
//! 修复：CAS 成功点单挂 `StartPointLoadGuard`（ObserverDropGuard 先例形态），
//! Drop 臂兜底复位 `NoStartPoints`；起点 id 0 改经 `claim_start_id` 幂等认领
//! （取消残影占用位重认领无二次计数），重试可收敛。
//!
//! 注入面：一次性写闸（仅命中起点向量记录写：context = CTX|Vector 项位、
//! 键 = id 0 小端 4 字节）+ 无 waker 忙驱，首插恰在 `write_iid` 挂起点被
//! 整体丢弃——同 `fsm_cancel_inflight_release` 挂起注入先例，全程单线程
//! 确定性，无竞态。
//!
//! 契约（修复后由守卫 Drop 构造性保证，旧码此测试预算耗尽必挂）：
//! 1. 取消丢弃后状态机复位——后续 VADD 在 poll 预算内完成起点装载并插入
//!    成功（旧码等待臂自旋永挂，预算耗尽以断言失败收场而非测试挂死）；
//! 2. 装载窗等待者不永挂：A 挂闸期间 B 入等待臂，A 丢弃后 B 自行 CAS
//!    赢位、幂等重认领 id 0 并完成装载（写面解冻，键级冻结的独阻源消除）；
//! 3. 取消零残留：被取消装载不留下起点记录，B 完成后起点记录方达。

use std::{
  future::{Future, pending},
  pin::{Pin, pin},
  sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
  },
  task::{Context as TaskContext, Poll, Waker},
};

use wvector::{
  Callbacks, DiskANNService, DiskAnnInsertResult, IndexConfig, SearchParams, StoreCallbacks,
  VectorDistanceMetricType, VectorQuantType,
  store::{TERM_BITMASK, Term},
};
use wvector_test::{MemStore, f32_bytes, test_config};

const CTX: u64 = 16;

/// 重试/等待臂 poll 预算：正确路径装载即刻闭环（首检即过）；未复位时
/// 等待臂自旋永挂，预算耗尽以回归失败收场，杜绝测试整体挂死。
const POLL_BUDGET: usize = 10_000_000;

/// 起点向量记录键字节（内部 id 0 小端 4 字节）。
const START_POINT_KEY: [u8; 4] = 0u32.to_le_bytes();

/// 一次性写闸内存桥：武装后首个起点向量记录写（maybe_set_start_point 的
/// `write_iid(Term::Vector, 0)` await 点）即永久挂起，由驱动环丢弃承载
/// future 完成取消注入；其余调用直通内存桥。
struct GateStore {
  inner: MemStore,
  /// 一次性武装位（置位后首个起点写命中即清）。
  arm: AtomicBool,
  /// 挂起标志（驱动环观察到即丢弃承载 future）。
  parked: AtomicBool,
  /// 邻接表写一次性失败注入（模拟 Vector 成功但 Neighbors 失败的崩溃/写故障半截态）。
  fail_neighbors: AtomicBool,
  /// 量化记录写一次性失败注入。
  fail_quantized: AtomicBool,
}

impl GateStore {
  /// 是否为起点向量记录写（context 含 Vector 项位、键 = id 0 小端）。
  fn is_start_point_write(context: u64, key: &[u8]) -> bool {
    context == CTX | (Term::Vector as u64 & TERM_BITMASK) && key == START_POINT_KEY
  }
}

impl StoreCallbacks for GateStore {
  async fn read_multi<F>(&self, context: u64, keys: &[u8], length_hint: usize, f: F) -> bool
  where
    F: FnMut(u32, &[u8]) + Send,
  {
    self.inner.read_multi(context, keys, length_hint, f).await
  }

  async fn read<F>(&self, context: u64, key: &[u8], f: F) -> bool
  where
    F: FnMut(&[u8]) + Send,
  {
    self.inner.read(context, key, f).await
  }

  async fn write(&self, context: u64, key: &[u8], value: &[u8]) -> bool {
    if Self::is_start_point_write(context, key) && self.arm.swap(false, Ordering::AcqRel) {
      self.parked.store(true, Ordering::Release);
      // 闸门永不开：承载 future 在起点装载的 write_iid await 点被丢弃（取消注入）
      pending::<()>().await;
    }
    if context == CTX | (Term::Neighbors as u64 & TERM_BITMASK)
      && key == START_POINT_KEY
      && self.fail_neighbors.swap(false, Ordering::AcqRel)
    {
      return false;
    }
    if context == CTX | (Term::Quantized as u64 & TERM_BITMASK)
      && key == START_POINT_KEY
      && self.fail_quantized.swap(false, Ordering::AcqRel)
    {
      return false;
    }
    self.inner.write(context, key, value).await
  }

  async fn delete(&self, context: u64, key: &[u8]) -> bool {
    self.inner.delete(context, key).await
  }

  async fn rmw<F>(&self, context: u64, key: &[u8], write_len: usize, f: F) -> bool
  where
    F: FnMut(&mut [u8]) + Send,
  {
    self.inner.rmw(context, key, write_len, f).await
  }

  async fn filter(&self, context: u64, internal_id: u32) -> bool {
    self.inner.filter(context, internal_id).await
  }

  async fn purge_context(&self, context: u64) -> bool {
    self.inner.purge_context(context).await
  }

  fn log(&self, context: u64, msg: &str) {
    self.inner.log(context, msg);
  }
}

/// 无 waker 忙驱：把 future 推进到闸门挂起点，随即由调用方丢弃（取消注入）。
fn drive_to_park<F: Future>(mut fut: Pin<&mut F>, parked: &AtomicBool) {
  let waker = Waker::noop();
  let mut cx = TaskContext::from_waker(waker);
  while !parked.load(Ordering::Acquire) {
    assert!(
      matches!(fut.as_mut().poll(&mut cx), Poll::Pending),
      "取消窗注入落空：装载在闸门命中前已完成"
    );
  }
}

/// 定额预算驱动至完成：取消窗未复位时等待臂自旋永挂，预算耗尽回归失败。
fn drive_to_done<F: Future>(mut fut: Pin<&mut F>) -> F::Output {
  let waker = Waker::noop();
  let mut cx = TaskContext::from_waker(waker);
  for _ in 0..POLL_BUDGET {
    if let Poll::Ready(value) = fut.as_mut().poll(&mut cx) {
      return value;
    }
  }
  panic!("poll 预算耗尽可能：取消窗状态机未复位，等待臂自旋永挂");
}

/// 建服务与闸桥（各测试同构前置）。
async fn setup() -> (Arc<GateStore>, Arc<DiskANNService<GateStore>>) {
  let store = Arc::new(GateStore {
    inner: MemStore::new(),
    arm: AtomicBool::new(false),
    parked: AtomicBool::new(false),
    fail_neighbors: AtomicBool::new(false),
    fail_quantized: AtomicBool::new(false),
  });
  let service = Arc::new(DiskANNService::default());
  assert_eq!(
    service
      .create_index(
        CTX,
        test_config(VectorQuantType::NoQuant),
        Callbacks::new(Arc::clone(&store))
      )
      .await,
    Ok(false)
  );
  (store, service)
}

/// 契约一/三：取消丢弃后守卫兜底复位＋幂等重认领，后续 VADD 完成起点装载。
#[compio::test]
async fn cancel_drop_resets_state_and_next_insert_completes_load() {
  let (store, service) = setup().await;

  // 张闸：新集合首插必经起点装载，write_iid(Vector, 0) await 点挂起即丢弃
  store.arm.store(true, Ordering::Release);
  {
    let aborted = pin!(async {
      service
        .insert(CTX, b"e_abort", &f32_bytes(&[1.0, 1.0]), b"")
        .await
    });
    drive_to_park(aborted, &store.parked);
    // 出作用域即丢弃：取消窗主体，旧码 state 永久滞 SettingStartPoints(1)
  }

  // 契约三：被取消装载未落起点记录
  assert_eq!(
    store.inner.peek(CTX, Term::Vector, &START_POINT_KEY),
    None,
    "取消的装载不应留下起点向量记录"
  );

  // 契约一：守卫复位 NoStartPoints 后下一条 VADD 预算内完成装载并插入
  //（旧码等待臂自旋永挂，此处预算耗尽 panic 收场）
  let result = drive_to_done(pin!(async {
    service
      .insert(CTX, b"e1", &f32_bytes(&[2.0, 2.0]), b"")
      .await
  }));
  assert!(
    matches!(result, DiskAnnInsertResult::True),
    "取消后 VADD 应完成起点装载并插入成功: {result:?}"
  );

  // 起点记录随后置装载落定
  assert_eq!(
    store.inner.peek(CTX, Term::Vector, &START_POINT_KEY),
    Some(f32_bytes(&[2.0, 2.0])),
    "重试装载应落起点 id 0 向量记录"
  );
}

/// 契约二：装载窗等待者在被取消装载丢弃后自行赢位完成装载，写面解冻不永挂。
#[compio::test]
async fn waiter_in_load_window_finishes_after_cancel_drop() {
  let (store, service) = setup().await;

  // A 挂闸：state 置 SettingStartPoints，B 入等待臂自旋让位；出作用域
  // 即丢弃 A（KILL/注销取消语义），守卫 Drop 复位 NoStartPoints
  store.arm.store(true, Ordering::Release);
  let wait_fut = async {
    service
      .insert(CTX, b"e_wait", &f32_bytes(&[3.0, 3.0]), b"")
      .await
  };
  let mut waiter = pin!(wait_fut);
  {
    let abort_fut = async {
      service
        .insert(CTX, b"e_abort", &f32_bytes(&[1.0, 1.0]), b"")
        .await
    };
    let mut aborted = pin!(abort_fut);
    drive_to_park(aborted.as_mut(), &store.parked);

    let waker = Waker::noop();
    let mut cx = TaskContext::from_waker(waker);
    assert!(
      matches!(waiter.as_mut().poll(&mut cx), Poll::Pending),
      "B 应在装载窗入等待臂挂起"
    );
  }

  // B 自行 CAS 赢位、幂等重认领 id 0（闸已耗尽直通）并完成装载插入
  let result = drive_to_done(waiter.as_mut());
  assert!(
    matches!(result, DiskAnnInsertResult::True),
    "等待者应在取消丢弃后完成起点装载: {result:?}"
  );
  assert!(
    service
      .internal_id_of(CTX, b"e_wait")
      .await
      .unwrap()
      .is_some(),
    "等待者插入应落位"
  );
  assert_eq!(
    store.inner.peek(CTX, Term::Vector, &START_POINT_KEY),
    Some(f32_bytes(&[3.0, 3.0])),
    "起点记录应由等待者装载落定"
  );
}

/// 票：wvector-start-point-partial-write-crash-window-hard-error-no-self-heal (§192)
/// 场景一：首个 VADD 在 maybe_set_start_point 写入 Vector 0 成功后，Neighbors 0 写入故障
/// （返回 StoreError）；重试时 read_start_point_core 自动补写空邻接表收敛半截态，VADD 不再报 ERR。
#[compio::test]
async fn startpoint_neighbors_write_failure_self_heals_on_retry() {
  let (store, service) = setup().await;

  // 武装：首次写 Neighbors 0 失败
  store.fail_neighbors.store(true, Ordering::Release);

  // 首次 VADD：Vector 0 写入成功，Neighbors 0 注入写失败
  let res1 = service
    .insert(CTX, b"e1", &f32_bytes(&[1.0, 1.0]), b"")
    .await;
  assert_eq!(
    res1,
    DiskAnnInsertResult::StoreError,
    "首插在 Neighbors 0 失败时应返回 StoreError"
  );

  // 此时存储中 Vector 0 已落盘，但 Neighbors 0 缺失（半截态）
  assert!(
    store
      .inner
      .peek(CTX, Term::Vector, &START_POINT_KEY)
      .is_some(),
    "Vector 0 应已落盘"
  );
  assert_eq!(
    store.inner.peek(CTX, Term::Neighbors, &START_POINT_KEY),
    None,
    "Neighbors 0 注入失败，盘面应无此键"
  );

  // 重试 VADD：read_start_point_core 触发半截态自愈补写空邻接表，随后建图并插入成功
  let res2 = service
    .insert(CTX, b"e1", &f32_bytes(&[1.0, 1.0]), b"")
    .await;
  assert_eq!(
    res2,
    DiskAnnInsertResult::True,
    "半截态自愈后重试 VADD 应成功收敛"
  );

  // 验证 Neighbors 0 记录已由自愈臂补写
  assert!(
    store
      .inner
      .peek(CTX, Term::Neighbors, &START_POINT_KEY)
      .is_some(),
    "自愈臂应已补写 Neighbors 0 记录"
  );

  // 后续 VADD 与检索验证索引健康
  let res3 = service
    .insert(CTX, b"e2", &f32_bytes(&[2.0, 2.0]), b"")
    .await;
  assert_eq!(res3, DiskAnnInsertResult::True);

  let search_res = service
    .search_vector(
      CTX,
      &f32_bytes(&[1.0, 1.0]),
      SearchParams {
        count: 2,
        search_exploration_factor: 16,
        filter_len: 0,
        max_filtering_effort: 0,
      },
    )
    .await;
  assert!(search_res.is_ok(), "自愈后检索应成功");
}

/// 票：wvector-start-point-partial-write-crash-window-hard-error-no-self-heal (§192)
/// 场景二：Vector 0 落盘后、Neighbors 0 落盘前进程崩溃（通过 Neighbors 注入写失败模拟）；
/// 重启后通过 create_index 重新装配 Provider，read_start_point_core 在构造期自愈补写
/// 空邻接表，杜绝原码建籍即败（CreateIndexError）与索引永久砖死。
#[compio::test]
async fn startpoint_crash_window_recovers_on_restart() {
  let store = Arc::new(GateStore {
    inner: MemStore::new(),
    arm: AtomicBool::new(false),
    parked: AtomicBool::new(false),
    fail_neighbors: AtomicBool::new(false),
    fail_quantized: AtomicBool::new(false),
  });
  let service1 = Arc::new(DiskANNService::default());
  assert_eq!(
    service1
      .create_index(
        CTX,
        test_config(VectorQuantType::NoQuant),
        Callbacks::new(Arc::clone(&store))
      )
      .await,
    Ok(false)
  );

  // 模拟首插中途崩溃在 Neighbors 0
  store.fail_neighbors.store(true, Ordering::Release);
  let res = service1
    .insert(CTX, b"e_crash", &f32_bytes(&[1.0, 1.0]), b"")
    .await;
  assert_eq!(res, DiskAnnInsertResult::StoreError);

  // 抛弃旧 service，模拟进程重启
  drop(service1);

  // 重启创建/恢复索引：旧码此处因 read_start_point_core 硬报 StoreError::Read，
  // 导致 create_index 报 Err(CreateIndexError) 彻底死锁；新码就地自愈
  let service2 = Arc::new(DiskANNService::default());
  let create_res = service2
    .create_index(
      CTX,
      test_config(VectorQuantType::NoQuant),
      Callbacks::new(Arc::clone(&store)),
    )
    .await;
  assert!(
    create_res.is_ok(),
    "重启装载起点半截态索引应自愈成功: {create_res:?}"
  );

  // 重启后 VADD 应成功
  let res_after = service2
    .insert(CTX, b"e1", &f32_bytes(&[2.0, 2.0]), b"")
    .await;
  assert_eq!(
    res_after,
    DiskAnnInsertResult::True,
    "重启自愈后 VADD 应正常写入"
  );

  let search_res = service2
    .search_vector(
      CTX,
      &f32_bytes(&[2.0, 2.0]),
      SearchParams {
        count: 1,
        search_exploration_factor: 16,
        filter_len: 0,
        max_filtering_effort: 0,
      },
    )
    .await;
  assert!(search_res.is_ok(), "重启自愈后检索应成功");
}

/// 票：wvector-start-point-partial-write-crash-window-hard-error-no-self-heal (§192)
/// 场景三：Q8 量化态下，Vector 0 写入成功但 Quantized 0 失败；重试/重启时自愈臂
/// 从全精度向量重建并补写量化记录，消除 StoreError::Read。
#[compio::test]
async fn startpoint_quantized_failure_and_reload_self_heals() {
  let store = Arc::new(GateStore {
    inner: MemStore::new(),
    arm: AtomicBool::new(false),
    parked: AtomicBool::new(false),
    fail_neighbors: AtomicBool::new(false),
    fail_quantized: AtomicBool::new(false),
  });
  let q8_cfg = IndexConfig {
    dims: 2,
    reduce_dims: 0,
    quant_type: VectorQuantType::Q8,
    distance_metric: VectorDistanceMetricType::L2,
    build_exploration_factor: 64,
    num_links: 8,
  };
  let service = Arc::new(DiskANNService::default());
  assert_eq!(
    service
      .create_index(CTX, q8_cfg, Callbacks::new(Arc::clone(&store)))
      .await,
    Ok(false)
  );

  // 武装：量化记录写失败
  store.fail_quantized.store(true, Ordering::Release);

  let res1 = service
    .insert(CTX, b"q1", &f32_bytes(&[1.5, 2.5]), b"")
    .await;
  assert_eq!(
    res1,
    DiskAnnInsertResult::StoreError,
    "Quantized 0 写入失败时应返回 StoreError"
  );

  // Vector 0 已落盘，但 Quantized 0 缺失
  assert!(
    store
      .inner
      .peek(CTX, Term::Vector, &START_POINT_KEY)
      .is_some()
  );
  assert_eq!(
    store.inner.peek(CTX, Term::Quantized, &START_POINT_KEY),
    None
  );

  // 重试：自愈重建并补写 Quantized 0 与 Neighbors 0
  let res2 = service
    .insert(CTX, b"q1", &f32_bytes(&[1.5, 2.5]), b"")
    .await;
  assert_eq!(
    res2,
    DiskAnnInsertResult::True,
    "量化半截态自愈后 VADD 应成功"
  );
  assert!(
    store
      .inner
      .peek(CTX, Term::Quantized, &START_POINT_KEY)
      .is_some()
  );

  // 重启验证
  drop(service);
  let service2 = Arc::new(DiskANNService::default());
  assert!(
    service2
      .create_index(CTX, q8_cfg, Callbacks::new(Arc::clone(&store)))
      .await
      .is_ok()
  );

  let search_res = service2
    .search_vector(
      CTX,
      &f32_bytes(&[1.5, 2.5]),
      SearchParams {
        count: 1,
        search_exploration_factor: 16,
        filter_len: 0,
        max_filtering_effort: 0,
      },
    )
    .await;
  assert!(search_res.is_ok(), "量化自愈后检索应成功");
}
