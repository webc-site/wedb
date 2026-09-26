//! 量化项持条带共享锁全程与并发 DEL 竞速回归（票：wnode-quant-lock-drop-before-backfill）
//!
//! 对标 C# 一手形态 libs/server/Resp/Vector/VectorManager.Quantization.cs:
//! TryProcessQuantizationRequest——`using (ReadVectorIndexCore(nonBlocking: true))`
//! 把条带共享锁罩住 BuildQuantizationTable（:152）与 BackfillQuantizedVectors
//!（:166）全程；锁域内并发 DEL 的排他锁（VectorManager.Locking.cs:556
//! ReadForDeleteVectorIndex → :566 AcquireExclusiveLocks）只能等量化写完成。
//! rust 旧码在 `read_vector_index_core` 命中后即 `drop(lock)` 再建表/回填，锁域
//! 被截短，删除链（request_deletion 同步 drop_index → process_request_cleanup
//! mark_cleaning_up → process_cleanup purge_context → finished_cleaning_up 归还
//! context）可与回填并发完成，两害：
//!   * 孤儿量化记录：wkv `purge_vector_context` 为两段式（扫描区间端点取调用时
//!     快照，再逐键墓碑），快照之后落盘的记录不在收集集内 → 永久残留，context
//!     已归还再无人清扫（存储泄漏）；
//!   * 复用写穿：next_vector_set_context 自低位顺扫复用归还槽位，旧回填继续按
//!     `context.term(Quantized)` 逐 id 寻址物理键（无代际校验），直接写进新集合
//!     的同名量化轨（同 iid 被旧量化器产物覆盖，检索错距）。
//!
//! 注入面：内存桥接存储（同 vector_delete_exclusive_lock_race.rs 夹具口径），
//! 回填逐 id 量化轨写点与 purge「快照 → 逐键删除」之间以 `yield_now` 让出内核
//! ——忠实建模生产形态（wkv 会话写与全日志扫描均为真异步 I/O await，该让出窗
//! 正是快照契约自陈「依赖调用方隔离保证」的对象），非无依据假桩；两测竞速时序
//! 全由协程队列的确定性交替推进，无 sleep 观察窗。
//!
//! 断言：
//!   1. 量化项处理体内，DEL 的条带独占锁被挡到本项返回之后：purge 快照取自
//!      「该 context 全部回填写已落盘」之后（快照后量化轨零新写），终态存储该
//!      context 基址零残留（无孤儿量化记录）；
//!   2. 归还的低位 context 被新集合复用时，旧回填不可能越屏障触达新集合——
//!      新集合的量化轨（Term::Quantized 与量化器状态 `_qnt`）恒零。

use std::sync::{
  Arc,
  atomic::{AtomicBool, AtomicUsize, Ordering},
};

use compio::runtime::spawn;
use parking_lot::Mutex;
use wbase::{future::yield_now, map::HashMap};
use wdev::SegmentedDevice;
use wkv::WedbStore;
use wnode::resp::vector::{
  vector_manager::{VectorAddArgs, VectorManager, VectorManagerOptions, VectorManagerResult},
  vector_manager_index::Index,
  vector_manager_locking::{CreateIndexParams, registry_key},
  vector_manager_quantization::{QuantizationState, QuantizationStep},
  vector_store_callbacks::OwnedActiveVectorSession,
};
use wval::SessionPrefixBuf;
use wvector::{
  Callbacks, StoreCallbacks, VectorDistanceMetricType, VectorQuantType, VectorValueType,
  store::Term,
};

/// 训练样本门槛（Spherical1Bit::required_vectors 恒 1000，对标
/// wvector/tests/quant_backfill_restart.rs 同值）；+1 使建表调度判据成立。
const ELEMENTS: u32 = 1001;

/// 复用集合的元素数（覆盖旧回填的剩余 id 区间，令写穿可观测）。
const REUSE_ELEMENTS: u32 = 200;

/// 量化状态键（`_qnt`，与 dynamic_quant 的 QUANT_STATE_KEY 同值）。
const QUANT_STATE_KEY: &[u8] = &u32::from_be_bytes(*b"_qnt").to_le_bytes();

/// 上下文项类型子域掩码（低 3 位为 Term 子域，基址 = `ctx & !MASK`）。
const TERM_MASK: u64 = 0b111;

type TestMap = HashMap<(u64, Vec<u8>), Vec<u8>>;

/// 竞速观测内存桥接存储：回填量化轨写点与 purge 快照/删除之间让出内核。
struct GateStore {
  data: Mutex<TestMap>,
  /// 回填逐 id 量化向量写次数（回填进度观测口）
  backfill_writes: AtomicUsize,
  /// 已有回填量化向量落盘（回填项确在途）
  backfill_in_flight: AtomicBool,
  /// purge 快照已取（物理清扫已进入两段式）
  purge_snapshot_taken: AtomicBool,
  /// 快照之后仍落盘的量化轨写数（孤儿量化记录计数——快照收集必不含这些键）
  writes_after_snapshot: AtomicUsize,
}

impl GateStore {
  fn new() -> Self {
    Self {
      data: Mutex::new(HashMap::default()),
      backfill_writes: AtomicUsize::new(0),
      backfill_in_flight: AtomicBool::new(false),
      purge_snapshot_taken: AtomicBool::new(false),
      writes_after_snapshot: AtomicUsize::new(0),
    }
  }

  /// 是否本测危害面对象的写点（回填产物：Quantized 域逐 id 量化向量 + 量化器
  /// 状态 `_qnt`；起点/邻接表/属性/ID 映射/FSM 等非对象不让出，杜绝与危害
  /// 无关的调度轮转）
  fn is_quant_track(context: u64, key: &[u8]) -> bool {
    let term = context & TERM_MASK;
    term == Term::Quantized as u64 || (term == Term::Metadata as u64 && key == QUANT_STATE_KEY)
  }

  /// 指定上下文基址的残留记录数（全 Term 域，孤儿观测口）
  fn records_of(&self, base: u64) -> usize {
    self
      .data
      .lock()
      .keys()
      .filter(|&&(ctx, _)| ctx & !TERM_MASK == base)
      .count()
  }

  /// 指定上下文基址的量化轨记录数（复用写穿观测口）
  fn quant_records_of(&self, base: u64) -> usize {
    self
      .data
      .lock()
      .iter()
      .filter(|((ctx, key), _)| ctx & !TERM_MASK == base && Self::is_quant_track(*ctx, key))
      .count()
  }
}

impl StoreCallbacks for GateStore {
  async fn read_multi<F>(&self, context: u64, keys: &[u8], _length_hint: usize, mut f: F) -> bool
  where
    F: FnMut(u32, &[u8]) + Send,
  {
    let mut index = 0u32;
    let mut rest = keys;
    let guard = self.data.lock();
    while rest.len() >= 4 {
      let len = u32::from_le_bytes(rest[..4].try_into().unwrap()) as usize;
      let total = 4 + len;
      if rest.len() < total {
        break;
      }
      if let Some(value) = guard.get(&(context, rest[4..total].to_vec())) {
        f(index, value);
      }
      index += 1;
      rest = &rest[total..];
    }
    true
  }

  async fn read<F>(&self, context: u64, key: &[u8], mut f: F) -> bool
  where
    F: FnMut(&[u8]) + Send,
  {
    match self.data.lock().get(&(context, key.to_vec())) {
      Some(value) => {
        f(value);
        true
      }
      None => false,
    }
  }

  async fn write(&self, context: u64, key: &[u8], value: &[u8]) -> bool {
    self
      .data
      .lock()
      .insert((context, key.to_vec()), value.to_vec());
    if Self::is_quant_track(context, key) {
      if self.purge_snapshot_taken.load(Ordering::Acquire) {
        self.writes_after_snapshot.fetch_add(1, Ordering::Relaxed);
      }
      if context & TERM_MASK == Term::Quantized as u64 {
        self.backfill_writes.fetch_add(1, Ordering::Relaxed);
        self.backfill_in_flight.store(true, Ordering::Release);
        // 生产形态：回填逐 id 写为 wkv 会话异步 I/O，让出内核令并发 DEL 抢入
        yield_now().await;
      }
    }
    true
  }

  async fn delete(&self, context: u64, key: &[u8]) -> bool {
    self.data.lock().remove(&(context, key.to_vec())).is_some()
  }

  async fn rmw<F>(&self, context: u64, key: &[u8], write_len: usize, mut f: F) -> bool
  where
    F: FnMut(&mut [u8]) + Send,
  {
    // 纯读短路（对齐生产回调与 C# 谓词 WriteDesiredSize == 0 判假）
    if write_len == 0 {
      return true;
    }
    // 对齐生产内核口径：write_len 即目标记录尺寸，整值写回
    let mut buf = self
      .data
      .lock()
      .get(&(context, key.to_vec()))
      .cloned()
      .unwrap_or_default();
    buf.resize(write_len, 0);
    f(&mut buf);
    self.data.lock().insert((context, key.to_vec()), buf);
    true
  }

  async fn filter(&self, _context: u64, _internal_id: u32) -> bool {
    true
  }

  async fn purge_context(&self, context: u64) -> bool {
    // 段 1：快照收集（对齐 wkv session/vector_cleanup.rs:purge_vector_context
    // 「扫描区间端点为调用时快照」——快照后追加的记录不在收集集内）
    let victims: Vec<(u64, Vec<u8>)> = self
      .data
      .lock()
      .keys()
      .filter(|&&(ctx, _)| ctx & !TERM_MASK == context)
      .cloned()
      .collect();
    self.purge_snapshot_taken.store(true, Ordering::Release);
    // 段 2：逐键物理墓碑为存储异步操作（生产全日志扫描跨多次 I/O await）；
    // 快照与删除之间的让出窗即调用方隔离契约的保护对象
    yield_now().await;
    yield_now().await;
    let mut guard = self.data.lock();
    for key in victims {
      guard.remove(&key);
    }
    true
  }

  fn log(&self, _context: u64, _msg: &str) {}
}

/// 真 wkv 会话绑定（后台臂的执行域兜底口径，同
/// vector_delete_exclusive_lock_race::race_store；登记表写透须有会话可用）
fn bound_domain() -> (
  tempfile::TempDir,
  Arc<WedbStore<SegmentedDevice>>,
  OwnedActiveVectorSession<SegmentedDevice>,
) {
  let dir = tempfile::tempdir().unwrap();
  let device = Arc::new(SegmentedDevice::single_file(dir.path().join("quant-del.db")).unwrap());
  let store = Arc::new(WedbStore::open(wtest_base::test_store_config(), device).unwrap());
  let bound = OwnedActiveVectorSession::new(store.new_session().unwrap());
  (dir, store, bound)
}

/// 单分片量化装配（回填分片数固定 1，令竞速窗口单点可观测）
fn manager(store: Arc<GateStore>) -> Arc<VectorManager<GateStore>> {
  Arc::new(VectorManager::new(
    VectorManagerOptions {
      is_enabled: true,
      quantization_task_count: 1,
    },
    Callbacks::new(store),
  ))
}

fn bin_params() -> CreateIndexParams {
  CreateIndexParams {
    hash_slot: 0,
    dims: 2,
    reduce_dims: 0,
    quant: VectorQuantType::Bin,
    build_exploration_factor: 64,
    num_links: 8,
    distance_metric: VectorDistanceMetricType::L2,
  }
}

fn f32_bytes(vals: &[f32]) -> Vec<u8> {
  vals.iter().flat_map(|v| v.to_le_bytes()).collect()
}

/// 建 Bin 集合并预插 `count` 个二维向量（建表前只落完整向量轨）。
async fn seed_bin(mgr: &VectorManager<GateStore>, key: &[u8], count: u32) -> Index {
  let (index, guard) = mgr
    .read_or_create_vector_index(SessionPrefixBuf::ROOT.as_slice(), key, Some(&bin_params()))
    .await
    .unwrap();
  // 建集臂守卫即放：预插走直调链路，锁窗口归量化项处理体本身观测
  drop(guard);
  let bytes = index.to_bytes();
  for i in 0..count {
    let element = i.to_le_bytes();
    let values = f32_bytes(&[i as f32 * 0.02, 1.0 + (i % 7) as f32]);
    let mut args = VectorAddArgs::new(&element, VectorValueType::FP32, &values, b"");
    args.quant_type = VectorQuantType::Bin;
    let res = mgr
      .try_add(SessionPrefixBuf::ROOT.as_slice(), key, &bytes, &args)
      .await;
    assert_eq!(res, Ok(VectorManagerResult::OK), "预插元素 {i} 失败");
  }
  index
}

/// 走生产 worker 处理体消费建表调度（建集与 VADD 两路自投的建表项），取回其
/// 派发的回填分片项；重复建表依约幂等（已量化项返回 false 不再派发）。
async fn schedule_backfill(mgr: &VectorManager<GateStore>, key: &[u8]) -> QuantizationState {
  let rk = registry_key(SessionPrefixBuf::ROOT.as_slice(), key);
  let mut builds = 0usize;
  while let Some(state) = mgr.quantization_channel.try_pop() {
    if state.step == QuantizationStep::BackfillQuantizedVectors {
      assert!(builds >= 1, "回填项必由建表项派发");
      assert_eq!(
        mgr.quantization_requests_processed.load(Ordering::Relaxed),
        1,
        "训练样本达标后建表仅一次成功（重复建表项幂等）"
      );
      return state;
    }
    assert_eq!(
      state.step,
      QuantizationStep::BuildQuantizationTable,
      "通道待消费项应为建表项"
    );
    assert_eq!(state.key, rk.to_vec(), "建表项键域须为登记表复合键");
    assert!(
      mgr.try_process_quantization_request(&state).await,
      "建表项必须按终态收敛"
    );
    builds += 1;
  }
  panic!("建表项处理完毕应派发回填分片项");
}

/// 主清理链按生产处理体逐环驱动（对标常驻协程 run_request_cleanup_task_loop →
/// run_cleanup_task_loop 的同一执行体），返回是否确有上下文进入物理清扫。
async fn drain_cleanup(mgr: &VectorManager<GateStore>) -> bool {
  let Some(context) = mgr.request_cleanup_task_channel.try_pop() else {
    return false;
  };
  mgr.process_request_cleanup(context).await;
  let queued = mgr.cleanup_task_channel.try_pop();
  assert_eq!(queued, Some(context), "请求清理必须投递主清理通道");
  mgr.process_cleanup(context).await;
  true
}

/// 危害一：回填在途并发 DEL——条带独占锁必须被量化项的共享守卫挡到本项返回，
/// purge 快照取自全部回填写落盘之后，终态存储零残留（无孤儿量化记录）。
#[compio::test]
async fn delete_waits_for_backfill_and_leaves_no_orphan() {
  let (_dir, _kv, _bound) = bound_domain();
  let store = Arc::new(GateStore::new());
  let mgr = manager(Arc::clone(&store));
  let key = b"quant-backfill-del";

  let index = seed_bin(&mgr, key, ELEMENTS).await;
  let context = index.context;
  let backfill = schedule_backfill(&mgr, key).await;

  // 回填项在独立协程中执行（生产 worker 的一次处理项形态）
  let b_mgr = Arc::clone(&mgr);
  let backfill_task = spawn(async move { b_mgr.try_process_quantization_request(&backfill).await });

  // 确定性放行：等到首条回填量化向量落盘（回填项确已在途且已过锁协议读阶段）
  while !store.backfill_in_flight.load(Ordering::Acquire) {
    yield_now().await;
  }
  assert!(
    store.backfill_writes.load(Ordering::Relaxed) < ELEMENTS as usize,
    "前置：放行时回填必须尚在进行"
  );

  // 同键 DEL：修复后条带独占锁排在共享守卫之后，删除链整体后移至回填返回
  assert!(
    mgr
      .delete_vector_set(SessionPrefixBuf::ROOT.as_slice(), key)
      .await,
    "DEL 必须摘除登记"
  );
  assert!(
    drain_cleanup(&mgr).await,
    "删除必须投递请求清理通道并完成物理清扫"
  );
  assert!(
    store.purge_snapshot_taken.load(Ordering::Acquire),
    "前置：本测必须跑到物理清扫"
  );

  // 危害面直断：purge 快照之后不得再有量化轨写（快照收集不含这些键 → 永久孤儿）
  assert_eq!(
    store.writes_after_snapshot.load(Ordering::Relaxed),
    0,
    "量化项持锁全程外泄的写点数（孤儿量化记录）"
  );
  assert!(
    backfill_task.await.expect("回填项不得 panic"),
    "回填项必须按终态收敛"
  );
  assert_eq!(
    mgr.quantization_backfills_processed.load(Ordering::Relaxed),
    1,
    "回填项照常收敛一次"
  );
  assert_eq!(
    store.records_of(context),
    0,
    "清扫后该 context 基址必须零残留（存储泄漏面）"
  );
}

/// 危害二：context 低位复用——归还的同号上下文被新集合取用后，旧回填不得越
/// 屏障写穿新集合的量化轨（同 iid 被旧量化器产物覆盖 = 检索错距）。
#[compio::test]
async fn reused_context_is_never_written_through_by_stale_backfill() {
  let (_dir, _kv, _bound) = bound_domain();
  let store = Arc::new(GateStore::new());
  let mgr = manager(Arc::clone(&store));
  let old_key = b"quant-backfill-old";

  let index = seed_bin(&mgr, old_key, ELEMENTS).await;
  let context = index.context;
  let backfill = schedule_backfill(&mgr, old_key).await;

  let b_mgr = Arc::clone(&mgr);
  let backfill_task = spawn(async move { b_mgr.try_process_quantization_request(&backfill).await });
  while !store.backfill_in_flight.load(Ordering::Acquire) {
    yield_now().await;
  }

  assert!(
    mgr
      .delete_vector_set(SessionPrefixBuf::ROOT.as_slice(), old_key)
      .await,
    "DEL 必须摘除登记"
  );
  assert!(
    drain_cleanup(&mgr).await,
    "删除链必须完成请求清理与物理清扫"
  );

  // 低位优先复用：归还的槽位必被新集合取为同号上下文
  let new_key = b"quant-backfill-new";
  let new_index = seed_bin(&mgr, new_key, REUSE_ELEMENTS).await;
  assert_eq!(
    new_index.context, context,
    "前置：本测必须跑到上下文低位复用同号场景"
  );

  // 旧回填收尾（修复后其全部写点必在 DEL 取得独占锁之前完成）
  assert!(
    backfill_task.await.expect("回填项不得 panic"),
    "回填项必须按终态收敛"
  );
  assert_eq!(
    store.writes_after_snapshot.load(Ordering::Relaxed),
    0,
    "清扫后仍落盘的量化轨写数"
  );
  assert_eq!(
    store.quant_records_of(context),
    0,
    "新集合量化轨被旧回填写穿的记录数（Term::Quantized 与量化器状态 _qnt）"
  );
}
