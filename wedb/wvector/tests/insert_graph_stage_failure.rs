//! VADD 图阶段失败回滚与错误信号回归（票：zcode-r27-vectordiskann 发现四）
//!
//! 缺陷形态：上游 diskann insert 先 set_element（id 分配 + Vector/Quantized/
//! ExtMap/IntMap 四记录落盘 + fsm 占用）后执行图搜索/剪枝/邻接写，其后任一
//! 存储错误使包装层返回 Err；service.insert 原把该 Err 一律折为
//! DiskAnnInsertResult::False——元素四记录与 fsm 占用位已持久，VADD 应答
//! 「已存在」，重试恒 Duplicate（external_id_exists 命中 IntMap）、存储故障
//! 信号被吞，客户端按重复键语义重试永不收敛。
//!
//! 修复契约（与属性写失败臂同机制）：图阶段失败（记录已持久，exists 为真）
//! 先尽力 remove 回滚摘除收敛回「未插入」再报
//! [`DiskAnnInsertResult::StoreError`]；set_element 自身失败已由 provider
//! 失败出口逆序清道（exists 为假，存储无残留），维持插入期拒绝语义（False，
//! 由 set_element_rollback.rs 锁定）。
//!
//! 注入面：桥按 armed 项类型对 **rmw** 落盘口返回 false——图阶段的邻接写
//! （set_neighbors/append_vector）走 rmw，而 set_element 四步全走 direct
//! write，故 Neighbors 域 rmw 故障恰好只斩断图阶段。

use std::sync::{
  Arc,
  atomic::{AtomicU64, Ordering},
};

use parking_lot::Mutex;
use wbase::map::HashMap;
use wvector::{
  Callbacks, DiskANNService, DiskAnnInsertResult, IndexConfig, StoreCallbacks,
  VectorDistanceMetricType, VectorQuantType,
  store::{TERM_BITMASK, Term},
};

const CTX: u64 = 8;
/// keeper 元素内部 id（起点 0 后首铸）。
const IID_KEEP: u32 = 1;
/// ghost 元素内部 id（LIFO 序下一枚新铸）。
const IID_GHOST: u32 = 2;
/// 解除注入哨兵。
const DISARMED: u64 = u64::MAX;

type StoreMap = HashMap<(u64, Vec<u8>), Vec<u8>>;

/// rmw 落盘口故障内存存储（其余读写删全通）。
struct FaultyRmwStore {
  data: Mutex<StoreMap>,
  fail_term: AtomicU64,
}

impl FaultyRmwStore {
  fn new() -> Self {
    Self {
      data: Mutex::new(HashMap::default()),
      fail_term: AtomicU64::new(DISARMED),
    }
  }

  fn arm(&self, kind: Term) {
    self.fail_term.store(kind as u64, Ordering::Release);
  }

  fn disarm(&self) {
    self.fail_term.store(DISARMED, Ordering::Release);
  }

  fn peek(&self, kind: Term, key: &[u8]) -> Option<Vec<u8>> {
    self
      .data
      .lock()
      .get(&(CTX | kind as u64, key.to_vec()))
      .cloned()
  }
}

impl StoreCallbacks for FaultyRmwStore {
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
      let key = &rest[4..total];
      if let Some(value) = guard.get(&(context, key.to_vec())) {
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
    true
  }

  async fn delete(&self, context: u64, key: &[u8]) -> bool {
    self.data.lock().remove(&(context, key.to_vec())).is_some()
  }

  async fn rmw<F>(&self, context: u64, key: &[u8], write_len: usize, mut f: F) -> bool
  where
    F: FnMut(&mut [u8]) + Send,
  {
    let fail = self.fail_term.load(Ordering::Acquire);
    if fail != DISARMED && context & TERM_BITMASK == fail {
      return false;
    }
    // 纯读短路（对齐生产回调）：不建不写
    if write_len == 0 {
      return true;
    }
    // 对齐生产内核口径（wnode WedbVectorStoreCallbacks::rmw）：write_len 即
    // 目标记录尺寸，旧值截短/补零后闭包改写、整值写回
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
    false
  }

  async fn purge_context(&self, context: u64) -> bool {
    self
      .data
      .lock()
      .retain(|&(ctx, _), _| ctx & !TERM_BITMASK != context);
    true
  }

  fn log(&self, _context: u64, _msg: &str) {}
}

fn f32_bytes(vals: &[f32]) -> Vec<u8> {
  vals.iter().flat_map(|v| v.to_le_bytes()).collect()
}

fn config() -> IndexConfig {
  IndexConfig {
    dims: 2,
    reduce_dims: 0,
    quant_type: VectorQuantType::NoQuant,
    distance_metric: VectorDistanceMetricType::L2,
    build_exploration_factor: 64,
    num_links: 8,
  }
}

/// 图阶段失败主靶：Neighbors 域 rmw 故障斩断图邻接写，插入应答存储错误
/// （非 False/Duplicate），回滚摘除后存储收敛「未插入」，重试可成功。
#[compio::test]
async fn graph_stage_failure_rolls_back_and_reports_store_error() {
  let store = Arc::new(FaultyRmwStore::new());
  let service = DiskANNService::default();
  assert_eq!(
    service
      .create_index(CTX, config(), Callbacks::new(Arc::clone(&store)))
      .await,
    Ok(false)
  );
  // keeper 正常插入（起点 0 + keeper 1，含图邻接 rmw）
  assert_eq!(
    service
      .insert(CTX, b"k1", &f32_bytes(&[1.0, 0.0]), b"")
      .await,
    DiskAnnInsertResult::True
  );

  // 注入 Neighbors 域 rmw 故障：set_element 四步（direct write）不受影响，
  // 图阶段邻接写（rmw）失败 → diskann insert Err
  store.arm(Term::Neighbors);
  assert_eq!(
    service
      .insert(CTX, b"g1", &f32_bytes(&[0.0, 1.0]), b"")
      .await,
    DiskAnnInsertResult::StoreError,
    "图阶段失败严禁折 False 误报 Duplicate"
  );

  // 回滚摘除后存储收敛「未插入」：四记录与 fsm 占用位均无残留
  let ghost = IID_GHOST.to_le_bytes();
  assert!(store.peek(Term::Vector, &ghost).is_none(), "向量残留");
  assert!(store.peek(Term::ExtMap, &ghost).is_none(), "ExtMap 残留");
  assert!(store.peek(Term::IntMap, b"g1").is_none(), "IntMap 残留");
  assert!(
    !service.check_internal_id_valid(CTX, IID_GHOST).await,
    "失败槽位应已归还"
  );
  assert!(
    !service.check_external_id_valid(CTX, b"g1").await,
    "回滚后不得按 eid 存在（否则重试恒 Duplicate）"
  );
  assert_eq!(service.card(CTX), 1, "计数不得含失败元素");
  // keeper 不受波及
  assert!(store.peek(Term::Vector, &IID_KEEP.to_le_bytes()).is_some());

  // 解除故障重试：LIFO 复用失败槽位，插入成功
  store.disarm();
  assert_eq!(
    service
      .insert(CTX, b"g2", &f32_bytes(&[1.0, 1.0]), b"")
      .await,
    DiskAnnInsertResult::True
  );
  assert_eq!(
    service.internal_id_of(CTX, b"g2").await,
    Some(IID_GHOST),
    "重试应复用回滚归还的槽位"
  );
  assert!(!service.check_external_id_valid(CTX, b"g1").await);
}
