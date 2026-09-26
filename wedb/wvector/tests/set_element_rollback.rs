//! set_element 分步写失败回滚回归（票：wvector-set-element-partial-term-write-residue-on-failure）
//!
//! 缺陷形态：provider `set_element` 四步序贯写（Vector → Quantized → ExtMap →
//! IntMap）任一失败仅 `mark_free` 归还槽位，已写项一概不清；(d) 档（IntMap 失败）
//! ExtMap 残留被 `random_members`（VRANDMEMBER 后端，对 `[0, max_internal_id]`
//! 均匀采样不过滤 `is_free`）投影成幽灵外部 id。
//! 修复契约（对标 C# `garnet/libs/server/Resp/Vector/DiskANNService.cs:Insert`
//! 单次原生调用失败即 managed 无残留）：失败出口按已写档位单次逆序回滚，
//! Vector/Quantized/ExtMap 按 internal_id 与 IntMap 按 eid 全部不可见，
//! fsm 计数据实归位，id 复用覆写与跨 load_state 重建后残留均不回灌。
//!
//! 注入面：`FaultyStore` 按 `context & TERM_BITMASK == fail_term` 在写落盘口
//! 返回 false（生产 `WedbVectorStoreCallbacks::write` IO 失败同形返回）。

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

/// 集合上下文基址（低 3 位保留给项类型）。
const CTX: u64 = 8;
/// 起点内部 id。
const IID_START: u32 = 0;
/// keeper 元素内部 id（起点占 0 后首个新铸）。
const IID_KEEP: u32 = 1;
/// 失败元素内部 id（LIFO 序下一枚新铸）。
const IID_GHOST: u32 = 2;
/// 解除注入哨兵（任何项类型位域都不可能取到）。
const DISARMED: u64 = u64::MAX;

type StoreMap = HashMap<(u64, Vec<u8>), Vec<u8>>;

/// 档位写故障内存存储：`fail_term` armed 时对匹配项类型的写落盘口返回 false。
struct FaultyStore {
  data: Mutex<StoreMap>,
  fail_term: AtomicU64,
}

impl FaultyStore {
  fn new() -> Self {
    Self {
      data: Mutex::new(HashMap::default()),
      fail_term: AtomicU64::new(DISARMED),
    }
  }

  /// 装载指定项类型的写故障。
  fn arm(&self, kind: Term) {
    self.fail_term.store(kind as u64, Ordering::Release);
  }

  /// 解除写故障。
  fn disarm(&self) {
    self.fail_term.store(DISARMED, Ordering::Release);
  }

  /// 直读落盘条目（存储侧观测口）。
  fn peek(&self, kind: Term, key: &[u8]) -> Option<Vec<u8>> {
    self
      .data
      .lock()
      .get(&(CTX | kind as u64, key.to_vec()))
      .cloned()
  }

  fn iid_key(id: u32) -> Vec<u8> {
    id.to_le_bytes().to_vec()
  }
}

impl StoreCallbacks for FaultyStore {
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
    let fail = self.fail_term.load(Ordering::Acquire);
    if fail != DISARMED && context & TERM_BITMASK == fail {
      return false;
    }
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

/// 装配：keeper `k1` 正常插入（占起点 0 + 元素 1）→ 注入档位故障 → ghost `g1`
/// 插入失败（ghost 铸 id 2）。返回存储与服务句柄供逐档断言。
async fn scenario(fail: Term) -> (Arc<FaultyStore>, DiskANNService<FaultyStore>) {
  let store = Arc::new(FaultyStore::new());
  let service = DiskANNService::default();
  assert_eq!(
    service
      .create_index(CTX, config(), Callbacks::new(Arc::clone(&store)))
      .await,
    Ok(false)
  );
  assert_eq!(
    service
      .insert(CTX, b"k1", &f32_bytes(&[1.0, 0.0]), b"")
      .await,
    DiskAnnInsertResult::True
  );
  store.arm(fail);
  // provider set_element 写失败 → diskann insert Err → service.rs:897 折叠 False
  assert_eq!(
    service
      .insert(CTX, b"g1", &f32_bytes(&[0.0, 1.0]), b"")
      .await,
    DiskAnnInsertResult::False
  );
  (store, service)
}

/// 三档写失败的统一不可见断言：Vector/Quantized/ExtMap 按 internal_id、
/// IntMap 按 eid 全部 miss，fsm 计数据实归位，采样面无幽灵。
async fn assert_all_invisible(store: &FaultyStore, service: &DiskANNService<FaultyStore>) {
  assert!(
    store
      .peek(Term::Vector, &FaultyStore::iid_key(IID_GHOST))
      .is_none(),
    "向量本体残留"
  );
  assert!(
    store
      .peek(Term::Quantized, &FaultyStore::iid_key(IID_GHOST))
      .is_none(),
    "量化向量残留"
  );
  assert!(
    store
      .peek(Term::ExtMap, &FaultyStore::iid_key(IID_GHOST))
      .is_none(),
    "ExtMap 残留"
  );
  assert!(store.peek(Term::IntMap, b"g1").is_none(), "IntMap 残留");
  // fsm：槽位空闲且计数不含失败元素（起点 + keeper = 2）
  assert!(
    !service.check_internal_id_valid(CTX, IID_GHOST).await,
    "失败槽位应空闲"
  );
  assert!(
    !service.check_external_id_valid(CTX, b"g1").await,
    "失败元素不应按 eid 存在"
  );
  assert_eq!(service.card(CTX), 1, "计数不得含失败元素");
  let mut sample = service.sample(CTX, 5).await;
  sample.sort();
  assert_eq!(sample, vec![b"k1".to_vec()], "采样面不得出现幽灵 eid");
}

#[compio::test]
async fn full_vector_write_failure_rolls_back() {
  let (store, service) = scenario(Term::Vector).await;
  assert_all_invisible(&store, &service).await;
  // 起点与 keeper 不受波及
  assert!(
    store
      .peek(Term::Vector, &FaultyStore::iid_key(IID_START))
      .is_some()
  );
  assert!(
    store
      .peek(Term::Vector, &FaultyStore::iid_key(IID_KEEP))
      .is_some()
  );
}

#[compio::test]
async fn ext_map_write_failure_rolls_back() {
  let (store, service) = scenario(Term::ExtMap).await;
  assert_all_invisible(&store, &service).await;
}

/// (d) 档主靶：IntMap 失败 ⇒ ExtMap 残留即 VRANDMEMBER 幽灵 eid。
/// 修复后残留尽清；解除故障重试经 LIFO 复用同槽覆写，旧 eid 全面 miss。
#[compio::test]
async fn int_map_write_failure_rolls_back_and_id_reuse_overwrites() {
  let (store, service) = scenario(Term::IntMap).await;
  assert_all_invisible(&store, &service).await;

  store.disarm();
  assert_eq!(
    service
      .insert(CTX, b"g2", &f32_bytes(&[1.0, 1.0]), b"")
      .await,
    DiskAnnInsertResult::True
  );
  // LIFO 复用：mark_free 归还的 IID_GHOST 由 next_id 优先弹出
  assert_eq!(
    service.internal_id_of(CTX, b"g2").await,
    Some(IID_GHOST),
    "重试应复用失败槽位"
  );
  assert_eq!(
    store
      .peek(Term::ExtMap, &FaultyStore::iid_key(IID_GHOST))
      .as_deref(),
    Some(&b"g2"[..])
  );
  assert!(
    store.peek(Term::IntMap, b"g1").is_none(),
    "旧 eid 映射不得复活"
  );
  let mut sample = service.sample(CTX, 5).await;
  sample.sort();
  assert_eq!(sample, vec![b"g2".to_vec(), b"k1".to_vec()]);
}

/// 跨重启回灌：同存储新建服务（`WedbProvider::new` → fsm `load_state`
/// 只从位图重建，不清 ExtMap），残留必须已在失败当场被逆序回滚清道、
/// 不得复活；复用槽位覆写后读侧两面收敛一致。
#[compio::test]
async fn int_map_write_failure_residue_absent_after_state_reload() {
  let (store, _service) = scenario(Term::IntMap).await;
  store.disarm();

  let reloaded = DiskANNService::default();
  assert_eq!(
    reloaded
      .create_index(CTX, config(), Callbacks::new(Arc::clone(&store)))
      .await,
    Ok(false),
    "load_state 重建应成功"
  );
  assert!(
    store
      .peek(Term::ExtMap, &FaultyStore::iid_key(IID_GHOST))
      .is_none(),
    "重启后 ExtMap 残留回灌"
  );
  assert!(
    store
      .peek(Term::Vector, &FaultyStore::iid_key(IID_GHOST))
      .is_none(),
    "重启后向量残留回灌"
  );
  assert_eq!(reloaded.card(CTX), 1);
  assert_eq!(reloaded.internal_id_of(CTX, b"g1").await, None);
  let mut sample = reloaded.sample(CTX, 5).await;
  sample.sort();
  assert_eq!(sample, vec![b"k1".to_vec()], "重启后采样面不得出现幽灵 eid");

  // 重启后新插入复用同槽覆写，全部读面对旧 eid miss
  assert_eq!(
    reloaded
      .insert(CTX, b"g2", &f32_bytes(&[1.0, 1.0]), b"")
      .await,
    DiskAnnInsertResult::True
  );
  assert_eq!(reloaded.internal_id_of(CTX, b"g2").await, Some(IID_GHOST));
  assert!(!reloaded.check_external_id_valid(CTX, b"g1").await);
  let mut sample = reloaded.sample(CTX, 5).await;
  sample.sort();
  assert_eq!(sample, vec![b"g2".to_vec(), b"k1".to_vec()]);
}
