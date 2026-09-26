//! 训练锁全序与回填收尾自愈回归（票：zcode-r123c-vechain1 案二 P3）
//!
//! 缺陷一：train_quantizer 把 `_qnt` 落盘移出 training_lock 临界段后，
//! 「持久化→启用→回填调度」全序被重排成「启用可先于持久化达」——并发
//! 早退者可在落盘未达时补齐屏障并对调度器返 true 派发分片；缺陷二：
//! 回填收尾 `==task_count` 恰等判据一旦被弃跑轮次或重复建表项的越界计数
//! 错过即永不复等、流水线永久停摆，且末片读缺 `_qnt` 裸 return false 零
//! 日志不可观测（原生 provider.rs:backfill_quant_vectors 各失败臂均带
//! 「Index will operate full precision only mode」log，移植中丢失）。
//!
//! 修复契约：`_qnt` 落盘与 enable_quantization 收回 training_lock 锁段
//! （async_lock 守卫 Send 可跨 await）；早退臂仅在本实例确观测到落盘后的
//! is_trained（重启崩溃窗形态，`_qnt` 已在盘）时补屏障并返 true，其余
//! （他实例锁在途、本进程落盘失败、上界已收口）返 false 按弃由首训者
//! 派发；收尾判据 `>= task_count` 化，缺 `_qnt`/写失败两臂补 log 上抛
//! 留痕，允许后续轮次重翻。
//!
//! 注入面：内存桥接存储（quant_backfill_restart.rs 同型 MemStore 桥），
//! 定向丢弃/失败 `_qnt` 读写并捕获 log 通道。

use std::sync::{
  Arc,
  atomic::{AtomicBool, Ordering},
};

use parking_lot::Mutex;
use wbase::map::HashMap;
use wvector::{
  Callbacks, DiskANNService, DiskAnnInsertResult, IndexConfig, StoreCallbacks,
  VectorDistanceMetricType, VectorQuantType,
  store::{TERM_BITMASK, Term},
};

const CTX: u64 = 8;

/// 量化状态键（`_qnt`，与 dynamic_quant 的 QUANT_STATE_KEY 同值）。
const QUANT_STATE_KEY: u32 = u32::from_be_bytes(*b"_qnt");

/// 训练样本门槛（Spherical1Bit::required_vectors 恒 1000）。
const TRAIN_ROWS: usize = 1000;
const ELEMENTS: usize = TRAIN_ROWS + 1;

type StoreMap = HashMap<(u64, Vec<u8>), Vec<u8>>;

/// 内存桥接存储：`_qnt` 读写故障注入 + log 捕获。
struct FaultStore {
  data: Mutex<StoreMap>,
  logs: Mutex<Vec<String>>,
  /// 置位时丢弃 `_qnt` 状态记录写（模拟落盘失败/未达）。
  fail_qnt_write: AtomicBool,
  /// 置位时 `_qnt` 状态记录读报缺（模拟收尾读缺）。
  drop_qnt_read: AtomicBool,
}

impl FaultStore {
  fn new() -> Self {
    Self {
      data: Mutex::new(HashMap::default()),
      logs: Mutex::new(Vec::new()),
      fail_qnt_write: AtomicBool::new(false),
      drop_qnt_read: AtomicBool::new(false),
    }
  }

  fn peek(&self, kind: Term, key: &[u8]) -> Option<Vec<u8>> {
    self
      .data
      .lock()
      .get(&(CTX | kind as u64, key.to_vec()))
      .cloned()
  }

  fn qnt_flag(&self) -> Option<u8> {
    self
      .peek(Term::Metadata, &QUANT_STATE_KEY.to_le_bytes())
      .map(|state| *state.first().unwrap_or(&0xFF))
  }

  fn logs_contain(&self, needle: &str) -> bool {
    self.logs.lock().iter().any(|line| line.contains(needle))
  }
}

fn is_qnt_key(context: u64, key: &[u8]) -> bool {
  context == (CTX | Term::Metadata as u64) && key == QUANT_STATE_KEY.to_le_bytes()
}

impl StoreCallbacks for FaultStore {
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
    if self.drop_qnt_read.load(Ordering::Acquire) && is_qnt_key(context, key) {
      return false;
    }
    match self.data.lock().get(&(context, key.to_vec())) {
      Some(value) => {
        f(value);
        true
      }
      None => false,
    }
  }

  async fn write(&self, context: u64, key: &[u8], value: &[u8]) -> bool {
    if self.fail_qnt_write.load(Ordering::Acquire) && is_qnt_key(context, key) {
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

  fn log(&self, _context: u64, msg: &str) {
    self.logs.lock().push(msg.to_string());
  }
}

fn f32_bytes(vals: &[f32]) -> Vec<u8> {
  vals.iter().flat_map(|v| v.to_le_bytes()).collect()
}

fn config() -> IndexConfig {
  IndexConfig {
    dims: 2,
    reduce_dims: 0,
    quant_type: VectorQuantType::Bin,
    distance_metric: VectorDistanceMetricType::L2,
    build_exploration_factor: 64,
    num_links: 8,
  }
}

/// 铸 Bin 集合：插满 ELEMENTS 条用户向量（iid 1..=ELEMENTS）。
async fn seed(store: &Arc<FaultStore>) -> DiskANNService<FaultStore> {
  let service = DiskANNService::default();
  assert_eq!(
    service
      .create_index(CTX, config(), Callbacks::new(Arc::clone(store)))
      .await,
    Ok(false)
  );
  for i in 0..ELEMENTS {
    let x = (i % 32) as f32 * 0.5;
    let y = (i / 32) as f32 * 0.5;
    let id = format!("e{i:0>6}");
    let res = service
      .insert(CTX, id.as_bytes(), &f32_bytes(&[x, y]), b"")
      .await;
    assert!(
      matches!(
        res,
        DiskAnnInsertResult::True | DiskAnnInsertResult::QuantizationRequested
      ),
      "第 {i} 条插入失败: {res:?}"
    );
  }
  service
}

/// 早退臂不重派发：首训成功（锁段内落盘 + 收口并返 true 派发）后，重复
/// 建表项经早退臂返 false 按弃（对标原生「已训练臂直返 false，不触发回填
/// 调度」），杜绝双轮分片叠加。
#[compio::test]
async fn duplicate_table_build_after_success_does_not_redispatch() {
  let store = Arc::new(FaultStore::new());
  let service = seed(&store).await;

  assert!(
    service.build_quantization_table(CTX).await,
    "首训应成功派发"
  );
  assert_eq!(store.qnt_flag(), Some(0), "建表后完成标志应为 0");

  // 旧码：早退臂走共享尾恒返 true → 调度器再派 N 分片，计数越界即触发
  // 恰等停摆；新码：上界已收口 → false 按弃
  assert!(
    !service.build_quantization_table(CTX).await,
    "已收口实例的重复建表项不得再派发回填"
  );
}

/// 收尾自愈主靶：末片读缺 `_qnt` 弃跑留痕（log 上抛），恢复通道经
/// `>= task_count` 判据重翻——重投分片最终置位 `_qnt` 完成标志并启用
/// 量化轨；旧码恰等判据下计数已耗尽、永不复等，永久停摆于全精度且静默。
#[compio::test]
async fn backfill_finish_recovers_after_qnt_read_fault() {
  let store = Arc::new(FaultStore::new());
  let service = seed(&store).await;
  assert!(service.build_quantization_table(CTX).await);

  // 注入：收尾读 `_qnt` 报缺 → 弃跑
  store.drop_qnt_read.store(true, Ordering::Release);
  service.backfill_quantized_vectors(CTX, 0, 1).await;
  assert_eq!(store.qnt_flag(), Some(0), "缺 _qnt 弃跑轮不得翻标志");
  assert!(
    store.logs_contain("Quantizer state not found"),
    "末片读缺 _qnt 弃跑必须 log 留痕（对账原生失败臂日志，杜绝静默停摆）"
  );

  // 撤障重投：>= 判据下越线轮次仍可重翻收尾（幂等自愈）
  store.drop_qnt_read.store(false, Ordering::Release);
  service.backfill_quantized_vectors(CTX, 0, 1).await;
  assert_eq!(
    store.qnt_flag(),
    Some(1),
    "重投分片应越线翻标志：>= 化后收尾弃跑不再是永久停摆点"
  );

  // 量化轨已生效：全量点读在案 + id 复用启用（删后重插复用同槽）
  let victim_iid = ELEMENTS as u32 / 3 + 1;
  let victim = format!("e{:0>6}", victim_iid - 1);
  assert!(service.remove(CTX, victim.as_bytes()).await);
  assert_eq!(
    service
      .insert(CTX, b"reborn", &f32_bytes(&[3.0, 9.0]), b"")
      .await,
    DiskAnnInsertResult::True
  );
  assert_eq!(
    service.internal_id_of(CTX, b"reborn").await,
    Some(victim_iid),
    "收尾自愈后 id 复用应生效"
  );
}

/// `_qnt` 落盘失败按弃：首训写失败 → false（不落盘不启用不派发）；后续
/// 建表项早退臂观测到 `_qnt` 不在盘 → false，不再在 `_qnt` 未达时补屏障
/// 派发分片；上界未收口期回填守卫臂 log 留痕（对账原生守卫失败日志）。
#[compio::test]
async fn qnt_write_failure_never_dispatches_backfill() {
  let store = Arc::new(FaultStore::new());
  let service = seed(&store).await;

  // 注入：`_qnt` 写失败
  store.fail_qnt_write.store(true, Ordering::Release);
  assert!(
    !service.build_quantization_table(CTX).await,
    "落盘失败不得返 true 派发（全序「持久化→启用→调度」首段未达即按弃）"
  );
  assert!(
    store
      .peek(Term::Metadata, &QUANT_STATE_KEY.to_le_bytes())
      .is_none(),
    "落盘失败后 _qnt 不得在盘"
  );
  store.fail_qnt_write.store(false, Ordering::Release);

  // 量化器已在内存训成而 `_qnt` 不在盘：早退臂不得补屏障派发（旧码：
  // 上界恒 u32::MAX 即 enable + 返 true，派发出去的回填全在守卫臂空转）
  assert!(
    !service.build_quantization_table(CTX).await,
    "_qnt 未落盘时早退臂不得派发分片"
  );

  // 上界未收口期分片入场的守卫臂必须 log 留痕而非静默弃跑
  service.backfill_quantized_vectors(CTX, 0, 1).await;
  assert!(
    store.logs_contain("Couldn't calculate max id"),
    "上界未发布期回填守卫臂应 log（原生 :631-646 失败臂日志对标）"
  );
  assert!(
    store
      .peek(Term::Metadata, &QUANT_STATE_KEY.to_le_bytes())
      .is_none(),
    "守卫弃跑轮不得产出 _qnt 状态"
  );
}
