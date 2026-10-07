//! wvector 集成测试共用桥 (`wvector_test`)
//!
//! 内存桥接存储 [`MemStore`]：[`wvector::StoreCallbacks`] 的纯内存实现，
//! 各臂对齐生产回调口径（`wnode::WedbVectorStoreCallbacks`）——rmw 按
//! write_len 截短/补零整值写回、read_multi 按 `[4B LE 长度][键]` 对串解包、
//! purge_context 按 Term 位域清命名空间。收口自 wvector/wnode 向量测试的
//! 逐字同形副本，测试侧经辅助口（`len_of`/`peek`/`poke`）或直曝字段
//! `data` 检视、注入落盘字节。
//!
//! 注入面：[`MemStore::arm`] 按「注臂 × 项类型位域」对 read/write/rmw 单臂
//! 注障（AtomicU64 注入槽 + arm/disarm 形态，跨任务可见性对齐 wbftree
//! `SCAN_FAIL_INJECT` 门控纪律）；失败臂 `log` 留痕全量捕获，经
//! [`MemStore::logs_contain`] 对账。

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

use parking_lot::Mutex;
use wbase::map::HashMap;
use wvector::{
  DiskANNService, DiskAnnInsertResult, IndexConfig, StoreCallbacks, VectorDistanceMetricType,
  VectorQuantType,
  store::{TERM_BITMASK, Term},
};

/// f32 切片 → 小端字节串（向量落盘载荷组帧单源：`[4B LE]…`）
///
/// 收口自 wvector/tests 13 份逐字节同形私义 `f32_bytes` 副本
#[must_use]
pub fn f32_bytes(vals: &[f32]) -> Vec<u8> {
  vals.iter().flat_map(|v| v.to_le_bytes()).collect()
}

/// 铸训练集合：`context` 上插 `count` 条用户向量（eid `e{i:0>6}`、坐标网格
/// `(i % 32, i / 32) * 0.5`），逐条断言 True / QuantizationRequested。
///
/// 收口自 wvector/tests 三份逐字同形插入断言循环（量化族 seed 主体）
pub async fn seed_elements<S: StoreCallbacks>(
  service: &DiskANNService<S>,
  context: u64,
  count: usize,
) {
  for i in 0..count {
    let x = (i % 32) as f32 * 0.5;
    let y = (i / 32) as f32 * 0.5;
    let res = service
      .insert(
        context,
        format!("e{i:0>6}").as_bytes(),
        &f32_bytes(&[x, y]),
        b"",
      )
      .await;
    assert!(
      matches!(
        res,
        DiskAnnInsertResult::True | DiskAnnInsertResult::QuantizationRequested
      ),
      "第 {i} 条插入失败: {res:?}"
    );
  }
}

/// 默认调参索引配置单源（dims=2 / NoQuant / L2 / bef=64 / num_links 缺省；
/// 收口自 wvector/tests 检索失败族 5 册同形 `config`；量化变体经 `quant`
/// 参数切换）
#[must_use]
pub fn test_config(quant: VectorQuantType) -> IndexConfig {
  IndexConfig {
    dims: 2,
    reduce_dims: 0,
    quant_type: quant,
    distance_metric: VectorDistanceMetricType::L2,
    build_exploration_factor: 64,
    num_links: 8,
  }
}

/// 解除注入哨兵（armed 编码占用 bit3+，任何项类型位域都取不到）。
const DISARMED: u64 = u64::MAX;

/// 故障注臂（StoreCallbacks 单臂选择：read / write / rmw 落盘口）。
#[repr(u64)]
pub enum FaultArm {
  /// 读口（read 臂）。
  Read = 1,
  /// 写口（write 臂）。
  Write = 2,
  /// 读改写口（rmw 臂）。
  Rmw = 3,
}

/// 存储映射：`(context, key) → 值`（crate 内部字段类型，零外部消费）。
type MemStoreMap = HashMap<(u64, Vec<u8>), Vec<u8>>;

/// 内存桥接存储（(context, key) → 值），测试侧直接检视各 Term 落盘字节。
pub struct MemStore {
  pub data: Mutex<MemStoreMap>,
  /// filter 回调放行开关（默认 false 全拒；内联过滤检索臂的注障册置真
  /// 放行，保过滤臂推进到二次展开）。
  pub filter_pass: AtomicBool,
  /// 故障注入槽：armed 编码 = `注臂 << 3 | 项类型位域`，[`DISARMED`] 解除。
  fault: AtomicU64,
  /// 失败臂 log 留痕捕获（`callbacks.log` 上抛原文，按序追加）。
  logs: Mutex<Vec<String>>,
}

/// 项类型命名空间合成：`base` 低位并入 Term 位域（base 恒项类型位对齐）。
#[inline]
fn slot(base: u64, kind: Term) -> u64 {
  base | (kind as u64 & TERM_BITMASK)
}

impl MemStore {
  pub fn new() -> Self {
    Self {
      data: Mutex::new(HashMap::default()),
      filter_pass: AtomicBool::new(false),
      fault: AtomicU64::new(DISARMED),
      logs: Mutex::new(Vec::new()),
    }
  }

  /// 装载注障：`term` 域的 `arm` 落盘口返回 false（其余读写删全通）。
  pub fn arm(&self, arm: FaultArm, term: Term) {
    self
      .fault
      .store(term as u64 | (arm as u64) << 3, Ordering::Release);
  }

  /// 解除注障。
  pub fn disarm(&self) {
    self.fault.store(DISARMED, Ordering::Release);
  }

  /// 注入命中判定：armed 且注臂与项类型位域双匹配。
  #[inline]
  fn fault_hit(&self, arm: FaultArm, context: u64) -> bool {
    let slot = self.fault.load(Ordering::Acquire);
    slot != DISARMED && slot >> 3 == arm as u64 && context & TERM_BITMASK == slot & TERM_BITMASK
  }

  /// log 留痕是否含 `needle`（失败臂上抛对账口）。
  pub fn logs_contain(&self, needle: &str) -> bool {
    self.logs.lock().iter().any(|line| line.contains(needle))
  }

  /// 指定项类型命名空间下内部 id 记录的字节数（缺失 → None）。
  pub fn len_of(&self, base: u64, term: Term, iid: u32) -> Option<usize> {
    self
      .data
      .lock()
      .get(&(slot(base, term), iid.to_le_bytes().to_vec()))
      .map(Vec::len)
  }

  /// 直读落盘条目（存储侧观测口）。
  pub fn peek(&self, base: u64, kind: Term, key: &[u8]) -> Option<Vec<u8>> {
    self
      .data
      .lock()
      .get(&(slot(base, kind), key.to_vec()))
      .cloned()
  }

  /// 直写落盘条目（异长记录注入口）。
  pub fn poke(&self, base: u64, kind: Term, key: &[u8], value: Vec<u8>) {
    self
      .data
      .lock()
      .insert((slot(base, kind), key.to_vec()), value);
  }
}

impl Default for MemStore {
  fn default() -> Self {
    Self::new()
  }
}

impl StoreCallbacks for MemStore {
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
    if self.fault_hit(FaultArm::Read, context) {
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
    if self.fault_hit(FaultArm::Write, context) {
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
    if self.fault_hit(FaultArm::Rmw, context) {
      return false;
    }
    // 纯读短路（对齐生产回调）：不建不写
    if write_len == 0 {
      return true;
    }
    // 对齐生产内核口径（wnode WedbVectorStoreCallbacks::rmw）：write_len 即
    // 目标记录尺寸，旧值截短/补零后闭包改写、整值写回——桥若保留旧记录
    // 全长，rmw 缩记录类缺陷对本桥不可见
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
    self.filter_pass.load(Ordering::Acquire)
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

/// 真内存 KV 存储桩（基座 [`MemStore`] 全臂转发 + filter 恒放行钩子；收口自
/// wnode/tests 向量锁竞速族 4 册逐字同形私义副本——delete/rmw 语义与基座
/// 一致，filter 臂默认放行供内联过滤检索推进）
pub struct MemKvStore {
  /// 基座直曝（测试侧经 [`MemStore`] 字段/辅助口检视与注入落盘字节）
  pub base: MemStore,
}

impl MemKvStore {
  /// 建桩（基座空表）
  pub fn new() -> Self {
    Self {
      base: MemStore::new(),
    }
  }
}

impl Default for MemKvStore {
  fn default() -> Self {
    Self::new()
  }
}

impl StoreCallbacks for MemKvStore {
  async fn filter(&self, _context: u64, _internal_id: u32) -> bool {
    true
  }

  async fn read_multi<F>(&self, context: u64, keys: &[u8], length_hint: usize, f: F) -> bool
  where
    F: FnMut(u32, &[u8]) + Send,
  {
    self.base.read_multi(context, keys, length_hint, f).await
  }

  async fn read<F>(&self, context: u64, key: &[u8], f: F) -> bool
  where
    F: FnMut(&[u8]) + Send,
  {
    self.base.read(context, key, f).await
  }

  async fn write(&self, context: u64, key: &[u8], value: &[u8]) -> bool {
    self.base.write(context, key, value).await
  }

  async fn delete(&self, context: u64, key: &[u8]) -> bool {
    self.base.delete(context, key).await
  }

  async fn rmw<F>(&self, context: u64, key: &[u8], write_len: usize, f: F) -> bool
  where
    F: FnMut(&mut [u8]) + Send,
  {
    self.base.rmw(context, key, write_len, f).await
  }

  async fn purge_context(&self, context: u64) -> bool {
    self.base.purge_context(context).await
  }

  fn log(&self, context: u64, msg: &str) {
    self.base.log(context, msg);
  }
}
