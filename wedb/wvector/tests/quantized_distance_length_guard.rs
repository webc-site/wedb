//! 批量读回调值长度守卫回归（票：wvector-quantized-distance-length-guard）
//!
//! 五处批量读回调（expand_beam / expand_beam_filtered / expand_beam_accept_only /
//! PruneAccessor::fill / Rerank）统一 `v.len() != length_hint` 内联判定：异长
//! 值（磁盘静默损坏、异源写入）跳过该项并 log::warn 留痕，不再把异长字节喂
//! 进距离核（Spherical unwrap panic 链 / 全精度 unaligned_view 截断错距链）。
//!
//! 注入面：内存桥接存储手工 write_iid 短/超长字节（Term::Quantized /
//! Term::Vector 域），三型各一例——Q8（量化 beam 臂 + 短/长双注入）、
//! Bin（全量化态 Rerank 重排臂）、FP32 NoQuant（全精度 beam 臂，超长注入
//! 对位旧截断错距面）。

use std::sync::{
  Arc, Once,
  atomic::{AtomicUsize, Ordering},
};

use compio::runtime::Runtime;
use log::{Level, LevelFilter};
use wvector::{
  Callbacks, DiskANNService, DiskAnnInsertResult, IndexConfig, SearchParams,
  VectorDistanceMetricType, VectorQuantType, store::Term,
};
use wvector_test::MemStore;

const CTX: u64 = 8;

/// 训练样本门槛（Spherical1Bit::required_vectors 恒 1000）。
const TRAIN_ROWS: usize = 1000;

/// warn 级日志计数器（跳过留痕断言口）。
static WARN_HITS: AtomicUsize = AtomicUsize::new(0);

struct WarnCounter;

impl log::Log for WarnCounter {
  fn enabled(&self, metadata: &log::Metadata) -> bool {
    metadata.level() == Level::Warn
  }

  fn log(&self, record: &log::Record) {
    if record.level() == Level::Warn {
      WARN_HITS.fetch_add(1, Ordering::Relaxed);
    }
  }

  fn flush(&self) {}
}

fn install_logger() {
  static INSTALL: Once = Once::new();
  INSTALL.call_once(|| {
    let _ = log::set_boxed_logger(Box::new(WarnCounter));
    log::set_max_level(LevelFilter::Warn);
  });
}

fn warn_hits() -> usize {
  WARN_HITS.load(Ordering::Relaxed)
}

fn f32_bytes(vals: &[f32]) -> Vec<u8> {
  vals.iter().flat_map(|v| v.to_le_bytes()).collect()
}

fn config(quant_type: VectorQuantType) -> IndexConfig {
  IndexConfig {
    dims: 2,
    reduce_dims: 0,
    quant_type,
    distance_metric: VectorDistanceMetricType::L2,
    build_exploration_factor: 64,
    num_links: 8,
  }
}

fn params(count: usize) -> SearchParams {
  SearchParams {
    count,
    search_exploration_factor: 200,
    filter_len: 0,
    max_filtering_effort: 0,
  }
}

fn eid(index: usize) -> Vec<u8> {
  format!("e{index:0>6}").into_bytes()
}

/// 铸造集合：插 count 条用户向量（iid = 下标 + 1），返回服务。
async fn seed(
  store: &Arc<MemStore>,
  quant_type: VectorQuantType,
  count: usize,
) -> DiskANNService<MemStore> {
  let service = DiskANNService::default();
  assert_eq!(
    service
      .create_index(CTX, config(quant_type), Callbacks::new(Arc::clone(store)))
      .await,
    Ok(false)
  );
  for i in 0..count {
    let x = (i % 32) as f32 * 0.5;
    let y = (i / 32) as f32 * 0.5;
    let res = service
      .insert(CTX, eid(i).as_slice(), &f32_bytes(&[x, y]), b"")
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

/// 断言检索不 panic、异长项被跳过（不出现在结果）且 warn 留痕。
fn assert_skip_effect(out: &wvector::SearchOutput, banned: &[Vec<u8>], warn_before: usize) {
  assert!(out.found >= 1, "完好记录应仍可检索");
  for (id, _) in out.iter() {
    assert!(
      !banned.iter().any(|b| b.as_slice() == id),
      "异长记录 {id:?} 应被跳过，不应出现在检索结果"
    );
  }
  assert!(warn_hits() > warn_before, "异长跳过必须 log::warn 留痕");
}

/// Q8：量化 beam 臂——Term::Quantized 短/超长记录注入（超长对位旧守卫
/// 「只挡短不挡长」缺陷），检索不 panic、异长项跳过且留痕。
#[test]
fn q8_beam_skips_len_mismatched_quantized_records() {
  install_logger();
  Runtime::new().unwrap().block_on(async {
    let store = Arc::new(MemStore::new());
    let service = seed(&store, VectorQuantType::Q8, 16).await;

    // iid 3 短字节（3B < 量化 22B）、iid 4 超长（23B > 22B）
    store.poke(CTX, Term::Quantized, &3u32.to_le_bytes(), vec![0xAB; 3]);
    store.poke(CTX, Term::Quantized, &4u32.to_le_bytes(), vec![0xCD; 23]);

    let warn_before = warn_hits();
    let out = service
      .search_vector(CTX, &f32_bytes(&[0.0, 0.0]), params(10))
      .await
      .expect("Q8 异长记录不得引发检索失败");
    assert_skip_effect(&out, &[eid(2), eid(3)], warn_before);
  });
}

/// Bin：全量化态 Rerank 重排臂——Term::Vector 异长注入（重排按全精度
/// 精读候选），全部候选被跳过后结果清空而非错距/panic。
#[test]
fn bin_rerank_skips_len_mismatched_vector_records() {
  install_logger();
  Runtime::new().unwrap().block_on(async {
    let store = Arc::new(MemStore::new());
    let count = TRAIN_ROWS + 1;
    let service = seed(&store, VectorQuantType::Bin, count).await;
    assert!(service.build_quantization_table(CTX).await);
    service.backfill_quantized_vectors(CTX, 0, 1).await;

    // 全量用户 Term::Vector 记录替换为 3B 短字节：量化 beam 通道完好、
    // 候选仍产生，重排读全精度记录全部异长 → 全跳过 → 结果清空
    for iid in 1..=count as u32 {
      store.poke(CTX, Term::Vector, &iid.to_le_bytes(), vec![0x11; 3]);
    }

    let warn_before = warn_hits();
    let out = service
      .search_vector(CTX, &f32_bytes(&[0.0, 0.0]), params(10))
      .await
      .expect("Bin 重排异长记录不得引发检索失败");
    assert_eq!(out.found, 0, "全候选异长应全部跳过，不得错距混入");
    assert!(warn_hits() > warn_before, "异长跳过必须 log::warn 留痕");
  });
}

/// FP32 NoQuant：全精度 beam 臂——超长记录（9B，旧路径 unaligned_view
/// 截断成 2 元素静默错距）与短记录（3B）双注入，同判据跳过。
#[test]
fn fp32_beam_skips_len_mismatched_vector_records() {
  install_logger();
  Runtime::new().unwrap().block_on(async {
    let store = Arc::new(MemStore::new());
    let service = seed(&store, VectorQuantType::NoQuant, 16).await;

    // iid 3 短字节（3B < 8B）、iid 4 超长（9B > 8B，非元素整倍数）
    store.poke(CTX, Term::Vector, &3u32.to_le_bytes(), vec![0xEF; 3]);
    store.poke(CTX, Term::Vector, &4u32.to_le_bytes(), vec![0xBE; 9]);

    let warn_before = warn_hits();
    let out = service
      .search_vector(CTX, &f32_bytes(&[0.0, 0.0]), params(10))
      .await
      .expect("FP32 异长记录不得引发检索失败");
    assert_skip_effect(&out, &[eid(2), eid(3)], warn_before);
  });
}
