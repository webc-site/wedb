//! VADD REDUCE 降维物理消费回归（对标 C# DiskANNService.cs:CreateIndex 直传
//! reduceDims 与 VectorManager.Callbacks.cs:SetActiveReadGeometry 的
//! quantizedDims = reduceDims != 0 ? reduceDims : dimensions 判据）。
//!
//! 核心断言：量化近似通道（Term::Quantized）按降维规格落盘，全精度通道
//! （Term::Vector）维持全维；回退到「reduce_dims 只登记不消费」的旧实现
//! （TransformKind::DoubleHadamard{ target_dim: Same }）时，本文件全部
//! 降维尺寸断言必红。

use std::sync::Arc;

use wvector::{
  Callbacks, DiskANNService, DiskAnnInsertResult, IndexConfig, SearchParams,
  VectorDistanceMetricType, VectorQuantType, store::Term,
};
use wvector_test::MemStore;

fn f32_bytes(vals: &[f32]) -> Vec<u8> {
  vals.iter().flat_map(|v| v.to_le_bytes()).collect()
}

/// 确定性伪随机向量（xorshift64+，[0,1) 均匀）。
fn rand_vec(seed: &mut u64, n: usize) -> Vec<f32> {
  (0..n)
    .map(|_| {
      *seed ^= *seed << 13;
      *seed ^= *seed >> 7;
      *seed ^= *seed << 17;
      (*seed % 1_000_000) as f32 / 1_000_000.0
    })
    .collect()
}

fn config(dims: u32, reduce_dims: u32, quant: VectorQuantType) -> IndexConfig {
  IndexConfig::new(
    dims,
    reduce_dims,
    quant,
    VectorDistanceMetricType::L2,
    32,
    8,
  )
}

/// Q8 + REDUCE：量化记录按降维规格落盘，全精度记录维持全维。
///
/// 对标 C# SetActiveReadGeometry：Q8 量化记录 = quantizedDims 字节 + 20B
/// minmax canonical 元数据；旧实现落全维 256+20 字节，断言必红。
#[compio::test]
async fn q8_reduce_dims_quantized_records_are_reduced() {
  const DIMS: usize = 256;
  const REDUCE: u32 = 32;
  const Q8_META_BYTES: usize = 20;

  let store = Arc::new(MemStore::new());
  let service = DiskANNService::default();
  let base = 8;
  assert_eq!(
    service
      .create_index(
        base,
        config(DIMS as u32, REDUCE, VectorQuantType::Q8),
        Callbacks::new(Arc::clone(&store))
      )
      .await,
    Ok(false)
  );

  let mut seed = 0x1234_5678_9abc_def0u64;
  // 起点（iid 0）+ 4 个元素（iid 1..=4）
  for i in 0..4u32 {
    let v = rand_vec(&mut seed, DIMS);
    let id = format!("e{i}");
    assert_eq!(
      service
        .insert(base, id.as_bytes(), &f32_bytes(&v), b"")
        .await,
      DiskAnnInsertResult::True
    );
  }

  let reduced = REDUCE as usize + Q8_META_BYTES;
  for iid in 0..=4u32 {
    assert_eq!(
      store.len_of(base, Term::Quantized, iid),
      Some(reduced),
      "iid {iid} 量化记录应为降维 {reduced} 字节"
    );
    assert_eq!(
      store.len_of(base, Term::Vector, iid),
      Some(DIMS * 4),
      "iid {iid} 全精度记录必须维持全维"
    );
  }

  // KNN 近似（降维量化通道读几何）+ 全精度 rerank 链路闭环
  let mut seed = 0x1234_5678_9abc_def0u64;
  let first = rand_vec(&mut seed, DIMS);
  let out = service
    .search_vector(
      base,
      &f32_bytes(&first),
      SearchParams {
        count: 4,
        search_exploration_factor: 32,
        filter_len: 0,
        max_filtering_effort: 0,
      },
    )
    .await
    .unwrap();
  assert_eq!(out.found, 4);
  assert_eq!(out.iter().next().unwrap().0, b"e0");
}

/// Q8 重开（new_from_bytes 反序列化通道）：降维规格随序列化状态恢复。
#[compio::test]
async fn q8_reopen_restores_reduced_quantized_spec() {
  const DIMS: usize = 128;
  const REDUCE: u32 = 16;

  let store = Arc::new(MemStore::new());
  let service = DiskANNService::default();
  let base = 16;
  service
    .create_index(
      base,
      config(DIMS as u32, REDUCE, VectorQuantType::Q8),
      Callbacks::new(Arc::clone(&store)),
    )
    .await
    .unwrap();

  let mut seed = 42u64;
  let v = rand_vec(&mut seed, DIMS);
  let id0 = f32_bytes(&v);
  service.insert(base, b"a", &id0, b"").await;

  // 以同一存储新建服务实例（模拟重启回建：反序列化既有量化状态）
  let service2 = DiskANNService::default();
  service2
    .create_index(
      base,
      config(DIMS as u32, REDUCE, VectorQuantType::Q8),
      Callbacks::new(Arc::clone(&store)),
    )
    .await
    .unwrap();
  let v2 = rand_vec(&mut seed, DIMS);
  service2.insert(base, b"b", &f32_bytes(&v2), b"").await;
  for iid in 0..=2u32 {
    assert_eq!(
      store.len_of(base, Term::Quantized, iid),
      Some(REDUCE as usize + 20),
      "重开后 iid {iid} 量化记录仍为降维规格"
    );
  }
  let out = service2
    .search_vector(
      base,
      &id0,
      SearchParams {
        count: 2,
        search_exploration_factor: 16,
        filter_len: 0,
        max_filtering_effort: 0,
      },
    )
    .await
    .unwrap();
  assert_eq!(out.iter().next().unwrap().0, b"a");
}

/// Bin（球面 1-bit）+ REDUCE 全链路：训练/回填/检索均收敛至降维 packed 规格。
///
/// 对标 C# 二值量化读几何 (quantizedDims + 7) / 8：64 维 → 8 维时记录 =
/// 1B 位图 + 6B 元数据 = 7 字节；旧实现为 (64+7)/8 + 6 = 14 字节必红。
#[compio::test]
async fn bin_reduce_dims_train_backfill_and_search_pipeline() {
  const DIMS: usize = 64;
  const REDUCE: u32 = 8;
  const N: usize = 1000;

  let store = Arc::new(MemStore::new());
  let service = DiskANNService::default();
  let base = 24;
  service
    .create_index(
      base,
      config(DIMS as u32, REDUCE, VectorQuantType::Bin),
      Callbacks::new(Arc::clone(&store)),
    )
    .await
    .unwrap();

  let mut seed = 0xdead_beef_cafe_0001u64;
  let mut first = Vec::new();
  for i in 0..N {
    let v = rand_vec(&mut seed, DIMS);
    if i == 0 {
      first = v.clone();
    }
    let id = format!("e{i}");
    // 末次插入越过训练阈值返回 QuantizationRequested（建表调度信号），其余 True
    let res = service
      .insert(base, id.as_bytes(), &f32_bytes(&v), b"")
      .await;
    assert!(
      matches!(
        res,
        DiskAnnInsertResult::True | DiskAnnInsertResult::QuantizationRequested
      ),
      "插入 {id} 失败: {res:?}"
    );
  }

  // 双轨训练前：无量化记录（近似通道未启用），全精度记录维持全维
  assert_eq!(store.len_of(base, Term::Quantized, 1), None);
  assert_eq!(store.len_of(base, Term::Vector, 1), Some(DIMS * 4));

  assert!(
    service.build_quantization_table(base).await,
    "1000 样本训练应成功"
  );
  service.backfill_quantized_vectors(base, 0, 1).await;

  let reduced = REDUCE.div_ceil(8) as usize + 6;
  for iid in [0u32, 1, 500, N as u32] {
    assert_eq!(
      store.len_of(base, Term::Quantized, iid),
      Some(reduced),
      "回填后 iid {iid} 量化记录应为降维 {reduced} 字节"
    );
    assert_eq!(store.len_of(base, Term::Vector, iid), Some(DIMS * 4));
  }

  // 量化检索（降维读几何）+ 全精度 rerank 闭环
  let out = service
    .search_vector(
      base,
      &f32_bytes(&first),
      SearchParams {
        count: 10,
        search_exploration_factor: 64,
        filter_len: 0,
        max_filtering_effort: 0,
      },
    )
    .await
    .unwrap();
  assert!(out.found >= 1);
  assert_eq!(out.iter().next().unwrap().0, b"e0");
}

/// 无 REDUCE 对照组维持全维量化规格；REDUCE 超过全维必须显式报错。
#[compio::test]
async fn q8_without_reduce_full_dim_and_oversized_reduce_rejected() {
  const DIMS: usize = 256;
  let store = Arc::new(MemStore::new());
  let service = DiskANNService::default();
  let base = 32;
  service
    .create_index(
      base,
      config(DIMS as u32, 0, VectorQuantType::Q8),
      Callbacks::new(Arc::clone(&store)),
    )
    .await
    .unwrap();
  let mut seed = 7u64;
  let v = rand_vec(&mut seed, DIMS);
  service.insert(base, b"a", &f32_bytes(&v), b"").await;
  assert_eq!(
    store.len_of(base, Term::Quantized, 0),
    Some(DIMS + 20),
    "无 REDUCE 时量化记录保持全维规格（防降维误伤默认路径）"
  );

  // 与协议层 ERR_REDUCE_EXCEEDS_DIMS 同判据的服务端兜底：禁止静默不生效
  assert!(
    service
      .create_index(
        40,
        config(DIMS as u32, (DIMS + 1) as u32, VectorQuantType::Q8),
        Callbacks::new(Arc::new(MemStore::new())),
      )
      .await
      .is_err()
  );
}
