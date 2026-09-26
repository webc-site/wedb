//! 量化器（对标 diskann-garnet 的 quantization.rs，构建于 diskann-quantization 0.59.0）
//!
//! Redis 协议量化方式与 DiskANN 量化器的对应：
//! - `Q8` → [`MinMax8Bit`]（8 bit 标量量化，零训练、首次向量即可用）
//! - `Bin` / `XBinU8` / `XBinI8` → [`Spherical1Bit`]（1 bit/维球面量化，
//!   需数百至上千向量训练）
//! - `NoQuant` / `XnoQuantU8` / `XnoQuantI8` → 不量化（full precision）
//!
//! VADD REDUCE 降维经 `TargetDim::Override` 由 DoubleHadamard 正交旋转后
//! 子采样承担（对标 C# 原生 create_index 直传 reduceDims），量化记录与
//! 近似距离通道全部收敛至降维规格（[`reduce_target`] 单点判定）。

use std::{fmt::Display, num::NonZero};

use diskann_quantization::{
  CompressInto,
  algorithms::{Transform, TransformKind, transforms::TargetDim},
  alloc::{GlobalAllocator, Poly, ScopedAllocator},
  minmax::{self, MinMaxQuantizer},
  num::POSITIVE_ONE_F32,
  spherical::{
    Data, PreScale, SphericalQuantizer, SupportedMetric,
    iface::{self, Opaque, OpaqueMut, Quantizer},
  },
};
use diskann_utils::views::MatrixView;
use diskann_vector::{DistanceFunction, PreprocessedDistanceFunction, distance::Metric};
use enum_dispatch::enum_dispatch;
use parking_lot::RwLock;
use webc_diskann::utils::VectorRepr;
use webc_diskann_providers::common::{BufferedFnPtr, FnPtr, MinMax8};

use crate::{
  error::QuantizerError,
  provider::{DistanceComputer, QueryComputer},
};

/// 量化器 trait（diskann-garnet GarnetQuantizer 的等价承接）。
#[enum_dispatch]
pub(crate) trait WedbQuantizer: Send + Sync {
  /// 训练前需要的向量数。
  fn required_vectors(&self) -> usize;
  /// 量化向量的字节数。
  fn bytes(&self) -> usize;
  /// 是否已训练。
  fn is_trained(&self) -> bool;
  /// 训练（矩阵每行为一个向量）。
  fn train(&self, metric: Metric, data: MatrixView<f32>) -> Result<(), QuantizerError>;
  /// 量化一个向量。
  fn compress(&self, v: &[f32], into: &mut [u8]) -> Result<(), QuantizerError>;
  /// 量化向量两两比较的距离计算机。
  fn distance_computer(&self) -> Result<DistanceComputer, QuantizerError>;
  /// 对特定查询向量的预融合距离计算机。
  fn query_computer(&self, query: &[f32]) -> Result<QueryComputer, QuantizerError>;
  /// 序列化量化器状态。
  fn serialize(&self) -> Result<Poly<[u8], GlobalAllocator>, QuantizerError>;
  /// 反序列化量化器状态。
  fn deserialize(&self, state: &[u8]) -> Result<(), QuantizerError>;
}

/// 静态分派的量化器实现枚举。
#[enum_dispatch(WedbQuantizer)]
pub(crate) enum QuantizerImpl {
  Spherical1Bit(Spherical1Bit),
  MinMax8Bit(MinMax8Bit),
}

/// 原始字节距离计算机（量化/全精度向量两两比较，零动态分派）。
pub(crate) trait RawDistanceComputer: Send + Sync {
  fn evaluate_similarity(&self, a: &[u8], b: &[u8]) -> f32;
}

/// 原始字节查询距离计算机（查询向量到量化/全精度向量，零动态分派）。
pub(crate) trait RawQueryComputer: Send + Sync {
  fn evaluate_similarity(&self, a: &[u8]) -> f32;
}

/// 形态单源常量：`*_BITS` 为每维 bit 数（同时充当 const generic 实参），
/// `SPHERICAL_TRAIN_VECTORS` 为球面量化训练门槛向量数。
const SPHERICAL_BITS: usize = 1;
const MINMAX_BITS: usize = 8;
const SPHERICAL_TRAIN_VECTORS: usize = 1000;
/// 球面量化器句柄（训练产物，存于读锁内）。
type SphericalInner = iface::Impl<{ SPHERICAL_BITS }, GlobalAllocator>;
/// `dim` 维 8 bit MinMax 量化记录视图。
type MinMaxData<'a> = minmax::DataMutRef<'a, { MINMAX_BITS }>;

/// `Display` 错误 → 量化错误变体，收口 `.map_err(|e| X(e.to_string()))` 样板。
trait QErr<T, E: Display> {
  fn qerr<E2>(self, ctor: impl FnOnce(String) -> E2) -> Result<T, E2>;
}

impl<T, E: Display> QErr<T, E> for Result<T, E> {
  fn qerr<E2>(self, ctor: impl FnOnce(String) -> E2) -> Result<T, E2> {
    self.map_err(|e| ctor(e.to_string()))
  }
}

/// REDUCE 降维后的 DoubleHadamard 正交旋转规格（两形态共用）。
#[inline]
fn hadamard(dim: usize, quant_dim: usize) -> TransformKind {
  TransformKind::DoubleHadamard {
    target_dim: reduce_target(dim, quant_dim),
  }
}

/// `dim` 维 8 bit MinMax 量化记录的规范字节数。
#[inline]
fn minmax_bytes(dim: usize) -> usize {
  minmax::Data::<{ MINMAX_BITS }>::canonical_bytes(dim)
}

/// 把 `into` 前端解释为 `dim` 维 8 bit MinMax 量化记录。
#[inline]
fn minmax_data(into: &mut [u8], dim: usize) -> Result<MinMaxData<'_>, QuantizerError> {
  minmax::DataMutRef::<{ MINMAX_BITS }>::from_canonical_front_mut(into, dim)
    .qerr(QuantizerError::Compression)
}

/// 量化向量原始维度（DoubleHadamard 恒等消费，行为与未降维完全一致）；
/// 小于原始维度时启用 DoubleHadamard 正交旋转后子采样（对标 C# 原生
/// create_index 的 reduceDims 通道，DiskANNService.cs:CreateIndex）。
#[inline]
fn reduce_target(dim: usize, quant_dim: usize) -> TargetDim {
  match NonZero::new(quant_dim) {
    Some(t) if quant_dim < dim => TargetDim::Override(t),
    _ => TargetDim::Same,
  }
}

/// 球面 1-bit 量化（Redis `BIN`）。
///
/// 需数百至上千向量训练；量化向量 1 bit/有效维（REDUCE 降维后）+ 至多 6 字节开销。
pub(crate) struct Spherical1Bit {
  /// 全精度输入维度。
  dim: usize,
  /// 量化通道有效维度（REDUCE 降维后）。
  quant_dim: usize,
  inner: RwLock<Option<SphericalInner>>,
}

impl Spherical1Bit {
  pub fn new(dim: usize, quant_dim: usize) -> Self {
    Self {
      dim,
      quant_dim,
      inner: RwLock::new(None),
    }
  }

  /// 在已训练量化器上求值；未训练统一报 `NoQuantizer`（收口读锁 + Option 分支样板）。
  fn trained<R>(
    &self,
    f: impl FnOnce(&SphericalInner) -> Result<R, QuantizerError>,
  ) -> Result<R, QuantizerError> {
    let guard = self.inner.read();
    guard.as_ref().map_or(Err(QuantizerError::NoQuantizer), f)
  }
}

impl WedbQuantizer for Spherical1Bit {
  fn required_vectors(&self) -> usize {
    SPHERICAL_TRAIN_VECTORS
  }

  fn bytes(&self) -> usize {
    Data::<{ SPHERICAL_BITS }, GlobalAllocator>::canonical_bytes(self.quant_dim)
  }

  fn is_trained(&self) -> bool {
    self.inner.read().is_some()
  }

  fn train(&self, metric_type: Metric, data: MatrixView<f32>) -> Result<(), QuantizerError> {
    let mut rng = rand::rng();
    let quantizer = SphericalQuantizer::train(
      data.as_view(),
      hadamard(self.dim, self.quant_dim),
      SupportedMetric::try_from(metric_type).qerr(QuantizerError::Training)?,
      PreScale::ReciprocalMeanNorm,
      &mut rng,
      GlobalAllocator,
    )
    .qerr(QuantizerError::Training)?;

    let mut inner = self.inner.write();
    *inner = Some(SphericalInner::new(quantizer)?);
    Ok(())
  }

  fn compress(&self, v: &[f32], into: &mut [u8]) -> Result<(), QuantizerError> {
    self.trained(|quantizer| {
      Quantizer::<GlobalAllocator>::compress(
        quantizer,
        v,
        OpaqueMut::new(into),
        ScopedAllocator::global(),
      )
      .qerr(QuantizerError::Compression)
    })
  }

  fn distance_computer(&self) -> Result<DistanceComputer, QuantizerError> {
    self.trained(|quantizer| {
      Ok(DistanceComputer::Spherical(
        quantizer.distance_computer(GlobalAllocator)?,
      ))
    })
  }

  fn query_computer(&self, query: &[f32]) -> Result<QueryComputer, QuantizerError> {
    self.trained(|quantizer| {
      Ok(QueryComputer::Spherical(
        quantizer
          .fused_query_computer(
            query,
            iface::QueryLayout::FullPrecision,
            true,
            GlobalAllocator,
            ScopedAllocator::global(),
          )
          .qerr(QuantizerError::QueryComputer)?,
      ))
    })
  }

  fn serialize(&self) -> Result<Poly<[u8], GlobalAllocator>, QuantizerError> {
    self.trained(|quantizer| Ok(quantizer.serialize(GlobalAllocator)?))
  }

  fn deserialize(&self, state: &[u8]) -> Result<(), QuantizerError> {
    let mut guard = self.inner.write();
    if guard.is_some() {
      return Err(QuantizerError::UnsupportedSerialization);
    }
    *guard = Some(
      SphericalInner::try_deserialize(state, GlobalAllocator)
        .qerr(QuantizerError::Deserialization)?,
    );

    Ok(())
  }
}

// unwrap 不变式（unwrap 规范第 7 条「100% 确定安全」锚点）：全部喂入方
// 已前置精确等长守卫——dynamic_quant 五处批量读回调经 value_len_ok 以
// quantizer.bytes() 判 v.len() != expected 即跳过；起点量化缓存
// （start_point_quant_cache）条目仅由 read_single_iid 精确等长读取或本地
// compress 缓冲装配。故 from_opaque 的 NotCanonical/UnequalLengths 校验
// 失败（长度错位）不可达；守卫若松动，此处将首当其位 panic。
impl RawDistanceComputer for iface::DistanceComputer {
  fn evaluate_similarity(&self, a: &[u8], b: &[u8]) -> f32 {
    <Self as DistanceFunction<Opaque<'_>, Opaque<'_>, _>>::evaluate_similarity(
      self,
      Opaque::new(a),
      Opaque::new(b),
    )
    .unwrap()
  }
}

// 同上：查询侧入参仅来自批量读回调守卫后的值或精确等长装配的起点缓存，
// unwrap 长度错位臂不可达（见上方不变式论证）。
impl RawQueryComputer for iface::QueryComputer {
  fn evaluate_similarity(&self, a: &[u8]) -> f32 {
    <Self as PreprocessedDistanceFunction<Opaque<'_>, _>>::evaluate_similarity(self, Opaque::new(a))
      .unwrap()
  }
}

/// 8-bit MinMax 标量量化（Redis `Q8`）。
///
/// 零训练、首个向量即可用；量化向量 8 bit/维 + 20 字节开销。
pub(crate) struct MinMax8Bit {
  metric: Metric,
  inner: minmax::MinMaxQuantizer,
}

impl MinMax8Bit {
  pub fn new(dim: usize, quant_dim: usize, metric: Metric) -> Result<Self, QuantizerError> {
    let Some(dim) = NonZero::new(dim) else {
      return Err(QuantizerError::ZeroDim);
    };

    let mut rng = rand::rng();
    let transform = Transform::new(
      hadamard(dim.get(), quant_dim),
      dim,
      Some(&mut rng),
      GlobalAllocator,
    )?;

    Ok(Self {
      metric,
      inner: MinMaxQuantizer::new(transform, POSITIVE_ONE_F32),
    })
  }

  pub fn new_from_bytes(metric: Metric, bytes: &[u8]) -> Result<Self, QuantizerError> {
    let inner = MinMaxQuantizer::try_deserialize(bytes).qerr(QuantizerError::Deserialization)?;
    Ok(Self { metric, inner })
  }
}

impl WedbQuantizer for MinMax8Bit {
  fn required_vectors(&self) -> usize {
    0
  }

  fn bytes(&self) -> usize {
    minmax_bytes(self.inner.output_dim())
  }

  fn is_trained(&self) -> bool {
    true
  }

  fn train(&self, _metric: Metric, _data: MatrixView<f32>) -> Result<(), QuantizerError> {
    Ok(())
  }

  fn compress(&self, v: &[f32], into: &mut [u8]) -> Result<(), QuantizerError> {
    let dim = self.inner.output_dim();
    self
      .inner
      .compress_into(v, minmax_data(into, dim)?)
      .qerr(QuantizerError::Compression)?;
    Ok(())
  }

  fn distance_computer(&self) -> Result<DistanceComputer, QuantizerError> {
    Ok(DistanceComputer::MinMax(<MinMax8 as VectorRepr>::distance(
      self.metric,
      Some(self.inner.output_dim()),
    )))
  }

  fn query_computer(&self, query: &[f32]) -> Result<QueryComputer, QuantizerError> {
    Ok(QueryComputer::MinMax(MinMax8BitQueryComputer::new(
      &self.inner,
      query,
      self.inner.output_dim(),
      self.metric,
    )?))
  }

  fn serialize(&self) -> Result<Poly<[u8], GlobalAllocator>, QuantizerError> {
    Ok(self.inner.serialize(GlobalAllocator)?)
  }

  fn deserialize(&self, _state: &[u8]) -> Result<(), QuantizerError> {
    Err(QuantizerError::UnsupportedSerialization)
  }
}

impl RawDistanceComputer for FnPtr<MinMax8> {
  fn evaluate_similarity(&self, a: &[u8], b: &[u8]) -> f32 {
    let a = MinMax8::from_bytes(a);
    let b = MinMax8::from_bytes(b);
    <Self as DistanceFunction<_, _>>::evaluate_similarity(self, a, b)
  }
}

/// MinMax8 查询距离计算机（查询向量就地量化后按 8 bit 距离求值）。
pub(crate) struct MinMax8BitQueryComputer(pub BufferedFnPtr<MinMax8>);

impl MinMax8BitQueryComputer {
  pub fn new(
    quantizer: &minmax::MinMaxQuantizer,
    query: &[f32],
    dim: usize,
    metric: Metric,
  ) -> Result<Self, QuantizerError> {
    let mut v = vec![Default::default(); minmax_bytes(dim)];
    quantizer
      .compress_into(query, minmax_data(&mut v, dim)?)
      .qerr(QuantizerError::Compression)?;
    Ok(Self(MinMax8::query_distance(
      MinMax8::from_bytes(&v),
      metric,
    )))
  }
}

impl RawQueryComputer for MinMax8BitQueryComputer {
  fn evaluate_similarity(&self, a: &[u8]) -> f32 {
    let a = MinMax8::from_bytes(a);
    self.0.evaluate_similarity(a)
  }
}

#[cfg(test)]
mod tests {
  use diskann_utils::views::Matrix;
  use diskann_vector::distance::Metric;

  use super::{MinMax8Bit, QuantizerError, Spherical1Bit, WedbQuantizer};

  /// 两形态共用断言：压缩记录非全零，且两两距离与查询距离通道均可求值非零。
  fn assert_quant<Q: WedbQuantizer>(quantizer: &Q, test_v: &[f32]) {
    let mut test_q = vec![0u8; quantizer.bytes()];
    quantizer.compress(test_v, &mut test_q).unwrap();
    assert!(!test_q.iter().all(|&b| b == 0));

    let mut quant_a = vec![0u8; quantizer.bytes()];
    quantizer.compress(&[0.0f32, 0.0], &mut quant_a).unwrap();
    let dist_comp = quantizer.distance_computer().unwrap();
    assert_ne!(dist_comp.evaluate_similarity(&quant_a, &test_q), 0.0);
    let query_comp = quantizer.query_computer(test_v).unwrap();
    assert_ne!(query_comp.evaluate_similarity(&quant_a), 0.0);
  }

  #[test]
  fn basic_spherical_1bit() {
    let quantizer = Spherical1Bit::new(2, 2);

    assert_eq!(quantizer.required_vectors(), 1000);
    assert_eq!(quantizer.bytes(), 1 + 6);
    assert!(!quantizer.is_trained());

    let test_v = [0.5f32, 0.5];
    let mut test_q = vec![0u8; quantizer.bytes()];

    assert!(matches!(
      quantizer.compress(&test_v, &mut test_q),
      Err(QuantizerError::NoQuantizer)
    ));
    assert!(matches!(
      quantizer.distance_computer(),
      Err(QuantizerError::NoQuantizer)
    ));
    assert!(matches!(
      quantizer.query_computer(&test_v),
      Err(QuantizerError::NoQuantizer)
    ));

    let mut test_data = Matrix::new(0.0f32, 1000, 2);
    for (i, row) in test_data.row_iter_mut().enumerate() {
      row.copy_from_slice(&[(i + 1) as f32, (i + 1) as f32]);
    }
    quantizer.train(Metric::L2, test_data.as_view()).unwrap();

    assert!(quantizer.is_trained());

    assert_quant(&quantizer, &test_v);
  }

  #[test]
  fn basic_minmax_8bit() {
    let quantizer = MinMax8Bit::new(2, 2, Metric::L2).unwrap();

    assert_eq!(quantizer.required_vectors(), 0);
    assert_eq!(quantizer.bytes(), 22);
    // MinMax8Bit 天然已训练
    assert!(quantizer.is_trained());

    let test_v = [0.5f32, 0.5];

    let mut test_data = Matrix::new(0.0f32, 1, 2);
    test_data.row_mut(0).copy_from_slice(&[1.0f32, 1.0]);

    // 训练为空操作，但成功
    quantizer.train(Metric::L2, test_data.as_view()).unwrap();

    assert_quant(&quantizer, &test_v);
  }
}
