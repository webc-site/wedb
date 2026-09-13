//! 量化器（对标 diskann-garnet 的 quantization.rs，构建于 diskann-quantization 0.59.0）
//!
//! Redis 协议量化方式与 DiskANN 量化器的对应：
//! - `Q8` → [`MinMax8Bit`]（8 bit 标量量化，零训练、首次向量即可用）
//! - `Bin` / `XBinU8` / `XBinI8` → [`Spherical1Bit`]（1 bit/维球面量化，
//!   需数百至上千向量训练）
//! - `NoQuant` / `XnoQuantU8` / `XnoQuantI8` → 不量化（full precision）

use std::num::NonZero;

use diskann::utils::VectorRepr;
use diskann_providers::common::{BufferedFnPtr, FnPtr, MinMax8};
use diskann_quantization::{
  CompressInto,
  algorithms::{
    Transform, TransformKind,
    transforms::{NewTransformError, TargetDim},
  },
  alloc::{AllocatorError, GlobalAllocator, Poly, ScopedAllocator},
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
use thiserror::Error;

use crate::provider::{DistanceComputer, QueryComputer};

#[derive(Debug, Error)]
pub enum QuantizerError {
  #[error("Quantization training error: {0}")]
  Training(String),
  #[error("Quantization alloc error: {0}")]
  Alloc(#[from] AllocatorError),
  #[error("Query computer error: {0}")]
  QueryComputer(String),
  #[error("Binary quantization error: {0}")]
  Compression(String),
  #[error("No quantizer found")]
  NoQuantizer,
  #[error("Got zero dimension")]
  ZeroDim,
  #[error("Transform error: {0}")]
  BadTransform(#[from] NewTransformError),
  #[error("Unsupported serialization/deserialization")]
  UnsupportedSerialization,
  #[error("Quantizer deserialization error: {0}")]
  Deserialization(String),
}

/// 量化器 trait（diskann-garnet GarnetQuantizer 的等价承接）。
#[enum_dispatch]
pub trait WedbQuantizer: Send + Sync {
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
pub enum QuantizerImpl {
  Spherical1Bit(Spherical1Bit),
  MinMax8Bit(MinMax8Bit),
}

/// 原始字节距离计算机（量化/全精度向量两两比较，零动态分派）。
pub trait RawDistanceComputer: Send + Sync {
  fn evaluate_similarity(&self, a: &[u8], b: &[u8]) -> f32;
}

pub use RawDistanceComputer as DynDistanceComputer;

/// 原始字节查询距离计算机（查询向量到量化/全精度向量，零动态分派）。
pub trait RawQueryComputer: Send + Sync {
  fn evaluate_similarity(&self, a: &[u8]) -> f32;
}

pub use RawQueryComputer as DynQueryComputer;

/// 球面 1-bit 量化（Redis `BIN`）。
///
/// 需数百至上千向量训练；量化向量 1 bit/维 + 至多 6 字节开销。
pub struct Spherical1Bit {
  dim: usize,
  inner: RwLock<Option<iface::Impl<1, GlobalAllocator>>>,
}

impl Spherical1Bit {
  pub fn new(dim: usize) -> Self {
    Self {
      dim,
      inner: RwLock::new(None),
    }
  }
}

impl WedbQuantizer for Spherical1Bit {
  fn required_vectors(&self) -> usize {
    1000
  }

  fn bytes(&self) -> usize {
    Data::<1, GlobalAllocator>::canonical_bytes(self.dim)
  }

  fn is_trained(&self) -> bool {
    self.inner.read().is_some()
  }

  fn train(&self, metric_type: Metric, data: MatrixView<f32>) -> Result<(), QuantizerError> {
    let mut rng = rand::rng();
    let quantizer = SphericalQuantizer::train(
      data.as_view(),
      TransformKind::DoubleHadamard {
        target_dim: TargetDim::Same,
      },
      SupportedMetric::try_from(metric_type)
        .map_err(|e| QuantizerError::Training(e.to_string()))?,
      PreScale::ReciprocalMeanNorm,
      &mut rng,
      GlobalAllocator,
    )
    .map_err(|e| QuantizerError::Training(e.to_string()))?;

    let mut inner = self.inner.write();
    *inner = Some(iface::Impl::<1>::new(quantizer)?);

    Ok(())
  }

  fn compress(&self, v: &[f32], into: &mut [u8]) -> Result<(), QuantizerError> {
    let guard = self.inner.read();
    if let Some(quantizer) = &*guard {
      Quantizer::<GlobalAllocator>::compress(
        quantizer,
        v,
        OpaqueMut::new(into),
        ScopedAllocator::global(),
      )
      .map_err(|e| QuantizerError::Compression(e.to_string()))?;
      Ok(())
    } else {
      Err(QuantizerError::NoQuantizer)
    }
  }

  fn distance_computer(&self) -> Result<DistanceComputer, QuantizerError> {
    let guard = self.inner.read();
    if let Some(quantizer) = &*guard {
      let computer = quantizer.distance_computer(GlobalAllocator)?;
      Ok(DistanceComputer::Spherical(computer))
    } else {
      Err(QuantizerError::NoQuantizer)
    }
  }

  fn query_computer(&self, query: &[f32]) -> Result<QueryComputer, QuantizerError> {
    let guard = self.inner.read();
    if let Some(quantizer) = &*guard {
      let computer = quantizer
        .fused_query_computer(
          query,
          iface::QueryLayout::FullPrecision,
          true,
          GlobalAllocator,
          ScopedAllocator::global(),
        )
        .map_err(|e| QuantizerError::QueryComputer(e.to_string()))?;
      Ok(QueryComputer::Spherical(computer))
    } else {
      Err(QuantizerError::NoQuantizer)
    }
  }

  fn serialize(&self) -> Result<Poly<[u8], GlobalAllocator>, QuantizerError> {
    let guard = self.inner.read();
    if let Some(quantizer) = &*guard {
      Ok(quantizer.serialize(GlobalAllocator)?)
    } else {
      Err(QuantizerError::NoQuantizer)
    }
  }

  fn deserialize(&self, state: &[u8]) -> Result<(), QuantizerError> {
    let mut guard = self.inner.write();
    if guard.is_some() {
      Err(QuantizerError::UnsupportedSerialization)
    } else {
      let q = iface::Impl::<1>::try_deserialize(state, GlobalAllocator)
        .map_err(|e| QuantizerError::Deserialization(e.to_string()))?;
      *guard = Some(q);
      Ok(())
    }
  }
}

impl DynDistanceComputer for iface::DistanceComputer {
  fn evaluate_similarity(&self, a: &[u8], b: &[u8]) -> f32 {
    <Self as DistanceFunction<Opaque<'_>, Opaque<'_>, _>>::evaluate_similarity(
      self,
      Opaque::new(a),
      Opaque::new(b),
    )
    .unwrap()
  }
}

impl DynQueryComputer for iface::QueryComputer {
  fn evaluate_similarity(&self, a: &[u8]) -> f32 {
    <Self as PreprocessedDistanceFunction<Opaque<'_>, _>>::evaluate_similarity(self, Opaque::new(a))
      .unwrap()
  }
}

/// 8-bit MinMax 标量量化（Redis `Q8`）。
///
/// 零训练、首个向量即可用；量化向量 8 bit/维 + 20 字节开销。
pub struct MinMax8Bit {
  metric: Metric,
  inner: minmax::MinMaxQuantizer,
}

impl MinMax8Bit {
  pub fn new(dim: usize, metric: Metric) -> Result<Self, QuantizerError> {
    let dim = match NonZero::new(dim) {
      Some(d) => d,
      None => return Err(QuantizerError::ZeroDim),
    };

    let mut rng = rand::rng();
    let transform = Transform::new(
      TransformKind::DoubleHadamard {
        target_dim: TargetDim::Same,
      },
      dim,
      Some(&mut rng),
      GlobalAllocator,
    )?;
    let grid_scale = POSITIVE_ONE_F32;

    Ok(Self {
      metric,
      inner: MinMaxQuantizer::new(transform, grid_scale),
    })
  }

  pub fn new_from_bytes(metric: Metric, bytes: &[u8]) -> Result<Self, QuantizerError> {
    let inner = MinMaxQuantizer::try_deserialize(bytes)
      .map_err(|e| QuantizerError::Deserialization(e.to_string()))?;
    Ok(Self { metric, inner })
  }
}

impl WedbQuantizer for MinMax8Bit {
  fn required_vectors(&self) -> usize {
    0
  }

  fn bytes(&self) -> usize {
    minmax::Data::<8>::canonical_bytes(self.inner.dim())
  }

  fn is_trained(&self) -> bool {
    true
  }

  fn train(&self, _metric: Metric, _data: MatrixView<f32>) -> Result<(), QuantizerError> {
    Ok(())
  }

  fn compress(&self, v: &[f32], into: &mut [u8]) -> Result<(), QuantizerError> {
    let into = minmax::DataMutRef::<8>::from_canonical_front_mut(into, self.inner.dim())
      .map_err(|e| QuantizerError::Compression(e.to_string()))?;
    self
      .inner
      .compress_into(v, into)
      .map_err(|e| QuantizerError::Compression(e.to_string()))?;
    Ok(())
  }

  fn distance_computer(&self) -> Result<DistanceComputer, QuantizerError> {
    let computer = DistanceComputer::MinMax(<MinMax8 as VectorRepr>::distance(
      self.metric,
      Some(self.inner.dim()),
    ));
    Ok(computer)
  }

  fn query_computer(&self, query: &[f32]) -> Result<QueryComputer, QuantizerError> {
    let computer = QueryComputer::MinMax(MinMax8BitQueryComputer::new(
      &self.inner,
      query,
      self.inner.dim(),
      self.metric,
    )?);
    Ok(computer)
  }

  fn serialize(&self) -> Result<Poly<[u8], GlobalAllocator>, QuantizerError> {
    Ok(self.inner.serialize(GlobalAllocator)?)
  }

  fn deserialize(&self, _state: &[u8]) -> Result<(), QuantizerError> {
    Err(QuantizerError::UnsupportedSerialization)
  }
}

impl DynDistanceComputer for FnPtr<MinMax8> {
  fn evaluate_similarity(&self, a: &[u8], b: &[u8]) -> f32 {
    let a = MinMax8::from_bytes(a);
    let b = MinMax8::from_bytes(b);
    <Self as DistanceFunction<_, _>>::evaluate_similarity(self, a, b)
  }
}

/// MinMax8 查询距离计算机（查询向量就地量化后按 8 bit 距离求值）。
pub struct MinMax8BitQueryComputer(pub BufferedFnPtr<MinMax8>);

impl MinMax8BitQueryComputer {
  pub fn new(
    quantizer: &minmax::MinMaxQuantizer,
    query: &[f32],
    dim: usize,
    metric: Metric,
  ) -> Result<Self, QuantizerError> {
    let mut v = vec![Default::default(); minmax::Data::<8>::canonical_bytes(dim)];
    quantizer
      .compress_into(
        query,
        minmax::DataMutRef::<8>::from_canonical_front_mut(&mut v, dim)
          .map_err(|e| QuantizerError::Compression(e.to_string()))?,
      )
      .map_err(|e| QuantizerError::Compression(e.to_string()))?;
    let inner = MinMax8::query_distance(MinMax8::from_bytes(&v), metric);
    Ok(Self(inner))
  }
}

impl DynQueryComputer for MinMax8BitQueryComputer {
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

  #[test]
  fn basic_spherical_1bit() {
    let quantizer = Spherical1Bit::new(2);

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

    quantizer.compress(&test_v, &mut test_q).unwrap();
    assert!(!test_q.iter().all(|&b| b == 0));

    let dist_comp = quantizer.distance_computer().unwrap();
    let full_a = [0.0f32, 0.0];
    let mut quant_a = vec![0u8; quantizer.bytes()];
    quantizer.compress(&full_a, &mut quant_a).unwrap();

    let d = dist_comp.evaluate_similarity(&quant_a, &test_q);
    assert_ne!(d, 0.0);

    let query_comp = quantizer.query_computer(&test_v).unwrap();
    let d = query_comp.evaluate_similarity(&quant_a);
    assert_ne!(d, 0.0);
  }

  #[test]
  fn basic_minmax_8bit() {
    let quantizer = MinMax8Bit::new(2, Metric::L2).unwrap();

    assert_eq!(quantizer.required_vectors(), 0);
    assert_eq!(quantizer.bytes(), 22);
    // MinMax8Bit 天然已训练
    assert!(quantizer.is_trained());

    let test_v = [0.5f32, 0.5];
    let mut test_q = vec![0u8; quantizer.bytes()];

    let mut test_data = Matrix::new(0.0f32, 1, 2);
    test_data.row_mut(0).copy_from_slice(&[1.0f32, 1.0]);

    // 训练为空操作，但成功
    quantizer.train(Metric::L2, test_data.as_view()).unwrap();

    quantizer.compress(&test_v, &mut test_q).unwrap();
    assert!(!test_q.iter().all(|&b| b == 0));

    let dist_comp = quantizer.distance_computer().unwrap();
    let full_a = [0.0f32, 0.0];
    let mut quant_a = vec![0u8; quantizer.bytes()];
    quantizer.compress(&full_a, &mut quant_a).unwrap();

    let d = dist_comp.evaluate_similarity(&quant_a, &test_q);
    assert_ne!(d, 0.0);

    let query_comp = quantizer.query_computer(&test_v).unwrap();
    let d = query_comp.evaluate_similarity(&quant_a);
    assert_ne!(d, 0.0);
  }
}
