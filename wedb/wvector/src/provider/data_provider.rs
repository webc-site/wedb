//! DiskANN DataProvider 核心实现与外部/内部 ID 映射桥接
//!
//! 提供存储适配层核心结构体 [`WedbProvider`]、原生元素与全精度距离计算机封装。

use std::{
  any::TypeId,
  future,
  marker::PhantomData,
  mem,
  sync::atomic::{AtomicBool, AtomicU64},
  thread::available_parallelism,
};

use bytemuck::{bytes_of, bytes_of_mut};
use diskann::{
  graph::{BufferState, config::defaults::MAX_OCCLUSION_SIZE},
  neighbor::Neighbor,
  provider::{DataProvider, Delete, ElementStatus, NoopGuard, SetElement},
  utils::{VectorRepr, vector_repr::BufferedDistance},
};
use diskann_providers::common::{FnPtr, MinMax8};
use diskann_quantization::{alloc::Poly, spherical::iface};
use diskann_utils::object_pool::{ObjectPool, Undef};
use diskann_vector::{
  DistanceFunction, PreprocessedDistanceFunction, UnalignedSlice,
  distance::{Distance, DistanceProvider, Metric},
};
use gxhash::HashSet;
use parking_lot::Mutex;
use rand::seq::index::sample;
use wbase::map::{ConcurrentMap, new_concurrent_map};

use super::{
  cache::{AdjList, AlignToEight},
  dynamic_quant::{QUANT_STATE_KEY, RERANK_BUFFER_LENGTH},
};
use crate::{
  error::{QuantizerError, StoreError, WedbProviderError},
  fsm::FreeSpaceMap,
  quantization::{
    self, MinMax8BitQueryComputer, QuantizerImpl, RawDistanceComputer, RawQueryComputer,
    Spherical1Bit, WedbQuantizer,
  },
  service::SearchResults,
  store::{Callbacks, Context, StoreCallbacks, Term, VectorSetId},
  types::VectorQuantType,
};

/// 原生全精度元素（u8/i8/f32）：其 [`VectorRepr::Distance`] /
/// [`VectorRepr::QueryDistance`] 恒为 diskann 原生 [`Distance`] /
/// [`BufferedDistance`]，二者官方实现 `UnalignedSlice` 入参的零拷贝 SIMD
/// 距离核，据此将任意字节区间（含未对齐页内向量）无分配提升为元素视图。
pub trait NativeElement: VectorRepr + DistanceProvider<Self> {}
impl NativeElement for u8 {}
impl NativeElement for i8 {}
impl NativeElement for f32 {}

/// 字节区间提升为未对齐 `T` 元素视图（长度按元素尺寸截断，与原 cast 语义一致）。
///
/// # Safety
/// `s` 在返回的 [`UnalignedSlice`] 借用期内整段可读；`UnalignedSlice` 契约仅
/// 要求 `read_unaligned` 有效性，对指针对齐无任何要求（diskann-vector
/// unaligned.rs），故任意合法字节切片均满足。
#[inline]
unsafe fn unaligned_view<T>(s: &[u8]) -> UnalignedSlice<'_, T> {
  // SAFETY: 见函数级安全论证（UnalignedSlice 仅要求 read_unaligned 有效性）
  unsafe { UnalignedSlice::new(s.as_ptr().cast(), s.len() / mem::size_of::<T>()) }
}

/// 全精度两两距离包装器。
pub struct FullPrecisionDistance<T: NativeElement>(Distance<T, T>);

impl<T: NativeElement> RawDistanceComputer for FullPrecisionDistance<T> {
  #[inline]
  fn evaluate_similarity(&self, a: &[u8], b: &[u8]) -> f32 {
    // 零拷贝：HNSW 遍历中页内邻居向量地址几乎必然非对齐，统一走官方
    // UnalignedSlice SIMD 核（read_unaligned 语义），消除每次距离计算的
    // 堆分配与整体拷贝
    unsafe {
      self
        .0
        .evaluate_similarity(unaligned_view::<T>(a), unaligned_view::<T>(b))
    }
  }
}

/// 全精度查询距离包装器。
pub struct FullPrecisionQueryDistance<T: NativeElement>(BufferedDistance<T, T>);

impl<T: NativeElement> RawQueryComputer for FullPrecisionQueryDistance<T> {
  #[inline]
  fn evaluate_similarity(&self, a: &[u8]) -> f32 {
    // 零拷贝：同 FullPrecisionDistance，查询侧邻居向量未对齐直读
    unsafe { self.0.evaluate_similarity(unaligned_view::<T>(a)) }
  }
}

/// 静态分派距离计算机（量化/全精度两两比较）。
pub enum DistanceComputer {
  Spherical(iface::DistanceComputer),
  MinMax(FnPtr<MinMax8>),
  FullU8(FullPrecisionDistance<u8>),
  FullI8(FullPrecisionDistance<i8>),
  FullF32(FullPrecisionDistance<f32>),
}

impl DistanceComputer {
  #[inline]
  pub fn evaluate_similarity(&self, a: &[u8], b: &[u8]) -> f32 {
    match self {
      Self::Spherical(c) => RawDistanceComputer::evaluate_similarity(c, a, b),
      Self::MinMax(c) => RawDistanceComputer::evaluate_similarity(c, a, b),
      Self::FullU8(c) => RawDistanceComputer::evaluate_similarity(c, a, b),
      Self::FullI8(c) => RawDistanceComputer::evaluate_similarity(c, a, b),
      Self::FullF32(c) => RawDistanceComputer::evaluate_similarity(c, a, b),
    }
  }
}

impl DistanceFunction<&[u8], &[u8]> for DistanceComputer {
  #[inline]
  fn evaluate_similarity(&self, x: &[u8], y: &[u8]) -> f32 {
    self.evaluate_similarity(x, y)
  }
}

/// 静态分派查询距离计算机（查询向量到目标量化/全精度向量）。
pub enum QueryComputer {
  Spherical(iface::QueryComputer),
  MinMax(MinMax8BitQueryComputer),
  FullU8(FullPrecisionQueryDistance<u8>),
  FullI8(FullPrecisionQueryDistance<i8>),
  FullF32(FullPrecisionQueryDistance<f32>),
}

impl QueryComputer {
  #[inline]
  pub fn evaluate_similarity(&self, a: &[u8]) -> f32 {
    match self {
      Self::Spherical(c) => RawQueryComputer::evaluate_similarity(c, a),
      Self::MinMax(c) => RawQueryComputer::evaluate_similarity(c, a),
      Self::FullU8(c) => RawQueryComputer::evaluate_similarity(c, a),
      Self::FullI8(c) => RawQueryComputer::evaluate_similarity(c, a),
      Self::FullF32(c) => RawQueryComputer::evaluate_similarity(c, a),
    }
  }
}

impl PreprocessedDistanceFunction<&[u8]> for QueryComputer {
  #[inline]
  fn evaluate_similarity(&self, changing: &[u8]) -> f32 {
    self.evaluate_similarity(changing)
  }
}

/// 全精度距离/查询计算机装配 trait。
pub trait ToDistanceComputer: NativeElement {
  fn to_distance_computer(metric: Metric, dim: usize) -> DistanceComputer;
  fn to_query_computer(query: &[Self], metric: Metric) -> QueryComputer;
}

impl ToDistanceComputer for u8 {
  #[inline]
  fn to_distance_computer(metric: Metric, dim: usize) -> DistanceComputer {
    DistanceComputer::FullU8(FullPrecisionDistance(Self::distance_comparer(
      metric,
      Some(dim),
    )))
  }
  #[inline]
  fn to_query_computer(query: &[Self], metric: Metric) -> QueryComputer {
    QueryComputer::FullU8(FullPrecisionQueryDistance(BufferedDistance::new(
      query.into(),
      metric,
    )))
  }
}

impl ToDistanceComputer for i8 {
  #[inline]
  fn to_distance_computer(metric: Metric, dim: usize) -> DistanceComputer {
    DistanceComputer::FullI8(FullPrecisionDistance(Self::distance_comparer(
      metric,
      Some(dim),
    )))
  }
  #[inline]
  fn to_query_computer(query: &[Self], metric: Metric) -> QueryComputer {
    QueryComputer::FullI8(FullPrecisionQueryDistance(BufferedDistance::new(
      query.into(),
      metric,
    )))
  }
}

impl ToDistanceComputer for f32 {
  #[inline]
  fn to_distance_computer(metric: Metric, dim: usize) -> DistanceComputer {
    DistanceComputer::FullF32(FullPrecisionDistance(Self::distance_comparer(
      metric,
      Some(dim),
    )))
  }
  #[inline]
  fn to_query_computer(query: &[Self], metric: Metric) -> QueryComputer {
    QueryComputer::FullF32(FullPrecisionQueryDistance(BufferedDistance::new(
      query.into(),
      metric,
    )))
  }
}

/// Wedb 存储适配层 DataProvider 实现。
pub struct WedbProvider<T: ToDistanceComputer, S: StoreCallbacks> {
  /// 全精度向量维度。
  pub dim: usize,
  /// 距离度量。
  pub(crate) metric_type: Metric,
  /// 图最大出度。
  pub(crate) max_degree: usize,
  /// 存储回调通道。
  pub(crate) callbacks: Callbacks<S>,
  /// 量化器（NoQuant 时为 None）。
  pub(crate) quantizer: Option<QuantizerImpl>,
  /// 索引是否已完成回填并完全切换到量化运行态。
  pub(crate) all_quantized: AtomicBool,
  /// 回填任务完成计数。
  pub(crate) backfills_completed: AtomicU64,
  /// 训练独占锁（保证训练幂等）。
  pub(crate) training_lock: Mutex<()>,
  /// 邻接表对象池。
  pub(crate) id_buffer_pool: ObjectPool<AdjList>,
  /// 内部 id 批读缓冲池。
  pub(crate) filtered_ids_pool: ObjectPool<Vec<u32>>,
  /// 过滤决策缓冲池。
  pub(crate) filtered_decisions_pool: ObjectPool<Vec<bool>>,
  /// 重排序缓冲池。
  pub(crate) rerank_pool: ObjectPool<Vec<Neighbor<u32>>>,
  /// 量化向量缓冲池。
  pub(crate) quant_buffer_pool: ObjectPool<Vec<u8>>,
  /// 起点邻接表缓存（id 0）。
  pub(crate) neighbor_cache: ConcurrentMap<u32, Vec<u32>>,
  /// 起点全精度向量缓存（id 0）。
  pub(crate) start_point_cache: ConcurrentMap<u32, Poly<[u8], AlignToEight>>,
  /// 起点量化向量缓存（id 0）。
  pub(crate) start_point_quant_cache: ConcurrentMap<u32, Poly<[u8], AlignToEight>>,
  /// 内部 id 空闲空间映射。
  pub(crate) fsm: FreeSpaceMap<S>,
  pub(crate) _phantom: PhantomData<T>,
}

impl<T: ToDistanceComputer, S: StoreCallbacks> WedbProvider<T, S> {
  /// 构造 DataProvider 并自存储恢复元数据与起点。
  pub fn new(
    dim: usize,
    quant_type: VectorQuantType,
    metric_type: Metric,
    max_degree: usize,
    callbacks: Callbacks<S>,
    context: &Context,
  ) -> Result<Self, WedbProviderError> {
    let parallelism = available_parallelism().map(|p| p.get() * 2).unwrap_or(4);
    let id_buffer_pool =
      ObjectPool::new(Undef::new(max_degree + 1), parallelism, Some(parallelism));
    let filtered_ids_pool = ObjectPool::new(
      Undef::new(MAX_OCCLUSION_SIZE.get() as usize * 2),
      parallelism,
      Some(parallelism),
    );
    let filtered_decisions_pool = ObjectPool::new(
      Undef::new(MAX_OCCLUSION_SIZE.get() as usize),
      parallelism,
      Some(parallelism),
    );
    let rerank_pool = ObjectPool::new(
      Undef::new(RERANK_BUFFER_LENGTH),
      parallelism,
      Some(parallelism),
    );

    let start_point_cache = new_concurrent_map();
    let start_point_quant_cache = new_concurrent_map();
    let neighbor_cache = new_concurrent_map();

    // 尝试从存储加载已有起点（id 0）
    let mut v = Poly::broadcast(0u8, dim * mem::size_of::<T>(), AlignToEight)?;
    if callbacks.read_single_iid(&context.term(Term::Vector), 0, &mut v) {
      let mut neighbors = vec![0u32; max_degree + 1];
      if !callbacks.read_single_iid(&context.term(Term::Neighbors), 0, &mut neighbors) {
        return Err(StoreError::Read.into());
      }
      start_point_cache.pin().insert(0, v);
      let len = neighbors[max_degree] as usize;
      neighbors.truncate(len);
      neighbor_cache.pin().insert(0, neighbors);
    }

    let (quantizer, canonical_bytes, all_quantized) = match quant_type {
      VectorQuantType::NoQuant | VectorQuantType::XnoQuantU8 | VectorQuantType::XnoQuantI8 => {
        (None, 0, false)
      }
      VectorQuantType::Invalid => return Err(WedbProviderError::InvalidQuantizer),
      VectorQuantType::Q8 => {
        if TypeId::of::<T>() != TypeId::of::<f32>() {
          return Err(WedbProviderError::InvalidQuantizer);
        }
        let quantizer = if let Some(quant_state) =
          callbacks.read_varsize_iid::<u8>(&context.term(Term::Metadata), QUANT_STATE_KEY)
        {
          quantization::MinMax8Bit::new_from_bytes(metric_type, &quant_state)?
        } else {
          if start_point_cache.pin().contains_key(&0) {
            return Err(WedbProviderError::InvalidQuantizer);
          }
          let quantizer = quantization::MinMax8Bit::new(dim, metric_type)?;
          if !callbacks.write_iid(
            &context.term(Term::Metadata),
            QUANT_STATE_KEY,
            &quantizer.serialize()?,
          ) {
            return Err(StoreError::Write.into());
          }
          quantizer
        };

        let quantizer = QuantizerImpl::MinMax8Bit(quantizer);
        let canonical_bytes = quantizer.bytes();

        let mut qsv = Poly::broadcast(0u8, canonical_bytes, AlignToEight)?;
        if callbacks.read_single_iid(&context.term(Term::Quantized), 0, &mut qsv) {
          start_point_quant_cache.pin().insert(0, qsv);
        }

        (Some(quantizer), canonical_bytes, true)
      }
      VectorQuantType::Bin | VectorQuantType::XbinU8 | VectorQuantType::XbinI8 => {
        let quantizer = QuantizerImpl::Spherical1Bit(Spherical1Bit::new(dim));
        let canonical_bytes = quantizer.bytes();
        let mut all_quantized = false;

        if let Some(total_quant_state) =
          callbacks.read_varsize_iid::<u8>(&context.term(Term::Metadata), QUANT_STATE_KEY)
        {
          if total_quant_state.len() <= 1 {
            return Err(WedbProviderError::InvalidQuantizer);
          }
          all_quantized = total_quant_state[0] != 0;
          quantizer.deserialize(&total_quant_state[1..])?;

          let mut qsv = Poly::broadcast(0u8, canonical_bytes, AlignToEight)?;
          if callbacks.read_single_iid(&context.term(Term::Quantized), 0, &mut qsv) {
            start_point_quant_cache.pin().insert(0, qsv);
          } else if all_quantized {
            return Err(WedbProviderError::StartPoint);
          }
        }

        (Some(quantizer), canonical_bytes, all_quantized)
      }
    };

    let quant_buffer_pool =
      ObjectPool::new(Undef::new(canonical_bytes), parallelism, Some(parallelism));

    let fsm = FreeSpaceMap::new(
      context,
      callbacks.clone(),
      quantizer.is_some() && all_quantized,
      quantizer.is_none() || all_quantized,
    )?;

    Ok(Self {
      dim,
      metric_type,
      max_degree,
      callbacks,
      quantizer,
      all_quantized: AtomicBool::new(all_quantized),
      backfills_completed: AtomicU64::new(0),
      training_lock: Mutex::new(()),
      id_buffer_pool,
      filtered_ids_pool,
      filtered_decisions_pool,
      rerank_pool,
      quant_buffer_pool,
      neighbor_cache,
      start_point_cache,
      start_point_quant_cache,
      fsm,
      _phantom: PhantomData,
    })
  }

  /// 外部 ID 解析为内部 ID。
  pub fn to_internal_id(
    &self,
    context: &Context,
    id: &VectorSetId,
  ) -> Result<u32, WedbProviderError> {
    <Self as DataProvider>::to_internal_id(self, context, id)
  }

  /// 内部 ID 解析为外部 ID。
  pub fn to_external_id(
    &self,
    context: &Context,
    id: u32,
  ) -> Result<VectorSetId, WedbProviderError> {
    <Self as DataProvider>::to_external_id(self, context, id)
  }

  /// 距离度量。
  pub fn metric(&self) -> Metric {
    self.metric_type
  }

  /// 按外部 ID 检查元素是否存在。
  pub fn vector_id_exists(&self, context: &Context, id: &VectorSetId) -> bool {
    let iid = match self.to_internal_id(context, id) {
      Ok(iid) => iid,
      Err(_) => return false,
    };
    !self.fsm.is_free(context, iid).unwrap_or(true)
  }

  /// 同步删除元素及其外部 ID 映射与存储数据。
  pub fn delete_element(
    &self,
    context: &Context,
    gid: &VectorSetId,
  ) -> Result<(), WedbProviderError> {
    let id = self.to_internal_id(context, gid)?;

    let mut ok = true;
    ok &= self.callbacks.delete_iid(&context.term(Term::ExtMap), id);
    ok &= self.callbacks.delete_eid(&context.term(Term::IntMap), gid);
    let _: bool = self
      .callbacks
      .delete_iid(&context.term(Term::Attributes), id);
    ok &= self.callbacks.delete_iid(&context.term(Term::Vector), id);
    let _: bool = self
      .callbacks
      .delete_iid(&context.term(Term::Quantized), id);

    self.fsm.mark_free(context, id)?;

    if !ok {
      return Err(StoreError::Delete.into());
    }

    Ok(())
  }

  /// 按内部 ID 检查元素是否存在。
  #[inline]
  pub fn vector_iid_exists(&self, context: &Context, id: u32) -> bool {
    !self.fsm.is_free(context, id).unwrap_or(true)
  }

  /// 当前存活的用户向量总数（排除起点 id 0）。
  pub fn count(&self) -> usize {
    if self.start_points_exist() {
      self.fsm.total_used().saturating_sub(1)
    } else {
      self.fsm.total_used()
    }
  }

  /// 已铸造的最大内部 ID。
  #[inline]
  pub fn max_internal_id(&self) -> u32 {
    self.fsm.max_id()
  }

  /// 图最大出度。
  pub fn max_degree(&self) -> usize {
    self.max_degree
  }

  /// 随机取样元素（使用 gxhash 集合去重，零冗余堆分配）。
  pub fn random_members(
    &self,
    context: &Context,
    count: u32,
    output: &mut SearchResults<'_>,
  ) -> bool {
    let mut rng = rand::rng();
    let id_space = self.max_internal_id() as usize + 1;
    let total_vectors = self.fsm.total_used();
    let mut remaining = (count as usize).min(total_vectors);
    let mut chosen: HashSet<u32> = HashSet::default();

    let mut batch = remaining
      .saturating_mul(id_space)
      .div_ceil(total_vectors.max(1))
      .clamp(1, id_space);

    while remaining > 0 {
      for samp in sample(&mut rng, id_space, batch) {
        let samp = samp as u32;
        if !chosen.insert(samp) {
          continue;
        }
        let Ok(eid) = self.to_external_id(context, samp) else {
          continue;
        };

        let state = output.push_id(eid);
        remaining -= 1;
        if remaining == 0 || state == BufferState::Full {
          return true;
        }
      }

      if batch == id_space {
        break;
      }
      batch = batch.saturating_mul(2).min(id_space);
    }

    true
  }
}

impl<T: ToDistanceComputer, S: StoreCallbacks> DataProvider for WedbProvider<T, S> {
  type Context = Context;
  type InternalId = u32;
  type ExternalId = VectorSetId;
  type Error = WedbProviderError;
  type Guard = NoopGuard<u32>;

  fn to_internal_id(
    &self,
    context: &Context,
    gid: &VectorSetId,
  ) -> Result<Self::InternalId, Self::Error> {
    let mut id = 0u32;
    if !self
      .callbacks
      .read_single_eid(&context.term(Term::IntMap), gid, bytes_of_mut(&mut id))
    {
      return Err(WedbProviderError::Store(StoreError::Read));
    }
    Ok(id)
  }

  fn to_external_id(&self, context: &Context, id: u32) -> Result<Self::ExternalId, Self::Error> {
    match self
      .callbacks
      .read_varsize_iid(&context.term(Term::ExtMap), id)
    {
      Some(eid) => Ok(eid.into()),
      None => Err(WedbProviderError::Store(StoreError::Read)),
    }
  }
}

impl<T: ToDistanceComputer, S: StoreCallbacks> SetElement<&[T]> for WedbProvider<T, S> {
  type SetError = WedbProviderError;

  async fn set_element(
    &self,
    context: &Self::Context,
    id: &Self::ExternalId,
    element: &[T],
  ) -> Result<Self::Guard, Self::SetError> {
    let internal_id = self.fsm.next_id(context)?;

    if let Some(quantizer) = &self.quantizer
      && !internal_id.should_quantize()
      && !quantizer.is_trained()
      && self.fsm.total_used() > quantizer.required_vectors()
    {
      context.set_quantizer_ready();
    }

    let insert = || -> Result<(), Self::SetError> {
      self
        .callbacks
        .write_iid(&context.term(Term::Vector), internal_id.id(), element)
        .then_some(())
        .ok_or(StoreError::Write)?;
      if let Some(quantizer) = &self.quantizer
        && internal_id.should_quantize()
      {
        let mut quant = self
          .quant_buffer_pool
          .get_ref(Undef::new(quantizer.bytes()));
        let element_f32 = T::as_f32(element)
          .map_err(|e| WedbProviderError::Quantizer(QuantizerError::Compression(e.to_string())))?;
        quantizer.compress(&element_f32, &mut quant)?;
        self
          .callbacks
          .write_iid(&context.term(Term::Quantized), internal_id.id(), &quant)
          .then_some(())
          .ok_or(StoreError::Write)?;
      }
      self
        .callbacks
        .write_iid(&context.term(Term::ExtMap), internal_id.id(), id)
        .then_some(())
        .ok_or(StoreError::Write)?;
      self
        .callbacks
        .write_eid(&context.term(Term::IntMap), id, bytes_of(&internal_id.id()))
        .then_some(())
        .ok_or(StoreError::Write)?;
      Ok(())
    };

    match insert() {
      Ok(()) => (),
      Err(e) => {
        self.fsm.mark_free(context, internal_id.id())?;
        return Err(e);
      }
    }

    Ok(NoopGuard::new(internal_id.id()))
  }
}

impl<T: ToDistanceComputer, S: StoreCallbacks> Delete for WedbProvider<T, S> {
  fn delete(
    &self,
    context: &Context,
    gid: &VectorSetId,
  ) -> impl future::Future<Output = Result<(), Self::Error>> + Send {
    future::ready(self.delete_element(context, gid))
  }

  fn release(
    &self,
    _context: &Self::Context,
    _id: Self::InternalId,
  ) -> impl future::Future<Output = Result<(), Self::Error>> + Send {
    future::ready(Ok(()))
  }

  fn status_by_internal_id(
    &self,
    context: &Self::Context,
    id: Self::InternalId,
  ) -> impl future::Future<Output = Result<ElementStatus, Self::Error>> + Send {
    let status = match self.fsm.is_free(context, id) {
      Ok(true) => ElementStatus::Deleted,
      Ok(false) => ElementStatus::Valid,
      Err(e) => return future::ready(Err(e.into())),
    };
    future::ready(Ok(status))
  }

  async fn status_by_external_id(
    &self,
    context: &Self::Context,
    gid: &Self::ExternalId,
  ) -> Result<ElementStatus, Self::Error> {
    let id = self.to_internal_id(context, gid)?;
    self.status_by_internal_id(context, id).await
  }
}
