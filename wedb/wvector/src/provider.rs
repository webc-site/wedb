//! Wedb 向量数据提供者（对标微软官方 diskann-garnet 的 provider.rs）
//!
//! 实现 DiskANN 官方底层抽象：
//! - [`DataProvider`]：外部/内部 ID 映射桥接
//! - [`SetElement`] / [`Delete`]：向量与图元生命周期写入/剔除
//! - [`DynamicQuantization`]：动态量化透明自适应状态机（全精度/量化双轨检索与剪枝策略）
//! - 回调对接：通过 [`Callbacks`] 将向量、邻接表、量化状态、属性和元数据持久化至 Wedb 底座

use std::{
  alloc::Layout,
  any::TypeId,
  future,
  marker::PhantomData,
  mem,
  ops::{Deref, DerefMut},
  ptr::NonNull,
  sync::atomic::{AtomicBool, AtomicU64, Ordering},
  thread::available_parallelism,
};

use bytemuck::{bytes_of, bytes_of_mut, cast_slice};
use diskann::{
  ANNError, ANNResult,
  error::StandardError,
  graph::{
    AdjacencyList, BufferState, SearchOutputBuffer,
    config::defaults::MAX_OCCLUSION_SIZE,
    glue::{
      self, Accept, Decision, DefaultPostProcessor, FilteredAccessor, HybridPredicate,
      InplaceDeleteStrategy, InsertStrategy, Predicate, PredicateMut, PruneStrategy,
      SearchAccessor, SearchPostProcess, SearchPostProcessStep, SearchStrategy,
    },
    workingset::{
      self,
      map::{Capacity, Entry, Ref},
    },
  },
  neighbor::{Neighbor, ord::fast_distance},
  provider::{
    DataProvider, Delete, ElementStatus, HasId, NeighborAccessor, NeighborAccessorMut, NoopGuard,
    SetElement,
  },
  utils::{VectorRepr, vector_repr::BufferedDistance},
};
use diskann_providers::common::{FnPtr, MinMax8};
use diskann_quantization::{
  alloc::{AllocatorCore, AllocatorError, GlobalAllocator, Poly},
  spherical::iface,
};
use diskann_utils::{
  object_pool::{AsPooled, ObjectPool, PooledRef, Undef},
  views::{Matrix, MatrixView},
};
use diskann_vector::{
  DistanceFunction, PreprocessedDistanceFunction, UnalignedSlice,
  contains::ContainsSimd,
  distance::{Distance, DistanceProvider, Metric},
};
use gxhash::{HashMap, HashSet};
use parking_lot::{Mutex, RwLock};
use rand::seq::index::sample;

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

/// 量化状态与量化表存储在 Metadata 项下的专用键（`_qnt`）。
const QUANT_STATE_KEY: u32 = u32::from_be_bytes(*b"_qnt");

/// 重排序预分配缓冲初始容量。
const RERANK_BUFFER_LENGTH: usize = 1024;

/// 8 字节过对齐分配器（用于将任意切片安全提升为 8 字节对齐的 Poly 容器）。
#[derive(Debug, Clone, Copy)]
pub struct AlignToEight;

unsafe impl AllocatorCore for AlignToEight {
  #[inline]
  fn allocate(&self, layout: Layout) -> Result<NonNull<[u8]>, AllocatorError> {
    let layout = layout.align_to(8).map_err(|_| AllocatorError)?;
    GlobalAllocator.allocate(layout)
  }

  #[inline]
  unsafe fn deallocate(&self, ptr: NonNull<[u8]>, layout: Layout) {
    let layout = layout.align_to(8).unwrap_or(layout);
    unsafe { GlobalAllocator.deallocate(ptr, layout) }
  }
}

/// 池化邻接表包装（实现 `AsPooled` 供 `ObjectPool` 复用）。
#[derive(Clone)]
struct AdjList(AdjacencyList<u32>);

impl Deref for AdjList {
  type Target = AdjacencyList<u32>;

  #[inline]
  fn deref(&self) -> &Self::Target {
    &self.0
  }
}

impl DerefMut for AdjList {
  #[inline]
  fn deref_mut(&mut self) -> &mut Self::Target {
    &mut self.0
  }
}

impl AsPooled<Undef> for AdjList {
  fn create(args: Undef) -> Self {
    AdjList(AdjacencyList::with_capacity(args.len))
  }

  fn modify(&mut self, _args: Undef) {}
}

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
  metric_type: Metric,
  /// 图最大出度。
  max_degree: usize,
  /// 存储回调通道。
  callbacks: Callbacks<S>,
  /// 量化器（NoQuant 时为 None）。
  quantizer: Option<QuantizerImpl>,
  /// 索引是否已完成回填并完全切换到量化运行态。
  all_quantized: AtomicBool,
  /// 回填任务完成计数。
  backfills_completed: AtomicU64,
  /// 训练独占锁（保证训练幂等）。
  training_lock: Mutex<()>,
  /// 邻接表对象池。
  id_buffer_pool: ObjectPool<AdjList>,
  /// 内部 id 批读缓冲池。
  filtered_ids_pool: ObjectPool<Vec<u32>>,
  /// 过滤决策缓冲池。
  filtered_decisions_pool: ObjectPool<Vec<bool>>,
  /// 重排序缓冲池。
  rerank_pool: ObjectPool<Vec<Neighbor<u32>>>,
  /// 量化向量缓冲池。
  quant_buffer_pool: ObjectPool<Vec<u8>>,
  /// 起点邻接表缓存（id 0）。
  neighbor_cache: RwLock<HashMap<u32, Vec<u32>>>,
  /// 起点全精度向量缓存（id 0）。
  start_point_cache: RwLock<HashMap<u32, Poly<[u8], AlignToEight>>>,
  /// 起点量化向量缓存（id 0）。
  start_point_quant_cache: RwLock<HashMap<u32, Poly<[u8], AlignToEight>>>,
  /// 内部 id 空闲空间映射。
  fsm: FreeSpaceMap<S>,
  _phantom: PhantomData<T>,
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

    let start_point_cache = RwLock::new(HashMap::default());
    let start_point_quant_cache = RwLock::new(HashMap::default());
    let neighbor_cache = RwLock::new(HashMap::default());

    // 尝试从存储加载已有起点（id 0）
    let mut v = Poly::broadcast(0u8, dim * mem::size_of::<T>(), AlignToEight)?;
    if callbacks.read_single_iid(&context.term(Term::Vector), 0, &mut v) {
      let mut neighbors = vec![0u32; max_degree + 1];
      if !callbacks.read_single_iid(&context.term(Term::Neighbors), 0, &mut neighbors) {
        return Err(StoreError::Read.into());
      }
      start_point_cache.write().insert(0, v);
      let len = neighbors[max_degree] as usize;
      neighbors.truncate(len);
      neighbor_cache.write().insert(0, neighbors);
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
          if start_point_cache.read().contains_key(&0) {
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
          start_point_quant_cache.write().insert(0, qsv);
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
            start_point_quant_cache.write().insert(0, qsv);
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

  /// 插入前确保图具有合法起点。
  pub fn maybe_set_start_point(
    &self,
    context: &Context,
    point: &[T],
  ) -> Result<(), WedbProviderError> {
    let mut v = Poly::broadcast(0u8, self.dim * mem::size_of::<T>(), AlignToEight)?;
    if self
      .callbacks
      .read_single_iid(&context.term(Term::Vector), 0, &mut v)
    {
      let mut neighbors = vec![0u32; self.max_degree + 1];
      if !self
        .callbacks
        .read_single_iid(&context.term(Term::Neighbors), 0, &mut neighbors)
      {
        return Err(StoreError::Read.into());
      }

      if self.is_quantized()
        && let Some(quantizer) = self.quantizer()
      {
        let mut qpoint = vec![0u8; quantizer.bytes()];
        if !self
          .callbacks
          .read_single_iid(&context.term(Term::Quantized), 0, &mut qpoint)
        {
          return Err(StoreError::Read.into());
        }
        self
          .start_point_quant_cache
          .write()
          .insert(0, Poly::from_iter(qpoint.iter().copied(), AlignToEight)?);
      }

      self.start_point_cache.write().insert(0, v);
      let len = neighbors[self.max_degree] as usize;
      neighbors.truncate(len);
      self.neighbor_cache.write().insert(0, neighbors);
    } else {
      let neighbors = vec![0u32; self.max_degree + 1];
      let id = self.fsm.next_id(context)?;
      if id.id() != 0 {
        self.fsm.mark_free(context, id.id())?;
        return Err(WedbProviderError::StartPoint);
      }

      if !self
        .callbacks
        .write_iid(&context.term(Term::Vector), 0, point)
      {
        return Err(StoreError::Write.into());
      }

      if self.is_quantized()
        && let Some(quantizer) = self.quantizer()
      {
        let mut qpoint = vec![0u8; quantizer.bytes()];
        let point_f32 = T::as_f32(point).map_err(|e| QuantizerError::Compression(e.to_string()))?;
        quantizer.compress(&point_f32, &mut qpoint)?;
        if !self
          .callbacks
          .write_iid(&context.term(Term::Quantized), 0, &qpoint)
        {
          return Err(StoreError::Write.into());
        }
        self
          .start_point_quant_cache
          .write()
          .insert(0, Poly::from_iter(qpoint.iter().copied(), AlignToEight)?);
      }

      if !self
        .callbacks
        .write_iid(&context.term(Term::Neighbors), 0, &neighbors)
      {
        return Err(StoreError::Write.into());
      }

      self.start_point_cache.write().insert(
        0,
        Poly::from_iter(cast_slice::<T, u8>(point).iter().copied(), AlignToEight)?,
      );
      self
        .neighbor_cache
        .write()
        .insert(0, Vec::with_capacity(self.max_degree + 1));
    }

    Ok(())
  }

  /// 起点是否已存在。
  pub fn start_points_exist(&self) -> bool {
    self.start_point_cache.read().contains_key(&0) && self.neighbor_cache.read().contains_key(&0)
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

  /// 写入元素属性。
  pub fn set_attributes(
    &self,
    context: &Context,
    id: &VectorSetId,
    data: &[u8],
  ) -> Result<(), WedbProviderError> {
    let mut iid = u32::MAX;
    if !self
      .callbacks
      .read_single_eid(&context.term(Term::IntMap), id, bytes_of_mut(&mut iid))
    {
      return Err(StoreError::Read.into());
    }
    if self
      .callbacks
      .write_iid(&context.term(Term::Attributes), iid, data)
    {
      Ok(())
    } else {
      Err(StoreError::Write.into())
    }
  }

  /// 删除元素属性。
  pub fn delete_attributes(
    &self,
    context: &Context,
    id: &VectorSetId,
  ) -> Result<(), WedbProviderError> {
    let mut iid = u32::MAX;
    if !self
      .callbacks
      .read_single_eid(&context.term(Term::IntMap), id, bytes_of_mut(&mut iid))
    {
      return Err(StoreError::Read.into());
    }
    if self
      .callbacks
      .delete_iid(&context.term(Term::Attributes), iid)
    {
      Ok(())
    } else {
      Err(StoreError::Delete.into())
    }
  }

  /// 读取元素属性。
  pub fn get_attributes(&self, context: &Context, id: &VectorSetId) -> Option<Vec<u8>> {
    let mut iid = u32::MAX;
    if !self
      .callbacks
      .read_single_eid(&context.term(Term::IntMap), id, bytes_of_mut(&mut iid))
    {
      return None;
    }
    self
      .callbacks
      .read_varsize_iid(&context.term(Term::Attributes), iid)
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

  /// 训练量化器。
  pub fn train_quantizer(&self, context: &Context) -> bool {
    let _guard = match self.training_lock.try_lock() {
      Some(g) => g,
      None => return false,
    };

    let quantizer = match &self.quantizer {
      Some(q) if q.is_trained() => return true,
      Some(q) => q,
      None => return false,
    };

    let rows = quantizer.required_vectors();
    let mut data = Matrix::<T>::new(T::default(), rows, self.dim);
    let mut row_idx = 0usize;

    if self
      .fsm
      .visit_used(context, |id| {
        if id == 0 {
          return true;
        }
        if row_idx >= rows {
          return false;
        }
        let row = data.row_mut(row_idx);
        if !self
          .callbacks
          .read_single_iid(&context.term(Term::Vector), id, row)
        {
          return false;
        }
        row_idx += 1;
        true
      })
      .is_err()
    {
      return false;
    }

    if row_idx < quantizer.required_vectors() {
      return false;
    }

    let view = match data.subview(0..row_idx) {
      Some(v) => v,
      None => return false,
    };

    let converted = match T::as_f32(view.as_slice()) {
      Ok(v) => v,
      Err(_) => return false,
    };
    let view = match MatrixView::try_from(&*converted, view.nrows(), view.ncols()) {
      Ok(v) => v,
      Err(_) => return false,
    };

    match quantizer.train(self.metric_type, view) {
      Ok(()) => {
        let quant_state = match quantizer.serialize() {
          Ok(s) => s,
          Err(_) => return false,
        };
        let mut total_quant_state = vec![0u8; quant_state.len() + 1];
        total_quant_state[1..].copy_from_slice(&quant_state);

        if !self.callbacks.write_iid(
          &context.term(Term::Metadata),
          QUANT_STATE_KEY,
          &total_quant_state,
        ) {
          return false;
        }
        self.fsm.enable_quantization();
        true
      }
      Err(_) => false,
    }
  }

  /// 批量回填历史向量量化编码。
  pub fn backfill_quant_vectors(
    &self,
    context: &Context,
    task_idx: usize,
    task_count: usize,
  ) -> bool {
    let quantizer = match &self.quantizer {
      Some(q) => q,
      None => {
        self.callbacks.log(
          &context.term(Term::Quantized),
          "Error: backfill_quant_vectors: Quantizer not found.",
        );
        return false;
      }
    };

    let max_id = self.fsm.max_id_for_backfill() as usize;
    if max_id >= u32::MAX as usize {
      return false;
    }

    let task_count = task_count.min(max_id + 1);
    if task_idx >= task_count {
      return false;
    }

    let work_count = (max_id + 1).div_ceil(task_count);
    let start_id = (work_count * task_idx) as u32;
    let end_id = (work_count * (task_idx + 1)).min(max_id + 1) as u32;

    let mut v = vec![T::default(); self.dim];
    let mut f = vec![0f32; self.dim];
    let mut q = vec![0u8; quantizer.bytes()];
    for id in start_id..end_id {
      if !self
        .callbacks
        .read_single_iid(&context.term(Term::Vector), id, &mut v)
      {
        continue;
      }
      if T::as_f32_into(&v, &mut f).is_err() {
        continue;
      }
      if quantizer.compress(&f, &mut q).is_err() {
        continue;
      }
      if !self
        .callbacks
        .write_iid(&context.term(Term::Quantized), id, &q)
      {
        continue;
      }
    }

    let backfill_finished =
      self.backfills_completed.fetch_add(1, Ordering::AcqRel) + 1 == task_count as u64;

    if backfill_finished {
      if let Some(v) = self.start_point_cache.read().get(&0)
        && let Ok(v_f32) = T::as_f32(cast_slice::<u8, T>(v))
        && quantizer.compress(&v_f32, &mut q).is_ok()
      {
        let _ = self
          .callbacks
          .write_iid(&context.term(Term::Quantized), 0, &q);
        if let Ok(p) = Poly::from_iter(q.iter().copied(), AlignToEight) {
          self.start_point_quant_cache.write().insert(0, p);
        }
      }

      self.fsm.enable_reuse();

      if !self.callbacks.rmw_iid::<_, u8>(
        &context.term(Term::Metadata),
        QUANT_STATE_KEY,
        1,
        |data| {
          data[0] = 1;
        },
      ) {
        return false;
      }
      self.all_quantized.store(true, Ordering::Release);
    }

    true
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

  /// 获取指定元素的邻居与距离。
  pub fn neighbors(
    &self,
    context: &Context,
    id: &VectorSetId,
  ) -> ANNResult<Vec<Neighbor<VectorSetId>>> {
    let iid = self.to_internal_id(context, id)?;
    let v = self.get_full_vector(context, iid)?;
    let mut neighbors = AdjacencyList::with_capacity(self.max_degree + 1);

    if !self.get_neighbors(context, iid, &mut neighbors) {
      return Err(WedbProviderError::Store(StoreError::Read).into());
    }

    let d = <T as VectorRepr>::distance(self.metric_type, Some(self.dim));
    let mut result = Vec::with_capacity(self.max_degree);
    for &nbr_id in neighbors.iter() {
      if nbr_id == 0 {
        continue;
      }
      let nbr_v = self.get_full_vector(context, nbr_id)?;
      let nbr_eid = self.to_external_id(context, nbr_id)?;
      let nbr_d = d.evaluate_similarity(&v, &nbr_v);
      result.push(Neighbor::new(nbr_eid, nbr_d));
    }

    Ok(result)
  }

  /// 日志输出。
  pub fn log(&self, context: &Context, msg: &str) {
    self.callbacks.log(context, msg);
  }

  /// 关联量化器引用。
  pub fn quantizer(&self) -> Option<&QuantizerImpl> {
    self.quantizer.as_ref()
  }

  /// 索引是否已全量化运行。
  #[inline]
  pub fn is_quantized(&self) -> bool {
    self.quantizer.is_some() && self.all_quantized.load(Ordering::Acquire)
  }

  /// 是否需要调度量化建表。
  pub fn quantization_needed(&self) -> bool {
    if let Some(quantizer) = &self.quantizer {
      !self.is_quantized()
        && quantizer.is_trained()
        && self.max_internal_id() as usize > quantizer.required_vectors()
    } else {
      false
    }
  }

  /// 读取元素完整全精度向量。
  pub fn get_full_vector(&self, context: &Context, iid: u32) -> Result<Vec<T>, WedbProviderError> {
    let mut v = vec![T::default(); self.dim];
    if iid == 0 {
      let cache = self.start_point_cache.read();
      let guard = match cache.get(&iid) {
        Some(r) => r,
        None => return Err(StoreError::Read.into()),
      };
      v.copy_from_slice(cast_slice::<u8, T>(guard));
      return Ok(v);
    }

    if !self
      .callbacks
      .read_single_iid(&context.term(Term::Vector), iid, &mut v)
    {
      return Err(StoreError::Read.into());
    }

    Ok(v)
  }

  fn get_neighbors(&self, context: &Context, iid: u32, neighbors: &mut AdjacencyList<u32>) -> bool {
    let mut guard = neighbors.resize(self.max_degree + 1);

    if iid == 0
      && let Some(cached) = self.neighbor_cache.read().get(&iid)
    {
      guard[0..cached.len()].copy_from_slice(cached);
      guard.finish(cached.len());
      return true;
    }

    if !self
      .callbacks
      .read_single_iid(&context.term(Term::Neighbors), iid, &mut guard)
    {
      guard.finish(0);
      return false;
    }

    let len = guard[self.max_degree];
    guard.finish(len as usize);
    true
  }

  fn set_neighbors(
    &self,
    context: &Context,
    iid: u32,
    neighbors: &[u32],
    scratch: &mut AdjacencyList<u32>,
  ) -> Result<(), WedbProviderError> {
    let mut guard = scratch.resize(self.max_degree + 1);
    guard[0..neighbors.len()].copy_from_slice(neighbors);
    guard[self.max_degree] = neighbors.len() as u32;

    if !self.callbacks.rmw_iid(
      &context.term(Term::Neighbors),
      iid,
      (self.max_degree + 1) * mem::size_of::<u32>(),
      |data: &mut [u32]| {
        data.copy_from_slice(&guard);
        if iid == 0 {
          self.neighbor_cache.write().insert(iid, neighbors.to_vec());
        }
      },
    ) {
      return Err(StoreError::Write.into());
    }

    guard.finish(0);
    Ok(())
  }

  fn append_vector(
    &self,
    context: &Context,
    iid: u32,
    neighbors: &[u32],
  ) -> Result<(), WedbProviderError> {
    let max_degree = self.max_degree;
    if !self.callbacks.rmw_iid(
      &context.term(Term::Neighbors),
      iid,
      (max_degree + 1) * mem::size_of::<u32>(),
      |data: &mut [u32]| {
        let mut len = (data[max_degree] as usize).min(max_degree);
        for &nbr in neighbors {
          if len == max_degree {
            return;
          }
          if u32::contains_simd(&data[0..len], nbr) {
            continue;
          }
          data[len] = nbr;
          len += 1;
          data[max_degree] = len as u32;
        }
        if iid == 0
          && let Some(ns) = self.neighbor_cache.write().get_mut(&iid)
        {
          ns.clear();
          ns.extend(data.iter().copied().take(len));
        }
      },
    ) {
      return Err(StoreError::Write.into());
    }

    Ok(())
  }

  #[inline]
  fn full_vector_size(&self) -> usize {
    self.dim * mem::size_of::<T>()
  }

  #[inline]
  fn quant_vector_size(&self) -> usize {
    self.quantizer.as_ref().map(|q| q.bytes()).unwrap_or(0)
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

/// 动态量化执行策略：透明自适应全精度与量化双轨运行。
#[derive(Copy, Clone, Debug)]
pub struct DynamicQuantization;

/// 检索访问器：支持全精度与量化向量无缝切换。
pub struct DynamicAccessor<'a, T: ToDistanceComputer, S: StoreCallbacks> {
  provider: &'a WedbProvider<T, S>,
  context: &'a Context,
  quantized: bool,
  computer: QueryComputer,
  id_buffer: PooledRef<'a, AdjList>,
  filtered_ids: PooledRef<'a, Vec<u32>>,
  filtered_decisions: PooledRef<'a, Vec<bool>>,
  start_point_dist: Option<f32>,
}

impl<'a, T: ToDistanceComputer, S: StoreCallbacks> DynamicAccessor<'a, T, S> {
  const START_ID: u32 = 0;

  pub fn new(
    provider: &'a WedbProvider<T, S>,
    context: &'a Context,
    query: &'a [T],
    quantized: bool,
  ) -> Result<Self, WedbProviderError> {
    let id_buffer = provider
      .id_buffer_pool
      .get_ref(Undef::new(provider.max_degree + 1));
    let filtered_ids = provider
      .filtered_ids_pool
      .get_ref(Undef::new(MAX_OCCLUSION_SIZE.get() as usize * 2));
    let filtered_decisions = provider
      .filtered_decisions_pool
      .get_ref(Undef::new(MAX_OCCLUSION_SIZE.get() as usize));

    let computer = if quantized && let Some(quantizer) = provider.quantizer() {
      let from_f32 = T::as_f32(query)
        .map_err(|e| WedbProviderError::Quantizer(QuantizerError::Compression(e.to_string())))?;
      quantizer
        .query_computer(&from_f32)
        .map_err(|e| QuantizerError::QueryComputer(e.to_string()))?
    } else {
      T::to_query_computer(query, provider.metric_type)
    };

    Ok(DynamicAccessor {
      provider,
      context,
      quantized,
      computer,
      id_buffer,
      filtered_ids,
      filtered_decisions,
      start_point_dist: None,
    })
  }

  fn start_point_distance(&mut self) -> Result<f32, WedbProviderError> {
    if let Some(dist) = self.start_point_dist {
      return Ok(dist);
    }
    let dist = if self.quantized && self.provider.quantizer().is_some() {
      let cache = self.provider.start_point_quant_cache.read();
      match cache.get(&Self::START_ID) {
        Some(guard) => self.computer.evaluate_similarity(guard),
        None => return Err(StoreError::Read.into()),
      }
    } else {
      let cache = self.provider.start_point_cache.read();
      match cache.get(&Self::START_ID) {
        Some(guard) => self.computer.evaluate_similarity(guard),
        None => return Err(StoreError::Read.into()),
      }
    };
    self.start_point_dist = Some(dist);
    Ok(dist)
  }

  fn compute_filter_decisions(&mut self) {
    self.filtered_decisions.clear();
    let count = self.filtered_ids.len() / 2;
    self.filtered_decisions.reserve(count);
    for i in 0..count {
      let internal_id = self.filtered_ids[i * 2 + 1];
      let matches = self
        .provider
        .callbacks
        .matches_filter(self.context, internal_id);
      self.filtered_decisions.push(matches);
    }
  }
}

impl<T: ToDistanceComputer, S: StoreCallbacks> HasId for DynamicAccessor<'_, T, S> {
  type Id = u32;
}

impl<T: ToDistanceComputer, S: StoreCallbacks> SearchAccessor for DynamicAccessor<'_, T, S> {
  fn starting_points(&self) -> impl future::Future<Output = ANNResult<Vec<Self::Id>>> + Send {
    let points = if self.provider.start_points_exist() {
      vec![Self::START_ID]
    } else {
      vec![]
    };
    future::ready(Ok(points))
  }

  fn is_not_start_point(
    &self,
  ) -> impl future::Future<Output = ANNResult<impl Fn(Self::Id) -> bool + Send + Sync + 'static>> + Send
  {
    future::ready(Ok(move |id| id != Self::START_ID))
  }

  fn start_point_distances<F>(
    &mut self,
    mut f: F,
  ) -> impl future::Future<Output = ANNResult<()>> + Send
  where
    F: FnMut(Self::Id, f32) + Send,
  {
    if !self.provider.start_points_exist() {
      return future::ready(Ok(()));
    }
    let result = match self.start_point_distance() {
      Ok(dist) => {
        f(Self::START_ID, dist);
        Ok(())
      }
      Err(err) => Err(ANNError::from(err)),
    };
    future::ready(result)
  }

  fn expand_beam<Itr, P, F>(
    &mut self,
    ids: Itr,
    mut pred: P,
    mut on_neighbors: F,
  ) -> impl future::Future<Output = ANNResult<()>> + Send
  where
    Itr: Iterator<Item = Self::Id> + Send,
    P: HybridPredicate<Self::Id> + Send + Sync,
    F: FnMut(Self::Id, f32) + Send,
  {
    let mut id_buffer = mem::take(&mut **self.id_buffer);

    for nl_id in ids {
      self
        .provider
        .get_neighbors(self.context, nl_id, &mut id_buffer);
      self.filtered_ids.clear();
      for id in id_buffer.iter().copied().filter(|id| pred.eval_mut(id)) {
        if id == Self::START_ID {
          let dist = match self.start_point_distance() {
            Ok(dist) => dist,
            Err(err) => return future::ready(Err(ANNError::from(err))),
          };
          on_neighbors(id, dist);
        } else {
          self.filtered_ids.push(4);
          self.filtered_ids.push(id);
        }
      }

      let (ctx, length_hint) = if self.quantized {
        (
          self.context.term(Term::Quantized),
          self.provider.quant_vector_size(),
        )
      } else {
        (
          self.context.term(Term::Vector),
          self.provider.full_vector_size(),
        )
      };

      if !self.filtered_ids.is_empty() {
        self
          .provider
          .callbacks
          .read_multi_lpiid(&ctx, &self.filtered_ids, length_hint, |i, v| {
            if v.len() < length_hint {
              return;
            }
            let dist = self.computer.evaluate_similarity(v);
            on_neighbors(self.filtered_ids[i as usize * 2 + 1], dist);
          });
      }
    }

    **self.id_buffer = id_buffer;
    future::ready(Ok(()))
  }
}

impl<T: ToDistanceComputer, S: StoreCallbacks> FilteredAccessor for DynamicAccessor<'_, T, S> {
  fn start_point_distances<F>(
    &mut self,
    mut f: F,
  ) -> impl future::Future<Output = ANNResult<()>> + Send
  where
    F: FnMut(Decision<Self::Id>, f32) + Send,
  {
    if !self.provider.start_points_exist() {
      return future::ready(Ok(()));
    }
    let result = match self.start_point_distance() {
      Ok(dist) => {
        f(Decision::reject(Self::START_ID), dist);
        Ok(())
      }
      Err(err) => Err(ANNError::from(err)),
    };
    future::ready(result)
  }

  fn expand_beam_filtered<Itr, P, F>(
    &mut self,
    ids: Itr,
    mut pred: P,
    mut on_neighbors: F,
  ) -> impl future::Future<Output = ANNResult<()>> + Send
  where
    Itr: Iterator<Item = Self::Id> + Send,
    P: HybridPredicate<Self::Id> + Send + Sync,
    F: FnMut(Decision<Self::Id>, f32) + Send,
  {
    let mut id_buffer = mem::take(&mut **self.id_buffer);

    for nl_id in ids {
      self
        .provider
        .get_neighbors(self.context, nl_id, &mut id_buffer);
      self.filtered_ids.clear();

      for id in id_buffer.iter().copied().filter(|id| pred.eval_mut(id)) {
        if id == Self::START_ID {
          let dist = match self.start_point_distance() {
            Ok(dist) => dist,
            Err(err) => return future::ready(Err(ANNError::from(err))),
          };
          on_neighbors(Decision::reject(id), dist);
        } else {
          self.filtered_ids.push(4);
          self.filtered_ids.push(id);
        }
      }

      if self.filtered_ids.is_empty() {
        continue;
      }

      let (ctx, length_hint) = if self.quantized {
        (
          self.context.term(Term::Quantized),
          self.provider.quant_vector_size(),
        )
      } else {
        (
          self.context.term(Term::Vector),
          self.provider.full_vector_size(),
        )
      };

      self.compute_filter_decisions();

      self
        .provider
        .callbacks
        .read_multi_lpiid(&ctx, &self.filtered_ids, length_hint, |i, v| {
          let dist = self.computer.evaluate_similarity(v);
          let decision = if self.filtered_decisions[i as usize] {
            Decision::accept(self.filtered_ids[i as usize * 2 + 1])
          } else {
            Decision::reject(self.filtered_ids[i as usize * 2 + 1])
          };
          on_neighbors(decision, dist);
        });
    }

    **self.id_buffer = id_buffer;
    future::ready(Ok(()))
  }

  fn expand_beam_accept_only<Itr, P, F>(
    &mut self,
    ids: Itr,
    mut pred: P,
    mut on_neighbors: F,
  ) -> impl future::Future<Output = ANNResult<()>> + Send
  where
    Itr: Iterator<Item = Self::Id> + Send,
    P: Predicate<Self::Id> + PredicateMut<Accept<Self::Id>> + Send + Sync,
    F: FnMut(Accept<Self::Id>, f32) + Send,
  {
    let mut id_buffer = mem::take(&mut **self.id_buffer);

    for nl_id in ids {
      self
        .provider
        .get_neighbors(self.context, nl_id, &mut id_buffer);
      self.filtered_ids.clear();

      for id in id_buffer.iter().copied() {
        if id != Self::START_ID
          && pred.eval(&id)
          && self.provider.callbacks.matches_filter(self.context, id)
          && pred.eval_mut(&Accept::new(id))
        {
          self.filtered_ids.push(4);
          self.filtered_ids.push(id);
        }
      }

      if self.filtered_ids.is_empty() {
        continue;
      }

      let (ctx, length_hint) = if self.quantized {
        (
          self.context.term(Term::Quantized),
          self.provider.quant_vector_size(),
        )
      } else {
        (
          self.context.term(Term::Vector),
          self.provider.full_vector_size(),
        )
      };

      self
        .provider
        .callbacks
        .read_multi_lpiid(&ctx, &self.filtered_ids, length_hint, |i, v| {
          let dist = self.computer.evaluate_similarity(v);
          on_neighbors(Accept::new(self.filtered_ids[i as usize * 2 + 1]), dist);
        });
    }

    **self.id_buffer = id_buffer;
    future::ready(Ok(()))
  }

  fn num_starting_points(&self) -> impl future::Future<Output = ANNResult<usize>> + Send {
    if self.provider.start_points_exist() {
      future::ready(Ok(1))
    } else {
      future::ready(Ok(0))
    }
  }
}

/// 候选外部 ID 导出后处理器。
#[derive(Debug, Default, Clone, Copy)]
pub struct CopyExternalIds;

impl<'a, T: ToDistanceComputer, S: StoreCallbacks>
  SearchPostProcess<DynamicAccessor<'a, T, S>, &[T], VectorSetId> for CopyExternalIds
{
  type Error = WedbProviderError;

  fn post_process<I, B>(
    &self,
    accessor: &mut DynamicAccessor<'a, T, S>,
    _query: &[T],
    candidates: I,
    output: &mut B,
  ) -> impl future::Future<Output = Result<usize, Self::Error>> + Send
  where
    I: Iterator<Item = Neighbor<<DynamicAccessor<'a, T, S> as HasId>::Id>> + Send,
    B: SearchOutputBuffer<VectorSetId> + Send + ?Sized,
  {
    let initial = output.current_len();
    for n in candidates {
      let id = match accessor.provider.to_external_id(accessor.context, *n.id()) {
        Ok(id) => id,
        Err(_) => continue,
      };

      if output.push(Neighbor::new(id, *n.distance())).is_full() {
        break;
      }
    }

    let count = output.current_len() - initial;
    future::ready(Ok(count))
  }
}

/// 全精度精排后处理器。
#[derive(Debug, Default, Clone, Copy)]
pub struct Rerank;

impl<'a, 'b, T: ToDistanceComputer, S: StoreCallbacks>
  SearchPostProcessStep<DynamicAccessor<'a, T, S>, &'b [T], VectorSetId> for Rerank
{
  type Error<NextError>
    = WedbProviderError
  where
    NextError: StandardError;

  type NextAccessor = DynamicAccessor<'a, T, S>;

  async fn post_process_step<I, B, Next>(
    &self,
    next: &Next,
    accessor: &mut DynamicAccessor<'a, T, S>,
    query: &'b [T],
    candidates: I,
    output: &mut B,
  ) -> Result<usize, Self::Error<Next::Error>>
  where
    I: Iterator<Item = Neighbor<<DynamicAccessor<'a, T, S> as HasId>::Id>> + Send,
    B: SearchOutputBuffer<VectorSetId> + Send + ?Sized,
    Next: SearchPostProcess<Self::NextAccessor, &'b [T], VectorSetId> + Sync,
  {
    if !accessor.quantized {
      return next
        .post_process(accessor, query, candidates, output)
        .await
        .map_err(|e| WedbProviderError::PostProcessing(e.to_string()));
    }

    let provider = accessor.provider;
    let f = T::distance(provider.metric_type, Some(provider.dim));

    let mut reranked = provider
      .rerank_pool
      .get_ref(Undef::new(RERANK_BUFFER_LENGTH));
    reranked.clear();

    accessor.filtered_ids.clear();
    for nbor in candidates {
      accessor.filtered_ids.push(4);
      accessor.filtered_ids.push(*nbor.id());
    }

    if !accessor.filtered_ids.is_empty() {
      provider.callbacks.read_multi_lpiid(
        &accessor.context.term(Term::Vector),
        &accessor.filtered_ids,
        provider.full_vector_size(),
        |i, v| {
          let dist = f.evaluate_similarity(query, cast_slice::<u8, T>(v));
          reranked.push(Neighbor::new(
            accessor.filtered_ids[i as usize * 2 + 1],
            dist,
          ));
        },
      );
    }

    reranked.sort_unstable_by(fast_distance);

    next
      .post_process(accessor, query, reranked.iter().copied(), output)
      .await
      .map_err(|e| WedbProviderError::PostProcessing(e.to_string()))
  }
}

/// 剪枝访问器：缓存反向边与局部拓扑。
pub struct PruneAccessor<'a, T, S>
where
  T: ToDistanceComputer,
  S: StoreCallbacks,
{
  provider: &'a WedbProvider<T, S>,
  context: &'a Context,
  quantized: bool,
  id_buffer: PooledRef<'a, AdjList>,
  filtered_ids: PooledRef<'a, Vec<u32>>,
  distance: DistanceComputer,
  set: workingset::Map<u32, Box<[u8]>, Ref<[u8]>>,
}

impl<'a, T, S> PruneAccessor<'a, T, S>
where
  T: ToDistanceComputer,
  S: StoreCallbacks,
{
  pub fn new(
    provider: &'a WedbProvider<T, S>,
    context: &'a Context,
    quantized: bool,
    capacity: usize,
  ) -> Result<Self, WedbProviderError> {
    let distance = if quantized && let Some(quantizer) = provider.quantizer() {
      quantizer.distance_computer()?
    } else {
      T::to_distance_computer(provider.metric_type, provider.dim)
    };

    let id_buffer = provider
      .id_buffer_pool
      .get_ref(Undef::new(provider.max_degree + 1));
    let filtered_ids = provider
      .filtered_ids_pool
      .get_ref(Undef::new(MAX_OCCLUSION_SIZE.get() as usize * 2));
    let set = workingset::map::Builder::new(Capacity::Default).build(capacity);

    Ok(Self {
      provider,
      context,
      quantized,
      id_buffer,
      filtered_ids,
      distance,
      set,
    })
  }
}

impl<T: ToDistanceComputer, S: StoreCallbacks> HasId for PruneAccessor<'_, T, S> {
  type Id = u32;
}

impl<T: ToDistanceComputer, S: StoreCallbacks> glue::PruneAccessor for PruneAccessor<'_, T, S> {
  type ElementRef<'a> = &'a [u8];
  type View<'a>
    = workingset::map::View<'a, u32, Box<[u8]>, Ref<[u8]>>
  where
    Self: 'a;
  type Distance<'a>
    = &'a DistanceComputer
  where
    Self: 'a;
  type Neighbors<'a>
    = DelegateNeighborAccessor<'a, T, S>
  where
    Self: 'a;

  async fn fill<Itr>(&mut self, itr: Itr) -> ANNResult<(Self::View<'_>, Self::Distance<'_>)>
  where
    Itr: ExactSizeIterator<Item = Self::Id> + Clone + Send + Sync,
  {
    self.set.prepare(itr.clone());
    self.filtered_ids.clear();

    for id in itr {
      if id == 0 {
        if self.quantized
          && let Entry::Vacant(e) = self.set.entry(id)
        {
          let cache = self.provider.start_point_quant_cache.read();
          if let Some(guard) = cache.get(&id) {
            e.insert((&**guard).into());
          } else {
            return Err(WedbProviderError::StartPoint.into());
          }
        } else if let Entry::Vacant(e) = self.set.entry(id) {
          let cache = self.provider.start_point_cache.read();
          if let Some(guard) = cache.get(&id) {
            e.insert((&**guard).into());
          } else {
            return Err(WedbProviderError::StartPoint.into());
          }
        }
      } else if !self.set.contains_key(&id) {
        self.filtered_ids.push(4);
        self.filtered_ids.push(id);
      }
    }

    let (ctx, length_hint) = if self.quantized {
      (
        self.context.term(Term::Quantized),
        self.provider.quant_vector_size(),
      )
    } else {
      (
        self.context.term(Term::Vector),
        self.provider.full_vector_size(),
      )
    };

    if !self.filtered_ids.is_empty() {
      self
        .provider
        .callbacks
        .read_multi_lpiid(&ctx, &self.filtered_ids, length_hint, |id, v| {
          self
            .set
            .insert(self.filtered_ids[id as usize * 2 + 1], v.into());
        });
    }

    Ok((self.set.view(), &self.distance))
  }

  fn neighbors(&mut self) -> Self::Neighbors<'_> {
    DelegateNeighborAccessor {
      provider: self.provider,
      context: self.context,
      scratch: &mut self.id_buffer,
    }
  }
}

/// 邻接表操作代理访问器。
pub struct DelegateNeighborAccessor<'a, T, S>
where
  T: ToDistanceComputer,
  S: StoreCallbacks,
{
  provider: &'a WedbProvider<T, S>,
  context: &'a Context,
  scratch: &'a mut AdjacencyList<u32>,
}

impl<T: ToDistanceComputer, S: StoreCallbacks> HasId for DelegateNeighborAccessor<'_, T, S> {
  type Id = u32;
}

impl<T: ToDistanceComputer, S: StoreCallbacks> NeighborAccessor
  for DelegateNeighborAccessor<'_, T, S>
{
  fn get_neighbors(
    &mut self,
    id: Self::Id,
    neighbors: &mut AdjacencyList<Self::Id>,
  ) -> impl future::Future<Output = ANNResult<()>> + Send {
    let result = if self.provider.get_neighbors(self.context, id, neighbors) {
      Ok(())
    } else {
      Err(ANNError::from(WedbProviderError::Store(StoreError::Read)))
    };
    future::ready(result)
  }
}

impl<T: ToDistanceComputer, S: StoreCallbacks> NeighborAccessorMut
  for DelegateNeighborAccessor<'_, T, S>
{
  fn set_neighbors(
    &mut self,
    id: Self::Id,
    neighbors: &[Self::Id],
  ) -> impl future::Future<Output = ANNResult<()>> + Send {
    let result = self
      .provider
      .set_neighbors(self.context, id, neighbors, self.scratch)
      .map_err(ANNError::from);
    future::ready(result)
  }

  fn append_vector(
    &mut self,
    id: Self::Id,
    neighbors: &[Self::Id],
  ) -> impl future::Future<Output = ANNResult<()>> + Send {
    let result = self
      .provider
      .append_vector(self.context, id, neighbors)
      .map_err(ANNError::from);
    future::ready(result)
  }
}

impl<'a, T: ToDistanceComputer, S: StoreCallbacks> SearchStrategy<'a, WedbProvider<T, S>, &'a [T]>
  for DynamicQuantization
{
  type SearchAccessor = DynamicAccessor<'a, T, S>;
  type SearchAccessorError = WedbProviderError;

  fn search_accessor(
    &'a self,
    provider: &'a WedbProvider<T, S>,
    context: &'a <WedbProvider<T, S> as DataProvider>::Context,
    query: &'a [T],
  ) -> Result<Self::SearchAccessor, Self::SearchAccessorError> {
    let quantized = provider.is_quantized();
    DynamicAccessor::new(provider, context, query, quantized)
  }
}

impl<'a, T: ToDistanceComputer, S: StoreCallbacks>
  DefaultPostProcessor<'a, WedbProvider<T, S>, &'a [T], VectorSetId> for DynamicQuantization
{
  diskann::default_post_processor!(
    glue::Pipeline<glue::FilterStartPoints, glue::Pipeline<Rerank, CopyExternalIds>>
  );
}

impl<T: ToDistanceComputer, S: StoreCallbacks> PruneStrategy<WedbProvider<T, S>>
  for DynamicQuantization
{
  type PruneAccessor<'a> = PruneAccessor<'a, T, S>;
  type PruneAccessorError = WedbProviderError;

  fn prune_accessor<'a>(
    &'a self,
    provider: &'a WedbProvider<T, S>,
    context: &'a <WedbProvider<T, S> as DataProvider>::Context,
    capacity: usize,
  ) -> Result<Self::PruneAccessor<'a>, Self::PruneAccessorError> {
    let quantized = provider.is_quantized();
    PruneAccessor::new(provider, context, quantized, capacity)
  }
}

impl<'a, T: ToDistanceComputer, S: StoreCallbacks> InsertStrategy<'a, WedbProvider<T, S>, &'a [T]>
  for DynamicQuantization
{
  type PruneStrategy = Self;

  fn insert_search_accessor(
    &'a self,
    provider: &'a WedbProvider<T, S>,
    context: &'a <WedbProvider<T, S> as DataProvider>::Context,
    vector: &'a [T],
  ) -> Result<Self::SearchAccessor, Self::SearchAccessorError> {
    let quantized = provider.is_quantized();
    DynamicAccessor::new(provider, context, vector, quantized)
  }

  fn prune_strategy(&self) -> Self::PruneStrategy {
    *self
  }
}

impl<T: ToDistanceComputer, S: StoreCallbacks> InplaceDeleteStrategy<WedbProvider<T, S>>
  for DynamicQuantization
{
  type DeleteElement<'a> = &'a [T];
  type DeleteElementGuard = Box<[T]>;
  type DeleteElementError = WedbProviderError;
  type PruneStrategy = Self;
  type DeleteSearchAccessor<'a> = DynamicAccessor<'a, T, S>;
  type SearchPostProcessor = glue::CopyIds;
  type SearchStrategy = Self;

  fn prune_strategy(&self) -> Self::PruneStrategy {
    Self
  }

  fn search_strategy(&self) -> Self::SearchStrategy {
    Self
  }

  fn search_post_processor(&self) -> Self::SearchPostProcessor {
    glue::CopyIds
  }

  fn get_delete_element<'a>(
    &'a self,
    provider: &'a WedbProvider<T, S>,
    context: &'a <WedbProvider<T, S> as DataProvider>::Context,
    id: <WedbProvider<T, S> as DataProvider>::InternalId,
  ) -> impl future::Future<Output = Result<Self::DeleteElementGuard, Self::DeleteElementError>> + Send
  {
    let mut v = vec![T::default(); provider.dim];
    if !provider.callbacks.read_single_iid(context, id, &mut v) {
      return future::ready(Err(StoreError::Read.into()));
    }
    future::ready(Ok(v.into()))
  }
}
