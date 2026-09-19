//! 向量索引服务（对标 diskann-garnet 的 lib.rs + dyn_index.rs）
//!
//! [`DiskANNService`] 以 `(context)` 为键的并发注册表承接 C#
//! `DiskANNService`（P/Invoke 薄封装）的语义层：context → 索引实例，
//! 索引实例为类型擦除的 [`DiskANNIndex<WedbProvider<T>>`]（官方
//! diskann-providers 的同步包装），数据全部经存储回调持久化。
//!
//! 生命周期对标 diskann-garnet：
//! - [`IndexState`]：NoStartPoints → SettingStartPoints → Ready（起点状态机）
//! - insert：ensure_index_ready_or_init → maybe_set_start_point → 图插入 →
//!   写属性 → 量化就绪信号（SuccessStartTraining）
//! - search：Knn / InlineFilterSearch（AdaptiveL 自适应 L）+
//!   [`SearchResults`] 输出缓冲（id i32 长度前缀 + 距离）
//! - remove：inplace_delete（TwoHopAndOneHop）

use std::{
  hint::spin_loop,
  mem,
  sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
  },
  thread::yield_now,
};

use diskann::{
  graph::{
    BufferState, InplaceDeleteMethod, SearchOutputBuffer,
    config::{self, defaults::GRAPH_SLACK_FACTOR},
    index::SearchStats,
    search::{self, AdaptiveL},
  },
  neighbor::Neighbor,
};
use diskann_providers::index::wrapped_async::DiskANNIndex;
use diskann_vector::{DistanceFunction, distance::Metric};
use enum_dispatch::enum_dispatch;
use wbase::map::ConcurrentMap;

use crate::{
  error::WedbProviderError,
  provider::{DynamicQuantization, ToDistanceComputer, WedbProvider},
  store::{Callbacks, Context, LengthPrefixedIter, StoreCallbacks, VectorSetId},
  types::{VectorDistanceMetricType, VectorQuantType},
};

/// 自适应 L 的取样数（对齐 diskann-garnet ADAPTIVE_L_SAMPLES）。
const ADAPTIVE_L_SAMPLES: usize = 1000;

/// 索引就绪状态（对标 diskann-garnet IndexState）。
#[derive(Debug, PartialEq)]
enum IndexState {
  /// 图中尚无起点。
  NoStartPoints,
  /// 某线程正在设置起点。
  SettingStartPoints,
  /// 起点已设置，索引就绪。
  Ready,
}

impl From<usize> for IndexState {
  fn from(value: usize) -> Self {
    match value {
      0 => IndexState::NoStartPoints,
      1 => IndexState::SettingStartPoints,
      2 => IndexState::Ready,
      _ => IndexState::Ready,
    }
  }
}

/// 检索输出缓冲（对标 diskann-garnet lib.rs SearchResults）。
///
/// id 以 4 字节长度前缀串接写入 `ids`；`ids` 容量不足时余项进入
/// overflow 缓冲（C# 侧 overflow_results 语义；wnode 以
/// [`SearchOutput`] 一次性合并承载，无需分页续检）。
pub struct SearchResults<'a> {
  k: usize,
  ids: &'a mut [u8],
  dists: &'a mut [f32],
  index: usize,
  id_index: usize,
  overflow_ids: Vec<u8>,
  overflow_dists: Vec<f32>,
}

impl<'a> SearchResults<'a> {
  /// 以输出缓冲构造（k = 期望结果数）。
  pub fn new(k: usize, ids: &'a mut [u8], dists: &'a mut [f32]) -> Self {
    Self {
      k,
      ids,
      dists,
      index: 0,
      id_index: 0,
      overflow_ids: Vec::new(),
      overflow_dists: Vec::new(),
    }
  }

  /// 仅推送 id（random_members 用，距离为 0）。
  pub fn push_id(&mut self, id: VectorSetId) -> BufferState {
    self.push(Neighbor::new(id, 0.0))
  }

  /// 缓冲是否溢出（余项在 overflow 中）。
  pub fn overflowing(&self) -> bool {
    !self.overflow_ids.is_empty()
  }

  /// 把缓冲内结果 + overflow 物化合并为单一输出结构。
  pub fn into_search_output(self) -> SearchOutput {
    let found = self.index + self.overflow_dists.len();
    let mut ids = Vec::with_capacity(self.id_index + self.overflow_ids.len());
    ids.extend_from_slice(&self.ids[..self.id_index]);
    ids.extend_from_slice(&self.overflow_ids);

    let mut dists = Vec::with_capacity(self.index + self.overflow_dists.len());
    dists.extend_from_slice(&self.dists[..self.index]);
    dists.extend_from_slice(&self.overflow_dists);
    SearchOutput {
      ids,
      distances: dists,
      found,
    }
  }

  fn is_full(&self) -> bool {
    self.index + self.overflow_dists.len() >= self.k
  }
}

impl SearchOutputBuffer<VectorSetId> for SearchResults<'_> {
  fn size_hint(&self) -> Option<usize> {
    Some(self.k - self.index - self.overflow_dists.len())
  }

  fn push(&mut self, neighbor: Neighbor<VectorSetId>) -> BufferState {
    let (id, distance) = neighbor.as_tuple();

    if self.is_full() {
      return BufferState::Full;
    }
    if self.overflowing()
      || self.index >= self.dists.len()
      || self.id_index + mem::size_of::<u32>() + id.len() > self.ids.len()
    {
      self.overflow_ids.extend_from_slice(id.as_key_bytes());
      self.overflow_dists.push(distance);

      return if self.is_full() {
        BufferState::Full
      } else {
        BufferState::Available
      };
    }

    let id_len = id.len() as u32;
    self.ids[self.id_index..self.id_index + mem::size_of::<u32>()]
      .copy_from_slice(&id_len.to_le_bytes());
    self.id_index += mem::size_of::<u32>();

    self.ids[self.id_index..self.id_index + id.len()].copy_from_slice(&id);
    self.dists[self.index] = distance;
    self.index += 1;
    self.id_index += id.len();

    if self.is_full() {
      BufferState::Full
    } else {
      BufferState::Available
    }
  }

  fn current_len(&self) -> usize {
    self.index
  }

  fn extend<Itr>(&mut self, itr: Itr) -> usize
  where
    Itr: IntoIterator<Item = Neighbor<VectorSetId>>,
  {
    let initial = self.current_len();

    for neighbor in itr {
      if self.push(neighbor).is_full() {
        break;
      }
    }

    self.current_len() - initial
  }
}

/// 已物化的检索输出（i32 长度前缀 id 流 + 距离流）。
#[derive(Debug, Clone, Default, PartialEq)]
pub struct SearchOutput {
  /// 命中元素 id（4 字节长度前缀串接）。
  pub ids: Vec<u8>,
  /// 命中距离（与 id 一一对应）。
  pub distances: Vec<f32>,
  /// 命中数。
  pub found: usize,
}

impl SearchOutput {
  /// 迭代各命中项（id 切片, 距离）。
  pub fn iter(&self) -> impl Iterator<Item = (&[u8], f32)> {
    LengthPrefixedIter::new(&self.ids).zip(self.distances.iter().copied())
  }

  /// 物化为 SearchHit 列表。
  pub fn hits(&self) -> Vec<SearchHit> {
    self
      .iter()
      .map(|(id, distance)| SearchHit {
        external_id: id.to_vec(),
        distance,
      })
      .collect()
  }
}

/// 检索单条命中。
#[derive(Debug, Clone, PartialEq)]
pub struct SearchHit {
  pub external_id: Vec<u8>,
  pub distance: f32,
}

/// 类型擦除的索引操作面（对标 diskann-garnet dyn_index.rs DynIndex）。
#[enum_dispatch]
pub trait DynIndex: Send + Sync {
  /// 插入向量（字节切片按元素类型重解释）。
  fn insert(&self, context: &Context, id: &VectorSetId, data: &[u8]) -> diskann::ANNResult<()>;

  /// 写元素属性。
  fn set_attributes(
    &self,
    context: &Context,
    id: &VectorSetId,
    data: &[u8],
  ) -> diskann::ANNResult<()>;

  /// 删除元素属性。
  fn delete_attributes(&self, context: &Context, id: &VectorSetId) -> diskann::ANNResult<()>;

  /// 读元素属性。
  fn attributes(&self, context: &Context, id: &VectorSetId) -> Option<Vec<u8>>;

  /// 以查询向量检索。
  fn search_vector(
    &self,
    context: &Context,
    data: &[u8],
    params: search::Knn,
    output: &mut SearchResults<'_>,
  ) -> diskann::ANNResult<SearchStats>;

  /// 以既有元素为查询中心检索。
  fn search_element(
    &self,
    context: &Context,
    id: &VectorSetId,
    params: search::Knn,
    output: &mut SearchResults<'_>,
  ) -> diskann::ANNResult<SearchStats>;

  /// 以查询向量做内联过滤检索。
  fn filtered_search_vector(
    &self,
    context: &Context,
    data: &[u8],
    params: search::InlineFilterSearch,
    output: &mut SearchResults<'_>,
  ) -> diskann::ANNResult<SearchStats>;

  /// 以既有元素为查询中心做内联过滤检索。
  fn filtered_search_element(
    &self,
    context: &Context,
    id: &VectorSetId,
    params: search::InlineFilterSearch,
    output: &mut SearchResults<'_>,
  ) -> diskann::ANNResult<SearchStats>;

  /// 删除元素。
  fn remove(&self, context: &Context, id: &VectorSetId) -> diskann::ANNResult<()>;

  /// 近似计数（已铸造的最大内部 id）。
  fn approximate_count(&self) -> u64;

  /// 图的最大度。
  fn max_degree(&self) -> usize;

  /// 全精度向量维度。
  fn dims(&self) -> usize;

  /// 无起点时以 `data` 设起点。
  fn maybe_set_start_point(&self, context: &Context, data: &[u8]) -> diskann::ANNResult<()>;

  /// 按内部 id 判存在。
  fn internal_id_exists(&self, context: &Context, id: u32) -> bool;

  /// 按外部 id 判存在。
  fn external_id_exists(&self, context: &Context, id: &VectorSetId) -> bool;

  /// 读完整向量（原始字节）。
  fn full_vector(&self, context: &Context, id: &VectorSetId) -> Option<Vec<u8>>;

  /// 读元素嵌入（展开为全精度 f32）。
  fn embedding(&self, context: &Context, id: &VectorSetId) -> Option<Vec<f32>>;

  /// 元素邻接表 + 距离。
  fn neighbors(
    &self,
    context: &Context,
    id: &VectorSetId,
  ) -> diskann::ANNResult<Vec<Neighbor<VectorSetId>>>;

  /// 训练量化器。
  fn train_quantizer(&self, context: &Context) -> bool;

  /// 批量回填量化向量。
  fn backfill_quant_vectors(&self, context: &Context, task_idx: usize, task_count: usize) -> bool;

  /// 是否需要调度量化。
  fn quantization_needed(&self) -> bool;

  /// 随机取样元素。
  fn random_members(&self, context: &Context, count: u32, output: &mut SearchResults<'_>) -> bool;

  /// 外部 ID 解析为内部 ID。
  fn internal_id_of(&self, context: &Context, id: &VectorSetId) -> Option<u32>;

  /// 读内部 ID 对应的完整全精度向量。
  fn full_vector_of_iid(&self, context: &Context, iid: u32) -> Option<Vec<u8>>;

  /// 计算两向量间距离。
  fn distance(&self, context: &Context, a: &[u8], b: &[u8]) -> f32;
}

impl<T: ToDistanceComputer, S: StoreCallbacks> DynIndex for DiskANNIndex<WedbProvider<T, S>> {
  fn insert(&self, context: &Context, id: &VectorSetId, data: &[u8]) -> diskann::ANNResult<()> {
    self.insert(
      &DynamicQuantization,
      context,
      id,
      bytemuck::cast_slice::<u8, T>(data),
    )
  }

  fn set_attributes(
    &self,
    context: &Context,
    id: &VectorSetId,
    data: &[u8],
  ) -> diskann::ANNResult<()> {
    self
      .inner
      .provider()
      .set_attributes(context, id, data)
      .map_err(|e| e.into())
  }

  fn delete_attributes(&self, context: &Context, id: &VectorSetId) -> diskann::ANNResult<()> {
    self
      .inner
      .provider()
      .delete_attributes(context, id)
      .map_err(|e| e.into())
  }

  fn attributes(&self, context: &Context, id: &VectorSetId) -> Option<Vec<u8>> {
    self.inner.provider().get_attributes(context, id)
  }

  fn search_vector(
    &self,
    context: &Context,
    data: &[u8],
    params: search::Knn,
    output: &mut SearchResults<'_>,
  ) -> diskann::ANNResult<SearchStats> {
    let query = bytemuck::cast_slice::<u8, T>(data);
    self.search(params, &DynamicQuantization, context, query, output)
  }

  fn search_element(
    &self,
    context: &Context,
    id: &VectorSetId,
    params: search::Knn,
    output: &mut SearchResults<'_>,
  ) -> diskann::ANNResult<SearchStats> {
    // 内部 id → 完整向量 → 以向量检索
    let iid = self.inner.provider().to_internal_id(context, id)?;
    let data = self.inner.provider().get_full_vector(context, iid)?;
    let data_bytes = bytemuck::cast_slice::<T, u8>(&data);
    self.search_vector(context, data_bytes, params, output)
  }

  fn filtered_search_vector(
    &self,
    context: &Context,
    data: &[u8],
    params: search::InlineFilterSearch,
    output: &mut SearchResults<'_>,
  ) -> diskann::ANNResult<SearchStats> {
    let query = bytemuck::cast_slice::<u8, T>(data);
    self.search(params, &DynamicQuantization, context, query, output)
  }

  fn filtered_search_element(
    &self,
    context: &Context,
    id: &VectorSetId,
    params: search::InlineFilterSearch,
    output: &mut SearchResults<'_>,
  ) -> diskann::ANNResult<SearchStats> {
    let iid = self.inner.provider().to_internal_id(context, id)?;
    let data = self.inner.provider().get_full_vector(context, iid)?;
    let data_bytes = bytemuck::cast_slice::<T, u8>(&data);
    self.filtered_search_vector(context, data_bytes, params, output)
  }

  fn remove(&self, context: &Context, id: &VectorSetId) -> diskann::ANNResult<()> {
    self.inplace_delete(
      DynamicQuantization,
      context,
      id,
      3,
      InplaceDeleteMethod::TwoHopAndOneHop,
    )
  }

  fn approximate_count(&self) -> u64 {
    self.inner.provider().count() as u64
  }

  fn max_degree(&self) -> usize {
    self.inner.provider().max_degree()
  }

  fn dims(&self) -> usize {
    self.inner.provider().dim
  }

  fn maybe_set_start_point(&self, context: &Context, data: &[u8]) -> diskann::ANNResult<()> {
    self
      .inner
      .provider()
      .maybe_set_start_point(context, bytemuck::cast_slice::<u8, T>(data))
      .map_err(|e| e.into())
  }

  fn internal_id_exists(&self, context: &Context, id: u32) -> bool {
    self.inner.provider().vector_iid_exists(context, id)
  }

  fn external_id_exists(&self, context: &Context, id: &VectorSetId) -> bool {
    self.inner.provider().vector_id_exists(context, id)
  }

  fn full_vector(&self, context: &Context, id: &VectorSetId) -> Option<Vec<u8>> {
    let provider = self.inner.provider();
    let iid = provider.to_internal_id(context, id).ok()?;
    let v = provider.get_full_vector(context, iid).ok()?;
    Some(bytemuck::cast_slice::<T, u8>(&v).to_vec())
  }

  fn embedding(&self, context: &Context, id: &VectorSetId) -> Option<Vec<f32>> {
    let provider = self.inner.provider();
    let iid = provider.to_internal_id(context, id).ok()?;
    let v = provider.get_full_vector(context, iid).ok()?;
    let f = T::as_f32(&v).ok()?;
    Some(f.iter().copied().collect())
  }

  fn neighbors(
    &self,
    context: &Context,
    id: &VectorSetId,
  ) -> diskann::ANNResult<Vec<Neighbor<VectorSetId>>> {
    self.inner.provider().neighbors(context, id)
  }

  fn train_quantizer(&self, context: &Context) -> bool {
    self.inner.provider().train_quantizer(context)
  }

  fn backfill_quant_vectors(&self, context: &Context, task_idx: usize, task_count: usize) -> bool {
    self
      .inner
      .provider()
      .backfill_quant_vectors(context, task_idx, task_count)
  }

  fn quantization_needed(&self) -> bool {
    self.inner.provider().quantization_needed()
  }

  fn random_members(&self, context: &Context, count: u32, output: &mut SearchResults<'_>) -> bool {
    self.inner.provider().random_members(context, count, output)
  }

  fn internal_id_of(&self, context: &Context, id: &VectorSetId) -> Option<u32> {
    self.inner.provider().to_internal_id(context, id).ok()
  }

  fn full_vector_of_iid(&self, context: &Context, iid: u32) -> Option<Vec<u8>> {
    let v = self.inner.provider().get_full_vector(context, iid).ok()?;
    Some(bytemuck::cast_slice::<T, u8>(&v).to_vec())
  }

  fn distance(&self, _context: &Context, a: &[u8], b: &[u8]) -> f32 {
    let f = T::distance(
      self.inner.provider().metric(),
      Some(self.inner.provider().dim),
    );
    f.evaluate_similarity(
      bytemuck::cast_slice::<u8, T>(a),
      bytemuck::cast_slice::<u8, T>(b),
    )
  }
}

/// 静态分派的索引具体实现。
#[enum_dispatch(DynIndex)]
pub enum IndexImpl<S: StoreCallbacks> {
  U8(DiskANNIndex<WedbProvider<u8, S>>),
  I8(DiskANNIndex<WedbProvider<i8, S>>),
  F32(DiskANNIndex<WedbProvider<f32, S>>),
}

/// 索引实例（静态分派 + 量化类型 + 就绪状态）。
pub struct Index<S: StoreCallbacks> {
  /// 静态分派的索引实例。
  inner: IndexImpl<S>,
  /// 量化类型。
  quant_type: VectorQuantType,
  /// 全精度向量维度。
  dims: usize,
  /// 就绪状态标记（IndexState 值）。
  state: AtomicUsize,
}

impl<S: StoreCallbacks> Index<S> {
  fn element_size(&self) -> usize {
    match self.quant_type {
      VectorQuantType::XnoQuantU8
      | VectorQuantType::XbinU8
      | VectorQuantType::XnoQuantI8
      | VectorQuantType::XbinI8 => 1,
      _ => 4,
    }
  }

  fn expected_vector_len(&self) -> usize {
    self.dims * self.element_size()
  }
}

/// 索引几何与量化参数（对标 C# CreateIndex 入参子集）。

#[derive(Debug, Clone, Copy)]
pub struct IndexConfig {
  /// 向量维度。
  pub dims: u32,
  /// 降维后维度（0 = 不降维）。
  pub reduce_dims: u32,
  /// 量化类型。
  pub quant_type: VectorQuantType,
  /// 距离度量。
  pub distance_metric: VectorDistanceMetricType,
  /// 构建期探索因子（L_build）。
  pub build_exploration_factor: u32,
  /// 每层链接数（M，即图最大度）。
  pub num_links: u32,
}

impl IndexConfig {
  pub fn new(
    dims: u32,
    reduce_dims: u32,
    quant_type: VectorQuantType,
    distance_metric: VectorDistanceMetricType,
    build_exploration_factor: u32,
    num_links: u32,
  ) -> Self {
    Self {
      dims,
      reduce_dims,
      quant_type,
      distance_metric,
      build_exploration_factor,
      num_links,
    }
  }
}

/// 插入结果（对标 C# NativeDiskANNMethods.DiskANNInsertResult）。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DiskAnnInsertResult {
  /// 插入失败（重复元素 / 维度不匹配 / 索引不存在）。
  False = 0,
  /// 插入成功。
  True = 1,
  /// 插入成功且达到量化训练阈值（需调度建表）。
  QuantizationRequested = 2,
}

/// 保障索引就绪（无起点时运行 `init` 设起点），失败返回错误。
fn ensure_index_ready_or_init<S: StoreCallbacks, F, E>(index: &Index<S>, init: F) -> Option<E>
where
  F: FnOnce() -> Option<E>,
{
  let mut spin_count = 0usize;
  loop {
    match index.state.load(Ordering::Acquire).into() {
      IndexState::Ready => break,
      IndexState::SettingStartPoints => {
        spin_count += 1;
        if spin_count < 32 {
          spin_loop();
        } else {
          yield_now();
        }
        continue;
      }
      IndexState::NoStartPoints => {
        if index
          .state
          .compare_exchange(
            IndexState::NoStartPoints as usize,
            IndexState::SettingStartPoints as usize,
            Ordering::AcqRel,
            Ordering::Acquire,
          )
          .is_ok()
        {
          if let Some(err) = init() {
            index
              .state
              .store(IndexState::NoStartPoints as usize, Ordering::Release);
            return Some(err);
          }
          index
            .state
            .store(IndexState::Ready as usize, Ordering::Release);
          break;
        }
      }
    }
  }
  None
}

/// 按量化类型选择向量元素类型并构造静态分派索引（对标 create_index_impl）。
fn create_index_impl<T: ToDistanceComputer, S: StoreCallbacks>(
  params: &IndexConfig,
  config: config::Config,
  metric_type: Metric,
  callbacks: Callbacks<S>,
  context: &Context,
  wrap: impl FnOnce(DiskANNIndex<WedbProvider<T, S>>) -> IndexImpl<S>,
) -> Result<(Arc<Index<S>>, bool), WedbProviderError> {
  let dim = params.dims as usize;
  let max_degree = params.num_links as usize;
  let quant_type = params.quant_type;
  let provider =
    WedbProvider::<T, S>::new(dim, quant_type, metric_type, max_degree, callbacks, context)?;
  let state = if provider.start_points_exist() {
    IndexState::Ready as usize
  } else {
    IndexState::NoStartPoints as usize
  };

  // 仅需训练的量化器（Bin 系）需要调度建表
  let quant_needed = match quant_type {
    VectorQuantType::Bin | VectorQuantType::XbinI8 | VectorQuantType::XbinU8 => {
      provider.quantization_needed()
    }
    _ => false,
  };

  let dims = provider.dim;
  let index_inst = DiskANNIndex::new_with_current_thread_runtime(config, provider);
  Ok((
    Arc::new(Index {
      inner: wrap(index_inst),
      quant_type,
      dims,
      state: AtomicUsize::new(state),
    }),
    quant_needed,
  ))
}

/// 检索参数（对标 C# search_vector/search_element 入参子集）。
#[derive(Debug, Clone, Copy)]
pub struct SearchParams {
  /// 检索结果数（K，对齐 C# count：输出缓冲以 K 定容）。
  pub count: usize,
  /// 检索表长（L， Accuracy vs speed）。
  pub search_exploration_factor: usize,
  /// 过滤表达式字节数（0 = 普通 KNN 检索）。
  pub filter_len: usize,
  /// 内联过滤的自适应 L 放大因子（maxFilteringEffort）。
  pub max_filtering_effort: usize,
}

/// 向量索引服务：context → 索引实例的并发注册表。
pub struct DiskANNService<S: StoreCallbacks> {
  indexes: ConcurrentMap<u64, Arc<Index<S>>>,
}

impl<S: StoreCallbacks> Default for DiskANNService<S> {
  fn default() -> Self {
    Self {
      indexes: ConcurrentMap::default(),
    }
  }
}

/// 索引创建错误。
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[error("创建向量索引失败")]
pub struct CreateIndexError;

/// 向量检索错误。
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[error("向量检索失败")]
pub struct SearchError;

impl<S: StoreCallbacks> DiskANNService<S> {
  /// libs/server/Resp/Vector/DiskANNService.cs:CreateIndex
  ///
  /// 为 context 建立索引；几何按官方 diskann config 装配
  /// （target_degree = max_degree / GRAPH_SLACK_FACTOR，MaxDegree::Value(max_degree)）。
  /// 返回是否需要调度量化建表（C# out quantizationRequested 语义）。
  #[inline]
  pub fn create_index(
    &self,
    context: u64,
    params: impl Into<IndexConfig>,
    callbacks: Callbacks<S>,
  ) -> Result<bool, CreateIndexError> {
    let params = params.into();
    let metric_type =
      Metric::try_from(params.distance_metric as i32).map_err(|_| CreateIndexError)?;

    let target_degree = (params.num_links as f32 / GRAPH_SLACK_FACTOR) as usize;
    let config = config::Builder::new(
      target_degree,
      config::MaxDegree::Value(params.num_links as usize),
      params.build_exploration_factor as usize,
      metric_type.into(),
    )
    .build()
    .map_err(|_| CreateIndexError)?;

    let ctx = Context::new(context);

    // 量化类型 → 元素类型（对标 C# 端 X 系量化走 8 bit 元素）
    let created = match params.quant_type {
      VectorQuantType::Invalid => Err(CreateIndexError),
      VectorQuantType::XnoQuantU8 | VectorQuantType::XbinU8 => {
        create_index_impl::<u8, S>(&params, config, metric_type, callbacks, &ctx, IndexImpl::U8)
          .map_err(|_| CreateIndexError)
      }
      VectorQuantType::XnoQuantI8 | VectorQuantType::XbinI8 => {
        create_index_impl::<i8, S>(&params, config, metric_type, callbacks, &ctx, IndexImpl::I8)
          .map_err(|_| CreateIndexError)
      }
      VectorQuantType::NoQuant | VectorQuantType::Bin | VectorQuantType::Q8 => {
        create_index_impl::<f32, S>(
          &params,
          config,
          metric_type,
          callbacks,
          &ctx,
          IndexImpl::F32,
        )
        .map_err(|_| CreateIndexError)
      }
    };

    match created {
      Ok((index, quant_needed)) => {
        self.indexes.pin().insert(context, index);
        Ok(quant_needed)
      }
      Err(e) => Err(e),
    }
  }

  /// diskann-garnet/DiskANNService.cs:DropIndex
  pub fn drop_index(&self, context: u64) {
    self.indexes.pin().remove(&context);
  }

  /// diskann-garnet/DiskANNService.cs:Insert
  ///
  /// 起点保障 → 图插入 → 写属性 → 量化就绪判定（对标 insert C 函数）。
  pub fn insert(
    &self,
    context: u64,
    external_id: &[u8],
    vector: &[u8],
    attributes: &[u8],
  ) -> DiskAnnInsertResult {
    let Some(index) = self.indexes.pin().get(&context).cloned() else {
      return DiskAnnInsertResult::False;
    };
    let ctx = Context::new(context);
    let id = VectorSetId::from(external_id);

    if vector.len() != index.expected_vector_len() {
      return DiskAnnInsertResult::False;
    }

    if index.inner.external_id_exists(&ctx, &id) {
      return DiskAnnInsertResult::False;
    }

    if let Some(_err) = ensure_index_ready_or_init(&index, || {
      index.inner.maybe_set_start_point(&ctx, vector).err()
    }) {
      return DiskAnnInsertResult::False;
    }

    let old_ready = ctx.quantizer_ready();

    if index.inner.insert(&ctx, &id, vector).is_ok() {
      // 属性在插入后写入（以内部 id 为键）
      if index.inner.set_attributes(&ctx, &id, attributes).is_err() {
        return DiskAnnInsertResult::False;
      }

      let ready = ctx.quantizer_ready();
      if !old_ready && ready {
        DiskAnnInsertResult::QuantizationRequested
      } else {
        DiskAnnInsertResult::True
      }
    } else {
      DiskAnnInsertResult::False
    }
  }

  /// diskann-garnet/DiskANNService.cs:Remove
  pub fn remove(&self, context: u64, external_id: &[u8]) -> bool {
    let Some(index) = self.indexes.pin().get(&context).cloned() else {
      return false;
    };
    let ctx = Context::new(context);
    let id = VectorSetId::from(external_id);

    if !index.inner.external_id_exists(&ctx, &id) {
      return false;
    }

    index.inner.remove(&ctx, &id).is_ok()
  }

  /// diskann-garnet/DiskANNService.cs:BuildQuantizationTable
  pub fn build_quantization_table(&self, context: u64) -> bool {
    let Some(index) = self.indexes.pin().get(&context).cloned() else {
      return false;
    };
    index.inner.train_quantizer(&Context::new(context))
  }

  /// diskann-garnet/DiskANNService.cs:BackfillQuantizedVectors
  pub fn backfill_quantized_vectors(&self, context: u64, task_index: usize, task_count: usize) {
    if let Some(index) = self.indexes.pin().get(&context).cloned() {
      index
        .inner
        .backfill_quant_vectors(&Context::new(context), task_index, task_count);
    }
  }

  /// 是否需要调度量化建表。
  pub fn needs_quantization(&self, context: u64) -> bool {
    self
      .indexes
      .pin()
      .get(&context)
      .is_some_and(|i| i.inner.quantization_needed())
  }

  /// 执行检索并物化输出（Knn / InlineFilterSearch 分派 + overflow 合并）。
  fn run_search(
    &self,
    context: u64,
    index: &Index<S>,
    query: &[u8],
    params: SearchParams,
  ) -> Result<SearchOutput, SearchError> {
    let ctx = Context::new(context);

    // 束宽对齐 C# 原生 search_vector（无 beam width 入参 → diskann 默认 1）
    let knn_params =
      search::Knn::new_default(params.search_exploration_factor).map_err(|_| SearchError)?;

    // 输出缓冲以 K 定容（对齐 C# EnsureIdBufferSize(outputIds, count)）：
    // id 至少 4B 前缀 + 8B id，距离 count × f32
    let count = params.count.max(1);
    let mut ids = vec![0u8; count * (4 + 8)];
    let mut dists = vec![0f32; count];
    let mut output = SearchResults::new(count, &mut ids, &mut dists);

    let res = if params.filter_len == 0 {
      index
        .inner
        .search_vector(&ctx, query, knn_params, &mut output)
    } else {
      let adaptive_l = AdaptiveL::new(ADAPTIVE_L_SAMPLES, params.max_filtering_effort as f64)
        .map_err(|_| SearchError)?;
      let filter_params = search::InlineFilterSearch::new(knn_params, Some(adaptive_l));
      index
        .inner
        .filtered_search_vector(&ctx, query, filter_params, &mut output)
    };

    res.map_err(|_| SearchError)?;

    Ok(output.into_search_output())
  }

  /// 以既有元素为查询中心执行检索。
  fn run_element_search(
    &self,
    context: u64,
    index: &Index<S>,
    external_id: &[u8],
    params: SearchParams,
  ) -> Result<SearchOutput, SearchError> {
    let ctx = Context::new(context);
    let id = VectorSetId::from(external_id);

    // 束宽对齐 C# 原生 search_element（无 beam width 入参 → diskann 默认 1）
    let knn_params =
      search::Knn::new_default(params.search_exploration_factor).map_err(|_| SearchError)?;

    // 输出缓冲以 K 定容（对齐 C# EnsureIdBufferSize(outputIds, count)）
    let count = params.count.max(1);
    let mut ids = vec![0u8; count * (4 + 8)];
    let mut dists = vec![0f32; count];
    let mut output = SearchResults::new(count, &mut ids, &mut dists);

    let res = if params.filter_len == 0 {
      index
        .inner
        .search_element(&ctx, &id, knn_params, &mut output)
    } else {
      let adaptive_l = AdaptiveL::new(ADAPTIVE_L_SAMPLES, params.max_filtering_effort as f64)
        .map_err(|_| SearchError)?;
      let filter_params = search::InlineFilterSearch::new(knn_params, Some(adaptive_l));
      index
        .inner
        .filtered_search_element(&ctx, &id, filter_params, &mut output)
    };

    res.map_err(|_| SearchError)?;

    Ok(output.into_search_output())
  }

  /// diskann-garnet/DiskANNService.cs:SearchVector
  pub fn search_vector(
    &self,
    context: u64,
    vector: &[u8],
    params: SearchParams,
  ) -> Result<SearchOutput, SearchError> {
    let Some(index) = self.indexes.pin().get(&context).cloned() else {
      return Err(SearchError);
    };
    if vector.len() != index.expected_vector_len() {
      return Err(SearchError);
    }
    self.run_search(context, &index, vector, params)
  }

  /// diskann-garnet/DiskANNService.cs:SearchElement
  pub fn search_element(
    &self,
    context: u64,
    external_id: &[u8],
    params: SearchParams,
  ) -> Result<SearchOutput, SearchError> {
    let Some(index) = self.indexes.pin().get(&context).cloned() else {
      return Err(SearchError);
    };
    if !index
      .inner
      .external_id_exists(&Context::new(context), &VectorSetId::from(external_id))
    {
      return Err(SearchError);
    }
    self.run_element_search(context, &index, external_id, params)
  }

  /// diskann-garnet/DiskANNService.cs:CheckInternalIdValid
  pub fn check_internal_id_valid(&self, context: u64, internal_id: u32) -> bool {
    self.indexes.pin().get(&context).is_some_and(|i| {
      i.inner
        .internal_id_exists(&Context::new(context), internal_id)
    })
  }

  /// diskann-garnet/DiskANNService.cs:CheckExternalIdValid
  pub fn check_external_id_valid(&self, context: u64, external_id: &[u8]) -> bool {
    self.indexes.pin().get(&context).is_some_and(|i| {
      i.inner
        .external_id_exists(&Context::new(context), &VectorSetId::from(external_id))
    })
  }

  /// diskann-garnet/DiskANNService.cs:SetAttribute
  ///
  /// 空属性等价于删除（对标 set_attribute C 函数语义）。
  pub fn set_attribute(&self, context: u64, external_id: &[u8], attribute: &[u8]) -> bool {
    let Some(index) = self.indexes.pin().get(&context).cloned() else {
      return false;
    };
    let ctx = Context::new(context);
    let id = VectorSetId::from(external_id);

    if !index.inner.external_id_exists(&ctx, &id) {
      return false;
    }

    if attribute.is_empty() {
      index.inner.delete_attributes(&ctx, &id).is_ok()
    } else {
      index.inner.set_attributes(&ctx, &id, attribute).is_ok()
    }
  }

  /// 属性项读取（Attributes 项；以外部 id 解析）。
  pub fn get_attribute(&self, context: u64, external_id: &[u8]) -> Option<Vec<u8>> {
    let index = self.indexes.pin().get(&context).cloned()?;
    index
      .inner
      .attributes(&Context::new(context), &VectorSetId::from(external_id))
  }

  /// 完整向量读取（FullVector 项；原始字节）。
  pub fn get_full_vector(&self, context: u64, external_id: &[u8]) -> Option<Vec<u8>> {
    let index = self.indexes.pin().get(&context).cloned()?;
    index
      .inner
      .full_vector(&Context::new(context), &VectorSetId::from(external_id))
  }

  /// 嵌入向量展开为全精度 f32（VEMB 通道）。
  pub fn embedding_of(&self, context: u64, external_id: &[u8]) -> Option<Vec<f32>> {
    let index = self.indexes.pin().get(&context).cloned()?;
    index
      .inner
      .embedding(&Context::new(context), &VectorSetId::from(external_id))
  }

  /// 内部 id 解析通道（InternalIdMap 项读取）。
  pub fn internal_id_of(&self, context: u64, external_id: &[u8]) -> Option<u32> {
    let index = self.indexes.pin().get(&context).cloned()?;
    let ctx = Context::new(context);
    index
      .inner
      .external_id_exists(&ctx, &VectorSetId::from(external_id))
      .then(|| {
        // 外部 id 有效性成立时映射必存在；经 to_internal_id 解析
        index
          .inner
          .internal_id_of(&ctx, &VectorSetId::from(external_id))
      })
      .flatten()
  }

  /// 量化类型查询。
  pub fn quant_of(&self, context: u64) -> Option<VectorQuantType> {
    Some(self.indexes.pin().get(&context)?.quant_type)
  }

  /// 近似计数（已铸造的内部 id 总数）。
  pub fn card(&self, context: u64) -> u64 {
    self
      .indexes
      .pin()
      .get(&context)
      .map_or(0, |i| i.inner.approximate_count())
  }

  /// 层 0 邻接读取（VLINKS 通道）。
  pub fn links_of(&self, context: u64, external_id: &[u8]) -> Option<Vec<Vec<u8>>> {
    let index = self.indexes.pin().get(&context).cloned()?;
    let neighbors = index
      .inner
      .neighbors(&Context::new(context), &VectorSetId::from(external_id))
      .ok()?;
    Some(
      neighbors
        .iter()
        .map(|n| n.id().as_key_bytes().to_vec())
        .collect(),
    )
  }

  /// 随机取样元素外部 id（VRANDMEMBER 通道）。
  pub fn sample(&self, context: u64, count: usize) -> Vec<Vec<u8>> {
    let Some(index) = self.indexes.pin().get(&context).cloned() else {
      return Vec::new();
    };
    let mut ids = vec![0u8; count * (4 + 8)];
    let mut dists = vec![0f32; count];
    let mut output = SearchResults::new(count, &mut ids, &mut dists);
    if !index
      .inner
      .random_members(&Context::new(context), count as u32, &mut output)
    {
      return Vec::new();
    }

    let out = output.into_search_output();
    out.iter().map(|(id, _)| id.to_vec()).collect()
  }
}
