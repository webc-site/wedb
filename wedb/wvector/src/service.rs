//! 向量索引服务（对标 diskann-garnet 的 lib.rs + dyn_index.rs）
//!
//! [`DiskANNService`] 以 `(context)` 为键的并发注册表承接 C#
//! `DiskANNService`（P/Invoke 薄封装）的语义层：context → 索引实例，
//! 索引实例为类型擦除的 [`DiskANNIndex<WedbProvider<T>>`]（本地
//! runtime-agnostic diskann 的纯异步 façade，零 `block_on`），数据全部
//! 经存储回调持久化。
//!
//! 生命周期对标 diskann-garnet：
//! - [`IndexState`]：NoStartPoints → SettingStartPoints → Ready（起点状态机）
//! - insert：ensure_index_ready_or_init → maybe_set_start_point → 图插入 →
//!   写属性（空属性跳过；写失败回滚摘除并报 StoreError）→
//!   量化就绪信号（SuccessStartTraining）
//! - search：Knn / InlineFilterSearch（AdaptiveL 自适应 L）+
//!   [`SearchResults`] 输出缓冲（id i32 长度前缀 + 距离）
//! - remove：inplace_delete（TwoHopAndOneHop）

use std::{
  future::Future,
  hint::spin_loop,
  mem,
  num::NonZeroUsize,
  sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
  },
};

use diskann_vector::distance::Metric;
use wbase::{future::yield_now, map::ConcurrentMap};
use webc_diskann::{
  ANNResult,
  graph::{
    BufferState, DiskANNIndex as GraphIndex, InplaceDeleteMethod, SearchOutputBuffer,
    config::{self, defaults::GRAPH_SLACK_FACTOR},
    index::SearchStats,
    search::{self, AdaptiveL},
  },
  neighbor::Neighbor,
  provider::DataProvider,
};

use crate::{
  error::WedbProviderError,
  provider::{DynamicQuantization, ToDistanceComputer, WedbProvider},
  store::{Callbacks, Context, LengthPrefixedIter, StoreCallbacks, Term, VectorSetId},
  types::{VectorDistanceMetricType, VectorQuantType},
};

/// 自适应 L 的取样数（对齐 diskann-garnet ADAPTIVE_L_SAMPLES）。
const ADAPTIVE_L_SAMPLES: usize = 1000;

/// id 长度前缀字节数（`[4B LE 长度][原始载荷]` 协议，对标 C#
/// `VectorIdFormat.I32LengthPrefixed`；消费端 RespServerSessionVectors.cs
/// WriteRESP2Result/WriteRESP3Result 以 `BinaryPrimitives.ReadInt32LittleEndian`
/// 逐项解包，故内联与溢出两条写出路径必须共用本协议）。
const ID_PREFIX_BYTES: usize = mem::size_of::<u32>();

/// 单元素内联预估容量（对标 C# VectorManager.cs:67
/// `MinimumSpacePerId = sizeof(int) + 8`）。
const MIN_SPACE_PER_ID: usize = ID_PREFIX_BYTES + 8;

/// 按 `[4B LE 长度][载荷]` 协议向目标切片写出一条 id——内联缓冲与
/// 溢出缓冲共用的唯一写出机制，杜绝两路编码漂移。
///
/// 调用方保证 `dst.len() == ID_PREFIX_BYTES + id.len()`。
#[inline]
fn write_length_prefixed(dst: &mut [u8], id: &[u8]) {
  let total = ID_PREFIX_BYTES + id.len();
  debug_assert_eq!(dst.len(), total);
  dst[..ID_PREFIX_BYTES].copy_from_slice(&(id.len() as u32).to_le_bytes());
  dst[ID_PREFIX_BYTES..total].copy_from_slice(id);
}

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

/// 检索查询中心：向量或既有元素（run_search 单点分派）
enum SearchQuery<'a> {
  Vector(&'a [u8]),
  Element(&'a [u8]),
}

/// 检索输出缓冲（对标 diskann-garnet lib.rs SearchResults）。
///
/// 全部 id 均按 [`write_length_prefixed`] 的 4 字节长度前缀协议串接；
/// `ids` 容量不足时余项进入 overflow 缓冲（C# 侧 overflow_results 语义；
/// wnode 以 [`SearchOutput`] 一次性合并承载，无需分页续检），溢出缓冲
/// 与内联缓冲共用同一编码，消费端 [`LengthPrefixedIter`] 可无差别解包。
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
    let found = self.pushed();
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

  /// 已推送元素数（内联 + 溢出，`current_len` 契约的单一实现，
  /// 与 `is_full` / `size_hint` 闭环）。
  #[inline]
  fn pushed(&self) -> usize {
    self.index + self.overflow_dists.len()
  }

  fn is_full(&self) -> bool {
    self.pushed() >= self.k
  }
}

impl SearchOutputBuffer<VectorSetId> for SearchResults<'_> {
  fn size_hint(&self) -> Option<usize> {
    Some(self.k - self.pushed())
  }

  fn push(&mut self, neighbor: Neighbor<VectorSetId>) -> BufferState {
    let (id, distance) = neighbor.as_tuple();
    let id_bytes = id.as_key_bytes();
    let total = ID_PREFIX_BYTES + id_bytes.len();

    if self.is_full() {
      return BufferState::Full;
    }
    if self.overflowing()
      || self.index >= self.dists.len()
      || self.id_index + total > self.ids.len()
    {
      // 溢出缓冲与内联缓冲共用同一写出协议
      let start = self.overflow_ids.len();
      self.overflow_ids.resize(start + total, 0);
      write_length_prefixed(&mut self.overflow_ids[start..], id_bytes);
      self.overflow_dists.push(distance);

      return if self.is_full() {
        BufferState::Full
      } else {
        BufferState::Available
      };
    }

    write_length_prefixed(
      &mut self.ids[self.id_index..self.id_index + total],
      id_bytes,
    );
    self.dists[self.index] = distance;
    self.index += 1;
    self.id_index += total;

    if self.is_full() {
      BufferState::Full
    } else {
      BufferState::Available
    }
  }

  fn current_len(&self) -> usize {
    self.pushed()
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

/// 纯异步索引 façade：直接持有 `Arc<graph::DiskANNIndex>`，在宿主（compio
/// TPC）事件循环上就地 await 驱动——零 `block_on`、零 runtime 句柄字段，
/// 天然 `Send + Sync`（取代官方 `wrapped_async` 同步包装；后者随 tokio
/// 退出库依赖树而移除）。
pub(crate) struct DiskANNIndex<DP: DataProvider> {
  /// 图索引实例（provider 访问器经 `.provider()` 取得）。
  pub inner: Arc<GraphIndex<DP>>,
}

impl<DP: DataProvider> DiskANNIndex<DP> {
  /// 纯异步构造：不建任何 runtime。
  ///
  /// thread_hint 传 `Some(MIN)`（即 1）：compio TPC 单线程串行 await，
  /// graph scratch 池容量 1 稳态复用（对位官方 wrapped_async
  /// new_with_current_thread_runtime 的取值），避免 insert/search 每次
  /// 全量重建 SearchScratch。
  pub fn new(config: config::Config, provider: DP) -> Self {
    Self {
      inner: Arc::new(GraphIndex::new(config, provider, Some(NonZeroUsize::MIN))),
    }
  }
}

/// 类型擦除的索引操作面（对标 diskann-garnet dyn_index.rs DynIndex）。
///
/// 纯同步派发面：仅保留零 I/O 的元数据查询；图操作（插入/删除/检索）
/// 全部收敛到 [`AsyncDynIndex`]，由宿主事件循环就地 await。
pub(crate) trait DynIndex: Send + Sync {
  /// 近似计数（已铸造的最大内部 id）。
  fn approximate_count(&self) -> u64;

  /// 是否需要调度量化。
  fn quantization_needed(&self) -> bool;
}

/// 存储执行域异步操作面（[`DynIndex`] 的 async 伴面；拆分原因见其注释）。
///
/// 方法均为存储 I/O 或图异步检索型。RPITIT + Send 声明，实现方以 `async fn` 满足签名。
pub trait AsyncDynIndex: Send + Sync {
  /// 插入向量（字节切片按元素类型重解释）。
  fn insert(
    &self,
    context: &Context,
    id: &VectorSetId,
    data: &[u8],
  ) -> impl Future<Output = ANNResult<()>> + Send;

  /// 删除元素。
  fn remove(
    &self,
    context: &Context,
    id: &VectorSetId,
  ) -> impl Future<Output = ANNResult<()>> + Send;

  /// 读元素属性。
  fn attributes(
    &self,
    context: &Context,
    id: &VectorSetId,
  ) -> impl Future<Output = Option<Vec<u8>>> + Send;

  /// 写元素属性。
  fn set_attributes(
    &self,
    context: &Context,
    id: &VectorSetId,
    data: &[u8],
  ) -> impl Future<Output = ANNResult<()>> + Send;

  /// 删除元素属性。
  fn delete_attributes(
    &self,
    context: &Context,
    id: &VectorSetId,
  ) -> impl Future<Output = ANNResult<()>> + Send;

  /// 以查询向量检索。
  fn search_vector<'a>(
    &'a self,
    context: &'a Context,
    data: &'a [u8],
    params: search::Knn,
    output: &'a mut SearchResults<'_>,
  ) -> impl Future<Output = ANNResult<SearchStats>> + Send + 'a;

  /// 以既有元素为查询中心检索。
  fn search_element<'a>(
    &'a self,
    context: &'a Context,
    id: &'a VectorSetId,
    params: search::Knn,
    output: &'a mut SearchResults<'_>,
  ) -> impl Future<Output = ANNResult<SearchStats>> + Send + 'a;

  /// 以查询向量做内联过滤检索。
  fn filtered_search_vector<'a>(
    &'a self,
    context: &'a Context,
    data: &'a [u8],
    params: search::InlineFilterSearch,
    output: &'a mut SearchResults<'_>,
  ) -> impl Future<Output = ANNResult<SearchStats>> + Send + 'a;

  /// 以既有元素为查询中心做内联过滤检索。
  fn filtered_search_element<'a>(
    &'a self,
    context: &'a Context,
    id: &'a VectorSetId,
    params: search::InlineFilterSearch,
    output: &'a mut SearchResults<'_>,
  ) -> impl Future<Output = ANNResult<SearchStats>> + Send + 'a;

  /// 按外部 id 判存在。
  fn external_id_exists(
    &self,
    context: &Context,
    id: &VectorSetId,
  ) -> impl Future<Output = bool> + Send;

  /// 读完整向量（原始字节）。
  fn full_vector(
    &self,
    context: &Context,
    id: &VectorSetId,
  ) -> impl Future<Output = Option<Vec<u8>>> + Send;

  /// 读量化记录（原始字节，QuantizedVector 项）。
  fn quant_vector(
    &self,
    context: &Context,
    id: &VectorSetId,
  ) -> impl Future<Output = Option<Vec<u8>>> + Send;

  /// 枚举全部存活元素外部 id（fsm 占用位精确扫描，起点 0 排除）。
  fn enumerate_elements(&self, context: &Context) -> impl Future<Output = Vec<Vec<u8>>> + Send;

  /// 读元素嵌入（展开为全精度 f32）。
  fn embedding(
    &self,
    context: &Context,
    id: &VectorSetId,
  ) -> impl Future<Output = Option<Vec<f32>>> + Send;

  /// 元素邻接表 + 距离。
  fn neighbors(
    &self,
    context: &Context,
    id: &VectorSetId,
  ) -> impl Future<Output = ANNResult<Vec<Neighbor<VectorSetId>>>> + Send;

  /// 随机取样元素。
  fn random_members<'a>(
    &'a self,
    context: &'a Context,
    count: u32,
    output: &'a mut SearchResults<'_>,
  ) -> impl Future<Output = bool> + Send + 'a;

  /// 外部 ID 解析为内部 ID。
  fn internal_id_of(
    &self,
    context: &Context,
    id: &VectorSetId,
  ) -> impl Future<Output = Option<u32>> + Send;

  /// 无起点时以 `data` 设起点。
  fn maybe_set_start_point(
    &self,
    context: &Context,
    data: &[u8],
  ) -> impl Future<Output = ANNResult<()>> + Send;

  /// 训练量化器。
  fn train_quantizer(&self, context: &Context) -> impl Future<Output = bool> + Send;

  /// 批量回填量化向量。
  fn backfill_quant_vectors(
    &self,
    context: &Context,
    task_idx: usize,
    task_count: usize,
  ) -> impl Future<Output = bool> + Send;

  /// 按内部 id 判存在。
  fn internal_id_exists(&self, context: &Context, id: u32) -> impl Future<Output = bool> + Send;
}

impl<T: ToDistanceComputer, S: StoreCallbacks> DynIndex for DiskANNIndex<WedbProvider<T, S>> {
  fn approximate_count(&self) -> u64 {
    self.inner.provider().count() as u64
  }

  fn quantization_needed(&self) -> bool {
    self.inner.provider().quantization_needed()
  }
}

impl<T: ToDistanceComputer, S: StoreCallbacks> AsyncDynIndex for DiskANNIndex<WedbProvider<T, S>> {
  async fn insert(&self, context: &Context, id: &VectorSetId, data: &[u8]) -> ANNResult<()> {
    self
      .inner
      .insert(
        &DynamicQuantization,
        context,
        id,
        bytemuck::cast_slice::<u8, T>(data),
      )
      .await
  }

  async fn remove(&self, context: &Context, id: &VectorSetId) -> ANNResult<()> {
    self
      .inner
      .inplace_delete(
        DynamicQuantization,
        context,
        id,
        3,
        InplaceDeleteMethod::TwoHopAndOneHop,
      )
      .await
  }

  async fn attributes(&self, context: &Context, id: &VectorSetId) -> Option<Vec<u8>> {
    self.inner.provider().get_attributes(context, id).await
  }

  async fn set_attributes(
    &self,
    context: &Context,
    id: &VectorSetId,
    data: &[u8],
  ) -> ANNResult<()> {
    self
      .inner
      .provider()
      .set_attributes(context, id, data)
      .await
      .map_err(|e| e.into())
  }

  async fn delete_attributes(&self, context: &Context, id: &VectorSetId) -> ANNResult<()> {
    self
      .inner
      .provider()
      .delete_attributes(context, id)
      .await
      .map_err(|e| e.into())
  }

  async fn search_vector<'a>(
    &'a self,
    context: &'a Context,
    data: &'a [u8],
    params: search::Knn,
    output: &'a mut SearchResults<'_>,
  ) -> ANNResult<SearchStats> {
    let query = bytemuck::cast_slice::<u8, T>(data);
    self
      .inner
      .search(params, &DynamicQuantization, context, query, output)
      .await
  }

  async fn search_element<'a>(
    &'a self,
    context: &'a Context,
    id: &'a VectorSetId,
    params: search::Knn,
    output: &'a mut SearchResults<'_>,
  ) -> ANNResult<SearchStats> {
    let iid = self.inner.provider().to_internal_id(context, id).await?;
    let data = self.inner.provider().get_full_vector(context, iid).await?;
    let data_bytes = bytemuck::cast_slice::<T, u8>(&data);
    self
      .search_vector(context, data_bytes, params, output)
      .await
  }

  async fn filtered_search_vector<'a>(
    &'a self,
    context: &'a Context,
    data: &'a [u8],
    params: search::InlineFilterSearch,
    output: &'a mut SearchResults<'_>,
  ) -> ANNResult<SearchStats> {
    let query = bytemuck::cast_slice::<u8, T>(data);
    self
      .inner
      .search(params, &DynamicQuantization, context, query, output)
      .await
  }

  async fn filtered_search_element<'a>(
    &'a self,
    context: &'a Context,
    id: &'a VectorSetId,
    params: search::InlineFilterSearch,
    output: &'a mut SearchResults<'_>,
  ) -> ANNResult<SearchStats> {
    let iid = self.inner.provider().to_internal_id(context, id).await?;
    let data = self.inner.provider().get_full_vector(context, iid).await?;
    let data_bytes = bytemuck::cast_slice::<T, u8>(&data);
    self
      .filtered_search_vector(context, data_bytes, params, output)
      .await
  }

  async fn external_id_exists(&self, context: &Context, id: &VectorSetId) -> bool {
    self.inner.provider().vector_id_exists(context, id).await
  }

  async fn full_vector(&self, context: &Context, id: &VectorSetId) -> Option<Vec<u8>> {
    let provider = self.inner.provider();
    let iid = provider.to_internal_id(context, id).await.ok()?;
    let v = provider.get_full_vector(context, iid).await.ok()?;
    Some(bytemuck::cast_slice::<T, u8>(&v).to_vec())
  }

  async fn quant_vector(&self, context: &Context, id: &VectorSetId) -> Option<Vec<u8>> {
    let provider = self.inner.provider();
    let iid = provider.to_internal_id(context, id).await.ok()?;
    provider
      .callbacks
      .read_varsize_iid::<u8>(&context.term(Term::Quantized), iid)
      .await
  }

  async fn enumerate_elements(&self, context: &Context) -> Vec<Vec<u8>> {
    let provider = self.inner.provider();
    let mut iids = Vec::new();
    if provider
      .fsm
      .visit_used(context, |id| {
        // 起点 0 为图入口非用户元素
        if id != 0 {
          iids.push(id);
        }
        true
      })
      .await
      .is_err()
    {
      return Vec::new();
    }
    let mut out = Vec::with_capacity(iids.len());
    for iid in iids {
      if let Ok(eid) = provider.to_external_id(context, iid).await {
        out.push(eid.as_key_bytes().to_vec());
      }
    }
    out
  }

  async fn embedding(&self, context: &Context, id: &VectorSetId) -> Option<Vec<f32>> {
    let provider = self.inner.provider();
    let iid = provider.to_internal_id(context, id).await.ok()?;
    let v = provider.get_full_vector(context, iid).await.ok()?;
    let f = T::as_f32(&v).ok()?;
    Some(f.iter().copied().collect())
  }

  async fn neighbors(
    &self,
    context: &Context,
    id: &VectorSetId,
  ) -> ANNResult<Vec<Neighbor<VectorSetId>>> {
    self.inner.provider().neighbors(context, id).await
  }

  async fn random_members<'a>(
    &'a self,
    context: &'a Context,
    count: u32,
    output: &'a mut SearchResults<'_>,
  ) -> bool {
    self
      .inner
      .provider()
      .random_members(context, count, output)
      .await
  }

  async fn internal_id_of(&self, context: &Context, id: &VectorSetId) -> Option<u32> {
    self.inner.provider().to_internal_id(context, id).await.ok()
  }

  async fn maybe_set_start_point(&self, context: &Context, data: &[u8]) -> ANNResult<()> {
    self
      .inner
      .provider()
      .maybe_set_start_point(context, bytemuck::cast_slice::<u8, T>(data))
      .await
      .map_err(|e| e.into())
  }

  async fn train_quantizer(&self, context: &Context) -> bool {
    self.inner.provider().train_quantizer(context).await
  }

  async fn backfill_quant_vectors(
    &self,
    context: &Context,
    task_idx: usize,
    task_count: usize,
  ) -> bool {
    self
      .inner
      .provider()
      .backfill_quant_vectors(context, task_idx, task_count)
      .await
  }

  async fn internal_id_exists(&self, context: &Context, id: u32) -> bool {
    self.inner.provider().vector_iid_exists(context, id).await
  }
}

/// [`IndexImpl`] 三臂静态分派宏（enum_dispatch 不支持 RPITIT，手写保零动态分派）：
/// 展开 `match self { U8/I8/F32(i) => i.同名方法(参数…) [.await] }`；async 与同步
/// `fn` 各一 arm，同步仅 `&self` 零参。
macro_rules! fwd {
  ($(
    async fn $name:ident $(<$lt:lifetime>)? (
      & $($slt:lifetime)? self $(, $($arg:ident : $ty:ty),* $(,)?)?
    ) -> $ret:ty;
  )*) => {
    $(
      async fn $name $(<$lt>)? (
        & $($slt)? self $(, $($arg: $ty),*)?
      ) -> $ret {
        match self {
          Self::U8(i) => i.$name($( $($arg),* )?).await,
          Self::I8(i) => i.$name($( $($arg),* )?).await,
          Self::F32(i) => i.$name($( $($arg),* )?).await,
        }
      }
    )*
  };
  ($(
    fn $name:ident (&self) -> $ret:ty;
  )*) => {
    $(
      fn $name(&self) -> $ret {
        match self {
          Self::U8(i) => i.$name(),
          Self::I8(i) => i.$name(),
          Self::F32(i) => i.$name(),
        }
      }
    )*
  };
}

/// [`AsyncDynIndex`] 的枚举静态分派（三臂 match 由 [`fwd!`] 生成，零动态分派）。
impl<S: StoreCallbacks> AsyncDynIndex for IndexImpl<S> {
  fwd! {
    async fn insert(&self, context: &Context, id: &VectorSetId, data: &[u8]) -> ANNResult<()>;
    async fn remove(&self, context: &Context, id: &VectorSetId) -> ANNResult<()>;
    async fn attributes(&self, context: &Context, id: &VectorSetId) -> Option<Vec<u8>>;
    async fn set_attributes(
      &self, context: &Context, id: &VectorSetId, data: &[u8],
    ) -> ANNResult<()>;
    async fn delete_attributes(&self, context: &Context, id: &VectorSetId) -> ANNResult<()>;
    async fn search_vector<'a>(
      &'a self, context: &'a Context, data: &'a [u8], params: search::Knn,
      output: &'a mut SearchResults<'_>,
    ) -> ANNResult<SearchStats>;
    async fn search_element<'a>(
      &'a self, context: &'a Context, id: &'a VectorSetId, params: search::Knn,
      output: &'a mut SearchResults<'_>,
    ) -> ANNResult<SearchStats>;
    async fn filtered_search_vector<'a>(
      &'a self, context: &'a Context, data: &'a [u8], params: search::InlineFilterSearch,
      output: &'a mut SearchResults<'_>,
    ) -> ANNResult<SearchStats>;
    async fn filtered_search_element<'a>(
      &'a self, context: &'a Context, id: &'a VectorSetId, params: search::InlineFilterSearch,
      output: &'a mut SearchResults<'_>,
    ) -> ANNResult<SearchStats>;
    async fn external_id_exists(&self, context: &Context, id: &VectorSetId) -> bool;
    async fn full_vector(&self, context: &Context, id: &VectorSetId) -> Option<Vec<u8>>;
    async fn quant_vector(&self, context: &Context, id: &VectorSetId) -> Option<Vec<u8>>;
    async fn enumerate_elements(&self, context: &Context) -> Vec<Vec<u8>>;
    async fn embedding(&self, context: &Context, id: &VectorSetId) -> Option<Vec<f32>>;
    async fn neighbors(
      &self, context: &Context, id: &VectorSetId,
    ) -> ANNResult<Vec<Neighbor<VectorSetId>>>;
    async fn random_members<'a>(
      &'a self, context: &'a Context, count: u32, output: &'a mut SearchResults<'_>,
    ) -> bool;
    async fn internal_id_of(&self, context: &Context, id: &VectorSetId) -> Option<u32>;
    async fn maybe_set_start_point(&self, context: &Context, data: &[u8]) -> ANNResult<()>;
    async fn train_quantizer(&self, context: &Context) -> bool;
    async fn backfill_quant_vectors(
      &self, context: &Context, task_idx: usize, task_count: usize,
    ) -> bool;
    async fn internal_id_exists(&self, context: &Context, id: u32) -> bool;
  }
}

/// 静态分派的索引具体实现。
pub(crate) enum IndexImpl<S: StoreCallbacks> {
  U8(DiskANNIndex<WedbProvider<u8, S>>),
  I8(DiskANNIndex<WedbProvider<i8, S>>),
  F32(DiskANNIndex<WedbProvider<f32, S>>),
}

impl<S: StoreCallbacks> DynIndex for IndexImpl<S> {
  fwd! {
    fn approximate_count(&self) -> u64;
    fn quantization_needed(&self) -> bool;
  }
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
  /// 存储写失败（起点装载 / 图阶段失败 / 属性写失败——后两者均已先行
  /// 回滚摘除）。
  ///
  /// C# 属性随原生 insert 一次性落盘，不存在「图已插入、属性单独写失败」
  /// 的中间可观察态，[`False`] 仅承载插入期拒绝；本变体为 Rust 侧分步
  /// 写入面独有的独立错误态（本枚举一处产生，消费端一处映射进存储错误
  /// 通道，严禁折进 [`False`] 误报 Duplicate）。
  StoreError = 3,
}

/// 保障索引就绪（无起点时运行 `init` 设起点），失败返回错误。
///
/// 异步形态：起点装载经存储回调（async）闭环。自旋让位改协程协作
/// `yield_now().await`（同步 `thread::yield_now` 在任务栈内让出的是线程
/// 时间片而非执行权，compio 任务队列无法推进）。
async fn ensure_index_ready_or_init<S: StoreCallbacks, F, E>(index: &Index<S>, init: F) -> Option<E>
where
  F: AsyncFnOnce() -> Option<E>,
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
          yield_now().await;
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
          if let Some(err) = init().await {
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
async fn create_index_impl<T: ToDistanceComputer, S: StoreCallbacks>(
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
  let provider = WedbProvider::<T, S>::new(
    dim,
    params.reduce_dims,
    quant_type,
    metric_type,
    max_degree,
    callbacks,
    context,
  )
  .await?;
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
  let index_inst = DiskANNIndex::new(config, provider);
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

/// 装配 KNN 检索参数（束宽对齐 C# 原生 search_vector/search_element：
/// 无 beam width 入参 → diskann 默认 1）。
fn knn_params(params: &SearchParams) -> Result<search::Knn, SearchError> {
  search::Knn::new_default(params.search_exploration_factor).map_err(|_| SearchError)
}

/// 装配内联过滤检索参数（AdaptiveL 自适应 L，effort = maxFilteringEffort）。
fn filter_params(
  params: &SearchParams,
  knn: search::Knn,
) -> Result<search::InlineFilterSearch, SearchError> {
  let adaptive = AdaptiveL::new(ADAPTIVE_L_SAMPLES, params.max_filtering_effort as f64)
    .map_err(|_| SearchError)?;
  Ok(search::InlineFilterSearch::new(knn, Some(adaptive)))
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
  /// 取 context 的索引实例（注册表读路径单源）。
  fn index(&self, context: u64) -> Option<Arc<Index<S>>> {
    self.indexes.pin().get(&context).cloned()
  }

  /// libs/server/Resp/Vector/DiskANNService.cs:CreateIndex
  ///
  /// libs/server/Resp/Vector/DiskANNService.cs:RecreateIndex 合并承接：
  /// C# RecreateIndex 即纯转发 `=> CreateIndex(...)`（迁移/恢复后同参数
  /// 重建），rust 重建路径（wnode recreate_index_locked）直调本入口。
  ///
  /// 为 context 建立索引；几何按官方 diskann config 装配
  /// （target_degree = max_degree / GRAPH_SLACK_FACTOR，MaxDegree::Value(max_degree)）。
  /// 返回是否需要调度量化建表（C# out quantizationRequested 语义）。
  #[inline]
  pub async fn create_index(
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
          .await
          .map_err(|_| CreateIndexError)
      }
      VectorQuantType::XnoQuantI8 | VectorQuantType::XbinI8 => {
        create_index_impl::<i8, S>(&params, config, metric_type, callbacks, &ctx, IndexImpl::I8)
          .await
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
        .await
        .map_err(|_| CreateIndexError)
      }
    };

    let (index, quant_needed) = created?;
    self.indexes.pin().insert(context, index);
    Ok(quant_needed)
  }

  /// libs/server/Resp/Vector/DiskANNService.cs:DropIndex
  pub fn drop_index(&self, context: u64) {
    self.indexes.pin().remove(&context);
  }

  /// libs/server/Resp/Vector/DiskANNService.cs:Insert
  ///
  /// libs/server/Resp/Vector/DiskANNService.cs:insert 合并承接：C# 小写
  /// `insert` 是 diskann_garnet 原生库 P/Invoke 声明（LibraryImport），rust
  /// 直链官方 diskann crate（下方 `index.inner.insert`），原生 FFI 边界与
  /// 托管控件层在本函数合一。
  ///
  /// 起点保障 → 图插入 → 写属性（失败回滚摘除）→ 量化就绪判定（对标
  /// insert C 函数；C# 属性随原生 insert 一次落盘无中间态，rust 分步写入
  /// 以 [`DiskAnnInsertResult::StoreError`] 承载存储失败）。
  pub async fn insert(
    &self,
    context: u64,
    external_id: &[u8],
    vector: &[u8],
    attributes: &[u8],
  ) -> DiskAnnInsertResult {
    let Some(index) = self.index(context) else {
      return DiskAnnInsertResult::False;
    };
    let ctx = Context::new(context);
    let id = VectorSetId::from(external_id);

    if vector.len() != index.expected_vector_len() {
      return DiskAnnInsertResult::False;
    }

    if index.inner.external_id_exists(&ctx, &id).await {
      return DiskAnnInsertResult::False;
    }

    // 起点装载失败 = 存储写失败，非插入期拒绝 → 独立存储错误态
    if ensure_index_ready_or_init(&index, async || {
      index.inner.maybe_set_start_point(&ctx, vector).await.err()
    })
    .await
    .is_some()
    {
      return DiskAnnInsertResult::StoreError;
    }

    let old_ready = ctx.quantizer_ready();

    if index.inner.insert(&ctx, &id, vector).await.is_err() {
      // 图阶段失败（set_element 已落四记录与 fsm 占用位）⇒ 记录已持久，
      // 先回滚摘除收敛回「未插入」再报存储错误——严禁折 False 误报
      // Duplicate（重试恒拒绝、存储故障信号被吞，与属性臂同机制）。
      // set_element 自身失败已由 provider 失败出口逆序清道，存储无残留
      // （exists 为假），维持插入期拒绝语义
      if index.inner.external_id_exists(&ctx, &id).await {
        // 尽力摘除；回滚失败属存储层双故障，应答同为存储错误
        let _ = index.inner.remove(&ctx, &id).await;
        return DiskAnnInsertResult::StoreError;
      }
      return DiskAnnInsertResult::False;
    }

    // 属性在插入后写入（以内部 id 为键）；空属性不发起写调用
    if !attributes.is_empty()
      && index
        .inner
        .set_attributes(&ctx, &id, attributes)
        .await
        .is_err()
    {
      // 属性写失败 ⇒ 元素已提交图结构，先回滚摘除（inplace_delete 清理
      // 图连接、五族记录与 ID 映射、释放 fsm 槽位），存储收敛回「未插入」
      // 再报存储错误——严禁误报 Duplicate 造成应答与存储分叉。回滚失败属
      // 存储层双故障，应答同为存储错误（尽力摘除，不再二次隐藏）
      let _ = index.inner.remove(&ctx, &id).await;
      return DiskAnnInsertResult::StoreError;
    }

    let ready = ctx.quantizer_ready();
    if !old_ready && ready {
      DiskAnnInsertResult::QuantizationRequested
    } else {
      DiskAnnInsertResult::True
    }
  }

  /// libs/server/Resp/Vector/DiskANNService.cs:Remove
  ///
  /// libs/server/Resp/Vector/DiskANNService.cs:remove 合并承接：C# 小写
  /// `remove` 为原生库 P/Invoke 声明，rust 直链官方 diskann crate（下方
  /// `index.inner.remove`），同 insert 注。
  pub async fn remove(&self, context: u64, external_id: &[u8]) -> bool {
    let Some(index) = self.index(context) else {
      return false;
    };
    let ctx = Context::new(context);
    let id = VectorSetId::from(external_id);

    if !index.inner.external_id_exists(&ctx, &id).await {
      return false;
    }

    index.inner.remove(&ctx, &id).await.is_ok()
  }

  /// libs/server/Resp/Vector/DiskANNService.cs:BuildQuantizationTable
  pub async fn build_quantization_table(&self, context: u64) -> bool {
    let Some(index) = self.index(context) else {
      return false;
    };
    index.inner.train_quantizer(&Context::new(context)).await
  }

  /// libs/server/Resp/Vector/DiskANNService.cs:BackfillQuantizedVectors
  pub async fn backfill_quantized_vectors(
    &self,
    context: u64,
    task_index: usize,
    task_count: usize,
  ) {
    if let Some(index) = self.index(context) {
      index
        .inner
        .backfill_quant_vectors(&Context::new(context), task_index, task_count)
        .await;
    }
  }

  /// 是否需要调度量化建表。
  pub fn needs_quantization(&self, context: u64) -> bool {
    self
      .index(context)
      .is_some_and(|i| i.inner.quantization_needed())
  }

  /// 执行检索并物化输出（Knn / InlineFilterSearch 分派 + overflow 合并）。
  async fn run_search(
    &self,
    context: u64,
    index: &Index<S>,
    query: SearchQuery<'_>,
    params: SearchParams,
  ) -> Result<SearchOutput, SearchError> {
    let ctx = Context::new(context);
    let knn = knn_params(&params)?;
    // 输出缓冲以 K 定容（对齐 C# EnsureIdBufferSize(outputIds, count)）：
    // id 至少 4B 前缀 + 8B id，距离 count × f32
    let count = params.count.max(1);
    let mut ids = vec![0u8; count * MIN_SPACE_PER_ID];
    let mut dists = vec![0f32; count];
    let mut output = SearchResults::new(count, &mut ids, &mut dists);

    let res = match query {
      SearchQuery::Vector(q) if params.filter_len == 0 => {
        index.inner.search_vector(&ctx, q, knn, &mut output).await
      }
      SearchQuery::Vector(q) => {
        let filter = filter_params(&params, knn)?;
        index
          .inner
          .filtered_search_vector(&ctx, q, filter, &mut output)
          .await
      }
      SearchQuery::Element(eid) if params.filter_len == 0 => {
        let id = VectorSetId::from(eid);
        index
          .inner
          .search_element(&ctx, &id, knn, &mut output)
          .await
      }
      SearchQuery::Element(eid) => {
        let filter = filter_params(&params, knn)?;
        let id = VectorSetId::from(eid);
        index
          .inner
          .filtered_search_element(&ctx, &id, filter, &mut output)
          .await
      }
    };
    res.map_err(|_| SearchError)?;
    Ok(output.into_search_output())
  }

  /// libs/server/Resp/Vector/DiskANNService.cs:SearchVector
  pub async fn search_vector(
    &self,
    context: u64,
    vector: &[u8],
    params: SearchParams,
  ) -> Result<SearchOutput, SearchError> {
    let Some(index) = self.index(context) else {
      return Err(SearchError);
    };
    if vector.len() != index.expected_vector_len() {
      return Err(SearchError);
    }
    self
      .run_search(context, &index, SearchQuery::Vector(vector), params)
      .await
  }

  /// libs/server/Resp/Vector/DiskANNService.cs:SearchElement
  pub async fn search_element(
    &self,
    context: u64,
    external_id: &[u8],
    params: SearchParams,
  ) -> Result<SearchOutput, SearchError> {
    let Some(index) = self.index(context) else {
      return Err(SearchError);
    };
    if !index
      .inner
      .external_id_exists(&Context::new(context), &VectorSetId::from(external_id))
      .await
    {
      return Err(SearchError);
    }
    self
      .run_search(context, &index, SearchQuery::Element(external_id), params)
      .await
  }

  /// libs/server/Resp/Vector/DiskANNService.cs:CheckInternalIdValid
  pub async fn check_internal_id_valid(&self, context: u64, internal_id: u32) -> bool {
    let Some(index) = self.index(context) else {
      return false;
    };
    index
      .inner
      .internal_id_exists(&Context::new(context), internal_id)
      .await
  }

  /// libs/server/Resp/Vector/DiskANNService.cs:CheckExternalIdValid
  pub async fn check_external_id_valid(&self, context: u64, external_id: &[u8]) -> bool {
    let Some(index) = self.index(context) else {
      return false;
    };
    index
      .inner
      .external_id_exists(&Context::new(context), &VectorSetId::from(external_id))
      .await
  }

  /// libs/server/Resp/Vector/DiskANNService.cs:SetAttribute
  ///
  /// 空属性等价于删除（对标 set_attribute C 函数语义）。
  pub async fn set_attribute(&self, context: u64, external_id: &[u8], attribute: &[u8]) -> bool {
    let Some(index) = self.index(context) else {
      return false;
    };
    let ctx = Context::new(context);
    let id = VectorSetId::from(external_id);

    if !index.inner.external_id_exists(&ctx, &id).await {
      return false;
    }

    if attribute.is_empty() {
      index.inner.delete_attributes(&ctx, &id).await.is_ok()
    } else {
      index
        .inner
        .set_attributes(&ctx, &id, attribute)
        .await
        .is_ok()
    }
  }

  /// 属性项读取（Attributes 项；以外部 id 解析）。
  pub async fn get_attribute(&self, context: u64, external_id: &[u8]) -> Option<Vec<u8>> {
    let index = self.index(context)?;
    index
      .inner
      .attributes(&Context::new(context), &VectorSetId::from(external_id))
      .await
  }

  /// 完整向量读取（FullVector 项；原始字节）。
  pub async fn get_full_vector(&self, context: u64, external_id: &[u8]) -> Option<Vec<u8>> {
    let index = self.index(context)?;
    index
      .inner
      .full_vector(&Context::new(context), &VectorSetId::from(external_id))
      .await
  }

  /// 量化记录读取（QuantizedVector 项；原始字节，VEMB RAW 量化臂通道）。
  pub async fn get_quant_vector(&self, context: u64, external_id: &[u8]) -> Option<Vec<u8>> {
    let index = self.index(context)?;
    index
      .inner
      .quant_vector(&Context::new(context), &VectorSetId::from(external_id))
      .await
  }

  /// 存活元素枚举（fsm.visit_used 精确占用位扫描，起点 0 排除；迁移导出面）。
  pub async fn all_elements(&self, context: u64) -> Vec<Vec<u8>> {
    let Some(index) = self.index(context) else {
      return Vec::new();
    };
    index.inner.enumerate_elements(&Context::new(context)).await
  }

  /// 嵌入向量展开为全精度 f32（VEMB 通道）。
  pub async fn embedding_of(&self, context: u64, external_id: &[u8]) -> Option<Vec<f32>> {
    let index = self.index(context)?;
    index
      .inner
      .embedding(&Context::new(context), &VectorSetId::from(external_id))
      .await
  }

  /// 内部 id 解析通道（InternalIdMap 项读取）。
  pub async fn internal_id_of(&self, context: u64, external_id: &[u8]) -> Option<u32> {
    let index = self.index(context)?;
    let ctx = Context::new(context);
    let id = VectorSetId::from(external_id);
    if index.inner.external_id_exists(&ctx, &id).await {
      index.inner.internal_id_of(&ctx, &id).await
    } else {
      None
    }
  }

  /// 量化类型查询。
  pub fn quant_of(&self, context: u64) -> Option<VectorQuantType> {
    Some(self.indexes.pin().get(&context)?.quant_type)
  }

  /// 近似计数（已铸造的内部 id 总数）。
  pub fn card(&self, context: u64) -> u64 {
    self
      .index(context)
      .map_or(0, |i| i.inner.approximate_count())
  }

  /// 层 0 邻接读取（VLINKS 通道）。
  pub async fn links_of(&self, context: u64, external_id: &[u8]) -> Option<Vec<Vec<u8>>> {
    let index = self.index(context)?;
    let neighbors = index
      .inner
      .neighbors(&Context::new(context), &VectorSetId::from(external_id))
      .await
      .ok()?;
    Some(
      neighbors
        .iter()
        .map(|n| n.id().as_key_bytes().to_vec())
        .collect(),
    )
  }

  /// 随机取样元素外部 id（VRANDMEMBER 通道）。
  pub async fn sample(&self, context: u64, count: usize) -> Vec<Vec<u8>> {
    let Some(index) = self.index(context) else {
      return Vec::new();
    };
    let mut ids = vec![0u8; count * MIN_SPACE_PER_ID];
    let mut dists = vec![0f32; count];
    let mut output = SearchResults::new(count, &mut ids, &mut dists);
    if !index
      .inner
      .random_members(&Context::new(context), count as u32, &mut output)
      .await
    {
      return Vec::new();
    }

    let out = output.into_search_output();
    out.iter().map(|(id, _)| id.to_vec()).collect()
  }
}
