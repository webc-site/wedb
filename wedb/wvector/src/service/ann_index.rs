//! ANN 索引类型擦除面：DiskANNIndex 纯异步 façade、DynIndex/AsyncDynIndex
//! trait 与 WedbProvider 实现、IndexImpl 三臂静态分派（对标 dyn_index.rs）

use super::*;

/// 原地删除的补邻居槽位数（webc-diskann `inplace_delete` 的 `num_to_replace`
/// 形参位；TwoHopAndOneHop 策略下被删点的补邻居上限，diskann-garnet 删除臂同形）。
const INPLACE_DELETE_NUM_TO_REPLACE: usize = 3;

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
/// 实现全落 crate 内静态分派类型（[`IndexImpl`]），不外露。
pub(crate) trait AsyncDynIndex: Send + Sync {
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

  /// 读元素属性（`Ok(None)`=属性缺失合法稳态；Err=存储读失败/元素缺席）。
  fn attributes(
    &self,
    context: &Context,
    id: &VectorSetId,
  ) -> impl Future<Output = ANNResult<Option<Vec<u8>>>> + Send;

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

  /// 按外部 id 判存在（存储读失败 Err 上抛，严禁折叠假阴性）。
  fn external_id_exists(
    &self,
    context: &Context,
    id: &VectorSetId,
  ) -> impl Future<Output = ANNResult<bool>> + Send;

  /// 读完整向量（原始字节；读失败/缺失 Err，由调用方按语义分臂）。
  fn full_vector(
    &self,
    context: &Context,
    id: &VectorSetId,
  ) -> impl Future<Output = ANNResult<Vec<u8>>> + Send;

  /// 读量化记录（原始字节，QuantizedVector 项）。
  fn quant_vector(
    &self,
    context: &Context,
    id: &VectorSetId,
  ) -> impl Future<Output = Option<Vec<u8>>> + Send;

  /// 枚举全部存活元素外部 id（fsm 占用位精确扫描，起点 0 排除；
  /// 占用位图读失败 Err 上抛，严禁折叠成空集）。
  fn enumerate_elements(
    &self,
    context: &Context,
  ) -> impl Future<Output = ANNResult<Vec<Vec<u8>>>> + Send;

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

  /// 按内部 id 判存在（存储读失败 Err 上抛，严禁折叠假阴性）。
  fn internal_id_exists(
    &self,
    context: &Context,
    id: u32,
  ) -> impl Future<Output = ANNResult<bool>> + Send;
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
        INPLACE_DELETE_NUM_TO_REPLACE,
        InplaceDeleteMethod::TwoHopAndOneHop,
      )
      .await
  }

  async fn attributes(&self, context: &Context, id: &VectorSetId) -> ANNResult<Option<Vec<u8>>> {
    self
      .inner
      .provider()
      .get_attributes(context, id)
      .await
      .map_err(|e| e.into())
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

  async fn external_id_exists(&self, context: &Context, id: &VectorSetId) -> ANNResult<bool> {
    self
      .inner
      .provider()
      .vector_id_exists(context, id)
      .await
      .map_err(|e| e.into())
  }

  async fn full_vector(&self, context: &Context, id: &VectorSetId) -> ANNResult<Vec<u8>> {
    let provider = self.inner.provider();
    let iid = provider.to_internal_id(context, id).await?;
    let v = provider.get_full_vector(context, iid).await?;
    Ok(bytemuck::cast_slice::<T, u8>(&v).to_vec())
  }

  async fn quant_vector(&self, context: &Context, id: &VectorSetId) -> Option<Vec<u8>> {
    let provider = self.inner.provider();
    let iid = provider.to_internal_id(context, id).await.ok()?;
    provider
      .callbacks
      .read_varsize_iid::<u8>(&context.term(Term::Quantized), iid)
      .await
  }

  async fn enumerate_elements(&self, context: &Context) -> ANNResult<Vec<Vec<u8>>> {
    let provider = self.inner.provider();
    let mut iids = Vec::new();
    // 占用位图读失败透明上抛（store.rs 回调契约），禁止折叠成空集让迁移
    // 导出静默丢整集
    provider
      .fsm
      .visit_used(context, |id| {
        // 起点 0 为图入口非用户元素
        if id != 0 {
          iids.push(id);
        }
        true
      })
      .await
      .map_err(WedbProviderError::from)?;
    let mut out = Vec::with_capacity(iids.len());
    for iid in iids {
      // ExtMap 缺失读=并发删除窗（delete_element 先清 ExtMap 后置空闲位）
      // 的合法瞬态，跳过该 id；读失败与缺失在单读契约（false=缺失）下同形，
      // 位图读失败已在上一步整体中止，不构成静默缺员
      if let Ok(eid) = provider.to_external_id(context, iid).await {
        out.push(eid.as_key_bytes().to_vec());
      }
    }
    Ok(out)
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

  async fn internal_id_exists(&self, context: &Context, id: u32) -> ANNResult<bool> {
    self
      .inner
      .provider()
      .vector_iid_exists(context, id)
      .await
      .map_err(|e| e.into())
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
    async fn attributes(&self, context: &Context, id: &VectorSetId) -> ANNResult<Option<Vec<u8>>>;
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
    async fn external_id_exists(&self, context: &Context, id: &VectorSetId) -> ANNResult<bool>;
    async fn full_vector(&self, context: &Context, id: &VectorSetId) -> ANNResult<Vec<u8>>;
    async fn quant_vector(&self, context: &Context, id: &VectorSetId) -> Option<Vec<u8>>;
    async fn enumerate_elements(&self, context: &Context) -> ANNResult<Vec<Vec<u8>>>;
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
    async fn internal_id_exists(&self, context: &Context, id: u32) -> ANNResult<bool>;
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
