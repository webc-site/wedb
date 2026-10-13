//! 向量索引服务（对标 diskann-garnet 的 lib.rs + dyn_index.rs）
//!
//! [`DiskANNService`] 以 `(context)` 为键的并发注册表承接 C#
//! `DiskANNService`（P/Invoke 薄封装）的语义层：context → 索引实例，
//! 索引实例为类型擦除的 `DiskANNIndex<WedbProvider<T>>`（本地
//! runtime-agnostic diskann 的纯异步 façade，零 `block_on`），数据全部
//! 经存储回调持久化。
//!
//! 生命周期对标 diskann-garnet：
//! - `IndexState`：NoStartPoints → SettingStartPoints → Ready（起点状态机）
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

use async_lock::Mutex as AsyncMutex;
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
  error::{StoreError, WedbProviderError},
  provider::{DynamicQuantization, ToDistanceComputer, WedbProvider},
  store::{Callbacks, Context, LengthPrefixedIter, StoreCallbacks, Term, VectorSetId},
  types::{VectorDistanceMetricType, VectorQuantType},
};

mod ann_index;
mod index_lifecycle;
mod output;
mod search_params;

use ann_index::{AsyncDynIndex, DynIndex, IndexImpl};
pub use index_lifecycle::IndexConfig;
use index_lifecycle::{create_index_impl, ensure_index_ready_or_init};
pub use output::{SearchHit, SearchOutput, SearchResults};
pub use search_params::SearchParams;
use search_params::{filter_params, knn_params};

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

/// 检索查询中心：向量或既有元素（run_search 单点分派）
enum SearchQuery<'a> {
  Vector(&'a [u8]),
  Element(&'a [u8]),
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
  /// 本集合的插入线性化闸（每 context 一把，任务态锁，可跨 await 持有）。
  ///
  /// 图插入链（存在性预检 → set_element 四记录 → 图搜索/剪枝/回边）跨多个
  /// await 且非原子：webc-diskann `add_edge_and_prune` 的「读邻接（锁外）→
  /// 剪枝（锁外）→ set_neighbors 整写覆盖（条带锁内）」窗在同源邻接被并发
  /// 改写时按构造丢边（后写者以陈旧读基值覆盖先写者刚提交的回边），图
  /// 被打碎成弱连通碎片后自召回漏评。条带锁只能保证单记录 rmw 原子，罩
  /// 不住跨记录的读—算—写全程，故本集合的插入必须在更上层串行：
  ///   * 活路径（VADD 命令）已由 wnode 写臂每键独占锁线性化
  ///     （`read_or_create_vector_index_exclusive` 全程持锁，见
  ///     wnode vector_manager_locking.rs 写臂专用形态文档），本闸对其
  ///     零吞吐影响（同键 VADD 本就不交叠）；
  ///   * 绕过命令层的调用面（迁移导入 `import_migrated_element` 直调
  ///     `try_add`、测试直调 `DiskANNService::insert`）由本闸承接同一
  ///     线性化纪律，杜绝无锁并发图构建。
  insert_gate: AsyncMutex<()>,
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
  /// 的中间可观察态，`False` 仅承载插入期拒绝；本变体为 Rust 侧分步
  /// 写入面独有的独立错误态（本枚举一处产生，消费端一处映射进存储错误
  /// 通道，严禁折进 `False` 误报 Duplicate）。
  StoreError = 3,
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

    // 存在性判定读失败＝故障窗内无从判存，禁折「不存在」放行重复插入，
    // 走独立存储错误态（应答 ERR、不写 AOF）
    //
    // 插入线性化闸先于存在性预检取得、罩至属性写完成（见
    // [`Index::insert_gate`] 文档）：预检 → 图插入 → 属性全程同锁，绕过
    // 命令层独占锁的调用面（迁移导入/测试直调）同键插入在此串行，杜绝
    // 无锁并发图构建打碎图拓扑
    let _gate = index.insert_gate.lock().await;
    match index.inner.external_id_exists(&ctx, &id).await {
      Ok(true) => return DiskAnnInsertResult::False,
      Err(_) => return DiskAnnInsertResult::StoreError,
      Ok(false) => {}
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
      // （exists 为假），维持插入期拒绝语义；exists 判定本身读失败
      // （含回滚前双故障）一律存储错误态。摘除按外部 id 寻址的安全性
      // 前提：VADD 写臂持同键独占条带锁（read_or_create_vector_index_
      // exclusive），并发同键插入已在锁面串行化，本 id 不可能指到并发
      // 写者刚提交的活元素
      match index.inner.external_id_exists(&ctx, &id).await {
        Ok(true) => {
          // 尽力摘除；回滚失败属存储层双故障，应答同为存储错误
          let _ = index.inner.remove(&ctx, &id).await;
          return DiskAnnInsertResult::StoreError;
        }
        Err(_) => return DiskAnnInsertResult::StoreError,
        Ok(false) => {}
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
  ///
  /// 返回值三态：`Ok(true)`=删除成功，`Ok(false)`=索引/元素缺席（对标 C#
  /// TryRemove false→MissingElement），`Err`=存储读失败上抛（存在性判定
  /// 与图删除写失败均不得折叠成「不存在」假成功，store.rs 回调契约）。
  pub async fn remove(&self, context: u64, external_id: &[u8]) -> Result<bool, StoreError> {
    let Some(index) = self.index(context) else {
      return Ok(false);
    };
    let ctx = Context::new(context);
    let id = VectorSetId::from(external_id);

    if !index
      .inner
      .external_id_exists(&ctx, &id)
      .await
      .map_err(|_| StoreError::Read)?
    {
      return Ok(false);
    }

    index
      .inner
      .remove(&ctx, &id)
      .await
      .map(|_| true)
      .map_err(|_| StoreError::Delete)
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
    // 存在性判定读失败按检索报错收口（SearchError→命令 ERR），严禁折「元素
    // 不存在」假阴性应答
    if !index
      .inner
      .external_id_exists(&Context::new(context), &VectorSetId::from(external_id))
      .await
      .map_err(|_| SearchError)?
    {
      return Err(SearchError);
    }
    self
      .run_search(context, &index, SearchQuery::Element(external_id), params)
      .await
  }

  /// libs/server/Resp/Vector/DiskANNService.cs:CheckInternalIdValid
  ///
  /// `Err`=占用位图读失败上抛，禁止折叠成「无效」假阴性（store.rs 回调契约）。
  pub async fn check_internal_id_valid(
    &self,
    context: u64,
    internal_id: u32,
  ) -> Result<bool, StoreError> {
    let Some(index) = self.index(context) else {
      return Ok(false);
    };
    index
      .inner
      .internal_id_exists(&Context::new(context), internal_id)
      .await
      .map_err(|_| StoreError::Read)
  }

  /// libs/server/Resp/Vector/DiskANNService.cs:CheckExternalIdValid
  ///
  /// `Err`=存储读失败上抛，禁止折叠成「不存在」假阴性（store.rs 回调契约）。
  pub async fn check_external_id_valid(
    &self,
    context: u64,
    external_id: &[u8],
  ) -> Result<bool, StoreError> {
    let Some(index) = self.index(context) else {
      return Ok(false);
    };
    index
      .inner
      .external_id_exists(&Context::new(context), &VectorSetId::from(external_id))
      .await
      .map_err(|_| StoreError::Read)
  }

  /// libs/server/Resp/Vector/DiskANNService.cs:SetAttribute
  ///
  /// 空属性等价于删除（对标 set_attribute C 函数语义）。存在性判定读失败
  /// 与属性写失败均上抛，禁折「失败/不存在」假阴性（store.rs 回调契约）。
  pub async fn set_attribute(
    &self,
    context: u64,
    external_id: &[u8],
    attribute: &[u8],
  ) -> Result<bool, StoreError> {
    let Some(index) = self.index(context) else {
      return Ok(false);
    };
    let ctx = Context::new(context);
    let id = VectorSetId::from(external_id);

    if !index
      .inner
      .external_id_exists(&ctx, &id)
      .await
      .map_err(|_| StoreError::Read)?
    {
      return Ok(false);
    }

    if attribute.is_empty() {
      index
        .inner
        .delete_attributes(&ctx, &id)
        .await
        .map(|_| true)
        .map_err(|_| StoreError::Delete)
    } else {
      index
        .inner
        .set_attributes(&ctx, &id, attribute)
        .await
        .map(|_| true)
        .map_err(|_| StoreError::Write)
    }
  }

  /// 属性项读取（Attributes 项；以外部 id 解析）。
  ///
  /// `Ok(None)`=属性缺失合法稳态；`Err`=id 解析/向量项读失败（对命令面
  /// 消费臂维持缺席语义、对迁移导出面中止导出，按调用方分臂）。
  pub async fn get_attribute(
    &self,
    context: u64,
    external_id: &[u8],
  ) -> Result<Option<Vec<u8>>, StoreError> {
    let Some(index) = self.index(context) else {
      return Err(StoreError::Read);
    };
    index
      .inner
      .attributes(&Context::new(context), &VectorSetId::from(external_id))
      .await
      .map_err(|_| StoreError::Read)
  }

  /// 完整向量读取（FullVector 项；原始字节）。
  ///
  /// `Err`=读失败或缺失（单读契约下同形）：命令面消费臂 `.ok()` 回落缺席
  /// 应答（对标 C#），迁移导出面 `?` 即时中止，禁空载荷照常导出。
  pub async fn get_full_vector(
    &self,
    context: u64,
    external_id: &[u8],
  ) -> Result<Vec<u8>, StoreError> {
    let Some(index) = self.index(context) else {
      return Err(StoreError::Read);
    };
    index
      .inner
      .full_vector(&Context::new(context), &VectorSetId::from(external_id))
      .await
      .map_err(|_| StoreError::Read)
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
  ///
  /// 占用位图读失败 Err 上抛，禁止折叠成空集让导出静默丢整集（store.rs
  /// 回调契约 + sync_transport「严禁降级为空导出」口径）。
  pub async fn all_elements(&self, context: u64) -> Result<Vec<Vec<u8>>, StoreError> {
    let Some(index) = self.index(context) else {
      return Err(StoreError::Read);
    };
    index
      .inner
      .enumerate_elements(&Context::new(context))
      .await
      .map_err(|_| StoreError::Read)
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
  ///
  /// `Err`=存在性判定读失败上抛，禁折「缺席」假阴性；`Ok(None)`=元素缺席。
  pub async fn internal_id_of(
    &self,
    context: u64,
    external_id: &[u8],
  ) -> Result<Option<u32>, StoreError> {
    let Some(index) = self.index(context) else {
      return Ok(None);
    };
    let ctx = Context::new(context);
    let id = VectorSetId::from(external_id);
    if index
      .inner
      .external_id_exists(&ctx, &id)
      .await
      .map_err(|_| StoreError::Read)?
    {
      Ok(index.inner.internal_id_of(&ctx, &id).await)
    } else {
      Ok(None)
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
  ///
  /// 返回元素/距离对（WITHSCORES 透传遍历臂已算距离，零新机制）；None 仅
  /// 键/元素真缺席——悬垂邻接已在 neighbors 遍历臂跳过，不毒化整包。
  pub async fn links_of(&self, context: u64, external_id: &[u8]) -> Option<Vec<(Vec<u8>, f32)>> {
    let index = self.index(context)?;
    let neighbors = index
      .inner
      .neighbors(&Context::new(context), &VectorSetId::from(external_id))
      .await
      .ok()?;
    Some(
      neighbors
        .iter()
        .map(|n| (n.id().as_key_bytes().to_vec(), *n.distance()))
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
