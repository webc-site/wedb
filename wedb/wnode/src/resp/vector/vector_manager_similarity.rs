//! 相似度检索族（对标 libs/server/Resp/Vector/VectorManager.cs 的
//! ValueSimilarity / ElementSimilarity / 共享检索段与输出组装），
//! 自 vector_manager.rs 核心拆出。

use wvector::{
  SearchHit, SearchParams, VectorIdFormat, VectorValueType, prepare_vector_data,
  store::StoreCallbacks, try_compile,
};

use super::{
  types::{
    ERR_COMPILING_FILTER, ERR_VECTOR_SERVICE_RESPONSE, ERR_VECTOR_SET_INDEX, SimilarityOutput,
    SimilarityQuery, VectorManagerResult, VectorOpError, VectorSearchOptions, err,
  },
  vector_manager::VectorManager,
  vector_manager_element_data::AttributeView,
  vector_manager_filter::InlineFilterSearchBound,
  vector_manager_index::Index,
};

impl<S: StoreCallbacks> VectorManager<S> {
  // ======================== 相似度检索 ========================

  /// libs/server/Resp/Vector/VectorManager.cs:ValueSimilarity
  ///
  /// 以查询向量做相似度检索；`filter` 非空时执行内联过滤 + 结果位图，
  /// 候选队列按 `max_filtering_effort` 放大；`delta` 为最大距离截断（EPSILON）。
  pub async fn value_similarity(
    &self,
    index_value: &[u8],
    value_type: VectorValueType,
    values: &[u8],
    opts: &VectorSearchOptions<'_>,
  ) -> Result<SimilarityOutput, VectorOpError> {
    self.assert_have_storage_session();

    let Some(index) = Index::from_bytes(index_value) else {
      return err(VectorManagerResult::BadParams, ERR_VECTOR_SET_INDEX);
    };

    // 查询向量规约
    let prepared = match prepare_vector_data(index.quant_type, value_type, values) {
      Ok(p) => p,
      Err(e) => return err(VectorManagerResult::BadParams, e.message()),
    };
    if prepared.element_count != index.dimensions as usize {
      return err(
        VectorManagerResult::BadParams,
        b"ERR Dimensions provided do not match Vector Set dimensions",
      );
    }

    self
      .similarity_search(&index, opts, SimilarityQuery::Vector(&prepared.bytes))
      .await
  }

  /// libs/server/Resp/Vector/VectorManager.cs:ElementSimilarity
  ///
  /// 以既有元素为查询中心做相似度检索（过滤/截断语义同 [`Self::value_similarity`]）。
  /// 刻意偏差：元素存在性前置检查激活 C# 会话层死分支文案，见 `doc/zh/deviations.md` §79。
  pub async fn element_similarity(
    &self,
    index_value: &[u8],
    element: &[u8],
    opts: &VectorSearchOptions<'_>,
  ) -> Result<SimilarityOutput, VectorOpError> {
    self.assert_have_storage_session();

    let Some(index) = Index::from_bytes(index_value) else {
      return err(VectorManagerResult::BadParams, ERR_VECTOR_SET_INDEX);
    };

    // 刻意偏差（见 doc/zh/deviations.md §79）：前置元素存在性检查激活 C# 会话层死分支
    // 文案 "Element not in Vector Set"（C# 实际回落量化 mismatch 误导文案；严禁回改）；
    // 判定读失败上抛 Err（存储错误帧），禁折「元素不存在」假阴性
    match self
      .service
      .check_external_id_valid(index.context, element)
      .await
    {
      Ok(true) => {}
      Ok(false) => {
        return err(
          VectorManagerResult::MissingElement,
          super::resp_server_session_vectors::ERR_ELEMENT_NOT_IN_SET,
        );
      }
      Err(e) => {
        log::error!("元素相似度存在性判定读失败: {e}");
        return err(VectorManagerResult::Invalid, ERR_VECTOR_SERVICE_RESPONSE);
      }
    }

    self
      .similarity_search(&index, opts, SimilarityQuery::Element(element))
      .await
  }

  /// 相似度检索的共享检索段（[`Self::value_similarity`] 与
  /// [`Self::element_similarity`] 两路唯一实现）：两路差异仅查询载荷与前置
  /// 校验（留在各自入口），本段承接有效 EF 折算 → 过滤程序单次编译 → 守卫下
  /// 服务调用 → EPSILON 截断 → 输出组装，错误文案与调用次序逐字保持原双路实现。
  async fn similarity_search(
    &self,
    index: &Index,
    opts: &VectorSearchOptions<'_>,
    query: SimilarityQuery<'_>,
  ) -> Result<SimilarityOutput, VectorOpError> {
    // 编译一次并持有程序（内联过滤装配与此校验同源，禁二次编译）；口径
    // 不变：非法 FILTER → ERR Compiling filter failed
    let program = if opts.filter.is_empty() {
      None
    } else {
      match try_compile(opts.filter) {
        Ok(program) => Some(program),
        Err(_) => return err(VectorManagerResult::BadParams, ERR_COMPILING_FILTER),
      }
    };

    let search_params = SearchParams {
      count: opts.count,
      search_exploration_factor: effective_search_ef(opts),
      filter_len: opts.filter.len(),
      max_filtering_effort: opts.max_filtering_effort,
    };

    // 内联过滤装配（对标 C# ValueSimilarity 的 filterState 构造 →
    // InlineFilterStatePtr = &filterState → finally 置 null，
    // libs/server/Resp/Vector/VectorManager.cs:862-905 与 ElementSimilarity
    // 的同构装配 :1034-1075）：已编译程序与 filter 切片所有权移入包装
    // future，每次 poll 前重绑线程槽、poll 尾（含 panic/早退路径）还原——
    // C# 装配窗为纯同步嵌套窗（检索全程同步，槽内必为当前正在执行的这一次
    // 检索的状态），rust 检索为 async（filter 回调内存在 await 让位点），槽
    // 绝不跨 `.await` 持有，同线程交错任务互不串扰（等价性论证见
    // [`InlineFilterSearchBound`] 文档）。无过滤时无状态，回调回落放行。
    let output = {
      let search = async {
        match query {
          SimilarityQuery::Vector(vector) => {
            self
              .service
              .search_vector(index.context, vector, search_params)
              .await
          }
          SimilarityQuery::Element(element) => {
            self
              .service
              .search_element(index.context, element, search_params)
              .await
          }
        }
      };
      let hits = match program {
        Some(program) => InlineFilterSearchBound::new(opts.filter, program, search).await,
        None => search.await,
      };
      hits.map_err(|_| {
        VectorOpError::new(VectorManagerResult::BadParams, ERR_VECTOR_SERVICE_RESPONSE)
      })?
    };
    let mut hits = output.hits();
    apply_delta_cutoff(&mut hits, opts.delta);

    self
      .build_similarity_output(index.context, hits, opts.filter, opts.include_attributes)
      .await
  }

  /// 组装检索输出：id/距离/属性/位图缓冲（对齐 C# 出参布局）。
  async fn build_similarity_output(
    &self,
    context: u64,
    hits: Vec<SearchHit>,
    filter: &[u8],
    include_attributes: bool,
  ) -> Result<SimilarityOutput, VectorOpError> {
    let found = hits.len();
    let mut output = SimilarityOutput {
      found,
      id_format: VectorIdFormat::I32LengthPrefixed,
      ..Default::default()
    };

    // id（长度前缀）+ 距离
    for hit in &hits {
      output
        .output_ids
        .extend_from_slice(&(hit.external_id.len() as i32).to_le_bytes());
      output.output_ids.extend_from_slice(&hit.external_id);
      output.output_distances.push(hit.distance);
    }

    // 属性（长度前缀；缺失元素长度 0）——单次读取，过滤求值与应答序列化
    // 共用同一份（禁二次存储读取）
    if include_attributes || !filter.is_empty() {
      output.output_attributes = self
        .fetch_vector_element_attributes(context, &output.output_ids)
        .await;
    }

    // 过滤位图（后置过滤）：零压实契约——位图按原结果下标置位，序列化端
    // write_resp2/write_resp3 按位跳过未过项（C# ApplyPostFilter 同款
    // "No in-place compaction"，保留命中槽位与位图索引的 1:1 对应）；
    // output.found 保持全量命中数，应答上限由序列化端 popcount 收敛
    if !filter.is_empty() {
      Self::ensure_filter_bitmap_size(&mut output.filter_bitmap, found);
      let view = AttributeView {
        raw: &output.output_attributes,
      };
      super::vector_manager_filter::apply_post_filter(
        filter,
        found,
        &view,
        &mut output.filter_bitmap,
      );
      // 属性仅为过滤求值而读（调用方未选属性）——求值完即释放，不随
      // 输出滞留
      if !include_attributes {
        output.output_attributes = Vec::new();
      }
    }

    Ok(output)
  }

  // ======================== 缓冲区尺寸保障 ========================

  /// libs/server/Resp/Vector/VectorManager.cs:EnsureFilterBitmapSize
  ///
  /// 保障过滤位图缓冲至少 `ceil(resultCount / 8)` 字节。
  fn ensure_filter_bitmap_size(buffer: &mut Vec<u8>, result_count: usize) {
    let size_bytes = (result_count + 7) >> 3;
    if buffer.len() < size_bytes {
      buffer.resize(size_bytes, 0);
    }
  }
}

/// 有效检索探索因子：`max(EF, count)`（对标 C#
/// VectorManager.cs:797 effectiveEF = Math.Max(searchExplorationFactor, count)）。
/// FILTER-EF effort 不预乘——它仅作 wvector 侧 AdaptiveL 的 maxFilteringEffort
/// 自适应放大入参（对齐 C#:891 原生入参语义）；预乘即 effort² 双重计入
/// （合法 FILTER-EF 256 × COUNT 100_000_000 命令即触发数百 GB 级取队容量
/// 分配的拒绝服务面），严禁回改。
fn effective_search_ef(opts: &VectorSearchOptions<'_>) -> usize {
  opts.search_exploration_factor.max(opts.count)
}

/// EPSILON 最大距离截断（有限 delta 时剔除超距命中）。
fn apply_delta_cutoff(hits: &mut Vec<SearchHit>, delta: f32) {
  if delta.is_finite() {
    hits.retain(|h| h.distance <= delta);
  }
}
