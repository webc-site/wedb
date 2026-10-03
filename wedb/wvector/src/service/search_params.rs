//! 检索参数装配：SearchParams 与 Knn / InlineFilterSearch（AdaptiveL）束宽装配

use super::*;

/// 自适应 L 的取样数（对齐 diskann-garnet ADAPTIVE_L_SAMPLES）。
const ADAPTIVE_L_SAMPLES: usize = 1000;

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
pub(super) fn knn_params(params: &SearchParams) -> Result<search::Knn, SearchError> {
  search::Knn::new_default(params.search_exploration_factor).map_err(|_| SearchError)
}

/// 装配内联过滤检索参数（AdaptiveL 自适应 L，effort = maxFilteringEffort）。
pub(super) fn filter_params(
  params: &SearchParams,
  knn: search::Knn,
) -> Result<search::InlineFilterSearch, SearchError> {
  let adaptive = AdaptiveL::new(ADAPTIVE_L_SAMPLES, params.max_filtering_effort as f64)
    .map_err(|_| SearchError)?;
  Ok(search::InlineFilterSearch::new(knn, Some(adaptive)))
}
