//! 过滤表达式与向量相似度检索的衔接（对标 libs/server/Resp/Vector/VectorManager.Filter.cs）
//!
//! C# 侧缓冲自会话 ScratchBufferBuilder 借用（容量约束见 expr_compiler 常量），
//! 超限时优雅降级：编译失败 → 0 结果通过；运行池耗尽 → 数组按 Null 处理。
//! Rust 侧程序为自有 Vec 缓冲，容量上限保持一致。

use super::{
  attribute_extractor::{SelectorRange, extract_fields},
  expr_compiler::{MAX_SELECTORS, try_compile},
  expr_runner::{ExprStack, default_stack, run},
  vector_filter_expression::{ExprProgram, ExprToken, ExprTokenType},
  vector_manager::{AttributeView, VectorManager},
};

/// libs/server/Resp/Vector/VectorManager.Filter.cs:ApplyPostFilter
///
/// 以编译后的过滤表达式对检索结果做后置过滤（C# 为静态方法）。
/// `attributes` 为 i32 长度前缀串接的属性流；`filter_bitmap` 按位标记
/// 各结果是否通过（bit i = 结果 i）；返回通过数。
pub fn apply_post_filter(
  filter: &[u8],
  num_results: usize,
  attributes: &AttributeView,
  filter_bitmap: &mut [u8],
) -> usize {
  if num_results == 0 {
    return 0;
  }

  // ── 编译 ──
  let mut program = match try_compile(filter) {
    Ok(p) => p,
    // 编译失败 → 无结果通过
    Err(_) => return 0,
  };

  filter_bitmap.fill(0);

  // ── 收集唯一选择器 ──
  let selector_ranges = collect_selector_ranges(&program, filter);
  let mut fields = vec![ExprToken::default(); selector_ranges.len().max(1)];

  let mut filtered_count = 0;
  let mut stack = default_stack();

  for (i, attr_data) in attributes
    .segments()
    .into_iter()
    .take(num_results)
    .enumerate()
  {
    program.reset_runtime_pool();

    extract_fields(
      attr_data,
      filter,
      &selector_ranges,
      &mut fields,
      &mut program,
    );

    if run(
      &program,
      attr_data,
      filter,
      &selector_ranges,
      &fields,
      &mut stack,
    ) {
      filter_bitmap[i >> 3] |= 1 << (i & 7);
      filtered_count += 1;
    }
  }

  filtered_count
}

/// libs/server/Resp/Vector/VectorManager.Filter.cs:GetSelectorRanges
///
/// 从编译指令中提取唯一选择器字节区间（去重，上限 MAX_SELECTORS）。
pub fn get_selector_ranges(
  instructions: &[super::vector_filter_expression::ExprToken],
  filter_bytes: &[u8],
  output: &mut Vec<SelectorRange>,
) -> usize {
  let mut count = 0;
  for inst in instructions {
    if inst.token_type != ExprTokenType::Selector {
      continue;
    }
    let start = inst.utf8_start;
    let len = inst.utf8_length;
    let span = slice_at(filter_bytes, start, len);
    let found = (0..count).any(|j| {
      let (s, l) = output[j];
      slice_at(filter_bytes, s, l) == span
    });
    if !found && count < MAX_SELECTORS {
      if output.len() > count {
        output[count] = (start, len);
      } else {
        output.push((start, len));
      }
      count += 1;
    }
  }
  count
}

/// 从程序指令收集选择器区间（ApplyPostFilter 的内部形态）。
fn collect_selector_ranges(program: &ExprProgram, filter: &[u8]) -> Vec<SelectorRange> {
  let mut ranges: Vec<SelectorRange> = Vec::with_capacity(MAX_SELECTORS);
  let _ = get_selector_ranges(&program.instructions, filter, &mut ranges);
  ranges
}

/// 逐候选复用的过滤求值状态：选择器区间一次收集，字段槽与求值栈跨候选复用
/// （内联过滤按图内每个被访问候选调用一次，禁止逐候选重扫描指令与重分配）。
pub struct CandidateFilterState {
  /// 唯一选择器区间（构造时一次性收集）。
  selector_ranges: Vec<SelectorRange>,
  /// 提取字段槽（跨候选复用）。
  fields: Vec<ExprToken>,
  /// 求值栈（跨候选复用）。
  stack: ExprStack,
}

impl CandidateFilterState {
  /// 以编译好的程序与过滤字节构造状态。
  pub fn new(program: &ExprProgram, filter_bytes: &[u8]) -> Self {
    let selector_ranges = collect_selector_ranges(program, filter_bytes);
    let fields = vec![ExprToken::default(); selector_ranges.len().max(1)];
    Self {
      selector_ranges,
      fields,
      stack: default_stack(),
    }
  }
}

/// 按绝对偏移切片（防越界辅助）。
fn slice_at(buf: &[u8], start: i32, len: i32) -> &[u8] {
  let start = start.max(0) as usize;
  let len = len.max(0) as usize;
  buf
    .get(start..)
    .map_or(&[] as &[u8], |tail| &tail[..len.min(tail.len())])
}

impl VectorManager {
  /// libs/server/Resp/Vector/VectorManager.Filter.cs:EvaluateCandidateFilter
  ///
  /// 共享的单候选过滤判定：读外部 id 属性 → 提取字段 → 执行过滤程序。
  /// 找不到外部 id 或属性缺失 → 排除候选。
  /// 选择器区间/字段槽/求值栈经 `state` 跨候选复用。
  pub fn evaluate_candidate_filter(
    &self,
    context: u64,
    external_id: &[u8],
    program: &mut ExprProgram,
    filter_bytes: &[u8],
    state: &mut CandidateFilterState,
  ) -> bool {
    // 1. 属性读取（找不到属性 → 排除）
    let Some(attr_data) = self.service.get_attribute(context, external_id) else {
      return false;
    };

    // 2. 重置运行池 + 提取字段
    program.reset_runtime_pool();
    extract_fields(
      &attr_data,
      filter_bytes,
      &state.selector_ranges,
      &mut state.fields,
      program,
    );

    // 3. 执行
    run(
      program,
      &attr_data,
      filter_bytes,
      &state.selector_ranges,
      &state.fields,
      &mut state.stack,
    )
  }
}

#[cfg(test)]
mod tests {
  use super::{
    super::{
      hnsw::HnswConfig,
      vector_manager::{AttributeView, VectorManager, VectorManagerOptions},
    },
    *,
  };

  #[test]
  fn selector_range_collection_dedupes() {
    let program = try_compile(b".a > 1 or .a < 5 or .b == \"x\"").unwrap();
    let filter = b".a > 1 or .a < 5 or .b == \"x\"";
    let mut out = Vec::new();
    let n = get_selector_ranges(&program.instructions, filter, &mut out);
    // .a 去重为一条，.b 一条
    assert_eq!(n, 2);
    assert_eq!(
      &filter[out[0].0 as usize..(out[0].0 + out[0].1) as usize],
      b"a"
    );
    assert_eq!(
      &filter[out[1].0 as usize..(out[1].0 + out[1].1) as usize],
      b"b"
    );
  }

  #[test]
  fn post_filter_bitmap_semantics() {
    // 三个候选：0 通过，1 不通过，2 通过
    let mut attributes = Vec::new();
    for attr in [
      b"{\"v\": 10}".as_slice(),
      b"{\"v\": 1}".as_slice(),
      b"{\"v\": 5}".as_slice(),
    ] {
      attributes.extend_from_slice(&(attr.len() as i32).to_le_bytes());
      attributes.extend_from_slice(attr);
    }
    let view = AttributeView { raw: &attributes };
    let mut bitmap = vec![0u8; 1];
    let passed = apply_post_filter(b".v >= 5", 3, &view, &mut bitmap);
    assert_eq!(passed, 2);
    // bit0 与 bit2 置位
    assert_eq!(bitmap[0], 0b101);

    // 编译失败 → 0
    assert_eq!(apply_post_filter(b".v >= >", 3, &view, &mut bitmap), 0);
    // 空结果 → 0
    assert_eq!(apply_post_filter(b".v >= 5", 0, &view, &mut bitmap), 0);
  }

  #[test]
  fn candidate_filter_via_attributes() {
    let manager = VectorManager::new(VectorManagerOptions {
      is_enabled: true,
      ..Default::default()
    });
    let mut program = try_compile(b".year > 2000").unwrap();
    let filter = b".year > 2000";

    // 属性缺失 → 排除
    let mut state = CandidateFilterState::new(&program, filter);
    assert!(!manager.evaluate_candidate_filter(1, b"ghost", &mut program, filter, &mut state));

    manager.service.create_index(
      1,
      HnswConfig::new(
        1,
        0,
        super::super::vector_types::VectorQuantType::NoQuant,
        super::super::vector_types::VectorDistanceMetricType::L2,
        8,
        2,
      ),
    );
    manager
      .service
      .insert(1, b"c1", &2000f32.to_le_bytes(), b"{\"year\": 2001}");
    manager
      .service
      .insert(1, b"c2", &2000f32.to_le_bytes(), b"{\"year\": 1999}");

    // 状态跨候选复用
    let mut state = CandidateFilterState::new(&program, filter);
    assert!(manager.evaluate_candidate_filter(1, b"c1", &mut program, filter, &mut state));
    assert!(!manager.evaluate_candidate_filter(1, b"c2", &mut program, filter, &mut state));
  }
}
