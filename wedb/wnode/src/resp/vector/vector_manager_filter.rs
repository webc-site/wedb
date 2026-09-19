//! 过滤表达式与向量相似度检索的衔接（对标 libs/server/Resp/Vector/VectorManager.Filter.cs）
//!
//! C# 侧缓冲自会话 ScratchBufferBuilder 借用（容量约束见 expr_compiler 常量），
//! 超限时优雅降级：编译失败 → 0 结果通过；运行池耗尽 → 数组按 Null 处理。
//! Rust 侧程序为自有 Vec 缓冲，容量上限保持一致。

use wvector::filter::{
  attribute_extractor::{SelectorRange, extract_fields},
  compiler::{MAX_SELECTORS, try_compile},
  expression::{ExprProgram, ExprToken, ExprTokenType},
  runner::{default_stack, run},
};

use super::vector_manager::AttributeView;

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

  for (i, attr_data) in attributes.iter().take(num_results).enumerate() {
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
  instructions: &[ExprToken],
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

/// 按绝对偏移切片（防越界辅助）。
fn slice_at(buf: &[u8], start: i32, len: i32) -> &[u8] {
  let start = start.max(0) as usize;
  let len = len.max(0) as usize;
  buf
    .get(start..)
    .map_or(&[] as &[u8], |tail| &tail[..len.min(tail.len())])
}
