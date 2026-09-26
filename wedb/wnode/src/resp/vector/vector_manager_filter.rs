//! 过滤表达式与向量相似度检索的衔接（对标 libs/server/Resp/Vector/VectorManager.Filter.cs）
//!
//! C# 侧缓冲自会话 ScratchBufferBuilder 借用（容量约束见 expr_compiler 常量），
//! 超限时优雅降级：编译失败 → 0 结果通过；运行池耗尽 → 数组按 Null 处理。
//! Rust 侧程序为自有 Vec 缓冲，容量上限保持一致。
//!
//! 内联过滤（对标同文件 :237-322 的 `[ThreadStatic] InlineFilterStatePtr` +
//! `InlineFilterState` + `EvaluateCandidateFilter`）：检索入口编译 FILTER 后以
//! [`InlineFilterGuard`] 把 [`InlineFilterState`] 绑定到线程槽，贪婪图探索
//! （InlineFilterSearch + AdaptiveL）逐候选触发 [`StoreCallbacks::filter`]
//! 回调时经 [`with_inline_filter_state`] 取状态内联求值，标量过滤高选择性
//! （低命中率）时仍能动态放大探索半径召回满足条件的近邻；检索退出守卫自动
//! 还原。C# 为纯栈 ref struct + 固定缓冲；rust 状态为自有 Vec 缓冲，槽位
//! 纪律与会话绑定的执行域私有化同款（线程槽 + RAII 守卫，绝不跨 `.await`）。
//! C# 的 ExtMap 反查外部 ID 与按外部 ID 读属性两步，在本仓拓扑下归并为一次
//! 读：属性记录以 internal_id 为键（`wvector/src/provider/callbacks.rs` 的
//! `write_iid/read_varsize_iid`），逐候选省去 ExtMap 反查 I/O。

use std::{cell::Cell, ptr};

use wvector::filter::{
  attribute_extractor::{SelectorRange, extract_fields},
  compiler::{MAX_SELECTORS, try_compile},
  expression::{ExprProgram, ExprToken, ExprTokenType},
  runner::{ExprStack, default_stack, run},
  slice_at,
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

/// 内联过滤上下文（对标 libs/server/Resp/Vector/VectorManager.Filter.cs:InlineFilterState，
/// :248-260 的 ref struct）：检索入口装配一次，图检索逐候选复用求值。
///
/// C# 持 pinned scratch 缓冲的 Span 视图；rust 持自有编译程序与复用缓冲，
/// filter 字节以裸切片指针引用（守卫持原引用保证存活期）。裸指针成员令本
/// 类型天然 `!Send + !Sync`，只可栖身 [`thread_local!`] 槽，经
/// [`with_inline_filter_state`] 在绑定线程内取用。
pub struct InlineFilterState {
  /// 原始 filter 字节切片（裸切片指针：[`InlineFilterGuard`] 持原引用保证存活）
  filter: *const [u8],
  /// 编译后的过滤程序（检索入口校验编译单点传入，禁二次编译）
  program: ExprProgram,
  /// 唯一选择器字节区间（去重，上限 MAX_SELECTORS）
  selector_ranges: Vec<SelectorRange>,
  /// 字段提取缓冲（逐候选复用）
  fields: Vec<ExprToken>,
  /// 求值栈（逐候选复用）
  stack: ExprStack,
}

thread_local! {
  /// 当前线程的内联过滤上下文槽（对标 C# `[ThreadStatic] InlineFilterStatePtr`，
  /// VectorManager.Filter.cs:237-240）
  static INLINE_FILTER_STATE: Cell<*mut InlineFilterState> = const {
    Cell::new(ptr::null_mut())
  };
}

/// 内联过滤上下文的 RAII 守卫（对标 C# ValueSimilarity/ElementSimilarity 的
/// `InlineFilterStatePtr = &filterState` → `finally InlineFilterStatePtr = null`
/// 装配窗口，libs/server/Resp/Vector/VectorManager.cs:862-905/:1034-1075）。
///
/// 与 [`super::vector_store_callbacks::ActiveVectorSessionGuard`] 同款纪律：
/// 绑前存旧、`Drop` 先还原槽位再随守卫释放状态（槽位可读到该指针的任一时刻
/// 状态必未释放）、绝不跨 `.await` 持有。
#[must_use = "守卫离开作用域即解绑：必须持有至本同步段结束"]
pub(crate) struct InlineFilterGuard {
  /// 绑定前的槽值（`Drop` 回填目标）
  prev: *mut InlineFilterState,
  /// 仅承接所有权与稳定地址（槽位另存裸指针）：析构次序由 `Drop` 定——
  /// 先还原槽位，再随结构体释放状态
  _state: Box<InlineFilterState>,
}

impl InlineFilterGuard {
  /// 装配并绑定内联过滤上下文：以编译后的程序收集选择器区间、预置复用
  /// 缓冲，挂到当前线程槽（禁二次编译：程序由检索入口的校验编译单点传入）。
  pub(crate) fn bind(filter: &[u8], program: ExprProgram) -> Self {
    let selector_ranges = collect_selector_ranges(&program, filter);
    let fields = vec![ExprToken::default(); selector_ranges.len().max(1)];
    let mut state = Box::new(InlineFilterState {
      filter,
      program,
      selector_ranges,
      fields,
      stack: default_stack(),
    });
    let prev = INLINE_FILTER_STATE.with(|slot| slot.replace(&mut *state));
    Self {
      prev,
      _state: state,
    }
  }
}

impl Drop for InlineFilterGuard {
  #[inline]
  fn drop(&mut self) {
    // 先还原槽位再随结构体析构释放 Box：槽位可读到该指针的任一时刻状态必存活
    INLINE_FILTER_STATE.with(|slot| slot.set(self.prev));
  }
}

/// 以当前线程绑定的内联过滤上下文执行闭包（对标 C# FilterCallbackUnmanaged
/// 直读 `[ThreadStatic] InlineFilterStatePtr`，VectorManager.Callbacks.cs:422-424）。
///
/// 未绑定（无守卫：未携 FILTER 的检索臂、后台臂）即返回 `None`，调用方回落
/// 放行，绝不 panic。
#[inline]
pub fn with_inline_filter_state<'s, R>(
  f: impl FnOnce(&'s mut InlineFilterState) -> R,
) -> Option<R> {
  let slot = INLINE_FILTER_STATE.with(Cell::get);
  if slot.is_null() {
    return None;
  }
  // SAFETY: 槽位仅由 `InlineFilterGuard::bind` 写入（指针取自守卫自有的 `Box`
  // 内容唯一引用），`Drop` 先还原槽位、Box 随守卫析构释放，指针存活期被守卫
  // 持有期严格包住；`thread_local!` 保证仅绑定线程读写，无跨线程竞争，故此刻
  // 非空指针必指向存活且无其他借用的状态。
  Some(f(unsafe { &mut *slot }))
}

/// libs/server/Resp/Vector/VectorManager.Filter.cs:EvaluateCandidateFilter
///（:266-322）
///
/// 对单个候选的属性字节执行内联过滤求值：提取字段 → 重置运行池 → Run，
/// 复用状态内的程序与缓冲，逐候选零分配。C# 的 ExtMap 反查外部 ID 与按
/// 外部 ID 读属性两步在本仓拓扑下归并为检索回调侧的一次属性读（属性记录以
/// internal_id 为键，`wvector/src/provider/callbacks.rs`），本函数只承接求值。
///
/// `attr` 为候选属性字节（JSON）；属性缺失（空）即排除，对齐 C# 读失败
/// `return 0` 的缺失即排除口径。
pub fn evaluate_candidate_filter(state: &mut InlineFilterState, attr: &[u8]) -> bool {
  if attr.is_empty() {
    return false;
  }
  // SAFETY: `filter` 为守卫绑定时存入的原始切片指针，守卫持有期内恒指向
  // 存活的 filter 字节（原引用由守卫持有，生存期与状态槽位同寿）。
  let filter = unsafe { &*state.filter };
  state.program.reset_runtime_pool();
  extract_fields(
    attr,
    filter,
    &state.selector_ranges,
    &mut state.fields,
    &mut state.program,
  );
  run(
    &state.program,
    attr,
    filter,
    &state.selector_ranges,
    &state.fields,
    &mut state.stack,
  )
}
