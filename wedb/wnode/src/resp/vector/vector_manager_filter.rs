//! 过滤表达式与向量相似度检索的衔接（对标 libs/server/Resp/Vector/VectorManager.Filter.cs）
//!
//! C# 侧缓冲自会话 ScratchBufferBuilder 借用（容量约束见 expr_compiler 常量），
//! 超限时优雅降级：编译失败 → 0 结果通过；运行池耗尽 → 数组按 Null 处理。
//! Rust 侧程序为自有 Vec 缓冲，容量上限保持一致。
//!
//! 内联过滤（对标同文件 :237-322 的 `[ThreadStatic] InlineFilterStatePtr` +
//! `InlineFilterState` + `EvaluateCandidateFilter`）：检索入口编译 FILTER 后把
//! [`InlineFilterState`] 移入 [`InlineFilterSearchBound`] 包装 future，每次
//! poll 前重绑线程槽，贪婪图探索（InlineFilterSearch + AdaptiveL）逐候选触发
//! [`wvector::store::StoreCallbacks::filter`] 回调时经 [`with_inline_filter_state`] 取状态内联
//! 求值，标量过滤高选择性（低命中率）时仍能动态放大探索半径召回满足条件的
//! 近邻。C# 为纯栈 ref struct + 固定缓冲 + 同步装配窗；rust 状态为自有 Vec
//! 缓冲，槽位纪律为 poll 边界重绑（同 SlowPollSessionBound 先例：槽内值仅
//! 存活于单次 poll 同步段，绝不跨 `.await`）。
//! C# 的 ExtMap 反查外部 ID 与按外部 ID 读属性两步，在本仓拓扑下归并为一次
//! 读：属性记录以 internal_id 为键（`wvector/src/provider/callbacks.rs` 的
//! `write_iid/read_varsize_iid`），逐候选省去 ExtMap 反查 I/O。

use std::{
  cell::Cell,
  future::Future,
  pin::Pin,
  ptr,
  task::{Context, Poll},
};

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
/// filter 字节以裸切片指针引用（[`InlineFilterSearchBound`] 持原借用保证存活
/// 期）。裸指针成员令本类型天然 `!Send + !Sync`，栖身 [`InlineFilterSearchBound`]
/// 时随宿主钉在属主线程（compio thread-per-core 下任务不迁移线程），经
/// [`with_inline_filter_state`] 在绑定线程内取用。
pub struct InlineFilterState {
  /// 原始 filter 字节切片（裸切片指针：[`InlineFilterSearchBound`] 持原借用
  /// 保证存活）
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

impl InlineFilterState {
  /// 原始 filter 字节视图（绑定窗内必存活）
  #[inline]
  pub fn filter_bytes(&self) -> &[u8] {
    // SAFETY: filter 在 InlineFilterSearchBound 存活期内恒有效
    unsafe { &*self.filter }
  }
}

thread_local! {
  /// 当前线程的内联过滤上下文槽（对标 C# `[ThreadStatic] InlineFilterStatePtr`，
  /// VectorManager.Filter.cs:237-240）
  static INLINE_FILTER_STATE: Cell<*mut InlineFilterState> = const {
    Cell::new(ptr::null_mut())
  };
}

// ── 线程槽置/清两原语（poll 边界重绑的两半） ──

/// 置槽：把状态挂到当前线程槽，返回绑定前的槽值（回填目标）。
#[inline]
fn bind_slot(state: &mut InlineFilterState) -> *mut InlineFilterState {
  INLINE_FILTER_STATE.with(|slot| slot.replace(state))
}

/// 清槽：回填 [`bind_slot`] 返回的旧值（对标 C# `finally InlineFilterStatePtr
/// = null` 的还原半边）。
#[inline]
fn restore_slot(prev: *mut InlineFilterState) {
  INLINE_FILTER_STATE.with(|slot| slot.set(prev));
}

/// poll 同步段内的槽位还原守卫：`Drop` 回填 [`bind_slot`] 返回的旧值——poll
/// 返回（Pending/Ready 皆然）与 panic unwind 均经 `Drop` 还原，槽内值仅
/// 存活于单次 poll 同步段。
struct SlotRestoreGuard {
  /// 绑定前的槽值（`Drop` 回填目标）
  prev: *mut InlineFilterState,
}

impl Drop for SlotRestoreGuard {
  #[inline]
  fn drop(&mut self) {
    // 先还原槽位再随守卫析构：槽位可读到该指针的任一时刻状态必存活
    restore_slot(self.prev);
  }
}

/// 检索 future 的 poll 边界重绑包装（对标 C# ValueSimilarity/ElementSimilarity
/// 的装配窗 `InlineFilterStatePtr = &filterState` → 同步检索 → `finally 置
/// null`，libs/server/Resp/Vector/VectorManager.cs:862-905/:1034-1075；机制
/// 先例同 crate `garnet_api::SlowPollSessionBound`）。
///
/// C# 的 `[ThreadStatic]` 槽装配窗是纯同步嵌套窗：SearchVector/SearchElement
/// 对原生 DiskANN 的调用全程同步，回调与置位同线程同栈，任意时刻槽内必为
/// 当前正在执行的这一次检索的状态，多命令并发靠线程天然隔离。rust 检索为
/// async（逐候选 filter 回调内读回与求值间存在 await 让位点），守卫若随
/// async 栈帧跨 `.await` 持有，同线程交错 poll 的两个带 FILTER 检索的
/// bind/drop 对不再 LIFO 嵌套：后 bind 者覆写槽（串扰）、先完成者提前还原
/// 槽（过滤失效）、全序列 drop 完槽还原为悬垂指针（UAF）。本包装恢复同步嵌
/// 套窗语义：状态所有权移入 future（构造时装配一次，禁二次编译），每次 poll
/// 前重绑线程槽、poll 尾还原——槽内值仅存活于单次 poll 同步段，任意同步段
/// 内必为当前被 poll 任务的状态，两次 poll 间隙恒空，同线程交错任务互不可
/// 见，与 C# 同步窗的线程隔离 + 严格嵌套纪律等价。
pub struct InlineFilterSearchBound<'a, F> {
  /// 内联过滤状态（构造时装配一次；poll 内其地址经 [`bind_slot`] 入槽——
  /// poll 期本 future 被钉住地址稳定，间隙槽位已还原，poll 间移动安全）
  state: InlineFilterState,
  /// filter 原始字节借用（`state.filter` 裸指针指向它，借用保活保证解引用
  /// 安全）
  _filter: &'a [u8],
  /// 被包装的检索执行体（结构性 pin：只在 Unpin 字段之后投影，永不 mov）
  inner: F,
}

impl<'a, F: Future> InlineFilterSearchBound<'a, F> {
  /// 装配并包装：以编译后的程序收集选择器区间、预置复用缓冲（禁二次编译：
  /// 程序由检索入口的校验编译单点传入），filter 字节由本包装借用保活。
  pub fn new(filter: &'a [u8], program: ExprProgram, inner: F) -> Self {
    let selector_ranges = collect_selector_ranges(&program, filter);
    let fields = vec![ExprToken::default(); selector_ranges.len().max(1)];
    Self {
      state: InlineFilterState {
        filter,
        program,
        selector_ranges,
        fields,
        stack: default_stack(),
      },
      _filter: filter,
      inner,
    }
  }
}

impl<F: Future> Future for InlineFilterSearchBound<'_, F> {
  type Output = F::Output;

  #[inline]
  fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
    // SAFETY: 结构固定投影——state/_filter 均 Unpin，本 poll 只以 `Pin` 移交
    // inner，全程不移动任何字段，满足结构性 pin 契约（SlowPollSessionBound
    // 同款）。
    let this = unsafe { self.get_unchecked_mut() };
    // poll 前置槽（重绑）：本同步段内槽内必为当前被 poll 任务的状态；守卫
    // 构造与 bind_slot 之间无任何可 panic 操作
    let _restore = SlotRestoreGuard {
      prev: bind_slot(&mut this.state),
    };
    // SAFETY: 同上，inner 一经交付即结构性 pin 于本地址
    unsafe { Pin::new_unchecked(&mut this.inner) }.poll(cx)
    // poll 尾还原：_restore Drop 于 poll 返回时回填旧值（早退/panic unwind 同路）
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
  // SAFETY: 槽位仅由 [`bind_slot`] 写入（指针取自 [`InlineFilterSearchBound`]
  // 自有的状态字段），每次 poll 尾经 [`SlotRestoreGuard`] 先还原槽位——槽内值
  // 仅存活于单次 poll 同步段，poll 期宿主被钉住地址稳定，指针存活期被绑定窗
  // 严格包住；`thread_local!` 保证仅绑定线程读写，无跨线程竞争，故此刻非空
  // 指针必指向存活且无其他借用的状态。
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
  // SAFETY: `filter` 为装配时存入的原始切片指针，[`InlineFilterSearchBound`]
  // 持原借用保活，绑定窗内（状态在槽的任一时刻）恒指向存活的 filter 字节。
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
