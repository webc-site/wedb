#![recursion_limit = "512"] // 泛型设备实例化下 async 状态机嵌套深（R27 CI 实证）
//! 向量检索内联过滤绑定（InlineFilterSearchBound）与线程槽隔离集成测试
//! （对应 libs/server/Resp/Vector/VectorManager.Filter.cs）

use std::{
  future::Future,
  mem::take,
  pin::Pin,
  task::{Context, Poll, Waker},
};

use wnode::resp::vector::vector_manager_filter::{
  InlineFilterSearchBound, with_inline_filter_state,
};
use wvector::filter::compiler::try_compile;

/// 两个可区分的过滤表达式（探针以原始 filter 字节标记识别槽内状态归属）
const FILTER_A: &[u8] = b".n > 1";
const FILTER_B: &[u8] = b".n > 2";

/// 槽探针 future：每次 poll 记录本同步段线程槽内状态的 filter 标记，前
/// `left` 次挂起（Pending），最后一次返回标记序列。
struct SlotProbe {
  left: u32,
  seen: Vec<Vec<u8>>,
}

impl Future for SlotProbe {
  type Output = Vec<Vec<u8>>;

  fn poll(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Self::Output> {
    let this = self.get_mut();
    this
      .seen
      .push(with_inline_filter_state(|state| state.filter_bytes().to_vec()).unwrap_or_default());
    if this.left > 0 {
      this.left -= 1;
      Poll::Pending
    } else {
      Poll::Ready(take(&mut this.seen))
    }
  }
}

#[test]
fn interleaved_polls_rebind_no_crosstalk() {
  let waker = Waker::noop();
  let mut cx = Context::from_waker(waker);

  let mut a = Box::pin(InlineFilterSearchBound::new(
    FILTER_A,
    try_compile(FILTER_A).unwrap(),
    SlotProbe {
      left: 2,
      seen: Vec::new(),
    },
  ));
  let mut b = Box::pin(InlineFilterSearchBound::new(
    FILTER_B,
    try_compile(FILTER_B).unwrap(),
    SlotProbe {
      left: 1,
      seen: Vec::new(),
    },
  ));

  // 初始槽空
  assert!(with_inline_filter_state(|_| ()).is_none());

  // 交错 poll（旧缺陷的交错序：A 挂 → B 挂 → B 完 → A 挂 → A 完）
  assert!(a.as_mut().poll(&mut cx).is_pending());
  // poll 尾槽必还原：无跨 poll 持有（守卫不跨 `.await` 存活）
  assert!(with_inline_filter_state(|_| ()).is_none());
  assert!(b.as_mut().poll(&mut cx).is_pending());
  assert!(with_inline_filter_state(|_| ()).is_none());
  // B 先完成：A 仍在飞，A 后续 poll 必重绑自己的状态（旧缺陷：B 的守卫
  // drop 把槽还原为旧值，A 的过滤静默失效/串扰）
  let out_b = b.as_mut().poll(&mut cx);
  assert!(with_inline_filter_state(|_| ()).is_none());
  assert!(a.as_mut().poll(&mut cx).is_pending());
  assert!(with_inline_filter_state(|_| ()).is_none());
  let out_a = a.as_mut().poll(&mut cx);
  assert!(with_inline_filter_state(|_| ()).is_none());

  // 各探针每次 poll 只见过自己的标记：槽内必为当前被 poll 任务的状态
  assert_eq!(
    out_a,
    Poll::Ready(vec![
      FILTER_A.to_vec(),
      FILTER_A.to_vec(),
      FILTER_A.to_vec()
    ])
  );
  assert_eq!(
    out_b,
    Poll::Ready(vec![FILTER_B.to_vec(), FILTER_B.to_vec()])
  );
}

#[test]
fn dropped_midflight_leaves_slot_clean_next_search_survives() {
  let waker = Waker::noop();
  let mut cx = Context::from_waker(waker);

  // 中断形态：检索挂起后任务被取消（future 未 poll 完即 drop）
  let mut aborted = Box::pin(InlineFilterSearchBound::new(
    FILTER_A,
    try_compile(FILTER_A).unwrap(),
    SlotProbe {
      left: 5,
      seen: Vec::new(),
    },
  ));
  assert!(aborted.as_mut().poll(&mut cx).is_pending());
  drop(aborted);
  // 槽不留悬垂/滞留：后续无过滤检索的回调读到 None 回落放行，绝不解引用
  assert!(with_inline_filter_state(|_| ()).is_none());

  // 常规检索存活：完整跑完，标记全为自己的
  let mut fresh = Box::pin(InlineFilterSearchBound::new(
    FILTER_B,
    try_compile(FILTER_B).unwrap(),
    SlotProbe {
      left: 1,
      seen: Vec::new(),
    },
  ));
  assert!(fresh.as_mut().poll(&mut cx).is_pending());
  let out = fresh.as_mut().poll(&mut cx);
  assert_eq!(out, Poll::Ready(vec![FILTER_B.to_vec(), FILTER_B.to_vec()]));
  assert!(with_inline_filter_state(|_| ()).is_none());
}

#[test]
fn unwrapped_probe_sees_empty_slot() {
  // 无包装（无过滤检索臂）：槽恒空，回调回落放行
  let waker = Waker::noop();
  let mut cx = Context::from_waker(waker);
  let mut bare = Box::pin(SlotProbe {
    left: 1,
    seen: Vec::new(),
  });
  assert!(bare.as_mut().poll(&mut cx).is_pending());
  let out = bare.as_mut().poll(&mut cx);
  assert_eq!(out, Poll::Ready(vec![Vec::new(), Vec::new()]));
}
