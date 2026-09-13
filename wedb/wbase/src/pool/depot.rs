//! 全局条带化共享仓库 (对标 C# `DepotStripe[] depot`)
//!
//! 每个 size class 划分为 8 个并发条带锁（28×8=224 条带），用于：
//! 1. 大容量 class（>256KB）的全局条带共享复用（避免线程本地持有造成内存膨胀）；
//! 2. 线程本地私有栈溢出时的溢出承载；
//! 3. 线程退出时遗留缓冲区的安全暂存与跨线程工作窃取（Work Stealing）。

use parking_lot::Mutex;

use super::{CachedBuf, DEPOT_STRIPE_CAP, DEPOT_STRIPE_MASK, DEPOT_STRIPES, NUM_CLASSES};
use crate::align::CachePadded64;

/// 单条带内部状态
///
/// `closed` 标志与 `items` 推入在同一把锁的同一临界区内判定（对标 C# `DepotStripe` 的
/// lock + closed 设计）：若改为无锁 CAS，`BufferPool::free` 清空条带与并发推入之间存在
/// 竞态窗口，迟到的推入会把缓冲连同其字节预算许可永久滞留在已清空的条带中，违背
/// `Free` 之后配额必须可归零的最终性契约。仓库仅承载冷路径（本地栈与收件箱均未命中
/// 时的溢出/窃取），uncontended `parking_lot` 锁开销可忽略。
struct StripeState {
  items: Vec<CachedBuf>,
  closed: bool,
}

/// 单条带：互斥锁保护的栈式仓库
///
/// 强制 64 字节缓存行对齐，彻底消除并发条带间的 CPU 缓存行伪共享 (False Sharing)
struct DepotStripe {
  state: CachePadded64<Mutex<StripeState>>,
}

impl DepotStripe {
  const fn new() -> Self {
    Self {
      state: CachePadded64::new(Mutex::new(StripeState {
        items: Vec::new(),
        closed: false,
      })),
    }
  }

  /// 推入：条带已关闭或已达容量上限则失败 (关闭判定与推入原子，保证 close 无竞态)
  #[inline]
  fn push(&self, buf: CachedBuf) -> bool {
    let mut state = self.state.lock();
    if state.closed || state.items.len() >= DEPOT_STRIPE_CAP {
      return false;
    }
    state.items.push(buf);
    true
  }

  /// 弹出栈顶缓冲
  #[inline]
  fn pop(&self) -> Option<CachedBuf> {
    self.state.lock().items.pop()
  }

  /// 当前缓存数量
  #[inline]
  fn len(&self) -> usize {
    self.state.lock().items.len()
  }

  /// 清空条带中已缓存的全部缓冲，逐一回调释放许可（不关闭条带，供低内存/按需清理使用）
  fn drain(&self, mut on_drop: impl FnMut(CachedBuf)) {
    let mut state = self.state.lock();
    if state.closed {
      return;
    }
    for buf in state.items.drain(..) {
      on_drop(buf);
    }
  }

  /// 原子关闭条带并清空全部缓冲，逐一回调释放许可 (对标 C# `DepotStripe.Close(DropBuffer)`)
  fn close(&self, mut on_drop: impl FnMut(CachedBuf)) {
    let mut state = self.state.lock();
    state.closed = true;
    for buf in state.items.drain(..) {
      on_drop(buf);
    }
  }
}

/// 全局条带化共享仓库 (对标 C# DepotStripe[] depot)
pub(crate) struct Depot {
  /// 内联条带数组：免堆分配与指针间接，`const` 构造零运行时初始化开销
  stripes: [DepotStripe; NUM_CLASSES * DEPOT_STRIPES],
}

impl Depot {
  pub(crate) const fn new() -> Self {
    Self {
      stripes: [const { DepotStripe::new() }; NUM_CLASSES * DEPOT_STRIPES],
    }
  }

  /// 推入指定条带 (按线程 ID 条带化分流，削减锁争用)
  #[inline]
  pub(crate) fn push(&self, cls: usize, buf: CachedBuf, tid: u64) -> bool {
    self.stripes[cls * DEPOT_STRIPES + ((tid as usize) & DEPOT_STRIPE_MASK)].push(buf)
  }

  /// 弹出：优先尝试本线程条带，若空则工作窃取其他条带
  pub(crate) fn pop(&self, cls: usize, tid: u64) -> Option<CachedBuf> {
    let base = cls * DEPOT_STRIPES;
    let start = (tid as usize) & DEPOT_STRIPE_MASK;
    (0..DEPOT_STRIPES).find_map(|i| self.stripes[base + ((start + i) & DEPOT_STRIPE_MASK)].pop())
  }

  /// 统计指定 class 当前已缓存的总数量
  pub(crate) fn total_cached(&self, cls: usize) -> usize {
    let base = cls * DEPOT_STRIPES;
    (0..DEPOT_STRIPES)
      .map(|i| self.stripes[base + i].len())
      .sum()
  }

  /// 清空所有条带中当前已缓存的缓冲，回调释放许可（不关闭条带，仍可继续使用）
  pub(crate) fn drain(&self, mut on_drop: impl FnMut(usize, usize)) {
    for cls in 0..NUM_CLASSES {
      let base = cls * DEPOT_STRIPES;
      for stripe in &self.stripes[base..base + DEPOT_STRIPES] {
        stripe.drain(|buf| on_drop(cls, buf.cap));
      }
    }
  }

  /// 原子关闭所有条带并清空缓冲，回调释放许可 (对标 libs/storage/Tsavorite/cs/src/core/Utilities/BufferPool.OriginReturn.cs:Free 中 `foreach stripe: stripe.Close(DropBuffer)`)
  pub(crate) fn clear(&self, mut on_drop: impl FnMut(usize, usize)) {
    for cls in 0..NUM_CLASSES {
      let base = cls * DEPOT_STRIPES;
      for stripe in &self.stripes[base..base + DEPOT_STRIPES] {
        stripe.close(|buf| on_drop(cls, buf.cap));
      }
    }
  }
}
