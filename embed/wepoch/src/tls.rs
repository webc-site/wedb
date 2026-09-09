//! 线程本地 (TLS) 纪元槽位登记与单槽快速缓存
//!
//! 对照 C# LightEpoch.Metadata（[ThreadStatic] Entries 按 instanceId 索引）：
//! Rust 以 `LocalEpochEntries` 按实例 ID 登记每线程槽位，免去 `ThreadLocal<T>`
//! 开销；`FastEntry` 单槽缓存将 resume/suspend 热路径降为 O(1)。

use std::{
  cell::{Cell, RefCell},
  ptr,
  sync::{
    Weak,
    atomic::{Ordering, fence},
  },
};

use crate::EpochEntry;

/// 线程内联登记的实例数上限，超出走 overflow 向量
const MAX_LOCAL_ENTRIES: usize = 4;
/// overflow 向量触发清理的堆积上限，防止长寿线程面对海量瞬态实例时无界累积
const MAX_OVERFLOW_STATES: usize = 16;

/// 本线程在某 LightEpoch 实例上的登记状态
#[derive(Clone)]
struct LocalEntryState {
  instance_id: u64,
  active_entry: usize,
  cached_slot: usize,
  entries: Option<Weak<[EpochEntry]>>,
}

/// 本线程跨全部 LightEpoch 实例的槽位登记表
struct LocalEpochEntries {
  count: usize,
  inline: [LocalEntryState; MAX_LOCAL_ENTRIES],
  overflow: Vec<LocalEntryState>,
}

impl Drop for LocalEpochEntries {
  /// 线程退出兜底：代为释放本线程遗留的全部活动槽位（对照 C# 无此机制，
  /// C# 依赖调用方严格配对 Release，线程异常退出会永久钉住纪元）
  fn drop(&mut self) {
    let mut need_fence = false;
    for state in self.inline[..self.count]
      .iter_mut()
      .chain(self.overflow.iter_mut())
    {
      if state.active_entry != 0
        && let Some(entries) = state.entries.as_ref().and_then(Weak::upgrade)
        && state.active_entry <= entries.len()
      {
        entries[state.active_entry - 1].reset();
        need_fence = true;
      }
      state.active_entry = 0;
    }
    if need_fence {
      fence(Ordering::SeqCst);
    }
  }
}

impl LocalEpochEntries {
  const fn new() -> Self {
    Self {
      count: 0,
      inline: [const {
        LocalEntryState {
          instance_id: 0,
          active_entry: 0,
          cached_slot: 0,
          entries: None,
        }
      }; MAX_LOCAL_ENTRIES],
      overflow: Vec::new(),
    }
  }

  /// 查找本线程在某实例上登记的状态 (active_entry, cached_slot)
  #[inline]
  fn find(&self, instance_id: u64) -> Option<(usize, usize)> {
    self.inline[..self.count]
      .iter()
      .chain(self.overflow.iter())
      .find(|s| s.instance_id == instance_id)
      .map(|s| (s.active_entry, s.cached_slot))
  }

  /// 可变查找本线程在某实例上登记的状态
  #[inline]
  fn find_mut(&mut self, instance_id: u64) -> Option<&mut LocalEntryState> {
    self.inline[..self.count]
      .iter_mut()
      .chain(self.overflow.iter_mut())
      .find(|s| s.instance_id == instance_id)
  }

  fn set_active<F>(&mut self, instance_id: u64, entry: usize, get_weak: F)
  where
    F: FnOnce() -> Weak<[EpochEntry]>,
  {
    if let Some(item) = self.find_mut(instance_id) {
      item.active_entry = entry;
      if entry != 0 {
        item.cached_slot = entry;
        if item.entries.as_ref().is_none_or(|e| e.strong_count() == 0) {
          item.entries = Some(get_weak());
        }
      }
      return;
    }
    let new_state = LocalEntryState {
      instance_id,
      active_entry: entry,
      cached_slot: entry,
      entries: (entry != 0).then(get_weak),
    };
    if self.count < MAX_LOCAL_ENTRIES {
      self.inline[self.count] = new_state;
      self.count += 1;
    } else {
      // 适度清理已废弃实例，防止长寿线程面对海量瞬态 LightEpoch 时 overflow 无界累积
      if self.overflow.len() >= MAX_OVERFLOW_STATES {
        self.overflow.retain(|s| {
          s.active_entry != 0 || s.entries.as_ref().is_some_and(|w| w.strong_count() > 0)
        });
      }
      self.overflow.push(new_state);
    }
  }
}

thread_local! {
  static THREAD_LOCAL_ENTRIES: RefCell<LocalEpochEntries> = const { RefCell::new(LocalEpochEntries::new()) };
  /// 单槽快速缓存：免去每次 resume/suspend 的 RefCell 借用 + 线性 find
  pub(crate) static FAST_ENTRY: Cell<FastEntry> = const { Cell::new(FastEntry::EMPTY) };
  /// Participant 槽位单槽快速缓存：使 thread_protected_entry 对 Participant
  /// 机制的保护判定 O(1)（对照 C# ProtectAndDrain 经 TLS 索引 O(1) 定位条目）
  pub(crate) static FAST_PARTICIPANT: Cell<FastEntry> = const { Cell::new(FastEntry::EMPTY) };
}

/// 最近一次登记的 (实例 ID, 1-based 槽位, 已解析条目指针)
///
/// 同时服务 `FAST_ENTRY`（TLS resume/suspend 机制）与 `FAST_PARTICIPANT`
/// （Participant 显式句柄机制）两个单槽缓存。
///
/// SAFETY 不变量：`ptr` 指向 `LightEpoch.entries` 内部元素；实例 ID 由全局单调
/// 计数器产出、永不复用，故仅当 `instance_id` 与当前存活实例匹配时才解引用，
/// 此时该实例持有 `Arc<[EpochEntry]>`，指针不可能悬垂。缓存命中后仍须校验
/// `thread_id == 当前线程 && is_protected()`，槽位换绑/清除时经
/// `set_thread_entry`/`clear_thread_entry` 同步刷新失效。
#[derive(Clone, Copy)]
pub(crate) struct FastEntry {
  pub(crate) instance_id: u64,
  pub(crate) slot: usize,
  pub(crate) ptr: *const EpochEntry,
}

impl FastEntry {
  pub(crate) const EMPTY: Self = Self {
    instance_id: 0,
    slot: 0,
    ptr: ptr::null(),
  };
}

/// 登记本线程最近创建的 Participant 槽位到单槽缓存（register 仅在创建线程上调用）
#[inline]
pub(crate) fn note_participant_slot(instance_id: u64, idx: usize, entry: &EpochEntry) {
  FAST_PARTICIPANT.set(FastEntry {
    instance_id,
    slot: idx + 1,
    ptr: ptr::from_ref(entry),
  });
}

/// 查询本线程在某实例上的活动槽位（1-based，0 表示未登记）
#[inline]
pub(crate) fn get_thread_entry(instance_id: u64) -> usize {
  THREAD_LOCAL_ENTRIES.with(|cell| {
    cell
      .borrow()
      .find(instance_id)
      .map_or(0, |(active, _)| active)
  })
}

/// 查询本线程在某实例上缓存的候选槽位（上次占用槽，0 表示无）
#[inline]
pub(crate) fn cached_slot(instance_id: u64) -> usize {
  THREAD_LOCAL_ENTRIES.with(|cell| {
    cell
      .borrow()
      .find(instance_id)
      .map_or(0, |(_, cached)| cached)
  })
}

#[inline]
pub(crate) fn set_thread_entry<F>(instance_id: u64, entry: usize, get_weak: F)
where
  F: FnOnce() -> Weak<[EpochEntry]>,
{
  let resolved = THREAD_LOCAL_ENTRIES.with(|cell| {
    let mut entries = cell.borrow_mut();
    entries.set_active(instance_id, entry, get_weak);
    // 同步刷新单槽快速缓存：升级弱引用一次性解析条目地址（仅慢路径付出此开销）
    if entry != 0 {
      entries
        .find_mut(instance_id)
        .and_then(|s| s.entries.as_ref())
        .and_then(Weak::upgrade)
        .filter(|e| entry <= e.len())
        .map(|e| ptr::from_ref(&e[entry - 1]))
    } else {
      None
    }
  });
  match resolved {
    Some(p) => FAST_ENTRY.set(FastEntry {
      instance_id,
      slot: entry,
      ptr: p,
    }),
    None => FAST_ENTRY.set(FastEntry::EMPTY),
  }
}

#[inline]
pub(crate) fn clear_thread_entry(instance_id: u64) {
  THREAD_LOCAL_ENTRIES.with(|cell| {
    let mut entries = cell.borrow_mut();
    if let Some(item) = entries.find_mut(instance_id) {
      item.active_entry = 0;
    }
  });
  // 仅当快速缓存归属本实例时失效，保留其他实例的缓存命中能力
  if FAST_ENTRY.get().instance_id == instance_id {
    FAST_ENTRY.set(FastEntry::EMPTY);
  }
}
