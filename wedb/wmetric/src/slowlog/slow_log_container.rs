//! 在 garnet 中的相对路径: libs/server/Metrics/Slowlog/SlowLogContainer.cs(对标 C# SlowLogContainer)
use std::{
  collections::VecDeque,
  sync::atomic::{AtomicI64, Ordering::Relaxed},
};

use parking_lot::Mutex;

use super::slowlog_entry::SlowLogEntry;

/// 线程安全的慢日志容器
///（对标 libs/server/Metrics/Slowlog/SlowLogContainer.cs:SlowLogContainer）。
///
/// C# 用 ConcurrentQueue + 容量裁剪；Rust 以 `Mutex<VecDeque>` 承接：
/// 入队/出队同一临界区，裁剪天然一次到位（避免 C# 的 while 竞态重试）。
/// id 取号同样置于该临界区内，与入队强串行绑定，杜绝 C# 锁外取号在
/// 调度错位下的 ID 与物理顺序倒置竞态。
/// id 位宽 i64 与临界区取号序列化双宗刻意分叉的登记锚：
/// deviations.md §167（严禁 int 截断与锁外取号接回）。
pub struct SlowLogContainer {
  /// 容量上限。
  size: usize,
  /// 环形条目缓冲。
  log_entries: Mutex<VecDeque<SlowLogEntry>>,
  /// 自增 id 源。
  id: AtomicI64,
}

impl SlowLogContainer {
  /// libs/server/Metrics/Slowlog/SlowLogContainer.cs:SlowLogContainer（构造）。
  ///
  /// 缓冲以 [`VecDeque::new`] 惰性分配、随入队几何增长——严禁改回
  /// `VecDeque::with_capacity(size)` 按上限即刻预分配：slowlog-max-len 允许
  /// 配到 2^31-1 量级，预分配形启动即巨量分配 abort；C# 侧 ConcurrentQueue
  /// 无预分配容量概念（段链懒增长），`size` 仅作逻辑裁剪上限，本形态与其
  /// 懒增长语义逐位对齐（满容稳态后条目驻留量封顶于 size，与环形裁剪一致）。
  pub fn new(size: i32) -> Self {
    let cap = size.max(0) as usize;
    Self {
      size: cap,
      log_entries: Mutex::new(VecDeque::new()),
      id: AtomicI64::new(0),
    }
  }

  /// libs/server/Metrics/Slowlog/SlowLogContainer.cs:Count
  ///
  /// 当前条目数。
  pub fn count(&self) -> i32 {
    self.log_entries.lock().len() as i32
  }

  /// libs/server/Metrics/Slowlog/SlowLogContainer.cs:Add
  ///
  /// 以自动分配 id 入库，满容时先出后入环形裁剪；缓冲维持构造口
  /// 惰性分配约定（见 [`Self::new`] 文注），push_back 按需几何增长，
  /// 满容稳态条目驻留量封顶于 size，绝无按上限预分配。
  /// 容量为 0 直接短路，零锁且不消耗自增 id；取号在临界区内完成，
  /// 与 push_back 原子绑定，物理顺序与 id 单调严格一致。
  pub fn add(&self, mut entry: SlowLogEntry) {
    if self.size == 0 {
      return;
    }
    let mut entries = self.log_entries.lock();
    entry.id = self.id.fetch_add(1, Relaxed);
    if entries.len() >= self.size {
      entries.pop_front();
    }
    entries.push_back(entry);
  }

  /// 预置自增 id 源至 `id`（下条入库条目即取 `id` 起号）。
  ///
  /// 仅供集成测试复核 2^31 越界形态——等价复现生产长跑自然累积到的计数
  /// 状态，不参与任何运行路径语义。
  #[doc(hidden)]
  pub fn seed_id(&self, id: i64) {
    self.id.store(id, Relaxed);
  }

  /// libs/server/Metrics/Slowlog/SlowLogContainer.cs:Clear
  ///
  /// 清空慢日志缓冲。
  pub fn clear(&self) {
    self.log_entries.lock().clear();
  }

  /// libs/server/Metrics/Slowlog/SlowLogContainer.cs:GetEntries
  ///
  /// 取最新 `count` 条快照（-1 返回全部）。
  pub fn get_entries(&self, count: i32) -> Vec<SlowLogEntry> {
    let entries = self.log_entries.lock();
    let total = entries.len();
    let take_count = if count < 0 {
      total
    } else {
      (count as usize).min(total)
    };
    let skip_count = total - take_count;
    let mut result = Vec::with_capacity(take_count);
    result.extend(entries.iter().skip(skip_count).cloned());
    result
  }
}
