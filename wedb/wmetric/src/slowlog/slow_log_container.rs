//! 在 garnet 中的相对路径: libs/server/Metrics/SlowLog? 对标 C# SlowLogContainer
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
  pub fn new(size: i32) -> Self {
    let cap = size.max(0) as usize;
    Self {
      size: cap,
      log_entries: Mutex::new(VecDeque::with_capacity(cap)),
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
  /// 以自动分配 id 入库，满容时先出后入环形裁剪，零动态扩容。
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

#[cfg(test)]
mod tests {
  use wresp::command::RespCommand;

  use super::{SlowLogContainer, SlowLogEntry};

  fn entry() -> SlowLogEntry {
    SlowLogEntry {
      id: 0,
      timestamp: 1000,
      duration: 42,
      command: RespCommand::Get,
      arguments: None,
      client_ip_port: "127.0.0.1:1234".into(),
      client_name: "cli".into(),
    }
  }

  #[test]
  fn add_assigns_ids_and_enforces_capacity() {
    let log = SlowLogContainer::new(3);
    for _ in 0..5 {
      log.add(entry());
    }
    assert_eq!(log.count(), 3);

    let entries = log.get_entries(-1);
    // 保留最新的 3 条，id 连续递增。
    assert_eq!(
      entries.iter().map(|e| e.id).collect::<Vec<_>>(),
      vec![2, 3, 4]
    );
  }

  #[test]
  fn get_entries_tail_snapshot() {
    let log = SlowLogContainer::new(10);
    for _ in 0..4 {
      log.add(entry());
    }
    let latest_two = log.get_entries(2);
    assert_eq!(
      latest_two.iter().map(|e| e.id).collect::<Vec<_>>(),
      vec![2, 3]
    );
    assert_eq!(log.get_entries(-1).len(), 4);
    // 超出数量时返回全部。
    assert_eq!(log.get_entries(100).len(), 4);
  }

  #[test]
  fn clear_empties_log() {
    let log = SlowLogContainer::new(4);
    log.add(entry());
    log.clear();
    assert_eq!(log.count(), 0);
  }
}
