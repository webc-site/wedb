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
/// id 以 `AtomicI64` 独立无锁递增，与条目锁解耦。
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
    Self {
      size: size.max(0) as usize,
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
  /// 以自动分配 id 入库，超出容量即裁掉最旧条目。
  pub fn add(&self, mut entry: SlowLogEntry) {
    entry.id = self.id.fetch_add(1, Relaxed) as i32;
    let mut entries = self.log_entries.lock();
    entries.push_back(entry);
    while entries.len() > self.size {
      entries.pop_front();
    }
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
    if count < 0 || count as usize >= entries.len() {
      return entries.iter().cloned().collect();
    }
    entries
      .iter()
      .skip(entries.len() - count as usize)
      .cloned()
      .collect()
  }
}

#[cfg(test)]
mod tests {
  use wresp::RespCommand;

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
