//! 按键的未完成清理工作集合（对标 libs/server/Resp/Vector/Cleanup/VectorSetCleanupWorkSet.cs）
//!
//! 条目仅在清理完成后移除，因此"集合中不存在"即意味着工作已完成（而非仅出队）。
//! 基于 papaya 并发字典与 event_listener 异步事件通知，消除忙轮询自旋。

use std::{hint::spin_loop, thread};

use event_listener::{Event, Listener};
use whasher::{GxPapayaMap as ConcurrentMap, new_papaya_map};

/// 按键的未完成清理工作集合（按键字节的字典序等价比较）。
pub struct VectorSetCleanupWorkSet<TValue> {
  entries: ConcurrentMap<Vec<u8>, TValue>,
  event: Event,
}

impl<TValue> Default for VectorSetCleanupWorkSet<TValue> {
  fn default() -> Self {
    Self {
      entries: new_papaya_map(),
      event: Event::new(),
    }
  }
}

impl<TValue> VectorSetCleanupWorkSet<TValue> {
  /// 创建空集合。
  pub fn new() -> Self {
    Self::default()
  }

  /// 是否还有待处理工作。
  pub fn is_empty(&self) -> bool {
    self.entries.is_empty()
  }

  /// 待处理工作数量。
  pub fn len(&self) -> usize {
    self.entries.pin().len()
  }

  /// key 是否仍有待处理工作。
  pub fn contains(&self, key: &[u8]) -> bool {
    self.entries.pin().contains_key(key)
  }

  /// libs/server/Resp/Vector/Cleanup/VectorSetCleanupWorkSet.cs:WaitForCompletion
  ///
  /// 同步等待 key 的工作完成；禁止在持有任何 Vector Set 锁时调用（会死锁）。
  /// 采用短自旋 + 事件等待，消除死循环忙轮询 CPU 100%。
  pub fn wait_for_completion(&self, key: &[u8]) {
    let mut spins = 0u32;
    while self.contains(key) {
      if spins < 32 {
        spin_loop();
        spins += 1;
      } else if spins < 64 {
        thread::yield_now();
        spins += 1;
      } else {
        let listener = self.event.listen();
        if !self.contains(key) {
          break;
        }
        listener.wait();
      }
    }
  }

  /// 异步等待 key 的工作完成；事件驱动，0 CPU 忙轮询。
  pub async fn wait_for_completion_async(&self, key: &[u8]) {
    while self.contains(key) {
      let listener = self.event.listen();
      if !self.contains(key) {
        break;
      }
      listener.await;
    }
  }

  /// 为 key 登记工作；已有待处理工作时返回 false。
  pub fn try_add(&self, key: Vec<u8>, value: TValue) -> bool {
    self.entries.pin().try_insert(key, value).is_ok()
  }

  /// libs/server/Resp/Vector/Cleanup/VectorSetCleanupWorkSet.cs:TryComplete
  ///
  /// 移除条目，标记工作完成；不存在时返回 false。
  /// 移除成功后广播通知所有等待者。
  pub fn try_complete(&self, key: &[u8]) -> bool {
    let removed = self.entries.pin().remove(key).is_some();
    if removed {
      self.event.notify(usize::MAX);
    }
    removed
  }

  /// 快照全部待处理工作（供消费者单趟处理整个积压）。
  pub fn snapshot(&self) -> Vec<(Vec<u8>, TValue)>
  where
    TValue: Clone,
  {
    self
      .entries
      .pin()
      .iter()
      .map(|(k, v)| (k.clone(), v.clone()))
      .collect()
  }
}
