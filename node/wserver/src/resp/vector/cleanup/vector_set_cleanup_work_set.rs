//! 按键的未完成清理工作集合（对标 libs/server/Resp/Vector/Cleanup/VectorSetCleanupWorkSet.cs）
//!
//! 条目仅在清理完成后移除，因此"集合中不存在"即意味着工作已完成（而非仅出队）。

use std::thread;

use papaya::HashMap as ConcurrentMap;

/// 按键的未完成清理工作集合（按键字节的字典序等价比较）。
pub struct VectorSetCleanupWorkSet<TValue> {
  entries: ConcurrentMap<Vec<u8>, TValue>,
}

impl<TValue> Default for VectorSetCleanupWorkSet<TValue> {
  fn default() -> Self {
    Self {
      entries: ConcurrentMap::new(),
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

  /// key 是否仍有待处理工作。
  pub fn contains(&self, key: &[u8]) -> bool {
    self.entries.pin().contains_key(key)
  }

  /// libs/server/Resp/Vector/Cleanup/VectorSetCleanupWorkSet.cs:WaitForCompletion
  ///
  /// 自旋等待 key 的工作完成；禁止在持有任何 Vector Set 锁时调用（会死锁）。
  pub fn wait_for_completion(&self, key: &[u8]) {
    while self.contains(key) {
      thread::yield_now();
    }
  }

  /// 为 key 登记工作；已有待处理工作时返回 false。
  pub fn try_add(&self, key: Vec<u8>, value: TValue) -> bool {
    self.entries.pin().try_insert(key, value).is_ok()
  }

  /// libs/server/Resp/Vector/Cleanup/VectorSetCleanupWorkSet.cs:TryComplete
  ///
  /// 移除条目，标记工作完成；不存在时返回 false。
  pub fn try_complete(&self, key: &[u8]) -> bool {
    self.entries.pin().remove(key).is_some()
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

#[cfg(test)]
mod tests {
  use super::*;

  #[test]
  fn add_complete_lifecycle() {
    let set: VectorSetCleanupWorkSet<u64> = VectorSetCleanupWorkSet::new();
    assert!(set.is_empty());

    assert!(set.try_add(b"k1".to_vec(), 7));
    // 重复登记失败
    assert!(!set.try_add(b"k1".to_vec(), 8));
    assert!(set.contains(b"k1"));
    assert_eq!(set.snapshot(), vec![(b"k1".to_vec(), 7)]);

    // 完成后不可再查
    assert!(set.try_complete(b"k1"));
    assert!(!set.contains(b"k1"));
    assert!(!set.try_complete(b"k1"));
    assert!(set.is_empty());

    // wait_for_completion 对无工作 key 立即返回
    set.wait_for_completion(b"nothing");
  }
}
