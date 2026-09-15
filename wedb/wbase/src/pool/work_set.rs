use std::{
  borrow::Borrow,
  collections::{HashMap, hash_map::Entry},
  hash::Hash,
  hint::spin_loop,
  thread,
};

use event_listener::{Event, Listener};
use parking_lot::Mutex;

/// 自旋阶段上限（对齐 `crate::backoff` 三阶退避第一阶段的 SPIN_LIMIT = 32 语义；
/// 因 backoff 为独立 feature 门控，此处按同一阶梯取值命名常量）
const SPIN_LIMIT: u32 = 32;
/// 让核阶段上限（超过后转入事件阻塞等待，避免 sleep 轮询）
const YIELD_LIMIT: u32 = 64;

/// 标准通用的按键未完成工作集合（对标 Garnet VectorSetCleanupWorkSet）
///
/// 条目仅在工作完成后移除，因此"集合中不存在"即意味着工作已完成（而非仅出队）。
/// 采用 Mutex<HashMap<K, V>> 结合短自旋 + event_listener::Event 异步/同步等待，
/// 零预分配大块内存，杜绝忙轮询 CPU 100% 消耗。
pub struct EventWorkSet<K, V> {
  entries: Mutex<HashMap<K, V>>,
  event: Event,
}

impl<K: Eq + Hash, V> Default for EventWorkSet<K, V> {
  fn default() -> Self {
    Self::new()
  }
}

impl<K: Eq + Hash, V> EventWorkSet<K, V> {
  /// 创建空工作集
  pub fn new() -> Self {
    Self {
      entries: Mutex::new(HashMap::new()),
      event: Event::new(),
    }
  }

  /// 是否还有待处理工作
  #[inline]
  pub fn is_empty(&self) -> bool {
    self.entries.lock().is_empty()
  }

  /// 待处理工作数量
  #[inline]
  pub fn len(&self) -> usize {
    self.entries.lock().len()
  }

  /// 检查 key 是否仍有待处理工作
  #[inline]
  pub fn contains<Q>(&self, key: &Q) -> bool
  where
    K: Borrow<Q>,
    Q: ?Sized + Hash + Eq,
  {
    self.entries.lock().contains_key(key)
  }

  /// 同步等待 key 的工作完成；禁止在持有相关锁时调用（防死锁）
  /// 采用短自旋 + 事件等待，消除死循环忙轮询
  pub fn wait_for_completion<Q>(&self, key: &Q)
  where
    K: Borrow<Q>,
    Q: ?Sized + Hash + Eq,
  {
    let mut spins = 0u32;
    while self.contains(key) {
      if spins < SPIN_LIMIT {
        spin_loop();
        spins += 1;
      } else if spins < YIELD_LIMIT {
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

  /// 异步等待 key 的工作完成；事件驱动，零 CPU 忙轮询
  pub async fn wait_for_completion_async<Q>(&self, key: &Q)
  where
    K: Borrow<Q>,
    Q: ?Sized + Hash + Eq,
  {
    while self.contains(key) {
      let listener = self.event.listen();
      if !self.contains(key) {
        break;
      }
      listener.await;
    }
  }

  /// 登记工作；已有待处理工作时返回 false
  #[inline]
  pub fn try_add(&self, key: K, value: V) -> bool {
    let mut map = self.entries.lock();
    if let Entry::Vacant(e) = map.entry(key) {
      e.insert(value);
      true
    } else {
      false
    }
  }

  /// 移除条目，标记工作完成；不存在时返回 false。
  /// 移除成功后广播通知所有等待者。
  #[inline]
  pub fn try_complete<Q>(&self, key: &Q) -> bool
  where
    K: Borrow<Q>,
    Q: ?Sized + Hash + Eq,
  {
    let removed = self.entries.lock().remove(key).is_some();
    if removed {
      self.event.notify(usize::MAX);
    }
    removed
  }

  /// 快照全部待处理工作（供消费者单趟处理整个积压）
  pub fn snapshot(&self) -> Vec<(K, V)>
  where
    K: Clone,
    V: Clone,
  {
    self
      .entries
      .lock()
      .iter()
      .map(|(k, v)| (k.clone(), v.clone()))
      .collect()
  }
}
