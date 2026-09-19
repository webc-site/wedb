use std::{
  borrow::Borrow,
  fmt,
  hash::Hash,
  sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
  },
};

use wbase::map::{ConcurrentMap, new_concurrent_map};

/// 驱动生命周期管理接口
pub trait DriverLifecycle: Send + Sync {
  /// 驱动是否处于活动连接状态
  fn is_active(&self) -> bool;

  /// 释放驱动持有的外部资源与连接
  fn dispose(&self);
}

/// 通用并发安全驱动生命周期注册表，消除多会话存储在增删查与清理上的重复样板代码
///
/// 基于 `papaya::HashMap` 与 `GxBuildHasher` 提供无锁并发访问能力，
/// 为 aof_sync_driver.rs 与 replica_replay_driver_store.rs 共用的
/// 泛型底座（驱动登记/移除/快照/dispose 生命周期编排）；C# AofSyncDriverStore
/// 的权威映射在 aof_sync_driver.rs，本泛型容器无 C# 同名对应物。
pub struct DriverRegistry<K, V> {
  drivers: ConcurrentMap<K, Arc<V>>,
  is_disposed: AtomicBool,
}

impl<K, V> fmt::Debug for DriverRegistry<K, V>
where
  K: Eq + Hash,
{
  fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    f.debug_struct("DriverRegistry")
      .field("count", &self.count())
      .field("is_disposed", &self.is_disposed())
      .finish()
  }
}

impl<K, V> Default for DriverRegistry<K, V>
where
  K: Eq + Hash,
{
  fn default() -> Self {
    Self::new()
  }
}

impl<K, V> DriverRegistry<K, V>
where
  K: Eq + Hash,
{
  /// 创建新的驱动注册表容器
  pub fn new() -> Self {
    Self {
      drivers: new_concurrent_map(),
      is_disposed: AtomicBool::new(false),
    }
  }

  /// 检查容器是否已被处置关闭
  #[inline]
  pub fn is_disposed(&self) -> bool {
    self.is_disposed.load(Ordering::Acquire)
  }

  /// 获取指定键的驱动句柄引用克隆
  pub fn get<Q>(&self, key: &Q) -> Option<Arc<V>>
  where
    K: Borrow<Q>,
    Q: Hash + Eq + ?Sized,
  {
    if self.is_disposed() {
      return None;
    }
    let pin = self.drivers.pin();
    pin.get(key).cloned()
  }

  /// 移除指定键的驱动并返回已移除实例
  pub fn remove<Q>(&self, key: &Q) -> Option<Arc<V>>
  where
    K: Borrow<Q>,
    Q: Hash + Eq + ?Sized,
  {
    if self.is_disposed() {
      return None;
    }
    let pin = self.drivers.pin();
    pin.remove(key).cloned()
  }

  /// 实例匹配条件移除：仅当键对应的驱动与传入实例为同一对象时原子移除并返回
  ///
  /// 对标 C# AofSyncDriverStore.TryRemove(AofSyncDriver) 的
  /// `syncDriver == aofSyncDriver` 引用匹配（libs/cluster/Server/Replication/
  /// PrimaryOps/AofOperations/AofSyncDriverStore.cs）——退场驱动与重挂置换
  /// 并发时绝不误删同键新驱动
  pub fn remove_if_current<Q>(&self, key: &Q, current: &Arc<V>) -> Option<Arc<V>>
  where
    K: Borrow<Q>,
    Q: Hash + Eq + ?Sized,
  {
    if self.is_disposed() {
      return None;
    }
    let pin = self.drivers.pin();
    pin
      .remove_if(key, |_, v| Arc::ptr_eq(v, current))
      .ok()
      .flatten()
      .map(|(_, v)| Arc::clone(v))
  }

  /// 当前在册驱动数量
  pub fn count(&self) -> usize {
    if self.is_disposed() {
      return 0;
    }
    self.drivers.pin().len()
  }

  /// 当前在册驱动是否为空
  #[inline]
  pub fn is_empty(&self) -> bool {
    if self.is_disposed() {
      return true;
    }
    self.drivers.pin().is_empty()
  }

  /// 收集当前在册全部驱动句柄快照
  pub fn snapshot(&self) -> Vec<Arc<V>> {
    if self.is_disposed() {
      return Vec::new();
    }
    let pin = self.drivers.pin();
    pin.values().cloned().collect()
  }

  /// 遍历在册驱动执行只读借用闭包（无读写锁争用，零堆分配与零 Arc 克隆）
  pub fn for_each(&self, mut f: impl FnMut(&V)) {
    if self.is_disposed() {
      return;
    }
    let pin = self.drivers.pin();
    for v in pin.values() {
      f(v.as_ref());
    }
  }

  /// 遍历并统计符合条件的驱动数量
  pub fn count_by(&self, mut predicate: impl FnMut(&V) -> bool) -> usize {
    if self.is_disposed() {
      return 0;
    }
    let pin = self.drivers.pin();
    pin.values().filter(|d| predicate(d.as_ref())).count()
  }
}

impl<K, V> DriverRegistry<K, V>
where
  K: Eq + Hash + Clone,
  V: DriverLifecycle,
{
  /// 登记或更新驱动实例；若容器已关闭则返回 None 并触发新实例释放，杜绝孤儿驱动残留
  pub fn register(&self, key: K, driver: Arc<V>) -> Option<Arc<V>> {
    if self.is_disposed() {
      return None;
    }
    let pin = self.drivers.pin();
    if self.is_disposed() {
      return None;
    }
    let key_clone = key.clone();
    let prev = pin.insert(key, driver).cloned();
    if self.is_disposed() {
      if let Some(d) = pin.remove(&key_clone) {
        d.dispose();
      }
      return None;
    }
    prev
  }

  /// 若键不存在则执行初始化闭包并原子插入，返回实例句柄；容器关闭时安全回滚
  pub fn get_or_insert_with(&self, key: K, f: impl FnOnce() -> Arc<V>) -> Option<Arc<V>> {
    if self.is_disposed() {
      return None;
    }
    let pin = self.drivers.pin();
    if self.is_disposed() {
      return None;
    }
    let key_clone = key.clone();
    let val = pin.get_or_insert_with(key, f).clone();
    if self.is_disposed() {
      if let Some(d) = pin.remove(&key_clone) {
        d.dispose();
      }
      return None;
    }
    Some(val)
  }

  /// 释放所有注册驱动并置位已关闭标志；逐项原子移出并排空，杜绝孤儿驱动残留与重复释放
  pub fn dispose(&self) {
    if self
      .is_disposed
      .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
      .is_err()
    {
      return;
    }

    loop {
      let pin = self.drivers.pin();
      if pin.is_empty() {
        break;
      }
      let keys: Vec<K> = pin.keys().cloned().collect();
      if keys.is_empty() {
        break;
      }
      for key in keys {
        if let Some(driver) = pin.remove(&key) {
          driver.dispose();
        }
      }
    }
  }

  /// 清空并释放当前在册驱动，逐项原子移出，杜绝并发误删与重复释放
  pub fn reset(&self) {
    let pin = self.drivers.pin();
    if pin.is_empty() {
      return;
    }
    let keys: Vec<K> = pin.keys().cloned().collect();
    for key in keys {
      if let Some(driver) = pin.remove(&key) {
        driver.dispose();
      }
    }
  }
}

#[cfg(test)]
mod tests {
  use std::{sync::atomic::AtomicUsize, thread::spawn};

  use super::*;

  struct MockDriver {
    active: AtomicBool,
    dispose_count: AtomicUsize,
  }

  impl MockDriver {
    fn new(active: bool) -> Self {
      Self {
        active: AtomicBool::new(active),
        dispose_count: AtomicUsize::new(0),
      }
    }
  }

  impl DriverLifecycle for MockDriver {
    fn is_active(&self) -> bool {
      self.active.load(Ordering::Acquire)
    }

    fn dispose(&self) {
      self.dispose_count.fetch_add(1, Ordering::AcqRel);
      self.active.store(false, Ordering::Release);
    }
  }

  #[test]
  fn test_driver_registry_lifecycle() {
    let registry: DriverRegistry<String, MockDriver> = DriverRegistry::new();
    assert_eq!(registry.count(), 0);
    assert!(registry.is_empty());

    let d1 = Arc::new(MockDriver::new(true));
    let d2 = Arc::new(MockDriver::new(false));

    registry.register("node-1".to_string(), d1.clone());
    registry.register("node-2".to_string(), d2.clone());

    assert_eq!(registry.count(), 2);
    assert!(!registry.is_empty());
    assert_eq!(registry.count_by(|d| d.is_active()), 1);
    assert!(registry.get("node-1").is_some());

    // snapshot
    let snapshot = registry.snapshot();
    assert_eq!(snapshot.len(), 2);

    // for_each
    let mut visited = 0;
    registry.for_each(|_| visited += 1);
    assert_eq!(visited, 2);

    // get_or_insert_with
    let existing =
      registry.get_or_insert_with("node-1".to_string(), || Arc::new(MockDriver::new(false)));
    assert!(existing.is_some());
    assert_eq!(registry.count(), 2);

    let created =
      registry.get_or_insert_with("node-3".to_string(), || Arc::new(MockDriver::new(true)));
    assert!(created.is_some());
    assert_eq!(registry.count(), 3);

    // 移除单个驱动
    let removed = registry.remove("node-2");
    assert!(removed.is_some());
    assert_eq!(registry.count(), 2);

    // reset 清理与释放
    registry.reset();
    assert_eq!(registry.count(), 0);
    assert!(registry.is_empty());
    assert_eq!(d1.dispose_count.load(Ordering::Acquire), 1);
    assert!(!registry.is_disposed());

    // dispose 彻底关闭
    registry.dispose();
    assert!(registry.is_disposed());
    assert!(
      registry
        .register("node-4".to_string(), Arc::new(MockDriver::new(true)))
        .is_none()
    );
    assert!(registry.get("node-1").is_none());
    assert!(registry.remove("node-1").is_none());
    assert_eq!(registry.count(), 0);
    assert!(registry.is_empty());
    assert!(registry.snapshot().is_empty());
    assert!(
      registry
        .get_or_insert_with("node-4".to_string(), || { Arc::new(MockDriver::new(true)) })
        .is_none()
    );
  }

  /// 实例匹配条件移除：同键不同实例不匹配不移除，同实例原子移除
  ///（对标 C# TryRemove(AofSyncDriver) 引用匹配语义）
  #[test]
  fn test_driver_registry_remove_if_current() {
    let registry: DriverRegistry<String, MockDriver> = DriverRegistry::new();
    let d1 = Arc::new(MockDriver::new(true));
    registry.register("n".to_string(), Arc::clone(&d1));

    // 同键不同实例：不匹配不移除
    let impostor = Arc::new(MockDriver::new(false));
    assert!(registry.remove_if_current("n", &impostor).is_none());
    assert_eq!(registry.count(), 1);
    assert_eq!(d1.dispose_count.load(Ordering::Acquire), 0);

    // 同实例：原子移除并返回
    let removed = registry.remove_if_current("n", &d1);
    assert!(removed.is_some());
    assert!(Arc::ptr_eq(&removed.unwrap(), &d1));
    assert_eq!(registry.count(), 0);
    assert!(registry.remove_if_current("n", &d1).is_none());
  }

  #[test]
  fn test_driver_registry_concurrent() {
    let registry = Arc::new(DriverRegistry::<usize, MockDriver>::new());
    let mut handles = Vec::new();

    for thread_id in 0..8 {
      let reg = Arc::clone(&registry);
      handles.push(spawn(move || {
        for i in 0..100 {
          let key = thread_id * 1000 + i;
          reg.register(key, Arc::new(MockDriver::new(i % 2 == 0)));
          assert!(reg.get(&key).is_some());
        }
      }));
    }

    for h in handles {
      h.join().unwrap();
    }

    assert_eq!(registry.count(), 800);
    assert_eq!(registry.count_by(|d| d.is_active()), 400);

    registry.dispose();
    assert!(registry.is_disposed());
    assert_eq!(registry.count(), 0);
  }
}
