use std::{
  borrow::Borrow,
  hash::Hash,
  mem::take,
  sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
  },
};

use gxhash::HashMap;
use parking_lot::RwLock;

/// 驱动生命周期管理接口
pub trait DriverLifecycle: Send + Sync {
  /// 驱动是否处于活动连接状态
  fn is_active(&self) -> bool;

  /// 释放驱动持有的外部资源与连接
  fn dispose(&self);
}

/// libs/cluster/Server/Replication/PrimaryOps/AofOperations/AofSyncDriverStore.cs:AofSyncDriverStore
///
/// 通用并发安全驱动生命周期注册表，消除多会话存储在增删查与清理上的重复样板代码
#[derive(Debug)]
pub struct DriverRegistry<K, V> {
  drivers: RwLock<HashMap<K, Arc<V>>>,
  is_disposed: AtomicBool,
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
      drivers: RwLock::new(HashMap::default()),
      is_disposed: AtomicBool::new(false),
    }
  }

  /// 检查容器是否已被处置关闭
  #[inline]
  pub fn is_disposed(&self) -> bool {
    self.is_disposed.load(Ordering::Acquire)
  }

  /// 登记或更新驱动实例；若容器已关闭则返回 None
  pub fn register(&self, key: K, driver: Arc<V>) -> Option<Arc<V>> {
    if self.is_disposed() {
      return None;
    }
    let mut map = self.drivers.write();
    if self.is_disposed() {
      return None;
    }
    map.insert(key, driver)
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
    let map = self.drivers.read();
    map.get(key).cloned()
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
    let mut map = self.drivers.write();
    map.remove(key)
  }

  /// 当前在册驱动数量
  pub fn count(&self) -> usize {
    if self.is_disposed() {
      return 0;
    }
    self.drivers.read().len()
  }

  /// 当前在册驱动是否为空
  #[inline]
  pub fn is_empty(&self) -> bool {
    if self.is_disposed() {
      return true;
    }
    self.drivers.read().is_empty()
  }

  /// 收集当前在册全部驱动句柄快照
  pub fn snapshot(&self) -> Vec<Arc<V>> {
    if self.is_disposed() {
      return Vec::new();
    }
    let map = self.drivers.read();
    map.values().cloned().collect()
  }

  /// 遍历在册驱动执行只读借用闭包（持读锁，零堆分配与零 Arc 克隆）
  pub fn for_each(&self, mut f: impl FnMut(&V)) {
    if self.is_disposed() {
      return;
    }
    let map = self.drivers.read();
    for v in map.values() {
      f(v.as_ref());
    }
  }

  /// 遍历并统计符合条件的驱动数量
  pub fn count_by(&self, mut predicate: impl FnMut(&V) -> bool) -> usize {
    if self.is_disposed() {
      return 0;
    }
    let map = self.drivers.read();
    map.values().filter(|d| predicate(d.as_ref())).count()
  }

  /// 若键不存在则执行初始化闭包并原子插入，返回实例句柄；线程安全无竞争
  pub fn get_or_insert_with(&self, key: K, f: impl FnOnce() -> Arc<V>) -> Option<Arc<V>> {
    if self.is_disposed() {
      return None;
    }
    let mut map = self.drivers.write();
    if self.is_disposed() {
      return None;
    }
    Some(map.entry(key).or_insert_with(f).clone())
  }
}

impl<K, V> DriverRegistry<K, V>
where
  K: Eq + Hash,
  V: DriverLifecycle,
{
  /// 批量保留符合条件的驱动，已淘汰的驱动将在释放写锁后执行 dispose 释放
  pub fn retain(&self, mut predicate: impl FnMut(&K, &V) -> bool) -> Vec<K>
  where
    K: Clone,
  {
    if self.is_disposed() {
      return Vec::new();
    }

    let (removed_keys, removed_drivers) = {
      let mut map = self.drivers.write();
      let mut removed_keys = Vec::new();
      let mut removed_drivers = Vec::new();
      map.retain(|k, v| {
        if predicate(k, v.as_ref()) {
          true
        } else {
          removed_keys.push(k.clone());
          removed_drivers.push(v.clone());
          false
        }
      });
      (removed_keys, removed_drivers)
    };

    for driver in removed_drivers {
      driver.dispose();
    }

    removed_keys
  }

  /// 释放所有注册驱动并置位已关闭标志（在释放写锁后执行驱动清理，杜绝锁倒挂死锁）
  pub fn dispose(&self) {
    if self
      .is_disposed
      .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
      .is_err()
    {
      return;
    }

    let drivers = {
      let mut map = self.drivers.write();
      take(&mut *map)
    };
    for driver in drivers.values() {
      driver.dispose();
    }
  }

  /// 清空并释放当前在册驱动，但保持容器开放可复用
  pub fn reset(&self) {
    let drivers = {
      let mut map = self.drivers.write();
      take(&mut *map)
    };
    for driver in drivers.values() {
      driver.dispose();
    }
  }
}

#[cfg(test)]
mod tests {
  use std::sync::atomic::AtomicUsize;

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

    let d1 = Arc::new(MockDriver::new(true));
    let d2 = Arc::new(MockDriver::new(false));

    registry.register("node-1".to_string(), d1.clone());
    registry.register("node-2".to_string(), d2.clone());

    assert_eq!(registry.count(), 2);
    assert_eq!(registry.count_by(|d| d.is_active()), 1);
    assert!(registry.get("node-1").is_some());

    // 移除单个驱动
    let removed = registry.remove("node-2");
    assert!(removed.is_some());
    assert_eq!(registry.count(), 1);

    // reset 清理与释放
    registry.reset();
    assert_eq!(registry.count(), 0);
    assert_eq!(d1.dispose_count.load(Ordering::Acquire), 1);
    assert!(!registry.is_disposed());

    // dispose 彻底关闭
    registry.dispose();
    assert!(registry.is_disposed());
    assert!(
      registry
        .register("node-3".to_string(), Arc::new(MockDriver::new(true)))
        .is_none()
    );
  }
}
