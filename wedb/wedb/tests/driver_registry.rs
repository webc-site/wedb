#![recursion_limit = "512"] // 泛型设备实例化下 async 状态机嵌套深（R27 CI 实证）
//! 通用并发驱动注册表生命周期语义（迁自 src 内联 tests，零私有依赖）
//!
//! DriverRegistry 泛型底座的登记/移除/快照/dispose 生命周期编排与并发
//! 登记并发度，MockDriver 自实现 [`DriverLifecycle`]。

use std::{
  sync::{
    Arc,
    atomic::{AtomicBool, AtomicUsize, Ordering},
  },
  thread::spawn,
};

use wedb::server::replication::driver_registry::{DriverLifecycle, DriverRegistry};

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
