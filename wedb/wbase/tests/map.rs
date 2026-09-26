//! papaya + gxhash 并发 map / set 公开 API 行为冒烟（自 src/map.rs 内嵌测试迁出；
//! seed 随机化与进程级共享属私有实现面，仍留 src 内嵌白盒测试）

#![cfg(all(feature = "map", feature = "set"))]

use wbase::map::{new_concurrent_map, new_concurrent_set};

#[test]
fn test_map() {
  let map = new_concurrent_map();
  let pin = map.pin();
  assert!(pin.is_empty());
  pin.insert("a", 1);
  assert_eq!(pin.get("a"), Some(&1));
  assert_eq!(pin.len(), 1);
  assert_eq!(pin.remove(&"a"), Some(&1));
  assert!(pin.is_empty());
}

#[test]
fn test_set() {
  let set = new_concurrent_set();
  let pin = set.pin();
  assert!(pin.is_empty());
  pin.insert("a");
  assert!(pin.contains("a"));
  assert_eq!(pin.len(), 1);
  assert!(!pin.is_empty());
  assert!(pin.remove(&"a"));
  assert!(pin.is_empty());
}
