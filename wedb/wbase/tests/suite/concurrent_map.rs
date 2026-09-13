//! 并发字典与集合（自 whasher 门面迁回定义地；对标 C# `ConcurrentDictionary` 语义）

use std::{sync::Arc, thread};

use wbase::map::{ConcurrentMap, new_concurrent_map, new_concurrent_set};

/// 多线程并发写入后读全量（原 whasher::tests::test_papaya_map）
#[test]
fn concurrent_map_parallel_insert_then_read() {
  let map: Arc<ConcurrentMap<u64, u64>> = Arc::new(new_concurrent_map());

  let mut handles = Vec::new();
  for t in 0..4u64 {
    let map_clone = Arc::clone(&map);
    handles.push(thread::spawn(move || {
      let map_pin = map_clone.pin();
      for i in 0..1000u64 {
        let key = t * 1000 + i;
        map_pin.insert(key, key * 2);
      }
    }));
  }

  for h in handles {
    h.join().unwrap();
  }

  let pin = map.pin();
  assert_eq!(map.len(), 4000);

  for key in 0..4000u64 {
    assert_eq!(pin.get(&key), Some(&(key * 2)));
  }
  assert_eq!(pin.get(&9999), None);
}

/// 并发集合基本插入与成员判定
#[test]
fn concurrent_set_insert_and_contains() {
  let set = new_concurrent_set();
  {
    let pin = set.pin();
    pin.insert(1u64);
    pin.insert(2u64);
    assert!(pin.contains(&1));
    assert!(!pin.contains(&3));
  }
  assert_eq!(set.len(), 2);
}
