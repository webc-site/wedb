//! 线程 ID 分配面测试（自 tests/main.rs 迁入）

#[test]
fn test_thread_id_uniqueness() {
  use std::{
    sync::{Arc, Mutex},
    thread,
  };

  use wbase::{map::HashSet, thread::*};

  let id1 = current_thread_id();
  let id2 = current_thread_id();
  assert_eq!(id1, id2, "Same thread should have stable ID");
  assert!(id1 > 0);

  let set = Arc::new(Mutex::new(HashSet::default()));
  let mut handles = Vec::new();

  for _ in 0..16 {
    let set = Arc::clone(&set);
    handles.push(thread::spawn(move || {
      let tid = current_thread_id();
      let mut lock = set.lock().unwrap();
      assert!(lock.insert(tid), "Thread ID must be globally unique");
    }));
  }

  for h in handles {
    h.join().unwrap();
  }

  assert_eq!(set.lock().unwrap().len(), 16);
}
