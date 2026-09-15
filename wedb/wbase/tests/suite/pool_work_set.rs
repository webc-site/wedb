use std::{sync::Arc, thread, time::Duration};

use wbase::pool::EventWorkSet;

#[test]
fn test_event_work_set_basic() {
  let work_set: EventWorkSet<Vec<u8>, u64> = EventWorkSet::new();
  assert!(work_set.is_empty());
  assert_eq!(work_set.len(), 0);

  let key = b"key1".to_vec();
  assert!(work_set.try_add(key.clone(), 42));
  assert!(!work_set.try_add(key.clone(), 99));
  assert_eq!(work_set.len(), 1);
  assert!(work_set.contains(b"key1".as_slice()));
  assert!(!work_set.contains(b"key2".as_slice()));

  let snap = work_set.snapshot();
  assert_eq!(snap.len(), 1);
  assert_eq!(snap[0].0, b"key1".to_vec());
  assert_eq!(snap[0].1, 42);

  assert!(work_set.try_complete(b"key1".as_slice()));
  assert!(!work_set.try_complete(b"key1".as_slice()));
  assert!(work_set.is_empty());
}

#[test]
fn test_event_work_set_wait_completion() {
  let work_set = Arc::new(EventWorkSet::<Vec<u8>, u32>::new());
  let key = b"async_key".to_vec();
  assert!(work_set.try_add(key.clone(), 100));

  let set_clone = Arc::clone(&work_set);
  let handle = thread::spawn(move || {
    thread::sleep(Duration::from_millis(50));
    assert!(set_clone.try_complete(b"async_key".as_slice()));
  });

  work_set.wait_for_completion(b"async_key".as_slice());
  assert!(!work_set.contains(b"async_key".as_slice()));
  handle.join().unwrap();
}
