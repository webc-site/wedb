use std::sync::Arc;

use wtxn::{TxnKeyEntryComparison, TxnWatchedKeysContainer, WatchVersionMap};

fn container() -> (TxnWatchedKeysContainer, Arc<WatchVersionMap>) {
  let map = Arc::new(WatchVersionMap::new(64));
  (TxnWatchedKeysContainer::new(Arc::clone(&map)), map)
}

#[test]
fn add_watch_then_untouched_key_validates() {
  let (mut c, _) = container();
  c.add_watch(b"user:1");
  assert!(c.validate_watch_version());
}

#[test]
fn modified_watched_key_fails_validation() {
  let (mut c, map) = container();
  c.add_watch(b"user:1");
  map.increment_version(TxnKeyEntryComparison::key_hash(b"user:1") as u64);
  assert!(!c.validate_watch_version());
}

#[test]
fn reset_clears_all_watches() {
  let (mut c, map) = container();
  c.add_watch(b"a");
  c.add_watch(b"b");
  map.increment_version(TxnKeyEntryComparison::key_hash(b"a") as u64);
  c.reset();
  assert!(c.validate_watch_version());
}
