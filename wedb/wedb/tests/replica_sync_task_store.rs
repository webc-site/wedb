//! 主端磁盘同步在册任务仓去重登记语义（迁自 src 内联 tests，零私有依赖）
//!
//! 对标 C# ReplicaSyncSessionTaskStore 的登记册口径：同副本去重、摘除重登、
//! 清空复位。

use wedb::server::replication::replica_sync_task_store::ReplicaSyncSessionTaskStore;

#[test]
fn duplicate_add_rejected_until_removed() {
  let store = ReplicaSyncSessionTaskStore::new();
  assert!(store.try_add(0xA1));
  // 同副本重复发起：次路被拒（并发覆盖竞态的入口收口）
  assert!(!store.try_add(0xA1));
  // 不同副本并存（C# 多会话数组形态）
  assert!(store.try_add(0xB2));
  // 摘除后可再次登记（断链重连场景）
  assert!(store.try_remove(0xA1));
  assert!(store.try_add(0xA1));
}

#[test]
fn remove_absent_is_false() {
  let store = ReplicaSyncSessionTaskStore::new();
  assert!(!store.try_remove(0xA1));
  store.try_add(0xA1);
  assert!(store.try_remove(0xA1));
  assert!(!store.try_remove(0xA1));
}

#[test]
fn clear_drops_all_registrations() {
  let store = ReplicaSyncSessionTaskStore::new();
  store.try_add(0xA1);
  store.try_add(0xB2);
  store.clear();
  // 清空后原键可重新登记
  assert!(store.try_add(0xA1));
  assert!(store.try_add(0xB2));
}
