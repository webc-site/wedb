//! 主端磁盘同步在册任务仓（ReplicaSyncSessionTaskStore）
//!
//! 对标 C# ReplicaSyncSessionTaskStore（数组 + SingleWriterMultiReaderLock 的
//! 去重登记册）：磁盘同步链入口按副本节点 id 去重登记，同一副本并发重复
//! 发起 CLUSTER INITIATE_REPLICA_SYNC 时次路被拒，杜绝两路同步体互拆推流
//! 驱动与截断钉线的并发覆盖。C# 会话对象携带 per-session 状态
//! （replicaNodeId / checkpointEntry / AOF 起止位点），rust 会话为无状态壳
//! （状态在 [`super::replica_sync_session::ReplicaSyncSession::
//! initiate_replica_sync`] 调用参数），故仓只登记在册副本节点 id。diskless
//! 链的会话册子（leader 攒批 / IsFirst / Clear）由
//! [`super::diskless_replication::replication_sync_manager`] 承接，不经本仓。

use wbase::map::{ConcurrentSet, new_concurrent_set};

/// 主端磁盘同步在册任务仓
///
/// 在 garnet 中的相对路径:
/// libs/cluster/Server/Replication/PrimaryOps/ReplicaSyncSessionTaskStore.cs
pub struct ReplicaSyncSessionTaskStore {
  sessions: ConcurrentSet<u128>,
}

impl ReplicaSyncSessionTaskStore {
  /// 构造空仓（C# 构造器 sessions[1] / numSessions=0 的无锁等价）
  pub fn new() -> Self {
    Self {
      sessions: new_concurrent_set(),
    }
  }

  /// 去重登记在册副本（C# TryAddReplicaSyncSession 磁盘链重载：同
  /// replicaNodeId 已在册即拒绝并记日志；insert 返回 false 即已存在）
  pub fn try_add(&self, replica_node_id: u128) -> bool {
    let fresh = self.sessions.pin().insert(replica_node_id);
    if !fresh {
      log::error!("Error syncSession for replica {replica_node_id:x} already exists");
    }
    fresh
  }

  /// 摘除登记（C# TryRemove；C# 会话 Dispose 的驱动清理在 rust 由会话体
  /// 自有退场路径承接，仓侧仅去登记）
  pub fn try_remove(&self, replica_node_id: u128) -> bool {
    self.sessions.pin().remove(&replica_node_id)
  }

  /// 清空在册（C# Dispose 的 `_disposed = true` + Array.Clear 半边，挂
  /// [`super::replication_manager::ReplicationManager::dispose`]；rust 会话
  /// 无持有资源，仅清登记）
  pub fn clear(&self) {
    self.sessions.pin().clear();
  }
}

impl Default for ReplicaSyncSessionTaskStore {
  fn default() -> Self {
    Self::new()
  }
}

#[cfg(test)]
mod tests {
  use super::*;

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
}
