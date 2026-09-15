use std::sync::Arc;

use crate::server::replication::{
  driver_registry::DriverRegistry, replica_replay_driver::ReplicaReplayDriver,
};

/// libs/cluster/Server/Replication/ReplicaOps/AOFReplay/ReplicaReplayDriverStore.cs:ReplicaReplayDriverStore
///
/// 副本重放驱动存储容器，管理多物理子日志的重放生命周期（组合复用通用驱动注册表抽象）
#[derive(Debug)]
pub struct ReplicaReplayDriverStore {
  registry: DriverRegistry<usize, ReplicaReplayDriver>,
  sublog_count: usize,
}

impl ReplicaReplayDriverStore {
  /// libs/cluster/Server/Replication/ReplicaOps/AOFReplay/ReplicaReplayDriverStore.cs:ReplicaReplayDriverStore
  pub fn new(sublog_count: usize) -> Self {
    Self {
      registry: DriverRegistry::new(),
      sublog_count,
    }
  }

  /// 物理子日志数量
  #[inline]
  pub fn sublog_count(&self) -> usize {
    self.sublog_count
  }

  /// libs/cluster/Server/Replication/ReplicaOps/AOFReplay/ReplicaReplayDriverStore.cs:GetReplayDriver
  ///
  /// 获取指定物理子日志的重放驱动
  pub fn get_replay_driver(&self, physical_sublog_idx: usize) -> Option<Arc<ReplicaReplayDriver>> {
    self.registry.get(&physical_sublog_idx)
  }

  /// libs/cluster/Server/Replication/ReplicaOps/AOFReplay/ReplicaReplayDriverStore.cs:AddReplicaReplayDriver
  ///
  /// 原子添加或获取指定物理子日志的重放驱动（杜绝并发竞争与重复创建）
  pub fn add_replica_replay_driver(&self, physical_sublog_idx: usize) -> Arc<ReplicaReplayDriver> {
    self
      .registry
      .get_or_insert_with(physical_sublog_idx, || {
        Arc::new(ReplicaReplayDriver::new(physical_sublog_idx))
      })
      .unwrap_or_else(|| Arc::new(ReplicaReplayDriver::new(physical_sublog_idx)))
  }

  /// libs/cluster/Server/Replication/ReplicaOps/AOFReplay/ReplicaReplayDriverStore.cs:Dispose
  ///
  /// 释放并清理所有重放驱动
  pub fn dispose(&self) {
    self.registry.dispose();
  }

  /// 重置驱动存储（重置在册驱动以备复用）
  pub fn reset(&self) {
    self.registry.reset();
  }

  /// 检查是否有活跃/在册驱动
  #[inline]
  pub fn has_drivers(&self) -> bool {
    !self.registry.is_empty()
  }

  /// 检查驱动存储是否为空
  #[inline]
  pub fn is_empty(&self) -> bool {
    self.registry.is_empty()
  }

  /// 在册驱动数量
  #[inline]
  pub fn count(&self) -> usize {
    self.registry.count()
  }
}

#[cfg(test)]
mod tests {
  use super::*;

  #[test]
  fn test_replay_driver_store() {
    let store = ReplicaReplayDriverStore::new(2);
    assert!(store.get_replay_driver(0).is_none());

    let d0 = store.add_replica_replay_driver(0);
    assert_eq!(d0.physical_sublog_idx, 0);
    assert!(store.get_replay_driver(0).is_some());

    store.dispose();
    assert!(store.get_replay_driver(0).is_none());
  }
}
