use std::sync::{Arc, Weak};

use crate::server::replication::{
  driver_registry::DriverRegistry, replica_replay_driver::ReplicaReplayDriver,
  replica_replay_task::ReplayAssets, replication_manager::ReplicationManager,
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
  /// 原子添加或获取指定物理子日志的重放驱动（杜绝并发竞争与重复创建）；
  /// 容器已关闭返回 None（断链处置后须经理 recovery 面重建，杜绝孤儿驱动）
  pub fn add_replica_replay_driver(
    &self,
    physical_sublog_idx: usize,
    assets: Option<Arc<ReplayAssets>>,
    rm: Weak<ReplicationManager>,
  ) -> Option<Arc<ReplicaReplayDriver>> {
    self.registry.get_or_insert_with(physical_sublog_idx, || {
      Arc::new(ReplicaReplayDriver::new(physical_sublog_idx, assets, rm))
    })
  }

  /// libs/cluster/Server/Replication/ReplicaOps/AOFReplay/ReplicaReplayDriverStore.cs:Dispose
  ///
  /// 释放并清理所有重放驱动（幂等：内部 disposed 标志经 CAS 守卫，重复
  /// 调用空转）。代际容器关闭即拒新注册，重连须由 recovery 面换代重建
  /// （对标 C# ResetReplicaReplayDriverStore 的 dispose + new 换代替换）
  pub fn dispose(&self) {
    self.registry.dispose();
  }

  /// 检查驱动存储是否已处置关闭
  #[inline]
  pub fn is_disposed(&self) -> bool {
    self.registry.is_disposed()
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

    let d0 = store
      .add_replica_replay_driver(0, None, Weak::new())
      .expect("开放容器可注册");
    assert_eq!(d0.physical_sublog_idx, 0);
    assert!(store.get_replay_driver(0).is_some());

    // dispose 后注册被拒（杜绝孤儿驱动，重连须经理 recovery 面重建）
    store.dispose();
    assert!(store.get_replay_driver(0).is_none());
    assert!(
      store
        .add_replica_replay_driver(1, None, Weak::new())
        .is_none()
    );
  }
}
