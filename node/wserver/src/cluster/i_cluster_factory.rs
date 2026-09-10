//! 集群工厂（对标 libs/server/Cluster/IClusterFactory.cs）
//!
//! C# 为接口 + 分布式工厂（ClusterFactory 装配 ClusterProvider 与
//! Tsavorite 检查点管理器）；rust 侧集群 provider 面已由
//! [`SingleNodeClusterProvider`] 承接单机语义，检查点管理器面由 wkv
//! `CheckpointManager` + databases 域承接。

use super::i_cluster_provider::SingleNodeClusterProvider;

/// 集群工厂
pub struct IClusterFactory;

impl IClusterFactory {
  /// libs/server/Cluster/IClusterFactory.cs:CreateClusterProvider
  ///
  /// 装配集群 provider：单机形态恒为 [`SingleNodeClusterProvider`]。
  /// C# 入参 store / rangeIndexManager 为分布式配置依赖，单机 provider
  /// 无集群配置面，参数省略
  pub fn create_cluster_provider(&self) -> SingleNodeClusterProvider {
    SingleNodeClusterProvider::default()
  }

  /// libs/server/Cluster/IClusterFactory.cs:CreateCheckpointManager
  ///
  /// 装配检查点管理器。缺口说明：C# 产出 Tsavorite
  /// DeviceLogCommitCheckpointManager；rust 检查点面由 wkv
  /// `CheckpointManager` + databases 域承担，本域无对应装配入口
  /// （豁免登记见 js/check/ignore/libs/server/Cluster/IClusterFactory.yml）
  pub fn create_checkpoint_manager() {}
}
