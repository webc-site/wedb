//! 集群工厂抽象（对标 libs/server/Cluster/IClusterFactory.cs）

use std::sync::Arc;

use super::i_cluster_provider::IClusterProvider;

/// libs/server/Cluster/IClusterFactory.cs:IClusterFactory
///
/// 集群工厂抽象
pub trait IClusterFactory: Send + Sync {
  /// 集群提供者类型
  type Provider: IClusterProvider;

  /// 创建集群提供者实例
  fn create_cluster_provider(&self) -> Arc<Self::Provider>;
}
