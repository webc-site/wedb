//! 集群工厂抽象（对标 libs/server/Cluster/IClusterFactory.cs）

use std::sync::Arc;

use super::i_cluster_provider::IClusterProvider;

/// 集群工厂抽象
pub trait IClusterFactory: Send + Sync {
  /// 创建集群提供者实例
  fn create_cluster_provider(&self) -> Arc<dyn IClusterProvider>;
}
