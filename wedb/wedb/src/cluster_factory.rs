use std::sync::Arc;

use crate::server::cluster_provider::ClusterProvider;

/// libs/cluster/ClusterFactory.cs:ClusterFactory
pub struct ClusterFactory;

impl ClusterFactory {
  /// libs/cluster/ClusterFactory.cs:CreateClusterProvider
  pub fn create_cluster_provider(&self) -> Arc<ClusterProvider> {
    ClusterProvider::new()
  }
}
