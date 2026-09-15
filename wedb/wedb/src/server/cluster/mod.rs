//! 集群抽象与契约层（对标 libs/server/Cluster/ 与 libs/cluster/）

pub mod cluster_preferred_endpoint_type;
pub mod i_cluster_factory;
pub mod i_cluster_provider;

pub use cluster_preferred_endpoint_type::ClusterPreferredEndpointType;
pub use i_cluster_factory::IClusterFactory;
pub use i_cluster_provider::{CheckpointCallbackFace, IClusterProvider};

pub use crate::server::replication::checkpoint_entry::CheckpointMetadata;
