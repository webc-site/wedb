//! 集群抽象与契约层（对标 libs/server/Cluster/ 与 libs/cluster/）

pub mod cluster_preferred_endpoint_type;
pub mod cluster_slot_verification_input;
pub mod i_cluster_factory;
pub mod i_cluster_provider;
pub mod i_cluster_session;
pub mod key_spec;
pub mod role_info;

pub use cluster_preferred_endpoint_type::ClusterPreferredEndpointType;
pub use cluster_slot_verification_input::ClusterSlotVerificationInput;
pub use i_cluster_factory::IClusterFactory;
pub use i_cluster_provider::{CheckpointCallbackFace, IClusterProvider, ManagerType};
pub use i_cluster_session::IClusterSession;
pub use key_spec::{
  KeySpecificationFlags, SimpleRespKeySpec, SimpleRespKeySpecBeginSearch, SimpleRespKeySpecFindKeys,
};
pub use role_info::{NodeRole, RoleInfo};
pub use wnode::StoreType;

pub use crate::server::replication::checkpoint_entry::CheckpointMetadata;
