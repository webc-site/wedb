use crate::endpoint::Endpoint;
use crate::types::NodeId;

#[derive(bitcode::Encode, bitcode::Decode, Debug, Default, Clone, PartialEq, Eq)]
pub struct JoinRequest {
  pub node_id: NodeId,
  pub endpoint: Endpoint,
}
