use crate::types::NodeId;

#[derive(bitcode::Encode, bitcode::Decode, Debug, Default, Clone, PartialEq, Eq)]
pub struct LeaveRequest {
  pub node_id: NodeId,
}
