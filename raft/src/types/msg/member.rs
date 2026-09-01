use std::collections::BTreeMap;

use crate::types::{Node, NodeId};

#[derive(bitcode::Encode, bitcode::Decode, Debug, Clone, PartialEq, Eq, Default)]
pub struct GetMemberReq {}

#[derive(bitcode::Encode, bitcode::Decode, Debug, Clone, PartialEq, Eq, Default)]
pub struct GetMemberReply {
  pub node_id: NodeId,
  pub current_leader: Option<NodeId>,
  pub membership: BTreeMap<NodeId, Node>,
}
