use zenoh::key_expr::KeyExpr;

use crate::types::NodeId;

pub const RAFT_PREFIX: &str = "wedb/raft";
pub const LIVELINESS_SUB_PATTERN: &str = "wedb/raft/*/liveliness";
pub const FORWARD_BROADCAST_KEY: &str = "wedb/raft/*/forward";

#[inline]
pub fn raft_keyexpr_str(target_id: NodeId, action: &str) -> String {
  format!("{RAFT_PREFIX}/{target_id}/{action}")
}

#[inline]
pub fn raft_keyexpr(target_id: NodeId, action: &str) -> KeyExpr<'static> {
  KeyExpr::new(raft_keyexpr_str(target_id, action)).expect("valid raft keyexpr")
}

#[inline]
pub fn raft_append_key(node_id: NodeId) -> String {
  raft_keyexpr_str(node_id, "append_entries")
}

#[inline]
pub fn raft_vote_key(node_id: NodeId) -> String {
  raft_keyexpr_str(node_id, "vote")
}

#[inline]
pub fn raft_snapshot_key(node_id: NodeId) -> String {
  raft_keyexpr_str(node_id, "snapshot")
}

#[inline]
pub fn raft_forward_key(node_id: NodeId) -> String {
  raft_keyexpr_str(node_id, "forward")
}

#[inline]
pub fn raft_liveliness_key(node_id: NodeId) -> String {
  raft_keyexpr_str(node_id, "liveliness")
}

#[inline]
pub fn raft_stream_data_key(node_id: NodeId) -> KeyExpr<'static> {
  raft_keyexpr(node_id, "stream_data")
}

#[inline]
pub fn raft_stream_ack_key(node_id: NodeId) -> KeyExpr<'static> {
  raft_keyexpr(node_id, "stream_ack")
}
