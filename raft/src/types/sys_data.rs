use std::collections::BTreeMap;

use super::{LogId, Node, NodeId, StoredMembership};

#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct SysData {
  pub last_applied: Option<LogId>,
  pub nodes: BTreeMap<NodeId, Node>,
  pub membership: StoredMembership,
}
