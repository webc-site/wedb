use std::fmt::{Display, Formatter, Result as FmtResult};

use std::fs::File;

use bytes::Bytes;
use zenoh_raft::Raft as OpenRaft;
use zenoh_raft::alias::{
  EntryOf, LeaderIdOf, LogIdOf, SnapshotMetaOf, SnapshotOf, StoredMembershipOf, VoteOf,
};
use zenoh_raft::error::ForwardToLeader as OpenRaftForwardToLeader;
use zenoh_raft::storage::LogState as OpenRaftLogState;

use super::AppliedState;
use super::log_entry::LogEntry;
use crate::endpoint::Endpoint;
use crate::store::FjallStateMachine;

pub type SnapshotData = File;
pub type NodeId = u64;

#[derive(Debug, Clone, PartialEq, Eq, bitcode::Encode, bitcode::Decode)]
pub struct Node {
  pub node_id: NodeId,
  pub endpoint: Endpoint,
}

impl Display for Node {
  fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
    let node_id = self.node_id;
    let endpoint = &self.endpoint;
    write!(f, "{node_id}={endpoint}")
  }
}

#[derive(Debug, Clone, Default)]
pub struct KeyValue {
  pub key: Bytes,
  pub value: Bytes,
}

zenoh_raft::declare_raft_types!(
    pub TypeConfig:
        D = LogEntry,
        R = AppliedState,
        NodeId = NodeId,
        Node = Node
);

pub type Raft = OpenRaft<TypeConfig, FjallStateMachine>;
pub type Entry = EntryOf<TypeConfig>;
pub type LogState = OpenRaftLogState<TypeConfig>;
pub type LogId = LogIdOf<TypeConfig>;
pub type LeaderId = LeaderIdOf<TypeConfig>;
pub type Vote = VoteOf<TypeConfig>;

pub type ForwardToLeader = OpenRaftForwardToLeader<TypeConfig>;
pub type StoredMembership = StoredMembershipOf<TypeConfig>;
pub type Snapshot = SnapshotOf<TypeConfig, SnapshotData>;
pub type SnapshotMeta = SnapshotMetaOf<TypeConfig>;
pub use zenoh_raft::ReadPolicy;
