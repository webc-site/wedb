use std::collections::{BTreeMap, BTreeSet};

use bitcode::{Decode, Encode};
use bytes::Bytes;
use zenoh_raft::alias::{EntryOf, LogIdOf, SnapshotMetaOf, StoredMembershipOf, VoteOf};
use zenoh_raft::raft::{AppendEntriesRequest, AppendEntriesResponse, VoteRequest, VoteResponse};
use zenoh_raft::vote::RaftLeaderId;
use zenoh_raft::{EntryPayload, Membership};

use super::log_entry::LogEntry;
use super::msg::snapshot::{ChunkSnapshotRequest, ChunkSnapshotResponse};
use super::raft_type::{LeaderId, Node, TypeConfig};

#[derive(Encode, Decode, Debug, Clone, PartialEq, Eq, Default)]
pub struct CompactLogId {
  pub term: u64,
  pub node_id: u64,
  pub index: u64,
}

impl From<&LogIdOf<TypeConfig>> for CompactLogId {
  #[inline]
  fn from(id: &LogIdOf<TypeConfig>) -> Self {
    Self {
      term: id.leader_id.term,
      node_id: id.leader_id.node_id,
      index: id.index,
    }
  }
}

impl From<LogIdOf<TypeConfig>> for CompactLogId {
  #[inline]
  fn from(id: LogIdOf<TypeConfig>) -> Self {
    CompactLogId::from(&id)
  }
}

impl From<CompactLogId> for LogIdOf<TypeConfig> {
  #[inline]
  fn from(c: CompactLogId) -> Self {
    LogIdOf::<TypeConfig>::new(LeaderId::new(c.term, c.node_id), c.index)
  }
}

#[derive(Encode, Decode, Debug, Clone, PartialEq, Eq, Default)]
pub struct CompactVote {
  pub term: u64,
  pub node_id: u64,
  pub committed: bool,
}

impl From<&VoteOf<TypeConfig>> for CompactVote {
  #[inline]
  fn from(v: &VoteOf<TypeConfig>) -> Self {
    Self {
      term: v.leader_id().term,
      node_id: v.leader_id().node_id,
      committed: v.is_committed(),
    }
  }
}

impl From<VoteOf<TypeConfig>> for CompactVote {
  #[inline]
  fn from(v: VoteOf<TypeConfig>) -> Self {
    CompactVote::from(&v)
  }
}

impl From<CompactVote> for VoteOf<TypeConfig> {
  #[inline]
  fn from(c: CompactVote) -> Self {
    if c.committed {
      VoteOf::<TypeConfig>::new_committed(c.term, c.node_id)
    } else {
      VoteOf::<TypeConfig>::new(c.term, c.node_id)
    }
  }
}

#[derive(Encode, Decode, Debug, Clone, PartialEq, Eq, Default)]
pub struct CompactMembership {
  pub log_id: Option<CompactLogId>,
  pub configs: Vec<Vec<u64>>,
  pub nodes: Vec<(u64, Node)>,
}

impl From<&Membership<u64, Node>> for CompactMembership {
  fn from(m: &Membership<u64, Node>) -> Self {
    let configs = m
      .get_joint_config()
      .iter()
      .map(|c| c.iter().copied().collect())
      .collect();
    let nodes = m.nodes().map(|(id, n)| (*id, n.clone())).collect();
    Self {
      log_id: None,
      configs,
      nodes,
    }
  }
}

impl From<&StoredMembershipOf<TypeConfig>> for CompactMembership {
  fn from(sm: &StoredMembershipOf<TypeConfig>) -> Self {
    let mut cm = Self::from(sm.membership());
    cm.log_id = sm.log_id().map(CompactLogId::from);
    cm
  }
}

impl From<CompactMembership> for StoredMembershipOf<TypeConfig> {
  fn from(c: CompactMembership) -> Self {
    let configs: Vec<BTreeSet<u64>> = c
      .configs
      .into_iter()
      .map(|s| s.into_iter().collect())
      .collect();
    let nodes: BTreeMap<u64, Node> = c.nodes.into_iter().collect();
    let membership = Membership::new(configs, nodes).unwrap_or_default();
    let log_id = c.log_id.map(Into::into);
    StoredMembershipOf::<TypeConfig>::new(log_id, membership)
  }
}

#[derive(Encode, Decode, Debug, Clone, PartialEq, Eq)]
pub enum CompactEntryPayload {
  Blank,
  Normal(LogEntry),
  Membership(CompactMembership),
}

#[derive(Encode, Decode, Debug, Clone, PartialEq, Eq)]
pub struct CompactEntry {
  pub log_id: CompactLogId,
  pub payload: CompactEntryPayload,
}

impl From<&EntryOf<TypeConfig>> for CompactEntry {
  fn from(entry: &EntryOf<TypeConfig>) -> Self {
    let log_id = CompactLogId::from(&entry.log_id);
    let payload = match &entry.payload {
      EntryPayload::Blank => CompactEntryPayload::Blank,
      EntryPayload::Normal(data) => CompactEntryPayload::Normal(data.clone()),
      EntryPayload::Membership(m) => CompactEntryPayload::Membership(CompactMembership::from(m)),
    };
    Self { log_id, payload }
  }
}

impl From<EntryOf<TypeConfig>> for CompactEntry {
  #[inline]
  fn from(entry: EntryOf<TypeConfig>) -> Self {
    let log_id = CompactLogId::from(&entry.log_id);
    let payload = match entry.payload {
      EntryPayload::Blank => CompactEntryPayload::Blank,
      EntryPayload::Normal(data) => CompactEntryPayload::Normal(data),
      EntryPayload::Membership(m) => CompactEntryPayload::Membership(CompactMembership::from(&m)),
    };
    Self { log_id, payload }
  }
}

impl From<CompactEntry> for EntryOf<TypeConfig> {
  fn from(c: CompactEntry) -> Self {
    let log_id: LogIdOf<TypeConfig> = c.log_id.into();
    let payload = match c.payload {
      CompactEntryPayload::Blank => EntryPayload::Blank,
      CompactEntryPayload::Normal(data) => EntryPayload::Normal(data),
      CompactEntryPayload::Membership(cm) => {
        let configs: Vec<BTreeSet<u64>> = cm
          .configs
          .into_iter()
          .map(|s| s.into_iter().collect())
          .collect();
        let nodes: BTreeMap<u64, Node> = cm.nodes.into_iter().collect();
        let m = Membership::new(configs, nodes).unwrap_or_default();
        EntryPayload::Membership(m)
      }
    };
    EntryOf::<TypeConfig> { log_id, payload }
  }
}

#[derive(Encode, Decode, Debug, Clone, PartialEq, Eq)]
pub struct CompactSnapshotMeta {
  pub last_log_id: Option<CompactLogId>,
  pub last_membership: CompactMembership,
}

impl From<&SnapshotMetaOf<TypeConfig>> for CompactSnapshotMeta {
  fn from(meta: &SnapshotMetaOf<TypeConfig>) -> Self {
    Self {
      last_log_id: meta.last_log_id.as_ref().map(CompactLogId::from),
      last_membership: CompactMembership::from(&meta.last_membership),
    }
  }
}

impl From<CompactSnapshotMeta> for SnapshotMetaOf<TypeConfig> {
  fn from(c: CompactSnapshotMeta) -> Self {
    Self {
      last_log_id: c.last_log_id.map(Into::into),
      last_membership: c.last_membership.into(),
    }
  }
}

#[derive(Encode, Decode, Debug, Clone)]
pub struct CompactAppendEntriesRequest {
  pub vote: CompactVote,
  pub prev_log_id: Option<CompactLogId>,
  pub entries: Vec<CompactEntry>,
  pub leader_commit: Option<CompactLogId>,
}

impl From<&AppendEntriesRequest<TypeConfig>> for CompactAppendEntriesRequest {
  #[inline]
  fn from(req: &AppendEntriesRequest<TypeConfig>) -> Self {
    Self {
      vote: CompactVote::from(&req.vote),
      prev_log_id: req.prev_log_id.as_ref().map(CompactLogId::from),
      entries: req.entries.iter().map(CompactEntry::from).collect(),
      leader_commit: req.leader_commit.as_ref().map(CompactLogId::from),
    }
  }
}

impl From<AppendEntriesRequest<TypeConfig>> for CompactAppendEntriesRequest {
  #[inline]
  fn from(req: AppendEntriesRequest<TypeConfig>) -> Self {
    Self {
      vote: CompactVote::from(&req.vote),
      prev_log_id: req.prev_log_id.as_ref().map(CompactLogId::from),
      entries: req.entries.into_iter().map(CompactEntry::from).collect(),
      leader_commit: req.leader_commit.as_ref().map(CompactLogId::from),
    }
  }
}

impl From<CompactAppendEntriesRequest> for AppendEntriesRequest<TypeConfig> {
  #[inline]
  fn from(c: CompactAppendEntriesRequest) -> Self {
    Self {
      vote: c.vote.into(),
      prev_log_id: c.prev_log_id.map(Into::into),
      entries: c.entries.into_iter().map(Into::into).collect(),
      leader_commit: c.leader_commit.map(Into::into),
    }
  }
}

#[derive(Encode, Decode, Debug, Clone)]
pub enum CompactAppendEntriesResponse {
  Success,
  PartialSuccess(Option<CompactLogId>),
  HigherVote(CompactVote),
  Conflict,
}

impl From<&AppendEntriesResponse<TypeConfig>> for CompactAppendEntriesResponse {
  #[inline]
  fn from(resp: &AppendEntriesResponse<TypeConfig>) -> Self {
    match resp {
      AppendEntriesResponse::Success => Self::Success,
      AppendEntriesResponse::PartialSuccess(log_id) => {
        Self::PartialSuccess(log_id.as_ref().map(CompactLogId::from))
      }
      AppendEntriesResponse::HigherVote(v) => Self::HigherVote(CompactVote::from(v)),
      AppendEntriesResponse::Conflict => Self::Conflict,
    }
  }
}

impl From<CompactAppendEntriesResponse> for AppendEntriesResponse<TypeConfig> {
  #[inline]
  fn from(c: CompactAppendEntriesResponse) -> Self {
    match c {
      CompactAppendEntriesResponse::Success => Self::Success,
      CompactAppendEntriesResponse::PartialSuccess(log_id) => {
        Self::PartialSuccess(log_id.map(Into::into))
      }
      CompactAppendEntriesResponse::HigherVote(v) => Self::HigherVote(v.into()),
      CompactAppendEntriesResponse::Conflict => Self::Conflict,
    }
  }
}

#[derive(Encode, Decode, Debug, Clone)]
pub struct CompactVoteRequest {
  pub vote: CompactVote,
  pub last_log_id: Option<CompactLogId>,
  pub leadership_transfer: bool,
  pub is_pre_vote: bool,
}

impl From<&VoteRequest<TypeConfig>> for CompactVoteRequest {
  #[inline]
  fn from(req: &VoteRequest<TypeConfig>) -> Self {
    Self {
      vote: CompactVote::from(&req.vote),
      last_log_id: req.last_log_id.as_ref().map(CompactLogId::from),
      leadership_transfer: req.leadership_transfer,
      is_pre_vote: req.is_pre_vote,
    }
  }
}

impl From<VoteRequest<TypeConfig>> for CompactVoteRequest {
  #[inline]
  fn from(req: VoteRequest<TypeConfig>) -> Self {
    Self::from(&req)
  }
}

impl From<CompactVoteRequest> for VoteRequest<TypeConfig> {
  #[inline]
  fn from(c: CompactVoteRequest) -> Self {
    Self {
      vote: c.vote.into(),
      last_log_id: c.last_log_id.map(Into::into),
      leadership_transfer: c.leadership_transfer,
      is_pre_vote: c.is_pre_vote,
    }
  }
}

#[derive(Encode, Decode, Debug, Clone)]
pub struct CompactVoteResponse {
  pub vote: CompactVote,
  pub vote_granted: bool,
  pub last_log_id: Option<CompactLogId>,
}

impl From<&VoteResponse<TypeConfig>> for CompactVoteResponse {
  #[inline]
  fn from(resp: &VoteResponse<TypeConfig>) -> Self {
    Self {
      vote: CompactVote::from(&resp.vote),
      vote_granted: resp.vote_granted,
      last_log_id: resp.last_log_id.as_ref().map(CompactLogId::from),
    }
  }
}

impl From<CompactVoteResponse> for VoteResponse<TypeConfig> {
  #[inline]
  fn from(c: CompactVoteResponse) -> Self {
    Self {
      vote: c.vote.into(),
      vote_granted: c.vote_granted,
      last_log_id: c.last_log_id.map(Into::into),
    }
  }
}

#[derive(Encode, Decode, Debug, Clone)]
pub struct CompactChunkSnapshotRequest {
  pub vote: CompactVote,
  pub snapshot_id: String,
  pub meta: CompactSnapshotMeta,
  pub offset: u64,
  pub data: Vec<u8>,
  pub done: bool,
}

impl From<&ChunkSnapshotRequest> for CompactChunkSnapshotRequest {
  fn from(req: &ChunkSnapshotRequest) -> Self {
    Self {
      vote: CompactVote::from(&req.vote),
      snapshot_id: req.snapshot_id.clone(),
      meta: CompactSnapshotMeta::from(&req.meta),
      offset: req.offset,
      data: req.data.to_vec(),
      done: req.done,
    }
  }
}

impl From<ChunkSnapshotRequest> for CompactChunkSnapshotRequest {
  fn from(req: ChunkSnapshotRequest) -> Self {
    Self {
      vote: CompactVote::from(&req.vote),
      snapshot_id: req.snapshot_id,
      meta: CompactSnapshotMeta::from(&req.meta),
      offset: req.offset,
      // Bytes 单引用时零拷贝转 Vec<u8>
      data: req.data.into(),
      done: req.done,
    }
  }
}

impl From<CompactChunkSnapshotRequest> for ChunkSnapshotRequest {
  fn from(c: CompactChunkSnapshotRequest) -> Self {
    Self {
      vote: c.vote.into(),
      snapshot_id: c.snapshot_id,
      meta: c.meta.into(),
      offset: c.offset,
      data: Bytes::from(c.data),
      done: c.done,
    }
  }
}

#[derive(Encode, Decode, Debug, Clone)]
pub struct CompactChunkSnapshotResponse {
  pub vote: CompactVote,
}

impl From<&ChunkSnapshotResponse> for CompactChunkSnapshotResponse {
  #[inline]
  fn from(resp: &ChunkSnapshotResponse) -> Self {
    Self {
      vote: CompactVote::from(&resp.vote),
    }
  }
}

impl From<CompactChunkSnapshotResponse> for ChunkSnapshotResponse {
  #[inline]
  fn from(c: CompactChunkSnapshotResponse) -> Self {
    Self {
      vote: c.vote.into(),
    }
  }
}

#[derive(Encode, Decode, Debug, Clone)]
pub struct CompactStreamAppendReq {
  pub seq_id: u64,
  pub leader_id: u64,
  pub req: CompactAppendEntriesRequest,
}

#[derive(Encode, Decode, Debug, Clone)]
pub struct CompactStreamAppendAck {
  pub seq_id: u64,
  pub follower_id: u64,
  pub resp: CompactAppendEntriesResponse,
}
