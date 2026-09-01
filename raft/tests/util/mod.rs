use wedb_raft::types::{LeaderId, LogId};

pub fn create_log_id(term: u64, node_id: u64, index: u64) -> LogId {
  LogId {
    leader_id: LeaderId { term, node_id },
    index,
  }
}
