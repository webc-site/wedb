pub fn default_raft_heartbeat_interval() -> Option<u64> {
  Some(50)
}

pub fn default_raft_election_timeout_min() -> Option<u64> {
  Some(150)
}

pub fn default_raft_election_timeout_max() -> Option<u64> {
  Some(300)
}

pub fn default_redis_addr() -> String {
  "127.0.0.1:4909".to_string()
}
