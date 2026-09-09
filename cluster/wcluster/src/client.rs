use crate::server::failover::failover_option::FailoverOption;

pub struct GarnetClient {
  pub is_connected: bool,
}

impl GarnetClient {
  pub fn new() -> Self {
    Self {
      is_connected: false,
    }
  }

  pub async fn connect_async(&self) {}

  pub async fn reconnect_async(&self) {}

  pub async fn execute_cluster_fail_replication_offset_async(&self, _offset: u64) -> String {
    String::new()
  }

  pub async fn execute_cluster_fail_stop_writes_async(&self, _node_id: &[u8]) -> String {
    String::new()
  }

  pub async fn failover(&self, _option: FailoverOption) -> bool {
    true
  }

  pub async fn gossip_async(&self, _data: &[u8]) -> Vec<u8> {
    Vec::new()
  }

  pub async fn replica_of(&self, _ip: &str, _port: i32) -> String {
    "OK".to_string()
  }

  pub fn dispose(&self) {}
}

impl Default for GarnetClient {
  fn default() -> Self {
    Self::new()
  }
}

pub struct AofAddress;

impl AofAddress {
  pub fn from_string(_s: &str) -> Self {
    Self
  }

  pub fn equals_all(&self, _other: u64) -> bool {
    true
  }

  pub fn any_greater(&self, _other: u64) -> bool {
    false
  }
}
