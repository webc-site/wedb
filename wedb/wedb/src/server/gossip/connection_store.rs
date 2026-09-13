use std::sync::Arc;

use gxhash::HashMap;
use parking_lot::RwLock;

use crate::server::{connection_info::ConnectionInfo, gossip::node_connection::NodeConnection};

struct StoreInner {
  connections: Vec<Arc<NodeConnection>>,
  connection_map: HashMap<String, usize>,
  disposed: bool,
}

impl StoreInner {
  fn new() -> Self {
    Self {
      connections: Vec::new(),
      connection_map: HashMap::default(),
      disposed: false,
    }
  }

  fn clear(&mut self) {
    for conn in &self.connections {
      conn.dispose();
    }
    self.connections.clear();
    self.connection_map.clear();
  }
}

/// libs/cluster/Server/Gossip/GarnetClusterConnectionStore.cs:GarnetClusterConnectionStore
pub struct ConnectionStore {
  inner: RwLock<StoreInner>,
}

impl ConnectionStore {
  /// libs/cluster/Server/Gossip/GarnetClusterConnectionStore.cs:GarnetClusterConnectionStore
  pub fn new() -> Self {
    Self {
      inner: RwLock::new(StoreInner::new()),
    }
  }

  /// libs/cluster/Server/Gossip/GarnetClusterConnectionStore.cs:Count
  #[inline]
  pub fn count(&self) -> usize {
    self.inner.read().connections.len()
  }

  /// libs/cluster/Server/Gossip/GarnetClusterConnectionStore.cs:GetConnection
  pub fn get_connection(&self, node_id: &str) -> Option<Arc<NodeConnection>> {
    let inner = self.inner.read();
    if inner.disposed {
      return None;
    }
    let offset = *inner.connection_map.get(node_id)?;
    inner.connections.get(offset).cloned()
  }

  /// libs/cluster/Server/Gossip/GarnetClusterConnectionStore.cs:GetConnectionAtOffset
  #[inline]
  pub fn get_connection_at_offset(&self, offset: usize) -> Option<Arc<NodeConnection>> {
    let inner = self.inner.read();
    if inner.disposed {
      return None;
    }
    inner.connections.get(offset).cloned()
  }

  /// libs/cluster/Server/Gossip/GarnetClusterConnectionStore.cs:GetRandomConnection
  #[inline]
  pub fn get_random_connection(&self) -> Option<Arc<NodeConnection>> {
    let inner = self.inner.read();
    if inner.disposed || inner.connections.is_empty() {
      return None;
    }
    let idx = fastrand::usize(..inner.connections.len());
    inner.connections.get(idx).cloned()
  }

  /// libs/cluster/Server/Gossip/GarnetClusterConnectionStore.cs:GetConnectionInfo
  pub fn get_connection_info(&self, node_id: &str) -> Option<ConnectionInfo> {
    self
      .get_connection(node_id)
      .map(|c| c.get_connection_info())
  }

  /// libs/cluster/Server/Gossip/GarnetClusterConnectionStore.cs:GetOrAddAsync
  pub fn get_or_add(&self, node_id: &str, address: &str, port: i32) -> Arc<NodeConnection> {
    self.get_or_add_with_auth(node_id, address, port, None, None)
  }

  /// 获取或创建指定节点的连接（带集群认证凭证）
  pub fn get_or_add_with_auth(
    &self,
    node_id: &str,
    address: &str,
    port: i32,
    auth_username: Option<&str>,
    auth_password: Option<&str>,
  ) -> Arc<NodeConnection> {
    {
      let inner = self.inner.read();
      if !inner.disposed
        && let Some(&offset) = inner.connection_map.get(node_id)
        && let Some(conn) = inner.connections.get(offset)
      {
        return Arc::clone(conn);
      }
    }
    let mut inner = self.inner.write();
    if !inner.disposed
      && let Some(&offset) = inner.connection_map.get(node_id)
      && let Some(conn) = inner.connections.get(offset)
    {
      return Arc::clone(conn);
    }
    let conn = Arc::new(NodeConnection::new(
      node_id.to_string(),
      address.to_string(),
      port,
      auth_username.map(String::from),
      auth_password.map(String::from),
    ));
    if inner.disposed {
      return conn;
    }
    let offset = inner.connections.len();
    inner.connections.push(Arc::clone(&conn));
    inner.connection_map.insert(node_id.to_string(), offset);
    conn
  }

  /// libs/cluster/Server/Gossip/GarnetClusterConnectionStore.cs:TryRemoveConnectionAsync
  pub fn try_remove(&self, node_id: &str) -> Option<Arc<NodeConnection>> {
    let mut inner = self.inner.write();
    if inner.disposed {
      return None;
    }
    let offset = inner.connection_map.remove(node_id)?;
    let last_idx = inner.connections.len().saturating_sub(1);
    let removed = inner.connections.swap_remove(offset);
    if offset < last_idx && offset < inner.connections.len() {
      let moved_node_id = inner.connections[offset].node_id.clone();
      inner.connection_map.insert(moved_node_id, offset);
    }
    removed.dispose();
    Some(removed)
  }

  /// libs/cluster/Server/Gossip/GarnetClusterConnectionStore.cs:CloseAll
  pub fn close_all(&self) {
    let mut inner = self.inner.write();
    if inner.disposed {
      return;
    }
    inner.clear();
  }

  /// libs/cluster/Server/Gossip/GarnetClusterConnectionStore.cs:Dispose
  pub fn dispose(&self) {
    let mut inner = self.inner.write();
    inner.clear();
    inner.disposed = true;
  }
}

impl Default for ConnectionStore {
  fn default() -> Self {
    Self::new()
  }
}
