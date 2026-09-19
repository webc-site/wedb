use std::sync::Arc;

use gxhash::HashMap;
use parking_lot::RwLock;

use crate::server::{
  cluster_provider::ClusterProvider, connection_info::ConnectionInfo,
  gossip::node_connection::NodeConnection,
};

/// MEET 前置的临时连接键：高位段标记 + 地址端口 gxhash。
/// 正式节点 id 为随机 u128，落入本标记段的概率 2^-64 可忽略；
/// 同 (address, port) 的并发 MEET 复用同一临时键（对位 C# "address:port"）
#[inline]
pub fn meet_temp_id(address: &str, port: i32) -> u128 {
  use whasher::fast_hash;
  // 布局：[tag 32 | addr hash 64 | port 16]，零分配；不同地址哈希区分，
  // 同地址不同端口由低位端口字段区分
  (u128::from(0xFFFF_FFFFu32) << 96)
    | (u128::from(fast_hash(address.as_bytes())) << 16)
    | u128::from(port as u16)
}

struct StoreInner {
  connections: Vec<Arc<NodeConnection>>,
  connection_map: HashMap<u128, usize>,
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

/// 持写锁状态下的移除（swap_remove 后回填被移动连接的 map 索引并 dispose）
fn remove_locked(inner: &mut StoreInner, node_id: u128) -> Option<Arc<NodeConnection>> {
  let offset = inner.connection_map.remove(&node_id)?;
  let last_idx = inner.connections.len().saturating_sub(1);
  let removed = inner.connections.swap_remove(offset);
  if offset < last_idx && offset < inner.connections.len() {
    let moved_node_id = inner.connections[offset].node_id;
    inner.connection_map.insert(moved_node_id, offset);
  }
  removed.dispose();
  Some(removed)
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

  /// 存在性判定（基于 GetConnection 内部映射表，广播游标兜底推进用），读锁 O(1)
  #[inline]
  pub fn contains(&self, node_id: u128) -> bool {
    let inner = self.inner.read();
    !inner.disposed && inner.connection_map.contains_key(&node_id)
  }

  /// libs/cluster/Server/Gossip/GarnetClusterConnectionStore.cs:GetConnection
  pub fn get_connection(&self, node_id: u128) -> Option<Arc<NodeConnection>> {
    let inner = self.inner.read();
    if inner.disposed {
      return None;
    }
    let offset = *inner.connection_map.get(&node_id)?;
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
  pub fn get_connection_info(&self, node_id: u128) -> Option<ConnectionInfo> {
    self
      .get_connection(node_id)
      .map(|c| c.get_connection_info())
  }

  /// libs/cluster/Server/Gossip/GarnetClusterConnectionStore.cs:GetOrAddAsync
  ///
  /// 认证凭证、Gossip-{local endpoint} 连接身份与在途超时统一由
  /// NodeConnection::new 从 cluster_provider 派生（对标 C# GetOrAddAsync
  /// 传 clusterProvider、GarnetServerNode 构造臂自取的形态）
  pub fn get_or_add(
    &self,
    node_id: u128,
    address: &str,
    port: i32,
    cluster_provider: &ClusterProvider,
  ) -> Arc<NodeConnection> {
    {
      let inner = self.inner.read();
      if !inner.disposed
        && let Some(&offset) = inner.connection_map.get(&node_id)
        && let Some(conn) = inner.connections.get(offset)
      {
        return Arc::clone(conn);
      }
    }
    let mut inner = self.inner.write();
    if !inner.disposed
      && let Some(&offset) = inner.connection_map.get(&node_id)
      && let Some(conn) = inner.connections.get(offset)
    {
      return Arc::clone(conn);
    }
    let conn = Arc::new(NodeConnection::new(
      node_id,
      address.to_string(),
      port,
      cluster_provider,
    ));
    if inner.disposed {
      return conn;
    }
    let offset = inner.connections.len();
    inner.connections.push(Arc::clone(&conn));
    inner.connection_map.insert(node_id, offset);
    conn
  }

  /// libs/cluster/Server/Gossip/GarnetClusterConnectionStore.cs:TryRemoveConnectionAsync
  pub fn try_remove(&self, node_id: u128) -> Option<Arc<NodeConnection>> {
    let mut inner = self.inner.write();
    if inner.disposed {
      return None;
    }
    remove_locked(&mut inner, node_id)
  }

  /// 仅当 node_id 当前仍指向 conn 时移除（gossip 派发任务回收失败连接用，
  /// 防止误删广播主循环已重建的新连接）
  pub fn try_remove_if_current(&self, node_id: u128, conn: &Arc<NodeConnection>) -> bool {
    let mut inner = self.inner.write();
    if inner.disposed {
      return false;
    }
    let is_current = inner
      .connection_map
      .get(&node_id)
      .and_then(|&offset| inner.connections.get(offset))
      .is_some_and(|cur| Arc::ptr_eq(cur, conn));
    if !is_current {
      return false;
    }
    remove_locked(&mut inner, node_id).is_some()
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

#[cfg(test)]
mod tests {
  use super::meet_temp_id;

  /// MEET 临时键：确定性生成，同 (address, port) 复用，端口参与区分
  #[test]
  fn meet_temp_id_is_stable_and_port_aware() {
    let a = meet_temp_id("127.0.0.1", 7000);
    assert_eq!(a, meet_temp_id("127.0.0.1", 7000), "同端点应得同一临时键");
    assert_ne!(a, meet_temp_id("127.0.0.1", 7001), "不同端口须区分");
    assert_ne!(a, meet_temp_id("10.0.0.1", 7000), "不同地址须区分");
    // 正式节点 id 为全空间随机 u128，与标记段相撞概率 2^-64：
    // 随手构造的测试 id 不得落入标记段
    assert_ne!(a, 0x1234_5678_9abc_def0_1234_5678_9abc_def0);
  }
}
