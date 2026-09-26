use std::sync::Arc;

use parking_lot::RwLock;
use wbase::map::HashMap;

use crate::server::{
  cluster_provider::ClusterProvider, connection_info::ConnectionInfo,
  gossip::node_connection::NodeConnection,
};

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
  /// 获取已有连接或原子插入新连接，返回 `(连接引用, 是否新插入)`。
  pub fn get_or_add_entry(
    &self,
    node_id: u128,
    address: &str,
    port: i32,
    cluster_provider: &ClusterProvider,
  ) -> (Arc<NodeConnection>, bool) {
    {
      let inner = self.inner.read();
      if !inner.disposed
        && let Some(&offset) = inner.connection_map.get(&node_id)
        && let Some(conn) = inner.connections.get(offset)
      {
        return (Arc::clone(conn), false);
      }
    }
    let mut inner = self.inner.write();
    if !inner.disposed
      && let Some(&offset) = inner.connection_map.get(&node_id)
      && let Some(conn) = inner.connections.get(offset)
    {
      return (Arc::clone(conn), false);
    }
    let conn = Arc::new(NodeConnection::new(
      node_id,
      address.to_string(),
      port,
      cluster_provider,
    ));
    if inner.disposed {
      return (conn, false);
    }
    let offset = inner.connections.len();
    inner.connections.push(Arc::clone(&conn));
    inner.connection_map.insert(node_id, offset);
    (conn, true)
  }

  /// [`Self::get_or_add_entry`] 的去布尔适配臂（C# GetOrAddAsync 锚由
  /// get_or_add_entry 单点持有，本函数为 rust 侧便捷封装，不重复挂锚）
  ///
  /// 认证凭证、Gossip-{local endpoint} 连接身份与在途超时统一由
  /// NodeConnection::new 从 cluster_provider 派生（对标 C# GetOrAddAsync
  /// 传 clusterProvider、GarnetServerNode 构造臂自取的形态）
  #[inline]
  pub fn get_or_add(
    &self,
    node_id: u128,
    address: &str,
    port: i32,
    cluster_provider: &ClusterProvider,
  ) -> Arc<NodeConnection> {
    self
      .get_or_add_entry(node_id, address, port, cluster_provider)
      .0
  }

  /// libs/cluster/Server/Gossip/GarnetClusterConnectionStore.cs:AddConnectionAsync
  ///
  /// MEET 成功后的连接所有权交接：将调用方已完成握手的活跃连接实例直接
  /// 移交连接池统一管理（不经 get_or_add 重建，杜绝握手后断连销毁与冷桩
  /// 替换）。已释放或同 node_id 连接已存在（并发 MEET 竞争）返回 false，
  /// 由调用方负责 dispose 回收
  pub fn add_connection(&self, conn: Arc<NodeConnection>) -> bool {
    let node_id = conn.node_id;
    let mut inner = self.inner.write();
    if inner.disposed || inner.connection_map.contains_key(&node_id) {
      return false;
    }
    let offset = inner.connections.len();
    inner.connections.push(conn);
    inner.connection_map.insert(node_id, offset);
    true
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

  /// 事件重拉起池（C# 无对应：Dispose 即终局）
  ///
  /// 分叉 C# finally 拆池裁决语（刻意更严侧的配套收口，码注登记于此）：
  /// C# GossipMainAsync finally 一律 Dispose（Gossip.cs:369 →
  /// GarnetClusterConnectionStore.cs:60-72 置 _disposed 恒拒新增），且
  /// TryStartGossipTasks 唯一入口在 ClusterManager.Start——环死即全池终局，
  /// 复活只随集群整重启重建新 store。rust 采事件重拉（MEET / 入站 gossip
  /// 事件在下一安全点重拉主循环）并复用同一 GossipManager 实例，终局臂照抄
  /// C# 拆池后必须允许 start 的 CAS 门单点清位复原，否则重拉环永远面对拒收
  /// 死池。连接集在 dispose 时刻已拆空（disposed 态下无任何入池口，空集
  /// 不变式成立），此处仅重开闸门位，等价 C# 重启建池后的 fresh 形态
  pub fn revive(&self) {
    self.inner.write().disposed = false;
  }
}

impl Default for ConnectionStore {
  fn default() -> Self {
    Self::new()
  }
}
