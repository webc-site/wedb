//! 集群提供者抽象面（对标 libs/server/Cluster/IClusterProvider.cs）
//!
//! C# 为接口 + 分布式实现（ClusterProvider）；本周期仅对标抽象面与
//! 单机 provider 语义：单机形态下角色恒为主、gossip/复制信息为空集、
//! 发布与截断为直通 no-op、RunID 保持进程内稳定。
//! 分布式实现（cluster_factory 背后的真实集群域）不在本域文件范围。

use std::{
  collections::BTreeSet,
  sync::{
    Arc,
    atomic::{AtomicBool, AtomicU64, Ordering},
  },
};

use parking_lot::Mutex;

use crate::{
  aof::aof_address::AofAddress, cluster::i_cluster_session::IClusterSession,
  metrics::metrics_item::MetricsItem, types::RespCommand,
};

/// 缓冲池所属管理器（对齐 C# ManagerType：主存储 / 对象存储 / AOF）。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ManagerType {
  /// 主存储。
  Main,
  /// 对象存储。
  Object,
  /// AOF。
  Aof,
}

/// 角色信息（RoleInfo 的域内承接：主/从视角的角色与复制偏移）。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RoleInfo {
  /// 角色名："primary" / "replica"。
  pub role: String,
  /// 复制偏移（主侧为本地写偏移，从侧为已同步偏移）。
  pub replication_offset: i64,
  /// 从属的节点 id（主侧视角的副本列表项；单机为空）。
  pub node_id: Option<String>,
}

impl RoleInfo {
  /// 主角色信息。
  pub fn primary(replication_offset: i64) -> Self {
    Self {
      role: "primary".to_string(),
      replication_offset,
      node_id: None,
    }
  }

  /// 从角色信息。
  pub fn replica(replication_offset: i64, node_id: Option<String>) -> Self {
    Self {
      role: "replica".to_string(),
      replication_offset,
      node_id,
    }
  }
}

/// 集群提供者抽象面（单机 provider 语义即可对标）。
pub trait IClusterProvider: Send + Sync {
  /// libs/server/Cluster/IClusterProvider.cs:CreateClusterSession
  ///
  /// 创建集群会话（会话参数经会话域承接，抽象面不感知）。
  fn create_cluster_session(&self) -> IClusterSession;

  /// 是否允许承受 AOF 数据丢失（null AOF 设备或纯内存复制且无按需检查点）。
  fn allow_data_loss(&self) -> bool;

  /// 刷新配置（持久化运行期 CONFIG 变更）。
  fn flush_config(&self);

  /// libs/server/Cluster/IClusterProvider.cs:GetGossipStats
  fn get_gossip_stats(&self, metrics_disabled: bool) -> Vec<MetricsItem>;

  /// libs/server/Cluster/IClusterProvider.cs:GetReplicationInfo
  fn get_replication_info(&self) -> Vec<MetricsItem>;

  /// 缓冲池统计。
  fn get_buffer_pool_stats(&self) -> Vec<MetricsItem>;

  /// libs/server/Cluster/IClusterProvider.cs:GetPrimaryInfo
  ///
  /// 从副本视角取主侧信息：(复制偏移, 副本角色列表)。
  fn get_primary_info(&self) -> (AofAddress, Vec<RoleInfo>);

  /// libs/server/Cluster/IClusterProvider.cs:GetReplicaInfo
  ///
  /// 从主侧视角取本节点副本信息。
  fn get_replica_info(&self) -> RoleInfo;

  /// libs/server/Cluster/IClusterProvider.cs:PurgeBufferPool
  fn purge_buffer_pool(&self, manager_type: ManagerType);

  /// libs/server/Cluster/IClusterProvider.cs:ClusterPublishAsync
  ///
  /// 向远端节点投递集群发布消息。
  fn cluster_publish_async(
    &self,
    cmd: RespCommand,
    channel: &[u8],
    message: &[u8],
  ) -> impl Future<Output = ()> + Send;

  /// 是否为主。
  fn is_primary(&self) -> bool;

  /// 是否为副本。
  fn is_replica(&self) -> bool;

  /// 按当前集群配置判断给定节点是否为副本。
  fn is_replica_node(&self, node_id: &str) -> bool;

  /// libs/server/Cluster/IClusterProvider.cs:OnCheckpointInitiated
  fn on_checkpoint_initiated(&self, checkpoint_covered_aof_address: &mut AofAddress);

  /// 恢复集群。
  fn recover(&self);

  /// libs/server/Cluster/IClusterProvider.cs:ResetGossipStats
  fn reset_gossip_stats(&self);

  /// libs/server/Cluster/IClusterProvider.cs:AddNewCheckpointEntry
  ///
  /// 登记新检查点条目（full = 全量检查点）。
  fn add_new_checkpoint_entry(
    &self,
    full: bool,
    checkpoint_covered_aof_address: AofAddress,
    store_checkpoint_token: u128,
    object_store_checkpoint_token: u128,
  );

  /// libs/server/Cluster/IClusterProvider.cs:SafeTruncateAOF
  ///
  /// 安全截断 AOF 至指定地址。
  fn safe_truncate_aof(&self, truncate_until: &AofAddress);

  /// 启动集群操作。
  fn start(&self);

  /// libs/server/Cluster/IClusterProvider.cs:UpdateClusterAuth
  ///
  /// 原子更新集群鉴权（None = 不变更）。
  fn update_cluster_auth(&self, cluster_username: Option<&str>, cluster_password: Option<&str>);

  /// libs/server/Cluster/IClusterProvider.cs:GetCheckpointInfo
  fn get_checkpoint_info(&self) -> Vec<MetricsItem>;

  /// libs/server/Cluster/IClusterProvider.cs:GetRunId
  ///
  /// 标识检查点历史的 RunID。
  fn get_run_id(&self) -> String;

  /// libs/server/Cluster/IClusterProvider.cs:PreventRoleChange
  ///
  /// 阻止本节点变更当前角色；返回 true 后必须配对调用 [`Self::allow_role_change`]。
  fn prevent_role_change(&self) -> bool;

  /// libs/server/Cluster/IClusterProvider.cs:AllowRoleChange
  ///
  /// 解除角色变更阻止。
  fn allow_role_change(&self);
}

/// 单机集群提供者：无 gossip、恒为主、RunID 稳定、
/// 发布/截断直通、检查点条目记录于内存。
#[derive(Debug)]
pub struct SingleNodeClusterProvider {
  /// RunID（进程内稳定）。
  run_id: String,
  /// 角色变更阻止标志（prevent/allow 配对）。
  role_change_blocked: AtomicBool,
  /// 本地写偏移（主视角复制偏移的域内承接）。
  local_offset: AtomicU64,
  /// 已登记检查点条目数。
  checkpoint_entries: AtomicU64,
  /// 最近一次检查点覆盖的 AOF 地址。
  last_checkpoint_aof: Mutex<Option<AofAddress>>,
  /// 集群鉴权（用户名/密码的承接，None = 未设置）。
  cluster_auth: Mutex<Option<(String, String)>>,
  /// 已见副本节点集合（单机为空集）。
  replica_nodes: Mutex<BTreeSet<String>>,
}

impl SingleNodeClusterProvider {
  /// 以固定 RunID 创建单机提供者。
  pub fn new(run_id: String) -> Self {
    Self {
      run_id,
      role_change_blocked: AtomicBool::new(false),
      local_offset: AtomicU64::new(0),
      checkpoint_entries: AtomicU64::new(0),
      last_checkpoint_aof: Mutex::new(None),
      cluster_auth: Mutex::new(None),
      replica_nodes: Mutex::new(BTreeSet::new()),
    }
  }

  /// 推进本地写偏移（AOF 写入路径的域内承接）。
  pub fn advance_local_offset(&self, delta: u64) {
    self.local_offset.fetch_add(delta, Ordering::AcqRel);
  }
}

impl Default for SingleNodeClusterProvider {
  fn default() -> Self {
    Self::new("single-node-run-id".to_string())
  }
}

impl IClusterProvider for SingleNodeClusterProvider {
  fn create_cluster_session(&self) -> IClusterSession {
    // 单机语义：会话域为单机承接态（恒为主 / 全槽自有），直接构造默认实例
    IClusterSession::new()
  }

  fn allow_data_loss(&self) -> bool {
    // 单机无复制：默认不允许
    false
  }

  fn flush_config(&self) {
    // 单机无持久化配置差异
  }

  fn get_gossip_stats(&self, metrics_disabled: bool) -> Vec<MetricsItem> {
    if metrics_disabled {
      return Vec::new();
    }
    vec![MetricsItem::new(
      "NODE_INFO",
      format!("myid={},{}", "self", self.run_id),
    )]
  }

  fn get_replication_info(&self) -> Vec<MetricsItem> {
    vec![
      MetricsItem::new("role", "primary"),
      MetricsItem::new(
        "master_repl_offset",
        self.local_offset.load(Ordering::Acquire).to_string(),
      ),
    ]
  }

  fn get_buffer_pool_stats(&self) -> Vec<MetricsItem> {
    Vec::new()
  }

  fn get_primary_info(&self) -> (AofAddress, Vec<RoleInfo>) {
    let offset = AofAddress::default();
    (offset, Vec::new())
  }

  fn get_replica_info(&self) -> RoleInfo {
    RoleInfo::primary(self.local_offset.load(Ordering::Acquire) as i64)
  }

  fn purge_buffer_pool(&self, manager_type: ManagerType) {
    // 单机缓冲池随会话回收，无需处理
    let _ = manager_type;
  }

  async fn cluster_publish_async(&self, _cmd: RespCommand, _channel: &[u8], _message: &[u8]) {
    // 单机：无远端节点，直通
  }

  fn is_primary(&self) -> bool {
    true
  }

  fn is_replica(&self) -> bool {
    false
  }

  fn is_replica_node(&self, node_id: &str) -> bool {
    self.replica_nodes.lock().contains(node_id)
  }

  fn on_checkpoint_initiated(&self, checkpoint_covered_aof_address: &mut AofAddress) {
    *self.last_checkpoint_aof.lock() = Some(*checkpoint_covered_aof_address);
    let _ = checkpoint_covered_aof_address;
  }

  fn recover(&self) {
    // 单机：恢复为无操作
  }

  fn reset_gossip_stats(&self) {
    // 单机：无 gossip 统计
  }

  fn add_new_checkpoint_entry(
    &self,
    _full: bool,
    _checkpoint_covered_aof_address: AofAddress,
    _store_checkpoint_token: u128,
    _object_store_checkpoint_token: u128,
  ) {
    self.checkpoint_entries.fetch_add(1, Ordering::AcqRel);
  }

  fn safe_truncate_aof(&self, _truncate_until: &AofAddress) {
    // 单机：AOF 截断由 waof 承接，提供者面为直通
  }

  fn start(&self) {
    // 单机：无集群后台任务
  }

  fn update_cluster_auth(&self, cluster_username: Option<&str>, cluster_password: Option<&str>) {
    let mut auth = self.cluster_auth.lock();
    *auth = match (cluster_username, cluster_password) {
      (Some(u), Some(p)) => Some((u.to_string(), p.to_string())),
      _ => None,
    };
  }

  fn get_checkpoint_info(&self) -> Vec<MetricsItem> {
    vec![
      MetricsItem::new(
        "CheckpointEntries",
        self.checkpoint_entries.load(Ordering::Acquire).to_string(),
      ),
      MetricsItem::new("RunID", self.run_id.clone()),
    ]
  }

  fn get_run_id(&self) -> String {
    self.run_id.clone()
  }

  fn prevent_role_change(&self) -> bool {
    // CAS 语义：未被阻止时置位并成功
    self
      .role_change_blocked
      .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
      .is_ok()
  }

  fn allow_role_change(&self) {
    self.role_change_blocked.store(false, Ordering::Release);
  }
}

/// 共享句柄别名。
pub type SharedClusterProvider = Arc<dyn IClusterProvider>;

#[cfg(test)]
mod tests {
  use super::*;

  #[test]
  fn single_node_role_semantics() {
    let provider = SingleNodeClusterProvider::default();
    assert!(provider.is_primary());
    assert!(!provider.is_replica());
    assert!(!provider.is_replica_node("anyone"));

    let replica_info = provider.get_replica_info();
    assert_eq!(replica_info.role, "primary");

    let (offset, replicas) = provider.get_primary_info();
    assert_eq!(replicas, Vec::new());
    assert_eq!(offset, AofAddress::default());

    provider.advance_local_offset(128);
    assert_eq!(provider.get_replica_info().replication_offset, 128);
  }

  #[test]
  fn role_change_gate_is_pairwise() {
    let provider = SingleNodeClusterProvider::default();
    assert!(provider.prevent_role_change());
    // 已阻止时再阻止失败（CAS）
    assert!(!provider.prevent_role_change());
    provider.allow_role_change();
    // 解除后可再次阻止
    assert!(provider.prevent_role_change());
    provider.allow_role_change();
  }

  #[test]
  fn checkpoint_bookkeeping() {
    let provider = SingleNodeClusterProvider::default();
    let info = provider.get_checkpoint_info();
    assert_eq!(info[0].name, "CheckpointEntries");
    assert_eq!(info[0].value, "0");
    assert_eq!(info[1].value, "single-node-run-id");

    let mut covered = AofAddress::default();
    provider.on_checkpoint_initiated(&mut covered);
    provider.add_new_checkpoint_entry(true, covered, 1, 2);
    provider.add_new_checkpoint_entry(false, covered, 3, 4);

    let info = provider.get_checkpoint_info();
    assert_eq!(info[0].value, "2");
  }

  #[test]
  fn cluster_auth_atomic_update() {
    let provider = SingleNodeClusterProvider::default();
    // 原子换入/换出（域内承接，仅保证接口形态）
    provider.update_cluster_auth(Some("u"), Some("p"));
    provider.update_cluster_auth(None, None);
    provider.update_cluster_auth(Some("u2"), Some("p2"));
  }

  #[test]
  fn gossip_and_buffers() {
    let provider = SingleNodeClusterProvider::default();
    // 禁用指标 → 空
    assert!(provider.get_gossip_stats(true).is_empty());
    // 启用 → 节点信息行
    assert_eq!(provider.get_gossip_stats(false).len(), 1);
    // 缓冲池统计（单机为空）
    assert!(provider.get_buffer_pool_stats().is_empty());
    provider.purge_buffer_pool(ManagerType::Main);
    provider.purge_buffer_pool(ManagerType::Object);
    provider.purge_buffer_pool(ManagerType::Aof);

    // 复制信息
    let info = provider.get_replication_info();
    assert_eq!(info[0].value, "primary");

    // 发布与截断直通
    provider.flush_config();
    provider.start();
    provider.recover();
    provider.reset_gossip_stats();
    provider.safe_truncate_aof(&AofAddress::default());

    // RunID 稳定
    assert_eq!(provider.get_run_id(), "single-node-run-id");
    assert!(!provider.allow_data_loss());
  }
}
