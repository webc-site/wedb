//! 本地节点（`LOCAL_WORKER_ID`）查询与本地 worker 状态写面：身份/角色/端点/
//! epoch 读写、复制关系变更与 epoch 碰撞仲裁。
//!
//! 对位 garnet/libs/cluster/Server/ClusterConfig.cs 的 `#region GetLocalNodeInfo`
//! 与 InitializeLocalWorker / MakeReplicaOf / SetLocalWorkerRole /
//! TakeOverFromPrimary / SetLocalWorkerConfigEpoch / BumpLocalNodeConfigEpoch /
//! LazyUpdateLocalReplicationOffset / HandleConfigEpochCollision / TryReset。

use hipstr::HipStr;
use log::warn;
use wbase::hex::hex_str_u128;

use super::*;
use crate::server::{
  hash_slot::SlotState,
  worker::{LocalWorkerSpec, NodeRole},
};

impl ClusterConfig {
  /// libs/cluster/Server/ClusterConfig.cs:InitializeLocalWorker
  ///
  /// 原地更新本地 worker。C# 版每次复制重建 workers 数组；调用方均持有
  /// 写锁，此处直接改写，省去整份 slot_map（64KB）克隆。
  /// C# 散参入参聚合为 [`LocalWorkerSpec`]，免 too_many_arguments
  pub fn initialize_local_worker(&mut self, spec: LocalWorkerSpec) {
    let w = &mut self.workers[LOCAL_WORKER_ID];
    w.address = HipStr::from(spec.address);
    w.port = spec.port;
    w.nodeid = Some(spec.node_id);
    w.config_epoch = spec.config_epoch;
    w.role = spec.role;
    w.replica_of_node_id = spec.replica_of_node_id;
    w.replication_offset = 0;
    w.hostname = spec.hostname.map(HipStr::from);
  }

  /// libs/cluster/Server/ClusterConfig.cs:IsLocal
  #[inline]
  pub fn is_local(&self, slot: u16, read_write_session: bool) -> bool {
    let slot = slot as usize;
    if slot >= CLUSTER_SLOT_COUNT {
      return false;
    }
    self.slot_map[slot].eff_worker_id() as usize == LOCAL_WORKER_ID
      || self.is_local_expensive(slot, read_write_session)
  }

  /// libs/cluster/Server/ClusterConfig.cs:IsLocalExpensive
  fn is_local_expensive(&self, slot: usize, read_write_session: bool) -> bool {
    if slot >= CLUSTER_SLOT_COUNT {
      return false;
    }
    if self.slot_map[slot].state == SlotState::Migrating {
      return true;
    }
    if read_write_session
      && self
        .workers
        .get(LOCAL_WORKER_ID)
        .is_some_and(|w| w.role == NodeRole::Replica)
    {
      let owner_id = self.slot_map[slot].worker_id as usize;
      if owner_id > 1
        && let Some(my_primary) = self.workers[LOCAL_WORKER_ID].replica_of_node_id
        && let Some(w) = self.workers.get(owner_id)
      {
        return w.nodeid == Some(my_primary);
      }
    }
    false
  }

  /// libs/cluster/Server/ClusterConfig.cs:IsPrimary
  pub fn is_primary(&self) -> bool {
    self.local_node_role() == NodeRole::Primary
  }

  /// libs/cluster/Server/ClusterConfig.cs:IsReplica
  pub fn is_replica(&self) -> bool {
    self.local_node_role() == NodeRole::Replica
  }

  /// libs/cluster/Server/ClusterConfig.cs:LocalNodeIp
  pub fn local_node_ip(&self) -> &str {
    &self.workers[LOCAL_WORKER_ID].address
  }

  /// libs/cluster/Server/ClusterConfig.cs:LocalNodePort
  pub fn local_node_port(&self) -> i32 {
    self.workers[LOCAL_WORKER_ID].port
  }

  /// libs/cluster/Server/ClusterConfig.cs:LocalNodeEndpoint
  pub fn local_node_endpoint(&self) -> String {
    let w = &self.workers[LOCAL_WORKER_ID];
    format!("{}:{}", w.address, w.port)
  }

  /// libs/cluster/Server/ClusterConfig.cs:LocalNodeId
  pub fn local_node_id(&self) -> Option<u128> {
    self.workers[LOCAL_WORKER_ID].nodeid
  }

  /// LocalNodeIdShort 的渲染形态：日志与碰撞告警用短标识（hex 前 8 字符）。
  /// 仅诊断路径调用，String 分配可接受
  fn local_node_id_short(&self) -> String {
    const SHORT_LEN: usize = 8;
    match self.workers[LOCAL_WORKER_ID].nodeid {
      None => String::new(),
      // hex_str_u128 恒为 32 字符 ASCII，SHORT_LEN 切片必落字符界
      Some(id) => hex_str_u128(id)[..SHORT_LEN].to_string(),
    }
  }

  /// libs/cluster/Server/ClusterConfig.cs:LocalNodeRole
  pub fn local_node_role(&self) -> NodeRole {
    self.workers[LOCAL_WORKER_ID].role
  }

  /// libs/cluster/Server/ClusterConfig.cs:LocalNodePrimaryId
  pub fn local_node_primary_id(&self) -> Option<u128> {
    self.workers[LOCAL_WORKER_ID].replica_of_node_id
  }

  /// libs/cluster/Server/ClusterConfig.cs:LocalNodeConfigEpoch
  pub fn local_node_config_epoch(&self) -> i64 {
    self.workers[LOCAL_WORKER_ID].config_epoch
  }

  /// libs/cluster/Server/ClusterConfig.cs:GetLocalNodePrimaryAddress
  pub fn get_local_node_primary_address(&self) -> (Option<String>, i32) {
    if let Some(id) = self.workers[LOCAL_WORKER_ID].replica_of_node_id {
      self.get_worker_address_from_node_id(id)
    } else {
      (None, -1)
    }
  }

  /// libs/cluster/Server/ClusterConfig.cs:GetLocalNodeReplicaEndpoints
  pub fn get_local_node_replica_endpoints(&self) -> Vec<SocketAddr> {
    let Some(local_id) = self.local_node_id() else {
      return Vec::new();
    };
    self
      .workers
      .iter()
      .skip(2)
      .filter(|w| w.replica_of_node_id == Some(local_id))
      .filter_map(socket_of)
      .collect()
  }

  /// libs/cluster/Server/ClusterConfig.cs:GetLocalNodePrimaryEndpoints
  pub fn get_local_node_primary_endpoints(
    &self,
    include_my_primary_first: bool,
  ) -> Vec<SocketAddr> {
    let my_primary_id = if include_my_primary_first {
      self.local_node_primary_id()
    } else {
      None
    };
    let mut primaries = Vec::new();
    let mut first = None;
    for worker in self.workers.iter().skip(2) {
      let Some(node_id) = worker.nodeid else {
        continue;
      };
      // 地址只解析一次，供主端点与本主端点两分支共用
      let addr = socket_of(worker);
      let is_my_primary = Some(node_id) == my_primary_id;
      if worker.role == NodeRole::Primary
        && !is_my_primary
        && let Some(a) = addr
      {
        primaries.push(a);
      }
      if is_my_primary {
        first = addr;
      }
    }
    if let Some(f) = first {
      primaries.insert(0, f);
    }
    primaries
  }

  /// libs/cluster/Server/ClusterConfig.cs:LazyUpdateLocalReplicationOffset
  pub fn lazy_update_local_replication_offset(&mut self, offset: i64) {
    self.workers[LOCAL_WORKER_ID].replication_offset = offset;
  }

  /// libs/cluster/Server/ClusterConfig.cs:MakeReplicaOf
  pub fn make_replica_of(&mut self, nodeid: Option<u128>) -> &mut Self {
    let w = &mut self.workers[LOCAL_WORKER_ID];
    w.replica_of_node_id = nodeid;
    w.role = NodeRole::Replica;
    self
  }

  /// libs/cluster/Server/ClusterConfig.cs:SetLocalWorkerRole
  pub fn set_local_worker_role(&mut self, role: NodeRole) -> &mut Self {
    self.workers[LOCAL_WORKER_ID].role = role;
    self
  }

  /// libs/cluster/Server/ClusterConfig.cs:TakeOverFromPrimary
  ///
  /// 先按现主收集槽位再清 primary 指针，顺序不能反
  pub fn take_over_from_primary(&mut self) -> &mut Self {
    let slots = self.get_local_primary_slots();
    for slot in slots {
      self.slot_map[slot].worker_id = LOCAL_WORKER_ID as u16;
      self.slot_map[slot].state = SlotState::Stable;
    }
    let w = &mut self.workers[LOCAL_WORKER_ID];
    w.role = NodeRole::Primary;
    w.replica_of_node_id = None;
    self
  }

  /// libs/cluster/Server/ClusterConfig.cs:SetLocalWorkerConfigEpoch
  ///
  /// 语义对齐 C#：仅允许"从 0 初始化"且新值必须为正；后续单调递增只能走
  /// [`Self::bump_local_node_config_epoch`]，防止覆写既有 epoch。
  /// 返回是否实际生效
  pub fn set_local_worker_config_epoch(&mut self, config_epoch: i64) -> bool {
    let w = &mut self.workers[LOCAL_WORKER_ID];
    // 仅当本地 epoch 尚未初始化且新值为正时生效
    if w.config_epoch == 0 && config_epoch > 0 {
      w.config_epoch = config_epoch;
      true
    } else {
      false
    }
  }

  /// libs/cluster/Server/ClusterConfig.cs:BumpLocalNodeConfigEpoch
  pub fn bump_local_node_config_epoch(&mut self) -> &mut Self {
    let mx = self.get_max_config_epoch();
    self.workers[LOCAL_WORKER_ID].config_epoch = mx + 1;
    self
  }

  /// libs/cluster/Server/ClusterConfig.cs:HandleConfigEpochCollision
  ///
  /// 原地处理 epoch 碰撞，返回是否发生碰撞并自增（true 时需落盘）
  pub fn handle_config_epoch_collision(&mut self, sender_config: &ClusterConfig) -> bool {
    let local_node_config_epoch = self.local_node_config_epoch();
    let sender_config_epoch = sender_config.local_node_config_epoch();

    if local_node_config_epoch != sender_config_epoch {
      return false;
    }

    // 对齐 C# 字符串字典序仲裁语义：u128 数值序同为确定性全序（内部身份
    // 已收敛二进制，无字符串形态），双方各退一步避免死循环；
    // 缺席身份按 0 参与比较（对位 C# unwrap_or("")）
    let sender_node_id = sender_config.local_node_id().unwrap_or(0);
    let local_node_id = self.local_node_id().unwrap_or(0);
    if sender_node_id <= local_node_id {
      return false;
    }

    warn!(
      "Epoch Collision {} <> {} [{}:{},{}] [{}:{},{}]",
      local_node_config_epoch,
      sender_config_epoch,
      self.local_node_ip(),
      self.local_node_port(),
      self.local_node_id_short(),
      sender_config.local_node_ip(),
      sender_config.local_node_port(),
      sender_config.local_node_id_short()
    );

    self.bump_local_node_config_epoch();
    true
  }

  /// 原地重置集群配置（对标 C# TryReset）
  ///
  /// 复用已有的 64KB slot_map 堆内存与 workers 容量，避免重复分配；
  /// 本地 worker 的 address、port、hostname 原地保留，消除 String 堆分配与 HipStr 重建开销。
  pub fn reset(&mut self, new_node_id: u128, config_epoch: i64) {
    self.slot_map.fill(HashSlot::default());
    self.workers.truncate(2);
    self.initialize_unassigned_worker();
    let w = &mut self.workers[LOCAL_WORKER_ID];
    w.nodeid = Some(new_node_id);
    w.config_epoch = config_epoch;
    w.role = NodeRole::Primary;
    w.replica_of_node_id = None;
    w.replication_offset = 0;
  }
}
