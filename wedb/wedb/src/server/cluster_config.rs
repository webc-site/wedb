use std::{array::from_fn, cmp::Ordering, fmt::Write as _, net::SocketAddr};

use bitcode::{Decode, Encode};
use gxhash::{HashMap, HashSet};
use log::warn;

use crate::{
  error::{Error, Result},
  server::{
    cluster_provider::ClusterProvider,
    connection_info::ConnectionInfo,
    hash_slot::{HashSlot, SLOT_STATE_KINDS, SlotState},
    worker::{LocalWorkerSpec, NodeRole, Worker},
  },
};

/// 集群配置线格式版本：v2 起由 .NET BinaryWriter 布局换为 bitcode 编码，
/// 无向下兼容负担，异版本载荷在解码前即被拒绝
pub const CLUSTER_CONFIG_VERSION: u8 = 2;

/// 槽位空间上界（Redis Cluster 语义：16384 槽；下界 0 对 usize 恒真无需常量）
pub const MAX_HASH_SLOT_VALUE: usize = 16384;

/// CLUSTER NODES 中 bus 端口偏移（garnet 语义：bus port = port + 10000）
const BUS_PORT_OFFSET: i32 = 10000;

// worker id 常量定义域在 [`crate::server::worker`]，此处转出口维持
// 槽位/配置方法群的单一引用路径
pub use crate::server::{
  cluster::ClusterPreferredEndpointType,
  worker::{LOCAL_WORKER_ID, RESERVED_WORKER_ID},
};

/// libs/cluster/Server/ClusterConfig.cs
#[derive(Debug, Clone)]
pub struct ClusterConfig {
  pub slot_map: Box<[HashSlot; MAX_HASH_SLOT_VALUE]>,
  pub workers: Vec<Worker>,
}

impl Default for ClusterConfig {
  fn default() -> Self {
    Self::new()
  }
}

impl ClusterConfig {
  /// libs/cluster/Server/ClusterConfig.cs:OutOfRange
  pub fn out_of_range(slot: usize) -> bool {
    slot >= MAX_HASH_SLOT_VALUE
  }

  /// libs/cluster/Server/ClusterConfig.cs:NumWorkers
  pub fn num_workers(&self) -> usize {
    self.workers.len().saturating_sub(1)
  }

  /// libs/cluster/Server/ClusterConfig.cs:ClusterConfig
  pub fn new() -> Self {
    // 数组索引即为槽位号，闭包参数显式命名为 _slot_idx
    let slot_map = Box::new(from_fn(|_slot_idx| HashSlot::default()));
    let workers = vec![Worker::default(); 2];
    let mut config = Self { slot_map, workers };
    config.initialize_unassigned_worker();
    config
  }

  /// libs/cluster/Server/ClusterConfig.cs:InitializeUnassignedWorker
  fn initialize_unassigned_worker(&mut self) {
    self.workers[RESERVED_WORKER_ID] = Worker {
      address: "unassigned".to_string(),
      ..Worker::default()
    };
  }

  /// libs/cluster/Server/ClusterConfig.cs:InitializeLocalWorker
  ///
  /// 原地更新本地 worker。C# 版每次复制重建 workers 数组；调用方均持有
  /// 写锁，此处直接改写，省去整份 slot_map（64KB）克隆。
  /// C# 散参入参聚合为 [`LocalWorkerSpec`]，免 too_many_arguments
  pub fn initialize_local_worker(&mut self, spec: LocalWorkerSpec<'_>) {
    let w = &mut self.workers[LOCAL_WORKER_ID];
    w.address = spec.address.to_string();
    w.port = spec.port;
    w.nodeid = Some(spec.node_id.to_string());
    w.config_epoch = spec.config_epoch;
    w.role = spec.role;
    w.replica_of_node_id = spec.replica_of_node_id.map(String::from);
    w.replication_offset = 0;
    w.hostname = spec.hostname.map(String::from);
  }

  /// libs/cluster/Server/ClusterConfig.cs:HasAssignedSlots
  ///
  /// 对齐 C#（ClusterConfig.cs:157）按 eff 属主判定：Migrating 槽 eff=LOCAL，
  /// 计入本地名下（TryAddReplica 的 `HasAssignedSlots(1)` 拒绝门依赖此语义）
  pub fn has_assigned_slots(&self, worker_id: u16) -> bool {
    self
      .slot_map
      .iter()
      .any(|slot| slot.eff_worker_id() == worker_id)
  }

  /// libs/cluster/Server/ClusterConfig.cs:IsLocal
  #[inline]
  pub fn is_local(&self, slot: u16, read_write_session: bool) -> bool {
    let slot = slot as usize;
    if slot >= MAX_HASH_SLOT_VALUE {
      return false;
    }
    self.slot_map[slot].eff_worker_id() as usize == LOCAL_WORKER_ID
      || self.is_local_expensive(slot, read_write_session)
  }

  /// libs/cluster/Server/ClusterConfig.cs:IsLocalExpensive
  fn is_local_expensive(&self, slot: usize, read_write_session: bool) -> bool {
    if slot >= MAX_HASH_SLOT_VALUE {
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
        && let Some(ref my_primary) = self.workers[LOCAL_WORKER_ID].replica_of_node_id
        && let Some(w) = self.workers.get(owner_id)
        && let Some(ref owner_node_id) = w.nodeid
      {
        return owner_node_id.eq_ignore_ascii_case(my_primary);
      }
    }
    false
  }

  /// libs/cluster/Server/ClusterConfig.cs:IsKnown
  pub fn is_known(&self, nodeid: &str) -> bool {
    self.worker_by_node_id(nodeid).is_some()
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

  /// libs/cluster/Server/ClusterConfig.cs:LocalNodeId
  pub fn local_node_id(&self) -> Option<&str> {
    self.workers[LOCAL_WORKER_ID].nodeid.as_deref()
  }

  /// libs/cluster/Server/ClusterConfig.cs:LocalNodeIdShort
  pub fn local_node_id_short(&self) -> String {
    let Some(id) = &self.workers[LOCAL_WORKER_ID].nodeid else {
      return String::new();
    };
    // get 而非切片：nodeid 可能来自外部配置，非 ASCII 边界切片会 panic
    id.get(..8).unwrap_or(id).to_string()
  }

  /// libs/cluster/Server/ClusterConfig.cs:LocalNodeRole
  pub fn local_node_role(&self) -> NodeRole {
    self.workers[LOCAL_WORKER_ID].role
  }

  /// libs/cluster/Server/ClusterConfig.cs:LocalNodePrimaryId
  pub fn local_node_primary_id(&self) -> Option<&str> {
    self.workers[LOCAL_WORKER_ID].replica_of_node_id.as_deref()
  }

  /// libs/cluster/Server/ClusterConfig.cs:LocalNodeConfigEpoch
  pub fn local_node_config_epoch(&self) -> i64 {
    self.workers[LOCAL_WORKER_ID].config_epoch
  }

  /// libs/cluster/Server/ClusterConfig.cs:LocalNodeEndpoint
  pub fn local_node_endpoint(&self) -> String {
    format!(
      "{}:{}",
      self.workers[LOCAL_WORKER_ID].address, self.workers[LOCAL_WORKER_ID].port
    )
  }

  /// libs/cluster/Server/ClusterConfig.cs:GetLocalNodePrimaryAddress
  pub fn get_local_node_primary_address(&self) -> (Option<String>, i32) {
    if let Some(id) = self.workers[LOCAL_WORKER_ID].replica_of_node_id.as_deref() {
      self.get_worker_address_from_node_id(id)
    } else {
      (None, -1)
    }
  }

  /// libs/cluster/Server/ClusterConfig.cs:GetLocalNodeReplicaIds
  pub fn get_local_node_replica_ids(&self) -> Vec<String> {
    if let Some(id) = self.local_node_id() {
      self.get_replica_ids(id)
    } else {
      vec![]
    }
  }
}

impl ClusterConfig {
  /// libs/cluster/Server/ClusterConfig.cs:GetLocalNodeReplicaEndpoints
  pub fn get_local_node_replica_endpoints(&self) -> Vec<SocketAddr> {
    let Some(local_id) = self.local_node_id() else {
      return Vec::new();
    };
    let mut replicas = Vec::new();
    for worker in self.workers.iter().skip(2) {
      if let Some(ref replica_of) = worker.replica_of_node_id
        && replica_of.eq_ignore_ascii_case(local_id)
        && let Ok(ip) = worker.address.parse()
      {
        replicas.push(SocketAddr::new(ip, worker.port as u16));
      }
    }
    replicas
  }

  /// libs/cluster/Server/ClusterConfig.cs:GetLocalNodePrimaryEndpoints
  pub fn get_local_node_primary_endpoints(
    &self,
    include_my_primary_first: bool,
  ) -> Vec<SocketAddr> {
    let my_primary_id = if include_my_primary_first {
      self.local_node_primary_id().unwrap_or("")
    } else {
      ""
    };
    let mut primaries = Vec::new();
    let mut first = None;
    for worker in self.workers.iter().skip(2) {
      let Some(node_id) = &worker.nodeid else {
        continue;
      };
      // 地址只解析一次，供主端点与本主端点两分支共用
      let addr = worker
        .address
        .parse()
        .ok()
        .map(|ip| SocketAddr::new(ip, worker.port as u16));
      let is_my_primary = node_id.eq_ignore_ascii_case(my_primary_id);
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

  /// libs/cluster/Server/ClusterConfig.cs:GetLocalPrimarySlots
  ///
  /// 对齐 C#（ClusterConfig.cs:325）按 eff 属主判定（`slotMap[i].workerId > 0`
  /// 且属主节点即主节点 id）：TakeOverFromPrimary 经此收集现主槽位
  pub fn get_local_primary_slots(&self) -> Vec<usize> {
    let Some(pid) = self.local_node_primary_id() else {
      return Vec::new();
    };
    let target_wid = self.get_worker_id_from_node_id(pid);
    if target_wid == 0 {
      return Vec::new();
    }
    (0..MAX_HASH_SLOT_VALUE)
      .filter(|&i| self.slot_map[i].eff_worker_id() == target_wid)
      .collect()
  }

  /// libs/cluster/Server/ClusterConfig.cs:GetMaxConfigEpoch
  ///
  /// 对齐 C# 以 0 为下界折叠（`mx = Math.Max(epoch, mx=0)`）：负 epoch 不参与
  /// 最大值竞争。`[1..]` 等价于 `1..=num_workers()`（num_workers = len-1），
  /// 但对 len==1 的退化配置不 panic
  pub fn get_max_config_epoch(&self) -> i64 {
    self.workers[1..]
      .iter()
      .map(|w| w.config_epoch)
      .fold(0, i64::max)
  }

  /// libs/cluster/Server/ClusterConfig.cs:GetRemoteNodeIds
  pub fn get_remote_node_ids(&self) -> Vec<String> {
    self
      .workers
      .iter()
      .skip(2)
      .filter_map(|w| w.nodeid.clone())
      .collect()
  }

  /// 按节点 id 查找 worker（下标从 1 起，0 号保留位除外），大小写不敏感。
  fn worker_by_node_id(&self, node_id: &str) -> Option<(usize, &Worker)> {
    self.workers.iter().enumerate().skip(1).find(|(_idx, w)| {
      w.nodeid
        .as_deref()
        .is_some_and(|id| id.eq_ignore_ascii_case(node_id))
    })
  }

  /// libs/cluster/Server/ClusterConfig.cs:GetWorkerIdFromNodeId
  pub fn get_worker_id_from_node_id(&self, node_id: &str) -> u16 {
    // 保留 _worker 形参以明确解构项含义，避免裸下划线
    self
      .worker_by_node_id(node_id)
      .map_or(0, |(i, _worker)| i as u16)
  }

  /// libs/cluster/Server/ClusterConfig.cs:GetNodeRoleFromNodeId
  #[inline]
  pub fn get_node_role_from_node_id(&self, node_id: &str) -> NodeRole {
    self
      .get_worker_from_node_id(node_id)
      .map_or(NodeRole::Unassigned, |w| w.role)
  }

  /// libs/cluster/Server/ClusterConfig.cs:GetWorkerFromNodeId
  pub fn get_worker_from_node_id(&self, node_id: &str) -> Option<&Worker> {
    self.workers.iter().skip(1).find(|w| {
      w.nodeid
        .as_deref()
        .is_some_and(|id| id.eq_ignore_ascii_case(node_id))
    })
  }

  /// libs/cluster/Server/ClusterConfig.cs:GetWorkerAddressFromNodeId
  pub fn get_worker_address_from_node_id(&self, node_id: &str) -> (Option<String>, i32) {
    match self.get_worker_from_node_id(node_id) {
      Some(w) => (Some(w.address.clone()), w.port),
      None => (None, -1),
    }
  }

  /// libs/cluster/Server/ClusterConfig.cs:GetHostNameFromNodeId
  pub fn get_host_name_from_node_id(&self, node_id: &str) -> Option<String> {
    self
      .get_worker_from_node_id(node_id)
      .and_then(|w| w.hostname.clone())
  }
}

impl ClusterConfig {
  /// libs/cluster/Server/ClusterConfig.cs:IsImportingSlot
  #[inline]
  pub fn is_importing_slot(&self, slot: u16) -> bool {
    self
      .slot_map
      .get(slot as usize)
      .is_some_and(|s| s.state == SlotState::Importing)
  }

  /// libs/cluster/Server/ClusterConfig.cs:IsMigratingSlot
  #[inline]
  pub fn is_migrating_slot(&self, slot: u16) -> bool {
    self
      .slot_map
      .get(slot as usize)
      .is_some_and(|s| s.state == SlotState::Migrating)
  }

  /// libs/cluster/Server/ClusterConfig.cs:GetState
  #[inline]
  pub fn get_state(&self, slot: u16) -> SlotState {
    self
      .slot_map
      .get(slot as usize)
      .map_or(SlotState::Invalid, |s| s.state)
  }

  /// libs/cluster/Server/ClusterConfig.cs:GetWorkerIdFromSlot
  #[inline]
  pub fn get_worker_id_from_slot(&self, slot: u16) -> usize {
    // 对齐 C#（ClusterConfig.cs:447 经 HashSlot.workerId 投影）返回 eff 属主：
    // Migrating 槽仍由源节点持有并对外负责（C# ClusterManagerSlotState.cs:122
    // "return this node as the current owner"），迁移目标仅作 ASK 重定向
    self
      .slot_map
      .get(slot as usize)
      .map_or(0, |s| s.eff_worker_id() as usize)
  }

  /// libs/cluster/Server/ClusterConfig.cs:GetNodeIdFromSlot
  ///
  /// 经 eff 属主投影（C# ClusterConfig.cs:455）
  #[inline]
  pub fn get_node_id_from_slot(&self, slot: u16) -> Option<String> {
    let wid = self.get_worker_id_from_slot(slot);
    if wid < self.workers.len() {
      self.workers[wid].nodeid.clone()
    } else {
      None
    }
  }

  /// libs/cluster/Server/ClusterConfig.cs:GetOwnerIdFromSlot
  ///
  /// 对齐 C#（ClusterConfig.cs:463）取 `_workerId` raw：即便 Migrating 也报
  /// 迁移目标（与 [`Self::get_node_id_from_slot`] 的 eff 语义刻意区分）
  #[inline]
  pub fn get_owner_id_from_slot(&self, slot: u16) -> Option<String> {
    let wid = self.slot_map[slot as usize].worker_id as usize;
    if wid < self.workers.len() {
      self.workers[wid].nodeid.clone()
    } else {
      None
    }
  }

  /// libs/cluster/Server/ClusterConfig.cs:GetEndpointFromSlot
  ///
  /// eff 属主投影（C# ClusterConfig.cs:471 经 GetWorkerIdFromSlot）：
  /// MOVED 端点指向仍服务该槽的源节点
  #[inline]
  pub fn get_endpoint_from_slot(
    &self,
    slot: u16,
    pref_type: ClusterPreferredEndpointType,
  ) -> (String, i32) {
    let wid = self.get_worker_id_from_slot(slot);
    if wid < self.workers.len() {
      (
        self.get_endpoint_by_preferred_type(wid, pref_type),
        self.workers[wid].port,
      )
    } else {
      ("?".to_string(), -1)
    }
  }

  /// libs/cluster/Server/ClusterConfig.cs:AskEndpointFromSlot
  ///
  /// 对齐 C#（ClusterConfig.cs:484）取 `_workerId` raw：ASK 重定向指向
  /// 迁移目标（与 [`Self::get_endpoint_from_slot`] 的 eff 源节点语义配对）
  #[inline]
  pub fn ask_endpoint_from_slot(
    &self,
    slot: u16,
    pref_type: ClusterPreferredEndpointType,
  ) -> (String, i32) {
    let wid = self.slot_map[slot as usize].worker_id as usize;
    if wid < self.workers.len() {
      (
        self.get_endpoint_by_preferred_type(wid, pref_type),
        self.workers[wid].port,
      )
    } else {
      ("?".to_string(), -1)
    }
  }

  /// libs/cluster/Server/ClusterConfig.cs:GetEndpointByPreferredType
  fn get_endpoint_by_preferred_type(
    &self,
    worker_id: usize,
    pref_type: ClusterPreferredEndpointType,
  ) -> String {
    match pref_type {
      ClusterPreferredEndpointType::Ip => self.workers[worker_id].address.clone(),
      ClusterPreferredEndpointType::Hostname => {
        if let Some(ref h) = self.workers[worker_id].hostname
          && !h.is_empty()
        {
          return h.clone();
        }
        "?".to_string()
      }
      ClusterPreferredEndpointType::Unknown => "?".to_string(),
    }
  }

  /// libs/cluster/Server/ClusterConfig.cs:GetEndpointFromNodeId
  #[inline]
  pub fn get_endpoint_from_node_id(&self, nodeid: &str) -> Option<SocketAddr> {
    self
      .worker_by_node_id(nodeid)
      .and_then(|(_, w)| Some(SocketAddr::new(w.address.parse().ok()?, w.port as u16)))
  }
}

impl ClusterConfig {
  /// libs/cluster/Server/ClusterConfig.cs:GetReplicaIds
  pub fn get_replica_ids(&self, nodeid: &str) -> Vec<String> {
    let mut replicas = Vec::new();
    for worker in self.workers.iter().skip(1) {
      if let Some(ref rep_of) = worker.replica_of_node_id
        && rep_of.eq_ignore_ascii_case(nodeid)
        && let Some(ref id) = worker.nodeid
      {
        replicas.push(id.clone());
      }
    }
    replicas
  }

  /// libs/cluster/Server/ClusterConfig.cs:GetReplicaEndpoints
  pub fn get_replica_endpoints(&self, nodeid: &str) -> Vec<(String, i32)> {
    let mut endpoints = Vec::new();
    for worker in self.workers.iter().skip(1) {
      if let Some(ref rep_of) = worker.replica_of_node_id
        && rep_of.eq_ignore_ascii_case(nodeid)
      {
        endpoints.push((worker.address.clone(), worker.port));
      }
    }
    endpoints
  }

  /// libs/cluster/Server/ClusterConfig.cs:GetWorkerAddress
  #[inline]
  pub fn get_worker_address(&self, worker_id: u16) -> (String, i32) {
    let w = &self.workers[worker_id as usize];
    (w.address.clone(), w.port)
  }

  /// libs/cluster/Server/ClusterConfig.cs:GetWorkerInfoForGossip
  pub fn get_worker_info_for_gossip(&self) -> Vec<(String, String, i32)> {
    let mut result = Vec::new();
    for worker in self.workers.iter().skip(2) {
      if let Some(ref id) = worker.nodeid {
        result.push((id.clone(), worker.address.clone(), worker.port));
      }
    }
    result
  }

  /// libs/cluster/Server/ClusterConfig.cs:GetSlotCountForState
  pub fn get_slot_count_for_state(&self, state: SlotState) -> usize {
    self.slot_map.iter().filter(|s| s.state == state).count()
  }

  /// 单遍扫描统计全部槽位状态计数；CLUSTER INFO 需要 4 个状态计数时
  /// 复用本方法，避免 4 次全表遍历
  pub fn slot_state_counts(&self) -> [usize; SLOT_STATE_KINDS] {
    let mut counts = [0usize; SLOT_STATE_KINDS];
    for slot in self.slot_map.iter() {
      counts[slot.state as usize] += 1;
    }
    counts
  }

  /// libs/cluster/Server/ClusterConfig.cs:GetPrimaryCount
  pub fn get_primary_count(&self) -> usize {
    self.workers[1..]
      .iter()
      .filter(|w| w.role == NodeRole::Primary)
      .count()
  }

  /// libs/cluster/Server/ClusterConfig.cs:GetWorkerNodeIdFromAddress
  pub fn get_worker_node_id_from_address(&self, address: &str, port: i32) -> Option<String> {
    self.workers[1..]
      .iter()
      .find(|w| w.address == address && w.port == port)
      .and_then(|w| w.nodeid.clone())
  }

  /// libs/cluster/Server/ClusterConfig.cs:GetWorkerNodeIdFromAddressOrHostname
  pub fn get_worker_node_id_from_address_or_hostname(
    &self,
    address: &str,
    port: i32,
  ) -> Option<String> {
    self
      .workers
      .get(2..=self.num_workers())?
      .iter()
      .find(|w| w.port == port && (w.address == address || w.hostname.as_deref() == Some(address)))
      .and_then(|w| w.nodeid.clone())
  }

  /// libs/cluster/Server/ClusterConfig.cs:LazyUpdateLocalReplicationOffset
  pub fn lazy_update_local_replication_offset(&mut self, offset: i64) {
    self.workers[LOCAL_WORKER_ID].replication_offset = offset;
  }
}

impl ClusterConfig {
  /// libs/cluster/Server/ClusterConfig.cs:RemoveWorker
  ///
  /// 差异：C# 未找到目标节点时仍按 worker_id=0 执行，会误删 0 号保留位；
  /// 此处直接原样返回，调用方语义不变但杜绝配置损坏
  pub fn remove_worker(&self, nodeid: &str) -> Self {
    let Some((worker_id, _)) = self.worker_by_node_id(nodeid) else {
      return self.clone();
    };

    let mut new_slot_map = self.slot_map.clone();
    for slot in new_slot_map.iter_mut() {
      let state = slot.state;
      let wid = slot.worker_id as usize;

      if state == SlotState::Stable && wid == worker_id {
        slot.worker_id = RESERVED_WORKER_ID as u16;
        slot.state = SlotState::Offline;
      } else if state == SlotState::Migrating && wid == worker_id {
        slot.worker_id = LOCAL_WORKER_ID as u16;
        slot.state = SlotState::Stable;
      } else if state == SlotState::Importing && wid < self.workers.len() {
        if let Some(ref nid) = self.workers[wid].nodeid
          && nid.eq_ignore_ascii_case(nodeid)
        {
          slot.worker_id = RESERVED_WORKER_ID as u16;
          slot.state = SlotState::Offline;
        }
      } else if wid > worker_id {
        // 与 C# 不同处：此处按 raw id 递减。C# 用 eff id 比较，Migrating
        // 槽 eff 恒为 LOCAL(1)，移除低位节点时高位迁移目标的 raw id 不随
        // workers 收缩前移，留下指向越界下标的悬空引用；raw 递减对
        // Migrating 槽同样安全——raw==被删节点已在前面分支处理
        slot.worker_id -= 1;
      }
    }

    let new_workers = self
      .workers
      .iter()
      .enumerate()
      .filter(|&(i, _)| i != worker_id)
      .map(|(_, w)| w.clone())
      .collect();

    Self {
      slot_map: new_slot_map,
      workers: new_workers,
    }
  }

  /// libs/cluster/Server/ClusterConfig.cs:MakeReplicaOf
  pub fn make_replica_of(&mut self, nodeid: Option<&str>) -> &mut Self {
    let w = &mut self.workers[LOCAL_WORKER_ID];
    w.replica_of_node_id = nodeid.map(String::from);
    w.role = NodeRole::Replica;
    self
  }

  /// libs/cluster/Server/ClusterConfig.cs:SetLocalWorkerRole
  pub fn set_local_worker_role(&mut self, role: NodeRole) -> &mut Self {
    self.workers[LOCAL_WORKER_ID].role = role;
    self
  }

  /// libs/cluster/Server/ClusterConfig.cs:TakeOverFromPrimary
  pub fn take_over_from_primary(&mut self) -> &mut Self {
    // 先按现主收集槽位再清 primary 指针，顺序不能反
    let slots = self.get_local_primary_slots();
    for slot in slots {
      let s = &mut self.slot_map[slot];
      s.worker_id = LOCAL_WORKER_ID as u16;
      s.state = SlotState::Stable;
    }
    let w = &mut self.workers[LOCAL_WORKER_ID];
    w.role = NodeRole::Primary;
    w.replica_of_node_id = None;
    self
  }

  /// libs/cluster/Server/ClusterConfig.cs:TryAddSlots
  ///
  /// 先整体校验再占位：与 C# 的"新配置上试错"等价的 all-or-nothing 语义，
  /// 但无需整份克隆
  pub fn try_add_slots(&mut self, slots: Option<&HashSet<usize>>, state: SlotState) -> Result<()> {
    let Some(s) = slots else {
      return Ok(());
    };
    for &slot in s {
      // C#（ClusterConfig.cs:1351）按 eff 属主判定空闲（workerId == 0；
      // eff 仅将 Migrating 投影为 LOCAL(1)，eff==0 ⟺ raw==0，此处直读等价）
      if self.slot_map[slot].worker_id != 0 {
        return Err(Error::SlotNotFree(slot));
      }
    }
    for &slot in s {
      let e = &mut self.slot_map[slot];
      e.worker_id = LOCAL_WORKER_ID as u16;
      e.state = state;
    }
    Ok(())
  }

  /// libs/cluster/Server/ClusterConfig.cs:AssignSlots
  pub fn assign_slots(&mut self, slots: &[usize], worker_id: u16, state: SlotState) -> &mut Self {
    for &slot in slots {
      let e = &mut self.slot_map[slot];
      e.worker_id = worker_id;
      e.state = state;
    }
    self
  }

  /// libs/cluster/Server/ClusterConfig.cs:TryRemoveSlots
  pub fn try_remove_slots(&mut self, slots: Option<&HashSet<usize>>) -> Result<()> {
    let Some(s) = slots else {
      return Ok(());
    };
    for &slot in s {
      // C#（ClusterConfig.cs:1402）按 eff 属主判定非本地（eff==0 ⟺ raw==0，
      // 同 try_add_slots 的直读等价性）
      if self.slot_map[slot].worker_id == 0 {
        return Err(Error::SlotNotLocal(slot));
      }
    }
    for &slot in s {
      let e = &mut self.slot_map[slot];
      e.worker_id = 0;
      e.state = SlotState::Offline;
    }
    Ok(())
  }

  /// libs/cluster/Server/ClusterConfig.cs:UpdateSlotState
  pub fn update_slot_state(&mut self, slot: usize, worker_id: u16, state: SlotState) -> &mut Self {
    let e = &mut self.slot_map[slot];
    e.worker_id = worker_id;
    e.state = state;
    self
  }

  /// libs/cluster/Server/ClusterConfig.cs:UpdateMultiSlotState
  pub fn update_multi_slot_state(
    &mut self,
    slots: &HashSet<usize>,
    worker_id: u16,
    state: SlotState,
  ) -> &mut Self {
    for &slot in slots {
      let e = &mut self.slot_map[slot];
      e.worker_id = worker_id;
      e.state = state;
    }
    self
  }

  /// libs/cluster/Server/ClusterConfig.cs:ResetMultiSlotState
  pub fn reset_multi_slot_state(&mut self, slots: &HashSet<usize>) -> &mut Self {
    for &slot in slots {
      // Migrating 槽归本地源节点，其余按 eff 属主回稳
      let st = self.get_state(slot as u16);
      let wid = if st == SlotState::Migrating {
        LOCAL_WORKER_ID as u16
      } else {
        self.get_worker_id_from_slot(slot as u16) as u16
      };
      let e = &mut self.slot_map[slot];
      e.worker_id = wid;
      e.state = SlotState::Stable;
    }
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
}

impl ClusterConfig {
  /// libs/cluster/Server/ClusterConfig.cs:MergeWorkerInfo
  ///
  /// 原地合并单个 worker：同名节点仅在 epoch 严格更大时更新，否则追加。
  /// 返回是否发生变化。对齐 C# 仅复制 7 个元数据字段（不含
  /// replication_offset——副本位点不随 gossip 传播）。
  /// C# 版每次调用重建 workers 数组，本版配合 [`Self::merge`] 只克隆一次
  fn merge_worker_info(&mut self, worker: &Worker) -> bool {
    let Some(node_id) = worker.nodeid.as_deref() else {
      return false;
    };
    if let Some((i, _)) = self.worker_by_node_id(node_id) {
      if worker.config_epoch <= self.workers[i].config_epoch {
        return false;
      }
      // 对齐 C#：仅覆盖 7 个元数据字段，replication_offset 保留本地值
      // （副本位点不随 gossip 传播）
      let local_offset = self.workers[i].replication_offset;
      self.workers[i].clone_from(worker);
      self.workers[i].replication_offset = local_offset;
      return true;
    }
    let mut w = worker.clone();
    w.replication_offset = 0;
    self.workers.push(w);
    true
  }

  /// libs/cluster/Server/ClusterConfig.cs:MergeSlotMap
  ///
  /// 原地合并槽位图，返回是否有槽位变化。调用方需先做整体克隆
  /// （与 C# Copy 一次 slotMap 相同开销）
  pub fn merge_slot_map(&mut self, sender_config: &ClusterConfig) -> bool {
    let sender_slot_map = &sender_config.slot_map;
    let mut assign_to_worker_id = match sender_config.local_node_id() {
      Some(id) => self.get_worker_id_from_node_id(id),
      None => 0,
    };
    // 发送方身份与角色在整轮合并中不变，提升出 16384 槽循环外
    let sender_node_id = sender_config.local_node_id();
    let sender_primary_id = sender_config.local_node_primary_id();
    let sender_is_primary = sender_config.is_primary();
    let sender_epoch = sender_config.local_node_config_epoch();

    let mut updated = false;
    for i in 0..MAX_HASH_SLOT_VALUE {
      if sender_slot_map[i].state != SlotState::Stable {
        continue;
      }

      // 对齐 C#（ClusterConfig.cs:1168 同经 HashSlot.workerId 投影取 eff）：
      // 本地 Migrating 槽的当前归属按 LOCAL(1) 判定——迁移目标节点 gossip
      // 认领时走 epoch 比较直接移交，而非误判为"目标已是属主"把槽重置为
      // Offline 造成短暂失主
      let current_owner_id = self.slot_map[i].eff_worker_id() as usize;

      // 发送方非本槽认领者且是主：若本地认为属主即发送方（epoch 碰撞后
      // 的错位状态），重置为 Offline 给真实属主重新认领的机会
      if sender_slot_map[i].worker_id as usize != LOCAL_WORKER_ID && sender_is_primary {
        let current_owner_node_id = self
          .workers
          .get(current_owner_id)
          .and_then(|w| w.nodeid.as_deref());
        if let Some(conid) = current_owner_node_id
          && let Some(sid) = sender_node_id
          && conid.eq_ignore_ascii_case(sid)
        {
          let slot = &mut self.slot_map[i];
          slot.worker_id = RESERVED_WORKER_ID as u16;
          slot.state = SlotState::Offline;
          updated = true;
        }
        continue;
      }

      if sender_is_primary {
        // 发送方是本槽认领者且为主：仅当其 epoch 更高才可改写本槽
        if sender_epoch != 0
          && self
            .workers
            .get(current_owner_id)
            .is_some_and(|w| w.config_epoch >= sender_epoch)
        {
          continue;
        }
      } else if current_owner_id != RESERVED_WORKER_ID {
        // 副本场景：仅当本槽现属主即发送方（旧主）才允许移交其副本，
        // 保证计划内 failover 下多副本乱序 gossip 只有接管者生效
        let owner_is_sender = self.workers.get(current_owner_id).is_some_and(|w| {
          w.nodeid
            .as_deref()
            .is_some_and(|id| sender_node_id.is_some_and(|sid| id.eq(sid)))
        });
        if !owner_is_sender {
          continue;
        }
        assign_to_worker_id = match sender_primary_id {
          Some(pid) => self.get_worker_id_from_node_id(pid),
          None => 0,
        };
      }

      // 仅当属主或状态变化才算更新：避免 sender epoch=0 时的消息风暴
      updated |= self.slot_map[i].worker_id != assign_to_worker_id
        || self.slot_map[i].state != SlotState::Stable;

      let slot = &mut self.slot_map[i];
      slot.worker_id = assign_to_worker_id;
      slot.state = SlotState::Stable;
    }
    updated
  }

  /// libs/cluster/Server/ClusterConfig.cs:Merge
  ///
  /// 全程仅一次整份克隆（slot_map 64KB）：先逐 worker 原地合并，再原地
  /// 合并槽位图。原实现每 worker 全量克隆一次，N 个 worker 的 gossip
  /// 合并要做 N+2 次 64KB 拷贝。无变化返回 None（对标 C# TryMerge 的
  /// `currentCopy == next` 快速失败，避免无谓落盘）
  pub fn merge(
    &self,
    sender_config: &ClusterConfig,
    worker_ban_list: &HashMap<String, i64>,
  ) -> Option<Self> {
    let local_id = self.local_node_id();
    let mut merged = self.clone();
    let mut changed = false;

    for worker in &sender_config.workers[1..=sender_config.num_workers()] {
      let Some(ref sid) = worker.nodeid else {
        continue;
      };
      if local_id.is_some_and(|lid| lid.eq_ignore_ascii_case(sid))
        || worker_ban_list.contains_key(sid)
      {
        continue;
      }
      changed |= merged.merge_worker_info(worker);
    }

    changed |= merged.merge_slot_map(sender_config);
    changed.then_some(merged)
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

    let sender_node_id = sender_config.local_node_id().unwrap_or("");
    let local_node_id = self.local_node_id().unwrap_or("");

    // 对齐 C#：仅当发送方 id 字典序更大才自增，双方各退一步避免死循环
    if sender_node_id.cmp(local_node_id) != Ordering::Greater {
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
}

impl ClusterConfig {
  /// libs/cluster/Server/ClusterConfig.cs:GetClusterInfo
  pub fn get_cluster_info(&self, cluster_provider: Option<&ClusterProvider>) -> String {
    let mut sb = String::new();
    for i in 1..=self.num_workers() {
      let info = if let Some(cp) = cluster_provider {
        if let Some(ref id) = self.workers[i].nodeid {
          cp.get_connection_info(id)
        } else {
          ConnectionInfo::default()
        }
      } else {
        ConnectionInfo::default()
      };
      self.append_node_info(i, &info, &mut sb);
    }
    sb
  }

  /// libs/cluster/Server/ClusterConfig.cs:GetNodeInfo
  pub fn get_node_info(&self, worker_id: usize, info: &ConnectionInfo) -> String {
    let mut sb = String::new();
    self.append_node_info(worker_id, info, &mut sb);
    sb
  }

  fn append_node_info(&self, worker_id: usize, info: &ConnectionInfo, sb: &mut String) {
    let w = &self.workers[worker_id];
    let _ = write!(
      sb,
      "{} {}:{}@{}",
      w.nodeid.as_deref().unwrap_or(""),
      w.address,
      w.port,
      w.port + BUS_PORT_OFFSET
    );

    if let Some(ref h) = w.hostname
      && !h.is_empty()
    {
      let _ = write!(sb, ",{}", h);
    }

    let _ = write!(
      sb,
      " {}{} {} {} {} {} {}",
      if worker_id == LOCAL_WORKER_ID {
        "myself,"
      } else {
        ""
      },
      if w.role == NodeRole::Primary {
        "master"
      } else {
        "slave"
      },
      if w.role == NodeRole::Replica {
        w.replica_of_node_id.as_deref().unwrap_or("-")
      } else {
        "-"
      },
      info.ping,
      info.pong,
      w.config_epoch,
      if info.connected || worker_id == LOCAL_WORKER_ID {
        "connected"
      } else {
        "disconnected"
      }
    );

    self.append_slot_range(sb, worker_id as u16);
    self.append_special_states(sb, worker_id as u16);
    sb.push('\n');
  }

  fn append_slot_range(&self, sb: &mut String, worker_id: u16) {
    let mut start = u16::MAX;
    let mut end = 0;
    for i in 0..MAX_HASH_SLOT_VALUE {
      // 对齐 C#（ClusterConfig.cs:641）按 eff 属主分段：Migrating 槽 eff=LOCAL
      // 计入本地区间（Redis 语义：迁移槽仍在源节点名下，另有 [slot->-target] 标注）
      if self.slot_map[i].eff_worker_id() == worker_id {
        if (i as u16) < start {
          start = i as u16;
        }
        if (i as u16) > end {
          end = i as u16;
        }
      } else {
        if start != u16::MAX {
          if end == start {
            let _ = write!(sb, " {}", start);
          } else {
            let _ = write!(sb, " {}-{}", start, end);
          }
          start = u16::MAX;
          end = 0;
        }
      }
    }
    if start != u16::MAX {
      if end == start {
        let _ = write!(sb, " {}", start);
      } else {
        let _ = write!(sb, " {}-{}", start, end);
      }
    }
  }

  fn append_special_states(&self, sb: &mut String, worker_id: u16) {
    if worker_id as usize != LOCAL_WORKER_ID {
      return;
    }
    for slot in 0..self.slot_map.len() {
      let worker_id = self.slot_map[slot].worker_id as usize;
      let state = self.slot_map[slot].state;

      if state == SlotState::Stable {
        continue;
      }
      if worker_id > self.num_workers() {
        continue;
      }

      if let Some(ref node_id) = self.workers[worker_id].nodeid {
        match state {
          SlotState::Migrating => {
            let _ = write!(sb, " [{}->-{}]", slot, node_id);
          }
          SlotState::Importing => {
            let _ = write!(sb, " [{}-<-{}]", slot, node_id);
          }
          _ => {}
        }
      }
    }
  }
}

impl ClusterConfig {
  /// libs/cluster/Server/ClusterConfig.cs:GetShardRanges
  ///
  /// 单遍扫描输出连续槽区间；哨兵扫描自然闭合末区间。
  /// 对齐 C#（ClusterConfig.cs:641）按 eff 属主分段（Migrating 槽随源节点）
  pub fn get_shard_ranges(&self, worker_id: usize) -> Vec<(u16, u16)> {
    let mut ranges = Vec::new();
    let mut start: Option<u16> = None;
    for (i, slot) in self.slot_map.iter().enumerate() {
      match (start, slot.eff_worker_id() as usize == worker_id) {
        (None, true) => start = Some(i as u16),
        (Some(s), false) => {
          ranges.push((s, i as u16 - 1));
          start = None;
        }
        _ => {}
      }
    }
    if let Some(s) = start {
      ranges.push((s, MAX_HASH_SLOT_VALUE as u16 - 1));
    }
    ranges
  }

  /// libs/cluster/Server/ClusterConfig.cs:GetWorkerReplicas
  pub fn get_worker_replicas(&self, worker_id: usize) -> Vec<usize> {
    let primary_id = self.workers[worker_id].nodeid.clone().unwrap_or_default();
    self
      .workers
      .iter()
      .enumerate()
      .take(self.num_workers() + 1)
      .skip(1)
      .filter(|(_, w)| {
        w.replica_of_node_id
          .as_deref()
          .is_some_and(|rep_of| rep_of.eq_ignore_ascii_case(&primary_id))
      })
      .map(|(i, _)| i)
      .collect()
  }

  /// libs/cluster/Server/ClusterConfig.cs:GetAllNodeIds
  pub fn get_all_node_ids(&self) -> Vec<(String, SocketAddr)> {
    let mut all_node_ids = Vec::new();
    for worker in self.workers.iter().skip(2) {
      if let Some(ref id) = worker.nodeid
        && let Ok(ip) = worker.address.parse()
      {
        all_node_ids.push((id.clone(), SocketAddr::new(ip, worker.port as u16)));
      }
    }
    all_node_ids
  }

  /// libs/cluster/Server/ClusterConfig.cs:GetNodeIdsForShard
  pub fn get_node_ids_for_shard(&self) -> Vec<(String, SocketAddr)> {
    let primary_id = if self.local_node_role() == NodeRole::Primary {
      self.local_node_id().map(|s| s.to_string())
    } else {
      self.workers[1].replica_of_node_id.clone()
    };

    let mut shard_node_ids = Vec::new();
    for worker in self.workers.iter().skip(2) {
      if let Some(ref pid) = primary_id {
        let is_replica_of_primary = worker
          .replica_of_node_id
          .as_deref()
          .map(|s| s.eq_ignore_ascii_case(pid))
          .unwrap_or(false);
        let is_primary = worker.nodeid.as_deref().map(|s| pid.eq(s)).unwrap_or(false);
        if (is_replica_of_primary || is_primary)
          && let Some(ref nid) = worker.nodeid
          && let Ok(ip) = worker.address.parse()
        {
          shard_node_ids.push((nid.clone(), SocketAddr::new(ip, worker.port as u16)));
        }
      }
    }
    shard_node_ids
  }

  /// libs/cluster/Server/ClusterConfig.cs:GetSlotList
  ///
  /// 对齐 C#（ClusterConfig.cs:918）按 eff 属主收集：TryStopWrites 的
  /// `GetSlotList(1)` 须把 Migrating 槽一并移交接管者
  pub fn get_slot_list(&self, worker_id: u16) -> Vec<usize> {
    let mut result = Vec::new();
    for i in 0..MAX_HASH_SLOT_VALUE {
      if self.slot_map[i].eff_worker_id() == worker_id {
        result.push(i);
      }
    }
    result
  }
}

impl ClusterConfig {
  /// libs/cluster/Server/ClusterConfig.cs:GetShardsInfo
  pub fn get_shards_info(
    &self,
    cluster_connection: Option<&ClusterProvider>,
    pref_type: ClusterPreferredEndpointType,
  ) -> String {
    let mut sb = String::new();
    let mut shard_count = 0;
    let mut shards_str = String::new();

    for i in 1..=self.num_workers() {
      if self.workers[i].role == NodeRole::Primary {
        let shard_ranges = self.get_shard_ranges(i);
        let replica_worker_ids = self.get_worker_replicas(i);
        self.append_formatted_shard_info(
          &mut shards_str,
          i,
          &shard_ranges,
          &replica_worker_ids,
          cluster_connection,
          pref_type,
        );
        shard_count += 1;
      }
    }
    let _ = write!(sb, "*{}\r\n{}", shard_count, shards_str);
    sb
  }

  fn append_formatted_shard_info(
    &self,
    sb: &mut String,
    primary_worker_id: usize,
    shard_ranges: &[(u16, u16)],
    replica_worker_ids: &[usize],
    cluster_connection: Option<&ClusterProvider>,
    pref_type: ClusterPreferredEndpointType,
  ) {
    sb.push_str("*4\r\n");
    sb.push_str("$5\r\nslots\r\n");
    let _ = write!(sb, "*{}\r\n", shard_ranges.len() * 2);
    for range in shard_ranges {
      let _ = write!(sb, ":{}\r\n:{}\r\n", range.0, range.1);
    }

    sb.push_str("$5\r\nnodes\r\n");
    let _ = write!(sb, "*{}\r\n", 1 + replica_worker_ids.len());

    if primary_worker_id == LOCAL_WORKER_ID {
      self.append_formatted_node_info(sb, primary_worker_id, true, pref_type);
    } else {
      let connected = if let Some(cp) = cluster_connection {
        if let Some(ref nid) = self.workers[primary_worker_id].nodeid {
          cp.get_connection_info(nid).connected
        } else {
          false
        }
      } else {
        false
      };
      self.append_formatted_node_info(sb, primary_worker_id, connected, pref_type);
    }

    for &id in replica_worker_ids {
      let connected = if let Some(cp) = cluster_connection {
        if let Some(ref nid) = self.workers[id].nodeid {
          cp.get_connection_info(nid).connected
        } else {
          false
        }
      } else {
        false
      };
      self.append_formatted_node_info(sb, id, connected, pref_type);
    }
  }

  fn append_formatted_node_info(
    &self,
    sb: &mut String,
    worker_id: usize,
    connected: bool,
    pref_type: ClusterPreferredEndpointType,
  ) {
    let ip = &self.workers[worker_id].address;
    let hostname = self.workers[worker_id].hostname.as_deref().unwrap_or("");
    let has_hostname = !hostname.is_empty();
    let role = if self.workers[worker_id].role == NodeRole::Primary {
      "master"
    } else {
      "slave"
    };

    let endpoint = match pref_type {
      ClusterPreferredEndpointType::Hostname => {
        if has_hostname {
          hostname
        } else {
          "?"
        }
      }
      ClusterPreferredEndpointType::Unknown => "?",
      _ => ip,
    };

    let field_count = if has_hostname { 16 } else { 14 };

    let _ = write!(sb, "*{}\r\n", field_count);
    sb.push_str("$2\r\nid\r\n");
    let nodeid = self.workers[worker_id].nodeid.as_deref().unwrap_or("");
    // 长度动态计算：nodeid 并非恒 40 字符（uuid simple 为 32），RESP bulk
    // string 长度声明错会让客户端解析错位
    let _ = write!(sb, "${}\r\n{}\r\n", nodeid.len(), nodeid);
    sb.push_str("$4\r\nport\r\n");
    let _ = write!(sb, ":{}\r\n", self.workers[worker_id].port);
    sb.push_str("$2\r\nip\r\n");
    let _ = write!(sb, "${}\r\n{}\r\n", ip.len(), ip);
    sb.push_str("$8\r\nendpoint\r\n");
    let _ = write!(sb, "${}\r\n{}\r\n", endpoint.len(), endpoint);
    if has_hostname {
      sb.push_str("$8\r\nhostname\r\n");
      let _ = write!(sb, "${}\r\n{}\r\n", hostname.len(), hostname);
    }
    sb.push_str("$4\r\nrole\r\n");
    let _ = write!(sb, "${}\r\n{}\r\n", role.len(), role);
    sb.push_str("$18\r\nreplication-offset\r\n");
    let _ = write!(sb, ":{}\r\n", self.workers[worker_id].replication_offset);
    sb.push_str("$6\r\nhealth\r\n");
    if connected {
      sb.push_str("$6\r\nonline\r\n");
    } else {
      sb.push_str("$7\r\noffline\r\n");
    }
  }
}

impl ClusterConfig {
  /// libs/cluster/Server/ClusterConfig.cs:GetSlotsInfo
  ///
  /// 对齐 C#（ClusterConfig.cs:890-894）按 eff 属主分段：Migrating 槽
  /// eff=LOCAL 随源节点区间上报（Redis 语义：迁移未完成前属主不变）
  pub fn get_slots_info(&self, pref_type: ClusterPreferredEndpointType) -> String {
    let mut sb = String::new();
    let mut slot_ranges = 0;
    let mut slot_start = 0;
    let mut slots_str = String::new();

    while slot_start < MAX_HASH_SLOT_VALUE {
      if self.slot_map[slot_start].state == SlotState::Offline {
        slot_start += 1;
        continue;
      }

      let mut slot_end = slot_start;
      while slot_end < MAX_HASH_SLOT_VALUE {
        if self.slot_map[slot_end].state == SlotState::Offline
          || self.slot_map[slot_start].eff_worker_id() != self.slot_map[slot_end].eff_worker_id()
        {
          break;
        }
        slot_end += 1;
      }

      slot_end -= 1;
      let curr_worker_id = self.slot_map[slot_start].eff_worker_id() as usize;
      // 区间属主以借用传递，免每区间 4 份字符串克隆
      let owner = &self.workers[curr_worker_id];
      let replica_ids = self.get_replica_ids(owner.nodeid.as_deref().unwrap_or_default());

      self.append_formatted_slot_info(
        &mut slots_str,
        slot_start,
        slot_end,
        owner,
        &replica_ids,
        pref_type,
      );
      slot_ranges += 1;
      slot_start = slot_end + 1;
    }

    let _ = write!(sb, "*{}\r\n{}", slot_ranges, slots_str);
    sb
  }

  fn append_formatted_slot_info(
    &self,
    sb: &mut String,
    slot_start: usize,
    slot_end: usize,
    owner: &Worker,
    replica_ids: &[String],
    pref_type: ClusterPreferredEndpointType,
  ) {
    let count_a = 3 + replica_ids.len();
    let _ = write!(sb, "*{}\r\n:{}\r\n:{}\r\n", count_a, slot_start, slot_end);

    self.append_node_networking_info(
      sb,
      &owner.address,
      owner.port,
      owner.nodeid.as_deref().unwrap_or_default(),
      owner.hostname.as_deref(),
      pref_type,
    );

    for replica_id in replica_ids {
      match self.worker_by_node_id(replica_id) {
        Some((_, w)) => self.append_node_networking_info(
          sb,
          &w.address,
          w.port,
          w.nodeid.as_deref().unwrap_or(replica_id),
          w.hostname.as_deref(),
          pref_type,
        ),
        // 副本行不在配置内时按 C# 空地址语义输出（$-1 + port -1）
        None => self.append_node_networking_info(sb, "", -1, replica_id, None, pref_type),
      }
    }
  }

  fn append_node_networking_info(
    &self,
    sb: &mut String,
    ip_address: &str,
    port: i32,
    nodeid: &str,
    hostname: Option<&str>,
    pref_type: ClusterPreferredEndpointType,
  ) {
    sb.push_str("*4\r\n");
    let is_null_or_empty_hostname = hostname.is_none_or(|h| h.is_empty());

    match pref_type {
      ClusterPreferredEndpointType::Ip => {
        self.append_value_or_null(sb, Some(ip_address));
        let _ = write!(
          sb,
          ":{}\r\n${}\r\n{}\r\n*{}\r\n",
          port,
          nodeid.len(),
          nodeid,
          if is_null_or_empty_hostname { 0 } else { 2 }
        );
        if !is_null_or_empty_hostname {
          sb.push_str("$8\r\nhostname\r\n");
          self.append_value_or_null(sb, hostname);
        }
      }
      ClusterPreferredEndpointType::Hostname => {
        let hostname_for_resp = if is_null_or_empty_hostname {
          "?"
        } else {
          hostname.unwrap()
        };
        self.append_value_or_null(sb, Some(hostname_for_resp));
        let _ = write!(
          sb,
          ":{}\r\n${}\r\n{}\r\n*2\r\n$2\r\nip\r\n",
          port,
          nodeid.len(),
          nodeid
        );
        self.append_value_or_null(sb, Some(ip_address));
      }
      ClusterPreferredEndpointType::Unknown => {
        self.append_value_or_null(sb, None);
        let _ = write!(
          sb,
          ":{}\r\n${}\r\n{}\r\n*{}\r\n$2\r\nip\r\n",
          port,
          nodeid.len(),
          nodeid,
          if is_null_or_empty_hostname { 2 } else { 4 }
        );
        self.append_value_or_null(sb, Some(ip_address));
        if !is_null_or_empty_hostname {
          sb.push_str("$8\r\nhostname\r\n");
          self.append_value_or_null(sb, hostname);
        }
      }
    }
  }

  fn append_value_or_null(&self, sb: &mut String, value: Option<&str>) {
    if let Some(v) = value {
      if v.is_empty() {
        sb.push_str("$-1\r\n");
      } else {
        let _ = write!(sb, "${}\r\n{}\r\n", v.len(), v);
      }
    } else {
      sb.push_str("$-1\r\n");
    }
  }
}

/// 集群配置线格式（bitcode 编码）
///
/// 槽位图以 RLE 段传输（连续同 (worker_id, state) 的槽数远多于段数，
/// 典型集群个位数段即可覆盖 16384 槽），worker 自 1 号起序列化，
/// 0 号保留位反序列化时按 default 重建——与 C# 布局语义一致，但编码
/// 由 .NET BinaryWriter 的 7-bit 变长整数 hack 换为 bitcode 位压缩，
/// 且解码不再吞错（原实现对截断/越界静默补 0/空串，会产出损坏配置）
#[derive(Encode, Decode)]
struct ConfigWire {
  segments: Vec<SlotSegmentWire>,
  workers: Vec<Worker>,
}

/// 一段连续同状态槽位
#[derive(Encode, Decode)]
struct SlotSegmentWire {
  count: u16,
  worker_id: u16,
  /// SlotState 的 u8 表示（显式字节而非枚举直编，状态含义不依赖位布局）
  state: u8,
}

impl ClusterConfig {
  /// libs/cluster/Server/ClusterConfig.cs:TryPeekVersion
  ///
  /// 全量解码前快速校验版本号（gossip 接收端先用它拒绝异版本节点）
  #[inline]
  pub fn try_peek_version(data: &[u8]) -> Option<u8> {
    data.first().copied()
  }

  /// libs/cluster/Server/ClusterConfig.cs:ToByteArray
  pub fn to_byte_array(&self) -> Vec<u8> {
    let segments: Vec<SlotSegmentWire> = self
      .slot_map
      .iter()
      .map(|s| (s.worker_id, s.state as u8))
      .fold(Vec::new(), |mut segs, (worker_id, state)| {
        if let Some(last) = segs.last_mut()
          && last.worker_id == worker_id
          && last.state == state
        {
          last.count += 1;
        } else {
          segs.push(SlotSegmentWire {
            count: 1,
            worker_id,
            state,
          });
        }
        segs
      });

    let wire = ConfigWire {
      segments,
      workers: self.workers[1..].to_vec(),
    };

    let mut out = Vec::with_capacity(wire.workers.len() * 64 + 16);
    out.push(CLUSTER_CONFIG_VERSION);
    out.extend_from_slice(&bitcode::encode(&wire));
    out
  }

  /// libs/cluster/Server/ClusterConfig.cs:FromByteArray
  pub fn from_byte_array(data: &[u8]) -> Result<Self> {
    let Some((&version, payload)) = data.split_first() else {
      return Err(Error::PayloadTooShort);
    };
    if version != CLUSTER_CONFIG_VERSION {
      return Err(Error::Version {
        got: version,
        expect: CLUSTER_CONFIG_VERSION,
      });
    }

    let wire: ConfigWire = bitcode::decode(payload)?;

    // 线格式自 1 号本地 worker 起序列化，空列表即结构损坏：放行会产出无本地
    // 位的配置，后续 LOCAL_WORKER_ID 索引 panic（C# 同场景在解码后首次访问
    // workers[1] 时才崩溃，此处前置为解码期 fail-loud）
    if wire.workers.is_empty() {
      return Err(Error::MissingWorkers);
    }

    let mut slot_map = Box::new([HashSlot::default(); MAX_HASH_SLOT_VALUE]);
    // worker_id 越界校验先于槽位展开：越界 id 若入库，CLUSTER SLOTS 与
    // 副本读路径（is_local_expensive）按属主下标直取 workers 会 panic，
    // 恶意/损坏 gossip 载荷即可击穿节点进程
    let worker_limit = wire.workers.len();
    let mut offset = 0usize;
    for seg in &wire.segments {
      let state = SlotState::from_repr(seg.state).ok_or(Error::SlotState(seg.state))?;
      if seg.worker_id as usize > worker_limit {
        return Err(Error::SlotWorkerId(seg.worker_id));
      }
      let end = offset + seg.count as usize;
      if end > MAX_HASH_SLOT_VALUE {
        return Err(Error::SlotOverflow);
      }
      for slot in &mut slot_map[offset..end] {
        slot.worker_id = seg.worker_id;
        slot.state = state;
      }
      offset = end;
    }

    // 0 号保留位不在线格式内，按 unassigned 重建（对应 C# skip(1) 布局）
    let mut workers = vec![
      Worker {
        address: "unassigned".to_string(),
        ..Worker::default()
      };
      wire.workers.len() + 1
    ];
    workers[1..].clone_from_slice(&wire.workers);

    Ok(Self { slot_map, workers })
  }
}
