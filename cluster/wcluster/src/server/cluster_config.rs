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

/// 槽位空间上下界（Redis Cluster 语义：16384 槽）
pub const MIN_HASH_SLOT_VALUE: usize = 0;
pub const MAX_HASH_SLOT_VALUE: usize = 16384;

/// CLUSTER NODES 中 bus 端口偏移（garnet 语义：bus port = port + 10000）
const BUS_PORT_OFFSET: i32 = 10000;

// worker id 常量定义域在 [`crate::server::worker`]，此处转出口维持
// 槽位/配置方法群的单一引用路径
pub use crate::server::worker::{LOCAL_WORKER_ID, RESERVED_WORKER_ID};

/// garnet相对路径:Server:ClusterPreferredEndpointType
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ClusterPreferredEndpointType {
  Ip,
  Hostname,
  Unknown,
}

/// garnet相对路径:Server:ClusterConfig
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
  /// garnet相对路径:Server:ClusterConfig:OutOfRange
  pub fn out_of_range(slot: usize) -> bool {
    slot >= MAX_HASH_SLOT_VALUE
  }

  /// garnet相对路径:Server:ClusterConfig:NumWorkers
  pub fn num_workers(&self) -> usize {
    self.workers.len().saturating_sub(1)
  }

  /// garnet相对路径:Server:ClusterConfig:ClusterConfig
  pub fn new() -> Self {
    let slot_map = Box::new(from_fn(|_| HashSlot::default()));
    let workers = vec![Worker::default(); 2];
    let mut config = Self { slot_map, workers };
    config.initialize_unassigned_worker();
    config
  }

  pub fn with_data(slot_map: Box<[HashSlot; MAX_HASH_SLOT_VALUE]>, workers: Vec<Worker>) -> Self {
    Self { slot_map, workers }
  }

  /// garnet相对路径:Server:ClusterConfig:InitializeUnassignedWorker
  ///
  /// 保留位恒为 [`Worker::default`]（全零/None），杜绝逐字段赋值漂移
  fn initialize_unassigned_worker(&mut self) {
    self.workers[RESERVED_WORKER_ID] = Worker::default();
  }

  /// garnet相对路径:Server:ClusterConfig:InitializeLocalWorker
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

  /// garnet相对路径:Server:ClusterConfig:HasAssignedSlots
  pub fn has_assigned_slots(&self, worker_id: u16) -> bool {
    for i in 0..MAX_HASH_SLOT_VALUE {
      if self.slot_map[i].eff_worker_id() == worker_id {
        return true;
      }
    }
    false
  }

  /// garnet相对路径:Server:ClusterConfig:IsLocal
  #[inline]
  pub fn is_local(&self, slot: u16, read_write_session: bool) -> bool {
    let slot = slot as usize;
    self.slot_map[slot].eff_worker_id() as usize == LOCAL_WORKER_ID
      || self.is_local_expensive(slot, read_write_session)
  }

  /// garnet相对路径:Server:ClusterConfig:IsLocalExpensive
  fn is_local_expensive(&self, slot: usize, read_write_session: bool) -> bool {
    if self.slot_map[slot].state == SlotState::Migrating {
      return true;
    }
    if read_write_session && self.workers[LOCAL_WORKER_ID].role == NodeRole::Replica {
      let owner_id = self.slot_map[slot].worker_id as usize;
      if owner_id > 1
        && let Some(ref my_primary) = self.workers[LOCAL_WORKER_ID].replica_of_node_id
        && let Some(ref owner_node_id) = self.workers[owner_id].nodeid
      {
        return owner_node_id.eq_ignore_ascii_case(my_primary);
      }
    }
    false
  }

  /// garnet相对路径:Server:ClusterConfig:IsKnown
  pub fn is_known(&self, nodeid: &str) -> bool {
    self.worker_by_node_id(nodeid).is_some()
  }

  /// garnet相对路径:Server:ClusterConfig:IsPrimary
  pub fn is_primary(&self) -> bool {
    self.local_node_role() == NodeRole::Primary
  }

  /// garnet相对路径:Server:ClusterConfig:IsReplica
  pub fn is_replica(&self) -> bool {
    self.local_node_role() == NodeRole::Replica
  }

  /// garnet相对路径:Server:ClusterConfig:LocalNodeIp
  pub fn local_node_ip(&self) -> &str {
    &self.workers[LOCAL_WORKER_ID].address
  }

  /// garnet相对路径:Server:ClusterConfig:LocalNodePort
  pub fn local_node_port(&self) -> i32 {
    self.workers[LOCAL_WORKER_ID].port
  }

  /// garnet相对路径:Server:ClusterConfig:LocalNodeId
  pub fn local_node_id(&self) -> Option<&str> {
    self.workers[LOCAL_WORKER_ID].nodeid.as_deref()
  }

  /// garnet相对路径:Server:ClusterConfig:LocalNodeIdShort
  pub fn local_node_id_short(&self) -> String {
    let Some(id) = &self.workers[LOCAL_WORKER_ID].nodeid else {
      return String::new();
    };
    // get 而非切片：nodeid 可能来自外部配置，非 ASCII 边界切片会 panic
    id.get(..8).unwrap_or(id).to_string()
  }

  /// garnet相对路径:Server:ClusterConfig:LocalNodeRole
  pub fn local_node_role(&self) -> NodeRole {
    self.workers[LOCAL_WORKER_ID].role
  }

  /// garnet相对路径:Server:ClusterConfig:LocalNodePrimaryId
  pub fn local_node_primary_id(&self) -> Option<&str> {
    self.workers[LOCAL_WORKER_ID].replica_of_node_id.as_deref()
  }

  /// garnet相对路径:Server:ClusterConfig:LocalNodeConfigEpoch
  pub fn local_node_config_epoch(&self) -> i64 {
    self.workers[LOCAL_WORKER_ID].config_epoch
  }

  /// garnet相对路径:Server:ClusterConfig:LocalNodeEndpoint
  pub fn local_node_endpoint(&self) -> String {
    format!(
      "{}:{}",
      self.workers[LOCAL_WORKER_ID].address, self.workers[LOCAL_WORKER_ID].port
    )
  }

  /// garnet相对路径:Server:ClusterConfig:GetLocalNodePrimaryAddress
  pub fn get_local_node_primary_address(&self) -> (Option<String>, i32) {
    if let Some(id) = self.workers[LOCAL_WORKER_ID].replica_of_node_id.as_deref() {
      self.get_worker_address_from_node_id(id)
    } else {
      (None, -1)
    }
  }

  /// garnet相对路径:Server:ClusterConfig:GetLocalNodeReplicaIds
  pub fn get_local_node_replica_ids(&self) -> Vec<String> {
    if let Some(id) = self.local_node_id() {
      self.get_replica_ids(id)
    } else {
      vec![]
    }
  }
}

impl ClusterConfig {
  /// garnet相对路径:Server:ClusterConfig:GetLocalNodeReplicaEndpoints
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

  /// garnet相对路径:Server:ClusterConfig:GetLocalNodePrimaryEndpoints
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

  /// garnet相对路径:Server:ClusterConfig:GetLocalPrimarySlots
  pub fn get_local_primary_slots(&self) -> Vec<usize> {
    let primary_id = self.local_node_primary_id();
    let mut slots = Vec::new();
    if let Some(pid) = primary_id {
      for i in 0..MAX_HASH_SLOT_VALUE {
        let wid = self.slot_map[i].eff_worker_id() as usize;
        if wid > 0
          && wid < self.workers.len()
          && let Some(nid) = &self.workers[wid].nodeid
          && nid.eq_ignore_ascii_case(pid)
        {
          slots.push(i);
        }
      }
    }
    slots
  }

  /// garnet相对路径:Server:ClusterConfig:GetMaxConfigEpoch
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

  /// garnet相对路径:Server:ClusterConfig:GetRemoteNodeIds
  pub fn get_remote_node_ids(&self) -> Vec<String> {
    self
      .workers
      .iter()
      .skip(2)
      .filter_map(|w| w.nodeid.clone())
      .collect()
  }

  /// 按节点 id 查找 worker（下标从 1 起，0 号保留位除外），大小写不敏感。
  /// 全部 node_id→worker 投影方法共用此单一查找定义，替代原先各写一遍
  /// 的"id 查找 + 越界回退"样板
  fn worker_by_node_id(&self, node_id: &str) -> Option<(usize, &Worker)> {
    self.workers.iter().enumerate().skip(1).find(|(_, w)| {
      w.nodeid
        .as_deref()
        .is_some_and(|id| id.eq_ignore_ascii_case(node_id))
    })
  }

  /// garnet相对路径:Server:ClusterConfig:GetWorkerIdFromNodeId
  pub fn get_worker_id_from_node_id(&self, node_id: &str) -> u16 {
    self.worker_by_node_id(node_id).map_or(0, |(i, _)| i as u16)
  }

  /// garnet相对路径:Server:ClusterConfig:GetNodeRoleFromNodeId
  #[inline]
  pub fn get_node_role_from_node_id(&self, node_id: &str) -> NodeRole {
    self
      .worker_by_node_id(node_id)
      .map_or(NodeRole::Unassigned, |(_, w)| w.role)
  }

  /// garnet相对路径:Server:ClusterConfig:GetWorkerFromNodeId
  pub fn get_worker_from_node_id(&self, node_id: &str) -> Option<&Worker> {
    self.worker_by_node_id(node_id).map(|(_, w)| w)
  }

  /// garnet相对路径:Server:ClusterConfig:GetWorkerAddressFromNodeId
  pub fn get_worker_address_from_node_id(&self, node_id: &str) -> (Option<String>, i32) {
    match self.worker_by_node_id(node_id) {
      Some((_, w)) => (Some(w.address.clone()), w.port),
      None => (None, -1),
    }
  }

  /// garnet相对路径:Server:ClusterConfig:GetHostNameFromNodeId
  pub fn get_host_name_from_node_id(&self, node_id: &str) -> Option<String> {
    self
      .worker_by_node_id(node_id)
      .and_then(|(_, w)| w.hostname.clone())
  }
}

impl ClusterConfig {
  /// garnet相对路径:Server:ClusterConfig:IsImportingSlot
  #[inline]
  pub fn is_importing_slot(&self, slot: u16) -> bool {
    self.slot_map[slot as usize].state == SlotState::Importing
  }

  /// garnet相对路径:Server:ClusterConfig:IsMigratingSlot
  #[inline]
  pub fn is_migrating_slot(&self, slot: u16) -> bool {
    self.slot_map[slot as usize].state == SlotState::Migrating
  }

  /// garnet相对路径:Server:ClusterConfig:GetState
  #[inline]
  pub fn get_state(&self, slot: u16) -> SlotState {
    self.slot_map[slot as usize].state
  }

  /// garnet相对路径:Server:ClusterConfig:GetWorkerIdFromSlot
  #[inline]
  pub fn get_worker_id_from_slot(&self, slot: u16) -> usize {
    self.slot_map[slot as usize].eff_worker_id() as usize
  }

  /// garnet相对路径:Server:ClusterConfig:GetNodeIdFromSlot
  #[inline]
  pub fn get_node_id_from_slot(&self, slot: u16) -> Option<String> {
    let wid = self.get_worker_id_from_slot(slot);
    if wid < self.workers.len() {
      self.workers[wid].nodeid.clone()
    } else {
      None
    }
  }

  /// garnet相对路径:Server:ClusterConfig:GetOwnerIdFromSlot
  #[inline]
  pub fn get_owner_id_from_slot(&self, slot: u16) -> Option<String> {
    let wid = self.slot_map[slot as usize].worker_id as usize;
    if wid < self.workers.len() {
      self.workers[wid].nodeid.clone()
    } else {
      None
    }
  }

  /// garnet相对路径:Server:ClusterConfig:GetEndpointFromSlot
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

  /// garnet相对路径:Server:ClusterConfig:AskEndpointFromSlot
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

  /// garnet相对路径:Server:ClusterConfig:GetEndpointByPreferredType
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

  /// garnet相对路径:Server:ClusterConfig:GetEndpointFromNodeId
  #[inline]
  pub fn get_endpoint_from_node_id(&self, nodeid: &str) -> Option<SocketAddr> {
    self
      .worker_by_node_id(nodeid)
      .and_then(|(_, w)| Some(SocketAddr::new(w.address.parse().ok()?, w.port as u16)))
  }
}

impl ClusterConfig {
  /// garnet相对路径:Server:ClusterConfig:GetReplicaIds
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

  /// garnet相对路径:Server:ClusterConfig:GetReplicaEndpoints
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

  /// garnet相对路径:Server:ClusterConfig:GetWorkerAddress
  #[inline]
  pub fn get_worker_address(&self, worker_id: u16) -> (String, i32) {
    let w = &self.workers[worker_id as usize];
    (w.address.clone(), w.port)
  }

  /// garnet相对路径:Server:ClusterConfig:GetWorkerInfoForGossip
  pub fn get_worker_info_for_gossip(&self) -> Vec<(String, String, i32)> {
    let mut result = Vec::new();
    for worker in self.workers.iter().skip(2) {
      if let Some(ref id) = worker.nodeid {
        result.push((id.clone(), worker.address.clone(), worker.port));
      }
    }
    result
  }

  /// garnet相对路径:Server:ClusterConfig:GetSlotCountForState
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

  /// garnet相对路径:Server:ClusterConfig:GetPrimaryCount
  pub fn get_primary_count(&self) -> usize {
    self.workers[1..]
      .iter()
      .filter(|w| w.role == NodeRole::Primary)
      .count()
  }

  /// garnet相对路径:Server:ClusterConfig:GetWorkerNodeIdFromAddress
  pub fn get_worker_node_id_from_address(&self, address: &str, port: i32) -> Option<String> {
    self.workers[1..]
      .iter()
      .find(|w| w.address == address && w.port == port)
      .and_then(|w| w.nodeid.clone())
  }

  /// garnet相对路径:Server:ClusterConfig:GetWorkerNodeIdFromAddressOrHostname
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

  /// garnet相对路径:Server:ClusterConfig:LazyUpdateLocalReplicationOffset
  pub fn lazy_update_local_replication_offset(&mut self, offset: i64) {
    self.workers[LOCAL_WORKER_ID].replication_offset = offset;
  }
}

impl ClusterConfig {
  /// garnet相对路径:Server:ClusterConfig:RemoveWorker
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

  /// garnet相对路径:Server:ClusterConfig:MakeReplicaOf
  pub fn make_replica_of(&mut self, nodeid: Option<&str>) -> &mut Self {
    let w = &mut self.workers[LOCAL_WORKER_ID];
    w.replica_of_node_id = nodeid.map(String::from);
    w.role = NodeRole::Replica;
    self
  }

  /// garnet相对路径:Server:ClusterConfig:SetLocalWorkerRole
  pub fn set_local_worker_role(&mut self, role: NodeRole) -> &mut Self {
    self.workers[LOCAL_WORKER_ID].role = role;
    self
  }

  /// garnet相对路径:Server:ClusterConfig:TakeOverFromPrimary
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

  /// garnet相对路径:Server:ClusterConfig:TryAddSlots
  ///
  /// 先整体校验再占位：与 C# 的"新配置上试错"等价的 all-or-nothing 语义，
  /// 但无需整份克隆
  pub fn try_add_slots(&mut self, slots: Option<&HashSet<usize>>, state: SlotState) -> Result<()> {
    let Some(s) = slots else {
      return Ok(());
    };
    for &slot in s {
      if self.slot_map[slot].eff_worker_id() != 0 {
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

  /// garnet相对路径:Server:ClusterConfig:AssignSlots
  pub fn assign_slots(&mut self, slots: &[usize], worker_id: u16, state: SlotState) -> &mut Self {
    for &slot in slots {
      let e = &mut self.slot_map[slot];
      e.worker_id = worker_id;
      e.state = state;
    }
    self
  }

  /// garnet相对路径:Server:ClusterConfig:TryRemoveSlots
  pub fn try_remove_slots(&mut self, slots: Option<&HashSet<usize>>) -> Result<()> {
    let Some(s) = slots else {
      return Ok(());
    };
    for &slot in s {
      if self.slot_map[slot].eff_worker_id() == 0 {
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

  /// garnet相对路径:Server:ClusterConfig:UpdateSlotState
  pub fn update_slot_state(&mut self, slot: usize, worker_id: u16, state: SlotState) -> &mut Self {
    let e = &mut self.slot_map[slot];
    e.worker_id = worker_id;
    e.state = state;
    self
  }

  /// garnet相对路径:Server:ClusterConfig:UpdateMultiSlotState
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

  /// garnet相对路径:Server:ClusterConfig:ResetMultiSlotState
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

  /// garnet相对路径:Server:ClusterConfig:SetLocalWorkerConfigEpoch
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

  /// garnet相对路径:Server:ClusterConfig:BumpLocalNodeConfigEpoch
  pub fn bump_local_node_config_epoch(&mut self) -> &mut Self {
    let mx = self.get_max_config_epoch();
    self.workers[LOCAL_WORKER_ID].config_epoch = mx + 1;
    self
  }
}

impl ClusterConfig {
  /// garnet相对路径:Server:ClusterConfig:MergeWorkerInfo
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

  /// garnet相对路径:Server:ClusterConfig:MergeSlotMap
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

      // 与 C# 一致取 eff id：本地 Migrating 槽的当前归属按 LOCAL(1) 判定，
      // 迁移目标节点 gossip 认领时走 epoch 比较直接移交，而非误判为
      // "目标已是属主"把槽重置为 Offline 造成短暂失主
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

  /// garnet相对路径:Server:ClusterConfig:Merge
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

  /// garnet相对路径:Server:ClusterConfig:HandleConfigEpochCollision
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
  /// garnet相对路径:Server:ClusterConfig:GetClusterInfo
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

  /// garnet相对路径:Server:ClusterConfig:GetNodeInfo
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
      let _worker_id = self.slot_map[slot].worker_id as usize;
      let _state = self.slot_map[slot].state;

      if _state == SlotState::Stable {
        continue;
      }
      if _worker_id > self.num_workers() {
        continue;
      }

      if let Some(ref node_id) = self.workers[_worker_id].nodeid {
        match _state {
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
  /// garnet相对路径:Server:ClusterConfig:GetShardRanges
  ///
  /// 单遍扫描输出连续槽区间；哨兵扫描自然闭合末区间
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

  /// garnet相对路径:Server:ClusterConfig:GetWorkerReplicas
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

  /// garnet相对路径:Server:ClusterConfig:GetAllNodeIds
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

  /// garnet相对路径:Server:ClusterConfig:GetNodeIdsForShard
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

  /// garnet相对路径:Server:ClusterConfig:GetSlotList
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
  /// garnet相对路径:Server:ClusterConfig:GetShardsInfo
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
  /// garnet相对路径:Server:ClusterConfig:GetSlotsInfo
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
        // 与 C# 一致按 eff id 分段：Migrating 槽归源节点（LOCAL）名下，
        // CLUSTER SLOTS 不把它误报到迁移目标
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
  /// garnet相对路径:Server:ClusterConfig:TryPeekVersion
  ///
  /// 全量解码前快速校验版本号（gossip 接收端先用它拒绝异版本节点）
  #[inline]
  pub fn try_peek_version(data: &[u8]) -> Option<u8> {
    data.first().copied()
  }

  /// garnet相对路径:Server:ClusterConfig:ToByteArray
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

  /// garnet相对路径:Server:ClusterConfig:FromByteArray
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
    let mut offset = 0usize;
    for seg in &wire.segments {
      let state = SlotState::from_repr(seg.state).ok_or(Error::SlotState(seg.state))?;
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

    // 0 号保留位不在线格式内，按 default 重建（对应 C# skip(1) 布局）
    let mut workers = vec![Worker::default(); wire.workers.len() + 1];
    workers[1..].clone_from_slice(&wire.workers);

    Ok(Self { slot_map, workers })
  }
}

#[cfg(test)]
mod tests {
  use super::*;
  use crate::server::{connection_info::ConnectionInfo, worker::LocalWorkerSpec};

  /// 仅保留位的空配置
  fn empty() -> ClusterConfig {
    ClusterConfig::new()
  }

  /// 初始化本地 worker 的配置
  fn local(id: &str, epoch: i64, role: NodeRole) -> ClusterConfig {
    let mut c = empty();
    c.initialize_local_worker(LocalWorkerSpec {
      node_id: id,
      address: "127.0.0.1",
      port: 7000,
      config_epoch: epoch,
      role,
      replica_of_node_id: None,
      hostname: None,
    });
    c
  }

  /// 追加一个远端 worker
  fn remote(config: &mut ClusterConfig, id: &str, role: NodeRole, replica_of: Option<&str>) -> u16 {
    config.workers.push(Worker {
      nodeid: Some(id.to_string()),
      address: "10.0.0.1".to_string(),
      port: 7000,
      config_epoch: 0,
      role,
      replica_of_node_id: replica_of.map(String::from),
      replication_offset: 0,
      hostname: None,
    });
    (config.workers.len() - 1) as u16
  }

  #[test]
  fn initialize_local_worker_sets_identity() {
    let mut c = empty();
    c.initialize_local_worker(LocalWorkerSpec {
      node_id: "n1",
      address: "1.2.3.4",
      port: 7000,
      config_epoch: 7,
      role: NodeRole::Primary,
      replica_of_node_id: None,
      hostname: Some("h1"),
    });
    assert_eq!(c.local_node_id(), Some("n1"));
    assert_eq!((c.local_node_ip(), c.local_node_port()), ("1.2.3.4", 7000));
    assert_eq!(c.local_node_config_epoch(), 7);
    assert!(c.is_primary() && !c.is_replica());
    assert_eq!(c.local_node_id_short(), "n1");
    // 保留位保持 unassigned
    assert!(c.workers[RESERVED_WORKER_ID].nodeid.is_none());
  }

  #[test]
  fn local_node_id_short_handles_short_and_multibyte() {
    let mut c = empty();
    c.workers[LOCAL_WORKER_ID].nodeid = Some("abc".to_string());
    assert_eq!(c.local_node_id_short(), "abc");
    // 非 ASCII：字节 8 恰为边界时按字节截断
    c.workers[LOCAL_WORKER_ID].nodeid = Some("汉汉AB汉汉汉汉".to_string());
    assert_eq!(c.local_node_id_short(), "汉汉AB");
    // 截断点落在多字节字符中间：get 返回 None，整体返回而不 panic
    c.workers[LOCAL_WORKER_ID].nodeid = Some("汉字汉字汉字汉字汉字".to_string());
    assert_eq!(c.local_node_id_short(), "汉字汉字汉字汉字汉字");
    c.workers[LOCAL_WORKER_ID].nodeid = None;
    assert_eq!(c.local_node_id_short(), "");
  }

  #[test]
  fn slot_assign_all_or_nothing() {
    let mut c = local("n1", 0, NodeRole::Primary);
    let slots: gxhash::HashSet<usize> = [1usize, 2].into_iter().collect();
    c.try_add_slots(Some(&slots), SlotState::Stable).unwrap();
    assert_eq!(c.get_worker_id_from_slot(1), LOCAL_WORKER_ID);
    assert_eq!(c.get_state(2), SlotState::Stable);
    // 已占用槽 → 报错且不留下半占状态
    let bad: gxhash::HashSet<usize> = [2usize, 99].into_iter().collect();
    assert!(matches!(
      c.try_add_slots(Some(&bad), SlotState::Stable),
      Err(Error::SlotNotFree(2))
    ));
    assert_eq!(c.get_worker_id_from_slot(99), 0);
    // 移除后槽位回 Offline
    c.try_remove_slots(Some(&slots)).unwrap();
    assert_eq!(c.get_state(1), SlotState::Offline);
  }

  #[test]
  fn set_config_epoch_only_from_zero_then_bump() {
    let mut c = local("n1", 0, NodeRole::Primary);
    // 非 0 初始化被拒
    assert!(!c.set_local_worker_config_epoch(0));
    assert!(c.set_local_worker_config_epoch(5));
    assert_eq!(c.local_node_config_epoch(), 5);
    // 非 0 epoch 不可覆写，单调递增只能 bump
    assert!(!c.set_local_worker_config_epoch(9));
    // bump 取全员最大 +1
    remote(&mut c, "n2", NodeRole::Primary, None);
    c.workers[2].config_epoch = 10;
    c.bump_local_node_config_epoch();
    assert_eq!(c.local_node_config_epoch(), 11);
  }

  #[test]
  fn epoch_collision_bumps_only_for_greater_sender() {
    let mut c = local("bbb", 5, NodeRole::Primary);
    let lesser = local("aaa", 5, NodeRole::Primary);
    assert!(!c.handle_config_epoch_collision(&lesser));
    assert_eq!(c.local_node_config_epoch(), 5);
    let greater = local("ccc", 5, NodeRole::Primary);
    assert!(c.handle_config_epoch_collision(&greater));
    assert_eq!(c.local_node_config_epoch(), 6);
    // epoch 不同则互不理会
    let other = local("ccc", 9, NodeRole::Primary);
    assert!(!c.handle_config_epoch_collision(&other));
  }

  #[test]
  fn merge_adopts_worker_and_claims_slot_by_epoch() {
    let mut c = local("n1", 1, NodeRole::Primary);
    let n2 = remote(&mut c, "n2", NodeRole::Primary, None);
    c.assign_slots(&[10, 11], LOCAL_WORKER_ID as u16, SlotState::Stable);

    // sender n2 自称 epoch 5 并认领槽 10
    let mut s = local("n2", 5, NodeRole::Primary);
    s.assign_slots(&[10], LOCAL_WORKER_ID as u16, SlotState::Stable);

    let merged = c.merge(&s, &gxhash::HashMap::default()).unwrap();
    // worker 元数据随 gossip 更新
    assert_eq!(merged.workers[n2 as usize].config_epoch, 5);
    // 槽 10 因 sender epoch 更高移交 sender
    assert_eq!(merged.get_worker_id_from_slot(10), n2 as usize);
    assert_eq!(merged.get_state(10), SlotState::Stable);
    // 槽 11 未被 sender 稳定持有，仍属本地
    assert_eq!(merged.get_worker_id_from_slot(11), LOCAL_WORKER_ID);
  }

  #[test]
  fn merge_rejects_stale_claim_and_noop_sender() {
    let mut c = local("n1", 9, NodeRole::Primary);
    let n2 = remote(&mut c, "n2", NodeRole::Primary, None);
    c.assign_slots(&[3], n2, SlotState::Stable);

    // sender n2 epoch 更低仍认领自家槽 → epoch 检查挡下，属主不变
    let mut s = local("n2", 2, NodeRole::Primary);
    s.assign_slots(&[3], LOCAL_WORKER_ID as u16, SlotState::Stable);
    let merged = c.merge(&s, &gxhash::HashMap::default()).unwrap();
    assert_eq!(merged.get_worker_id_from_slot(3), n2 as usize);
    assert_eq!(merged.get_state(3), SlotState::Stable);

    // 完全一致的槽位图且 worker epoch 不增长 → 无变化返回 None
    let mut same = local("n2", 0, NodeRole::Primary);
    same.assign_slots(&[3], LOCAL_WORKER_ID as u16, SlotState::Stable);
    assert!(c.merge(&same, &gxhash::HashMap::default()).is_none());
  }

  #[test]
  fn merge_bans_listed_node() {
    let c = local("n1", 3, NodeRole::Primary);
    let s = local("evil", 9, NodeRole::Primary);
    let mut ban = gxhash::HashMap::default();
    ban.insert("evil".to_string(), 1);
    assert!(c.merge(&s, &ban).is_none());
  }

  /// 迁移完成路径：本地持 Migrating 槽（raw=目标），目标节点以更高 epoch
  /// gossip 认领时应直接移交而非重置 Offline（eff-id 语义回归测试）
  #[test]
  fn merge_hands_migrating_slot_to_claiming_target() {
    let mut c = local("n1", 3, NodeRole::Primary);
    let n2 = remote(&mut c, "n2", NodeRole::Primary, None);
    c.update_slot_state(5, n2, SlotState::Migrating);

    let mut s = local("n2", 5, NodeRole::Primary);
    s.assign_slots(&[5], LOCAL_WORKER_ID as u16, SlotState::Stable);

    let merged = c.merge(&s, &gxhash::HashMap::default()).unwrap();
    assert_eq!(merged.get_state(5), SlotState::Stable);
    assert_eq!(merged.get_worker_id_from_slot(5), n2 as usize);
  }

  #[test]
  fn remove_worker_frees_slots_and_shifts_ids() {
    let mut c = local("n1", 3, NodeRole::Primary);
    let n2 = remote(&mut c, "n2", NodeRole::Primary, None);
    let n3 = remote(&mut c, "n3", NodeRole::Primary, None);
    c.assign_slots(&[1], n2, SlotState::Stable);
    c.assign_slots(&[2], n3, SlotState::Stable);
    // n2 迁出槽 3 给 n3，n3 迁入槽 1 来自 n2
    c.update_slot_state(3, n3, SlotState::Migrating);
    c.update_slot_state(4, n2, SlotState::Importing);

    let after = c.remove_worker("n2");
    // n2 的 Stable 槽释放为 Offline
    assert_eq!(after.get_state(1), SlotState::Offline);
    assert_eq!(after.get_worker_id_from_slot(1), RESERVED_WORKER_ID);
    // n3 相关槽位 id 前移一位
    assert_eq!(after.get_worker_id_from_slot(2), n2 as usize);
    assert_eq!(after.slot_map[3].worker_id, n2);
    assert_eq!(after.slot_map[3].state, SlotState::Migrating);
    // Importing 槽（源为 n2）释放
    assert_eq!(after.get_state(4), SlotState::Offline);
    // workers 收缩且 n3 前移
    assert_eq!(after.workers.len(), c.workers.len() - 1);
    assert_eq!(after.workers[n2 as usize].nodeid.as_deref(), Some("n3"));
    // 未知名原样返回
    let untouched = c.remove_worker("ghost");
    assert_eq!(untouched.workers.len(), c.workers.len());
    assert_eq!(untouched.slot_map[1].state, SlotState::Stable);
  }

  #[test]
  fn takeover_moves_primary_slots_to_replica() {
    // 副本接管：主 n1 的槽全部转为本地下
    let mut primary = local("n1", 3, NodeRole::Primary);
    primary.assign_slots(&[7, 8], LOCAL_WORKER_ID as u16, SlotState::Stable);
    let mut replica = local("n2", 0, NodeRole::Replica);
    replica.workers[LOCAL_WORKER_ID].replica_of_node_id = Some("n1".to_string());
    let n1 = remote(&mut replica, "n1", NodeRole::Primary, None);
    // 副本视角下槽位归属主节点（远端 worker 下标），并感知主的 epoch
    replica.workers[n1 as usize].config_epoch = 3;
    replica.assign_slots(&[7, 8], n1, SlotState::Stable);

    assert!(replica.is_replica());
    replica
      .take_over_from_primary()
      .bump_local_node_config_epoch();
    assert!(replica.is_primary());
    assert_eq!(replica.local_node_primary_id(), None);
    assert_eq!(replica.get_worker_id_from_slot(7), LOCAL_WORKER_ID);
    assert_eq!(replica.get_state(8), SlotState::Stable);
    assert!(replica.local_node_config_epoch() > 3);
  }

  #[test]
  fn replica_read_path_sees_primary_slots() {
    let mut primary = local("n1", 3, NodeRole::Primary);
    primary.assign_slots(&[9], LOCAL_WORKER_ID as u16, SlotState::Stable);
    let mut replica = local("n2", 0, NodeRole::Replica);
    replica.workers[LOCAL_WORKER_ID].replica_of_node_id = Some("n1".to_string());
    remote(&mut replica, "n1", NodeRole::Primary, None);
    replica.update_slot_state(9, 2, SlotState::Stable);

    // 读写会话允许读主的槽；只读会话不允许
    assert!(replica.is_local(9, true));
    assert!(!replica.is_local(9, false));
    // 主自身恒 local
    assert!(primary.is_local(9, true));
    // 非属主且非副本读路径
    assert!(!replica.is_local(10, true));
  }

  #[test]
  fn config_wire_round_trip() {
    let mut c = local("n1", 3, NodeRole::Primary);
    c.workers[LOCAL_WORKER_ID].hostname = Some("host-a".to_string());
    c.workers[LOCAL_WORKER_ID].replication_offset = 42;
    let n2 = remote(&mut c, "n2", NodeRole::Replica, Some("n1"));
    c.workers[n2 as usize].config_epoch = 1;
    c.assign_slots(&[0, 1, 2], LOCAL_WORKER_ID as u16, SlotState::Stable);
    c.update_slot_state(3, n2, SlotState::Migrating);
    c.update_slot_state(4, n2, SlotState::Importing);

    let bytes = c.to_byte_array();
    assert_eq!(
      ClusterConfig::try_peek_version(&bytes),
      Some(CLUSTER_CONFIG_VERSION)
    );
    let back = ClusterConfig::from_byte_array(&bytes).unwrap();
    assert_eq!(back.slot_map.len(), MAX_HASH_SLOT_VALUE);
    for (a, b) in c.slot_map.iter().zip(back.slot_map.iter()) {
      assert_eq!(a.worker_id, b.worker_id);
      assert_eq!(a.state, b.state);
    }
    assert_eq!(back.workers.len(), c.workers.len());
    assert_eq!(back.workers[RESERVED_WORKER_ID].nodeid, None);
    for (a, b) in c.workers.iter().skip(1).zip(back.workers.iter().skip(1)) {
      assert_eq!(a.nodeid, b.nodeid);
      assert_eq!(a.address, b.address);
      assert_eq!(a.port, b.port);
      assert_eq!(a.config_epoch, b.config_epoch);
      assert_eq!(a.role, b.role);
      assert_eq!(a.replica_of_node_id, b.replica_of_node_id);
      assert_eq!(a.hostname, b.hostname);
    }
    // RLE 有效：纯稳定图远小于整图展开
    let mut flat = empty();
    flat.assign_slots(
      &(0..MAX_HASH_SLOT_VALUE).collect::<Vec<_>>(),
      LOCAL_WORKER_ID as u16,
      SlotState::Stable,
    );
    assert!(flat.to_byte_array().len() < 64);
  }

  #[test]
  fn config_wire_rejects_bad_payload() {
    assert!(matches!(
      ClusterConfig::from_byte_array(&[]),
      Err(Error::PayloadTooShort)
    ));
    assert!(matches!(
      ClusterConfig::from_byte_array(&[CLUSTER_CONFIG_VERSION.wrapping_sub(1), 0]),
      Err(Error::Version { got: 1, expect: 2 })
    ));
    // 版本对但载荷损坏 → Codec 而非静默成功
    assert!(matches!(
      ClusterConfig::from_byte_array(&[CLUSTER_CONFIG_VERSION, 0xff, 0xff]),
      Err(Error::Codec(_))
    ));
    // 空 workers 列表：结构损坏（缺本地 worker 位）→ 解码期 fail-loud
    let mut payload = vec![CLUSTER_CONFIG_VERSION];
    payload.extend_from_slice(&bitcode::encode(&ConfigWire {
      segments: vec![],
      workers: vec![],
    }));
    assert!(matches!(
      ClusterConfig::from_byte_array(&payload),
      Err(Error::MissingWorkers)
    ));
    // 越界 RLE：覆盖超 16384 槽（携带合法 worker，单独验证槽位段校验）
    let mut payload = vec![CLUSTER_CONFIG_VERSION];
    payload.extend_from_slice(&bitcode::encode(&ConfigWire {
      segments: vec![SlotSegmentWire {
        count: u16::MAX,
        worker_id: 1,
        state: SlotState::Stable as u8,
      }],
      workers: vec![Worker::default()],
    }));
    assert!(matches!(
      ClusterConfig::from_byte_array(&payload),
      Err(Error::SlotOverflow)
    ));
    // 非法状态字节（携带合法 worker，单独验证状态字节校验）
    let mut payload = vec![CLUSTER_CONFIG_VERSION];
    payload.extend_from_slice(&bitcode::encode(&ConfigWire {
      segments: vec![SlotSegmentWire {
        count: 1,
        worker_id: 1,
        state: 0xee,
      }],
      workers: vec![Worker::default()],
    }));
    assert!(matches!(
      ClusterConfig::from_byte_array(&payload),
      Err(Error::SlotState(0xee))
    ));
  }

  #[test]
  fn cluster_info_reports_slot_groupings() {
    let mut c = local("n1", 3, NodeRole::Primary);
    let n2 = remote(&mut c, "n2", NodeRole::Replica, Some("n1"));
    c.assign_slots(&[0, 1, 2], LOCAL_WORKER_ID as u16, SlotState::Stable);
    c.update_slot_state(3, n2, SlotState::Migrating);
    // 迁移槽经 eff id 解析仍归属源节点
    assert_eq!(c.get_node_id_from_slot(3).as_deref(), Some("n1"));
    // raw 属主是迁移目标
    assert_eq!(c.slot_map[3].worker_id, n2);

    let counts = c.slot_state_counts();
    assert_eq!(counts[SlotState::Stable as usize], 3);
    assert_eq!(counts[SlotState::Migrating as usize], 1);
    assert_eq!(counts[SlotState::Offline as usize], MAX_HASH_SLOT_VALUE - 4);

    // CLUSTER NODES：Migrating 槽归源节点名下，且带 [slot->-target] 标注
    let node_info = c.get_node_info(LOCAL_WORKER_ID, &ConnectionInfo::default());
    assert!(node_info.contains("0-3"));
    assert!(node_info.contains("[3->-n2]"));
    assert!(node_info.contains("myself,master"));

    // CLUSTER SLOTS：Migrating 槽按 eff id 报在源节点，且副本行在列
    let slots = c.get_slots_info(ClusterPreferredEndpointType::Ip);
    assert!(slots.contains("*4\r\n:0\r\n:3\r\n"));
    assert!(slots.contains("n2"), "副本节点行必须出现在 CLUSTER SLOTS");

    // CLUSTER SHARDS：槽范围含迁移槽
    let shards = c.get_shards_info(None, ClusterPreferredEndpointType::Ip);
    assert!(shards.contains(":0\r\n:3"));
  }

  #[test]
  fn shard_ranges_and_replica_queries() {
    let mut c = local("n1", 3, NodeRole::Primary);
    let n2 = remote(&mut c, "n2", NodeRole::Replica, Some("n1"));
    c.assign_slots(&[5, 6], LOCAL_WORKER_ID as u16, SlotState::Stable);
    c.assign_slots(&[10], LOCAL_WORKER_ID as u16, SlotState::Stable);

    assert_eq!(c.get_shard_ranges(LOCAL_WORKER_ID), vec![(5, 6), (10, 10)]);
    assert!(c.has_assigned_slots(LOCAL_WORKER_ID as u16));
    assert!(!c.has_assigned_slots(16383));
    assert_eq!(c.get_replica_ids("n1"), vec!["n2".to_string()]);
    assert_eq!(c.get_local_node_replica_ids(), vec!["n2".to_string()]);
    // 副本 id 对应的 worker 即追加的 n2
    assert_eq!(c.get_node_role_from_node_id("n2"), NodeRole::Replica);
    assert_eq!(
      c.get_worker_id_from_node_id("n2"),
      n2,
      "副本 id 解析回其 worker 下标"
    );
    assert_eq!(c.get_slot_list(LOCAL_WORKER_ID as u16), vec![5, 6, 10]);
    assert_eq!(c.get_primary_count(), 1);
    assert_eq!(c.num_workers(), 2);
    assert!(c.is_known("N2"), "节点 id 比较大小写不敏感");
    assert_eq!(
      c.get_endpoint_from_node_id("n2").unwrap().to_string(),
      "10.0.0.1:7000"
    );
  }
}
