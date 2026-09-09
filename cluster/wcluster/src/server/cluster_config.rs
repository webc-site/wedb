use std::{array::from_fn, cmp::Ordering, net::SocketAddr};

// if needed
use log::warn;

use crate::server::{
  hash_slot::{HashSlot, SlotState},
  worker::{NodeRole, Worker},
};

pub const RESERVED_WORKER_ID: usize = 0;
pub const LOCAL_WORKER_ID: usize = 1;
pub const MIN_HASH_SLOT_VALUE: usize = 0;
pub const MAX_HASH_SLOT_VALUE: usize = 16384;
pub const CLUSTER_CONFIG_VERSION: u8 = 1;

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
  fn initialize_unassigned_worker(&mut self) {
    self.workers[RESERVED_WORKER_ID].nodeid = None;
    self.workers[RESERVED_WORKER_ID].address = String::new();
    self.workers[RESERVED_WORKER_ID].port = 0;
    self.workers[RESERVED_WORKER_ID].config_epoch = 0;
    self.workers[RESERVED_WORKER_ID].role = NodeRole::Unassigned;
    self.workers[RESERVED_WORKER_ID].replica_of_node_id = None;
    self.workers[RESERVED_WORKER_ID].replication_offset = 0;
    self.workers[RESERVED_WORKER_ID].hostname = None;
  }

  /// garnet相对路径:Server:ClusterConfig:InitializeLocalWorker
  #[allow(clippy::too_many_arguments)]
  pub fn initialize_local_worker(
    &self,
    node_id: &str,
    address: &str,
    port: i32,
    config_epoch: i64,
    role: NodeRole,
    replica_of_node_id: Option<&str>,
    hostname: Option<&str>,
  ) -> Self {
    let mut new_config = self.clone();
    new_config.workers[LOCAL_WORKER_ID].address = address.to_string();
    new_config.workers[LOCAL_WORKER_ID].port = port;
    new_config.workers[LOCAL_WORKER_ID].nodeid = Some(node_id.to_string());
    new_config.workers[LOCAL_WORKER_ID].config_epoch = config_epoch;
    new_config.workers[LOCAL_WORKER_ID].role = role;
    new_config.workers[LOCAL_WORKER_ID].replica_of_node_id = replica_of_node_id.map(String::from);
    new_config.workers[LOCAL_WORKER_ID].replication_offset = 0;
    new_config.workers[LOCAL_WORKER_ID].hostname = hostname.map(String::from);
    new_config
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
    self.workers[1..=self.num_workers()].iter().any(|w| {
      w.nodeid
        .as_deref()
        .is_some_and(|id| id.eq_ignore_ascii_case(nodeid))
    })
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
    if let Some(id) = &self.workers[LOCAL_WORKER_ID].nodeid {
      if id.len() >= 8 {
        id[0..8].to_string()
      } else {
        id.to_string()
      }
    } else {
      "".to_string()
    }
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
    let mut replicas = Vec::new();
    let local_id = self.local_node_id();
    for worker in self.workers.iter().skip(2) {
      if let Some(ref replica_of) = worker.replica_of_node_id
        && let Some(id) = local_id
        && replica_of.eq_ignore_ascii_case(id)
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
      if let Some(node_id) = &worker.nodeid {
        if worker.role == NodeRole::Primary
          && !node_id.eq_ignore_ascii_case(my_primary_id)
          && let Ok(ip) = worker.address.parse()
        {
          primaries.push(SocketAddr::new(ip, worker.port as u16));
        }
        if node_id.eq_ignore_ascii_case(my_primary_id)
          && let Ok(ip) = worker.address.parse()
        {
          first = Some(SocketAddr::new(ip, worker.port as u16));
        }
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
  pub fn get_max_config_epoch(&self) -> i64 {
    self.workers[1..=self.num_workers()]
      .iter()
      .map(|w| w.config_epoch)
      .max()
      .unwrap_or(0)
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

  /// garnet相对路径:Server:ClusterConfig:GetWorkerIdFromNodeId
  pub fn get_worker_id_from_node_id(&self, node_id: &str) -> u16 {
    for (i, worker) in self
      .workers
      .iter()
      .enumerate()
      .take(self.num_workers() + 1)
      .skip(1)
    {
      if let Some(id) = &worker.nodeid
        && id.eq_ignore_ascii_case(node_id)
      {
        return i as u16;
      }
    }
    0
  }

  /// garnet相对路径:Server:ClusterConfig:GetNodeRoleFromNodeId
  #[inline]
  pub fn get_node_role_from_node_id(&self, node_id: &str) -> NodeRole {
    let wid = self.get_worker_id_from_node_id(node_id) as usize;
    if wid < self.workers.len() {
      self.workers[wid].role
    } else {
      NodeRole::Unassigned
    }
  }

  /// garnet相对路径:Server:ClusterConfig:GetWorkerFromNodeId
  pub fn get_worker_from_node_id(&self, node_id: &str) -> Option<&Worker> {
    let wid = self.get_worker_id_from_node_id(node_id) as usize;
    if wid > 0 && wid < self.workers.len() {
      Some(&self.workers[wid])
    } else {
      None
    }
  }

  /// garnet相对路径:Server:ClusterConfig:GetWorkerAddressFromNodeId
  pub fn get_worker_address_from_node_id(&self, node_id: &str) -> (Option<String>, i32) {
    let wid = self.get_worker_id_from_node_id(node_id) as usize;
    if wid == 0 || wid >= self.workers.len() {
      (None, -1)
    } else {
      (
        Some(self.workers[wid].address.clone()),
        self.workers[wid].port,
      )
    }
  }

  /// garnet相对路径:Server:ClusterConfig:GetHostNameFromNodeId
  pub fn get_host_name_from_node_id(&self, node_id: &str) -> Option<String> {
    let wid = self.get_worker_id_from_node_id(node_id) as usize;
    if wid == 0 || wid >= self.workers.len() {
      None
    } else {
      self.workers[wid].hostname.clone()
    }
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
    let wid = self.get_worker_id_from_node_id(nodeid) as usize;
    if wid > 0
      && wid < self.workers.len()
      && let Ok(ip) = self.workers[wid].address.parse()
    {
      return Some(SocketAddr::new(ip, self.workers[wid].port as u16));
    }
    None
  }
}

use gxhash::HashSet;

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
    let mut count = 0;
    for i in 0..MAX_HASH_SLOT_VALUE {
      if self.slot_map[i].state == state {
        count += 1;
      }
    }
    count
  }

  /// garnet相对路径:Server:ClusterConfig:GetPrimaryCount
  pub fn get_primary_count(&self) -> usize {
    self.workers[1..=self.num_workers()]
      .iter()
      .filter(|w| w.role == NodeRole::Primary)
      .count()
  }

  /// garnet相对路径:Server:ClusterConfig:GetWorkerNodeIdFromAddress
  pub fn get_worker_node_id_from_address(&self, address: &str, port: i32) -> Option<String> {
    self.workers[1..=self.num_workers()]
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
  pub fn remove_worker(&self, nodeid: &str) -> Self {
    let worker_id = self
      .workers
      .iter()
      .enumerate()
      .skip(1)
      .find(|(_, w)| {
        w.nodeid
          .as_deref()
          .is_some_and(|id| id.eq_ignore_ascii_case(nodeid))
      })
      .map(|(i, _)| i)
      .unwrap_or(0);

    let mut new_slot_map = self.slot_map.clone();
    for i in 0..MAX_HASH_SLOT_VALUE {
      let state = new_slot_map[i].state;
      let wid = new_slot_map[i].worker_id as usize;

      if state == SlotState::Stable && wid == worker_id {
        new_slot_map[i].worker_id = RESERVED_WORKER_ID as u16;
        new_slot_map[i].state = SlotState::Offline;
      } else if state == SlotState::Migrating && new_slot_map[i].worker_id as usize == worker_id {
        new_slot_map[i].worker_id = LOCAL_WORKER_ID as u16;
        new_slot_map[i].state = SlotState::Stable;
      } else if state == SlotState::Importing && wid < self.workers.len() {
        if let Some(ref nid) = self.workers[wid].nodeid
          && nid.eq_ignore_ascii_case(nodeid)
        {
          new_slot_map[i].worker_id = RESERVED_WORKER_ID as u16;
          new_slot_map[i].state = SlotState::Offline;
        }
      } else if wid > worker_id {
        new_slot_map[i].worker_id -= 1;
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
  pub fn make_replica_of(&self, nodeid: Option<&str>) -> Self {
    let mut new_config = self.clone();
    new_config.workers[LOCAL_WORKER_ID].replica_of_node_id = nodeid.map(|s| s.to_string());
    new_config.workers[LOCAL_WORKER_ID].role = NodeRole::Replica;
    new_config
  }

  /// garnet相对路径:Server:ClusterConfig:SetLocalWorkerRole
  pub fn set_local_worker_role(&self, role: NodeRole) -> Self {
    let mut new_config = self.clone();
    new_config.workers[LOCAL_WORKER_ID].role = role;
    new_config
  }

  /// garnet相对路径:Server:ClusterConfig:TakeOverFromPrimary
  pub fn take_over_from_primary(&self) -> Self {
    let mut new_config = self.clone();
    new_config.workers[LOCAL_WORKER_ID].role = NodeRole::Primary;
    new_config.workers[LOCAL_WORKER_ID].replica_of_node_id = None;

    let slots = self.get_local_primary_slots();
    for slot in slots {
      new_config.slot_map[slot].worker_id = LOCAL_WORKER_ID as u16;
      new_config.slot_map[slot].state = SlotState::Stable;
    }
    new_config
  }

  /// garnet相对路径:Server:ClusterConfig:TryAddSlots
  pub fn try_add_slots(
    &self,
    slots: Option<&HashSet<usize>>,
    state: SlotState,
  ) -> Result<Self, usize> {
    let mut new_config = self.clone();
    if let Some(s) = slots {
      for &slot in s {
        if new_config.slot_map[slot].eff_worker_id() != 0 {
          return Err(slot);
        }
        new_config.slot_map[slot].worker_id = LOCAL_WORKER_ID as u16;
        new_config.slot_map[slot].state = state;
      }
    }
    Ok(new_config)
  }

  /// garnet相对路径:Server:ClusterConfig:AssignSlots
  pub fn assign_slots(&self, slots: &[usize], worker_id: u16, state: SlotState) -> Self {
    let mut new_config = self.clone();
    for &slot in slots {
      new_config.slot_map[slot].worker_id = worker_id;
      new_config.slot_map[slot].state = state;
    }
    new_config
  }

  /// garnet相对路径:Server:ClusterConfig:TryRemoveSlots
  pub fn try_remove_slots(&self, slots: Option<&HashSet<usize>>) -> Result<Self, usize> {
    let mut new_config = self.clone();
    if let Some(s) = slots {
      for &slot in s {
        if new_config.slot_map[slot].eff_worker_id() == 0 {
          return Err(slot);
        }
        new_config.slot_map[slot].worker_id = 0;
        new_config.slot_map[slot].state = SlotState::Offline;
      }
    }
    Ok(new_config)
  }

  /// garnet相对路径:Server:ClusterConfig:UpdateSlotState
  pub fn update_slot_state(&self, slot: usize, worker_id: u16, state: SlotState) -> Self {
    let mut new_config = self.clone();
    new_config.slot_map[slot].worker_id = worker_id;
    new_config.slot_map[slot].state = state;
    new_config
  }

  /// garnet相对路径:Server:ClusterConfig:UpdateMultiSlotState
  pub fn update_multi_slot_state(
    &self,
    slots: &HashSet<usize>,
    worker_id: u16,
    state: SlotState,
  ) -> Self {
    let mut new_config = self.clone();
    for &slot in slots {
      new_config.slot_map[slot].worker_id = worker_id;
      new_config.slot_map[slot].state = state;
    }
    new_config
  }

  /// garnet相对路径:Server:ClusterConfig:ResetMultiSlotState
  pub fn reset_multi_slot_state(&self, slots: &HashSet<usize>) -> Self {
    let mut new_config = self.clone();
    for &slot in slots {
      let st = self.get_state(slot as u16);
      let wid = if st == SlotState::Migrating {
        LOCAL_WORKER_ID as u16
      } else {
        self.get_worker_id_from_slot(slot as u16) as u16
      };
      new_config.slot_map[slot].worker_id = wid;
      new_config.slot_map[slot].state = SlotState::Stable;
    }
    new_config
  }

  /// garnet相对路径:Server:ClusterConfig:SetLocalWorkerConfigEpoch
  pub fn set_local_worker_config_epoch(&self, config_epoch: i64) -> Option<Self> {
    let mut new_config = self.clone();
    if self.workers[LOCAL_WORKER_ID].config_epoch == 0
      || self.workers[LOCAL_WORKER_ID].config_epoch < config_epoch
    {
      new_config.workers[LOCAL_WORKER_ID].config_epoch = config_epoch;
      Some(new_config)
    } else {
      None
    }
  }

  /// garnet相对路径:Server:ClusterConfig:BumpLocalNodeConfigEpoch
  pub fn bump_local_node_config_epoch(&self) -> Self {
    let mx = self.get_max_config_epoch();
    let mut new_config = self.clone();
    new_config.workers[LOCAL_WORKER_ID].config_epoch = mx + 1;
    new_config
  }
}

impl ClusterConfig {
  /// garnet相对路径:Server:ClusterConfig:MergeWorkerInfo
  fn merge_worker_info(&self, worker: &Worker) -> Self {
    let mut worker_id = RESERVED_WORKER_ID;
    for i in 1..self.workers.len() {
      if let Some(ref id) = self.workers[i].nodeid
        && let Some(ref wid) = worker.nodeid
        && id.eq_ignore_ascii_case(wid)
      {
        if worker.config_epoch <= self.workers[i].config_epoch {
          return self.clone();
        }
        worker_id = i;
        break;
      }
    }

    let mut new_config = self.clone();
    if worker_id == RESERVED_WORKER_ID {
      worker_id = new_config.workers.len();
      new_config.workers.push(Worker::default());
    }

    new_config.workers[worker_id].address = worker.address.clone();
    new_config.workers[worker_id].port = worker.port;
    new_config.workers[worker_id].nodeid = worker.nodeid.clone();
    new_config.workers[worker_id].config_epoch = worker.config_epoch;
    new_config.workers[worker_id].role = worker.role;
    new_config.workers[worker_id].replica_of_node_id = worker.replica_of_node_id.clone();
    new_config.workers[worker_id].hostname = worker.hostname.clone();

    new_config
  }

  /// garnet相对路径:Server:ClusterConfig:MergeSlotMap
  pub fn merge_slot_map(&self, sender_config: &ClusterConfig) -> Self {
    let mut updated = false;
    let sender_slot_map = &sender_config.slot_map;
    let mut assign_to_worker_id = if let Some(id) = sender_config.local_node_id() {
      self.get_worker_id_from_node_id(id)
    } else {
      0
    };

    let mut new_config = self.clone();

    for i in 0..MAX_HASH_SLOT_VALUE {
      let current_owner_id = new_config.slot_map[i].worker_id as usize;

      if sender_slot_map[i].state != SlotState::Stable {
        continue;
      }

      if sender_slot_map[i].worker_id as usize != LOCAL_WORKER_ID && sender_config.is_primary() {
        let current_owner_node_id = if current_owner_id < self.workers.len() {
          self.workers[current_owner_id].nodeid.clone()
        } else {
          None
        };

        if let Some(conid) = current_owner_node_id
          && let Some(sid) = sender_config.local_node_id()
          && conid.eq_ignore_ascii_case(sid)
        {
          new_config.slot_map[i].worker_id = RESERVED_WORKER_ID as u16;
          new_config.slot_map[i].state = SlotState::Offline;
          updated = true;
        }
        continue;
      }

      if sender_config.is_primary() {
        if sender_config.local_node_config_epoch() != 0
          && current_owner_id < self.workers.len()
          && self.workers[current_owner_id].config_epoch >= sender_config.local_node_config_epoch()
        {
          continue;
        }
      } else if current_owner_id != RESERVED_WORKER_ID {
        if current_owner_id < self.workers.len()
          && let Some(ref id) = self.workers[current_owner_id].nodeid
          && let Some(sid) = sender_config.local_node_id()
          && !id.eq(sid)
        {
          continue;
        }
        assign_to_worker_id = if let Some(pid) = sender_config.local_node_primary_id() {
          self.get_worker_id_from_node_id(pid)
        } else {
          0
        };
      }

      updated |= new_config.slot_map[i].worker_id != assign_to_worker_id
        || new_config.slot_map[i].state != SlotState::Stable;

      new_config.slot_map[i].worker_id = assign_to_worker_id;
      new_config.slot_map[i].state = SlotState::Stable;
    }

    if updated { new_config } else { self.clone() }
  }

  /// garnet相对路径:Server:ClusterConfig:Merge
  pub fn merge(
    &self,
    sender_config: &ClusterConfig,
    worker_ban_list: &gxhash::HashMap<String, i64>,
  ) -> Self {
    let local_id = self.local_node_id();
    let mut new_config = self.clone();

    for worker in &sender_config.workers[1..=sender_config.num_workers()] {
      if let Some(ref sid) = worker.nodeid {
        if let Some(lid) = local_id
          && lid.eq_ignore_ascii_case(sid)
        {
          continue;
        }
        if worker_ban_list.contains_key(sid) {
          continue;
        }
        new_config = new_config.merge_worker_info(worker);
      }
    }

    new_config.merge_slot_map(sender_config)
  }

  /// garnet相对路径:Server:ClusterConfig:HandleConfigEpochCollision
  pub fn handle_config_epoch_collision(&self, sender_config: &ClusterConfig) -> Self {
    let local_node_config_epoch = self.local_node_config_epoch();
    let sender_config_epoch = sender_config.local_node_config_epoch();

    if local_node_config_epoch != sender_config_epoch {
      return self.clone();
    }

    let sender_node_id = sender_config.local_node_id().unwrap_or("");
    let local_node_id = self.local_node_id().unwrap_or("");

    if sender_node_id.cmp(local_node_id) != Ordering::Greater {
      return self.clone();
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

    self.bump_local_node_config_epoch()
  }
}

use std::fmt::Write;

use crate::server::{cluster_provider::ClusterProvider, connection_info::ConnectionInfo};

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
      w.port + 10000
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
  pub fn get_shard_ranges(&self, worker_id: usize) -> Vec<(u16, u16)> {
    let mut ranges = Vec::new();
    let mut start_range = u16::MAX;
    for i in 0..=MAX_HASH_SLOT_VALUE {
      if i < self.slot_map.len() && self.slot_map[i].eff_worker_id() as usize == worker_id {
        if start_range == u16::MAX {
          start_range = i as u16;
        }
      } else if start_range != u16::MAX {
        ranges.push((start_range, (i - 1) as u16));
        start_range = u16::MAX;
      }
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
    let _ = write!(sb, "$40\r\n{}\r\n", nodeid);
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
        if self.slot_map[slot_end].state == SlotState::Offline
          || self.slot_map[slot_start].worker_id != self.slot_map[slot_end].worker_id
        {
          break;
        }
        slot_end += 1;
      }

      let curr_worker_id = self.slot_map[slot_start].worker_id as usize;
      let address = self.workers[curr_worker_id].address.clone();
      let port = self.workers[curr_worker_id].port;
      let nodeid = self.workers[curr_worker_id]
        .nodeid
        .clone()
        .unwrap_or_default();
      let hostname = self.workers[curr_worker_id].hostname.clone();
      let replicas = self.get_replica_ids(&nodeid);

      slot_end -= 1;
      self.append_formatted_slot_info(
        &mut slots_str,
        slot_start,
        slot_end,
        &address,
        port,
        &nodeid,
        hostname.as_deref(),
        &replicas,
        pref_type,
      );
      slot_ranges += 1;
      slot_start = slot_end + 1;
    }

    let _ = write!(sb, "*{}\r\n{}", slot_ranges, slots_str);
    sb
  }

  #[allow(clippy::too_many_arguments)]
  fn append_formatted_slot_info(
    &self,
    sb: &mut String,
    slot_start: usize,
    slot_end: usize,
    ip_address: &str,
    port: i32,
    nodeid: &str,
    hostname: Option<&str>,
    replica_ids: &[String],
    pref_type: ClusterPreferredEndpointType,
  ) {
    let count_a = if replica_ids.is_empty() {
      3
    } else {
      3 + replica_ids.len()
    };
    let _ = write!(sb, "*{}\r\n:{}\r\n:{}\r\n", count_a, slot_start, slot_end);

    self.append_node_networking_info(sb, ip_address, port, nodeid, hostname, pref_type);

    for replica_id in replica_ids {
      let (replica_ip_opt, replica_port) = self.get_worker_address_from_node_id(replica_id);
      let replica_ip = replica_ip_opt.unwrap_or_default();
      let replica_hostname = self.get_host_name_from_node_id(replica_id);
      self.append_node_networking_info(
        sb,
        &replica_ip,
        replica_port,
        replica_id,
        replica_hostname.as_deref(),
        pref_type,
      );
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

use std::io::{Cursor, Read};

use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};

impl ClusterConfig {
  /// garnet相对路径:Server:ClusterConfig:TryPeekVersion
  pub fn try_peek_version(data: &[u8]) -> Option<u8> {
    if data.is_empty() { None } else { Some(data[0]) }
  }

  /// garnet相对路径:Server:ClusterConfig:ToByteArray
  pub fn to_byte_array(&self) -> Vec<u8> {
    let mut ms = Vec::new();
    // Write serialization format version
    ms.write_u8(CLUSTER_CONFIG_VERSION).unwrap();

    self.serialize_slot_map(&mut ms);

    // Serialize worker info
    ms.write_i32::<LittleEndian>(self.workers.len() as i32)
      .unwrap();
    for worker in self.workers.iter().skip(1) {
      write_string(&mut ms, worker.nodeid.as_deref().unwrap_or(""));
      write_string(&mut ms, &worker.address);
      ms.write_i32::<LittleEndian>(worker.port).unwrap();
      ms.write_i64::<LittleEndian>(worker.config_epoch).unwrap();
      ms.write_u8(worker.role as u8).unwrap();

      if worker.replica_of_node_id.is_none() {
        ms.write_u8(0).unwrap();
      } else {
        ms.write_u8(1).unwrap();
        write_string(&mut ms, worker.replica_of_node_id.as_deref().unwrap());
      }

      ms.write_i64::<LittleEndian>(worker.replication_offset)
        .unwrap();

      if worker.hostname.is_none() {
        ms.write_u8(0).unwrap();
      } else {
        ms.write_u8(1).unwrap();
        write_string(&mut ms, worker.hostname.as_deref().unwrap());
      }
    }

    ms
  }

  fn serialize_slot_map(&self, ms: &mut Vec<u8>) {
    let segment_count_position = ms.len();
    ms.write_u16::<LittleEndian>(0).unwrap(); // placeholder

    let mut segment_count: u16 = 0;
    let mut count: u16 = 1;
    let mut worker_id = self.slot_map[0].worker_id;
    let mut state = self.slot_map[0].state as u8;

    for i in 1..self.slot_map.len() {
      let _state = self.slot_map[i].state as u8;

      if self.slot_map[i].worker_id != worker_id || _state != state {
        segment_count += 1;
        ms.write_u16::<LittleEndian>(count).unwrap();
        ms.write_u16::<LittleEndian>(worker_id).unwrap();
        ms.write_u8(state).unwrap();

        count = 1;
        worker_id = self.slot_map[i].worker_id;
        state = _state;
        continue;
      }
      count += 1;
    }

    segment_count += 1;
    ms.write_u16::<LittleEndian>(count).unwrap();
    ms.write_u16::<LittleEndian>(worker_id).unwrap();
    ms.write_u8(state).unwrap();

    let mut cursor = Cursor::new(ms);
    cursor.set_position(segment_count_position as u64);
    cursor.write_u16::<LittleEndian>(segment_count).unwrap();
  }

  /// garnet相对路径:Server:ClusterConfig:FromByteArray
  pub fn from_byte_array(other: &[u8]) -> Result<Self, &'static str> {
    let mut reader = Cursor::new(other);
    if other.is_empty() {
      return Err("Invalid ClusterConfig payload: too short to contain a version");
    }
    let version = reader.read_u8().unwrap();
    if version != CLUSTER_CONFIG_VERSION {
      return Err("Incompatible ClusterConfig version");
    }

    let new_slot_map = Self::deserialize_slot_map(&mut reader);
    let num_workers = reader.read_i32::<LittleEndian>().unwrap_or(0);
    let mut new_workers = vec![Worker::default(); num_workers as usize];

    for worker in new_workers.iter_mut().skip(1) {
      worker.nodeid = Some(read_string(&mut reader));
      worker.address = read_string(&mut reader);
      worker.port = reader.read_i32::<LittleEndian>().unwrap_or(0);
      worker.config_epoch = reader.read_i64::<LittleEndian>().unwrap_or(0);
      worker.role = NodeRole::from_repr(reader.read_u8().unwrap_or(0)).unwrap_or_default();

      let is_null = reader.read_u8().unwrap_or(0);
      if is_null > 0 {
        worker.replica_of_node_id = Some(read_string(&mut reader));
      }

      worker.replication_offset = reader.read_i64::<LittleEndian>().unwrap_or(0);

      let is_null = reader.read_u8().unwrap_or(0);
      if is_null > 0 {
        worker.hostname = Some(read_string(&mut reader));
      }
    }

    Ok(Self::with_data(new_slot_map, new_workers))
  }

  fn deserialize_slot_map(reader: &mut Cursor<&[u8]>) -> Box<[HashSlot; MAX_HASH_SLOT_VALUE]> {
    let mut new_slot_map = Box::new([HashSlot::default(); MAX_HASH_SLOT_VALUE]);
    let segment_count = reader.read_u16::<LittleEndian>().unwrap_or(0);
    let mut slot_offset = 0;

    for _ in 0..segment_count {
      let count = reader.read_u16::<LittleEndian>().unwrap_or(0);
      let worker_id = reader.read_u16::<LittleEndian>().unwrap_or(0);
      let state_byte = reader.read_u8().unwrap_or(0);
      let state = SlotState::from_repr(state_byte).unwrap_or(SlotState::Offline);

      let end = count as usize + slot_offset;
      while slot_offset < end {
        if slot_offset < MAX_HASH_SLOT_VALUE {
          new_slot_map[slot_offset].worker_id = worker_id;
          new_slot_map[slot_offset].state = state;
        }
        slot_offset += 1;
      }
    }
    new_slot_map
  }
}

fn write_string(writer: &mut Vec<u8>, s: &str) {
  let bytes = s.as_bytes();
  write_7bit_encoded_int(writer, bytes.len() as u32);
  writer.extend_from_slice(bytes);
}

fn read_string(reader: &mut Cursor<&[u8]>) -> String {
  let len = read_7bit_encoded_int(reader) as usize;
  let mut bytes = vec![0u8; len];
  let _ = reader.read_exact(&mut bytes);
  String::from_utf8(bytes).unwrap_or_default()
}

fn write_7bit_encoded_int(writer: &mut Vec<u8>, mut value: u32) {
  while value >= 0x80 {
    writer.write_u8((value as u8) | 0x80).unwrap();
    value >>= 7;
  }
  writer.write_u8(value as u8).unwrap();
}

fn read_7bit_encoded_int(reader: &mut Cursor<&[u8]>) -> u32 {
  let mut count = 0;
  let mut shift = 0;
  let mut b;
  loop {
    b = reader.read_u8().unwrap_or(0);
    count |= ((b & 0x7F) as u32) << shift;
    shift += 7;
    if (b & 0x80) == 0 {
      break;
    }
  }
  count
}
