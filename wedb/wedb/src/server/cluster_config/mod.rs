pub mod serializer;

use std::{array::from_fn, net::SocketAddr};

use hipstr::HipStr;
use log::warn;
pub use serializer::CLUSTER_CONFIG_VERSION;
use wbase::{
  hash_slot::CLUSTER_SLOT_COUNT,
  hex::hex_str_u128,
  map::{ConcurrentMap, HashSet},
};

// worker id 常量定义域在 [`crate::server::worker`]，此处转出口维持
// 槽位/配置方法群的单一引用路径
pub use crate::server::{
  cluster::ClusterPreferredEndpointType,
  worker::{LOCAL_WORKER_ID, RESERVED_WORKER_ID},
};
use crate::{
  error::{Error, Result},
  server::{
    hash_slot::{HashSlot, SLOT_STATE_KINDS, SlotState},
    worker::{LocalWorkerSpec, NodeRole, Worker},
  },
};

/// worker 宣告地址 + 端口换算为 `SocketAddr`；地址非法（含 0 号位 "unassigned"）
/// 返回 None。收敛本文件内 5 处 `address.parse().ok() → SocketAddr::new(ip, port as u16)`
/// 的地址解析与端口换算成对样板（语义逐字节等价：地址按 `IpAddr` 解析、端口 `as u16`）
fn socket_of(w: &Worker) -> Option<SocketAddr> {
  let ip = w.address.parse().ok()?;
  Some(SocketAddr::new(ip, w.port as u16))
}

/// libs/cluster/Server/ClusterConfig.cs
/// libs/cluster/Server/ClusterConfig.cs:Copy
///
/// C# `Copy()` 以两次 `Array.Copy` 逐元素深拷贝 slotMap/workers 后重建实例，
/// rust 侧同义承接为 `#[derive(Clone)]`（`slot_map: Box<[HashSlot; N]>` 与
/// `workers: Vec<Worker>` 均为深拷贝语义），活调用点
/// `failover_session.rs` `cm.current_config().clone()`，故不单设 `copy()` 件。
#[derive(Debug, Clone)]
pub struct ClusterConfig {
  pub slot_map: Box<[HashSlot; CLUSTER_SLOT_COUNT]>,
  pub workers: Vec<Worker>,
}

impl Default for ClusterConfig {
  fn default() -> Self {
    Self::new()
  }
}

impl ClusterConfig {
  /// libs/cluster/Server/ClusterConfig.cs:OutOfRange
  #[inline]
  pub fn out_of_range(slot: i64) -> bool {
    slot < 0 || slot >= CLUSTER_SLOT_COUNT as i64
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
    self.workers[RESERVED_WORKER_ID] = Worker::unassigned();
  }

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

  /// libs/cluster/Server/ClusterConfig.cs:IsKnown
  pub fn is_known(&self, nodeid: u128) -> bool {
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

  /// 本地节点宣告主机名（取自本地 worker 槽位 hostname 字段）
  pub fn local_node_hostname(&self) -> Option<&str> {
    self.workers[LOCAL_WORKER_ID].hostname.as_deref()
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
  pub fn local_node_id_short(&self) -> String {
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
}

impl ClusterConfig {
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

  /// 按节点 id 查找 worker（下标从 1 起，0 号保留位除外）。
  /// u128 单指令整数比较；节点数即 workers 数组长度（个位~百位），
  /// 哈希索引需在 merge/clone/remove 维护第二结构，线性扫描更简
  pub(crate) fn worker_by_node_id(&self, node_id: u128) -> Option<(usize, &Worker)> {
    self
      .workers
      .iter()
      .enumerate()
      .skip(1)
      .find(|(_idx, w)| w.nodeid == Some(node_id))
  }

  /// libs/cluster/Server/ClusterConfig.cs:GetWorkerIdFromNodeId
  pub fn get_worker_id_from_node_id(&self, node_id: u128) -> u16 {
    // 保留 _worker 形参以明确解构项含义，避免裸下划线
    self
      .worker_by_node_id(node_id)
      .map_or(0, |(i, _worker)| i as u16)
  }

  /// libs/cluster/Server/ClusterConfig.cs:GetNodeRoleFromNodeId
  #[inline]
  pub fn get_node_role_from_node_id(&self, node_id: u128) -> NodeRole {
    self
      .get_worker_from_node_id(node_id)
      .map_or(NodeRole::Unassigned, |w| w.role)
  }

  /// libs/cluster/Server/ClusterConfig.cs:GetWorkerFromNodeId
  pub fn get_worker_from_node_id(&self, node_id: u128) -> Option<&Worker> {
    self
      .workers
      .iter()
      .skip(1)
      .find(|w| w.nodeid == Some(node_id))
  }

  /// libs/cluster/Server/ClusterConfig.cs:GetWorkerAddressFromNodeId
  pub fn get_worker_address_from_node_id(&self, node_id: u128) -> (Option<String>, i32) {
    match self.get_worker_from_node_id(node_id) {
      Some(w) => (Some(w.address.to_string()), w.port),
      None => (None, -1),
    }
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
  pub fn get_node_id_from_slot(&self, slot: u16) -> Option<u128> {
    let wid = self.get_worker_id_from_slot(slot);
    self.workers.get(wid).and_then(|w| w.nodeid)
  }

  /// libs/cluster/Server/ClusterConfig.cs:GetOwnerIdFromSlot
  ///
  /// 对齐 C#（ClusterConfig.cs:463）取 `_workerId` raw：即便 Migrating 也报
  /// 迁移目标（与 [`Self::get_node_id_from_slot`] 的 eff 语义刻意区分）
  #[inline]
  pub fn get_owner_id_from_slot(&self, slot: u16) -> Option<u128> {
    let wid = self.slot_map[slot as usize].worker_id as usize;
    self.workers.get(wid).and_then(|w| w.nodeid)
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
      ClusterPreferredEndpointType::Ip => self.workers[worker_id].address.to_string(),
      ClusterPreferredEndpointType::Hostname => {
        if let Some(ref h) = self.workers[worker_id].hostname
          && !h.is_empty()
        {
          return h.to_string();
        }
        "?".to_string()
      }
      ClusterPreferredEndpointType::Unknown => "?".to_string(),
    }
  }

  /// libs/cluster/Server/ClusterConfig.cs:GetEndpointFromNodeId
  #[inline]
  pub fn get_endpoint_from_node_id(&self, nodeid: u128) -> Option<SocketAddr> {
    self
      .worker_by_node_id(nodeid)
      .and_then(|(_, w)| socket_of(w))
  }
}

impl ClusterConfig {
  /// libs/cluster/Server/ClusterConfig.cs:GetWorkerNodeIdFromAddressOrHostname
  pub fn get_worker_node_id_from_address_or_hostname(
    &self,
    address: &str,
    port: i32,
  ) -> Option<u128> {
    self.workers.iter().skip(2).find_map(|worker| {
      (worker.port == port
        && (worker.address.eq_ignore_ascii_case(address)
          || worker
            .hostname
            .as_deref()
            .is_some_and(|h| h.eq_ignore_ascii_case(address))))
      .then_some(worker.nodeid)?
    })
  }

  /// libs/cluster/Server/ClusterConfig.cs:GetReplicaIds
  pub fn get_replica_ids(&self, nodeid: u128) -> Vec<u128> {
    self
      .workers
      .iter()
      .skip(1)
      .filter(|w| w.replica_of_node_id == Some(nodeid))
      .filter_map(|w| w.nodeid)
      .collect()
  }

  /// libs/cluster/Server/ClusterConfig.cs:GetWorkerAddress
  #[inline]
  pub fn get_worker_address(&self, worker_id: u16) -> (String, i32) {
    let w = &self.workers[worker_id as usize];
    (w.address.to_string(), w.port)
  }

  /// libs/cluster/Server/ClusterConfig.cs:GetWorkerInfoForGossip
  pub fn get_worker_info_for_gossip(&self) -> Vec<(u128, String, i32)> {
    self
      .workers
      .iter()
      .skip(2)
      .filter_map(|worker| Some((worker.nodeid?, worker.address.to_string(), worker.port)))
      .collect()
  }

  /// 单遍扫描统计全部槽位状态计数；CLUSTER INFO 需要 4 个状态计数时
  /// 复用本方法，避免 4 次全表遍历
  pub fn slot_state_counts(&self) -> [usize; SLOT_STATE_KINDS] {
    self
      .slot_map
      .iter()
      .fold([0usize; SLOT_STATE_KINDS], |mut counts, slot| {
        counts[slot.state as usize] += 1;
        counts
      })
  }

  /// libs/cluster/Server/ClusterConfig.cs:GetPrimaryCount
  pub fn get_primary_count(&self) -> usize {
    self.workers[1..]
      .iter()
      .filter(|w| w.role == NodeRole::Primary)
      .count()
  }

  /// libs/cluster/Server/ClusterConfig.cs:GetWorkerNodeIdFromAddress
  #[inline]
  pub fn get_worker_node_id_from_address(&self, address: &str, port: i32) -> Option<u128> {
    self.workers[1..]
      .iter()
      .find(|w| w.address == address && w.port == port)
      .and_then(|w| w.nodeid)
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
  pub fn remove_worker(&self, nodeid: u128) -> Self {
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
      } else if state == SlotState::Importing
        && wid < self.workers.len()
        && self.workers[wid].nodeid == Some(nodeid)
      {
        // 对齐 C#（ClusterConfig.cs:1268）：nodeid 匹配属分支条件本身，仅
        // 源即被删节点的槽转 Offline；源非被删节点或 wid 越界（C# 该形态
        // 直接数组越界崩溃，rust 防御后同样下沉）的 IMPORTING 槽落入下方
        // 递减分支，随 workers 收缩前移下标，杜绝 wid 悬空错位
        slot.worker_id = RESERVED_WORKER_ID as u16;
        slot.state = SlotState::Offline;
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
    let Some(node_id) = worker.nodeid else {
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
    let assign_to_worker_id = sender_config
      .local_node_id()
      .map_or(0, |id| self.get_worker_id_from_node_id(id));
    if assign_to_worker_id == 0 {
      return false;
    }
    let mut updated = false;

    if sender_config.is_primary() {
      let sender_epoch = sender_config.local_node_config_epoch();

      for (slot, sender_slot) in self.slot_map.iter_mut().zip(sender_slot_map) {
        if sender_slot.state != SlotState::Stable {
          continue;
        }

        // 对齐 C#（ClusterConfig.cs:1168 同经 HashSlot.workerId 投影取 eff）：
        // 本地 Migrating 槽的当前归属按 LOCAL(1) 判定——迁移目标节点 gossip
        // 认领时走 epoch 比较直接移交，而非误判为"目标已是属主"把槽重置为
        // Offline 造成短暂失主
        let current_owner_id = slot.eff_worker_id() as usize;

        // 发送方非本槽认领者且是主：若本地认为属主即发送方（epoch 碰撞后
        // 的错位状态），重置为 Offline 给真实属主重新认领的机会
        if sender_slot.worker_id as usize != LOCAL_WORKER_ID {
          if current_owner_id == assign_to_worker_id as usize {
            slot.worker_id = RESERVED_WORKER_ID as u16;
            slot.state = SlotState::Offline;
            updated = true;
          }
          continue;
        }

        // 发送方是本槽认领者且为主：仅当其 epoch 更高才可改写本槽
        if sender_epoch != 0
          && self
            .workers
            .get(current_owner_id)
            .is_some_and(|w| w.config_epoch >= sender_epoch)
        {
          continue;
        }

        // 仅当属主或状态变化才算更新：避免 sender epoch=0 时的消息风暴
        updated |= slot.worker_id != assign_to_worker_id || slot.state != SlotState::Stable;
        slot.worker_id = assign_to_worker_id;
        slot.state = SlotState::Stable;
      }
    } else {
      // 发送方为副本：主节点目标在循环前一次性解析，杜绝循环内重复线性扫描；
      // 若目标主节点未在本地登记，禁止移交，防止将槽位挂至 0 号保留节点导致状态机破损
      let handoff_worker_id = sender_config
        .local_node_primary_id()
        .map_or(0, |pid| self.get_worker_id_from_node_id(pid));
      if handoff_worker_id == 0 {
        return false;
      }

      for (slot, sender_slot) in self.slot_map.iter_mut().zip(sender_slot_map) {
        if sender_slot.state != SlotState::Stable {
          continue;
        }

        let current_owner_id = slot.eff_worker_id() as usize;

        // 副本场景：仅当本槽现属主即发送方（旧主）才允许移交其主节点，
        // 保证计划内 failover 下多副本乱序 gossip 只有接管者生效；
        // assign_to_worker_id > 0，故此处天然排除了 current_owner_id == RESERVED_WORKER_ID (0)
        if current_owner_id != assign_to_worker_id as usize {
          continue;
        }

        updated |= slot.worker_id != handoff_worker_id || slot.state != SlotState::Stable;
        slot.worker_id = handoff_worker_id;
        slot.state = SlotState::Stable;
      }
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
    worker_ban_list: &ConcurrentMap<u128, i64>,
  ) -> Option<Self> {
    let local_id = self.local_node_id();
    let mut merged = self.clone();
    let mut changed = false;

    // 封禁表以并发容器直收（C# ClusterConfig.cs:Merge 的
    // `ConcurrentDictionary<string, long> workerBanList` 形参同形），逐 worker
    // 点查在册判定：一次 pin 覆盖整轮枚举，锁-free，无需持任何外层锁
    let ban_list = worker_ban_list.pin();

    for worker in sender_config.workers.iter().skip(1) {
      let Some(sid) = worker.nodeid else {
        continue;
      };
      if local_id == Some(sid) || ban_list.contains_key(&sid) {
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
      ranges.push((s, CLUSTER_SLOT_COUNT as u16 - 1));
    }
    ranges
  }

  /// libs/cluster/Server/ClusterConfig.cs:GetWorkerReplicas
  pub fn get_worker_replicas(&self, worker_id: usize) -> Vec<usize> {
    let Some(primary_id) = self.workers.get(worker_id).and_then(|w| w.nodeid) else {
      return Vec::new();
    };
    self
      .workers
      .iter()
      .enumerate()
      .skip(1)
      .filter_map(|(i, w)| (w.replica_of_node_id == Some(primary_id)).then_some(i))
      .collect()
  }

  /// libs/cluster/Server/ClusterConfig.cs:GetAllNodeIds
  pub fn get_all_node_ids(&self) -> Vec<(u128, SocketAddr)> {
    self.collect_worker_endpoints(false)
  }

  /// 仅主节点的端点枚举（本端口换号广播定向帧用：总线只连 Master，
  /// C# ClusterProvider 仅对 Primary 建总线连接同口径）
  pub fn get_primary_node_ids(&self) -> Vec<(u128, SocketAddr)> {
    self.collect_worker_endpoints(true)
  }

  /// worker → (nodeid, endpoint) 收敛枚举（保留位与本地位不承载远端节点，
  /// 自 2 号起；primary_only 追加 Primary 角色过滤）
  fn collect_worker_endpoints(&self, primary_only: bool) -> Vec<(u128, SocketAddr)> {
    self
      .workers
      .iter()
      .skip(2)
      .filter(|w| !primary_only || w.role == NodeRole::Primary)
      .filter_map(|worker| Some((worker.nodeid?, socket_of(worker)?)))
      .collect()
  }

  /// libs/cluster/Server/ClusterConfig.cs:GetNodeIdsForShard
  pub fn get_node_ids_for_shard(&self) -> Vec<(u128, SocketAddr)> {
    let primary_id = if self.local_node_role() == NodeRole::Primary {
      self.local_node_id()
    } else {
      self.local_node_primary_id()
    };

    let Some(pid) = primary_id else {
      return Vec::new();
    };

    self
      .workers
      .iter()
      .skip(2)
      .filter_map(|worker| {
        let is_match = worker.replica_of_node_id == Some(pid) || worker.nodeid == Some(pid);
        if is_match {
          Some((worker.nodeid?, socket_of(worker)?))
        } else {
          None
        }
      })
      .collect()
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

  /// 按 eff 属主生成指定 worker 负责的槽位迭代器（零分配）
  pub fn slot_list_iter(&self, worker_id: u16) -> impl Iterator<Item = usize> + '_ {
    self
      .slot_map
      .iter()
      .enumerate()
      .filter_map(move |(i, slot)| (slot.eff_worker_id() == worker_id).then_some(i))
  }

  /// libs/cluster/Server/ClusterConfig.cs:GetSlotList
  ///
  /// 对齐 C#（ClusterConfig.cs:918）按 eff 属主收集：TryStopWrites 的
  /// `GetSlotList(1)` 须把 Migrating 槽一并移交接管者
  pub fn get_slot_list(&self, worker_id: u16) -> Vec<usize> {
    self.slot_list_iter(worker_id).collect()
  }

  /// libs/cluster/Server/ClusterConfig.cs:GetLocalPrimarySlots
  ///
  /// 获取当前节点的本地主节点所负责的槽位列表
  pub fn get_local_primary_slots(&self) -> Vec<usize> {
    let Some(primary_id) = self.local_node_primary_id() else {
      return Vec::new();
    };
    let target_wid = self.get_worker_id_from_node_id(primary_id);
    if target_wid == 0 {
      return Vec::new();
    }
    self
      .slot_map
      .iter()
      .enumerate()
      .filter_map(|(i, slot)| (slot.worker_id == target_wid).then_some(i))
      .collect()
  }
}

#[cfg(test)]
mod tests {
  use super::*;

  /// 测试节点 id（任意稳定的 u128 值，hex 渲染仅诊断用）
  const NODE_A: u128 = 0xA000_0000_0000_0000_0000_0000_0000_0001;
  const NODE_B: u128 = 0xB000_0000_0000_0000_0000_0000_0000_0002;
  const NODE_Z: u128 = 0xF000_0000_0000_0000_0000_0000_0000_000A;

  /// 本地主节点配置（epoch 可指定，便于碰撞仲裁测试）
  fn config_with_local(node_id: u128, config_epoch: i64) -> ClusterConfig {
    let mut config = ClusterConfig::new();
    config.initialize_local_worker(LocalWorkerSpec {
      node_id,
      address: "127.0.0.1",
      port: 7000,
      config_epoch,
      role: NodeRole::Primary,
      replica_of_node_id: None,
      hostname: None,
    });
    config
  }

  /// C# SetLocalWorkerConfigEpoch：仅允许从 0 初始化且新值为正，
  /// 二次设置（覆写/倒退/非正）一律拒绝，单调递增只能走 bump
  #[test]
  fn test_set_local_epoch_rejects_overwrite_and_regression() {
    let mut config = config_with_local(NODE_A, 0);
    assert!(config.set_local_worker_config_epoch(5));
    // 非 0 现值：更大值也不得覆写
    assert!(!config.set_local_worker_config_epoch(9));
    // 倒退拒绝
    assert!(!config.set_local_worker_config_epoch(1));
    // 非正值拒绝
    assert!(!config.set_local_worker_config_epoch(0));
    assert_eq!(config.local_node_config_epoch(), 5);
  }

  /// C# BumpLocalNodeConfigEpoch：取全体 worker 最大 epoch + 1
  /// （全局最大纪元检查，本地纪元永不倒退到集群水位之下）
  #[test]
  fn test_bump_takes_global_max_epoch() {
    let mut config = config_with_local(NODE_A, 5);
    config.workers.push(Worker {
      nodeid: Some(NODE_B),
      config_epoch: 99,
      ..Worker::default()
    });
    config.bump_local_node_config_epoch();
    assert_eq!(config.local_node_config_epoch(), 100);
  }

  /// C# HandleConfigEpochCollision：等值碰撞且发送方 id 序更大才自愈
  /// （提升为全局 max+1 并返回 true 供调用方置脏落盘），否则原样返回 false；
  /// 内部身份收敛 u128 后按数值序仲裁（同为确定性全序）
  #[test]
  fn test_handle_config_epoch_collision() {
    // epoch 不等：不触发
    let mut local = config_with_local(NODE_A, 5);
    let sender = config_with_local(NODE_B, 6);
    assert!(!local.handle_config_epoch_collision(&sender));
    assert_eq!(local.local_node_config_epoch(), 5);

    // epoch 相等但发送方 id 更小：不触发（双方各退一步避免死循环）
    let mut local = config_with_local(NODE_Z, 5);
    let sender = config_with_local(NODE_B, 5);
    assert!(!local.handle_config_epoch_collision(&sender));
    assert_eq!(local.local_node_config_epoch(), 5);

    // 等值碰撞且发送方 id 更大：自增为全局 max+1
    let mut local = config_with_local(NODE_A, 5);
    let sender = config_with_local(NODE_B, 5);
    assert!(local.handle_config_epoch_collision(&sender));
    assert_eq!(local.local_node_config_epoch(), 6);
  }

  #[test]
  fn test_get_local_primary_slots() {
    let mut config = ClusterConfig::new();
    config.initialize_local_worker(LocalWorkerSpec {
      node_id: NODE_A,
      address: "127.0.0.1",
      port: 7001,
      config_epoch: 1,
      role: NodeRole::Replica,
      replica_of_node_id: Some(NODE_B),
      hostname: None,
    });
    // 未加 primary (NODE_B) 节点时为空
    assert!(config.get_local_primary_slots().is_empty());

    // 加入 NODE_B
    config.workers.push(Worker {
      nodeid: Some(NODE_B),
      address: HipStr::borrowed("127.0.0.1"),
      port: 7000,
      config_epoch: 1,
      role: NodeRole::Primary,
      ..Worker::default()
    });
    let wid = config.get_worker_id_from_node_id(NODE_B);
    assert_eq!(wid, 2);

    config.slot_map[0].worker_id = wid;
    config.slot_map[5].worker_id = wid;
    config.slot_map[10].worker_id = wid;

    let slots = config.get_local_primary_slots();
    assert_eq!(slots, vec![0, 5, 10]);

    // 故障接管
    config.take_over_from_primary();
    assert_eq!(config.local_node_role(), NodeRole::Primary);
    assert_eq!(config.local_node_primary_id(), None);
    assert_eq!(config.slot_map[0].worker_id, LOCAL_WORKER_ID as u16);
    assert_eq!(config.slot_map[5].worker_id, LOCAL_WORKER_ID as u16);
    assert_eq!(config.slot_map[10].worker_id, LOCAL_WORKER_ID as u16);
    assert!(config.get_local_primary_slots().is_empty());
  }
}
