//! worker 拓扑查找与合并面：按 nodeid/地址查 worker、角色/副本关系枚举、
//! gossip 合并（worker 信息面）与节点摘除。
//!
//! 对位 garnet/libs/cluster/Server/ClusterConfig.cs 的 `#region GetFromNodeId`
//! 与 Merge / MergeWorkerInfo / RemoveWorker / GetAllNodeIds / GetNodeIdsForShard。

use std::net::SocketAddr;

use wbase::map::ConcurrentMap;

use super::*;
use crate::server::{hash_slot::SlotState, worker::NodeRole};

impl ClusterConfig {
  /// libs/cluster/Server/ClusterConfig.cs:NumWorkers
  pub fn num_workers(&self) -> usize {
    self.workers.len().saturating_sub(1)
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

  /// libs/cluster/Server/ClusterConfig.cs:IsKnown
  pub fn is_known(&self, nodeid: u128) -> bool {
    self.worker_by_node_id(nodeid).is_some()
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

  /// 互指环判谓词（票 wedb-repl-mutual-replicaof-ring-no-receive-role-gate）：
  /// 自 node 沿本地配置视图的 primary 链（replica_of_node_id）回溯，命中
  /// target 即「node 已（直接或间接）复制自 target」——此时再以 node 为复制
  /// 目标必成环（target → node → … → target）。链长以 workers 数为界：
  /// 视图内既有环不含 target 时靠界收束，不死循环
  pub fn primary_chain_hits(&self, node: u128, target: u128) -> bool {
    let mut next = self
      .worker_by_node_id(node)
      .and_then(|(_idx, w)| w.replica_of_node_id);
    for _ in 0..self.workers.len() {
      let Some(id) = next else {
        return false;
      };
      if id == target {
        return true;
      }
      next = self
        .worker_by_node_id(id)
        .and_then(|(_idx, w)| w.replica_of_node_id);
    }
    false
  }

  /// libs/cluster/Server/ClusterConfig.cs:GetWorkerFromNodeId
  pub fn get_worker_from_node_id(&self, node_id: u128) -> Option<&Worker> {
    self.worker_by_node_id(node_id).map(|(_idx, w)| w)
  }

  /// libs/cluster/Server/ClusterConfig.cs:GetWorkerAddressFromNodeId
  pub fn get_worker_address_from_node_id(&self, node_id: u128) -> (Option<String>, i32) {
    match self.get_worker_from_node_id(node_id) {
      Some(w) => (Some(w.address.to_string()), w.port),
      None => (None, -1),
    }
  }

  /// libs/cluster/Server/ClusterConfig.cs:GetEndpointFromNodeId
  #[inline]
  pub fn get_endpoint_from_node_id(&self, nodeid: u128) -> Option<SocketAddr> {
    self
      .worker_by_node_id(nodeid)
      .and_then(|(_, w)| socket_of(w))
  }

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
}
