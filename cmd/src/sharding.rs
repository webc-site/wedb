use std::cmp::Reverse;
use std::collections::BTreeMap;
use std::fmt::Write;

use rapidhash::v3::rapidhash_v3;
use rapidhash::{RapidHashMap as HashMap, RapidHashSet as HashSet};
use wedb_embed::error::{Error, Result};
use wedb_resp::RespValue;

/// 默认虚拟分片槽位总数（1024 个虚拟分片组可完美支撑数百万命名空间与数百物理节点）
pub const DEFAULT_SHARD_COUNT: u32 = 1024;

/// Redis Cluster 标准槽位总数 (0..=16383)
pub const CLUSTER_SLOTS_TOTAL: u32 = 16384;

/// 默认每个分片的目标副本数（严格 3 副本：1 Leader + 2 Followers）
pub const DEFAULT_REPLICAS_PER_SHARD: usize = 3;

/// 默认物理节点基准权重（100 表示 100% 标准基线性能，0 表示空间耗尽或维护中不可分配新分片）
pub const DEFAULT_NODE_WEIGHT: u32 = 100;

/// 计算 N 节点集群下第 i 个节点分配的标准槽位区间 (0-indexed, 0 <= node_idx < num_nodes)
#[inline]
pub fn calc_node_slot_range(node_idx: usize, num_nodes: usize) -> (u32, u32) {
  if num_nodes == 0 {
    return (0, 16383);
  }
  let start = ((node_idx as u64 * 16384 * 2 + num_nodes as u64) / (num_nodes as u64 * 2)) as u32;
  let end =
    (((node_idx + 1) as u64 * 16384 * 2 + num_nodes as u64) / (num_nodes as u64 * 2) - 1) as u32;
  (start, end.min(16383))
}

/// 计算指定命名空间所映射的虚拟分片组编号
#[inline]
pub fn calculate_shard_id(namespace: &str, shard_count: u32) -> u32 {
  let count = if shard_count == 0 {
    DEFAULT_SHARD_COUNT
  } else {
    shard_count
  };
  let hash = rapidhash_v3(namespace.as_bytes());
  if count.is_power_of_two() {
    (hash as u32) & (count - 1)
  } else {
    (hash % count as u64) as u32
  }
}

/// 单个分片组的拓扑信息（分片编号、当前领导者节点、所有副本节点）
#[derive(Debug, Clone, PartialEq, Eq, bitcode::Encode, bitcode::Decode)]
pub struct ShardInfo {
  pub shard_id: u32,
  pub leader_node_id: u64,
  pub replicas: Vec<u64>,
}

impl ShardInfo {
  /// 创建新分片信息
  #[inline]
  pub const fn new(shard_id: u32, leader_node_id: u64, replicas: Vec<u64>) -> Self {
    Self {
      shard_id,
      leader_node_id,
      replicas,
    }
  }

  /// 检查指定节点是否为该分片的副本
  #[inline]
  pub fn contains_replica(&self, node_id: u64) -> bool {
    self.replicas.contains(&node_id)
  }

  /// 检查指定节点是否为该分片的领导者
  #[inline]
  pub fn is_leader(&self, node_id: u64) -> bool {
    self.leader_node_id == node_id
  }
}

/// 节点物理拓扑位置（支持地区 Region、可用区/机房 Zone/DC、机架 Rack、主机 Host 四级故障域）
#[derive(Debug, Clone, PartialEq, Eq, Hash, Default, bitcode::Encode, bitcode::Decode)]
pub struct NodeLocation {
  /// 地区/大区 (如 cn-beijing, us-west)
  pub region: String,
  /// 可用区/机房/数据中心 (如 zone-a, dc-01)
  pub zone: String,
  /// 机架/机柜编号 (如 rack-01)
  pub rack: String,
  /// 物理主机标识 (如 192.168.1.10)
  pub host: String,
}

impl NodeLocation {
  /// 创建完整四级物理位置
  pub fn new(
    region: impl Into<String>,
    zone: impl Into<String>,
    rack: impl Into<String>,
    host: impl Into<String>,
  ) -> Self {
    Self {
      region: region.into(),
      zone: zone.into(),
      rack: rack.into(),
      host: host.into(),
    }
  }

  /// 基于机架编号快速创建
  pub fn from_rack_id(rack_id: u32) -> Self {
    if rack_id == 0 {
      Self::default()
    } else {
      Self {
        region: String::new(),
        zone: String::new(),
        rack: format!("rack-{rack_id}"),
        host: String::new(),
      }
    }
  }

  /// 是否未配置任何拓扑标签
  #[inline]
  pub fn is_empty(&self) -> bool {
    self.region.is_empty() && self.zone.is_empty() && self.rack.is_empty() && self.host.is_empty()
  }

  /// 计算两个节点之间的拓扑故障域距离 (0..=4)
  /// 4: 跨 Region (异地跨大区)
  /// 3: 跨 Zone/DC (同地区跨可用区/机房)
  /// 2: 跨 Rack (同机房跨机架)
  /// 1: 跨 Host (同机架跨宿主机)
  /// 0: 同物理机/未配置
  #[inline]
  pub fn distance(&self, other: &Self) -> u8 {
    if self.is_empty() || other.is_empty() {
      return 0;
    }
    if !self.region.is_empty() && !other.region.is_empty() && self.region != other.region {
      4
    } else if !self.zone.is_empty() && !other.zone.is_empty() && self.zone != other.zone {
      3
    } else if !self.rack.is_empty() && !other.rack.is_empty() && self.rack != other.rack {
      2
    } else if !self.host.is_empty() && !other.host.is_empty() && self.host != other.host {
      1
    } else {
      0
    }
  }
}

/// 集群全局分片与 3 副本拓扑管理器
#[derive(Debug, Clone, bitcode::Encode, bitcode::Decode)]
pub struct ShardTopology {
  pub shard_count: u32,
  pub shards: Vec<ShardInfo>,
  /// 节点编号 -> 节点网络地址（保持有序以确保重平衡可重现）
  pub nodes: BTreeMap<u64, String>,
  /// 节点编号 -> 节点配置权重（默认 100，0 代表空间耗尽/待下线不分配新分片）
  pub weights: BTreeMap<u64, u32>,
  /// 节点编号 -> 物理机架/可用区编号（默认 0，保留兼容）
  pub racks: BTreeMap<u64, u32>,
  /// 节点编号 -> 完整四级物理位置（地区/机房/机架/主机）
  pub locations: BTreeMap<u64, NodeLocation>,
}

impl Default for ShardTopology {
  fn default() -> Self {
    Self::new(DEFAULT_SHARD_COUNT)
  }
}

impl ShardTopology {
  /// 计算 N 节点集群下第 i 个节点分配的标准槽位区间 (0-indexed, 0 <= node_idx < num_nodes)
  #[inline]
  pub fn calc_node_slot_range(node_idx: usize, num_nodes: usize) -> (u32, u32) {
    calc_node_slot_range(node_idx, num_nodes)
  }

  /// 创建指定分片数量的拓扑管理器
  pub fn new(shard_count: u32) -> Self {
    let count = if shard_count == 0 {
      DEFAULT_SHARD_COUNT
    } else {
      shard_count
    };
    let shards = (0..count)
      .map(|id| ShardInfo {
        shard_id: id,
        leader_node_id: 1,
        replicas: vec![1],
      })
      .collect();
    let mut nodes = BTreeMap::new();
    nodes.insert(1, "127.0.0.1:15001".to_string());
    let mut weights = BTreeMap::new();
    weights.insert(1, DEFAULT_NODE_WEIGHT);
    let mut racks = BTreeMap::new();
    racks.insert(1, 0);
    let mut locations = BTreeMap::new();
    locations.insert(1, NodeLocation::default());
    Self {
      shard_count: count,
      shards,
      nodes,
      weights,
      racks,
      locations,
    }
  }

  /// 批量注册节点（如初始化 100 台机器集群，默认赋予标准基准权重 100）
  pub fn register_nodes<I, S>(&mut self, node_iter: I)
  where
    I: IntoIterator<Item = (u64, S)>,
    S: Into<String>,
  {
    for (id, addr) in node_iter {
      self.register_node(id, addr);
    }
  }

  /// 注册/更新单个节点地址（默认赋予标准基准权重 100）
  #[inline]
  pub fn register_node(&mut self, node_id: u64, addr: impl Into<String>) {
    self.register_node_with_weight(node_id, addr, DEFAULT_NODE_WEIGHT);
  }

  /// 注册/更新单个节点地址及专属权重（高性能机器如 200，低配机器 50，无空间 0）
  #[inline]
  pub fn register_node_with_weight(&mut self, node_id: u64, addr: impl Into<String>, weight: u32) {
    self.register_node_with_rack(node_id, addr, weight, 0);
  }

  /// 注册/更新单个节点地址、专属权重与所属机架/可用区（支持跨机架容灾隔离）
  #[inline]
  pub fn register_node_with_rack(
    &mut self,
    node_id: u64,
    addr: impl Into<String>,
    weight: u32,
    rack_id: u32,
  ) {
    let loc = NodeLocation::from_rack_id(rack_id);
    self.register_node_with_location(node_id, addr, weight, loc);
  }

  /// 注册/更新单个节点地址、专属权重与完整物理位置（支持地区、机房、机架、主机标签）
  #[inline]
  pub fn register_node_with_location(
    &mut self,
    node_id: u64,
    addr: impl Into<String>,
    weight: u32,
    location: NodeLocation,
  ) {
    self.nodes.insert(node_id, addr.into());
    self.weights.insert(node_id, weight);
    let rack_id = if let Some(stripped) = location.rack.strip_prefix("rack-") {
      stripped.parse::<u32>().unwrap_or(0)
    } else {
      0
    };
    self.racks.insert(node_id, rack_id);
    self.locations.insert(node_id, location);
  }

  /// 获取指定节点当前机架/可用区编号
  #[inline]
  pub fn get_node_rack(&self, node_id: u64) -> u32 {
    self.racks.get(&node_id).copied().unwrap_or(0)
  }

  /// 设置/更新指定物理节点机架/可用区编号
  #[inline]
  pub fn set_node_rack(&mut self, node_id: u64, rack_id: u32) {
    if self.nodes.contains_key(&node_id) {
      self.racks.insert(node_id, rack_id);
      let loc = self.locations.entry(node_id).or_default();
      loc.rack = if rack_id == 0 {
        String::new()
      } else {
        format!("rack-{rack_id}")
      };
    }
  }

  /// 获取指定节点当前物理位置（地区/机房/机架/主机）
  #[inline]
  pub fn get_node_location(&self, node_id: u64) -> Option<&NodeLocation> {
    self.locations.get(&node_id)
  }

  /// 设置/更新指定物理节点物理位置
  #[inline]
  pub fn set_node_location(&mut self, node_id: u64, location: NodeLocation) {
    if self.nodes.contains_key(&node_id) {
      let rack_id = if let Some(stripped) = location.rack.strip_prefix("rack-") {
        stripped.parse::<u32>().unwrap_or(0)
      } else {
        0
      };
      self.racks.insert(node_id, rack_id);
      self.locations.insert(node_id, location);
    }
  }

  /// 快捷设置节点的 Region, Zone, Rack, Host 标签
  #[inline]
  pub fn set_node_tags(
    &mut self,
    node_id: u64,
    region: impl Into<String>,
    zone: impl Into<String>,
    rack: impl Into<String>,
    host: impl Into<String>,
  ) {
    let loc = NodeLocation::new(region, zone, rack, host);
    self.set_node_location(node_id, loc);
  }

  /// 计算候选节点与给定副本集合之间的最小拓扑距离 (零堆分配，零拷贝)
  /// 4: 跨 Region (异地大区)
  /// 3: 跨 Zone/DC (同大区跨可用区/机房)
  /// 2: 跨 Rack (同机房跨机柜)
  /// 1: 跨 Host (同机柜跨宿主物理机)
  /// 0: 同物理机/未配置/机架冲突
  #[inline]
  pub fn calc_cand_min_dist_from_maps(
    locations: &BTreeMap<u64, NodeLocation>,
    racks: &BTreeMap<u64, u32>,
    cand_node_id: u64,
    replicas: &[u64],
  ) -> u8 {
    if replicas.is_empty() {
      return 4;
    }
    let default_loc = NodeLocation::default();
    let cand_loc = locations.get(&cand_node_id).unwrap_or(&default_loc);

    if cand_loc.is_empty() {
      let r_cand = racks.get(&cand_node_id).copied().unwrap_or(0);
      let has_conflict = r_cand != 0
        && replicas
          .iter()
          .any(|&r| racks.get(&r).copied().unwrap_or(0) == r_cand);
      if has_conflict { 0 } else { 2 }
    } else {
      replicas
        .iter()
        .map(|&r| {
          let p_loc = locations.get(&r).unwrap_or(&default_loc);
          cand_loc.distance(p_loc)
        })
        .min()
        .unwrap_or(4)
    }
  }

  /// 计算候选节点与给定副本集合之间的最小拓扑距离
  #[inline]
  pub fn calc_cand_min_dist(&self, cand_node_id: u64, replicas: &[u64]) -> u8 {
    Self::calc_cand_min_dist_from_maps(&self.locations, &self.racks, cand_node_id, replicas)
  }

  /// 在候选节点中挑选最优副本放置节点 (按拓扑故障域最大距离 > 最轻副本负载 > 节点ID 排序)
  #[inline]
  pub fn select_best_candidate(
    replica_counts: &HashMap<u64, usize>,
    weights: &BTreeMap<u64, u32>,
    locations: &BTreeMap<u64, NodeLocation>,
    racks: &BTreeMap<u64, u32>,
    existing_replicas: &[u64],
    dist_reference_replicas: &[u64],
  ) -> Option<u64> {
    Self::select_best_candidate_filtered(
      replica_counts,
      weights,
      locations,
      racks,
      existing_replicas,
      dist_reference_replicas,
      |_, _| true,
    )
  }

  /// 在候选节点中按额外条件过滤并挑选最优副本放置节点
  #[inline]
  pub fn select_best_candidate_filtered(
    replica_counts: &HashMap<u64, usize>,
    weights: &BTreeMap<u64, u32>,
    locations: &BTreeMap<u64, NodeLocation>,
    racks: &BTreeMap<u64, u32>,
    existing_replicas: &[u64],
    dist_reference_replicas: &[u64],
    predicate: impl Fn(u64, usize) -> bool,
  ) -> Option<u64> {
    replica_counts
      .iter()
      .filter(|(id, cnt)| {
        weights.get(id).copied().unwrap_or(DEFAULT_NODE_WEIGHT) > 0
          && !existing_replicas.contains(id)
          && predicate(**id, **cnt)
      })
      .min_by_key(|&(id, cnt)| {
        let min_dist =
          Self::calc_cand_min_dist_from_maps(locations, racks, *id, dist_reference_replicas);
        (Reverse(min_dist), *cnt, *id)
      })
      .map(|(&id, _)| id)
  }

  /// 获取指定节点当前权重
  #[inline]
  pub fn get_node_weight(&self, node_id: u64) -> u32 {
    self
      .weights
      .get(&node_id)
      .copied()
      .unwrap_or(DEFAULT_NODE_WEIGHT)
  }

  /// 设置/更新指定物理节点权重
  #[inline]
  pub fn set_node_weight(&mut self, node_id: u64, weight: u32) {
    if self.nodes.contains_key(&node_id) {
      self.weights.insert(node_id, weight);
    }
  }

  /// 标记节点空间耗尽（将权重置为 0，停止分配新分片）
  #[inline]
  pub fn mark_node_out_of_space(&mut self, node_id: u64) {
    self.set_node_weight(node_id, 0);
  }

  /// 移除下线节点并修正领导者
  pub fn remove_node(&mut self, node_id: u64) {
    self.nodes.remove(&node_id);
    self.weights.remove(&node_id);
    self.racks.remove(&node_id);
    self.locations.remove(&node_id);
    for shard in &mut self.shards {
      shard.replicas.retain(|&id| id != node_id);
      if shard.leader_node_id == node_id {
        shard.leader_node_id = shard.replicas.first().copied().unwrap_or(0);
      }
    }
  }

  /// 统计各物理节点当前承载的副本总数
  pub fn count_node_replicas(&self) -> HashMap<u64, usize> {
    let mut counts = HashMap::with_capacity_and_hasher(self.nodes.len(), Default::default());
    for &node_id in self.nodes.keys() {
      counts.insert(node_id, 0);
    }
    for shard in &self.shards {
      for &rep_id in &shard.replicas {
        *counts.entry(rep_id).or_default() += 1;
      }
    }
    counts
  }

  /// 统计各物理节点当前担任 Leader 的分片总数
  pub fn count_node_leaders(&self) -> HashMap<u64, usize> {
    let mut counts = HashMap::with_capacity_and_hasher(self.nodes.len(), Default::default());
    for &node_id in self.nodes.keys() {
      counts.insert(node_id, 0);
    }
    for shard in &self.shards {
      *counts.entry(shard.leader_node_id).or_default() += 1;
    }
    counts
  }

  /// 获取指定命名空间归属的分片信息与当前领导者节点编号
  #[inline]
  pub fn get_shard_for_namespace(&self, namespace: &str) -> Option<&ShardInfo> {
    let shard_id = calculate_shard_id(namespace, self.shard_count);
    self.shards.get(shard_id as usize)
  }

  /// 获取指定命名空间当前承载写入的领导者节点编号
  #[inline]
  pub fn get_leader_for_namespace(&self, namespace: &str) -> Option<u64> {
    self
      .get_shard_for_namespace(namespace)
      .map(|s| s.leader_node_id)
  }

  /// 转移指定分片的领导权至目标节点
  pub fn transfer_leader(&mut self, shard_id: u32, target_node_id: u64) -> Result<()> {
    if !self.nodes.contains_key(&target_node_id) {
      return Err(Error::redis(format!(
        "ERR target node {target_node_id} does not exist"
      )));
    }
    let shard = self
      .shards
      .get_mut(shard_id as usize)
      .ok_or_else(|| Error::redis(format!("ERR shard {shard_id} not found")))?;

    if !shard.replicas.contains(&target_node_id) {
      shard.replicas.push(target_node_id);
    }
    shard.leader_node_id = target_node_id;
    Ok(())
  }

  /// 自动均衡集群内所有分片组的 3 副本与 Leader 分布（针对 100+ 节点高均衡打散）
  pub fn rebalance_3replicas(&mut self) {
    // 如果存在差异化权重配置，采用平滑加权放置算法 SWRR
    let has_custom_weights = self.weights.values().any(|&w| w != DEFAULT_NODE_WEIGHT);
    if has_custom_weights {
      self.rebalance_weighted_with_target_replicas(DEFAULT_REPLICAS_PER_SHARD);
    } else {
      self.rebalance_with_target_replicas(DEFAULT_REPLICAS_PER_SHARD);
    }
  }

  /// 指定副本数的目标重平衡（等权重循环移位分配，极差 <= 1）
  pub fn rebalance_with_target_replicas(&mut self, target_replicas: usize) {
    // 过滤出权重 > 0 的有效健康节点
    let node_list: Vec<u64> = self
      .nodes
      .keys()
      .copied()
      .filter(|&id| self.get_node_weight(id) > 0)
      .collect();
    if node_list.is_empty() {
      return;
    }
    let num_nodes = node_list.len();
    let replicas_num = target_replicas.min(num_nodes);

    let total_shards = self.shards.len();
    for (idx, shard) in self.shards.iter_mut().enumerate() {
      let slot_mid =
        (idx as u64 * 16384 + 16384 / (total_shards.max(1) as u64 * 2)) / total_shards as u64;
      let primary_node_idx = (0..num_nodes)
        .find(|&i| {
          let (start, end) = Self::calc_node_slot_range(i, num_nodes);
          (slot_mid as u32) >= start && (slot_mid as u32) <= end
        })
        .unwrap_or_else(|| (idx * num_nodes) / total_shards);

      let new_reps: Vec<u64> = (0..replicas_num)
        .map(|i| node_list[(primary_node_idx + i) % num_nodes])
        .collect();
      shard.leader_node_id = new_reps[0];
      shard.replicas = new_reps;
    }
  }

  /// 基于平滑加权轮询（SWRR）对分片进行加权负载均衡（支持机架感知、Leader 二次微调与 0 权重无空间机器完全隔离）
  pub fn rebalance_weighted_with_target_replicas(&mut self, target_replicas: usize) {
    // 过滤出权重 > 0 的有效节点
    let active_nodes: Vec<(u64, u32)> = self
      .nodes
      .keys()
      .copied()
      .map(|id| (id, self.get_node_weight(id)))
      .filter(|(_, w)| *w > 0)
      .collect();

    if active_nodes.is_empty() {
      return;
    }

    let num_nodes = active_nodes.len();
    let replicas_num = target_replicas.min(num_nodes);
    let total_weight: u64 = active_nodes.iter().map(|(_, w)| *w as u64).sum();
    if total_weight == 0 {
      return;
    }

    let total_w = total_weight as i64;
    let mut current_weights = vec![0i64; num_nodes];
    let mut leader_counts: HashMap<u64, usize> =
      HashMap::with_capacity_and_hasher(num_nodes, Default::default());
    let locations = &self.locations;
    let racks = &self.racks;
    let weights = &self.weights;

    for shard in &mut self.shards {
      let mut new_reps = Vec::with_capacity(replicas_num);
      while new_reps.len() < replicas_num {
        for (i, &(_, w)) in active_nodes.iter().enumerate() {
          current_weights[i] += w as i64;
        }

        // 优先选择拓扑距离最大（跨Region/跨AZ/跨机架/跨主机）的候选节点，同距离下按平滑权重优先
        let best_idx = active_nodes
          .iter()
          .enumerate()
          .filter(|(_, (node_id, _))| !new_reps.contains(node_id))
          .max_by_key(|&(i, (node_id, _))| {
            (
              Self::calc_cand_min_dist_from_maps(locations, racks, *node_id, &new_reps),
              current_weights[i],
            )
          })
          .map(|(i, _)| i);

        if let Some(idx) = best_idx {
          new_reps.push(active_nodes[idx].0);
          current_weights[idx] -= total_w;
        } else {
          break;
        }
      }

      if !new_reps.is_empty() {
        // Leader 绝对均摊二次微调：在选出的副本中挑选当前 Leader 负载比率最低的节点
        let best_leader = *new_reps
          .iter()
          .min_by_key(|&&node_id| {
            let cnt = leader_counts.get(&node_id).copied().unwrap_or(0);
            let w = weights
              .get(&node_id)
              .copied()
              .unwrap_or(DEFAULT_NODE_WEIGHT)
              .max(1) as usize;
            (cnt * 10_000 / w, cnt, node_id)
          })
          .unwrap_or(&new_reps[0]);

        *leader_counts.entry(best_leader).or_default() += 1;
        shard.leader_node_id = best_leader;
        shard.replicas = new_reps;
      }
    }
  }

  /// 最小数据迁移重平衡：仅迁移超额节点上的多余分片副本至欠载节点，保留绝大多数分片物理位置不变（机架容灾感知）
  pub fn rebalance_incremental(&mut self, target_replicas: usize) -> Vec<(u32, u64, u64)> {
    let mut migration_plan = Vec::new();
    let active_nodes: Vec<(u64, u32)> = self
      .nodes
      .keys()
      .copied()
      .map(|id| (id, self.get_node_weight(id)))
      .filter(|(_, w)| *w > 0)
      .collect();

    if active_nodes.is_empty() {
      return migration_plan;
    }

    let num_nodes = active_nodes.len();
    let replicas_num = target_replicas.min(num_nodes);
    let total_weight: u64 = active_nodes.iter().map(|(_, w)| *w as u64).sum();
    if total_weight == 0 {
      return migration_plan;
    }

    let total_slots = self.shards.len() * replicas_num;
    let mut target_quotas: HashMap<u64, usize> =
      HashMap::with_capacity_and_hasher(num_nodes, Default::default());
    for &(id, w) in &active_nodes {
      let quota = ((w as u64 * total_slots as u64 + total_weight / 2) / total_weight) as usize;
      target_quotas.insert(id, quota);
    }

    let mut replica_counts = self.count_node_replicas();
    let locations = &self.locations;
    let racks = &self.racks;

    // 识别超载节点与欠载节点
    for shard in &mut self.shards {
      for pos in 0..shard.replicas.len() {
        let current_node = shard.replicas[pos];
        let current_cnt = replica_counts.get(&current_node).copied().unwrap_or(0);
        let target_quota = target_quotas.get(&current_node).copied().unwrap_or(0);

        let other_replicas: Vec<u64> = shard
          .replicas
          .iter()
          .copied()
          .filter(|&r| r != current_node)
          .collect();

        if current_cnt > target_quota
          && let Some(candidate_id) = Self::select_best_candidate_filtered(
            &replica_counts,
            &self.weights,
            locations,
            racks,
            &shard.replicas,
            &other_replicas,
            |id, cnt| {
              let cand_target = target_quotas.get(&id).copied().unwrap_or(0);
              cnt < cand_target
            },
          )
        {
          shard.replicas[pos] = candidate_id;
          if let Some(c) = replica_counts.get_mut(&current_node) {
            *c = c.saturating_sub(1);
          }
          *replica_counts.entry(candidate_id).or_default() += 1;
          migration_plan.push((shard.shard_id, current_node, candidate_id));

          if shard.leader_node_id == current_node {
            shard.leader_node_id = candidate_id;
          }
        }
      }
    }

    migration_plan
  }

  /// 保持兼容的简单重平衡接口
  pub fn rebalance(&mut self) {
    self.rebalance_3replicas();
  }

  /// 线上缩容/排空指定物理节点：将该节点的所有分片副本优雅迁移给其他健康节点（机架与多可用区感知）
  pub fn drain_node(&mut self, drain_node_id: u64) -> Result<()> {
    if !self.nodes.contains_key(&drain_node_id) {
      return Err(Error::redis(format!("ERR node {drain_node_id} not found")));
    }
    if self.nodes.len() <= DEFAULT_REPLICAS_PER_SHARD {
      return Err(Error::redis(
        "ERR cluster size must exceed replica count to drain node",
      ));
    }

    let mut replica_counts = self.count_node_replicas();
    replica_counts.remove(&drain_node_id);
    let locations = &self.locations;
    let racks = &self.racks;
    let weights = &self.weights;

    for shard in &mut self.shards {
      if let Some(pos) = shard.replicas.iter().position(|&id| id == drain_node_id) {
        let other_replicas: Vec<u64> = shard
          .replicas
          .iter()
          .copied()
          .filter(|&r| r != drain_node_id)
          .collect();

        // 挑选当前承载副本数最少且不在该分片内的健康存活节点（权重 > 0，优先跨机房/跨机架）
        if let Some(candidate_id) = Self::select_best_candidate(
          &replica_counts,
          weights,
          locations,
          racks,
          &shard.replicas,
          &other_replicas,
        ) {
          shard.replicas[pos] = candidate_id;
          *replica_counts.entry(candidate_id).or_default() += 1;

          // 若被排空节点为 Leader，将领导权优先移交至组内存活的原生 Follower
          if shard.leader_node_id == drain_node_id {
            shard.leader_node_id = shard
              .replicas
              .iter()
              .find(|&&id| id != candidate_id)
              .copied()
              .unwrap_or(candidate_id);
          }
        }
      }
    }

    self.nodes.remove(&drain_node_id);
    self.weights.remove(&drain_node_id);
    self.racks.remove(&drain_node_id);
    self.locations.remove(&drain_node_id);
    Ok(())
  }

  /// 宕机自愈：检测指定失效节点列表，自动从健康且有空间的节点中为受损分片补齐 3 副本并自愈 Leader（机架与多可用区感知）
  pub fn auto_heal(&mut self, dead_node_ids: &[u64]) -> Vec<(u32, u64, u64)> {
    let mut heal_actions = Vec::new();
    if dead_node_ids.is_empty() || self.nodes.len() <= dead_node_ids.len() {
      return heal_actions;
    }

    let dead_set: HashSet<u64> = dead_node_ids.iter().copied().collect();

    // 从集群存活视图中移除故障节点
    for &dead_id in &dead_set {
      self.nodes.remove(&dead_id);
      self.weights.remove(&dead_id);
      self.racks.remove(&dead_id);
      self.locations.remove(&dead_id);
    }

    let mut replica_counts = self.count_node_replicas();
    for &dead_id in &dead_set {
      replica_counts.remove(&dead_id);
    }
    let locations = &self.locations;
    let racks = &self.racks;
    let weights = &self.weights;

    for shard in &mut self.shards {
      let mut need_leader_election = false;
      for &dead_id in &dead_set {
        if let Some(pos) = shard.replicas.iter().position(|&id| id == dead_id) {
          shard.replicas.remove(pos);
          if shard.leader_node_id == dead_id {
            need_leader_election = true;
          }

          let alive_replicas: Vec<u64> = shard
            .replicas
            .iter()
            .copied()
            .filter(|r| !dead_set.contains(r))
            .collect();

          // 挑选最轻载健康存活候选节点（排除死节点、已有副本及 0 权重空间已满节点，优先跨机房/跨机架）
          if let Some(candidate_id) = Self::select_best_candidate(
            &replica_counts,
            weights,
            locations,
            racks,
            &shard.replicas,
            &alive_replicas,
          ) {
            shard.replicas.push(candidate_id);
            *replica_counts.entry(candidate_id).or_default() += 1;
            heal_actions.push((shard.shard_id, dead_id, candidate_id));
          }
        }
      }

      // 若原 Leader 宕机，自动在存活副本中选出新 Leader
      if need_leader_election || dead_set.contains(&shard.leader_node_id) {
        shard.leader_node_id = shard.replicas.first().copied().unwrap_or(0);
      }
    }

    heal_actions
  }

  /// 检查集群是否存在副本不足的降级分片（待扩容分片）
  #[inline]
  pub fn is_degraded(&self) -> bool {
    self.degraded_shard_count(DEFAULT_REPLICAS_PER_SHARD) > 0
  }

  /// 统计当前副本不足（< target_replicas）的降级分片总数
  pub fn degraded_shard_count(&self, target_replicas: usize) -> usize {
    let target = if target_replicas == 0 {
      DEFAULT_REPLICAS_PER_SHARD
    } else {
      target_replicas
    };
    self
      .shards
      .iter()
      .filter(|s| s.replicas.len() < target)
      .count()
  }

  /// 获取所有待扩容/副本不足的降级分片编号列表
  pub fn under_replicated_shards(&self, target_replicas: usize) -> Vec<u32> {
    let target = if target_replicas == 0 {
      DEFAULT_REPLICAS_PER_SHARD
    } else {
      target_replicas
    };
    self
      .shards
      .iter()
      .filter(|s| s.replicas.len() < target)
      .map(|s| s.shard_id)
      .collect()
  }

  /// 节点上线自动扩容/自愈待扩容分片：当新节点上线或机器资源充裕时，自动为所有副本不足的分片补齐至目标副本数（机架与多可用区感知与负载加权）
  pub fn auto_expand_under_replicated(&mut self, target_replicas: usize) -> Vec<(u32, u64)> {
    let mut expanded_actions = Vec::new();
    let target = if target_replicas == 0 {
      DEFAULT_REPLICAS_PER_SHARD
    } else {
      target_replicas
    };

    let active_nodes: Vec<u64> = self
      .nodes
      .keys()
      .copied()
      .filter(|&id| self.get_node_weight(id) > 0)
      .collect();

    if active_nodes.is_empty() {
      return expanded_actions;
    }

    // 目标副本数受限于当前实际存活的健康节点总数
    let effective_target = target.min(active_nodes.len());
    let mut replica_counts = self.count_node_replicas();
    let locations = &self.locations;
    let racks = &self.racks;
    let weights = &self.weights;

    for shard in &mut self.shards {
      while shard.replicas.len() < effective_target {
        // 寻找最轻载且不在此分片内的有效节点（优先跨机房/跨机架、高权重）
        let candidate = Self::select_best_candidate(
          &replica_counts,
          weights,
          locations,
          racks,
          &shard.replicas,
          &shard.replicas,
        );

        if let Some(candidate_id) = candidate {
          shard.replicas.push(candidate_id);
          *replica_counts.entry(candidate_id).or_default() += 1;
          expanded_actions.push((shard.shard_id, candidate_id));

          // 若该分片原先没有有效 Leader，将新节点设为 Leader
          if shard.leader_node_id == 0 || !shard.replicas.contains(&shard.leader_node_id) {
            shard.leader_node_id = candidate_id;
          }
        } else {
          break;
        }
      }
    }

    expanded_actions
  }

  /// 超过 3 节点自动清理与 GC：扫描所有分片，若副本数 > max_replicas 则自动剔除多余/旧副本
  pub fn prune_excess_replicas(&mut self, max_replicas: usize) -> Vec<(u32, u64)> {
    let mut pruned_actions = Vec::new();
    let target = if max_replicas == 0 {
      DEFAULT_REPLICAS_PER_SHARD
    } else {
      max_replicas
    };

    let mut replica_counts = self.count_node_replicas();
    let nodes = &self.nodes;
    let weights = &self.weights;

    for shard in &mut self.shards {
      while shard.replicas.len() > target {
        // 寻找修剪候选节点：绝不能是当前 Leader，优先淘汰未知/已注销节点/0权重节点，其次淘汰总负载最高的节点
        let excess_idx = shard
          .replicas
          .iter()
          .enumerate()
          .filter(|(_, node_id)| **node_id != shard.leader_node_id)
          .max_by_key(|(_, node_id)| {
            if !nodes.contains_key(node_id) {
              usize::MAX
            } else if weights.get(node_id).copied().unwrap_or(DEFAULT_NODE_WEIGHT) == 0 {
              usize::MAX - 1
            } else {
              replica_counts.get(node_id).copied().unwrap_or(0)
            }
          })
          .map(|(idx, _)| idx);

        if let Some(idx) = excess_idx {
          let pruned_node_id = shard.replicas.remove(idx);
          if let Some(cnt) = replica_counts.get_mut(&pruned_node_id) {
            *cnt = cnt.saturating_sub(1);
          }
          pruned_actions.push((shard.shard_id, pruned_node_id));
        } else {
          break;
        }
      }
    }

    pruned_actions
  }

  /// 根据当前 Raft 领导者与节点列表动态同步分片拓扑状态
  pub fn sync_raft_state(&mut self, current_leader: Option<u64>, sm_nodes: &[(u64, String)]) {
    let mut node_added = false;
    for (id, addr) in sm_nodes {
      if !self.nodes.contains_key(id) {
        self.register_node(*id, addr.clone());
        node_added = true;
      } else if let Some(existing_addr) = self.nodes.get_mut(id)
        && existing_addr != addr
        && !addr.is_empty()
      {
        *existing_addr = addr.clone();
      }
    }

    let active_nodes: Vec<u64> = self
      .nodes
      .keys()
      .copied()
      .filter(|&id| self.get_node_weight(id) > 0)
      .collect();

    if active_nodes.len() > 1 {
      let needs_rebalance = node_added
        || self.shards.is_empty()
        || self
          .shards
          .iter()
          .all(|s| s.leader_node_id == self.shards[0].leader_node_id);
      if needs_rebalance {
        self.rebalance_3replicas();
      }
    } else if let Some(leader_id) = current_leader
      && self.nodes.contains_key(&leader_id)
    {
      for shard in &mut self.shards {
        shard.leader_node_id = leader_id;
        if !shard.replicas.contains(&leader_id) {
          shard.replicas.push(leader_id);
        }
      }
    }
  }

  /// 获取负责指定 Slot (0..16383) 的 Leader 节点编号
  #[inline]
  pub fn get_leader_for_slot(&self, slot: u16) -> Option<u64> {
    let slot = (slot & 0x3FFF) as u32;
    let active_nodes: Vec<u64> = self
      .nodes
      .keys()
      .copied()
      .filter(|&id| self.get_node_weight(id) > 0)
      .collect();

    if active_nodes.is_empty() {
      return self.nodes.keys().next().copied();
    }

    let num_nodes = active_nodes.len();
    if num_nodes == 1 {
      return active_nodes.first().copied();
    }

    // 优先检查标准槽位划分 (如 3 节点时: 0..5460, 5461..10922, 10923..16383)
    for (i, &node_id) in active_nodes.iter().enumerate() {
      let (start, end) = Self::calc_node_slot_range(i, num_nodes);
      if slot >= start && slot <= end {
        return Some(node_id);
      }
    }

    // 回退按虚拟分片组查找
    let total_shards = self.shards.len();
    if total_shards > 0 {
      let shard_idx = (slot as usize * total_shards) / 16384;
      if let Some(shard) = self.shards.get(shard_idx)
        && shard.leader_node_id != 0
        && self.nodes.contains_key(&shard.leader_node_id)
      {
        return Some(shard.leader_node_id);
      }
    }

    active_nodes.first().copied()
  }

  /// 获取指定 Master 节点所负责的所有槽位区间
  pub fn get_node_slot_ranges(&self, node_id: u64) -> Vec<(u32, u32)> {
    let active_nodes: Vec<u64> = self
      .nodes
      .keys()
      .copied()
      .filter(|&id| self.get_node_weight(id) > 0)
      .collect();

    if active_nodes.is_empty() {
      if self.nodes.contains_key(&node_id) {
        return vec![(0, 16383)];
      }
      return Vec::new();
    }

    let num_nodes = active_nodes.len();
    if num_nodes > 1
      && let Some(pos) = active_nodes.iter().position(|&id| id == node_id)
    {
      return vec![Self::calc_node_slot_range(pos, num_nodes)];
    }

    let total_shards = self.shards.len();
    if total_shards == 0 {
      if node_id == 1 || self.nodes.first_key_value().map(|(k, _)| *k) == Some(node_id) {
        return vec![(0, 16383)];
      }
      return Vec::new();
    }

    let mut ranges = Vec::new();
    for (idx, shard) in self.shards.iter().enumerate() {
      if shard.leader_node_id == node_id {
        let start_slot = (idx as u64 * 16384 / total_shards as u64) as u32;
        let end_slot = (((idx + 1) as u64 * 16384 / total_shards as u64) - 1) as u32;
        ranges.push((start_slot, end_slot));
      }
    }

    // 如果没有分片显式分配给该节点，但该节点是唯一的已知节点或第一个节点，默认承担全部槽位
    if ranges.is_empty() && self.nodes.len() == 1 && self.nodes.contains_key(&node_id) {
      ranges.push((0, 16383));
    }

    ranges
  }

  /// 生成对标 Redis Cluster 的 CLUSTER SLOTS 响应格式
  pub fn to_cluster_slots_resp(&self) -> RespValue {
    let total_shards = self.shards.len();
    if total_shards == 0 {
      let master_id = 1;
      let master_addr = self
        .nodes
        .get(&master_id)
        .map(|s| s.as_str())
        .unwrap_or("127.0.0.1:6379");
      let (m_ip, m_port) = match master_addr.rsplit_once(':') {
        Some((h, p)) => (h, p.parse::<i64>().unwrap_or(0)),
        None => (master_addr, 0),
      };
      let default_entry = RespValue::Arr(vec![
        RespValue::Int(0),
        RespValue::Int(16383),
        RespValue::Arr(vec![
          RespValue::Blob(m_ip.as_bytes().to_vec()),
          RespValue::Int(m_port),
          RespValue::Blob(format!("{master_id:040x}").into_bytes()),
        ]),
      ]);
      return RespValue::Arr(vec![default_entry]);
    }

    let mut slot_arr = Vec::with_capacity(total_shards);
    for (idx, shard) in self.shards.iter().enumerate() {
      let start_slot = (idx as u64 * 16384 / total_shards as u64) as i64;
      let end_slot = (((idx + 1) as u64 * 16384 / total_shards as u64) - 1) as i64;

      let mut node_info_list = Vec::with_capacity(shard.replicas.len().max(1));
      // 主节点 (Master) 信息排在首位
      let master_id = shard.leader_node_id;
      let master_addr = self
        .nodes
        .get(&master_id)
        .map(|s| s.as_str())
        .unwrap_or("127.0.0.1:0");
      let (m_ip, m_port) = match master_addr.rsplit_once(':') {
        Some((h, p)) => (h, p.parse::<i64>().unwrap_or(0)),
        None => (master_addr, 0),
      };
      node_info_list.push(RespValue::Arr(vec![
        RespValue::Blob(m_ip.as_bytes().to_vec()),
        RespValue::Int(m_port),
        RespValue::Blob(format!("{master_id:040x}").into_bytes()),
      ]));

      // 从副本节点 (Replica) 依次排列
      for &rep_id in &shard.replicas {
        if rep_id == master_id {
          continue;
        }
        let rep_addr = self
          .nodes
          .get(&rep_id)
          .map(|s| s.as_str())
          .unwrap_or("127.0.0.1:0");
        let (r_ip, r_port) = match rep_addr.rsplit_once(':') {
          Some((h, p)) => (h, p.parse::<i64>().unwrap_or(0)),
          None => (rep_addr, 0),
        };
        node_info_list.push(RespValue::Arr(vec![
          RespValue::Blob(r_ip.as_bytes().to_vec()),
          RespValue::Int(r_port),
          RespValue::Blob(format!("{rep_id:040x}").into_bytes()),
        ]));
      }

      let mut slot_entry = vec![RespValue::Int(start_slot), RespValue::Int(end_slot)];
      slot_entry.extend(node_info_list);
      slot_arr.push(RespValue::Arr(slot_entry));
    }

    RespValue::Arr(slot_arr)
  }

  /// 生成对标 Redis Cluster 的 CLUSTER SHARDS 响应格式
  pub fn to_cluster_shards_resp(&self) -> RespValue {
    let k_id = RespValue::Simple("id".to_string());
    let k_addr = RespValue::Simple("addr".to_string());
    let k_ip = RespValue::Simple("ip".to_string());
    let k_port = RespValue::Simple("port".to_string());
    let k_role = RespValue::Simple("role".to_string());
    let k_health = RespValue::Simple("health".to_string());
    let v_online = RespValue::Simple("online".to_string());
    let v_master = RespValue::Simple("master".to_string());
    let v_replica = RespValue::Simple("replica".to_string());
    let k_slots = RespValue::Simple("slots".to_string());
    let k_nodes = RespValue::Simple("nodes".to_string());

    let total_shards = self.shards.len();
    if total_shards == 0 {
      let master_id = 1;
      let master_addr = self
        .nodes
        .get(&master_id)
        .map(|s| s.as_str())
        .unwrap_or("127.0.0.1:6379");
      let (ip, port) = match master_addr.rsplit_once(':') {
        Some((h, p)) => (h, p.parse::<i64>().unwrap_or(0)),
        None => (master_addr, 0),
      };
      let shard_map = vec![
        (
          k_slots,
          RespValue::Arr(vec![RespValue::Int(0), RespValue::Int(16383)]),
        ),
        (
          k_nodes,
          RespValue::Arr(vec![RespValue::Map(vec![
            (k_id, RespValue::Simple(format!("node_{master_id}"))),
            (k_addr, RespValue::Simple(master_addr.to_string())),
            (k_ip, RespValue::Simple(ip.to_string())),
            (k_port, RespValue::Int(port)),
            (k_role, v_master),
            (k_health, v_online),
          ])]),
        ),
      ];
      return RespValue::Arr(vec![RespValue::Map(shard_map)]);
    }

    let mut shard_arr = Vec::with_capacity(total_shards);
    for (idx, shard) in self.shards.iter().enumerate() {
      let start_slot = (idx as u64 * 16384 / total_shards as u64) as i64;
      let end_slot = (((idx + 1) as u64 * 16384 / total_shards as u64) - 1) as i64;

      let slots_pair = vec![RespValue::Int(start_slot), RespValue::Int(end_slot)];

      let mut node_maps = Vec::with_capacity(shard.replicas.len().max(1));
      // 主节点 (Master) 排在首位
      let master_id = shard.leader_node_id;
      let master_addr = self
        .nodes
        .get(&master_id)
        .map(|s| s.as_str())
        .unwrap_or("127.0.0.1:0");
      let (m_ip, m_port) = match master_addr.rsplit_once(':') {
        Some((h, p)) => (h, p.parse::<i64>().unwrap_or(0)),
        None => (master_addr, 0),
      };
      node_maps.push(RespValue::Map(vec![
        (k_id.clone(), RespValue::Simple(format!("node_{master_id}"))),
        (k_addr.clone(), RespValue::Simple(master_addr.to_string())),
        (k_ip.clone(), RespValue::Simple(m_ip.to_string())),
        (k_port.clone(), RespValue::Int(m_port)),
        (k_role.clone(), v_master.clone()),
        (k_health.clone(), v_online.clone()),
      ]));

      for &rep_id in &shard.replicas {
        if rep_id == master_id {
          continue;
        }
        let addr = self
          .nodes
          .get(&rep_id)
          .map(|s| s.as_str())
          .unwrap_or("127.0.0.1:0");
        let (ip, port) = match addr.rsplit_once(':') {
          Some((h, p)) => (h, p.parse::<i64>().unwrap_or(0)),
          None => (addr, 0),
        };
        node_maps.push(RespValue::Map(vec![
          (k_id.clone(), RespValue::Simple(format!("node_{rep_id}"))),
          (k_addr.clone(), RespValue::Simple(addr.to_string())),
          (k_ip.clone(), RespValue::Simple(ip.to_string())),
          (k_port.clone(), RespValue::Int(port)),
          (k_role.clone(), v_replica.clone()),
          (k_health.clone(), v_online.clone()),
        ]));
      }

      let shard_map = vec![
        (k_slots.clone(), RespValue::Arr(slots_pair)),
        (k_nodes.clone(), RespValue::Arr(node_maps)),
      ];
      shard_arr.push(RespValue::Map(shard_map));
    }
    RespValue::Arr(shard_arr)
  }

  /// 生成对标 Redis Cluster 的 CLUSTER NODES 响应格式
  pub fn to_cluster_nodes_resp(&self, my_node_id: u64) -> RespValue {
    let mut lines = String::with_capacity(self.nodes.len() * 128);
    for (&node_id, addr) in &self.nodes {
      let slot_ranges = self.get_node_slot_ranges(node_id);
      let is_master = !slot_ranges.is_empty();

      let flags = match (node_id == my_node_id, is_master) {
        (true, true) => "myself,master",
        (true, false) => "myself,slave",
        (false, true) => "master",
        (false, false) => "slave",
      };

      let master_field = if is_master {
        "-".to_string()
      } else {
        // 寻找该副本所属的主节点 ID
        let m_id = self
          .shards
          .iter()
          .find(|s| s.replicas.contains(&node_id))
          .map(|s| s.leader_node_id)
          .unwrap_or(1);
        format!("{m_id:040x}")
      };

      let (ip, port) = match addr.rsplit_once(':') {
        Some((h, p)) => (h, p.parse::<u16>().unwrap_or(0)),
        None => (addr.as_str(), 0),
      };
      let cport = (port as u32) + 10000;
      let slots_str = if is_master {
        format_slot_ranges(slot_ranges)
      } else {
        String::new()
      };

      if slots_str.is_empty() {
        let _ = writeln!(
          lines,
          "{node_id:040x} {ip}:{port}@{cport} {flags} {master_field} 0 0 1 connected"
        );
      } else {
        let _ = writeln!(
          lines,
          "{node_id:040x} {ip}:{port}@{cport} {flags} {master_field} 0 0 1 connected {slots_str}"
        );
      }
    }
    RespValue::Blob(lines.into_bytes())
  }

  /// 生成对标 Redis Cluster 的 CLUSTER INFO 响应格式
  pub fn to_cluster_info_resp(&self) -> RespValue {
    let total_nodes = self.nodes.len();
    let info = format!(
      "cluster_state:ok\r\ncluster_slots_assigned:16384\r\ncluster_slots_ok:16384\r\ncluster_slots_pfail:0\r\ncluster_slots_fail:0\r\ncluster_known_nodes:{total_nodes}\r\ncluster_size:{total_nodes}\r\ncluster_current_epoch:1\r\ncluster_my_epoch:1\r\ncluster_stats_messages_sent:0\r\ncluster_stats_messages_received:0\r\n"
    );
    RespValue::Blob(info.into_bytes())
  }
}

/// 合并连续的槽位区间并格式化为标准 Redis Cluster 槽位字符串（如 "0-16383" 或 "0-5460 5461-10922"）
pub fn format_slot_ranges(mut ranges: Vec<(u32, u32)>) -> String {
  if ranges.is_empty() {
    return String::new();
  }
  ranges.sort_unstable_by_key(|r| r.0);
  let mut merged: Vec<(u32, u32)> = Vec::with_capacity(ranges.len());
  for (start, end) in ranges {
    if let Some(last) = merged.last_mut() {
      if start <= last.1 + 1 {
        last.1 = last.1.max(end);
      } else {
        merged.push((start, end));
      }
    } else {
      merged.push((start, end));
    }
  }
  let mut s = String::new();
  for (i, (start, end)) in merged.into_iter().enumerate() {
    if i > 0 {
      s.push(' ');
    }
    if start == end {
      let _ = write!(s, "{start}");
    } else {
      let _ = write!(s, "{start}-{end}");
    }
  }
  s
}
