//! 最小可用 HNSW 图索引（对标 libs/server/Resp/Vector/DiskANNService.cs 背后的原生索引语义）
//!
//! wkv 无向量索引原语，DiskANN 原生库亦不可用；
//! 按本周期规范在本域内自建最小可用 HNSW（分层可导航小世界图），
//! 承载 C# 侧经 P/Invoke 委托给 diskann_garnet 的插入/删除/搜索/量化语义：
//! - 增删查：标记删除 + ef 限制的贪心搜索
//! - 距离度量：Cosine / InnerProduct / L2（+ 归一化余弦）
//! - 量化：Q8 标量量化（需建表）、Bin/XBin 符号二值量化、NoQuant/X 原样存储

use std::{
  cmp::{Ordering, Reverse},
  collections::BinaryHeap,
  mem,
};

use super::vector_types::{VectorDistanceMetricType, VectorQuantType};

/// 内部 id 非法哨兵。
pub const INVALID_INTERNAL_ID: u32 = u32::MAX;

/// 单条邻接边的上限倍数（层 0 为 `num_links * MUL_M0`，对齐 HNSW 惯例）。
const MUL_M0: usize = 2;

/// 二值量化的符号判定阈值。
const BIN_SIGN_THRESHOLD: f32 = 0.0;

/// 按向量存储格式返回每元素字节数（Bin 系为位打包，此处按 f32 字节计）。
fn native_element_size(quant: VectorQuantType) -> usize {
  match quant {
    VectorQuantType::XnoQuantU8 | VectorQuantType::XbinU8 => 1,
    VectorQuantType::XnoQuantI8 | VectorQuantType::XbinI8 => 1,
    _ => 4,
  }
}

/// HNSW 索引配置（对标 VectorManager.Index 的几何字段）。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct HnswConfig {
  /// 向量维度。
  pub dims: u32,
  /// 降维后维度（0 = 不降维）。
  pub reduce_dims: u32,
  /// 量化类型。
  pub quant: VectorQuantType,
  /// 距离度量。
  pub metric: VectorDistanceMetricType,
  /// 构建期探索因子（ef_construction）。
  pub build_exploration_factor: u32,
  /// 每层链接数（M）。
  pub num_links: u32,
}

impl HnswConfig {
  /// 构造 HNSW 索引配置。
  pub const fn new(
    dims: u32,
    reduce_dims: u32,
    quant: VectorQuantType,
    metric: VectorDistanceMetricType,
    build_exploration_factor: u32,
    num_links: u32,
  ) -> Self {
    Self {
      dims,
      reduce_dims,
      quant,
      metric,
      build_exploration_factor,
      num_links,
    }
  }
}

/// 单个图节点。
#[derive(Debug, Clone, Default)]
struct Node {
  /// 原生格式存储的向量字节（f32 LE / u8 / i8；Bin 系量化后仍按原生元素存储符号值）。
  vector: Vec<u8>,
  /// 各层邻接表；`links[0]` 为层 0，上限 `2 * num_links`。
  links: Vec<Vec<u32>>,
  /// 标记删除（VREM 后拓扑移除，槽位保留）。
  deleted: bool,
}

/// 候选队列项：距离越小越近。
#[derive(PartialEq, Debug, Clone)]
struct Candidate {
  dist: f32,
  id: u32,
}

impl Eq for Candidate {}

impl Ord for Candidate {
  fn cmp(&self, other: &Self) -> Ordering {
    self
      .dist
      .partial_cmp(&other.dist)
      .unwrap_or(Ordering::Equal)
  }
}

impl PartialOrd for Candidate {
  fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
    Some(self.cmp(other))
  }
}

/// 最小可用 HNSW 索引。
#[derive(Debug, Clone)]
pub struct HnswIndex {
  config: HnswConfig,
  /// 图节点槽位（含已删除标记位）。
  nodes: Vec<Node>,
  /// 可复用的已删除槽位。
  free_slots: Vec<u32>,
  /// 入口点内部 id。
  entry: u32,
  /// 当前最高层。
  max_level: i32,
  /// 活跃（未删除）元素数。
  live: usize,
  /// Q8 量化表：每维 (min, max)；None = 尚未建表。
  q8_table: Option<Vec<(f32, f32)>>,
  /// 已建表前插入、等待回填量化的向量数。
  pending_quantization: usize,
}

impl HnswIndex {
  /// 创建空索引。
  pub fn new(config: HnswConfig) -> Self {
    let num_links = config.num_links.max(1);
    Self {
      config: HnswConfig {
        num_links,
        ..config
      },
      nodes: Vec::new(),
      free_slots: Vec::new(),
      entry: INVALID_INTERNAL_ID,
      max_level: -1,
      live: 0,
      q8_table: None,
      pending_quantization: 0,
    }
  }

  /// 索引几何配置。
  pub fn config(&self) -> &HnswConfig {
    &self.config
  }

  /// 活跃元素数（VCARD 语义）。
  pub fn len(&self) -> usize {
    self.live
  }

  /// 是否为空。
  pub fn is_empty(&self) -> bool {
    self.live == 0
  }

  /// 内部 id 是否有效（未删除且在界内）。
  pub fn is_internal_id_valid(&self, internal_id: u32) -> bool {
    self
      .nodes
      .get(internal_id as usize)
      .is_some_and(|n| !n.deleted)
  }

  /// 读取指定内部 id 的向量字节。
  pub fn vector_of(&self, internal_id: u32) -> Option<&[u8]> {
    self
      .nodes
      .get(internal_id as usize)
      .filter(|n| !n.deleted)
      .map(|n| n.vector.as_slice())
  }

  /// 指定内部 id 的层 0 邻接（VLINKS 语义）。
  pub fn links_of(&self, internal_id: u32) -> Option<&[u32]> {
    self
      .nodes
      .get(internal_id as usize)
      .filter(|n| !n.deleted)
      .and_then(|n| n.links.first().map(|l| l.as_slice()))
  }

  /// 随机抽取 `count` 个活跃内部 id（VRANDMEMBER 语义；算法 R 水库抽样，不放回）。
  pub fn sample(&self, count: usize) -> Vec<u32> {
    if count == 0 {
      return Vec::new();
    }
    let mut rng = fastrand::Rng::new();
    let mut reservoir: Vec<u32> = Vec::with_capacity(count);
    let mut seen = 0usize;
    for (i, n) in self.nodes.iter().enumerate() {
      if n.deleted {
        continue;
      }
      if reservoir.len() < count {
        reservoir.push(i as u32);
      } else {
        // 以 count/(seen+1) 概率替换池内元素
        let j = rng.usize(..=seen);
        if j < count {
          reservoir[j] = i as u32;
        }
      }
      seen += 1;
    }
    reservoir
  }

  /// Q8 量化表访问器。
  pub fn quant_table(&self) -> Option<&[(f32, f32)]> {
    self.q8_table.as_deref()
  }

  /// 重建语义：清空图并套用新几何（CreateIndex 对既有 context 的 RecreateIndex）。
  pub fn clear_for_recreate(&mut self, config: HnswConfig) {
    let num_links = config.num_links.max(1);
    self.config = HnswConfig {
      num_links,
      ..config
    };
    self.nodes.clear();
    self.free_slots.clear();
    self.entry = INVALID_INTERNAL_ID;
    self.max_level = -1;
    self.live = 0;
    self.q8_table = None;
    self.pending_quantization = 0;
  }

  /// Q8 量化表是否就绪。
  pub fn quant_table_ready(&self) -> bool {
    match self.config.quant {
      VectorQuantType::Q8 => self.q8_table.is_some(),
      VectorQuantType::NoQuant | VectorQuantType::XnoQuantU8 | VectorQuantType::XnoQuantI8 => true,
      // Bin 系为符号量化，无需数据驱动的建表
      VectorQuantType::Bin | VectorQuantType::XbinU8 | VectorQuantType::XbinI8 => true,
      VectorQuantType::Invalid => false,
    }
  }

  /// 待回填量化的向量数。
  pub fn pending_quantization(&self) -> usize {
    self.pending_quantization
  }

  /// libs/server/Resp/Vector/DiskANNService.cs:build_quant_table
  ///
  /// 依据现有向量构建 Q8 每维 (min, max) 表，并回填全部待量化向量。
  pub fn build_quant_table(&mut self) -> bool {
    if self.config.quant != VectorQuantType::Q8 {
      // 非 Q8 无需建表
      return true;
    }
    if self.q8_table.is_some() {
      return true;
    }

    let dims = self.config.dims as usize;
    let mut table = vec![(f32::INFINITY, f32::NEG_INFINITY); dims];
    for node in &self.nodes {
      if node.deleted {
        continue;
      }
      let vals = decode_native(&node.vector, self.config.quant);
      for (i, v) in vals.iter().enumerate().take(dims) {
        let e = &mut table[i];
        if *v < e.0 {
          e.0 = *v;
        }
        if *v > e.1 {
          e.1 = *v;
        }
      }
    }
    // 无数据时给平凡区间，避免除零
    for e in &mut table {
      if e.0 > e.1 {
        *e = (0.0, 0.0);
      } else if (e.1 - e.0).abs() < f32::EPSILON {
        e.1 = e.0 + 1.0;
      }
    }

    // 就地量化所有仍以 f32 存储的向量（建表前插入的待量化池）
    for node in &mut self.nodes {
      if node.deleted || node.vector.len() != dims * 4 {
        continue;
      }
      let vals = decode_native(&node.vector, VectorQuantType::NoQuant);
      node.vector = quantize_q8(&vals, &table);
    }
    self.pending_quantization = 0;
    self.q8_table = Some(table);
    true
  }

  /// 插入向量（原生格式字节），返回新内部 id。
  pub fn insert(&mut self, vector_bytes: &[u8], fastrand_state: &mut fastrand::Rng) -> u32 {
    let dims = self.config.dims as usize;
    debug_assert_eq!(
      vector_bytes.len(),
      dims * native_element_size(self.config.quant)
    );

    let level = self.random_level(fastrand_state);
    let id = self.alloc_slot(level);
    // Q8 已建表：就地量化存储（与回填节点同格式）；未建表：暂存 f32，
    // 计入待量化池（build_quant_table 时回填）
    self.nodes[id as usize].vector = match (self.config.quant, self.q8_table.as_deref()) {
      (VectorQuantType::Q8, Some(table)) => quantize_q8(
        &decode_native(vector_bytes, VectorQuantType::NoQuant),
        table,
      ),
      _ => vector_bytes.to_vec(),
    };
    if self.config.quant == VectorQuantType::Q8 && self.q8_table.is_none() {
      self.pending_quantization += 1;
    }

    // 空图：直接成为入口
    if self.entry == INVALID_INTERNAL_ID {
      self.entry = id;
      self.max_level = level as i32;
      self.live += 1;
      return id;
    }

    let query = decode_native(vector_bytes, self.config.quant);
    let mut entry = self.entry;
    let mut ep_dist = self.distance_to(&query, self.entry);

    // 上层贪心下降（ef = 1）
    for lc in (level as i32 + 1..=self.max_level).rev() {
      let mut changed = true;
      while changed {
        changed = false;
        let Some(links) = self.nodes[entry as usize].links.get(lc as usize) else {
          break;
        };
        for &cand in links {
          let d = self.distance_to(&query, cand);
          if d < ep_dist {
            ep_dist = d;
            entry = cand;
            changed = true;
          }
        }
      }
    }

    // 逐层插入
    let ef = self
      .config
      .build_exploration_factor
      .max(self.config.num_links) as usize;
    let mut ep = entry;
    let mut ep_d = ep_dist;
    for lc in (0..=level as i32).rev() {
      let candidates = self.search_layer(&query, ep, ep_d, ef, lc, &mut |_| true);
      let m_max = if lc == 0 {
        self.config.num_links as usize * MUL_M0
      } else {
        self.config.num_links as usize
      };
      let selected = select_neighbors(&candidates, self.config.num_links as usize);

      let new_id = id;
      for cand in &selected {
        self.nodes[new_id as usize].links[lc as usize].push(cand.id);
        self.nodes[cand.id as usize].links[lc as usize].push(new_id);
        // 邻接超限收缩
        self.shrink_links(cand.id, lc as usize, m_max);
      }
      if let Some(first) = selected.first() {
        ep = first.id;
        ep_d = first.dist;
      }
    }

    if level as i32 > self.max_level {
      self.max_level = level as i32;
      self.entry = id;
    }

    self.live += 1;
    id
  }

  /// 标记删除（标记位 + 邻接摘除），返回是否确实存在。
  pub fn remove(&mut self, internal_id: u32) -> bool {
    if !self.is_internal_id_valid(internal_id) {
      return false;
    }
    self.nodes[internal_id as usize].deleted = true;
    let levels = mem::take(&mut self.nodes[internal_id as usize].links);
    self.nodes[internal_id as usize].links = vec![Vec::new(); levels.len()];
    for level_links in &levels {
      for nbr in level_links {
        if let Some(n) = self.nodes.get_mut(*nbr as usize) {
          n.links
            .iter_mut()
            .for_each(|l| l.retain(|x| *x != internal_id));
        }
      }
    }
    self.live -= 1;
    self.free_slots.push(internal_id);

    if self.live == 0 {
      self.entry = INVALID_INTERNAL_ID;
      self.max_level = -1;
    } else if self.entry == internal_id {
      // 入口被删：任取一个存活节点作为新入口
      self.entry = self.nodes.iter().position(|n| !n.deleted).unwrap_or(0) as u32;
    }
    true
  }

  /// K 近邻检索；`query` 为真实值空间查询向量，`filter` 为内部 id 谓词
  /// （内联过滤通道）。返回按距离升序的 (内部 id, 距离) 列表。
  pub fn search(
    &self,
    query: &[f32],
    k: usize,
    ef: usize,
    filter: &mut impl FnMut(u32) -> bool,
  ) -> Vec<(u32, f32)> {
    if self.entry == INVALID_INTERNAL_ID || k == 0 {
      return Vec::new();
    }

    let mut entry = self.entry;
    let mut ep_dist = self.distance_to(query, entry);

    // 上层贪心下降（无过滤：上层节点默认允许通过）
    for lc in (1..=self.max_level).rev() {
      let mut changed = true;
      while changed {
        changed = false;
        let Some(links) = self.nodes[entry as usize].links.get(lc as usize) else {
          break;
        };
        for &cand in links {
          if !self.is_internal_id_valid(cand) {
            continue;
          }
          let d = self.distance_to(query, cand);
          if d < ep_dist {
            ep_dist = d;
            entry = cand;
            changed = true;
          }
        }
      }
    }

    let mut candidates = self.search_layer(query, entry, ep_dist, ef.max(k), 0, filter);
    candidates.sort_by(|a, b| a.dist.partial_cmp(&b.dist).unwrap_or(Ordering::Equal));
    candidates
      .into_iter()
      .take(k)
      .map(|c| (c.id, c.dist))
      .collect()
  }

  /// 单层 best-first 搜索；返回候选集（按距离降序）。
  fn search_layer(
    &self,
    query: &[f32],
    entry: u32,
    entry_dist: f32,
    ef: usize,
    layer: i32,
    filter: &mut impl FnMut(u32) -> bool,
  ) -> Vec<Candidate> {
    let mut visited = gxhash::HashSet::default();
    // top_candidates 为 max-heap（顶点=最远，便于淘汰）
    let mut top_candidates: BinaryHeap<Candidate> = BinaryHeap::new();
    // 扩展集为 min-heap（顶点=最近，最近优先扩展）
    let mut candidate_set: BinaryHeap<Reverse<Candidate>> = BinaryHeap::new();

    visited.insert(entry);
    let entry_c = Candidate {
      dist: entry_dist,
      id: entry,
    };
    candidate_set.push(Reverse(entry_c.clone()));
    if filter(entry) {
      top_candidates.push(entry_c);
    }

    while let Some(Reverse(current)) = candidate_set.pop() {
      let worst = top_candidates.peek().map_or(f32::INFINITY, |c| c.dist);
      if current.dist > worst && top_candidates.len() >= ef {
        break;
      }

      let links: &[u32] = self.nodes[current.id as usize]
        .links
        .get(layer as usize)
        .map_or(&[], Vec::as_slice);
      for &nbr in links {
        if visited.contains(&nbr) || !self.is_internal_id_valid(nbr) {
          continue;
        }
        visited.insert(nbr);
        let d = self.distance_to(query, nbr);
        let worst = top_candidates.peek().map_or(f32::INFINITY, |c| c.dist);
        if top_candidates.len() < ef || d < worst {
          let c = Candidate { dist: d, id: nbr };
          candidate_set.push(Reverse(c.clone()));
          if filter(nbr) {
            top_candidates.push(c);
            if top_candidates.len() > ef {
              top_candidates.pop();
            }
          }
        }
      }
    }

    let mut out: Vec<Candidate> = top_candidates.into_iter().collect();
    out.sort_by(|a, b| b.dist.partial_cmp(&a.dist).unwrap_or(Ordering::Equal));
    out
  }

  /// 收缩指定节点某层邻接至 `m_max`（保留最近的；低频路径，允许一次解码分配）。
  fn shrink_links(&mut self, id: u32, layer: usize, m_max: usize) {
    let links = mem::take(&mut self.nodes[id as usize].links[layer]);
    if links.len() <= m_max {
      self.nodes[id as usize].links[layer] = links;
      return;
    }
    let vec_self = decode_values(
      &self.nodes[id as usize].vector,
      self.config.quant,
      self.q8_table.as_deref(),
    );
    let mut scored: Vec<(u32, f32)> = links
      .iter()
      .map(|&nbr| (nbr, self.distance_to(&vec_self, nbr)))
      .collect();
    scored.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap_or(Ordering::Equal));
    self.nodes[id as usize].links[layer] =
      scored.into_iter().take(m_max).map(|(nbr, _)| nbr).collect();
  }

  /// 计算查询向量（真实值空间）与内部 id 节点的距离（零分配）。
  fn distance_to(&self, query: &[f32], id: u32) -> f32 {
    self.distance_to_bytes(query, &self.nodes[id as usize].vector)
  }

  /// 查询真实值 vs 存储原生字节：按量化方式现场解码（Q8 已建表时逐维反量化），
  /// 不再展开为中间 Vec（检索热路径每次距离计算省一次堆分配）。
  fn distance_to_bytes(&self, query: &[f32], bytes: &[u8]) -> f32 {
    let metric = self.config.metric;
    match (self.config.quant, self.q8_table.as_deref()) {
      (VectorQuantType::Q8, Some(table)) => distance_against(
        query,
        table.len(),
        |i| {
          let (min, max) = table[i];
          min + (f32::from(bytes[i]) / 255.0) * (max - min)
        },
        metric,
      ),
      (q, _) => {
        let size = native_element_size(q);
        distance_against(
          query,
          bytes.len() / size,
          |i| match q {
            VectorQuantType::XnoQuantU8 | VectorQuantType::XbinU8 => f32::from(bytes[i]),
            VectorQuantType::XnoQuantI8 | VectorQuantType::XbinI8 => {
              f32::from(i8::from_le_bytes([bytes[i]]))
            }
            _ => f32::from_le_bytes([
              bytes[i * 4],
              bytes[i * 4 + 1],
              bytes[i * 4 + 2],
              bytes[i * 4 + 3],
            ]),
          },
          metric,
        )
      }
    }
  }

  /// 分配节点槽位并按层数初始化邻接表（向量随后写入）。
  fn alloc_slot(&mut self, level: u8) -> u32 {
    let links = (0..=level as usize)
      .map(|l| Vec::with_capacity(Self::m_for(l)))
      .collect();
    let node = Node {
      vector: Vec::new(),
      links,
      deleted: false,
    };
    match self.free_slots.pop() {
      Some(slot) => {
        self.nodes[slot as usize] = node;
        slot
      }
      None => {
        self.nodes.push(node);
        (self.nodes.len() - 1) as u32
      }
    }
  }

  /// 各层邻接表预分配容量（层 0 上限 2M，按典型 M≥4 估计；上层按 M 的一半）。
  fn m_for(layer: usize) -> usize {
    if layer == 0 { 8 } else { 4 }
  }

  /// 几何随机层数（1/(ln M) 指数衰减）。
  fn random_level(&self, rng: &mut fastrand::Rng) -> u8 {
    let mult = 1.0 / (self.config.num_links as f64).ln().max(0.1);
    let r = -rng.f64().ln().max(f64::MIN_POSITIVE) * mult;
    r.clamp(0.0, 255.0) as u8
  }
}

/// 启发式邻居选择：按距离升序取前 m 个（最小可用实现）。
fn select_neighbors(candidates: &[Candidate], m: usize) -> Vec<Candidate> {
  let mut sorted: Vec<Candidate> = candidates.to_vec();
  sorted.sort_by(|a, b| a.dist.partial_cmp(&b.dist).unwrap_or(Ordering::Equal));
  sorted.truncate(m);
  sorted
}

/// 原生格式字节 → f32 向量。
pub fn decode_native(bytes: &[u8], quant: VectorQuantType) -> Vec<f32> {
  match quant {
    VectorQuantType::XnoQuantU8 | VectorQuantType::XbinU8 => {
      bytes.iter().map(|b| f32::from(*b)).collect()
    }
    VectorQuantType::XnoQuantI8 | VectorQuantType::XbinI8 => bytes
      .iter()
      .map(|b| f32::from(i8::from_le_bytes([*b])))
      .collect(),
    _ => bytes
      .as_chunks::<4>()
      .0
      .iter()
      .map(|c| f32::from_le_bytes([c[0], c[1], c[2], c[3]]))
      .collect(),
  }
}

/// Q8 量化：按每维 (min, max) 线性映射到 u8。
fn quantize_q8(values: &[f32], table: &[(f32, f32)]) -> Vec<u8> {
  values
    .iter()
    .zip(table.iter())
    .map(|(&v, &(min, max))| {
      let scale = 255.0 / (max - min).max(f32::EPSILON);
      let q = ((v - min) * scale).round().clamp(0.0, 255.0);
      q as u8
    })
    .collect()
}

/// Q8 反量化。
pub fn dequantize_q8(bytes: &[u8], table: &[(f32, f32)]) -> Vec<f32> {
  bytes
    .iter()
    .zip(table.iter())
    .map(|(&q, &(min, max))| min + (q as f32 / 255.0) * (max - min))
    .collect()
}

/// 距离度量实现（值越小越相似，对齐 DiskANN Metric 语义）。
pub fn distance(a: &[f32], b: &[f32], metric: VectorDistanceMetricType) -> f32 {
  distance_against(a, b.len(), |i| b[i], metric)
}

/// 距离度量核心：查询真实值 vs 逐维取值器（存储侧按量化现场解码，零中间分配）。
fn distance_against(
  query: &[f32],
  other_len: usize,
  other: impl Fn(usize) -> f32,
  metric: VectorDistanceMetricType,
) -> f32 {
  let n = query.len().min(other_len);
  let pairs = query.iter().take(n).zip((0..n).map(other));
  match metric {
    VectorDistanceMetricType::L2 => pairs
      .map(|(&a, b)| {
        let d = a - b;
        d * d
      })
      .sum(),
    // DiskANN 惯例：距离 = 1 - ip，保证越小越近
    VectorDistanceMetricType::InnerProduct => 1.0 - pairs.map(|(&a, b)| a * b).sum::<f32>(),
    VectorDistanceMetricType::Cosine | VectorDistanceMetricType::XCosineNormalized => {
      let (mut dot, mut na, mut nb) = (0.0f32, 0.0f32, 0.0f32);
      for (&a, b) in pairs {
        dot += a * b;
        na += a * a;
        nb += b * b;
      }
      if na == 0.0 || nb == 0.0 {
        return 1.0;
      }
      1.0 - dot / (na.sqrt() * nb.sqrt())
    }
  }
}

/// 原生格式字节 → 真实值空间 f32（Q8 需建表反量化；服务层嵌入/按元素检索共用）。
pub fn decode_values(
  bytes: &[u8],
  quant: VectorQuantType,
  table: Option<&[(f32, f32)]>,
) -> Vec<f32> {
  match (quant, table) {
    (VectorQuantType::Q8, Some(t)) => dequantize_q8(bytes, t),
    (q, _) => decode_native(bytes, q),
  }
}

/// 将查询/存储向量规约到索引的原生格式字节（量化入口）。
pub fn encode_native(
  values: &[f32],
  quant: VectorQuantType,
  table: Option<&[(f32, f32)]>,
) -> Vec<u8> {
  match quant {
    VectorQuantType::XnoQuantU8 => values.iter().map(|v| *v as u8).collect(),
    VectorQuantType::XnoQuantI8 => values.iter().map(|v| *v as i8 as u8).collect(),
    VectorQuantType::XbinU8 => values
      .iter()
      .map(|v| u8::from(*v > BIN_SIGN_THRESHOLD))
      .collect(),
    VectorQuantType::XbinI8 => values
      .iter()
      .map(|v| (*v > BIN_SIGN_THRESHOLD) as i8 as u8)
      .collect(),
    VectorQuantType::Bin => values
      .iter()
      .map(|v| u8::from(*v > BIN_SIGN_THRESHOLD))
      .collect(),
    VectorQuantType::Q8 => match table {
      Some(t) => quantize_q8(values, t),
      None => values.iter().flat_map(|v| v.to_le_bytes()).collect(),
    },
    _ => values.iter().flat_map(|v| v.to_le_bytes()).collect(),
  }
}

#[cfg(test)]
mod tests {
  use super::*;

  fn rng() -> fastrand::Rng {
    fastrand::Rng::with_seed(42)
  }

  fn f32_bytes(vals: &[f32]) -> Vec<u8> {
    vals.iter().flat_map(|v| v.to_le_bytes()).collect()
  }

  fn cfg(dims: u32, metric: VectorDistanceMetricType) -> HnswConfig {
    HnswConfig {
      dims,
      reduce_dims: 0,
      quant: VectorQuantType::NoQuant,
      metric,
      build_exploration_factor: 64,
      num_links: 8,
    }
  }

  #[test]
  fn insert_search_consistency_l2() {
    let mut idx = HnswIndex::new(cfg(3, VectorDistanceMetricType::L2));
    let mut rng = rng();

    // 插入 200 个随机向量
    let mut vectors = Vec::new();
    for i in 0..200u32 {
      let v = vec![i as f32, (i * 7 % 100) as f32, (i * 13 % 100) as f32];
      let id = idx.insert(&f32_bytes(&v), &mut rng);
      vectors.push((id, v));
    }
    assert_eq!(idx.len(), 200);

    // 精确最近邻应命中自身（或距离一致）
    for (id, v) in &vectors {
      let hits = idx.search(v, 1, 32, &mut |_| true);
      assert_eq!(hits.len(), 1);
      assert_eq!(hits[0].0, *id, "exact self match expected");
      assert!(hits[0].1.abs() < 1e-6);
    }
  }

  #[test]
  fn search_recall_on_clusters() {
    // 两个聚类的近邻检索召回验证
    let mut idx = HnswIndex::new(cfg(4, VectorDistanceMetricType::L2));
    let mut rng = rng();
    for i in 0..100u32 {
      let base = if i % 2 == 0 { 0.0 } else { 100.0 };
      let v: Vec<f32> = (0..4).map(|_d| base + (i % 17) as f32 * 0.01).collect();
      idx.insert(&f32_bytes(&v), &mut rng);
    }

    let q = vec![0.005, 0.005, 0.005, 0.005];
    let hits = idx.search(&q, 10, 64, &mut |_| true);
    assert_eq!(hits.len(), 10);
    // 前几名应来自 0 聚类（距离远小于另一聚类）
    assert!(hits[0].1 < 1.0);
  }

  #[test]
  fn remove_tombstones() {
    let mut idx = HnswIndex::new(cfg(2, VectorDistanceMetricType::L2));
    let mut rng = rng();
    let mut ids = Vec::new();
    for i in 0..50u32 {
      ids.push(idx.insert(&f32_bytes(&[i as f32, 0.0]), &mut rng));
    }
    // 删除一半
    for &id in &ids[..25] {
      assert!(idx.remove(id));
    }
    assert_eq!(idx.len(), 25);
    // 已删除 id 检索不可见
    let q = [ids[0] as f32, 0.0];
    let hits = idx.search(&q, 50, 64, &mut |_| true);
    assert!(!hits.iter().any(|(id, _)| ids[..25].contains(id)));
    // 重复删除失败
    assert!(!idx.remove(ids[0]));
    // 无效 id
    assert!(!idx.remove(9999));
  }

  #[test]
  fn metrics_values() {
    let a = [1.0, 0.0, 0.0];
    let b = [1.0, 0.0, 0.0];
    assert!((distance(&a, &b, VectorDistanceMetricType::L2)).abs() < 1e-6);
    assert!((distance(&a, &b, VectorDistanceMetricType::Cosine)).abs() < 1e-6);
    assert!((distance(&a, &b, VectorDistanceMetricType::InnerProduct)).abs() < 1e-6);

    let c = [0.0, 1.0, 0.0];
    assert!((distance(&a, &c, VectorDistanceMetricType::L2) - 2.0).abs() < 1e-6);
    assert!((distance(&a, &c, VectorDistanceMetricType::Cosine) - 1.0).abs() < 1e-6);
    assert!((distance(&a, &c, VectorDistanceMetricType::InnerProduct) - 1.0).abs() < 1e-6);

    // 归一化余弦与余弦一致
    assert!((distance(&a, &c, VectorDistanceMetricType::XCosineNormalized) - 1.0).abs() < 1e-6);
  }

  #[test]
  fn q8_quantization_roundtrip() {
    let mut idx = HnswIndex::new(HnswConfig {
      dims: 2,
      reduce_dims: 0,
      quant: VectorQuantType::Q8,
      metric: VectorDistanceMetricType::L2,
      build_exploration_factor: 32,
      num_links: 4,
    });
    let mut rng = rng();
    // 建表前插入 → 存 f32 原样，计入待量化
    let id1 = idx.insert(&f32_bytes(&[0.0, 10.0]), &mut rng);
    let id2 = idx.insert(&f32_bytes(&[10.0, 0.0]), &mut rng);
    assert!(!idx.quant_table_ready());
    assert_eq!(idx.pending_quantization(), 2);

    assert!(idx.build_quant_table());
    assert!(idx.quant_table_ready());
    assert_eq!(idx.pending_quantization(), 0);

    // 量化后检索仍可区分两个正交向量
    let hits = idx.search(&[0.0, 10.0], 1, 32, &mut |_| true);
    assert_eq!(hits[0].0, id1);
    let hits = idx.search(&[10.0, 0.0], 1, 32, &mut |_| true);
    assert_eq!(hits[0].0, id2);
  }

  #[test]
  fn binary_and_extended_quant_encodings() {
    // Bin：符号量化
    let enc = encode_native(&[-1.0, 2.0, 0.0], VectorQuantType::Bin, None);
    assert_eq!(enc, vec![0, 1, 0]);

    // XnoQuantU8 截断
    let enc = encode_native(&[0.0, 127.5, 255.0], VectorQuantType::XnoQuantU8, None);
    assert_eq!(enc, vec![0, 127, 255]);

    // XnoQuantI8 有符号
    let enc = encode_native(&[-5.0, 6.0], VectorQuantType::XnoQuantI8, None);
    assert_eq!(enc, vec![(-5i8) as u8, 6]);

    // 解码一致
    let dec = decode_native(&enc, VectorQuantType::XnoQuantI8);
    assert_eq!(dec, vec![-5.0, 6.0]);

    let f = f32_bytes(&[1.5, -2.5]);
    let dec = decode_native(&f, VectorQuantType::NoQuant);
    assert_eq!(dec, vec![1.5, -2.5]);
  }

  #[test]
  fn links_and_sample_views() {
    let mut idx = HnswIndex::new(cfg(2, VectorDistanceMetricType::L2));
    let mut rng = rng();
    let id0 = idx.insert(&f32_bytes(&[0.0, 0.0]), &mut rng);
    let id1 = idx.insert(&f32_bytes(&[1.0, 0.0]), &mut rng);
    idx.insert(&f32_bytes(&[0.0, 1.0]), &mut rng);

    assert!(idx.links_of(id1).is_some());
    assert_eq!(idx.sample(2).len(), 2);
    // 水库抽样：请求量超过活跃数时全量返回；count=0 返回空
    assert_eq!(idx.sample(10).len(), 3);
    assert_eq!(idx.sample(0).len(), 0);
    // 抽样只含活跃 id
    assert!(idx.sample(3).iter().all(|id| idx.is_internal_id_valid(*id)));
    assert!(idx.is_internal_id_valid(id0));
    assert!(!idx.is_internal_id_valid(INVALID_INTERNAL_ID));
    assert!(idx.vector_of(id0).is_some());
  }

  #[test]
  fn filter_predicate_excludes() {
    let mut idx = HnswIndex::new(cfg(2, VectorDistanceMetricType::L2));
    let mut rng = rng();
    let a = idx.insert(&f32_bytes(&[1.0, 1.0]), &mut rng);
    idx.insert(&f32_bytes(&[2.0, 2.0]), &mut rng);
    idx.insert(&f32_bytes(&[3.0, 3.0]), &mut rng);

    // 只允许 a 通过
    let hits = idx.search(&[1.1, 1.1], 3, 32, &mut |id| id == a);
    assert_eq!(hits.len(), 1);
    assert_eq!(hits[0].0, a);
  }
}
