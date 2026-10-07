//! 检索输出缓冲与物化结果（对标 diskann-garnet lib.rs SearchResults：
//! i32 长度前缀 id 流 + 距离流，内联/溢出双缓冲共用同一写出协议）

use super::*;

/// 检索输出缓冲（对标 diskann-garnet lib.rs SearchResults）。
///
/// 全部 id 均按 [`write_length_prefixed`] 的 4 字节长度前缀协议串接；
/// `ids` 容量不足时余项进入 overflow 缓冲（C# 侧 overflow_results 语义；
/// wnode 以 [`SearchOutput`] 一次性合并承载，无需分页续检），溢出缓冲
/// 与内联缓冲共用同一编码，消费端 [`LengthPrefixedIter`] 可无差别解包。
pub struct SearchResults<'a> {
  k: usize,
  ids: &'a mut [u8],
  dists: &'a mut [f32],
  index: usize,
  id_index: usize,
  overflow_ids: Vec<u8>,
  overflow_dists: Vec<f32>,
}

impl<'a> SearchResults<'a> {
  /// 以输出缓冲构造（k = 期望结果数）。
  pub fn new(k: usize, ids: &'a mut [u8], dists: &'a mut [f32]) -> Self {
    Self {
      k,
      ids,
      dists,
      index: 0,
      id_index: 0,
      overflow_ids: Vec::new(),
      overflow_dists: Vec::new(),
    }
  }

  /// 仅推送 id（random_members 用，距离为 0）。
  pub fn push_id(&mut self, id: VectorSetId) -> BufferState {
    self.push(Neighbor::new(id, 0.0))
  }

  /// 缓冲是否溢出（余项在 overflow 中）。
  fn overflowing(&self) -> bool {
    !self.overflow_ids.is_empty()
  }

  /// 把缓冲内结果 + overflow 物化合并为单一输出结构。
  pub fn into_search_output(self) -> SearchOutput {
    let found = self.pushed();
    let mut ids = Vec::with_capacity(self.id_index + self.overflow_ids.len());
    ids.extend_from_slice(&self.ids[..self.id_index]);
    ids.extend_from_slice(&self.overflow_ids);

    let mut dists = Vec::with_capacity(self.index + self.overflow_dists.len());
    dists.extend_from_slice(&self.dists[..self.index]);
    dists.extend_from_slice(&self.overflow_dists);
    SearchOutput {
      ids,
      distances: dists,
      found,
    }
  }

  /// 已推送元素数（内联 + 溢出，`current_len` 契约的单一实现，
  /// 与 `is_full` / `size_hint` 闭环）。
  #[inline]
  fn pushed(&self) -> usize {
    self.index + self.overflow_dists.len()
  }

  fn is_full(&self) -> bool {
    self.pushed() >= self.k
  }
}

impl SearchOutputBuffer<VectorSetId> for SearchResults<'_> {
  fn size_hint(&self) -> Option<usize> {
    Some(self.k - self.pushed())
  }

  fn push(&mut self, neighbor: Neighbor<VectorSetId>) -> BufferState {
    let (id, distance) = neighbor.as_tuple();
    let id_bytes = id.as_key_bytes();
    let total = ID_PREFIX_BYTES + id_bytes.len();

    if self.is_full() {
      return BufferState::Full;
    }
    if self.overflowing()
      || self.index >= self.dists.len()
      || self.id_index + total > self.ids.len()
    {
      // 溢出缓冲与内联缓冲共用同一写出协议
      let start = self.overflow_ids.len();
      self.overflow_ids.resize(start + total, 0);
      write_length_prefixed(&mut self.overflow_ids[start..], id_bytes);
      self.overflow_dists.push(distance);

      return if self.is_full() {
        BufferState::Full
      } else {
        BufferState::Available
      };
    }

    write_length_prefixed(
      &mut self.ids[self.id_index..self.id_index + total],
      id_bytes,
    );
    self.dists[self.index] = distance;
    self.index += 1;
    self.id_index += total;

    if self.is_full() {
      BufferState::Full
    } else {
      BufferState::Available
    }
  }

  fn current_len(&self) -> usize {
    self.pushed()
  }

  fn extend<Itr>(&mut self, itr: Itr) -> usize
  where
    Itr: IntoIterator<Item = Neighbor<VectorSetId>>,
  {
    let initial = self.current_len();

    for neighbor in itr {
      if self.push(neighbor).is_full() {
        break;
      }
    }

    self.current_len() - initial
  }
}

/// 已物化的检索输出（i32 长度前缀 id 流 + 距离流）。
#[derive(Debug, PartialEq)]
pub struct SearchOutput {
  /// 命中元素 id（4 字节长度前缀串接）。
  pub ids: Vec<u8>,
  /// 命中距离（与 id 一一对应）。
  pub distances: Vec<f32>,
  /// 命中数。
  pub found: usize,
}

impl SearchOutput {
  /// 迭代各命中项（id 切片, 距离）。
  pub fn iter(&self) -> impl Iterator<Item = (&[u8], f32)> {
    LengthPrefixedIter::new(&self.ids).zip(self.distances.iter().copied())
  }

  /// 物化为 SearchHit 列表。
  pub fn hits(&self) -> Vec<SearchHit> {
    self
      .iter()
      .map(|(id, distance)| SearchHit {
        external_id: id.to_vec(),
        distance,
      })
      .collect()
  }
}

/// 检索单条命中。
#[derive(Debug, PartialEq)]
pub struct SearchHit {
  pub external_id: Vec<u8>,
  pub distance: f32,
}
