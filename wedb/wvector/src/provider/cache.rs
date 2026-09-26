//! 邻接表与起始点缓存管理
//!
//! 包含对齐分配器、邻接表池化包装、起点缓存装配与邻居访问代理。

use std::{
  alloc::Layout,
  mem,
  ops::{Deref, DerefMut},
  ptr::NonNull,
};

use bytemuck::cast_slice;
use diskann_quantization::alloc::{AllocatorCore, AllocatorError, GlobalAllocator, Poly};
use diskann_utils::object_pool::{AsPooled, Undef};
use diskann_vector::{DistanceFunction, contains::ContainsSimd};
use webc_diskann::{
  ANNError, ANNResult,
  graph::AdjacencyList,
  neighbor::Neighbor,
  provider::{HasId, NeighborAccessor, NeighborAccessorMut},
  utils::VectorRepr,
};

use super::data_provider::{ToDistanceComputer, WedbProvider};
use crate::{
  error::{QuantizerError, StoreError, WedbProviderError},
  quantization::WedbQuantizer,
  store::{Context, StoreCallbacks, Term, VectorSetId},
};

/// 8 字节过对齐分配器（用于将任意切片安全提升为 8 字节对齐的 Poly 容器）。
#[derive(Debug, Clone, Copy)]
pub(crate) struct AlignToEight;

unsafe impl AllocatorCore for AlignToEight {
  #[inline]
  fn allocate(&self, layout: Layout) -> Result<NonNull<[u8]>, AllocatorError> {
    let layout = layout.align_to(8).map_err(|_| AllocatorError)?;
    GlobalAllocator.allocate(layout)
  }

  #[inline]
  unsafe fn deallocate(&self, ptr: NonNull<[u8]>, layout: Layout) {
    let layout = layout.align_to(8).unwrap_or(layout);
    unsafe { GlobalAllocator.deallocate(ptr, layout) }
  }
}

/// 池化邻接表包装（实现 `AsPooled` 供 `ObjectPool` 复用）。
#[derive(Clone)]
pub(crate) struct AdjList(pub AdjacencyList<u32>);

impl Deref for AdjList {
  type Target = AdjacencyList<u32>;

  #[inline]
  fn deref(&self) -> &Self::Target {
    &self.0
  }
}

impl DerefMut for AdjList {
  #[inline]
  fn deref_mut(&mut self) -> &mut Self::Target {
    &mut self.0
  }
}

impl AsPooled<Undef> for AdjList {
  fn create(args: Undef) -> Self {
    AdjList(AdjacencyList::with_capacity(args.len))
  }

  fn modify(&mut self, _args: Undef) {}
}

impl<T: ToDistanceComputer, S: StoreCallbacks> WedbProvider<T, S> {
  /// 插入前确保图具有合法起点。
  pub async fn maybe_set_start_point(
    &self,
    context: &Context,
    point: &[T],
  ) -> Result<(), WedbProviderError> {
    let mut v = Poly::broadcast(0u8, self.dim * mem::size_of::<T>(), AlignToEight)?;
    if self
      .callbacks
      .read_single_iid(&context.term(Term::Vector), 0, &mut v)
      .await
    {
      let mut neighbors = vec![0u32; self.max_degree + 1];
      if !self
        .callbacks
        .read_single_iid(&context.term(Term::Neighbors), 0, &mut neighbors)
        .await
      {
        return Err(StoreError::Read.into());
      }

      if self.is_quantized()
        && let Some(quantizer) = self.quantizer()
      {
        let mut qpoint = vec![0u8; quantizer.bytes()];
        if !self
          .callbacks
          .read_single_iid(&context.term(Term::Quantized), 0, &mut qpoint)
          .await
        {
          return Err(StoreError::Read.into());
        }
        self
          .start_point_quant_cache
          .pin()
          .insert(0, Poly::from_iter(qpoint.iter().copied(), AlignToEight)?);
      }

      self.start_point_cache.pin().insert(0, v);
      let len = neighbors[self.max_degree] as usize;
      neighbors.truncate(len);
      self.neighbor_cache.pin().insert(0, neighbors);
    } else {
      let neighbors = vec![0u32; self.max_degree + 1];
      let id = self.fsm.next_id(context).await?;
      if id.id() != 0 {
        self.fsm.mark_free(context, id.id()).await?;
        return Err(WedbProviderError::StartPoint);
      }

      if !self
        .callbacks
        .write_iid(&context.term(Term::Vector), 0, point)
        .await
      {
        return Err(StoreError::Write.into());
      }

      if self.is_quantized()
        && let Some(quantizer) = self.quantizer()
      {
        let mut qpoint = vec![0u8; quantizer.bytes()];
        // 内层作用域收束 as_f32 视图生命周期：其返回类型非 Send，不得跨 await 存活
        {
          let point_f32 =
            T::as_f32(point).map_err(|e| QuantizerError::Compression(e.to_string()))?;
          quantizer.compress(&point_f32, &mut qpoint)?;
        }
        if !self
          .callbacks
          .write_iid(&context.term(Term::Quantized), 0, &qpoint)
          .await
        {
          return Err(StoreError::Write.into());
        }
        self
          .start_point_quant_cache
          .pin()
          .insert(0, Poly::from_iter(qpoint.iter().copied(), AlignToEight)?);
      }

      if !self
        .callbacks
        .write_iid(&context.term(Term::Neighbors), 0, &neighbors)
        .await
      {
        return Err(StoreError::Write.into());
      }

      self.start_point_cache.pin().insert(
        0,
        Poly::from_iter(cast_slice::<T, u8>(point).iter().copied(), AlignToEight)?,
      );
      self
        .neighbor_cache
        .pin()
        .insert(0, Vec::with_capacity(self.max_degree + 1));
    }

    Ok(())
  }

  /// 起点是否已存在。
  pub fn start_points_exist(&self) -> bool {
    self.start_point_cache.pin().contains_key(&0) && self.neighbor_cache.pin().contains_key(&0)
  }

  /// 获取指定元素的邻居与距离。
  pub async fn neighbors(
    &self,
    context: &Context,
    id: &VectorSetId,
  ) -> ANNResult<Vec<Neighbor<VectorSetId>>> {
    let iid = self.to_internal_id(context, id).await?;
    let v = self.get_full_vector(context, iid).await?;
    let mut neighbors = AdjacencyList::with_capacity(self.max_degree + 1);

    if !self.get_neighbors(context, iid, &mut neighbors).await {
      return Err(WedbProviderError::Store(StoreError::Read).into());
    }

    let d = <T as VectorRepr>::distance(self.metric_type, Some(self.dim));
    let mut result = Vec::with_capacity(self.max_degree);
    for &nbr_id in neighbors.iter() {
      if nbr_id == 0 {
        continue;
      }
      let nbr_v = self.get_full_vector(context, nbr_id).await?;
      let nbr_eid = self.to_external_id(context, nbr_id).await?;
      let nbr_d = d.evaluate_similarity(&v, &nbr_v);
      result.push(Neighbor::new(nbr_eid, nbr_d));
    }

    Ok(result)
  }

  pub(crate) async fn get_neighbors(
    &self,
    context: &Context,
    iid: u32,
    neighbors: &mut AdjacencyList<u32>,
  ) -> bool {
    let mut guard = neighbors.resize(self.max_degree + 1);

    if iid == 0
      && let Some(cached) = self.neighbor_cache.pin().get(&iid)
    {
      guard[0..cached.len()].copy_from_slice(cached);
      guard.finish(cached.len());
      return true;
    }

    if !self
      .callbacks
      .read_single_iid(&context.term(Term::Neighbors), iid, &mut guard)
      .await
    {
      guard.finish(0);
      return false;
    }

    let len = guard[self.max_degree];
    guard.finish(len as usize);
    true
  }

  pub(crate) async fn set_neighbors(
    &self,
    context: &Context,
    iid: u32,
    neighbors: &[u32],
    scratch: &mut AdjacencyList<u32>,
  ) -> Result<(), WedbProviderError> {
    let mut guard = scratch.resize(self.max_degree + 1);
    guard[0..neighbors.len()].copy_from_slice(neighbors);
    guard[self.max_degree] = neighbors.len() as u32;

    if !self
      .callbacks
      .rmw_iid(
        &context.term(Term::Neighbors),
        iid,
        (self.max_degree + 1) * mem::size_of::<u32>(),
        |data: &mut [u32]| {
          data.copy_from_slice(&guard);
          if iid == 0 {
            self.neighbor_cache.pin().insert(0, neighbors.to_vec());
          }
        },
      )
      .await
    {
      return Err(StoreError::Write.into());
    }

    guard.finish(0);
    Ok(())
  }

  pub(crate) async fn append_vector(
    &self,
    context: &Context,
    iid: u32,
    neighbors: &[u32],
  ) -> Result<(), WedbProviderError> {
    let max_degree = self.max_degree;
    if !self
      .callbacks
      .rmw_iid(
        &context.term(Term::Neighbors),
        iid,
        (max_degree + 1) * mem::size_of::<u32>(),
        move |data: &mut [u32]| {
          let mut len = (data[max_degree] as usize).min(max_degree);
          for &nbr in neighbors {
            if len == max_degree {
              return;
            }
            if u32::contains_simd(&data[0..len], nbr) {
              continue;
            }
            data[len] = nbr;
            len += 1;
            data[max_degree] = len as u32;
          }
          if iid == 0 && self.neighbor_cache.pin().contains_key(&0) {
            self.neighbor_cache.pin().insert(0, data[..len].to_vec());
          }
        },
      )
      .await
    {
      return Err(StoreError::Write.into());
    }

    Ok(())
  }
}

/// 邻接表操作代理访问器。
pub(crate) struct DelegateNeighborAccessor<'a, T, S>
where
  T: ToDistanceComputer,
  S: StoreCallbacks,
{
  pub(crate) provider: &'a WedbProvider<T, S>,
  pub(crate) context: &'a Context,
  pub(crate) scratch: &'a mut AdjacencyList<u32>,
}

impl<T: ToDistanceComputer, S: StoreCallbacks> HasId for DelegateNeighborAccessor<'_, T, S> {
  type Id = u32;
}

impl<T: ToDistanceComputer, S: StoreCallbacks> NeighborAccessor
  for DelegateNeighborAccessor<'_, T, S>
{
  async fn get_neighbors(
    &mut self,
    id: Self::Id,
    neighbors: &mut AdjacencyList<Self::Id>,
  ) -> ANNResult<()> {
    if self
      .provider
      .get_neighbors(self.context, id, &mut *neighbors)
      .await
    {
      Ok(())
    } else {
      Err(ANNError::from(WedbProviderError::Store(StoreError::Read)))
    }
  }
}

impl<T: ToDistanceComputer, S: StoreCallbacks> NeighborAccessorMut
  for DelegateNeighborAccessor<'_, T, S>
{
  async fn set_neighbors(&mut self, id: Self::Id, neighbors: &[Self::Id]) -> ANNResult<()> {
    self
      .provider
      .set_neighbors(self.context, id, neighbors, self.scratch)
      .await
      .map_err(ANNError::from)
  }

  async fn append_vector(&mut self, id: Self::Id, neighbors: &[Self::Id]) -> ANNResult<()> {
    self
      .provider
      .append_vector(self.context, id, neighbors)
      .await
      .map_err(ANNError::from)
  }
}
