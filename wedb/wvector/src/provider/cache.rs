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
use wbase::map::ConcurrentMap;
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
  quantization::{QuantizerImpl, WedbQuantizer},
  store::{Callbacks, Context, StoreCallbacks, Term, VectorSetId},
};

/// 起点全精度向量与邻接表（id 0）读取单点：向量缺失返回 `None`（起点不存在）；
/// 向量在而邻接缺失（崩溃窗半截态）则就地补写空邻接表自愈；命中时邻接按尾槽
/// 长度截断后交回，由调用方写入各自缓存。
///
/// `WedbProvider::new`（构造恢复）与 [`maybe_set_start_point`]（插入前恢复臂）
/// 的公共主体收口；`dim`/`max_degree` 显式传参——`new()` 期 Self 未成形。
pub(super) async fn read_start_point_core<T: ToDistanceComputer, S: StoreCallbacks>(
  callbacks: &Callbacks<S>,
  context: &Context,
  dim: usize,
  max_degree: usize,
) -> Result<Option<(Poly<[u8], AlignToEight>, Vec<u32>)>, WedbProviderError> {
  let mut v = Poly::broadcast(0u8, dim * mem::size_of::<T>(), AlignToEight)?;
  if !callbacks
    .read_single_iid(&context.term(Term::Vector), 0, &mut v)
    .await
  {
    return Ok(None);
  }
  let mut neighbors = vec![0u32; max_degree + 1];
  if !callbacks
    .read_single_iid(&context.term(Term::Neighbors), 0, &mut neighbors)
    .await
  {
    // 起点半截态自愈：claim 在先且 Vector 0 已落盘，但 Neighbors 0 缺失（崩溃窗或瞬态写失败）。
    // 起点邻接按构造恒为空表（尾槽 len = 0），就地补写空邻接表收敛半截态。
    if !callbacks
      .write_iid(&context.term(Term::Neighbors), 0, &neighbors)
      .await
    {
      return Err(StoreError::Write.into());
    }
  }
  let len = neighbors[max_degree] as usize;
  neighbors.truncate(len);
  Ok(Some((v, neighbors)))
}

/// 起点量化向量（id 0）缓存装载单点：命中即写 `quant_cache` 并返回 `true`；
/// 缺失时若已训练且起点全精度向量在场，则自愈重建并补写；仍缺失返回 `false`。
pub(super) async fn load_start_point_quant_cache<S: StoreCallbacks>(
  callbacks: &Callbacks<S>,
  context: &Context,
  quantizer: &QuantizerImpl,
  quant_cache: &ConcurrentMap<u32, Poly<[u8], AlignToEight>>,
) -> Result<bool, WedbProviderError> {
  let mut qsv = Poly::broadcast(0u8, quantizer.bytes(), AlignToEight)?;
  if callbacks
    .read_single_iid(&context.term(Term::Quantized), 0, &mut qsv)
    .await
  {
    quant_cache.pin().insert(0, qsv);
    return Ok(true);
  }

  // 起点量化记录半截态自愈（针对已训练量化器如 Q8，或重启恢复）：
  // 若 Term::Vector 0 存在且量化器已训练，自全精度向量重建并补写 Quantized 0
  if quantizer.is_trained()
    && let Some(v_f32) = callbacks
      .read_varsize_iid::<f32>(&context.term(Term::Vector), 0)
      .await
    && quantizer.compress(&v_f32, &mut qsv).is_ok()
  {
    if !callbacks
      .write_iid(&context.term(Term::Quantized), 0, &qsv)
      .await
    {
      return Err(StoreError::Write.into());
    }
    quant_cache.pin().insert(0, qsv);
    return Ok(true);
  }

  Ok(false)
}

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
    if let Some((v, neighbors)) =
      read_start_point_core::<T, S>(&self.callbacks, context, self.dim, self.max_degree).await?
    {
      if self.is_quantized()
        && let Some(quantizer) = self.quantizer()
        && !load_start_point_quant_cache(
          &self.callbacks,
          context,
          quantizer,
          &self.start_point_quant_cache,
        )
        .await?
      {
        // 起点量化记录半截态自愈：Vector 0 存在但 Quantized 0 缺失（崩溃窗或瞬态写失败）。
        // 自全精度起点向量经 T::as_f32 重建并就地补写量化记录。
        let mut qpoint = vec![0u8; quantizer.bytes()];
        {
          let v_slice = cast_slice::<u8, T>(&v);
          let point_f32 =
            T::as_f32(v_slice).map_err(|e| QuantizerError::Compression(e.to_string()))?;
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

      self.start_point_cache.pin().insert(0, v);
      self.neighbor_cache.pin().insert(0, neighbors);
    } else {
      let neighbors = vec![0u32; self.max_degree + 1];
      // 起点 id 0 恒锚定预留槽，经 fsm 幂等认领位点置占用位：装载 future
      // 被取消丢弃后（状态机守卫复位 NoStartPoints），下次 VADD 重入本臂
      // 幂等重认领即收敛，杜绝重铸非零 id 死路
      self.fsm.claim_start_id(context).await?;

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

  /// 起点是否已存在（全精度起点向量、邻接表及量化记录均已就绪）。
  pub fn start_points_exist(&self) -> bool {
    self.start_point_cache.pin().contains_key(&0)
      && self.neighbor_cache.pin().contains_key(&0)
      && (!self.is_quantized() || self.start_point_quant_cache.pin().contains_key(&0))
  }

  /// 获取指定元素的邻居与距离。
  ///
  /// 悬垂邻接跳过臂：删除先摘数据并释放 fsm 槽位（delete_element 的
  /// mark_free 在前），图边回收（inplace_delete 的 add_edge_and_prune /
  /// drop_adj_list）在其后；删除半途失败使他人邻接表残留悬空 id 成稳态。
  /// 对齐 C# 读完成面「元素可能已被删」纪律（VectorManager.cs:TryGetEmbedding
  /// 读后 CheckInternalIdValid）与 diskann 遍历臂墓碑邻居经
  /// status_by_internal_id 就地跳过（webc-diskann graph/index.rs）：每个邻居
  /// 先 [`WedbProvider::vector_iid_exists`]（fsm 占用位，与库遍历同单源）
  /// 前置过滤，判定/向量/映射读残余失败跳过留痕，严禁 `?` 整链上抛——单个
  /// 已删邻居不得毒化整包应答（null 单源保留给键/元素真缺席）。
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
      // 占用位判定读失败＝与下方记录读臂同型的竞态窗残余失败，跳过留痕
      // （悬垂邻居不得毒化整包应答，与本函数头自陈纪律同轨）
      match self.vector_iid_exists(context, nbr_id).await {
        Ok(true) => {}
        Ok(false) => continue,
        Err(_) => {
          log::warn!(
            "邻接占用位判定读失败，悬垂邻居跳过: context={:#x} iid={nbr_id}",
            context.inner()
          );
          continue;
        }
      }
      // 残余失败（占用位与记录读的竞态窗）跳过留痕，与 dynamic_quant
      // value_len_ok「异常跳过留痕」同臂
      let (nbr_v, nbr_eid) = match (
        self.get_full_vector(context, nbr_id).await,
        self.to_external_id(context, nbr_id).await,
      ) {
        (Ok(v), Ok(eid)) => (v, eid),
        _ => {
          log::warn!(
            "邻接记录读取失败，悬垂邻居跳过: context={:#x} iid={nbr_id}",
            context.inner()
          );
          continue;
        }
      };
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
