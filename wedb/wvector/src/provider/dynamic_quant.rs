//! 动态量化双轨策略与检索/剪枝访问器
//!
//! 实现全精度与量化向量透明自适应双轨运行，包含训练、回填、访问器及重排后处理。

use std::{future, mem, mem::size_of, sync::atomic::Ordering};

use bytemuck::cast_slice;
use diskann::{
  ANNError, ANNResult,
  error::StandardError,
  graph::{
    SearchOutputBuffer,
    config::defaults::MAX_OCCLUSION_SIZE,
    glue::{
      self, Accept, Decision, DefaultPostProcessor, FilteredAccessor, HybridPredicate,
      InplaceDeleteStrategy, InsertStrategy, Predicate, PredicateMut, PruneStrategy,
      SearchAccessor, SearchPostProcess, SearchPostProcessStep, SearchStrategy,
    },
    workingset::{
      self,
      map::{Capacity, Entry, Ref},
    },
  },
  neighbor::{Neighbor, ord::fast_distance},
  provider::{DataProvider, HasId},
};
use diskann_quantization::alloc::Poly;
use diskann_utils::{
  object_pool::{PooledRef, Undef},
  views::{Matrix, MatrixView},
};
use diskann_vector::DistanceFunction;

use super::{
  cache::{AdjList, AlignToEight, DelegateNeighborAccessor},
  data_provider::{DistanceComputer, QueryComputer, ToDistanceComputer, WedbProvider},
};
use crate::{
  error::{QuantizerError, StoreError, WedbProviderError},
  quantization::{QuantizerImpl, WedbQuantizer},
  store::{Context, StoreCallbacks, Term, VectorSetId},
};

/// 量化状态与量化表存储在 Metadata 项下的专用键（`_qnt`）。
pub(crate) const QUANT_STATE_KEY: u32 = u32::from_be_bytes(*b"_qnt");

/// 重排序预分配缓冲初始容量。
pub(crate) const RERANK_BUFFER_LENGTH: usize = 1024;

impl<T: ToDistanceComputer, S: StoreCallbacks> WedbProvider<T, S> {
  /// 关联量化器引用。
  pub fn quantizer(&self) -> Option<&QuantizerImpl> {
    self.quantizer.as_ref()
  }

  /// 索引是否已全量化运行。
  #[inline]
  pub fn is_quantized(&self) -> bool {
    self.quantizer.is_some() && self.all_quantized.load(Ordering::Acquire)
  }

  /// 是否需要调度量化建表。
  pub fn quantization_needed(&self) -> bool {
    if let Some(quantizer) = &self.quantizer {
      !self.is_quantized()
        && quantizer.is_trained()
        && self.max_internal_id() as usize > quantizer.required_vectors()
    } else {
      false
    }
  }

  /// 训练量化器。
  pub fn train_quantizer(&self, context: &Context) -> bool {
    let _guard = match self.training_lock.try_lock() {
      Some(g) => g,
      None => return false,
    };

    let quantizer = match &self.quantizer {
      Some(q) if q.is_trained() => return true,
      Some(q) => q,
      None => return false,
    };

    let rows = quantizer.required_vectors();
    let mut data = Matrix::<T>::new(T::default(), rows, self.dim);
    let mut row_idx = 0usize;

    if self
      .fsm
      .visit_used(context, |id| {
        if id == 0 {
          return true;
        }
        if row_idx >= rows {
          return false;
        }
        let row = data.row_mut(row_idx);
        if !self
          .callbacks
          .read_single_iid(&context.term(Term::Vector), id, row)
        {
          return false;
        }
        row_idx += 1;
        true
      })
      .is_err()
    {
      return false;
    }

    if row_idx < quantizer.required_vectors() {
      return false;
    }

    let view = match data.subview(0..row_idx) {
      Some(v) => v,
      None => return false,
    };

    let converted = match T::as_f32(view.as_slice()) {
      Ok(v) => v,
      Err(_) => return false,
    };
    let view = match MatrixView::try_from(&*converted, view.nrows(), view.ncols()) {
      Ok(v) => v,
      Err(_) => return false,
    };

    match quantizer.train(self.metric_type, view) {
      Ok(()) => {
        let quant_state = match quantizer.serialize() {
          Ok(s) => s,
          Err(_) => return false,
        };
        let mut total_quant_state = vec![0u8; quant_state.len() + 1];
        total_quant_state[1..].copy_from_slice(&quant_state);

        if !self.callbacks.write_iid(
          &context.term(Term::Metadata),
          QUANT_STATE_KEY,
          &total_quant_state,
        ) {
          return false;
        }
        self.fsm.enable_quantization();
        true
      }
      Err(_) => false,
    }
  }

  /// 批量回填历史向量量化编码。
  pub fn backfill_quant_vectors(
    &self,
    context: &Context,
    task_idx: usize,
    task_count: usize,
  ) -> bool {
    let quantizer = match &self.quantizer {
      Some(q) => q,
      None => {
        self.callbacks.log(
          &context.term(Term::Quantized),
          "Error: backfill_quant_vectors: Quantizer not found.",
        );
        return false;
      }
    };

    let max_id = self.fsm.max_id_for_backfill() as usize;
    if max_id >= u32::MAX as usize {
      return false;
    }

    let task_count = task_count.min(max_id + 1);
    if task_idx >= task_count {
      return false;
    }

    let work_count = (max_id + 1).div_ceil(task_count);
    let start_id = (work_count * task_idx) as u32;
    let end_id = (work_count * (task_idx + 1)).min(max_id + 1) as u32;

    let mut v = vec![T::default(); self.dim];
    let mut f = vec![0f32; self.dim];
    let mut q = vec![0u8; quantizer.bytes()];
    for id in start_id..end_id {
      if !self
        .callbacks
        .read_single_iid(&context.term(Term::Vector), id, &mut v)
      {
        continue;
      }
      if T::as_f32_into(&v, &mut f).is_err() {
        continue;
      }
      if quantizer.compress(&f, &mut q).is_err() {
        continue;
      }
      if !self
        .callbacks
        .write_iid(&context.term(Term::Quantized), id, &q)
      {
        continue;
      }
    }

    let backfill_finished =
      self.backfills_completed.fetch_add(1, Ordering::AcqRel) + 1 == task_count as u64;

    if backfill_finished {
      if let Some(v) = self.start_point_cache.pin().get(&0)
        && let Ok(v_f32) = T::as_f32(cast_slice::<u8, T>(v))
        && quantizer.compress(&v_f32, &mut q).is_ok()
      {
        let _ = self
          .callbacks
          .write_iid(&context.term(Term::Quantized), 0, &q);
        if let Ok(p) = Poly::from_iter(q.iter().copied(), AlignToEight) {
          self.start_point_quant_cache.pin().insert(0, p);
        }
      }

      self.fsm.enable_reuse();

      if !self.callbacks.rmw_iid::<_, u8>(
        &context.term(Term::Metadata),
        QUANT_STATE_KEY,
        1,
        |data| {
          data[0] = 1;
        },
      ) {
        return false;
      }
      self.all_quantized.store(true, Ordering::Release);
    }

    true
  }

  #[inline]
  pub(crate) fn quant_vector_size(&self) -> usize {
    self.quantizer.as_ref().map(|q| q.bytes()).unwrap_or(0)
  }
}

/// 动态量化执行策略：透明自适应全精度与量化双轨运行。
#[derive(Copy, Clone, Debug)]
pub struct DynamicQuantization;

/// 检索访问器：支持全精度与量化向量无缝切换。
pub struct DynamicAccessor<'a, T: ToDistanceComputer, S: StoreCallbacks> {
  pub(crate) provider: &'a WedbProvider<T, S>,
  pub(crate) context: &'a Context,
  pub(crate) quantized: bool,
  pub(crate) computer: QueryComputer,
  pub(crate) id_buffer: PooledRef<'a, AdjList>,
  pub(crate) filtered_ids: PooledRef<'a, Vec<u32>>,
  pub(crate) filtered_decisions: PooledRef<'a, Vec<bool>>,
  pub(crate) start_point_dist: Option<f32>,
}

impl<'a, T: ToDistanceComputer, S: StoreCallbacks> DynamicAccessor<'a, T, S> {
  const START_ID: u32 = 0;

  pub fn new(
    provider: &'a WedbProvider<T, S>,
    context: &'a Context,
    query: &'a [T],
    quantized: bool,
  ) -> Result<Self, WedbProviderError> {
    let id_buffer = provider
      .id_buffer_pool
      .get_ref(Undef::new(provider.max_degree + 1));
    let filtered_ids = provider
      .filtered_ids_pool
      .get_ref(Undef::new(MAX_OCCLUSION_SIZE.get() as usize * 2));
    let filtered_decisions = provider
      .filtered_decisions_pool
      .get_ref(Undef::new(MAX_OCCLUSION_SIZE.get() as usize));

    let computer = if quantized && let Some(quantizer) = provider.quantizer() {
      let from_f32 = T::as_f32(query)
        .map_err(|e| WedbProviderError::Quantizer(QuantizerError::Compression(e.to_string())))?;
      quantizer
        .query_computer(&from_f32)
        .map_err(|e| QuantizerError::QueryComputer(e.to_string()))?
    } else {
      T::to_query_computer(query, provider.metric_type)
    };

    Ok(DynamicAccessor {
      provider,
      context,
      quantized,
      computer,
      id_buffer,
      filtered_ids,
      filtered_decisions,
      start_point_dist: None,
    })
  }

  fn start_point_distance(&mut self) -> Result<f32, WedbProviderError> {
    if let Some(dist) = self.start_point_dist {
      return Ok(dist);
    }
    let dist = if self.quantized && self.provider.quantizer().is_some() {
      let cache = self.provider.start_point_quant_cache.pin();
      match cache.get(&Self::START_ID) {
        Some(guard) => self.computer.evaluate_similarity(guard),
        None => return Err(StoreError::Read.into()),
      }
    } else {
      let cache = self.provider.start_point_cache.pin();
      match cache.get(&Self::START_ID) {
        Some(guard) => self.computer.evaluate_similarity(guard),
        None => return Err(StoreError::Read.into()),
      }
    };
    self.start_point_dist = Some(dist);
    Ok(dist)
  }

  fn compute_filter_decisions(&mut self) {
    self.filtered_decisions.clear();
    let (chunks, _) = self.filtered_ids.as_chunks::<2>();
    self.filtered_decisions.reserve(chunks.len());
    for chunk in chunks {
      let internal_id = chunk[1];
      let matches = self
        .provider
        .callbacks
        .matches_filter(self.context, internal_id);
      self.filtered_decisions.push(matches);
    }
  }
}

impl<T: ToDistanceComputer, S: StoreCallbacks> HasId for DynamicAccessor<'_, T, S> {
  type Id = u32;
}

impl<T: ToDistanceComputer, S: StoreCallbacks> SearchAccessor for DynamicAccessor<'_, T, S> {
  fn starting_points(&self) -> impl future::Future<Output = ANNResult<Vec<Self::Id>>> + Send {
    let points = if self.provider.start_points_exist() {
      vec![Self::START_ID]
    } else {
      vec![]
    };
    future::ready(Ok(points))
  }

  fn is_not_start_point(
    &self,
  ) -> impl future::Future<Output = ANNResult<impl Fn(Self::Id) -> bool + Send + Sync + 'static>> + Send
  {
    future::ready(Ok(move |id| id != Self::START_ID))
  }

  fn start_point_distances<F>(
    &mut self,
    mut f: F,
  ) -> impl future::Future<Output = ANNResult<()>> + Send
  where
    F: FnMut(Self::Id, f32) + Send,
  {
    if !self.provider.start_points_exist() {
      return future::ready(Ok(()));
    }
    let result = match self.start_point_distance() {
      Ok(dist) => {
        f(Self::START_ID, dist);
        Ok(())
      }
      Err(err) => Err(ANNError::from(err)),
    };
    future::ready(result)
  }

  fn expand_beam<Itr, P, F>(
    &mut self,
    ids: Itr,
    mut pred: P,
    mut on_neighbors: F,
  ) -> impl future::Future<Output = ANNResult<()>> + Send
  where
    Itr: Iterator<Item = Self::Id> + Send,
    P: HybridPredicate<Self::Id> + Send + Sync,
    F: FnMut(Self::Id, f32) + Send,
  {
    let mut id_buffer = mem::take(&mut **self.id_buffer);

    for nl_id in ids {
      self
        .provider
        .get_neighbors(self.context, nl_id, &mut id_buffer);
      self.filtered_ids.clear();
      for id in id_buffer.iter().copied().filter(|id| pred.eval_mut(id)) {
        if id == Self::START_ID {
          let dist = match self.start_point_distance() {
            Ok(dist) => dist,
            Err(err) => return future::ready(Err(ANNError::from(err))),
          };
          on_neighbors(id, dist);
        } else {
          self.filtered_ids.push(4);
          self.filtered_ids.push(id);
        }
      }

      let (ctx, length_hint) = if self.quantized {
        (
          self.context.term(Term::Quantized),
          self.provider.quant_vector_size(),
        )
      } else {
        (
          self.context.term(Term::Vector),
          self.provider.full_vector_size(),
        )
      };

      if !self.filtered_ids.is_empty() {
        self
          .provider
          .callbacks
          .read_multi_lpiid(&ctx, &self.filtered_ids, length_hint, |i, v| {
            if v.len() < length_hint {
              return;
            }
            let dist = self.computer.evaluate_similarity(v);
            on_neighbors(self.filtered_ids[i as usize * 2 + 1], dist);
          });
      }
    }

    **self.id_buffer = id_buffer;
    future::ready(Ok(()))
  }
}

impl<T: ToDistanceComputer, S: StoreCallbacks> FilteredAccessor for DynamicAccessor<'_, T, S> {
  fn start_point_distances<F>(
    &mut self,
    mut f: F,
  ) -> impl future::Future<Output = ANNResult<()>> + Send
  where
    F: FnMut(Decision<Self::Id>, f32) + Send,
  {
    if !self.provider.start_points_exist() {
      return future::ready(Ok(()));
    }
    let result = match self.start_point_distance() {
      Ok(dist) => {
        f(Decision::reject(Self::START_ID), dist);
        Ok(())
      }
      Err(err) => Err(ANNError::from(err)),
    };
    future::ready(result)
  }

  fn expand_beam_filtered<Itr, P, F>(
    &mut self,
    ids: Itr,
    mut pred: P,
    mut on_neighbors: F,
  ) -> impl future::Future<Output = ANNResult<()>> + Send
  where
    Itr: Iterator<Item = Self::Id> + Send,
    P: HybridPredicate<Self::Id> + Send + Sync,
    F: FnMut(Decision<Self::Id>, f32) + Send,
  {
    let mut id_buffer = mem::take(&mut **self.id_buffer);

    for nl_id in ids {
      self
        .provider
        .get_neighbors(self.context, nl_id, &mut id_buffer);
      self.filtered_ids.clear();

      for id in id_buffer.iter().copied().filter(|id| pred.eval_mut(id)) {
        if id == Self::START_ID {
          let dist = match self.start_point_distance() {
            Ok(dist) => dist,
            Err(err) => return future::ready(Err(ANNError::from(err))),
          };
          on_neighbors(Decision::reject(id), dist);
        } else {
          self.filtered_ids.push(4);
          self.filtered_ids.push(id);
        }
      }

      if self.filtered_ids.is_empty() {
        continue;
      }

      let (ctx, length_hint) = if self.quantized {
        (
          self.context.term(Term::Quantized),
          self.provider.quant_vector_size(),
        )
      } else {
        (
          self.context.term(Term::Vector),
          self.provider.full_vector_size(),
        )
      };

      self.compute_filter_decisions();

      self
        .provider
        .callbacks
        .read_multi_lpiid(&ctx, &self.filtered_ids, length_hint, |i, v| {
          let dist = self.computer.evaluate_similarity(v);
          let decision = if self.filtered_decisions[i as usize] {
            Decision::accept(self.filtered_ids[i as usize * 2 + 1])
          } else {
            Decision::reject(self.filtered_ids[i as usize * 2 + 1])
          };
          on_neighbors(decision, dist);
        });
    }

    **self.id_buffer = id_buffer;
    future::ready(Ok(()))
  }

  fn expand_beam_accept_only<Itr, P, F>(
    &mut self,
    ids: Itr,
    mut pred: P,
    mut on_neighbors: F,
  ) -> impl future::Future<Output = ANNResult<()>> + Send
  where
    Itr: Iterator<Item = Self::Id> + Send,
    P: Predicate<Self::Id> + PredicateMut<Accept<Self::Id>> + Send + Sync,
    F: FnMut(Accept<Self::Id>, f32) + Send,
  {
    let mut id_buffer = mem::take(&mut **self.id_buffer);

    for nl_id in ids {
      self
        .provider
        .get_neighbors(self.context, nl_id, &mut id_buffer);
      self.filtered_ids.clear();

      for id in id_buffer.iter().copied() {
        if id != Self::START_ID
          && pred.eval(&id)
          && self.provider.callbacks.matches_filter(self.context, id)
          && pred.eval_mut(&Accept::new(id))
        {
          self.filtered_ids.push(4);
          self.filtered_ids.push(id);
        }
      }

      if self.filtered_ids.is_empty() {
        continue;
      }

      let (ctx, length_hint) = if self.quantized {
        (
          self.context.term(Term::Quantized),
          self.provider.quant_vector_size(),
        )
      } else {
        (
          self.context.term(Term::Vector),
          self.provider.full_vector_size(),
        )
      };

      self
        .provider
        .callbacks
        .read_multi_lpiid(&ctx, &self.filtered_ids, length_hint, |i, v| {
          let dist = self.computer.evaluate_similarity(v);
          on_neighbors(Accept::new(self.filtered_ids[i as usize * 2 + 1]), dist);
        });
    }

    **self.id_buffer = id_buffer;
    future::ready(Ok(()))
  }

  fn num_starting_points(&self) -> impl future::Future<Output = ANNResult<usize>> + Send {
    if self.provider.start_points_exist() {
      future::ready(Ok(1))
    } else {
      future::ready(Ok(0))
    }
  }
}

/// 候选外部 ID 导出后处理器。
#[derive(Debug, Default, Clone, Copy)]
pub struct CopyExternalIds;

impl<'a, T: ToDistanceComputer, S: StoreCallbacks>
  SearchPostProcess<DynamicAccessor<'a, T, S>, &[T], VectorSetId> for CopyExternalIds
{
  type Error = WedbProviderError;

  fn post_process<I, B>(
    &self,
    accessor: &mut DynamicAccessor<'a, T, S>,
    _query: &[T],
    candidates: I,
    output: &mut B,
  ) -> impl future::Future<Output = Result<usize, Self::Error>> + Send
  where
    I: Iterator<Item = Neighbor<<DynamicAccessor<'a, T, S> as HasId>::Id>> + Send,
    B: SearchOutputBuffer<VectorSetId> + Send + ?Sized,
  {
    let initial = output.current_len();
    for n in candidates {
      let id = match accessor.provider.to_external_id(accessor.context, *n.id()) {
        Ok(id) => id,
        Err(_) => continue,
      };

      if output.push(Neighbor::new(id, *n.distance())).is_full() {
        break;
      }
    }

    let count = output.current_len() - initial;
    future::ready(Ok(count))
  }
}

/// 全精度精排后处理器。
#[derive(Debug, Default, Clone, Copy)]
pub struct Rerank;

impl<'a, 'b, T: ToDistanceComputer, S: StoreCallbacks>
  SearchPostProcessStep<DynamicAccessor<'a, T, S>, &'b [T], VectorSetId> for Rerank
{
  type Error<NextError>
    = WedbProviderError
  where
    NextError: StandardError;

  type NextAccessor = DynamicAccessor<'a, T, S>;

  async fn post_process_step<I, B, Next>(
    &self,
    next: &Next,
    accessor: &mut DynamicAccessor<'a, T, S>,
    query: &'b [T],
    candidates: I,
    output: &mut B,
  ) -> Result<usize, Self::Error<Next::Error>>
  where
    I: Iterator<Item = Neighbor<<DynamicAccessor<'a, T, S> as HasId>::Id>> + Send,
    B: SearchOutputBuffer<VectorSetId> + Send + ?Sized,
    Next: SearchPostProcess<Self::NextAccessor, &'b [T], VectorSetId> + Sync,
  {
    if !accessor.quantized {
      return next
        .post_process(accessor, query, candidates, output)
        .await
        .map_err(|e| WedbProviderError::PostProcessing(e.to_string()));
    }

    let provider = accessor.provider;
    let f = T::distance(provider.metric_type, Some(provider.dim));

    let mut reranked = provider
      .rerank_pool
      .get_ref(Undef::new(RERANK_BUFFER_LENGTH));
    reranked.clear();

    accessor.filtered_ids.clear();
    for nbor in candidates {
      accessor.filtered_ids.push(4);
      accessor.filtered_ids.push(*nbor.id());
    }

    if !accessor.filtered_ids.is_empty() {
      let mut fallback_buf = Vec::<T>::new();
      provider.callbacks.read_multi_lpiid(
        &accessor.context.term(Term::Vector),
        &accessor.filtered_ids,
        provider.full_vector_size(),
        |i, v| {
          let dist = match bytemuck::try_cast_slice::<u8, T>(v) {
            Ok(s) => f.evaluate_similarity(query, s),
            Err(_) => {
              let count = if size_of::<T>() > 0 {
                v.len() / size_of::<T>()
              } else {
                0
              };
              let valid_bytes = count * size_of::<T>();
              fallback_buf.resize(count, bytemuck::Zeroable::zeroed());
              let dest_bytes = bytemuck::cast_slice_mut::<T, u8>(&mut fallback_buf[..count]);
              dest_bytes.copy_from_slice(&v[..valid_bytes]);
              f.evaluate_similarity(query, &fallback_buf[..count])
            }
          };
          reranked.push(Neighbor::new(
            accessor.filtered_ids[i as usize * 2 + 1],
            dist,
          ));
        },
      );
    }

    reranked.sort_unstable_by(fast_distance);

    next
      .post_process(accessor, query, reranked.iter().copied(), output)
      .await
      .map_err(|e| WedbProviderError::PostProcessing(e.to_string()))
  }
}

/// 剪枝访问器：缓存反向边与局部拓扑。
pub struct PruneAccessor<'a, T, S>
where
  T: ToDistanceComputer,
  S: StoreCallbacks,
{
  provider: &'a WedbProvider<T, S>,
  context: &'a Context,
  quantized: bool,
  id_buffer: PooledRef<'a, AdjList>,
  filtered_ids: PooledRef<'a, Vec<u32>>,
  distance: DistanceComputer,
  set: workingset::Map<u32, Box<[u8]>, Ref<[u8]>>,
}

impl<'a, T, S> PruneAccessor<'a, T, S>
where
  T: ToDistanceComputer,
  S: StoreCallbacks,
{
  pub fn new(
    provider: &'a WedbProvider<T, S>,
    context: &'a Context,
    quantized: bool,
    capacity: usize,
  ) -> Result<Self, WedbProviderError> {
    let distance = if quantized && let Some(quantizer) = provider.quantizer() {
      quantizer.distance_computer()?
    } else {
      T::to_distance_computer(provider.metric_type, provider.dim)
    };

    let id_buffer = provider
      .id_buffer_pool
      .get_ref(Undef::new(provider.max_degree + 1));
    let filtered_ids = provider
      .filtered_ids_pool
      .get_ref(Undef::new(MAX_OCCLUSION_SIZE.get() as usize * 2));
    let set = workingset::map::Builder::new(Capacity::Default).build(capacity);

    Ok(Self {
      provider,
      context,
      quantized,
      id_buffer,
      filtered_ids,
      distance,
      set,
    })
  }
}

impl<T: ToDistanceComputer, S: StoreCallbacks> HasId for PruneAccessor<'_, T, S> {
  type Id = u32;
}

impl<T: ToDistanceComputer, S: StoreCallbacks> glue::PruneAccessor for PruneAccessor<'_, T, S> {
  type ElementRef<'a> = &'a [u8];
  type View<'a>
    = workingset::map::View<'a, u32, Box<[u8]>, Ref<[u8]>>
  where
    Self: 'a;
  type Distance<'a>
    = &'a DistanceComputer
  where
    Self: 'a;
  type Neighbors<'a>
    = DelegateNeighborAccessor<'a, T, S>
  where
    Self: 'a;

  async fn fill<Itr>(&mut self, itr: Itr) -> ANNResult<(Self::View<'_>, Self::Distance<'_>)>
  where
    Itr: ExactSizeIterator<Item = Self::Id> + Clone + Send + Sync,
  {
    self.set.prepare(itr.clone());
    self.filtered_ids.clear();

    for id in itr {
      if id == 0 {
        if self.quantized
          && let Entry::Vacant(e) = self.set.entry(id)
        {
          let cache = self.provider.start_point_quant_cache.pin();
          if let Some(guard) = cache.get(&id) {
            e.insert((&**guard).into());
          } else {
            return Err(WedbProviderError::StartPoint.into());
          }
        } else if let Entry::Vacant(e) = self.set.entry(id) {
          let cache = self.provider.start_point_cache.pin();
          if let Some(guard) = cache.get(&id) {
            e.insert((&**guard).into());
          } else {
            return Err(WedbProviderError::StartPoint.into());
          }
        }
      } else if !self.set.contains_key(&id) {
        self.filtered_ids.push(4);
        self.filtered_ids.push(id);
      }
    }

    let (ctx, length_hint) = if self.quantized {
      (
        self.context.term(Term::Quantized),
        self.provider.quant_vector_size(),
      )
    } else {
      (
        self.context.term(Term::Vector),
        self.provider.full_vector_size(),
      )
    };

    if !self.filtered_ids.is_empty() {
      self
        .provider
        .callbacks
        .read_multi_lpiid(&ctx, &self.filtered_ids, length_hint, |id, v| {
          self
            .set
            .insert(self.filtered_ids[id as usize * 2 + 1], v.into());
        });
    }

    Ok((self.set.view(), &self.distance))
  }

  fn neighbors(&mut self) -> Self::Neighbors<'_> {
    DelegateNeighborAccessor {
      provider: self.provider,
      context: self.context,
      scratch: &mut self.id_buffer,
    }
  }
}

impl<'a, T: ToDistanceComputer, S: StoreCallbacks> SearchStrategy<'a, WedbProvider<T, S>, &'a [T]>
  for DynamicQuantization
{
  type SearchAccessor = DynamicAccessor<'a, T, S>;
  type SearchAccessorError = WedbProviderError;

  fn search_accessor(
    &'a self,
    provider: &'a WedbProvider<T, S>,
    context: &'a <WedbProvider<T, S> as DataProvider>::Context,
    query: &'a [T],
  ) -> Result<Self::SearchAccessor, Self::SearchAccessorError> {
    let quantized = provider.is_quantized();
    DynamicAccessor::new(provider, context, query, quantized)
  }
}

impl<'a, T: ToDistanceComputer, S: StoreCallbacks>
  DefaultPostProcessor<'a, WedbProvider<T, S>, &'a [T], VectorSetId> for DynamicQuantization
{
  diskann::default_post_processor!(
    glue::Pipeline<glue::FilterStartPoints, glue::Pipeline<Rerank, CopyExternalIds>>
  );
}

impl<T: ToDistanceComputer, S: StoreCallbacks> PruneStrategy<WedbProvider<T, S>>
  for DynamicQuantization
{
  type PruneAccessor<'a> = PruneAccessor<'a, T, S>;
  type PruneAccessorError = WedbProviderError;

  fn prune_accessor<'a>(
    &'a self,
    provider: &'a WedbProvider<T, S>,
    context: &'a <WedbProvider<T, S> as DataProvider>::Context,
    capacity: usize,
  ) -> Result<Self::PruneAccessor<'a>, Self::PruneAccessorError> {
    let quantized = provider.is_quantized();
    PruneAccessor::new(provider, context, quantized, capacity)
  }
}

impl<'a, T: ToDistanceComputer, S: StoreCallbacks> InsertStrategy<'a, WedbProvider<T, S>, &'a [T]>
  for DynamicQuantization
{
  type PruneStrategy = Self;

  fn insert_search_accessor(
    &'a self,
    provider: &'a WedbProvider<T, S>,
    context: &'a <WedbProvider<T, S> as DataProvider>::Context,
    vector: &'a [T],
  ) -> Result<Self::SearchAccessor, Self::SearchAccessorError> {
    let quantized = provider.is_quantized();
    DynamicAccessor::new(provider, context, vector, quantized)
  }

  fn prune_strategy(&self) -> Self::PruneStrategy {
    *self
  }
}

impl<T: ToDistanceComputer, S: StoreCallbacks> InplaceDeleteStrategy<WedbProvider<T, S>>
  for DynamicQuantization
{
  type DeleteElement<'a> = &'a [T];
  type DeleteElementGuard = Box<[T]>;
  type DeleteElementError = WedbProviderError;
  type PruneStrategy = Self;
  type DeleteSearchAccessor<'a> = DynamicAccessor<'a, T, S>;
  type SearchPostProcessor = glue::CopyIds;
  type SearchStrategy = Self;

  fn prune_strategy(&self) -> Self::PruneStrategy {
    Self
  }

  fn search_strategy(&self) -> Self::SearchStrategy {
    Self
  }

  fn search_post_processor(&self) -> Self::SearchPostProcessor {
    glue::CopyIds
  }

  fn get_delete_element<'a>(
    &'a self,
    provider: &'a WedbProvider<T, S>,
    context: &'a <WedbProvider<T, S> as DataProvider>::Context,
    id: <WedbProvider<T, S> as DataProvider>::InternalId,
  ) -> impl future::Future<Output = Result<Self::DeleteElementGuard, Self::DeleteElementError>> + Send
  {
    let mut v = vec![T::default(); provider.dim];
    if !provider.callbacks.read_single_iid(context, id, &mut v) {
      return future::ready(Err(StoreError::Read.into()));
    }
    future::ready(Ok(v.into()))
  }
}
