//! 动态量化双轨策略与检索/剪枝访问器
//!
//! 实现全精度与量化向量透明自适应双轨运行，包含训练、回填、访问器及重排后处理。

use std::{future, mem, mem::size_of, sync::atomic::Ordering};

use bytemuck::cast_slice;
use diskann_quantization::alloc::Poly;
use diskann_utils::{
  object_pool::{PooledRef, Undef},
  views::{Matrix, MatrixView},
};
use diskann_vector::DistanceFunction;
use webc_diskann::{
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

use super::{
  cache::{AdjList, AlignToEight, DelegateNeighborAccessor},
  data_provider::{DistanceComputer, QueryComputer, ToDistanceComputer, WedbProvider},
};
use crate::{
  error::{QuantizerError, StoreError, WedbProviderError},
  quantization::{QuantizerImpl, WedbQuantizer},
  store::{Callbacks, Context, StoreCallbacks, Term, VectorSetId},
};

/// 量化状态与量化表存储在 Metadata 项下的专用键（`_qnt`）。
pub(crate) const QUANT_STATE_KEY: u32 = u32::from_be_bytes(*b"_qnt");

/// 重排序预分配缓冲初始容量。
pub(crate) const RERANK_BUFFER_LENGTH: usize = 1024;

/// 批读 pair 串的长度前缀字（`[LPI, I1, LPI, I2, …]`，对标 garnet lpiid 协议）。
const LPI: u32 = 4;

/// 批量读回调值长度守卫：值长度与距离核期望（量化 `quantizer.bytes()` /
/// 全精度 `full_vector_size`）精确等长方可进距离核；异长（磁盘静默损坏、
/// 异源写入）跳过该项并 warn 留痕。Spherical canonical 校验要求精确等长
/// ——超长与截短同弃，全精度臂 unaligned_view 截断面同判据，杜绝静默
/// 错距。契约锚：板块 4.1 异常收敛纪律 + unwrap 规范第 7 条（守卫前置后
/// 下游 unwrap 不可达）。
#[inline]
fn value_len_ok(context: u64, iid: u32, v: &[u8], expected: usize) -> bool {
  if v.len() == expected {
    return true;
  }
  log::warn!(
    "向量记录值长度异常，距离计算跳过: context={context:#x} iid={iid} len={} expected={expected}",
    v.len()
  );
  false
}

/// 批量读通道选择：量化态读 Quantized 域（量化字节数），否则读 Vector 域
/// （全精度字节数）；三条 beam 臂与剪枝工作集四路共用，杜绝双形态。
#[inline]
fn batch_read_ctx<T: ToDistanceComputer, S: StoreCallbacks>(
  context: &Context,
  provider: &WedbProvider<T, S>,
  quantized: bool,
) -> (Context, usize) {
  if quantized {
    (context.term(Term::Quantized), provider.quant_vector_size())
  } else {
    (context.term(Term::Vector), provider.full_vector_size())
  }
}

/// 冷区批量收割单源：pair 串按对解码 → 长度守卫 → 交付 `on(对下标, 内部 id, 值字节)`。
/// 距离核求值留给 `on`（双轨访问器与全精度重排各自的核不同）；空批由存储层短路
/// （[`Callbacks::read_multi_lpiid`] 空入参恒真），收割失败 ⇒ 本批交付不完整，
/// 报错终止（禁止静默半截结果）。
async fn harvest<T: ToDistanceComputer, S: StoreCallbacks, F>(
  provider: &WedbProvider<T, S>,
  ids: &[u32],
  context: &Context,
  expected: usize,
  mut on: F,
) -> ANNResult<()>
where
  F: FnMut(usize, u32, &[u8]) + Send,
{
  if provider
    .callbacks
    .read_multi_lpiid(context, ids, expected, |i, v| {
      let iid = ids[i as usize * 2 + 1];
      if value_len_ok(context.inner(), iid, v, expected) {
        on(i as usize, iid, v);
      }
    })
    .await
  {
    Ok(())
  } else {
    Err(StoreError::Read.into())
  }
}

/// backfill 弃跑留痕（原生 log 文案逐字节不变）：恒返 false 供调用位直接 return。
fn backfill_bail<S: StoreCallbacks>(
  callbacks: &Callbacks<S>,
  context: &Context,
  reason: &str,
) -> bool {
  callbacks.log(
    &context.term(Term::Quantized),
    &format!(
      "Error: backfill_quant_vectors: {reason}. Index will operate full precision only mode."
    ),
  );
  false
}

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
    self.quantizer.as_ref().is_some_and(|quantizer| {
      !self.is_quantized()
        && quantizer.is_trained()
        && self.max_internal_id() as usize > quantizer.required_vectors()
    })
  }

  /// 训练量化器并导出量化状态字节（标志位 + 序列化体）。
  ///
  /// `as_f32` 视图返回类型非 Send：整段收在本函数内，视图随返回释放，不跨
  /// 调用方后续落盘 await；任一环节失手返 None 由调用方整体按弃。
  fn train_state(
    &self,
    quantizer: &QuantizerImpl,
    data: &Matrix<T>,
    rows: usize,
  ) -> Option<Vec<u8>> {
    let view = data.subview(0..rows)?;
    let converted = T::as_f32(view.as_slice()).ok()?;
    let view = MatrixView::try_from(&*converted, view.nrows(), view.ncols()).ok()?;
    quantizer.train(self.metric_type, view).ok()?;
    let state = quantizer.serialize().ok()?;
    let mut total = vec![0u8; state.len() + 1];
    total[1..].copy_from_slice(&state);
    Some(total)
  }

  /// 训练量化器（对标 diskann-garnet provider.rs:train_quantizer；对原生
  /// 「已训臂恒 false 不调度」与「恰等收尾」的刻意改写已登 deviations §128）。
  ///
  /// 契约全序「持久化→启用→回填调度」：serialize→写 `_qnt`→
  /// `enable_quantization` 全部收在 `training_lock` 临界段内同步闭环
  /// （async_lock 守卫 Send 可跨 await，冷 I/O 全程无同步锁持有，不触
  /// compio 禁忌）；落盘或屏障失败即返 false 不派发，杜绝并发早退者在
  /// `_qnt` 未达、上界未收口时抢先派发分片。锁竞争 try_lock 失败按弃返回
  /// false，由首训者派发，杜绝双轮分片叠加。
  pub async fn train_quantizer(&self, context: &Context) -> bool {
    let Some(_training_guard) = self.training_lock.try_lock() else {
      return false;
    };

    let Some(quantizer) = &self.quantizer else {
      return false;
    };

    if quantizer.is_trained() {
      // 早退臂：仅重启崩溃窗形态补齐启用屏障并返 true 派发回填（r27
      // 发现二）——判据为本实例确已观测到落盘的 `_qnt`（训练已写态、标志
      // 未置即重启，量化器反序列化即 is_trained，而本实例回填上界恒
      // u32::MAX）；本进程落盘失败者（量化器已训而 `_qnt` 不在盘）与其他
      // 首训者已在锁段内落盘 + 收口的重复建表项（上界已有限）均返 false
      // 按弃，对标原生「已训练臂直返 false，不触发回填调度」，杜绝 `_qnt`
      // 未达时派发分片与双轮分片叠加
      if self.fsm.max_id_for_backfill() == u32::MAX
        && self
          .callbacks
          .read_varsize_iid::<u8>(&context.term(Term::Metadata), QUANT_STATE_KEY)
          .await
          .is_some_and(|state| !state.is_empty())
      {
        self.fsm.enable_quantization().await;
        return true;
      }
      return false;
    }

    let rows = quantizer.required_vectors();
    let mut data = Matrix::<T>::new(T::default(), rows, self.dim);

    // 两阶段训练取样：visit_used 的同步闭包内不可 await（ID 映射读取已
    // async 化），先收集存活 id，再逐个异步读取向量行——顺序与原单趟一致
    let mut sample_ids = Vec::with_capacity(rows);
    if self
      .fsm
      .visit_used(context, |id| {
        if id == 0 {
          return true;
        }
        if sample_ids.len() >= rows {
          return false;
        }
        sample_ids.push(id);
        true
      })
      .await
      .is_err()
    {
      return false;
    }

    let sampled = sample_ids.len();
    for (row, id) in sample_ids.into_iter().enumerate() {
      if !self
        .callbacks
        .read_single_iid(&context.term(Term::Vector), id, data.row_mut(row))
        .await
      {
        return false;
      }
    }

    if sampled < quantizer.required_vectors() {
      return false;
    }

    // 训练 + 序列化（as_f32 视图返回类型非 Send：收在函数边界内，随返回释放，
    // 不跨后续落盘 await）；任一环节失手整体按弃返回 false
    let Some(quant_state) = self.train_state(quantizer, &data, sampled) else {
      return false;
    };

    // 落盘与启用屏障在锁段内闭环（全序前两段；第三段「回填调度」由本函数
    // 返 true 后经调度器派发）
    if !self
      .callbacks
      .write_iid(&context.term(Term::Metadata), QUANT_STATE_KEY, &quant_state)
      .await
    {
      return false;
    }
    self.fsm.enable_quantization().await;
    true
  }

  /// 批量回填历史向量量化编码。
  pub async fn backfill_quant_vectors(
    &self,
    context: &Context,
    task_idx: usize,
    task_count: usize,
  ) -> bool {
    let Some(quantizer) = &self.quantizer else {
      return backfill_bail(&self.callbacks, context, "Quantizer not found");
    };

    let max_id = self.fsm.max_id_for_backfill() as usize;
    if max_id >= u32::MAX as usize {
      return backfill_bail(
        &self.callbacks,
        context,
        "Couldn't calculate max id to backfill",
      );
    }

    let task_count = task_count.min(max_id + 1);
    if task_idx >= task_count {
      return backfill_bail(&self.callbacks, context, "Bad task index");
    }

    let work_count = (max_id + 1).div_ceil(task_count);
    let start_id = (work_count * task_idx) as u32;
    let end_id = (work_count * (task_idx + 1)).min(max_id + 1) as u32;

    let mut v = vec![T::default(); self.dim];
    let mut f = vec![0f32; self.dim];
    let mut q = vec![0u8; quantizer.bytes()];
    for id in start_id..end_id {
      // 逐级失手即弃本 id（对标原生四段 continue 臂）：读失败留空，
      // 读成功方有合法 v，其后转换失配 / 压缩失败 / 写失败均静默跳项
      if self
        .callbacks
        .read_single_iid(&context.term(Term::Vector), id, &mut v)
        .await
        && T::as_f32_into(&v, &mut f).is_ok()
        && quantizer.compress(&f, &mut q).is_ok()
      {
        let _ = self
          .callbacks
          .write_iid(&context.term(Term::Quantized), id, &q)
          .await;
      }
    }

    // 完成判据 >= 化：恰等条件一旦被收尾弃跑轮次或重复建表项的越界计数
    // 错过即永不复等、流水线永久停摆；>= 使后续重投分片仍可越线收尾
    // （收尾段幂等，允许重翻）
    let backfill_finished =
      self.backfills_completed.fetch_add(1, Ordering::AcqRel) + 1 >= task_count as u64;

    if backfill_finished {
      // papaya pin 守卫（hazard 指针）非 Send：读段同步闭环（压缩进本地 `q`
      // 即放守卫），起点量化回写 await 在守卫外
      let start_point_backfilled = {
        let cache = self.start_point_cache.pin();
        match cache.get(&0) {
          Some(v) => match T::as_f32(cast_slice::<u8, T>(v)) {
            Ok(v_f32) => quantizer.compress(&v_f32, &mut q).is_ok(),
            Err(_) => false,
          },
          None => false,
        }
      };
      if start_point_backfilled {
        let _ = self
          .callbacks
          .write_iid(&context.term(Term::Quantized), 0, &q)
          .await;
        if let Ok(p) = Poly::from_iter(q.iter().copied(), AlignToEight) {
          self.start_point_quant_cache.pin().insert(0, p);
        }
      }

      self.fsm.enable_reuse();

      // 翻标志不缩记录：读全量量化状态（标志字节 + 序列化量化器）置位标志
      // 后整值写回。rmw 的 write_len 即目标记录尺寸（生产内核按其重建记录、
      // 旧值仅拷前缀），write_len=1 会把状态记录永久截断成单字节，重启后
      // Bin 臂 len<=1 判 InvalidQuantizer——数据完好在盘却恒重建失败不可达
      // 缺 `_qnt` 弃跑留痕（对账原生收尾失败 log 臂）：>= 判据下计数已推进，
      // 后续重投分片仍会重试收尾，本臂不再是永久停摆点
      let Some(mut quant_state) = self
        .callbacks
        .read_varsize_iid::<u8>(&context.term(Term::Metadata), QUANT_STATE_KEY)
        .await
        .filter(|state| !state.is_empty())
      else {
        return backfill_bail(
          &self.callbacks,
          context,
          "Quantizer state not found; failed to finish backfill",
        );
      };
      quant_state[0] = 1;
      if !self
        .callbacks
        .write_iid(&context.term(Term::Metadata), QUANT_STATE_KEY, &quant_state)
        .await
      {
        self.callbacks.log(
          &context.term(Term::Quantized),
          "Error saving quantizer state; failed to finish backfill. Index will operate full precision only mode.",
        );
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
pub(crate) struct DynamicQuantization;

/// 检索访问器：支持全精度与量化向量无缝切换。
pub(crate) struct DynamicAccessor<'a, T: ToDistanceComputer, S: StoreCallbacks> {
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
    // 两形态起点缓存同型，双轨唯一差在句柄选择（量化态需量化器在场）；
    // provider 先脱引用复制，令 pin 守卫只挂在 provider 上、不锁 self
    let provider = self.provider;
    let map = if self.quantized && provider.quantizer().is_some() {
      &provider.start_point_quant_cache
    } else {
      &provider.start_point_cache
    };
    let cache = map.pin();
    let dist = self
      .computer
      .evaluate_similarity(cache.get(&Self::START_ID).ok_or(StoreError::Read)?);
    self.start_point_dist = Some(dist);
    Ok(dist)
  }

  async fn compute_filter_decisions(&mut self) {
    self.filtered_decisions.clear();
    let (chunks, _) = self.filtered_ids.as_chunks::<2>();
    self.filtered_decisions.reserve(chunks.len());
    for chunk in chunks {
      let internal_id = chunk[1];
      let matches = self
        .provider
        .callbacks
        .matches_filter(self.context, internal_id)
        .await;
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
      Vec::new()
    };
    future::ready(Ok(points))
  }

  fn is_not_start_point(
    &self,
  ) -> impl future::Future<Output = ANNResult<impl Fn(Self::Id) -> bool + Send + Sync + 'static>> + Send
  {
    future::ready(Ok(move |id| id != Self::START_ID))
  }

  async fn start_point_distances<F>(&mut self, mut f: F) -> ANNResult<()>
  where
    F: FnMut(Self::Id, f32) + Send,
  {
    if !self.provider.start_points_exist() {
      return Ok(());
    }
    let dist = self.start_point_distance().map_err(ANNError::from)?;
    f(Self::START_ID, dist);
    Ok(())
  }

  async fn expand_beam<Itr, P, F>(
    &mut self,
    ids: Itr,
    mut pred: P,
    mut on_neighbors: F,
  ) -> ANNResult<()>
  where
    Itr: Iterator<Item = Self::Id> + Send,
    P: HybridPredicate<Self::Id> + Send + Sync,
    F: FnMut(Self::Id, f32) + Send,
  {
    let provider = self.provider;
    let context = self.context;
    // 邻域缓冲借出本地：筛臂内须调 &mut self 的起点距离，避免与 self 借用冲突
    let mut id_buffer = mem::take(&mut **self.id_buffer);

    for nl_id in ids {
      provider.get_neighbors(context, nl_id, &mut id_buffer).await;
      self.filtered_ids.clear();
      for id in id_buffer.iter().copied().filter(|id| pred.eval_mut(id)) {
        if id == Self::START_ID {
          on_neighbors(id, self.start_point_distance().map_err(ANNError::from)?);
        } else {
          self.filtered_ids.extend_from_slice(&[LPI, id]);
        }
      }

      let (ctx, expected) = batch_read_ctx(context, provider, self.quantized);

      // 冷区收割失败 ⇒ 本批邻域交付不完整，报错终止（禁止静默半截结果）
      if let Err(err) = harvest(provider, &self.filtered_ids, &ctx, expected, |_, iid, v| {
        on_neighbors(iid, self.computer.evaluate_similarity(v));
      })
      .await
      {
        **self.id_buffer = id_buffer;
        return Err(err);
      }
    }

    **self.id_buffer = id_buffer;
    Ok(())
  }
}

impl<T: ToDistanceComputer, S: StoreCallbacks> FilteredAccessor for DynamicAccessor<'_, T, S> {
  async fn start_point_distances<F>(&mut self, mut f: F) -> ANNResult<()>
  where
    F: FnMut(Decision<Self::Id>, f32) + Send,
  {
    if !self.provider.start_points_exist() {
      return Ok(());
    }
    let dist = self.start_point_distance().map_err(ANNError::from)?;
    f(Decision::reject(Self::START_ID), dist);
    Ok(())
  }

  async fn expand_beam_filtered<Itr, P, F>(
    &mut self,
    ids: Itr,
    mut pred: P,
    mut on_neighbors: F,
  ) -> ANNResult<()>
  where
    Itr: Iterator<Item = Self::Id> + Send,
    P: HybridPredicate<Self::Id> + Send + Sync,
    F: FnMut(Decision<Self::Id>, f32) + Send,
  {
    let provider = self.provider;
    let context = self.context;
    // 邻域缓冲借出本地：筛臂内须调 &mut self 的起点距离，避免与 self 借用冲突
    let mut id_buffer = mem::take(&mut **self.id_buffer);

    for nl_id in ids {
      provider.get_neighbors(context, nl_id, &mut id_buffer).await;
      self.filtered_ids.clear();

      for id in id_buffer.iter().copied().filter(|id| pred.eval_mut(id)) {
        if id == Self::START_ID {
          let dist = self.start_point_distance().map_err(ANNError::from)?;
          on_neighbors(Decision::reject(id), dist);
        } else {
          self.filtered_ids.extend_from_slice(&[LPI, id]);
        }
      }

      let (ctx, expected) = batch_read_ctx(context, provider, self.quantized);

      self.compute_filter_decisions().await;

      // 冷区收割失败 ⇒ 本批邻域交付不完整，报错终止（禁止静默半截结果）
      if let Err(err) = harvest(provider, &self.filtered_ids, &ctx, expected, |i, iid, v| {
        let decision = if self.filtered_decisions[i] {
          Decision::accept(iid)
        } else {
          Decision::reject(iid)
        };
        on_neighbors(decision, self.computer.evaluate_similarity(v));
      })
      .await
      {
        **self.id_buffer = id_buffer;
        return Err(err);
      }
    }

    **self.id_buffer = id_buffer;
    Ok(())
  }

  async fn expand_beam_accept_only<Itr, P, F>(
    &mut self,
    ids: Itr,
    mut pred: P,
    mut on_neighbors: F,
  ) -> ANNResult<()>
  where
    Itr: Iterator<Item = Self::Id> + Send,
    P: Predicate<Self::Id> + PredicateMut<Accept<Self::Id>> + Send + Sync,
    F: FnMut(Accept<Self::Id>, f32) + Send,
  {
    let provider = self.provider;
    let context = self.context;
    // 邻域缓冲借出本地：筛臂内须调 &mut self 的回调位，避免与 self 借用冲突
    let mut id_buffer = mem::take(&mut **self.id_buffer);

    for nl_id in ids {
      provider.get_neighbors(context, nl_id, &mut id_buffer).await;
      self.filtered_ids.clear();

      // 内联过滤为 async 回调：谓词短路链拆出 await 位（谓词求值本身仍同步）
      for id in id_buffer.iter().copied() {
        if id == Self::START_ID || !pred.eval(&id) {
          continue;
        }
        if !provider.callbacks.matches_filter(context, id).await || !pred.eval_mut(&Accept::new(id))
        {
          continue;
        }
        self.filtered_ids.extend_from_slice(&[LPI, id]);
      }

      let (ctx, expected) = batch_read_ctx(context, provider, self.quantized);

      // 冷区收割失败 ⇒ 本批邻域交付不完整，报错终止（禁止静默半截结果）
      if let Err(err) = harvest(provider, &self.filtered_ids, &ctx, expected, |_, iid, v| {
        on_neighbors(Accept::new(iid), self.computer.evaluate_similarity(v));
      })
      .await
      {
        **self.id_buffer = id_buffer;
        return Err(err);
      }
    }

    **self.id_buffer = id_buffer;
    Ok(())
  }

  fn num_starting_points(&self) -> impl future::Future<Output = ANNResult<usize>> + Send {
    future::ready(Ok(usize::from(self.provider.start_points_exist())))
  }
}

/// 候选外部 ID 导出后处理器。
#[derive(Debug, Default, Clone, Copy)]
pub(crate) struct CopyExternalIds;

impl<'a, T: ToDistanceComputer, S: StoreCallbacks>
  SearchPostProcess<DynamicAccessor<'a, T, S>, &[T], VectorSetId> for CopyExternalIds
{
  type Error = WedbProviderError;

  async fn post_process<I, B>(
    &self,
    accessor: &mut DynamicAccessor<'a, T, S>,
    _query: &[T],
    candidates: I,
    output: &mut B,
  ) -> Result<usize, Self::Error>
  where
    I: Iterator<Item = Neighbor<<DynamicAccessor<'a, T, S> as HasId>::Id>> + Send,
    B: SearchOutputBuffer<VectorSetId> + Send + ?Sized,
  {
    let initial = output.current_len();
    for n in candidates {
      let Ok(id) = accessor
        .provider
        .to_external_id(accessor.context, *n.id())
        .await
      else {
        continue;
      };

      if output.push(Neighbor::new(id, *n.distance())).is_full() {
        break;
      }
    }

    Ok(output.current_len() - initial)
  }
}

/// 全精度精排后处理器。
#[derive(Debug, Default, Clone, Copy)]
pub(crate) struct Rerank;

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
      accessor.filtered_ids.extend_from_slice(&[LPI, *nbor.id()]);
    }

    let ctx = accessor.context.term(Term::Vector);
    let expected = provider.full_vector_size();
    let mut fallback_buf = Vec::<T>::new();
    // 冷区收割失败 ⇒ 本批重排候选交付不完整，报错终止（禁止静默半截结果）
    if harvest(
      provider,
      &accessor.filtered_ids,
      &ctx,
      expected,
      |_, iid, v| {
        let dist = match bytemuck::try_cast_slice::<u8, T>(v) {
          Ok(s) => f.evaluate_similarity(query, s),
          Err(_) => {
            // 长度守卫前置后 Err 仅剩目标对齐失配：整段拷贝对齐缓冲按
            // 全维求值（截短错距臂已由守卫杜绝，v.len() 恒为整元素倍数）
            const { assert!(size_of::<T>() > 0, "NativeElement 元素尺寸恒正") }
            let count = v.len() / size_of::<T>();
            fallback_buf.resize(count, bytemuck::Zeroable::zeroed());
            bytemuck::cast_slice_mut::<T, u8>(&mut fallback_buf[..count]).copy_from_slice(v);
            f.evaluate_similarity(query, &fallback_buf[..count])
          }
        };
        reranked.push(Neighbor::new(iid, dist));
      },
    )
    .await
    .is_err()
    {
      return Err(WedbProviderError::from(StoreError::Read));
    }

    reranked.sort_unstable_by(fast_distance);

    next
      .post_process(accessor, query, reranked.iter().copied(), output)
      .await
      .map_err(|e| WedbProviderError::PostProcessing(e.to_string()))
  }
}

/// 剪枝访问器：缓存反向边与局部拓扑。
pub(crate) struct PruneAccessor<'a, T, S>
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
    let provider = self.provider;
    self.set.prepare(itr.clone());
    self.filtered_ids.clear();

    for id in itr {
      if id == 0 {
        // 两形态起点缓存同型，唯一差在句柄选择（量化态读量化起点）
        let map = if self.quantized {
          &provider.start_point_quant_cache
        } else {
          &provider.start_point_cache
        };
        if let Entry::Vacant(e) = self.set.entry(id) {
          let cache = map.pin();
          e.insert((&**cache.get(&id).ok_or(WedbProviderError::StartPoint)?).into());
        }
      } else if !self.set.contains_key(&id) {
        self.filtered_ids.extend_from_slice(&[LPI, id]);
      }
    }

    let (ctx, expected) = batch_read_ctx(self.context, provider, self.quantized);

    // 冷区收割失败 ⇒ 本批工作集交付不完整，报错终止（禁止静默半截结果）；
    // 异长值由守卫跳过插入，工作集缺项走既有 StartPoint/Read 错误路径兜底
    harvest(provider, &self.filtered_ids, &ctx, expected, |_, iid, v| {
      self.set.insert(iid, v.into());
    })
    .await?;

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
    DynamicAccessor::new(provider, context, query, provider.is_quantized())
  }
}

impl<'a, T: ToDistanceComputer, S: StoreCallbacks>
  DefaultPostProcessor<'a, WedbProvider<T, S>, &'a [T], VectorSetId> for DynamicQuantization
{
  webc_diskann::default_post_processor!(
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
    PruneAccessor::new(provider, context, provider.is_quantized(), capacity)
  }
}

impl<'a, T: ToDistanceComputer, S: StoreCallbacks> InsertStrategy<'a, WedbProvider<T, S>, &'a [T]>
  for DynamicQuantization
{
  // 候选生成复用检索臂访问器（与 SearchStrategy 同型：插入贪心搜索与查询
  // 检索共用同一全精度/量化双轨访问器）
  type SearchAccessor = DynamicAccessor<'a, T, S>;
  type SearchAccessorError = WedbProviderError;
  type PruneStrategy = Self;

  fn insert_search_accessor(
    &'a self,
    provider: &'a WedbProvider<T, S>,
    context: &'a <WedbProvider<T, S> as DataProvider>::Context,
    vector: &'a [T],
  ) -> Result<Self::SearchAccessor, Self::SearchAccessorError> {
    DynamicAccessor::new(provider, context, vector, provider.is_quantized())
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

  async fn get_delete_element<'a>(
    &'a self,
    provider: &'a WedbProvider<T, S>,
    context: &'a <WedbProvider<T, S> as DataProvider>::Context,
    id: <WedbProvider<T, S> as DataProvider>::InternalId,
  ) -> Result<Self::DeleteElementGuard, Self::DeleteElementError> {
    let mut v = vec![T::default(); provider.dim];
    if !provider
      .callbacks
      .read_single_iid(context, id, &mut v)
      .await
    {
      return Err(StoreError::Read.into());
    }
    Ok(v.into())
  }
}
