//! 索引生命周期：起点状态机（IndexState）、取消守卫、就绪保障与按量化
//! 类型构造索引（对标 diskann-garnet IndexState / create_index_impl）

use super::{ann_index::DiskANNIndex, *};

/// 索引就绪状态（对标 diskann-garnet IndexState）。
#[derive(Debug, PartialEq)]
enum IndexState {
  /// 图中尚无起点。
  NoStartPoints,
  /// 某线程正在设置起点。
  SettingStartPoints,
  /// 起点已设置，索引就绪。
  Ready,
}

impl IndexState {
  #[inline]
  pub const fn from_usize(value: usize) -> Self {
    match value {
      0 => IndexState::NoStartPoints,
      1 => IndexState::SettingStartPoints,
      _ => IndexState::Ready,
    }
  }
}

impl From<usize> for IndexState {
  #[inline]
  fn from(value: usize) -> Self {
    Self::from_usize(value)
  }
}

/// 索引几何与量化参数（对标 C# CreateIndex 入参子集）。

#[derive(Debug, Clone, Copy)]
pub struct IndexConfig {
  /// 向量维度。
  pub dims: u32,
  /// 降维后维度（0 = 不降维）。
  pub reduce_dims: u32,
  /// 量化类型。
  pub quant_type: VectorQuantType,
  /// 距离度量。
  pub distance_metric: VectorDistanceMetricType,
  /// 构建期探索因子（L_build）。
  pub build_exploration_factor: u32,
  /// 每层链接数（M，即图最大度）。
  pub num_links: u32,
}

/// CAS 成功至状态落定窗的取消守卫（rust 异步化独有取消安全面；C# 侧起点
/// 装载在原生操作栈内同步闭环，无 mid-flight 丢弃窗）。
///
/// 形态对齐 `wnode` 的 `ObserverDropGuard` 先例：栈上零分配守卫，仅持状态
/// 原子引用＋落定标志。init future 在任一 await 点被取消丢弃（KILL/注销
/// 胜出，见 `wnode::resp::slow_path` 头注取消面自证）时 Drop 兜底复位
/// `NoStartPoints`，杜绝状态机永卡非 Ready 冻结写面；正常落定两臂先解除
/// 守卫再 store，杜绝 drop 与 store 竞写。
struct StartPointLoadGuard<'a> {
  state: &'a AtomicUsize,
  done: bool,
}

impl Drop for StartPointLoadGuard<'_> {
  #[inline]
  fn drop(&mut self) {
    if !self.done {
      self
        .state
        .store(IndexState::NoStartPoints as usize, Ordering::Release);
    }
  }
}

/// 保障索引就绪（无起点时运行 `init` 设起点），失败返回错误。
///
/// 异步形态：起点装载经存储回调（async）闭环。自旋让位改协程协作
/// `yield_now().await`（同步 `thread::yield_now` 在任务栈内让出的是线程
/// 时间片而非执行权，compio 任务队列无法推进）。等待臂自旋让位语义不变；
/// CAS 成功点单点挂 [`StartPointLoadGuard`]，覆盖 init 体全部 await 点。
pub(super) async fn ensure_index_ready_or_init<S: StoreCallbacks, F, E>(
  index: &Index<S>,
  init: F,
) -> Option<E>
where
  F: AsyncFnOnce() -> Option<E>,
{
  let mut spin_count = 0usize;
  loop {
    match index.state.load(Ordering::Acquire).into() {
      IndexState::Ready => break,
      IndexState::SettingStartPoints => {
        spin_count += 1;
        if spin_count < 32 {
          spin_loop();
        } else {
          yield_now().await;
        }
        continue;
      }
      IndexState::NoStartPoints => {
        if index
          .state
          .compare_exchange(
            IndexState::NoStartPoints as usize,
            IndexState::SettingStartPoints as usize,
            Ordering::AcqRel,
            Ordering::Acquire,
          )
          .is_ok()
        {
          // 守卫横跨 init 全 await 窗：取消丢弃由 Drop 兜底复位，
          // 落定臂先解除守卫再 store
          let mut guard = StartPointLoadGuard {
            state: &index.state,
            done: false,
          };
          if let Some(err) = init().await {
            guard.done = true;
            index
              .state
              .store(IndexState::NoStartPoints as usize, Ordering::Release);
            return Some(err);
          }
          guard.done = true;
          index
            .state
            .store(IndexState::Ready as usize, Ordering::Release);
          break;
        }
      }
    }
  }
  None
}

/// 按量化类型选择向量元素类型并构造静态分派索引（对标 create_index_impl）。
pub(super) async fn create_index_impl<T: ToDistanceComputer, S: StoreCallbacks>(
  params: &IndexConfig,
  config: config::Config,
  metric_type: Metric,
  callbacks: Callbacks<S>,
  context: &Context,
  wrap: impl FnOnce(DiskANNIndex<WedbProvider<T, S>>) -> IndexImpl<S>,
) -> Result<(Arc<Index<S>>, bool), WedbProviderError> {
  let dim = params.dims as usize;
  let max_degree = params.num_links as usize;
  let quant_type = params.quant_type;
  let provider = WedbProvider::<T, S>::new(
    dim,
    params.reduce_dims,
    quant_type,
    metric_type,
    max_degree,
    callbacks,
    context,
  )
  .await?;
  let state = if provider.start_points_exist() {
    IndexState::Ready as usize
  } else {
    IndexState::NoStartPoints as usize
  };

  // 仅需训练的量化器（Bin 系）需要调度建表
  let quant_needed = match quant_type {
    VectorQuantType::Bin | VectorQuantType::XbinI8 | VectorQuantType::XbinU8 => {
      provider.quantization_needed()
    }
    _ => false,
  };

  let dims = provider.dim;
  let index_inst = DiskANNIndex::new(config, provider);
  Ok((
    Arc::new(Index {
      inner: wrap(index_inst),
      quant_type,
      dims,
      state: AtomicUsize::new(state),
      insert_gate: AsyncMutex::new(()),
    }),
    quant_needed,
  ))
}
