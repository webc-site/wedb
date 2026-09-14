//! 向量操作复制面（对标 libs/server/Resp/Vector/VectorManager.Replication.cs）
//!
//! VADD/VREM/VSETATTR 以合成写注入 AOF 供副本重放；主侧经重放通道
//! （VADDReplicationState）把全量同步数据应用到本机索引。
//! C# 的多 reader Channel + CountingEventSlim 以
//! [`wbase::pool::EventWorkQueue`]
//! 与等待计数承接；多日志场景的多写者语义由通道的并发安全保证。

use std::{
  hint::spin_loop,
  sync::{
    Arc,
    atomic::{AtomicBool, AtomicUsize, Ordering},
  },
  time::{Duration, Instant},
};

use compio::{
  runtime::{JoinHandle, spawn},
  time::timeout as compio_timeout,
};
use event_listener::{Event, Listener};
use wbase::pool::EventWorkQueue;
use wvector::{VectorDistanceMetricType, VectorQuantType, VectorSetFlags, VectorValueType};

use super::vector_manager::{
  VADD_APPEND_LOG_ARG, VADD_SET_FLAGS_ARG, VREM_APPEND_LOG_ARG, VSETATTR_APPEND_LOG_ARG,
  VectorManager,
};

/// VADD 重放状态（对齐 C# VADDReplicationState record struct）。
#[derive(Debug, Clone, PartialEq)]
pub struct VaddReplicationState {
  /// Vector Set 键。
  pub key: Vec<u8>,
  /// 向量维度。
  pub dims: u32,
  /// 降维后维度。
  pub reduce_dims: u32,
  /// 值格式。
  pub value_type: VectorValueType,
  /// 向量数据。
  pub values: Vec<u8>,
  /// 元素键。
  pub element: Vec<u8>,
  /// 量化类型。
  pub quantizer: VectorQuantType,
  /// 构建期探索因子。
  pub build_exploration_factor: u32,
  /// 属性。
  pub attributes: Vec<u8>,
  /// 链接数（M）。
  pub num_links: u32,
  /// 距离度量。
  pub distance_metric: VectorDistanceMetricType,
}

/// 单条复制记录（合成写的通道形态）。
#[derive(Debug, Clone, PartialEq)]
pub struct ReplicationRecord {
  /// 特殊 RMW 操作哨兵（arg1）。
  pub log_arg: i64,
  /// 命名空间字节。
  pub namespace_bytes: Vec<u8>,
  /// 键字节。
  pub key: Vec<u8>,
  /// 值字节。
  pub value: Vec<u8>,
}

/// 副本运行时：重放通道 + 阻塞事件 + 活动标志。
pub struct ReplicationRuntime {
  /// 重放通道（主 → 本机重放应用）。
  replay_channel: EventWorkQueue<VaddReplicationState>,
  /// 合成写记录流（对齐 AOF 注入语义）。
  replication_log: EventWorkQueue<ReplicationRecord>,
  /// 阻塞事件计数（副本操作进行中时置位，对齐 CountingEventSlim）。
  blocked: AtomicUsize,
  /// 阻塞事件驱动通知（对齐 CountingEventSlim 事件驱动通知）。
  block_event: Event,
  /// 重放任务是否已启动。
  replay_started: AtomicUsize,
  /// 副本任务活动标志（对齐 AreReplicationTasksActive）。
  active: AtomicBool,
  /// 最近一条记录的 arg（测试观测）。
  last_arg: AtomicUsize,
  /// 累计注入的合成写条数（测试观测）。
  log_count: AtomicUsize,
}

impl Default for ReplicationRuntime {
  fn default() -> Self {
    Self::new()
  }
}

impl ReplicationRuntime {
  /// 创建运行时。
  pub fn new() -> Self {
    Self {
      replay_channel: EventWorkQueue::new(),
      replication_log: EventWorkQueue::new(),
      blocked: AtomicUsize::new(0),
      block_event: Event::new(),
      replay_started: AtomicUsize::new(0),
      active: AtomicBool::new(false),
      last_arg: AtomicUsize::new(0),
      log_count: AtomicUsize::new(0),
    }
  }

  /// 注入一条合成复制写。
  pub(crate) fn replicate(&self, log_arg: i64, namespace_bytes: &[u8], key: &[u8], value: &[u8]) {
    self.last_arg.store(log_arg as usize, Ordering::Relaxed);
    self.log_count.fetch_add(1, Ordering::Relaxed);
    let _ = self.replication_log.push(ReplicationRecord {
      log_arg,
      namespace_bytes: namespace_bytes.to_vec(),
      key: key.to_vec(),
      value: value.to_vec(),
    });
  }

  /// 累计注入的合成写条数（测试观测；不消费记录流）。
  pub fn replay_len(&self) -> usize {
    self.log_count.load(Ordering::Relaxed)
  }

  /// 最近一条记录的 arg（测试观测）。
  pub fn last_arg(&self) -> i64 {
    self.last_arg.load(Ordering::Relaxed) as i64
  }

  /// 副本操作进入/退出（阻塞事件计数）。
  pub fn enter_operation(&self) {
    self.blocked.fetch_add(1, Ordering::AcqRel);
  }

  pub fn exit_operation(&self) {
    self.blocked.fetch_sub(1, Ordering::AcqRel);
    self.block_event.notify(usize::MAX);
  }

  /// 是否有副本操作进行中。
  pub fn is_blocked(&self) -> bool {
    self.blocked.load(Ordering::Acquire) > 0
  }

  /// 副本重放与操作是否已处于完全静默状态（通道关闭或无排队且无进行中操作）。
  #[inline(always)]
  pub fn is_quiescent(&self) -> bool {
    self.replay_channel.is_closed() || (self.replay_channel.is_empty() && !self.is_blocked())
  }
}

use wvector::store::StoreCallbacks;

impl<S: StoreCallbacks> VectorManager<S> {
  /// libs/server/Resp/Vector/VectorManager.Replication.cs:StartReplicationTasksAsync
  ///
  /// 启动复制重放任务（带取消标志）。
  pub fn start_replication_tasks_async(self: &Arc<Self>) -> Option<JoinHandle<()>> {
    self.replication.active.store(true, Ordering::Release);
    self.start_replication_replay_tasks()
  }

  /// libs/server/Resp/Vector/VectorManager.Replication.cs:ReplicateVectorSetAdd
  ///
  /// VADD 的复制注入（合成 VADD 写，AOF: YES）。
  pub fn replicate_vector_set_add(
    &self,
    key: &[u8],
    element: &[u8],
    values: &[u8],
    attributes: &[u8],
    dims: u32,
    quant: VectorQuantType,
  ) {
    self
      .replication
      .replicate(VADD_APPEND_LOG_ARG, &[], key, element);
    // 数据面经重放通道随队（对齐 C# 的 parseState 参数携带）
    if !self.replication.replay_channel.push(VaddReplicationState {
      key: key.to_vec(),
      dims,
      reduce_dims: 0,
      value_type: VectorValueType::FP32,
      values: values.to_vec(),
      element: element.to_vec(),
      quantizer: quant,
      build_exploration_factor: 0,
      attributes: attributes.to_vec(),
      num_links: 0,
      distance_metric: VectorDistanceMetricType::Cosine,
    }) {
      log::warn!("向量重放通道发布失败");
    }
  }

  /// libs/server/Resp/Vector/VectorManager.Replication.cs:ReplicateVectorSetRemove
  ///
  /// VREM 的复制注入（合成 VREM 写，AOF: YES）。
  pub fn replicate_vector_set_remove(&self, key: &[u8], element: &[u8]) {
    self
      .replication
      .replicate(VREM_APPEND_LOG_ARG, &[], key, element);
  }

  /// libs/server/Resp/Vector/VectorManager.Replication.cs:ReplicateVectorSetSetAttribute
  ///
  /// VSETATTR 的复制注入（合成写，AOF: YES）。
  /// 保留 _element 形参以匹配 ReplicateVectorSetSetAttribute 对标签名规范
  pub fn replicate_vector_set_set_attribute(&self, key: &[u8], _element: &[u8], attribute: &[u8]) {
    // 属性作为合成写的值字节随 AOF 重放（对齐 C# parseState 参数携带）
    self
      .replication
      .replicate(VSETATTR_APPEND_LOG_ARG, &[], key, attribute);
  }

  /// libs/server/Resp/Vector/VectorManager.Replication.cs:HandleVectorSetAddReplication
  ///
  /// 副本侧处理 VADD 复制：解析参数并应用到本机索引。
  pub fn handle_vector_set_add_replication(&self, state: &VaddReplicationState) {
    self.apply_vector_set_add(state);
  }

  /// libs/server/Resp/Vector/VectorManager.Replication.cs:StartReplicationReplayTasks
  ///
  /// 启动重放任务轻量协程（基于 compio::runtime::spawn）。
  pub fn start_replication_replay_tasks(self: &Arc<Self>) -> Option<JoinHandle<()>> {
    if self
      .replication
      .replay_started
      .compare_exchange(0, 1, Ordering::AcqRel, Ordering::Acquire)
      .is_err()
    {
      return None; // 已启动
    }
    self.replication.active.store(true, Ordering::Release);
    let manager = Arc::clone(self);
    Some(spawn(async move {
      manager.run_replication_replay_task_loop().await;
    }))
  }

  /// libs/server/Resp/Vector/VectorManager.Replication.cs:StartReplicaTaskAsync
  ///
  /// 异步重放协程主循环：由 channel.wait_to_read().await 驱动，批量消费重放项。
  pub async fn run_replication_replay_task_loop(self: Arc<Self>) {
    let channel = &self.replication.replay_channel;
    while channel.wait_to_read().await {
      while let Some(state) = channel.try_pop() {
        self.replication.enter_operation();
        self.handle_vector_set_add_replication(&state);
        self.replication.exit_operation();
      }
    }
    self.replication.active.store(false, Ordering::Release);
    self.replication.block_event.notify(usize::MAX);
  }

  /// libs/server/Resp/Vector/VectorManager.Replication.cs:ApplyVectorSetAdd
  ///
  /// 重放状态应用：在本地索引上执行一次 VADD 语义。
  /// 上下文按 Vector Set 键自登记表解析；键缺失时以复制参数补建（幂等）。
  pub fn apply_vector_set_add(&self, state: &VaddReplicationState) {
    let Some(context) = self.resolve_replay_context(state) else {
      // 上下文耗尽（超出分配上限），丢弃该条重放
      return;
    };
    let prepared =
      match wvector::prepare_vector_data(state.quantizer, state.value_type, &state.values) {
        Ok(p) => p,
        Err(_) => return,
      };
    let res = self
      .service
      .insert(context, &state.element, &prepared.bytes, &state.attributes);
    log::trace!("向量重放插入结果: {res:?}");
  }

  /// 重放上下文解析：命中既有记录取其 context；缺失则建原生索引并落新记录
  /// （ReadOrCreateVectorIndex 的重放侧形态）。
  fn resolve_replay_context(&self, state: &VaddReplicationState) -> Option<u64> {
    if let Some(bytes) = self.read_stored_index(&state.key) {
      return super::vector_manager_index::Index::from_bytes(&bytes).map(|i| i.context);
    }

    // 新键：分配上下文 + 建原生索引 + 落记录
    let context = self.next_vector_set_context(0)?;
    let record = super::vector_manager_index::Index {
      context,
      index_ptr: 1,
      dimensions: state.dims,
      reduce_dims: state.reduce_dims,
      num_links: state.num_links.max(1),
      build_exploration_factor: state.build_exploration_factor,
      quant_type: state.quantizer,
      distance_metric: state.distance_metric,
      flags: VectorSetFlags::NONE,
    };
    let _ = self
      .service
      .create_index(context, record.index_config(), self.callbacks.clone());
    self.write_stored_index(&state.key, &record.to_bytes());
    Some(context)
  }

  /// libs/server/Resp/Vector/VectorManager.Replication.cs:ResetReplayTasksAsync
  ///
  /// 重置重放任务（返回剩余未消费项数）。
  pub fn reset_replay_tasks_async(&self) -> usize {
    self.replication.replay_started.store(0, Ordering::Release);
    let count = self.replication.replay_channel.drain().len();
    self.replication.block_event.notify(usize::MAX);
    count
  }

  /// libs/server/Resp/Vector/VectorManager.Replication.cs:ShutdownReplayTasks
  ///
  /// 关闭重放通道（不再接收新项）。
  pub fn shutdown_replay_tasks(&self) {
    self.replication.replay_channel.close();
    self.replication.active.store(false, Ordering::Release);
    self
      .replication
      .replay_started
      .store(usize::MAX, Ordering::Release);
    self.replication.block_event.notify(usize::MAX);
  }

  /// libs/server/Resp/Vector/VectorManager.Replication.cs:HandleVectorSetRemoveReplication
  ///
  /// 副本侧处理 VREM 复制：按键解析上下文后执行 TryRemove 语义。
  pub fn handle_vector_set_remove_replication(&self, key: &[u8], element: &[u8]) {
    let Some(bytes) = self.read_stored_index(key) else {
      return;
    };
    let Some(index) = super::vector_manager_index::Index::from_bytes(&bytes) else {
      return;
    };
    let _ = self.service.remove(index.context, element);
  }

  /// libs/server/Resp/Vector/VectorManager.Replication.cs:HandleVectorSetSetAttributeReplication
  ///
  /// 副本侧处理 VSETATTR 复制：按键解析上下文后执行 TrySetAttribute 语义。
  pub fn handle_vector_set_set_attribute_replication(
    &self,
    key: &[u8],
    element: &[u8],
    attribute: &[u8],
  ) {
    let Some(bytes) = self.read_stored_index(key) else {
      return;
    };
    let Some(index) = super::vector_manager_index::Index::from_bytes(&bytes) else {
      return;
    };
    if !self
      .service
      .set_attribute(index.context, element, attribute)
    {
      log::debug!("设置向量属性未生效: context={}", index.context);
    }
  }

  /// libs/server/Resp/Vector/VectorManager.Replication.cs:WaitForVectorOperationsToComplete
  ///
  /// 等待进行中的向量操作完成（重放通道排空且无阻塞事件）。
  /// 结合前置轻量自旋与事件驱动超时等待，彻底消灭盲目阻塞休眠。
  pub fn wait_for_vector_operations_to_complete(&self, timeout_ms: u64) -> bool {
    let timeout = Duration::from_millis(timeout_ms);
    let deadline = Instant::now() + timeout;

    // 前置轻量自旋快速检查微秒级操作
    for _ in 0..32 {
      if self.replication.is_quiescent() {
        return true;
      }
      spin_loop();
    }

    loop {
      if self.replication.is_quiescent() {
        return true;
      }
      let now = Instant::now();
      if now >= deadline {
        return false;
      }
      let listener = self.replication.block_event.listen();
      if self.replication.is_quiescent() {
        return true;
      }
      let _ = listener.wait_timeout(deadline - now);
    }
  }

  /// 异步等待进行中的向量操作完成（重放通道排空且无阻塞事件）。
  pub async fn wait_for_vector_operations_to_complete_async(&self, timeout: Duration) -> bool {
    let deadline = Instant::now() + timeout;

    for _ in 0..16 {
      if self.replication.is_quiescent() {
        return true;
      }
      spin_loop();
    }

    loop {
      if self.replication.is_quiescent() {
        return true;
      }
      let now = Instant::now();
      if now >= deadline {
        return false;
      }
      let listener = self.replication.block_event.listen();
      if self.replication.is_quiescent() {
        return true;
      }
      if compio_timeout(deadline - now, listener).await.is_err() {
        return false;
      }
    }
  }

  /// libs/server/Resp/Vector/VectorManager.Replication.cs:HandleVectorSetRenameCopy
  ///
  /// 重命名复制：旧键 SuppressCleanup + 新键登记（对齐 C# 的复制路径）。
  pub fn handle_vector_set_rename_copy(&self, old_key: &[u8], new_key: &[u8], value: &[u8]) {
    // 旧键：抑制清理（另一持有者接管索引）
    if let Some(mut stored) = self.read_stored_index(old_key) {
      stored[40] = VectorSetFlags::SUPPRESS_CLEANUP.bits();
      self.write_stored_index(old_key, &stored);
    }
    // 新键：登记复制写
    self
      .replication
      .replicate(VADD_SET_FLAGS_ARG, &[], new_key, value);
  }
}
