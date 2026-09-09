//! 清理任务族（对标 libs/server/Resp/Vector/VectorManager.Cleanup.cs）
//!
//! C# 侧三条常驻任务（cleanup / requestCleanup / requestDrop）经无界通道
//! 串联，配合暂停闸门（cleanupGate）与等待原语保证检查点/静默语义；
//! Rust 侧以 std 线程 + [`VectorSetCleanupWorkChannel`] + [`CleanupGate`]
//! 承接，处理体同样支持同步单步驱动（确定性测试）。

use std::{
  panic::{self, AssertUnwindSafe},
  sync::Arc,
  thread,
  time::{Duration, Instant},
};

use parking_lot::{Condvar, Mutex};

use super::{
  vector_manager::{INDEX_SIZE_BYTES, VectorManager},
  vector_types::VectorSetFlags,
};

/// 清理暂停闸门（C# SemaphoreSlim cleanupGate 的承接）。
#[derive(Default)]
pub struct CleanupGate {
  paused: Mutex<bool>,
  signal: Condvar,
}

impl CleanupGate {
  /// 创建闸门。
  pub fn new() -> Self {
    Self::default()
  }

  /// 等待闸门放行（暂停期间阻塞）。
  pub fn wait(&self) {
    let mut paused = self.paused.lock();
    while *paused {
      self.signal.wait(&mut paused);
    }
  }

  /// 是否处于暂停状态。
  pub fn is_paused(&self) -> bool {
    *self.paused.lock()
  }

  /// 内部置位。
  fn set_paused(&self, value: bool) {
    let mut paused = self.paused.lock();
    *paused = value;
    if !value {
      self.signal.notify_all();
    }
  }
}

/// 清理任务运行时：生命周期计数 + 静默事件。
#[derive(Default)]
pub struct CleanupRuntime {
  running: Mutex<usize>,
  signal: Condvar,
}

impl CleanupRuntime {
  /// 创建运行时。
  pub fn new() -> Self {
    Self::default()
  }

  /// 任务开始登记。
  fn on_start(&self) {
    *self.running.lock() += 1;
  }

  /// 任务结束登记。
  fn on_stop(&self) {
    let mut running = self.running.lock();
    *running = running.saturating_sub(1);
    if *running == 0 {
      self.signal.notify_all();
    }
  }

  /// 是否全部静默。
  pub fn is_quiescent(&self) -> bool {
    *self.running.lock() == 0
  }
}

impl VectorManager {
  /// libs/server/Resp/Vector/VectorManager.Cleanup.cs:OnException
  ///
  /// 清理循环的异常钩子：记录并保持循环存活（C# 以 catch 包裹整轮）。
  pub fn on_exception(&self, context: u64, error: &str) {
    log::error!("During Vector Set cleanup for context {context}: {error}");
  }

  /// libs/server/Resp/Vector/VectorManager.Cleanup.cs:OnStart
  ///
  /// 清理任务开始（登记运行时）。
  pub fn on_start(&self) {
    self.cleanup_runtime.on_start();
  }

  /// libs/server/Resp/Vector/VectorManager.Cleanup.cs:OnStop
  ///
  /// 清理任务结束（注销运行时并唤醒静默等待者）。
  pub fn on_stop(&self) {
    self.cleanup_runtime.on_stop();
  }

  /// libs/server/Resp/Vector/VectorManager.Cleanup.cs:RunCleanupTaskAsync
  ///
  /// 主清理循环：从 cleanup 通道取 context，执行元数据层的上下文回收。
  /// 常驻形态为线程；此处提供单轮处理以支撑确定性驱动。
  pub fn run_cleanup_task_async(self: &Arc<Self>) {
    let manager = Arc::clone(self);
    let _ = thread::Builder::new()
      .name("vector-cleanup".into())
      .spawn(move || {
        manager.on_start();
        let channel = &manager.cleanup_task_channel;
        while channel.wait_to_read(250) {
          manager.cleanup_gate.wait();
          let Some(context) = channel.try_read() else {
            continue;
          };
          if let Err(e) = panic::catch_unwind(AssertUnwindSafe(|| {
            manager.process_cleanup(context);
          })) {
            manager.on_exception(context, &format!("panic: {e:?}"));
          }
        }
        manager.on_stop();
      })
      .expect("清理线程创建失败");
  }

  /// libs/server/Resp/Vector/VectorManager.Cleanup.cs:RunRequestCleanupTaskAsync
  ///
  /// 请求清理循环：把"待清理"标记落进元数据并调度主清理。
  pub fn run_request_cleanup_task_async(self: &Arc<Self>) {
    let manager = Arc::clone(self);
    let _ = thread::Builder::new()
      .name("vector-request-cleanup".into())
      .spawn(move || {
        manager.on_start();
        let channel = &manager.request_cleanup_task_channel;
        while channel.wait_to_read(250) {
          let Some(context) = channel.try_read() else {
            continue;
          };
          manager.process_request_cleanup(context);
        }
        manager.on_stop();
      })
      .expect("请求清理线程创建失败");
  }

  /// libs/server/Resp/Vector/VectorManager.Cleanup.cs:RunRequestDropTaskAsync
  ///
  /// 请求丢弃循环：每轮服务整个 requested_drops 积压；逐键以独占锁
  /// 保护 DropIndex，锁释放后标记完成（对齐 C# 的锁序与 TryComplete 时机）。
  pub fn run_request_drop_task_async(self: &Arc<Self>) {
    let manager = Arc::clone(self);
    let _ = thread::Builder::new()
      .name("vector-request-drop".into())
      .spawn(move || {
        manager.on_start();
        let channel = &manager.request_drop_task_channel;
        while channel.wait_to_read(250) {
          if channel.try_read().is_none() {
            continue;
          }
          // 每趟服务整个积压（信号本身无载荷，多余信号直接排空）
          channel.drain_pending();
          manager.process_request_drop_once();
        }
        manager.on_stop();
      })
      .expect("请求丢弃线程创建失败");
  }

  /// libs/server/Resp/Vector/VectorManager.Cleanup.cs:PauseCleanupAsync
  ///
  /// 暂停清理（检查点前调用），闸门置位。
  pub fn pause_cleanup_async(&self) {
    self.cleanup_gate.set_paused(true);
  }

  /// libs/server/Resp/Vector/VectorManager.Cleanup.cs:ResumeCleanup
  ///
  /// 恢复清理；调用方必须保证每次 PauseCleanupAsync 后最终配对调用。
  pub fn resume_cleanup(&self) {
    self.cleanup_gate.set_paused(false);
  }

  /// libs/server/Resp/Vector/VectorManager.Cleanup.cs:DropRequested
  ///
  /// 该键是否已登记内存索引丢弃请求。
  pub fn drop_requested(&self, key: &[u8]) -> bool {
    self.requested_drops.lock().contains_key(key)
  }

  /// libs/server/Resp/Vector/VectorManager.Cleanup.cs:WaitForDiskANNIndexDrop
  ///
  /// 自旋等待指定键的丢弃完成；禁止持有任何向量集合锁调用。
  pub fn wait_for_disk_ann_index_drop(&self, key: &[u8]) {
    while self.drop_requested(key) {
      thread::yield_now();
    }
  }

  /// libs/server/Resp/Vector/VectorManager.Cleanup.cs:WaitForQuiescence
  ///
  /// 等待全部清理任务静默。
  pub fn wait_for_quiescence(&self, timeout_ms: u64) -> bool {
    let deadline = Instant::now() + Duration::from_millis(timeout_ms);
    let mut running = self.cleanup_runtime.running.lock();
    while *running > 0 {
      let remain = deadline.saturating_duration_since(Instant::now());
      if remain.is_zero() {
        return false;
      }
      self.cleanup_runtime.signal.wait_for(&mut running, remain);
    }
    true
  }

  /// libs/server/Resp/Vector/VectorManager.Cleanup.cs:VectorSetPotentiallyDeleted
  ///
  /// 键可能已删除（压缩期发现）；记录键 → 上下文，供检查点完成后活性复查。
  pub fn vector_set_potentially_deleted(&self, key: &[u8], value: &[u8]) {
    let Some(index) = super::vector_manager__index::Index::from_bytes(value) else {
      log::error!(
        "Unexpected index size on Vector Set during compaction, {} != {}",
        value.len(),
        INDEX_SIZE_BYTES
      );
      return;
    };

    // 记录可能已死，但对 Vector Set 不做任何推断
    if index.flags.contains(VectorSetFlags::SUPPRESS_CLEANUP) {
      return;
    }

    self
      .potentially_deleted
      .lock()
      .insert(key.to_vec(), index.context);
  }

  /// libs/server/Resp/Vector/VectorManager.Cleanup.cs:CheckpointCompleted
  ///
  /// 检查点完成：对全部"可能已删除"键做活性复查，键缺失或上下文已变
  /// 即判定旧集合已死，发布请求清理（对齐 C# QueueCleanups 的复查路径）。
  pub fn checkpoint_completed(&self) {
    let entries: Vec<(Vec<u8>, u64)> = self.potentially_deleted.lock().drain().collect();
    for (key, context) in entries {
      // 活性复查：WRONGTYPE / 缺失 / 上下文已变 → 旧 Vector Set 已死
      let needs_delete = match self.read_stored_index(&key) {
        None => true,
        Some(bytes) => super::vector_manager__index::Index::from_bytes(&bytes)
          .is_none_or(|live| live.context != context),
      };
      if needs_delete && !self.request_cleanup_task_channel.try_publish(context) {
        log::warn!("Could not request delete of abandoned Vector Set");
      }
    }
  }

  /// libs/server/Resp/Vector/VectorManager.Cleanup.cs:QueueCleanups
  ///
  /// 把全部"清理中"上下文投入主清理通道（恢复完成后调用）。
  pub fn queue_cleanups(&self) -> usize {
    let metas = self.context_metadatas.lock();
    let mut queued = 0;
    for (i, meta) in metas.iter().enumerate() {
      if let Some(contexts) = meta.get_need_cleanup() {
        let offset = Self::offset_for_context_metadata(i);
        for ctx in contexts {
          if self
            .cleanup_task_channel
            .try_publish(offset + u64::from(ctx))
          {
            queued += 1;
          }
        }
      }
    }
    queued
  }

  // ======================== 处理体（同步形态，供线程与测试共用） ========================

  /// 单个 context 的元数据层回收：终结清理标记并归还上下文。
  pub fn process_cleanup(&self, context: u64) {
    let (context_index, context_value) = Self::decompose_context(context);
    let mut metas = self.context_metadatas.lock();
    let Some(meta) = metas.get_mut(context_index) else {
      return;
    };
    let allow_zero = context_index != 0;
    if meta.is_cleaning_up(allow_zero, context_value) {
      meta.finished_cleaning_up(allow_zero, context_value);
      drop(metas);
      self.dirty_context_metadatas.lock().insert(context_index);
      self.update_context_metadata();
    }
  }

  /// 请求清理处理：索引服务侧丢弃 + 元数据标记清理中。
  pub fn process_request_cleanup(&self, context: u64) {
    // 索引服务侧自清
    self.service.drop_index(context);

    let (context_index, context_value) = Self::decompose_context(context);
    let mut metas = self.context_metadatas.lock();
    if let Some(meta) = metas.get_mut(context_index) {
      let allow_zero = context_index != 0;
      if meta.is_in_use(allow_zero, context_value)
        && !meta.is_cleaning_up(allow_zero, context_value)
      {
        meta.mark_cleaning_up(allow_zero, context_value);
        drop(metas);
        self.dirty_context_metadatas.lock().insert(context_index);
        self.update_context_metadata();
        let _ = self.cleanup_task_channel.try_publish(context);
      }
    }
  }

  /// 索引服务侧丢弃执行。
  fn perform_drop(&self, context: u64) {
    self.service.drop_index(context);
  }

  /// 请求丢弃的单步同步处理（worker 循环体等价，供确定性测试）。
  /// 逐键独占锁内 DropIndex，锁释放后完成标记（对齐 C# 锁序）。
  pub fn process_request_drop_once(&self) {
    let keys: Vec<Vec<u8>> = self.requested_drops.lock().keys().cloned().collect();
    for key in keys {
      if let Some(context) = self.requested_drops.lock().remove(&key) {
        let _guard = self.vector_set_locks.acquire_exclusive(&key);
        self.perform_drop(context);
      }
    }
  }
}

#[cfg(test)]
mod tests {
  use super::{
    super::vector_manager::{CONTEXT_METADATA_SIZE, VectorManager, VectorManagerOptions},
    *,
  };

  #[test]
  fn gate_pauses_and_resumes() {
    let gate = CleanupGate::new();
    assert!(!gate.is_paused());
    gate.wait(); // 未暂停时立即通过

    gate.set_paused(true);
    assert!(gate.is_paused());
    gate.set_paused(false);
    gate.wait();
    assert!(!gate.is_paused());
  }

  #[test]
  fn quiescence_lifecycle() {
    let manager = VectorManager::new(VectorManagerOptions {
      is_enabled: true,
      ..Default::default()
    });
    assert!(manager.cleanup_runtime.is_quiescent());

    manager.on_start();
    assert!(!manager.wait_for_quiescence(10));
    manager.on_stop();
    assert!(manager.wait_for_quiescence(10));
  }

  #[test]
  fn request_cleanup_to_cleanup_pipeline() {
    let manager = VectorManager::new(VectorManagerOptions {
      is_enabled: true,
      ..Default::default()
    });

    // 分配上下文并标记在用
    let context = manager.next_vector_set_context(2).unwrap();
    assert!(manager.get_context_state(context).0);

    // 请求清理：索引服务丢弃 + 标记清理中 + 投递主清理
    manager.process_request_cleanup(context);
    let (in_use, cleaning_up, _) = manager.get_context_state(context);
    assert!(in_use && cleaning_up);
    assert_eq!(manager.cleanup_task_channel.try_read(), Some(context));

    // 主清理：终结并归还上下文
    manager.process_cleanup(context);
    assert_eq!(manager.get_context_state(context), (false, false, false));
  }

  #[test]
  fn queue_cleanups_walks_metadata() {
    let manager = VectorManager::new(VectorManagerOptions {
      is_enabled: true,
      ..Default::default()
    });
    let context = manager.next_vector_set_context(3).unwrap();

    // 直接标记清理中
    {
      let (context_index, context_value) = VectorManager::decompose_context(context);
      let mut metas = manager.context_metadatas.lock();
      metas[context_index].mark_cleaning_up(context_index != 0, context_value);
    }

    assert_eq!(manager.queue_cleanups(), 1);
    assert_eq!(manager.cleanup_task_channel.try_read(), Some(context));
    // 上下文仍标记"清理中"时会重复投递（C# 每个信号服务全部待清理上下文）
    assert_eq!(manager.queue_cleanups(), 1);
    // 主清理完成后归还上下文，不再投递
    manager.process_cleanup(context);
    assert_eq!(manager.queue_cleanups(), 0);
  }

  #[test]
  fn drop_request_wait_and_checkpoint_flow() {
    let manager = VectorManager::new(VectorManagerOptions {
      is_enabled: true,
      ..Default::default()
    });
    manager.service.create_index(
      9,
      1,
      0,
      super::super::vector_types::VectorQuantType::NoQuant,
      8,
      2,
      super::super::vector_types::VectorDistanceMetricType::L2,
    );

    let key = b"drop-me".to_vec();
    let record = super::super::vector_manager__index::Index {
      context: 9,
      index_ptr: 1,
      dimensions: 1,
      reduce_dims: 0,
      num_links: 2,
      build_exploration_factor: 8,
      quant_type: super::super::vector_types::VectorQuantType::NoQuant,
      distance_metric: super::super::vector_types::VectorDistanceMetricType::L2,
      flags: super::VectorSetFlags::NONE,
    };
    manager.vector_set_potentially_deleted(&key, &record.to_bytes());
    assert!(manager.potentially_deleted.lock().contains_key(&key));

    manager.pause_cleanup_async();
    assert!(manager.cleanup_gate.is_paused());

    // 模拟 requestDrop 登记 → 同步处理 → 等待解除
    manager.requested_drops.lock().insert(key.clone(), 9);
    assert!(manager.drop_requested(&key));
    manager.process_request_drop_once();
    manager.wait_for_disk_ann_index_drop(&key);
    assert_eq!(manager.service.card(9), 0);

    // 键仍存活且上下文未变 → 检查点复查不发布清理
    manager.write_stored_index(&key, &record.to_bytes());
    manager.checkpoint_completed();
    assert!(manager.potentially_deleted.lock().is_empty());
    assert!(!manager.request_cleanup_task_channel.has_pending());

    // 键已消失 → 复查判定已死，发布请求清理
    manager.vector_set_potentially_deleted(&key, &record.to_bytes());
    manager.remove_stored_index(&key);
    manager.checkpoint_completed();
    assert_eq!(manager.request_cleanup_task_channel.try_read(), Some(9));

    // 异常钩子不 panic
    manager.on_exception(0, "synthetic");
  }

  #[test]
  fn context_metadata_size_alias_matches() {
    assert_eq!(CONTEXT_METADATA_SIZE, 160);
    assert_eq!(super::INDEX_SIZE_BYTES, 56);
  }
}
