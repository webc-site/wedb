//! 清理任务族（对标 libs/server/Resp/Vector/VectorManager.Cleanup.cs）
//!
//! C# 侧三条常驻任务（cleanup / requestCleanup / requestDrop）经无界通道
//! 串联，配合暂停闸门（cleanupGate）与等待原语保证检查点/静默语义；
//! Rust 侧基于 compio 异步运行时协程与事件驱动原语实现，消灭阻塞自旋与 OS 线程绑定。

use std::{
  panic::{self, AssertUnwindSafe},
  sync::Arc,
  time::{Duration, Instant},
};

use compio::{
  runtime::{JoinHandle, spawn},
  time::timeout as compio_timeout,
};
use event_listener::{Event, Listener};
use parking_lot::Mutex;
use wvector::VectorSetFlags;

use super::vector_manager::{INDEX_SIZE_BYTES, VectorManager};

/// 清理暂停闸门（对标 C# SemaphoreSlim cleanupGate）。
#[derive(Default)]
pub struct CleanupGate {
  paused: Mutex<bool>,
  event: Event,
}

impl CleanupGate {
  /// 创建闸门。
  pub fn new() -> Self {
    Self::default()
  }

  /// 异步等待闸门放行（暂停期间异步挂起，0 OS 线程阻塞）。
  pub async fn wait(&self) {
    loop {
      let listener = {
        let paused = self.paused.lock();
        if !*paused {
          return;
        }
        self.event.listen()
      };
      listener.await;
    }
  }

  /// 同步等待闸门放行（供同步测试与兼容调用）。
  pub fn wait_sync(&self) {
    while self.is_paused() {
      let listener = self.event.listen();
      if !self.is_paused() {
        break;
      }
      listener.wait();
    }
  }

  /// 是否处于暂停状态。
  pub fn is_paused(&self) -> bool {
    *self.paused.lock()
  }

  /// 内部置位并按需唤醒等待者。
  pub fn set_paused(&self, value: bool) {
    {
      let mut paused = self.paused.lock();
      *paused = value;
    }
    if !value {
      self.event.notify(usize::MAX);
    }
  }
}

/// 清理任务运行时：生命周期计数 + 静默事件驱动。
#[derive(Default)]
pub struct CleanupRuntime {
  running: Mutex<usize>,
  event: Event,
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
      self.event.notify(usize::MAX);
    }
  }

  /// 是否全部静默。
  pub fn is_quiescent(&self) -> bool {
    *self.running.lock() == 0
  }

  /// 同步等待全部清理任务静默。
  pub fn wait_for_quiescence(&self, timeout_ms: u64) -> bool {
    let deadline = Instant::now() + Duration::from_millis(timeout_ms);
    while !self.is_quiescent() {
      let listener = self.event.listen();
      if self.is_quiescent() {
        return true;
      }
      let now = Instant::now();
      if now >= deadline {
        return self.is_quiescent();
      }
      let _ = listener.wait_timeout(deadline - now);
    }
    true
  }

  /// 异步等待全部清理任务静默。
  pub async fn wait_for_quiescence_async(&self, timeout: Duration) -> bool {
    let deadline = Instant::now() + timeout;
    loop {
      let listener = {
        let running = self.running.lock();
        if *running == 0 {
          return true;
        }
        self.event.listen()
      };
      let now = Instant::now();
      if now >= deadline {
        return self.is_quiescent();
      }
      if compio_timeout(deadline - now, listener).await.is_err() {
        return self.is_quiescent();
      }
    }
  }
}

use wvector::store::StoreCallbacks;

impl<S: StoreCallbacks> VectorManager<S> {
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

  /// 主清理协程循环体。
  pub async fn run_cleanup_task_loop(self: Arc<Self>) {
    self.on_start();
    let channel = &self.cleanup_task_channel;
    while channel.wait_to_read().await {
      self.cleanup_gate.wait().await;
      let Some(context) = channel.try_read() else {
        continue;
      };
      if let Err(e) = panic::catch_unwind(AssertUnwindSafe(|| {
        self.process_cleanup(context);
      })) {
        self.on_exception(context, &format!("panic: {e:?}"));
      }
    }
    self.on_stop();
  }

  /// libs/server/Resp/Vector/VectorManager.Cleanup.cs:RunCleanupTaskAsync
  ///
  /// 启动主清理任务轻量协程（基于 compio::runtime::spawn）。
  pub fn run_cleanup_task_async(self: &Arc<Self>) -> JoinHandle<()> {
    let manager = Arc::clone(self);
    spawn(async move {
      manager.run_cleanup_task_loop().await;
    })
  }

  /// 请求清理协程循环体。
  pub async fn run_request_cleanup_task_loop(self: Arc<Self>) {
    self.on_start();
    let channel = &self.request_cleanup_task_channel;
    while channel.wait_to_read().await {
      let Some(context) = channel.try_read() else {
        continue;
      };
      self.process_request_cleanup(context);
    }
    self.on_stop();
  }

  /// libs/server/Resp/Vector/VectorManager.Cleanup.cs:RunRequestCleanupTaskAsync
  ///
  /// 启动请求清理任务轻量协程（基于 compio::runtime::spawn）。
  pub fn run_request_cleanup_task_async(self: &Arc<Self>) -> JoinHandle<()> {
    let manager = Arc::clone(self);
    spawn(async move {
      manager.run_request_cleanup_task_loop().await;
    })
  }

  /// 请求丢弃协程循环体。
  pub async fn run_request_drop_task_loop(self: Arc<Self>) {
    self.on_start();
    let channel = &self.request_drop_task_channel;
    while channel.wait_to_read().await {
      if channel.try_read().is_none() {
        continue;
      }
      // 每趟服务整个积压（信号本身无载荷，多余信号直接排空）
      channel.drain_pending();
      self.process_request_drop_once();
    }
    self.on_stop();
  }

  /// libs/server/Resp/Vector/VectorManager.Cleanup.cs:RunRequestDropTaskAsync
  ///
  /// 启动请求丢弃任务轻量协程（基于 compio::runtime::spawn）。
  pub fn run_request_drop_task_async(self: &Arc<Self>) -> JoinHandle<()> {
    let manager = Arc::clone(self);
    spawn(async move {
      manager.run_request_drop_task_loop().await;
    })
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
    self.requested_drops.contains(key)
  }

  /// libs/server/Resp/Vector/VectorManager.Cleanup.cs:WaitForDiskANNIndexDrop
  ///
  /// 同步等待指定键的丢弃完成；禁止持有任何向量集合锁调用。
  pub fn wait_for_disk_ann_index_drop(&self, key: &[u8]) {
    self.requested_drops.wait_for_completion(key);
  }

  /// 异步等待指定键的丢弃完成；禁止持有任何向量集合锁调用。
  pub async fn wait_for_disk_ann_index_drop_async(&self, key: &[u8]) {
    self.requested_drops.wait_for_completion_async(key).await;
  }

  /// libs/server/Resp/Vector/VectorManager.Cleanup.cs:WaitForQuiescence
  ///
  /// 等待全部清理任务静默。
  pub fn wait_for_quiescence(&self, timeout_ms: u64) -> bool {
    self.cleanup_runtime.wait_for_quiescence(timeout_ms)
  }

  /// 异步等待全部清理任务静默。
  pub async fn wait_for_quiescence_async(&self, timeout: Duration) -> bool {
    self
      .cleanup_runtime
      .wait_for_quiescence_async(timeout)
      .await
  }

  /// libs/server/Resp/Vector/VectorManager.Cleanup.cs:VectorSetPotentiallyDeleted
  ///
  /// 键可能已删除（压缩期发现）；记录键 → 上下文，供检查点完成后活性复查。
  pub fn vector_set_potentially_deleted(&self, key: &[u8], value: &[u8]) {
    let Some(index) = super::vector_manager_index::Index::from_bytes(value) else {
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
        Some(bytes) => super::vector_manager_index::Index::from_bytes(&bytes)
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

  // ======================== 处理体（供协程与测试共用） ========================

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
        if !self.cleanup_task_channel.try_publish(context) {
          log::warn!("Could not request cleanup of Vector Set context: {context}");
        }
      }
    }
  }

  /// 索引服务侧丢弃执行。
  fn perform_drop(&self, context: u64) {
    self.service.drop_index(context);
  }

  /// 请求丢弃的单步处理（worker 循环体等价，供确定性测试）。
  /// 逐键独占锁内 DropIndex，锁释放后完成标记（对齐 C# 锁序与 TryComplete 时机）。
  pub fn process_request_drop_once(&self) {
    let pending = self.requested_drops.snapshot();
    for (key, context) in pending {
      let _guard = self.vector_set_locks.acquire_exclusive(&key);
      self.perform_drop(context);
      drop(_guard);
      if !self.requested_drops.try_complete(&key) {
        log::error!("Drop for raced with some other cleanup, this should never happen");
      }
    }
  }
}
