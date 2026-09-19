//! 清理任务族（对标 libs/server/Resp/Vector/VectorManager.Cleanup.cs）
//!
//! C# 侧三条常驻任务（cleanup / requestCleanup / requestDrop）经无界通道
//! 串联，配合暂停闸门（cleanupGate）与等待原语保证检查点/静默语义；
//! Rust 侧基于 compio 异步运行时协程与事件驱动原语实现，消灭阻塞自旋与 OS 线程绑定。

use std::{
  panic::{self, AssertUnwindSafe},
  sync::{
    Arc,
    atomic::{AtomicBool, AtomicUsize, Ordering},
  },
  time::Duration,
};

use coarsetime::{Duration as InstantDuration, Instant};
use compio::{
  runtime::{JoinHandle, spawn},
  time::timeout as compio_timeout,
};
use event_listener::{Event, Listener};
use parking_lot::Mutex;

use super::vector_manager::VectorManager;

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

/// 清理协程类别（对标 C# 三条常驻任务的收敛句柄）。
///
/// 在 garnet 中的相对路径:libs/server/Resp/Vector/VectorManager.cs:Dispose
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum CleanupTaskKind {
  /// 请求丢弃协程（RunRequestDropTaskAsync）
  RequestDrop,
  /// 请求清理协程（RunRequestCleanupTaskAsync）
  RequestCleanup,
  /// 主清理协程（RunCleanupTaskAsync）
  Cleanup,
}

impl CleanupTaskKind {
  /// 通道数组下标（对齐 C# Dispose 关闭顺序：requestDrop → requestCleanup → cleanup）。
  fn index(self) -> usize {
    match self {
      CleanupTaskKind::RequestDrop => 0,
      CleanupTaskKind::RequestCleanup => 1,
      CleanupTaskKind::Cleanup => 2,
    }
  }
}

/// 清理任务运行时：协程在跑计数（按类别）+ 收敛事件驱动 + 生产拉起句柄托管。
///
/// compio 的 `JoinHandle` 一旦 drop 即 `task.cancel`（见 compio-executor
/// join_handle.rs Drop 实现），会让清理协程被静默取消——这正是原实现「生产零拉起
/// 后通道无消费者」的根因。故拉起得到的三个句柄必须收进本结构长期托管，
/// 仅在 Dispose 收敛（协程自然退出）后释放。
///
/// 在 garnet 中的相对路径:libs/server/Resp/Vector/VectorManager.cs:Dispose
#[derive(Default)]
pub struct CleanupRuntime {
  /// 每类协程当前在跑标志（0/1）。下标由 [`CleanupTaskKind::index`] 决定。
  running: [AtomicUsize; 3],
  /// 协程结束通知，供 [`Self::wait_stopped`] 同步收敛等待。
  event: Event,
  /// 生产拉起托管的三个协程句柄（compio JoinHandle，须存活至协程退出）。
  handles: Mutex<Vec<JoinHandle<()>>>,
  /// 生产链路是否已拉起清理协程，保证 [`VectorManager::ensure_cleanup_tasks_started`] 幂等。
  started: AtomicBool,
}

impl CleanupRuntime {
  /// 创建运行时。
  pub fn new() -> Self {
    Self {
      running: [const { AtomicUsize::new(0) }; 3],
      event: Event::new(),
      handles: Mutex::new(Vec::new()),
      started: AtomicBool::new(false),
    }
  }

  /// 协程开始登记（对应指定类别）。
  fn start(&self, kind: CleanupTaskKind) {
    self.running[kind.index()].store(1, Ordering::Release);
  }

  /// 协程结束登记（对应指定类别），唤醒静默/收敛等待者。
  fn stop(&self, kind: CleanupTaskKind) {
    self.running[kind.index()].store(0, Ordering::Release);
    self.event.notify(usize::MAX);
  }

  /// 指定类别协程当前是否在跑。
  fn is_running(&self, kind: CleanupTaskKind) -> bool {
    self.running[kind.index()].load(Ordering::Acquire) != 0
  }

  /// 是否全部静默。
  pub fn is_quiescent(&self) -> bool {
    !self.is_running(CleanupTaskKind::RequestDrop)
      && !self.is_running(CleanupTaskKind::RequestCleanup)
      && !self.is_running(CleanupTaskKind::Cleanup)
  }

  /// 同步等待指定类别协程退出（通道已关闭后调用），带超时。
  ///
  /// 对译 C# Dispose 内 `CompleteAndWaitForConsumerTask` 的逐通道收敛等待。
  fn wait_stopped(&self, kind: CleanupTaskKind, timeout: InstantDuration) -> bool {
    let deadline = Instant::now() + timeout;
    loop {
      // 先注册监听再判状态，避免与 stop 的 notify 竞态丢唤醒
      let listener = self.event.listen();
      if !self.is_running(kind) {
        return true;
      }
      let now = Instant::now();
      if now >= deadline {
        return !self.is_running(kind);
      }
      let _ = listener.wait_timeout((deadline - now).into());
    }
  }

  /// 同步等待全部清理任务静默。
  pub fn wait_for_quiescence(&self, timeout_ms: u64) -> bool {
    let deadline = Instant::now() + InstantDuration::from_millis(timeout_ms);
    while !self.is_quiescent() {
      let listener = self.event.listen();
      if self.is_quiescent() {
        return true;
      }
      let now = Instant::now();
      if now >= deadline {
        return self.is_quiescent();
      }
      let _ = listener.wait_timeout((deadline - now).into());
    }
    true
  }

  /// 异步等待全部清理任务静默。
  pub async fn wait_for_quiescence_async(&self, timeout: Duration) -> bool {
    let deadline = Instant::now() + timeout.into();
    loop {
      let listener = {
        if self.is_quiescent() {
          return true;
        }
        self.event.listen()
      };
      let now = Instant::now();
      if now >= deadline {
        return self.is_quiescent();
      }
      if compio_timeout((deadline - now).into(), listener)
        .await
        .is_err()
      {
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

  /// 生产链路拉起：确保三条清理常驻协程已在当前 compio 运行时启动并托管。
  ///
  /// 对标 C# `VectorManager` 构造器（VectorManager.cs:213-215）——C# 在构造器
  /// 内直接 spawn 三任务；Rust 构造发生在 compio 运行时之外，故沿用量化协程
  /// 的「首会话 get_session 惰性拉起」范式，在首个存储会话建立时启动。
  /// 以 `started` 标志保证幂等，三个 `JoinHandle` 收进 [`CleanupRuntime::handles`]
  /// 托管（drop 会 cancel 协程），协程自然退出后由 [`Self::dispose_cleanup`] 释放。
  ///
  /// 在 garnet 中的相对路径:libs/server/Resp/Vector/VectorManager.cs:VectorManager
  pub fn ensure_cleanup_tasks_started(self: &Arc<Self>) {
    if self.cleanup_runtime.started.swap(true, Ordering::AcqRel) {
      return;
    }
    let mut handles = self.cleanup_runtime.handles.lock();
    handles.push(self.run_cleanup_task_async());
    handles.push(self.run_request_cleanup_task_async());
    handles.push(self.run_request_drop_task_async());
  }

  /// 停机收敛：按 requestDrop → requestCleanup → cleanup 顺序关闭通道并等待
  /// 对应协程消费退出，杜绝清理任务在关闭后丢失。
  ///
  /// 对标 C# `VectorManager.Dispose`（VectorManager.cs:468-497）逐通道
  /// `CompleteAndWaitForConsumerTask`。必须在主线程、`shutdown_coordinator.stop()`
  /// 之前调用，此时 worker 运行时仍在驱动排空。cleanup 通道最后关闭，保证
  /// requestCleanup 协程退出前投入主清理通道的清理任务不被丢弃。
  ///
  /// 在 garnet 中的相对路径:libs/server/Resp/Vector/VectorManager.cs:Dispose
  pub fn dispose_cleanup(&self) -> bool {
    let timeout = InstantDuration::from_millis(30_000);
    let mut converged = true;

    self.request_drop_task_channel.close();
    if !self
      .cleanup_runtime
      .wait_stopped(CleanupTaskKind::RequestDrop, timeout)
    {
      converged = false;
      log::warn!("Vector cleanup coroutine RequestDrop did not converge before timeout");
    }

    self.request_cleanup_task_channel.close();
    if !self
      .cleanup_runtime
      .wait_stopped(CleanupTaskKind::RequestCleanup, timeout)
    {
      converged = false;
      log::warn!("Vector cleanup coroutine RequestCleanup did not converge before timeout");
    }

    self.cleanup_task_channel.close();
    if !self
      .cleanup_runtime
      .wait_stopped(CleanupTaskKind::Cleanup, timeout)
    {
      converged = false;
      log::warn!("Vector cleanup coroutine Cleanup did not converge before timeout");
    }

    // 协程已自然退出（或超时），释放托管句柄；对已完成任务的 cancel 为空操作
    self.cleanup_runtime.handles.lock().clear();
    self.cleanup_runtime.started.store(false, Ordering::Release);
    converged
  }

  /// 主清理协程循环体。
  pub async fn run_cleanup_task_loop(self: Arc<Self>) {
    self.cleanup_runtime.start(CleanupTaskKind::Cleanup);
    let channel = &self.cleanup_task_channel;
    while channel.wait_to_read().await {
      self.cleanup_gate.wait().await;
      let Some(context) = channel.try_pop() else {
        continue;
      };
      if let Err(e) = panic::catch_unwind(AssertUnwindSafe(|| {
        self.process_cleanup(context);
      })) {
        self.on_exception(context, &format!("panic: {e:?}"));
      }
    }
    self.cleanup_runtime.stop(CleanupTaskKind::Cleanup);
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
    self.cleanup_runtime.start(CleanupTaskKind::RequestCleanup);
    let channel = &self.request_cleanup_task_channel;
    while channel.wait_to_read().await {
      let Some(context) = channel.try_pop() else {
        continue;
      };
      self.process_request_cleanup(context);
    }
    self.cleanup_runtime.stop(CleanupTaskKind::RequestCleanup);
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
    self.cleanup_runtime.start(CleanupTaskKind::RequestDrop);
    let channel = &self.request_drop_task_channel;
    while channel.wait_to_read().await {
      if channel.try_pop().is_none() {
        continue;
      }
      // 每趟服务整个积压（信号本身无载荷，多余信号直接排空）
      channel.drain().len();
      self.process_request_drop_once();
    }
    self.cleanup_runtime.stop(CleanupTaskKind::RequestDrop);
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
  /// 该键是否已登记内存索引丢弃请求；`rk` 为登记表复合键（丢弃通道
  /// 载荷与锁协议同为复合键域）。
  pub fn drop_requested(&self, rk: &[u8]) -> bool {
    self.requested_drops.contains(rk)
  }

  /// libs/server/Resp/Vector/VectorManager.Cleanup.cs:WaitForDiskANNIndexDrop
  ///
  /// 同步等待指定键的丢弃完成（复合键域）；禁止持有任何向量集合锁调用。
  pub fn wait_for_disk_ann_index_drop(&self, rk: &[u8]) {
    self.requested_drops.wait_for_completion(rk);
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
          if self.cleanup_task_channel.push(offset + u64::from(ctx)) {
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
        if !self.cleanup_task_channel.push(context) {
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
    // 载荷即登记表复合键，锁轴同域
    for (rk, context) in pending {
      let _guard = self.vector_set_locks.acquire_exclusive(&rk);
      self.perform_drop(context);
      drop(_guard);
      if !self.requested_drops.try_complete(&rk) {
        log::error!("Drop for raced with some other cleanup, this should never happen");
      }
    }
  }
}
