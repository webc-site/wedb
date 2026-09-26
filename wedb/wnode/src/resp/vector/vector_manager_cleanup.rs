//! 清理任务族（对标 libs/server/Resp/Vector/VectorManager.Cleanup.cs）
//!
//! C# 侧 cleanup / requestCleanup 两条常驻任务经无界通道串联，配合暂停闸门
//!（cleanupGate）与等待原语保证检查点/静默语义；C# 第三条 requestDrop 任务的唯一
//! 生产点是主存记录逐出触发器（GarnetRecordTriggers 的 OnEvict 臂），rust 索引
//! 记录驻留 VectorManager 登记表、不入 wkv 值域，无逐出事件可接，故整链不落地。
//! Rust 侧基于 compio 异步运行时协程与事件驱动原语实现，消灭阻塞自旋与 OS 线程绑定。

use std::{
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
use wbase::supervise::{supervise_item, supervise_task};
use wvector::store::StoreCallbacks;

use super::vector_manager::VectorManager;

/// 监督快照里的任务名（wbase::supervise 归组键，INFO bg_task_health 可见）
const VECTOR_CLEANUP_TASK: &str = "vector_cleanup";
/// 同上（请求清理协程）
const VECTOR_REQUEST_CLEANUP_TASK: &str = "vector_request_cleanup";
/// 逐项监督归组名（主清理处理体）
const VECTOR_CLEANUP_ITEM: &str = "vector_cleanup_item";
/// 同上（请求清理处理体）
const VECTOR_REQUEST_CLEANUP_ITEM: &str = "vector_request_cleanup_item";

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

/// 清理协程类别（对标 C# 常驻清理任务的收敛句柄）。
///
/// 在 garnet 中的相对路径:libs/server/Resp/Vector/VectorManager.cs:Dispose
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum CleanupTaskKind {
  /// 请求清理协程（RunRequestCleanupTaskAsync）
  RequestCleanup,
  /// 主清理协程（RunCleanupTaskAsync）
  Cleanup,
}

impl CleanupTaskKind {
  /// 协程类别数（计数数组定长，与 [`Self::index`] 取值域一致）。
  pub const COUNT: usize = 2;

  /// 通道数组下标（对齐 C# Dispose 关闭顺序：requestCleanup → cleanup）。
  fn index(self) -> usize {
    match self {
      CleanupTaskKind::RequestCleanup => 0,
      CleanupTaskKind::Cleanup => 1,
    }
  }
}

/// 清理任务运行时：协程在跑计数（按类别）+ 收敛事件驱动 + 生产拉起句柄托管。
///
/// compio 的 `JoinHandle` 一旦 drop 即 `task.cancel`（见 compio-executor
/// join_handle.rs Drop 实现），会让清理协程被静默取消——这正是原实现「生产零拉起
/// 后通道无消费者」的根因。故拉起得到的协程句柄必须收进本结构长期托管，
/// 仅在 Dispose 收敛（协程自然退出）后释放。
///
/// 在 garnet 中的相对路径:libs/server/Resp/Vector/VectorManager.cs:Dispose
#[derive(Default)]
pub struct CleanupRuntime {
  /// 每类协程当前在跑标志（0/1）。下标由 [`CleanupTaskKind::index`] 决定。
  running: [AtomicUsize; CleanupTaskKind::COUNT],
  /// 协程结束通知，供 [`Self::wait_stopped`] 同步收敛等待。
  event: Event,
  /// 生产链路托管的协程句柄（compio JoinHandle，须存活至协程退出）。
  handles: Mutex<Vec<JoinHandle<()>>>,
  /// 生产链路是否已拉起清理协程，保证 [`VectorManager::ensure_cleanup_tasks_started`] 幂等。
  started: AtomicBool,
}

impl CleanupRuntime {
  /// 创建运行时。
  pub fn new() -> Self {
    Self {
      running: [const { AtomicUsize::new(0) }; CleanupTaskKind::COUNT],
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
    !self.is_running(CleanupTaskKind::RequestCleanup) && !self.is_running(CleanupTaskKind::Cleanup)
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

impl<S: StoreCallbacks> VectorManager<S> {
  /// libs/server/Resp/Vector/VectorManager.Cleanup.cs:OnException
  ///
  /// 清理循环的异常钩子：记录并保持循环存活（C# 以 catch 包裹整轮）。
  pub fn on_exception(&self, context: u64, error: &str) {
    log::error!("During Vector Set cleanup for context {context}: {error}");
  }

  /// 生产链路拉起：确保两条清理常驻协程已在当前 compio 运行时启动并托管。
  ///
  /// 对标 C# `VectorManager` 构造器（VectorManager.cs:213-215）——C# 在构造器
  /// 内直接 spawn 三条任务；rust 只承接有生产点的两条（见模块头说明）。
  /// Rust 构造发生在 compio 运行时之外，故沿用量化协程
  /// 的「首会话 get_session 惰性拉起」范式，在首个存储会话建立时启动。
  /// 以 `started` 标志保证幂等，协程句柄收进 [`CleanupRuntime::handles`]
  /// 托管（drop 会 cancel 协程），协程自然退出后由 [`Self::dispose_cleanup`] 释放。
  ///
  ///（C# 对位 VectorManager.cs 构造器内的清理任务装配段，本函数非构造本身）
  pub fn ensure_cleanup_tasks_started(self: &Arc<Self>) {
    if self.cleanup_runtime.started.swap(true, Ordering::AcqRel) {
      return;
    }
    let mut handles = self.cleanup_runtime.handles.lock();
    handles.push(self.run_cleanup_task_async());
    handles.push(self.run_request_cleanup_task_async());
  }

  /// 停机收敛：按 requestCleanup → cleanup 顺序关闭通道并等待
  /// 对应协程消费退出，杜绝清理任务在关闭后丢失。
  ///
  /// 对标 C# `VectorManager.Dispose`（VectorManager.cs:468-497）逐通道
  /// `CompleteAndWaitForConsumerTask`。必须在主线程、`shutdown_coordinator.stop()`
  /// 之后（Phase 1 已阻断新连接，收敛期无新入写任务撞已闭通道）且 worker
  /// join 之前调用——消费协程栖 worker 运行时，硬约束仅先于 join（join 即
  /// 运行时析构屏障）。cleanup 通道最后关闭，保证
  /// requestCleanup 协程退出前投入主清理通道的清理任务不被丢弃。
  ///
  /// 在 garnet 中的相对路径:libs/server/Resp/Vector/VectorManager.cs:Dispose
  pub fn dispose_cleanup(&self) -> bool {
    let timeout = InstantDuration::from_millis(30_000);
    let mut converged = true;

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
      // 会话绑定归处理体内置（[`Self::process_cleanup`] 顶：对标 C#
      // RunCleanupTaskAsync 的一次性 `dropSession`，每项自持自解绑，
      // 守卫绝不跨上方的 `wait_to_read()` / `wait()` 持有）
      // panic 隔离走 wbase [`supervise_item`] 单点（与请求清理臂同口径，
      // 对标 C# RunCleanupTaskAsync 逐项 catch 后进入下轮），只断本项不清扫任务
      if let Err(e) = supervise_item(VECTOR_CLEANUP_ITEM, self.process_cleanup(context)).await {
        self.on_exception(context, &format!("panic: {}", e.text()));
      }
    }
    self.cleanup_runtime.stop(CleanupTaskKind::Cleanup);
  }

  /// libs/server/Resp/Vector/VectorManager.Cleanup.cs:RunCleanupTaskAsync
  ///
  /// 启动主清理任务轻量协程（基于 compio::runtime::spawn）。任务体经 wbase
  /// [`supervise_task`] 监督：panic 落日志与监督计数（逐项守卫下仅为兜底），
  /// INFO bg_task_health 可见。
  pub fn run_cleanup_task_async(self: &Arc<Self>) -> JoinHandle<()> {
    let manager = Arc::clone(self);
    spawn(async move {
      let _ = supervise_task(VECTOR_CLEANUP_TASK, manager.run_cleanup_task_loop()).await;
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
      // 同主清理臂：会话绑定归处理体内置（[`Self::process_request_cleanup`] 顶）；
      // panic 隔离走 wbase [`supervise_item`] 单点（原裸调与主清理臂双口径，
      // 对标 C# RunRequestCleanupTaskAsync 整体 try/catch 循环续跑）——panic
      // 落 on_exception 账，协程永续，杜绝通道积压的会话销毁清理永停
      if let Err(e) = supervise_item(
        VECTOR_REQUEST_CLEANUP_ITEM,
        self.process_request_cleanup(context),
      )
      .await
      {
        self.on_exception(context, &format!("panic: {}", e.text()));
      }
    }
    self.cleanup_runtime.stop(CleanupTaskKind::RequestCleanup);
  }

  /// libs/server/Resp/Vector/VectorManager.Cleanup.cs:RunRequestCleanupTaskAsync
  ///
  /// 启动请求清理任务轻量协程（基于 compio::runtime::spawn）。任务体经 wbase
  /// [`supervise_task`] 监督：panic 落日志与监督计数（逐项守卫下仅为兜底），
  /// INFO bg_task_health 可见。
  pub fn run_request_cleanup_task_async(self: &Arc<Self>) -> JoinHandle<()> {
    let manager = Arc::clone(self);
    spawn(async move {
      let _ = supervise_task(
        VECTOR_REQUEST_CLEANUP_TASK,
        manager.run_request_cleanup_task_loop(),
      )
      .await;
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

  /// 单个 context 的 drop 清扫：先物理清扫日志中该上下文全部元素记录
  ///（对标 C# RunCleanupTaskAsync 的 IterateLookupSnapshot 扫描删除段，经
  /// [`StoreCallbacks::purge_context`] 下发宿主存储），成功后终结清理标记并
  /// 归还上下文。
  ///
  /// 次序即并发纪律：上下文仅在清扫完成后归还复用（C# FinishedCleaningUp
  /// 同序，清理期间标记在位使新分配跳过本块），清扫与复用无竞态。清扫失败
  /// 时保持清理中隔离并重投主清理通道待重扫（C# 清理循环异常留标记重试，
  /// 同语义；通道已关闭即停机收敛中，残留交重启恢复面的清理中标记承接）。
  pub async fn process_cleanup(&self, context: u64) {
    // 处理项自持专用会话（协程臂与测试直调共用口径，对标 C# dropSession）：
    // purge 与元数据写透经本执行域绑定的会话落盘，离开即解绑销毁
    let _domain = self.bind_dedicated_session();
    // 清扫为存储异步回调（purge_context 契约 async 化）：直接 `.await` 闭环
    //（compio 单线程 runtime 下专用会话守卫随本 async 栈帧跨 await 存活，
    // 任务不迁线程，守卫线程槽纪律保持；线程槽守卫自身绝不另行跨 await）
    if !self.callbacks.store().purge_context(context).await {
      log::error!(
        "Failure during background cleanup of deleted vector sets, implies storage leak: \
         context={context}"
      );
      if !self.cleanup_task_channel.push(context) {
        log::warn!("Could not re-queue cleanup of Vector Set context: {context}");
      }
      return;
    }

    let (context_index, context_value) = Self::decompose_context(context);
    // 元数据守卫块作用域精确限定，写透 `.await` 前已让位（parking_lot
    // 守卫绝不跨 await）
    let finished = {
      let mut metas = self.context_metadatas.lock();
      let Some(meta) = metas.get_mut(context_index) else {
        return;
      };
      let allow_zero = context_index != 0;
      if meta.is_cleaning_up(allow_zero, context_value) {
        meta.finished_cleaning_up(allow_zero, context_value);
        true
      } else {
        false
      }
    };
    if finished {
      self.dirty_context_metadatas.lock().insert(context_index);
      self.update_context_metadata().await;
    }
  }

  /// 请求清理处理：元数据标记清理中并投递主清理通道。
  ///
  /// 对齐 C# RunRequestCleanupTaskAsync：仅负责标记清理中状态 + 唤醒主清理
  /// 任务，绝不重复执行索引丢弃——`Service.DropIndex` 由持条带独占写锁的
  /// 主删除链路（[`VectorManager::delete_vector_set`] 等入口 →
  /// request_deletion → drop_index）单点收敛；后台无锁二次丢弃与 C# 契约
  /// 分叉，且脱离锁保护存在砸中复用上下文的并发竞态（ptr=0 记录主链路
  /// 依约不丢弃，旧实现此处会无条件强丢）。
  pub async fn process_request_cleanup(&self, context: u64) {
    // 同主清理臂：处理项自持专用会话（update_context_metadata 写透登记表
    // 旁路记录须有当前执行域会话可用）；dev 侧收口后本臂不再执行索引丢弃
    // （drop_index 由持条带独占写锁的主删除链路单点收敛），但专用会话绑定
    // 仍为登记表写透所必需，两纪律并存。
    let _domain = self.bind_dedicated_session();
    let (context_index, context_value) = Self::decompose_context(context);
    let marked = {
      let mut metas = self.context_metadatas.lock();
      if let Some(meta) = metas.get_mut(context_index) {
        let allow_zero = context_index != 0;
        if meta.is_in_use(allow_zero, context_value)
          && !meta.is_cleaning_up(allow_zero, context_value)
        {
          meta.mark_cleaning_up(allow_zero, context_value);
          drop(metas);
          self.dirty_context_metadatas.lock().insert(context_index);
          true
        } else {
          false
        }
      } else {
        false
      }
    };
    if marked {
      self.update_context_metadata().await;
      if !self.cleanup_task_channel.push(context) {
        log::warn!("Could not request cleanup of Vector Set context: {context}");
      }
    }
  }
}
