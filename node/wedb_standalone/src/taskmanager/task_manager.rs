//! 后台任务管理器：注册、运行、取消与等待
//!
//! 对标 libs/server/TaskManager/TaskManager.cs。C# 侧依赖
//! CancellationTokenSource/Task 与 ConcurrentDictionary（任务可落在任意线程池
//! 线程）；compio 为一线程一 cpu 的线程局部运行时（任务不可跨线程迁移），
//! 故映射为：CancellationTokenSource → [`CancelToken`]（链接令牌语义由"每条目
//! 独立令牌 + dispose 逐条取消"承载），Task → [`JoinHandle`]，注册表为运行时
//! 线程内 `Rc<RefCell<..>>`，无需并发字典。句柄不可克隆且被多端等待（WaitAsync
//! 与 CancelAsync 可并存），完成通知经共享 [`TaskState`] 事件分发，句柄本体
//! 随条目存续（Drop 即隐式强取消，故仅在确认完成后释放）。

use std::{
  cell::{Cell, RefCell},
  future::Future,
  pin::Pin,
  rc::Rc,
  task::{Context, Poll, Waker},
};

use compio::runtime::{CancelToken, JoinHandle, spawn};
use log::warn;
use whasher::HashMap;

use super::task_type::{TaskPlacementCategory, TaskType};

/// 完成事件广播原语（线程局部：Rc/RefCell 免锁免原子；对标 async Event 的一写多读形态）
///
/// 先置完成标志再唤醒，等待方 poll 内双检标志，通知先到不丢
struct DoneEvent {
  /// 完成标志（快检路径，避免无谓登记 waker）
  done: Cell<bool>,
  /// 登记中的等待者 waker（notify 时全部唤醒并清空）
  waiters: RefCell<Vec<Waker>>,
}

impl DoneEvent {
  fn new() -> Self {
    Self {
      done: Cell::new(false),
      waiters: RefCell::new(Vec::new()),
    }
  }

  /// 标记完成并唤醒全部等待者
  fn notify_done(&self) {
    self.done.set(true);
    for w in self.waiters.borrow_mut().drain(..) {
      w.wake();
    }
  }

  /// 是否已完成
  fn completed(&self) -> bool {
    self.done.get()
  }

  /// 可轮询等待：已完成立即 Ready，否则登记 waker
  fn poll_wait(&self, cx: &mut Context<'_>) -> Poll<()> {
    if self.done.get() {
      return Poll::Ready(());
    }
    let mut waiters = self.waiters.borrow_mut();
    // 双检：登记间隙到来的完成通知
    if self.done.get() {
      return Poll::Ready(());
    }
    if !waiters.iter().any(|w| w.will_wake(cx.waker())) {
      waiters.push(cx.waker().clone());
    }
    Poll::Pending
  }
}

/// [`DoneEvent::poll_wait`] 的可等待 future
struct DoneWait<'a> {
  event: &'a DoneEvent,
}

impl Future for DoneWait<'_> {
  type Output = ();

  fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
    self.event.poll_wait(cx)
  }
}

/// 单条任务的可共享状态：取消令牌 + 完成事件 + 句柄
struct TaskState {
  /// 本任务专属取消令牌（对标链接的 CancellationTokenSource）
  cts: CancelToken,
  /// 运行中的任务实例；确认完成后置空释放（存活期 Drop 会强取消任务）
  task: RefCell<Option<JoinHandle<()>>>,
  /// 完成事件：包装器在任务收敛后广播唤醒全部等待者
  done: DoneEvent,
}

impl TaskState {
  /// 等待任务收敛
  fn wait_done(&self) -> DoneWait<'_> {
    DoneWait { event: &self.done }
  }
}

/// 单条任务注册表：gxhash 哈希（约束：hash 一律 gxhash），随运行时线程驻留
type Registry = Rc<RefCell<HashMap<TaskType, Rc<TaskState>>>>;

/// 创建新的 TaskManager 实例
///
/// 需在 compio 运行时内构造（CancelToken 依赖线程局部 driver）
///
/// libs/server/TaskManager/TaskManager.cs:TaskManager
#[derive(Clone)]
pub struct TaskManager {
  /// 根取消令牌（对标 cts），销毁时统一撤除全部任务
  cts: CancelToken,
  registry: Registry,
}

impl TaskManager {
  pub fn new() -> Self {
    Self {
      cts: CancelToken::new(),
      registry: Rc::new(RefCell::new(HashMap::default())),
    }
  }

  /// 检查指定类型的任务是否仍在运行
  ///
  /// libs/server/TaskManager/TaskManager.cs:IsRunning
  #[must_use]
  pub fn is_running(&self, task_type: TaskType) -> bool {
    self
      .registry
      .borrow()
      .get(&task_type)
      .is_some_and(|state| !state.done.completed())
  }

  /// 检查指定类型的任务是否仍注册在册
  ///
  /// libs/server/TaskManager/TaskManager.cs:IsRegistered
  #[must_use]
  pub fn is_registered(&self, task_type: TaskType) -> bool {
    self.registry.borrow().contains_key(&task_type)
  }

  /// 以给定任务类型注册并启动新任务
  ///
  /// `cleanup_on_completion` 为真时，任务完成后自动从注册表移除条目（对标
  /// `WrapWithCleanupAsync` 的 finally：C# 还会对自身条目 CancelAsync 并 await
  /// 句柄——任务已完成时撤令牌与等待均无可观测量，且 await 自身句柄构成
  /// 自环等待，此处仅保留有观测效应的"移除条目"）
  ///
  /// 返回 false 表示该类型已在册（C# disposed 早退由 [`TaskManager::drop`]
  /// 的取消语义覆盖，注册表随实例消亡）
  ///
  /// libs/server/TaskManager/TaskManager.cs:RegisterAndRun
  pub fn register_and_run<F, Fut>(
    &self,
    task_type: TaskType,
    task_factory: F,
    cleanup_on_completion: bool,
  ) -> bool
  where
    F: FnOnce(CancelToken) -> Fut,
    Fut: Future<Output = ()> + 'static,
  {
    let state = Rc::new(TaskState {
      cts: CancelToken::new(),
      task: RefCell::new(None),
      done: DoneEvent::new(),
    });
    {
      let mut registry = self.registry.borrow_mut();
      if registry.contains_key(&task_type) {
        warn!("{task_type:?} already registered!");
        return false;
      }
      // 先产 future 再入册：spawn 仅入队不执行，包装器对注册表/状态的
      // 再借用恒发生在本借用释放之后
      let fut = task_factory(state.cts.clone());
      let task_state = Rc::clone(&state);
      let registry_for_cleanup = Rc::clone(&self.registry);
      let task = spawn(async move {
        fut.await;
        // 完成广播先于注册表摘除：等待方持 Rc 独立于条目，次序无碍
        task_state.done.notify_done();
        if cleanup_on_completion {
          registry_for_cleanup.borrow_mut().remove(&task_type);
        }
      });
      state.task.borrow_mut().replace(task);
      registry.insert(task_type, state);
    }
    true
  }

  /// 取消并移除指定类型的任务，等待其收敛
  ///
  /// 撤销本条目令牌后等待完成事件（而非接管句柄：句柄可能正被 WaitAsync
  /// 侧借出等待）；收敛后释放句柄（此时 Drop 不再触发强取消）
  ///
  /// libs/server/TaskManager/TaskManager.cs:CancelAsync
  pub async fn cancel_async(&self, task_type: TaskType) {
    let state = self.registry.borrow_mut().remove(&task_type);
    if let Some(state) = state {
      state.cts.clone().cancel();
      state.wait_done().await;
      drop(state.task.borrow_mut().take());
    }
  }

  /// 按放置类别批量取消任务（对应 CancelAsync(TaskPlacementCategory) 类别重载）
  pub async fn cancel_category_async(&self, task_placement_category: TaskPlacementCategory) {
    for task_type in TaskType::get_task_types(task_placement_category) {
      self.cancel_async(task_type).await;
    }
  }

  /// 等待指定类型的任务完成；未注册返回 false
  ///
  /// 任务 panic 经完成事件暴露为默认收敛（C# WaitAsync 会原样续抛任务
  /// 异常，此处 panic 已由运行时隔离为 JoinError，不跨边界续抛）
  ///
  /// libs/server/TaskManager/TaskManager.cs:WaitAsync
  pub async fn wait_async(&self, task_type: TaskType) -> bool {
    let Some(state) = self.registry.borrow().get(&task_type).cloned() else {
      return false;
    };
    state.wait_done().await;
    true
  }
}

impl Default for TaskManager {
  fn default() -> Self {
    Self::new()
  }
}

impl Drop for TaskManager {
  /// 销毁实例：撤销根令牌并逐条撤除条目令牌
  ///
  /// C# Dispose 以 BlockingWait 同步等待全部任务收敛；compio 线程局部运行时
  /// 无法在 Drop 内再入等待，收敛改为异步语义：等待方（wait_async/
  /// cancel_async）持 Rc 独立于注册表生命周期，令牌撤除后任务自行收敛；
  /// 条目随注册表消亡释放句柄，未收敛任务在下一 await 点被强取消——关闭
  /// 路径的既有任务不应再被依赖
  ///
  /// libs/server/TaskManager/TaskManager.cs:Dispose
  fn drop(&mut self) {
    self.cts.clone().cancel();
    for (_, state) in self.registry.borrow_mut().drain() {
      state.cts.clone().cancel();
    }
  }
}

#[cfg(test)]
mod tests {
  use std::{rc::Rc, time::Duration};

  use compio::{runtime::Runtime, time::sleep};
  use parking_lot::Mutex;

  use super::{TaskManager, TaskType};
  use crate::taskmanager::task_type::TaskPlacementCategory;

  /// 注册即运行：句柄在册、is_running 为真，收敛后 cleanup 摘除条目
  #[test]
  fn register_and_run_with_cleanup() {
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
      let ran = Rc::new(Mutex::new(false));
      let ran2 = Rc::clone(&ran);
      let tm = TaskManager::new();
      assert!(tm.register_and_run(
        TaskType::CommitTask,
        move |_| async move {
          *ran2.lock() = true;
        },
        true
      ));
      assert!(tm.is_registered(TaskType::CommitTask));
      assert!(tm.is_running(TaskType::CommitTask));

      // 重复注册同类型被拒
      assert!(!tm.register_and_run(TaskType::CommitTask, |_| async {}, false));

      tm.wait_async(TaskType::CommitTask).await;
      assert!(*ran.lock());
      // cleanup_on_completion → 收敛后条目摘除
      assert!(!tm.is_registered(TaskType::CommitTask));
      assert!(!tm.is_running(TaskType::CommitTask));
    });
  }

  /// 无 cleanup 的任务收敛后条目保留，is_running 翻假
  #[test]
  fn register_without_cleanup_keeps_entry() {
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
      let tm = TaskManager::new();
      tm.register_and_run(TaskType::CompactionTask, |_| async {}, false);
      tm.wait_async(TaskType::CompactionTask).await;
      assert!(tm.is_registered(TaskType::CompactionTask));
      assert!(!tm.is_running(TaskType::CompactionTask));
    });
  }

  /// cancel_async 撤令牌驱动循环任务收敛并移除条目
  #[test]
  fn cancel_async_stops_long_running_task() {
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
      let tm = TaskManager::new();
      let cancelled = Rc::new(Mutex::new(false));
      let cancelled2 = Rc::clone(&cancelled);
      tm.register_and_run(
        TaskType::ExpiredKeyDeletionTask,
        move |token| async move {
          // C# 协作取消映射：等待令牌撤销
          token.wait().await;
          *cancelled2.lock() = true;
        },
        false,
      );
      assert!(tm.is_running(TaskType::ExpiredKeyDeletionTask));
      tm.cancel_async(TaskType::ExpiredKeyDeletionTask).await;
      assert!(*cancelled.lock());
      assert!(!tm.is_registered(TaskType::ExpiredKeyDeletionTask));
    });
  }

  /// 按放置类别批量取消：仅命中 Primary 类别任务
  #[test]
  fn cancel_category_cancels_matching_placement_only() {
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
      let tm = TaskManager::new();
      tm.register_and_run(
        TaskType::AofSizeLimitTask,
        |t| async move { t.wait().await },
        false,
      );
      tm.register_and_run(
        TaskType::VectorReplicationReplayTask,
        |t| async move { t.wait().await },
        false,
      );
      tm.cancel_category_async(TaskPlacementCategory::PRIMARY)
        .await;
      assert!(!tm.is_registered(TaskType::AofSizeLimitTask));
      assert!(tm.is_registered(TaskType::VectorReplicationReplayTask));
    });
  }

  /// 周期任务经 sleep 分片时 cancel_async 亦能收敛（句柄不泄漏）
  #[test]
  fn cancel_async_during_sleep_resumes() {
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
      let tm = TaskManager::new();
      tm.register_and_run(
        TaskType::IndexAutoGrowTask,
        |t| async move {
          // C# 协作取消映射：令牌撤销后退出循环（否则 wait 立即就绪变热循环，任务永不收敛）
          while !t.is_cancelled() {
            t.clone().wait().await;
          }
        },
        false,
      );
      tm.cancel_async(TaskType::IndexAutoGrowTask).await;
      assert!(!tm.is_registered(TaskType::IndexAutoGrowTask));
      sleep(Duration::from_millis(1)).await;
    });
  }

  /// wait_async 未注册返回 false
  #[test]
  fn wait_async_unregistered_returns_false() {
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
      let tm = TaskManager::new();
      assert!(!tm.wait_async(TaskType::CommitTask).await);
    });
  }
}
