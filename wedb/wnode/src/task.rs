//! 后台维护任务类型与放置类别
//!
//! 对标 libs/server/TaskManager/TaskType.cs（TaskType + TaskTypeExtensions）与
//! libs/server/TaskManager/TaskPlacementCategory.cs（`[Flags]` 位枚举）

use bitflags::bitflags;

bitflags! {
  /// 任务放置类别约束
  ///
  /// libs/server/TaskManager/TaskPlacementCategory.cs:TaskPlacementCategory
  #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
  pub struct TaskPlacementCategory: u8 {
    /// 仅可安全运行于主节点
    const PRIMARY = 1 << 0;
    /// 仅可安全运行于副本节点
    const REPLICA = 1 << 1;
    /// 所有节点类型均可安全运行
    const ALL = Self::PRIMARY.bits() | Self::REPLICA.bits();
  }
}

/// 可由 TaskManager 托管的后台维护任务类型
///
/// libs/server/TaskManager/TaskType.cs:TaskType
#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum TaskType {
  /// 监控 AOF 大小，超限触发 checkpoint
  AofSizeLimitTask = 0,
  /// 周期性提交 AOF 保证持久性
  CommitTask,
  /// 日志紧缩回收已删除记录空间
  CompactionTask,
  /// 收集对象存储集合中的过期成员
  ObjectCollectTask,
  /// 扫描并删除主/对象存储中的过期键
  ExpiredKeyDeletionTask,
  /// 溢出阈值满足时自动扩容哈希索引
  IndexAutoGrowTask,
  /// 副本端并行重放 VADD
  VectorReplicationReplayTask,
}

/// 按枚举下标索引的"任务类型 → 放置类别"映射表（编译期定长，与 C#
/// Enum.GetValues 长度一致；新增变体时在 [`TaskType::ALL`] 与本表尾部同步补位）
///
/// libs/server/TaskManager/TaskType.cs:TaskTypeExtensions.TaskPlacementMapping
const TASK_PLACEMENT_MAPPING: [TaskPlacementCategory; TaskType::COUNT] = [
  TaskPlacementCategory::PRIMARY, // AofSizeLimitTask
  TaskPlacementCategory::PRIMARY, // CommitTask
  TaskPlacementCategory::PRIMARY, // CompactionTask
  TaskPlacementCategory::PRIMARY, // ObjectCollectTask
  TaskPlacementCategory::PRIMARY, // ExpiredKeyDeletionTask
  TaskPlacementCategory::ALL,     // IndexAutoGrowTask
  TaskPlacementCategory::REPLICA, // VectorReplicationReplayTask
];

impl TaskType {
  /// 枚举成员总数
  pub const COUNT: usize = 7;

  /// 全部任务类型（枚举声明序）
  pub const ALL: [TaskType; Self::COUNT] = [
    TaskType::AofSizeLimitTask,
    TaskType::CommitTask,
    TaskType::CompactionTask,
    TaskType::ObjectCollectTask,
    TaskType::ExpiredKeyDeletionTask,
    TaskType::IndexAutoGrowTask,
    TaskType::VectorReplicationReplayTask,
  ];

  /// 按下标取任务类型（表驱动，下标超出 COUNT 返回 None）
  #[inline]
  #[must_use]
  pub const fn from_index(i: usize) -> Option<Self> {
    if i < Self::COUNT {
      Some(Self::ALL[i])
    } else {
      None
    }
  }

  /// 该任务的放置类别
  #[inline]
  #[must_use]
  pub const fn placement(self) -> TaskPlacementCategory {
    TASK_PLACEMENT_MAPPING[self as usize]
  }

  /// 取出匹配放置类别的全部任务类型（保持枚举声明序，零分配）
  ///
  /// libs/server/TaskManager/TaskType.cs:GetTaskTypes
  pub fn get_task_types(
    lookup_placement_category: TaskPlacementCategory,
  ) -> impl Iterator<Item = Self> + Clone {
    Self::ALL.into_iter().filter(move |&task_type| {
      Self::match_placement_category(task_type.placement(), lookup_placement_category)
    })
  }

  /// 判定任务放置类别是否匹配查询类别
  ///
  /// "All" 任务可在任意节点运行；查询 "All" 时恒匹配；其余按位包含判定
  ///
  /// libs/server/TaskManager/TaskType.cs:MatchPlacementCategory
  #[inline]
  #[must_use]
  pub const fn match_placement_category(
    task_placement_category: TaskPlacementCategory,
    lookup_placement_category: TaskPlacementCategory,
  ) -> bool {
    // const 上下文禁用派生的 PartialEq，按位比较（bitflags 无别名位，语义等价）
    if task_placement_category.bits() == TaskPlacementCategory::ALL.bits()
      || lookup_placement_category.bits() == TaskPlacementCategory::ALL.bits()
    {
      return true;
    }
    lookup_placement_category.contains(task_placement_category)
  }
}

impl TryFrom<u8> for TaskType {
  type Error = u8;
  #[inline]
  fn try_from(v: u8) -> Result<Self, Self::Error> {
    if (v as usize) < Self::COUNT {
      Ok(Self::ALL[v as usize])
    } else {
      Err(v)
    }
  }
}

#[cfg(test)]
mod tests {
  use super::{TaskPlacementCategory as P, TaskType as T};

  #[test]
  fn placement_mapping_matches_csharp() {
    assert_eq!(T::AofSizeLimitTask.placement(), P::PRIMARY);
    assert_eq!(T::CommitTask.placement(), P::PRIMARY);
    assert_eq!(T::CompactionTask.placement(), P::PRIMARY);
    assert_eq!(T::ObjectCollectTask.placement(), P::PRIMARY);
    assert_eq!(T::ExpiredKeyDeletionTask.placement(), P::PRIMARY);
    assert_eq!(T::IndexAutoGrowTask.placement(), P::ALL);
    assert_eq!(T::VectorReplicationReplayTask.placement(), P::REPLICA);
  }

  #[test]
  fn match_placement_category_semantics() {
    // "All" 任务任意类别可运行；查询 "All" 恒匹配
    assert!(T::match_placement_category(P::ALL, P::PRIMARY));
    assert!(T::match_placement_category(P::PRIMARY, P::ALL));
    // 位包含判定
    assert!(T::match_placement_category(P::PRIMARY, P::PRIMARY));
    assert!(!T::match_placement_category(P::PRIMARY, P::REPLICA));
    assert!(!T::match_placement_category(P::REPLICA, P::PRIMARY));
  }

  #[test]
  fn get_task_types_filters_in_declaration_order() {
    let primary: Vec<T> = T::get_task_types(P::PRIMARY).collect();
    assert_eq!(
      primary,
      vec![
        T::AofSizeLimitTask,
        T::CommitTask,
        T::CompactionTask,
        T::ObjectCollectTask,
        T::ExpiredKeyDeletionTask,
        // C# MatchPlacementCategory(All, Primary) 恒真：All 任务命中任意查询
        T::IndexAutoGrowTask,
      ]
    );

    let replica: Vec<T> = T::get_task_types(P::REPLICA).collect();
    assert_eq!(
      replica,
      vec![T::IndexAutoGrowTask, T::VectorReplicationReplayTask]
    );

    // C# 查询 All 时：All 任务与位包含判定双双命中 → 全量按声明序
    let all: Vec<T> = T::get_task_types(P::ALL).collect();
    assert_eq!(all, T::ALL.to_vec());
  }
}

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

/// 完成事件广播原语（线程局部：Rc/RefCell 免锁免原子；对标 async Event 的一写多读形态）
///
/// 先置完成标志再唤醒，等待方 poll 内双检标志，通知先到不丢
struct DoneEvent {
  /// 完成标志（快检路径，避免无谓登记 waker）
  done: Cell<bool>,
  /// 登记中的等待者 waker（notify 时全部唤醒并清空）
  waiters: RefCell<Vec<Waker>>,
}

impl Default for DoneEvent {
  fn default() -> Self {
    Self::new()
  }
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

/// 后台任务管理器：注册、运行、取消与等待
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
  /// `cleanup_on_completion` 为真时，任务完成后自动从注册表移除条目
  ///
  /// 返回 false 表示该类型已在册
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
      let fut = task_factory(state.cts.clone());
      let task_state = Rc::clone(&state);
      let registry_for_cleanup = Rc::clone(&self.registry);
      let task = spawn(Self::wrap_with_cleanup_async(
        fut,
        task_state,
        registry_for_cleanup,
        task_type,
        cleanup_on_completion,
      ));
      state.task.borrow_mut().replace(task);
      registry.insert(task_type, state);
    }
    true
  }

  /// 包装任务执行并在完成后根据配置清理注册项
  ///
  /// libs/server/TaskManager/TaskManager.cs:WrapWithCleanupAsync
  async fn wrap_with_cleanup_async(
    fut: impl Future<Output = ()>,
    task_state: Rc<TaskState>,
    registry: Registry,
    task_type: TaskType,
    cleanup_on_completion: bool,
  ) {
    fut.await;
    task_state.done.notify_done();
    if cleanup_on_completion {
      registry.borrow_mut().remove(&task_type);
    }
  }

  /// 取消并移除指定类型的任务，等待其收敛
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
  ///
  /// 先并发向所有匹配任务广播取消信号，再逐一等待收敛，避免串行阻塞
  pub async fn cancel_category_async(&self, task_placement_category: TaskPlacementCategory) {
    let tasks: Vec<_> = TaskType::get_task_types(task_placement_category).collect();
    {
      let reg = self.registry.borrow();
      for &task_type in &tasks {
        if let Some(state) = reg.get(&task_type) {
          state.cts.clone().cancel();
        }
      }
    }
    for task_type in tasks {
      self.cancel_async(task_type).await;
    }
  }

  /// 等待指定类型的任务完成；未注册返回 false
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
  /// libs/server/TaskManager/TaskManager.cs:Dispose
  fn drop(&mut self) {
    self.cts.clone().cancel();
    for (_, state) in self.registry.borrow_mut().drain() {
      state.cts.clone().cancel();
    }
  }
}
