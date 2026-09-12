//! 集合项经纪（对标 libs/server/Objects/ItemBroker/CollectionItemBroker.cs）
//!
//! 阻塞命令（BLPOP/BRPOP/BLMOVE/BLMPOP/BZPOPMIN/BZPOPMAX/BZMPOP）的取件仲裁：
//! - 会话发起阻塞等待 → 注册观察者并入队 NewObserver 事件；
//! - 存储层集合更新 → 入队 CollectionUpdated 事件；
//! - 主循环消费事件：新观察者立即试取一次，未果则挂到键的观察者队列；
//!   集合更新时把可用项指派给队首观察者。
//!
//! 刻意差异（对照 C#）：
//! - C# 经 storageSession 事务直取对象；Rust 以 [`CollectionItemStore`] trait
//!   注入取件源（由存储会话域适配，见汇报接线项）；
//! - C# 的 Task.Run 以 compio 宿主注入的 [`TaskSpawner`] 等价实现；
//! - 等待超时的计时调度由会话层承担，观察者仅暴露可取消的完成通知。

use std::{
  collections::VecDeque,
  sync::{
    Arc,
    atomic::{AtomicBool, AtomicI32, Ordering},
  },
};

use parking_lot::{Mutex, RwLock};
use whasher::{GxPapayaMap as HashMap, new_papaya_map};

/// 键观察者队列（按订阅顺序；对应 C# ConcurrentQueue<CollectionItemObserver>）
type ObserverQueue = Mutex<VecDeque<Arc<CollectionItemObserver>>>;
/// 观察键 → 观察者队列映射（gxhash 构建器，全项目并发容器唯一出处约束）
type KeysToObservers = HashMap<Vec<u8>, ObserverQueue>;
use crate::{
  objects::{
    itembroker::{
      collection_item_broker_event::{CollectionItemBrokerEvent, CollectionItemBrokerEventType},
      collection_item_observer::{
        CollectionItemObserver, CollectionItemResult, ObserverStatus, Wakeup,
      },
    },
    list::list_object::{ListObject, OperationDirection},
    sortedset::sorted_set_object::SortedSetObject,
  },
  types::RespCommand,
};

/// keysToObservers 两次清理的最小间隔
///
/// libs/server/Objects/ItemBroker/CollectionItemBroker.cs:MIN_SECS_BETWEEN_KEYS_TO_OBSERVERS_CLEANS
const MIN_SECS_BETWEEN_KEYS_TO_OBSERVERS_CLEANS: u64 = 5 * 60;

/// 主循环状态
const MAIN_LOOP_NOT_STARTED: i32 = 0;
const MAIN_LOOP_STARTED: i32 = 1;
const MAIN_LOOP_DISPOSED: i32 = 2;

/// 取件源抽象：从 key 处的集合对象取出下一可用项
///
/// 对标 C# TryGetResult 内联的 storageSession.GET + 事务取件路径；
/// 返回 None 表示源不可用（键缺失/类型不符未失败/集合为空），
/// Some((元素数, 结果)) 中元素数用于"集合非空但观察者不匹配"的继续判定。
pub trait CollectionItemStore: Send + Sync {
  /// 返回 (当前集合元素数, 取件结果)；结果为 None 表示本次不可取
  /// （键缺失 / 类型不符未失败 / 集合为空），元素数供继续判定
  fn try_get_result(
    &self,
    key: &[u8],
    command: RespCommand,
    cmd_args: &[Vec<u8>],
    fail_on_src_type_mismatch: bool,
  ) -> (usize, Option<CollectionItemResult>);
}

impl<T: CollectionItemStore + ?Sized> CollectionItemStore for Arc<T> {
  #[inline]
  fn try_get_result(
    &self,
    key: &[u8],
    command: RespCommand,
    cmd_args: &[Vec<u8>],
    fail_on_src_type_mismatch: bool,
  ) -> (usize, Option<CollectionItemResult>) {
    (**self).try_get_result(key, command, cmd_args, fail_on_src_type_mismatch)
  }
}

/// 主循环启动器（C# Task.Run 的运行时注入点）
pub trait TaskSpawner: Send + Sync {
  fn spawn<F>(&self, fut: F)
  where
    F: Future<Output = ()> + Send + 'static;
}

impl<T: TaskSpawner> TaskSpawner for Arc<T> {
  fn spawn<F>(&self, fut: F)
  where
    F: Future<Output = ()> + Send + 'static,
  {
    (**self).spawn(fut);
  }
}

/// 默认 compio 任务启动器
#[derive(Default, Clone, Copy)]
pub struct CompioTaskSpawner;

impl TaskSpawner for CompioTaskSpawner {
  fn spawn<F>(&self, fut: F)
  where
    F: Future<Output = ()> + Send + 'static,
  {
    compio::runtime::spawn(fut).detach();
  }
}

/// 集合项经纪
///
/// libs/server/Objects/ItemBroker/CollectionItemBroker.cs:CollectionItemBroker
pub struct CollectionItemBroker<S, Spawner = CompioTaskSpawner> {
  /// 事件队列（AsyncQueue 等价：Mutex 双端队列 + 信号量唤醒）
  broker_events_queue: Mutex<VecDeque<CollectionItemBrokerEvent>>,
  events_notify: Wakeup,

  /// 会话 ID → 观察者
  session_id_to_observer: HashMap<usize, Arc<CollectionItemObserver>>,

  /// 观察键 → 观察者队列（按订阅顺序；惰性建表；队列自身并发安全，对应 C# ConcurrentQueue）
  keys_to_observers: RwLock<Option<KeysToObservers>>,

  /// 上次清理时刻（Inst ticks 计）
  keys_to_observers_time_last_clean: Mutex<coarsetime::Instant>,

  /// 取件源
  store: S,

  /// 主循环状态
  main_loop_task_status: AtomicI32,

  /// 取消标记（C# CancellationTokenSource）
  cts_cancelled: AtomicBool,

  /// dispose 完成通知（C# ManualResetEventSlim）
  done: Wakeup,

  /// 主循环启动器（宿主运行时注入；C# 内建 Task.Run）
  spawner: Mutex<Option<Arc<Spawner>>>,
}

impl<S: CollectionItemStore + 'static> CollectionItemBroker<S, CompioTaskSpawner> {
  /// 构造经纪（绑定取件源，默认使用 CompioTaskSpawner）
  pub fn new(store: S) -> Self {
    Self::new_with_spawner(store, CompioTaskSpawner)
  }
}

impl<S: CollectionItemStore + 'static, Spawner: TaskSpawner + 'static>
  CollectionItemBroker<S, Spawner>
{
  /// 构造经纪（指定启动器）
  pub fn new_with_spawner(store: S, spawner: Spawner) -> Self {
    Self {
      broker_events_queue: Mutex::new(VecDeque::new()),
      events_notify: Wakeup::new(),
      session_id_to_observer: new_papaya_map(),
      keys_to_observers: RwLock::new(None),
      keys_to_observers_time_last_clean: Mutex::new(coarsetime::Instant::now()),
      store,
      main_loop_task_status: AtomicI32::new(MAIN_LOOP_NOT_STARTED),
      cts_cancelled: AtomicBool::new(false),
      done: Wakeup::new(),
      spawner: Mutex::new(Some(Arc::new(spawner))),
    }
  }

  /// 注入主循环启动器（须在首个阻塞命令前完成）
  pub fn set_spawner(&self, spawner: Arc<Spawner>) {
    *self.spawner.lock() = Some(spawner);
  }

  /// 尝试获取会话对应的观察者
  ///
  /// libs/server/Objects/ItemBroker/CollectionItemBroker.cs:TryGetObserver
  pub fn try_get_observer(&self, session_id: usize) -> Option<Arc<CollectionItemObserver>> {
    self.session_id_to_observer.pin().get(&session_id).cloned()
  }

  /// 注册观察者到会话映射（供外部/集成测试直接注入）
  pub fn register_session_observer(&self, observer: Arc<CollectionItemObserver>) {
    self
      .session_id_to_observer
      .pin()
      .insert(observer.session_id, observer);
  }

  /// 弹出队首事件（供确定性步进测试消费）
  pub fn pop_broker_event(&self) -> Option<CollectionItemBrokerEvent> {
    self.broker_events_queue.lock().pop_front()
  }

  /// 异步等待集合对象出件（阻塞命令入口）
  ///
  /// libs/server/Objects/ItemBroker/CollectionItemBroker.cs:GetCollectionItemAsync(command, keys, session, timeout, cmdArgs)
  ///
  /// 刻意差异：`timeout_seconds` 仅作 API 对齐保留；计时等待由会话层以
  /// select 竞速 [`CollectionItemObserver::wait_result`] 实现
  pub async fn get_collection_item_async(
    self: &Arc<Self>,
    command: RespCommand,
    keys: Vec<Vec<u8>>,
    session_id: usize,
    _timeout_seconds: f64,
    cmd_args: Vec<Vec<u8>>,
  ) -> CollectionItemResult {
    let observer = Arc::new(CollectionItemObserver::new(session_id, command, cmd_args));
    self.get_collection_item_async_inner(observer, keys).await
  }

  /// 异步等待 srcKey 出件并移入 dstKey（BLMOVE 语义，见 C# MoveCollectionItemAsync）
  ///
  /// libs/server/Objects/ItemBroker/CollectionItemBroker.cs:MoveCollectionItemAsync
  pub async fn move_collection_item_async(
    self: &Arc<Self>,
    command: RespCommand,
    src_key: Vec<u8>,
    session_id: usize,
    timeout_seconds: f64,
    cmd_args: Vec<Vec<u8>>,
  ) -> CollectionItemResult {
    self
      .get_collection_item_async(
        command,
        vec![src_key],
        session_id,
        timeout_seconds,
        cmd_args,
      )
      .await
  }

  /// 内部公共路径（对应 GetCollectionItemAsync(observer, keys, timeout) 实现）：
  /// 登记观察者 → 启动主循环 → 入队 NewObserver → 等待
  async fn get_collection_item_async_inner(
    self: &Arc<Self>,
    observer: Arc<CollectionItemObserver>,
    keys: Vec<Vec<u8>>,
  ) -> CollectionItemResult {
    // 会话 ID → 观察者映射
    self
      .session_id_to_observer
      .pin()
      .insert(observer.session_id, observer.clone());

    // 首次使用时启动主循环
    self.start_main_loop();

    // NewObserver 事件入队
    self.enqueue_event(CollectionItemBrokerEvent::create_new_observer_event(
      observer.clone(),
      keys,
    ));

    // 等待结果就绪或会话销毁
    observer.wait_result().await;

    // 从会话映射摘除
    self
      .session_id_to_observer
      .pin()
      .remove(&observer.session_id);

    // 超时/销毁路径：仍处等待则置空结果
    if observer.status() == ObserverStatus::WaitingForResult {
      observer.handle_set_result(CollectionItemResult::empty());
    }

    observer.result()
  }

  /// 主循环启动（CAS 保证仅启动一次）；启动器经 [`Self::set_spawner`] 注入
  ///
  /// libs/server/Objects/ItemBroker/CollectionItemBroker.cs:StartMainLoop
  pub fn start_main_loop(self: &Arc<Self>) {
    if self.main_loop_task_status.load(Ordering::SeqCst) == MAIN_LOOP_NOT_STARTED
      && self
        .main_loop_task_status
        .compare_exchange(
          MAIN_LOOP_NOT_STARTED,
          MAIN_LOOP_STARTED,
          Ordering::SeqCst,
          Ordering::SeqCst,
        )
        .is_ok()
    {
      let spawner = self.spawner.lock().clone();
      match spawner {
        Some(spawner) => {
          let broker = Arc::downgrade(self);
          spawner.spawn(async move {
            if let Some(broker) = broker.upgrade() {
              broker.start_async().await;
            }
          });
        }
        None => {
          // 无启动器：回退状态，待注入后由后续调用再次启动
          self
            .main_loop_task_status
            .store(MAIN_LOOP_NOT_STARTED, Ordering::SeqCst);
        }
      }
    }
  }

  /// 集合更新通知（存储层写后调用）
  ///
  /// libs/server/Objects/ItemBroker/CollectionItemBroker.cs:HandleCollectionUpdate
  pub fn handle_collection_update(&self, key: &[u8]) {
    let map = self.keys_to_observers.read();
    let Some(m) = map.as_ref() else {
      return;
    };
    let pin = m.pin();
    let Some(queue) = pin.get(key) else {
      return;
    };
    if queue.lock().is_empty() {
      pin.remove(key);
      return;
    }

    // CollectionUpdated 事件入队：仅在确认有等待观察者时才分配 key.to_vec()
    self.enqueue_event(CollectionItemBrokerEvent::create_collection_updated_event(
      key.to_vec(),
    ));
  }

  /// 会话销毁通知
  ///
  /// libs/server/Objects/ItemBroker/CollectionItemBroker.cs:HandleSessionDisposed
  pub fn handle_session_disposed(&self, session_id: usize) {
    let removed = {
      let pin = self.session_id_to_observer.pin();
      pin.remove(&session_id).cloned()
    };
    let Some(observer) = removed else {
      return;
    };
    observer.handle_session_disposed();
  }

  /// 事件分派
  ///
  /// libs/server/Objects/ItemBroker/CollectionItemBroker.cs:HandleBrokerEvent
  pub fn handle_broker_event(&self, broker_event: CollectionItemBrokerEvent) {
    match broker_event.event_type {
      CollectionItemBrokerEventType::NewObserver => {
        if let (Some(observer), Some(keys)) = (broker_event.observer, broker_event.keys) {
          self.initialize_observer(observer, &keys);
        }
      }
      CollectionItemBrokerEventType::CollectionUpdated => {
        if let Some(key) = broker_event.key {
          self.try_assign_item_from_key(&key);
        }
      }
      CollectionItemBrokerEventType::NotSet => {}
    }
  }

  /// 新观察者登记：先试取（failOnSrcTypeMismatch=true），未果挂队
  ///
  /// libs/server/Objects/ItemBroker/CollectionItemBroker.cs:InitializeObserver
  pub fn initialize_observer(&self, observer: Arc<CollectionItemObserver>, keys: &[Vec<u8>]) {
    let mut map = self.keys_to_observers.write();

    // 与 C# 相同：键已在且队列非空 → 有其他观察者在等，跳过试取
    for key in keys {
      let has_waiting = match map.as_ref() {
        Some(m) => {
          let pin = m.pin();
          pin.get(key).is_some_and(|q| !q.lock().is_empty())
        }
        None => false,
      };
      if has_waiting {
        continue;
      }

      let (_, result) = self.try_get_result(key, observer.command, &observer.command_args, true);
      let Some(result) = result else {
        continue;
      };

      // 取到项：置结果并回收该键的空队列
      self
        .session_id_to_observer
        .pin()
        .remove(&observer.session_id);
      observer.handle_set_result(result);

      if let Some(m) = map.as_mut()
        && let Some(queue) = m.pin().get(key)
        && queue.lock().is_empty()
      {
        m.pin().remove(key);
      }
      return;
    }

    // 未取到项：挂队到每个观察键
    let m = map.get_or_insert_with(new_papaya_map);
    let pin = m.pin();
    for key in keys {
      match pin.get(key) {
        Some(queue) => queue.lock().push_back(observer.clone()),
        None => {
          pin.insert(key.clone(), Mutex::new(VecDeque::from([observer.clone()])));
        }
      }
    }
  }

  /// 把键处的可用项指派给等待观察者（持续满足直到无法满足队首）
  ///
  /// libs/server/Objects/ItemBroker/CollectionItemBroker.cs:TryAssignItemFromKey
  fn try_assign_item_from_key(&self, key: &[u8]) -> bool {
    let mut assigned_any = false;
    // 快路径读锁：字典结构不变，队列经自身锁并发修改（C# ConcurrentQueue 等价）
    let map = self.keys_to_observers.read();
    let Some(m) = map.as_ref() else {
      return false;
    };
    let pin = m.pin();
    if let Some(queue) = pin.get(key) {
      let mut queue = queue.lock();
      while let Some(observer) = queue.front() {
        if observer.status() != ObserverStatus::WaitingForResult {
          queue.pop_front();
          continue;
        }

        // 观察者状态互斥由其内部锁保证（C# ObserverStatusLock 等价）
        let (_, result) = self.try_get_result(key, observer.command, &observer.command_args, false);
        let Some(result) = result else {
          // 队首观察者无法满足时停止（FIFO 保证，不能跳过队首继续窥视）
          break;
        };

        let observer = queue.pop_front().unwrap();
        self
          .session_id_to_observer
          .pin()
          .remove(&observer.session_id);
        observer.handle_set_result(result);
        assigned_any = true;
      }

      if queue.is_empty() {
        drop(queue);
        pin.remove(key);
      }
    }
    assigned_any
  }

  /// 经取件源试取（currCount 出参随结果返回）
  ///
  /// libs/server/Objects/ItemBroker/CollectionItemBroker.cs:TryGetResult
  fn try_get_result(
    &self,
    key: &[u8],
    command: RespCommand,
    cmd_args: &[Vec<u8>],
    fail_on_src_type_mismatch: bool,
  ) -> (usize, Option<CollectionItemResult>) {
    // 类型不符即失败（BLMOVE 的 dst 校验在存储适配层完成）
    self
      .store
      .try_get_result(key, command, cmd_args, fail_on_src_type_mismatch)
  }

  /// 清理 keysToObservers：弹出已终结观察者并回收空键
  ///
  /// libs/server/Objects/ItemBroker/CollectionItemBroker.cs:CleanKeysToObservers
  pub fn clean_keys_to_observers(&self) {
    let map = self.keys_to_observers.read();
    if let Some(m) = map.as_ref() {
      let pin = m.pin();
      let mut empty_keys = Vec::new();
      for (key, queue) in pin.iter() {
        let mut queue = queue.lock();
        while let Some(observer) = queue.front() {
          if observer.status() != ObserverStatus::WaitingForResult {
            queue.pop_front();
          } else {
            break;
          }
        }
        if queue.is_empty() {
          empty_keys.push(key.clone());
        }
      }
      for key in empty_keys {
        pin.remove(&key);
      }
    }
    *self.keys_to_observers_time_last_clean.lock() = coarsetime::Instant::now();
  }

  /// 主循环：消费事件队列，周期性清理观察表
  ///
  /// libs/server/Objects/ItemBroker/CollectionItemBroker.cs:StartAsync
  pub async fn start_async(&self) {
    loop {
      if self.cts_cancelled.load(Ordering::SeqCst) {
        break;
      }

      let next_event = {
        let mut queue = self.broker_events_queue.lock();
        queue.pop_front()
      };

      let Some(next_event) = next_event else {
        // 队列空：挂起等待新事件（dispose 亦经此唤醒）
        self.events_notify.wait().await;
        continue;
      };

      self.handle_broker_event(next_event);

      // 观察表周期清理
      let elapsed = self.keys_to_observers_time_last_clean.lock().elapsed();
      if elapsed > coarsetime::Duration::from_secs(MIN_SECS_BETWEEN_KEYS_TO_OBSERVERS_CLEANS) {
        self.clean_keys_to_observers();
      }
    }

    self.done.notify_one();
  }

  /// 销毁：取消主循环并解除全部等待中的观察者
  ///
  /// libs/server/Objects/ItemBroker/CollectionItemBroker.cs:Dispose
  pub fn dispose(&self) {
    self.cts_cancelled.store(true, Ordering::SeqCst);

    self.session_id_to_observer.pin().iter().for_each(|(_, o)| {
      if o.status() == ObserverStatus::WaitingForResult {
        o.try_force_unblock(false);
      }
    });

    let prev = self
      .main_loop_task_status
      .swap(MAIN_LOOP_DISPOSED, Ordering::SeqCst);

    // 唤醒主循环感知取消（C# done.Wait() 的收尾等价：取消标记 + 唤醒，
    // 阻塞等待异步通知在 Rust 侧以运行时 join 承担，见汇报）
    if prev == MAIN_LOOP_STARTED {
      self.events_notify.notify_one();
    }
  }

  /// 事件入队并唤醒主循环
  fn enqueue_event(&self, event: CollectionItemBrokerEvent) {
    self.broker_events_queue.lock().push_back(event);
    self.events_notify.notify_one();
  }
}

/// 列表出件：按命令方向弹出
///
/// libs/server/Objects/ItemBroker/CollectionItemBroker.cs:TryGetNextListResult
pub fn try_get_next_list_item(list_obj: &mut ListObject, command: RespCommand) -> Option<Vec<u8>> {
  let list = &mut list_obj.list;
  if list.is_empty() {
    return None;
  }

  match command {
    RespCommand::Brpop => list.pop_back(),
    RespCommand::Blpop => list.pop_front(),
    _ => None,
  }
}

/// 列表搬移：src 弹出、dst 按方向推入
///
/// libs/server/Objects/ItemBroker/CollectionItemBroker.cs:TryMoveNextListItem
pub fn try_move_next_list_item(
  src_list_obj: &mut ListObject,
  dst_list_obj: &mut ListObject,
  src_direction: OperationDirection,
  dst_direction: OperationDirection,
) -> Option<Vec<u8>> {
  let next_item = {
    let src = &mut src_list_obj.list;
    if src.is_empty() {
      return None;
    }
    match src_direction {
      OperationDirection::Right => src.pop_back(),
      OperationDirection::Left => src.pop_front(),
      OperationDirection::Unknown => return None,
    }
  }?;

  {
    let dst = &mut dst_list_obj.list;
    match dst_direction {
      OperationDirection::Right => dst.push_back(next_item.clone()),
      OperationDirection::Left => dst.push_front(next_item.clone()),
      OperationDirection::Unknown => return None,
    }
  }

  Some(next_item)
}

/// 有序集合出件（BZPOPMIN/BZPOPMAX 共用；BZMPOP 按 cmd_args 弹出多项）
///
/// libs/server/Objects/ItemBroker/CollectionItemBroker.cs:TryGetNextSortedSetResult
pub fn try_get_next_sorted_set_item(
  key: &[u8],
  sorted_set_obj: &mut SortedSetObject,
  count: usize,
  command: RespCommand,
  cmd_args: &[Vec<u8>],
) -> Option<CollectionItemResult> {
  if count == 0 {
    return None;
  }

  match command {
    RespCommand::Bzpopmin | RespCommand::Bzpopmax => {
      let (score, element) = sorted_set_obj.pop_min_or_max(command == RespCommand::Bzpopmax)?;
      Some(CollectionItemResult::single_with_score(
        key.to_vec(),
        score,
        element,
      ))
    }
    RespCommand::Bzmpop => {
      // cmd_args: [lowScoresFirst(bool 1B), popCount(i32 LE 4B)]
      if cmd_args.len() < 2 || cmd_args[1].len() < 4 {
        return None;
      }
      let low_scores_first = cmd_args[0].first() != Some(&0);
      let pop_count = usize::try_from(i32::from_le_bytes(cmd_args[1][..4].try_into().unwrap()))
        .unwrap_or(0)
        .min(count);

      let mut scores = Vec::with_capacity(pop_count);
      let mut items = Vec::with_capacity(pop_count);
      for _ in 0..pop_count {
        let Some((score, element)) = sorted_set_obj.pop_min_or_max(!low_scores_first) else {
          break;
        };
        scores.push(score);
        items.push(element);
      }

      Some(CollectionItemResult::multiple_with_scores(
        key.to_vec(),
        scores,
        items,
      ))
    }
    _ => None,
  }
}
