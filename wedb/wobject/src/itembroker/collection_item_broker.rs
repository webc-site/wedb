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
    atomic::{AtomicBool, AtomicI32, AtomicU64, Ordering},
  },
};

use compio::runtime::spawn;
use crossfire::{
  AsyncRx, MTx,
  mpsc::{List, unbounded_async},
  oneshot::{RxOneshot as OneshotAsyncRx, TxOneshot as OneshotTx, oneshot},
};
use parking_lot::Mutex;
use wbase::map::{ConcurrentMap as HashMap, new_concurrent_map};

/// 键观察者队列（按订阅顺序；对应 C# ConcurrentQueue<CollectionItemObserver>）
type ObserverQueue = Mutex<VecDeque<Arc<CollectionItemObserver>>>;
/// 观察键 → 观察者队列映射（gxhash 构建器，全项目并发容器唯一出处约束）
type KeysToObservers = HashMap<Vec<u8>, ObserverQueue>;
use wresp::RespCommand;

use crate::{
  itembroker::{
    collection_item_broker_event::{CollectionItemBrokerEvent, CollectionItemBrokerEventType},
    collection_item_observer::{CollectionItemObserver, CollectionItemResult, ObserverStatus},
  },
  list::list_object::{ListObject, OperationDirection},
  sortedset::sorted_set_object::SortedSetObject,
};

/// keysToObservers 两次清理的最小间隔
///
/// libs/server/Objects/ItemBroker/CollectionItemBroker.cs:MIN_SECS_BETWEEN_KEYS_TO_OBSERVERS_CLEANS
const MIN_SECS_BETWEEN_KEYS_TO_OBSERVERS_CLEANS: u64 = 5 * 60;

/// 主循环状态
const MAIN_LOOP_NOT_STARTED: i32 = 0;
const MAIN_LOOP_STARTED: i32 = 1;
const MAIN_LOOP_DISPOSED: i32 = 2;

/// 取件源试取结果（currCount 出参 + BLMOVE 目标键唤醒回传）
///
/// 对标 C# TryGetResult 的 `out currCount` 与 `out byte[] notifyKey`
///（BLMOVE 弹出后经事件队列唤醒目标键观察者，C# 在 finally 块入队）
#[derive(Debug, Default)]
pub struct TryGetOutcome {
  /// 试取前集合元素数（供"集合非空但观察者不匹配"的继续判定）
  pub curr_count: usize,
  /// 取件结果（None 表示本次不可取：键缺失 / 类型不符未失败 / 集合为空 /
  /// 磁盘候选降级）
  pub result: Option<CollectionItemResult>,
  /// 需在提交后唤醒的键（BLMOVE 目标键；C# notifyKey 经事件队列异步唤醒）
  pub notify_key: Option<Vec<u8>>,
}

impl TryGetOutcome {
  /// 不可取结果（curr_count 为 0、无通知键）
  pub fn none() -> Self {
    Self::default()
  }

  /// 带元素数的不可取结果
  pub fn with_count(curr_count: usize) -> Self {
    Self {
      curr_count,
      ..Self::none()
    }
  }

  /// 单结果构造（curr_count 随附）
  pub fn found(curr_count: usize, result: CollectionItemResult) -> Self {
    Self {
      curr_count,
      result: Some(result),
      notify_key: None,
    }
  }

  /// BLMOVE 目标键通知构造
  pub fn moved(curr_count: usize, result: CollectionItemResult, notify_key: Vec<u8>) -> Self {
    Self {
      curr_count,
      result: Some(result),
      notify_key: Some(notify_key),
    }
  }
}

/// 取件源抽象：从 key 处的集合对象取出下一可用项
///
/// 对标 C# TryGetResult 内联的 storageSession.GET + 事务取件路径；
/// result 为 None 表示源不可用（键缺失/类型不符未失败/集合为空/磁盘候选
/// 降级），curr_count 供"集合非空但观察者不匹配"的继续判定，
/// notify_key 为 BLMOVE 搬入键（提交后经事件队列唤醒其观察者）。
pub trait CollectionItemStore: Send + Sync {
  /// 试取下一可用项（语义见 trait 文档）
  fn try_get_result(
    &self,
    key: &[u8],
    command: RespCommand,
    cmd_args: &[Vec<u8>],
    fail_on_src_type_mismatch: bool,
  ) -> TryGetOutcome;
}

impl<T: CollectionItemStore + ?Sized> CollectionItemStore for Arc<T> {
  #[inline]
  fn try_get_result(
    &self,
    key: &[u8],
    command: RespCommand,
    cmd_args: &[Vec<u8>],
    fail_on_src_type_mismatch: bool,
  ) -> TryGetOutcome {
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
    spawn(fut).detach();
  }
}

/// 集合项经纪
///
/// libs/server/Objects/ItemBroker/CollectionItemBroker.cs:CollectionItemBroker
pub struct CollectionItemBroker<S, Spawner = CompioTaskSpawner> {
  /// 事件发送端（对应 Garnet AsyncQueue<CollectionItemBrokerEvent>，零锁并发投递）
  events_tx: MTx<List<CollectionItemBrokerEvent>>,
  /// 事件接收端（单主循环消费，启动时 take 独占）
  events_rx: Mutex<Option<AsyncRx<List<CollectionItemBrokerEvent>>>>,

  /// 会话 ID → 观察者
  session_id_to_observer: HashMap<usize, Arc<CollectionItemObserver>>,

  /// 观察键 → 观察者队列（按订阅顺序；并发无锁哈希表分片索引，对应 C# ConcurrentQueue）
  keys_to_observers: KeysToObservers,

  /// 上次清理时刻（Unix 秒数）
  keys_to_observers_time_last_clean: AtomicU64,

  /// 取件源
  store: S,

  /// 主循环状态
  main_loop_task_status: AtomicI32,

  /// 取消标记（C# CancellationTokenSource）
  cts_cancelled: AtomicBool,

  /// dispose 完成通知发送端（单次非阻塞投递，对标 C# ManualResetEventSlim / TaskCompletionSource）
  done_tx: Mutex<Option<OneshotTx<()>>>,
  /// dispose 完成通知接收端
  done_rx: Mutex<Option<OneshotAsyncRx<()>>>,

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
    let (events_tx, events_rx) = unbounded_async();
    let (done_tx, done_rx) = oneshot();
    Self {
      events_tx,
      events_rx: Mutex::new(Some(events_rx)),
      session_id_to_observer: new_concurrent_map(),
      keys_to_observers: new_concurrent_map(),
      keys_to_observers_time_last_clean: AtomicU64::new(
        coarsetime::Clock::now_since_epoch().as_secs(),
      ),
      store,
      main_loop_task_status: AtomicI32::new(MAIN_LOOP_NOT_STARTED),
      cts_cancelled: AtomicBool::new(false),
      done_tx: Mutex::new(Some(done_tx)),
      done_rx: Mutex::new(Some(done_rx)),
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
    let mut lock = self.events_rx.lock();
    lock.as_mut().and_then(|rx| rx.try_recv().ok())
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

  /// 登记观察者并启动等待（GetCollectionItemAsync 前半拆解：登记映射 →
  /// 启动主循环 → NewObserver 事件入队），等待由调用方驱动
  /// （compio 挂起语义见 BlockedWait，C# 由网络线程 BlockingWait 承担）
  pub fn start_wait(
    self: &Arc<Self>,
    command: RespCommand,
    keys: Vec<Vec<u8>>,
    session_id: usize,
    cmd_args: Vec<Vec<u8>>,
  ) -> Arc<CollectionItemObserver> {
    let observer = Arc::new(CollectionItemObserver::new(session_id, command, cmd_args));
    self.register_observer(observer.clone(), keys);

    // 首次使用时启动主循环
    self.start_main_loop();

    observer
  }

  /// 等待结束收尾（GetCollectionItemAsync 后半拆解）：摘除会话映射，
  /// 仍在等待则置空结果（超时/销毁路径），返回最终结果
  pub fn finish_wait(&self, observer: &Arc<CollectionItemObserver>) -> CollectionItemResult {
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
  /// 登记观察者 → 启动主循环 → 入队 NewObserver → 等待 → 收尾
  async fn get_collection_item_async_inner(
    self: &Arc<Self>,
    observer: Arc<CollectionItemObserver>,
    keys: Vec<Vec<u8>>,
  ) -> CollectionItemResult {
    self.register_observer(observer.clone(), keys);
    self.start_main_loop();

    // 等待结果就绪或会话销毁
    observer.wait_result().await;

    self.finish_wait(&observer)
  }

  /// 登记观察者：会话映射写入 + NewObserver 事件入队
  ///（start_wait 与 get_collection_item_async_inner 的共同前半）
  fn register_observer(&self, observer: Arc<CollectionItemObserver>, keys: Vec<Vec<u8>>) {
    self
      .session_id_to_observer
      .pin()
      .insert(observer.session_id, observer.clone());

    self.enqueue_event(CollectionItemBrokerEvent::create_new_observer_event(
      observer, keys,
    ));
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
    let pin = self.keys_to_observers.pin();
    let Some(queue) = pin.get(key) else {
      return;
    };
    if queue.lock().is_empty() {
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
    let pin = self.keys_to_observers.pin();

    // 与 C# 相同：键已在且队列非空 → 有其他观察者在等，跳过试取
    for key in keys {
      let has_waiting = pin.get(key).is_some_and(|q| !q.lock().is_empty());
      if has_waiting {
        continue;
      }

      let outcome = self.try_get_result(key, observer.command, &observer.command_args, true);
      let Some(result) = outcome.result else {
        continue;
      };

      // 取到项：置结果并回收该键的空队列
      self
        .session_id_to_observer
        .pin()
        .remove(&observer.session_id);
      observer.handle_set_result(result);

      if let Some(queue) = pin.get(key)
        && queue.lock().is_empty()
      {
        pin.remove(key);
      }

      // BLMOVE 搬入键提交后唤醒（C# TryGetResult finally 入队）
      if let Some(notify_key) = outcome.notify_key {
        self.enqueue_event(CollectionItemBrokerEvent::create_collection_updated_event(
          notify_key,
        ));
      }
      return;
    }

    // 未取到项：挂队到每个观察键
    for key in keys {
      let queue = match pin.get(key) {
        Some(q) => q,
        None => pin.get_or_insert_with(key.clone(), || Mutex::new(VecDeque::new())),
      };
      queue.lock().push_back(observer.clone());
    }
  }

  /// 把键处的可用项指派给等待观察者（持续满足直到无法满足队首）
  ///
  /// libs/server/Objects/ItemBroker/CollectionItemBroker.cs:TryAssignItemFromKey
  fn try_assign_item_from_key(&self, key: &[u8]) -> bool {
    let mut assigned_any = false;
    let pin = self.keys_to_observers.pin();
    if let Some(queue) = pin.get(key) {
      let mut queue = queue.lock();
      while let Some(observer) = queue.front() {
        if observer.status() != ObserverStatus::WaitingForResult {
          queue.pop_front();
          continue;
        }

        // 观察者状态互斥由其内部锁保证（C# ObserverStatusLock 等价）
        let outcome = self.try_get_result(key, observer.command, &observer.command_args, false);
        let Some(result) = outcome.result else {
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

        // BLMOVE 搬入键提交后唤醒（C# TryGetResult finally 入队；此处直入
        // 队列而非 HandleCollectionUpdate，因调用方持有 keysToObservers 读锁）
        if let Some(notify_key) = outcome.notify_key {
          self.enqueue_event(CollectionItemBrokerEvent::create_collection_updated_event(
            notify_key,
          ));
        }
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
  ) -> TryGetOutcome {
    // 类型不符即失败（BLMOVE 的 dst 校验在存储适配层完成）
    self
      .store
      .try_get_result(key, command, cmd_args, fail_on_src_type_mismatch)
  }

  /// 清理 keysToObservers：弹出已终结观察者并回收空键
  ///
  /// libs/server/Objects/ItemBroker/CollectionItemBroker.cs:CleanKeysToObservers
  pub fn clean_keys_to_observers(&self) {
    let pin = self.keys_to_observers.pin();
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
        empty_keys.push(key.as_slice());
      }
    }
    for key in empty_keys {
      if let Some(queue) = pin.get(key)
        && queue.lock().is_empty()
      {
        pin.remove(key);
      }
    }
    self.keys_to_observers_time_last_clean.store(
      coarsetime::Clock::now_since_epoch().as_secs(),
      Ordering::Relaxed,
    );
  }

  /// 主循环：消费事件队列，周期性清理观察表
  ///
  /// libs/server/Objects/ItemBroker/CollectionItemBroker.cs:StartAsync
  pub async fn start_async(&self) {
    let rx = {
      let mut lock = self.events_rx.lock();
      lock.take()
    };
    let Some(rx) = rx else {
      return;
    };

    while !self.cts_cancelled.load(Ordering::SeqCst) {
      let next_event = match rx.recv().await {
        Ok(event) => event,
        Err(_) => break,
      };

      self.handle_broker_event(next_event);

      // 观察表周期清理
      let now = coarsetime::Clock::now_since_epoch().as_secs();
      let last = self
        .keys_to_observers_time_last_clean
        .load(Ordering::Relaxed);
      if now.saturating_sub(last) >= MIN_SECS_BETWEEN_KEYS_TO_OBSERVERS_CLEANS {
        self.clean_keys_to_observers();
      }
    }

    if let Some(tx) = self.done_tx.lock().take() {
      tx.send(());
    }
  }

  /// 等待主循环退出（对标 C# done.Wait()）
  pub async fn wait_done(&self) {
    let rx = self.done_rx.lock().take();
    if let Some(rx) = rx {
      let _ = rx.await;
    }
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

    if prev == MAIN_LOOP_STARTED {
      let _ = self.events_tx.try_send(CollectionItemBrokerEvent {
        event_type: CollectionItemBrokerEventType::NotSet,
        key: None,
        keys: None,
        observer: None,
      });
    }
  }

  /// 检查经纪是否已释放
  #[inline]
  pub fn is_disposed(&self) -> bool {
    self.main_loop_task_status.load(Ordering::SeqCst) == MAIN_LOOP_DISPOSED
  }

  /// 事件入队并唤醒主循环（crossfire 无锁并发推入）
  fn enqueue_event(&self, event: CollectionItemBrokerEvent) {
    let _ = self.events_tx.try_send(event);
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
  if src_direction == OperationDirection::Unknown || dst_direction == OperationDirection::Unknown {
    return None;
  }

  let next_item = {
    let src = &mut src_list_obj.list;
    if src.is_empty() {
      return None;
    }
    match src_direction {
      OperationDirection::Right => src.pop_back(),
      OperationDirection::Left => src.pop_front(),
      OperationDirection::Unknown => unreachable!(),
    }
  }?;

  {
    let dst = &mut dst_list_obj.list;
    match dst_direction {
      OperationDirection::Right => dst.push_back(next_item.clone()),
      OperationDirection::Left => dst.push_front(next_item.clone()),
      OperationDirection::Unknown => unreachable!(),
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
      let Ok(pop_bytes) = cmd_args[1][..4].try_into() else {
        return None;
      };
      let pop_count = usize::try_from(i32::from_le_bytes(pop_bytes))
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

#[cfg(test)]
mod tests {
  use compio::runtime::{Runtime, spawn};

  use super::*;

  struct DummyStore;
  impl CollectionItemStore for DummyStore {
    fn try_get_result(
      &self,
      _key: &[u8],
      _command: RespCommand,
      _cmd_args: &[Vec<u8>],
      _fail_on_src_type_mismatch: bool,
    ) -> TryGetOutcome {
      TryGetOutcome::none()
    }
  }

  #[test]
  fn broker_done_flow() {
    let broker = Arc::new(CollectionItemBroker::new(DummyStore));
    let broker_clone = broker.clone();

    Runtime::new().unwrap().block_on(async {
      let handle = spawn(async move {
        broker_clone.start_async().await;
      });

      // 销毁经纪并退出主循环
      broker.dispose();
      assert!(broker.is_disposed());

      broker.wait_done().await;
      // 再次 wait 应立即返回（幂等）
      broker.wait_done().await;

      handle.await.unwrap();
    });
  }
}
