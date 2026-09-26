//! 自研依据: 信封条目事件 broker/observer 分发（集合条目级事件面，C# 无对应组件）
use std::{
  collections::VecDeque,
  future::Future,
  mem::take,
  pin::Pin,
  slice::from_ref,
  sync::{
    Arc,
    atomic::{AtomicBool, AtomicUsize, Ordering},
    mpsc,
  },
  thread,
  time::Duration,
};

use compio::{
  runtime::{Runtime, spawn},
  time::{sleep, timeout},
};
use parking_lot::Mutex;
use wbase::{map::HashMap, ns_prefix::NsPrefix};
use wcol::{
  ObjectOutput,
  itembroker::{
    collection_item_broker::{
      CollectionItemBroker, CollectionItemStore, TaskSpawner, TryGetOutcome,
      try_get_next_list_item, try_move_next_list_item,
    },
    collection_item_broker_event::{CollectionItemBrokerEvent, CollectionItemBrokerEventType},
    collection_item_observer::{
      CollectionItemObserver as Obs, CollectionItemResult, ObserverStatus,
    },
  },
  list::list_object::{ListObject, ListOperation, OperationDirection},
};
use wresp::command::RespCommand;

/// 内存取件源：key → 项队列
///
/// 附带物理弹出计数与一次性「出件窗口」注入钩子（钩子在弹出动作前执行，
/// 用于确定性地让取消/销毁落在出件过程内）；另有读后一次性钩子（试取读取
/// store 完成后、结果返回前执行，用于把写提交与通知精确钉进登记侧
/// 「试取读取→挂队」的窗口）
struct MemStore {
  items: Mutex<HashMap<Vec<u8>, VecDeque<Vec<u8>>>>,
  popped: AtomicUsize,
  inject: Mutex<Option<Arc<dyn Fn() + Send + Sync>>>,
  post_inject: Mutex<Option<Arc<dyn Fn() + Send + Sync>>>,
  /// 同步不可出件开关（键 park 后升阶分层的测试注入：真值时试取恒报
  /// is_degrade，对位生产侧活跃分层键的 ObjLoad::Degrade 装载臂）
  degrade: AtomicBool,
}

impl MemStore {
  fn new() -> Self {
    Self {
      items: Mutex::new(HashMap::default()),
      popped: AtomicUsize::new(0),
      inject: Mutex::new(None),
      post_inject: Mutex::new(None),
      degrade: AtomicBool::new(false),
    }
  }

  /// 翻转同步不可出件开关
  fn set_degrade(&self, on: bool) {
    self.degrade.store(on, Ordering::Relaxed);
  }

  fn push(&self, ns: u64, db: u64, key: &[u8], item: &[u8]) {
    let folded = NsPrefix::new(ns).join(db).isolate(key);
    self
      .items
      .lock()
      .entry(folded)
      .or_default()
      .push_back(item.to_vec());
  }

  /// 武装一次性注入：下一次试取进入时先执行回调
  fn arm_injection<F: Fn() + Send + Sync + 'static>(&self, f: F) {
    *self.inject.lock() = Some(Arc::new(f));
  }

  /// 武装一次性读后注入：下一次试取完成 store 读取后执行回调
  fn arm_post_read_injection<F: Fn() + Send + Sync + 'static>(&self, f: F) {
    *self.post_inject.lock() = Some(Arc::new(f));
  }

  /// 存储层物理弹出量
  fn popped_count(&self) -> usize {
    self.popped.load(Ordering::Relaxed)
  }

  /// 集合中剩余元素量
  fn remaining_count(&self) -> usize {
    self.items.lock().values().map(|q| q.len()).sum()
  }

  /// 出件主体：判型/弹出（读后钩子的包裹对象）
  fn serve(&self, ns: u64, db: u64, key: &[u8], command: RespCommand) -> TryGetOutcome {
    // 键已离开同步服务域（分层）：装载恒 Degrade，同步臂不可出件
    if self.degrade.load(Ordering::Relaxed) {
      return TryGetOutcome::degrade();
    }

    let mut map = self.items.lock();
    let folded = NsPrefix::new(ns).join(db).isolate(key);
    let Some(queue) = map.get_mut(&folded) else {
      return TryGetOutcome::none();
    };
    let count = queue.len();
    match command {
      RespCommand::Blpop | RespCommand::Brpop => {
        let item = if command == RespCommand::Blpop {
          queue.pop_front()
        } else {
          queue.pop_back()
        };
        match item {
          Some(item) => {
            self.popped.fetch_add(1, Ordering::Relaxed);
            TryGetOutcome::found(count - 1, CollectionItemResult::single(key.to_vec(), item))
          }
          None => TryGetOutcome::with_count(count),
        }
      }
      _ => TryGetOutcome::with_count(count),
    }
  }
}

impl CollectionItemStore for MemStore {
  fn try_get_result(
    &self,
    ns: u64,
    db: u64,
    key: &[u8],
    command: RespCommand,
    _cmd_args: &[Vec<u8>],
    _move_item: bool,
  ) -> TryGetOutcome {
    // 注入回调在持锁外执行，仅一次性生效
    let hook = self.inject.lock().take();
    if let Some(hook) = hook {
      hook();
    }

    let outcome = self.serve(ns, db, key, command);

    let post = self.post_inject.lock().take();
    if let Some(hook) = post {
      hook();
    }
    outcome
  }
}

#[test]
fn broker_assigns_item_to_waiting_observer() {
  let store = Arc::new(MemStore::new());
  let broker = Arc::new(CollectionItemBroker::new(store.clone()));
  let observer = Arc::new(Obs::new(1, RespCommand::Blpop, vec![], (0, 0)));

  // 预置键数据后登记观察者：InitializeObserver 同步试取即得
  //（注册键为域折叠键形态，与生产入口 start_wait 折叠产物一致）
  store.push(0, 0, b"k", b"item-1");
  let folded = NsPrefix::new(0).join(0).isolate(b"k");
  broker.initialize_observer(observer.clone(), &[folded]);

  assert_eq!(observer.status(), ObserverStatus::ResultSet);
  let result = observer.result();
  assert_eq!(result.item.as_deref(), Some(b"item-1".as_slice()));
  // 观察者已从会话映射摘除
  assert!(broker.try_get_observer(1).is_none());
}

/// 轮询等待观察者进入指定状态（事件由主循环异步消费，给确定性行断言让渡）
async fn wait_for_status(observer: &Obs, status: ObserverStatus) {
  for _ in 0..10_000 {
    if observer.status() == status {
      return;
    }
    sleep(Duration::from_millis(1)).await;
  }
  panic!(
    "wait_for_status timeout, current status: {:?}",
    observer.status()
  );
}

#[compio::test]
async fn broker_queues_observer_until_update() -> aok::Result<()> {
  let store = Arc::new(MemStore::new());
  let broker = Arc::new(CollectionItemBroker::new(store.clone()));

  // 键无数据：经生产入口 start_wait 完成会话映射登记并挂队等待
  let observer = broker.start_wait(RespCommand::Blpop, vec![b"k".to_vec()], 2, vec![], (0, 0));
  wait_for_status(&observer, ObserverStatus::WaitingForResult).await;
  assert!(broker.try_get_observer(2).is_some());

  // 数据到达 → CollectionUpdated 事件由主循环消费并指派
  store.push(0, 0, b"k", b"item-2");
  broker.handle_collection_update((0, 0), b"k");
  wait_for_status(&observer, ObserverStatus::ResultSet).await;
  assert_eq!(
    observer.result().item.as_deref(),
    Some(b"item-2".as_slice())
  );
  Ok(())
}

#[compio::test]
async fn handle_session_disposed_removes_observer() -> aok::Result<()> {
  let store = Arc::new(MemStore::new());
  let broker = Arc::new(CollectionItemBroker::new(store));
  let observer = broker.start_wait(RespCommand::Blpop, vec![b"k".to_vec()], 9, vec![], (0, 0));
  wait_for_status(&observer, ObserverStatus::WaitingForResult).await;
  assert!(broker.try_get_observer(9).is_some());

  broker.handle_session_disposed(9);
  assert!(broker.try_get_observer(9).is_none());
  assert_eq!(observer.status(), ObserverStatus::SessionDisposed);
  Ok(())
}

#[compio::test]
async fn clean_removes_finished_observers_and_empty_keys() -> aok::Result<()> {
  let store = Arc::new(MemStore::new());
  let broker = Arc::new(CollectionItemBroker::new(store.clone()));

  // 先到的观察者经数据到达被指派终结
  let done = broker.start_wait(RespCommand::Blpop, vec![b"k".to_vec()], 3, vec![], (0, 0));
  wait_for_status(&done, ObserverStatus::WaitingForResult).await;
  store.push(0, 0, b"k", b"item-1");
  broker.handle_collection_update((0, 0), b"k");
  wait_for_status(&done, ObserverStatus::ResultSet).await;

  // 后到的观察者同键挂队等待
  let waiting = broker.start_wait(RespCommand::Blpop, vec![b"k".to_vec()], 4, vec![], (0, 0));
  wait_for_status(&waiting, ObserverStatus::WaitingForResult).await;

  broker.clean_keys_to_observers();
  // 已终结观察者被弹出，等待者仍在
  assert_eq!(waiting.status(), ObserverStatus::WaitingForResult);
  Ok(())
}

/// 周期清理全量收敛：队首为活跃等待者时，队中/队尾的失效观察者一并剔除
///
/// C# CleanKeysToObservers 受 ConcurrentQueue 容器接口所限只能队首窥探，遇
/// 活跃队首即止；rust 容器 Mutex<VecDeque> 持排他锁且具备随机访问能力，
/// retain 单次 O(N) 收敛。旧队首窥探实现下死节点被阻隔滞留，队列恒为 3、
/// 失效观察者 Arc 仍被队列持有（strong_count 2），本用例判红
#[test]
fn clean_converges_dead_observers_behind_active_head() {
  let store = Arc::new(MemStore::new());
  let broker = Arc::new(CollectionItemBroker::new(store));
  let folded = NsPrefix::new(0).join(0).isolate(b"clean_conv_k");

  // 空集合同步登记三个观察者（initialize_observer 直挂队列，无主循环时序）：
  // 队首为长等待活跃节点（等价 timeout=0 阻塞客户端）
  let head = Arc::new(Obs::new(201, RespCommand::Blpop, vec![], (0, 0)));
  broker.initialize_observer(head.clone(), from_ref(&folded));
  let expired = Arc::new(Obs::new(202, RespCommand::Blpop, vec![], (0, 0)));
  broker.initialize_observer(expired.clone(), from_ref(&folded));
  let disconnected = Arc::new(Obs::new(203, RespCommand::Blpop, vec![], (0, 0)));
  broker.initialize_observer(disconnected.clone(), from_ref(&folded));

  // 队中：超时到期后置 ResultSet 且携带他键满足的结果载荷（泄漏主体）；
  // 队尾：断连置 SessionDisposed
  expired.handle_set_result(CollectionItemResult::single(
    b"other_k".to_vec(),
    vec![b'x'; 128],
  ));
  disconnected.handle_session_disposed();
  assert_eq!(broker.waiting_observer_count(&folded), Some(3));

  broker.clean_keys_to_observers();

  // 队列收敛为 1 且仅留原队首活跃观察者（head 仍被队列持有一份引用，
  // 失效者全部出队）
  assert_eq!(broker.waiting_observer_count(&folded), Some(1));
  assert_eq!(Arc::strong_count(&head), 2, "队首活跃观察者应保留在队列中");
  assert_eq!(
    Arc::strong_count(&expired),
    1,
    "ResultSet 失效观察者被队首窥探阻隔滞留"
  );
  assert_eq!(
    Arc::strong_count(&disconnected),
    1,
    "SessionDisposed 失效观察者被队首窥探阻隔滞留"
  );

  // 幸存者语义：head 未被误伤、仍可被正常出件
  assert_eq!(head.status(), ObserverStatus::WaitingForResult);
}

/// 空队列回收闭环：全队列失效时键从 keys_to_observers 映射表摘除
#[test]
fn clean_detaches_key_when_all_observers_dead() {
  let store = Arc::new(MemStore::new());
  let broker = Arc::new(CollectionItemBroker::new(store));
  let folded = NsPrefix::new(0).join(0).isolate(b"clean_drain_k");

  let first = Arc::new(Obs::new(211, RespCommand::Blpop, vec![], (0, 0)));
  broker.initialize_observer(first.clone(), from_ref(&folded));
  let second = Arc::new(Obs::new(212, RespCommand::Blpop, vec![], (0, 0)));
  broker.initialize_observer(second.clone(), from_ref(&folded));
  assert_eq!(broker.waiting_observer_count(&folded), Some(2));

  first.handle_session_disposed();
  second.handle_set_result(CollectionItemResult::empty());

  broker.clean_keys_to_observers();

  // 队列清空后键随既有回收逻辑从映射表摘除，观察者引用全部释放
  assert_eq!(broker.waiting_observer_count(&folded), None);
  assert_eq!(Arc::strong_count(&first), 1);
  assert_eq!(Arc::strong_count(&second), 1);
}

#[test]
fn list_item_helpers() {
  let mut src = ListObject::new();
  let mut dst = ListObject::new();
  src.operate_basic(ListOperation::Rpush, b"l");
  src.operate_basic(ListOperation::Rpush, b"r");

  // BLPOP 方向弹出队首
  assert_eq!(
    try_get_next_list_item(&mut src, RespCommand::Blpop),
    Some(b"l".to_vec())
  );
  // BRPOP 弹出队尾
  assert_eq!(
    try_get_next_list_item(&mut src, RespCommand::Brpop),
    Some(b"r".to_vec())
  );
  assert_eq!(try_get_next_list_item(&mut src, RespCommand::Blpop), None);

  // 搬移：src 右弹 → dst 左推
  src.operate_basic(ListOperation::Rpush, b"a");
  src.operate_basic(ListOperation::Rpush, b"b");
  let moved = try_move_next_list_item(
    &mut src,
    &mut dst,
    OperationDirection::Right,
    OperationDirection::Left,
  );
  assert_eq!(moved, Some(b"b".to_vec()));
  // 断言走 operate 面（LINDEX → list_index 单点实现）
  let mut sink = Vec::new();
  let mut output = ObjectOutput::mount(&mut sink);
  assert!(dst.operate(ListOperation::Lindex as u8, &[], 0, 0, &mut output, 2));
  assert_eq!(output.result1, 1);
  assert_eq!(output.payload_view(), b"$1\r\nb\r\n");
}

/// 确定性单线程驱动主循环：验证 阻塞等待 → 挂队 → 集合更新 → 指派 → 解除 全链路
#[compio::test]
async fn main_loop_wakes_waiting_observer() -> aok::Result<()> {
  let store = Arc::new(MemStore::new());
  let broker = Arc::new(CollectionItemBroker::new(store.clone()));

  let broker_clone = Arc::clone(&broker);
  let store_clone = Arc::clone(&store);
  spawn(async move {
    for _ in 0..10_000 {
      if broker_clone.try_get_observer(6).is_some() {
        store_clone.push(0, 0, b"k", b"late-item");
        broker_clone.handle_collection_update((0, 0), b"k");
        break;
      }
      sleep(Duration::from_millis(1)).await;
    }
  })
  .detach();

  // 生产出口三段式：start_wait 登记挂队 → wait_result 等待 → finish_wait 收尾
  // （会话 BlockedWait 同构形态）
  let observer = broker.start_wait(RespCommand::Blpop, vec![b"k".to_vec()], 6, vec![], (0, 0));
  observer.wait_result().await;
  let result = broker.finish_wait(&observer);
  assert_eq!(result.item.as_deref(), Some(b"late-item".as_slice()));
  assert!(broker.try_get_observer(6).is_none());
  Ok(())
}

#[compio::test]
async fn collection_update_wakes_multiple_waiting_observers() -> aok::Result<()> {
  let store = Arc::new(MemStore::new());
  let broker = Arc::new(CollectionItemBroker::new(store.clone()));

  // 三个观察者经生产入口 start_wait 同键挂队
  let obs1 = broker.start_wait(
    RespCommand::Blpop,
    vec![b"batch_k".to_vec()],
    101,
    vec![],
    (0, 0),
  );
  let obs2 = broker.start_wait(
    RespCommand::Blpop,
    vec![b"batch_k".to_vec()],
    102,
    vec![],
    (0, 0),
  );
  let obs3 = broker.start_wait(
    RespCommand::Blpop,
    vec![b"batch_k".to_vec()],
    103,
    vec![],
    (0, 0),
  );
  wait_for_status(&obs3, ObserverStatus::WaitingForResult).await;

  // 集合推入 2 个元素，单次触发 update：主循环连续满足队首 obs1、obs2，obs3 保持等待
  store.push(0, 0, b"batch_k", b"item-a");
  store.push(0, 0, b"batch_k", b"item-b");
  broker.handle_collection_update((0, 0), b"batch_k");

  wait_for_status(&obs2, ObserverStatus::ResultSet).await;
  assert_eq!(obs1.status(), ObserverStatus::ResultSet);
  assert_eq!(obs1.result().item.as_deref(), Some(b"item-a".as_slice()));
  assert_eq!(obs2.result().item.as_deref(), Some(b"item-b".as_slice()));
  assert_eq!(obs3.status(), ObserverStatus::WaitingForResult);
  assert!(broker.try_get_observer(101).is_none());
  assert!(broker.try_get_observer(102).is_none());
  assert!(broker.try_get_observer(103).is_some());
  Ok(())
}

/// 跨域隔离：同名键观察者按域折叠键挂队，跨域更新不唤醒、不越域取件
#[compio::test]
async fn domains_do_not_cross_assign() -> aok::Result<()> {
  let store = Arc::new(MemStore::new());
  let broker = Arc::new(CollectionItemBroker::new(store.clone()));

  // ns1 / ns2 同名键各自挂队（生产入口 start_wait 折叠为 "1:0:k" / "2:0:k"）
  let obs_ns1 = broker.start_wait(RespCommand::Blpop, vec![b"k".to_vec()], 11, vec![], (1, 0));
  let obs_ns2 = broker.start_wait(RespCommand::Blpop, vec![b"k".to_vec()], 12, vec![], (2, 0));
  wait_for_status(&obs_ns1, ObserverStatus::WaitingForResult).await;
  wait_for_status(&obs_ns2, ObserverStatus::WaitingForResult).await;

  // 等待后台任务处理 NewObserver 事件，确保两个观察者都已挂队
  sleep(Duration::from_millis(100)).await;

  // ns2 域推入并唤醒：仅 ns2 观察者被指派，ns1 观察者不受扰
  store.push(2, 0, b"k", b"item-x");
  broker.handle_collection_update((2, 0), b"k");
  wait_for_status(&obs_ns2, ObserverStatus::ResultSet).await;
  assert_eq!(obs_ns2.result().key.as_deref(), Some(b"k".as_slice()));
  assert_eq!(obs_ns2.result().item.as_deref(), Some(b"item-x".as_slice()));
  assert_eq!(obs_ns1.status(), ObserverStatus::WaitingForResult);

  // ns1 域推入并唤醒：ns1 观察者取本域数据
  store.push(1, 0, b"k", b"item-y");
  broker.handle_collection_update((1, 0), b"k");
  wait_for_status(&obs_ns1, ObserverStatus::ResultSet).await;
  assert_eq!(obs_ns1.result().item.as_deref(), Some(b"item-y".as_slice()));

  // 跨域更新对本域观察表键无命中，误发为无害空操作
  broker.handle_collection_update((3, 0), b"k");
  assert!(broker.try_get_observer(11).is_none());
  assert!(broker.try_get_observer(12).is_none());
  Ok(())
}

/// 出件窗口注入的判别用例：客户端超时恰好落在「校验状态 → 物理弹出」之间
///
/// 注入形态：出件弹出前先放行取消线程（旧分离写法此刻不持任何观察者锁，
/// 超时得以插入），再驻留 200ms 保证超时必定落地。
/// - 修复态：出件在观察者状态锁临界区内，超时线程被互斥挡在门外，指派完毕
///   释锁后超时线程见状态已 ResultSet，保留弹出的有效数据（C#
///   CollectionItemBroker.cs:GetCollectionItemAsync 与写锁段的对齐语义）；
/// - 旧分离写法：超时线程抢先置空，随后弹出的元素被 handle_set_result 静默
///   drop —— 存储层弹出 1 个、客户端收到空，判红。
#[test]
fn timeout_landing_in_pop_window_keeps_popped_item() {
  let store = Arc::new(MemStore::new());
  let broker = Arc::new(CollectionItemBroker::new(store.clone()));
  let folded = NsPrefix::new(0).join(0).isolate(b"race-k");

  // 空集合登记 → 观察者挂为队首等待
  let observer = Arc::new(Obs::new(50, RespCommand::Blpop, vec![], (0, 0)));
  broker.initialize_observer(observer.clone(), from_ref(&folded));
  assert_eq!(observer.status(), ObserverStatus::WaitingForResult);

  store.push(0, 0, b"race-k", b"hot-item");

  // 取消线程：收到入场信号即执行会话侧超时收尾
  let (enter_tx, enter_rx) = mpsc::channel();
  store.arm_injection(move || {
    let _ = enter_tx.send(());
    thread::sleep(Duration::from_millis(200));
  });
  let cancel_broker = broker.clone();
  let cancel_observer = observer.clone();
  let cancel = thread::spawn(move || cancel_broker.finish_wait(&cancel_observer));

  // 主线程同步驱动 CollectionUpdated 指派（驻留在注入窗口内给超时让路）
  broker.handle_broker_event(CollectionItemBrokerEvent::create_collection_updated_event(
    folded,
  ));
  assert!(enter_rx.recv_timeout(Duration::from_secs(5)).is_ok());
  let timed_out = cancel.join().unwrap();

  // 元素弹出一次且落袋于观察者，超时侧应答仍携有效数据
  assert_eq!(store.popped_count(), 1);
  assert_eq!(store.remaining_count(), 0);
  assert!(observer.result().found(), "出件结果被静默丢弃");
  assert_eq!(timed_out.item.as_deref(), Some(b"hot-item".as_slice()));
}

/// 出件窗口注入的判别用例：会话销毁落在「校验状态 → 物理弹出」之间
///
/// 对位 C# HandleSessionDisposed 与出件写锁段互斥：销毁晚于指派时结果已落袋
/// （旧分离写法下弹出项被 drop，状态 SessionDisposed 且 result 为空）
#[test]
fn session_dispose_landing_in_pop_window_keeps_result() {
  let store = Arc::new(MemStore::new());
  let broker = Arc::new(CollectionItemBroker::new(store.clone()));
  let folded = NsPrefix::new(0).join(0).isolate(b"dispose-k");

  let observer = Arc::new(Obs::new(60, RespCommand::Brpop, vec![], (0, 0)));
  broker.initialize_observer(observer.clone(), from_ref(&folded));

  store.push(0, 0, b"dispose-k", b"doomed-item");

  let (enter_tx, enter_rx) = mpsc::channel();
  store.arm_injection(move || {
    let _ = enter_tx.send(());
    thread::sleep(Duration::from_millis(200));
  });
  let dispose_observer = observer.clone();
  let disposer = thread::spawn(move || dispose_observer.handle_session_disposed());

  broker.handle_broker_event(CollectionItemBrokerEvent::create_collection_updated_event(
    folded,
  ));
  assert!(enter_rx.recv_timeout(Duration::from_secs(5)).is_ok());
  disposer.join().unwrap();

  assert_eq!(store.popped_count(), 1);
  assert_eq!(store.remaining_count(), 0);
  assert_eq!(observer.status(), ObserverStatus::SessionDisposed);
  assert!(observer.result().found(), "销毁竞态丢件");
  assert_eq!(
    observer.result().item.as_deref(),
    Some(b"doomed-item".as_slice())
  );
}

/// 宿主注入：经纪主循环落到专用 compio 线程（客户端各自持运行时，真实跨线程竞争）
struct ThreadSpawner;

impl TaskSpawner for ThreadSpawner {
  fn spawn<F>(&self, fut: F)
  where
    F: Future<Output = ()> + Send + 'static,
  {
    thread::spawn(move || {
      if let Ok(runtime) = Runtime::new() {
        runtime.block_on(fut);
      }
    });
  }
}

/// 竞态回归：多线程极短超时 BLPOP/BRPOP 与多线程并发推入正面对撞，
/// 断言存储层弹出量与客户端非空接收量严格守恒（零丢失）
#[test]
fn concurrent_short_timeout_pops_lose_no_item() -> aok::Result<()> {
  const CLIENTS: usize = 6;
  const ROUNDS: usize = 40;
  const PUSHERS: usize = 3;

  let store = Arc::new(MemStore::new());
  let broker = Arc::new(CollectionItemBroker::new_with_spawner(
    store.clone(),
    ThreadSpawner,
  ));
  broker.start_main_loop();

  let pushed = Arc::new(AtomicUsize::new(0));
  let received = Arc::new(AtomicUsize::new(0));
  let next_session = Arc::new(AtomicUsize::new(1000));
  let stop = Arc::new(AtomicBool::new(false));

  // 推入洪峰：写侧存储弹出通知
  let pushers: Vec<_> = (0..PUSHERS)
    .map(|tag| {
      let (store, broker, pushed, stop) =
        (store.clone(), broker.clone(), pushed.clone(), stop.clone());
      thread::spawn(move || {
        let mut n = 0usize;
        while !stop.load(Ordering::Relaxed) {
          store.push(0, 0, b"race_stream", format!("p{tag}-{n}").as_bytes());
          pushed.fetch_add(1, Ordering::Relaxed);
          broker.handle_collection_update((0, 0), b"race_stream");
          n += 1;
          thread::sleep(Duration::from_micros(200));
        }
      })
    })
    .collect();

  // 客户端：生产三段式（start_wait → 极短超时竞速 → finish_wait 收尾）
  let clients: Vec<_> = (0..CLIENTS)
    .map(|tag| {
      let (broker, received, next_session) =
        (broker.clone(), received.clone(), next_session.clone());
      thread::spawn(move || -> aok::Result<()> {
        let runtime = Runtime::new()?;
        runtime.block_on(async {
          for _ in 0..ROUNDS {
            let command = if tag % 2 == 0 {
              RespCommand::Blpop
            } else {
              RespCommand::Brpop
            };
            let session_id = next_session.fetch_add(1, Ordering::Relaxed);
            let observer = broker.start_wait(
              command,
              vec![b"race_stream".to_vec()],
              session_id,
              vec![],
              (0, 0),
            );
            let _ = timeout(Duration::from_micros(500), observer.wait_result()).await;
            if broker.finish_wait(&observer).found() {
              received.fetch_add(1, Ordering::Relaxed);
            }
          }
        });
        Ok(())
      })
    })
    .collect();

  for client in clients {
    client.join().unwrap()?;
  }
  stop.store(true, Ordering::Relaxed);
  for pusher in pushers {
    pusher.join().unwrap();
  }

  // 在途事件排空后定格计数（此后全部观察者已终结，不可能再出件）
  thread::sleep(Duration::from_millis(200));
  broker.dispose();

  let (pushed, popped, received) = (
    pushed.load(Ordering::Relaxed),
    store.popped_count(),
    received.load(Ordering::Relaxed),
  );
  // 用例有效性底线：确有出件发生，守恒断言非空转
  assert!(
    popped > CLIENTS,
    "并发压力未产生出件（popped {popped}），用例失效"
  );
  assert_eq!(
    popped, received,
    "存储层弹出 {popped} 项，客户端仅收 {received} 项，超时竞态丢件"
  );
  assert_eq!(
    pushed,
    popped + store.remaining_count(),
    "推入量与弹出量不守恒"
  );
  Ok(())
}

/// 确定性交错回归：写者「提交＋通知」钉进登记侧「试取读取→挂队」窗口
///
/// 对标 C# keysToObserversLock 写锁/读侧互斥（CollectionItemBroker.cs:255
/// "This lock is for synchronization with incoming collection updated events"）。
/// 主循环线程（ThreadSpawner 专用 OS 线程，与写者天然异核）经
/// initialize_observer 完成 store 空集读取后经读后注入放行写者线程：写者
/// 按 write.rs「提交先于通知」同形 push 元素并同步调用
/// handle_collection_update。旧形态（试取与挂队分属独立临界区、挂队前队列
/// 未发布）下通知读空/缺席早退、事件不入链——元素滞留 store、timeout=0
/// 观察者永悬、waiting_observer_count 不归零，判红；新形态下通知被键队列锁
/// 挡在挂队临界区之外（注入侧 200ms 有界等待即放行主循环挂队放锁，写者
/// 随后发通知），事件必达、观察者经 CollectionUpdated 正常指派。握手全程
/// 确定性，无轮询碰运气成分
#[compio::test]
async fn notify_landing_in_registration_window_wakes_observer() -> aok::Result<()> {
  let store = Arc::new(MemStore::new());
  let broker = Arc::new(CollectionItemBroker::new_with_spawner(
    store.clone(),
    ThreadSpawner,
  ));
  broker.start_main_loop();

  // 写者线程：入场信号 → 提交元素 → 同步通知 → 完成信号
  let (enter_tx, enter_rx) = mpsc::channel();
  let (done_tx, done_rx) = mpsc::channel();
  let writer_store = store.clone();
  let writer_broker = broker.clone();
  let writer = thread::spawn(move || {
    if enter_rx.recv_timeout(Duration::from_secs(5)).is_ok() {
      writer_store.push(0, 0, b"regwin-k", b"hot-item");
      writer_broker.handle_collection_update((0, 0), b"regwin-k");
    }
    let _ = done_tx.send(());
  });

  // 读后注入（一次性，跑在主循环线程的试取返回前）：放行写者并等其闭环。
  // 旧形态下写者不与任何锁竞争、毫秒级完成；新形态下写者被登记侧
  // 「判空→试取→挂队」同一临界区的键锁挡住，此处 200ms 超时后主循环
  // 挂队放锁，写者随即补发通知（Receiver 非 Sync，套 Mutex 供 Fn 闭包捕获）
  let done_rx = Mutex::new(done_rx);
  store.arm_post_read_injection(move || {
    let _ = enter_tx.send(());
    let _ = done_rx.lock().recv_timeout(Duration::from_millis(200));
  });

  // timeout=0 生产入口登记（park 前空集合，试取必未果入窗）
  let observer = broker.start_wait(
    RespCommand::Blpop,
    vec![b"regwin-k".to_vec()],
    888,
    vec![],
    (0, 0),
  );

  // 新形态：通知入链 → 观察者被指派、元素出清；旧形态：永悬于
  // WaitingForResult，此处判红
  wait_for_status(&observer, ObserverStatus::ResultSet).await;
  assert_eq!(
    observer.result().item.as_deref(),
    Some(b"hot-item".as_slice())
  );
  assert_eq!(store.popped_count(), 1, "元素须经指派正常出件一次");
  assert_eq!(store.remaining_count(), 0, "元素不得滞留 store");

  // 挂队-清退闭环：指派后队列摘除、会话映射回收、写者线程解阻塞收尾
  let folded = NsPrefix::new(0).join(0).isolate(b"regwin-k");
  assert_eq!(broker.waiting_observer_count(&folded), None);
  assert!(broker.try_get_observer(888).is_none());
  writer.join().unwrap();
  broker.dispose();
  Ok(())
}

/// 同步不可出件（键 park 后升阶分层）：assign 臂命中 degrade 对全队列送空
/// 应答、清队摘键，元素留集不丢——票 zcode-r23-broker 发现一。旧形态下队首
/// 试取恒 None 即 FIFO break，分层键上观察者永久饥饿（元素持续到达而无人
/// 可被服务，timeout=0 无解除路径）
#[compio::test]
async fn assign_degrade_unblocks_whole_queue() -> aok::Result<()> {
  let store = Arc::new(MemStore::new());
  let broker = Arc::new(CollectionItemBroker::new(store.clone()));

  // 两观察者同键挂队（次obs经 has_waiting 跳过试取直挂）
  let first = broker.start_wait(RespCommand::Blpop, vec![b"k".to_vec()], 21, vec![], (0, 0));
  wait_for_status(&first, ObserverStatus::WaitingForResult).await;
  let second = broker.start_wait(RespCommand::Blpop, vec![b"k".to_vec()], 22, vec![], (0, 0));
  wait_for_status(&second, ObserverStatus::WaitingForResult).await;

  // 挂起期间元素到达且键升阶分层（大写入升阶收尾必 notify）
  store.push(0, 0, b"k", b"item-x");
  store.set_degrade(true);
  broker.handle_collection_update((0, 0), b"k");

  // 全队列送空应答（ResultSet、非数据形态），客户端重试经预探路由慢路径
  wait_for_status(&first, ObserverStatus::ResultSet).await;
  wait_for_status(&second, ObserverStatus::ResultSet).await;
  assert!(!first.result().found());
  assert!(!second.result().found());

  // 队列清空摘键；元素未弹出未丢（仍留集合，慢路径可服务）
  let folded = NsPrefix::new(0).join(0).isolate(b"k");
  assert_eq!(broker.waiting_observer_count(&folded), None);
  assert_eq!(store.popped_count(), 0);
  assert_eq!(store.remaining_count(), 1);
  Ok(())
}

/// 新观察者首试即命中同步不可出件：送空应答、回收会话映射、不挂队——
/// 不入经纪域饥饿（客户端空回复重试经预探整体路由慢路径）
#[compio::test]
async fn initialize_degrade_returns_empty_without_queueing() -> aok::Result<()> {
  let store = Arc::new(MemStore::new());
  store.set_degrade(true);
  let broker = Arc::new(CollectionItemBroker::new(store.clone()));

  let observer = broker.start_wait(RespCommand::Blpop, vec![b"k".to_vec()], 31, vec![], (0, 0));
  wait_for_status(&observer, ObserverStatus::ResultSet).await;
  assert!(!observer.result().found());
  assert!(broker.try_get_observer(31).is_none());

  let folded = NsPrefix::new(0).join(0).isolate(b"k");
  assert_eq!(broker.waiting_observer_count(&folded), None);
  assert_eq!(store.popped_count(), 0);
  Ok(())
}

/// 同步不可出件的 zset 命令形态：BZPOPMIN 首试命中 degrade 送空应答、
/// 不挂队——送客机制与命令族无关（TryGetOutcome 单源分派），此处钉
/// Bzpopmin 观察者的空应答形态（write_collection_item_result 空值臂
/// 依赖 result().found() == false，BLPOP 族空数组、BZPOP 族空值）
#[compio::test]
async fn initialize_degrade_zset_unblocks_without_queueing() -> aok::Result<()> {
  let store = Arc::new(MemStore::new());
  store.set_degrade(true);
  let broker = Arc::new(CollectionItemBroker::new(store.clone()));

  let observer = broker.start_wait(
    RespCommand::Bzpopmin,
    vec![b"z".to_vec()],
    41,
    vec![],
    (0, 0),
  );
  wait_for_status(&observer, ObserverStatus::ResultSet).await;
  assert!(!observer.result().found());
  assert!(broker.try_get_observer(41).is_none());
  assert_eq!(store.popped_count(), 0);
  Ok(())
}

/// 事件处理体 panic 不打死主循环（r30-bgthread 发现三：wbase supervise_item
/// 逐事件守卫，对标 C# 处理链 try/catch + StartAsync 外层 finally done.Set()）：
/// 注入一次试取 panic 后，该事件被弃、观察者保持等待，主循环续跑，后续正常
/// 集合更新照常出件——杜绝「单事件 panic → 主循环死亡 → main_loop_task_status
/// 永卡 STARTED → 阻塞族观察者永挂」
#[compio::test]
async fn event_panic_does_not_kill_main_loop() -> aok::Result<()> {
  let store = Arc::new(MemStore::new());
  let broker = Arc::new(CollectionItemBroker::new_with_spawner(
    store.clone(),
    ThreadSpawner,
  ));
  broker.start_main_loop();

  // 观察者正常注册挂队（首次试取 items 空，未果挂队）；以观察表在册为
  // 挂队完成真据（NewObserver 首试与注入 armed 有跨线程竞态，先行等待）
  let observer = broker.start_wait(RespCommand::Blpop, vec![b"k".to_vec()], 77, vec![], (0, 0));
  let folded = NsPrefix::new(0).join(0).isolate(b"k");
  for _ in 0..10_000 {
    if broker.waiting_observer_count(folded.as_slice()) == Some(1) {
      break;
    }
    sleep(Duration::from_millis(1)).await;
  }
  assert_eq!(
    broker.waiting_observer_count(folded.as_slice()),
    Some(1),
    "观察者须先真实挂队"
  );

  // 注入一次性试取 panic 后推入元素：CollectionUpdated 事件处理体 panic，
  // 事件被弃（观察者保持挂队等待、主循环不死）
  store.arm_injection(|| panic!("injected broker panic"));
  store.push(0, 0, b"k", b"doomed-item");
  broker.handle_collection_update((0, 0), b"k");
  sleep(Duration::from_millis(50)).await;
  assert_eq!(observer.status(), ObserverStatus::WaitingForResult);
  assert_eq!(
    broker.waiting_observer_count(folded.as_slice()),
    Some(1),
    "panic 事件弃件后观察者保持挂队"
  );

  // 后续正常事件照常处理：队列残留项正常弹出，主循环永续
  broker.handle_collection_update((0, 0), b"k");
  wait_for_status(&observer, ObserverStatus::ResultSet).await;
  assert_eq!(
    observer.result().item.as_deref(),
    Some(b"doomed-item".as_slice())
  );
  Ok(())
}

/// 争用重投取件源（票 wtxn-wkv-keybucket-hash-scope-desync 补强注记）：
/// 武装位在时试取报取件窗桶闩争用（is_contended，对位外层事务 EXEC 重放期
/// 持本键 scoped 桶闩），武装位消耗后按存项正常出件
struct ContendedOnceStore {
  items: Mutex<HashMap<Vec<u8>, VecDeque<Vec<u8>>>>,
  armed: AtomicBool,
}

impl ContendedOnceStore {
  fn with_item(key: &[u8], item: &[u8]) -> Self {
    let mut items = HashMap::default();
    items.insert(key.to_vec(), VecDeque::from([item.to_vec()]));
    Self {
      items: Mutex::new(items),
      armed: AtomicBool::new(true),
    }
  }

  fn rearm(&self) {
    self.armed.store(true, Ordering::Relaxed);
  }
}

impl CollectionItemStore for ContendedOnceStore {
  fn try_get_result(
    &self,
    _ns: u64,
    _db: u64,
    key: &[u8],
    _command: RespCommand,
    _cmd_args: &[Vec<u8>],
    _fail_on_src_type_mismatch: bool,
  ) -> TryGetOutcome {
    if self.armed.swap(false, Ordering::Relaxed) {
      return TryGetOutcome::contended();
    }
    let mut map = self.items.lock();
    match map.get_mut(key).and_then(|q| q.pop_front()) {
      Some(item) => TryGetOutcome::found(0, CollectionItemResult::single(key.to_vec(), item)),
      None => TryGetOutcome::none(),
    }
  }
}

/// 争用重投闭环：首试遇取件窗桶闩争用（外层事务持本键 scoped 桶闩）时观察者
/// 照常挂队、事件分派带回执；让核重投（等价主循环 start_async 重投臂）后
/// 外层放闩即出件——非空键不因「外层唯一写点已过、后续无写入」而
/// timeout=0 永久悬挂。CollectionUpdated 通路遇争用同款带回执、FIFO 队首
/// 保持挂队
#[test]
fn broker_contended_outcome_requeues_and_retry_delivers() {
  let store = Arc::new(ContendedOnceStore::with_item(b"k", b"m1"));
  let broker = Arc::new(CollectionItemBroker::new(store.clone()));
  let folded = NsPrefix::new(0).join(0).isolate(b"k");
  let observer = Arc::new(Obs::new(21, RespCommand::Bzpopmin, vec![], (0, 0)));

  // NewObserver 首试：争用位命中——挂队不丢、带回执（键为域折叠形态原样）
  let retry = broker.handle_broker_event(CollectionItemBrokerEvent::create_new_observer_event(
    observer.clone(),
    vec![folded.clone()],
  ));
  assert_eq!(retry.as_deref(), Some(folded.as_slice()));
  assert_eq!(observer.status(), ObserverStatus::WaitingForResult);
  assert_eq!(
    broker.waiting_observer_count(&folded),
    Some(1),
    "争用试取不可得时观察者必须挂队等待重投"
  );

  // 主循环让核后重投 CollectionUpdated：外层持闩臂（EXEC 提交收尾）已放闩，
  // 出件即得——不悬挂
  let retry2 = broker.handle_broker_event(
    CollectionItemBrokerEvent::create_collection_updated_event(folded.clone()),
  );
  assert!(retry2.is_none(), "出件成功不得再带回执重投");
  assert_eq!(observer.status(), ObserverStatus::ResultSet);
  assert_eq!(observer.result().item.as_deref(), Some(b"m1".as_slice()));
  assert_eq!(
    broker.waiting_observer_count(&folded),
    None,
    "出件后空队列必须摘键"
  );

  // CollectionUpdated 通路争用（队首 FIFO 保守）：保持挂队并带回执，重投后出件
  store.rearm();
  store
    .items
    .lock()
    .entry(b"k".to_vec())
    .or_default()
    .push_back(b"m2".to_vec());
  let observer2 = Arc::new(Obs::new(22, RespCommand::Bzpopmin, vec![], (0, 0)));
  let retry3 = broker.handle_broker_event(CollectionItemBrokerEvent::create_new_observer_event(
    observer2.clone(),
    vec![folded.clone()],
  ));
  assert_eq!(retry3.as_deref(), Some(folded.as_slice()));
  assert_eq!(observer2.status(), ObserverStatus::WaitingForResult);

  // 重投仍争用：队首保持挂队、再带回执（主循环继续让核重投，收敛于放闩轮）
  store.rearm();
  let retry4 = broker.handle_broker_event(
    CollectionItemBrokerEvent::create_collection_updated_event(folded.clone()),
  );
  assert_eq!(retry4.as_deref(), Some(folded.as_slice()));
  assert_eq!(observer2.status(), ObserverStatus::WaitingForResult);

  // 放闩轮：重投即出件
  let retry5 = broker.handle_broker_event(
    CollectionItemBrokerEvent::create_collection_updated_event(folded.clone()),
  );
  assert!(retry5.is_none());
  assert_eq!(observer2.status(), ObserverStatus::ResultSet);
  assert_eq!(observer2.result().item.as_deref(), Some(b"m2".as_slice()));
}

// ===== 主循环 panic 死亡重挂回归（票 wcol-itembroker-main-loop-panic-dead-no-remount）=====

/// 宿主注入：吞掉任务体——事件入队却无消费者，等价「主循环死亡」形态
///（外层循环 panic 面无法经存储注入器确定性触发，故以空启动器模拟死亡态，
/// 再直调恢复臂验证重建/补扫/复位三效；spawner 注入缝是 trait 既有能力，非领域逻辑假 mock）
struct NoopSpawner;

impl TaskSpawner for NoopSpawner {
  fn spawn<F>(&self, _fut: F)
  where
    F: Future<Output = ()> + Send + 'static,
  {
  }
}

type BoxTask = Pin<Box<dyn Future<Output = ()> + Send + 'static>>;

/// 宿主注入：暂存任务体供测试确定性地择机泵起（验证重挂后新循环消费补扫库存）
#[derive(Default)]
struct GateSpawner {
  queue: Arc<Mutex<Vec<BoxTask>>>,
}

impl TaskSpawner for GateSpawner {
  fn spawn<F>(&self, fut: F)
  where
    F: Future<Output = ()> + Send + 'static,
  {
    self.queue.lock().push(Box::pin(fut));
  }
}

/// 三形态合一：panic 恢复臂重建事件通道、丢弃死通道洪峰（堆积有界）、按在册库存
/// 补扫重投——票面第 1/2/3 条（通道 ArcSwap 换装丢弃旧通道、有界重投而非无界堆积、
/// keys_to_observers 与 session_id_to_observer 双类补扫）
#[test]
fn panic_recovery_rebuilds_channel_discards_dead_backlog_and_replays_stock() {
  let store = Arc::new(MemStore::new());
  let broker = Arc::new(CollectionItemBroker::new_with_spawner(
    store.clone(),
    NoopSpawner,
  ));
  // 主循环「死亡」：状态置 STARTED 却无消费者（NoopSpawner 吞任务体）
  broker.start_main_loop();

  let folded = NsPrefix::new(0).join(0).isolate(b"k");
  // 类 1：一观察者挂在键队列上（集合更新事件的补扫对象；initialize_observer 不入会话映射）
  let attached = Arc::new(Obs::new(1, RespCommand::Blpop, vec![], (0, 0)));
  broker.initialize_observer(attached.clone(), from_ref(&folded));
  // 类 2：另一观察者仅入 session_id_to_observer（NewObserver 事件滞留死通道未消费、未挂队）
  let waiting = broker.start_wait(RespCommand::Blpop, vec![b"k".to_vec()], 2, vec![], (0, 0));

  // 死通道灌入千级集合更新洪峰（无消费者，旧形态本应无界堆积）
  for _ in 0..1000 {
    broker.handle_collection_update((0, 0), b"k");
  }

  // panic 恢复臂：重建通道（死通道洪峰随旧 tx/rx 一并丢弃）+ 存量补扫 + 复位状态位
  assert!(
    broker.recover_after_panic(),
    "STARTED 态恢复臂须复位成功（非 DISPOSED 终态）"
  );

  // 新通道事件严格有界：仅结构补扫量——1 条 CollectionUpdated（attached 非空键队列）
  // + 1 条 NewObserver（unqueued 的 waiting）；千级洪峰与 start_wait 原始滞留
  // NewObserver 全随旧通道丢弃
  let events = broker.drain_events_for_test();
  assert_eq!(
    events.len(),
    2,
    "新通道仅补扫出在册结构量，死通道洪峰与滞留原始事件须被丢弃"
  );
  let updated = events
    .iter()
    .filter(|e| e.event_type == CollectionItemBrokerEventType::CollectionUpdated)
    .count();
  let new_obs = events
    .iter()
    .filter(|e| e.event_type == CollectionItemBrokerEventType::NewObserver)
    .count();
  assert_eq!(updated, 1, "非空键队列补扫一条 CollectionUpdated");
  assert_eq!(new_obs, 1, "unqueued 在册观察者补扫一条 NewObserver");
  assert!(
    events.iter().any(
      |e| e.event_type == CollectionItemBrokerEventType::CollectionUpdated
        && e.key.as_deref() == Some(folded.as_slice())
    ),
    "CollectionUpdated 须携带补扫键（域折叠形态）"
  );
  let replayed = events
    .iter()
    .find(|e| e.event_type == CollectionItemBrokerEventType::NewObserver)
    .expect("须补扫 unqueued 观察者的 NewObserver 事件");
  assert_eq!(
    replayed.observer.as_ref().unwrap().session_id,
    2,
    "补扫的 NewObserver 须为仅入会话映射的 waiting 观察者"
  );
  assert_eq!(
    replayed.keys,
    waiting.keys().cloned(),
    "补扫 NewObserver 键组须取观察者登记定格的 keys"
  );
}

/// 终态保全：dispose 置 MAIN_LOOP_DISPOSED 后，复位与恢复臂的 STARTED→NOT_STARTED
/// CAS 必失败——绝不把已销毁经纪复回可重挂态（票面第 4 条）
#[test]
fn dispose_terminal_state_rejects_main_loop_reset() {
  let store = Arc::new(MemStore::new());
  let broker = Arc::new(CollectionItemBroker::new_with_spawner(
    store.clone(),
    NoopSpawner,
  ));
  broker.start_main_loop(); // STARTED
  broker.dispose(); // → DISPOSED 终态
  assert!(broker.is_disposed());

  // DISPOSED 终态拒绝复位：reset 与 recover 的 STARTED→NOT_STARTED CAS 必失败
  assert!(
    !broker.reset_main_loop_status(),
    "DISPOSED 终态拒绝复位为 NOT_STARTED"
  );
  assert!(
    !broker.recover_after_panic(),
    "DISPOSED 下恢复臂复位臂必拒（返回 false）"
  );
  assert!(
    broker.is_disposed(),
    "恢复/复位臂不得回退 MAIN_LOOP_DISPOSED 终态"
  );
}

/// 死亡形态根除：panic 复位留 NOT_STARTED 态可被后续 start_main_loop 反复重新认领
///（NOT_STARTED↔STARTED 往复不受阻）——杜绝旧形态「状态位永卡 STARTED、CAS 恒失败、
/// 拒一切重拉」的死亡终局。重挂预算上限 REMOUNT_LIMIT 为代码常量有界，多轮连败耗尽
/// 后留 NOT_STARTED 复位态即由本路径承接下一次认领
#[test]
fn panic_reset_leaves_relaunchable_not_started_state() {
  let store = Arc::new(MemStore::new());
  let broker = Arc::new(CollectionItemBroker::new_with_spawner(
    store.clone(),
    NoopSpawner,
  ));
  broker.start_main_loop(); // STARTED（NoopSpawner 吞体 → 死亡态）

  // 恢复臂复位为 NOT_STARTED 成功
  assert!(broker.recover_after_panic());

  // NOT_STARTED 可被 start_main_loop 重新认领（CAS NOT_STARTED→STARTED 成功），
  // 再次恢复仍可复位——状态机往复不受「位永真」封锁
  broker.start_main_loop();
  assert!(
    broker.recover_after_panic(),
    "重新认领后的 STARTED 态仍可复位（非一次性死锁）"
  );

  broker.start_main_loop();
  assert!(!broker.is_disposed(), "全程未触 DISPOSED 终态");
}

/// 重挂闭环端到端：死亡态下观察者永悬 → 恢复臂重建+补扫 → 新循环泵起消费补扫库存
/// → 滞留观察者被指派出件、解除 timeout=0 悬挂（票面第 1/2/3 条联效，对标 C#
/// CollectionItemBroker StartMainLoop 死而复生；以 GateSpawner 确定性择机泵起新循环，
/// 遵协程让渡不内联驱动调度器）
#[compio::test]
async fn remount_after_panic_serves_stuck_observer() -> aok::Result<()> {
  let store = Arc::new(MemStore::new());
  let gate = Arc::new(Mutex::new(Vec::<BoxTask>::new()));
  let broker = Arc::new(CollectionItemBroker::new_with_spawner(
    store.clone(),
    GateSpawner {
      queue: gate.clone(),
    },
  ));

  // start_wait 登记观察者并入队 NewObserver；循环体被 GateSpawner 暂存未运行（死亡态）
  let observer = broker.start_wait(RespCommand::Blpop, vec![b"k".to_vec()], 5, vec![], (0, 0));
  // 数据到达存储（未通知：keys_to_observers 尚无该键队列，通知侧读空早退）
  store.push(0, 0, b"k", b"late-item");

  // 死亡态：观察者滞留等待，NewObserver 事件在未消费通道中
  assert_eq!(observer.status(), ObserverStatus::WaitingForResult);

  // 模拟 panic 后重挂：丢弃被暂存的死亡循环体 → 恢复臂重建通道+补扫+复位 →
  // 重新认领拉起新循环 → 泵起新循环体消费补扫库存
  gate.lock().clear();
  assert!(broker.recover_after_panic(), "STARTED 态恢复臂须复位成功");
  broker.start_main_loop(); // NOT_STARTED→STARTED 重新认领，暂存新循环体
  let tasks: Vec<BoxTask> = take(&mut *gate.lock());
  for task in tasks {
    spawn(task).detach();
  }

  // 补扫的 NewObserver 经 initialize_observer 命中存储中的 late-item → 指派出件
  wait_for_status(&observer, ObserverStatus::ResultSet).await;
  assert_eq!(
    observer.result().item.as_deref(),
    Some(b"late-item".as_slice()),
    "重挂后须消费补扫库存并正常出件"
  );
  assert!(broker.try_get_observer(5).is_none());

  broker.dispose();
  Ok(())
}
