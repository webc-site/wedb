use std::{
  collections::VecDeque,
  pin::Pin,
  sync::Arc,
  task::{Context, Poll, Waker},
};

use gxhash::HashMap;
use parking_lot::Mutex;
use wnode::{
  objects::{
    itembroker::{
      collection_item_broker::{
        CollectionItemBroker, CollectionItemStore, TaskSpawner, try_get_next_list_item,
        try_move_next_list_item,
      },
      collection_item_observer::{
        CollectionItemObserver as Obs, CollectionItemResult, ObserverStatus,
      },
    },
    list::list_object::{ListObject, ListOperation, OperationDirection},
  },
  types::RespCommand,
};

/// 内存取件源：key → 项队列
struct MemStore(Mutex<HashMap<Vec<u8>, VecDeque<Vec<u8>>>>);

impl MemStore {
  fn new() -> Self {
    Self(Mutex::new(HashMap::default()))
  }

  fn push(&self, key: &[u8], item: &[u8]) {
    self
      .0
      .lock()
      .entry(key.to_vec())
      .or_default()
      .push_back(item.to_vec());
  }
}

impl CollectionItemStore for MemStore {
  fn try_get_result(
    &self,
    key: &[u8],
    command: RespCommand,
    _cmd_args: &[Vec<u8>],
    _move_item: bool,
  ) -> (usize, Option<CollectionItemResult>) {
    let mut map = self.0.lock();
    let Some(queue) = map.get_mut(key) else {
      return (0, None);
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
          Some(item) => (
            count - 1,
            Some(CollectionItemResult::single(key.to_vec(), item)),
          ),
          None => (0, None),
        }
      }
      _ => (count, None),
    }
  }
}

#[test]
fn broker_assigns_item_to_waiting_observer() {
  let store = Arc::new(MemStore::new());
  let broker = Arc::new(CollectionItemBroker::new(store.clone()));
  let observer = Arc::new(Obs::new(1, RespCommand::Blpop, vec![]));

  // 预置键数据后登记观察者：InitializeObserver 同步试取即得
  store.push(b"k", b"item-1");
  broker.initialize_observer(observer.clone(), &[b"k".to_vec()]);

  assert_eq!(observer.status(), ObserverStatus::ResultSet);
  let result = observer.result();
  assert_eq!(result.item.as_deref(), Some(b"item-1".as_slice()));
  // 观察者已从会话映射摘除
  assert!(broker.try_get_observer(1).is_none());
}

#[test]
fn broker_queues_observer_until_update() {
  let store = Arc::new(MemStore::new());
  let broker = Arc::new(CollectionItemBroker::new(store.clone()));
  let observer = Arc::new(Obs::new(2, RespCommand::Blpop, vec![]));
  // C# 在 GetCollectionItemAsync 登记会话映射；直驱路径手工补登记
  broker.register_session_observer(observer.clone());

  // 键无数据：观察者入队等待
  broker.initialize_observer(observer.clone(), &[b"k".to_vec()]);
  assert_eq!(observer.status(), ObserverStatus::WaitingForResult);
  assert!(broker.try_get_observer(2).is_some());

  // 数据到达 → CollectionUpdated 入队；同步测试手动消费事件
  store.push(b"k", b"item-2");
  broker.handle_collection_update(b"k");
  while let Some(event) = broker.pop_broker_event() {
    broker.handle_broker_event(event);
  }
  assert_eq!(observer.status(), ObserverStatus::ResultSet);
  assert_eq!(
    observer.result().item.as_deref(),
    Some(b"item-2".as_slice())
  );
}

#[test]
fn handle_session_disposed_removes_observer() {
  let store = Arc::new(MemStore::new());
  let broker = Arc::new(CollectionItemBroker::new(store));
  let observer = Arc::new(Obs::new(9, RespCommand::Blpop, vec![]));
  broker.register_session_observer(observer.clone());
  broker.initialize_observer(observer.clone(), &[b"k".to_vec()]);
  assert!(broker.try_get_observer(9).is_some());

  broker.handle_session_disposed(9);
  assert!(broker.try_get_observer(9).is_none());
  assert_eq!(observer.status(), ObserverStatus::SessionDisposed);
}

#[test]
fn clean_removes_finished_observers_and_empty_keys() {
  let store = Arc::new(MemStore::new());
  let broker = Arc::new(CollectionItemBroker::new(store));
  let done = Arc::new(Obs::new(3, RespCommand::Blpop, vec![]));
  let waiting = Arc::new(Obs::new(4, RespCommand::Blpop, vec![]));

  broker.register_session_observer(done.clone());
  broker.initialize_observer(done.clone(), &[b"k".to_vec()]);
  broker.initialize_observer(waiting.clone(), &[b"k".to_vec()]);

  broker.clean_keys_to_observers();
  // 已终结观察者被弹出，等待者仍在
  assert_eq!(waiting.status(), ObserverStatus::WaitingForResult);
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
  assert_eq!(dst.index(0), Some(b"b".to_vec()));
}

/// 确定性单线程驱动主循环：自旋执行器逐轮 poll 主等待与派生任务，
/// 验证 阻塞等待 → 挂队 → 集合更新 → 指派 → 解除 全链路
#[test]
fn main_loop_wakes_waiting_observer() {
  struct TaskRunner {
    state: *mut (),
    poll_fn: unsafe fn(*mut (), &mut Context<'_>) -> Poll<()>,
    drop_fn: unsafe fn(*mut ()),
  }
  unsafe impl Send for TaskRunner {}

  impl TaskRunner {
    fn new<F: Future<Output = ()> + Send + 'static>(fut: F) -> Self {
      let b = Box::into_raw(Box::new(fut));
      unsafe fn poll_impl<F: Future<Output = ()>>(ptr: *mut (), cx: &mut Context<'_>) -> Poll<()> {
        Pin::new_unchecked(&mut *ptr.cast::<F>()).poll(cx)
      }
      unsafe fn drop_impl<F>(ptr: *mut ()) {
        drop(Box::from_raw(ptr.cast::<F>()));
      }
      Self {
        state: b.cast(),
        poll_fn: poll_impl::<F>,
        drop_fn: drop_impl::<F>,
      }
    }

    fn poll(&mut self, cx: &mut Context<'_>) -> Poll<()> {
/// 确定性单线程驱动主循环：验证 阻塞等待 → 挂队 → 集合更新 → 指派 → 解除 全链路
#[test]
fn main_loop_wakes_waiting_observer() -> aok::Result<()> {
  compio::runtime::Runtime::new()?.block_on(async {
    let store = Arc::new(MemStore::new());
    let broker = Arc::new(CollectionItemBroker::new(store.clone()));

    let broker_clone = Arc::clone(&broker);
    let store_clone = Arc::clone(&store);
    compio::runtime::spawn(async move {
      for _ in 0..10_000 {
        if broker_clone.try_get_observer(6).is_some() {
          store_clone.push(b"k", b"late-item");
          broker_clone.handle_collection_update(b"k");
          break;
        }
        compio::time::sleep(std::time::Duration::from_millis(1)).await;
      }
    })
    .detach();

    let result = broker
      .get_collection_item_async(RespCommand::Blpop, vec![b"k".to_vec()], 6, 0.0, vec![])
      .await;
    assert_eq!(result.item.as_deref(), Some(b"late-item".as_slice()));
    assert!(broker.try_get_observer(6).is_none());
    Ok(())
  })
}

#[test]
fn collection_update_wakes_multiple_waiting_observers() {
  let store = Arc::new(MemStore::new());
  let broker = Arc::new(CollectionItemBroker::new(store.clone()));
  let obs1 = Arc::new(Obs::new(101, RespCommand::Blpop, vec![]));
  let obs2 = Arc::new(Obs::new(102, RespCommand::Blpop, vec![]));
  let obs3 = Arc::new(Obs::new(103, RespCommand::Blpop, vec![]));

  broker.register_session_observer(obs1.clone());
  broker.register_session_observer(obs2.clone());
  broker.register_session_observer(obs3.clone());

  // 键无数据：3个观察者均挂队等待
  broker.initialize_observer(obs1.clone(), &[b"batch_k".to_vec()]);
  broker.initialize_observer(obs2.clone(), &[b"batch_k".to_vec()]);
  broker.initialize_observer(obs3.clone(), &[b"batch_k".to_vec()]);

  assert_eq!(obs1.status(), ObserverStatus::WaitingForResult);
  assert_eq!(obs2.status(), ObserverStatus::WaitingForResult);
  assert_eq!(obs3.status(), ObserverStatus::WaitingForResult);

  // 集合推入 2 个元素，单次触发 update
  store.push(b"batch_k", b"item-a");
  store.push(b"batch_k", b"item-b");
  broker.handle_collection_update(b"batch_k");

  while let Some(event) = broker.pop_broker_event() {
    broker.handle_broker_event(event);
  }

  // 单次 update 应当连续满足队首的 obs1 和 obs2，obs3 保持等待
  assert_eq!(obs1.status(), ObserverStatus::ResultSet);
  assert_eq!(obs1.result().item.as_deref(), Some(b"item-a".as_slice()));
  assert_eq!(obs2.status(), ObserverStatus::ResultSet);
  assert_eq!(obs2.result().item.as_deref(), Some(b"item-b".as_slice()));
  assert_eq!(obs3.status(), ObserverStatus::WaitingForResult);
  assert!(broker.try_get_observer(101).is_none());
  assert!(broker.try_get_observer(102).is_none());
  assert!(broker.try_get_observer(103).is_some());
}
