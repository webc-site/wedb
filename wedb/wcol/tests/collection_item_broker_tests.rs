use std::{collections::VecDeque, sync::Arc, time::Duration};

use compio::{
  runtime::{Runtime, spawn},
  time::sleep,
};
use parking_lot::Mutex;
use wbase::map::HashMap;
use wcol::{
  ObjectOutput,
  itembroker::{
    collection_item_broker::{
      CollectionItemBroker, CollectionItemStore, TryGetOutcome, try_get_next_list_item,
      try_move_next_list_item,
    },
    collection_item_observer::{
      CollectionItemObserver as Obs, CollectionItemResult, ObserverStatus,
    },
  },
  list::list_object::{ListObject, ListOperation, OperationDirection},
};
use wresp::command::RespCommand;

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
  ) -> TryGetOutcome {
    let mut map = self.0.lock();
    let Some(queue) = map.get_mut(key) else {
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
            TryGetOutcome::found(count - 1, CollectionItemResult::single(key.to_vec(), item))
          }
          None => TryGetOutcome::with_count(count),
        }
      }
      _ => TryGetOutcome::with_count(count),
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

/// 轮询等待观察者进入指定状态（事件由主循环异步消费，给确定性行断言让渡）
async fn wait_for_status(observer: &Obs, status: ObserverStatus) {
  for _ in 0..10_000 {
    if observer.status() == status {
      return;
    }
    sleep(Duration::from_millis(1)).await;
  }
  panic!("等待观察者进入目标状态超时");
}

#[test]
fn broker_queues_observer_until_update() -> aok::Result<()> {
  Runtime::new()?.block_on(async {
    let store = Arc::new(MemStore::new());
    let broker = Arc::new(CollectionItemBroker::new(store.clone()));

    // 键无数据：经生产入口 start_wait 完成会话映射登记并挂队等待
    let observer = broker.start_wait(RespCommand::Blpop, vec![b"k".to_vec()], 2, vec![]);
    wait_for_status(&observer, ObserverStatus::WaitingForResult).await;
    assert!(broker.try_get_observer(2).is_some());

    // 数据到达 → CollectionUpdated 事件由主循环消费并指派
    store.push(b"k", b"item-2");
    broker.handle_collection_update(b"k");
    wait_for_status(&observer, ObserverStatus::ResultSet).await;
    assert_eq!(
      observer.result().item.as_deref(),
      Some(b"item-2".as_slice())
    );
    Ok(())
  })
}

#[test]
fn handle_session_disposed_removes_observer() -> aok::Result<()> {
  Runtime::new()?.block_on(async {
    let store = Arc::new(MemStore::new());
    let broker = Arc::new(CollectionItemBroker::new(store));
    let observer = broker.start_wait(RespCommand::Blpop, vec![b"k".to_vec()], 9, vec![]);
    wait_for_status(&observer, ObserverStatus::WaitingForResult).await;
    assert!(broker.try_get_observer(9).is_some());

    broker.handle_session_disposed(9);
    assert!(broker.try_get_observer(9).is_none());
    assert_eq!(observer.status(), ObserverStatus::SessionDisposed);
    Ok(())
  })
}

#[test]
fn clean_removes_finished_observers_and_empty_keys() -> aok::Result<()> {
  Runtime::new()?.block_on(async {
    let store = Arc::new(MemStore::new());
    let broker = Arc::new(CollectionItemBroker::new(store.clone()));

    // 先到的观察者经数据到达被指派终结
    let done = broker.start_wait(RespCommand::Blpop, vec![b"k".to_vec()], 3, vec![]);
    wait_for_status(&done, ObserverStatus::WaitingForResult).await;
    store.push(b"k", b"item-1");
    broker.handle_collection_update(b"k");
    wait_for_status(&done, ObserverStatus::ResultSet).await;

    // 后到的观察者同键挂队等待
    let waiting = broker.start_wait(RespCommand::Blpop, vec![b"k".to_vec()], 4, vec![]);
    wait_for_status(&waiting, ObserverStatus::WaitingForResult).await;

    broker.clean_keys_to_observers();
    // 已终结观察者被弹出，等待者仍在
    assert_eq!(waiting.status(), ObserverStatus::WaitingForResult);
    Ok(())
  })
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
#[test]
fn main_loop_wakes_waiting_observer() -> aok::Result<()> {
  Runtime::new()?.block_on(async {
    let store = Arc::new(MemStore::new());
    let broker = Arc::new(CollectionItemBroker::new(store.clone()));

    let broker_clone = Arc::clone(&broker);
    let store_clone = Arc::clone(&store);
    spawn(async move {
      for _ in 0..10_000 {
        if broker_clone.try_get_observer(6).is_some() {
          store_clone.push(b"k", b"late-item");
          broker_clone.handle_collection_update(b"k");
          break;
        }
        sleep(Duration::from_millis(1)).await;
      }
    })
    .detach();

    // 生产出口三段式：start_wait 登记挂队 → wait_result 等待 → finish_wait 收尾
    // （会话 BlockedWait 同构形态）
    let observer = broker.start_wait(RespCommand::Blpop, vec![b"k".to_vec()], 6, vec![]);
    observer.wait_result().await;
    let result = broker.finish_wait(&observer);
    assert_eq!(result.item.as_deref(), Some(b"late-item".as_slice()));
    assert!(broker.try_get_observer(6).is_none());
    Ok(())
  })
}

#[test]
fn collection_update_wakes_multiple_waiting_observers() -> aok::Result<()> {
  Runtime::new()?.block_on(async {
    let store = Arc::new(MemStore::new());
    let broker = Arc::new(CollectionItemBroker::new(store.clone()));

    // 三个观察者经生产入口 start_wait 同键挂队
    let obs1 = broker.start_wait(RespCommand::Blpop, vec![b"batch_k".to_vec()], 101, vec![]);
    let obs2 = broker.start_wait(RespCommand::Blpop, vec![b"batch_k".to_vec()], 102, vec![]);
    let obs3 = broker.start_wait(RespCommand::Blpop, vec![b"batch_k".to_vec()], 103, vec![]);
    wait_for_status(&obs3, ObserverStatus::WaitingForResult).await;

    // 集合推入 2 个元素，单次触发 update：主循环连续满足队首 obs1、obs2，obs3 保持等待
    store.push(b"batch_k", b"item-a");
    store.push(b"batch_k", b"item-b");
    broker.handle_collection_update(b"batch_k");

    wait_for_status(&obs2, ObserverStatus::ResultSet).await;
    assert_eq!(obs1.status(), ObserverStatus::ResultSet);
    assert_eq!(obs1.result().item.as_deref(), Some(b"item-a".as_slice()));
    assert_eq!(obs2.result().item.as_deref(), Some(b"item-b".as_slice()));
    assert_eq!(obs3.status(), ObserverStatus::WaitingForResult);
    assert!(broker.try_get_observer(101).is_none());
    assert!(broker.try_get_observer(102).is_none());
    assert!(broker.try_get_observer(103).is_some());
    Ok(())
  })
}
