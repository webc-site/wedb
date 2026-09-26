//! 阻塞命令观察者（对标 libs/server/Objects/ItemBroker/CollectionItemObserver.cs）
//!
//! 1:1 对标微软 Garnet 原版的 `TaskCompletionSource` / `SemaphoreSlim(0,1)`，
//! 使用 `event_listener::Event` 实现轻量异步事件通知，消灭通道与堆分配；
//! 内部状态与结果由单一 `parking_lot::Mutex` 保护。
//!
//! 自研依据: 条目观察者（C# 无对应组件）

use std::{fmt, sync::OnceLock};

use event_listener::Event;
use parking_lot::Mutex;
use wbase::ns_prefix::NsPrefix;
use wresp::command::RespCommand;

use crate::itembroker::collection_item_broker::TryGetOutcome;

/// 观察者状态
///
/// libs/server/Objects/ItemBroker/CollectionItemObserver.cs:ObserverStatus
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum ObserverStatus {
  /// 等待结果
  #[default]
  WaitingForResult,
  /// 结果已设置
  ResultSet,
  /// 调用会话已销毁
  SessionDisposed,
}

/// 观察者内部可变状态（状态 + 结果）
#[derive(Debug, Default)]
struct ObserverState {
  status: ObserverStatus,
  result: CollectionItemResult,
}

/// 阻塞命令观察者
///
/// libs/server/Objects/ItemBroker/CollectionItemObserver.cs:CollectionItemObserver
pub struct CollectionItemObserver {
  /// 发起阻塞命令的会话 ID（C# 为 RespServerSession 引用 + ObjectStoreSessionID）
  pub session_id: usize,
  /// 阻塞命令类型
  pub command: RespCommand,
  /// 命令附加参数（BLMOVE 目标键/方向、BZMPOP 的 count 等）
  pub command_args: Vec<Vec<u8>>,
  /// 观察者所属域 (ns, db)（wedb 多租户面，C# 无对位；C# 取件域随
  /// observer.Session.storageSession，rust 以此域在经纪会话上等价切换）
  pub domain: (u64, u64),
  /// 域隔离前缀（`[ns ':'][db ':']`；共享观察表键折叠/剥离与唤醒键折叠
  /// 的唯一来源）
  pub prefix: NsPrefix,
  /// 本观察者订阅的域折叠键组（`start_wait` 登记时一次性定格；C# 观察者无
  /// 对位——观察者不落键，键随 NewObserver 事件传递。本字段专为「主循环 panic
  /// 死亡重挂前的存量补扫」保留：NewObserver 事件滞留旧通道随重建丢弃时，重挂
  /// 任务体据本字段对「仅入 session_id_to_observer 未挂队」观察者重投 NewObserver
  /// （见 `CollectionItemBroker::rescan_stale_observers`，票
  /// wcol-itembroker-main-loop-panic-dead-no-remount）。用 [`OnceLock`] 承接：
  /// 登记侧一次性写入、补扫侧无锁只读，无观察者级常驻锁开销）
  keys: OnceLock<Vec<Vec<u8>>>,
  /// 状态与结果互斥锁（单锁保护，对应 C# ObserverStatusLock）
  state: Mutex<ObserverState>,
  /// 结果就绪事件通知（对应 Garnet ResultFoundSemaphore(0,1) / TaskCompletionSource）
  event: Event,
}

impl fmt::Debug for CollectionItemObserver {
  fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    f.debug_struct("CollectionItemObserver")
      .field("session_id", &self.session_id)
      .field("command", &self.command)
      .field("command_args", &self.command_args)
      .field("domain", &self.domain)
      .field("state", &self.state)
      .finish()
  }
}

impl CollectionItemObserver {
  /// 新建观察者（初始状态 WaitingForResult，结果为 Empty；域前缀由
  /// (ns, db) 单次构造）
  pub fn new(
    session_id: usize,
    command: RespCommand,
    command_args: Vec<Vec<u8>>,
    domain: (u64, u64),
  ) -> Self {
    Self {
      session_id,
      command,
      command_args,
      domain,
      prefix: NsPrefix::new(domain.0).join(domain.1),
      keys: OnceLock::new(),
      state: Mutex::new(ObserverState::default()),
      event: Event::new(),
    }
  }

  /// 一次性定格订阅键组（域折叠形态）：仅 `start_wait` 登记路径调用（写入即冻结，
  /// 二次调用被忽略，保持首次登记值为唯一真源）；供重挂补扫读取
  pub fn set_keys(&self, keys: Vec<Vec<u8>>) {
    let _ = self.keys.set(keys);
  }

  /// 订阅键组（域折叠形态）；未登记返回 `None`
  #[inline]
  pub fn keys(&self) -> Option<&Vec<Vec<u8>>> {
    self.keys.get()
  }

  /// 触发就绪唤醒（广播唤醒全部监听者，对标 TaskCompletionSource.SetResult）
  #[inline]
  fn notify_done(&self) {
    self.event.notify(usize::MAX);
  }

  /// 当前状态
  #[inline]
  pub fn status(&self) -> ObserverStatus {
    self.state.lock().status
  }

  /// 当前结果（克隆语义；C# 侧为引用读取）
  #[inline]
  pub fn result(&self) -> CollectionItemResult {
    self.state.lock().result.clone()
  }

  /// 等待结果就绪（对应 ResultFoundSemaphore.WaitAsync(0,1)；超时调度由会话层承担）
  pub async fn wait_result(&self) {
    while self.status() == ObserverStatus::WaitingForResult {
      let listener = self.event.listen();
      if self.status() != ObserverStatus::WaitingForResult {
        break;
      }
      listener.await;
    }
  }

  /// 持状态锁原子试取指派：闭包仅在临界区内、且确认仍 WaitingForResult 时
  /// 执行；命中则同临界区落结果并置 ResultSet，随后唤醒等待者
  ///
  /// 出件必须在锁内完成：锁外判状态、锁外弹出、事后补设结果的分离写法与
  /// 超时（handle_set_result）/ 会话销毁（handle_session_disposed）竞态时，
  /// 已物理弹出的元素会因状态已变被静默丢弃，集合数据永久丢失。本接口把
  /// 状态校验与出件并成一个临界区，二者互斥后要么结果落袋、要么元素留集。
  ///
  /// 返回 None 表示观察者已终结、闭包未被执行（未弹出任何元素）
  ///
  /// libs/server/Objects/ItemBroker/CollectionItemBroker.cs 的 TryAssignItemFromKey
  /// 的 ObserverStatusLock 写锁临界区
  pub fn try_assign_with<F>(&self, f: F) -> Option<TryGetOutcome>
  where
    F: FnOnce() -> TryGetOutcome,
  {
    let mut state = self.state.lock();
    if state.status != ObserverStatus::WaitingForResult {
      return None;
    }

    let outcome = f();
    let assigned = match &outcome.result {
      Some(result) => {
        state.result = result.clone();
        state.status = ObserverStatus::ResultSet;
        true
      }
      None => false,
    };
    drop(state);

    if assigned {
      self.notify_done();
    }
    Some(outcome)
  }

  /// 安全设置结果：仅当仍处 WaitingForResult 时生效并唤醒等待者
  ///
  /// libs/server/Objects/ItemBroker/CollectionItemObserver.cs:HandleSetResult
  pub fn handle_set_result(&self, result: CollectionItemResult) {
    {
      let mut state = self.state.lock();
      if state.status != ObserverStatus::WaitingForResult {
        return;
      }
      state.result = result;
      state.status = ObserverStatus::ResultSet;
    }
    self.notify_done();
  }

  /// 强制解除阻塞（UNBLOCK 语义）；返回是否生效
  ///
  /// libs/server/Objects/ItemBroker/CollectionItemObserver.cs:TryForceUnblock
  pub fn try_force_unblock(&self, throw_error: bool) -> bool {
    {
      let mut state = self.state.lock();
      if state.status != ObserverStatus::WaitingForResult {
        return false;
      }
      state.result = if throw_error {
        CollectionItemResult::force_unblocked()
      } else {
        CollectionItemResult::empty()
      };
      state.status = ObserverStatus::ResultSet;
    }
    self.notify_done();
    true
  }

  /// 调用会话已销毁：状态置 SessionDisposed 并取消等待
  ///
  /// libs/server/Objects/ItemBroker/CollectionItemObserver.cs:HandleSessionDisposed
  pub fn handle_session_disposed(&self) {
    {
      let mut state = self.state.lock();
      state.status = ObserverStatus::SessionDisposed;
    }
    self.notify_done();
  }
}

/// 集合取件结果
///
/// libs/server/Objects/ItemBroker/CollectionItemResult.cs:CollectionItemResult
#[derive(Debug, Clone, Default, PartialEq)]
pub struct CollectionItemResult {
  /// 取到项的集合键（None 表示未取到）
  pub key: Option<Vec<u8>>,
  /// 单项结果
  pub item: Option<Vec<u8>>,
  /// 单项分值（BZPOPMIN/MAX）
  pub score: Option<f64>,
  /// 多项结果（ZMPOP/LMPOP）
  pub items: Option<Vec<Vec<u8>>>,
  /// 多项分值
  pub scores: Option<Vec<f64>>,
  /// 被强制解除阻塞
  pub is_force_unblocked: bool,
  /// 源对象类型不匹配
  pub is_type_mismatch: bool,
}

impl CollectionItemResult {
  /// 空结果实例
  ///
  /// libs/server/Objects/ItemBroker/CollectionItemResult.cs:Empty
  pub fn empty() -> Self {
    Self::default()
  }

  /// 单项结果
  pub fn single(key: Vec<u8>, item: Vec<u8>) -> Self {
    Self {
      key: Some(key),
      item: Some(item),
      ..Self::default()
    }
  }

  /// 单项带分值结果
  pub fn single_with_score(key: Vec<u8>, score: f64, item: Vec<u8>) -> Self {
    Self {
      key: Some(key),
      item: Some(item),
      score: Some(score),
      ..Self::default()
    }
  }

  /// 多项结果
  pub fn multiple(key: Vec<u8>, items: Vec<Vec<u8>>) -> Self {
    Self {
      key: Some(key),
      items: Some(items),
      ..Self::default()
    }
  }

  /// 多项带分值结果
  pub fn multiple_with_scores(key: Vec<u8>, scores: Vec<f64>, items: Vec<Vec<u8>>) -> Self {
    Self {
      key: Some(key),
      scores: Some(scores),
      items: Some(items),
      ..Self::default()
    }
  }

  /// 是否取到项
  ///
  /// libs/server/Objects/ItemBroker/CollectionItemResult.cs:Found
  #[inline]
  pub fn found(&self) -> bool {
    self.key.is_some()
  }

  /// ForceUnblocked 实例
  ///
  /// libs/server/Objects/ItemBroker/CollectionItemResult.cs:ForceUnblocked
  pub fn force_unblocked() -> Self {
    Self {
      is_force_unblocked: true,
      ..Self::default()
    }
  }

  /// TypeMismatch 实例
  ///
  /// libs/server/Objects/ItemBroker/CollectionItemResult.cs:TypeMismatch
  pub fn type_mismatch() -> Self {
    Self {
      is_type_mismatch: true,
      ..Self::default()
    }
  }
}

#[cfg(test)]
mod tests {
  use wbase::future::yield_now;

  use super::*;

  #[test]
  fn set_result_once_only() {
    let obs = CollectionItemObserver::new(7, RespCommand::Blpop, vec![], (0, 0));
    assert_eq!(obs.status(), ObserverStatus::WaitingForResult);

    obs.handle_set_result(CollectionItemResult::single(b"k".to_vec(), b"v".to_vec()));
    assert_eq!(obs.status(), ObserverStatus::ResultSet);
    assert!(obs.result().found());

    // 二次设置被忽略
    obs.handle_set_result(CollectionItemResult::empty());
    assert!(obs.result().found());
  }

  #[test]
  fn force_unblock_and_dispose() {
    let obs = CollectionItemObserver::new(1, RespCommand::Bzpopmin, vec![], (0, 0));
    assert!(obs.try_force_unblock(false)); // 空结果解除
    assert!(!obs.try_force_unblock(false)); // 已终结，二次无效
    assert!(!obs.result().found());

    let obs2 = CollectionItemObserver::new(2, RespCommand::Blpop, vec![], (0, 0));
    assert!(obs2.try_force_unblock(true));
    assert!(obs2.result().is_force_unblocked);

    let obs3 = CollectionItemObserver::new(3, RespCommand::Blpop, vec![], (0, 0));
    obs3.handle_session_disposed();
    assert_eq!(obs3.status(), ObserverStatus::SessionDisposed);
    // 销毁后不可再设置结果
    obs3.handle_set_result(CollectionItemResult::single(b"k".to_vec(), b"v".to_vec()));
    assert!(!obs3.result().found());
  }

  #[test]
  fn result_shapes() {
    let multi = CollectionItemResult::multiple_with_scores(
      b"z".to_vec(),
      vec![1.0, 2.0],
      vec![b"x".to_vec(), b"y".to_vec()],
    );
    assert!(multi.found());
    assert_eq!(multi.scores.as_ref().unwrap().len(), 2);

    assert!(!CollectionItemResult::default().found());
    assert!(CollectionItemResult::type_mismatch().is_type_mismatch);
  }

  #[test]
  fn wait_result_flow() {
    use std::{sync::Arc, time::Duration};

    use compio::{
      runtime::{Runtime, spawn},
      time::timeout,
    };

    let obs = Arc::new(CollectionItemObserver::new(
      4,
      RespCommand::Blpop,
      vec![],
      (0, 0),
    ));
    let obs_clone = obs.clone();

    Runtime::new().unwrap().block_on(async {
      timeout(Duration::from_secs(5), async {
        let handle = spawn(async move {
          yield_now().await;
          obs_clone.handle_set_result(CollectionItemResult::single(
            b"key".to_vec(),
            b"val".to_vec(),
          ));
        });

        obs.wait_result().await;
        // 再次 wait 应立即返回（幂等）
        obs.wait_result().await;
        handle.await.unwrap();
      })
      .await
      .expect("wait_result_flow should not timeout");
    });

    assert_eq!(obs.status(), ObserverStatus::ResultSet);
    assert!(obs.result().found());
  }
}
