//! 阻塞命令观察者（对标 libs/server/Objects/ItemBroker/CollectionItemObserver.cs）
//!
//! C# 的 `SemaphoreSlim(0,1)` + `CancellationTokenSource` 在 Rust 侧收敛为
//! `async_lock::Notify`（单次唤醒）；状态与结果由 `parking_lot::Mutex`
//! 一并保护（C# 的 SingleWriterMultiReaderLock 双检在此基础上仍保留语义）。

use std::future::Future;
use std::sync::atomic::{AtomicBool, Ordering};
use std::task::{Context, Poll, Waker};

use parking_lot::Mutex;

use crate::types::RespCommand;

/// 观察者状态
///
/// libs/server/Objects/ItemBroker/CollectionItemObserver.cs:ObserverStatus
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ObserverStatus {
  /// 等待结果
  WaitingForResult,
  /// 结果已设置
  ResultSet,
  /// 调用会话已销毁
  SessionDisposed,
}

/// 观察者内部可变状态（状态 + 结果同锁）
#[derive(Debug)]
struct ObserverState {
  status: ObserverStatus,
  result: CollectionItemResult,
}

/// 极简单次唤醒原语（notify 先到不丢；等价 async_lock::Notify 的可轮询形态，
/// 规避 async-lock 3.4 内部 event-listener 在同线程 poll 交织下的自锁）
#[derive(Debug, Default)]
pub(crate) struct Wakeup {
  fired: AtomicBool,
  waiters: Mutex<Vec<Waker>>,
}

impl Wakeup {
  #[inline]
  pub fn new() -> Self {
    Self::default()
  }

  /// 唤醒一个等待者（无等待者则置位，后续等待直接通过）
  pub fn notify_one(&self) {
    self.fired.store(true, Ordering::SeqCst);
    if let Some(w) = self.waiters.lock().pop() {
      w.wake();
    }
  }

  /// 可轮询等待：通知已到立即 Ready，否则登记 waker
  pub fn poll_wait(&self, cx: &mut Context<'_>) -> Poll<()> {
    if self.fired.swap(false, Ordering::SeqCst) {
      return Poll::Ready(());
    }
    let mut waiters = self.waiters.lock();
    // 双检：登记间隙到来的通知
    if self.fired.load(Ordering::SeqCst) {
      return Poll::Ready(());
    }
    if !waiters.iter().any(|w| w.will_wake(cx.waker())) {
      waiters.push(cx.waker().clone());
    }
    Poll::Pending
  }
}

/// [`Wakeup`] 的可等待 future
pub(crate) struct WakeupFuture<'a> {
  wakeup: &'a Wakeup,
}

impl Future for WakeupFuture<'_> {
  type Output = ();

  fn poll(self: std::pin::Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
    self.wakeup.poll_wait(cx)
  }
}

impl Wakeup {
  /// 可 await 的等待形态
  pub fn wait(&self) -> WakeupFuture<'_> {
    WakeupFuture { wakeup: self }
  }
}

/// 阻塞命令观察者
///
/// libs/server/Objects/ItemBroker/CollectionItemObserver.cs:CollectionItemObserver
#[derive(Debug)]
pub struct CollectionItemObserver {
  /// 发起阻塞命令的会话 ID（C# 为 RespServerSession 引用 + ObjectStoreSessionID）
  pub session_id: usize,
  /// 阻塞命令类型
  pub command: RespCommand,
  /// 命令附加参数（BLMOVE 目标键/方向、BZMPOP 的 count 等）
  pub command_args: Vec<Vec<u8>>,
  state: Mutex<ObserverState>,
  /// 结果就绪通知（对应 ResultFoundSemaphore(0,1)）
  result_found: Wakeup,
}

impl CollectionItemObserver {
  /// 新建观察者（初始状态 WaitingForResult，结果为 Empty）
  pub fn new(session_id: usize, command: RespCommand, command_args: Vec<Vec<u8>>) -> Self {
    Self {
      session_id,
      command,
      command_args,
      state: Mutex::new(ObserverState {
        status: ObserverStatus::WaitingForResult,
        result: CollectionItemResult::empty(),
      }),
      result_found: Wakeup::new(),
    }
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
    self.result_found.wait().await;
  }

  /// 可轮询形态（供无运行时的确定性驱动）
  pub fn poll_wait(&self, cx: &mut Context<'_>) -> Poll<()> {
    self.result_found.poll_wait(cx)
  }

  /// 安全设置结果：仅当仍处 WaitingForResult 时生效并唤醒等待者
  ///
  /// libs/server/Objects/ItemBroker/CollectionItemObserver.cs:HandleSetResult
  pub fn handle_set_result(&self, result: CollectionItemResult) {
    let mut state = self.state.lock();
    if state.status != ObserverStatus::WaitingForResult {
      return;
    }
    state.result = result;
    state.status = ObserverStatus::ResultSet;
    drop(state);
    self.result_found.notify_one();
  }

  /// 强制解除阻塞（UNBLOCK 语义）；返回是否生效
  ///
  /// libs/server/Objects/ItemBroker/CollectionItemObserver.cs:TryForceUnblock
  pub fn try_force_unblock(&self, throw_error: bool) -> bool {
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
    drop(state);
    self.result_found.notify_one();
    true
  }

  /// 调用会话已销毁：状态置 SessionDisposed 并取消等待
  ///
  /// libs/server/Objects/ItemBroker/CollectionItemObserver.cs:HandleSessionDisposed
  pub fn handle_session_disposed(&self) {
    let mut state = self.state.lock();
    state.status = ObserverStatus::SessionDisposed;
    drop(state);
    self.result_found.notify_one();
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

  /// 常量实例（供初始化复用）
  pub const EMPTY: CollectionItemResult = CollectionItemResult {
    key: None,
    item: None,
    score: None,
    items: None,
    scores: None,
    is_force_unblocked: false,
    is_type_mismatch: false,
  };

  pub const FORCE_UNBLOCKED: CollectionItemResult = CollectionItemResult {
    is_force_unblocked: true,
    ..CollectionItemResult::EMPTY
  };

  pub const TYPE_MISMATCH: CollectionItemResult = CollectionItemResult {
    is_type_mismatch: true,
    ..CollectionItemResult::EMPTY
  };
}

#[cfg(test)]
mod tests {
  use super::*;

  #[test]
  fn set_result_once_only() {
    let obs = CollectionItemObserver::new(7, RespCommand::Blpop, vec![]);
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
    let obs = CollectionItemObserver::new(1, RespCommand::Bzpopmin, vec![]);
    assert!(obs.try_force_unblock(false)); // 空结果解除
    assert!(!obs.try_force_unblock(false)); // 已终结，二次无效
    assert!(!obs.result().found());

    let obs2 = CollectionItemObserver::new(2, RespCommand::Blpop, vec![]);
    assert!(obs2.try_force_unblock(true));
    assert!(obs2.result().is_force_unblocked);

    let obs3 = CollectionItemObserver::new(3, RespCommand::Blpop, vec![]);
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

    assert!(!CollectionItemResult::EMPTY.found());
    assert!(CollectionItemResult::TYPE_MISMATCH.is_type_mismatch);
    assert!(!CollectionItemResult::FORCE_UNBLOCKED.found());
  }
}
