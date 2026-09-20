//! 阻塞命令观察者（对标 libs/server/Objects/ItemBroker/CollectionItemObserver.cs）
//!
//! 1:1 对标微软 Garnet 原版的 `TaskCompletionSource` / `SemaphoreSlim(0,1)`，
//! 使用 `event_listener::Event` 实现轻量异步事件通知，消灭通道与堆分配；
//! 内部状态与结果由单一 `parking_lot::Mutex` 保护。

use std::fmt;

use event_listener::Event;
use parking_lot::Mutex;
use wresp::command::RespCommand;

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
      .field("state", &self.state)
      .finish()
  }
}

impl CollectionItemObserver {
  /// 新建观察者（初始状态 WaitingForResult，结果为 Empty）
  pub fn new(session_id: usize, command: RespCommand, command_args: Vec<Vec<u8>>) -> Self {
    Self {
      session_id,
      command,
      command_args,
      state: Mutex::new(ObserverState::default()),
      event: Event::new(),
    }
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

    let obs = Arc::new(CollectionItemObserver::new(4, RespCommand::Blpop, vec![]));
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
