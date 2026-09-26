//! 集合项经纪承载面（会话 ← 经纪连接桥）
//!
//! C# 会话经 `storeWrapper.itemBroker` 直取具体 CollectionItemBroker
//! （具体类，无接口抽象）；Rust 同构直连——[`SharedItemBroker`] 内持
//! 具体经纪 Arc（start_wait 需 Arc 启动主循环），装配处注入会话。
//!
//! 等待驱动模型（对照 C#）：C# 网络线程在 ListBlockingPop 内
//! `AsyncUtils.BlockingWait(GetCollectionItemAsync(...))` 阻塞专线网络线程；
//! Rust 网络泵为 compio 线程池任务，同步阻塞会拖死同核全部连接，故拆为：
//! 命令层 `start_wait` 登记观察者 → 会话挂起（pending_block）→ 网络泵
//! await [`BlockedWait::resolve`]（compio 挂起不占线程）→ 超时竞速 →
//! `finish_wait` 收尾 → 会话写回应答后继续消费流水线。

use std::{sync::Arc, time::Duration};

use compio::time::timeout;
use wresp::command::RespCommand;

use super::{
  collection_item_broker::{
    CollectionItemBroker, CollectionItemStore, CompioTaskSpawner, TaskSpawner,
  },
  collection_item_observer::{CollectionItemObserver, CollectionItemResult},
};

/// 共享经纪句柄：服务器级共享持有形态（内持经纪 Arc，start_wait 经其
/// 启动主循环）
///
/// libs/server/StoreWrapper.cs:itemBroker（服务器级单例持有形态）
pub struct SharedItemBroker<S, Spawner = CompioTaskSpawner> {
  /// 具体经纪（主循环启动需 Arc）
  inner: Arc<CollectionItemBroker<S, Spawner>>,
}

impl<S: CollectionItemStore + 'static> SharedItemBroker<S> {
  /// 以默认 compio 启动器构造共享经纪句柄
  pub fn new(broker: Arc<CollectionItemBroker<S>>) -> Self {
    Self { inner: broker }
  }
}

impl<S: CollectionItemStore + 'static, Spawner: TaskSpawner + 'static>
  SharedItemBroker<S, Spawner>
{
  /// 登记观察者并入队 NewObserver 事件（等待由调用方经
  /// [`CollectionItemObserver::wait_result`] 驱动；`domain` 为发起会话域
  /// (ns, db)，观察键折叠与取件域同源）
  #[inline]
  pub fn start_wait(
    &self,
    command: RespCommand,
    keys: Vec<Vec<u8>>,
    session_id: usize,
    cmd_args: Vec<Vec<u8>>,
    domain: (u64, u64),
  ) -> Arc<CollectionItemObserver> {
    self
      .inner
      .start_wait(command, keys, session_id, cmd_args, domain)
  }

  /// 等待结束收尾：摘除会话映射，仍在等待则置空结果（超时/销毁路径）
  #[inline]
  pub fn finish_wait(&self, observer: &Arc<CollectionItemObserver>) -> CollectionItemResult {
    self.inner.finish_wait(observer)
  }

  /// 集合更新通知（存储层写后调用；`domain` 为写会话域 (ns, db)）
  #[inline]
  pub fn handle_collection_update(&self, domain: (u64, u64), key: &[u8]) {
    self.inner.handle_collection_update(domain, key);
  }

  /// 会话销毁通知
  #[inline]
  pub fn handle_session_disposed(&self, session_id: usize) {
    self.inner.handle_session_disposed(session_id);
  }

  /// 尝试获取会话对应的观察者（CLIENT UNBLOCK 用）
  #[inline]
  pub fn try_get_observer(&self, session_id: usize) -> Option<Arc<CollectionItemObserver>> {
    self.inner.try_get_observer(session_id)
  }

  /// 停机收口转发（C# StoreWrapper.Dispose 的 `itemBroker?.Dispose()`）：
  /// 置取消位、解除全部等待观察者（空应答）、投事件唤醒主循环退出
  #[inline]
  pub fn dispose(&self) {
    self.inner.dispose()
  }

  /// 经纪是否已释放（停机链收口断言面）
  #[inline]
  pub fn is_disposed(&self) -> bool {
    self.inner.is_disposed()
  }

  /// 等待主循环退出（嵌入式宿主显式排空用；服务器停机链经 worker join
  /// 天然排空，不经此口）
  #[inline]
  pub async fn wait_done(&self) {
    self.inner.wait_done().await
  }
}

/// 经纪等待面抽象（剥离取件源泛型，供 BlockedWait 无泛型持有与慢路径
/// 执行域的类型擦除注入：慢路径阻塞闭环在存储执行域内联等待——C#
/// BlockingWait 的 compio 投影——无会话可达面，经本 trait 对象驱动）
pub trait ItemBrokerFinisher: Send + Sync {
  /// 登记观察者并入队 NewObserver 事件（语义同
  /// [`SharedItemBroker::start_wait`]）
  fn start_wait(
    &self,
    command: RespCommand,
    keys: Vec<Vec<u8>>,
    session_id: usize,
    cmd_args: Vec<Vec<u8>>,
    domain: (u64, u64),
  ) -> Arc<CollectionItemObserver>;
  fn finish_wait(&self, observer: &Arc<CollectionItemObserver>) -> CollectionItemResult;
  fn handle_session_disposed(&self, session_id: usize);
}

impl<S: CollectionItemStore + 'static, Spawner: TaskSpawner + 'static> ItemBrokerFinisher
  for SharedItemBroker<S, Spawner>
{
  fn start_wait(
    &self,
    command: RespCommand,
    keys: Vec<Vec<u8>>,
    session_id: usize,
    cmd_args: Vec<Vec<u8>>,
    domain: (u64, u64),
  ) -> Arc<CollectionItemObserver> {
    self.start_wait(command, keys, session_id, cmd_args, domain)
  }

  fn finish_wait(&self, observer: &Arc<CollectionItemObserver>) -> CollectionItemResult {
    self.finish_wait(observer)
  }

  fn handle_session_disposed(&self, session_id: usize) {
    self.handle_session_disposed(session_id);
  }
}

impl<T: ItemBrokerFinisher + ?Sized> ItemBrokerFinisher for Arc<T> {
  fn start_wait(
    &self,
    command: RespCommand,
    keys: Vec<Vec<u8>>,
    session_id: usize,
    cmd_args: Vec<Vec<u8>>,
    domain: (u64, u64),
  ) -> Arc<CollectionItemObserver> {
    (**self).start_wait(command, keys, session_id, cmd_args, domain)
  }

  fn finish_wait(&self, observer: &Arc<CollectionItemObserver>) -> CollectionItemResult {
    (**self).finish_wait(observer)
  }

  fn handle_session_disposed(&self, session_id: usize) {
    (**self).handle_session_disposed(session_id);
  }
}

/// 阻塞命令挂起体：网络泵持有并 await，竞速超时后经纪收尾
///
/// 对照 C# `AsyncUtils.BlockingWait(GetCollectionItemAsync(command, keys,
/// session, timeout, cmdArgs))`——同一等待语义的 compio 挂起形态
///（网络线程不阻塞，挂起期间同核连接继续调度）
pub struct BlockedWait<B: ItemBrokerFinisher> {
  /// 所属经纪（finish_wait 收尾用）
  broker: B,
  /// 等待中的观察者
  observer: Arc<CollectionItemObserver>,
  /// 阻塞命令（应答格式化用，C# 各阻塞命令尾部 switch）
  command: RespCommand,
  /// 超时秒数（0 = 无限等待，C# TimeSpan.FromMilliseconds(-1)）
  timeout_secs: f64,
  /// 是否已闭环（出件/超时已走 finish_wait；未闭环 drop 触发 abort 注销）
  resolved: bool,
}

impl<B: ItemBrokerFinisher> BlockedWait<B> {
  /// 构造挂起体
  pub fn new(
    broker: B,
    observer: Arc<CollectionItemObserver>,
    command: RespCommand,
    timeout_secs: f64,
  ) -> Self {
    Self {
      broker,
      observer,
      command,
      timeout_secs,
      resolved: false,
    }
  }

  /// 阻塞命令类型（应答格式化用）
  #[inline]
  pub fn command(&self) -> RespCommand {
    self.command
  }

  /// 会话销毁解除（C# broker.HandleSessionDisposed）：置观察者
  /// SessionDisposed 并唤醒等待方；网络泵的 resolve 随后自然收尾
  pub fn abort(&self) {
    self
      .broker
      .handle_session_disposed(self.observer.session_id);
  }

  /// 驱动等待至完成：观察者结果就绪 / 超时 / 会话销毁，随后经纪收尾，
  /// 返回 (命令, 最终结果) 供会话写回应答
  ///
  /// 对照 C# 网络线程 BlockingWait 的整段等待语义（含超时与销毁解除）
  pub async fn resolve(&mut self) -> (RespCommand, CollectionItemResult) {
    if self.timeout_secs <= 0.0 {
      // 0 = 无限等待（C# TimeSpan.FromMilliseconds(-1)）
      self.observer.wait_result().await;
    } else {
      let _ = timeout(
        Duration::from_secs_f64(self.timeout_secs),
        self.observer.wait_result(),
      )
      .await;
    }
    self.resolved = true;
    let res = self.broker.finish_wait(&self.observer);
    (self.command, res)
  }
}

impl<B: ItemBrokerFinisher> Drop for BlockedWait<B> {
  fn drop(&mut self) {
    if !self.resolved {
      self.abort();
    }
  }
}
