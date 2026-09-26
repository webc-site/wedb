//! 集合项经纪（对标 libs/server/Objects/ItemBroker/CollectionItemBroker.cs）
//!
//! 阻塞命令（BLPOP/BRPOP/BLMOVE/BLMPOP/BZPOPMIN/BZPOPMAX/BZMPOP）的取件仲裁：
//! - 会话发起阻塞等待 → 注册观察者并入队 NewObserver 事件；
//! - 存储层集合更新 → 入队 CollectionUpdated 事件；
//! - 主循环消费事件：新观察者立即试取一次，未果则挂到键的观察者队列；
//!   集合更新时把可用项指派给队首观察者。
//!
//! 刻意差异（对照 C#）：
//! - C# 经观察者自身会话的 storageSession 事务直取对象
//!   （CollectionItemBroker.cs:269/:337 `TryGetResult(key,
//!   observer.Session.storageSession, ...)`，取件域随观察者）；Rust 以
//!   [`CollectionItemStore`] trait 注入取件源（由经纪单例存储会话域适配），
//!   观察者域 (ns, db) 随事件传递、取件前在单例会话上切换——主循环单消费者
//!   串行换域无竞态，语义与 C# 取件随观察者会话同构；
//! - C# 无 namespace 维度，keysToObservers 裸键收发（CollectionItemBroker.cs:38）；
//!   wedb 多租户下进程级共享观察表以 [`NsPrefix`] 域折叠键隔离（注册键/
//!   唤醒键/BLMOVE 目标唤醒键同径折叠，裸键只在取件域内出现），对接 pubsub
//!   通道域既有先例；
//! - C# 的 Task.Run 以 compio 宿主注入的 [`TaskSpawner`] 等价实现；
//! - 等待超时的计时调度由会话层承担，观察者仅暴露可取消的完成通知。
//!
//! 自研依据: 条目事件 broker（C# 无对应组件）

use std::{
  collections::{HashSet, VecDeque},
  sync::{
    Arc, Weak,
    atomic::{AtomicBool, AtomicI32, AtomicU64, Ordering},
  },
};

use arc_swap::ArcSwap;
use compio::runtime::spawn;
use crossfire::{
  AsyncRx, MTx,
  mpsc::{List, unbounded_async},
  oneshot::{RxOneshot as OneshotAsyncRx, TxOneshot as OneshotTx, oneshot},
};
use parking_lot::Mutex;
use wbase::{
  future::yield_now,
  map::{ConcurrentMap, new_concurrent_map},
  ns_prefix::NsPrefix,
  supervise::{supervise_item, supervise_task},
  time::now_secs,
};

/// 监督快照里的任务名（wbase::supervise 归组键，INFO bg_task_health 可见）
const BROKER_MAIN_TASK: &str = "broker_main";
/// 逐项监督归组名（事件分派处理体）
const BROKER_EVENT_ITEM: &str = "broker_event_item";

/// 主循环 panic 臂有界重挂上限：连败后留复位态（NOT_STARTED）与监督快照 panic
/// 计数退出，交后续首个 start_wait（新阻塞命令）重新拉起——绝不回到「CAS 位永真
/// 拒一切重拉」的死亡形态（对标 wkv gc/reclaim.rs `REMOUNT_LIMIT` 先例；
/// 物理回收与阻塞出件同属正确性面）
const REMOUNT_LIMIT: u32 = 3;
/// 主循环 panic 臂重挂让核窗口（协程让渡轮数）：复位后先让核若干轮再 CAS 重新认领，
/// 窗内别处 start_wait 已重拉则本环 CAS 让位（对标 reclaim.rs `REMOUNT_BACKOFF_POLLS`
/// 乘数形态）。取 `yield_now` 而非计时 `sleep`：compio `sleep` future 非 `Send`，与
/// [`TaskSpawner::spawn`] 的 `Send` 未来约束相冲（ThreadSpawner 需跨 OS 线程搬运任务
/// 体），且票面纪律明令重拉机制走协程让渡、绝不内联驱动调度器——`yield_now` 让出本核
/// 交同核就绪任务（含并发的客户端 start_wait）推进，正是让核窗口的语义本体
const REMOUNT_BACKOFF_YIELDS: u32 = 5;

/// 键观察者队列（按订阅顺序；对应 C# ConcurrentQueue<CollectionItemObserver>，
/// libs/server/Objects/ItemBroker/CollectionItemBroker.cs:38）。外层
/// keysToObserversLock（SingleWriterMultiReaderLock，:47，:255 注释明言其契约
/// "This lock is for synchronization with incoming collection updated events"）
/// 在本 rust 形态由本 Mutex 升格承接为登记协议锁：initialize_observer 的
/// 「判空→试取→挂队」与 handle_collection_update 的「判空→发事件」在同一把
/// 键锁上互斥，丢失唤醒窗于单键临界区闭合；键间定序由 papaya 无锁分片与
/// 主循环单线程侧承担（挂队/出件/摘键皆主循环单侧，通知侧持单键锁不再取
/// 第二把，跨键环不可构造，无需多键取锁定序）
type ObserverQueue = Mutex<VecDeque<Arc<CollectionItemObserver>>>;
/// 观察键 → 观察者队列映射（键为 [`NsPrefix`] 域折叠键；gxhash 构建器，
/// 全项目并发容器唯一出处约束）
type KeysToObservers = ConcurrentMap<Vec<u8>, ObserverQueue>;
use wresp::command::RespCommand;

use crate::{
  itembroker::{
    collection_item_broker_event::{CollectionItemBrokerEvent, CollectionItemBrokerEventType},
    collection_item_observer::{CollectionItemObserver, CollectionItemResult, ObserverStatus},
  },
  list::list_object::{ListObject, OperationDirection},
  zset::sorted_set_object::SortedSetObject,
};

/// keysToObservers 两次清理的最小间隔
///
/// libs/server/Objects/ItemBroker/CollectionItemBroker.cs:MIN_SECS_BETWEEN_KEYS_TO_OBSERVERS_CLEANS
const MIN_SECS_BETWEEN_KEYS_TO_OBSERVERS_CLEANS: u64 = 5 * 60;

/// 主循环状态
const MAIN_LOOP_NOT_STARTED: i32 = 0;
const MAIN_LOOP_STARTED: i32 = 1;
const MAIN_LOOP_DISPOSED: i32 = 2;

/// 取件源试取结果（currCount 出参 + BLMOVE 目标键唤醒回传 + 同步不可出件位 +
/// 桶闩争用重投位）
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
  /// 键已离开经纪同步服务域（活跃分层键同步装载恒 `ObjLoad::Degrade`），
  /// 区别于键缺失/空集合的普通不可取：观察者继续挂队将永久饥饿（升阶后
  /// 每次更新事件重复同一不可取循环），经纪域须送客清队——客户端按 BLPOP
  /// 空回复重试，重试时 park 前预探（any_sync_degrade）命中分层键整体
  /// 路由慢路径异步臂，不再回经纪。C# 无对位（TryGetResult 经事务恒可
  /// 取件，无"同步不可出件"概念；wedb 分层存储的域边界面对位）
  pub is_degrade: bool,
  /// 取件窗桶闩争用（非键缺/空集的暂时性不可取）：外层事务（EXEC 重放期
  /// 与本经纪同键 scoped 桶闩互斥，票 wtxn-wkv-keybucket-hash-scope-desync）
  /// 或他读改写窗正持本键桶排他闩，稍后重试即可得手。C# 无对位
  ///（CollectionItemBroker.cs:585-598 `txnManager.state==Running` 时经
  /// `TransactionalContext` 并入外层事务直取，恒不遇闩；rust 取件源为经纪
  /// 独立会话不可并入他事务，以重投事件让核重试承接同一"非空键不悬挂"语义
  /// ——只挂队不重投则外层唯一写点已过、后续无写入时 timeout=0 永久悬挂）
  pub is_contended: bool,
}

impl TryGetOutcome {
  /// 不可取结果（curr_count 为 0、无通知键）
  pub fn none() -> Self {
    Self::default()
  }

  /// 同步不可出件结果（键已真升阶分层，见 [`TryGetOutcome::is_degrade`] 文档；
  /// 仅限取件源经 Meta 闸复判的真升阶——磁盘候选冷键是过渡态，须回
  /// [`Self::none`] 挂队等更新事件物化信封自愈，冷墓碑挂起等待（唤醒出件 /
  /// 超时空回）是阻塞族契约，报本位即「应挂起却立即空回」语义回归）
  pub fn degrade() -> Self {
    Self {
      is_degrade: true,
      ..Self::none()
    }
  }

  /// 取件窗桶闩争用结果（暂时性不可取，见 [`TryGetOutcome::is_contended`]；
  /// 经纪挂队后让核重投 CollectionUpdated 事件闭环，非空键不悬挂）
  pub fn contended() -> Self {
    Self {
      is_contended: true,
      ..Self::none()
    }
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
      result: Some(result),
      curr_count,
      ..Self::none()
    }
  }

  /// BLMOVE 目标键通知构造
  pub fn moved(curr_count: usize, result: CollectionItemResult, notify_key: Vec<u8>) -> Self {
    Self {
      result: Some(result),
      notify_key: Some(notify_key),
      curr_count,
      ..Self::none()
    }
  }
}

/// 取件源抽象：从 key 处的集合对象取出下一可用项
///
/// 对标 C# TryGetResult 内联的 storageSession.GET + 事务取件路径
///（C# storageSession 即观察者会话的存储执行域，rust 以 (ns, db) 域入参
/// 在经纪会话上等价切换）；result 为 None 表示源不可用（键缺失/类型不符
/// 未失败/集合为空/磁盘候选降级），curr_count 供"集合非空但观察者不匹配"
/// 的继续判定，notify_key 为 BLMOVE 搬入键（裸键，提交后经事件队列唤醒其
/// 观察者，折叠由经纪侧承担）。
pub trait CollectionItemStore: Send + Sync {
  /// 试取下一可用项（语义见 trait 文档）
  fn try_get_result(
    &self,
    ns: u64,
    db: u64,
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
    ns: u64,
    db: u64,
    key: &[u8],
    command: RespCommand,
    cmd_args: &[Vec<u8>],
    fail_on_src_type_mismatch: bool,
  ) -> TryGetOutcome {
    (**self).try_get_result(ns, db, key, command, cmd_args, fail_on_src_type_mismatch)
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
  /// 事件发送端（对应 Garnet AsyncQueue<CollectionItemBrokerEvent>，
  /// libs/server/Objects/ItemBroker/CollectionItemBroker.cs:31；C# AsyncQueue 为
  /// ConcurrentQueue + SemaphoreSlim（libs/storage/Tsavorite/cs/src/core/
  /// Utilities/AsyncQueue.cs:25/:27-28），crossfire mpsc::List 为其零锁等价物，
  /// 多生产者投递 + 单主循环 recv 消费，零锁并发投递）。
  ///
  /// `ArcSwap` 承接主循环 panic 重挂时的通道换装：panic 臂 `unbounded_async` 新建
  /// `(tx, rx)` 后原子 `store` 换装本字段（罕见控制面路径），`enqueue_event` 热路径
  /// 仅 `load()` 无锁读最新 tx（遵守数据面纪律——通知写入路径不加常驻同步锁，见票
  /// wcol-itembroker-main-loop-panic-dead-no-remount 第 2 条）
  events_tx: ArcSwap<MTx<List<CollectionItemBrokerEvent>>>,
  /// 事件接收端（单主循环消费，启动时 take 独占）
  events_rx: Mutex<Option<AsyncRx<List<CollectionItemBrokerEvent>>>>,

  /// 会话 ID → 观察者
  session_id_to_observer: ConcurrentMap<usize, Arc<CollectionItemObserver>>,

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

  /// dispose 完成通知发送端（单次非阻塞投递，对标 C# ManualResetEventSlim done，
  /// libs/server/Objects/ItemBroker/CollectionItemBroker.cs:53；oneshot 单次
  /// 语义较事件标志更精确且免 Reset）
  done_tx: Mutex<Option<OneshotTx<()>>>,
  /// dispose 完成通知接收端
  done_rx: Mutex<Option<OneshotAsyncRx<()>>>,

  /// 主循环启动器（构造期定格；C# 内建 Task.Run 的宿主注入形态）
  spawner: Arc<Spawner>,
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
      events_tx: ArcSwap::from_pointee(events_tx),
      events_rx: Mutex::new(Some(events_rx)),
      session_id_to_observer: new_concurrent_map(),
      keys_to_observers: new_concurrent_map(),
      keys_to_observers_time_last_clean: AtomicU64::new(now_secs()),
      store,
      main_loop_task_status: AtomicI32::new(MAIN_LOOP_NOT_STARTED),
      cts_cancelled: AtomicBool::new(false),
      done_tx: Mutex::new(Some(done_tx)),
      done_rx: Mutex::new(Some(done_rx)),
      spawner: Arc::new(spawner),
    }
  }

  /// 尝试获取会话对应的观察者
  ///
  /// libs/server/Objects/ItemBroker/CollectionItemBroker.cs:TryGetObserver
  pub fn try_get_observer(&self, session_id: usize) -> Option<Arc<CollectionItemObserver>> {
    self.session_id_to_observer.pin().get(&session_id).cloned()
  }

  /// 登记观察者并启动等待（C# GetCollectionItemAsync 的前半：登记映射 → 启动主循环 →
  /// NewObserver 事件入队），等待由调用方驱动；计时等待由会话层以 select
  /// 竞速 [`CollectionItemObserver::wait_result`] 实现
  /// （compio 挂起语义见 BlockedWait，C# 由网络线程 BlockingWait 承担）
  ///
  /// `domain` 为发起会话所属域 (ns, db)：观察者键折叠与取件域的单一真值源
  ///（C# 域随 observer.Session.storageSession 的等价承接）
  pub fn start_wait(
    self: &Arc<Self>,
    command: RespCommand,
    keys: Vec<Vec<u8>>,
    session_id: usize,
    cmd_args: Vec<Vec<u8>>,
    domain: (u64, u64),
  ) -> Arc<CollectionItemObserver> {
    let observer = Arc::new(CollectionItemObserver::new(
      session_id, command, cmd_args, domain,
    ));
    let prefix = observer.prefix;
    let keys: Vec<Vec<u8>> = keys.into_iter().map(|k| prefix.isolate(&k)).collect();
    // 一次性定格订阅键组：NewObserver 事件若随主循环 panic 滞留旧通道被丢弃，
    // 重挂补扫据此对未挂队观察者重投（见 rescan_stale_observers，票
    // wcol-itembroker-main-loop-panic-dead-no-remount 第 3 条）
    observer.set_keys(keys.clone());
    self.register_observer(observer.clone(), keys);

    // 首次使用时启动主循环
    self.start_main_loop();

    observer
  }

  /// 等待结束收尾（GetCollectionItemAsync 的后半）：摘除会话映射，
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

  /// 登记观察者：会话映射写入 + NewObserver 事件入队
  ///（start_wait 的公共前半）
  fn register_observer(&self, observer: Arc<CollectionItemObserver>, keys: Vec<Vec<u8>>) {
    self
      .session_id_to_observer
      .pin()
      .insert(observer.session_id, observer.clone());

    self.enqueue_event(CollectionItemBrokerEvent::create_new_observer_event(
      observer, keys,
    ));
  }

  /// 主循环启动（CAS 保证仅启动一次）；启动器构造期定格
  ///
  /// libs/server/Objects/ItemBroker/CollectionItemBroker.cs:StartMainLoop
  ///
  /// 外层监督（wbase [`supervise_task`] 单点，对标 wkv gc/reclaim.rs
  /// `spawn_bftree_reclaimer`）：主循环体 [`Self::start_async`] 若整体 panic（数据面
  /// 纪律违例等逐事件 [`supervise_item`] 未兜住的逃逸面），旧形态 `let _ =` 直接丢弃
  /// Err 载荷、`main_loop_task_status` 永卡 STARTED——后续任何 start_wait 的
  /// CAS(NOT_STARTED→STARTED) 恒失败，通道 rx 已被 take 且随 panic 丢弃、再无消费者，
  /// 无界事件队列只增不减、timeout=0 阻塞客户端永挂（票
  /// wcol-itembroker-main-loop-panic-dead-no-remount）。本改为 Err 臂进入有界重挂闭环
  /// [`Self::remount_main_loop`]：复位状态位 → 重建事件通道 → 存量补扫 → 重新 CAS 拉起，
  /// 连败达 [`REMOUNT_LIMIT`] 则留 NOT_STARTED 复位态与监督快照 panic 计数退出，交后续
  /// 首个 start_wait 重新认领（绝不回到「CAS 位永真拒一切重拉」的死亡形态）
  pub fn start_main_loop(self: &Arc<Self>) {
    if self.main_loop_task_status.load(Ordering::SeqCst) != MAIN_LOOP_NOT_STARTED
      || self
        .main_loop_task_status
        .compare_exchange(
          MAIN_LOOP_NOT_STARTED,
          MAIN_LOOP_STARTED,
          Ordering::SeqCst,
          Ordering::SeqCst,
        )
        .is_err()
    {
      return;
    }

    let broker = Arc::downgrade(self);
    self.spawner.spawn(async move {
      if supervise_task(BROKER_MAIN_TASK, run_main_loop(broker.clone()))
        .await
        .is_err()
      {
        remount_main_loop(broker, REMOUNT_LIMIT).await;
      }
    });
  }

  /// 集合更新通知（存储层写后调用；`domain` 为写会话所属域 (ns, db)，
  /// 裸键经域折叠后命中观察表）
  ///
  /// libs/server/Objects/ItemBroker/CollectionItemBroker.cs:HandleCollectionUpdate
  ///
  /// 锁内判空（对标 C# ReadLock :181 + IsEmpty :193 臂）与 initialize_observer
  /// 的「判空→试取→挂队」临界区在同一把键锁上互斥：此处读空/缺席早退时，
  /// 登记侧锁内试取必已晚于写提交（提交先于通知）而见该元素；读非空则事件
  /// 必达。缺席早退臂的定序依赖登记侧「先发布队列再取锁」（见
  /// initialize_observer）。本函数仅短取一把键锁、不再取第二把，与主循环
  /// 侧无锁序逆环。
  pub fn handle_collection_update(&self, domain: (u64, u64), key: &[u8]) {
    let folded = NsPrefix::new(domain.0).join(domain.1).isolate(key);
    let pin = self.keys_to_observers.pin();
    let Some(queue) = pin.get(&folded) else {
      return;
    };
    if queue.lock().is_empty() {
      return;
    }

    // CollectionUpdated 事件入队：仅在确认有等待观察者时才分配折叠键副本
    self.enqueue_event(CollectionItemBrokerEvent::create_collection_updated_event(
      folded,
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
  ///
  /// 返回 `Some(域折叠键)` = 试取窗桶闩争用（[`TryGetOutcome::is_contended`]），
  /// 主循环让核后按该键重投 CollectionUpdated 事件闭环重试（见
  /// [`Self::start_async`]；C# 无对位，其 TryGetResult 经外层事务直取恒不遇闩）
  pub fn handle_broker_event(&self, broker_event: CollectionItemBrokerEvent) -> Option<Vec<u8>> {
    match broker_event.event_type {
      CollectionItemBrokerEventType::NewObserver => {
        if let (Some(observer), Some(keys)) = (broker_event.observer, broker_event.keys) {
          return self.initialize_observer(observer, &keys);
        }
        None
      }
      CollectionItemBrokerEventType::CollectionUpdated => {
        if let Some(key) = broker_event.key {
          return self.try_assign_item_from_key(&key);
        }
        None
      }
      CollectionItemBrokerEventType::NotSet => None,
    }
  }

  /// 新观察者登记：逐键「发布队列→取锁→判空→锁内试取→未果同锁挂队」单
  /// 临界区完成（failOnSrcTypeMismatch=true）
  ///
  /// libs/server/Objects/ItemBroker/CollectionItemBroker.cs:InitializeObserver
  ///
  /// `keys` 为域折叠键，取件前经观察者前缀剥离还原裸键。
  ///
  /// 锁纪律（承接 C# keysToObserversLock 写锁 :256-294 与
  /// HandleCollectionUpdateWorker 读侧 :181-214 的互斥契约，C# :255 注释
  /// 即本契约本体）：试取与挂队必须和该键队列判空同处一个临界区，与
  /// handle_collection_update 的锁内判空互斥。存储侧提交先于通知
  /// （list_commands/write.rs 写成功后才 notify），于是通知落在本临界区
  /// 之前则读空/缺席早退，本侧锁内试取必已见其已提交元素；通知落在其后
  /// 则必见非空队列并发事件。「试取-挂队」两态分离的丢失唤醒窗（旧形态
  /// 下通知落于间隙即早退，元素滞留 store、观察者永悬至该键下一次写）于
  /// 单键锁闭合。纪律约束：锁内试取期间严禁同线程同步调用
  /// handle_collection_update（parking_lot 非重入）——BLMOVE 目标键唤醒
  /// 一律走 enqueue_event 事件队列臂；锁内试取对 store 键闩维持有界 try
  /// 语义（wnode collection_item_source::zset_outcome try_rmw_window，
  /// 失闩回 none 挂队等下一事件，绝不改阻塞——写侧持闩期 notify 可消耗
  /// 成假空取，属 zset 臂已知残留，本函数不扩大该面）；本函数全程同步无
  /// await。试取命中即不挂该键队并摘其空队列（C# :278 同形）；早先键在
  /// 未果臂已挂入的队列为已终结死节点，事件链触键时经 try_assign_with
  /// 拒绝弹出、周期清理 retain 兜底，自清无饥饿。
  ///
  /// 返回值语义见 [`Self::handle_broker_event`]（争用键照常挂队，另带回执
  /// 交主循环让核重投，票 wtxn-wkv-keybucket-hash-scope-desync）
  pub fn initialize_observer(
    &self,
    observer: Arc<CollectionItemObserver>,
    keys: &[Vec<u8>],
  ) -> Option<Vec<u8>> {
    let pin = self.keys_to_observers.pin();
    let mut contended_key: Option<Vec<u8>> = None;
    let (ns, db) = observer.domain;

    for key in keys {
      // 剥离域前缀还原裸键（同域折叠，剥离必中；非本域键防御性跳过，
      // 不发布不挂队）
      let Some(raw) = observer.prefix.strip(key) else {
        continue;
      };

      // 该键队列先发布再取锁（缺席早退臂定序论证的前提，见
      // handle_collection_update 头注）
      let queue = match pin.get(key) {
        Some(q) => q,
        None => pin.get_or_insert_with(key.clone(), || Mutex::new(VecDeque::new())),
      };
      let mut waiting = queue.lock();

      // 等价 C# :265 跳试取 + :289 挂队：键上已有他人等待（元素归属队首）
      // 则不试取，同一临界区内直接挂队续下一键
      if !waiting.is_empty() {
        waiting.push_back(observer.clone());
        continue;
      }

      // 持观察者状态锁试取（C# :269-275 状态临界区等价，§33 锁内原子出件
      // 契约）；None 表明观察者挂队前已超时或注销，元素未弹出
      let Some(outcome) = observer.try_assign_with(|| {
        self.try_get_result(ns, db, raw, observer.command, &observer.command_args, true)
      }) else {
        drop(waiting);
        pin.remove(key);
        return None;
      };
      if outcome.result.is_none() {
        if outcome.is_degrade {
          // 新观察者首试即命中同步不可出件（键已分层，park 前预探后的升阶
          // 或过渡窗竞态）：送空应答、回收会话映射、不挂队——继续挂队即
          // 入"park 后升阶"永久饥饿（此后每次更新事件试取恒 Degrade）；
          // 客户端空回复重试经预探整体路由慢路径闭环
          drop(waiting);
          pin.remove(key);
          observer.try_force_unblock(false);
          self
            .session_id_to_observer
            .pin()
            .remove(&observer.session_id);
          return None;
        }
        // 桶闩争用（非键缺/空集）：照常挂队，另带回执让主循环让核重投
        //（不重投则外层唯一写点已过、后续无写入时观察者永久悬挂）
        if outcome.is_contended && contended_key.is_none() {
          contended_key = Some(key.clone());
        }
        // 未果：与判空同一临界区内挂队后放锁（与通知侧互斥即窗闭），续试
        // 下一键
        waiting.push_back(observer.clone());
        continue;
      }

      // 取到项（结果已随试取原子落袋）：放锁摘该键队列（入本临界区时
      // 判空且本线程未挂队，挂队仅发生在主循环单侧，空队必摘）
      drop(waiting);
      pin.remove(key);

      self
        .session_id_to_observer
        .pin()
        .remove(&observer.session_id);

      // BLMOVE 搬入键提交后唤醒（裸键经观察者域折叠后入队；C# TryGetResult
      // finally 入队）
      if let Some(notify_key) = outcome.notify_key {
        self.enqueue_event(CollectionItemBrokerEvent::create_collection_updated_event(
          observer.prefix.isolate(&notify_key),
        ));
      }
      return None;
    }
    contended_key
  }

  /// 把键处的可用项指派给等待观察者（持续满足直到无法满足队首）
  ///
  /// libs/server/Objects/ItemBroker/CollectionItemBroker.cs:TryAssignItemFromKey
  ///
  /// `key` 为域折叠键；同队列观察者同域，取件前剥离还原裸键。返回值语义见
  /// [`Self::handle_broker_event`]：队首试取遇桶闩争用即停止（FIFO 保守不变）
  /// 并带回该键，交主循环让核重投
  fn try_assign_item_from_key(&self, key: &[u8]) -> Option<Vec<u8>> {
    let mut contended = false;
    let pin = self.keys_to_observers.pin();
    if let Some(queue) = pin.get(key) {
      let mut queue = queue.lock();
      while let Some(observer) = queue.front().cloned() {
        // 剥离域前缀还原裸键，取件域切换到观察者所属域（C# 取件随
        // observer.Session.storageSession 的等价承接）
        let Some(raw) = observer.prefix.strip(key) else {
          queue.pop_front();
          continue;
        };
        let (ns, db) = observer.domain;

        // 持观察者状态锁出件（C# :326-358 写锁临界区等价）：状态校验与弹出
        // 同一临界区，与超时/销毁路径互斥，弹出的元素必有所属
        let Some(outcome) = observer.try_assign_with(|| {
          self.try_get_result(ns, db, raw, observer.command, &observer.command_args, false)
        }) else {
          // 观察者已终结：未弹出元素，直接消费下一观察者
          queue.pop_front();
          continue;
        };
        if outcome.result.is_none() {
          if outcome.is_degrade {
            // 队首命中同步不可出件（键已分层）：对该键全队列 WaitingForResult
            // 观察者统一送空应答、清队摘键（try_force_unblock 对已终结观察者
            // 幂等无副作用，无需预判状态）——队首 break 的 FIFO 保守在此会
            // 退化为全队列永久饥饿（元素持续到达而无人可被服务），送客后
            // 客户端空回复重试经预探路由慢路径，经纪域不留分层键观察者
            for waiting in queue.iter() {
              waiting.try_force_unblock(false);
            }
            queue.clear();
            break;
          }
          if outcome.is_contended {
            // 队首遇取件窗桶闩争用：保持挂队，带回执交主循环让核重投
            //（持闩臂——外层 EXEC 提交收尾 / 他读改写窗微窗——让核推进后
            // 重试即可得手，非空键不悬挂）
            contended = true;
          }
          // 队首观察者无法满足时停止（FIFO 保证，不能跳过队首继续窥视）
          break;
        }

        // 元素已原子弹出且结果已落袋：出队并摘除会话映射
        queue.pop_front();
        self
          .session_id_to_observer
          .pin()
          .remove(&observer.session_id);

        // BLMOVE 搬入键提交后唤醒（裸键经观察者域折叠后入队；C# TryGetResult
        // finally 入队；此处直入队列而非 HandleCollectionUpdate，因调用方持有
        // keysToObservers 读锁）
        if let Some(notify_key) = outcome.notify_key {
          self.enqueue_event(CollectionItemBrokerEvent::create_collection_updated_event(
            observer.prefix.isolate(&notify_key),
          ));
        }
      }

      if queue.is_empty() {
        drop(queue);
        pin.remove(key);
      }
    }
    contended.then(|| key.to_vec())
  }

  /// 经取件源试取（currCount 出参随结果返回）
  ///
  /// libs/server/Objects/ItemBroker/CollectionItemBroker.cs:TryGetResult
  fn try_get_result(
    &self,
    ns: u64,
    db: u64,
    key: &[u8],
    command: RespCommand,
    cmd_args: &[Vec<u8>],
    fail_on_src_type_mismatch: bool,
  ) -> TryGetOutcome {
    // 类型不符即失败（BLMOVE 的 dst 校验在存储适配层完成）
    self
      .store
      .try_get_result(ns, db, key, command, cmd_args, fail_on_src_type_mismatch)
  }

  /// 清理 keysToObservers：弹出已终结观察者并回收空键
  ///
  /// libs/server/Objects/ItemBroker/CollectionItemBroker.cs:CleanKeysToObservers
  pub fn clean_keys_to_observers(&self) {
    let pin = self.keys_to_observers.pin();
    let mut empty_keys = Vec::new();
    for (key, queue) in pin.iter() {
      let mut queue = queue.lock();
      // 不照搬 C# 队首窥探（CleanKeysToObservers 受 ConcurrentQueue 无锁单向
      // 队列接口所限，只能队首 TryPeek/TryDequeue）：rust 侧 ObserverQueue 为
      // Mutex<VecDeque>，清理时已持队列排他锁、容器具备随机访问能力，队首探测
      // 属反模式——队首一旦是长等待的活跃观察者，循环即中止，排在其后的失效
      // 观察者（超时到期/多键已被满足置 ResultSet、断连置 SessionDisposed）
      // 被永久阻隔滞留，其持有的 command_args 与结果载荷无人回收，队列无界
      // 膨胀。retain 保留 FIFO 相对顺序，单次 O(N) 剔除队内全部死节点。
      queue.retain(|observer| observer.status() == ObserverStatus::WaitingForResult);
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
    self
      .keys_to_observers_time_last_clean
      .store(now_secs(), Ordering::Relaxed);
  }

  /// 观察键（域折叠形态）当前在表观察者数；None 表示键已从映射表摘除
  ///
  /// C# 无对位（keysToObservers 为私有字段，清理闭环由调试器目测）；本访问器
  /// 是 clean_keys_to_observers「空队列摘键」闭环对外可验收的唯一观测面
  pub fn waiting_observer_count(&self, folded_key: &[u8]) -> Option<usize> {
    self
      .keys_to_observers
      .pin()
      .get(folded_key)
      .map(|queue| queue.lock().len())
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

      // 逐事件 panic 守卫（wbase [`supervise_item`] 单点，对标 C# 处理链内层
      // 多处 try/catch + StartAsync 外层 finally done.Set()）：panic 臂弃事件
      // 继续，主循环永续——main_loop_task_status 语义不变，done 必发，
      // 阻塞族观察者不再因单事件 panic 永挂（timeout=0 无解除）
      if supervise_item(BROKER_EVENT_ITEM, async {
        // 争用重投（票 wtxn-wkv-keybucket-hash-scope-desync 补强注记；C# 无
        // 对位，其 TryGetResult 经外层事务直取恒不遇闩）：试取窗桶闩被外层
        // 事务（EXEC 重放期 scoped 同桶互斥）或他读改写窗持有时，观察者照常
        // 挂队，本循环让核一次再重投 CollectionUpdated 事件——持闩臂让核推进
        // （EXEC 提交收尾放闩 / 他窗微窗即放）后重试即得手，非空键不因
        // 「外层唯一写点已过、后续无写入」而 timeout=0 永久悬挂。让核重投
        // 与 rmw 域「让核重试预算」同款让步语义，每次重投必先让出本核，
        // 绝不占核空转饿死持闩者
        if let Some(retry_key) = self.handle_broker_event(next_event) {
          yield_now().await;
          self.enqueue_event(CollectionItemBrokerEvent::create_collection_updated_event(
            retry_key,
          ));
        }
      })
      .await
      .is_err()
      {
        // 日志已由监督单点落，弃事件进入下轮
      }

      // 观察表周期清理
      let now = now_secs();
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
      let _ = self.events_tx.load().try_send(CollectionItemBrokerEvent {
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
  ///
  /// 热路径经 `ArcSwap::load()` 无锁读最新发送端（罕见 panic 重挂才 `store` 换装，
  /// 见 [`Self::rebuild_event_channel`]）——遵守数据面纪律：通知写入路径不加常驻
  /// 同步锁（票 wcol-itembroker-main-loop-panic-dead-no-remount 第 2 条）
  fn enqueue_event(&self, event: CollectionItemBrokerEvent) {
    let _ = self.events_tx.load().try_send(event);
  }

  /// panic 重挂前重建事件通道：新建 `(tx, rx)` 并原子换装发送端、回填接收端
  ///
  /// 旧 `rx` 已随 [`Self::start_async`] 的 `take` + panic 丢弃、旧通道滞留事件无人
  /// 消费（换装即随旧 `tx`/`rx` 一并释放丢弃，见票第 2 条「有界堆积」——死通道不再
  /// 无界增长）。换装先于复位状态位（见 [`Self::recover_after_panic`]），令并发
  /// [`Self::start_main_loop`] 重拉必见新 `rx` 而非缺席早退。全程同步、parking_lot
  /// 锁不跨 await
  fn rebuild_event_channel(&self) {
    let (tx, rx) = unbounded_async();
    self.events_tx.store(Arc::new(tx));
    *self.events_rx.lock() = Some(rx);
  }

  /// 存量自愈补扫：重挂前把「已登记未处理」的库存事件重投进新通道（见票第 3 条）
  ///
  /// 两形态（均走 enqueue_event 入新通道，遵逐事件 supervise_item 隔离，绝不在补扫
  /// 体内直接分派出件——保持主循环单消费者语义）：
  /// - 类 1 keys_to_observers：某键队列仍非空 → 重投 CollectionUpdated，令重启主循环
  ///   重走「判空→试取→出件」闭环（panic 前未及处理的集合更新不丢）；
  /// - 类 2 session_id_to_observer：登记在册但尚未挂任何键队列的观察者（其
  ///   NewObserver 事件滞留旧通道随重建丢弃）→ 据 [`CollectionItemObserver::keys`]
  ///   重投 NewObserver 补齐
  fn rescan_stale_observers(&self) {
    // 先定格当前已挂在某键队列上的会话集（类 1 覆盖，免类 2 重复投 NewObserver）
    let mut attached: HashSet<usize> = HashSet::new();
    {
      let pin = self.keys_to_observers.pin();
      for (key, queue) in pin.iter() {
        // 队列锁在 collect 语句末即释放（不持队列锁调用 enqueue_event）
        let queued: Vec<Arc<CollectionItemObserver>> = queue.lock().iter().cloned().collect();
        if queued.is_empty() {
          continue;
        }
        for observer in &queued {
          attached.insert(observer.session_id);
        }
        self.enqueue_event(CollectionItemBrokerEvent::create_collection_updated_event(
          key.clone(),
        ));
      }
    }

    let pin = self.session_id_to_observer.pin();
    for (_, observer) in pin.iter() {
      if attached.contains(&observer.session_id) {
        continue;
      }
      if let Some(keys) = observer.keys() {
        self.enqueue_event(CollectionItemBrokerEvent::create_new_observer_event(
          observer.clone(),
          keys.clone(),
        ));
      }
    }
  }

  /// panic 臂恢复：重建通道 → 存量补扫 → 复位状态位（STARTED→NOT_STARTED）
  ///
  /// 定序论证：重建必须先于复位——复位后并发 start_wait 可立即 CAS 重拉，此时若通道
  /// 未重建则新循环 `take` 到 None 即缺席早退（重挂失效）。复位用
  /// `compare_exchange(STARTED, NOT_STARTED)`：仅在主循环确实 panic（状态仍 STARTED）
  /// 时生效；[`Self::dispose`] 已置 MAIN_LOOP_DISPOSED 终态时本 CAS 自然失败返回 false，
  /// 交 [`remount_main_loop`] 留终态退出（见票第 4 条「保 MAIN_LOOP_DISPOSED 终态」）
  ///
  /// `pub`：本恢复臂是 [`remount_main_loop`] 的编排内核，亦作确定性测试缝——外层循环
  /// panic 面（逐事件 [`supervise_item`] 未兜住的逃逸点）无法经存储注入器确定性触发，
  /// 故按 gossip「测试面直调臂」先例公开本臂供回归直调（对标 pub `dispose`）
  pub fn recover_after_panic(&self) -> bool {
    self.rebuild_event_channel();
    self.rescan_stale_observers();
    self
      .main_loop_task_status
      .compare_exchange(
        MAIN_LOOP_STARTED,
        MAIN_LOOP_NOT_STARTED,
        Ordering::SeqCst,
        Ordering::SeqCst,
      )
      .is_ok()
  }

  /// 重挂预算耗尽后的公开复位入口（供测试与运维面驱动有界重挂终态）：仅当状态仍
  /// STARTED 时复位为 NOT_STARTED（DISPOSED 终态拒绝复位返回 false）
  #[inline]
  pub fn reset_main_loop_status(&self) -> bool {
    self
      .main_loop_task_status
      .compare_exchange(
        MAIN_LOOP_STARTED,
        MAIN_LOOP_NOT_STARTED,
        Ordering::SeqCst,
        Ordering::SeqCst,
      )
      .is_ok()
  }

  /// 测试面：取出当前接收端并尽力排空已入队事件（非阻塞、尽力而为）
  ///
  /// 用于确定性验证 panic 恢复后的通道重建与存量补扫事件形态/数量（有界堆积）。
  /// 排空后回填接收端，不影响后续正常消费。C# 无对位（AsyncQueue 私有字段）
  pub fn drain_events_for_test(&self) -> Vec<CollectionItemBrokerEvent> {
    let rx = self.events_rx.lock().take();
    let Some(rx) = rx else {
      return Vec::new();
    };
    let mut drained = Vec::new();
    while let Ok(event) = rx.try_recv() {
      drained.push(event);
    }
    *self.events_rx.lock() = Some(rx);
    drained
  }
}

/// 主循环监督体（[`CollectionItemBroker::start_main_loop`] 与
/// [`remount_main_loop`] 的共用任务体）：弱引用升格后驱动
/// [`CollectionItemBroker::start_async`]；升格失败（经纪已释放）即正常退出
///
/// 泛型于取件源与启动器，使本自由函数不需 [`CollectionItemStore`] 之外的额外约束
///（对标 wkv gc/reclaim.rs `reclaimer_loop` 自由函数形态）
async fn run_main_loop<S, Sp>(broker: Weak<CollectionItemBroker<S, Sp>>)
where
  S: CollectionItemStore + 'static,
  Sp: TaskSpawner + 'static,
{
  if let Some(broker) = broker.upgrade() {
    broker.start_async().await;
  }
}

/// panic 臂有界重挂环（对标 wkv gc/reclaim.rs `remount_reclaimer`；票
/// wcol-itembroker-main-loop-panic-dead-no-remount 第 1 条）
///
/// 每轮：升格 → [`CollectionItemBroker::recover_after_panic`]（重建通道 + 存量补扫
/// + 复位状态位 STARTED→NOT_STARTED，DISPOSED 终态拒绝复位返回 false → 留终态退出，
///   见第 4 条）→ 退避 → CAS 重新认领（NOT_STARTED→STARTED，失败 = 退避窗内别处
///   start_wait 已抢先重拉，本环立即让位）→ 再监督一轮 [`run_main_loop`]（Ok 即闭环
///   退出；再 Err 则续下一轮复位重挂）。连败耗尽 [`REMOUNT_LIMIT`] 预算则留
///   NOT_STARTED 复位态与新通道退出，交后续首个 start_wait 重新认领——绝不禁回「状态位
///   永卡 STARTED 拒一切重拉」的死亡形态。全程协程让渡（`yield_now` 让核），绝不内联驱动
///   调度器（遵守 compio 禁 block_on 重入纪律）
async fn remount_main_loop<S, Sp>(broker: Weak<CollectionItemBroker<S, Sp>>, mut retries: u32)
where
  S: CollectionItemStore + 'static,
  Sp: TaskSpawner + 'static,
{
  loop {
    let Some(b) = broker.upgrade() else {
      return;
    };
    // 复位（含通道重建与存量补扫）；DISPOSED 终态拒绝复位，留终态退出
    if !b.recover_after_panic() {
      return;
    }
    drop(b);

    // 重挂预算耗尽：留 NOT_STARTED 复位态与新通道，交后续 start_wait 拉起
    if retries == 0 {
      return;
    }
    retries -= 1;

    // 让核退避窗：`yield_now` 让出本核交同核就绪任务推进（含并发客户端 start_wait 的
    // 重拉），窗内别处已抢先重拉则本环下方 CAS 让位退出
    for _ in 0..REMOUNT_BACKOFF_YIELDS {
      yield_now().await;
    }

    let Some(b) = broker.upgrade() else {
      return;
    };
    if b
      .main_loop_task_status
      .compare_exchange(
        MAIN_LOOP_NOT_STARTED,
        MAIN_LOOP_STARTED,
        Ordering::SeqCst,
        Ordering::SeqCst,
      )
      .is_err()
    {
      return;
    }
    drop(b);

    // 重新挂载监督：正常退出即闭环；再 panic 则续下一轮复位重挂
    if supervise_task(BROKER_MAIN_TASK, run_main_loop(broker.clone()))
      .await
      .is_ok()
    {
      return;
    }
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
/// 对应 C# BLMOVE 弹推内核段（TryGetNextListResult 族；精确锚点见本文件 661 行）
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
        element.to_vec(),
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
        items.push(element.to_vec());
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
  use wbase::supervise::snapshots;

  use super::*;

  struct DummyStore;
  impl CollectionItemStore for DummyStore {
    fn try_get_result(
      &self,
      _ns: u64,
      _db: u64,
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

  /// 外层主循环体 panic 经 [`supervise_task`] 归组登记于监督快照（INFO
  /// bg_task_health 可见 `BROKER_MAIN_TASK`）并计入 panic 计数——票
  /// wcol-itembroker-main-loop-panic-dead-no-remount 第 5 条「panic 计数/快照登记」面。
  /// 计数单调不回退，进程级快照在并行测试下仍稳定可判（仅断言在册 + 计数下界，
  /// 存活位因他测试并发在册而不作定型断言）
  #[test]
  fn main_loop_panic_registers_in_supervision_snapshot() {
    Runtime::new().unwrap().block_on(async {
      let res = supervise_task(BROKER_MAIN_TASK, async {
        panic!("injected outer main-loop panic");
      })
      .await;
      assert!(res.is_err(), "外层循环 panic 须产出 Err 载荷交有界重挂环");
    });

    let row = snapshots()
      .into_iter()
      .find(|r| r.name == BROKER_MAIN_TASK)
      .expect("主循环任务须登记于监督快照");
    assert!(row.panics >= 1, "外层循环 panic 须计入监督快照 panic 计数");
  }
}
