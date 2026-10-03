//! 订阅者投递面（C# Garnet.networking/IMessageConsumer 的发布订阅投影）
//!
//! C# SubscribeBroker 广播时直调 `ServerSessionBase.Publish / PatternPublish`
//! 写会话输出缓冲；Rust 会话为单线程属主结构，中枢经 [`PubSubSink`] 投递，
//! 会话侧以 [`PubSubMailbox`] 收取后在自身线程编码回放。
//!
//! 有界水位与满时拒收：C# Broadcast 在发布线程同步直写订阅会话网络发送器
//! （libs/server/PubSub/SubscribeBroker.cs:87/:108），发送器为固定尺寸应答
//! 缓冲（libs/common/Networking/GarnetTcpNetworkSender.cs:120-134），在途
//! 发送超门限时 `throttle.Wait()` 阻塞发布线程传导背压、零丢弃（同文件
//! :310-330）；Rust 会话为单线程属主、发布线程可恰为对端会话属主线程，
//! 阻塞版背压在互订拓扑下成环即死锁，故以 crossfire::flavor::Array 有界
//! 队列收口水位：发布端 0 锁 0 阻塞无损入队至容量上限，满即拒收新帧
//! （丢尾不丢头），慢订阅者积压刚性封顶 capacity。与 C# 的阻塞/拒收
//! 修复性分叉登记于 doc/zh/deviations.md §14；拒收的丢弃量经 [`PubSubMailbox`]
//! 内置计数单调累计，会话侧读出并入 ClientView 发布轨（rn14 丢弃观测面）。
//! 数据面丢尾策略本文件所辖恒一字不动；唯服务端主动状态通知
//! （[`PubSubMessageKind::ShardUnsubscribe`] 槽迁出强制退订帧）满即拒收
//! 改有界等待重试兜底臂（[`PubSubMailbox::try_publish_control`]：自旋+
//! yield 让出、逐邮箱预算、入列即止；超界 warn 留痕放弃并递增
//! [`PubSubMailbox::dropped_shard_notify`] 可观测计数）——C# throttle.Wait
//! 阻塞传导的有界投影，悬挂定性为连接同 ns 生存期内（AUTH 换租/会话
//! 释放整体清退收口），非数据面策略变更

use std::{
  sync::{
    Arc,
    atomic::{AtomicU64, Ordering},
  },
  thread::yield_now,
};

use crossfire::flavor::{Array, Queue};
use event_listener::Event;

/// 控制通知满水位有界等待预算（逐订阅者持有的让位重试轮数上界，非墙钟
/// 时限）：每轮一次入列尝试 + 一次 [`std::thread::yield_now`] 让出，消费端
/// 排空即腾位复采；取 0x100（256）覆盖慢订阅者单次排空批窗，量级对标
/// C# 发送节流窗 ThrottleMax=8（libs/common/Networking/
/// GarnetTcpNetworkSender.cs:47）的有界等待形态——控制面低频路径
/// （每订阅至多一帧），预算消耗可忽略；超界必弃，杜绝无限等待
const NOTIFY_WAIT_ROUNDS: u64 = 0x100;

/// 消息类别（通道直投 / 模式命中 / 分片直投 / 槽迁移强制退订通知）
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PubSubMessageKind {
  /// SUBSCRIBE 通道消息（C# session.Publish）
  Channel,
  /// PSUBSCRIBE 模式消息（C# session.PatternPublish）
  Pattern,
  /// SSUBSCRIBE 分片通道消息（C# session.Publish / ShardPublish）
  Shard,
  /// 槽迁移强制退订通知（服务端主动 sunsubscribe 推送，channel 承载
  /// 隔离键、value 空；会话 drain 臂回帧并本地递减活跃计数——投递纪律
  /// 的邮箱单通道形态，见 [`PubSubSink::shard_forced_unsubscribe`]）
  ShardUnsubscribe,
}

/// 一条待投递的发布消息
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PubSubMessage {
  /// 消息类别
  pub kind: PubSubMessageKind,
  /// 命中的模式（仅模式消息携带）
  pub pattern: Option<Box<[u8]>>,
  /// 通道名
  pub channel: Box<[u8]>,
  /// 负载
  pub value: Box<[u8]>,
}

/// 订阅者投递面（C# ServerSessionBase.Publish / PatternPublish 的域内投影）
///
/// 实现方须保证 `publish*` 不同步再入订阅中枢发布面（一律经邮箱入列异步
/// 化）：订阅集合以 papaya 无锁并发表承接（广播遍历不持锁、不阻塞订阅
/// 变更），再入约束防的是回调内同步递归广播的扩散，非锁面。
pub trait PubSubSink: Send + Sync {
  /// 通道消息投递（libs/server/Sessions/ServerSessionBase.cs:Publish）
  fn publish(&self, channel: &[u8], value: &[u8]) -> bool;
  /// 模式消息投递（libs/server/Sessions/ServerSessionBase.cs:PatternPublish）
  fn pattern_publish(&self, pattern: &[u8], channel: &[u8], value: &[u8]) -> bool;
  /// 分片通道消息投递（libs/server/Sessions/ServerSessionBase.cs:Publish）
  fn shard_publish(&self, channel: &[u8], value: &[u8]) -> bool;
  /// 槽迁移强制退订通知（Redis pubsub.c:pubsubShardUnsubscribeAllChannelsInSlot
  /// 的推送面对位）：槽权离本节点时由槽事件钩对命中锚的分片订阅者投递，
  /// 会话 drain 臂据此回 sunsubscribe 帧并本地递减活跃计数——绝不在
  /// broker/迁移线程直改会话状态（守会话单写者）。邮箱实现面的入列
  /// 对满即拒收改有界等待重试兜底（[`PubSubMailbox::try_publish_control`]，
  /// C# throttle.Wait 阻塞传导的有界投影）：服务端主动状态通知被丢即令
  /// 会话订阅旗悬挂，数据面 push 臂（publish/pattern_publish/shard_publish）
  /// 维持满即拒收丢尾（§14）一字不动
  fn shard_forced_unsubscribe(&self, channel: &[u8]) -> bool;
}

/// 邮箱投递面：基于 crossfire::flavor::Array 的无锁有界队列（IS_BOUNDED=true）
///
/// 有界水位收口（对位 C# 固定尺寸发送缓冲 + 在途发送门限，
/// libs/common/Networking/GarnetTcpNetworkSender.cs:120-134/:310-330）：
/// - 无锁环形数组队列，push 0 锁 0 CAS 自旋损耗
/// - 满时拒收：积压到顶 try_publish 返回 false 丢尾帧，发布端零阻塞，
///   慢订阅者内存水位刚性封顶 capacity（修复性分叉见 doc/zh/deviations.md §14）
/// - 消费端批量 pop 排空，极低同步开销
pub struct PubSubMailbox {
  /// 内部有界无锁队列
  queue: Array<PubSubMessage>,
  /// 到达事件（广播线程 push → 会话连接任务唤醒；订阅推送及时投递的
  /// 通知面，订阅态空闲会话的双路等待源）
  arrived: Event,
  /// 满水位拒收累计帧数（发布端逐次 try_publish 失败即递增；观测面
  /// 慢订阅者丢尾量，经会话 `PubSubSession::dropped` 并入 ClientView
  /// 发布轨导出——rn14 登记的丢弃计数盲区收口点）
  dropped: AtomicU64,
  /// 控制通知超界放弃累计帧数（try_publish_control 有界等待仍超界的
  /// 放弃量；观测面：槽迁移强制退订通知丢失即会话订阅旗悬挂风险，
  /// 与数据面 dropped 分账登记）
  dropped_shard_notify: AtomicU64,
  /// 控制通知有界等待剩余额度（逐邮箱持有——邮箱即逐会话投递面；
  /// 通知帧成功入列即回充满额，保证每个后续通知都获得完整有界窗口，
  /// 深慢积压下退避成本不随通知次数累积）
  shard_notify_budget: AtomicU64,
}

impl PubSubMailbox {
  /// 创建邮箱（capacity 为积压水位上限，即慢订阅者最多可囤积的待投帧数）
  pub fn new(capacity: usize) -> Self {
    Self {
      queue: Array::new(capacity),
      arrived: Event::new(),
      dropped: AtomicU64::new(0),
      dropped_shard_notify: AtomicU64::new(0),
      shard_notify_budget: AtomicU64::new(NOTIFY_WAIT_ROUNDS),
    }
  }

  /// 尝试发布消息入队（基于 crossfire::flavor::Array 无锁 push，零阻塞；
  /// 队列满即拒收返回 false 丢尾帧，并累计进 [`Self::dropped`] 观测面）
  #[inline]
  pub fn try_publish(&self, message: PubSubMessage) -> bool {
    if self.queue.push(message).is_err() {
      // 满水位拒收即计数（观测面：慢订阅者丢尾量；Relaxed 足够——
      // 纯统计无同步依赖，读取方容忍发布在途的纳秒级陈旧）
      self.dropped.fetch_add(1, Ordering::Relaxed);
      return false;
    }
    // 唤醒全部等待者（订阅会话连接任务的双路等待；广播多投时
    // 单次唤醒批量排空，notify(usize::MAX) 杜绝漏醒）
    self.arrived.notify(usize::MAX);
    true
  }

  /// 控制通知入队（满水位有界等待重试，服务端主动状态通知专用兜底臂）
  ///
  /// C# 对位：广播线程对订阅会话同步直写发送器，在途超门限经
  /// `throttle.Wait()` 阻塞发布端传导背压、推送零丢弃
  /// （libs/common/Networking/GarnetTcpNetworkSender.cs:310-330 的
  /// ThrottleMax 有界发送窗 + SemaphoreSlim 阻塞等待形态）；rust 数据面
  /// 已改有界拒收（§14 在册，本臂一字不动），唯服务端主动状态通知帧
  /// （[`PubSubMessageKind::ShardUnsubscribe`]）被丢即令会话活跃计数与
  /// 订阅旗无自愈路径悬挂，故其入列单点改满即拒收为有界等待重试：
  /// - 自旋 + [`std::thread::yield_now`] 让出，逐轮复查排空腾位、入列即止
  ///   （通知仍走邮箱单队列，恒先于其后到站数据帧出列，保序不变）
  /// - 超界 warn 留痕放弃并递增 [`Self::dropped_shard_notify`] 可观测计数；
  ///   严禁实现成无限等待——RESP3 自推窗（订阅会话不过白名单门、自身发
  ///   SETSLOT 命中自身满邮箱）同任务无人在排空，本臂是该窗唯一兜底，
  ///   有界方不成新死锁源
  /// - 悬挂定性：放弃臂丢帧的订阅态悬挂仅在连接同 ns 生存期内
  ///   （AUTH 换租 materialize_authenticated_handle 与 dispose 的
  ///   remove_subscription 会话级整体清退可清零），非绝对永久死状态
  /// - 控制面低频路径（每订阅至多一帧），有界自旋成本可忽略
  fn try_publish_control(&self, message: PubSubMessage) -> bool {
    let mut message = message;
    loop {
      // 入列即止（与 try_publish 同一单队列，队满天然保序：通知帧落于
      // 队尾，恒先于其后到站数据帧出列）
      match self.queue.push(message) {
        Ok(()) => {
          self.arrived.notify(usize::MAX);
          // 通知帧成功入列即投递闭环，退避预算回充满额
          self
            .shard_notify_budget
            .store(NOTIFY_WAIT_ROUNDS, Ordering::Release);
          return true;
        }
        Err(back) => message = back,
      }
      // 本轮让出重试的额度判据：额度有余才扣减复投；扣减后额度为 0 即
      // 超界。通知成功入列时回充满额，深慢积压下退避成本不随通知次数累积。
      if self
        .shard_notify_budget
        .try_update(Ordering::AcqRel, Ordering::Acquire, |b| b.checked_sub(1))
        .is_err()
      {
        // 超界放弃：warn 留痕 + 可观测计数（放弃后会话侧无自愈路径，
        // 仅能靠会话级整体清退收口；计数经 [`Self::dropped_shard_notify`]
        // 单调累计）
        self.dropped_shard_notify.fetch_add(1, Ordering::Relaxed);
        log::warn!(
          "wpubsub 控制通知入列超界放弃：槽迁移强制退订帧满水位 {NOTIFY_WAIT_ROUNDS} 轮未获排空腾位，本帧丢弃（会话订阅态悬挂于连接同 ns 生存期内，至 AUTH 换租/会话释放整体清退收口；观测计数见 dropped_shard_notify）",
        );
        return false;
      }
      // 满即让位复查：腾位由消费端排空承接，本臂逐轮复投轮询到空位即止
      // （绝不在队满时 pop-回推——那会轮转积压帧序，违「保序不变」）
      yield_now();
    }
  }

  /// 满水位拒收累计帧数（单调不回退；发布端计数、会话侧经
  /// `PubSubSession::dropped` 读出并入 ClientView 发布轨）
  #[inline]
  pub fn dropped(&self) -> u64 {
    self.dropped.load(Ordering::Relaxed)
  }

  /// 控制通知超界放弃累计帧数（单调不回退；槽迁移强制退订通知经
  /// [`Self::try_publish_control`] 有界等待仍超界的放弃量——区别于数据面
  /// [`Self::dropped`] 满水位丢尾计数，本计数即订阅旗悬挂风险的运维观测面）
  #[inline]
  pub fn dropped_shard_notify(&self) -> u64 {
    self.dropped_shard_notify.load(Ordering::Relaxed)
  }

  /// 取走全部待投递消息排入指定缓冲中，返回排出的消息数（复用外部缓冲，单次预分配容量）
  #[inline]
  pub fn drain_into(&self, buf: &mut Vec<PubSubMessage>) -> usize {
    let count = self.queue.len();
    if count > 0 {
      buf.reserve(count);
    }
    let mut drained = 0;
    while let Some(msg) = self.queue.pop() {
      buf.push(msg);
      drained += 1;
    }
    drained
  }

  /// 当前积压长度
  #[inline]
  pub fn len(&self) -> usize {
    self.queue.len()
  }

  /// 队列是否为空（双路等待的快速路径判定：非空即有待投递消息，与
  /// [`Self::listen`] 注册后双检配套——注册后复查非空则无需等待，杜绝唤醒丢失）
  #[inline]
  pub fn is_empty(&self) -> bool {
    self.queue.is_empty()
  }

  /// 注册一次到达监听（返回的 listener 为标准 Future，await 即
  /// 「可能有消息到达」；配合 [`Self::is_empty`] 双检使用——
  /// 注册后复查非空则无需等待，杜绝唤醒丢失）
  #[inline]
  pub fn listen(&self) -> event_listener::EventListener {
    self.arrived.listen()
  }

  /// 组帧入队单点（sink 数据臂共用骨架）：kind/pattern 为唯一差位，
  /// 满即拒收丢尾（§14 数据面策略，本臂一字不动）
  #[inline]
  fn enqueue(
    &self,
    kind: PubSubMessageKind,
    pattern: Option<&[u8]>,
    channel: &[u8],
    value: &[u8],
  ) -> bool {
    self.try_publish(PubSubMessage {
      kind,
      pattern: pattern.map(Into::into),
      channel: channel.into(),
      value: value.into(),
    })
  }

  /// 服务端主动状态通知组帧入队单点（[`PubSubSink::shard_forced_unsubscribe`]
  /// 臂专用）：满水位改有界等待重试（见 [`Self::try_publish_control`]），
  /// 消除「控制通知被丢 → 会话订阅旗悬挂至连接同 ns 生存期收口」死状态
  #[inline]
  fn enqueue_control(
    &self,
    kind: PubSubMessageKind,
    pattern: Option<&[u8]>,
    channel: &[u8],
    value: &[u8],
  ) -> bool {
    self.try_publish_control(PubSubMessage {
      kind,
      pattern: pattern.map(Into::into),
      channel: channel.into(),
      value: value.into(),
    })
  }
}

/// 邮箱投递面 trait 实现：数据臂（publish/pattern_publish/shard_publish）
/// 拒收计数已在 [`PubSubMailbox::try_publish`] 失败臂单点收口，实现臂透传
/// bool 供发布中枢核准实达计数，满水位丢尾策略（§14）一字不动；
/// 控制通知臂 [`PubSubSink::shard_forced_unsubscribe`] 独走
/// [`PubSubMailbox::enqueue_control`] 有界等待兜底——服务端主动状态通知
/// 被丢即令会话订阅旗悬挂，满水位对其改短程重试、超界留痕（C# 广播线程
/// 同步直写恒零丢弃的兜底投影，throttle.Wait 的对位收口）
impl PubSubSink for PubSubMailbox {
  #[inline]
  fn publish(&self, channel: &[u8], value: &[u8]) -> bool {
    self.enqueue(PubSubMessageKind::Channel, None, channel, value)
  }

  #[inline]
  fn pattern_publish(&self, pattern: &[u8], channel: &[u8], value: &[u8]) -> bool {
    self.enqueue(PubSubMessageKind::Pattern, Some(pattern), channel, value)
  }

  #[inline]
  fn shard_publish(&self, channel: &[u8], value: &[u8]) -> bool {
    self.enqueue(PubSubMessageKind::Shard, None, channel, value)
  }

  #[inline]
  fn shard_forced_unsubscribe(&self, channel: &[u8]) -> bool {
    self.enqueue_control(PubSubMessageKind::ShardUnsubscribe, None, channel, &[])
  }
}

/// Arc 转发面：逐臂按名直转发内层实现（数据臂维持满即拒收丢尾，
/// 控制通知臂经内层 shard_forced_unsubscribe 的有界等待兜底）
impl<T: PubSubSink + ?Sized> PubSubSink for Arc<T> {
  #[inline]
  fn publish(&self, channel: &[u8], value: &[u8]) -> bool {
    (**self).publish(channel, value)
  }

  #[inline]
  fn pattern_publish(&self, pattern: &[u8], channel: &[u8], value: &[u8]) -> bool {
    (**self).pattern_publish(pattern, channel, value)
  }

  #[inline]
  fn shard_publish(&self, channel: &[u8], value: &[u8]) -> bool {
    (**self).shard_publish(channel, value)
  }

  #[inline]
  fn shard_forced_unsubscribe(&self, channel: &[u8]) -> bool {
    (**self).shard_forced_unsubscribe(channel)
  }
}
