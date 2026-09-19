//! 订阅者投递面（C# Garnet.networking/IMessageConsumer 的发布订阅投影）
//!
//! C# SubscribeBroker 广播时直调 `ServerSessionBase.Publish / PatternPublish`
//! 写会话输出缓冲；Rust 会话为单线程属主结构，中枢经 [`PubSubSink`] 投递，
//! 会话侧以 [`PubSubMailbox`] 收取后在自身线程编码回放。
//!
//! 背压语义域界（与 C# 的刻意分歧，登记于此）：C# Broadcast 直写订阅会话
//! 网络发送器（libs/server/PubSub/SubscribeBroker.cs:Broadcast/:87/:108，
//! 推送编码收尾 RespServerSession.SendAndReset），慢消费者由网络发送器
//! 阻塞传导背压，无丢帧语义；Rust 侧广播线程不得阻塞在单会话上（广播持
//! 中枢读锁，阻塞会拖垮全部投递），邮箱满容量丢尾帧、原子累计 dropped
//! 计数，经会话指标面可观测（CLIENT INFO 的 pubsub-dropped 字段与
//! INFO clients 段的 pubsub_dropped 聚合项，见 wnode / wmetric 接线）。

use std::{
  collections::VecDeque,
  sync::{
    Arc,
    atomic::{AtomicU64, Ordering::Relaxed},
  },
};

use event_listener::Event;
use parking_lot::Mutex;

/// 消息类别（通道直投 / 模式命中 / 分片直投）
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PubSubMessageKind {
  /// SUBSCRIBE 通道消息（C# session.Publish）
  Channel,
  /// PSUBSCRIBE 模式消息（C# session.PatternPublish）
  Pattern,
  /// SSUBSCRIBE 分片通道消息（C# session.Publish / ShardPublish）
  Shard,
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
/// 实现方须保证 `publish*` 不回调订阅中枢（广播持读锁）。
pub trait PubSubSink: Send + Sync {
  /// 通道消息投递（libs/server/Sessions/ServerSessionBase.cs:Publish）
  fn publish(&self, channel: &[u8], value: &[u8]);
  /// 模式消息投递（libs/server/Sessions/ServerSessionBase.cs:PatternPublish）
  fn pattern_publish(&self, pattern: &[u8], channel: &[u8], value: &[u8]);
  /// 分片通道消息投递（libs/server/Sessions/ServerSessionBase.cs:Publish）
  fn shard_publish(&self, channel: &[u8], value: &[u8]);
}

/// 邮箱投递面：有界队列 + 溢出丢弃计数
///
/// C# 侧无会话邮箱：SubscribeBroker.Broadcast 直调 `session.Publish` 写会话
/// 输出缓冲（libs/server/PubSub/SubscribeBroker.cs:87/:108，实现见
/// libs/server/Resp/RespServerSession.cs），背压由网络发送器承担；Rust 面以
/// 轻量有界邮箱解耦线程（会话单线程属主模型的域内投影）。
/// 采用 parking_lot::Mutex<VecDeque> 有锁队列架构（低争用 → 有锁队列：
/// 临界段仅 push_back / 整批 take，无 CAS 重试与内存序开销）：
/// - 零预分配：初始无大块内存空置，按需扩容
/// - 极低锁竞争：仅发布端短时推入，属主会话线程单次批量 drain 消费
/// - 有界保护：达到上限容量时安全丢弃并原子递增 dropped 计数
pub struct PubSubMailbox {
  /// 内部有界双端队列
  queue: Mutex<VecDeque<PubSubMessage>>,
  /// 最大容量上限
  capacity: usize,
  /// 溢出丢弃计数
  dropped: AtomicU64,
  /// 到达事件（广播线程 push → 会话连接任务唤醒；订阅推送及时投递的
  /// 通知面，订阅态空闲会话的双路等待源）
  arrived: Event,
}

impl PubSubMailbox {
  /// 创建容量为 `capacity` 的邮箱
  pub fn new(capacity: usize) -> Self {
    Self {
      queue: Mutex::new(VecDeque::new()),
      capacity: capacity.max(1),
      dropped: AtomicU64::new(0),
      arrived: Event::new(),
    }
  }

  /// 尝试发布消息入队（满则丢弃并原子递增溢出计数）
  #[inline]
  pub fn try_publish(&self, message: PubSubMessage) -> bool {
    let mut q = self.queue.lock();
    if q.len() >= self.capacity {
      self.dropped.fetch_add(1, Relaxed);
      false
    } else {
      q.push_back(message);
      drop(q);
      // 唤醒全部等待者（订阅会话连接任务的双路等待；广播多投时
      // 单次唤醒批量排空，notify(usize::MAX) 杜绝漏醒）
      self.arrived.notify(usize::MAX);
      true
    }
  }

  /// 取走全部待投递消息排入指定缓冲中，返回排出的消息数（复用外部缓冲）
  #[inline]
  pub fn drain_into(&self, buf: &mut Vec<PubSubMessage>) -> usize {
    let mut q = self.queue.lock();
    let count = q.len();
    if count > 0 {
      buf.reserve(count);
      buf.extend(q.drain(..));
    }
    count
  }

  /// 当前积压长度
  #[inline]
  pub fn len(&self) -> usize {
    self.queue.lock().len()
  }

  /// 队列是否为空
  #[inline]
  pub fn is_empty(&self) -> bool {
    self.queue.lock().is_empty()
  }

  /// 累计溢出丢弃数
  #[inline]
  pub fn dropped_count(&self) -> u64 {
    self.dropped.load(Relaxed)
  }

  /// 是否有待投递消息（轻量快速检查，双路等待的快速路径判定）
  #[inline]
  pub fn has_messages(&self) -> bool {
    !self.queue.lock().is_empty()
  }

  /// 注册一次到达监听（返回的 listener 为标准 Future，await 即
  /// 「可能有消息到达」；配合 [`Self::has_messages`] 双检使用——
  /// 注册后复查非空则无需等待，杜绝唤醒丢失）
  #[inline]
  pub fn listen(&self) -> event_listener::EventListener {
    self.arrived.listen()
  }
}

impl PubSubSink for PubSubMailbox {
  #[inline]
  fn publish(&self, channel: &[u8], value: &[u8]) {
    self.try_publish(PubSubMessage {
      kind: PubSubMessageKind::Channel,
      pattern: None,
      channel: channel.into(),
      value: value.into(),
    });
  }

  #[inline]
  fn pattern_publish(&self, pattern: &[u8], channel: &[u8], value: &[u8]) {
    self.try_publish(PubSubMessage {
      kind: PubSubMessageKind::Pattern,
      pattern: Some(pattern.into()),
      channel: channel.into(),
      value: value.into(),
    });
  }

  #[inline]
  fn shard_publish(&self, channel: &[u8], value: &[u8]) {
    self.try_publish(PubSubMessage {
      kind: PubSubMessageKind::Shard,
      pattern: None,
      channel: channel.into(),
      value: value.into(),
    });
  }
}

impl<T: PubSubSink + ?Sized> PubSubSink for Arc<T> {
  #[inline]
  fn publish(&self, channel: &[u8], value: &[u8]) {
    (**self).publish(channel, value);
  }

  #[inline]
  fn pattern_publish(&self, pattern: &[u8], channel: &[u8], value: &[u8]) {
    (**self).pattern_publish(pattern, channel, value);
  }

  #[inline]
  fn shard_publish(&self, channel: &[u8], value: &[u8]) {
    (**self).shard_publish(channel, value);
  }
}

#[cfg(test)]
mod tests {
  use super::*;

  #[test]
  fn mailbox_bounded_and_drops_tail() {
    let mailbox = PubSubMailbox::new(2);
    mailbox.publish(b"a", b"1");
    mailbox.publish(b"b", b"2");
    mailbox.publish(b"c", b"3");
    assert_eq!(mailbox.len(), 2);
    assert_eq!(mailbox.dropped_count(), 1);

    let mut buf = Vec::new();
    assert_eq!(mailbox.drain_into(&mut buf), 2);
    assert_eq!(buf[0].channel.as_ref(), b"a");
    assert_eq!(buf[1].channel.as_ref(), b"b");
    assert!(mailbox.is_empty());
  }

  #[test]
  fn mailbox_drain_into_reuses_buffer() {
    let mailbox = PubSubMailbox::new(4);
    mailbox.publish(b"c1", b"v1");
    mailbox.publish(b"c2", b"v2");

    let mut buf = Vec::new();
    let count = mailbox.drain_into(&mut buf);
    assert_eq!(count, 2);
    assert_eq!(buf.len(), 2);
    assert_eq!(buf[0].channel.as_ref(), b"c1");
    assert_eq!(buf[1].channel.as_ref(), b"c2");
    assert!(mailbox.is_empty());

    mailbox.publish(b"c3", b"v3");
    buf.clear();
    let count2 = mailbox.drain_into(&mut buf);
    assert_eq!(count2, 1);
    assert_eq!(buf.len(), 1);
    assert_eq!(buf[0].channel.as_ref(), b"c3");
  }

  #[test]
  fn mailbox_pattern_message_keeps_pattern() {
    let mailbox = PubSubMailbox::new(4);
    mailbox.pattern_publish(b"a*", b"ab", b"v");
    let mut buf = Vec::new();
    mailbox.drain_into(&mut buf);
    let messages = buf;
    assert_eq!(messages[0].kind, PubSubMessageKind::Pattern);
    assert_eq!(messages[0].pattern.as_deref(), Some(b"a*".as_slice()));
  }

  #[test]
  fn mailbox_try_publish_and_drain() {
    let mailbox = PubSubMailbox::new(1);
    assert!(mailbox.try_publish(PubSubMessage {
      kind: PubSubMessageKind::Channel,
      pattern: None,
      channel: b"ch".to_vec().into_boxed_slice(),
      value: b"v".to_vec().into_boxed_slice(),
    }));
    assert_eq!(mailbox.len(), 1);
    assert!(!mailbox.is_empty());
    // 满队列时 try_publish 返回 false 并递增 dropped
    assert!(!mailbox.try_publish(PubSubMessage {
      kind: PubSubMessageKind::Channel,
      pattern: None,
      channel: b"ch2".to_vec().into_boxed_slice(),
      value: b"v2".to_vec().into_boxed_slice(),
    }));
    assert_eq!(mailbox.dropped_count(), 1);

    let mut buf = Vec::new();
    assert_eq!(mailbox.drain_into(&mut buf), 1);
    assert!(mailbox.is_empty());
    assert_eq!(buf[0].channel.as_ref(), b"ch");
  }
}
