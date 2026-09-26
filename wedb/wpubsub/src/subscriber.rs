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
//! 内置计数单调累计，会话侧读出并入 ClientView 发布轨（rn14 丢弃观测面）

use std::sync::{
  Arc,
  atomic::{AtomicU64, Ordering},
};

use crossfire::flavor::{Array, Queue};
use event_listener::Event;

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
}

impl PubSubMailbox {
  /// 创建邮箱（capacity 为积压水位上限，即慢订阅者最多可囤积的待投帧数）
  pub fn new(capacity: usize) -> Self {
    Self {
      queue: Array::new(capacity),
      arrived: Event::new(),
      dropped: AtomicU64::new(0),
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

  /// 满水位拒收累计帧数（单调不回退；发布端计数、会话侧经
  /// `PubSubSession::dropped` 读出并入 ClientView 发布轨）
  #[inline]
  pub fn dropped(&self) -> u64 {
    self.dropped.load(Ordering::Relaxed)
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

  /// 队列是否为空
  #[inline]
  pub fn is_empty(&self) -> bool {
    self.queue.is_empty()
  }

  /// 是否有待投递消息（轻量快速检查，双路等待的快速路径判定）
  #[inline]
  pub fn has_messages(&self) -> bool {
    !self.queue.is_empty()
  }

  /// 注册一次到达监听（返回的 listener 为标准 Future，await 即
  /// 「可能有消息到达」；配合 [`Self::has_messages`] 双检使用——
  /// 注册后复查非空则无需等待，杜绝唤醒丢失）
  #[inline]
  pub fn listen(&self) -> event_listener::EventListener {
    self.arrived.listen()
  }
}

/// 邮箱投递面 trait 实现：拒收计数已在 [`PubSubMailbox::try_publish`]
/// 失败臂单点收口，实现臂吞 bool 不再构成观测盲区（rn14 登记点）
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
  fn mailbox_bounded_rejects_when_full() {
    let mailbox = PubSubMailbox::new(2);
    mailbox.publish(b"a", b"1");
    mailbox.publish(b"b", b"2");
    assert_eq!(mailbox.len(), 2);
    // 满水位：发布端零阻塞、拒收丢尾帧（不丢头）
    mailbox.publish(b"c", b"3");
    assert_eq!(mailbox.len(), 2);
    assert!(!mailbox.try_publish(PubSubMessage {
      kind: PubSubMessageKind::Channel,
      pattern: None,
      channel: b"d".to_vec().into_boxed_slice(),
      value: b"4".to_vec().into_boxed_slice(),
    }));

    let mut buf = Vec::new();
    assert_eq!(mailbox.drain_into(&mut buf), 2);
    assert_eq!(buf[0].channel.as_ref(), b"a");
    assert_eq!(buf[1].channel.as_ref(), b"b");
    assert!(mailbox.is_empty());
    // 排空后水位回落，恢复收帧
    assert!(mailbox.try_publish(PubSubMessage {
      kind: PubSubMessageKind::Channel,
      pattern: None,
      channel: b"e".to_vec().into_boxed_slice(),
      value: b"5".to_vec().into_boxed_slice(),
    }));
    assert_eq!(mailbox.len(), 1);
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
    let mailbox = PubSubMailbox::new(2);
    assert!(mailbox.try_publish(PubSubMessage {
      kind: PubSubMessageKind::Channel,
      pattern: None,
      channel: b"ch".to_vec().into_boxed_slice(),
      value: b"v".to_vec().into_boxed_slice(),
    }));
    assert_eq!(mailbox.len(), 1);
    assert!(!mailbox.is_empty());
    // 有界队列满即拒收：第二个入队、第三个触顶失败
    assert!(mailbox.try_publish(PubSubMessage {
      kind: PubSubMessageKind::Channel,
      pattern: None,
      channel: b"ch2".to_vec().into_boxed_slice(),
      value: b"v2".to_vec().into_boxed_slice(),
    }));
    assert_eq!(mailbox.len(), 2);

    let mut buf = Vec::new();
    assert_eq!(mailbox.drain_into(&mut buf), 2);
    assert!(mailbox.is_empty());
    assert_eq!(buf[0].channel.as_ref(), b"ch");
    assert_eq!(buf[1].channel.as_ref(), b"ch2");
  }
}
