//! 订阅者投递面（C# Garnet.networking/IMessageConsumer 的发布订阅投影）
//!
//! C# SubscribeBroker 广播时直调 `ServerSessionBase.Publish / PatternPublish`
//! 写会话输出缓冲；Rust 会话为单线程属主结构，中枢经 [`PubSubSink`] 投递，
//! 会话侧以 [`PubSubMailbox`] 收取后在自身线程编码回放。

use std::{
  collections::VecDeque,
  sync::{
    Arc,
    atomic::{AtomicU64, Ordering::Relaxed},
  },
};

use parking_lot::Mutex;

/// 消息类别（通道直投 / 模式命中）
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PubSubMessageKind {
  /// SUBSCRIBE 通道消息（C# session.Publish）
  Channel,
  /// PSUBSCRIBE 模式消息（C# session.PatternPublish）
  Pattern,
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
}

/// 邮箱投递面：有界队列 + 溢出丢弃计数
///
/// C# 侧发布即直写会话缓冲（背压由网络发送器承担）；托管面以有界邮箱
/// 解耦线程，溢出丢弃最旧消息并计数（保新弃旧，对齐 Redis 发布不可靠
/// 语义的安全方向）。
pub struct PubSubMailbox {
  /// 队列容量上限
  capacity: usize,
  /// 待投递队列
  queue: Mutex<VecDeque<PubSubMessage>>,
  /// 溢出丢弃计数
  dropped: AtomicU64,
}

impl PubSubMailbox {
  /// 创建容量为 `capacity` 的邮箱
  pub fn new(capacity: usize) -> Self {
    Self {
      capacity: capacity.max(1),
      queue: Mutex::new(VecDeque::new()),
      dropped: AtomicU64::new(0),
    }
  }

  /// 取走全部待投递消息（会话线程收敛点）
  pub fn drain(&self) -> Vec<PubSubMessage> {
    self.queue.lock().drain(..).collect()
  }

  /// 当前积压长度
  pub fn len(&self) -> usize {
    self.queue.lock().len()
  }

  /// 队列是否为空
  pub fn is_empty(&self) -> bool {
    self.queue.lock().is_empty()
  }

  /// 累计溢出丢弃数
  pub fn dropped_count(&self) -> u64 {
    self.dropped.load(Relaxed)
  }

  /// 入队（满则丢弃最旧并计数）
  fn push(&self, message: PubSubMessage) {
    let mut queue = self.queue.lock();
    if queue.len() >= self.capacity {
      queue.pop_front();
      self.dropped.fetch_add(1, Relaxed);
    }
    queue.push_back(message);
  }
}

impl PubSubSink for PubSubMailbox {
  fn publish(&self, channel: &[u8], value: &[u8]) {
    self.push(PubSubMessage {
      kind: PubSubMessageKind::Channel,
      pattern: None,
      channel: channel.into(),
      value: value.into(),
    });
  }

  fn pattern_publish(&self, pattern: &[u8], channel: &[u8], value: &[u8]) {
    self.push(PubSubMessage {
      kind: PubSubMessageKind::Pattern,
      pattern: Some(pattern.into()),
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

    let messages = mailbox.drain();
    assert_eq!(messages.len(), 2);
    assert_eq!(messages[0].channel.as_ref(), b"b");
    assert!(mailbox.is_empty());
  }

  #[test]
  fn mailbox_pattern_message_keeps_pattern() {
    let mailbox = PubSubMailbox::new(4);
    mailbox.pattern_publish(b"a*", b"ab", b"v");
    let messages = mailbox.drain();
    assert_eq!(messages[0].kind, PubSubMessageKind::Pattern);
    assert_eq!(messages[0].pattern.as_deref(), Some(b"a*".as_slice()));
  }
}
