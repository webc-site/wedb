//! 订阅者投递面（C# Garnet.networking/IMessageConsumer 的发布订阅投影）
//!
//! C# SubscribeBroker 广播时直调 `ServerSessionBase.Publish / PatternPublish`
//! 写会话输出缓冲；Rust 会话为单线程属主结构，中枢经 [`PubSubSink`] 投递，
//! 会话侧以 [`PubSubMailbox`] 收取后在自身线程编码回放。

use std::sync::{
  Arc,
  atomic::{AtomicU64, Ordering::Relaxed},
};

use crossfire::flavor::{Array, Queue};

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
/// C# 侧发布即直写会话缓冲（背压由网络发送器承担）；Rust 面以有界邮箱
/// 解耦线程。基于 crossfire::flavor::Array 纯原子无锁队列实现快路径零锁投递与单属主无锁消费，
/// 彻底消除 Waker 注册表、Future 轮询机与 channel 包装开销。
pub struct PubSubMailbox {
  /// 纯原子无锁队列
  queue: Array<PubSubMessage>,
  /// 溢出丢弃计数
  dropped: AtomicU64,
}

impl PubSubMailbox {
  /// 创建容量为 `capacity` 的邮箱
  pub fn new(capacity: usize) -> Self {
    Self {
      queue: Array::new(capacity.max(1)),
      dropped: AtomicU64::new(0),
    }
  }

  /// 尝试发布消息入队（无锁推入，满则丢弃并原子递增溢出计数）
  #[inline]
  pub fn try_publish(&self, message: PubSubMessage) -> bool {
    if self.queue.push(message).is_err() {
      self.dropped.fetch_add(1, Relaxed);
      false
    } else {
      true
    }
  }

  /// 取走全部待投递消息（会话线程收敛点）
  #[inline]
  pub fn drain(&self) -> Vec<PubSubMessage> {
    let mut msgs = Vec::with_capacity(self.len());
    self.drain_into(&mut msgs);
    msgs
  }

  /// 取走全部待投递消息排入指定缓冲中，返回排出的消息数（复用外部缓冲）
  #[inline]
  pub fn drain_into(&self, buf: &mut Vec<PubSubMessage>) -> usize {
    let pending = self.queue.len();
    if pending > 0 {
      buf.reserve(pending);
    }
    let start_len = buf.len();
    while let Some(msg) = self.queue.pop() {
      buf.push(msg);
    }
    buf.len() - start_len
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

  /// 累计溢出丢弃数
  #[inline]
  pub fn dropped_count(&self) -> u64 {
    self.dropped.load(Relaxed)
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
    assert_eq!(messages[0].channel.as_ref(), b"a");
    assert_eq!(messages[1].channel.as_ref(), b"b");
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
    let messages = mailbox.drain();
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
