//! 发布订阅中枢（对标 libs/server/PubSub/SubscribeBroker.cs:SubscribeBroker）
//!
//! C# 以 TsavoriteLog 专日志（页边界截断）+ 后台消费任务（StartAsync →
//! ConsumeAllAsync）分发；Rust 托管面以待发队列承载同一管线：
//! [`SubscribeBroker::publish`] 入队（对标 aof.Enqueue），
//! [`SubscribeBroker::consume`] / [`SubscribeBroker::consume_pending`]
//! 解码并广播（对标 Consume → Broadcast）。订阅者集合的并发结构
//! （ConcurrentDictionary + ReadOptimizedConcurrentSet）以 papaya 无锁
//! 并发表承接，广播遍历不阻塞订阅变更。
//!
//! C# 的懒初始化（sid/initialized 竞态协调）在构造即完成的 Rust 结构下
//! 无存在必要；Broadcast 中首达即写的会话输出路径由 [`PubSubSink`] 承接。

use std::{
  collections::VecDeque,
  mem,
  sync::{
    Arc,
    atomic::{
      AtomicBool, AtomicU64,
      Ordering::{Acquire, Relaxed, Release},
    },
  },
};

use parking_lot::Mutex;
use wbase::{
  glob::glob_match,
  map::{ConcurrentMap, new_concurrent_map},
};

use crate::{
  pattern_subscription_entry::{PatternSubscriberSet, PatternSubscriptionEntry},
  subscriber::{PubSubMailbox, PubSubSink},
};

/// 通道订阅表：通道 -> 订阅者集合（C# subscriptions，
/// ConcurrentDictionary<ByteArrayWrapper, ReadOptimizedConcurrentSet<..>>）
type ChannelSubscriptions<S> = ConcurrentMap<Box<[u8]>, PatternSubscriberSet<S>>;

/// 模式订阅表：模式 -> 条目（C# patternSubscriptions，
/// ReadOptimizedConcurrentSet<PatternSubscriptionEntry>；条目按模式字节
/// 相等去重，与 C# Equals(pattern.SequenceEqual) 同一判定，故键化等价）
type PatternSubscriptions<S> = ConcurrentMap<Box<[u8]>, PatternSubscriptionEntry<S>>;

/// 一条待分发负载：通道 / 负载字节对（C# TsavoriteLog Enqueue 的负载形状）
type PendingEntry = (Box<[u8]>, Box<[u8]>);

/// 发布订阅中枢
pub struct SubscribeBroker<S = Arc<PubSubMailbox>> {
  /// 通道订阅表（C# subscriptions）
  subscriptions: ChannelSubscriptions<S>,
  /// 模式订阅表（C# patternSubscriptions）
  pattern_subscriptions: PatternSubscriptions<S>,
  /// 待分发队列（C# pub/sub 专日志 TsavoriteLog 的托管等价）
  pending: Mutex<VecDeque<PendingEntry>>,
  /// 上一次消费的日志地址（C# previousAddress；跳页检测）
  previous_address: AtomicU64,
  /// 专日志页大小位宽（C# pageSizeBits；跳页告警判定用）
  page_size_bits: u32,
  /// 是否已释放（C# disposed）
  disposed: AtomicBool,
}

/// 消息中枢别名（支持单机与集群统一复用）
pub type MessageBroker<S = Arc<PubSubMailbox>> = SubscribeBroker<S>;

impl<S: PubSubSink> SubscribeBroker<S> {
  /// 构造中枢
  ///
  /// `page_size_bytes` 为分发日志页大小（C# 构造入参 pageSize，取 2 的幂
  /// 位宽供跳页告警判定）。C# 的设备 / TsavoriteLog 初始化与
  /// TruncateUntil(CommittedUntilAddress) 属存储介质面，托管队列天然从零开始。
  pub fn new(page_size_bytes: usize) -> Self {
    Self {
      subscriptions: new_concurrent_map(),
      pattern_subscriptions: new_concurrent_map(),
      pending: Mutex::new(VecDeque::new()),
      previous_address: AtomicU64::new(0),
      page_size_bits: page_size_bytes.max(2).ilog2(),
      disposed: AtomicBool::new(false),
    }
  }

  /// 移除某会话的全部订阅（会话释放时调用）
  ///
  /// libs/server/PubSub/SubscribeBroker.cs:RemoveSubscription
  ///
  /// C# 只摘会话、保留空集（查询面以 Count > 0 过滤兜底）；此处顺势清理
  /// 空条目，观测语义不变（全部读取口本就过滤空集）。
  pub fn remove_subscription(&self, subscriber: u64) {
    let subscriptions = self.subscriptions.pin();
    for (_, set) in subscriptions.iter() {
      set.pin().remove(&subscriber);
    }
    subscriptions.retain(|_channel, set| !set.is_empty());

    let patterns = self.pattern_subscriptions.pin();
    for (_, entry) in patterns.iter() {
      entry.subscriptions.pin().remove(&subscriber);
    }
    patterns.retain(|_pattern, entry| !entry.subscriptions.is_empty());
  }

  /// 广播一条消息给通道与模式订阅者，返回通知数
  ///
  /// libs/server/PubSub/SubscribeBroker.cs:Broadcast
  fn broadcast(&self, key: &[u8], value: &[u8]) -> usize {
    let mut num_subscribers = 0;

    let subscriptions = self.subscriptions.pin();
    if let Some(sessions) = subscriptions.get(key) {
      for (_, session) in sessions.pin().iter() {
        session.publish(key, value);
        num_subscribers += 1;
      }
    }

    for (_, entry) in self.pattern_subscriptions.pin().iter() {
      if glob_match(&entry.pattern, key) {
        for (_, session) in entry.subscriptions.pin().iter() {
          session.pattern_publish(&entry.pattern, key, value);
          num_subscribers += 1;
        }
      }
    }
    num_subscribers
  }

  /// 消费一条分发日志负载并广播，返回通知数
  ///
  /// libs/server/PubSub/SubscribeBroker.cs:Consume
  ///
  /// 负载布局与 C# 指针解码一致：`[i32 keyLen][key][i32 valueLen][value]`。
  /// `current_address` / `next_address` 供跳页检测与截断水位推进
  /// （C# aof.TruncateUntil(nextAddress)；托管队列即消费即出队，
  /// 截断水位仅作地址推进记录）。`is_protected` 在 C# 侧亦未参与逻辑。
  pub fn consume(&self, payload: &[u8], current_address: u64, next_address: u64) -> usize {
    if self.disposed.load(Acquire) {
      return 0;
    }

    // 地址水位仅作跳页告警判定（C# 单消费线程直读直写的托管对应）
    let previous_address = self.previous_address.load(Relaxed);
    if previous_address > 0 && current_address > previous_address {
      let page_mask = (1usize << self.page_size_bits) as u64 - 1;
      let payload_len = payload.len() as u64;
      // 跳页检测：非页边界跳转，或越过期望的下一地址（C# 同款两条件）
      if (current_address & page_mask) != 0 || current_address >= previous_address + payload_len {
        log::warn!("SubscribeBroker: Skipping from {previous_address} to {current_address}");
      }
    }

    let Some((key, value)) = decode_payload(payload) else {
      log::warn!("SubscribeBroker.Consume: malformed payload at {current_address}");
      return 0;
    };
    let notified = self.broadcast(&key, &value);
    self.previous_address.store(next_address, Relaxed);
    notified
  }

  /// 消费并广播队列中全部待发消息，返回累计通知数
  ///
  /// C# 后台消费循环（StartAsync → ConsumeAllAsync → Consume）的同步收敛点：
  /// 发布路径即时入队，宿主会话线程 / 定时任务经此完成分发。整队摘出后
  /// 再广播，发布者在广播期间不被排队锁阻塞。
  pub fn consume_pending(&self) -> usize {
    if self.disposed.load(Acquire) {
      return 0;
    }
    let pending = mem::take(&mut *self.pending.lock());
    pending
      .iter()
      .fold(0, |n, (key, value)| n + self.broadcast(key, value))
  }

  /// 订阅通道（返回是否为新订阅）
  ///
  /// libs/server/PubSub/SubscribeBroker.cs:Subscribe
  pub fn subscribe(&self, channel: &[u8], subscriber: u64, sink: S) -> bool {
    if self.disposed.load(Acquire) {
      return false;
    }
    let subscriptions = self.subscriptions.pin();
    let sessions = subscriptions.get_or_insert_with(channel.into(), new_concurrent_map);
    sessions.pin().insert(subscriber, sink).is_none()
  }

  /// 订阅模式（返回是否为新订阅；同模式复用同一条目）
  ///
  /// libs/server/PubSub/SubscribeBroker.cs:PatternSubscribe
  pub fn pattern_subscribe(&self, pattern: &[u8], subscriber: u64, sink: S) -> bool {
    if self.disposed.load(Acquire) {
      return false;
    }
    let patterns = self.pattern_subscriptions.pin();
    let entry = patterns.get_or_insert_with(pattern.into(), || {
      PatternSubscriptionEntry::with_sink(pattern.into())
    });
    entry.subscriptions.pin().insert(subscriber, sink).is_none()
  }

  /// 退订通道（返回是否确有退订）
  ///
  /// libs/server/PubSub/SubscribeBroker.cs:Unsubscribe
  ///
  /// C# 保留空集由查询面过滤；此处退订后顺势清理空条目，观测语义不变。
  pub fn unsubscribe(&self, channel: &[u8], subscriber: u64) -> bool {
    let subscriptions = self.subscriptions.pin();
    let removed = subscriptions
      .get(channel)
      .is_some_and(|sessions| sessions.pin().remove(&subscriber).is_some());
    if removed {
      subscriptions.retain(|_channel, set| !set.is_empty());
    }
    removed
  }

  /// 退订模式（返回是否确有退订）
  ///
  /// libs/server/PubSub/SubscribeBroker.cs:PatternUnsubscribe
  pub fn pattern_unsubscribe(&self, pattern: &[u8], subscriber: u64) -> bool {
    let patterns = self.pattern_subscriptions.pin();
    let Some(entry) = patterns.get(pattern) else {
      return false;
    };
    let removed = entry.subscriptions.pin().remove(&subscriber).is_some();
    if removed && entry.subscriptions.is_empty() {
      patterns.retain(|_pattern, entry| !entry.subscriptions.is_empty());
    }
    removed
  }

  /// 列出全部有订阅者的通道
  ///
  /// libs/server/PubSub/SubscribeBroker.cs:ListAllSubscriptions
  ///
  /// C# 签名携带 session 但遍历全表（未按会话过滤），此处 1:1 保留该语义。
  pub fn list_all_subscriptions(&self) -> Vec<Vec<u8>> {
    self
      .subscriptions
      .pin()
      .iter()
      .filter(|(_, set)| !set.is_empty())
      .map(|(channel, _)| channel.to_vec())
      .collect()
  }

  /// 列出全部有订阅者的模式
  ///
  /// libs/server/PubSub/SubscribeBroker.cs:ListAllPatternSubscriptions
  pub fn list_all_pattern_subscriptions(&self) -> Vec<Vec<u8>> {
    self
      .pattern_subscriptions
      .pin()
      .iter()
      .filter(|(_, entry)| !entry.subscriptions.is_empty())
      .map(|(pattern, _)| pattern.to_vec())
      .collect()
  }

  /// 同步直投：立即广播给全部订阅者，返回通知数
  ///
  /// libs/server/PubSub/SubscribeBroker.cs:PublishNow
  pub fn publish_now(&self, key: &[u8], value: &[u8]) -> usize {
    if self.is_idle() {
      return 0;
    }
    self.broadcast(key, value)
  }

  /// 异步发布：入队待分发队列
  ///
  /// libs/server/PubSub/SubscribeBroker.cs:Publish
  pub fn publish(&self, key: &[u8], value: &[u8]) {
    if self.is_idle() {
      return;
    }
    self.pending.lock().push_back((key.into(), value.into()));
  }

  /// 列出全部有订阅者的通道（PUBSUB CHANNELS 无参形态）
  ///
  /// libs/server/PubSub/SubscribeBroker.cs:GetChannels
  pub fn get_channels(&self) -> Vec<Vec<u8>> {
    self.list_all_subscriptions()
  }

  /// 列出匹配给定模式的全部有订阅者通道
  ///
  /// 对应 GetChannels pattern 过滤重载
  pub fn get_channels_matching(&self, pattern: &[u8]) -> Vec<Vec<u8>> {
    self
      .subscriptions
      .pin()
      .iter()
      .filter(|(channel, set)| !set.is_empty() && glob_match(pattern, channel))
      .map(|(channel, _)| channel.to_vec())
      .collect()
  }

  /// 模式订阅数（PUBSUB NUMPAT）
  ///
  /// libs/server/PubSub/SubscribeBroker.cs:NumPatternSubscriptions
  pub fn num_pattern_subscriptions(&self) -> usize {
    self
      .pattern_subscriptions
      .pin()
      .iter()
      .filter(|(_, entry)| !entry.subscriptions.is_empty())
      .count()
  }

  /// 指定通道的订阅者数（PUBSUB NUMSUB）
  ///
  /// libs/server/PubSub/SubscribeBroker.cs:NumSubscriptions
  pub fn num_subscriptions(&self, channel: &[u8]) -> usize {
    self
      .subscriptions
      .pin()
      .get(channel)
      .map_or(0, PatternSubscriberSet::len)
  }

  /// 释放中枢：停止接收并清空全部订阅与待发队列
  ///
  /// libs/server/PubSub/SubscribeBroker.cs:Dispose
  pub fn dispose(&self) {
    self.disposed.store(true, Release);
    self.pending.lock().clear();
    self.subscriptions.pin().clear();
    self.pattern_subscriptions.pin().clear();
  }

  /// 是否无任何订阅（C# `subscriptions == null && patternSubscriptions == null`
  /// 的空表等价判定；发布路径的提前返回条件）
  fn is_idle(&self) -> bool {
    self.subscriptions.is_empty() && self.pattern_subscriptions.is_empty()
  }
}

/// 解码分发日志负载：`[i32 keyLen][key][i32 valueLen][value]`（小端）
fn decode_payload(payload: &[u8]) -> Option<PendingEntry> {
  let mut cursor = 0usize;
  let read_len = |payload: &[u8], cursor: &mut usize| -> Option<usize> {
    let head = payload.get(*cursor..cursor.checked_add(4)?)?;
    *cursor += 4;
    let len = i32::from_le_bytes(head.try_into().ok()?);
    usize::try_from(len).ok()
  };

  let key_len = read_len(payload, &mut cursor)?;
  let key = payload.get(cursor..cursor.checked_add(key_len)?)?;
  cursor += key_len;
  let value_len = read_len(payload, &mut cursor)?;
  let value = payload.get(cursor..cursor.checked_add(value_len)?)?;
  Some((key.into(), value.into()))
}

#[cfg(test)]
mod tests {
  use super::*;
  use crate::subscriber::PubSubMailbox;

  struct Fixture {
    broker: SubscribeBroker,
    mailbox: Arc<PubSubMailbox>,
  }

  fn fixture(page_size: usize) -> Fixture {
    let broker = SubscribeBroker::new(page_size);
    let mailbox = Arc::new(PubSubMailbox::new(16));
    Fixture { broker, mailbox }
  }

  #[test]
  fn subscribe_unsubscribe_channel_lifecycle() {
    let f = fixture(4096);
    assert!(f.broker.subscribe(b"news", 1, f.mailbox.clone()));
    // 重复订阅幂等（C# TryAdd 返回 false）
    assert!(!f.broker.subscribe(b"news", 1, f.mailbox.clone()));
    assert_eq!(f.broker.num_subscriptions(b"news"), 1);
    assert_eq!(f.broker.get_channels(), vec![b"news".to_vec()]);

    assert!(f.broker.unsubscribe(b"news", 1));
    assert!(!f.broker.unsubscribe(b"news", 1));
    assert_eq!(f.broker.num_subscriptions(b"news"), 0);
    assert!(f.broker.get_channels().is_empty());
  }

  #[test]
  fn pattern_subscribe_and_broadcast_match() {
    let f = fixture(4096);
    assert!(f.broker.pattern_subscribe(b"news.*", 1, f.mailbox.clone()));
    assert_eq!(f.broker.num_pattern_subscriptions(), 1);
    assert_eq!(
      f.broker.list_all_pattern_subscriptions(),
      vec![b"news.*".to_vec()]
    );

    let notified = f.broker.publish_now(b"news.tech", b"hello");
    assert_eq!(notified, 1);
    let messages = f.mailbox.drain();
    assert_eq!(messages.len(), 1);
    assert_eq!(messages[0].channel.as_ref(), b"news.tech");

    // 不命中模式：零通知
    assert_eq!(f.broker.publish_now(b"other", b"x"), 0);
    assert!(f.broker.pattern_unsubscribe(b"news.*", 1));
    assert_eq!(f.broker.num_pattern_subscriptions(), 0);
  }

  #[test]
  fn publish_now_reaches_channel_subscribers() {
    let f = fixture(4096);
    assert_eq!(f.broker.publish_now(b"ch", b"v"), 0);
    f.broker.subscribe(b"ch", 7, f.mailbox.clone());
    assert_eq!(f.broker.publish_now(b"ch", b"v"), 1);
    let messages = f.mailbox.drain();
    assert_eq!(messages[0].value.as_ref(), b"v");
  }

  #[test]
  fn publish_enqueue_then_consume_pending_broadcasts() {
    let f = fixture(4096);
    f.broker.subscribe(b"ch", 1, f.mailbox.clone());
    f.broker.publish(b"ch", b"queued");
    // 入队阶段不投递
    assert!(f.mailbox.is_empty());
    assert_eq!(f.broker.consume_pending(), 1);
    assert_eq!(f.mailbox.len(), 1);
    // 队列已清空
    assert_eq!(f.broker.consume_pending(), 0);
  }

  #[test]
  fn consume_decodes_length_prefixed_payload() {
    let f = fixture(4096);
    f.broker.subscribe(b"k", 1, f.mailbox.clone());

    let mut payload = Vec::new();
    payload.extend_from_slice(&1i32.to_le_bytes());
    payload.push(b'k');
    payload.extend_from_slice(&2i32.to_le_bytes());
    payload.extend_from_slice(b"v1");

    assert_eq!(f.broker.consume(&payload, 8, 24), 1);
    assert_eq!(f.mailbox.drain()[0].value.as_ref(), b"v1");

    // 跳页告警：非页边界 + 越过期望地址 → 日志路径（返回值不受影响）
    assert_eq!(f.broker.consume(&payload, 100, 132), 1);

    // 畸形负载安全返回
    assert_eq!(f.broker.consume(&[1, 2, 3], 200, 208), 0);
  }

  #[test]
  fn get_channels_matching_filters_by_glob() {
    let f = fixture(4096);
    f.broker.subscribe(b"apple", 1, f.mailbox.clone());
    f.broker.subscribe(b"banana", 2, f.mailbox.clone());
    assert_eq!(
      f.broker.get_channels_matching(b"a*"),
      vec![b"apple".to_vec()]
    );
    assert_eq!(f.broker.get_channels_matching(b"*").len(), 2);
  }

  #[test]
  fn remove_subscription_clears_all_kinds() {
    let f = fixture(4096);
    f.broker.subscribe(b"ch", 1, f.mailbox.clone());
    f.broker.pattern_subscribe(b"p*", 1, f.mailbox.clone());
    f.broker.remove_subscription(1);
    assert!(f.broker.get_channels().is_empty());
    assert_eq!(f.broker.num_pattern_subscriptions(), 0);
  }

  #[test]
  fn dispose_rejects_further_operations() {
    let f = fixture(4096);
    f.broker.subscribe(b"ch", 1, f.mailbox.clone());
    f.broker.dispose();
    assert!(!f.broker.subscribe(b"ch2", 2, f.mailbox.clone()));
    assert_eq!(f.broker.publish_now(b"ch", b"v"), 0);
    // 释放后入队静默丢弃，消费亦不再分发
    f.broker.publish(b"ch", b"v");
    assert_eq!(f.broker.consume_pending(), 0);
    assert!(f.broker.get_channels().is_empty());
  }

  #[test]
  fn equals_on_pattern_entry() {
    let a = PatternSubscriptionEntry::new(Box::from(b"ab*".as_slice()));
    let b = PatternSubscriptionEntry::new(Box::from(b"ab*".as_slice()));
    let c = PatternSubscriptionEntry::new(Box::from(b"ba*".as_slice()));
    assert!(a.equals(&b));
    assert!(!a.equals(&c));
  }
}
