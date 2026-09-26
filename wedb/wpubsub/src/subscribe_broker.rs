//! 发布订阅中枢（对标 libs/server/PubSub/SubscribeBroker.cs:SubscribeBroker）
//!
//! C# 以 TsavoriteLog 专日志承载待投递负载（Publish 入队 aof）、由常驻
//! StartAsync 后台迭代器回调解码 Consume -> Broadcast 分发；Rust 无该磁盘
//! 日志介质，全部发布路径（含集群接收端 NetworkClusterPublish）统一收敛为
//! [`SubscribeBroker::publish_now`] 同步直投至订阅会话邮箱（对标
//! C# PublishNow，StartAsync/Consume 异步介质面不移植，见
//! js/check/ignore/garnet/libs/server/PubSub/SubscribeBroker.yml）。
//! 订阅者集合的并发结构
//! （ConcurrentDictionary + ReadOptimizedConcurrentSet）以 papaya 无锁
//! 并发表承接，广播遍历不阻塞订阅变更。
//!
//! 支持标准通道订阅（SUBSCRIBE/UNSUBSCRIBE/PUBLISH）、模式订阅（PSUBSCRIBE/PUNSUBSCRIBE）
//! 以及分片订阅（SSUBSCRIBE/SUNSUBSCRIBE/SPUBLISH）。分片订阅与标准/模式订阅在路由上严格隔离。

use std::sync::{
  Arc,
  atomic::{
    AtomicBool,
    Ordering::{Acquire, Release},
  },
};

use smallvec::SmallVec;
use wbase::{
  glob::glob_match,
  map::{ConcurrentMap, new_concurrent_map},
};
use wresp::ext::RespVecExt;

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

/// 通道快照的栈上内联容量（对标 C# GetChannels 物化 `List` 快照；常见通道数
/// 落于此内联域即零堆分配，超出后 SmallVec 自动溢出至堆，不影响正确性）
const CHANNELS_INLINE_CAPACITY: usize = 32;

/// 单个通道 bulk string 帧头的预留字节估算（`$<len>\r\n<name>\r\n` 定长开销上界，
/// 仅用于一次性预留输出容量、避免逐条写出触发的多次扩容，不参与帧内容）
const RESP_BULK_STRING_HEAD_HINT: usize = 24;

/// 写出订阅表中有订阅者的通道为 RESP 数组（单次遍历快照收集，零拷贝借用一致成帧）
///
/// `write_channels` 的单点实现：订阅表以入参给出，遍历与判定只有一处定义。
///
/// 成帧一致性锚点（对标 C# SubscribeBroker.cs:GetChannels 先物化
/// `List<ByteArrayWrapper>` 快照、PubSubCommands.cs:NetworkPUBSUB_CHANNELS 再以
/// 同一份 `channels.Count` 落长度头、`foreach` 写元素）：以单趟 `pin.iter()` 收集
/// 满足条件的通道裸名借用切片入 `matched`，长度前缀与实际写出元素严格取同一
/// `matched`。杜绝双趟遍历之间并发 subscribe/unsubscribe/remove_subscription
/// 使内层集合 `is_empty` 翻转或外层表增删键，造成 RESP 数组长度与实际元素数脱节、
/// 帧损坏与连接串包。
///
/// namespace 隔离承载点（见 [`crate::channel_ns`]）：订阅表键为隔离键
/// `[ns 前缀] + [裸通道]`，`caller_prefix` 为本会话隔离前缀——前缀不命中即他 ns，
/// 过滤不可见；命中则按剥离后的裸通道应用用户 glob 模式并写出裸通道名。
/// 空前缀 = 不过滤（ns 无关的全局视图，broker 自测面用）。
#[inline]
fn write_channel_array<S>(
  output: &mut Vec<u8>,
  subscriptions: &ChannelSubscriptions<S>,
  caller_prefix: &[u8],
  pattern: Option<&[u8]>,
) {
  if subscriptions.is_empty() {
    output.write_resp_array_len(0);
    return;
  }
  // 归属判定 + 用户模式过滤：裸名生命周期显式随入参通道，闭包无法表达该高阶约束
  fn matches_channel<'a>(
    channel: &'a [u8],
    caller_prefix: &[u8],
    pattern: Option<&[u8]>,
  ) -> Option<&'a [u8]> {
    channel
      .strip_prefix(caller_prefix)
      .filter(|raw| pattern.is_none_or(|pat| glob_match(pat, raw)))
  }
  // 单趟收集快照：借用字典内已有键剥离后的裸名切片（生命周期随 pin 钉住窗口），
  // 与长度头共用同一 matched，构成唯一真源
  let pin = subscriptions.pin();
  let mut matched: SmallVec<[&[u8]; CHANNELS_INLINE_CAPACITY]> = SmallVec::new();
  for (channel, set) in pin.iter() {
    if !set.is_empty()
      && let Some(raw) = matches_channel(channel, caller_prefix, pattern)
    {
      matched.push(raw);
    }
  }
  output.write_resp_array_len(matched.len());
  if !matched.is_empty() {
    output.reserve(matched.len() * RESP_BULK_STRING_HEAD_HINT);
    for raw in matched {
      output.write_resp_bulk_string(raw);
    }
  }
}

#[inline]
fn channel_sub_insert<S: PubSubSink>(
  subs: &ChannelSubscriptions<S>,
  channel: &[u8],
  subscriber: u64,
  sink: S,
) -> bool {
  let pin = subs.pin();
  let sessions = if let Some(sessions) = pin.get(channel) {
    sessions
  } else {
    pin.get_or_insert_with(channel.into(), new_concurrent_map)
  };
  sessions.pin().insert(subscriber, sink).is_none()
}

#[inline]
fn channel_sub_remove<S>(subs: &ChannelSubscriptions<S>, channel: &[u8], subscriber: u64) -> bool {
  if subs.is_empty() {
    return false;
  }
  let pin = subs.pin();
  pin
    .get(channel)
    .is_some_and(|sessions| sessions.pin().remove(&subscriber).is_some())
}

#[inline]
fn channel_sub_for_each<S, F: FnMut(&[u8])>(subs: &ChannelSubscriptions<S>, mut f: F) {
  if subs.is_empty() {
    return;
  }
  let pin = subs.pin();
  for (channel, set) in pin.iter() {
    if !set.is_empty() {
      f(channel);
    }
  }
}

#[inline]
fn channel_sub_list_all<S>(subs: &ChannelSubscriptions<S>) -> Vec<Vec<u8>> {
  let mut res = Vec::with_capacity(subs.len());
  channel_sub_for_each(subs, |ch| res.push(ch.to_vec()));
  res
}

#[inline]
fn channel_sub_broadcast<S: PubSubSink>(
  subs: &ChannelSubscriptions<S>,
  key: &[u8],
  deliver: impl Fn(&S),
) -> usize {
  if subs.is_empty() {
    return 0;
  }
  let mut count = 0;
  let pin = subs.pin();
  if let Some(sessions) = pin.get(key) {
    let sessions_pin = sessions.pin();
    for (_, session) in sessions_pin.iter() {
      deliver(session);
      count += 1;
    }
  }
  count
}

/// 发布订阅中枢
pub struct SubscribeBroker<S = Arc<PubSubMailbox>> {
  /// 通道订阅表（C# subscriptions）
  subscriptions: ChannelSubscriptions<S>,
  /// 模式订阅表（C# patternSubscriptions）
  pattern_subscriptions: PatternSubscriptions<S>,
  /// 分片通道订阅表（Rust 自有设计，C# SubscribeBroker 无分片域；
  /// 见 doc/zh/deviations.md §6）
  shard_subscriptions: ChannelSubscriptions<S>,
  /// 是否已释放（C# disposed）
  disposed: AtomicBool,
}

impl<S: PubSubSink> SubscribeBroker<S> {
  /// 构造中枢
  ///
  /// C# 的构造入参（日志目录、专日志页大小、纪元）属 TsavoriteLog 介质面，
  /// Rust 统一同步直投、不落日志，故无需任何装配参数。
  pub fn new() -> Self {
    Self {
      subscriptions: new_concurrent_map(),
      pattern_subscriptions: new_concurrent_map(),
      shard_subscriptions: new_concurrent_map(),
      disposed: AtomicBool::new(false),
    }
  }

  /// 移除某会话的全部订阅（会话释放时调用，包含通道、模式与分片订阅）
  ///
  /// libs/server/PubSub/SubscribeBroker.cs:RemoveSubscription
  ///
  /// 逐通道 / 逐模式仅摘内层集合元素，绝不删外层字典键：C# 对每个键调用
  /// `Unsubscribe`（仅 `sessions.TryRemove(session)`）、对每个模式条目仅
  /// `entry.subscriptions.TryRemove(session)`，外层键生命周期稳定驻留。
  /// 空集合的可见性收口交由读侧 `is_empty` 过滤，杜绝「内层摘除 + 外层删键」
  /// 复合操作与并发 subscribe 之间的非原子竞态（否则会把刚插入的新订阅者
  /// 连同其集合从外层拔除，导致孤儿化与广播静默丢失）。
  pub fn remove_subscription(&self, subscriber: u64) {
    if self.is_idle() {
      return;
    }
    let remove_from = |subs: &ChannelSubscriptions<S>| {
      if !subs.is_empty() {
        let pin = subs.pin();
        for set in pin.values() {
          set.pin().remove(&subscriber);
        }
      }
    };
    remove_from(&self.subscriptions);
    remove_from(&self.shard_subscriptions);

    if !self.pattern_subscriptions.is_empty() {
      let patterns = self.pattern_subscriptions.pin();
      for entry in patterns.values() {
        entry.subscriptions.pin().remove(&subscriber);
      }
    }
  }

  /// 广播一条消息给通道与模式订阅者，返回通知数（路由隔离：不投递分片订阅）
  ///
  /// libs/server/PubSub/SubscribeBroker.cs:Broadcast
  #[inline]
  fn broadcast(&self, key: &[u8], value: &[u8]) -> usize {
    let mut num_subscribers =
      channel_sub_broadcast(&self.subscriptions, key, |s| s.publish(key, value));

    if !self.pattern_subscriptions.is_empty() {
      let patterns = self.pattern_subscriptions.pin();
      for (_, entry) in patterns.iter() {
        // 中枢级模式匹配（C# 私有薄壳 SubscribeBroker.cs:Match，即
        // GlobUtils.Match 直转；原语锚于 wbase/src/glob.rs glob_match）
        // libs/server/PubSub/SubscribeBroker.cs:Match
        if glob_match(&entry.pattern, key) {
          let sessions_pin = entry.subscriptions.pin();
          for (_, session) in sessions_pin.iter() {
            session.pattern_publish(&entry.pattern, key, value);
            num_subscribers += 1;
          }
        }
      }
    }
    num_subscribers
  }

  /// 广播一条分片消息给分片订阅者，返回通知数（路由隔离：仅投递分片订阅）
  ///
  /// 分片隔离广播为 rust 自有面（C# SubscribeBroker 无 shard 对应；非分片广播见本文件 Publish/PublishNow）
  #[inline]
  pub fn broadcast_shard(&self, key: &[u8], value: &[u8]) -> usize {
    channel_sub_broadcast(&self.shard_subscriptions, key, |s| {
      s.shard_publish(key, value)
    })
  }

  /// 订阅通道（返回是否为新订阅）
  ///
  /// libs/server/PubSub/SubscribeBroker.cs:Subscribe
  #[inline]
  pub fn subscribe(&self, channel: &[u8], subscriber: u64, sink: S) -> bool {
    !self.disposed.load(Acquire)
      && channel_sub_insert(&self.subscriptions, channel, subscriber, sink)
  }

  /// 订阅模式（返回是否为新订阅；同模式复用同一条目）
  ///
  /// libs/server/PubSub/SubscribeBroker.cs:PatternSubscribe
  pub fn pattern_subscribe(&self, pattern: &[u8], subscriber: u64, sink: S) -> bool {
    if self.disposed.load(Acquire) {
      return false;
    }
    let patterns = self.pattern_subscriptions.pin();
    let entry = if let Some(entry) = patterns.get(pattern) {
      entry
    } else {
      patterns.get_or_insert_with(pattern.into(), || {
        PatternSubscriptionEntry::with_sink(pattern.into())
      })
    };
    entry.subscriptions.pin().insert(subscriber, sink).is_none()
  }

  /// 订阅分片通道（SSUBSCRIBE，返回是否为新订阅；槽位分片订阅隔离）
  ///
  /// 分片订阅为 rust 自有面（C# SubscribeBroker 无 shard 对应；非分片订阅见本文件 Subscribe）
  #[inline]
  pub fn shard_subscribe(&self, channel: &[u8], subscriber: u64, sink: S) -> bool {
    !self.disposed.load(Acquire)
      && channel_sub_insert(&self.shard_subscriptions, channel, subscriber, sink)
  }

  /// 退订通道（返回是否确有退订）
  ///
  /// libs/server/PubSub/SubscribeBroker.cs:Unsubscribe
  #[inline]
  pub fn unsubscribe(&self, channel: &[u8], subscriber: u64) -> bool {
    channel_sub_remove(&self.subscriptions, channel, subscriber)
  }

  /// 退订模式（返回是否确有退订）
  ///
  /// libs/server/PubSub/SubscribeBroker.cs:PatternUnsubscribe
  ///
  /// 仅摘内层集合元素（C# `entry.subscriptions.TryRemove(session)`），绝不删
  /// 外层模式键，理由同 [`SubscribeBroker::unsubscribe`]。
  pub fn pattern_unsubscribe(&self, pattern: &[u8], subscriber: u64) -> bool {
    if self.pattern_subscriptions.is_empty() {
      return false;
    }
    let patterns = self.pattern_subscriptions.pin();
    let Some(entry) = patterns.get(pattern) else {
      return false;
    };
    entry.subscriptions.pin().remove(&subscriber).is_some()
  }

  /// 退订分片通道（SUNSUBSCRIBE，返回是否确有退订）
  ///
  /// 分片退订为 rust 自有面（C# SubscribeBroker 无 shard 对应；非分片退订见本文件 Unsubscribe）
  #[inline]
  pub fn shard_unsubscribe(&self, channel: &[u8], subscriber: u64) -> bool {
    channel_sub_remove(&self.shard_subscriptions, channel, subscriber)
  }

  /// 回调枚举全部有订阅者的通道（零堆分配）
  #[inline]
  pub fn for_each_channel<F: FnMut(&[u8])>(&self, f: F) {
    channel_sub_for_each(&self.subscriptions, f);
  }

  /// 回调枚举全部有订阅者的模式（零堆分配）
  #[inline]
  pub fn for_each_pattern<F: FnMut(&[u8])>(&self, mut f: F) {
    if self.pattern_subscriptions.is_empty() {
      return;
    }
    let pin = self.pattern_subscriptions.pin();
    for (pattern, entry) in pin.iter() {
      if !entry.subscriptions.is_empty() {
        f(pattern);
      }
    }
  }

  /// 直接向输出缓冲写入 RESP 通道数组（单次遍历快照收集，零拷贝借用一致成帧）
  ///
  /// `caller_prefix` 承载会话 ns 隔离前缀过滤与裸通道名还原（见 [`write_channel_array`]）
  ///
  /// libs/server/PubSub/SubscribeBroker.cs:GetChannels
  pub fn write_channels(&self, output: &mut Vec<u8>, caller_prefix: &[u8], pattern: Option<&[u8]>) {
    write_channel_array(output, &self.subscriptions, caller_prefix, pattern);
  }

  /// 列出全部有订阅者的通道
  ///
  /// libs/server/PubSub/SubscribeBroker.cs:ListAllSubscriptions
  #[inline]
  pub fn list_all_subscriptions(&self) -> Vec<Vec<u8>> {
    channel_sub_list_all(&self.subscriptions)
  }

  /// 列出全部有订阅者的模式
  ///
  /// libs/server/PubSub/SubscribeBroker.cs:ListAllPatternSubscriptions
  pub fn list_all_pattern_subscriptions(&self) -> Vec<Vec<u8>> {
    let mut res = Vec::with_capacity(self.pattern_subscriptions.len());
    self.for_each_pattern(|pat| res.push(pat.to_vec()));
    res
  }

  /// 列出全部有订阅者的分片通道
  ///
  /// 分片通道列表为 rust 自有面（C# SubscribeBroker 无 shard 对应；非分片列表见本文件 GetChannels 族）
  #[inline]
  pub fn list_all_shard_subscriptions(&self) -> Vec<Vec<u8>> {
    channel_sub_list_all(&self.shard_subscriptions)
  }

  /// 同步直投：立即广播给全部通道与模式订阅者，返回通知数
  ///
  /// C# 本地 NetworkPUBLISH 与集群接收端 NetworkClusterPublish 分走
  /// PublishNow / Publish 双路径（后者经 TsavoriteLog 异步消费闭环）；
  /// Rust 无磁盘日志介质，两条路径统一收敛至此单点直投（见 doc/zh/deviations.md §65）
  ///
  /// libs/server/PubSub/SubscribeBroker.cs:PublishNow
  pub fn publish_now(&self, key: &[u8], value: &[u8]) -> usize {
    if self.is_idle() {
      return 0;
    }
    self.broadcast(key, value)
  }

  /// 同步分片直投（SPUBLISH）：立即广播给对应分片通道订阅者，返回通知数（路由隔离）
  ///
  /// 分片同步直投为 rust 自有面（C# SubscribeBroker 无 shard 对应；非分片直投见本文件 PublishNow）
  pub fn publish_shard_now(&self, key: &[u8], value: &[u8]) -> usize {
    if self.shard_subscriptions.is_empty() {
      return 0;
    }
    self.broadcast_shard(key, value)
  }

  /// 模式订阅数（PUBSUB NUMPAT）
  ///
  /// `caller_prefix` 为会话 ns 隔离前缀：仅统计本 ns 模式（空前缀 = 全量，见
  /// [`crate::channel_ns`]）
  ///
  /// libs/server/PubSub/SubscribeBroker.cs:NumPatternSubscriptions
  pub fn num_pattern_subscriptions(&self, caller_prefix: &[u8]) -> usize {
    if self.pattern_subscriptions.is_empty() {
      return 0;
    }
    self
      .pattern_subscriptions
      .pin()
      .iter()
      .filter(|(pattern, entry)| {
        !entry.subscriptions.is_empty() && pattern.starts_with(caller_prefix)
      })
      .count()
  }

  /// 指定通道的订阅者数（PUBSUB NUMSUB）
  ///
  /// libs/server/PubSub/SubscribeBroker.cs:NumSubscriptions
  pub fn num_subscriptions(&self, channel: &[u8]) -> usize {
    if self.subscriptions.is_empty() {
      return 0;
    }
    self
      .subscriptions
      .pin()
      .get(channel)
      .map_or(0, PatternSubscriberSet::len)
  }

  /// 释放中枢：停止接收并清空全部订阅表
  ///
  /// C# Dispose 的 cts.Cancel / done.WaitOne / aof.Dispose / device.Dispose
  /// 均属 TsavoriteLog 介质与后台消费循环的收口面，Rust 无该介质与常驻任务，
  /// 仅余 disposed 翻转与订阅表清理（C# 同一顺序）
  ///
  /// libs/server/PubSub/SubscribeBroker.cs:Dispose
  pub fn dispose(&self) {
    self.disposed.store(true, Release);
    if !self.subscriptions.is_empty() {
      self.subscriptions.pin().clear();
    }
    if !self.pattern_subscriptions.is_empty() {
      self.pattern_subscriptions.pin().clear();
    }
    if !self.shard_subscriptions.is_empty() {
      self.shard_subscriptions.pin().clear();
    }
  }

  /// 是否已停机收口（C# disposed 字段读取面；宿主停机链可观测判据）
  ///
  /// libs/server/PubSub/SubscribeBroker.cs:Dispose
  #[inline]
  pub fn is_disposed(&self) -> bool {
    self.disposed.load(Acquire)
  }

  /// 是否无任何订阅（包含普通通道、模式与分片订阅）
  #[inline]
  pub fn is_idle(&self) -> bool {
    self.subscriptions.is_empty()
      && self.pattern_subscriptions.is_empty()
      && self.shard_subscriptions.is_empty()
  }
}

impl<S: PubSubSink> Default for SubscribeBroker<S> {
  fn default() -> Self {
    Self::new()
  }
}

#[cfg(test)]
mod tests {
  use std::thread;

  use super::*;
  use crate::subscriber::{PubSubMailbox, PubSubMessageKind};

  struct Fixture {
    broker: SubscribeBroker,
    mailbox: Arc<PubSubMailbox>,
  }

  fn fixture() -> Fixture {
    let broker = SubscribeBroker::new();
    let mailbox = Arc::new(PubSubMailbox::new(16));
    Fixture { broker, mailbox }
  }

  #[test]
  fn subscribe_unsubscribe_channel_lifecycle() {
    let f = fixture();
    assert!(f.broker.subscribe(b"news", 1, f.mailbox.clone()));
    // 重复订阅幂等（C# TryAdd 返回 false）
    assert!(!f.broker.subscribe(b"news", 1, f.mailbox.clone()));
    assert_eq!(f.broker.num_subscriptions(b"news"), 1);
    assert_eq!(f.broker.list_all_subscriptions(), vec![b"news".to_vec()]);

    assert!(f.broker.unsubscribe(b"news", 1));
    assert!(!f.broker.unsubscribe(b"news", 1));
    assert_eq!(f.broker.num_subscriptions(b"news"), 0);
    assert!(f.broker.list_all_subscriptions().is_empty());
  }

  #[test]
  fn shard_subscribe_unsubscribe_and_routing_isolation() {
    let f = fixture();
    let mb_shard = Arc::new(PubSubMailbox::new(16));
    let mb_std = Arc::new(PubSubMailbox::new(16));

    // 订阅分片通道与普通通道
    assert!(f.broker.shard_subscribe(b"slot:100", 1, mb_shard.clone()));
    assert!(!f.broker.shard_subscribe(b"slot:100", 1, mb_shard.clone()));
    assert!(f.broker.subscribe(b"slot:100", 2, mb_std.clone()));

    assert_eq!(
      f.broker.list_all_shard_subscriptions(),
      vec![b"slot:100".to_vec()]
    );

    // 1. SPUBLISH 仅路由给分片订阅者（mb_shard），普通订阅者（mb_std）不收到
    let shard_notified = f.broker.publish_shard_now(b"slot:100", b"shard_val");
    assert_eq!(shard_notified, 1);
    assert_eq!(mb_shard.len(), 1);
    assert_eq!(mb_std.len(), 0);

    let mut shard_msgs = Vec::new();
    mb_shard.drain_into(&mut shard_msgs);
    assert_eq!(shard_msgs[0].value.as_ref(), b"shard_val");
    assert_eq!(shard_msgs[0].kind, PubSubMessageKind::Shard);

    // 2. PUBLISH 仅路由给普通订阅者（mb_std），分片订阅者（mb_shard）不收到
    let std_notified = f.broker.publish_now(b"slot:100", b"std_val");
    assert_eq!(std_notified, 1);
    assert_eq!(mb_std.len(), 1);
    assert_eq!(mb_shard.len(), 0);

    // 3. SUNSUBSCRIBE
    assert!(f.broker.shard_unsubscribe(b"slot:100", 1));
    assert!(f.broker.list_all_shard_subscriptions().is_empty());
  }

  #[test]
  fn pattern_subscribe_and_broadcast_match() {
    let f = fixture();
    assert!(f.broker.pattern_subscribe(b"news.*", 1, f.mailbox.clone()));
    assert_eq!(f.broker.num_pattern_subscriptions(b""), 1);
    assert_eq!(
      f.broker.list_all_pattern_subscriptions(),
      vec![b"news.*".to_vec()]
    );

    let mut patterns = Vec::new();
    f.broker.for_each_pattern(|p| patterns.push(p.to_vec()));
    assert_eq!(patterns, vec![b"news.*".to_vec()]);

    let notified = f.broker.publish_now(b"news.tech", b"hello");
    assert_eq!(notified, 1);
    let mut buf = Vec::new();
    assert_eq!(f.mailbox.drain_into(&mut buf), 1);
    assert_eq!(buf[0].channel.as_ref(), b"news.tech");

    // 不命中模式：零通知
    assert_eq!(f.broker.publish_now(b"other", b"x"), 0);
    assert!(f.broker.pattern_unsubscribe(b"news.*", 1));
    assert_eq!(f.broker.num_pattern_subscriptions(b""), 0);
  }

  #[test]
  fn write_channels_and_for_each() {
    let f = fixture();
    f.broker.subscribe(b"chat.room", 1, f.mailbox.clone());
    f.broker.subscribe(b"chat.general", 2, f.mailbox.clone());
    f.broker.subscribe(b"news.tech", 3, f.mailbox.clone());

    let mut channels = Vec::new();
    f.broker.for_each_channel(|c| channels.push(c.to_vec()));
    assert_eq!(channels.len(), 3);

    let mut out = Vec::new();
    f.broker.write_channels(&mut out, b"", None);
    assert!(out.starts_with(b"*3\r\n"));

    let mut out_matched = Vec::new();
    f.broker
      .write_channels(&mut out_matched, b"", Some(b"chat.*"));
    assert!(out_matched.starts_with(b"*2\r\n"));

    let empty = fixture();
    let mut empty_out = Vec::new();
    empty.broker.write_channels(&mut empty_out, b"", None);
    assert_eq!(empty_out, b"*0\r\n");
  }

  #[test]
  fn publish_now_reaches_channel_subscribers() {
    let f = fixture();
    assert_eq!(f.broker.publish_now(b"ch", b"v"), 0);
    f.broker.subscribe(b"ch", 7, f.mailbox.clone());
    assert_eq!(f.broker.publish_now(b"ch", b"v"), 1);
    let mut messages = Vec::new();
    f.mailbox.drain_into(&mut messages);
    assert_eq!(messages[0].value.as_ref(), b"v");
  }

  #[test]
  fn remove_subscription_clears_all_kinds() {
    let f = fixture();
    f.broker.subscribe(b"ch", 1, f.mailbox.clone());
    f.broker.pattern_subscribe(b"p*", 1, f.mailbox.clone());
    f.broker.shard_subscribe(b"sh", 1, f.mailbox.clone());
    f.broker.remove_subscription(1);
    assert!(f.broker.list_all_subscriptions().is_empty());
    assert_eq!(f.broker.num_pattern_subscriptions(b""), 0);
    assert!(f.broker.list_all_shard_subscriptions().is_empty());
  }

  #[test]
  fn dispose_rejects_further_operations() {
    let f = fixture();
    f.broker.subscribe(b"ch", 1, f.mailbox.clone());
    f.broker.shard_subscribe(b"sh", 1, f.mailbox.clone());
    f.broker.dispose();
    assert!(!f.broker.subscribe(b"ch2", 2, f.mailbox.clone()));
    assert!(!f.broker.shard_subscribe(b"sh2", 2, f.mailbox.clone()));
    assert_eq!(f.broker.publish_now(b"ch", b"v"), 0);
    assert_eq!(f.broker.publish_shard_now(b"sh", b"v"), 0);
    assert!(f.broker.list_all_subscriptions().is_empty());
    assert!(f.broker.list_all_shard_subscriptions().is_empty());
    assert!(f.broker.is_idle());
  }

  #[test]
  fn equals_on_pattern_entry() {
    let a: PatternSubscriptionEntry =
      PatternSubscriptionEntry::with_sink(Box::from(b"ab*".as_slice()));
    let b: PatternSubscriptionEntry =
      PatternSubscriptionEntry::with_sink(Box::from(b"ab*".as_slice()));
    let c: PatternSubscriptionEntry =
      PatternSubscriptionEntry::with_sink(Box::from(b"ba*".as_slice()));
    assert!(a.equals(&b));
    assert!(!a.equals(&c));
  }

  #[test]
  fn concurrent_publishers_direct_delivery() {
    let f = fixture();
    let mailbox = Arc::new(PubSubMailbox::new(10_000));
    f.broker.subscribe(b"bench", 1, mailbox.clone());

    let broker = Arc::new(f.broker);
    let num_threads = 4;
    let msgs_per_thread = 250;

    let mut handles = Vec::new();
    for i in 0..num_threads {
      let b = broker.clone();
      handles.push(thread::spawn(move || {
        for j in 0..msgs_per_thread {
          if (i + j) % 2 == 0 {
            b.publish_now(b"bench", b"fast");
          } else {
            b.publish_now(b"bench", b"chan");
          }
        }
      }));
    }

    for h in handles {
      h.join().unwrap();
    }

    // 直投无积压：通知数与邮箱消息数一致
    assert_eq!(mailbox.len(), num_threads * msgs_per_thread);
  }

  #[test]
  fn publish_now_preserves_fifo_order() {
    let f = fixture();
    f.broker.subscribe(b"ch", 1, f.mailbox.clone());

    assert_eq!(f.broker.publish_now(b"ch", b"msg1"), 1);
    assert_eq!(f.broker.publish_now(b"ch", b"msg2"), 1);
    assert_eq!(f.broker.publish_now(b"ch", b"msg3"), 1);
    let mut msgs = Vec::new();
    f.mailbox.drain_into(&mut msgs);
    assert_eq!(msgs.len(), 3);
    assert_eq!(msgs[0].value.as_ref(), b"msg1");
    assert_eq!(msgs[1].value.as_ref(), b"msg2");
    assert_eq!(msgs[2].value.as_ref(), b"msg3");
  }
}
