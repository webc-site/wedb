//! 发布订阅中枢（对标 libs/server/PubSub/SubscribeBroker.cs:SubscribeBroker）
//!
//! C# 以 TsavoriteLog 专日志承载待投递负载、由后台迭代器回调解码分发；
//! Rust 托管面以待发队列承接同一管线且不再落日志：[`SubscribeBroker::publish`]
//! 入队（对标 aof.Enqueue），[`SubscribeBroker::consume_pending`] 出队并广播
//! （对标 Broadcast）。订阅者集合的并发结构
//! （ConcurrentDictionary + ReadOptimizedConcurrentSet）以 papaya 无锁
//! 并发表承接，广播遍历不阻塞订阅变更。
//!
//! 支持标准通道订阅（SUBSCRIBE/UNSUBSCRIBE/PUBLISH）、模式订阅（PSUBSCRIBE/PUNSUBSCRIBE）
//! 以及分片订阅（SSUBSCRIBE/SUNSUBSCRIBE/SPUBLISH）。分片订阅与标准/模式订阅在路由上严格隔离。

use std::{
  sync::{
    Arc,
    atomic::{
      AtomicBool,
      Ordering::{Acquire, Release},
    },
  },
  time::Duration,
};

use crossfire::flavor::{List, Queue};
use event_listener::{Event, Listener};
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

/// 一条待分发负载：通道 / 负载字节对 + 投递域标记
///
/// C# TsavoriteLog Enqueue 的负载形状为通道/负载对；C# 订阅图仅一张
/// （SSUBSCRIBE 复用普通频道图）故无域概念，rust 三表分离后以域标记
/// 指明消费侧投给哪张表，杜绝跨节点 SPUBLISH 落地普通图串台
enum PendingEntry {
  /// 普通域：投通道与模式订阅表（broadcast）
  Standard(Box<[u8]>, Box<[u8]>),
  /// 分片域：仅投分片订阅表（broadcast_shard）
  Shard(Box<[u8]>, Box<[u8]>),
}

/// 收口等待消费循环退出的时间上界（C# `done.WaitOne()` 无超时；rust 以有界
/// 等待防停机链挂死，超时放行由宿主告警）
const CONSUMER_EXIT_TIMEOUT: Duration = Duration::from_secs(10);

/// 写出订阅表中有订阅者的通道为 RESP 数组（单锁两趟遍历，零临时堆分配）
///
/// `write_channels` 的单点实现：订阅表以入参给出，遍历与判定只有一处定义。
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
  let pin = subscriptions.pin();
  let count = pin
    .iter()
    .filter(|(channel, set)| {
      !set.is_empty() && matches_channel(channel, caller_prefix, pattern).is_some()
    })
    .count();
  output.write_resp_array_len(count);
  if count > 0 {
    output.reserve(count * 24);
    for (channel, set) in pin.iter() {
      if !set.is_empty()
        && let Some(raw) = matches_channel(channel, caller_prefix, pattern)
      {
        output.write_resp_bulk_string(raw);
      }
    }
  }
}

/// 发布订阅中枢
pub struct SubscribeBroker<S = Arc<PubSubMailbox>> {
  /// 通道订阅表（C# subscriptions）
  subscriptions: ChannelSubscriptions<S>,
  /// 模式订阅表（C# patternSubscriptions）
  pattern_subscriptions: PatternSubscriptions<S>,
  /// 分片通道订阅表（C# shardSubscriptions / Sharded PubSub）
  shard_subscriptions: ChannelSubscriptions<S>,
  /// 待分发队列（基于 crossfire::flavor::List 的无锁待发链表，push 0 锁 0 CAS 自旋损耗）
  pending_queue: List<PendingEntry>,
  /// 待发就绪事件脉冲（唤醒等待的后台消费任务）
  pending_event: Event,
  /// 后台消费循环在跑标志（C# done 的状态面：`done.Reset()` 置位 / `done.Set()` 清零；
  /// false = 无消费任务或已退出，收口等待直接放行）
  consumer_live: AtomicBool,
  /// 消费循环退出通知（C# done 的唤醒面：`done.Set()` 唤醒收口等待方）
  consumer_done: Event,
  /// 是否已释放（C# disposed）
  disposed: AtomicBool,
}

impl<S: PubSubSink> SubscribeBroker<S> {
  /// 构造中枢
  ///
  /// C# 的构造入参（日志目录、专日志页大小、纪元）属 TsavoriteLog 介质面，
  /// Rust 待发队列天然从零开始、不落日志，故无需任何装配参数。
  pub fn new() -> Self {
    Self {
      subscriptions: new_concurrent_map(),
      pattern_subscriptions: new_concurrent_map(),
      shard_subscriptions: new_concurrent_map(),
      pending_queue: List::new(),
      pending_event: Event::new(),
      consumer_live: AtomicBool::new(false),
      consumer_done: Event::new(),
      disposed: AtomicBool::new(false),
    }
  }

  /// 移除某会话的全部订阅（会话释放时调用，包含通道、模式与分片订阅）
  ///
  /// libs/server/PubSub/SubscribeBroker.cs:RemoveSubscription
  pub fn remove_subscription(&self, subscriber: u64) {
    if self.is_idle() {
      return;
    }
    if !self.subscriptions.is_empty() {
      let subscriptions = self.subscriptions.pin();
      subscriptions.retain(|_channel, set| {
        set.pin().remove(&subscriber);
        !set.is_empty()
      });
    }

    if !self.pattern_subscriptions.is_empty() {
      let patterns = self.pattern_subscriptions.pin();
      patterns.retain(|_pattern, entry| {
        entry.subscriptions.pin().remove(&subscriber);
        !entry.subscriptions.is_empty()
      });
    }

    if !self.shard_subscriptions.is_empty() {
      let shards = self.shard_subscriptions.pin();
      shards.retain(|_channel, set| {
        set.pin().remove(&subscriber);
        !set.is_empty()
      });
    }
  }

  /// 广播一条消息给通道与模式订阅者，返回通知数（路由隔离：不投递分片订阅）
  ///
  /// libs/server/PubSub/SubscribeBroker.cs:Broadcast
  #[inline]
  fn broadcast(&self, key: &[u8], value: &[u8]) -> usize {
    let mut num_subscribers = 0;

    if !self.subscriptions.is_empty() {
      let subscriptions = self.subscriptions.pin();
      if let Some(sessions) = subscriptions.get(key) {
        let sessions_pin = sessions.pin();
        for (_, session) in sessions_pin.iter() {
          session.publish(key, value);
          num_subscribers += 1;
        }
      }
    }

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
    if self.shard_subscriptions.is_empty() {
      return 0;
    }
    let mut num_subscribers = 0;
    let shards = self.shard_subscriptions.pin();
    if let Some(sessions) = shards.get(key) {
      let sessions_pin = sessions.pin();
      for (_, session) in sessions_pin.iter() {
        session.shard_publish(key, value);
        num_subscribers += 1;
      }
    }
    num_subscribers
  }

  /// 消费并广播队列中全部待发消息，返回累计通知数（按条目投递域分派广播面）
  ///
  /// 承接 C# 日志消费回调的逐条目广播职责（出队即 Broadcast，队列载荷已为
  /// 解码后的键值，故无 C# 钉住指针解码臂与 TruncateUntil 截断臂）
  ///
  /// libs/server/PubSub/SubscribeBroker.cs:Consume
  pub fn consume_pending(&self) -> usize {
    if self.disposed.load(Acquire) {
      self.clear();
      return 0;
    }
    if self.is_idle() {
      self.clear();
      return 0;
    }

    let mut total_notified = 0;
    while let Some(entry) = self.pending_queue.pop() {
      total_notified += match entry {
        PendingEntry::Standard(key, value) => self.broadcast(&key, &value),
        PendingEntry::Shard(key, value) => self.broadcast_shard(&key, &value),
      };
      if self.is_idle() {
        self.clear();
        break;
      }
    }
    total_notified
  }

  /// 订阅通道（返回是否为新订阅）
  ///
  /// libs/server/PubSub/SubscribeBroker.cs:Subscribe
  pub fn subscribe(&self, channel: &[u8], subscriber: u64, sink: S) -> bool {
    if self.disposed.load(Acquire) {
      return false;
    }
    let subscriptions = self.subscriptions.pin();
    let sessions = if let Some(sessions) = subscriptions.get(channel) {
      sessions
    } else {
      subscriptions.get_or_insert_with(channel.into(), new_concurrent_map)
    };
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
  pub fn shard_subscribe(&self, channel: &[u8], subscriber: u64, sink: S) -> bool {
    if self.disposed.load(Acquire) {
      return false;
    }
    let shards = self.shard_subscriptions.pin();
    let sessions = if let Some(sessions) = shards.get(channel) {
      sessions
    } else {
      shards.get_or_insert_with(channel.into(), new_concurrent_map)
    };
    sessions.pin().insert(subscriber, sink).is_none()
  }

  /// 退订通道（返回是否确有退订）
  ///
  /// libs/server/PubSub/SubscribeBroker.cs:Unsubscribe
  pub fn unsubscribe(&self, channel: &[u8], subscriber: u64) -> bool {
    if self.subscriptions.is_empty() {
      return false;
    }
    let subscriptions = self.subscriptions.pin();
    if let Some(sessions) = subscriptions.get(channel) {
      let removed = sessions.pin().remove(&subscriber).is_some();
      if removed && sessions.is_empty() {
        subscriptions.remove(channel);
      }
      removed
    } else {
      false
    }
  }

  /// 退订模式（返回是否确有退订）
  ///
  /// libs/server/PubSub/SubscribeBroker.cs:PatternUnsubscribe
  pub fn pattern_unsubscribe(&self, pattern: &[u8], subscriber: u64) -> bool {
    if self.pattern_subscriptions.is_empty() {
      return false;
    }
    let patterns = self.pattern_subscriptions.pin();
    let Some(entry) = patterns.get(pattern) else {
      return false;
    };
    let removed = entry.subscriptions.pin().remove(&subscriber).is_some();
    if removed && entry.subscriptions.is_empty() {
      patterns.remove(pattern);
    }
    removed
  }

  /// 退订分片通道（SUNSUBSCRIBE，返回是否确有退订）
  ///
  /// 分片退订为 rust 自有面（C# SubscribeBroker 无 shard 对应；非分片退订见本文件 Unsubscribe）
  pub fn shard_unsubscribe(&self, channel: &[u8], subscriber: u64) -> bool {
    if self.shard_subscriptions.is_empty() {
      return false;
    }
    let shards = self.shard_subscriptions.pin();
    if let Some(sessions) = shards.get(channel) {
      let removed = sessions.pin().remove(&subscriber).is_some();
      if removed && sessions.is_empty() {
        shards.remove(channel);
      }
      removed
    } else {
      false
    }
  }

  /// 回调枚举全部有订阅者的通道（零堆分配）
  #[inline]
  pub fn for_each_channel<F: FnMut(&[u8])>(&self, mut f: F) {
    if self.subscriptions.is_empty() {
      return;
    }
    let pin = self.subscriptions.pin();
    for (channel, set) in pin.iter() {
      if !set.is_empty() {
        f(channel);
      }
    }
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

  /// 回调枚举全部有分片订阅者的通道（零堆分配）
  #[inline]
  pub fn for_each_shard_channel<F: FnMut(&[u8])>(&self, mut f: F) {
    if self.shard_subscriptions.is_empty() {
      return;
    }
    let pin = self.shard_subscriptions.pin();
    for (channel, set) in pin.iter() {
      if !set.is_empty() {
        f(channel);
      }
    }
  }

  /// 直接向输出缓冲写入 RESP 通道数组（单锁两趟遍历，零临时堆分配）
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
  pub fn list_all_subscriptions(&self) -> Vec<Vec<u8>> {
    let mut res = Vec::with_capacity(self.subscriptions.len());
    self.for_each_channel(|ch| res.push(ch.to_vec()));
    res
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
  pub fn list_all_shard_subscriptions(&self) -> Vec<Vec<u8>> {
    let mut res = Vec::with_capacity(self.shard_subscriptions.len());
    self.for_each_shard_channel(|ch| res.push(ch.to_vec()));
    res
  }

  /// 同步直投：立即广播给全部通道与模式订阅者，返回通知数
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

  /// 异步发布：入队待分发队列（完全无锁 push 入链表，0 锁 0 CAS 自旋损耗，并触发事件通知唤醒后台消费任务）
  ///
  /// libs/server/PubSub/SubscribeBroker.cs:Publish
  #[inline]
  pub fn publish(&self, key: &[u8], value: &[u8]) {
    self.enqueue(PendingEntry::Standard(key.into(), value.into()));
  }

  /// 分片异步发布（跨节点 SPUBLISH 接收侧）：入队待分发队列并标记分片域，
  /// 消费时仅广播给分片订阅者
  ///
  /// 分片入队为 rust 自有面（C# SubscribeBroker 无 shard 对应；普通入队见本文件 Publish）
  #[inline]
  pub fn publish_shard(&self, key: &[u8], value: &[u8]) {
    self.enqueue(PendingEntry::Shard(key.into(), value.into()));
  }

  /// 入队单点：已释放或全空闲早退，否则压链并唤醒后台消费任务
  #[inline]
  fn enqueue(&self, entry: PendingEntry) {
    if self.disposed.load(Acquire) || self.is_idle() {
      return;
    }
    let _ = self.pending_queue.push(entry);
    self.pending_event.notify(1);
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

  /// 异步等待直至待分发队列可能有元素（false = 队列已关闭）
  pub async fn wait_pending(&self) -> bool {
    loop {
      if self.disposed.load(Acquire) {
        return false;
      }
      if !self.pending_queue.is_empty() {
        return true;
      }
      let listener = self.pending_event.listen();
      if self.disposed.load(Acquire) {
        return false;
      }
      if !self.pending_queue.is_empty() {
        return true;
      }
      listener.await;
    }
  }

  /// 排空待分发队列（丢弃积压消息）
  pub fn clear(&self) {
    while self.pending_queue.pop().is_some() {}
  }

  /// 后台消费循环启动挂钩（宿主 spawn 消费任务体首行调用）
  ///
  /// C# Initialize 的 `done.Reset()`：置在跑标志，收口等待自此才有等待对象
  ///
  /// libs/server/PubSub/SubscribeBroker.cs:Initialize
  pub fn consumer_start(&self) {
    self.consumer_live.store(true, Release);
  }

  /// 后台消费循环退出回报（宿主消费任务体退出循环后调用）
  ///
  /// C# StartAsync 的 `finally { done.Set(); }` 半边：清在跑标志并唤醒收口
  /// 等待方；StartAsync 循环本体的锚点在宿主侧 spawn_pubsub_consume_task，
  /// 此处为子步骤不重复登记
  pub fn consumer_finish(&self) {
    self.consumer_live.store(false, Release);
    self.consumer_done.notify(usize::MAX);
  }

  /// 等后台消费循环退出（false = 超时未收敛）
  ///
  /// C# Dispose 的 `done.WaitOne()`：同步等消费循环跑完当前批次退出。差异：
  /// C# 无超时，rust 以 [`CONSUMER_EXIT_TIMEOUT`] 有界等待防停机链挂死。
  /// listen 前后双查在跑标志，杜绝 listen 与退出唤醒擦肩错过
  fn wait_consumer_exit(&self) -> bool {
    while self.consumer_live.load(Acquire) {
      let listener = self.consumer_done.listen();
      if !self.consumer_live.load(Acquire) {
        return true;
      }
      if listener.wait_timeout(CONSUMER_EXIT_TIMEOUT).is_none() {
        return false;
      }
    }
    true
  }

  /// 释放中枢：停止接收、等后台消费循环退出并清空全部订阅与待发队列
  ///
  /// 返回 false = 消费循环未在超时内收敛（订阅表仍被清理，停机继续）
  ///
  /// libs/server/PubSub/SubscribeBroker.cs:Dispose
  pub fn dispose(&self) -> bool {
    self.disposed.store(true, Release);
    // cts.Cancel 的对译：唤醒后台消费任务的 wait_pending 令其退出
    self.pending_event.notify(usize::MAX);
    // done.WaitOne() 的对译：等在途批次跑完，再清表（C# 同一顺序）
    let converged = self.wait_consumer_exit();
    self.clear();
    if !self.subscriptions.is_empty() {
      self.subscriptions.pin().clear();
    }
    if !self.pattern_subscriptions.is_empty() {
      self.pattern_subscriptions.pin().clear();
    }
    if !self.shard_subscriptions.is_empty() {
      self.shard_subscriptions.pin().clear();
    }
    converged
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
  use std::{future::Future, pin::pin, thread::yield_now};

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
  fn publish_shard_enqueue_consumes_only_shard_domain() {
    let f = fixture();
    let mb_shard = Arc::new(PubSubMailbox::new(16));
    let mb_std = Arc::new(PubSubMailbox::new(16));
    f.broker.shard_subscribe(b"0:ch", 1, mb_shard.clone());
    f.broker.subscribe(b"0:ch", 2, mb_std.clone());

    // 分片域入队：消费仅投分片订阅者，普通订阅者不串台
    f.broker.publish_shard(b"0:ch", b"sm");
    assert!(mb_shard.is_empty(), "入队阶段不投递");
    assert_eq!(f.broker.consume_pending(), 1);
    assert_eq!(mb_shard.len(), 1);
    assert_eq!(mb_std.len(), 0);

    // 普通域入队：消费仅投普通订阅者，分片订阅者不串台
    f.broker.publish(b"0:ch", b"pm");
    assert_eq!(f.broker.consume_pending(), 1);
    assert_eq!(mb_std.len(), 1);
    assert_eq!(mb_shard.len(), 1, "分片邮箱保持上一轮消息");

    let mut shard_msgs = Vec::new();
    mb_shard.drain_into(&mut shard_msgs);
    assert_eq!(shard_msgs[0].kind, PubSubMessageKind::Shard);
    assert_eq!(shard_msgs[0].value.as_ref(), b"sm");
    let mut std_msgs = Vec::new();
    mb_std.drain_into(&mut std_msgs);
    assert_eq!(std_msgs[0].value.as_ref(), b"pm");
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
  fn publish_enqueue_then_consume_pending_broadcasts() {
    let f = fixture();
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
    // 释放后入队静默丢弃，消费亦不再分发
    f.broker.publish(b"ch", b"v");
    assert_eq!(f.broker.consume_pending(), 0);
    assert!(f.broker.list_all_subscriptions().is_empty());
    assert!(f.broker.list_all_shard_subscriptions().is_empty());
  }

  #[test]
  fn equals_on_pattern_entry() {
    let a = PatternSubscriptionEntry::new(Box::from(b"ab*".as_slice()));
    let b = PatternSubscriptionEntry::new(Box::from(b"ab*".as_slice()));
    let c = PatternSubscriptionEntry::new(Box::from(b"ba*".as_slice()));
    assert!(a.equals(&b));
    assert!(!a.equals(&c));
  }

  #[test]
  fn publish_variants_and_clear() {
    let f = fixture();
    f.broker.subscribe(b"ch1", 1, f.mailbox.clone());
    f.broker.pattern_subscribe(b"pat.*", 2, f.mailbox.clone());

    f.broker.publish(b"ch1", b"v1");
    f.broker.publish(b"ch1", b"v2");
    f.broker.publish(b"pat.1", b"v3");

    // clear 排空积压，不触发投递
    f.broker.clear();
    assert_eq!(f.broker.consume_pending(), 0);
    assert!(f.mailbox.is_empty());

    // 再次入队验证正常分发
    f.broker.publish(b"ch1", b"v4");
    assert_eq!(f.broker.consume_pending(), 1);
    assert_eq!(f.mailbox.len(), 1);
    let mut buf4 = Vec::new();
    f.mailbox.drain_into(&mut buf4);
    assert_eq!(buf4[0].value.as_ref(), b"v4");
  }

  #[test]
  fn concurrent_publishers_consume_pending() {
    use std::thread;

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
            b.publish(b"bench", b"fast");
          } else {
            b.publish(b"bench", b"chan");
          }
        }
      }));
    }

    for h in handles {
      h.join().unwrap();
    }

    let notified = broker.consume_pending();
    assert_eq!(notified, num_threads * msgs_per_thread);
    assert_eq!(mailbox.len(), num_threads * msgs_per_thread);
    assert_eq!(broker.consume_pending(), 0);
  }

  fn block_on<F: Future>(f: F) -> F::Output {
    use std::task::{Context, Poll, Waker};
    let waker = Waker::noop();
    let mut cx = Context::from_waker(waker);
    let mut f = pin!(f);
    loop {
      if let Poll::Ready(res) = f.as_mut().poll(&mut cx) {
        return res;
      }
      yield_now();
    }
  }

  #[test]
  fn publish_preserves_fifo_order() {
    let f = fixture();
    f.broker.subscribe(b"ch", 1, f.mailbox.clone());

    f.broker.publish(b"ch", b"msg1");
    f.broker.publish(b"ch", b"msg2");
    f.broker.publish(b"ch", b"msg3");

    assert_eq!(f.broker.consume_pending(), 3);
    let mut msgs = Vec::new();
    f.mailbox.drain_into(&mut msgs);
    assert_eq!(msgs.len(), 3);
    assert_eq!(msgs[0].value.as_ref(), b"msg1");
    assert_eq!(msgs[1].value.as_ref(), b"msg2");
    assert_eq!(msgs[2].value.as_ref(), b"msg3");
  }

  #[test]
  fn wait_pending_and_dispose_lifecycle() {
    use std::{thread, time::Duration};

    let f = fixture();
    f.broker.subscribe(b"ch", 1, f.mailbox.clone());
    let broker = Arc::new(f.broker);

    let b1 = broker.clone();
    let handle = thread::spawn(move || {
      thread::sleep(Duration::from_millis(20));
      b1.publish(b"ch", b"wake");
    });

    let ok = block_on(broker.wait_pending());
    assert!(ok);
    assert_eq!(broker.consume_pending(), 1);
    handle.join().unwrap();

    let b2 = broker.clone();
    let handle_dispose = thread::spawn(move || {
      thread::sleep(Duration::from_millis(20));
      b2.dispose();
    });

    let ok_disposed = block_on(broker.wait_pending());
    assert!(!ok_disposed);
    handle_dispose.join().unwrap();

    // 已经 disposed 的中枢，直接调用 wait_pending 立即返回 false
    assert!(!block_on(broker.wait_pending()));
  }

  #[test]
  fn dispose_waits_for_consumer_exit() {
    use std::{thread, time::Duration};

    let f = fixture();
    f.broker.subscribe(b"ch", 1, f.mailbox.clone());
    f.broker.publish(b"ch", b"pending");
    let broker = Arc::new(f.broker);

    // 宿主消费任务形态：consumer_start → 消费循环 → consumer_finish
    let b = broker.clone();
    let handle = thread::spawn(move || {
      b.consumer_start();
      while block_on(b.wait_pending()) {
        b.consume_pending();
      }
      b.consumer_finish();
    });

    // 等消费任务进入等待后再收口，确保等待路径真实覆盖
    thread::sleep(Duration::from_millis(50));
    assert!(broker.dispose());
    handle.join().unwrap();

    // dispose 返回 = 消费循环已退出（C# done.WaitOne 语义）且三面清理完成
    assert!(broker.list_all_subscriptions().is_empty());
    assert_eq!(broker.consume_pending(), 0);
    assert!(broker.is_idle());
  }

  #[test]
  fn dispose_without_consumer_no_wait() {
    let f = fixture();
    f.broker.subscribe(b"ch", 1, f.mailbox.clone());
    // 无消费任务在跑（C# done 初始为 set：WaitOne 直接过）
    assert!(f.broker.dispose());
    assert!(f.broker.list_all_subscriptions().is_empty());
  }
}
