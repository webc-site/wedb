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

use papaya::Operation;
use parking_lot::Mutex;
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

/// 单条分片订阅的槽锚记录（挂在订阅者名下：broker 频道外层键只折叠 ns 无
/// db 维度，同频道不同现役库的订阅者锚可异，锚必须随订阅者走）
///
/// 槽位唯一真值源为 `wbase::hash_slot::slot_of(namespace, db)`——锚在
/// SSUBSCRIBE 成功时按订阅时刻的会话域取槽登记，槽事件钩据此对「槽权
/// 离本节点」的命中锚订阅清表推帧（Redis pubsub.c:
/// pubsubShardUnsubscribeAllChannelsInSlot 的订阅生命周期对位，属 rust
/// 自有分片域收口，C# 无对应）
#[derive(Clone)]
struct ShardAnchor<S> {
  /// 订阅时刻的 `slot_of(namespace, active_db)`（订阅生命周期内不随 SELECT 漂移）
  slot: u16,
  /// 频道隔离键（与 `shard_subscriptions` 外层键同构）
  channel: Box<[u8]>,
  /// 订阅者投递面（槽事件时经此推 sunsubscribe 通知）
  sink: S,
}

/// 分片订阅槽锚表：订阅者 -> 其名下全部锚记录（槽事件钩全量扫描，
/// 运维频次路径不设第二套倒排索引）
type ShardSlotAnchors<S> = ConcurrentMap<u64, Vec<ShardAnchor<S>>>;

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
  deliver: impl Fn(&S) -> bool,
) -> usize {
  if subs.is_empty() {
    return 0;
  }
  let mut count = 0;
  let pin = subs.pin();
  if let Some(sessions) = pin.get(key) {
    let sessions_pin = sessions.pin();
    for (_, session) in sessions_pin.iter() {
      if deliver(session) {
        count += 1;
      }
    }
  }
  count
}

#[inline]
fn channel_sub_count<S>(subs: &ChannelSubscriptions<S>, channel: &[u8]) -> usize {
  if subs.is_empty() {
    return 0;
  }
  subs.pin().get(channel).map_or(0, PatternSubscriberSet::len)
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
  /// 分片订阅槽锚表（订阅者维度；槽事件收口的判据面，见 [`ShardAnchor`]）
  shard_slot_anchors: ShardSlotAnchors<S>,
  /// 分片管理面串行闸：SSUBSCRIBE 的「登表 + 登锚」两步对槽迁移收口钩
  /// （shard_slot_migrated_out 键快照 → 摘锚 → 清表推帧）必须整体原子，
  /// 否则撤销钩快照落于两步之间即新订阅逃逸清表与 sunsubscribe 通知——
  /// 槽权已离节点上的僵尸订阅（本地照投、分片定向新属主不投，同频道双
  /// 订阅图分裂；Redis 单线程下 SETSLOT 与 SSUBSCRIBE 串行无此窗）。闸仅
  /// 覆盖订阅管理冷路径，不触发布热路径；收口钩持闸期间的 sink 控制通知
  /// 为有界等待入列（subscriber.rs shard_forced_unsubscribe 契约），不作
  /// 无界阻塞
  shard_mu: parking_lot::Mutex<()>,
  /// 是否已释放（C# disposed）
  disposed: AtomicBool,
}

impl<S> SubscribeBroker<S> {
  /// 构造中枢
  ///
  /// C# 的构造入参（日志目录、专日志页大小、纪元）属 TsavoriteLog 介质面，
  /// Rust 统一同步直投、不落日志，故无需任何装配参数。
  pub fn new() -> Self {
    Self {
      subscriptions: new_concurrent_map(),
      pattern_subscriptions: new_concurrent_map(),
      shard_subscriptions: new_concurrent_map(),
      shard_slot_anchors: new_concurrent_map(),
      shard_mu: Mutex::new(()),
      disposed: AtomicBool::new(false),
    }
  }
}

impl<S> Default for SubscribeBroker<S> {
  fn default() -> Self {
    Self::new()
  }
}

impl<S: PubSubSink> SubscribeBroker<S> {
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
    // 槽锚随订阅者生命周期整体摘除（会话 dispose / AUTH 换租防御单点）
    self.shard_slot_anchors.pin().remove(&subscriber);

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
        // 空集条目（退订/会话释放仅摘内层、外层键驻留同 C# 形态）本就零
        // 投递，先短路跳过 glob 匹配——与 for_each_pattern /
        // num_pattern_subscriptions 的读侧 is_empty 过滤收敛为同一单源
        // 形态，PUBLISH 热路径发布成本不随历史模式总数无界增长
        // （C# Broadcast 空集条目照跑 Match，此处属 rust 优侧纯增益）
        if !entry.subscriptions.is_empty()
          // 中枢级模式匹配（C# 私有薄壳 SubscribeBroker.cs:Match，即
          // GlobUtils.Match 直转；原语锚于 wbase/src/glob.rs glob_match）
          // libs/server/PubSub/SubscribeBroker.cs:Match
          && glob_match(&entry.pattern, key)
        {
          let sessions_pin = entry.subscriptions.pin();
          for (_, session) in sessions_pin.iter() {
            if session.pattern_publish(&entry.pattern, key, value) {
              num_subscribers += 1;
            }
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
  fn broadcast_shard(&self, key: &[u8], value: &[u8]) -> usize {
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
  /// `slot` 为订阅时刻 `slot_of(namespace, active_db)` 的槽锚（订阅者域单源
  /// 取槽，不新增第二套取槽路径）：新订阅成立即登锚，供槽权离本节点事件的
  /// 收口钩判定命中。登表与登锚经 [`Self::shard_mu`] 串行闸对收口钩整体
  /// 原子。锚登记经 papaya compute 纯替换（闭包只从共享视图构造新值，重试
  /// 安全）。分片订阅为 rust 自有面（C# SubscribeBroker 无 shard 对应；非
  /// 分片订阅见本文件 Subscribe）
  pub fn shard_subscribe(&self, slot: u16, channel: &[u8], subscriber: u64, sink: S) -> bool
  where
    S: Clone,
  {
    if self.disposed.load(Acquire) {
      return false;
    }
    let _shard_mu = self.shard_mu.lock();
    let inserted = channel_sub_insert(&self.shard_subscriptions, channel, subscriber, sink.clone());
    if inserted {
      let anchor = ShardAnchor {
        slot,
        channel: channel.into(),
        sink,
      };
      let anchors = self.shard_slot_anchors.pin();
      anchors.compute(subscriber, |entry| -> Operation<Vec<ShardAnchor<S>>, ()> {
        match entry {
          Some((_, v)) => {
            // 同订阅者同频道去重（重复订阅幂等；摘锚摘空后重新订阅即新登）
            let mut next = v.clone();
            if !next
              .iter()
              .any(|a| a.slot == slot && a.channel.as_ref() == channel)
            {
              next.push(anchor.clone());
            }
            Operation::Insert(next)
          }
          None => Operation::Insert(vec![anchor.clone()]),
        }
      });
    }
    inserted
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

  /// 退订分片通道（SUNSUBSCRIBE，返回是否确有退订；成功即同步摘锚，
  /// 杜绝陈旧锚在后续槽事件误清新订阅）
  ///
  /// 分片退订为 rust 自有面（C# SubscribeBroker 无 shard 对应；非分片退订见本文件 Unsubscribe）
  pub fn shard_unsubscribe(&self, channel: &[u8], subscriber: u64) -> bool
  where
    S: Clone,
  {
    // 串行闸：摘表与摘锚对订阅装配/迁移收口原子（见 [`Self::shard_mu`] 字段注）
    let _shard_mu = self.shard_mu.lock();
    let removed = channel_sub_remove(&self.shard_subscriptions, channel, subscriber);
    if removed {
      let anchors = self.shard_slot_anchors.pin();
      anchors.compute(subscriber, |entry| -> Operation<Vec<ShardAnchor<S>>, ()> {
        match entry {
          Some((_, v)) => {
            let kept: Vec<_> = v
              .iter()
              .filter(|a| a.channel.as_ref() != channel)
              .cloned()
              .collect();
            if kept.is_empty() {
              Operation::Remove
            } else {
              Operation::Insert(kept)
            }
          }
          None => Operation::Abort(()),
        }
      });
    }
    removed
  }

  /// 槽权离本节点的分片订阅收口钩（Redis pubsub.c:
  /// pubsubShardUnsubscribeAllChannelsInSlot 的对位，rust 自有分片域）：
  /// 对锚定该槽的全部分片订阅清表并经 sink 邮箱推 sunsubscribe 通知帧
  /// （帧形与应答计数由会话 drain 臂回放——broker/迁移线程不直改会话
  /// num_active_channels，守会话单写者）；已退订的空锚不推帧。返回实际
  /// 清理的订阅条数（幂等：重复调用零命中零动作）
  ///
  /// 锚摘除经 papaya compute 纯替换闭包完成（闭包只从共享视图分流
  /// kept/taken，副作用推帧在闭窗外对生效集执行——compute 闭包可能因
  /// 冲突重试多次，taken 每次调用先清空重填，最终内容即生效摘除集）
  pub fn shard_slot_migrated_out(&self, slot: u16) -> usize
  where
    S: Clone,
  {
    // 串行闸：对 SSUBSCRIBE 两步装配与 SUNSUBSCRIBE 摘除整体原子
    //（见 [`Self::shard_mu`] 字段注；空表短路在闸内判，杜绝快照窗新登逃逸）
    let _shard_mu = self.shard_mu.lock();
    if self.shard_slot_anchors.is_empty() {
      return 0;
    }
    let mut evicted = 0usize;
    let anchors = self.shard_slot_anchors.pin();
    // 键快照后逐键 compute（迭代器活跃窗口内不做变更）
    let subscribers: Vec<u64> = anchors.iter().map(|(k, _)| *k).collect();
    for subscriber in subscribers {
      let mut taken: Vec<ShardAnchor<S>> = Vec::new();
      anchors.compute(subscriber, |entry| -> Operation<Vec<ShardAnchor<S>>, ()> {
        taken.clear();
        match entry {
          Some((_, v)) => {
            let mut kept: Vec<_> = Vec::with_capacity(v.len());
            for a in v {
              if a.slot == slot {
                taken.push(a.clone());
              } else {
                kept.push(a.clone());
              }
            }
            if kept.is_empty() {
              Operation::Remove
            } else {
              Operation::Insert(kept)
            }
          }
          None => Operation::Abort(()),
        }
      });
      // 真订阅在场才推帧（空锚/已退订残留零动作）；投递经
      // shard_forced_unsubscribe 控制通知臂：满即拒收改有界等待重试，
      // 超界留痕放弃（会话订阅旗悬挂面的兜底收口，见
      // PubSubMailbox::try_publish_control）
      for a in taken {
        if channel_sub_remove(&self.shard_subscriptions, &a.channel, subscriber) {
          a.sink.shard_forced_unsubscribe(&a.channel);
          evicted += 1;
        }
      }
    }
    evicted
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

  /// 直接向输出缓冲写入 RESP 分片通道数组（PUBSUB SHARDCHANNELS，
  /// 单次遍历快照收集，零拷贝借用一致成帧）
  ///
  /// `caller_prefix` 承载会话 ns 隔离前缀过滤与裸通道名还原；遍历与成帧
  /// 复用 [`write_channel_array`] 单机制，仅订阅表入参换分片表（Redis 7.0
  /// pubsub.c:pubsubCommandShardChannels 语义对齐源）
  ///
  /// 分片通道写出为 rust 自有面（C# SubscribeBroker 无 shard 对应；非分片写出见本文件 GetChannels）
  pub fn write_shard_channels(
    &self,
    output: &mut Vec<u8>,
    caller_prefix: &[u8],
    pattern: Option<&[u8]>,
  ) {
    write_channel_array(output, &self.shard_subscriptions, caller_prefix, pattern);
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
  #[inline]
  pub fn num_subscriptions(&self, channel: &[u8]) -> usize {
    channel_sub_count(&self.subscriptions, channel)
  }

  /// 指定分片通道的订阅者数（PUBSUB SHARDNUMSUB；查询形态与
  /// [`Self::num_subscriptions`] 同构，仅订阅表换分片表——Redis 7.0
  /// pubsub.c:pubsubCommandShardNumSub 语义对齐源）
  ///
  /// 分片订阅计数为 rust 自有面（C# SubscribeBroker 无 shard 对应；非分片计数见本文件 NumSubscriptions）
  #[inline]
  pub fn num_shard_subscriptions(&self, channel: &[u8]) -> usize {
    channel_sub_count(&self.shard_subscriptions, channel)
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
    if !self.shard_slot_anchors.is_empty() {
      self.shard_slot_anchors.pin().clear();
    }
  }

  /// 是否已停机收口（C# disposed 字段读取面；生产链零读取点——宿主停机收口
  /// 判定不消费本位，仅 wnode 集成测试的 stop 相位断言消费，故
  /// `#[doc(hidden)]` 声明测试专用，禁新增生产读取点）；
  /// C# 同名收口锚留 [`Self::dispose`] 一处
  #[doc(hidden)]
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
