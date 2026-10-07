//! 模式订阅条目（对标 libs/server/PubSub/PatternSubscriptionEntry.cs）

use std::sync::Arc;

use wbase::map::{ConcurrentMap, new_concurrent_map};

use crate::subscriber::PubSubMailbox;

/// 模式订阅表内的会话集合（C# `ReadOptimizedConcurrentSet<ServerSessionBase>`
/// 的 papaya 承接：订阅者 id -> 投递面；通道订阅表复用同型）
pub type PatternSubscriberSet<S = Arc<PubSubMailbox>> = ConcurrentMap<u64, S>;

/// 模式订阅条目：一个 glob 模式及订阅它的会话集合
///
/// 托管面以"模式 -> 条目"键化并发表承接 C# 的条目集合，模式字段与键
/// 同源（C# 条目即携模式副本，此处键为去重面、字段为广播面）。
/// C# `PatternSubscriptionEntry.Equals`（`pattern.ReadOnlySpan
/// .SequenceEqual(other.pattern...)`）的对位语义由托管 map 键判等吸收：
/// 模式字节序相等即同键同条目，无须条目级 equals 方法。
pub struct PatternSubscriptionEntry<S = Arc<PubSubMailbox>> {
  /// 订阅的 glob 模式（C# pattern）
  pub pattern: Box<[u8]>,
  /// 订阅该模式的会话集合（C# subscriptions）
  pub subscriptions: PatternSubscriberSet<S>,
}

impl<S> PatternSubscriptionEntry<S> {
  /// 以模式创建条目（唯一构造点，C# 对象初始化器 `{ pattern = .., subscriptions = new(..) }`；
  /// 投递面类型经泛型参数 S 选定，构造体与 S 无关）
  pub fn with_sink(pattern: Box<[u8]>) -> Self {
    Self {
      pattern,
      subscriptions: new_concurrent_map(),
    }
  }
}
