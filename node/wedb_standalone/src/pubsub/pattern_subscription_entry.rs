//! 模式订阅条目（对标 libs/server/PubSub/PatternSubscriptionEntry.cs）

use std::sync::Arc;

use gxhash::HashMap as GxHashMap;

use super::subscriber::PubSubSink;

/// 模式订阅表内的会话集合（C# ReadOptimizedConcurrentSet<ServerSessionBase>
/// 的托管等价：订阅者 id -> 投递面）
pub type PatternSubscriberSet = GxHashMap<u64, Arc<dyn PubSubSink>>;

/// 模式订阅条目：一个 glob 模式及订阅它的会话集合
pub struct PatternSubscriptionEntry {
  /// 订阅的 glob 模式（C# pattern）
  pub pattern: Box<[u8]>,
  /// 订阅该模式的会话集合（C# subscriptions）
  pub subscriptions: PatternSubscriberSet,
}

impl PatternSubscriptionEntry {
  /// 以模式创建条目（C# 对象初始化器 `{ pattern = .., subscriptions = new(..) }`）
  pub fn new(pattern: Box<[u8]>) -> Self {
    Self {
      pattern,
      subscriptions: GxHashMap::default(),
    }
  }

  /// 条目相等性：模式字节序相等（libs/server/PubSub/PatternSubscriptionEntry.cs:Equals
  /// —— `pattern.ReadOnlySpan.SequenceEqual(other.pattern.ReadOnlySpan)`）
  pub fn equals(&self, other: &Self) -> bool {
    self.pattern == other.pattern
  }
}
