//! 模式订阅条目（对标 libs/server/PubSub/PatternSubscriptionEntry.cs）

use std::sync::Arc;

use whasher::GxPapayaMap;

use super::subscriber::PubSubSink;

/// 模式订阅表内的会话集合（C# ReadOptimizedConcurrentSet<ServerSessionBase>
/// 的 papaya 承接：订阅者 id -> 投递面；通道订阅表复用同型）
pub type PatternSubscriberSet = GxPapayaMap<u64, Arc<dyn PubSubSink>>;

/// 模式订阅条目：一个 glob 模式及订阅它的会话集合
///
/// 托管面以"模式 -> 条目"键化并发表承接 C# 的条目集合，模式字段与键
/// 同源（C# 条目即携模式副本，此处键为去重面、字段为广播面）。
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
      subscriptions: GxPapayaMap::default(),
    }
  }

  /// 条目相等性：模式字节序相等（libs/server/PubSub/PatternSubscriptionEntry.cs:Equals
  /// —— `pattern.ReadOnlySpan.SequenceEqual(other.pattern.ReadOnlySpan)`）
  pub fn equals(&self, other: &Self) -> bool {
    self.pattern == other.pattern
  }
}

#[cfg(test)]
mod tests {
  use std::sync::Arc;

  use super::*;

  #[test]
  fn entry_holds_pattern_and_set() {
    let entry = PatternSubscriptionEntry::new(Box::from(b"a*".as_slice()));
    assert!(entry.equals(&PatternSubscriptionEntry::new(b"a*".as_slice().into())));
    assert!(entry.subscriptions.is_empty());
    let sink: Arc<dyn PubSubSink> = Arc::new(super::super::subscriber::PubSubMailbox::new(1));
    assert!(entry.subscriptions.pin().insert(1, sink).is_none());
    assert_eq!(entry.subscriptions.len(), 1);
  }
}
