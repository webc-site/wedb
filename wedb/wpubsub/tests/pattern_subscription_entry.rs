use std::sync::Arc;

use wpubsub::{pattern_subscription_entry::PatternSubscriptionEntry, subscriber::PubSubMailbox};

#[test]
fn entry_holds_pattern_and_set() {
  let entry: PatternSubscriptionEntry =
    PatternSubscriptionEntry::with_sink(Box::from(b"a*".as_slice()));
  let same: PatternSubscriptionEntry = PatternSubscriptionEntry::with_sink(b"a*".as_slice().into());
  let other: PatternSubscriptionEntry =
    PatternSubscriptionEntry::with_sink(Box::from(b"b*".as_slice()));
  // C# Equals 对位语义经 map 键判：模式字节序相等即同键（broker 侧不设
  // 第二例测，判等覆盖收口于此）
  assert_eq!(entry.pattern, same.pattern);
  assert_ne!(entry.pattern, other.pattern);
  assert!(entry.subscriptions.is_empty());
  let sink = Arc::new(PubSubMailbox::new(1));
  assert!(entry.subscriptions.pin().insert(1, sink).is_none());
  assert_eq!(entry.subscriptions.len(), 1);
}
