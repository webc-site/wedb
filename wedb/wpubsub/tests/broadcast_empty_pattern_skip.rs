//! PUBLISH 广播空集模式条目短路测试
//! （对标 garnet/libs/server/PubSub/SubscribeBroker.cs:Broadcast + PatternUnsubscribe）
//!
//! C# 契约：PatternUnsubscribe / RemoveSubscription 仅摘内层订阅集元素，
//! 外层模式条目终身驻留（patternSubscriptions 键零收缩）；Broadcast 对每个
//! 条目无条件先 Match(key, pattern) 再遍历订阅集（SubscribeBroker.cs:93-114），
//! 空集条目同样付出 glob 匹配成本。Rust 读侧三消费点（broadcast /
//! for_each_pattern / num_pattern_subscriptions）统一以
//! entry.subscriptions.is_empty() 过滤：空集条目本就零投递，短路零行为变化，
//! 仅消除热路径对全历史模式条目的无界 glob 匹配成本。
//!
//! 行为断言锁该零行为变化不变式：空集条目在场（PUNSUBSCRIBE / 会话释放后
//! 外层驻留）时，publish_now 对命中空集模式的键恰好零投递、对其余在订模式
//! 照常投递、通知计数不虚计；读侧计数 / 列表同步过滤。

use std::sync::Arc;

use wpubsub::{
  subscribe_broker::SubscribeBroker,
  subscriber::{PubSubMailbox, PubSubMessageKind},
};

/// PUNSUBSCRIBE 空集驻留条目：广播短路零投递，邻接在订模式照常投递
#[test]
fn broadcast_skips_empty_entry_and_delivers_remaining_patterns() {
  let broker = SubscribeBroker::new();
  let mb_emptied = Arc::new(PubSubMailbox::new(16));
  let mb_active = Arc::new(PubSubMailbox::new(16));

  // 订阅者 1 退订 news.*：条目空集但外层键驻留（仅摘内层元素，同 C# 形态）
  assert!(broker.pattern_subscribe(b"news.*", 1, mb_emptied.clone()));
  assert!(broker.pattern_unsubscribe(b"news.*", 1));

  // 订阅者 2 在订同前缀族模式 news.tech.*（证明短路不误伤邻条目）
  assert!(broker.pattern_subscribe(b"news.tech.*", 2, mb_active.clone()));

  // 读侧统一空集过滤：驻留空条目不计入计数与列表
  assert_eq!(broker.num_pattern_subscriptions(b""), 1);
  assert_eq!(
    broker.list_all_pattern_subscriptions(),
    vec![b"news.tech.*".to_vec()]
  );

  // 发布同时命中空集模式（news.*）与在订模式（news.tech.*）的键：
  // 空集条目零投递，在订条目恰 1 通知
  assert_eq!(broker.publish_now(b"news.tech.db", b"v"), 1);
  assert_eq!(mb_emptied.len(), 0);
  assert_eq!(mb_active.len(), 1);
  let mut msgs = Vec::new();
  mb_active.drain_into(&mut msgs);
  assert_eq!(msgs[0].kind, PubSubMessageKind::Pattern);
  assert_eq!(msgs[0].pattern.as_deref(), Some(&b"news.tech.*"[..]));
  assert_eq!(msgs[0].channel.as_ref(), b"news.tech.db");
  assert_eq!(msgs[0].value.as_ref(), b"v");

  // 仅命中空集模式的键：零通知、零投递（外层驻留使 publish_now 不被
  // is_idle 提前返回，确实走到模式臂的空集短路）
  assert_eq!(broker.publish_now(b"news.sports", b"v"), 0);
  assert_eq!(mb_emptied.len(), 0);
  assert_eq!(mb_active.len(), 0);

  // 空集条目复用（C# TryAddAndGet 同键去重）：同模式再订阅走同一驻留
  // 条目，投递恢复，证明短路是集合空集判定而非键级摘除
  assert!(broker.pattern_subscribe(b"news.*", 3, mb_emptied.clone()));
  assert_eq!(broker.publish_now(b"news.sports", b"v2"), 1);
  let mut revived = Vec::new();
  mb_emptied.drain_into(&mut revived);
  assert_eq!(revived[0].kind, PubSubMessageKind::Pattern);
  assert_eq!(revived[0].pattern.as_deref(), Some(&b"news.*"[..]));
  assert_eq!(revived[0].value.as_ref(), b"v2");
}

/// 会话释放（remove_subscription）与 PUNSUBSCRIBE 同一空集驻留形态，
/// 广播短路对两会话收口路径统一生效
#[test]
fn broadcast_skips_entry_emptied_by_remove_subscription() {
  let broker = SubscribeBroker::new();
  let mb_gone = Arc::new(PubSubMailbox::new(16));
  let mb_kept = Arc::new(PubSubMailbox::new(16));

  broker.pattern_subscribe(b"gone.*", 1, mb_gone.clone());
  broker.pattern_subscribe(b"kept.*", 2, mb_kept.clone());
  broker.remove_subscription(1);

  // 空集驻留条目（gone.*）命中键零投递，在订条目（kept.*）照常投递
  assert_eq!(broker.num_pattern_subscriptions(b""), 1);
  assert_eq!(broker.publish_now(b"kept.alive", b"v"), 1);
  assert_eq!(mb_gone.len(), 0);
  assert_eq!(mb_kept.len(), 1);

  // 仅命中空集驻留条目的键：零通知
  assert_eq!(broker.publish_now(b"gone.stale", b"v"), 0);
  assert_eq!(mb_gone.len(), 0);
}
