//! 订阅经纪集成测试（自 src/subscribe_broker.rs 内嵌测试整体迁出）
//!
//! 对标 libs/server/PubSub/SubscribeBroker.cs：通道 / 模式订阅生命周期、
//! 发布投递、消费解码与并发生产者进度。

use std::sync::Arc;

use wpubsub::{PatternSubscriptionEntry, PubSubMailbox, SubscribeBroker};

#[ctor::ctor(unsafe)]
fn _log_init() {
  log_init::init();
}

struct Fixture {
  broker: SubscribeBroker,
  mailbox: Arc<PubSubMailbox>,
}

fn fixture(page_size: usize) -> Fixture {
  let broker = SubscribeBroker::new(page_size);
  let mailbox = Arc::new(PubSubMailbox::new(16));
  Fixture { broker, mailbox }
}

/// C# SubscribeBrokerTests：订阅 / 退订生命周期（重复订阅幂等，退订清空）
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

/// 模式订阅 + 发布命中分发
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

/// 即时发布直达通道订阅者
#[test]
fn publish_now_reaches_channel_subscribers() {
  let f = fixture(4096);
  assert_eq!(f.broker.publish_now(b"ch", b"v"), 0);
  f.broker.subscribe(b"ch", 7, f.mailbox.clone());
  assert_eq!(f.broker.publish_now(b"ch", b"v"), 1);
  let messages = f.mailbox.drain();
  assert_eq!(messages[0].value.as_ref(), b"v");
}

/// 入队 → 消费积压（入队阶段不投递，consume_pending 一次性分发）
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

/// 消费解码长度前缀负载（畸形负载安全返回 0）
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

/// PUBSUB CHANNELS 模式过滤（glob）
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

/// 会话拆除清空全部订阅形态（通道 + 模式）
#[test]
fn remove_subscription_clears_all_kinds() {
  let f = fixture(4096);
  f.broker.subscribe(b"ch", 1, f.mailbox.clone());
  f.broker.pattern_subscribe(b"p*", 1, f.mailbox.clone());
  f.broker.remove_subscription(1);
  assert!(f.broker.get_channels().is_empty());
  assert_eq!(f.broker.num_pattern_subscriptions(), 0);
}

/// dispose 后拒绝一切新操作（订阅 / 发布 / 入队静默丢弃）
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

/// 模式条目等值比较（同模式相等，异模式不等）
#[test]
fn equals_on_pattern_entry() {
  let a = PatternSubscriptionEntry::new(Box::from(b"ab*".as_slice()));
  let b = PatternSubscriptionEntry::new(Box::from(b"ab*".as_slice()));
  let c = PatternSubscriptionEntry::new(Box::from(b"ba*".as_slice()));
  assert!(a.equals(&b));
  assert!(!a.equals(&c));
}

/// 发布变体（channel / fast / pattern）与 clear 排空
#[test]
fn publish_variants_and_clear() {
  let f = fixture(4096);
  f.broker.subscribe(b"ch1", 1, f.mailbox.clone());
  f.broker.pattern_subscribe(b"pat.*", 2, f.mailbox.clone());

  f.broker.publish_to_channel(b"ch1", b"v1");
  f.broker.publish_fast(b"ch1", b"v2");
  f.broker.publish_to_pattern(b"pat.1", b"v3");

  // clear 排空积压，不触发投递
  f.broker.clear();
  assert_eq!(f.broker.consume_pending(), 0);
  assert!(f.mailbox.is_empty());

  // 再次入队验证正常分发
  f.broker.publish_fast(b"ch1", b"v4");
  assert_eq!(f.broker.consume_pending(), 1);
  assert_eq!(f.mailbox.len(), 1);
  assert_eq!(f.mailbox.drain()[0].value.as_ref(), b"v4");
}

/// 并发生产者：入队总量精确可达，消费一次全量分发（无丢失）
#[test]
fn concurrent_publishers_consume_pending() {
  use std::thread;

  let f = fixture(4096);
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
          b.publish_fast(b"bench", b"fast");
        } else {
          b.publish_to_channel(b"bench", b"chan");
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
