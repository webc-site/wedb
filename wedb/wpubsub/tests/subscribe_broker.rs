#![recursion_limit = "512"] // 泛型设备实例化下 async 状态机嵌套深（R27 CI 实证）
use std::{sync::Arc, thread};

use wbase::hash_slot::slot_of;
use wpubsub::{
  subscribe_broker::SubscribeBroker,
  subscriber::{PubSubMailbox, PubSubMessageKind},
};

/// 测试域槽锚（slot_of 单源真值；锚测试的「本槽/他槽」对照维度）
const SLOT_A: u16 = slot_of(0, 0);
const SLOT_B: u16 = slot_of(0, 1);

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
  assert!(
    f.broker
      .shard_subscribe(SLOT_A, b"slot:100", 1, mb_shard.clone())
  );
  assert!(
    !f.broker
      .shard_subscribe(SLOT_A, b"slot:100", 1, mb_shard.clone())
  );
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

/// 槽权离本节点的分片订阅收口（Redis pubsub.c:
/// pubsubShardUnsubscribeAllChannelsInSlot 对位）：命中锚订阅清表 +
/// sink 邮箱收到 ShardUnsubscribe 通知 + 未命中锚（他槽/已退订）零误清
#[test]
fn shard_slot_migrated_out_clears_anchored_and_spares_others() {
  let f = fixture();
  let mb_a = Arc::new(PubSubMailbox::new(16));
  let mb_b = Arc::new(PubSubMailbox::new(16));

  // 订阅者 1 锚 SLOT_A 两频道；订阅者 2 锚 SLOT_B 同名频道之一 + SLOT_A 一频道
  assert!(f.broker.shard_subscribe(SLOT_A, b"ch1", 1, mb_a.clone()));
  assert!(f.broker.shard_subscribe(SLOT_A, b"ch2", 1, mb_a.clone()));
  assert!(f.broker.shard_subscribe(SLOT_B, b"ch1", 2, mb_b.clone()));
  assert!(f.broker.shard_subscribe(SLOT_A, b"ch3", 2, mb_b.clone()));

  // SLOT_A 迁出：恰清三命中锚订阅（1 的两频道 + 2 的 ch3），推三帧通知
  assert_eq!(f.broker.shard_slot_migrated_out(SLOT_A), 3);
  assert_eq!(
    f.broker.list_all_shard_subscriptions(),
    vec![b"ch1".to_vec()]
  );
  assert_eq!(mb_a.len(), 2, "订阅者 1 两频道各收一帧强制退订通知");
  assert_eq!(mb_b.len(), 1, "订阅者 2 仅命中锚的 ch3 收帧");

  let mut msgs = Vec::new();
  mb_a.drain_into(&mut msgs);
  assert_eq!(msgs[0].kind, PubSubMessageKind::ShardUnsubscribe);
  assert_eq!(msgs[0].channel.as_ref(), b"ch1");
  assert_eq!(msgs[1].kind, PubSubMessageKind::ShardUnsubscribe);
  assert_eq!(msgs[1].channel.as_ref(), b"ch2");

  // 未命中槽不动：SLOT_B 订阅者 2 的 ch1 仍在，再投递可达
  assert_eq!(f.broker.publish_shard_now(b"ch1", b"v"), 1);

  // 幂等：重复迁出 SLOT_A 零命中零动作
  assert_eq!(f.broker.shard_slot_migrated_out(SLOT_A), 0);

  // mb_b 排空前积两帧（SLOT_A 迁出的 ch3 强制退订帧 + 上行 ch1 投递帧），
  // 令 SLOT_B 迁出段断言仅计本次迁出帧
  let mut prior = Vec::new();
  mb_b.drain_into(&mut prior);
  assert_eq!(prior.len(), 2, "迁出段前 mb_b 恰积 ch3 退订帧与 ch1 投递帧");

  // SLOT_B 迁出：清余下订阅并推帧
  assert_eq!(f.broker.shard_slot_migrated_out(SLOT_B), 1);
  assert!(f.broker.list_all_shard_subscriptions().is_empty());
  assert_eq!(mb_b.len(), 1);
  let mut msgs_b = Vec::new();
  mb_b.drain_into(&mut msgs_b);
  assert_eq!(msgs_b[0].kind, PubSubMessageKind::ShardUnsubscribe);
  assert_eq!(msgs_b[0].channel.as_ref(), b"ch1");
}

/// SUNSUBSCRIBE 摘锚后槽事件零推帧（无陈旧锚误清新订阅），锚随
/// remove_subscription 整体摘除
#[test]
fn shard_unsubscribe_removes_anchor_and_remove_subscription_clears_all() {
  let f = fixture();
  let mb = Arc::new(PubSubMailbox::new(16));

  assert!(f.broker.shard_subscribe(SLOT_A, b"ch", 1, mb.clone()));
  // 退订成功即摘锚：同槽再迁出不得对已退订频道推帧
  assert!(f.broker.shard_unsubscribe(b"ch", 1));
  assert_eq!(f.broker.shard_slot_migrated_out(SLOT_A), 0);
  assert_eq!(mb.len(), 0);

  // 重新订阅（锚重登）→ 会话释放整体摘锚 → 槽事件零推帧
  assert!(f.broker.shard_subscribe(SLOT_A, b"ch", 1, mb.clone()));
  f.broker.remove_subscription(1);
  assert_eq!(f.broker.shard_slot_migrated_out(SLOT_A), 0);
  assert_eq!(mb.len(), 0);
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
fn write_channels_and_list_all() {
  let f = fixture();
  f.broker.subscribe(b"chat.room", 1, f.mailbox.clone());
  f.broker.subscribe(b"chat.general", 2, f.mailbox.clone());
  f.broker.subscribe(b"news.tech", 3, f.mailbox.clone());

  assert_eq!(f.broker.list_all_subscriptions().len(), 3);

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
  f.broker
    .shard_subscribe(SLOT_A, b"sh", 1, f.mailbox.clone());
  f.broker.remove_subscription(1);
  assert!(f.broker.list_all_subscriptions().is_empty());
  assert_eq!(f.broker.num_pattern_subscriptions(b""), 0);
  assert!(f.broker.list_all_shard_subscriptions().is_empty());
}

#[test]
fn dispose_rejects_further_operations() {
  let f = fixture();
  f.broker.subscribe(b"ch", 1, f.mailbox.clone());
  f.broker
    .shard_subscribe(SLOT_A, b"sh", 1, f.mailbox.clone());
  f.broker.dispose();
  assert!(!f.broker.subscribe(b"ch2", 2, f.mailbox.clone()));
  assert!(
    !f.broker
      .shard_subscribe(SLOT_A, b"sh2", 2, f.mailbox.clone())
  );
  assert_eq!(f.broker.publish_now(b"ch", b"v"), 0);
  assert_eq!(f.broker.publish_shard_now(b"sh", b"v"), 0);
  assert!(f.broker.list_all_subscriptions().is_empty());
  assert!(f.broker.list_all_shard_subscriptions().is_empty());
  assert!(f.broker.is_idle());
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
