//! 并发 UNSUBSCRIBE 外层删键与 SUBSCRIBE 抢插的孤儿化竞态集成测试
//!
//! 对标 garnet/libs/server/PubSub/SubscribeBroker.cs:Unsubscribe（仅
//! `sessions.TryRemove(session)`）与 PatternUnsubscribe（仅
//! `entry.subscriptions.TryRemove(session)`）：Garnet 绝不从外层字典移除频道 /
//! 模式键，空集合不可见性由读侧 `Count > 0` 动态过滤承接。
//!
//! 验收 task/ing/wpubsub-unsubscribe-concurrent-channel-removal-race.md：
//! 「内层集合摘除 + 外层字典删键」是两步非原子复合操作，退订与并发订阅交织会
//! 把刚订阅进来的新订阅者连同其集合从外层拔除，导致孤儿化与广播静默丢失。
//!
//! 编排纪律：每轮以 Barrier(2) 同刻释放「A 退订」与「B 订阅」两个并发操作；
//! 先 collect 全部 JoinHandle 再统一 join（串行 join 会饿死 Barrier 而恒挂）；
//! 轮数写成常量，禁止裸 sleep 竞态伪证。期望值由协议推导：修复后外层键稳定
//! 驻留，每轮 B 均被真实广播送达（`for each round: B 收到 >= 1`），累计命中
//! 数恒等于轮数；恢复旧「外层删键」行为则存在轮次 B 被孤儿化（`delivered == 0`），
//! 断言必然失败。

use std::{
  sync::{Arc, Barrier},
  thread,
};

use wpubsub::{subscribe_broker::SubscribeBroker, subscriber::PubSubMailbox};

/// 竞态闭环轮数：窗口（内层摘除后集合短暂为空 → B 抢插同一集合 → 外层删键把含
/// B 的集合整体拔除）为亚微秒级，须以足够轮次在高争用下命中；固定为编译期常量。
const RACE_ROUNDS: usize = 5_000;

/// 目标频道（通道 / 模式 / 分片三域复用同名以对齐广播键）
const CHANNEL: &[u8] = b"race.channel";
/// 退订方会话 ID
const UNSUBSCRIBER: u64 = 1;
/// 并发订阅方（潜在受害者）会话 ID
const VICTIM: u64 = 2;

/// 排空邮箱并返回收到的消息条数
fn received(mbox: &PubSubMailbox) -> usize {
  let mut buf = Vec::new();
  mbox.drain_into(&mut buf)
}

/// 通道域：线程 A 快速交替 subscribe/unsubscribe 目标频道、线程 B 并发 subscribe
/// 同频道，随后 publish 必须稳定送达 B（每轮 1 次），零孤儿化。
#[test]
fn unsubscribe_key_removal_does_not_orphan_concurrent_subscriber() {
  let broker = Arc::new(SubscribeBroker::new());
  let mbox_a = Arc::new(PubSubMailbox::new(16));
  let mbox_b = Arc::new(PubSubMailbox::new(16));

  // victim 全程订阅不松手：仅在竞态窗口命中时会被外层删键拔除而收不到广播
  let mut victim_hits = 0_usize;

  for _ in 0..RACE_ROUNDS {
    // 前置：集合恰有唯一元素 A，复现票面「频道仅有订阅者 A，A 发起退订」场景
    broker.subscribe(CHANNEL, UNSUBSCRIBER, mbox_a.clone());

    let gate = Arc::new(Barrier::new(2));

    let b1 = broker.clone();
    let g1 = gate.clone();
    let h_unsub = thread::spawn(move || {
      g1.wait();
      b1.unsubscribe(CHANNEL, UNSUBSCRIBER);
    });

    let b2 = broker.clone();
    let g2 = gate.clone();
    let mb = mbox_b.clone();
    let h_sub = thread::spawn(move || {
      g2.wait();
      b2.subscribe(CHANNEL, VICTIM, mb.clone());
    });

    // 统一收集后 join：避免 arm().join(); arm().join() 串行 join 饿死 Barrier
    let handles = [h_unsub, h_sub];
    for h in handles {
      h.join().unwrap();
    }

    // 协议期望：退订方已摘除，集合应仅剩 B；publish 必送达 B
    let delivered = broker.publish_now(CHANNEL, b"v");
    victim_hits += (received(&mbox_b) >= 1) as usize;
    received(&mbox_a);
    let _ = delivered;

    // 复位下一轮前置：清空 B 订阅（外层键修复后稳定驻留，退订不删键）
    broker.unsubscribe(CHANNEL, VICTIM);
  }

  assert_eq!(
    victim_hits,
    RACE_ROUNDS,
    "并发退订/订阅竞态中每一轮新订阅者 B 都必须被广播送达；被孤儿化轮数 = {}",
    RACE_ROUNDS - victim_hits
  );
}

/// 模式域：PUNSUBSCRIBE 外层删模式键与 PSUBSCRIBE 抢插同条目的竞态，
/// 与通道域同构，publish 命中模式后必须稳定送达 B。
#[test]
fn pattern_unsubscribe_key_removal_does_not_orphan_concurrent_subscriber() {
  let broker = Arc::new(SubscribeBroker::new());
  let mbox_a = Arc::new(PubSubMailbox::new(16));
  let mbox_b = Arc::new(PubSubMailbox::new(16));

  let mut victim_hits = 0_usize;

  for _ in 0..RACE_ROUNDS {
    broker.pattern_subscribe(CHANNEL, UNSUBSCRIBER, mbox_a.clone());

    let gate = Arc::new(Barrier::new(2));

    let b1 = broker.clone();
    let g1 = gate.clone();
    let h_unsub = thread::spawn(move || {
      g1.wait();
      b1.pattern_unsubscribe(CHANNEL, UNSUBSCRIBER);
    });

    let b2 = broker.clone();
    let g2 = gate.clone();
    let mb = mbox_b.clone();
    let h_sub = thread::spawn(move || {
      g2.wait();
      b2.pattern_subscribe(CHANNEL, VICTIM, mb.clone());
    });

    let handles = [h_unsub, h_sub];
    for h in handles {
      h.join().unwrap();
    }

    // 模式命中广播（键等于模式串，glob 自匹配）必须送达 B
    broker.publish_now(CHANNEL, b"v");
    victim_hits += (received(&mbox_b) >= 1) as usize;
    received(&mbox_a);

    broker.pattern_unsubscribe(CHANNEL, VICTIM);
  }

  assert_eq!(
    victim_hits,
    RACE_ROUNDS,
    "模式退订/订阅竞态中每一轮新订阅者 B 都必须被广播送达；被孤儿化轮数 = {}",
    RACE_ROUNDS - victim_hits
  );
}

/// 分片域：SUNSUBSCRIBE 外层删分片键与 SSUBSCRIBE 抢插的竞态，
/// publish_shard_now 必须稳定送达 B。
#[test]
fn shard_unsubscribe_key_removal_does_not_orphan_concurrent_subscriber() {
  let broker = Arc::new(SubscribeBroker::new());
  let mbox_a = Arc::new(PubSubMailbox::new(16));
  let mbox_b = Arc::new(PubSubMailbox::new(16));

  let mut victim_hits = 0_usize;

  for _ in 0..RACE_ROUNDS {
    broker.shard_subscribe(CHANNEL, UNSUBSCRIBER, mbox_a.clone());

    let gate = Arc::new(Barrier::new(2));

    let b1 = broker.clone();
    let g1 = gate.clone();
    let h_unsub = thread::spawn(move || {
      g1.wait();
      b1.shard_unsubscribe(CHANNEL, UNSUBSCRIBER);
    });

    let b2 = broker.clone();
    let g2 = gate.clone();
    let mb = mbox_b.clone();
    let h_sub = thread::spawn(move || {
      g2.wait();
      b2.shard_subscribe(CHANNEL, VICTIM, mb.clone());
    });

    let handles = [h_unsub, h_sub];
    for h in handles {
      h.join().unwrap();
    }

    broker.publish_shard_now(CHANNEL, b"v");
    victim_hits += (received(&mbox_b) >= 1) as usize;
    received(&mbox_a);

    broker.shard_unsubscribe(CHANNEL, VICTIM);
  }

  assert_eq!(
    victim_hits,
    RACE_ROUNDS,
    "分片退订/订阅竞态中每一轮新订阅者 B 都必须被广播送达；被孤儿化轮数 = {}",
    RACE_ROUNDS - victim_hits
  );
}

/// 会话释放批量清理（remove_subscription）与并发订阅的竞态：A 侧清理路径不得把
/// B 抢插的新订阅者连同集合从外层拔除，后续 publish 必须送达 B。
#[test]
fn remove_subscription_does_not_orphan_concurrent_subscriber() {
  let broker = Arc::new(SubscribeBroker::new());
  let mbox_a = Arc::new(PubSubMailbox::new(16));
  let mbox_b = Arc::new(PubSubMailbox::new(16));

  let mut victim_hits = 0_usize;

  for _ in 0..RACE_ROUNDS {
    broker.subscribe(CHANNEL, UNSUBSCRIBER, mbox_a.clone());

    let gate = Arc::new(Barrier::new(2));

    let b1 = broker.clone();
    let g1 = gate.clone();
    let h_remove = thread::spawn(move || {
      g1.wait();
      b1.remove_subscription(UNSUBSCRIBER);
    });

    let b2 = broker.clone();
    let g2 = gate.clone();
    let mb = mbox_b.clone();
    let h_sub = thread::spawn(move || {
      g2.wait();
      b2.subscribe(CHANNEL, VICTIM, mb.clone());
    });

    let handles = [h_remove, h_sub];
    for h in handles {
      h.join().unwrap();
    }

    // 协议期望：清理方仅摘自身订阅，集合应剩新订阅者 B；publish 必送达 B
    broker.publish_now(CHANNEL, b"v");
    victim_hits += (received(&mbox_b) >= 1) as usize;
    received(&mbox_a);

    broker.unsubscribe(CHANNEL, VICTIM);
  }

  assert_eq!(
    victim_hits,
    RACE_ROUNDS,
    "remove_subscription 清理竞态中每一轮新订阅者 B 都必须被广播送达；被孤儿化轮数 = {}",
    RACE_ROUNDS - victim_hits
  );
}

/// 空集合惰性驻留的读侧过滤复核（确定性、非竞态）：全部退订后外层键虽驻留，
/// 但所有查询面（num_subscriptions / list_all_subscriptions / for_each_channel /
/// write_channels）均以 `!set.is_empty()` 过滤，无幽灵频道外泄，且再次订阅复用
/// 同一路径可正常送达——证明修复未引入双套机制。
#[test]
fn empty_retained_channel_is_hidden_from_read_surface() {
  let broker = SubscribeBroker::new();
  let mbox = Arc::new(PubSubMailbox::new(16));

  broker.subscribe(CHANNEL, UNSUBSCRIBER, mbox.clone());
  assert!(broker.unsubscribe(CHANNEL, UNSUBSCRIBER));

  // 读侧全部过滤空集合：零幽灵频道
  assert_eq!(broker.num_subscriptions(CHANNEL), 0);
  assert!(broker.list_all_subscriptions().is_empty());
  let mut seen = 0;
  broker.for_each_channel(|_| seen += 1);
  assert_eq!(seen, 0);
  let mut out = Vec::new();
  broker.write_channels(&mut out, b"", None);
  assert_eq!(out, b"*0\r\n");

  // 惰性驻留的键可被后续订阅直接复用并正常送达（无需重建外层键）
  assert!(broker.subscribe(CHANNEL, VICTIM, mbox.clone()));
  assert_eq!(broker.num_subscriptions(CHANNEL), 1);
  assert_eq!(broker.publish_now(CHANNEL, b"v"), 1);
  assert_eq!(received(&mbox), 1);
}
