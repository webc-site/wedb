#![recursion_limit = "512"] // 泛型设备实例化下 async 状态机嵌套深（R27 CI 实证）
use std::{sync::Arc, thread::spawn, time::Instant};

use wpubsub::subscriber::{PubSubMailbox, PubSubMessage, PubSubMessageKind, PubSubSink};

#[test]
fn mailbox_bounded_rejects_when_full() {
  let mailbox = PubSubMailbox::new(2);
  assert!(mailbox.publish(b"a", b"1"));
  assert!(mailbox.publish(b"b", b"2"));
  assert_eq!(mailbox.len(), 2);
  // 满水位：发布端零阻塞、拒收丢尾帧（不丢头）
  assert!(!mailbox.publish(b"c", b"3"));
  assert_eq!(mailbox.len(), 2);
  assert!(!mailbox.try_publish(PubSubMessage {
    kind: PubSubMessageKind::Channel,
    pattern: None,
    channel: b"d".to_vec().into_boxed_slice(),
    value: b"4".to_vec().into_boxed_slice(),
  }));

  let mut buf = Vec::new();
  assert_eq!(mailbox.drain_into(&mut buf), 2);
  assert_eq!(buf[0].channel.as_ref(), b"a");
  assert_eq!(buf[1].channel.as_ref(), b"b");
  assert!(mailbox.is_empty());
  // 排空后水位回落，恢复收帧
  assert!(mailbox.try_publish(PubSubMessage {
    kind: PubSubMessageKind::Channel,
    pattern: None,
    channel: b"e".to_vec().into_boxed_slice(),
    value: b"5".to_vec().into_boxed_slice(),
  }));
  assert_eq!(mailbox.len(), 1);
}

#[test]
fn mailbox_drain_into_reuses_buffer() {
  let mailbox = PubSubMailbox::new(4);
  assert!(mailbox.publish(b"c1", b"v1"));
  assert!(mailbox.publish(b"c2", b"v2"));

  let mut buf = Vec::new();
  let count = mailbox.drain_into(&mut buf);
  assert_eq!(count, 2);
  assert_eq!(buf.len(), 2);
  assert_eq!(buf[0].channel.as_ref(), b"c1");
  assert_eq!(buf[1].channel.as_ref(), b"c2");
  assert!(mailbox.is_empty());

  assert!(mailbox.publish(b"c3", b"v3"));
  buf.clear();
  let count2 = mailbox.drain_into(&mut buf);
  assert_eq!(count2, 1);
  assert_eq!(buf.len(), 1);
  assert_eq!(buf[0].channel.as_ref(), b"c3");
}

#[test]
fn mailbox_pattern_message_keeps_pattern() {
  let mailbox = PubSubMailbox::new(4);
  assert!(mailbox.pattern_publish(b"a*", b"ab", b"v"));
  let mut buf = Vec::new();
  mailbox.drain_into(&mut buf);
  let messages = buf;
  assert_eq!(messages[0].kind, PubSubMessageKind::Pattern);
  assert_eq!(messages[0].pattern.as_deref(), Some(b"a*".as_slice()));
}

/// 控制通知保序锚（票面「保序不变」）：通知帧与数据帧同走邮箱单队列，
/// 恒落于已排队数据帧之后、其后到站数据帧之前
#[test]
fn control_notify_keeps_mailbox_fifo_order() {
  let mailbox = PubSubMailbox::new(4);
  assert!(mailbox.shard_publish(b"d1", b"v"));
  assert!(mailbox.shard_publish(b"d2", b"v"));
  // 未满窗：控制通知即投即回（不入重试臂）
  assert!(mailbox.shard_forced_unsubscribe(b"c1"));
  assert!(mailbox.shard_publish(b"d3", b"v"));

  let mut buf = Vec::new();
  assert_eq!(mailbox.drain_into(&mut buf), 4);
  let kinds: Vec<_> = buf.iter().map(|m| m.kind).collect();
  assert_eq!(
    kinds,
    vec![
      PubSubMessageKind::Shard,
      PubSubMessageKind::Shard,
      PubSubMessageKind::ShardUnsubscribe,
      PubSubMessageKind::Shard,
    ],
    "通知帧必须恰在先前数据帧之后、其后到站数据帧之前出列"
  );
  assert_eq!(buf[2].channel.as_ref(), b"c1");
  assert_eq!(mailbox.dropped_shard_notify(), 0, "未满窗零放弃");
  assert_eq!(mailbox.dropped(), 0, "容量内零丢弃");
}

/// 满水位兜底臂之入列面：队列满时控制通知有界等待重试，消费端排空
/// 腾位即复投入列（独立发布线程自旋 + 主线程排空，真实邮箱零 mock）
#[test]
fn control_notify_full_mailbox_waits_then_lands_after_drain() {
  let mailbox = Arc::new(PubSubMailbox::new(2));
  assert!(mailbox.shard_publish(b"d1", b"v"));
  assert!(mailbox.shard_publish(b"d2", b"v"));
  assert_eq!(mailbox.len(), 2, "预置满水位");

  let publisher = mailbox.clone();
  let notifier = spawn(move || publisher.shard_forced_unsubscribe(b"c1"));
  // 发布线程满水位有界自旋中：主线程排空腾位（排空动作与复投入列的
  // 先后由有界重试臂自然收敛，join 即终局判据）
  let mut buf = Vec::new();
  let drained = mailbox.drain_into(&mut buf);
  assert_eq!(drained, 2);
  notifier.join().unwrap();

  // 通知帧恒落于队尾（其前无并发数据帧竞争时保序）
  buf.clear();
  assert_eq!(mailbox.drain_into(&mut buf), 1);
  assert_eq!(buf[0].kind, PubSubMessageKind::ShardUnsubscribe);
  assert_eq!(buf[0].channel.as_ref(), b"c1");
  assert_eq!(mailbox.dropped_shard_notify(), 0, "腾位后入列成功零放弃");
  assert_eq!(mailbox.dropped(), 0, "控制通知不得计入数据面丢尾观测");
}

/// 满水位兜底臂之放弃面：无消费端排空时有界自旋必终止（严禁无限
/// 等待），超界 warn 留痕臂递增可观测计数 dropped_shard_notify，且
/// 数据面 dropped 计数与既有积压帧零扰动（§14 策略一字不动）
#[test]
fn control_notify_bounded_abandon_increments_observable_counter() {
  let mailbox = PubSubMailbox::new(2);
  assert!(mailbox.shard_publish(b"d1", b"v"));
  assert!(mailbox.shard_publish(b"d2", b"v"));

  let started = Instant::now();
  assert!(!mailbox.shard_forced_unsubscribe(b"c1"));
  let elapsed = started.elapsed();
  // 有界性硬断言：预算 256 轮让出重试必在远小于挂死感测窗内收尾
  assert!(
    elapsed.as_secs() < 30,
    "控制通知超界放弃疑似无界等待，耗时 {elapsed:?}"
  );

  assert_eq!(mailbox.dropped_shard_notify(), 1, "超界放弃必入观测计数");
  assert_eq!(mailbox.dropped(), 0, "控制面放弃与数据面丢尾分账");
  // 积压帧零扰动：丢的是通知尾帧，既有数据帧原序全在
  let mut buf = Vec::new();
  assert_eq!(mailbox.drain_into(&mut buf), 2);
  assert_eq!(buf[0].channel.as_ref(), b"d1");
  assert_eq!(buf[1].channel.as_ref(), b"d2");
}

/// 超界放弃后额度归零不饿死后续通知：空箱即投即回充（每调用恒保
/// 一次直投尝试），深慢积压下退避成本不随通知次数累积
#[test]
fn control_notify_after_abandon_still_lands_when_space_exists() {
  let mailbox = PubSubMailbox::new(2);
  assert!(mailbox.shard_publish(b"d1", b"v"));
  assert!(mailbox.shard_publish(b"d2", b"v"));
  // 满位无消费端：首帧超界放弃，额度归零
  assert!(!mailbox.shard_forced_unsubscribe(b"c1"));
  assert_eq!(mailbox.dropped_shard_notify(), 1);

  // 排空恢复空位：额度虽为 0，单次直投臂必入列并回充满额
  let mut buf = Vec::new();
  assert_eq!(mailbox.drain_into(&mut buf), 2);
  assert!(mailbox.shard_forced_unsubscribe(b"c2"));
  assert_eq!(mailbox.len(), 1, "空箱即投即回臂失守（额度归零饿死）");
  assert_eq!(mailbox.dropped_shard_notify(), 1, "成功入列不追加放弃计数");
  buf.clear();
  assert_eq!(mailbox.drain_into(&mut buf), 1);
  assert_eq!(buf[0].kind, PubSubMessageKind::ShardUnsubscribe);
  assert_eq!(buf[0].channel.as_ref(), b"c2");
}
