//! 订阅邮箱有界水位测试（慢订阅者不 drain 高频发布，水位刚性受控）
//!
//! 设计对位：C# `SubscribeBroker.Broadcast` 在发布线程同步直写订阅会话
//! 网络发送器（garnet/libs/server/PubSub/SubscribeBroker.cs:87/:108），
//! 发送器承载固定尺寸应答缓冲（garnet/libs/common/Networking/
//! GarnetTcpNetworkSender.cs:120-134），在途发送超门限（ThrottleMax=8，
//! 同文件 :47）时 `throttle.Wait()` 阻塞发布线程传导背压（同文件 :310-330）
//! ——订阅会话待投积压在 C# 里天然有界。
//! Rust 侧等价收口为有界邮箱：`PubSubMailbox` 基于 crossfire::flavor::Array
//! （IS_BOUNDED=true），水位 = 构造容量，发布端零阻塞、满即拒收丢尾帧。
//! 阻塞/拒收的修复性分叉登记于 doc/zh/deviations.md §14。

use std::{
  sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
  },
  thread,
  time::Instant,
};

use wpubsub::{subscribe_broker::SubscribeBroker, subscriber::PubSubMailbox};

/// 水位上限（帧数）
const CAPACITY: usize = 64;

#[test]
fn slow_subscriber_watermark_bounded_and_publisher_never_blocks() {
  let broker = Arc::new(SubscribeBroker::new());
  // 慢订阅者：订阅后全程不 drain
  let mailbox = Arc::new(PubSubMailbox::new(CAPACITY));
  broker.subscribe(b"hot", 1, mailbox.clone());

  // 发布洪水在独立线程执行；主线程持续采样水位并断言受控。
  // 发布线程若因满队列阻塞（错误实现），洪水在期限内无法收尾，
  // 水位采样虽仍受控，但下方 elapsed 断言必红——零阻塞契约同样被锚定。
  let publishers: Vec<_> = (0..4u8)
    .map(|t| {
      let broker = broker.clone();
      thread::spawn(move || {
        for j in 0..5_000u32 {
          broker.publish_now(b"hot", format!("{t}:{j}").as_bytes());
        }
      })
    })
    .collect();

  let started = Instant::now();
  let flooded = AtomicBool::new(false);
  while !flooded.load(Ordering::Relaxed) {
    assert!(
      mailbox.len() <= CAPACITY,
      "慢订阅者积压破水位：len={} > capacity={CAPACITY}",
      mailbox.len()
    );
    flooded.store(
      publishers.iter().all(|h| h.is_finished()),
      Ordering::Relaxed,
    );
  }
  for p in publishers {
    p.join().unwrap();
  }
  let elapsed = started.elapsed();
  assert!(
    elapsed.as_secs() < 30,
    "发布端疑似被满队列阻塞，20000 帧洪水耗时 {elapsed:?}"
  );

  // 洪水收尾：积压封顶水位，发布端无损跑完（拒收只发生在订阅侧）
  assert_eq!(
    mailbox.len(),
    CAPACITY,
    "不 drain 的慢订阅者积压必须恰好封顶 capacity"
  );

  // 丢尾不丢头：排空所得为各发布线程最先入队的帧前缀（每线程单通道 FIFO），
  // 帧集合恰为成功入队的 CAPACITY 帧
  let mut buf = Vec::new();
  assert_eq!(mailbox.drain_into(&mut buf), CAPACITY);
  assert!(buf.iter().all(|m| m.channel.as_ref() == b"hot"));

  // 排空后水位回落，恢复收帧（拒收仅发生在满位窗口）
  assert_eq!(broker.publish_now(b"hot", b"after-drain"), 1);
  assert_eq!(mailbox.len(), 1);
  buf.clear();
  mailbox.drain_into(&mut buf);
  assert_eq!(buf[0].value.as_ref(), b"after-drain");
}

/// 丢弃计数观测面（rn14 盲区收口：满水位拒收须可运维定位，零计数
/// 即观测面失明）：成功帧不计数，拒收帧逐帧单调累加，排空恢复收帧
/// 后计数不回退（历史丢弃量恒可读）
#[test]
fn drop_counter_tracks_rejected_frames_monotonically() {
  let broker = SubscribeBroker::new();
  let mailbox = Arc::new(PubSubMailbox::new(CAPACITY));
  broker.subscribe(b"ch", 1, mailbox.clone());
  assert_eq!(mailbox.dropped(), 0, "空箱必须零丢弃");

  // 容量内洪水：发布端逐帧命中订阅者（publish_now=1），零丢弃
  for j in 0..CAPACITY {
    assert_eq!(broker.publish_now(b"ch", j.to_string().as_bytes()), 1);
  }
  assert_eq!(mailbox.dropped(), 0, "容量内成功帧不得计数");

  // 满位窗口逐帧拒收：计数逐帧累加（publish_now 返回匹配订阅者数 1，丢弃计入 mailbox.dropped）
  for rejected in 1..=8u64 {
    assert_eq!(broker.publish_now(b"ch", b"overflow"), 1);
    assert_eq!(mailbox.dropped(), rejected, "第 {rejected} 帧拒收未入账");
  }

  // 排空后水位回落恢复收帧，历史丢弃量保持可读（单调不回退）
  let mut buf = Vec::new();
  assert_eq!(mailbox.drain_into(&mut buf), CAPACITY);
  assert_eq!(broker.publish_now(b"ch", b"after-drain"), 1);
  assert_eq!(mailbox.dropped(), 8, "恢复收帧后历史丢弃量不得回退");
}

#[test]
fn single_publisher_drops_only_tail_frames() {
  let broker = SubscribeBroker::new();
  let mailbox = Arc::new(PubSubMailbox::new(CAPACITY));
  broker.subscribe(b"ch", 1, mailbox.clone());

  // 单发布线程有序洪水：容量内前缀严格保留，越界帧全部拒收
  const TOTAL: usize = 1_000;
  for j in 0..TOTAL {
    broker.publish_now(b"ch", j.to_string().as_bytes());
  }
  let mut buf = Vec::new();
  assert_eq!(mailbox.drain_into(&mut buf), CAPACITY);
  for (slot, message) in buf.iter().enumerate() {
    assert_eq!(
      message.value.as_ref(),
      slot.to_string().as_bytes(),
      "丢尾不丢头：前 {CAPACITY} 帧应严格按序在箱"
    );
  }
}
