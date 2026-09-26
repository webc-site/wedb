//! 退订清旗窗邮箱残帧的写回点回归（消费泵双路等待臂接线 + 会话侧 ack 序）
//!
//! 缺陷（task/todo/wnode-unsubscribe-mailed-frame-writeback-gap.md）：
//! C# 广播线程经订阅会话批锁内联直写发送器（`SubscribeBroker.cs:87/:108` →
//! `RespServerSession.cs:492/:575`），退订处理窗内已落地帧恒即时写回；rust 推送
//! 经会话邮箱中转，写回点仅三处（轮头 drain、读挂起双路等待臂、会话自身 PUBLISH
//! 体内 drain）。全退清旗那轮 `set_subscription_session(false)` 门死双路等待臂
//! （其门控原为 `is_subscription_session`），窗内落地帧本轮无写回点，滞留至下一
//! 输入批轮头才冲出（活跃客户端 ack 后插帧、静默客户端滞留至连接关闭）。
//!
//! 修复两点（本文件锁定）：退订族命令体摘订阅前先冲邮箱，令已落地帧严格先于
//! ack 出流（`unsubscribe_output_carries_mailed_frame_before_ack`，trait 侧三命令
//! 逐条锁在 `wpubsub/tests/unsubscribe_pending_frame_writeback.rs`）；读挂起唤醒臂
//! 门控由「订阅态」扩为「订阅态或邮箱非空」
//! （`cleared_session_with_mailed_frame_keeps_dual_path_arm_wired`）。
//! `wake_arm_gate_form_preserved` 钉住原形态：非订阅且空邮箱恒不接线，
//! 杜绝门控演化为恒开唤醒的第二形态。

use std::sync::Arc;

use tempfile::TempDir;
use wdev::SegmentedDevice;
use wkv::WedbStore;
use wnode::{
  MessageConsumerFace, RespSessionConsumer,
  resp::{garnet_api::StoreGarnetApi, resp_server_session::RespServerSessionOptions},
};
use wnode_test::drain_output;
use wpubsub::{
  subscribe_broker::SubscribeBroker,
  subscriber::{PubSubMailbox, PubSubSink},
};
use wtest_base::{open_test_store, resp_frame as frame};

type TestStore = WedbStore<SegmentedDevice>;

/// 独立临时存储（真装配，测试 1/2 共用同一 store 的多个会话）
fn open_store() -> (TempDir, Arc<TestStore>) {
  open_test_store("unsub-writeback").unwrap()
}

/// 真存储装配的会话消费者（生产 thread-per-core 形态）
fn consumer_on(store: &Arc<TestStore>) -> RespSessionConsumer {
  RespSessionConsumer::new(
    1,
    RespServerSessionOptions::default(),
    Arc::new(StoreGarnetApi::new(store.new_session().unwrap())),
  )
}

/// 喂帧并取本轮应答字节（同步段即时输出形态，无挂起命令）
fn feed(c: &mut RespSessionConsumer, args: &[&[u8]]) -> Vec<u8> {
  let mut scratch = c.take_recv_scratch();
  scratch.extend_from_slice(&frame(args));
  c.return_recv_scratch(scratch);
  let mut out = Vec::new();
  assert!(c.try_consume_messages_into(&mut out).is_some(), "协议违规");
  out
}

/// 本会话接线邮箱
fn mailbox(c: &RespSessionConsumer) -> Arc<PubSubMailbox> {
  c.session().pubsub.mailbox().expect("pubsub 接线在场")
}

/// 全退清旗后邮箱有残帧：读挂起唤醒臂必须接线并即时冲净该帧
///
/// 修复前 `pubsub_mailbox` 门控恒取 `is_subscription_session`，清旗即返回 None
/// 落纯读臂，残帧无写回点（断言 1 即红）
#[test]
fn cleared_session_with_mailed_frame_keeps_dual_path_arm_wired() {
  let (_dir, store) = open_store();
  let mut c = consumer_on(&store);
  let broker = Arc::new(SubscribeBroker::new());
  c.session_mut().attach_pubsub(broker);
  assert!(c.session_mut().network_subscribe(false, &[b"ch"]));
  let mbox = mailbox(&c);
  assert!(c.session_mut().network_unsubscribe(&[b"ch"]));
  assert!(
    !c.session().is_subscription_session,
    "前置：末通道退订即清旗（订阅态门控源）"
  );
  drain_output(c.session_mut());

  // 退订窗内 in-flight 广播落地：发布线程在 broker 摘除前已 pin 本会话订阅者
  // 快照，摘除后 broker 直投不可达，故等价复刻为 sink 直投（同 PubSubSink 面）
  mbox.publish(b"0:ch", b"late");
  assert!(c.pubsub_mailbox().is_some(), "清旗残帧未接双路等待臂");
  let mut buf = Vec::new();
  c.drain_pubsub_into(&mut buf);
  assert_eq!(
    buf, b"*3\r\n$7\r\nmessage\r\n$2\r\nch\r\n$4\r\nlate\r\n",
    "唤醒臂排空写出的是残帧本体"
  );
  assert!(!mbox.has_messages(), "残帧未出邮箱");
}

/// 原形态守恒：非订阅 + 空邮箱恒不接线；订阅态空邮箱恒接线；无接线恒不接线
///
/// 钉住「邮箱非空」补臂不演化为恒开唤醒（禁第二形态）
#[test]
fn wake_arm_gate_form_preserved() {
  let (_dir, store) = open_store();
  // 无 broker 接线（--pubsub 关闭）：恒 None
  let bare = consumer_on(&store);
  assert!(bare.pubsub_mailbox().is_none());
  // 非订阅态 + 空邮箱：恒 None（纯读阻塞，零额外唤醒成本）
  let mut c = consumer_on(&store);
  c.session_mut()
    .attach_pubsub(Arc::new(SubscribeBroker::new()));
  assert!(c.pubsub_mailbox().is_none());
  // 订阅态：邮箱空亦接线（空闲推送即时投递）
  assert!(c.session_mut().network_subscribe(false, &[b"ch"]));
  assert!(c.pubsub_mailbox().is_some());
}

/// 真会话消费者链路：窗内已落地帧经退订命令体先冲，随 ack 同缓冲有序出流
///
/// 修复前退订命令体无 drain：本批应答仅 ack 字节，message 帧延到下一批
/// （断言「先 message 后 ack」即红）
#[test]
fn unsubscribe_output_carries_mailed_frame_before_ack() {
  let (_dir, store) = open_store();
  let mut c = consumer_on(&store);
  let broker = Arc::new(SubscribeBroker::new());
  c.session_mut().attach_pubsub(broker.clone());

  assert_eq!(
    feed(&mut c, &[b"SUBSCRIBE", b"ch"]),
    b"*3\r\n$9\r\nsubscribe\r\n$2\r\nch\r\n:1\r\n"
  );
  // 真广播路径：订阅在册即入本会话邮箱
  assert_eq!(broker.publish_now(b"0:ch", b"m1"), 1);
  assert!(mailbox(&c).has_messages(), "前置：帧已入邮箱");

  let out = feed(&mut c, &[b"UNSUBSCRIBE", b"ch"]);
  assert_eq!(
    out,
    b"*3\r\n$7\r\nmessage\r\n$2\r\nch\r\n$2\r\nm1\r\n*3\r\n$11\r\nunsubscribe\r\n$2\r\nch\r\n:0\r\n",
    "已落地帧未先于 unsubscribe ack 出流"
  );
  assert!(!mailbox(&c).has_messages());
}
