//! 满水位槽迁移强制退订通知兜底臂集成测试（工单
//! wpubsub-shard-forced-unsubscribe-frame-loss-stuck-subscription-flag）
//!
//! 覆盖两臂（真实 [`PubSubMailbox`] / [`SubscribeBroker`] / 会话 drain 面，
//! 零假 mock——MockSession 仅承接 [`PubSubSessionCommands`] 宿主切面七法，
//! 对位 tests/session_commands_unit.rs 既有夹具形态）：
//! - 入列臂：邮箱满时 `shard_slot_migrated_out` 经有界等待重试，消费端
//!   排空腾位后通知最终入列；会话 drain 见 sunsubscribe 帧、活跃计数
//!   归零、订阅旗收口；数据面满水位丢尾策略（deviations.md §14）零扰动
//! - 超界放弃臂：无消费端排空时有界自旋必终止（严禁无限等待锚），
//!   dropped_shard_notify 留痕且悬挂态如票面定性（同 ns 生存期内，
//!   drain 无从收口），后续腾位通知仍能即投即回充

use std::{
  sync::{Arc, mpsc},
  thread,
  time::Duration,
};

use wbase::hash_slot::slot_of;
use wpubsub::{
  session_commands::{PubSubSession, PubSubSessionCommands},
  subscribe_broker::SubscribeBroker,
  subscriber::{PubSubMailbox, PubSubMessageKind},
};

/// 邮箱水位（测试域小容量即满水位窗）
const CAPACITY: usize = 2;
/// 会话订阅槽锚单源（ns=0、db=0 与 MockSession 默认一致）
const SLOT: u16 = slot_of(0, 0);
/// 放弃臂有界收尾的挂死感测窗（预算 256 轮让出，远小于该窗即终止；
/// 超窗即坐实无限等待回归——测试本身随之失败，绝不挂起 CI）
const BOUND_TIMEOUT: Duration = Duration::from_secs(30);

/// 最小宿主切面（对位 tests/session_commands_unit.rs MockSession：
/// trait 必需法 + 集群门，零行为 mock，投递/收旗走真实 drain 臂）
struct MockSession {
  id: i64,
  output: Vec<u8>,
  is_subscription: bool,
  clustered: bool,
}

impl MockSession {
  fn new(id: i64) -> Self {
    Self {
      id,
      output: Vec::new(),
      is_subscription: false,
      clustered: false,
    }
  }
}

impl PubSubSessionCommands for MockSession {
  fn session_id(&self) -> i64 {
    self.id
  }

  fn output_mut(&mut self) -> &mut Vec<u8> {
    &mut self.output
  }

  fn set_subscription_session(&mut self, is_subscription: bool) {
    self.is_subscription = is_subscription;
  }

  fn has_cluster_session(&self) -> bool {
    self.clustered
  }
}

/// 入列臂全链路：SSUBSCRIBE 登锚 → 邮箱灌满数据帧 → 独立迁移线程推
/// 控制通知（满水位有界自旋）→ 主线程排空腾位 → 通知入列 → 会话 drain
/// 见 sunsubscribe 帧、计数归零、订阅旗收口；观测计数零放弃零数据丢弃
#[test]
fn slot_migration_notify_lands_after_drain_and_closes_subscription_flag() {
  let broker = Arc::new(SubscribeBroker::new());
  let mut wire = PubSubSession::with_mailbox_capacity(Some(broker.clone()), CAPACITY);
  let mut session = MockSession::new(7);
  session.clustered = true;

  // 真实订阅路径登槽锚（隔离键 ns=0 前缀折叠在 network_subscribe 内完成）
  assert!(session.network_subscribe(&mut wire, true, &[b"s1"]));
  assert!(session.is_subscription);
  assert_eq!(wire.num_active_channels, 1);
  session.output.clear();

  let mailbox = wire.mailbox().expect("接线态必有邮箱");
  // 灌满数据面水位：一帧分片推送触顶（丢尾策略 §14 所辖，本臂不触）
  assert_eq!(broker.publish_shard_now(b"0:s1", b"fill1"), 1);
  assert_eq!(broker.publish_shard_now(b"0:s1", b"fill2"), 1);
  assert_eq!(mailbox.len(), CAPACITY, "预置满水位");

  // 迁移线程：shard_slot_migrated_out 经控制通知臂满即有界等待重试
  let (tx, rx) = mpsc::channel::<usize>();
  {
    let broker = broker.clone();
    thread::spawn(move || {
      let _ = tx.send(broker.shard_slot_migrated_out(SLOT));
    });
  }
  // 主线程首次排空腾位（腾位与复投的先后由有界重试臂自然收敛；通知帧
  // 若恰随尾位一并出列亦保序——单队列 FIFO，恒在既有数据帧之后）
  assert!(
    session.drain_pubsub_frames(&mut wire) >= 1,
    "腾位排空须有帧可出"
  );

  let evicted = rx
    .recv_timeout(BOUND_TIMEOUT)
    .expect("迁移线程超界收尾或挂死（违反严禁无限等待约束）");
  assert_eq!(evicted, 1, "命中锚订阅恰清一条");

  // 通知帧入列观测面：零放弃、零数据面丢尾扰动
  assert_eq!(mailbox.dropped_shard_notify(), 0, "腾位后入列成功必零放弃");
  assert_eq!(mailbox.dropped(), 0, "控制臂不得扰动数据面丢尾计数");

  // 会话 drain 臂收口：见 sunsubscribe 帧（或已随首次排空出列）、活跃
  // 计数归零、订阅旗落下
  session.drain_pubsub_frames(&mut wire);
  let out = String::from_utf8_lossy(&session.output);
  assert!(
    out.contains("*3\r\n$12\r\nsunsubscribe\r\n$2\r\ns1\r\n:0\r\n"),
    "排空应答缺强制退订帧或计数未归零：{out}"
  );
  // 保序锚：既有数据帧恒先于控制通知出列（本测试帧序 fill1→fill2→通知）
  let (p_fill1, p_fill2, p_notify) = (
    out.find("fill1").expect("fill1 帧缺失"),
    out.find("fill2").expect("fill2 帧缺失"),
    out.find("sunsubscribe").expect("sunsubscribe 帧缺失"),
  );
  assert!(
    p_fill1 < p_fill2 && p_fill2 < p_notify,
    "保序违例：既有数据帧必恒先于控制通知出列：{out}"
  );
  assert_eq!(wire.num_active_channels, 0);
  assert!(!session.is_subscription, "活跃计数归零即订阅旗收口");
}

/// 超界放弃臂全链路：满水位无消费端 → 迁移线程有界自旋必终止（严禁
/// 挂死），dropped_shard_notify 留痕；悬挂态如票面定性（drain 无帧可收、
/// 计数/订阅旗维持悬挂）；既有积压数据帧零扰动；腾位后新通知即投即回充
#[test]
fn slot_migration_notify_bounded_abandon_is_observable_and_hang_scoped() {
  let broker = Arc::new(SubscribeBroker::new());
  let mut wire = PubSubSession::with_mailbox_capacity(Some(broker.clone()), CAPACITY);
  let mut session = MockSession::new(8);
  session.clustered = true;
  assert!(session.network_subscribe(&mut wire, true, &[b"s1"]));
  session.output.clear();

  let mailbox = wire.mailbox().expect("接线态必有邮箱");
  assert_eq!(broker.publish_shard_now(b"0:s1", b"fill1"), 1);
  assert_eq!(broker.publish_shard_now(b"0:s1", b"fill2"), 1);
  assert_eq!(mailbox.len(), CAPACITY);

  // 无消费端排空：迁移线程必在超界窗内放弃收尾（有界性硬锚；std 无
  // join_timeout 稳定面，以 mpsc 回传 + 主侧 recv_timeout 判界）
  let (tx, rx) = mpsc::channel::<usize>();
  {
    let broker = broker.clone();
    thread::spawn(move || {
      let _ = tx.send(broker.shard_slot_migrated_out(SLOT));
    });
  }
  let evicted = rx
    .recv_timeout(BOUND_TIMEOUT)
    .expect("放弃臂疑似无界等待，迁移线程未收尾");
  assert_eq!(evicted, 1, "锚订阅清理照常成立（放弃仅及于通知帧）");

  // 可观测计数留痕 + 数据面分账（§14 丢尾策略一字不动）
  assert_eq!(mailbox.dropped_shard_notify(), 1, "超界放弃必入可观测计数");
  assert_eq!(mailbox.dropped(), 0, "控制臂放弃不得计入数据面丢尾");
  let mut buf = Vec::new();
  assert_eq!(mailbox.drain_into(&mut buf), CAPACITY, "积压数据帧零扰动");
  assert_eq!(buf[0].value.as_ref(), b"fill1");

  // 悬挂定性：drain 无通知帧可收，活跃计数与订阅旗维持悬挂（同 ns
  // 生存期内，会话级整体清退方可收口——本臂如实锚定票面语义）
  assert_eq!(session.drain_pubsub_frames(&mut wire), 0);
  assert_eq!(wire.num_active_channels, 1, "放弃态订阅计数维持悬挂");
  assert!(session.is_subscription, "放弃态订阅旗维持悬挂");

  // 腾位后新通知即投即回充（额度归零不饿死后续通知）：真实再登锚、
  // 空箱迁出，通知帧必入列且放弃计数不追加
  let sink: Arc<PubSubMailbox> = mailbox.clone();
  assert!(broker.shard_subscribe(SLOT, b"0:s2", 8, sink));
  assert_eq!(broker.shard_slot_migrated_out(SLOT), 1);
  assert_eq!(mailbox.dropped_shard_notify(), 1, "成功入列不追加放弃计数");
  buf.clear();
  assert_eq!(mailbox.drain_into(&mut buf), 1);
  assert_eq!(buf[0].kind, PubSubMessageKind::ShardUnsubscribe);
  assert_eq!(buf[0].channel.as_ref(), b"0:s2");
}
