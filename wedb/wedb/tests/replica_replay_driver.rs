use std::sync::{Arc, Weak, atomic::Ordering};

use wedb::server::replication::{
  replica_replay_driver::ReplicaReplayDriver, replica_replay_driver_store::ReplicaReplayDriverStore,
};

/// 所有权面与生命周期：重放权 TryEnter/Exit 语义 + dispose 终止背景面
#[test]
fn test_replay_driver_ownership_lifecycle() {
  let driver = Arc::new(ReplicaReplayDriver::new(0, None, Weak::new()));
  assert_eq!(driver.physical_sublog_idx, 0);
  assert!(driver.is_active());

  // 重放权：空闲可获取，持有中重复获取失败，释放后可再获取
  assert!(driver.resume_replay());
  assert!(!driver.resume_replay());
  driver.suspend_replay();
  assert!(driver.resume_replay());
  driver.suspend_replay();

  // 退化形态（无资产）：背景重放不可启动，节流装配直通（None = 无需挂起）
  driver.initialize_background_replay_task(0);
  assert!(!driver.background_replay_started());
  assert!(driver.throttle_wait(0).is_none());

  // dispose：生命周期面关闭 + 节流等待者解除
  driver.dispose();
  assert!(!driver.is_active());
  assert!(driver.throttle_wait(0).is_none());
}

/// 时间脉冲：pending 单调记录（过期直退）；退化形态（无资产）applied
/// 不误推进
#[test]
fn test_signal_time_advance_monotonic() {
  let driver = ReplicaReplayDriver::new(0, None, Weak::new());
  driver.signal_time_advance(42);
  driver.signal_time_advance(41);
  driver.signal_time_advance(43);
  assert_eq!(
    driver.pending_pulse_sequence_number.load(Ordering::Acquire),
    43,
    "pending 单调记录最新脉冲"
  );
  assert_eq!(
    driver.applied_pulse_sequence_number.load(Ordering::Acquire),
    0,
    "无应用面时 applied 不推进"
  );
  // 内部重放记账位点保持
  assert_eq!(driver.replayed_offset.load(Ordering::Acquire), 0);
}

/// throttle_wait 装配门控真值面（对标 C# maxLag != -1 && tail - offset >
/// maxLag）：-1 禁用直通；资产缺失（lag=0）任何门限直通
#[test]
fn test_throttle_wait_gating() {
  let driver = Arc::new(ReplicaReplayDriver::new(0, None, Weak::new()));
  // -1 = 禁用
  assert!(driver.throttle_wait(-1).is_none());
  // 背景未启动（C# replayIterator == null）直通
  assert!(driver.throttle_wait(0).is_none());
  assert!(driver.throttle_wait(1024).is_none());
}

#[test]
fn test_replay_driver_store() {
  let store = ReplicaReplayDriverStore::new(2);
  assert!(store.get_replay_driver(0).is_none());

  let d0 = store
    .add_replica_replay_driver(0, None, Weak::new())
    .expect("开放容器可注册");
  assert_eq!(d0.physical_sublog_idx, 0);
  assert!(store.get_replay_driver(0).is_some());

  // 越上界 idx 拒注册（对标 C# 定长数组越界抛断连；禁孤儿驱动）
  assert!(!store.idx_in_bounds(2));
  assert!(store.idx_in_bounds(1));
  assert!(
    store
      .add_replica_replay_driver(2, None, Weak::new())
      .is_none()
  );
  assert!(store.get_replay_driver(2).is_none());
  assert_eq!(store.count(), 1);

  // dispose 后注册被拒（杜绝孤儿驱动，重连须经理 recovery 面重建）
  store.dispose();
  assert!(store.get_replay_driver(0).is_none());
  assert!(
    store
      .add_replica_replay_driver(1, None, Weak::new())
      .is_none()
  );
}
