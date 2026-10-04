//! 范围索引集群迁移活动追踪集成测试
//! （对应 libs/cluster/Server/Migration/RangeIndex/RangeIndexMigrationActivities.cs）

use std::{path::Path, thread::sleep, time::Duration};

use wbase::time::TICKS_PER_SECOND;
use wnode::range_index::range_index_migration_activities::{
  MigrateActivity, ReceiveActivity, TransmitActivity,
};

/// 单调域读数恒 > 0 且随区间推进严格不减：end 读数必须大于 start 读数
#[test]
fn migrate_activity_ticks_monotonic() {
  let mut a = MigrateActivity::start_activity(3);
  assert!(a.started_ticks > 0);
  sleep(Duration::from_millis(20));
  a.on_transmitting();
  a.on_deleting();
  a.end();
  assert!(a.ended_ticks > a.started_ticks);
  assert!(a.transmitting_ticks >= a.started_ticks);
  assert!(a.deleting_ticks >= a.transmitting_ticks);
  assert!(a.ended_ticks >= a.deleting_ticks);
  // 全阶段齐备时日志求差不为钳 0 的 -1 哨兵
  a.log_activity();
}

/// snapshot_ticks 为 100ns 单调刻度：换算毫秒后落在实际睡眠时长的合理区间，
/// 与墙钟时刻无关（墙钟回拨/前跳不影响单调域作差）
#[test]
fn transmit_activity_snapshot_ticks_monotonic_domain() {
  let mut a = TransmitActivity::start_activity();
  sleep(Duration::from_millis(20));
  a.on_snapshot_completed(4096);
  a.end();
  assert!(a.ended_ticks > a.started_ticks);
  // 下限：不小于实际睡眠的 90%（20ms 的下界近似）
  let lower_ticks = 18 * TICKS_PER_SECOND / 1000;
  assert!(a.snapshot_ticks >= lower_ticks);
  // 上限：远小于墙钟可干扰量级，仅由单调区间决定（60s 恒定宽松上界）
  let upper_ticks = 60 * TICKS_PER_SECOND;
  assert!(a.snapshot_ticks < upper_ticks);
  // 换算毫秒量级即睡眠量级（纳秒冒名刻度会在此放大十倍而被捕获）
  let snapshot_ms = a.snapshot_ticks / (TICKS_PER_SECOND / 1_000);
  assert!((18..60_000).contains(&snapshot_ms));
  a.log_activity(b"idx");
}

#[test]
fn receive_activity_ticks_monotonic() {
  let mut a = ReceiveActivity::start_activity(Path::new("/tmp/migrated.idx"));
  sleep(Duration::from_millis(20));
  a.on_chunk_received(128);
  a.on_publishing();
  a.end();
  assert!(a.ended_ticks > a.started_ticks);
  assert!(a.publish_ticks > 0);
  a.log_activity(b"k");
}
