//! 范围索引 AOF 复制活动追踪集成测试
//! （对应 libs/server/Resp/RangeIndex/RangeIndexReplicationActivities.cs）

use std::{thread::sleep, time::Duration};

use wbase::time::now_stopwatch_ticks;
use wnode::range_index::{
  range_index_manager_migration::PublishMigratedIndexResult,
  range_index_replication_activities::{ReassemblyActivity, StreamActivity},
};

#[test]
fn stream_activity_counters_and_first_error_wins() {
  let mut a = StreamActivity::start_activity(4096);
  assert_eq!(a.chunk_size, 4096);
  assert_eq!(a.chunk_count, 0);
  assert_eq!(a.total_bytes_enqueued, 0);
  assert_eq!(a.file_size_bytes, 0);
  assert!(a.error.is_none());

  a.on_file_length(100_000);
  assert_eq!(a.file_size_bytes, 100_000);

  a.on_chunk_enqueued(4096);
  a.on_chunk_enqueued(2048);
  assert_eq!(a.chunk_count, 2);
  assert_eq!(a.total_bytes_enqueued, 6144);

  // 首错固化语义：后续错误不覆盖首个错误
  a.on_error("first error");
  a.on_error("second error");
  assert_eq!(a.error.as_deref(), Some("first error"));

  a.end_and_log(b"index_key_1");
}

#[test]
fn stream_activity_success_path_logs() {
  let mut a = StreamActivity::start_activity(1024);
  a.on_file_length(2048);
  a.on_chunk_enqueued(1024);
  a.on_chunk_enqueued(1024);
  assert!(a.error.is_none());
  assert_eq!(a.chunk_count, 2);
  assert_eq!(a.total_bytes_enqueued, 2048);

  a.end_and_log(b"index_key_success");
}

#[test]
fn reassembly_activity_counters_and_publish_result() {
  let mut a = ReassemblyActivity::start_activity();
  assert_eq!(a.chunk_count, 0);
  assert_eq!(a.total_bytes_received, 0);
  assert!(a.publish_result.is_none());

  a.on_chunk_received(512);
  a.on_chunk_received(256);
  assert_eq!(a.chunk_count, 2);
  assert_eq!(a.total_bytes_received, 768);

  a.on_publish_result(PublishMigratedIndexResult::Success);
  assert_eq!(a.publish_result, Some(PublishMigratedIndexResult::Success));

  a.end_and_log(b"index_key_2", "Complete");
}

#[test]
fn activity_started_ticks_monotonic_domain() {
  let stream = StreamActivity::start_activity(1024);
  let reasm = ReassemblyActivity::start_activity();

  assert!(stream.started_ticks > 0);
  assert!(reasm.started_ticks > 0);

  sleep(Duration::from_millis(5));

  let now = now_stopwatch_ticks();
  assert!(now >= stream.started_ticks);
  assert!(now >= reasm.started_ticks);
}
