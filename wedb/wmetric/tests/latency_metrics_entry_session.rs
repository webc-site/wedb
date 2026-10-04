use wmetric::LatencyMetricsEntrySession;

#[test]
fn test_session_entry_lifecycle() {
  let mut entry = LatencyMetricsEntrySession::new();
  assert_eq!(entry.start_timestamp, 0);

  // 1. 正常 start 与 record_value
  entry.start(1_000);
  assert_eq!(entry.start_timestamp, 1_000);
  entry.record_value(0, 1_500);
  assert_eq!(entry.start_timestamp, 0);
  assert_eq!(entry.latency[0].len(), 1);
  assert_eq!(entry.latency[1].len(), 0);

  // 2. 无 start 直接 record_value 无操作
  entry.record_value(0, 2_000);
  assert_eq!(entry.latency[0].len(), 1);

  // 3. record_elapsed
  entry.record_elapsed(1, 200);
  assert_eq!(entry.latency[1].len(), 1);

  // 4. 0 或越界值处理
  entry.record_elapsed(1, 0);
  assert_eq!(entry.latency[1].len(), 1);

  // 5. return_to_pool 重置
  entry.return_to_pool();
  assert_eq!(entry.latency[0].len(), 0);
  assert_eq!(entry.latency[1].len(), 0);
}
