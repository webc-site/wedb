use wmetric::{GarnetSessionMetrics, SessionMetricsHandle};

#[test]
fn handle_incr_snapshot_roundtrip() {
  let h = SessionMetricsHandle::default();
  h.incr_total_net_input_bytes(100);
  h.incr_total_net_output_bytes(200);
  h.incr_total_commands_processed(7);
  h.incr_total_pending(2);
  h.incr_total_found(5);
  h.incr_total_notfound(2);
  h.incr_total_cluster_commands_processed(1);
  h.add_total_write_commands_processed(3);
  h.add_total_read_commands_processed(4);
  h.incr_total_number_resp_server_session_exceptions(0);

  let m = h.snapshot();
  assert_eq!(m.get_total_net_input_bytes(), 100);
  assert_eq!(m.get_total_net_output_bytes(), 200);
  assert_eq!(m.get_total_commands_processed(), 7);
  assert_eq!(m.get_total_pending(), 2);
  assert_eq!(m.get_total_found(), 5);
  assert_eq!(m.get_total_notfound(), 2);
  assert_eq!(m.get_total_cluster_commands_processed(), 1);
  assert_eq!(m.get_total_write_commands_processed(), 3);
  assert_eq!(m.get_total_read_commands_processed(), 4);
  assert_eq!(m.get_total_number_resp_server_session_exceptions(), 0);
}

#[test]
fn add_aggregates_every_counter() {
  let mut base = SessionMetricsHandle::default().snapshot();
  base.add(&GarnetSessionMetrics {
    total_commands_processed: 10,
    total_found: 6,
    ..Default::default()
  });
  base.add(&GarnetSessionMetrics {
    total_commands_processed: 3,
    total_found: 1,
    total_net_input_bytes: 64,
    ..Default::default()
  });
  assert_eq!(base.get_total_commands_processed(), 13);
  assert_eq!(base.get_total_found(), 7);
  assert_eq!(base.get_total_net_input_bytes(), 64);

  base.reset();
  assert_eq!(base, GarnetSessionMetrics::default());
}
