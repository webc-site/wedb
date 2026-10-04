use wmetric::{LatencyMetricsType, RespLatencyCommands};
use wresp::cmd_strings as cs;

#[test]
fn test_parse_events_dedup_and_invalid() {
  let (events, invalid) = RespLatencyCommands::parse_events(&[b"NET_RS_LAT", b"net_rs_lat"]);
  assert!(invalid.is_none());
  assert_eq!(events, Some(vec![LatencyMetricsType::NetRsLat]));

  let (events, invalid) = RespLatencyCommands::parse_events(&[b"NET_RS_LAT", b"UNKNOWN"]);
  assert_eq!(events, None);
  assert_eq!(invalid, Some(&b"UNKNOWN"[..]));
}

#[test]
fn test_network_latency_help() {
  let mut out = Vec::new();
  assert!(RespLatencyCommands::network_latency_help(0, &mut out).is_ok());
  assert!(out.starts_with(b"*9\r\n"));

  out.clear();
  assert!(RespLatencyCommands::network_latency_help(1, &mut out).is_err());
}

#[test]
fn test_network_latency_reset_and_histogram() {
  let mut out = Vec::new();
  let mut reset_count = 0;
  RespLatencyCommands::network_latency_reset(&[b"NET_RS_LAT"], &mut |_| reset_count += 1, &mut out);
  assert_eq!(reset_count, 1);
  assert_eq!(out, b":1\r\n");

  out.clear();
  RespLatencyCommands::network_latency_histogram(&[], None, &mut out);
  assert_eq!(out, b"*0\r\n");
}

/// 多非法项回显末个（对标 C# invalidEvent 覆写语义，工单
/// wmetric-invalid-item-echo-first-vs-last）：HISTOGRAM / RESET 两口
/// 同型锁测 + 合法项穿插混合形，错误帧整帧断言即覆盖「不误回数据帧」
#[test]
fn invalid_event_echoes_last_occurrence() {
  let mut out = Vec::new();
  RespLatencyCommands::network_latency_histogram(&[b"E1", b"E2"], None, &mut out);
  assert_eq!(out, b"-ERR Invalid event E2. Try LATENCY HELP\r\n");

  out.clear();
  let mut reset_count = 0;
  RespLatencyCommands::network_latency_reset(&[b"E1", b"E2"], &mut |_| reset_count += 1, &mut out);
  assert_eq!(reset_count, 0);
  assert_eq!(out, b"-ERR Invalid type E2\r\n");

  // 混合形 VALID1 BOGUS1 VALID2 BOGUS2：回显末个非法项
  out.clear();
  RespLatencyCommands::network_latency_histogram(
    &[b"NET_RS_LAT", b"BOGUS1", b"NET_RS_LAT", b"BOGUS2"],
    None,
    &mut out,
  );
  assert_eq!(out, b"-ERR Invalid event BOGUS2. Try LATENCY HELP\r\n");

  out.clear();
  let mut reset_count = 0;
  RespLatencyCommands::network_latency_reset(
    &[b"NET_RS_LAT", b"BOGUS1", b"NET_RS_LAT", b"BOGUS2"],
    &mut |_| reset_count += 1,
    &mut out,
  );
  assert_eq!(reset_count, 0);
  assert_eq!(out, b"-ERR Invalid type BOGUS2\r\n");
}

/// 帧注入回归：非法事件名含 CRLF 时，回显段经净化单点截断，一参数只出一帧
#[test]
fn invalid_event_crlf_cannot_inject_frame() {
  let mut out = Vec::new();
  RespLatencyCommands::network_latency_histogram(&[b"NET\r\n:1\r\n"], None, &mut out);
  assert_eq!(out, b"-ERR Invalid event NET. Try LATENCY HELP\r\n");

  out.clear();
  RespLatencyCommands::network_latency_reset(&[b"NET\n:2\r\n"], &mut |_| {}, &mut out);
  assert_eq!(out, b"-ERR Invalid type NET\r\n");

  // 超长事件名受 MAX_PARAM_NAME_LEN 帽约束，且不吞掉帧尾提示
  let long = vec![b'a'; 4096];
  out.clear();
  RespLatencyCommands::network_latency_histogram(&[long.as_slice()], None, &mut out);
  let expect = format!(
    "-ERR Invalid event {}. Try LATENCY HELP\r\n",
    "a".repeat(cs::MAX_PARAM_NAME_LEN)
  );
  assert_eq!(out, expect.as_bytes());
  assert_eq!(out.iter().filter(|&&b| b == b'\n').count(), 1);
}
