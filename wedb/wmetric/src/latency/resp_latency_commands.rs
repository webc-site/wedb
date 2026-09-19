use wresp::{
  cmd_strings as cs,
  ext::{RespVecExt, sanitize_error_str},
  resp_memory_writer::RespWriter,
};

use super::{
  garnet_latency_metrics::GarnetLatencyMetrics, latency_metrics_type::LatencyMetricsType,
};

/// LATENCY 命令的响应编码（对标
/// libs/server/Metrics/Latency/RespLatencyCommands.cs，
/// C# 内嵌于 RespServerSession partial）。
///
/// 会话缓冲管理（`SendAndReset` 循环）属会话域；此处以
/// `(参数切片, 指标句柄, 输出缓冲)` 的纯函数形式承接 1:1 语义。
pub struct RespLatencyCommands;

impl RespLatencyCommands {
  /// libs/server/Metrics/Latency/RespLatencyCommands.cs:NetworkLatencyHelp
  ///
  /// LATENCY HELP：不接受附加参数，输出子命令帮助文本数组；
  /// 参数个数不符时返回错误串（对齐 C# AbortWithErrorMessage）。
  pub fn network_latency_help(arg_count: usize, output: &mut Vec<u8>) -> Result<(), &'static str> {
    if arg_count != 0 {
      return Err("ERR Unknown subcommand or wrong number of arguments for LATENCY HELP.");
    }
    let latency_commands = super::resp_latency_help::RespLatencyHelp::get_latency_commands();
    output.write_resp_array_len(latency_commands.len());
    for command in latency_commands {
      output.write_resp_simple_string(command);
    }
    Ok(())
  }

  /// libs/server/Metrics/Latency/RespLatencyCommands.cs:NetworkLatencyHistogram
  ///
  /// LATENCY HISTOGRAM [EVENT...]：给定事件解析失败即整批报错；
  /// 未给事件则回复全部默认类别。无监视器（metrics 为 None）时回复 `*0\r\n`。
  pub fn network_latency_histogram(
    args: &[&[u8]],
    metrics: Option<&GarnetLatencyMetrics>,
    output: &mut Vec<u8>,
  ) {
    let (events, invalid_event) = Self::parse_events(args);

    if let Some(invalid) = invalid_event {
      // 事件名是客户原始 arg（bulk string 内的 CRLF 由解析器原样保留）：回显段
      // 先过门面净化单点 sanitize_error_str（CRLF 切断 + MAX_PARAM_NAME_LEN
      // 长度帽），整帧由 cs::abort_with_error_message 单点成帧，对标 C#
      // RespLatencyCommands.cs:NetworkLatencyHistogram 的 TryWriteError 单口
      let event_name = String::from_utf8_lossy(invalid);
      let clean = sanitize_error_str(&event_name, cs::MAX_PARAM_NAME_LEN);
      cs::abort_with_error_message(
        output,
        &format!("ERR Invalid event {clean}. Try LATENCY HELP"),
      );
      return;
    }

    match metrics {
      None => RespWriter::new_ref(output).write_empty_array(),
      Some(m) => m.get_resp_histograms(
        events.as_deref().unwrap_or(&LatencyMetricsType::ALL),
        output,
      ),
    }
  }

  /// libs/server/Metrics/Latency/RespLatencyCommands.cs:NetworkLatencyReset
  ///
  /// LATENCY RESET [EVENT...]：解析失败报 `ERR Invalid type <name>`；
  /// 成功则置位各类别的复位标志（经 `set_reset_flag`）并回复事件个数。
  pub fn network_latency_reset(
    args: &[&[u8]],
    set_reset_flag: &mut impl FnMut(LatencyMetricsType),
    output: &mut Vec<u8>,
  ) {
    let (events, invalid_event) = Self::parse_events(args);

    if let Some(invalid) = invalid_event {
      // 同 network_latency_histogram：回显段经净化单点，帧由单点成帧
      let event_name = String::from_utf8_lossy(invalid);
      let clean = sanitize_error_str(&event_name, cs::MAX_PARAM_NAME_LEN);
      cs::abort_with_error_message(output, &format!("ERR Invalid type {clean}"));
      return;
    }

    let events = events.as_deref().unwrap_or(&LatencyMetricsType::ALL);
    for &event in events {
      set_reset_flag(event);
    }
    output.write_resp_int(events.len() as i64);
  }

  /// 解析事件参数：返回 `(去重事件表, 首个非法事件)`。
  /// 未给参数时为 None（由调用方回落至全部默认类别）。
  fn parse_events<'a>(args: &[&'a [u8]]) -> (Option<Vec<LatencyMetricsType>>, Option<&'a [u8]>) {
    if args.is_empty() {
      return (None, None);
    }

    let mut events = Vec::with_capacity(args.len().min(LatencyMetricsType::ALL.len()));
    let mut seen = [false; LatencyMetricsType::ALL.len()];
    for &arg in args {
      match LatencyMetricsType::from_name(arg) {
        Some(event) => {
          let idx = event.idx();
          if !seen[idx] {
            seen[idx] = true;
            events.push(event);
          }
        }
        None => return (None, Some(arg)),
      }
    }
    (Some(events), None)
  }
}

#[cfg(test)]
mod tests {
  use super::*;

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
    RespLatencyCommands::network_latency_reset(
      &[b"NET_RS_LAT"],
      &mut |_| reset_count += 1,
      &mut out,
    );
    assert_eq!(reset_count, 1);
    assert_eq!(out, b":1\r\n");

    out.clear();
    RespLatencyCommands::network_latency_histogram(&[], None, &mut out);
    assert_eq!(out, b"*0\r\n");
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
}
