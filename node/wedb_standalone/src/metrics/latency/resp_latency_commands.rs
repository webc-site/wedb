use super::{
  garnet_latency_metrics::GarnetLatencyMetrics, latency_metrics_type::LatencyMetricsType,
};
use crate::metrics::resp_write_utils::RespWriteUtils;

/// LATENCY 命令的响应编码（对标
/// libs/server/Metrics/Latency/RespLatencyCommands.cs:RespLatencyCommands，
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
  pub fn network_latency_help(arg_count: usize, output: &mut String) -> Result<(), &'static str> {
    if arg_count != 0 {
      return Err("ERR Unknown subcommand or wrong number of arguments for LATENCY HELP.");
    }
    let latency_commands = super::resp_latency_help::RespLatencyHelp::get_latency_commands();
    output.push_str(&RespWriteUtils::array_length(latency_commands.len()));
    for command in latency_commands {
      output.push_str(&RespWriteUtils::simple_string(command));
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
    output: &mut String,
  ) {
    let (events, invalid_event) = Self::parse_events(args);

    if let Some(invalid) = invalid_event {
      output.push_str(&format!(
        "-ERR Invalid event {}. Try LATENCY HELP\r\n",
        String::from_utf8_lossy(invalid)
      ));
      return;
    }

    let response = metrics.map_or_else(
      || "*0\r\n".to_string(),
      |m| m.get_resp_histograms(&events.unwrap_or_else(|| LatencyMetricsType::ALL.to_vec())),
    );
    output.push_str(&response);
  }

  /// libs/server/Metrics/Latency/RespLatencyCommands.cs:NetworkLatencyReset
  ///
  /// LATENCY RESET [EVENT...]：解析失败报 `ERR Invalid type <name>`；
  /// 成功则置位各类别的复位标志（经 `set_reset_flag`）并回复事件个数。
  pub fn network_latency_reset(
    args: &[&[u8]],
    set_reset_flag: &mut impl FnMut(LatencyMetricsType),
    output: &mut String,
  ) {
    let (events, invalid_event) = Self::parse_events(args);

    if let Some(invalid) = invalid_event {
      output.push_str(&format!(
        "-ERR Invalid type {}\r\n",
        String::from_utf8_lossy(invalid)
      ));
      return;
    }

    let events = events.unwrap_or_else(|| LatencyMetricsType::ALL.to_vec());
    for &event in &events {
      set_reset_flag(event);
    }
    output.push_str(&RespWriteUtils::integer(events.len() as i64));
  }

  /// 解析事件参数：返回 `(去重事件表, 首个非法事件)`。
  /// 未给参数时为全部默认类别（对齐 C# defaultLatencyTypes）。
  fn parse_events<'a>(args: &[&'a [u8]]) -> (Option<Vec<LatencyMetricsType>>, Option<&'a [u8]>) {
    if args.is_empty() {
      return (Some(LatencyMetricsType::ALL.to_vec()), None);
    }

    let mut events = Vec::new();
    for &arg in args {
      match Self::try_parse_event(arg) {
        Some(event) => {
          if !events.contains(&event) {
            events.push(event);
          }
        }
        None => return (None, Some(arg)),
      }
    }
    (Some(events), None)
  }

  /// 事件名 → 类别（ASCII 大小写不敏感；对齐
  /// SessionParseStateExtensions.TryGetLatencyMetricsType 的解析语义）。
  fn try_parse_event(arg: &[u8]) -> Option<LatencyMetricsType> {
    LatencyMetricsType::ALL
      .iter()
      .copied()
      .find(|t| t.cs_name().as_bytes().eq_ignore_ascii_case(arg))
  }
}

#[cfg(test)]
mod tests {
  use std::sync::{
    Arc,
    atomic::{AtomicU64, Ordering},
  };

  use super::{LatencyMetricsType, RespLatencyCommands};
  use crate::metrics::latency::garnet_latency_metrics::GarnetLatencyMetrics;

  fn metrics_with_sample() -> GarnetLatencyMetrics {
    let mut m = GarnetLatencyMetrics::new(GarnetLatencyMetrics::DEFAULT_LATENCY_TYPES);
    m.metrics[LatencyMetricsType::NetRsLat.idx()]
      .record(50_000)
      .expect("50_000 tick 在直方图范围内");
    m
  }

  #[test]
  fn help_renders_commands() {
    let mut out = String::new();
    assert_eq!(
      RespLatencyCommands::network_latency_help(0, &mut out),
      Ok(())
    );
    assert!(out.starts_with("*9\r\n"));
    assert!(out.contains("+HISTOGRAM [EVENT [EVENT...]]\r\n"));

    out.clear();
    assert!(RespLatencyCommands::network_latency_help(1, &mut out).is_err());
    assert!(out.is_empty());
  }

  #[test]
  fn histogram_all_and_specific() {
    let m = metrics_with_sample();
    let mut out = String::new();
    RespLatencyCommands::network_latency_histogram(&[], Some(&m), &mut out);
    assert!(out.starts_with("*2\r\n"));
    assert!(out.contains("histogram_usec"));

    out.clear();
    RespLatencyCommands::network_latency_histogram(&[b"net_rs_lat"], Some(&m), &mut out);
    assert!(out.starts_with("*2\r\n"));

    out.clear();
    RespLatencyCommands::network_latency_histogram(&[b"bogus"], Some(&m), &mut out);
    assert_eq!(out, "-ERR Invalid event bogus. Try LATENCY HELP\r\n");

    // 无监视器：*0\r\n。
    out.clear();
    RespLatencyCommands::network_latency_histogram(&[], None, &mut out);
    assert_eq!(out, "*0\r\n");
  }

  #[test]
  fn reset_sets_flags_and_counts() {
    let flags = Arc::new(AtomicU64::new(0));
    let mut set_flag = |t: LatencyMetricsType| {
      flags.fetch_or(1 << (t as u8), Ordering::Relaxed);
    };
    let mut out = String::new();
    RespLatencyCommands::network_latency_reset(
      &[b"PENDING_LAT", b"TX_PROC_LAT"],
      &mut set_flag,
      &mut out,
    );
    assert_eq!(out, ":2\r\n");
    assert_eq!(
      flags.load(Ordering::Relaxed),
      (1 << LatencyMetricsType::PendingLat as u8) | (1 << LatencyMetricsType::TxProcLat as u8)
    );

    out.clear();
    RespLatencyCommands::network_latency_reset(&[b"nope"], &mut set_flag, &mut out);
    assert_eq!(out, "-ERR Invalid type nope\r\n");

    out.clear();
    RespLatencyCommands::network_latency_reset(&[], &mut set_flag, &mut out);
    assert_eq!(out, ":6\r\n");
  }
}
