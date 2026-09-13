use std::str::from_utf8;

use super::{
  garnet_latency_metrics::GarnetLatencyMetrics, latency_metrics_type::LatencyMetricsType,
};
use crate::resp_write_utils::RespWriteUtils;

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
    RespWriteUtils::push_array_length(output, latency_commands.len());
    for command in latency_commands {
      RespWriteUtils::push_simple_string(output, command);
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
      output.push_str("-ERR Invalid event ");
      if let Ok(valid) = from_utf8(invalid) {
        output.push_str(valid);
      } else {
        output.push_str(&String::from_utf8_lossy(invalid));
      }
      output.push_str(". Try LATENCY HELP\r\n");
      return;
    }

    let response = metrics.map_or_else(
      || "*0\r\n".to_string(),
      |m| m.get_resp_histograms(events.as_deref().unwrap_or(&LatencyMetricsType::ALL)),
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
      output.push_str("-ERR Invalid type ");
      if let Ok(valid) = from_utf8(invalid) {
        output.push_str(valid);
      } else {
        output.push_str(&String::from_utf8_lossy(invalid));
      }
      output.push_str("\r\n");
      return;
    }

    let events = events.as_deref().unwrap_or(&LatencyMetricsType::ALL);
    for &event in events {
      set_reset_flag(event);
    }
    RespWriteUtils::push_integer(output, events.len() as i64);
  }

  /// 解析事件参数：返回 `(去重事件表, 首个非法事件)`。
  /// 未给参数时为 None（由调用方回落至全部默认类别）。
  fn parse_events<'a>(args: &[&'a [u8]]) -> (Option<Vec<LatencyMetricsType>>, Option<&'a [u8]>) {
    if args.is_empty() {
      return (None, None);
    }

    let mut events = Vec::with_capacity(args.len());
    for &arg in args {
      match LatencyMetricsType::from_name(arg) {
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
}
