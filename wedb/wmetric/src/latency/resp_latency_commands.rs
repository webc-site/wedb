use wresp::{cmd_strings as cs, ext::RespVecExt, resp_memory_writer::RespWriter};

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
      // 由 cs::abort_with_args 单点成帧（内置净化单点 sanitize_error_str），对标 C#
      // RespLatencyCommands.cs:NetworkLatencyHistogram 的 TryWriteError 单口
      let event_name = String::from_utf8_lossy(invalid);
      cs::abort_with_args(
        output,
        "ERR Invalid event {0}. Try LATENCY HELP",
        &[&event_name],
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
      cs::abort_with_args(output, "ERR Invalid type {0}", &[&event_name]);
      return;
    }

    let events = events.as_deref().unwrap_or(&LatencyMetricsType::ALL);
    for &event in events {
      set_reset_flag(event);
    }
    output.write_resp_int(events.len() as i64);
  }

  /// 解析事件参数：返回 `(去重事件表, 末个非法事件)`。
  /// 未给参数时为 None（由调用方回落至全部默认类别）。
  #[doc(hidden)]
  pub fn parse_events<'a>(
    args: &[&'a [u8]],
  ) -> (Option<Vec<LatencyMetricsType>>, Option<&'a [u8]>) {
    if args.is_empty() {
      return (None, None);
    }

    let mut events = Vec::with_capacity(args.len().min(LatencyMetricsType::ALL.len()));
    let mut seen = [false; LatencyMetricsType::ALL.len()];
    // 对标 C# RespLatencyCommands.cs:57/:103：置位覆写不中断，扫尽后回显末个非法事件
    let mut invalid: Option<&[u8]> = None;
    for &arg in args {
      match LatencyMetricsType::from_name(arg) {
        Some(event) => {
          let idx = event.idx();
          if !seen[idx] {
            seen[idx] = true;
            events.push(event);
          }
        }
        None => invalid = Some(arg),
      }
    }
    (invalid.is_none().then_some(events), invalid)
  }
}
