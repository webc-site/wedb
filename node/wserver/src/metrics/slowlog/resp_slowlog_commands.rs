use super::{slow_log_container::SlowLogContainer, slowlog_entry::SlowLogEntry};
use crate::{
  metrics::{latency::latency_metrics_entry::time_stamp, resp_write_utils::RespWriteUtils},
  types::RespCommand,
};

/// SLOWLOG 命令的响应编码（对标
/// libs/server/Metrics/Slowlog/RespSlowlogCommands.cs:RespSlowlogCommands，
/// C# 内嵌于 RespServerSession partial）。
///
/// 会话缓冲管理（`SendAndReset` 循环）属会话域；此处以纯函数形式承接语义，
/// 输出直写 RESP 缓冲。慢日志未启用（container 为 None）时 GET 回空数组、
/// LEN 回 0、RESET 静默（对齐 C# 空传播）。
pub struct RespSlowlogCommands;

/// HandleSlowLog 的输入上下文（打包会话侧事实，避免超长参数表）。
pub struct SlowLogContext<'a> {
  /// 慢日志容器。
  pub container: &'a SlowLogContainer,
  /// 触发命令。
  pub cmd: RespCommand,
  /// 当前 Stopwatch tick。
  pub now_ticks: i64,
  /// 慢日志阈值（tick）。
  pub slow_log_threshold: i64,
  /// 客户端 IP:端口。
  pub client_ip_port: &'a str,
  /// 客户端名。
  pub client_name: &'a str,
  /// 解析状态快照。
  pub arguments: Option<Vec<u8>>,
}

/// C# `CmdStrings.GenericErrWrongNumArgs`（`{0}` 为子命令名）。
const GENERIC_ERR_WRONG_NUM_ARGS: &str = "ERR wrong number of arguments for '{0}' command";

/// C# `CmdStrings.RESP_ERR_COUNT_IS_OUT_OF_RANGE_N1`。
const RESP_ERR_COUNT_IS_OUT_OF_RANGE_N1: &str = "ERR count should be greater than or equal to -1.";

impl RespSlowlogCommands {
  /// libs/server/Metrics/Slowlog/RespSlowlogCommands.cs:NetworkSlowLogHelp
  ///
  /// SLOWLOG HELP：不接受附加参数，输出子命令帮助文本数组。
  pub fn network_slow_log_help(arg_count: usize, output: &mut String) -> Result<(), String> {
    if arg_count != 0 {
      return Err(wrong_num_args("slowlog help"));
    }

    let slow_log_commands = super::resp_slowlog_help::RespSlowlogHelp::get_slow_log_commands();
    output.push_str(&RespWriteUtils::array_length(slow_log_commands.len()));
    for command in slow_log_commands {
      output.push_str(&RespWriteUtils::simple_string(command));
    }
    Ok(())
  }

  /// libs/server/Metrics/Slowlog/RespSlowlogCommands.cs:NetworkSlowLogGet
  ///
  /// SLOWLOG GET [count]：count 缺省 10，负值仅接受 -1（全部）；
  /// 解析失败或 < -1 时报错。
  pub fn network_slow_log_get(
    args: &[&[u8]],
    container: Option<&SlowLogContainer>,
    output: &mut String,
  ) -> Result<(), String> {
    if args.len() > 1 {
      return Err(wrong_num_args("slowlog get"));
    }

    let mut count: i32 = 10;
    if let Some(arg) = args.first() {
      let Some(parsed) = parse_i32(arg).filter(|&c| c >= -1) else {
        output.push_str(&format!("-{RESP_ERR_COUNT_IS_OUT_OF_RANGE_N1}\r\n"));
        return Ok(());
      };
      count = parsed;
    }

    let Some(container) = container else {
      output.push_str(&RespWriteUtils::array_length(0));
      return Ok(());
    };

    let entries = container.get_entries(count);
    output.push_str(&RespWriteUtils::array_length(entries.len()));
    for entry in &entries {
      // 每条目：id、timestamp、duration、参数数组、client ip:port、client name。
      output.push_str(&RespWriteUtils::array_length(6));
      output.push_str(&RespWriteUtils::integer(i64::from(entry.id)));
      output.push_str(&RespWriteUtils::integer(i64::from(entry.timestamp)));
      output.push_str(&RespWriteUtils::integer(i64::from(entry.duration)));

      let command_name = format!("{:?}", entry.command);
      match &entry.arguments {
        None => {
          output.push_str(&RespWriteUtils::array_length(1));
          output.push_str(&RespWriteUtils::bulk_string(&command_name));
        }
        Some(bytes) => {
          // 反序列化解析状态快照（`[count i32][4B 长度前缀 + 数据]` 布局，
          // 对齐 SessionParseState.SerializeTo）。
          let tokens = deserialize_args(bytes);
          output.push_str(&RespWriteUtils::array_length(tokens.len() + 1));
          output.push_str(&RespWriteUtils::bulk_string(&command_name));
          for token in &tokens {
            output.push_str(&RespWriteUtils::bulk_string_bytes(token));
          }
        }
      }

      output.push_str(&RespWriteUtils::bulk_string(&entry.client_ip_port));
      output.push_str(&RespWriteUtils::bulk_string(&entry.client_name));
    }
    Ok(())
  }

  /// libs/server/Metrics/Slowlog/RespSlowlogCommands.cs:NetworkSlowLogLen
  ///
  /// SLOWLOG LEN：不接受附加参数。
  pub fn network_slow_log_len(
    arg_count: usize,
    container: Option<&SlowLogContainer>,
    output: &mut String,
  ) -> Result<(), String> {
    if arg_count != 0 {
      return Err(wrong_num_args("slowlog len"));
    }
    output.push_str(&RespWriteUtils::integer(i64::from(
      container.map_or(0, SlowLogContainer::count),
    )));
    Ok(())
  }

  /// libs/server/Metrics/Slowlog/RespSlowlogCommands.cs:NetworkSlowLogReset
  ///
  /// SLOWLOG RESET：不接受附加参数，清空并回复 +OK。
  pub fn network_slow_log_reset(
    arg_count: usize,
    container: Option<&SlowLogContainer>,
    output: &mut String,
  ) -> Result<(), String> {
    if arg_count != 0 {
      return Err(wrong_num_args("slowlog reset"));
    }
    if let Some(container) = container {
      container.clear();
    }
    output.push_str(&RespWriteUtils::simple_string("OK"));
    Ok(())
  }

  /// libs/server/Metrics/Slowlog/RespSlowlogCommands.cs:HandleSlowLog
  ///
  /// 命令耗时超阈值时入库慢日志并推进起始时间戳（批次内逐命令跟踪）。
  /// `ctx.now_ticks` 为当前 Stopwatch tick，`slow_log_start_time` 为本批起始
  /// tick，`slow_log_threshold` 为阈值（tick）；仅有效命令（非 INVALID）被
  /// 跟踪。`arguments` 为解析状态快照（对齐 C# SerializeTo 路径，序列化属
  /// 会话域）。
  pub fn handle_slow_log(ctx: &SlowLogContext<'_>, slow_log_start_time: &mut i64) {
    let elapsed = ctx.now_ticks - *slow_log_start_time;

    // 仅跟踪有效命令。
    if ctx.cmd != RespCommand::Invalid && elapsed > ctx.slow_log_threshold {
      let entry = SlowLogEntry {
        id: 0,
        timestamp: (ctx.now_ticks / time_stamp::TICKS_PER_SECOND_UNIT as i64) as i32,
        command: ctx.cmd,
        duration: (elapsed / time_stamp::TICKS_PER_MICROSECOND as i64) as i32,
        client_ip_port: ctx.client_ip_port.into(),
        client_name: ctx.client_name.into(),
        arguments: ctx.arguments.clone(),
      };
      ctx.container.add(entry);
    }

    // 推进起始时间戳，跟踪批次内的下一命令。
    *slow_log_start_time = ctx.now_ticks;
  }
}

/// C# AbortWithWrongNumberOfArguments 的错误串。
fn wrong_num_args(cmd_name: &str) -> String {
  GENERIC_ERR_WRONG_NUM_ARGS.replace("{0}", cmd_name)
}

/// ASCII 十进制整数解析（对齐 parseState.TryGetInt 的严格语义）。
fn parse_i32(arg: &[u8]) -> Option<i32> {
  str::from_utf8(arg).ok()?.parse::<i32>().ok()
}

/// 解析状态快照 → 参数序列（安全版 DeserializeFrom）。
/// 布局：`[count i32][每参数 4B 长度前缀 + 数据]`；截断即止。
fn deserialize_args(bytes: &[u8]) -> Vec<Vec<u8>> {
  let mut tokens = Vec::new();
  let Some(count_bytes) = bytes.first_chunk::<4>() else {
    return tokens;
  };
  let count = i32::from_le_bytes(*count_bytes).max(0) as usize;
  let mut cursor = &bytes[4..];
  for _ in 0..count {
    let Some(len_bytes) = cursor.first_chunk::<4>() else {
      break;
    };
    let len = i32::from_le_bytes(*len_bytes).max(0) as usize;
    cursor = &cursor[4..];
    if cursor.len() < len {
      break;
    }
    tokens.push(cursor[..len].to_vec());
    cursor = &cursor[len..];
  }
  tokens
}

#[cfg(test)]
mod tests {
  use super::{RespSlowlogCommands, SlowLogContext, deserialize_args};
  use crate::{
    metrics::slowlog::{slow_log_container::SlowLogContainer, slowlog_entry::SlowLogEntry},
    types::RespCommand,
  };

  fn entry(id: i32) -> SlowLogEntry {
    SlowLogEntry {
      id,
      timestamp: 1700000000,
      duration: 250,
      command: RespCommand::Get,
      arguments: None,
      client_ip_port: "127.0.0.1:7000".into(),
      client_name: "test".into(),
    }
  }

  #[test]
  fn help_renders() {
    let mut out = String::new();
    assert_eq!(
      RespSlowlogCommands::network_slow_log_help(0, &mut out),
      Ok(())
    );
    assert!(out.starts_with("*12\r\n"));
    assert!(out.contains("+LEN\r\n"));

    out.clear();
    assert!(RespSlowlogCommands::network_slow_log_help(1, &mut out).is_err());
    assert_eq!(out, "");
  }

  #[test]
  fn get_writes_entries() {
    let log = SlowLogContainer::new(10);
    log.add(entry(0));
    log.add(entry(1));

    let mut out = String::new();
    RespSlowlogCommands::network_slow_log_get(&[], Some(&log), &mut out).unwrap();
    // 默认 count=10：全部返回，每条 6 元素。
    eprintln!("DEBUG_OUT={out:?}");
    assert!(out.starts_with("*2\r\n"));

    out.clear();
    RespSlowlogCommands::network_slow_log_get(&[b"1"], Some(&log), &mut out).unwrap();
    assert!(out.starts_with("*1\r\n*6\r\n:1\r\n"));

    out.clear();
    RespSlowlogCommands::network_slow_log_get(&[b"-2"], Some(&log), &mut out).unwrap();
    assert_eq!(out, "-ERR count should be greater than or equal to -1.\r\n");

    out.clear();
    RespSlowlogCommands::network_slow_log_get(&[], None, &mut out).unwrap();
    assert_eq!(out, "*0\r\n");
  }

  #[test]
  fn len_and_reset() {
    let log = SlowLogContainer::new(10);
    log.add(entry(0));

    let mut out = String::new();
    RespSlowlogCommands::network_slow_log_len(0, Some(&log), &mut out).unwrap();
    assert_eq!(out, ":1\r\n");

    out.clear();
    RespSlowlogCommands::network_slow_log_reset(0, Some(&log), &mut out).unwrap();
    assert_eq!(out, "+OK\r\n");
    assert_eq!(log.count(), 0);

    out.clear();
    assert!(RespSlowlogCommands::network_slow_log_len(1, Some(&log), &mut out).is_err());
    assert!(RespSlowlogCommands::network_slow_log_reset(2, None, &mut out).is_err());
  }

  #[test]
  fn handle_slow_log_tracks_exceeded_commands() {
    let log = SlowLogContainer::new(10);
    let mut start = 1_000_000;

    // 5_000 tick 阈值：2_000 不入库，9_000 入库。
    let ctx = |cmd, now_ticks| SlowLogContext {
      container: &log,
      cmd,
      now_ticks,
      slow_log_threshold: 5_000,
      client_ip_port: "127.0.0.1:1",
      client_name: "n",
      arguments: None,
    };
    RespSlowlogCommands::handle_slow_log(&ctx(RespCommand::Get, 1_002_000), &mut start);
    assert_eq!(log.count(), 0);
    assert_eq!(start, 1_002_000);

    RespSlowlogCommands::handle_slow_log(&ctx(RespCommand::Set, 1_011_000), &mut start);
    assert_eq!(log.count(), 1);
    assert_eq!(start, 1_011_000);

    // INVALID 命令不入库，但起始时间戳仍推进。
    RespSlowlogCommands::handle_slow_log(&ctx(RespCommand::Invalid, 1_021_000), &mut start);
    assert_eq!(log.count(), 1);
    assert_eq!(start, 1_021_000);
  }

  #[test]
  fn args_roundtrip_layout() {
    // [count=2][len=3]"GET"[len=1]"x"
    let bytes: Vec<u8> = [
      2i32.to_le_bytes().as_slice(),
      3i32.to_le_bytes().as_slice(),
      b"GET",
      1i32.to_le_bytes().as_slice(),
      b"x",
    ]
    .concat();
    let tokens = deserialize_args(&bytes);
    assert_eq!(tokens, vec![b"GET".to_vec(), b"x".to_vec()]);
  }
}
