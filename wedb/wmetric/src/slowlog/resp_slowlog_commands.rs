use wbase::{
  convert::{TICKS_PER_SECOND, stopwatch::TICKS_PER_MICROSECOND},
  num::strict_i32,
};
use wresp::{cmd_strings::GENERIC_ERR_WRONG_NUM_ARGS, command::RespCommand, ext::RespVecExt};

use super::{slow_log_container::SlowLogContainer, slowlog_entry::SlowLogEntry};

/// SLOWLOG 命令的响应编码（对标
/// libs/server/Metrics/Slowlog/RespSlowlogCommands.cs，
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
  /// 当前 Stopwatch 刻度（`wbase::time::now_stopwatch_ticks`，单调计时域）。
  pub now_ticks: u64,
  /// 慢日志阈值（tick）。
  pub slow_log_threshold: u64,
  /// 客户端 IP:端口。
  pub client_ip_port: &'a str,
  /// 客户端名。
  pub client_name: &'a str,
  /// 解析状态快照。
  pub arguments: Option<Vec<u8>>,
}

/// C# `CmdStrings.RESP_ERR_COUNT_IS_OUT_OF_RANGE_N1`。
const RESP_ERR_COUNT_IS_OUT_OF_RANGE_N1: &str = "ERR count should be greater than or equal to -1.";

impl RespSlowlogCommands {
  /// libs/server/Metrics/Slowlog/RespSlowlogCommands.cs:NetworkSlowLogHelp
  ///
  /// SLOWLOG HELP：不接受附加参数，输出子命令帮助文本数组。
  pub fn network_slow_log_help(arg_count: usize, output: &mut Vec<u8>) -> Result<(), String> {
    if arg_count != 0 {
      // C# nameof(RespCommand.SLOWLOG_HELP) 逐字对位
      return Err(wrong_num_args("SLOWLOG_HELP"));
    }

    let slow_log_commands = super::resp_slowlog_help::RespSlowlogHelp::get_slow_log_commands();
    output.write_resp_array_len(slow_log_commands.len());
    for command in slow_log_commands {
      output.write_resp_simple_string(command);
    }
    Ok(())
  }

  /// libs/server/Metrics/Slowlog/RespSlowlogCommands.cs:NetworkSlowLogGet
  ///
  /// SLOWLOG GET \[count\]：count 缺省 10，负值仅接受 -1（全部）；
  /// 解析失败或 < -1 时报错。
  pub fn network_slow_log_get(
    args: &[&[u8]],
    container: Option<&SlowLogContainer>,
    output: &mut Vec<u8>,
  ) -> Result<(), String> {
    if args.len() > 1 {
      // C# nameof(RespCommand.SLOWLOG_GET) 逐字对位
      return Err(wrong_num_args("SLOWLOG_GET"));
    }

    let mut count: i32 = 10;
    if let Some(arg) = args.first() {
      let Some(parsed) = parse_i32(arg).filter(|&c| c >= -1) else {
        output.write_resp_error(RESP_ERR_COUNT_IS_OUT_OF_RANGE_N1);
        return Ok(());
      };
      count = parsed;
    }

    let Some(container) = container else {
      output.write_resp_array_len(0);
      return Ok(());
    };

    let entries = container.get_entries(count);
    output.write_resp_array_len(entries.len());
    for entry in &entries {
      // 每条目：id、timestamp、duration、参数数组、client ip:port、client name。
      output.write_resp_array_len(6);
      output.write_resp_int(i64::from(entry.id));
      output.write_resp_int(i64::from(entry.timestamp));
      output.write_resp_int(i64::from(entry.duration));

      let command_name = format!("{:?}", entry.command);
      match &entry.arguments {
        None => {
          output.write_resp_array_len(1);
          output.write_resp_bulk_string(command_name.as_bytes());
        }
        Some(bytes) => {
          // 反序列化解析状态快照（`[count i32][4B 长度前缀 + 数据]` 布局，
          // 对齐 SessionParseState.SerializeTo）。
          let tokens = deserialize_args(bytes);
          output.write_resp_array_len(tokens.len() + 1);
          output.write_resp_bulk_string(command_name.as_bytes());
          for token in &tokens {
            // 二进制安全直写，非 UTF-8 token 零破坏
            output.write_resp_bulk_string(token);
          }
        }
      }

      output.write_resp_bulk_string(entry.client_ip_port.as_bytes());
      output.write_resp_bulk_string(entry.client_name.as_bytes());
    }
    Ok(())
  }

  /// libs/server/Metrics/Slowlog/RespSlowlogCommands.cs:NetworkSlowLogLen
  ///
  /// SLOWLOG LEN：不接受附加参数。
  pub fn network_slow_log_len(
    arg_count: usize,
    container: Option<&SlowLogContainer>,
    output: &mut Vec<u8>,
  ) -> Result<(), String> {
    if arg_count != 0 {
      // C# nameof(RespCommand.SLOWLOG_LEN) 逐字对位
      return Err(wrong_num_args("SLOWLOG_LEN"));
    }
    output.write_resp_int(i64::from(container.map_or(0, SlowLogContainer::count)));
    Ok(())
  }

  /// libs/server/Metrics/Slowlog/RespSlowlogCommands.cs:NetworkSlowLogReset
  ///
  /// SLOWLOG RESET：不接受附加参数，清空并回复 +OK。
  pub fn network_slow_log_reset(
    arg_count: usize,
    container: Option<&SlowLogContainer>,
    output: &mut Vec<u8>,
  ) -> Result<(), String> {
    if arg_count != 0 {
      // C# nameof(RespCommand.SLOWLOG_RESET) 逐字对位
      return Err(wrong_num_args("SLOWLOG_RESET"));
    }
    if let Some(container) = container {
      container.clear();
    }
    output.write_resp_simple_string("OK");
    Ok(())
  }

  /// libs/server/Metrics/Slowlog/RespSlowlogCommands.cs:HandleSlowLog
  ///
  /// 命令耗时超阈值时入库慢日志并推进起始时间戳（批次内逐命令跟踪）。
  /// `ctx.now_ticks` 为当前 Stopwatch 刻度（单调计时域），`slow_log_start_time`
  /// 为本批起始刻度，`slow_log_threshold` 为阈值（tick）；仅有效命令（非
  /// INVALID）被跟踪。`arguments` 为解析状态快照（对齐 C# SerializeTo 路径，
  /// 序列化属会话域）。
  pub fn handle_slow_log(ctx: &SlowLogContext<'_>, slow_log_start_time: &mut u64) {
    // 计时域为同一单调源，正常路径下 now >= start；饱和作差兜住 start 由
    // 他源（延迟监视起始戳）注入的极端情形，禁裸减法回绕成巨值
    let elapsed = ctx.now_ticks.saturating_sub(*slow_log_start_time);

    // 仅跟踪有效命令。
    if ctx.cmd != RespCommand::Invalid && elapsed > ctx.slow_log_threshold {
      let entry = SlowLogEntry {
        id: 0,
        // 刻度因子取 wbase::convert 单点（C# OutputScalingFactor.TimeStampToSeconds /
        // TimeStampToMicroseconds；C# 该字段同为 Stopwatch 刻度换算的秒数，
        // 不引实时域时间戳）
        timestamp: (ctx.now_ticks / TICKS_PER_SECOND as u64) as i32,
        command: ctx.cmd,
        duration: (elapsed / TICKS_PER_MICROSECOND) as i32,
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

/// ASCII 十进制整数解析（单一实现 [`strict_i32`]，对齐 parseState.TryGetInt）。
fn parse_i32(arg: &[u8]) -> Option<i32> {
  strict_i32(arg)
}

/// 解析状态快照 → 参数序列（安全零拷贝版 DeserializeFrom）。
/// 布局：`[count i32][每参数 4B 长度前缀 + 数据]`；截断即止。
fn deserialize_args(bytes: &[u8]) -> Vec<&[u8]> {
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
    tokens.push(&cursor[..len]);
    cursor = &cursor[len..];
  }
  tokens
}
