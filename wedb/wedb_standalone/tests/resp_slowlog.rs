use wnode::{
  metrics::slowlog::{
    resp_slowlog_commands::{RespSlowlogCommands, SlowLogContext},
    slow_log_container::SlowLogContainer,
  },
  types::RespCommand,
};

/// 序列化快照参数辅助函数（布局：[count i32][len i32 + data]...）
fn serialize_args(args: &[&[u8]]) -> Vec<u8> {
  let mut bytes = Vec::new();
  bytes.extend_from_slice(&(args.len() as i32).to_le_bytes());
  for arg in args {
    bytes.extend_from_slice(&(arg.len() as i32).to_le_bytes());
    bytes.extend_from_slice(arg);
  }
  bytes
}

/// test/standalone/Garnet.test/RespSlowLogTests.cs:TestSlowLogHelp
#[test]
fn test_slow_log_help() {
  let mut out = String::new();
  assert!(RespSlowlogCommands::network_slow_log_help(0, &mut out).is_ok());
  // 12 个帮助条目
  assert!(out.starts_with("*12\r\n"));
}

/// test/standalone/Garnet.test/RespSlowLogTests.cs:TestSlowLogGet
#[test]
fn test_slow_log_get() {
  let container = SlowLogContainer::new(10);
  let mut out = String::new();
  assert!(RespSlowlogCommands::network_slow_log_get(&[], Some(&container), &mut out).is_ok());
  assert_eq!(out, "*0\r\n");
}

/// test/standalone/Garnet.test/RespSlowLogTests.cs:TestSlowLogGetCount
#[test]
fn test_slow_log_get_count() {
  let container = SlowLogContainer::new(10);
  let mut out = String::new();
  assert!(RespSlowlogCommands::network_slow_log_get(&[b"-1"], Some(&container), &mut out).is_ok());
  assert_eq!(out, "*0\r\n");
}

/// test/standalone/Garnet.test/RespSlowLogTests.cs:TestSlowLogGetWithEntry
#[test]
fn test_slow_log_get_with_entry() {
  let container = SlowLogContainer::new(10);
  let mut out = String::new();
  assert!(RespSlowlogCommands::network_slow_log_get(&[b"-1"], Some(&container), &mut out).is_ok());
  assert_eq!(out, "*0\r\n");

  let slow_log_threshold = 3_000_000;
  let timeout = format!("{}", 0.1 + (slow_log_threshold as f32 / 1_000_000.0));
  let args_bytes = serialize_args(&[b"foo", timeout.as_bytes()]);

  let mut start_time = 0;
  let now_ticks = 4_000_000; // 超出阈值
  let ctx = SlowLogContext {
    container: &container,
    cmd: RespCommand::Blpop,
    now_ticks,
    slow_log_threshold,
    client_ip_port: "127.0.0.1:6379",
    client_name: "",
    arguments: Some(args_bytes),
  };
  RespSlowlogCommands::handle_slow_log(&ctx, &mut start_time);

  out.clear();
  assert!(RespSlowlogCommands::network_slow_log_get(&[b"-1"], Some(&container), &mut out).is_ok());
  // 包含 1 条 entry，每条 6 个元素
  assert!(out.starts_with("*1\r\n*6\r\n:0\r\n"));
  assert!(out.contains("$5\r\nBlpop\r\n"));
  assert!(out.contains("$3\r\nfoo\r\n"));
  assert!(out.contains(&format!("${}\r\n{}\r\n", timeout.len(), timeout)));

  // SLOWLOG RESET
  out.clear();
  assert!(RespSlowlogCommands::network_slow_log_reset(0, Some(&container), &mut out).is_ok());
  assert_eq!(out, "+OK\r\n");

  out.clear();
  assert!(RespSlowlogCommands::network_slow_log_get(&[b"-1"], Some(&container), &mut out).is_ok());
  assert_eq!(out, "*0\r\n");
}

/// test/standalone/Garnet.test/RespSlowLogTests.cs:TestSlowLogLen
#[test]
fn test_slow_log_len() {
  let container = SlowLogContainer::new(10);
  let mut out = String::new();
  assert!(RespSlowlogCommands::network_slow_log_len(0, Some(&container), &mut out).is_ok());
  assert_eq!(out, ":0\r\n");
}

/// test/standalone/Garnet.test/RespSlowLogTests.cs:TestSlowLogReset
#[test]
fn test_slow_log_reset() {
  let container = SlowLogContainer::new(10);
  let mut out = String::new();
  assert!(RespSlowlogCommands::network_slow_log_reset(0, Some(&container), &mut out).is_ok());
  assert_eq!(out, "+OK\r\n");
}
