//! 慢日志解析状态快照畸形输入锁测（SLOWLOG GET 出口零 panic、输出有界）
//!
//! 对位 C# test/standalone/Garnet.test/RespReadUtilsTests.cs:298-386 的
//! TryRead* 截断/超界判定口径：快照布局 `[count i32][每参数 4B 长度前缀 +
//! 数据]`（对齐 SessionParseState.SerializeTo）在入库路径可能被截断或被
//! 恶意值污染（AOF/副本流回放、内存改写），SLOWLOG GET 读出面对三类畸形
//! —— 截断头 / 负 count / 负 len —— 必须安全降级（截断即止、负值钳零），
//! 不得 panic、不得以负长度切出越界切片。本文件钉死三类输入的逐字节应答帧。

use std::sync::Arc;

use wmetric::{RespSlowlogCommands, SlowLogContainer, SlowLogEntry};
use wresp::command::RespCommand;

fn entry_with_snapshot(snapshot: &[u8]) -> SlowLogEntry {
  SlowLogEntry {
    id: 0,
    timestamp: 1000,
    duration: 42,
    command: RespCommand::Get,
    arguments: Some(Arc::new(snapshot.to_vec())),
    client_ip_port: "127.0.0.1:1234".into(),
    client_name: "cli".into(),
  }
}

/// count 头截断（不足 4 字节）：零参数降级，条目退化为纯命令名 `*1` 帧。
#[test]
fn truncated_count_header_yields_command_name_only() {
  let container = SlowLogContainer::new(128);
  container.add(entry_with_snapshot(&[0x02, 0x00]));

  let mut out = Vec::new();
  assert!(RespSlowlogCommands::network_slow_log_get(&[b"-1"], Some(&container), &mut out).is_ok());
  // 1 条目 × 6 元素；参数数组臂截断为零 token，仅余命令名
  assert_eq!(
    out,
    b"*1\r\n*6\r\n:0\r\n:1000\r\n:42\r\n*1\r\n$3\r\nGET\r\n$14\r\n127.0.0.1:1234\r\n$3\r\ncli\r\n"
  );
}

/// 负 count（i32 负值头）：钳零后零参数，同样退化为纯命令名 `*1` 帧。
#[test]
fn negative_count_header_yields_command_name_only() {
  let container = SlowLogContainer::new(128);
  container.add(entry_with_snapshot(&(-1_i32).to_le_bytes()));

  let mut out = Vec::new();
  assert!(RespSlowlogCommands::network_slow_log_get(&[b"-1"], Some(&container), &mut out).is_ok());
  assert_eq!(
    out,
    b"*1\r\n*6\r\n:0\r\n:1000\r\n:42\r\n*1\r\n$3\r\nGET\r\n$14\r\n127.0.0.1:1234\r\n$3\r\ncli\r\n"
  );
}

/// 负长度前缀（count=1、首参数 len=-3）：长度钳零切出空 token，
/// 应答为命令名 + 空批量串 `*2` 帧，不越界不 panic。
#[test]
fn negative_length_prefix_yields_empty_token() {
  let container = SlowLogContainer::new(128);
  let mut snapshot = 1_i32.to_le_bytes().to_vec();
  snapshot.extend_from_slice(&(-3_i32).to_le_bytes());
  snapshot.extend_from_slice(b"ab");
  container.add(entry_with_snapshot(&snapshot));

  let mut out = Vec::new();
  assert!(RespSlowlogCommands::network_slow_log_get(&[b"-1"], Some(&container), &mut out).is_ok());
  assert_eq!(out, b"*1\r\n*6\r\n:0\r\n:1000\r\n:42\r\n*2\r\n$3\r\nGET\r\n$0\r\n\r\n$14\r\n127.0.0.1:1234\r\n$3\r\ncli\r\n");
}
