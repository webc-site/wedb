//! 慢日志条目 id 位宽对标测试
//!（对标 libs/server/Metrics/Slowlog/SlowLogContainer.cs:Add 的
//! Interlocked.Increment 起号与 RespSlowlogCommands.cs:NetworkSlowLogGet 的
//! id 写出；C# 用例参照 test/standalone/Garnet.test/RespSlowLogTests.cs:
//! TestSlowLogGetWithEntry 的 entry[0]==Id 断言口径。C# 计数器与字段同为
//! int、2^31 自然回绕负值系原型缺陷，rust 计数器为 AtomicI64，本文件钉死
//! 越界不回绕、非负单调、RESP 全值写出）
//!
//! 自研依据: SLOWLOG ID 位宽（C# 对应 RespSlowLogTests.cs）

use wmetric::{RespSlowlogCommands, SlowLogContainer, SlowLogEntry};
use wresp::command::RespCommand;

fn entry() -> SlowLogEntry {
  SlowLogEntry {
    id: 0,
    timestamp: 1000,
    duration: 42,
    command: RespCommand::Get,
    arguments: None,
    client_ip_port: "127.0.0.1:1234".into(),
    client_name: "cli".into(),
  }
}

/// i32::MAX 越界起号：跨 21 亿后 id 不回绕、不为负，连续递增。
#[test]
fn id_crosses_i32_max_without_wraparound() {
  let container = SlowLogContainer::new(128);
  // 预置到 i32 边界前一刻：as i32 截断形态下第 3 条起回绕为负
  let boundary_start = i64::from(i32::MAX) - 1;
  container.seed_id(boundary_start);
  for _ in 0..4 {
    container.add(entry());
  }
  let ids: Vec<i64> = container.get_entries(-1).iter().map(|e| e.id).collect();
  assert_eq!(
    ids,
    vec![
      boundary_start,
      i64::from(i32::MAX),
      i64::from(i32::MAX) + 1,
      i64::from(i32::MAX) + 2,
    ]
  );
  assert!(ids.iter().all(|&id| id >= 0), "id 必须非负: {ids:?}");
}

/// RESP 写出全值 id：超 i32 的 id 以完整 64 位整数帧回客户端。
#[test]
fn resp_get_writes_full_64bit_id() {
  let container = SlowLogContainer::new(128);
  container.seed_id(i64::from(i32::MAX) + 1);
  container.add(entry());

  let mut out = Vec::new();
  assert!(RespSlowlogCommands::network_slow_log_get(&[b"-1"], Some(&container), &mut out).is_ok());
  // 1 条目 × 6 元素帧头后紧跟全值 id（截断形态下此处为负数帧）
  assert!(
    out.starts_with(b"*1\r\n*6\r\n:2147483648\r\n"),
    "RESP 输出: {out:?}"
  );
}
