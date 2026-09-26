use std::sync::Arc;

use wmetric::slowlog::{
  resp_slowlog_commands::{RespSlowlogCommands, SlowLogContext},
  slow_log_container::SlowLogContainer,
};
use wresp::command::RespCommand;

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
  let mut out = Vec::new();
  assert!(RespSlowlogCommands::network_slow_log_help(0, &mut out).is_ok());
  // 12 个帮助条目
  assert!(out.starts_with(b"*12\r\n"));
}

/// test/standalone/Garnet.test/RespSlowLogTests.cs:TestSlowLogGet
#[test]
fn test_slow_log_get() {
  let container = SlowLogContainer::new(10);
  let mut out = Vec::new();
  assert!(RespSlowlogCommands::network_slow_log_get(&[], Some(&container), &mut out).is_ok());
  assert_eq!(out, b"*0\r\n");
}

/// test/standalone/Garnet.test/RespSlowLogTests.cs:TestSlowLogGetCount
#[test]
fn test_slow_log_get_count() {
  let container = SlowLogContainer::new(10);
  let mut out = Vec::new();
  assert!(RespSlowlogCommands::network_slow_log_get(&[b"-1"], Some(&container), &mut out).is_ok());
  assert_eq!(out, b"*0\r\n");
}

/// test/standalone/Garnet.test/RespSlowLogTests.cs:TestSlowLogGetWithEntry
#[test]
fn test_slow_log_get_with_entry() {
  let container = SlowLogContainer::new(10);
  let mut out = Vec::new();
  assert!(RespSlowlogCommands::network_slow_log_get(&[b"-1"], Some(&container), &mut out).is_ok());
  assert_eq!(out, b"*0\r\n");

  let slow_log_threshold = 3_000_000;
  let timeout = format!("{}", 0.1 + (slow_log_threshold as f32 / 1_000_000.0));
  let args_bytes = serialize_args(&[b"foo", timeout.as_bytes()]);

  let mut start_time = 0;
  let now_ticks = 4_000_000; // 超出阈值
  let ctx = SlowLogContext {
    container: &container,
    cmd: RespCommand::Blpop,
    ticks_stopwatch: now_ticks,
    slow_log_threshold,
    client_ip_port: "127.0.0.1:6379",
    client_name: "",
    arguments: Some(Arc::new(args_bytes)),
  };
  RespSlowlogCommands::handle_slow_log(ctx, &mut start_time);

  out.clear();
  assert!(RespSlowlogCommands::network_slow_log_get(&[b"-1"], Some(&container), &mut out).is_ok());
  // 包含 1 条 entry，每条 6 个元素
  assert!(out.starts_with(b"*1\r\n*6\r\n:0\r\n"));
  // C# :84/:96 entry.Command.ToString() → 枚举成员 SCREAMING 词形（BLPOP 5 字节）
  assert!(
    String::from_utf8_lossy(&out).contains("$5\r\nBLPOP\r\n"),
    "actual: {:?}",
    String::from_utf8_lossy(&out)
  );
  assert!(String::from_utf8_lossy(&out).contains("$3\r\nfoo\r\n"));
  assert!(String::from_utf8_lossy(&out).contains(&format!(
    "${}\r\n{}\r\n",
    timeout.len(),
    timeout
  )));

  // SLOWLOG RESET
  out.clear();
  assert!(RespSlowlogCommands::network_slow_log_reset(0, Some(&container), &mut out).is_ok());
  assert_eq!(out, b"+OK\r\n");

  out.clear();
  assert!(RespSlowlogCommands::network_slow_log_get(&[b"-1"], Some(&container), &mut out).is_ok());
  assert_eq!(out, b"*0\r\n");
}

/// test/standalone/Garnet.test/RespSlowLogTests.cs 的 TestSlowLogGetWithEntry
/// 的参数入库面补充：快照按所有权 move 进条目（C# :171 `entry.Arguments = args`
/// 一次分配直赋值），逐字节一致且起始戳推进；借用版实现的 `ctx.arguments.clone()`
/// 在按值消费签名下无法编译，move 语义由本用例编译期与运行期双重锁定
#[test]
fn test_slow_log_entry_owns_moved_snapshot() {
  let container = SlowLogContainer::new(10);
  let snapshot = serialize_args(&[b"GET", b"slowkey"]);
  let mut start_time = 1_000u64;
  let ctx = SlowLogContext {
    container: &container,
    cmd: RespCommand::Get,
    ticks_stopwatch: 5_000_000,
    slow_log_threshold: 3_000_000,
    client_ip_port: "127.0.0.1:7100",
    client_name: "cli",
    arguments: Some(Arc::new(snapshot.clone())),
  };
  RespSlowlogCommands::handle_slow_log(ctx, &mut start_time);

  let entries = container.get_entries(-1);
  assert_eq!(entries.len(), 1);
  assert_eq!(
    entries[0].arguments.as_deref().map(Vec::as_slice),
    Some(snapshot.as_slice())
  );
  assert_eq!(entries[0].command, RespCommand::Get);
  assert_eq!(entries[0].client_ip_port, "127.0.0.1:7100");
  assert_eq!(entries[0].client_name, "cli");
  // C# :178 `slowLogStartTime = currentTime`：起始戳推进到本次刻度
  assert_eq!(start_time, 5_000_000);
}

/// 未超阈值路径：上下文按值消费后快照随作用域析构，零入库且起始戳同样推进
///（C# HandleSlowLog 未命中分支仅更新时间戳）
#[test]
fn test_slow_log_fast_command_advances_start_without_entry() {
  let container = SlowLogContainer::new(10);
  let mut start_time = 1_000u64;
  let ctx = SlowLogContext {
    container: &container,
    cmd: RespCommand::Get,
    ticks_stopwatch: 2_000,
    slow_log_threshold: 3_000_000,
    client_ip_port: "127.0.0.1:7100",
    client_name: "",
    arguments: Some(Arc::new(serialize_args(&[b"GET", b"fast"]))),
  };
  RespSlowlogCommands::handle_slow_log(ctx, &mut start_time);
  assert_eq!(container.count(), 0);
  assert_eq!(start_time, 2_000);
}

/// test/standalone/Garnet.test/RespSlowLogTests.cs:TestSlowLogLen
#[test]
fn test_slow_log_len() {
  let container = SlowLogContainer::new(10);
  let mut out = Vec::new();
  assert!(RespSlowlogCommands::network_slow_log_len(0, Some(&container), &mut out).is_ok());
  assert_eq!(out, b":0\r\n");
}

/// test/standalone/Garnet.test/RespSlowLogTests.cs:TestSlowLogReset
#[test]
fn test_slow_log_reset() {
  let container = SlowLogContainer::new(10);
  let mut out = Vec::new();
  assert!(RespSlowlogCommands::network_slow_log_reset(0, Some(&container), &mut out).is_ok());
  assert_eq!(out, b"+OK\r\n");
}

/// 四臂 arity 错误文案令牌：C# AbortWithWrongNumberOfArguments 传
/// nameof(RespCommand.SLOWLOG_*)，经 GenericErrWrongNumArgs 模板得下划线大写名
#[test]
fn test_slow_log_wrong_arity_tokens() {
  let container = SlowLogContainer::new(10);
  let mut out = Vec::new();

  let err = RespSlowlogCommands::network_slow_log_help(1, &mut out).unwrap_err();
  assert_eq!(
    err,
    "ERR wrong number of arguments for 'SLOWLOG_HELP' command"
  );

  let err = RespSlowlogCommands::network_slow_log_get(&[b"1", b"2"], Some(&container), &mut out)
    .unwrap_err();
  assert_eq!(
    err,
    "ERR wrong number of arguments for 'SLOWLOG_GET' command"
  );

  let err = RespSlowlogCommands::network_slow_log_len(1, Some(&container), &mut out).unwrap_err();
  assert_eq!(
    err,
    "ERR wrong number of arguments for 'SLOWLOG_LEN' command"
  );

  let err = RespSlowlogCommands::network_slow_log_reset(1, Some(&container), &mut out).unwrap_err();
  assert_eq!(
    err,
    "ERR wrong number of arguments for 'SLOWLOG_RESET' command"
  );
}

/// C# RespSlowLogTests.cs:82 `AreEqual("BLPOP", args[0])` 契约锁：GET 条目
/// 命令名与协议命令名全等（to_cs_name 单点 SCREAMING 词形，对位 C# 枚举
/// ToString），逐字节锁定杜绝 derive Debug 的 PascalCase 词形回归
#[test]
fn test_slow_log_get_command_name_matches_protocol_name() {
  for (cmd, name) in [
    (RespCommand::Blpop, "BLPOP"),
    (RespCommand::Get, "GET"),
    (RespCommand::Mset, "MSET"),
  ] {
    let container = SlowLogContainer::new(10);
    let mut start_time = 0;
    let ctx = SlowLogContext {
      container: &container,
      cmd,
      ticks_stopwatch: 4_000_000,
      slow_log_threshold: 3_000_000,
      client_ip_port: "127.0.0.1:7100",
      client_name: "",
      arguments: Some(Arc::new(serialize_args(&[b"k"]))),
    };
    RespSlowlogCommands::handle_slow_log(ctx, &mut start_time);

    let mut out = Vec::new();
    assert!(
      RespSlowlogCommands::network_slow_log_get(&[b"-1"], Some(&container), &mut out).is_ok()
    );
    // 条目头三元素（id/timestamp/duration）+ 参数数组首 token 全等锁定
    let expect = format!(
      "*1\r\n*6\r\n:0\r\n:0\r\n:400000\r\n*2\r\n${}\r\n{name}\r\n$1\r\nk\r\n",
      name.len()
    );
    assert!(
      out.starts_with(expect.as_bytes()),
      "{cmd:?} 命令名词形分叉: {:?}",
      String::from_utf8_lossy(&out)
    );
  }
}

/// 大值参数（1MB 量级快照）GET -1 语义锁：应答逐字节含完整载荷（与现行为
/// 全等）；Arc 胶囊使 get_entries 克隆降引用计数浅拷贝（强计数 2 = 容器
/// 原件 + 读出件，零载荷字节复制），锁内时长回归 O(n) 元数据级
#[test]
fn test_slow_log_get_large_payload_semantics_locked() {
  let container = SlowLogContainer::new(10);
  let big = vec![b'x'; 1024 * 1024];
  let snapshot = serialize_args(&[b"SET", b"bigkey", &big]);
  let mut start_time = 0;
  let ctx = SlowLogContext {
    container: &container,
    cmd: RespCommand::Set,
    ticks_stopwatch: 4_000_000,
    slow_log_threshold: 3_000_000,
    client_ip_port: "127.0.0.1:7100",
    client_name: "",
    arguments: Some(Arc::new(snapshot)),
  };
  RespSlowlogCommands::handle_slow_log(ctx, &mut start_time);

  let entries = container.get_entries(-1);
  assert_eq!(entries.len(), 1);
  // 读出面与容器内条目共享同一快照（引用计数浅拷贝，无第二份载荷）
  let args = entries[0].arguments.as_ref().unwrap();
  assert_eq!(Arc::strong_count(args), 2, "get_entries 必须浅拷贝共享快照");

  // RESP 应答结构锁：6 元素条目 + 命令名 + 3 参数，1MB 载荷逐字节完整在帧
  let mut out = Vec::new();
  assert!(RespSlowlogCommands::network_slow_log_get(&[b"-1"], Some(&container), &mut out).is_ok());
  assert!(out.starts_with(b"*1\r\n*6\r\n:0\r\n"));
  assert!(String::from_utf8_lossy(&out).contains("$3\r\nSET\r\n"));
  assert!(String::from_utf8_lossy(&out).contains("$6\r\nbigkey\r\n"));
  assert!(
    out.windows(1024 * 1024).any(|w| w == big.as_slice()),
    "大值载荷必须逐字节完整出现在应答帧中"
  );
}
