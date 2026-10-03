//! MRU 命令缓存参数计数边界（对标 C# test/standalone/Garnet.test/Resp/
//! RespCommandCacheTests.cs:CachedCommandDoesNotTruncateArgumentCount）
//!
//! C# 四档参数计数（255/256/259/600）验证会话 MRU 双槽命令缓存
//!（_cachedCmd0/1，rust 对位 wnode/src/resp/parser/resp_command.rs 的
//! `MruCommandCache`，由 wnode/src/resp/resp_server_session/core.rs:543 的
//! 会话槽 `mru_cache` 消费）绝不截断参数计数：
//!
//! - 255 档（u8 上界内）：首命令经慢路径哈希查表解析后入缓存，第二命令与
//!   首命令共享同一定长 16B 命令名窗口（数组头 `*256` 逐字节一致），走 MRU
//!   命中臂按缓存计数解析——计数须原样保真；
//! - 256/259/600 档（超 u8）：`update_command_cache` 必须整体跳过缓存而非
//!   `as u8` 截断。若被截断，第二命令命中槽内陈旧计数后提前收束解析，剩余
//!   参数字节（含 C# 在 `argCount & 0xFF` 下标注入的 PING 形态参数）被误当
//!   后续命令解析，应答流必然混入错误帧。
//!
//! 四档均以 DEL 逐字节断言 `:0\r\n`（键不存在，删除计数 0），尾随 PING 钉住
//! 会话存活与解析位完整性。C# 经真服务器套接字断言；rust 侧 `MruCommandCache`
//! 为 pub(crate)，可观测面即 RESP 会话长命令行为，与 C# 等价。

use std::sync::Arc;

use wnode::{
  RespSessionConsumer,
  resp::{garnet_api::StoreGarnetApi, resp_server_session::RespServerSessionOptions},
};
use wnode_test::pump;
use wtest_base::open_test_store;

/// C# 注入参数形态：`Resp("PING")` 的完整 RESP 帧字节（作为 bulk string
/// 载荷注入，落点下标 = argCount & 0xFF）
const PING_PAYLOAD: &[u8] = b"*1\r\n$4\r\nPING\r\n";

/// 装配带真存储的会话消费者（DEL 经存储执行域出 `:0` 应答）
fn harness(tag: &str) -> RespSessionConsumer {
  let (_dir, store) = open_test_store(tag).unwrap();
  RespSessionConsumer::new(
    1,
    RespServerSessionOptions::default(),
    Arc::new(StoreGarnetApi::new(store.new_session().unwrap())),
  )
}

/// C# `Resp(command, arguments)` + `CreateArguments(count, pingIndex)` 单源：
/// 数组头 `*(参数数+1)` + 命令名 DEL + 逐参 bulk string；`ping_index` 档参数
/// 载荷替换为完整 PING 帧字节（None = 不注入，对位 C# 缺省 pingIndex = -1）
fn del_frame(arg_count: usize, ping_index: Option<usize>) -> Vec<u8> {
  let mut frame = format!("*{}\r\n$3\r\nDEL\r\n", arg_count + 1).into_bytes();
  for i in 0..arg_count {
    let payload: Vec<u8> = if ping_index == Some(i) {
      PING_PAYLOAD.to_vec()
    } else {
      format!("arg{i}").into_bytes()
    };
    frame.extend_from_slice(format!("${}\r\n", payload.len()).as_bytes());
    frame.extend_from_slice(&payload);
    frame.extend_from_slice(b"\r\n");
  }
  frame
}

/// 双命令往返（C# SendAsync 两次同构）：两帧数组头字节数一致，共享同一定长
/// 16B 命令名窗口；第二命令无论走 MRU 命中臂（255 档）还是慢路径重解析臂
///（256/259/600 档），都必须按真实参数计数解析出整帧 `:0\r\n`
fn assert_arg_count_roundtrip(arg_count: usize) {
  let mut c = harness(&format!("mru-arg-count-{arg_count}.db"));

  // 首命令：慢路径哈希查表解析（255 档自此填入 MRU 槽 0）
  let (consumed, resp) = pump(&mut c, &del_frame(arg_count, None));
  assert_eq!(consumed, Some(0), "首命令须整帧消费");
  assert_eq!(resp, b":0\r\n", "首命令 DEL 应答须逐字节为 :0");

  // 第二命令：注入下标 argCount & 0xFF（C# 同参）。若计数被截断入缓存，
  // 命中臂提前收束解析，注入参数错位成帧，应答必混错误帧或残留半帧
  let (consumed, resp) = pump(&mut c, &del_frame(arg_count, Some(arg_count & 0xFF)));
  assert_eq!(
    consumed,
    Some(0),
    "第二命令须整帧消费（计数截断即残留半帧）"
  );
  assert_eq!(
    resp, b":0\r\n",
    "第二命令 DEL 应答须逐字节为 :0（MRU 命中与慢路径都不得截断参数计数）"
  );

  // 尾随 PING（对位 C# ECHO response-complete 哨兵）：解析位完整、会话存活
  let (_, resp) = pump(&mut c, b"*1\r\n$4\r\nPING\r\n");
  assert_eq!(resp, b"+PONG\r\n", "两轮长命令后 PING 须正常应答");
}

/// 255 档：u8 上界内，第二命令经 MRU 命中臂按缓存计数解析
#[test]
fn cached_command_does_not_truncate_argument_count_255() {
  assert_arg_count_roundtrip(255);
}

/// 256 档：超 u8 一位，缓存必须整体跳过（截断为 0 即 DEL 空参错全帧）
#[test]
fn cached_command_does_not_truncate_argument_count_256() {
  assert_arg_count_roundtrip(256);
}

/// 259 档：截断为 3 即首 3 参后注入 PING 参数错位成帧
#[test]
fn cached_command_does_not_truncate_argument_count_259() {
  assert_arg_count_roundtrip(259);
}

/// 600 档：远超 u8，截断为 88 即中段参数错位成帧
#[test]
fn cached_command_does_not_truncate_argument_count_600() {
  assert_arg_count_roundtrip(600);
}
