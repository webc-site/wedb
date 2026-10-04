//! 超参数量协议拒绝 + 命令参数错全会话存活（帧级回归）
//!
//! 对标 C# test/standalone/Garnet.test/RespTests.cs：
//! - ExcessiveArgumentCountRejected（:5533）：`*2000000\r\n$3\r\nFOO\r\n`
//!   → `-ERR unknown command\r\n` +
//!   `-ERR Protocol Error: RESP array argument count '1999999' exceeds
//!   maximum allowed count of '1048576'.\r\n` → 发尽错误帧后断连
//!   （wedb 消费面 None 信号即断连对位）；已知命令（PING）同构只出
//!   Protocol Error 单帧。
//! - StringCommandsWrongArityReturnErrorAndKeepSessionAlive（:604）：
//!   每条畸形命令同批尾随 PING，错误帧 + `+PONG` 连发——证明错误不掐会话。
//!
//! 违例文案单源 resp_server_session/parse.rs:violation_excessive_arg_count
//!（对位 RespParsingException.ThrowExcessiveArgumentCount）；帧级直泵
//! RespSessionConsumer（真存储真协议帧，无 mock）。

use std::sync::Arc;

use wnode::{
  RespSessionConsumer,
  resp::{garnet_api::StoreGarnetApi, resp_server_session::RespServerSessionOptions},
};
use wnode_test::pump;
use wtest_base::{open_test_store, resp_frame};

/// RESP 数组参数计数上界（wnode::resp::parser::resp_command 单源同值；
/// 错误帧内 max 字面量即本值十进制）
const MAX_RESP_ARRAY_LENGTH: usize = 1 << 20;

/// 装配带真存储的会话消费者
fn harness(tag: &str) -> RespSessionConsumer {
  let (_dir, store) = open_test_store(tag).unwrap();
  RespSessionConsumer::new(
    1,
    RespServerSessionOptions::default(),
    Arc::new(StoreGarnetApi::new(store.new_session().unwrap())),
  )
}

/// 超参数量数组头被拒（garnet ExcessiveArgumentCountRejected :5533 对位）：
/// - 未知命令形态：unknown command 帧在前、Protocol Error 帧在后（同批
///   累积应答序），count/max 字面量逐字节锁定（count = 数组头 - 命令名）
/// - 已知命令形态（PING）：仅 Protocol Error 单帧
/// - 两形态消费面均回 None（C# Send 后 DisposeNetworkSender 断连对位；
///   测试会话对象续泵不再产出新应答即断连语义的会话级投影）
#[test]
fn excessive_argument_count_rejects_with_protocol_error() {
  // ===== 未知命令形态：*2000000 数组头 + 命令名 FOO
  let mut c = harness("excess-arg-foo.db");
  let (consumed, resp) = pump(&mut c, b"*2000000\r\n$3\r\nFOO\r\n");
  assert_eq!(
    resp,
    b"-ERR unknown command\r\n-ERR Protocol Error: RESP array argument count '1999999' exceeds maximum allowed count of '1048576'.\r\n",
    "超参数组头错误帧须与 garnet 期望逐字节一致（count=数组头-命令名，max=1<<20）"
  );
  assert_eq!(
    consumed, None,
    "超参数组头违例必须回 None（发尽错误帧后断连）"
  );
  // 违例后同会话续命令成功（错误帧已写、会话态未损坏；断连决策归网络泵
  // ——None 信号即泵断连指令，会话对象自身可继续消费，续答 +PONG 钉住
  // 「协议错误不污染会话态」）
  let (after, resp2) = pump(&mut c, b"*1\r\n$4\r\nPING\r\n");
  assert_eq!(
    resp2, b"+PONG\r\n",
    "违例后同会话续命令必须成功（会话态未污染）"
  );
  assert_eq!(after, Some(0), "续命令消费完整");

  // ===== 已知命令形态：PING 同构超限头（garnet :5555 第二段）
  let mut c2 = harness("excess-arg-ping.db");
  let (consumed2, resp3) = pump(&mut c2, b"*2000000\r\n$4\r\nPING\r\n");
  assert_eq!(
    resp3,
    format!(
      "-ERR Protocol Error: RESP array argument count '1999999' exceeds maximum allowed count of '{}'.\r\n",
      MAX_RESP_ARRAY_LENGTH
    )
    .as_bytes(),
    "已知命令超限头仅出 Protocol Error 单帧（max 字面量与常量单源）"
  );
  assert_eq!(consumed2, None, "已知命令超限头同须断连");

  // ===== 边界互补：恰在上界（count == max）不触违例（等待参数而非断连）
  let mut c3 = harness("excess-arg-boundary.db");
  let (consumed3, resp4) = pump(&mut c3, b"*1048577\r\n$4\r\nPING\r\n");
  assert!(
    consumed3.is_some(),
    "count==max 半包等待形态：游标推进等待参数，不触违例断连"
  );
  assert_eq!(resp4, b"", "上界内不写任何错误帧");
}

/// 命令参数错不掐会话（garnet StringCommandsWrongArityReturnErrorAndKeepSessionAlive
/// :604 全臂对位）：每条畸形命令同批尾随 PING，断言「错误帧 + +PONG 连发」
/// ——错误帧逐字节、会话存活由尾随 PONG 证明
#[test]
fn wrong_arity_commands_error_and_keep_session_alive() {
  let mut c = harness("wrong-arity-arms.db");

  // (畸形命令参数, 期望错误帧)；全部同批尾随 PING
  let arms: &[(&[&[u8]], &[u8])] = &[
    (
      &[b"SET", b"k"],
      b"-ERR wrong number of arguments for 'SET' command\r\n",
    ),
    (
      &[b"SET"],
      b"-ERR wrong number of arguments for 'SET' command\r\n",
    ),
    (&[b"SET", b"k", b"v", b"EX"], b"-ERR syntax error\r\n"),
    (&[b"SET", b"k", b"v", b"PX"], b"-ERR syntax error\r\n"),
    (
      &[b"GETSET", b"k"],
      b"-ERR wrong number of arguments for 'GETSET' command\r\n",
    ),
    (
      &[b"GETSET", b"k", b"v", b"extra"],
      b"-ERR wrong number of arguments for 'GETSET' command\r\n",
    ),
    (
      &[b"SETEX", b"k"],
      b"-ERR wrong number of arguments for 'SETEX' command\r\n",
    ),
    (
      &[b"SETEX", b"k", b"10"],
      b"-ERR wrong number of arguments for 'SETEX' command\r\n",
    ),
    (
      &[b"PSETEX", b"k", b"10"],
      b"-ERR wrong number of arguments for 'PSETEX' command\r\n",
    ),
    (
      &[b"SETRANGE", b"k"],
      b"-ERR wrong number of arguments for 'SETRANGE' command\r\n",
    ),
    (
      &[b"SETRANGE", b"k", b"0"],
      b"-ERR wrong number of arguments for 'SETRANGE' command\r\n",
    ),
    (
      &[b"APPEND", b"k"],
      b"-ERR wrong number of arguments for 'APPEND' command\r\n",
    ),
    (
      &[b"GETRANGE", b"k"],
      b"-ERR wrong number of arguments for 'GETRANGE' command\r\n",
    ),
    (
      &[b"GETRANGE", b"k", b"0"],
      b"-ERR wrong number of arguments for 'GETRANGE' command\r\n",
    ),
    (
      &[b"SUBSTR", b"k"],
      b"-ERR wrong number of arguments for 'SUBSTR' command\r\n",
    ),
    (
      &[b"SUBSTR", b"k", b"0"],
      b"-ERR wrong number of arguments for 'SUBSTR' command\r\n",
    ),
  ];
  for (parts, err) in arms {
    // 同批两帧：畸形命令 + PING（garnet SendCommands(cmd, "PING") 形态）
    let mut frame = resp_frame(parts);
    frame.extend_from_slice(&resp_frame(&[b"PING"]));
    let (consumed, resp) = pump(&mut c, &frame);
    let mut expected = err.to_vec();
    expected.extend_from_slice(b"+PONG\r\n");
    assert_eq!(
      resp,
      expected,
      "命令 {:?} 错误帧 + 尾随 PONG 须连发（错误不掐会话）",
      parts
        .iter()
        .map(|p| String::from_utf8_lossy(p))
        .collect::<Vec<_>>()
    );
    assert_eq!(consumed, Some(0), "参数错为命令级错误，消费完整不断连");
  }

  // 良构形态不受扰动（garnet 末段）：SET k v → +OK，GET 读回
  let (consumed, resp) = pump(&mut c, &resp_frame(&[b"SET", b"k", b"v"]));
  assert_eq!(consumed, Some(0));
  assert_eq!(resp, b"+OK\r\n");
  let (_, resp) = pump(&mut c, &resp_frame(&[b"GET", b"k"]));
  assert_eq!(resp, b"$1\r\nv\r\n");
}
