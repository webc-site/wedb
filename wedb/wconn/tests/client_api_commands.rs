//! GarnetClient 客户端命令面集成测试（RESP 命令往返）
//!
//! 对标 C# test/standalone/Garnet.test/GarnetClientTests.cs 的代表性命令族：
//! SimpleStringArrayTest / SimpleNoArgsTest / SimpleIncrTest / SimpleDecrTest /
//! CanUseSetNxStringResultAsync / CanUseMGetTests（MSET/MGET）/ CanDoBulkDeleteTests
//! （DEL）/ 列表族（RespList client tests）/ 有序集合族（RespSortedSet client
//! tests）/ 空数组应答（ShouldNotThrowExceptionForEmptyArrayResponseAsync 的
//! 数组臂补面，标量臂已由 client_features.rs 承担）。
//!
//! rust 侧形态差异（行为等价设计）：
//! - C# 40+ 便捷封装（SET/GET/INCR… 逐命令方法）在本仓已裁并——r7c 裁决后
//!   api.rs 只存留生产存活面（replica_of），同覆盖改走 client.rs 三个底层
//!   执行口（execute_for_string_result_async / execute_for_bytes_result_async /
//!   execute_for_string_array_result_async）+ execute_no_response_async，本册
//!   即按该既有 API 风格挑 14 个代表性命令逐个往返；
//! - C# SetGetWithCallback 的 callback 完成形态在 rust 以 async Result 承接
//!   （await 即完成回调），无第二套注册机制；
//! - 假服务端逐帧分派（RESP2 数组帧切分后按命令名应答），应答形态对齐
//!   Garnet 真实应答（+OK / bulk / 整数 / 数组 / 空数组 `*0`）；FLUSHDB 走
//!   发出即忘口（协议约定无应答），假端对其静默——滞留应答会错位后续帧配对。

use std::{str::from_utf8, time::Duration};

use aok::{OK, Void};
use compio::{
  BufResult,
  io::{AsyncRead, AsyncWriteExt},
  net::TcpListener,
  runtime::spawn,
  time::timeout,
};
use wconn::client::GarnetClient;

/// 解析缓冲中首个完整 RESP2 数组帧：返回 (帧总字节数, 全部参数切片)，
/// 不完整返回 None（RESP2 数组 + bulk string 元素的最小切分骨架）
fn try_parse_frame_args(buf: &[u8]) -> Option<(usize, Vec<&[u8]>)> {
  if buf.first() != Some(&b'*') {
    return None;
  }
  let header_end = buf.iter().position(|b| *b == b'\n')? + 1;
  let argc: usize = from_utf8(&buf[1..header_end - 2]).ok()?.parse().ok()?;
  let mut pos = header_end;
  let mut args = Vec::with_capacity(argc);
  for _ in 0..argc {
    if buf.get(pos) != Some(&b'$') {
      return None;
    }
    let len_line_end = buf[pos + 1..].iter().position(|b| *b == b'\n')? + pos + 2;
    let len: usize = from_utf8(&buf[pos + 1..len_line_end - 2])
      .ok()?
      .parse()
      .ok()?;
    let end = len_line_end + len;
    if end + 2 > buf.len() {
      return None;
    }
    args.push(&buf[len_line_end..end]);
    pos = end + 2;
  }
  Some((pos, args))
}

/// 单帧应答分派（按命令名，派发序即前缀序：长名命令先于其子串短名判定）
fn reply_for(args: &[&[u8]]) -> Option<&'static [u8]> {
  let req_is = |name: &[u8]| args.first().is_some_and(|a| a.eq_ignore_ascii_case(name));
  // 发出即忘（execute_no_response_async）：协议约定无应答，静默不答
  if req_is(b"FLUSHDB") {
    return None;
  }
  let resp: &[u8] = if req_is(b"REPLICAOF") {
    b"+OK\r\n"
  } else if req_is(b"SETNX") {
    // 对标 CanUseSetNxStringResultAsync：整数应答
    b":1\r\n"
  } else if req_is(b"MSET") {
    b"+OK\r\n"
  } else if req_is(b"MGET") {
    b"*2\r\n$2\r\nv1\r\n$2\r\nv2\r\n"
  } else if req_is(b"LPUSH") {
    b":1\r\n"
  } else if req_is(b"RPUSH") {
    b":2\r\n"
  } else if req_is(b"LRANGE") {
    b"*2\r\n$1\r\na\r\n$1\r\nb\r\n"
  } else if req_is(b"LLEN") {
    b":2\r\n"
  } else if args.iter().any(|a| *a == *b"zempty") {
    // 空数组应答（`*0`）：对标 ShouldNotThrowExceptionForEmptyArrayResponseAsync
    // 的数组臂——空结果解析不抛异常（KEYS 标量/数组臂已由 client_features.rs
    // 承担，此处换有序集合空集命令同型钉住）
    b"*0\r\n"
  } else if req_is(b"ZRANGE") {
    b"*2\r\n$2\r\nm1\r\n$2\r\nm2\r\n"
  } else if req_is(b"ZADD") {
    b":1\r\n"
  } else if req_is(b"ZCARD") {
    b":2\r\n"
  } else if req_is(b"ZSCORE") {
    b"$3\r\n0.5\r\n"
  } else if req_is(b"INCRBY") {
    b":106\r\n"
  } else if req_is(b"INCR") {
    // 对标 SimpleIncrTest：整数应答
    b":101\r\n"
  } else if req_is(b"DECRBY") {
    b":100\r\n"
  } else if req_is(b"DECR") {
    // 对标 SimpleDecrTest：整数应答
    b":105\r\n"
  } else if req_is(b"APPEND") || req_is(b"STRLEN") {
    b":4\r\n"
  } else if req_is(b"EXISTS") {
    b":1\r\n"
  } else if req_is(b"DEL") {
    // 对标 CanDoBulkDeleteTests：删除计数应答
    b":1\r\n"
  } else if args.iter().any(|a| *a == *b"bink") {
    // 二进制安全 bulk：非 UTF-8 字节原样承载（bytes 执行口形态）
    b"$5\r\n\xFF\x00\x01\xFE\x7F\r\n"
  } else if req_is(b"PING") {
    b"+PONG\r\n"
  } else if req_is(b"SET") {
    b"+OK\r\n"
  } else if req_is(b"GET") {
    b"$3\r\nbar\r\n"
  } else {
    b"+OK\r\n"
  };
  Some(resp)
}

/// 假应答循环：读块累积 → RESP2 数组帧切分 → 逐帧分派应答
/// （FLUSHDB 静默不答；余帧写回各自应答，帧序严格对齐）
async fn serve_fake<S>(mut sock: S)
where
  S: AsyncRead + AsyncWriteExt,
{
  let mut acc: Vec<u8> = Vec::new();
  let mut buf = vec![0u8; 8192];
  loop {
    let BufResult(res, next) = sock.read(buf).await;
    buf = next;
    let n = match res {
      Ok(n) if n > 0 => n,
      _ => break,
    };
    acc.extend_from_slice(&buf[..n]);
    while let Some((frame_len, args)) = try_parse_frame_args(&acc) {
      if let Some(resp) = reply_for(&args) {
        let BufResult(res, _) = sock.write_all(resp.to_vec()).await;
        if res.is_err() {
          return;
        }
      }
      acc.drain(..frame_len);
    }
  }
}

/// 字符串族 + 计数族 + 列表族 + 有序集合族 + 删除族往返（命令逐一 await，
/// 应答按帧序严格配对）
#[compio::test]
async fn client_command_roundtrips_across_three_execute_shapes() -> Void {
  let listener = TcpListener::bind("127.0.0.1:0").await?;
  let addr = listener.local_addr()?.to_string();
  spawn(async move {
    while let Ok((sock, _)) = listener.accept().await {
      spawn(serve_fake(sock)).detach();
    }
  })
  .detach();

  let mut client = GarnetClient::new(addr, None, None, None, 16, 0)?;
  client.connect_async().await?;
  assert!(client.is_connected(), "建连后客户端应处于已连态");

  // ── 字符串族（str 执行口）──
  assert_eq!(
    client
      .execute_for_string_result_async(&["SET", "foo", "v0"])
      .await?,
    "OK",
    "SET 应答 +OK"
  );
  assert_eq!(
    client
      .execute_for_string_result_async(&["GET", "foo"])
      .await?,
    "bar",
    "GET 应答 bulk 载荷"
  );
  assert_eq!(
    client
      .execute_for_string_result_async(&["SETNX", "nx", "1"])
      .await?,
    "1",
    "SETNX 应答整数 1"
  );
  assert_eq!(
    client
      .execute_for_string_result_async(&["EXISTS", "foo"])
      .await?,
    "1",
    "EXISTS 应答整数 1"
  );

  // ── 计数族（对标 SimpleIncrTest / SimpleDecrTest；应答为整数行）──
  assert_eq!(
    client
      .execute_for_string_result_async(&["INCR", "counter"])
      .await?,
    "101",
    "INCR 应答整数"
  );
  assert_eq!(
    client
      .execute_for_string_result_async(&["INCRBY", "counter", "5"])
      .await?,
    "106",
    "INCRBY 应答整数"
  );
  assert_eq!(
    client
      .execute_for_string_result_async(&["DECR", "counter"])
      .await?,
    "105",
    "DECR 应答整数"
  );
  assert_eq!(
    client
      .execute_for_string_result_async(&["DECRBY", "counter", "5"])
      .await?,
    "100",
    "DECRBY 应答整数"
  );
  assert_eq!(
    client
      .execute_for_string_result_async(&["APPEND", "ap", "val"])
      .await?,
    "4",
    "APPEND 应答长度整数"
  );
  assert_eq!(
    client
      .execute_for_string_result_async(&["STRLEN", "ap"])
      .await?,
    "4",
    "STRLEN 应答长度整数"
  );

  // ── 字节口：二进制安全 bulk 原样承载（对标 bytes 形执行口）──
  assert_eq!(
    client
      .execute_for_bytes_result_async(&[b"GET", b"bink"])
      .await?,
    vec![0xFF, 0x00, 0x01, 0xFE, 0x7F],
    "bytes 口应原样取回非 UTF-8 bulk 载荷"
  );

  // ── 多键族（对标 CanUseMGetTests：MSET 写 + MGET 数组读）──
  assert_eq!(
    client
      .execute_for_string_result_async(&["MSET", "k1", "v1", "k2", "v2"])
      .await?,
    "OK",
    "MSET 应答 +OK"
  );
  assert_eq!(
    client
      .execute_for_string_array_result_async(&["MGET", "k1", "k2"])
      .await?,
    vec!["v1".to_string(), "v2".to_string()],
    "MGET 应答字符串数组"
  );

  // ── 列表族（对标 RespList client tests 的 push/range/len 面）──
  assert_eq!(
    client
      .execute_for_string_result_async(&["LPUSH", "list", "a"])
      .await?,
    "1",
    "LPUSH 应答长度"
  );
  assert_eq!(
    client
      .execute_for_string_result_async(&["RPUSH", "list", "b"])
      .await?,
    "2",
    "RPUSH 应答长度"
  );
  assert_eq!(
    client
      .execute_for_string_array_result_async(&["LRANGE", "list", "0", "-1"])
      .await?,
    vec!["a".to_string(), "b".to_string()],
    "LRANGE 应答两元素数组"
  );
  assert_eq!(
    client
      .execute_for_string_result_async(&["LLEN", "list"])
      .await?,
    "2",
    "LLEN 应答长度"
  );

  // ── 有序集合族（对标 RespSortedSet client tests 的 add/card/score/range 面）──
  assert_eq!(
    client
      .execute_for_string_result_async(&["ZADD", "z", "0.5", "m1"])
      .await?,
    "1",
    "ZADD 应答新增数"
  );
  assert_eq!(
    client
      .execute_for_string_result_async(&["ZCARD", "z"])
      .await?,
    "2",
    "ZCARD 应答基数"
  );
  assert_eq!(
    client
      .execute_for_string_result_async(&["ZSCORE", "z", "m1"])
      .await?,
    "0.5",
    "ZSCORE 应答分值 bulk"
  );
  assert_eq!(
    client
      .execute_for_string_array_result_async(&["ZRANGE", "z", "0", "-1"])
      .await?,
    vec!["m1".to_string(), "m2".to_string()],
    "ZRANGE 应答成员数组"
  );

  // ── 空数组应答（`*0`）：数组臂解析为空结果且不抛异常 ──
  assert!(
    client
      .execute_for_string_array_result_async(&["ZRANGE", "zempty", "0", "-1"])
      .await
      .is_ok_and(|v| v.is_empty()),
    "`*0` 空数组应答应解析为空 Vec 且不报错"
  );

  // ── 删除族（对标 CanDoBulkDeleteTests 的 DEL 返回面）──
  assert_eq!(
    client
      .execute_for_string_result_async(&["DEL", "foo"])
      .await?,
    "1",
    "DEL 应答删除数"
  );

  // ── 发出即忘 + api.rs 存活命令面（对标 C# ExecuteNoResponse / ReplicaOf）──
  client
    .execute_no_response_async(&[b"FLUSHDB"])
    .await
    .expect("FLUSHDB 发出即忘不应报错");
  // 帧序未错位：紧随其后的带应答命令仍按序认领自身应答
  let pong = timeout(
    Duration::from_secs(5),
    client.execute_for_string_result_async(&["PING"]),
  )
  .await
  .expect("发出即忘后 PING 未结算：应答帧序已错位")
  .expect("PING 应答报错");
  assert_eq!(pong, "PONG", "发出即忘命令不得扰动后续应答配对");
  assert_eq!(
    client.replica_of("127.0.0.1", 6379).await?,
    "OK",
    "REPLICAOF 存活命令面应答 +OK"
  );

  OK
}
