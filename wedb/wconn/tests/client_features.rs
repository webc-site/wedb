//! 客户端行为面集成测试（对标 C# test/standalone/Garnet.test/GarnetClientTests.cs 两项行为）
//!
//! - 空数组应答 `*0\r\n`（对标 ShouldNotThrowExceptionForEmptyArrayResponseAsync）：
//!   空库 KEYS 的应答经读泵 dispatch_replies → parse_scalar / parse_array 真解析路径
//!   认领，标量臂解析为空串（C# 侧 ExecuteForStringResultAsync 对 *0 返回 null，
//!   rust 标量形态 Result<String> 以空串承接，与 $-1 null bulk 同一映射），数组臂
//!   解析为空结果，全程不抛异常；
//! - 两 socket 并存 PING（对标 MultipleSocketPing）：UDS 与 TCP 两臂各持一条
//!   GarnetClient 连接，先后往返互不串包。C# 该用例的 useTls 仅为参数化维度、
//!   非断言面本身，wconn 测试面无 TLS 测试证书基建，主体行为由明文臂承担。

use std::{path::PathBuf, time::Duration};

use aok::{OK, Void};
use compio::{
  BufResult,
  io::{AsyncRead, AsyncWriteExt},
  net::{TcpListener, UnixListener},
  runtime::spawn,
  time::timeout,
};
use tempfile::TempDir;
use wconn::client::GarnetClient;

/// 帧内容子串匹配（大小写不敏感，命令帧内 bulk string 正文含命令名，无歧义）
fn frame_contains(req: &[u8], needle: &[u8]) -> bool {
  req
    .windows(needle.len())
    .any(|w| w.eq_ignore_ascii_case(needle))
}

/// 假应答循环（TCP / UDS 流共用）：PING 帧回 +PONG，KEYS 帧回 `*0\r\n`
/// （空库 SCAN 族应答形态），其余回 +OK；持续认领同连接后续帧直至对端断开
async fn serve_fake<S>(mut sock: S)
where
  S: AsyncRead + AsyncWriteExt,
{
  let mut buf = vec![0u8; 4096];
  loop {
    let BufResult(res, b) = sock.read(buf).await;
    buf = b;
    let n = match res {
      Ok(n) if n > 0 => n,
      _ => break,
    };
    let req = &buf[..n];
    let resp: &[u8] = if frame_contains(req, b"PING") {
      b"+PONG\r\n"
    } else if frame_contains(req, b"KEYS") {
      b"*0\r\n"
    } else {
      b"+OK\r\n"
    };
    let BufResult(res, _) = sock.write_all(resp.to_vec()).await;
    if res.is_err() {
      break;
    }
  }
}

/// 空数组应答不抛异常且解析为空结果：标量臂收空串（C# null 的 rust 承接形态）、
/// 数组臂收空 Vec，随后 PING 仍正常应答，坐实 *0 帧边界被完整消费、应答流未错位
#[compio::test]
async fn empty_array_reply_parses_as_empty_result() -> Void {
  let listener = TcpListener::bind("127.0.0.1:0").await?;
  let addr = listener.local_addr()?.to_string();
  spawn(async move {
    let (sock, _) = listener.accept().await.expect("绑定后接受连接失败");
    serve_fake(sock).await;
  })
  .detach();

  let mut client = GarnetClient::new(addr, None, None, None, 16, 0)?;
  client.connect_async().await?;

  let keys = client
    .execute_for_string_result_async(&["KEYS", "*"])
    .await?;
  assert_eq!(keys, "", "*0 帧应经标量臂解析为空串，不得抛错");
  let keys_arr = client
    .execute_for_string_array_result_async(&["KEYS", "*"])
    .await?;
  assert!(keys_arr.is_empty(), "*0 帧应经数组臂解析为空结果");

  // 帧边界错位会让本条命令挂起或认领到残留字节，5s 兜底窗内必须结算
  let pong = timeout(
    Duration::from_secs(5),
    client.execute_for_string_result_async(&["PING"]),
  )
  .await
  .expect("PING 未在时限内结算，*0 帧边界疑似错位")
  .expect("PING 应答报错");
  assert_eq!(pong, "PONG");
  OK
}

/// 两 socket 并存 PING 不串包：UDS 客户端先往返，TCP 客户端建连并往返，
/// UDS 客户端再次往返仍得 PONG（对标 C# 时序：第二条 socket 接入不打扰
/// 第一条 socket 的在途与后续应答；TCP 侧补一次往返坐实两臂各自收发）
#[compio::test]
async fn multiple_socket_ping_over_unix_and_tcp() -> Void {
  let dir = TempDir::new().expect("创建临时目录");
  let uds_path: PathBuf = dir.path().join("multi_socket_ping.sock");
  let uds_listener = UnixListener::bind(&uds_path)
    .await
    .expect("绑定假 UDS 监听器");
  let tcp_listener = TcpListener::bind("127.0.0.1:0").await?;
  let tcp_addr = tcp_listener.local_addr()?.to_string();

  spawn(async move {
    while let Ok((sock, _)) = uds_listener.accept().await {
      spawn(serve_fake(sock)).detach();
    }
  })
  .detach();
  spawn(async move {
    while let Ok((sock, _)) = tcp_listener.accept().await {
      spawn(serve_fake(sock)).detach();
    }
  })
  .detach();

  let mut uds_client = GarnetClient::new(
    format!("unix:{}", uds_path.display()),
    None,
    None,
    None,
    16,
    0,
  )?;
  uds_client.connect_async().await?;
  assert_eq!(
    uds_client
      .execute_for_string_result_async(&["PING"])
      .await?,
    "PONG",
    "UDS 臂首次 Ping 应答失真"
  );

  let mut tcp_client = GarnetClient::new(tcp_addr, None, None, None, 16, 0)?;
  tcp_client.connect_async().await?;
  assert_eq!(
    tcp_client
      .execute_for_string_result_async(&["PING"])
      .await?,
    "PONG",
    "TCP 臂并存期间应答失真"
  );
  assert_eq!(
    uds_client
      .execute_for_string_result_async(&["PING"])
      .await?,
    "PONG",
    "第二条 socket 接入后 UDS 臂应答被串扰"
  );
  OK
}
