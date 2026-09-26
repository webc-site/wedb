//! 客户端在途命令超时集成测试（GarnetClient timeout_millis → TimeoutChecker）
//!
//! 假 server 收帧后不回包不断链（假死连接），三断言：
//! - timeout_millis=0 时命令挂起不返回（旋钮关闭零行为变化）；
//! - 设小超时时在途命令按 Error::Timeout 返回；
//! - 超时后 is_connected 翻假且后续命令即刻失败。

use std::{future::pending, time::Duration};

use compio::{
  buf::BufResult,
  io::AsyncRead,
  net::TcpListener,
  runtime::spawn,
  time::{sleep, timeout},
};
use wconn::{Error, client::GarnetClient};

/// 在途超时收场总时限：判成周期 + 读泵限时读粒度（250ms）+ 余量
const TIMEOUT_BUDGET: Duration = Duration::from_secs(5);

/// 假 server：accept 一条连接、读满首帧后永久静默（不回包不关闭）
async fn silent_server() -> String {
  let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
  let addr = listener.local_addr().unwrap().to_string();
  spawn(async move {
    let (mut sock, _) = listener.accept().await.unwrap();
    let buf = vec![0u8; 4096];
    let BufResult(res, _) = sock.read(buf).await;
    assert!(res.unwrap() > 0, "命令帧未到达假 server");
    // 假死：连接保持打开，此后不读不回（TCP 不 RST、对端不收不回）
    pending::<()>().await;
  })
  .detach();
  addr
}

/// 旋钮关闭（timeout_millis=0）：假死连接下命令挂起不返回，外层限时窗
/// 到期证明未收场（判成机制未启用，无拆客户端行为）
#[compio::test]
async fn timeout_disabled_keeps_command_pending() {
  let mut client = GarnetClient::new(silent_server().await, None, None, None, 16, 0).unwrap();
  client.connect_async().await.unwrap();
  assert!(client.is_connected());

  let probe = timeout(
    Duration::from_millis(80),
    client.execute_for_string_result_async(&["GET", "k"]),
  )
  .await;
  assert!(probe.is_err(), "timeout_millis=0 时命令应保持挂起不返回");
}

/// 旋钮开启：在途命令按 Error::Timeout 结算，超时后连接态翻假、
/// 后续命令即刻失败（对标 C# 判成 Dispose 后在途 TCS 全部置错）
#[compio::test]
async fn in_flight_timeout_fails_commands_and_marks_disconnected() {
  let mut client = GarnetClient::new(silent_server().await, None, None, None, 16, 100).unwrap();
  client.connect_async().await.unwrap();
  assert!(client.is_connected());

  let res = timeout(
    TIMEOUT_BUDGET,
    client.execute_for_string_result_async(&["GET", "k"]),
  )
  .await
  .unwrap_or_else(|_| panic!("在途超时未在 {TIMEOUT_BUDGET:?} 内收场"))
  .unwrap_err();
  assert!(
    matches!(res, Error::Timeout),
    "在途命令应按 Error::Timeout 结算，实际 {res:?}"
  );

  // 有界轮询至写泵收场、断连对外可见（存活轮询粒度 250ms，5s 上界）
  for _ in 0..200 {
    if !client.is_connected() {
      break;
    }
    sleep(Duration::from_millis(25)).await;
  }
  assert!(!client.is_connected(), "超时后 is_connected 应翻假");

  let next = client.execute_for_string_result_async(&["GET", "k2"]).await;
  assert!(next.is_err(), "断连后后续命令应即刻失败");
}
